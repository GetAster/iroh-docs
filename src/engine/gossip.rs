use std::collections::{hash_map, HashMap};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::EndpointId;
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender, JoinOptions},
    net::Gossip,
};
use n0_future::{
    task::{AbortHandle, JoinSet},
    StreamExt,
};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use super::live::{Op, ToLiveActor};
use crate::{actor::SyncHandle, ContentStatus, NamespaceId};

#[derive(Debug)]
struct ActiveState {
    sender: GossipSender,
    abort_handle: AbortHandle,
}

#[derive(Debug)]
pub struct GossipState {
    gossip: Gossip,
    sync: SyncHandle,
    to_live_actor: mpsc::Sender<ToLiveActor>,
    active: HashMap<NamespaceId, ActiveState>,
    active_tasks: JoinSet<(NamespaceId, Result<()>)>,
}

impl GossipState {
    pub fn new(gossip: Gossip, sync: SyncHandle, to_live_actor: mpsc::Sender<ToLiveActor>) -> Self {
        Self {
            gossip,
            sync,
            to_live_actor,
            active: Default::default(),
            active_tasks: Default::default(),
        }
    }

    pub async fn join(&mut self, namespace: NamespaceId, bootstrap: Vec<EndpointId>) -> Result<()> {
        match self.active.entry(namespace) {
            hash_map::Entry::Occupied(mut entry) => {
                if !bootstrap.is_empty() {
                    entry.get_mut().sender.join_peers(bootstrap).await?;
                }
            }
            hash_map::Entry::Vacant(entry) => {
                let sub = self
                    .gossip
                    .subscribe_with_opts(namespace.into(), JoinOptions::with_bootstrap(bootstrap))
                    .await?;

                let (sender, stream) = sub.split();
                let to_live_actor = self.to_live_actor.clone();
                let sync = self.sync.clone();
                let abort_handle = self.active_tasks.spawn(async move {
                    let res = receive_loop(namespace, stream, to_live_actor, sync).await;

                    (namespace, res)
                });
                entry.insert(ActiveState {
                    sender,
                    abort_handle,
                });
            }
        }
        Ok(())
    }

    pub fn quit(&mut self, topic: &NamespaceId) {
        if let Some(state) = self.active.remove(topic) {
            state.abort_handle.abort();
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        for (_, state) in self.active.drain() {
            state.abort_handle.abort();
        }
        self.progress().await
    }

    pub async fn broadcast(&mut self, namespace: &NamespaceId, message: Bytes) {
        if let Some(state) = self.active.get_mut(namespace) {
            state.sender.broadcast(message).await.ok();
        }
    }

    pub async fn broadcast_neighbors(&mut self, namespace: &NamespaceId, message: Bytes) {
        if let Some(state) = self.active.get_mut(namespace) {
            state.sender.broadcast_neighbors(message).await.ok();
        }
    }

    pub fn max_message_size(&self) -> usize {
        self.gossip.max_message_size()
    }

    /// Whether no receive loop is left for [`progress`](Self::progress) to
    /// wait on. The live actor polls `progress` only while this is false,
    /// so it must describe the task set: an `active` entry without a task
    /// made `progress` return at once on every turn of the actor's loop,
    /// which then spun without yielding.
    pub fn is_empty(&self) -> bool {
        self.active_tasks.is_empty()
    }

    /// Progress the internal task queues.
    ///
    /// Returns an error if any of the active tasks panic.
    ///
    /// ## Cancel safety
    ///
    /// This function is fully cancel-safe.
    pub async fn progress(&mut self) -> Result<()> {
        while let Some(res) = self.active_tasks.join_next().await {
            match res {
                // `quit` aborts a loop after removing its namespace; one
                // cancelled otherwise (its runtime shutting down) still has
                // an entry, which would stop a later `join` from starting
                // a new loop.
                Err(err) if err.is_cancelled() => {
                    let id = err.id();
                    self.active.retain(|_, state| state.abort_handle.id() != id);
                }
                Err(err) => return Err(err).context("gossip receive loop panicked"),
                Ok((namespace, res)) => {
                    self.active.remove(&namespace);
                    if let Err(err) = res {
                        warn!(?err, ?namespace, "gossip receive loop failed")
                    }
                }
            }
        }
        Ok(())
    }
}

#[instrument("gossip-recv", skip_all, fields(namespace=%namespace.fmt_short()))]
async fn receive_loop(
    namespace: NamespaceId,
    mut recv: GossipReceiver,
    to_sync_actor: mpsc::Sender<ToLiveActor>,
    sync: SyncHandle,
) -> Result<()> {
    for peer in recv.neighbors() {
        to_sync_actor
            .send(ToLiveActor::NeighborUp { namespace, peer })
            .await?;
    }
    while let Some(event) = recv.try_next().await? {
        match event {
            Event::Lagged => {
                debug!("gossip loop lagged - dropping gossip event");
                continue;
            }
            Event::Received(msg) => {
                let op: Op = postcard::from_bytes(&msg.content)?;
                match op {
                    Op::Put(entry) => {
                        debug!(peer = %msg.delivered_from.fmt_short(), namespace = %namespace.fmt_short(), "received entry via gossip");
                        // Insert the entry into our replica.
                        // If the message was broadcast with neighbor scope, or is received
                        // directly from the author, we assume that the content is available at
                        // that peer. Otherwise we don't.
                        // The download is not triggered here, but in the `on_replica_event`
                        // handler for the `InsertRemote` event.
                        let content_status = match msg.scope.is_direct() {
                            true => ContentStatus::Complete,
                            false => ContentStatus::Missing,
                        };
                        let from = *msg.delivered_from.as_bytes();
                        if let Err(err) = sync
                            .insert_remote(namespace, entry, from, content_status)
                            .await
                        {
                            debug!("ignoring entry received via gossip: {err}");
                        }
                    }
                    Op::ContentReady(hash) => {
                        to_sync_actor
                            .send(ToLiveActor::NeighborContentReady {
                                namespace,
                                node: msg.delivered_from,
                                hash,
                            })
                            .await?;
                    }
                    Op::SyncReport(report) => {
                        to_sync_actor
                            .send(ToLiveActor::IncomingSyncReport {
                                from: msg.delivered_from,
                                report,
                            })
                            .await?;
                    }
                }
            }
            Event::NeighborUp(peer) => {
                to_sync_actor
                    .send(ToLiveActor::NeighborUp { namespace, peer })
                    .await?;
            }
            Event::NeighborDown(peer) => {
                to_sync_actor
                    .send(ToLiveActor::NeighborDown { namespace, peer })
                    .await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use iroh::{endpoint::presets, Endpoint};

    use super::*;

    /// A receive loop cancelled other than by `quit` (as a shutting-down
    /// runtime cancels it) leaves nothing for `progress` to wait on, and the
    /// namespace can be joined again. Before, its `active` entry stayed:
    /// `is_empty` was false while the task set was empty, so the live
    /// actor's `progress` branch completed at once on every iteration and
    /// its loop spun inside one poll, blocking the runtime's shutdown
    /// (seen in Aster's FFI `gateway_identity` test).
    #[tokio::test]
    async fn a_cancelled_receive_loop_leaves_nothing_to_wait_on() -> Result<()> {
        let endpoint = Endpoint::bind(presets::Minimal).await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let sync = SyncHandle::spawn(crate::store::Store::memory(), None, "test".to_string());
        let (tx, _rx) = mpsc::channel(8);
        let mut state = GossipState::new(gossip, sync, tx);
        let namespace = NamespaceId::from(&[7u8; 32]);
        state.join(namespace, vec![]).await?;
        assert!(!state.is_empty());

        state.active[&namespace].abort_handle.abort();
        tokio::time::timeout(Duration::from_secs(10), state.progress()).await??;
        assert!(state.is_empty());
        assert!(!state.active.contains_key(&namespace));

        state.join(namespace, vec![]).await?;
        assert!(!state.is_empty());
        state.shutdown().await?;
        endpoint.close().await;
        Ok(())
    }
}

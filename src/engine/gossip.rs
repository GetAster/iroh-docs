use std::collections::{hash_map, HashMap};

use anyhow::{Context, Result};
use iroh::EndpointId;
use iroh_blobs::Hash;
use iroh_gossip::{
    api::{Event, GossipReceiver, GossipSender, JoinOptions},
    net::Gossip,
    TopicId,
};
use n0_future::{
    task::{AbortHandle, JoinSet},
    StreamExt,
};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use super::live::{Op, ToLiveActor, WarnLimiter};
use crate::{
    actor::{InsertRemoteError, SyncHandle},
    ContentStatus, NamespaceId,
};

/// Suffix hashed together with a namespace id to derive its inline-values
/// topic; see [`values_topic`].
const VALUES_TOPIC_SUFFIX: &[u8] = b"|inline-v1";

/// The derived topic a namespace's inline entry values ride on.
///
/// Entries themselves keep the frozen version-0 wire shape on the namespace
/// topic, so peers that predate inline values keep decoding them; the value
/// rides on this derived topic, and only inline-value-capable peers
/// subscribe to it, so old peers never see the new wire shape while old and
/// new peers keep gossiping entries together across a rolling upgrade. The
/// derivation is a hash of the namespace id and a suffix, so the topic is
/// stable per namespace and distinct from every real namespace.
fn values_topic(namespace: NamespaceId) -> TopicId {
    let mut bytes = [0u8; 32 + VALUES_TOPIC_SUFFIX.len()];
    bytes[..32].copy_from_slice(namespace.as_ref());
    bytes[32..].copy_from_slice(VALUES_TOPIC_SUFFIX);
    TopicId::from_bytes(*Hash::new(bytes).as_bytes())
}

#[derive(Debug)]
struct ActiveState {
    abort_handle: AbortHandle,
}

#[derive(Debug)]
pub struct GossipState {
    gossip: Gossip,
    sync: SyncHandle,
    to_live_actor: mpsc::Sender<ToLiveActor>,
    active: HashMap<NamespaceId, ActiveState>,
    active_tasks: JoinSet<(NamespaceId, Result<()>)>,
    /// Whether the engine runs with inline values enabled: only then is the
    /// derived values topic joined.
    inline_values: bool,
}

impl GossipState {
    pub fn new(
        gossip: Gossip,
        sync: SyncHandle,
        to_live_actor: mpsc::Sender<ToLiveActor>,
        inline_values: bool,
    ) -> Self {
        Self {
            gossip,
            sync,
            to_live_actor,
            active: Default::default(),
            active_tasks: Default::default(),
            inline_values,
        }
    }

    /// Joins a namespace's topics, spawning the receive loop for it.
    ///
    /// Returns the new senders when the namespace was joined fresh, for the
    /// caller to register with the announcer; `None` when the namespace was
    /// already joined (its peers were then just extended).
    pub async fn join(
        &mut self,
        namespace: NamespaceId,
        bootstrap: Vec<EndpointId>,
    ) -> Result<Option<(GossipSender, Option<GossipSender>)>> {
        match self.active.entry(namespace) {
            // The namespace is already joined: the caller extends its peers
            // through the announcer, which holds the sender.
            hash_map::Entry::Occupied(_) => Ok(None),
            hash_map::Entry::Vacant(entry) => {
                let sub = self
                    .gossip
                    .subscribe_with_opts(
                        namespace.into(),
                        JoinOptions::with_bootstrap(bootstrap.clone()),
                    )
                    .await?;

                let (sender, stream) = sub.split();

                let (values_sender, values_recv) = if self.inline_values {
                    let sub = self
                        .gossip
                        .subscribe_with_opts(
                            values_topic(namespace),
                            JoinOptions::with_bootstrap(bootstrap),
                        )
                        .await?;
                    let (sender, recv) = sub.split();
                    (Some(sender), Some(recv))
                } else {
                    (None, None)
                };

                let to_live_actor = self.to_live_actor.clone();
                let sync = self.sync.clone();
                let abort_handle = self.active_tasks.spawn(async move {
                    let res =
                        receive_loop(namespace, stream, values_recv, to_live_actor, sync).await;

                    (namespace, res)
                });
                entry.insert(ActiveState { abort_handle });
                Ok(Some((sender, values_sender)))
            }
        }
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

/// Which of the namespace's two topics a message arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    Entries,
    Values,
}

#[instrument("gossip-recv", skip_all, fields(namespace=%namespace.fmt_short()))]
async fn receive_loop(
    namespace: NamespaceId,
    mut recv: GossipReceiver,
    mut values_recv: Option<GossipReceiver>,
    to_sync_actor: mpsc::Sender<ToLiveActor>,
    sync: SyncHandle,
) -> Result<()> {
    let warn_limiter = WarnLimiter::default();
    for peer in recv.neighbors() {
        to_sync_actor
            .send(ToLiveActor::NeighborUp { namespace, peer })
            .await?;
    }
    loop {
        let (source, event) = match &mut values_recv {
            Some(values_recv) => tokio::select! {
                event = recv.try_next() => (Source::Entries, event),
                event = values_recv.try_next() => (Source::Values, event),
            },
            None => (Source::Entries, recv.try_next().await),
        };
        let event = match event {
            Ok(Some(event)) => event,
            Ok(None) => break,
            Err(err) => return Err(err).context("gossip receive stream failed"),
        };
        match event {
            Event::Lagged => {
                // The subscription's buffer overflowed and the skipped events are gone for
                // good unless a sync round runs. Ask the live actor to re-subscribe (the fresh
                // receive loop re-reports our neighbors) and re-sync.
                to_sync_actor
                    .send(ToLiveActor::GossipLagged { namespace })
                    .await?;
                continue;
            }
            Event::Received(msg) => {
                let op: Op = match postcard::from_bytes(&msg.content) {
                    Ok(op) => op,
                    Err(err) => {
                        // One undecodable message must never end the loop:
                        // that dropped the namespace from the active set
                        // with only a warn and never re-joined it, so one
                        // stray frame from a broken or malicious peer ended
                        // live gossip for the namespace on every listener
                        // until restart.
                        if warn_limiter.allow() {
                            warn!(
                                ?err,
                                namespace = %namespace.fmt_short(),
                                "failed to decode gossip message; skipping"
                            );
                        }
                        continue;
                    }
                };
                match op {
                    Op::Put { entry } => {
                        debug!(peer = %msg.delivered_from.fmt_short(), namespace = %namespace.fmt_short(), "received entry via gossip");
                        #[cfg(test)]
                        super::live::fault::maybe_delay_remote_insert(&namespace).await;
                        // Insert the entry into our replica.
                        // If the message was broadcast with neighbor scope, or is received
                        // directly from the author, we assume that the content is available at
                        // that peer. Otherwise we don't.
                        // The download is not triggered here, but in the `on_replica_event`
                        // handler for the `InsertRemote` event. The entry carries no value:
                        // values ride the derived inline-values topic (see
                        // `ToLiveActor::InlineValue`), and the entry is validated by the sync
                        // actor before any import happens.
                        let content_status = match msg.scope.is_direct() {
                            true => ContentStatus::Complete,
                            false => ContentStatus::Missing,
                        };
                        let from = *msg.delivered_from.as_bytes();
                        let hash = entry.content_hash();
                        match sync
                            .insert_remote(namespace, entry, from, content_status)
                            .await
                        {
                            Ok(()) => {}
                            Err(InsertRemoteError::Validation {
                                failure,
                                retry_after,
                            }) => {
                                // One warning per interval: a reject flood would otherwise log
                                // per entry.
                                if warn_limiter.allow() {
                                    warn!(
                                        peer = %msg.delivered_from.fmt_short(),
                                        namespace = %namespace.fmt_short(),
                                        %hash,
                                        %failure,
                                        "refused remote entry received via gossip"
                                    );
                                }
                                // A future-stamped entry becomes valid once the wall clock has
                                // caught up: re-sync then, or the entry stays missing until
                                // some unrelated trigger.
                                if let Some(retry_after) = retry_after {
                                    to_sync_actor
                                        .send(ToLiveActor::DeferredResync {
                                            namespace,
                                            retry_at: n0_future::time::Instant::now() + retry_after,
                                        })
                                        .await?;
                                }
                            }
                            Err(err) => debug!("ignoring entry received via gossip: {err}"),
                        }
                    }
                    Op::PutValue { hash, value } => {
                        // Forwarded regardless of which topic it arrived on;
                        // the live actor imports it only for wanted content
                        // and verifies it against the hash first.
                        to_sync_actor
                            .send(ToLiveActor::InlineValue {
                                namespace,
                                node: msg.delivered_from,
                                hash,
                                value,
                            })
                            .await?;
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
                // Only the entries topic's neighborhood defines gossip
                // neighbors; the values topic shares the same peers.
                if source == Source::Entries {
                    to_sync_actor
                        .send(ToLiveActor::NeighborUp { namespace, peer })
                        .await?;
                }
            }
            Event::NeighborDown(peer) => {
                if source == Source::Entries {
                    to_sync_actor
                        .send(ToLiveActor::NeighborDown { namespace, peer })
                        .await?;
                }
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
        let mut state = GossipState::new(gossip, sync, tx, false);
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

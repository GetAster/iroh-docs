#![allow(missing_docs)]

use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};

use anyhow::{Context, Result};
use iroh::{address_lookup::memory::MemoryLookup, Endpoint, EndpointAddr, EndpointId, PublicKey};
use iroh_blobs::{
    api::{
        blobs::BlobStatus,
        downloader::{ContentDiscovery, DownloadRequest, Downloader, SplitStrategy},
        Store,
    },
    Hash, HashAndFormat,
};
use iroh_gossip::net::Gossip;
use n0_future::{
    task::{Id as TaskId, JoinError, JoinSet},
    time::{Duration, Instant, SystemTime},
    FutureExt,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{self, mpsc, oneshot};
use tracing::{debug, error, info, instrument, trace, warn, Instrument, Span};

// use super::gossip::{GossipActor, ToGossipActor};
use super::state::{is_tiebreak_loss, NamespaceStates, Origin, SyncReason};
use crate::{
    actor::{OpenOpts, SyncHandle},
    engine::gossip::GossipState,
    metrics::Metrics,
    net::{
        connect_and_sync_with_timeout, handle_connection_with_timeout, AbortReason, AcceptError,
        AcceptOutcome, ConnectError, SyncFinished, DEFAULT_SYNC_INACTIVITY_TIMEOUT,
    },
    AuthorHeads, ContentStatus, NamespaceId, RejectedEntry, SignedEntry,
};

/// One warning per namespace per interval for events that can arrive in floods: validation
/// rejects come one per entry, and a wedged gossip subscription lags one tick after another.
const WARN_LIMITER_INTERVAL: Duration = Duration::from_secs(60);

/// A per-context rate limiter: `allow` returns true once per [`WARN_LIMITER_INTERVAL`].
#[derive(Debug, Default)]
pub(super) struct WarnLimiter {
    last: std::sync::Mutex<Option<Instant>>,
}

impl WarnLimiter {
    /// Whether the interval has passed since the last time this returned true.
    pub(super) fn allow(&self) -> bool {
        let mut last = self.last.lock().expect("warn limiter poisoned");
        let now = Instant::now();
        match *last {
            Some(prev) if now.duration_since(prev) < WARN_LIMITER_INTERVAL => false,
            _ => {
                *last = Some(now);
                true
            }
        }
    }
}

/// Whether an action for `namespace` may proceed: one per [`WARN_LIMITER_INTERVAL`].
fn reaction_allowed(last: &mut HashMap<NamespaceId, Instant>, namespace: NamespaceId) -> bool {
    let now = Instant::now();
    match last.get(&namespace) {
        Some(prev) if now.duration_since(*prev) < WARN_LIMITER_INTERVAL => false,
        _ => {
            last.insert(namespace, now);
            true
        }
    }
}

/// An iroh-docs operation
///
/// This is the message that is broadcast over iroh-gossip.
#[derive(Debug, Clone, Serialize, Deserialize, strum::Display)]
pub enum Op {
    /// A new entry was inserted into the document.
    Put(SignedEntry),
    /// A peer now has content available for a hash.
    ContentReady(Hash),
    /// We synced with another peer, here's the news.
    SyncReport(SyncReport),
}

/// Report of a successful sync with the new heads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncReport {
    namespace: NamespaceId,
    /// Encoded [`AuthorHeads`]
    heads: Vec<u8>,
}

/// Messages to the sync actor
#[derive(derive_more::Debug, strum::Display)]
pub enum ToLiveActor {
    DownloadStatus {
        namespace: NamespaceId,
        hash: Hash,
        reply: sync::oneshot::Sender<super::DownloadStatus>,
    },
    StartSync {
        namespace: NamespaceId,
        peers: Vec<EndpointAddr>,
        #[debug("onsehot::Sender")]
        reply: sync::oneshot::Sender<anyhow::Result<()>>,
    },
    Leave {
        namespace: NamespaceId,
        kill_subscribers: bool,
        #[debug("onsehot::Sender")]
        reply: sync::oneshot::Sender<anyhow::Result<()>>,
    },
    Shutdown {
        reply: sync::oneshot::Sender<()>,
    },
    Subscribe {
        namespace: NamespaceId,
        #[debug("sender")]
        sender: async_channel::Sender<Event>,
        #[debug("oneshot::Sender")]
        reply: sync::oneshot::Sender<Result<()>>,
    },
    HandleConnection {
        conn: iroh::endpoint::Connection,
    },
    AcceptSyncRequest {
        namespace: NamespaceId,
        peer: PublicKey,
        #[debug("oneshot::Sender")]
        reply: sync::oneshot::Sender<AcceptOutcome>,
    },

    IncomingSyncReport {
        from: PublicKey,
        report: SyncReport,
    },
    NeighborContentReady {
        namespace: NamespaceId,
        node: PublicKey,
        hash: Hash,
    },
    NeighborUp {
        namespace: NamespaceId,
        peer: PublicKey,
    },
    NeighborDown {
        namespace: NamespaceId,
        peer: PublicKey,
    },
    /// A remote entry was refused as too far in the future: re-sync once the wall clock has
    /// caught up enough for it to pass validation.
    DeferredResync {
        namespace: NamespaceId,
        retry_at: Instant,
    },
    /// The subscription's buffer overflowed and dropped events: re-subscribe and re-sync, or
    /// the dropped entries stay missing until some later sync trigger.
    GossipLagged {
        namespace: NamespaceId,
    },
}

/// Events informing about actions of the live sync progress.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, strum::Display)]
pub enum Event {
    /// The content of an entry was downloaded and is now available at the local node
    ContentReady {
        /// The content hash of the newly available entry content
        hash: Hash,
    },
    /// We have a new neighbor in the swarm.
    NeighborUp(PublicKey),
    /// We lost a neighbor in the swarm.
    NeighborDown(PublicKey),
    /// A set-reconciliation sync finished.
    SyncFinished(SyncEvent),
    /// All pending content is now ready.
    ///
    /// This event is only emitted after a sync completed and `Self::SyncFinished` was emitted at
    /// least once. It signals that all currently pending downloads have been completed.
    ///
    /// Receiving this event does not guarantee that all content in the document is available. If
    /// blobs failed to download, this event will still be emitted after all operations completed.
    PendingContentReady,
}

type SyncConnectRes = (
    NamespaceId,
    PublicKey,
    SyncReason,
    Result<SyncFinished, ConnectError>,
);
type SyncAcceptRes = Result<SyncFinished, AcceptError>;
type DownloadRes = (NamespaceId, Hash, Result<(), anyhow::Error>);
/// Set by an accept task once it has read which (namespace, peer) it serves.
type AcceptedRequest = Arc<std::sync::OnceLock<(NamespaceId, PublicKey)>>;

const MAX_REPLICA_EVENT_BATCH: usize = 1024;

#[derive(Debug, Clone, Copy)]
struct DownloadCandidate {
    namespace: NamespaceId,
    hash: Hash,
    node: PublicKey,
    only_if_missing: bool,
}

// Currently peers might double-sync in both directions.
pub struct LiveActor {
    /// Receiver for actor messages.
    inbox: mpsc::Receiver<ToLiveActor>,
    sync: SyncHandle,
    endpoint: Endpoint,
    bao_store: Store,
    downloader: Downloader,
    memory_lookup: MemoryLookup,
    replica_events_tx: async_channel::Sender<crate::Event>,
    replica_events_rx: async_channel::Receiver<crate::Event>,

    /// Send messages to self.
    /// Note: Must not be used in methods called from `Self::run` directly to prevent deadlocks.
    /// Only clone into newly spawned tasks.
    sync_actor_tx: mpsc::Sender<ToLiveActor>,
    gossip: GossipState,

    /// Running sync futures (from connect).
    running_sync_connect: JoinSet<SyncConnectRes>,
    /// What each running connect task is for, so a task that panics or is
    /// cancelled (and so returns no result) still releases its peer state.
    running_sync_connect_ids: HashMap<TaskId, (NamespaceId, PublicKey, SyncReason)>,
    /// Running sync futures (from accept).
    running_sync_accept: JoinSet<SyncAcceptRes>,
    /// The (namespace, peer) each running accept task has been asked for,
    /// once its request has been read.
    running_sync_accept_ids: HashMap<TaskId, AcceptedRequest>,
    /// Running download futures.
    download_tasks: JoinSet<DownloadRes>,
    /// What each running download task is for, like `running_sync_connect_ids`.
    download_task_ids: HashMap<TaskId, (NamespaceId, Hash)>,
    /// Content hashes which are wanted but not yet queued because no provider was found.
    missing_hashes: MissingHashes,
    /// Content hashes queued in downloader.
    queued_hashes: QueuedHashes,
    /// Nodes known to have a hash
    hash_providers: ProviderNodes,
    /// Download scheduling candidates collected while draining replica events.
    pending_downloads: Vec<DownloadCandidate>,

    /// Subscribers to actor events
    subscribers: SubscribersMap,

    /// Sync state per replica and peer
    state: NamespaceStates,
    metrics: Arc<Metrics>,
    /// How long a sync exchange may make no progress before it is abandoned.
    sync_inactivity_timeout: Duration,
    /// Current gossip neighbors per namespace, for deferred re-syncs.
    neighbors: HashMap<NamespaceId, HashSet<PublicKey>>,
    /// Earliest re-sync time per namespace for entries refused as too far in the future.
    deferred_resyncs: HashMap<NamespaceId, Instant>,
    /// When we last warned about validation rejects, per namespace.
    validation_warned: HashMap<NamespaceId, Instant>,
    /// When we last reacted to a gossip lag, per namespace.
    lagged_reacted: HashMap<NamespaceId, Instant>,
}
impl LiveActor {
    /// Create the live actor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sync: SyncHandle,
        endpoint: Endpoint,
        gossip: Gossip,
        bao_store: Store,
        downloader: Downloader,
        inbox: mpsc::Receiver<ToLiveActor>,
        sync_actor_tx: mpsc::Sender<ToLiveActor>,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        // Unbounded, and it must stay that way. `Subscribers::send` delivers
        // into this channel with a blocking `tx.send(..).await` from inside the
        // sync actor, so a bounded channel makes the sync actor stop the moment
        // the live actor falls behind. A remote insert burst larger than the
        // bound then wedges: with `bounded(1024)`, syncing a 2000-entry
        // namespace stalled at exactly 1027 delivered events and never
        // recovered. Bound the work this channel *causes* if it needs bounding
        // (see `MAX_REPLICA_EVENT_BATCH`), never the channel itself.
        let (replica_events_tx, replica_events_rx) = async_channel::unbounded();
        let gossip_state = GossipState::new(gossip, sync.clone(), sync_actor_tx.clone());
        let memory_lookup = MemoryLookup::new();
        endpoint.address_lookup()?.add(memory_lookup.clone());
        Ok(Self {
            inbox,
            sync,
            replica_events_rx,
            replica_events_tx,
            endpoint,
            memory_lookup,
            gossip: gossip_state,
            bao_store,
            downloader,
            sync_actor_tx,
            running_sync_connect: Default::default(),
            running_sync_connect_ids: Default::default(),
            running_sync_accept: Default::default(),
            running_sync_accept_ids: Default::default(),
            subscribers: Default::default(),
            download_tasks: Default::default(),
            download_task_ids: Default::default(),
            state: Default::default(),
            missing_hashes: Default::default(),
            queued_hashes: Default::default(),
            hash_providers: Default::default(),
            pending_downloads: Default::default(),
            metrics,
            sync_inactivity_timeout: DEFAULT_SYNC_INACTIVITY_TIMEOUT,
            neighbors: Default::default(),
            deferred_resyncs: Default::default(),
            validation_warned: Default::default(),
            lagged_reacted: Default::default(),
        })
    }

    /// Set how long a sync exchange may make no progress before it is abandoned.
    pub fn set_sync_inactivity_timeout(&mut self, timeout: Duration) {
        self.sync_inactivity_timeout = timeout;
    }

    /// Run the actor loop.
    pub async fn run(mut self) -> Result<()> {
        let shutdown_reply = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "Error during shutdown");
        }
        drop(self);
        match shutdown_reply {
            Ok(reply) => {
                reply.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        let mut i = 0;
        loop {
            i += 1;
            trace!(?i, "tick wait");
            self.metrics.doc_live_tick_main.inc();
            tokio::select! {
                biased;
                msg = self.inbox.recv() => {
                    let msg = msg.context("to_actor closed")?;
                    trace!(?i, %msg, "tick: to_actor");
                    self.metrics.doc_live_tick_actor.inc();
                    match msg {
                        ToLiveActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            self.on_actor_message(msg).await.context("on_actor_message")?;
                        }
                    }
                }
                event = self.replica_events_rx.recv() => {
                    trace!(?i, "tick: replica_event");
                    self.metrics.doc_live_tick_replica_event.inc();
                    let event = event.context("replica_events closed")?;
                    self.on_replica_events(event).await;
                }
                // A sync or download task that panics (or is cancelled) ends
                // that one exchange, not the actor: its peer or hash state is
                // released as a failure, so a later start_sync works. Upstream
                // returned the JoinError, which stopped the actor, gossip and
                // the sync actor, failing every later docs call.
                Some(res) = self.running_sync_connect.join_next_with_id(), if !self.running_sync_connect.is_empty() => {
                    trace!(?i, "tick: running_sync_connect");
                    self.metrics.doc_live_tick_running_sync_connect.inc();
                    match res {
                        Ok((id, (namespace, peer, reason, res))) => {
                            self.running_sync_connect_ids.remove(&id);
                            self.on_sync_via_connect_finished(namespace, peer, reason, res).await;
                        }
                        Err(err) => self.on_sync_connect_task_failed(err).await,
                    }
                }
                Some(res) = self.running_sync_accept.join_next_with_id(), if !self.running_sync_accept.is_empty() => {
                    trace!(?i, "tick: running_sync_accept");
                    self.metrics.doc_live_tick_running_sync_accept.inc();
                    match res {
                        Ok((id, res)) => {
                            self.running_sync_accept_ids.remove(&id);
                            self.on_sync_via_accept_finished(res).await;
                        }
                        Err(err) => self.on_sync_accept_task_failed(err).await,
                    }
                }
                Some(res) = self.download_tasks.join_next_with_id(), if !self.download_tasks.is_empty() => {
                    trace!(?i, "tick: pending_downloads");
                    self.metrics.doc_live_tick_pending_downloads.inc();
                    match res {
                        Ok((id, (namespace, hash, res))) => {
                            self.download_task_ids.remove(&id);
                            self.on_download_ready(namespace, hash, res).await;
                        }
                        Err(err) => self.on_download_task_failed(err).await,
                    }
                }
                res = self.gossip.progress(), if !self.gossip.is_empty() => {
                    if let Err(error) = res {
                        warn!(?error, "gossip state failed");
                    }
                }
                // Entries refused as too far in the future are worth re-syncing once the wall
                // clock has caught up: sleep until the earliest such deadline.
                _ = Self::deferred_resync_timer(&self.deferred_resyncs), if !self.deferred_resyncs.is_empty() => {
                    trace!(?i, "tick: deferred resync");
                    self.fire_deferred_resyncs();
                }
            }
        }
    }

    async fn on_replica_events(&mut self, first: crate::Event) {
        if let Err(err) = self.on_replica_event(first).await {
            error!(?err, "Failed to process replica event");
        }

        for _ in 0..MAX_REPLICA_EVENT_BATCH {
            match self.replica_events_rx.try_recv() {
                Ok(event) => {
                    self.metrics.doc_live_tick_replica_event.inc();
                    if let Err(err) = self.on_replica_event(event).await {
                        error!(?err, "Failed to process replica event");
                    }
                }
                Err(async_channel::TryRecvError::Empty) => break,
                Err(async_channel::TryRecvError::Closed) => break,
            }
        }

        self.flush_pending_downloads().await;
    }

    async fn on_actor_message(&mut self, msg: ToLiveActor) -> anyhow::Result<bool> {
        match msg {
            ToLiveActor::Shutdown { .. } => {
                unreachable!("handled in run");
            }
            ToLiveActor::DownloadStatus {
                namespace,
                hash,
                reply,
            } => {
                let status = self.download_status(namespace, hash);
                let _ = reply.send(status);
            }
            ToLiveActor::IncomingSyncReport { from, report } => {
                self.on_sync_report(from, report).await
            }
            ToLiveActor::NeighborUp { namespace, peer } => {
                debug!(peer = %peer.fmt_short(), namespace = %namespace.fmt_short(), "neighbor up");
                self.neighbors.entry(namespace).or_default().insert(peer);
                self.sync_with_peer(namespace, peer, SyncReason::NewNeighbor);
                self.subscribers
                    .send(&namespace, Event::NeighborUp(peer))
                    .await;
            }
            ToLiveActor::NeighborDown { namespace, peer } => {
                debug!(peer = %peer.fmt_short(), namespace = %namespace.fmt_short(), "neighbor down");
                if let Some(neighbors) = self.neighbors.get_mut(&namespace) {
                    neighbors.remove(&peer);
                }
                self.subscribers
                    .send(&namespace, Event::NeighborDown(peer))
                    .await;
            }
            ToLiveActor::DeferredResync {
                namespace,
                retry_at,
            } => {
                self.deferred_resyncs
                    .entry(namespace)
                    .and_modify(|at| *at = (*at).min(retry_at))
                    .or_insert(retry_at);
            }
            ToLiveActor::GossipLagged { namespace } => {
                self.on_gossip_lagged(namespace).await;
            }
            ToLiveActor::StartSync {
                namespace,
                peers,
                reply,
            } => {
                let res = self.start_sync(namespace, peers).await;
                reply.send(res).ok();
            }
            ToLiveActor::Leave {
                namespace,
                kill_subscribers,
                reply,
            } => {
                let res = self.leave(namespace, kill_subscribers).await;
                reply.send(res).ok();
            }
            ToLiveActor::Subscribe {
                namespace,
                sender,
                reply,
            } => {
                self.subscribers.subscribe(namespace, sender);
                reply.send(Ok(())).ok();
            }
            ToLiveActor::HandleConnection { conn } => {
                self.handle_connection(conn).await;
            }
            ToLiveActor::AcceptSyncRequest {
                namespace,
                peer,
                reply,
            } => {
                let outcome = self.accept_sync_request(namespace, peer);
                reply.send(outcome).ok();
            }
            ToLiveActor::NeighborContentReady {
                namespace,
                node,
                hash,
            } => {
                self.on_neighbor_content_ready(namespace, node, hash).await;
            }
        };
        Ok(true)
    }

    #[instrument("connect", skip_all, fields(peer = %peer.fmt_short(), namespace = %namespace.fmt_short()))]
    fn sync_with_peer(&mut self, namespace: NamespaceId, peer: PublicKey, reason: SyncReason) {
        if !self.state.start_connect(&namespace, peer, reason) {
            return;
        }
        let endpoint = self.endpoint.clone();
        let sync = self.sync.clone();
        let metrics = self.metrics.clone();
        let inactivity = self.sync_inactivity_timeout;
        let fut = async move {
            #[cfg(test)]
            fault::maybe_panic_connect(&namespace);
            let res = connect_and_sync_with_timeout(
                &endpoint,
                &sync,
                namespace,
                EndpointAddr::new(peer),
                Some(&metrics),
                inactivity,
            )
            .await;
            (namespace, peer, reason, res)
        }
        .instrument(Span::current());
        let task = self.running_sync_connect.spawn(fut);
        self.running_sync_connect_ids
            .insert(task.id(), (namespace, peer, reason));
    }

    /// A timer that completes when the earliest deferred re-sync is due.
    fn deferred_resync_timer(deferred: &HashMap<NamespaceId, Instant>) -> n0_future::time::Sleep {
        let earliest = deferred
            .values()
            .min()
            .copied()
            .unwrap_or_else(Instant::now);
        n0_future::time::sleep_until(earliest)
    }

    /// Start re-syncs for the namespaces whose deferred re-sync time has arrived.
    fn fire_deferred_resyncs(&mut self) {
        let now = Instant::now();
        let due: Vec<NamespaceId> = self
            .deferred_resyncs
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(namespace, _)| *namespace)
            .collect();
        for namespace in due {
            self.deferred_resyncs.remove(&namespace);
            if !self.state.is_syncing(&namespace) {
                continue;
            }
            let peers = self.neighbors.get(&namespace).cloned().unwrap_or_default();
            for peer in peers {
                self.sync_with_peer(namespace, peer, SyncReason::DeferredValidation);
            }
        }
    }

    /// Log one warning per interval and schedule re-syncs for entries refused during a sync.
    fn warn_and_schedule_rejected(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
        rejected: &[RejectedEntry],
    ) {
        let mut earliest: Option<Instant> = None;
        for entry in rejected {
            if let Some(retry_after) = entry.retry_after {
                let at = Instant::now() + retry_after;
                earliest = Some(earliest.map_or(at, |prev| prev.min(at)));
            }
        }
        if let Some(at) = earliest {
            self.deferred_resyncs
                .entry(namespace)
                .and_modify(|prev| *prev = (*prev).min(at))
                .or_insert(at);
        }
        if !reaction_allowed(&mut self.validation_warned, namespace) {
            return;
        }
        let first = &rejected[0];
        warn!(
            peer = %peer.fmt_short(),
            namespace = %namespace.fmt_short(),
            hash = %first.hash,
            reason = %first.failure,
            count = rejected.len(),
            "refused remote entries during sync; will re-sync when they can pass validation"
        );
    }

    /// A subscription dropped events: re-subscribe (the fresh receive loop reports our
    /// neighbors again, which triggers the repair syncs) or the dropped entries stay missing
    /// until some later trigger.
    async fn on_gossip_lagged(&mut self, namespace: NamespaceId) {
        // One reaction per interval per namespace: a wedged subscription lags one tick after
        // another.
        if !reaction_allowed(&mut self.lagged_reacted, namespace) {
            return;
        }
        self.metrics.doc_gossip_lagged.inc();
        // A lag report can arrive after the namespace left; its receive loop is gone then.
        if !self.state.is_syncing(&namespace) {
            return;
        }
        warn!(
            namespace = %namespace.fmt_short(),
            "gossip subscription dropped events; re-subscribing and re-syncing"
        );
        self.gossip.quit(&namespace);
        if let Err(err) = self.gossip.join(namespace, vec![]).await {
            warn!(?err, namespace = %namespace.fmt_short(), "failed to re-subscribe after gossip lag");
        }
    }

    /// A connect task ended without a result: fail its sync like any error.
    async fn on_sync_connect_task_failed(&mut self, err: JoinError) {
        let Some((namespace, peer, reason)) = self.running_sync_connect_ids.remove(&err.id())
        else {
            warn!(?err, "untracked sync connect task failed");
            return;
        };
        warn!(?err, namespace = %namespace.fmt_short(), peer = %peer.fmt_short(), "sync connect task failed");
        self.on_sync_finished(
            namespace,
            peer,
            Origin::Connect(reason),
            Err(anyhow::anyhow!("sync connect task failed: {err}")),
        )
        .await
    }

    /// An accept task ended without a result: fail its sync, if it had read
    /// the request, like any error.
    async fn on_sync_accept_task_failed(&mut self, err: JoinError) {
        let accepted = self
            .running_sync_accept_ids
            .remove(&err.id())
            .and_then(|request| request.get().copied());
        let Some((namespace, peer)) = accepted else {
            warn!(?err, "sync accept task failed before reading a request");
            return;
        };
        warn!(?err, namespace = %namespace.fmt_short(), peer = %peer.fmt_short(), "sync accept task failed");
        self.on_sync_finished(
            namespace,
            peer,
            Origin::Accept,
            Err(anyhow::anyhow!("sync accept task failed: {err}")),
        )
        .await
    }

    /// A download task ended without a result: fail its download like any
    /// error, so the hash is wanted again.
    async fn on_download_task_failed(&mut self, err: JoinError) {
        let Some((namespace, hash)) = self.download_task_ids.remove(&err.id()) else {
            warn!(?err, "untracked download task failed");
            return;
        };
        warn!(?err, namespace = %namespace.fmt_short(), hash = %hash.fmt_short(), "download task failed");
        self.on_download_ready(
            namespace,
            hash,
            Err(anyhow::anyhow!("download task failed: {err}")),
        )
        .await
    }

    async fn shutdown(&mut self) -> anyhow::Result<()> {
        // cancel all subscriptions
        self.subscribers.clear();
        let (gossip_shutdown_res, _store) = tokio::join!(
            // quit the gossip topics and task loops.
            self.gossip.shutdown(),
            // shutdown sync thread
            self.sync.shutdown()
        );
        gossip_shutdown_res?;
        // TODO: abort_all and join_next all JoinSets to catch panics
        // (they are aborted on drop, but that swallows panics)
        Ok(())
    }

    async fn start_sync(
        &mut self,
        namespace: NamespaceId,
        mut peers: Vec<EndpointAddr>,
    ) -> Result<()> {
        debug!(?namespace, peers = peers.len(), "start sync");
        // update state to allow sync
        if !self.state.is_syncing(&namespace) {
            let opts = OpenOpts::default()
                .sync()
                .subscribe(self.replica_events_tx.clone());
            self.sync.open(namespace, opts).await?;
            self.state.insert(namespace);
        }
        // add the peers stored for this document
        match self.sync.get_sync_peers(namespace).await {
            Ok(None) => {
                // no peers for this document
            }
            Ok(Some(known_useful_peers)) => {
                let as_node_addr = known_useful_peers.into_iter().filter_map(|peer_id_bytes| {
                    // peers are stored as bytes, don't fail the operation if they can't be
                    // decoded: simply ignore the peer
                    match PublicKey::from_bytes(&peer_id_bytes) {
                        Ok(public_key) => Some(EndpointAddr::new(public_key)),
                        Err(_signing_error) => {
                            warn!("potential db corruption: peers per doc can't be decoded");
                            None
                        }
                    }
                });
                peers.extend(as_node_addr);
            }
            Err(e) => {
                // try to continue if peers per doc can't be read since they are not vital for sync
                warn!(%e, "db error reading peers per document")
            }
        }
        self.join_peers(namespace, peers).await?;
        Ok(())
    }

    async fn leave(
        &mut self,
        namespace: NamespaceId,
        kill_subscribers: bool,
    ) -> anyhow::Result<()> {
        // self.subscribers.remove(&namespace);
        if self.state.remove(&namespace) {
            self.sync.set_sync(namespace, false).await?;
            self.sync
                .unsubscribe(namespace, self.replica_events_tx.clone())
                .await?;
            self.sync.close(namespace).await?;
            self.gossip.quit(&namespace);
        }
        self.neighbors.remove(&namespace);
        self.deferred_resyncs.remove(&namespace);
        self.validation_warned.remove(&namespace);
        self.lagged_reacted.remove(&namespace);
        if kill_subscribers {
            self.subscribers.remove(&namespace);
        }
        Ok(())
    }

    async fn join_peers(&mut self, namespace: NamespaceId, peers: Vec<EndpointAddr>) -> Result<()> {
        let mut peer_ids = Vec::new();

        // add addresses of peers to our endpoint address book
        for peer in peers.into_iter() {
            let peer_id = peer.id;
            // adding a node address without any addressing info fails with an error,
            // but we still want to include those peers because endpoint address lookup might find addresses for them
            if !peer.is_empty() {
                self.memory_lookup.add_endpoint_info(peer);
            }
            peer_ids.push(peer_id);
        }

        // tell gossip to join
        self.gossip.join(namespace, peer_ids.clone()).await?;

        if !peer_ids.is_empty() {
            // trigger initial sync with initial peers
            for peer in peer_ids {
                self.sync_with_peer(namespace, peer, SyncReason::DirectJoin);
            }
        }
        Ok(())
    }

    #[instrument("connect", skip_all, fields(peer = %peer.fmt_short(), namespace = %namespace.fmt_short()))]
    async fn on_sync_via_connect_finished(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
        reason: SyncReason,
        result: Result<SyncFinished, ConnectError>,
    ) {
        // `RemoteAbort(AlreadySyncing)` used to be swallowed here on the bet
        // that the reciprocal direction's sync — the one the remote preferred —
        // would complete on our accept side and reset the state. When the
        // remote could never reach us (a restarted peer whose address it held
        // was stale), the bet failed permanently: the state stayed
        // `Running{Connect}` with no timeout and no retry, and every later
        // attempt for this (namespace, peer) was refused as already running.
        // The abort must therefore reset the state when the connect still owns
        // it; `PeerState::finish` is origin-conditional, so if the reciprocal
        // accept has already taken ownership the abort is discarded and that
        // live exchange is left untouched.
        //
        // That reset is all it is. Losing the simultaneous-dial tie-break is
        // designed behaviour, not a failed sync: the reciprocal exchange does
        // the work and reports it (and an exchange that completes is always
        // reported by `on_sync_finished`, even if the abort or an accept took
        // its state slot over meanwhile). Emitting `SyncFinished(Err)` and a
        // `PendingContentReady` for it was noise to every subscriber, and the
        // success paths (peer registration, report broadcast, redrive) do not
        // apply. Real errors still go through `on_sync_finished`.
        if is_tiebreak_loss(&result) {
            debug!("lost the simultaneous-dial tie-break; reset without reporting");
            let origin = Origin::Connect(reason);
            if let Some(true) = self.state.finish_tiebreak_loss(&namespace, peer, &origin) {
                self.sync_with_peer(namespace, peer, SyncReason::Resync);
            }
            return;
        }
        self.on_sync_finished(
            namespace,
            peer,
            Origin::Connect(reason),
            result.map_err(Into::into),
        )
        .await
    }

    #[instrument("accept", skip_all, fields(peer = %fmt_accept_peer(&res), namespace = %fmt_accept_namespace(&res)))]
    async fn on_sync_via_accept_finished(&mut self, res: Result<SyncFinished, AcceptError>) {
        match res {
            Ok(state) => {
                self.on_sync_finished(state.namespace, state.peer, Origin::Accept, Ok(state))
                    .await
            }
            Err(AcceptError::Abort { reason, .. }) if reason == AbortReason::AlreadySyncing => {
                // In case we aborted the sync: do nothing (our outgoing sync is in progress)
                debug!(?reason, "aborted by us");
            }
            Err(err) => {
                if let (Some(peer), Some(namespace)) = (err.peer(), err.namespace()) {
                    self.on_sync_finished(
                        namespace,
                        peer,
                        Origin::Accept,
                        Err(anyhow::Error::from(err)),
                    )
                    .await;
                } else {
                    debug!(?err, "failed before reading the first message");
                }
            }
        }
    }

    async fn on_sync_finished(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
        origin: Origin,
        result: Result<SyncFinished>,
    ) {
        match &result {
            Err(ref err) => {
                warn!(?origin, ?err, "sync failed");
            }
            Ok(ref details) => {
                info!(
                    sent = %details.outcome.num_sent,
                    recv = %details.outcome.num_recv,
                    t_connect = ?details.timings.connect,
                    t_process = ?details.timings.process,
                    "sync finished",
                );

                // register the peer as useful for the document
                if let Err(e) = self
                    .sync
                    .register_useful_peer(namespace, *peer.as_bytes())
                    .await
                {
                    debug!(%e, "failed to register peer for document")
                }

                // broadcast a sync report to our neighbors, but only if we received new entries.
                if details.outcome.num_recv > 0 {
                    info!("broadcast sync report to neighbors");
                    match details
                        .outcome
                        .heads_received
                        .encode(Some(self.gossip.max_message_size()))
                    {
                        Err(err) => warn!(?err, "Failed to encode author heads for sync report"),
                        Ok(heads) => {
                            let report = SyncReport { namespace, heads };
                            self.broadcast_neighbors(namespace, &Op::SyncReport(report))
                                .await;
                        }
                    }
                }

                // Entries refused during the exchange: warn once per interval and re-sync once
                // the wall clock has caught up enough for them to pass validation.
                if !details.outcome.rejected.is_empty() {
                    self.metrics
                        .entries_rejected_validation
                        .inc_by(details.outcome.rejected.len() as u64);
                    self.warn_and_schedule_rejected(namespace, peer, &details.outcome.rejected);
                }
            }
        };

        let result_for_event = match &result {
            Ok(details) => Ok(details.into()),
            Err(err) => Err(err.to_string()),
        };

        // How long the exchange took, for the start time of a displaced success below.
        let elapsed = match &result {
            Ok(details) => details.timings.connect + details.timings.process,
            Err(_) => Duration::ZERO,
        };

        // Who owns the (namespace, peer) slot decides only the bookkeeping: whether the slot
        // goes back to idle and whether a queued resync runs. It never decides whether a
        // completed exchange is reported. `PeerState::finish` is origin-conditional (row 22), so
        // it returns `None` when the slot is idle or belongs to a different origin, as when an
        // accept took over a running connect and finished first, or a later connect was
        // rejected and reset the slot (row 39) while this one was still running. The exchange
        // itself ran to completion regardless: its entries are in the replica and its content
        // hashes are queued. Dropping its `SyncFinished` left waiters hanging, so a success is
        // reported with its real origin and result and the slot is left alone (the live owner's
        // own finish resets it, and no resync runs on its behalf).
        // A failed displaced exchange stays unreported: it carries no work, and a stale abort
        // must not look like a result for the exchange that owns the slot.
        let (started, resync) = match self.state.finish(&namespace, peer, &origin, result) {
            Some(finished) => finished,
            None if result_for_event.is_ok() => {
                debug!(
                    ?origin,
                    "completed sync no longer owns its state; reporting it anyway"
                );
                let now = SystemTime::now();
                (now.checked_sub(elapsed).unwrap_or(now), false)
            }
            None => return,
        };

        let sync_succeeded = result_for_event.is_ok();
        let ev = SyncEvent {
            peer,
            origin,
            result: result_for_event,
            finished: SystemTime::now(),
            started,
        };
        self.subscribers
            .send(&namespace, Event::SyncFinished(ev))
            .await;

        if sync_succeeded {
            self.redrive_missing_content_for_peer(namespace, peer).await;
        }

        // Check if there are queued pending content hashes for this namespace.
        // If hashes are pending, mark this namespace to be eglible for a PendingContentReady event once all
        // pending hashes have completed downloading.
        // If no hashes are pending, emit the PendingContentReady event right away. The next
        // PendingContentReady event may then only be emitted after the next sync completes.
        if self.queued_hashes.contains_namespace(&namespace) {
            self.state.set_may_emit_ready(&namespace, true);
        } else {
            self.subscribers
                .send(&namespace, Event::PendingContentReady)
                .await;
            self.state.set_may_emit_ready(&namespace, false);
        }

        if resync {
            self.sync_with_peer(namespace, peer, SyncReason::Resync);
        }
    }

    fn download_status(&self, namespace: NamespaceId, hash: Hash) -> super::DownloadStatus {
        download_status(
            namespace,
            hash,
            &self.pending_downloads,
            &self.queued_hashes,
        )
    }

    async fn redrive_missing_content_for_peer(&mut self, namespace: NamespaceId, peer: PublicKey) {
        let missing = self.missing_hashes.hashes_for_namespace(&namespace);
        if missing.is_empty() {
            return;
        }

        debug!(
            namespace = %namespace.fmt_short(),
            peer = %peer.fmt_short(),
            missing = missing.len(),
            "redrive missing content",
        );
        for hash in missing {
            self.queue_download(namespace, hash, peer, true);
        }
        self.flush_pending_downloads().await;
    }

    async fn broadcast_neighbors(&mut self, namespace: NamespaceId, op: &Op) {
        if !self.state.is_syncing(&namespace) {
            return;
        }

        let msg = match postcard::to_stdvec(op) {
            Ok(msg) => msg,
            Err(err) => {
                error!(?err, ?op, "Failed to serialize message:");
                return;
            }
        };
        // TODO: We should debounce and merge these neighbor announcements likely.
        self.gossip
            .broadcast_neighbors(&namespace, msg.into())
            .await;
    }

    async fn emit_content_ready(&mut self, namespace: NamespaceId, hash: Hash) {
        self.subscribers
            .send(&namespace, Event::ContentReady { hash })
            .await;
        self.broadcast_neighbors(namespace, &Op::ContentReady(hash))
            .await;
    }

    async fn on_download_ready(
        &mut self,
        namespace: NamespaceId,
        hash: Hash,
        res: Result<(), anyhow::Error>,
    ) {
        let removed = self.queued_hashes.remove_hash(&hash);
        debug!(namespace=%namespace.fmt_short(), success=res.is_ok(), namespaces=removed.namespaces.len(), completed_namespaces=removed.completed_namespaces.len(), "download ready");
        if res.is_ok() {
            for namespace in removed.namespaces.iter().copied() {
                self.missing_hashes.remove(hash, &namespace);
                self.emit_content_ready(namespace, hash).await;
            }
        } else {
            for namespace in &removed.namespaces {
                self.missing_hashes.insert(hash, *namespace);
            }
        }
        for namespace in removed.completed_namespaces.iter() {
            if let Some(true) = self.state.may_emit_ready(namespace) {
                self.subscribers
                    .send(namespace, Event::PendingContentReady)
                    .await;
            }
        }
    }

    async fn on_neighbor_content_ready(
        &mut self,
        namespace: NamespaceId,
        node: EndpointId,
        hash: Hash,
    ) {
        self.queue_download(namespace, hash, node, true);
        self.flush_pending_downloads().await;
    }

    #[instrument("on_sync_report", skip_all, fields(peer = %from.fmt_short(), namespace = %report.namespace.fmt_short()))]
    async fn on_sync_report(&mut self, from: PublicKey, report: SyncReport) {
        let namespace = report.namespace;
        if !self.state.is_syncing(&namespace) {
            return;
        }
        let heads = match AuthorHeads::decode(&report.heads) {
            Ok(heads) => heads,
            Err(err) => {
                warn!(?err, "failed to decode AuthorHeads");
                return;
            }
        };
        match self.sync.has_news_for_us(report.namespace, heads).await {
            Ok(Some(updated_authors)) => {
                info!(%updated_authors, "news reported: sync now");
                self.sync_with_peer(report.namespace, from, SyncReason::SyncReport);
            }
            Ok(None) => {
                debug!("no news reported: nothing to do");
            }
            Err(err) => {
                warn!("sync actor error: {err:?}");
            }
        }
    }

    async fn on_replica_event(&mut self, event: crate::Event) -> Result<()> {
        match event {
            crate::Event::LocalInsert { namespace, entry } => {
                debug!(namespace=%namespace.fmt_short(), "replica event: LocalInsert");
                // A new entry was inserted locally. Broadcast a gossip message.
                if self.state.is_syncing(&namespace) {
                    let op = Op::Put(entry.clone());
                    let message = postcard::to_stdvec(&op)?.into();
                    self.gossip.broadcast(&namespace, message).await;
                }
            }
            crate::Event::RemoteInsert {
                namespace,
                entry,
                from,
                should_download,
                remote_content_status,
            } => {
                debug!(namespace=%namespace.fmt_short(), "replica event: RemoteInsert");
                // A new entry was inserted from initial sync or gossip. Queue downloading the
                // content.
                if should_download {
                    let hash = entry.content_hash();
                    if matches!(remote_content_status, ContentStatus::Complete) {
                        let node_id = PublicKey::from_bytes(&from)?;
                        self.queue_download(namespace, hash, node_id, false);
                    } else {
                        self.missing_hashes.insert(hash, namespace);
                        // A neighbor's `ContentReady` for this hash may have
                        // been handled before this entry, while the hash was
                        // not missing yet, and ignored. Ask the neighbors
                        // known to have it.
                        for node in self.known_providers(hash) {
                            self.queue_download(namespace, hash, node, true);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn queue_download(
        &mut self,
        namespace: NamespaceId,
        hash: Hash,
        node: PublicKey,
        only_if_missing: bool,
    ) {
        self.pending_downloads.push(DownloadCandidate {
            namespace,
            hash,
            node,
            only_if_missing,
        });
    }

    async fn flush_pending_downloads(&mut self) {
        let pending = mem::take(&mut self.pending_downloads);
        if pending.is_empty() {
            return;
        }

        let mut by_hash: HashMap<Hash, Vec<DownloadCandidate>> = HashMap::new();
        for candidate in pending {
            by_hash.entry(candidate.hash).or_default().push(candidate);
        }

        let hashes = by_hash.keys().copied().collect::<Vec<_>>();
        let statuses = match self
            .bao_store
            .blobs()
            .status_many(hashes.iter().copied())
            .await
        {
            Ok(statuses) if statuses.len() == hashes.len() => statuses,
            Ok(statuses) => {
                warn!(
                    requested = hashes.len(),
                    received = statuses.len(),
                    "blob status batch response length mismatch; falling back to per-hash status"
                );
                self.status_hashes_one_by_one(&hashes).await
            }
            Err(err) => {
                warn!(
                    ?err,
                    requested = hashes.len(),
                    "blob status batch failed; falling back to per-hash status"
                );
                self.status_hashes_one_by_one(&hashes).await
            }
        };

        for (hash, status) in hashes.into_iter().zip(statuses) {
            let Some(candidates) = by_hash.remove(&hash) else {
                continue;
            };

            if matches!(status, BlobStatus::Complete { .. }) {
                for candidate in candidates {
                    let was_missing = self.missing_hashes.remove(hash, &candidate.namespace);
                    if !candidate.only_if_missing || was_missing {
                        self.emit_content_ready(candidate.namespace, hash).await;
                    }
                }
                continue;
            }

            self.record_hash_providers(hash, &candidates);

            if self.queued_hashes.contains_hash(&hash) {
                for candidate in candidates {
                    self.queued_hashes.insert(hash, candidate.namespace);
                }
                continue;
            }

            let namespaces = candidates
                .iter()
                .filter(|candidate| self.download_candidate_is_eligible(hash, candidate))
                .map(|candidate| candidate.namespace)
                .collect::<HashSet<_>>();

            if namespaces.is_empty() {
                continue;
            }

            let req = DownloadRequest::new(
                HashAndFormat::raw(hash),
                self.hash_providers.clone(),
                SplitStrategy::None,
            );
            let handle = self.downloader.download_with_opts(req);
            let download_namespace = candidates[0].namespace;

            for namespace in namespaces {
                self.queued_hashes.insert(hash, namespace);
                self.missing_hashes.remove(hash, &namespace);
            }
            let task = self.download_tasks.spawn(async move {
                (
                    download_namespace,
                    hash,
                    handle.await.map_err(|e| anyhow::anyhow!(e)),
                )
            });
            self.download_task_ids
                .insert(task.id(), (download_namespace, hash));
        }
    }

    async fn status_hashes_one_by_one(&self, hashes: &[Hash]) -> Vec<BlobStatus> {
        let mut statuses = Vec::with_capacity(hashes.len());
        for hash in hashes {
            statuses.push(
                self.bao_store
                    .blobs()
                    .status(*hash)
                    .await
                    .unwrap_or(BlobStatus::NotFound),
            );
        }
        statuses
    }

    fn known_providers(&self, hash: Hash) -> Vec<EndpointId> {
        let providers = self.hash_providers.0.lock().expect("poisoned");
        providers
            .get(&hash)
            .map(|nodes| nodes.iter().copied().collect())
            .unwrap_or_default()
    }

    fn record_hash_providers(&mut self, hash: Hash, candidates: &[DownloadCandidate]) {
        let mut providers = self.hash_providers.0.lock().expect("poisoned");
        let nodes = providers.entry(hash).or_default();
        for candidate in candidates {
            nodes.insert(candidate.node);
        }
    }

    fn download_candidate_is_eligible(&self, hash: Hash, candidate: &DownloadCandidate) -> bool {
        !candidate.only_if_missing || self.missing_hashes.contains(hash, &candidate.namespace)
    }

    #[instrument("accept", skip_all)]
    pub async fn handle_connection(&mut self, conn: iroh::endpoint::Connection) {
        let to_actor_tx = self.sync_actor_tx.clone();
        let accepted = AcceptedRequest::default();
        let accepted_cb = accepted.clone();
        let accept_request_cb = move |namespace, peer| {
            let to_actor_tx = to_actor_tx.clone();
            accepted_cb.set((namespace, peer)).ok();
            async move {
                let (reply_tx, reply_rx) = oneshot::channel();
                to_actor_tx
                    .send(ToLiveActor::AcceptSyncRequest {
                        namespace,
                        peer,
                        reply: reply_tx,
                    })
                    .await
                    .ok();
                match reply_rx.await {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        warn!(
                            "accept request callback failed to retrieve reply from actor: {err:?}"
                        );
                        AcceptOutcome::Reject(AbortReason::InternalServerError)
                    }
                }
            }
            .boxed()
        };
        debug!("incoming connection");
        let sync = self.sync.clone();
        let metrics = self.metrics.clone();
        let inactivity = self.sync_inactivity_timeout;
        let task = self.running_sync_accept.spawn(
            async move {
                handle_connection_with_timeout(
                    sync,
                    conn,
                    accept_request_cb,
                    Some(&metrics),
                    inactivity,
                )
                .await
            }
            .instrument(Span::current()),
        );
        self.running_sync_accept_ids.insert(task.id(), accepted);
    }

    pub fn accept_sync_request(
        &mut self,
        namespace: NamespaceId,
        peer: PublicKey,
    ) -> AcceptOutcome {
        self.state
            .accept_request(&self.endpoint.id(), &namespace, peer)
    }
}

/// Event emitted when a sync operation completes
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncEvent {
    /// Peer we synced with
    pub peer: PublicKey,
    /// Why this synchronisation started.
    ///
    /// This says what triggered the exchange, and nothing more. It does not
    /// identify which synchronisation carried a particular entry or content
    /// hash across, and two exchanges with different origins can do the same
    /// work.
    pub origin: Origin,
    /// Timestamp when the sync finished
    pub finished: SystemTime,
    /// Timestamp when the sync started
    pub started: SystemTime,
    /// Result of the sync operation
    pub result: std::result::Result<SyncDetails, String>,
}

/// What a completed synchronisation reconciled.
///
/// Counts only. A synchronisation that reconciled nothing is not thereby
/// redundant — it may simply have found the other side already up to date —
/// and one that reconciled entries is not thereby the *first* to have done so.
/// Use these to describe what happened, not to infer which exchange was
/// responsible for a particular entry.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncDetails {
    /// Number of entries received
    pub entries_received: usize,
    /// Number of entries sent
    pub entries_sent: usize,
}

impl From<&SyncFinished> for SyncDetails {
    fn from(value: &SyncFinished) -> Self {
        Self {
            entries_received: value.outcome.num_recv,
            entries_sent: value.outcome.num_sent,
        }
    }
}

#[derive(Debug, Default)]
struct SubscribersMap(HashMap<NamespaceId, Subscribers>);

impl SubscribersMap {
    fn subscribe(&mut self, namespace: NamespaceId, sender: async_channel::Sender<Event>) {
        self.0.entry(namespace).or_default().subscribe(sender);
    }

    async fn send(&mut self, namespace: &NamespaceId, event: Event) -> bool {
        debug!(namespace=%namespace.fmt_short(), %event, "emit event");
        let Some(subscribers) = self.0.get_mut(namespace) else {
            return false;
        };

        if !subscribers.send(event).await {
            self.0.remove(namespace);
        }
        true
    }

    fn remove(&mut self, namespace: &NamespaceId) {
        self.0.remove(namespace);
    }

    fn clear(&mut self) {
        self.0.clear();
    }
}

#[derive(Debug, Default)]
struct QueuedHashes {
    by_hash: HashMap<Hash, HashSet<NamespaceId>>,
    by_namespace: HashMap<NamespaceId, HashSet<Hash>>,
}

#[derive(Debug, Default)]
struct RemovedQueuedHash {
    namespaces: Vec<NamespaceId>,
    completed_namespaces: Vec<NamespaceId>,
}

#[derive(Debug, Default)]
struct MissingHashes {
    by_hash: HashMap<Hash, HashSet<NamespaceId>>,
    by_namespace: HashMap<NamespaceId, HashSet<Hash>>,
}

#[derive(Debug, Clone, Default)]
struct ProviderNodes(Arc<std::sync::Mutex<HashMap<Hash, HashSet<EndpointId>>>>);

impl ContentDiscovery for ProviderNodes {
    fn find_providers(&self, hash: HashAndFormat) -> n0_future::stream::Boxed<EndpointId> {
        let nodes = self
            .0
            .lock()
            .expect("poisoned")
            .get(&hash.hash)
            .into_iter()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(n0_future::stream::iter(nodes))
    }
}

impl QueuedHashes {
    fn insert(&mut self, hash: Hash, namespace: NamespaceId) {
        self.by_hash.entry(hash).or_default().insert(namespace);
        self.by_namespace.entry(namespace).or_default().insert(hash);
    }

    fn remove_hash(&mut self, hash: &Hash) -> RemovedQueuedHash {
        let namespaces = self.by_hash.remove(hash).unwrap_or_default();
        let mut removed = RemovedQueuedHash {
            namespaces: namespaces.iter().copied().collect(),
            completed_namespaces: Vec::new(),
        };
        for namespace in namespaces {
            if let Some(hashes) = self.by_namespace.get_mut(&namespace) {
                hashes.remove(hash);
                if hashes.is_empty() {
                    self.by_namespace.remove(&namespace);
                    removed.completed_namespaces.push(namespace);
                }
            }
        }
        removed
    }

    fn contains_hash(&self, hash: &Hash) -> bool {
        self.by_hash.contains_key(hash)
    }

    fn contains_namespace(&self, namespace: &NamespaceId) -> bool {
        self.by_namespace.contains_key(namespace)
    }
}

impl MissingHashes {
    fn insert(&mut self, hash: Hash, namespace: NamespaceId) {
        self.by_hash.entry(hash).or_default().insert(namespace);
        self.by_namespace.entry(namespace).or_default().insert(hash);
    }

    fn remove(&mut self, hash: Hash, namespace: &NamespaceId) -> bool {
        let mut removed = false;
        let mut remove_hash = false;
        if let Some(namespaces) = self.by_hash.get_mut(&hash) {
            removed = namespaces.remove(namespace);
            remove_hash = namespaces.is_empty();
        }
        if remove_hash {
            self.by_hash.remove(&hash);
        }

        let mut remove_namespace = false;
        if let Some(hashes) = self.by_namespace.get_mut(namespace) {
            hashes.remove(&hash);
            remove_namespace = hashes.is_empty();
        }
        if remove_namespace {
            self.by_namespace.remove(namespace);
        }
        removed
    }

    fn contains(&self, hash: Hash, namespace: &NamespaceId) -> bool {
        self.by_namespace
            .get(namespace)
            .is_some_and(|hashes| hashes.contains(&hash))
    }

    fn hashes_for_namespace(&self, namespace: &NamespaceId) -> Vec<Hash> {
        self.by_namespace
            .get(namespace)
            .map(|hashes| hashes.iter().copied().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Default)]
struct Subscribers(Vec<async_channel::Sender<Event>>);

impl Subscribers {
    fn subscribe(&mut self, sender: async_channel::Sender<Event>) {
        self.0.push(sender)
    }

    async fn send(&mut self, event: Event) -> bool {
        let futs = self.0.iter().map(|sender| sender.send(event.clone()));
        let res = futures_buffered::join_all(futs).await;
        // reverse the order so removing does not shift remaining indices
        for (i, res) in res.into_iter().enumerate().rev() {
            if res.is_err() {
                self.0.remove(i);
            }
        }
        !self.0.is_empty()
    }
}

fn fmt_accept_peer(res: &Result<SyncFinished, AcceptError>) -> String {
    match res {
        Ok(res) => res.peer.fmt_short().to_string(),
        Err(err) => err
            .peer()
            .map(|x| x.fmt_short().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}

fn fmt_accept_namespace(res: &Result<SyncFinished, AcceptError>) -> String {
    match res {
        Ok(res) => res.namespace.fmt_short(),
        Err(err) => err
            .namespace()
            .map(|x| x.fmt_short())
            .unwrap_or_else(|| "unknown".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sync_remove() {
        let pk = PublicKey::from_bytes(&[1; 32]).unwrap();
        let (a_tx, a_rx) = async_channel::unbounded();
        let (b_tx, b_rx) = async_channel::unbounded();
        let mut subscribers = Subscribers::default();
        subscribers.subscribe(a_tx);
        subscribers.subscribe(b_tx);
        drop(a_rx);
        drop(b_rx);
        subscribers.send(Event::NeighborUp(pk)).await;
    }

    #[test]
    fn missing_hashes_are_tracked_per_namespace() {
        let h1 = Hash::new(b"h1");
        let h2 = Hash::new(b"h2");
        let n1 = NamespaceId::from(&[1; 32]);
        let n2 = NamespaceId::from(&[2; 32]);

        let mut missing = MissingHashes::default();
        missing.insert(h1, n1);
        missing.insert(h1, n2);
        missing.insert(h2, n1);

        assert!(missing.contains(h1, &n1));
        assert!(missing.contains(h1, &n2));
        assert!(missing.contains(h2, &n1));
        assert!(!missing.contains(h2, &n2));

        let n1_hashes = missing.hashes_for_namespace(&n1);
        assert_eq!(n1_hashes.len(), 2);
        assert!(n1_hashes.contains(&h1));
        assert!(n1_hashes.contains(&h2));

        assert!(missing.remove(h1, &n1));
        assert!(!missing.contains(h1, &n1));
        assert!(missing.contains(h1, &n2));
        assert_eq!(missing.hashes_for_namespace(&n1), vec![h2]);

        assert!(missing.remove(h1, &n2));
        assert!(!missing.contains(h1, &n2));
        assert!(missing.hashes_for_namespace(&n2).is_empty());
    }
}

/// Test-only fault injection into the live actor's tasks.
#[cfg(test)]
pub(crate) mod fault {
    use std::{collections::HashSet, sync::Mutex};

    use n0_future::time::Duration;

    use crate::NamespaceId;

    static PANIC_CONNECT: Mutex<Option<HashSet<NamespaceId>>> = Mutex::new(None);

    /// The next sync connect task for `namespace` panics before connecting.
    pub(crate) fn panic_next_connect(namespace: NamespaceId) {
        PANIC_CONNECT
            .lock()
            .unwrap()
            .get_or_insert_with(HashSet::new)
            .insert(namespace);
    }

    pub(crate) fn maybe_panic_connect(namespace: &NamespaceId) {
        let armed = PANIC_CONNECT
            .lock()
            .unwrap()
            .as_mut()
            .is_some_and(|s| s.remove(namespace));
        if armed {
            panic!("injected sync connect panic");
        }
    }

    /// Armed stalls for remote inserts (gossip puts): (namespace, remaining, delay).
    static DELAYED_INSERTS: Mutex<Vec<(NamespaceId, usize, Duration)>> = Mutex::new(Vec::new());

    /// Stall the next `count` remote inserts for `namespace` by `delay` each.
    pub(crate) fn delay_remote_inserts(namespace: NamespaceId, count: usize, delay: Duration) {
        DELAYED_INSERTS
            .lock()
            .unwrap()
            .push((namespace, count, delay));
    }

    /// Stall once if a delayed insert is armed for this namespace.
    pub(crate) async fn maybe_delay_remote_insert(namespace: &NamespaceId) {
        let delay = {
            let mut armed = DELAYED_INSERTS.lock().unwrap();
            let Some(pos) = armed.iter().position(|(ns, _, _)| ns == namespace) else {
                return;
            };
            let (_, count, delay) = &mut armed[pos];
            *count -= 1;
            let delay = *delay;
            if *count == 0 {
                armed.remove(pos);
            }
            delay
        };
        n0_future::time::sleep(delay).await;
    }
}

#[cfg(test)]
mod task_failure_tests {
    use iroh::{endpoint::presets, protocol::Router};
    use n0_future::{time::Duration, StreamExt};

    use crate::{
        api::protocol::{AddrInfoOptions, ShareMode},
        engine::LiveEvent,
        protocol::Docs,
    };

    struct TestNode {
        router: Router,
        docs: Docs,
        metrics: std::sync::Arc<super::super::Metrics>,
    }

    async fn node() -> anyhow::Result<TestNode> {
        let ep = iroh::Endpoint::bind(presets::Minimal).await?;
        let store = iroh_blobs::store::mem::MemStore::new();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let downloader = store.downloader(&ep);
        let engine = super::super::Engine::spawn(
            ep.clone(),
            gossip.clone(),
            crate::store::Store::memory(),
            (*store).clone(),
            downloader,
            super::super::DefaultAuthorStorage::Mem,
            None,
        )
        .await?;
        let metrics = engine.metrics().clone();
        let docs = Docs::new(engine);
        let router = Router::builder(ep)
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&store, None),
            )
            .accept(iroh_gossip::ALPN, gossip)
            .accept(crate::ALPN, docs.clone())
            .spawn();
        Ok(TestNode {
            router,
            docs,
            metrics,
        })
    }

    /// A sync task that panics fails that one sync. The live actor keeps
    /// running, the peer is not left marked as syncing, and a later
    /// start_sync with the same peer reconciles. Upstream stopped the actor,
    /// so every later docs call failed with "channel closed".
    #[tokio::test]
    async fn panicked_sync_task_leaves_the_actor_alive() -> anyhow::Result<()> {
        let a = node().await?;
        let b = node().await?;
        let author = a.docs.api().author_create().await?;
        let doc_a = a.docs.api().create().await?;
        doc_a
            .set_bytes(author, b"key".to_vec(), b"value".to_vec())
            .await?;
        let ticket = doc_a
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await?;
        let doc_b = b
            .docs
            .api()
            .import_namespace(ticket.capability.clone())
            .await?;
        let mut events = doc_b.subscribe().await?;

        super::fault::panic_next_connect(doc_b.id());
        doc_b.start_sync(ticket.nodes.clone()).await?;

        // The panicked connect surfaces as a failed sync round.
        let failed = tokio::time::timeout(Duration::from_secs(10), async {
            while let Some(ev) = events.next().await {
                if let LiveEvent::SyncFinished(ev) = ev? {
                    if ev.peer == a.router.endpoint().id() {
                        return anyhow::Ok(ev.result);
                    }
                }
            }
            anyhow::bail!("event stream ended: the live actor stopped")
        })
        .await??;
        let err = failed.expect_err("the injected panic fails the round");
        assert!(err.contains("panic"), "{err}");

        // The actor still serves requests, and the peer can sync again.
        doc_b.start_sync(ticket.nodes.clone()).await?;
        let entry = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(entry) = doc_b.get_exact(author, b"key".to_vec(), false).await? {
                    return anyhow::Ok(entry);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await??;
        assert_eq!(entry.content_len(), 5);

        b.router.shutdown().await?;
        a.router.shutdown().await?;
        Ok(())
    }

    /// A subscription whose buffer overflowed (Lagged) re-subscribes and re-syncs, repairing
    /// the dropped entries. Without the repair the burst's tail would stay missing until some
    /// unrelated sync trigger.
    #[tokio::test]
    async fn gossip_lag_resubscribes_and_resyncs() -> anyhow::Result<()> {
        // More than the subscription buffer (2048) plus the topic's broadcast channel (256):
        // the overflow is what produces the Lagged.
        const N: usize = 3000;
        let a = node().await?;
        let b = node().await?;
        let author = a.docs.api().author_create().await?;
        let doc_a = a.docs.api().create().await?;
        let ticket = doc_a
            .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
            .await?;
        let doc_b = b
            .docs
            .api()
            .import_namespace(ticket.capability.clone())
            .await?;
        let mut events = doc_b.subscribe().await?;

        // Both sides join the gossip topic and run their initial syncs. Both rounds must be
        // done before the burst: an in-flight sync would otherwise repair the loss on its own
        // and the test would prove nothing.
        doc_a.start_sync(vec![]).await?;
        doc_b.start_sync(ticket.nodes.clone()).await?;
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut initial = 0;
            while initial < 2 {
                let Some(ev) = events.next().await else {
                    anyhow::bail!("event stream ended: the live actor stopped");
                };
                if let LiveEvent::SyncFinished(ev) = ev? {
                    if ev.peer == a.router.endpoint().id() {
                        initial += 1;
                    }
                }
            }
            anyhow::Ok(())
        })
        .await??;

        // Stall the receiver's first remote insert: the burst then fills its subscription
        // buffer (2048) while it drains nothing, the topic's broadcast channel (256) overflows,
        // and a Lagged goes on the wire for everything the burst dropped.
        super::fault::delay_remote_inserts(doc_b.id(), 1, Duration::from_secs(20));
        for i in 0..N {
            doc_a
                .set_bytes(
                    author,
                    format!("key{i}").into_bytes(),
                    format!("value{i}").into_bytes(),
                )
                .await?;
        }

        // The lag must have happened, or the test proves nothing.
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if b.metrics.doc_gossip_lagged.get() >= 1 {
                    return anyhow::Ok(());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await??;

        // The fix-driven repair round runs and succeeds.
        tokio::time::timeout(Duration::from_secs(60), async {
            while let Some(ev) = events.next().await {
                if let LiveEvent::SyncFinished(ev) = ev? {
                    if ev.peer == a.router.endpoint().id() && ev.result.is_ok() {
                        return anyhow::Ok(());
                    }
                }
            }
            anyhow::bail!("event stream ended: the live actor stopped")
        })
        .await??;

        // Every entry of the burst is present.
        let count = doc_b
            .get_many_vec(crate::store::Query::author(author))
            .await?
            .len();
        assert_eq!(count, N, "the repair round restores every dropped entry");

        b.router.shutdown().await?;
        a.router.shutdown().await?;
        Ok(())
    }
}

/// A neighbor's `ContentReady` can be handled before the entry it announces:
/// the actor's inbox outranks its replica events, and under load the gossip
/// loop's `insert_remote` reaches the replica-event queue behind it.
#[cfg(test)]
mod content_ready_order_tests {
    use iroh::SecretKey;

    use super::*;
    use crate::{Author, NamespaceSecret, Record};

    #[tokio::test]
    async fn content_ready_handled_before_its_entry_still_downloads() -> Result<()> {
        let (mut actor, _endpoint) = super::takeover_tests::actor().await?;
        let namespace = NamespaceSecret::from_bytes(&[5; 32]);
        let ns = namespace.id();
        let author = Author::from_bytes(&[6; 32]);
        let neighbor = SecretKey::from_bytes(&[7; 32]).public();
        let hash = Hash::new(b"relayed content");
        let entry = SignedEntry::from_parts(&namespace, &author, b"key", Record::new(hash, 15, 1));

        // The entry was relayed, so its content is missing until a neighbor
        // announces it; the announcement arrives first.
        actor.on_neighbor_content_ready(ns, neighbor, hash).await;
        actor
            .on_replica_events(crate::Event::RemoteInsert {
                namespace: ns,
                entry,
                from: *neighbor.as_bytes(),
                should_download: true,
                remote_content_status: ContentStatus::Missing,
            })
            .await;
        assert_eq!(
            actor.download_status(ns, hash),
            super::super::DownloadStatus::Downloading,
            "the announcing neighbor must be asked for the content"
        );
        Ok(())
    }
}

fn download_status(
    namespace: NamespaceId,
    hash: Hash,
    pending: &[DownloadCandidate],
    running: &QueuedHashes,
) -> super::DownloadStatus {
    if running
        .by_namespace
        .get(&namespace)
        .is_some_and(|hashes| hashes.contains(&hash))
    {
        super::DownloadStatus::Downloading
    } else if pending
        .iter()
        .any(|c| c.namespace == namespace && c.hash == hash)
    {
        super::DownloadStatus::Queued
    } else {
        super::DownloadStatus::NoProvider
    }
}

#[cfg(test)]
mod download_status_tests {
    use super::*;
    #[test]
    fn scheduler_states_are_scoped_and_removed_after_failure() {
        use super::super::DownloadStatus::*;
        let n1 = NamespaceId::from(&[1; 32]);
        let n2 = NamespaceId::from(&[2; 32]);
        let hash = Hash::new(b"content");
        let other = Hash::new(b"other");
        let node = PublicKey::from_bytes(&[1; 32]).unwrap();
        let mut pending = vec![DownloadCandidate {
            namespace: n1,
            hash,
            node,
            only_if_missing: false,
        }];
        let mut running = QueuedHashes::default();
        assert_eq!(download_status(n1, hash, &pending, &running), Queued);
        assert_eq!(download_status(n2, hash, &pending, &running), NoProvider);
        assert_eq!(download_status(n1, other, &pending, &running), NoProvider);
        running.insert(hash, n1);
        assert_eq!(download_status(n1, hash, &pending, &running), Downloading);
        pending.clear();
        assert_eq!(download_status(n1, hash, &pending, &running), Downloading);
        running.remove_hash(&hash);
        assert_eq!(download_status(n1, hash, &pending, &running), NoProvider);
    }
}

/// A completed exchange is reported even when its state slot was taken over meanwhile.
#[cfg(test)]
mod takeover_tests {
    use iroh::{endpoint::presets, SecretKey};

    use super::*;
    use crate::sync::SyncOutcome;

    pub(super) async fn actor() -> Result<(LiveActor, Endpoint)> {
        let endpoint = Endpoint::bind(presets::Minimal).await?;
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let blobs = iroh_blobs::store::mem::MemStore::new();
        let downloader = blobs.downloader(&endpoint);
        let sync = SyncHandle::spawn(crate::store::Store::memory(), None, "test".to_string());
        let (tx, rx) = mpsc::channel(8);
        let actor = LiveActor::new(
            sync,
            endpoint.clone(),
            gossip,
            (*blobs).clone(),
            downloader,
            rx,
            tx,
            Default::default(),
        )?;
        Ok((actor, endpoint))
    }

    fn finished(namespace: NamespaceId, peer: PublicKey) -> SyncFinished {
        SyncFinished {
            namespace,
            peer,
            outcome: SyncOutcome::default(),
            timings: Default::default(),
        }
    }

    async fn sync_origins(rx: &async_channel::Receiver<Event>) -> Vec<(Origin, bool)> {
        let mut out = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if let Event::SyncFinished(ev) = ev {
                out.push((ev.origin, ev.result.is_ok()));
            }
        }
        out
    }

    /// Row 42. Sequence from the Aster `revoke_reaches_a_direct_only_server...` failure:
    /// 1. our connect B (`Resync`) to P is running;
    /// 2. P's incoming accept takes over the (namespace, P) slot and finishes: Idle, B still running;
    /// 3. a new connect A (`DirectJoin`) to P is rejected with `AlreadySyncing`: quiet reset;
    /// 4. B completes successfully. Its `SyncFinished` must reach subscribers (it used to be
    ///    dropped with "sync state finish called but not in running state"), and A's
    ///    quiet reset must not have been reported.
    #[tokio::test]
    async fn a_completed_connect_is_reported_after_its_slot_was_taken_over() -> Result<()> {
        let (mut actor, endpoint) = actor().await?;
        let me = endpoint.id();
        // The accept only takes over a running connect when `me` has the higher id.
        let peer = (1u8..)
            .map(|i| SecretKey::from_bytes(&[i; 32]).public())
            .find(|p| p.as_bytes() < me.as_bytes())
            .unwrap();
        let ns = NamespaceId::from(&[9u8; 32]);
        let (tx, rx) = async_channel::unbounded();
        actor.subscribers.subscribe(ns, tx);
        actor.state.insert(ns);

        // 1. connect B is running
        assert!(actor.state.start_connect(&ns, peer, SyncReason::Resync));
        // 2. an accept from the peer takes over the slot and finishes
        assert!(matches!(
            actor.state.accept_request(&me, &ns, peer),
            crate::net::AcceptOutcome::Allow
        ));
        actor
            .on_sync_via_accept_finished(Ok(finished(ns, peer)))
            .await;
        assert_eq!(
            sync_origins(&rx).await,
            vec![(Origin::Accept, true)],
            "the accept is reported"
        );
        // 3. connect A starts and loses the tie-break
        assert!(actor.state.start_connect(&ns, peer, SyncReason::DirectJoin));
        actor
            .on_sync_via_connect_finished(
                ns,
                peer,
                SyncReason::DirectJoin,
                Err(ConnectError::RemoteAbort(AbortReason::AlreadySyncing)),
            )
            .await;
        assert!(
            sync_origins(&rx).await.is_empty(),
            "tie-break loss is quiet"
        );
        // 4. connect B completes
        actor
            .on_sync_via_connect_finished(ns, peer, SyncReason::Resync, Ok(finished(ns, peer)))
            .await;
        assert_eq!(
            sync_origins(&rx).await,
            vec![(Origin::Connect(SyncReason::Resync), true)],
            "the completed connect must be reported"
        );
        Ok(())
    }

    /// Row 22 still holds: a completed exchange of another origin is reported but does not
    /// reset a live exchange that owns the slot.
    #[tokio::test]
    async fn a_reported_displaced_exchange_leaves_the_live_one_running() -> Result<()> {
        let (mut actor, _endpoint) = actor().await?;
        let peer = SecretKey::from_bytes(&[3u8; 32]).public();
        let ns = NamespaceId::from(&[9u8; 32]);
        let (tx, rx) = async_channel::unbounded();
        actor.subscribers.subscribe(ns, tx);
        actor.state.insert(ns);
        // A (`DirectJoin`) owns the slot; displaced B (`Resync`) completes.
        assert!(actor.state.start_connect(&ns, peer, SyncReason::DirectJoin));
        actor
            .on_sync_via_connect_finished(ns, peer, SyncReason::Resync, Ok(finished(ns, peer)))
            .await;
        assert_eq!(
            sync_origins(&rx).await,
            vec![(Origin::Connect(SyncReason::Resync), true)]
        );
        assert!(
            !actor.state.start_connect(&ns, peer, SyncReason::Resync),
            "connect A must still own the slot"
        );
        Ok(())
    }

    /// Rejects reported by a finished sync schedule a deferred re-sync with the peer, and the
    /// fired re-sync starts a connect.
    #[tokio::test]
    async fn rejected_entries_schedule_a_deferred_resync() -> Result<()> {
        use crate::ValidationFailure;
        let (mut actor, _endpoint) = actor().await?;
        let peer = SecretKey::from_bytes(&[3u8; 32]).public();
        let ns = NamespaceId::from(&[9u8; 32]);
        actor.state.insert(ns);
        actor.neighbors.entry(ns).or_default().insert(peer);

        let mut details = finished(ns, peer);
        details.outcome.rejected.push(RejectedEntry {
            hash: Hash::new(b"content"),
            timestamp: 0,
            failure: ValidationFailure::TooFarInTheFuture,
            retry_after: Some(Duration::from_secs(60)),
        });
        let retry_at = Instant::now() + Duration::from_secs(60);
        actor
            .on_sync_finished(
                ns,
                peer,
                Origin::Connect(SyncReason::DirectJoin),
                Ok(details),
            )
            .await;
        let scheduled = actor
            .deferred_resyncs
            .get(&ns)
            .copied()
            .expect("a deferred re-sync is scheduled");
        assert!(
            scheduled >= retry_at && scheduled <= retry_at + Duration::from_secs(1),
            "scheduled roughly one minute out: {scheduled:?}"
        );

        // The deadline arrives: the fired re-sync starts a connect with the peer.
        actor.deferred_resyncs.insert(ns, Instant::now());
        actor.fire_deferred_resyncs();
        assert!(
            !actor.running_sync_connect.is_empty(),
            "the deferred re-sync starts a connect"
        );
        Ok(())
    }
}

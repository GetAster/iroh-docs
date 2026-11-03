//! Announcer task: gossip broadcasts run off the live actor's serial loop.
//!
//! The live actor used to await the gossip broadcast RPCs directly. A
//! gossiping peer that is slow to accept messages (or, before the gossip
//! actor's try_send hardening, wedged on stream credit) made those awaits
//! block, which stalled the serial loop and, through its inbox, every docs
//! API call. The announcer moves the broadcasts into a task of their own:
//! the actor only `try_send`s into a bounded queue, and on a full queue the
//! message is dropped, counted, and the actor schedules a gossip-lag
//! re-sync for the namespace (the same lag reaction a wedged subscription
//! uses), so a peer that missed the broadcast is repaired by the next sync.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use iroh::EndpointId;
use iroh_gossip::api::GossipSender;
use tokio::sync::mpsc;
use tracing::warn;

use crate::{metrics::Metrics, NamespaceId};

/// Cap of the announcer queue: bounded, so the live actor never awaits gossip.
pub const ANNOUNCE_QUEUE_CAP: usize = 1024;

/// Interval of the rate-limited "announcer queue full" warnings.
const ANNOUNCE_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Which topic a message is announced on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceKind {
    /// The namespace's entries topic.
    Entries,
    /// The namespace's derived inline-values topic.
    Values,
    /// To the namespace topic's direct neighbors only.
    Neighbors,
}

/// A queued broadcast.
#[derive(Debug)]
pub struct AnnounceMsg {
    pub namespace: NamespaceId,
    pub kind: AnnounceKind,
    pub message: Bytes,
}

/// The senders of a namespace's two topics.
#[derive(Debug)]
struct Senders {
    entries: GossipSender,
    values: Option<GossipSender>,
}

/// Broadcasts gossip messages off the live actor's loop.
pub struct Announcer {
    tx: mpsc::Sender<AnnounceMsg>,
    senders: Arc<Mutex<HashMap<NamespaceId, Senders>>>,
    metrics: Arc<Metrics>,
    /// Time of the last rate-limited "announcer queue full" warning.
    last_warned: Mutex<Option<std::time::Instant>>,
}

impl Announcer {
    /// Spawns the announcer task and returns its handle.
    pub fn spawn(metrics: Arc<Metrics>) -> Self {
        let (tx, rx) = mpsc::channel(ANNOUNCE_QUEUE_CAP);
        let senders = Arc::new(Mutex::new(HashMap::new()));
        n0_future::task::spawn(announce_loop(rx, senders.clone()));
        Self {
            tx,
            senders,
            metrics,
            last_warned: Mutex::new(None),
        }
    }

    /// Registers the senders for a namespace's topics. Replaces any previous
    /// registration, so a re-join after a lag reaction simply re-registers.
    pub fn register(
        &self,
        namespace: NamespaceId,
        entries: GossipSender,
        values: Option<GossipSender>,
    ) {
        self.senders
            .lock()
            .expect("announcer senders poisoned")
            .insert(namespace, Senders { entries, values });
    }

    /// Removes a namespace's senders. Messages still in the queue for it are
    /// dropped in the loop: the namespace has left, and no one listens.
    pub fn unregister(&self, namespace: &NamespaceId) {
        self.senders
            .lock()
            .expect("announcer senders poisoned")
            .remove(namespace);
    }

    /// Extends the joined peers of a namespace's entries topic. No-op when
    /// the peer set is empty or the namespace is not registered.
    pub async fn join_peers(&self, namespace: &NamespaceId, peers: Vec<EndpointId>) {
        if peers.is_empty() {
            return;
        }
        let Some(entries) = self
            .senders
            .lock()
            .expect("announcer senders poisoned")
            .get(namespace)
            .map(|s| s.entries.clone())
        else {
            return;
        };
        if let Err(err) = entries.join_peers(peers).await {
            warn!(namespace = %namespace.fmt_short(), "announcer join_peers failed: {err}");
        }
    }

    /// Queues a broadcast without awaiting gossip.
    ///
    /// Returns `false` when the queue is full and the message was dropped:
    /// the caller should schedule a gossip-lag reaction for the namespace,
    /// so the peers that missed the message are repaired by a re-sync.
    pub fn try_announce(&self, namespace: NamespaceId, kind: AnnounceKind, message: Bytes) -> bool {
        match self.tx.try_send(AnnounceMsg {
            namespace,
            kind,
            message,
        }) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Closed(_)) => false,
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.announce_queue_full_drops.inc();
                let mut last = self.last_warned.lock().expect("announcer warn poisoned");
                let now = std::time::Instant::now();
                if last.is_none_or(|l| now.duration_since(l) >= ANNOUNCE_WARN_INTERVAL) {
                    *last = Some(now);
                    warn!(
                        namespace = %namespace.fmt_short(),
                        "announcer queue full, dropping broadcast ({} dropped total)",
                        self.metrics.announce_queue_full_drops.get(),
                    );
                }
                false
            }
        }
    }
}

async fn announce_loop(
    mut rx: mpsc::Receiver<AnnounceMsg>,
    senders: Arc<Mutex<HashMap<NamespaceId, Senders>>>,
) {
    while let Some(msg) = rx.recv().await {
        let Some(senders) = senders
            .lock()
            .expect("announcer senders poisoned")
            .get(&msg.namespace)
            .map(|s| Senders {
                entries: s.entries.clone(),
                values: s.values.clone(),
            })
        else {
            // The namespace left between queueing and delivery.
            continue;
        };
        let sent = match msg.kind {
            AnnounceKind::Entries => senders.entries.broadcast(msg.message).await,
            AnnounceKind::Values => match &senders.values {
                Some(values) => values.broadcast(msg.message).await,
                None => Ok(()),
            },
            AnnounceKind::Neighbors => senders.entries.broadcast_neighbors(msg.message).await,
        };
        if let Err(err) = sent {
            // The gossip actor's channel closed: the endpoint is shutting
            // down, and the remaining messages are moot.
            warn!(namespace = %msg.namespace.fmt_short(), "announcer broadcast failed: {err}");
            break;
        }
    }
}

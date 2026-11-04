//! This contains an actor spawned on a separate thread to process replica and store operations.

use std::{
    collections::{hash_map, BTreeSet, HashMap},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

use crate::sync::system_time_now;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use iroh_blobs::{
    api::{
        blobs::{BlobStatus, Blobs},
        TempTag,
    },
    Hash,
};
use irpc::channel::mpsc;
use n0_future::{task::JoinSet, time::Duration, TryFutureExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, Notify};
#[cfg(wasm_browser)]
use tracing::Instrument;
use tracing::{debug, error, error_span, trace, warn};

use crate::{
    api::{
        protocol::{AuthorListResponse, ListResponse},
        RpcError, RpcResult,
    },
    metrics::Metrics,
    net::WireFormat,
    ranger::Message,
    store::{
        fs::{ContentHashesIterator, StoreInstance},
        DownloadPolicy, ImportNamespaceOutcome, Query, Store,
    },
    Author, AuthorHeads, AuthorId, Capability, Clock, ContentStatus, ContentStatusCallback, Event,
    InsertError, NamespaceId, NamespaceSecret, PeerIdBytes, Replica, ReplicaInfo,
    RetireWriteAuthorityOutcome, SignedEntry, SyncOutcome, ValidationFailure,
    MAX_TIMESTAMP_FUTURE_SHIFT,
};

const ACTION_CAP: usize = 1024;
pub(crate) const MAX_COMMIT_DELAY: Duration = Duration::from_millis(500);

#[derive(derive_more::Debug, derive_more::Display)]
enum Action {
    #[display("NewAuthor")]
    ImportAuthor {
        author: Author,
        #[debug("reply")]
        reply: oneshot::Sender<Result<AuthorId>>,
    },
    #[display("ExportAuthor")]
    ExportAuthor {
        author: AuthorId,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<Author>>>,
    },
    #[display("DeleteAuthor")]
    DeleteAuthor {
        author: AuthorId,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    /// Retire this node's write authority over a namespace, keeping its
    /// entries. Refuses while any handle is open — the caller closes first.
    #[display("RetireWriteAuthority")]
    RetireWriteAuthority {
        namespace: NamespaceId,
        #[debug("reply")]
        reply: oneshot::Sender<Result<RetireWriteAuthorityOutcome>>,
    },
    #[display("NewReplica")]
    ImportNamespace {
        capability: Capability,
        #[debug("reply")]
        reply: oneshot::Sender<Result<NamespaceId>>,
    },
    #[display("ListAuthors")]
    ListAuthors {
        #[debug("reply")]
        reply: mpsc::Sender<RpcResult<AuthorListResponse>>,
    },
    #[display("ListReplicas")]
    ListReplicas {
        #[debug("reply")]
        reply: mpsc::Sender<RpcResult<ListResponse>>,
    },
    #[display("ContentHashes")]
    ContentHashes {
        #[debug("reply")]
        reply: oneshot::Sender<Result<ContentHashesIterator>>,
    },
    #[display("FlushStore")]
    FlushStore {
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    #[display("Replica({}, {})", _0.fmt_short(), _1)]
    Replica(NamespaceId, ReplicaAction),
    #[display("Shutdown")]
    Shutdown {
        #[debug("reply")]
        reply: Option<oneshot::Sender<Store>>,
    },
    #[cfg(test)]
    #[display("DebugTasksLen")]
    DebugTasksLen {
        #[debug("reply")]
        reply: oneshot::Sender<usize>,
    },
}

#[derive(derive_more::Debug, strum::Display)]
enum ReplicaAction {
    Open {
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
        opts: OpenOpts,
    },
    Close {
        #[debug("reply")]
        reply: oneshot::Sender<Result<bool>>,
    },
    GetState {
        #[debug("reply")]
        reply: oneshot::Sender<Result<OpenState>>,
    },
    SetSync {
        sync: bool,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        sender: async_channel::Sender<Event>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    Unsubscribe {
        sender: async_channel::Sender<Event>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    InsertLocal {
        author: AuthorId,
        key: Bytes,
        hash: Hash,
        len: u64,
        /// The content bytes, when the set API held them and they are within
        /// the inline-value threshold; see [`Event::LocalInsert`].
        #[debug(skip)]
        value: Option<Bytes>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    DeletePrefix {
        author: AuthorId,
        key: Bytes,
        #[debug("reply")]
        reply: oneshot::Sender<Result<usize>>,
    },
    InsertRemote {
        entry: SignedEntry,
        from: PeerIdBytes,
        content_status: ContentStatus,
        #[debug("reply")]
        reply: oneshot::Sender<Result<(), InsertRemoteError>>,
    },
    SyncInitialMessage {
        /// The connection's negotiated wire format: values are only attached
        /// (and imported) on the inline-values wire, so a legacy peer never
        /// sees the new shape.
        wire: WireFormat,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Message<SignedEntry>>>,
    },
    SyncProcessMessage {
        message: Message<SignedEntry>,
        from: PeerIdBytes,
        state: SyncOutcome,
        /// The connection's negotiated wire format; see `SyncInitialMessage`.
        wire: WireFormat,
        #[debug("reply")]
        reply: oneshot::Sender<Result<(Option<Message<SignedEntry>>, SyncOutcome)>>,
    },
    GetSyncPeers {
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<Vec<PeerIdBytes>>>>,
    },
    RegisterUsefulPeer {
        peer: PeerIdBytes,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    GetExact {
        author: AuthorId,
        key: Bytes,
        include_empty: bool,
        reply: oneshot::Sender<Result<Option<SignedEntry>>>,
    },
    GetMany {
        query: Query,
        reply: mpsc::Sender<RpcResult<SignedEntry>>,
    },
    GetManyVec {
        query: Query,
        reply: oneshot::Sender<Result<Vec<SignedEntry>>>,
    },
    DropReplica {
        reply: oneshot::Sender<Result<()>>,
    },
    ExportSecretKey {
        reply: oneshot::Sender<Result<NamespaceSecret>>,
    },
    HasNewsForUs {
        heads: AuthorHeads,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<NonZeroU64>>>,
    },
    SetDownloadPolicy {
        policy: DownloadPolicy,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    GetDownloadPolicy {
        #[debug("reply")]
        reply: oneshot::Sender<Result<DownloadPolicy>>,
    },
    SetAllowedWriters {
        writers: Option<BTreeSet<AuthorId>>,
        #[debug("reply")]
        reply: oneshot::Sender<Result<()>>,
    },
    GetAllowedWriters {
        #[debug("reply")]
        reply: oneshot::Sender<Result<Option<BTreeSet<AuthorId>>>>,
    },
    Fingerprint {
        #[debug("reply")]
        reply: oneshot::Sender<Result<[u8; 32]>>,
    },
    Sequence {
        #[debug("reply")]
        reply: oneshot::Sender<Result<u64>>,
    },
    EntriesSince {
        seq: u64,
        limit: usize,
        #[debug("reply")]
        reply: oneshot::Sender<Result<Vec<(u64, SignedEntry)>>>,
    },
}

/// The state for an open replica.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenState {
    /// Whether to accept sync requests for this replica.
    pub sync: bool,
    /// How many event subscriptions are open
    pub subscribers: usize,
    /// By how many handles the replica is currently held open
    pub handles: usize,
}

#[derive(Debug)]
struct OpenReplica {
    info: ReplicaInfo,
    sync: bool,
    handles: usize,
}

/// The [`SyncHandle`] controls an actor thread which executes replica and store operations.
///
/// The [`SyncHandle`] exposes async methods which all send messages into the actor thread, usually
/// returning something via a return channel. The actor thread itself is a regular [`std::thread`]
/// which processes incoming messages sequentially.
///
/// The handle is cheaply cloneable. Once the last clone is dropped, the actor thread is joined.
/// The thread will finish processing all messages in the channel queue, and then exit.
/// To prevent this last drop from blocking the calling thread, you can call [`SyncHandle::shutdown`]
/// and await its result before dropping the last [`SyncHandle`]. This ensures that
/// waiting for the actor to finish happens in an async context, and therefore that the final
/// [`SyncHandle::drop`] will not block.
#[derive(Debug, Clone)]
pub struct SyncHandle {
    tx: async_channel::Sender<Action>,
    #[cfg(wasm_browser)]
    #[allow(unused)]
    join_handle: Arc<Option<n0_future::task::JoinHandle<()>>>,
    #[cfg(not(wasm_browser))]
    join_handle: Arc<Option<std::thread::JoinHandle<()>>>,
    metrics: Arc<Metrics>,
}

/// Options when opening a replica.
#[derive(Debug, Default)]
pub struct OpenOpts {
    /// Set to true to set sync state to true.
    pub sync: bool,
    /// Optionally subscribe to replica events.
    pub subscribe: Option<async_channel::Sender<Event>>,
}

impl OpenOpts {
    /// Set sync state to true.
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }
    /// Subscribe to replica events.
    pub fn subscribe(mut self, subscribe: async_channel::Sender<Event>) -> Self {
        self.subscribe = Some(subscribe);
        self
    }
}

/// Why a remote insert received via gossip was refused.
#[derive(thiserror::Error, derive_more::Debug)]
pub enum InsertRemoteError {
    /// The sync actor could not be reached.
    #[error("sync actor error")]
    Actor(#[source] anyhow::Error),
    /// The entry failed validation and was not stored.
    #[error("entry failed validation: {failure}")]
    Validation {
        /// Why validation failed.
        failure: ValidationFailure,
        /// When the entry can pass validation; see [`crate::RejectedEntry::retry_after`].
        retry_after: Option<Duration>,
    },
    /// The entry failed to insert for another reason.
    #[error("insert failed")]
    Insert(#[source] InsertError),
}

#[allow(missing_docs)]
impl SyncHandle {
    /// Spawn a sync actor and return a handle.
    pub fn spawn(
        store: Store,
        content_status_callback: Option<ContentStatusCallback>,
        me: String,
    ) -> SyncHandle {
        Self::spawn_with_clock(store, content_status_callback, me, None)
    }

    /// Like [`Self::spawn`], with a [`Clock`] applied to every replica the actor opens.
    pub fn spawn_with_clock(
        store: Store,
        content_status_callback: Option<ContentStatusCallback>,
        me: String,
        clock: Option<Arc<dyn Clock>>,
    ) -> SyncHandle {
        Self::spawn_with_options(
            store,
            content_status_callback,
            me,
            clock,
            MAX_TIMESTAMP_FUTURE_SHIFT,
            // The clock-variant surface predates inline values; it keeps the feature off.
            0,
            None,
            None,
        )
    }

    /// Like [`Self::spawn_with_clock`], with the future-shift limit applied to every replica
    /// the actor opens.
    ///
    /// `max_future_shift` is how far ahead of the wall clock a remote entry may be stamped
    /// before it is refused, in microseconds. It only applies without a [`Clock`]; see
    /// [`MAX_TIMESTAMP_FUTURE_SHIFT`].
    ///
    /// `max_inline_value_size` enables the inline-value feature: entries at or below this many
    /// bytes of content may carry their value inside sync and gossip messages. `blobs` and
    /// `tag_hold` wire the feature into the blob store; the shared holder guards the GC window
    /// for imported values (see [`TagHold`]). A threshold of 0, or no blob store, disables the
    /// feature.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_with_options(
        store: Store,
        content_status_callback: Option<ContentStatusCallback>,
        me: String,
        clock: Option<Arc<dyn Clock>>,
        max_future_shift: u64,
        max_inline_value_size: usize,
        blobs: Option<Blobs>,
        tag_hold: Option<TagHold>,
    ) -> SyncHandle {
        let metrics = Arc::new(Metrics::default());
        let (action_tx, action_rx) = async_channel::bounded(ACTION_CAP);
        let inline =
            blobs.and_then(|blobs| InlineContent::new(blobs, max_inline_value_size, tag_hold));
        let actor = Actor {
            store,
            states: Default::default(),
            action_rx,
            content_status_callback,
            clock,
            max_future_shift,
            inline,
            tasks: Default::default(),
            metrics: metrics.clone(),
        };

        let span = error_span!("sync", %me);
        #[cfg(wasm_browser)]
        let join_handle = n0_future::task::spawn(actor.run_async().instrument(span));

        #[cfg(not(wasm_browser))]
        let join_handle = std::thread::Builder::new()
            .name("sync-actor".to_string())
            .spawn(move || {
                let _enter = span.enter();

                if let Err(err) = actor.run_in_thread() {
                    error!("Sync actor closed with error: {err:?}");
                }
            })
            .expect("failed to spawn thread");

        let join_handle = Arc::new(Some(join_handle));
        SyncHandle {
            tx: action_tx,
            join_handle,
            metrics,
        }
    }

    /// Returns the metrics collected in this sync actor.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    pub async fn open(&self, namespace: NamespaceId, opts: OpenOpts) -> Result<()> {
        tracing::debug!("SyncHandle::open called");
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::Open { reply, opts };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn close(&self, namespace: NamespaceId) -> Result<bool> {
        let (reply, rx) = oneshot::channel();
        self.send_replica(namespace, ReplicaAction::Close { reply })
            .await?;
        rx.await?
    }

    pub async fn subscribe(
        &self,
        namespace: NamespaceId,
        sender: async_channel::Sender<Event>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send_replica(namespace, ReplicaAction::Subscribe { sender, reply })
            .await?;
        rx.await?
    }

    pub async fn unsubscribe(
        &self,
        namespace: NamespaceId,
        sender: async_channel::Sender<Event>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send_replica(namespace, ReplicaAction::Unsubscribe { sender, reply })
            .await?;
        rx.await?
    }

    pub async fn set_sync(&self, namespace: NamespaceId, sync: bool) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SetSync { sync, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn insert_local(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
        hash: Hash,
        len: u64,
        value: Option<Bytes>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::InsertLocal {
            author,
            key,
            hash,
            len,
            value,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn delete_prefix(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
    ) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::DeletePrefix { author, key, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn insert_remote(
        &self,
        namespace: NamespaceId,
        entry: SignedEntry,
        from: PeerIdBytes,
        content_status: ContentStatus,
    ) -> Result<(), InsertRemoteError> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::InsertRemote {
            entry,
            from,
            content_status,
            reply,
        };
        self.send_replica(namespace, action)
            .await
            .map_err(InsertRemoteError::Actor)?;
        rx.await
            .map_err(|_| InsertRemoteError::Actor(anyhow!("sync actor closed")))?
    }

    pub async fn sync_initial_message(
        &self,
        namespace: NamespaceId,
        wire: WireFormat,
    ) -> Result<Message<SignedEntry>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SyncInitialMessage { wire, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn sync_process_message(
        &self,
        namespace: NamespaceId,
        message: Message<SignedEntry>,
        from: PeerIdBytes,
        state: SyncOutcome,
        wire: WireFormat,
    ) -> Result<(Option<Message<SignedEntry>>, SyncOutcome)> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SyncProcessMessage {
            reply,
            message,
            from,
            state,
            wire,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_sync_peers(&self, namespace: NamespaceId) -> Result<Option<Vec<PeerIdBytes>>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetSyncPeers { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn register_useful_peer(
        &self,
        namespace: NamespaceId,
        peer: PeerIdBytes,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::RegisterUsefulPeer { reply, peer };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn has_news_for_us(
        &self,
        namespace: NamespaceId,
        heads: AuthorHeads,
    ) -> Result<Option<NonZeroU64>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::HasNewsForUs { reply, heads };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_many(
        &self,
        namespace: NamespaceId,
        query: Query,
        reply: mpsc::Sender<RpcResult<SignedEntry>>,
    ) -> Result<()> {
        let action = ReplicaAction::GetMany { query, reply };
        self.send_replica(namespace, action).await?;
        Ok(())
    }

    pub async fn get_many_vec(
        &self,
        namespace: NamespaceId,
        query: Query,
    ) -> Result<Vec<SignedEntry>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetManyVec { query, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_exact(
        &self,
        namespace: NamespaceId,
        author: AuthorId,
        key: Bytes,
        include_empty: bool,
    ) -> Result<Option<SignedEntry>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetExact {
            author,
            key,
            include_empty,
            reply,
        };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn drop_replica(&self, namespace: NamespaceId) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::DropReplica { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn export_secret_key(&self, namespace: NamespaceId) -> Result<NamespaceSecret> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::ExportSecretKey { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_state(&self, namespace: NamespaceId) -> Result<OpenState> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetState { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn shutdown(&self) -> Result<Store> {
        let (reply, rx) = oneshot::channel();
        let action = Action::Shutdown { reply: Some(reply) };
        self.send(action).await?;
        let store = rx.await?;
        Ok(store)
    }

    #[cfg(test)]
    async fn debug_tasks_len(&self) -> Result<usize> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::DebugTasksLen { reply }).await?;
        Ok(rx.await?)
    }

    pub async fn list_authors(
        &self,
        reply: mpsc::Sender<RpcResult<AuthorListResponse>>,
    ) -> Result<()> {
        self.send(Action::ListAuthors { reply }).await
    }

    pub async fn list_replicas(&self, reply: mpsc::Sender<RpcResult<ListResponse>>) -> Result<()> {
        self.send(Action::ListReplicas { reply }).await
    }

    pub async fn import_author(&self, author: Author) -> Result<AuthorId> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ImportAuthor { author, reply }).await?;
        rx.await?
    }

    pub async fn export_author(&self, author: AuthorId) -> Result<Option<Author>> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ExportAuthor { author, reply }).await?;
        rx.await?
    }

    pub async fn delete_author(&self, author: AuthorId) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::DeleteAuthor { author, reply }).await?;
        rx.await?
    }

    /// Retire this node's write authority over a namespace, keeping its
    /// entries. Refuses with `Busy` while any handle is open.
    pub async fn retire_write_authority(
        &self,
        namespace: NamespaceId,
    ) -> Result<RetireWriteAuthorityOutcome> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::RetireWriteAuthority { namespace, reply })
            .await?;
        rx.await?
    }

    pub async fn import_namespace(&self, capability: Capability) -> Result<NamespaceId> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ImportNamespace { capability, reply })
            .await?;
        rx.await?
    }

    pub async fn get_download_policy(&self, namespace: NamespaceId) -> Result<DownloadPolicy> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetDownloadPolicy { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn set_download_policy(
        &self,
        namespace: NamespaceId,
        policy: DownloadPolicy,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SetDownloadPolicy { reply, policy };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Restricts which authors may write to `namespace` (`None` lifts the restriction).
    ///
    /// The setting is flushed to the store before this returns.
    pub async fn set_allowed_writers(
        &self,
        namespace: NamespaceId,
        writers: Option<BTreeSet<AuthorId>>,
    ) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::SetAllowedWriters { reply, writers };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn get_allowed_writers(
        &self,
        namespace: NamespaceId,
    ) -> Result<Option<BTreeSet<AuthorId>>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::GetAllowedWriters { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Returns the XOR aggregate fingerprint over the namespace's current
    /// record rows.
    ///
    /// Rust-only surface for now (an Aster introspection query); a C ABI and
    /// binding parity pass is planned.
    pub async fn namespace_fingerprint(&self, namespace: NamespaceId) -> Result<[u8; 32]> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::Fingerprint { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Returns the insertion sequence of the last state-changing put into
    /// the namespace.
    ///
    /// Rust-only surface for now (an Aster introspection query); a C ABI and
    /// binding parity pass is planned.
    pub async fn namespace_sequence(&self, namespace: NamespaceId) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::Sequence { reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    /// Returns the keys touched by puts since `seq`, each with its latest
    /// entry and the sequence at which it was last touched, ordered by touch
    /// sequence. `limit` bounds the page: at most `limit` pairs, or the whole
    /// tail when 0.
    ///
    /// Rust-only surface for now (an Aster introspection query); a C ABI and
    /// binding parity pass is planned.
    pub async fn entries_since(
        &self,
        namespace: NamespaceId,
        seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, SignedEntry)>> {
        let (reply, rx) = oneshot::channel();
        let action = ReplicaAction::EntriesSince { seq, limit, reply };
        self.send_replica(namespace, action).await?;
        rx.await?
    }

    pub async fn content_hashes(&self) -> Result<ContentHashesIterator> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::ContentHashes { reply }).await?;
        rx.await?
    }

    /// Makes sure that all pending database operations are persisted to disk.
    ///
    /// Otherwise, database operations are batched into bigger transactions for speed.
    /// Use this if you need to make sure something is written to the database
    /// before another operation, e.g. to make sure sudden process exits don't corrupt
    /// your application state.
    ///
    /// It's not necessary to call this function before shutdown, as `shutdown` will
    /// trigger a flush on its own.
    pub async fn flush_store(&self) -> Result<()> {
        let (reply, rx) = oneshot::channel();
        self.send(Action::FlushStore { reply }).await?;
        rx.await?
    }

    async fn send(&self, action: Action) -> Result<()> {
        self.tx
            .send(action)
            .await
            .context("sending to iroh_docs actor failed")?;
        Ok(())
    }
    async fn send_replica(&self, namespace: NamespaceId, action: ReplicaAction) -> Result<()> {
        self.send(Action::Replica(namespace, action)).await?;
        Ok(())
    }
}

impl Drop for SyncHandle {
    fn drop(&mut self) {
        // this means we're dropping the last reference
        #[allow(unused)]
        if let Some(handle) = Arc::get_mut(&mut self.join_handle) {
            #[cfg(wasm_browser)]
            {
                let tx = self.tx.clone();
                n0_future::task::spawn(async move {
                    tx.send(Action::Shutdown { reply: None }).await.ok();
                });
            }
            #[cfg(not(wasm_browser))]
            {
                // this call is the reason tx can not be a tokio mpsc channel.
                // we have no control about where drop is called, yet tokio send_blocking panics
                // when called from inside a tokio runtime.
                self.tx.send_blocking(Action::Shutdown { reply: None }).ok();
                let handle = handle.take().expect("this can only run once");

                if let Err(err) = handle.join() {
                    warn!(?err, "Failed to join sync actor");
                }
            }
        }
    }
}

/// Shared batch holder for the temp tags of freshly published or imported
/// content.
///
/// A publish path's temp tag is the imported blob's GC protection between its
/// import and the document record naming it becoming visible to the protect
/// enumeration. The tag must therefore outlive one full enumeration cycle:
/// it releases only after the enumeration that observes the record has run,
/// plus the sweep that missed the record has finished marking. Per-tag
/// waiter tasks would hold one task per publish forever on a default config;
/// instead every tag joins a batch, and each enumeration-notify fire retires
/// the batch that was current at the previous fire. Two-phase rotation makes
/// the retirement safe: a tag enqueued while an enumeration is in flight is
/// still in `current` when that enumeration fires, moves to `retiring`, and
/// is only dropped when the *next* enumeration fires — every tag therefore
/// survives at least one full cycle after it was enqueued. A lost
/// notification only over-holds (safe); a dropped tag is the GC wedge.
///
/// One drain task per engine watches the notifier; the mutexes are held only
/// for the enqueue/rotation, never across an await. The task holds a weak
/// reference and exits once the last [`TagHold`] is gone, which notifies it
/// one final time, so engine shutdown cannot leak the task (or its last
/// batch of tags) into a silent wait on a notifier nobody will fire again.
#[derive(Debug, Clone)]
pub(crate) struct TagHold {
    inner: Arc<TagHoldInner>,
}

#[derive(Debug)]
struct TagHoldInner {
    notify: Arc<Notify>,
    current: Mutex<Vec<TempTag>>,
    retiring: Mutex<Vec<TempTag>>,
}

impl Drop for TagHoldInner {
    fn drop(&mut self) {
        // Wake the drain task one last time so it can observe the weak
        // upgrade failing and exit. `notify_one` stores a permit when the
        // task is mid-rotation rather than waiting, so it wakes on its next
        // `notified().await` either way.
        self.notify.notify_one();
    }
}

impl TagHold {
    /// Creates the holder and its drain task. `notify` is the engine's
    /// protect-enumeration notifier; callers enqueue tags and the task
    /// retires them on the fire cadence described on [`TagHold`].
    pub(crate) fn new(notify: Arc<Notify>) -> TagHold {
        let hold = TagHold {
            inner: Arc::new(TagHoldInner {
                notify,
                current: Mutex::new(Vec::new()),
                retiring: Mutex::new(Vec::new()),
            }),
        };
        let weak = Arc::downgrade(&hold.inner);
        n0_future::task::spawn(async move {
            loop {
                let Some(inner) = weak.upgrade() else {
                    return;
                };
                inner.notify.notified().await;
                let Some(inner) = weak.upgrade() else {
                    // The last holder dropped while this task was waiting,
                    // and its final notification woke us: nothing holds the
                    // tags anymore, so nothing can protect them. Exit.
                    return;
                };
                let old = {
                    let mut retiring = inner.retiring.lock().expect("tag hold poisoned");
                    let mut current = inner.current.lock().expect("tag hold poisoned");
                    std::mem::replace(&mut *retiring, std::mem::take(&mut *current))
                };
                // The previous retiring batch was observed by the enumeration
                // that just completed (and the one before it), so it drops now.
                drop(old);
            }
        });
        hold
    }

    /// Holds `tag` until the protect enumeration after the next one has
    /// completed. Callers that see no [`TagHold`] at all (no protect handler
    /// installed) drop the tag immediately instead.
    pub(crate) fn hold(&self, tag: TempTag) {
        self.inner
            .current
            .lock()
            .expect("tag hold poisoned")
            .push(tag);
    }
}

/// Inline-value plumbing for the set-reconciliation and gossip paths.
///
/// Entries whose content fits the configured threshold can carry their value
/// inside the sync/gossip message itself; the receiver imports it directly,
/// avoiding one round trip per entry's value-blob download. The actor holds
/// `None` when the feature is off (threshold 0, or no blob store wired in).
pub(crate) struct InlineContent {
    blobs: Blobs,
    /// Values at or below this many bytes may ride a message.
    max_value_size: usize,
    /// Guards the GC window: a temp tag on an imported value lives until the
    /// protect enumeration after the one that observes its document record.
    tag_hold: Option<TagHold>,
}

impl InlineContent {
    pub(crate) fn new(
        blobs: Blobs,
        max_value_size: usize,
        tag_hold: Option<TagHold>,
    ) -> Option<Self> {
        (max_value_size > 0).then_some(Self {
            blobs,
            max_value_size,
            tag_hold,
        })
    }

    /// Import a value carried inline with `entry`. Returns `true` when the
    /// content is locally complete afterwards. The value is verified against
    /// the entry before import; a value that fails verification, exceeds the
    /// threshold or trips a store error leaves the content status untouched.
    pub(crate) async fn import(&self, entry: &SignedEntry, value: Option<Bytes>) -> bool {
        let Some(value) = value else {
            return false;
        };
        if value.len() as u64 != entry.content_len() || value.len() > self.max_value_size {
            return false;
        }
        if Hash::new(&value) != entry.content_hash() {
            debug!(hash = %entry.content_hash(), "inline value failed verification; ignoring");
            return false;
        }
        match self.blobs.status(entry.content_hash()).await {
            Ok(BlobStatus::Complete { .. }) => true,
            Ok(_) => match self.blobs.add_bytes(value).temp_tag().await {
                Ok(tag) => {
                    self.hold_until_protected(tag);
                    true
                }
                Err(err) => {
                    debug!(?err, "failed to import inline value");
                    false
                }
            },
            Err(err) => {
                debug!(?err, "failed to check status before inline import");
                false
            }
        }
    }

    /// Attach locally complete values to the entries of an outgoing
    /// reconciliation message, in one batch read.
    pub(crate) async fn attach(&self, message: &mut Message<SignedEntry>) {
        let hashes: Vec<Hash> = message
            .values()
            .filter(|(entry, _, _)| entry.content_len() <= self.max_value_size as u64)
            .map(|(entry, _, _)| entry.content_hash())
            .collect();
        if hashes.is_empty() {
            return;
        }
        let results = match self.blobs.get_bytes_many_if_complete(hashes).await {
            Ok(results) => results,
            Err(err) => {
                debug!(?err, "inline value batch read failed");
                return;
            }
        };
        // The two passes walk the same predicate, so hashes and entries align.
        let mut results = results.into_iter();
        for (entry, _, content) in message.values_mut() {
            if entry.content_len() > self.max_value_size as u64 {
                continue;
            }
            let Some(result) = results.next() else {
                break;
            };
            if let Ok(Some(bytes)) = result {
                *content = Some(bytes);
            }
        }
    }

    fn hold_until_protected(&self, tag: TempTag) {
        let Some(hold) = &self.tag_hold else {
            drop(tag);
            return;
        };
        hold.hold(tag);
    }
}

struct Actor {
    store: Store,
    states: OpenReplicas,
    action_rx: async_channel::Receiver<Action>,
    content_status_callback: Option<ContentStatusCallback>,
    clock: Option<Arc<dyn Clock>>,
    max_future_shift: u64,
    inline: Option<InlineContent>,
    tasks: JoinSet<()>,
    metrics: Arc<Metrics>,
}

impl Actor {
    #[cfg(not(wasm_browser))]
    fn run_in_thread(self) -> Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()?;
        let local_set = tokio::task::LocalSet::new();
        local_set.block_on(&rt, async move { self.run_async().await });
        Ok(())
    }

    async fn run_async(mut self) {
        let reply = loop {
            let timeout = n0_future::time::sleep(MAX_COMMIT_DELAY);
            tokio::pin!(timeout);
            let action = tokio::select! {
                _ = &mut timeout => {
                    if let Err(cause) = self.store.flush() {
                        error!(?cause, "failed to flush store");
                    }
                    continue;
                }
                Some(res) = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    if let Err(err) = res {
                        if !err.is_cancelled() {
                            warn!(?err, "actor reply-streamer task panicked");
                        }
                    }
                    continue;
                }
                action = self.action_rx.recv() => {
                    match action {
                        Ok(action) => action,
                        Err(async_channel::RecvError) => {
                            debug!("action channel disconnected");
                            break None;
                        }

                    }
                }
            };
            trace!(%action, "tick");
            self.metrics.actor_tick_main.inc();
            match action {
                Action::Shutdown { reply } => {
                    break reply;
                }
                action => {
                    if self.on_action(action).await.is_err() {
                        warn!("failed to send reply: receiver dropped");
                    }
                }
            }
        };

        if let Err(cause) = self.store.flush() {
            warn!(?cause, "failed to flush store");
        }
        self.close_all();
        self.tasks.abort_all();
        debug!("docs actor shutdown");
        if let Some(reply) = reply {
            reply.send(self.store).ok();
        }
    }

    async fn on_action(&mut self, action: Action) -> Result<(), SendReplyError> {
        match action {
            Action::Shutdown { .. } => {
                unreachable!("Shutdown is handled in run()")
            }
            #[cfg(test)]
            Action::DebugTasksLen { reply } => send_reply(reply, self.tasks.len()),
            Action::ImportAuthor { author, reply } => {
                let id = author.id();
                send_reply(reply, self.store.import_author(author).map(|_| id))
            }
            Action::ExportAuthor { author, reply } => {
                send_reply(reply, self.store.get_author(&author))
            }
            Action::DeleteAuthor { author, reply } => {
                send_reply(reply, self.store.delete_author(author))
            }
            Action::RetireWriteAuthority { namespace, reply } => {
                send_reply_with(reply, self, |this| {
                    // Deliberately does **not** close anything. `close` is a
                    // reference-count decrement, so closing here would retire
                    // one handle out of however many the caller holds and
                    // silently depend on that count being one. The caller owns
                    // its handles and closes them; this only reports what it
                    // finds.
                    if let Ok(state) = this.states.get_mut(&namespace) {
                        return Ok(RetireWriteAuthorityOutcome::Busy {
                            handles: state.handles as u64,
                        });
                    }
                    let capability = this.store.retire_write_authority(&namespace)?;
                    Ok(RetireWriteAuthorityOutcome::Retired(capability))
                })
            }
            Action::ImportNamespace { capability, reply } => send_reply_with(reply, self, |this| {
                let id = capability.id();
                let outcome = this.store.import_namespace(capability.clone())?;
                if let ImportNamespaceOutcome::Upgraded = outcome {
                    if let Ok(state) = this.states.get_mut(&id) {
                        state.info.merge_capability(capability)?;
                    }
                }
                Ok(id)
            }),
            Action::ListAuthors { reply } => {
                let iter = self
                    .store
                    .list_authors()
                    .map(|a| a.map(|a| a.map(|a| AuthorListResponse { author_id: a.id() })));
                self.tasks.spawn_local(async move {
                    iter_to_irpc(reply, iter).await.ok();
                });
                Ok(())
            }
            Action::ListReplicas { reply } => {
                let iter = self.store.list_namespaces();
                let iter = iter.map(|inner| {
                    inner.map(|res| res.map(|(id, capability)| ListResponse { id, capability }))
                });
                self.tasks.spawn_local(async move {
                    iter_to_irpc(reply, iter).await.ok();
                });
                Ok(())
            }
            Action::ContentHashes { reply } => {
                send_reply_with(reply, self, |this| this.store.content_hashes())
            }
            Action::FlushStore { reply } => send_reply(reply, self.store.flush()),
            Action::Replica(namespace, action) => self.on_replica_action(namespace, action).await,
        }
    }

    async fn on_replica_action(
        &mut self,
        namespace: NamespaceId,
        action: ReplicaAction,
    ) -> Result<(), SendReplyError> {
        match action {
            ReplicaAction::Open { reply, opts } => {
                tracing::trace!("open in");
                let res = self.open(namespace, opts);
                tracing::trace!("open out");
                send_reply(reply, res)
            }
            ReplicaAction::Close { reply } => {
                let res = self.close(namespace);
                // ignore errors when no receiver is present for close
                reply.send(Ok(res)).ok();
                Ok(())
            }
            ReplicaAction::Subscribe { sender, reply } => send_reply_with(reply, self, |this| {
                let state = this.states.get_mut(&namespace)?;
                state.info.subscribe(sender);
                Ok(())
            }),
            ReplicaAction::Unsubscribe { sender, reply } => send_reply_with(reply, self, |this| {
                let state = this.states.get_mut(&namespace)?;
                state.info.unsubscribe(&sender);
                drop(sender);
                Ok(())
            }),
            ReplicaAction::SetSync { sync, reply } => send_reply_with(reply, self, |this| {
                let state = this.states.get_mut(&namespace)?;
                state.sync = sync;
                Ok(())
            }),
            ReplicaAction::InsertLocal {
                author,
                key,
                hash,
                len,
                value,
                reply,
            } => {
                send_reply_with_async(reply, self, async move |this| {
                    let author = get_author(&mut this.store, &author)?;
                    let mut replica = this.states.replica(namespace, &mut this.store)?;
                    replica
                        .insert_with_value(&key, &author, hash, len, value)
                        .await?;
                    this.metrics.new_entries_local.inc();
                    this.metrics.new_entries_local_size.inc_by(len);
                    Ok(())
                })
                .await
            }
            ReplicaAction::DeletePrefix { author, key, reply } => {
                send_reply_with_async(reply, self, async |this| {
                    let author = get_author(&mut this.store, &author)?;
                    let mut replica = this.states.replica(namespace, &mut this.store)?;
                    let res = replica.delete_prefix(&key, &author).await?;
                    Ok(res)
                })
                .await
            }
            ReplicaAction::InsertRemote {
                entry,
                from,
                content_status,
                reply,
            } => {
                let res = async {
                    let mut replica = self
                        .states
                        .replica_if_syncing(&namespace, &mut self.store)
                        .map_err(InsertRemoteError::Actor)?;
                    let len = entry.content_len();
                    let timestamp = entry.timestamp();
                    match replica
                        .insert_remote_entry(entry, from, content_status)
                        .await
                    {
                        Ok(_removed) => {
                            self.metrics.new_entries_remote.inc();
                            self.metrics.new_entries_remote_size.inc_by(len);
                            Ok(())
                        }
                        Err(InsertError::Validation(failure)) => {
                            self.metrics.entries_rejected_validation.inc();
                            // Same rule as the sync path: when a clock is
                            // configured, its bound decides the retry delay.
                            let future_bound = self
                                .clock
                                .as_deref()
                                .map(|clock| clock.max_future_shift())
                                .unwrap_or(self.max_future_shift);
                            Err(InsertRemoteError::Validation {
                                retry_after: if matches!(
                                    &failure,
                                    ValidationFailure::TooFarInTheFuture
                                ) {
                                    Some(Duration::from_micros(
                                        timestamp
                                            .saturating_sub(system_time_now())
                                            .saturating_sub(future_bound),
                                    ))
                                } else {
                                    None
                                },
                                failure,
                            })
                        }
                        Err(err) => Err(InsertRemoteError::Insert(err)),
                    }
                }
                .await;
                reply.send(res).map_err(send_reply_error)
            }

            ReplicaAction::SyncInitialMessage { wire, reply } => {
                send_reply_with_async(reply, self, async move |this| {
                    let mut replica = this
                        .states
                        .replica_if_syncing(&namespace, &mut this.store)?;
                    let mut res = replica.sync_initial_message()?;
                    // Values only ride the inline-values wire; a legacy peer
                    // must never receive the new message shape.
                    if wire == WireFormat::Inline {
                        if let Some(inline) = &this.inline {
                            inline.attach(&mut res).await;
                        }
                    }
                    Ok(res)
                })
                .await
            }
            ReplicaAction::SyncProcessMessage {
                message,
                from,
                mut state,
                wire,
                reply,
            } => {
                let res = async {
                    let mut replica = self
                        .states
                        .replica_if_syncing(&namespace, &mut self.store)?;
                    let inline = (wire == WireFormat::Inline)
                        .then_some(self.inline.as_ref())
                        .flatten();
                    let mut res = replica
                        .sync_process_message(message, from, &mut state, inline)
                        .await?;
                    if let Some(inline) = inline {
                        if let Some(ref mut msg) = res {
                            inline.attach(msg).await;
                        }
                    }
                    Ok((res, state))
                }
                .await;
                reply.send(res).map_err(send_reply_error)
            }
            ReplicaAction::GetSyncPeers { reply } => send_reply_with(reply, self, move |this| {
                this.states.ensure_open(&namespace)?;
                let peers = this.store.get_sync_peers(&namespace)?;
                Ok(peers.map(|iter| iter.collect()))
            }),
            ReplicaAction::RegisterUsefulPeer { peer, reply } => {
                let res = self.store.register_useful_peer(namespace, peer);
                send_reply(reply, res)
            }
            ReplicaAction::GetExact {
                author,
                key,
                include_empty,
                reply,
            } => send_reply_with(reply, self, move |this| {
                this.states.ensure_open(&namespace)?;
                this.store.get_exact(namespace, author, key, include_empty)
            }),
            ReplicaAction::GetMany { query, reply } => {
                let iter = self
                    .states
                    .ensure_open(&namespace)
                    .and_then(|_| self.store.get_many(namespace, query));
                self.tasks
                    .spawn_local(iter_to_irpc(reply, iter).map_ok_or_else(|_| (), |_| ()));
                Ok(())
            }
            ReplicaAction::GetManyVec { query, reply } => {
                send_reply_with(reply, self, move |this| {
                    this.states.ensure_open(&namespace)?;
                    this.store
                        .get_many(namespace, query)?
                        .collect::<Result<Vec<_>>>()
                })
            }
            ReplicaAction::DropReplica { reply } => send_reply_with(reply, self, |this| {
                this.close(namespace);
                this.store.remove_replica(&namespace)
            }),
            ReplicaAction::ExportSecretKey { reply } => {
                let res = self
                    .states
                    .get_mut(&namespace)
                    .and_then(|state| Ok(state.info.capability.secret_key()?.clone()));
                send_reply(reply, res)
            }
            ReplicaAction::GetState { reply } => send_reply_with(reply, self, move |this| {
                let state = this.states.get_mut(&namespace)?;
                let handles = state.handles;
                let sync = state.sync;
                let subscribers = state.info.subscribers_count();
                Ok(OpenState {
                    handles,
                    sync,
                    subscribers,
                })
            }),
            ReplicaAction::HasNewsForUs { heads, reply } => {
                let res = self.store.has_news_for_us(namespace, &heads);
                send_reply(reply, res)
            }
            ReplicaAction::SetDownloadPolicy { policy, reply } => {
                send_reply(reply, self.store.set_download_policy(&namespace, policy))
            }
            ReplicaAction::GetDownloadPolicy { reply } => {
                send_reply(reply, self.store.get_download_policy(&namespace))
            }
            ReplicaAction::SetAllowedWriters { writers, reply } => {
                // An access-control change is committed before it is acknowledged.
                let res = self
                    .store
                    .set_allowed_writers(&namespace, writers)
                    .and_then(|()| self.store.flush());
                send_reply(reply, res)
            }
            ReplicaAction::GetAllowedWriters { reply } => {
                send_reply(reply, self.store.get_allowed_writers(&namespace))
            }
            ReplicaAction::Fingerprint { reply } => send_reply_with(reply, self, |this| {
                let mut replica = this.states.replica(namespace, &mut this.store)?;
                Ok(replica.namespace_fingerprint()?.0)
            }),
            ReplicaAction::Sequence { reply } => send_reply_with(reply, self, |this| {
                let mut replica = this.states.replica(namespace, &mut this.store)?;
                replica.namespace_sequence()
            }),
            ReplicaAction::EntriesSince { seq, limit, reply } => {
                send_reply_with(reply, self, |this| {
                    let mut replica = this.states.replica(namespace, &mut this.store)?;
                    replica.entries_since(seq, limit)
                })
            }
        }
    }

    fn close(&mut self, namespace: NamespaceId) -> bool {
        let res = self.states.close(namespace);
        if res {
            self.store.close_replica(namespace);
        }
        res
    }

    fn close_all(&mut self) {
        for id in self.states.close_all() {
            self.store.close_replica(id);
        }
    }

    fn open(&mut self, namespace: NamespaceId, opts: OpenOpts) -> Result<()> {
        let open_cb = || {
            let mut info = self.store.load_replica_info(&namespace)?;
            if let Some(cb) = &self.content_status_callback {
                info.set_content_status_callback(Arc::clone(cb));
            }
            if let Some(clock) = &self.clock {
                info.set_clock(Arc::clone(clock));
            }
            info.set_max_future_shift(self.max_future_shift);
            Ok(info)
        };
        self.states.open_with(namespace, opts, open_cb)
    }
}

#[derive(Default)]
struct OpenReplicas(HashMap<NamespaceId, OpenReplica>);

impl OpenReplicas {
    fn replica<'a, 'b>(
        &'a mut self,
        namespace: NamespaceId,
        store: &'b mut Store,
    ) -> Result<Replica<'b, &'a mut ReplicaInfo>> {
        let state = self.get_mut(&namespace)?;
        Ok(Replica::new(
            StoreInstance::new(state.info.capability.id(), store),
            &mut state.info,
        ))
    }

    fn replica_if_syncing<'a, 'b>(
        &'a mut self,
        namespace: &NamespaceId,
        store: &'b mut Store,
    ) -> Result<Replica<'b, &'a mut ReplicaInfo>> {
        let state = self.get_mut(namespace)?;
        anyhow::ensure!(state.sync, "sync is not enabled for replica");
        Ok(Replica::new(
            StoreInstance::new(state.info.capability.id(), store),
            &mut state.info,
        ))
    }

    fn get_mut(&mut self, namespace: &NamespaceId) -> Result<&mut OpenReplica> {
        self.0.get_mut(namespace).context("replica not open")
    }

    fn is_open(&self, namespace: &NamespaceId) -> bool {
        self.0.contains_key(namespace)
    }

    fn ensure_open(&self, namespace: &NamespaceId) -> Result<()> {
        match self.is_open(namespace) {
            true => Ok(()),
            false => Err(anyhow!("replica not open")),
        }
    }
    fn open_with(
        &mut self,
        namespace: NamespaceId,
        opts: OpenOpts,
        mut open_cb: impl FnMut() -> Result<ReplicaInfo>,
    ) -> Result<()> {
        match self.0.entry(namespace) {
            hash_map::Entry::Vacant(e) => {
                let mut info = open_cb()?;
                if let Some(sender) = opts.subscribe {
                    info.subscribe(sender);
                }
                debug!(namespace = %namespace.fmt_short(), "open");
                let state = OpenReplica {
                    info,
                    sync: opts.sync,
                    handles: 1,
                };
                e.insert(state);
            }
            hash_map::Entry::Occupied(mut e) => {
                let state = e.get_mut();
                state.handles += 1;
                state.sync = state.sync || opts.sync;
                if let Some(sender) = opts.subscribe {
                    state.info.subscribe(sender);
                }
            }
        }
        Ok(())
    }
    fn close(&mut self, namespace: NamespaceId) -> bool {
        match self.0.entry(namespace) {
            hash_map::Entry::Vacant(_e) => {
                warn!(namespace = %namespace.fmt_short(), "received close request for closed replica");
                true
            }
            hash_map::Entry::Occupied(mut e) => {
                let state = e.get_mut();
                tracing::debug!("STATE {state:?}");
                state.handles = state.handles.wrapping_sub(1);
                if state.handles == 0 {
                    let _ = e.remove_entry();
                    debug!(namespace = %namespace.fmt_short(), "close");
                    true
                } else {
                    false
                }
            }
        }
    }

    fn close_all(&mut self) -> impl Iterator<Item = NamespaceId> + '_ {
        self.0.drain().map(|(n, _s)| n)
    }
}

async fn iter_to_irpc<T: irpc::RpcMessage>(
    channel: mpsc::Sender<RpcResult<T>>,
    iter: Result<impl Iterator<Item = Result<T>>>,
) -> Result<(), SendReplyError> {
    match iter {
        Err(err) => channel
            .send(Err(RpcError::new(&*err)))
            .await
            .map_err(send_reply_error)?,
        Ok(iter) => {
            for item in iter {
                let item = item.map_err(|err| RpcError::new(&*err));
                channel.send(item).await.map_err(send_reply_error)?;
            }
        }
    }
    Ok(())
}

fn get_author(store: &mut Store, id: &AuthorId) -> Result<Author> {
    store.get_author(id)?.context("author not found")
}

#[derive(Debug)]
struct SendReplyError;

fn send_reply<T>(sender: oneshot::Sender<T>, value: T) -> Result<(), SendReplyError> {
    sender.send(value).map_err(send_reply_error)
}

fn send_reply_with<T>(
    sender: oneshot::Sender<Result<T>>,
    this: &mut Actor,
    f: impl FnOnce(&mut Actor) -> Result<T>,
) -> Result<(), SendReplyError> {
    sender.send(f(this)).map_err(send_reply_error)
}

async fn send_reply_with_async<T>(
    sender: oneshot::Sender<Result<T>>,
    this: &mut Actor,
    f: impl AsyncFnOnce(&mut Actor) -> Result<T>,
) -> Result<(), SendReplyError> {
    sender.send(f(this).await).map_err(send_reply_error)
}

fn send_reply_error<T>(_err: T) -> SendReplyError {
    SendReplyError
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store;
    #[tokio::test]
    async fn open_close() -> anyhow::Result<()> {
        let store = store::Store::memory();
        let sync = SyncHandle::spawn(store, None, "foo".into());
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        sync.import_namespace(namespace.into()).await?;
        sync.open(id, Default::default()).await?;
        let (tx, rx) = async_channel::bounded(10);
        sync.subscribe(id, tx).await?;
        sync.close(id).await?;
        assert!(rx.recv().await.is_err());
        Ok(())
    }

    /// A validation failure from `insert_remote` is reported with the reason and, for
    /// future-stamped entries, a retry time.
    #[tokio::test]
    async fn insert_remote_reports_validation_failure() -> anyhow::Result<()> {
        let store = store::Store::memory();
        let sync = SyncHandle::spawn_with_options(
            store,
            None,
            "foo".into(),
            None,
            60 * 1_000_000,
            0,
            None,
            None,
        );
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let author = Author::new(&mut rand::rng());
        // Build the entry before the namespace secret moves into the store.
        let entry = SignedEntry::from_parts(
            &namespace,
            &author,
            b"k",
            crate::Record::from_data(b"v", system_time_now() + 120 * 1_000_000),
        );
        let id = namespace.id();
        sync.import_namespace(namespace.into()).await?;
        sync.open(id, Default::default()).await?;
        sync.set_sync(id, true).await?;

        match sync
            .insert_remote(id, entry, [0u8; 32], ContentStatus::Missing)
            .await
        {
            Err(InsertRemoteError::Validation {
                failure: ValidationFailure::TooFarInTheFuture,
                retry_after,
            }) => {
                assert!(retry_after.is_some());
            }
            res => panic!("expected a validation failure, got {res:?}"),
        }
        Ok(())
    }

    /// A clock with a tighter `observe` bound than the default future
    /// shift computes the reject's retry delay from its own bound: an
    /// entry two seconds ahead of a clock that accepts one second gets
    /// `retry_after` of about a second, so the deferred re-sync fires when
    /// the entry is acceptable. Computing the delay from the default
    /// ten-minute shift instead makes it zero (the entry is already within
    /// that bound), and the deferred re-sync fires at once, is rejected
    /// again, and loops as fast as round trips complete.
    #[tokio::test]
    async fn insert_remote_clock_reject_retries_on_the_clocks_bound() -> anyhow::Result<()> {
        /// Accepts remote entries at most one second ahead of system time.
        #[derive(Debug)]
        struct TightClock;
        impl crate::sync::Clock for TightClock {
            fn now(&self) -> Result<u64, crate::sync::ClockUnavailable> {
                Ok(system_time_now())
            }
            fn observe(&self, remote: u64) -> bool {
                remote <= system_time_now().saturating_add(1_000_000)
            }
            fn max_future_shift(&self) -> u64 {
                1_000_000
            }
        }

        let store = store::Store::memory();
        let sync = SyncHandle::spawn_with_options(
            store,
            None,
            "foo".into(),
            Some(std::sync::Arc::new(TightClock)),
            60 * 1_000_000,
            0,
            None,
            None,
        );
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let author = Author::new(&mut rand::rng());
        let entry = SignedEntry::from_parts(
            &namespace,
            &author,
            b"k",
            crate::Record::from_data(b"v", system_time_now() + 2 * 1_000_000),
        );
        let id = namespace.id();
        sync.import_namespace(namespace.into()).await?;
        sync.open(id, Default::default()).await?;
        sync.set_sync(id, true).await?;

        match sync
            .insert_remote(id, entry, [0u8; 32], ContentStatus::Missing)
            .await
        {
            Err(InsertRemoteError::Validation {
                failure: ValidationFailure::TooFarInTheFuture,
                retry_after,
            }) => {
                let retry_after = retry_after.expect("a retry delay for a future-stamped entry");
                assert!(
                    retry_after <= n0_future::time::Duration::from_secs(1) + n0_future::time::Duration::from_millis(100),
                    "retry delay must be computed from the clock's own 1 s bound, got {retry_after:?}"
                );
            }
            res => panic!("expected a validation failure, got {res:?}"),
        }
        Ok(())
    }

    /// Tests that streamer tasks spawned into `Actor.tasks` are reaped
    /// once they complete.
    ///
    /// The three streaming actions (`ListAuthors`, `ListReplicas`, and
    /// `ReplicaAction::GetMany`) each `spawn_local` a task into
    /// `Actor.tasks` to drive their reply channel. The actor must
    /// `join_next` those tasks once they finish, otherwise the
    /// `JoinSet` grows without bound for the lifetime of the actor.
    #[tokio::test]
    async fn actor_tasks_joinset_drain() -> anyhow::Result<()> {
        let store = store::Store::memory();
        let sync = SyncHandle::spawn(store, None, "drain".into());

        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        sync.import_namespace(namespace.into()).await?;
        sync.open(id, Default::default()).await?;

        const ITERATIONS: usize = 1000;

        for _ in 0..ITERATIONS {
            let (tx, mut rx) = mpsc::channel(64);
            sync.list_authors(tx).await?;
            while rx.recv().await?.is_some() {}
        }

        for _ in 0..ITERATIONS {
            let (tx, mut rx) = mpsc::channel(64);
            sync.list_replicas(tx).await?;
            while rx.recv().await?.is_some() {}
        }

        for _ in 0..ITERATIONS {
            let (tx, mut rx) = mpsc::channel(64);
            sync.get_many(id, store::Query::all().into(), tx).await?;
            while rx.recv().await?.is_some() {}
        }

        let mut last = sync.debug_tasks_len().await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while last > 16 && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
            last = sync.debug_tasks_len().await?;
        }

        assert!(
            last <= 16,
            "residual Actor.tasks JoinSet len = {last}, expected <= 16 \
             (was the join_next arm in run_async lost? streamer tasks \
             for ListAuthors / ListReplicas / GetMany are not being reaped)"
        );

        sync.close(id).await?;
        Ok(())
    }
}

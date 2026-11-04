//! On disk storage for replicas.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashSet},
    iter::{Chain, Flatten},
    num::NonZeroU64,
    ops::Bound,
};

use anyhow::{anyhow, Context, Result};
use iroh::{KeyParsingError, PublicKey};
use iroh_blobs::Hash;
use n0_future::time::SystemTime;
use rand::CryptoRng;
use redb::{Database, ReadableDatabase, ReadableMultimapTable, ReadableTable};
#[cfg(all(feature = "fs-store", feature = "redb-v2-migration"))]
use tracing::info;
use tracing::warn;

use super::{
    pubkeys::MemPublicKeyStore, DownloadPolicy, ImportNamespaceOutcome, OpenError, PublicKeyStore,
    Query,
};
use crate::{
    actor::MAX_COMMIT_DELAY,
    keys::Author,
    ranger::{Fingerprint, Range, RangeEntry},
    sync::{Entry, EntrySignature, Record, RecordIdentifier, Replica, SignedEntry},
    AuthorHeads, AuthorId, Capability, CapabilityKind, NamespaceId, NamespaceSecret, PeerIdBytes,
    ReplicaInfo,
};

mod bounds;
#[cfg(all(feature = "fs-store", feature = "redb-v2-migration"))]
mod migrate_redb_v2_tuples;
mod migrations;
mod query;
mod ranges;
pub(crate) mod tables;

pub use self::ranges::RecordsRange;
use self::{
    bounds::{ByKeyBounds, RecordsBounds},
    query::QueryIterator,
    ranges::RangeExt,
    tables::{
        LatestPerAuthorKey, LatestPerAuthorValue, ReadOnlyTables, RecordsId, RecordsTable,
        RecordsValue, Tables, TransactionAndTables,
    },
};

/// Manages the replicas and authors for an instance.
#[derive(Debug)]
pub struct Store {
    db: Database,
    transaction: CurrentTransaction,
    open_replicas: HashSet<NamespaceId>,
    pubkeys: MemPublicKeyStore,
}

impl Drop for Store {
    fn drop(&mut self) {
        if let Err(err) = self.flush() {
            warn!("failed to trigger final flush: {:?}", err);
        }
    }
}

impl AsRef<Store> for Store {
    fn as_ref(&self) -> &Store {
        self
    }
}

impl AsMut<Store> for Store {
    fn as_mut(&mut self) -> &mut Store {
        self
    }
}

#[derive(derive_more::Debug, Default)]
#[allow(clippy::large_enum_variant)]
enum CurrentTransaction {
    #[default]
    None,
    Read(ReadOnlyTables),
    Write(TransactionAndTables),
}

#[cfg(feature = "fs-store")]
fn open_database(path: &std::path::Path) -> Result<Database> {
    match Database::create(path) {
        Ok(db) => Ok(db),
        Err(redb::DatabaseError::UpgradeRequired(v)) => Err(anyhow!(
            "Opening the database failed: Upgrading from redb {v} no longer supported. Use an older redb version first."
        )),
        Err(err) => Err(err.into()),
    }
}

#[cfg(feature = "fs-store")]
fn is_redb_v2_tuple_mismatch(err: &anyhow::Error) -> bool {
    err.chain().any(|e| {
        matches!(
            e.downcast_ref::<redb::TableError>(),
            Some(redb::TableError::TableTypeMismatch { .. })
        )
    })
}

impl Store {
    /// Create a new store in memory.
    pub fn memory() -> Self {
        Self::memory_impl().expect("failed to create memory store")
    }

    fn memory_impl() -> Result<Self> {
        let db = Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;
        Self::new_impl(db)
    }

    /// Create or open a store from a `path` to a database file.
    ///
    /// The file will be created if it does not exist, otherwise it will be opened.
    #[cfg(feature = "fs-store")]
    pub fn persistent(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let db = open_database(path)?;
        match Self::new_impl(db) {
            Ok(store) => Ok(store),
            Err(err) if is_redb_v2_tuple_mismatch(&err) => {
                #[cfg(feature = "redb-v2-migration")]
                {
                    info!("redb 2.x tuple format detected, running migration");
                    migrate_redb_v2_tuples::run(path)?;
                    Self::new_impl(open_database(path)?)
                }
                #[cfg(not(feature = "redb-v2-migration"))]
                {
                    let _ = err;
                    Err(anyhow!(
                        "Opening the database failed: this store was written by iroh-docs 0.94..=0.98 (redb 2.x) and needs migration. Enable the `redb-v2-migration` feature on iroh-docs and re-open to migrate."
                    ))
                }
            }
            Err(err) => Err(err),
        }
    }

    fn new_impl(db: redb::Database) -> Result<Self> {
        // Setup all tables
        let write_tx = db.begin_write()?;
        let _ = Tables::new(&write_tx)?;
        write_tx.commit()?;

        // Run database migrations
        migrations::run_migrations(&db)?;

        Ok(Store {
            db,
            transaction: Default::default(),
            open_replicas: Default::default(),
            pubkeys: Default::default(),
        })
    }

    /// Flush the current transaction, if any.
    ///
    /// This is the cheapest way to ensure that the data is persisted.
    pub fn flush(&mut self) -> Result<()> {
        if let CurrentTransaction::Write(w) = std::mem::take(&mut self.transaction) {
            w.commit()?;
        }
        Ok(())
    }

    /// Get a read-only snapshot of the database.
    ///
    /// This has the side effect of committing any open write transaction,
    /// so it can be used as a way to ensure that the data is persisted.
    pub fn snapshot(&mut self) -> Result<&ReadOnlyTables> {
        let guard = &mut self.transaction;
        let tables = match std::mem::take(guard) {
            CurrentTransaction::None => {
                let tx = self.db.begin_read()?;
                ReadOnlyTables::new(tx)?
            }
            CurrentTransaction::Write(w) => {
                w.commit()?;
                let tx = self.db.begin_read()?;
                ReadOnlyTables::new(tx)?
            }
            CurrentTransaction::Read(tables) => tables,
        };
        *guard = CurrentTransaction::Read(tables);
        match &*guard {
            CurrentTransaction::Read(ref tables) => Ok(tables),
            _ => unreachable!(),
        }
    }

    /// Get an owned read-only snapshot of the database.
    ///
    /// This will open a new read transaction. The read transaction won't be reused for other
    /// reads.
    ///
    /// This has the side effect of committing any open write transaction,
    /// so it can be used as a way to ensure that the data is persisted.
    pub fn snapshot_owned(&mut self) -> Result<ReadOnlyTables> {
        // make sure the current transaction is committed
        self.flush()?;
        assert!(matches!(self.transaction, CurrentTransaction::None));
        let tx = self.db.begin_read()?;
        let tables = ReadOnlyTables::new(tx)?;
        Ok(tables)
    }

    /// Get access to the tables to read from them.
    ///
    /// The underlying transaction is a write transaction, but with a non-mut
    /// reference to the tables you can not write.
    ///
    /// There is no guarantee that this will be an independent transaction.
    /// You just get readonly access to the current state of the database.
    ///
    /// As such, there is also no guarantee that the data you see is
    /// already persisted.
    fn tables(&mut self) -> Result<&Tables<'_>> {
        let guard = &mut self.transaction;
        let tables = match std::mem::take(guard) {
            CurrentTransaction::None => {
                let tx = self.db.begin_write()?;
                TransactionAndTables::new(tx)?
            }
            CurrentTransaction::Write(w) => {
                if w.since.elapsed() > MAX_COMMIT_DELAY {
                    tracing::debug!("committing transaction because it's too old");
                    w.commit()?;
                    let tx = self.db.begin_write()?;
                    TransactionAndTables::new(tx)?
                } else {
                    w
                }
            }
            CurrentTransaction::Read(_) => {
                let tx = self.db.begin_write()?;
                TransactionAndTables::new(tx)?
            }
        };
        *guard = CurrentTransaction::Write(tables);
        match guard {
            CurrentTransaction::Write(ref mut tables) => Ok(tables.tables()),
            _ => unreachable!(),
        }
    }

    /// Get exclusive write access to the tables in the current transaction.
    ///
    /// There is no guarantee that this will be an independent transaction.
    /// As such, there is also no guarantee that the data you see or write
    /// will be persisted.
    ///
    /// To ensure that the data is persisted, acquire a snapshot of the database
    /// or call flush.
    fn modify<T>(&mut self, f: impl FnOnce(&mut Tables) -> Result<T>) -> Result<T> {
        let guard = &mut self.transaction;
        let tables = match std::mem::take(guard) {
            CurrentTransaction::None => {
                let tx = self.db.begin_write()?;
                TransactionAndTables::new(tx)?
            }
            CurrentTransaction::Write(w) => {
                if w.since.elapsed() > MAX_COMMIT_DELAY {
                    tracing::debug!("committing transaction because it's too old");
                    w.commit()?;
                    let tx = self.db.begin_write()?;
                    TransactionAndTables::new(tx)?
                } else {
                    w
                }
            }
            CurrentTransaction::Read(_) => {
                let tx = self.db.begin_write()?;
                TransactionAndTables::new(tx)?
            }
        };
        *guard = CurrentTransaction::Write(tables);
        let res = match &mut *guard {
            CurrentTransaction::Write(ref mut tables) => tables.with_tables_mut(f)?,
            _ => unreachable!(),
        };
        Ok(res)
    }
}

type PeersIter = std::vec::IntoIter<PeerIdBytes>;

impl Store {
    /// Create a new replica for `namespace` and persist in this store.
    pub fn new_replica(&mut self, namespace: NamespaceSecret) -> Result<Replica<'_>> {
        let id = namespace.id();
        self.import_namespace(namespace.into())?;
        self.open_replica(&id).map_err(Into::into)
    }

    /// Create a new author key and persist it in the store.
    pub fn new_author<R: CryptoRng>(&mut self, rng: &mut R) -> Result<Author> {
        let author = Author::new(rng);
        self.import_author(author.clone())?;
        Ok(author)
    }

    /// Check if a [`AuthorHeads`] contains entry timestamps that we do not have locally.
    ///
    /// Returns the number of authors that the other peer has updates for.
    pub fn has_news_for_us(
        &mut self,
        namespace: NamespaceId,
        heads: &AuthorHeads,
    ) -> Result<Option<NonZeroU64>> {
        // Heads of authors that may not write here are no news: their entries would be
        // rejected, so they must not trigger a sync.
        let filtered;
        let heads = match self.get_allowed_writers(&namespace)? {
            None => heads,
            Some(allowed) => {
                filtered = heads
                    .iter()
                    .filter(|(author, _)| allowed.contains(*author))
                    .map(|(author, timestamp)| (*author, *timestamp))
                    .collect::<AuthorHeads>();
                &filtered
            }
        };
        let our_heads = {
            let latest = self.get_latest_for_each_author(namespace)?;
            let mut heads = AuthorHeads::default();
            for e in latest {
                let (author, timestamp, _key) = e?;
                heads.insert(author, timestamp);
            }
            heads
        };
        let has_news_for_us = heads.has_news_for(&our_heads);
        Ok(has_news_for_us)
    }

    /// Open a replica from this store.
    ///
    /// This just calls load_replica_info and then creates a new replica with the info.
    pub fn open_replica(&mut self, namespace_id: &NamespaceId) -> Result<Replica<'_>, OpenError> {
        let info = self.load_replica_info(namespace_id)?;
        let instance = StoreInstance::new(*namespace_id, self);
        Ok(Replica::new(instance, Box::new(info)))
    }

    /// Load the replica info from the store.
    ///
    /// This is the single chokepoint every open path goes through (the docs
    /// actor's `open` callback and [`Store::open_replica`]), so it also
    /// rebuilds the namespace fingerprint once per open — see
    /// [`StoreInstance::rebuild_namespace_fingerprint`]. A rebuild failure
    /// fails the open: serving a possibly-stale aggregate would be worse.
    pub fn load_replica_info(
        &mut self,
        namespace_id: &NamespaceId,
    ) -> Result<ReplicaInfo, OpenError> {
        let tables = self.tables()?;
        let info = match tables.namespaces.get(namespace_id.as_bytes()) {
            Ok(Some(db_value)) => {
                let (raw_kind, raw_bytes) = db_value.value();
                let namespace = Capability::from_raw(raw_kind, raw_bytes)?;
                ReplicaInfo::new(namespace)
            }
            Ok(None) => return Err(OpenError::NotFound),
            Err(err) => return Err(OpenError::Other(err.into())),
        };
        self.open_replicas.insert(info.capability.id());
        StoreInstance::new(*namespace_id, self)
            .rebuild_namespace_fingerprint()
            .map(|_| ())
            .map_err(OpenError::Other)?;
        Ok(info)
    }

    /// Close a replica.
    pub fn close_replica(&mut self, id: NamespaceId) {
        self.open_replicas.remove(&id);
    }

    /// List all replica namespaces in this store.
    pub fn list_namespaces(
        &mut self,
    ) -> Result<impl Iterator<Item = Result<(NamespaceId, CapabilityKind)>>> {
        let snapshot = self.snapshot()?;
        let iter = snapshot.namespaces.range::<&'static [u8; 32]>(..)?;
        let iter = iter.map(|res| {
            let capability = parse_capability(res?.1.value())?;
            Ok((capability.id(), capability.kind()))
        });
        Ok(iter)
    }

    /// Get an author key from the store.
    pub fn get_author(&mut self, author_id: &AuthorId) -> Result<Option<Author>> {
        let tables = self.tables()?;
        let Some(author) = tables.authors.get(author_id.as_bytes())? else {
            return Ok(None);
        };
        let author = Author::from_bytes(author.value());
        Ok(Some(author))
    }

    /// Import an author key pair.
    pub fn import_author(&mut self, author: Author) -> Result<()> {
        self.modify(|tables| {
            tables
                .authors
                .insert(author.id().as_bytes(), &author.to_bytes())?;
            Ok(())
        })
    }

    /// Delete an author.
    pub fn delete_author(&mut self, author: AuthorId) -> Result<()> {
        self.modify(|tables| {
            tables.authors.remove(author.as_bytes())?;
            Ok(())
        })
    }

    /// List all author keys in this store.
    pub fn list_authors(&mut self) -> Result<impl Iterator<Item = Result<Author>>> {
        let tables = self.snapshot()?;
        let iter = tables
            .authors
            .range::<&'static [u8; 32]>(..)?
            .map(|res| match res {
                Ok((_key, value)) => Ok(Author::from_bytes(value.value())),
                Err(err) => Err(err.into()),
            });
        Ok(iter)
    }

    /// Import a new replica namespace.
    pub fn import_namespace(&mut self, capability: Capability) -> Result<ImportNamespaceOutcome> {
        self.modify(|tables| {
            let outcome = {
                let (capability, outcome) = {
                    let existing = tables.namespaces.get(capability.id().as_bytes())?;
                    if let Some(existing) = existing {
                        let mut existing = parse_capability(existing.value())?;
                        let outcome = if existing.merge(capability)? {
                            ImportNamespaceOutcome::Upgraded
                        } else {
                            ImportNamespaceOutcome::NoChange
                        };
                        (existing, outcome)
                    } else {
                        (capability, ImportNamespaceOutcome::Inserted)
                    }
                };
                let id = capability.id().to_bytes();
                let (kind, bytes) = capability.raw();
                tables.namespaces.insert(&id, (kind, &bytes))?;
                outcome
            };
            Ok(outcome)
        })
    }

    /// Retire this node's **write authority** over a namespace, keeping every
    /// entry and blob reference.
    ///
    /// This is a *local* operation and nothing more. It does not revoke
    /// anything cryptographically and does not reach any other node: a peer
    /// holding the namespace secret still holds it. What it retires is this
    /// store's own ability to sign new entries.
    ///
    /// The non-destructive counterpart to [`Self::remove_replica`]. A caller
    /// narrowing its own access needs the records to survive — it must keep
    /// serving reads of exactly the content it could previously write.
    /// `remove_replica` deletes the records too, and re-importing a read
    /// capability does nothing, because [`Capability::merge`] only ever
    /// upgrades. Neither can express this.
    ///
    /// **Requires the replica to be closed.** The caller closes its handles
    /// first; this refuses otherwise, for the same reason `remove_replica`
    /// does — an open replica holds a `ReplicaInfo` carrying the old
    /// capability, so proceeding would leave a live handle able to write
    /// against a namespace the store now records as read-only. It does *not*
    /// close anything on the caller's behalf, and it makes no claim about
    /// invalidating handles the caller still holds.
    ///
    /// Commits synchronously before returning: reporting success on an
    /// uncommitted write would let a crash undo a transition the caller has
    /// already acted on.
    ///
    /// Returns the capability kind that is now **stored**, so a caller verifies
    /// the outcome rather than assuming its request was honoured.
    ///
    /// Idempotent. A later import of a valid write capability upgrades again
    /// through the ordinary merge path — this forgets the secret, it does not
    /// blacklist the namespace.
    pub fn retire_write_authority(&mut self, namespace: &NamespaceId) -> Result<CapabilityKind> {
        if self.open_replicas.contains(namespace) {
            return Err(anyhow!("replica is not closed"));
        }
        let kind = self.modify(|tables| {
            // Parsed rather than overwritten blind: a namespace whose stored
            // capability cannot be read is not one to replace with a guess.
            // Read out and dropped before the insert, so the read borrow does
            // not overlap the write.
            let current = {
                let existing = tables
                    .namespaces
                    .get(namespace.as_bytes())?
                    .context("namespace not found")?;
                let (raw_kind, raw_bytes) = existing.value();
                Capability::from_raw(raw_kind, raw_bytes)?
            };
            let read = Capability::Read(current.id());
            let (kind, bytes) = read.raw();
            tables
                .namespaces
                .insert(namespace.as_bytes(), (kind, &bytes))?;
            Ok(CapabilityKind::Read)
        })?;
        // `modify` explicitly does not guarantee persistence — it may share an
        // open transaction that has not been committed. Reporting success
        // before the commit would let a crash roll back a capability change the
        // caller has already treated as done.
        self.flush()?;
        Ok(kind)
    }

    /// The capability kind currently **stored** for a namespace.
    ///
    /// Distinct from whatever a caller believes it imported: after a downgrade,
    /// this is what actually governs.
    pub fn namespace_capability_kind(&mut self, namespace: &NamespaceId) -> Result<CapabilityKind> {
        let tables = self.tables()?;
        let value = tables
            .namespaces
            .get(namespace.as_bytes())?
            .context("namespace not found")?;
        let (raw_kind, raw_bytes) = value.value();
        Ok(Capability::from_raw(raw_kind, raw_bytes)?.kind())
    }

    /// Remove a replica.
    ///
    /// Completely removes a replica and deletes both the namespace private key and all document
    /// entries.
    ///
    /// Note that a replica has to be closed before it can be removed. The store has to enforce
    /// that a replica cannot be removed while it is still open.
    pub fn remove_replica(&mut self, namespace: &NamespaceId) -> Result<()> {
        if self.open_replicas.contains(namespace) {
            return Err(anyhow!("replica is not closed"));
        }
        self.modify(|tables| {
            let bounds = RecordsBounds::namespace(*namespace);
            tables.records.retain_in(bounds.as_ref(), |_k, _v| false)?;
            let bounds = ByKeyBounds::namespace(*namespace);
            let _ = tables
                .records_by_key
                .retain_in(bounds.as_ref(), |_k, _v| false);
            let ns = namespace.as_bytes();
            tables.namespaces.remove(ns)?;
            tables.namespace_peers.remove_all(ns)?;
            tables.download_policy.remove(ns)?;
            tables.writer_allowlist.remove(ns)?;
            tables.namespace_fingerprint.remove(ns)?;
            tables.namespace_sequence.remove(ns)?;
            let _ = tables
                .records_by_seq
                .retain_in((ns, 0u64)..=(ns, u64::MAX), |_k, _v| false);
            // The key table's range is (ns, key, author); no contiguous
            // range covers a whole namespace, so remove per row.
            let bounds = RecordsBounds::namespace(*namespace);
            for row in tables.records.range(bounds.as_ref())? {
                let (compound_key, _value) = row?;
                let (_, author, key) = compound_key.value();
                tables.records_by_seq_key.remove((ns, key, author))?;
            }
            Ok(())
        })
    }

    /// Get an iterator over entries of a replica.
    pub fn get_many(
        &mut self,
        namespace: NamespaceId,
        query: impl Into<Query>,
    ) -> Result<QueryIterator> {
        let tables = self.snapshot_owned()?;
        QueryIterator::new(tables, namespace, query.into())
    }

    /// Get an entry by key and author.
    pub fn get_exact(
        &mut self,
        namespace: NamespaceId,
        author: AuthorId,
        key: impl AsRef<[u8]>,
        include_empty: bool,
    ) -> Result<Option<SignedEntry>> {
        get_exact(
            &self.tables()?.records,
            namespace,
            author,
            key,
            include_empty,
        )
    }

    /// Get all content hashes of all replicas in the store.
    pub fn content_hashes(&mut self) -> Result<ContentHashesIterator> {
        let tables = self.snapshot_owned()?;
        ContentHashesIterator::all(&tables.records)
    }

    /// Get the latest entry for each author in a namespace.
    pub fn get_latest_for_each_author(
        &mut self,
        namespace: NamespaceId,
    ) -> Result<LatestIterator<'_>> {
        LatestIterator::new(&self.tables()?.latest_per_author, namespace)
    }

    /// Register a peer that has been useful to sync a document.
    pub fn register_useful_peer(
        &mut self,
        namespace: NamespaceId,
        peer: crate::PeerIdBytes,
    ) -> Result<()> {
        let peer = &peer;
        let namespace = namespace.as_bytes();
        // calculate nanos since UNIX_EPOCH for a time measurement
        let nanos = SystemTime::UNIX_EPOCH
            .elapsed()
            .map(|duration| duration.as_nanos() as u64)?;
        self.modify(|tables| {
            // ensure the document exists
            anyhow::ensure!(
                tables.namespaces.get(namespace)?.is_some(),
                "document not created"
            );

            let mut namespace_peers = tables.namespace_peers.get(namespace)?;

            // get the oldest entry since it's candidate for removal
            let maybe_oldest = namespace_peers.next().transpose()?.map(|guard| {
                let (oldest_nanos, &oldest_peer) = guard.value();
                (oldest_nanos, oldest_peer)
            });
            match maybe_oldest {
                None => {
                    // the table is empty so the peer can be inserted without further checks since
                    // super::PEERS_PER_DOC_CACHE_SIZE is non zero
                    drop(namespace_peers);
                    tables.namespace_peers.insert(namespace, (nanos, peer))?;
                }
                Some((oldest_nanos, oldest_peer)) => {
                    let oldest_peer = &oldest_peer;

                    if oldest_peer == peer {
                        // oldest peer is the current one, so replacing the entry for the peer will
                        // maintain the size
                        drop(namespace_peers);
                        tables
                            .namespace_peers
                            .remove(namespace, (oldest_nanos, oldest_peer))?;
                        tables.namespace_peers.insert(namespace, (nanos, peer))?;
                    } else {
                        // calculate the len in the same loop since calling `len` is another fallible operation
                        let mut len = 1;
                        // find any previous entry for the same peer to remove it
                        let mut prev_peer_nanos = None;

                        for result in namespace_peers {
                            len += 1;
                            let guard = result?;
                            let (peer_nanos, peer_bytes) = guard.value();
                            if prev_peer_nanos.is_none() && peer_bytes == peer {
                                prev_peer_nanos = Some(peer_nanos)
                            }
                        }

                        match prev_peer_nanos {
                            Some(prev_nanos) => {
                                // the peer was already present, so we can remove the old entry and
                                // insert the new one without checking the size
                                tables
                                    .namespace_peers
                                    .remove(namespace, (prev_nanos, peer))?;
                                tables.namespace_peers.insert(namespace, (nanos, peer))?;
                            }
                            None => {
                                // the peer is new and the table is non empty, add it and check the
                                // size to decide if the oldest peer should be evicted
                                tables.namespace_peers.insert(namespace, (nanos, peer))?;
                                len += 1;
                                if len > super::PEERS_PER_DOC_CACHE_SIZE.get() {
                                    tables
                                        .namespace_peers
                                        .remove(namespace, (oldest_nanos, oldest_peer))?;
                                }
                            }
                        }
                    }
                }
            }
            Ok(())
        })
    }

    /// Get the peers that have been useful for a document.
    pub fn get_sync_peers(&mut self, namespace: &NamespaceId) -> Result<Option<PeersIter>> {
        let tables = self.tables()?;
        let mut peers = Vec::with_capacity(super::PEERS_PER_DOC_CACHE_SIZE.get());
        for result in tables.namespace_peers.get(namespace.as_bytes())?.rev() {
            let (_nanos, &peer) = result?.value();
            peers.push(peer);
        }
        if peers.is_empty() {
            Ok(None)
        } else {
            Ok(Some(peers.into_iter()))
        }
    }

    /// Set the download policy for a namespace.
    pub fn set_download_policy(
        &mut self,
        namespace: &NamespaceId,
        policy: DownloadPolicy,
    ) -> Result<()> {
        self.modify(|tables| {
            let namespace = namespace.as_bytes();

            // ensure the document exists
            anyhow::ensure!(
                tables.namespaces.get(&namespace)?.is_some(),
                "document not created"
            );

            let value = postcard::to_stdvec(&policy)?;
            tables.download_policy.insert(namespace, value.as_slice())?;
            Ok(())
        })
    }

    /// Get the download policy for a namespace.
    pub fn get_download_policy(&mut self, namespace: &NamespaceId) -> Result<DownloadPolicy> {
        let tables = self.tables()?;
        let value = tables.download_policy.get(namespace.as_bytes())?;
        Ok(match value {
            None => DownloadPolicy::default(),
            Some(value) => postcard::from_bytes(value.value())?,
        })
    }

    /// Set or clear the authors allowed to write to a namespace.
    ///
    /// With `Some(writers)`, entries from any other author are rejected, both on local insert and
    /// when received from a peer; an empty set rejects every entry. `None` removes the restriction,
    /// which is the default for a namespace. Entries stored before the call are kept.
    ///
    /// The setting is local to this store and is not replicated. It is removed together with the
    /// replica.
    pub fn set_allowed_writers(
        &mut self,
        namespace: &NamespaceId,
        writers: Option<BTreeSet<AuthorId>>,
    ) -> Result<()> {
        self.modify(|tables| {
            let namespace = namespace.as_bytes();
            anyhow::ensure!(
                tables.namespaces.get(&namespace)?.is_some(),
                "document not created"
            );
            match writers {
                Some(writers) => {
                    let value = postcard::to_stdvec(&writers)?;
                    tables
                        .writer_allowlist
                        .insert(namespace, value.as_slice())?;
                }
                None => {
                    tables.writer_allowlist.remove(namespace)?;
                }
            }
            Ok(())
        })
    }

    /// Get the authors allowed to write to a namespace, or `None` if any author may write.
    pub fn get_allowed_writers(
        &mut self,
        namespace: &NamespaceId,
    ) -> Result<Option<BTreeSet<AuthorId>>> {
        let tables = self.tables()?;
        let value = tables.writer_allowlist.get(namespace.as_bytes())?;
        Ok(match value {
            None => None,
            Some(value) => Some(postcard::from_bytes(value.value())?),
        })
    }
}

impl PublicKeyStore for Store {
    fn public_key(&self, id: &[u8; 32]) -> Result<PublicKey, KeyParsingError> {
        self.pubkeys.public_key(id)
    }
}

fn parse_capability((raw_kind, raw_bytes): (u8, &[u8; 32])) -> Result<Capability> {
    Capability::from_raw(raw_kind, raw_bytes)
}

fn get_exact(
    record_table: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    namespace: NamespaceId,
    author: AuthorId,
    key: impl AsRef<[u8]>,
    include_empty: bool,
) -> Result<Option<SignedEntry>> {
    let id = (namespace.as_bytes(), author.as_bytes(), key.as_ref());
    let record = record_table.get(id)?;
    Ok(record
        .map(|r| into_entry(id, r.value()))
        .filter(|entry| include_empty || !entry.is_empty()))
}

/// A wrapper around [`Store`] for a specific [`NamespaceId`]
#[derive(Debug)]
pub struct StoreInstance<'a> {
    namespace: NamespaceId,
    pub(crate) store: &'a mut Store,
}

impl<'a> StoreInstance<'a> {
    pub(crate) fn new(namespace: NamespaceId, store: &'a mut Store) -> Self {
        StoreInstance { namespace, store }
    }
}

impl StoreInstance<'_> {
    /// The authors allowed to write to this namespace, or `None` if any author may write.
    pub(crate) fn allowed_writers(&mut self) -> Result<Option<BTreeSet<AuthorId>>> {
        self.store.get_allowed_writers(&self.namespace)
    }

    /// The namespace fingerprint: the XOR aggregate over this namespace's
    /// record rows, maintained on insert (see [`crate::ranger::Fingerprint`]
    /// for the exact per-row bytes), readable in O(1).
    ///
    /// Two replicas that hold the same record rows produce the same
    /// fingerprint, independent of insertion order and of any consumer-side
    /// filtering (grants filter consumers, not the store). The first query
    /// on a replica written before the fingerprint table existed builds the
    /// aggregate once from the records table.
    pub fn namespace_fingerprint(&mut self) -> Result<Fingerprint> {
        let namespace = self.namespace.as_bytes();
        // Read through the open transaction rather than snapshotting: a
        // snapshot commits the batched writes, and this query must not turn
        // every read into a commit.
        let cached = {
            let tables = self.store.as_mut().tables()?;
            tables
                .namespace_fingerprint
                .get(namespace)?
                .map(|fp| Fingerprint(*fp.value()))
        };
        if let Some(fp) = cached {
            return Ok(fp);
        }
        // Build once, inside the write transaction, so the stored row and
        // the records it covers agree.
        self.store.as_mut().modify(|tables| {
            if let Some(fp) = tables.namespace_fingerprint.get(namespace)? {
                return Ok(Fingerprint(*fp.value()));
            }
            let mut fp = Fingerprint::empty();
            let bounds = RecordsBounds::namespace(self.namespace);
            let records = tables.records.range(bounds.as_ref())?;
            for row in records {
                let (compound_key, value) = row?;
                let (ns, author, key) = compound_key.value();
                let (timestamp, _sig_ns, _sig_author, _len, hash) = value.value();
                fp ^= crate::ranger::record_fingerprint(ns, author, key, timestamp, hash);
            }
            let bytes = fp.0;
            tables.namespace_fingerprint.insert(namespace, &bytes)?;
            Ok(fp)
        })
    }

    /// Rebuild the namespace fingerprint from the records table, overwriting
    /// the stored aggregate.
    ///
    /// Run once per replica open (see [`Store::load_replica_info`]): the
    /// sync protocol answers whole-set fingerprints from the aggregate
    /// maintained on writes, and a release build has no scan comparison, so
    /// an unconditional rebuild on open bounds any silent divergence between
    /// the aggregate and the rows to one process lifetime. One O(N) scan per
    /// open replaces the per-session scans the fast path removed.
    pub fn rebuild_namespace_fingerprint(&mut self) -> Result<Fingerprint> {
        let namespace = self.namespace.as_bytes();
        self.store.as_mut().modify(|tables| {
            let mut fp = Fingerprint::empty();
            let bounds = RecordsBounds::namespace(self.namespace);
            let records = tables.records.range(bounds.as_ref())?;
            for row in records {
                let (compound_key, value) = row?;
                let (ns, author, key) = compound_key.value();
                let (timestamp, _sig_ns, _sig_author, _len, hash) = value.value();
                fp ^= crate::ranger::record_fingerprint(ns, author, key, timestamp, hash);
            }
            let bytes = fp.0;
            tables.namespace_fingerprint.insert(namespace, &bytes)?;
            Ok(fp)
        })
    }

    /// Build the insertion-sequence index if it does not exist yet: assign
    /// the namespace's current record rows sequence numbers 1..=N in
    /// records-table order and store N. Until this runs, `entry_put` skips
    /// sequence maintenance, so a replica written before the sequence
    /// tables existed replays its pre-existing rows first.
    fn ensure_sequence_index(&mut self) -> Result<()> {
        let ns = self.namespace.as_bytes();
        // Through the open transaction: a snapshot here would commit the
        // batched writes before the build below runs in a fresh transaction.
        let built = {
            let tables = self.store.as_mut().tables()?;
            tables.namespace_sequence.get(ns)?.is_some()
        };
        if built {
            return Ok(());
        }
        self.store.as_mut().modify(|tables| {
            if tables.namespace_sequence.get(ns)?.is_some() {
                return Ok(());
            }
            let mut seq = 0u64;
            let bounds = RecordsBounds::namespace(self.namespace);
            let records = tables.records.range(bounds.as_ref())?;
            for row in records {
                let (compound_key, _value) = row?;
                let (ns_row, author, key) = compound_key.value();
                debug_assert_eq!(ns_row, ns);
                seq += 1;
                tables.records_by_seq.insert((ns, seq), (author, key))?;
                tables.records_by_seq_key.insert((ns, key, author), seq)?;
            }
            tables.namespace_sequence.insert(ns, seq)?;
            Ok(())
        })
    }

    /// The insertion sequence of this namespace: the sequence number of the
    /// last state-changing put, or 0 for an empty namespace. Strictly
    /// increases with each state-changing put and never bumps on a
    /// byte-identical re-insert.
    pub fn namespace_sequence(&mut self) -> Result<u64> {
        self.ensure_sequence_index()?;
        // Through the open transaction: no commit for a read.
        let tables = self.store.as_mut().tables()?;
        Ok(tables
            .namespace_sequence
            .get(self.namespace.as_bytes())?
            .map(|v| v.value())
            .unwrap_or(0))
    }

    /// The records touched by puts with sequence numbers greater than
    /// `seq`, as `(touch_sequence, record)` pairs ordered by touch
    /// sequence. A key touched several times appears once, with its latest
    /// touch and latest record, so a consumer tracking the last seen
    /// sequence converges on the current state. Tombstones replay as empty
    /// records; a key whose put was superseded by a later remove is
    /// naturally absent from the records table and is skipped.
    ///
    /// `limit` bounds the returned page: at most `limit` pairs, or the
    /// whole tail when `limit` is 0. A paging consumer passes the last
    /// received touch as the next `seq`.
    pub fn entries_since(&mut self, seq: u64, limit: usize) -> Result<Vec<(u64, SignedEntry)>> {
        use std::collections::BTreeMap;
        self.ensure_sequence_index()?;
        // Through the open transaction: no commit for a read.
        let tables = self.store.as_mut().tables()?;
        let ns = self.namespace.as_bytes();
        let start = (ns, seq.saturating_add(1));
        let end = (ns, u64::MAX);
        let index = tables.records_by_seq.range(start..=end)?;
        let mut latest: BTreeMap<(Vec<u8>, Vec<u8>), (u64, SignedEntry)> = BTreeMap::new();
        for row in index {
            let (compound_key, target) = row?;
            let (_, touch) = compound_key.value();
            let (author, key) = target.value();
            let id = (ns, author, key);
            if let Some(record) = tables.records.get(id)? {
                let entry = into_entry(id, record.value());
                latest.insert((author.to_vec(), key.to_vec()), (touch, entry));
            }
        }
        let mut out: Vec<_> = latest.into_values().collect();
        out.sort_by_key(|(touch, _)| *touch);
        if limit > 0 {
            out.truncate(limit);
        }
        Ok(out)
    }
}

impl PublicKeyStore for StoreInstance<'_> {
    fn public_key(&self, id: &[u8; 32]) -> std::result::Result<PublicKey, KeyParsingError> {
        self.store.public_key(id)
    }
}

impl super::DownloadPolicyStore for StoreInstance<'_> {
    fn get_download_policy(&mut self, namespace: &NamespaceId) -> Result<DownloadPolicy> {
        self.store.get_download_policy(namespace)
    }
}

impl<'a> crate::ranger::Store<SignedEntry> for StoreInstance<'a> {
    type Error = anyhow::Error;
    type RangeIterator<'x>
        = Chain<RecordsRange<'x>, Flatten<std::option::IntoIter<RecordsRange<'x>>>>
    where
        'a: 'x;
    type ParentIterator<'x>
        = ParentIterator
    where
        'a: 'x;

    /// Get a the first key (or the default if none is available).
    fn get_first(&mut self) -> Result<RecordIdentifier> {
        let tables = self.store.as_mut().tables()?;
        // TODO: verify this fetches all keys with this namespace
        let bounds = RecordsBounds::namespace(self.namespace);
        let mut records = tables.records.range(bounds.as_ref())?;

        let Some(record) = records.next() else {
            return Ok(RecordIdentifier::default());
        };
        let (compound_key, _value) = record?;
        let (namespace_id, author_id, key) = compound_key.value();
        let id = RecordIdentifier::new(namespace_id, author_id, key);
        Ok(id)
    }

    #[cfg(test)]
    fn get(&mut self, id: &RecordIdentifier) -> Result<Option<SignedEntry>> {
        self.store
            .as_mut()
            .get_exact(id.namespace(), id.author(), id.key(), true)
    }

    #[cfg(test)]
    fn len(&mut self) -> Result<usize> {
        let tables = self.store.as_mut().tables()?;
        let bounds = RecordsBounds::namespace(self.namespace);
        let records = tables.records.range(bounds.as_ref())?;
        Ok(records.count())
    }

    #[cfg(test)]
    fn is_empty(&mut self) -> Result<bool> {
        use redb::ReadableTableMetadata;
        let tables = self.store.as_mut().tables()?;
        Ok(tables.records.is_empty()?)
    }

    fn get_fingerprint(&mut self, range: &Range<RecordIdentifier>) -> Result<Fingerprint> {
        // The whole-set range is the aggregate maintained on every write, so
        // answer it without a scan. Every sync session opens with this range,
        // which makes the agreeing-replica case O(1) per side instead of one
        // row read, one hash and one XOR per record. Split ranges still scan:
        // that is the work needed to locate a difference.
        if range.is_all() {
            let fingerprint = self.namespace_fingerprint()?;
            #[cfg(debug_assertions)]
            {
                let mut scanned = Fingerprint::empty();
                for el in self.get_range(range.clone())? {
                    scanned ^= el?.as_fingerprint();
                }
                debug_assert_eq!(
                    fingerprint, scanned,
                    "maintained namespace fingerprint diverged from a record scan"
                );
            }
            return Ok(fingerprint);
        }

        let elements = self.get_range(range.clone())?;

        let mut fp = Fingerprint::empty();
        for el in elements {
            let el = el?;
            fp ^= el.as_fingerprint();
        }

        Ok(fp)
    }

    fn entry_put(&mut self, e: SignedEntry) -> Result<()> {
        let id = e.id();
        self.store.as_mut().modify(|tables| {
            let hash = e.content_hash(); // let binding is needed
            let value = (
                e.timestamp(),
                &e.signature().namespace().to_bytes(),
                &e.signature().author().to_bytes(),
                e.content_len(),
                hash.as_bytes(),
            );
            // Insert into record table
            let key = (
                &id.namespace().to_bytes(),
                &id.author().to_bytes(),
                id.key(),
            );
            // A byte-identical re-insert changes nothing: skip it entirely
            // so the fingerprint stays put and the insertion sequence does
            // not bump on a no-op. A different value at the same key
            // supersedes the old row: XOR it out of the maintained
            // fingerprint before the new one goes in (a `put` has already
            // removed it via `remove_prefix_filtered`, so this only fires
            // for direct overwrites).
            let superseded = {
                let old = tables.records.get(key)?;
                match old {
                    None => None,
                    Some(existing) => {
                        if existing.value() == value {
                            return Ok(());
                        }
                        let (timestamp, _sig_ns, _sig_author, _len, hash) = existing.value();
                        Some((timestamp, *hash))
                    }
                }
            };
            tables.records.insert(key, value)?;

            // insert into by key index table
            let key = (
                &id.namespace().to_bytes(),
                id.key(),
                &id.author().to_bytes(),
            );
            tables.records_by_key.insert(key, ())?;

            // insert into latest table
            let key = (&e.id().namespace().to_bytes(), &e.id().author().to_bytes());
            let value = (e.timestamp(), e.id().key());
            tables.latest_per_author.insert(key, value)?;

            // Maintain the namespace fingerprint: XOR the new entry in and
            // a superseded one out. A missing row means the aggregate has
            // not been built yet (a replica written before this table
            // existed) and the first query builds it from the records
            // table, so maintenance skips here rather than building a
            // partial aggregate.
            let stored = tables
                .namespace_fingerprint
                .get(id.namespace().as_bytes())?
                .map(|fp| *fp.value());
            if let Some(mut bytes) = stored {
                let mut delta = crate::ranger::record_fingerprint(
                    id.namespace().as_ref(),
                    id.author().as_ref(),
                    id.key(),
                    e.timestamp(),
                    hash.as_bytes(),
                );
                if let Some((old_timestamp, old_hash)) = superseded {
                    delta ^= crate::ranger::record_fingerprint(
                        id.namespace().as_ref(),
                        id.author().as_ref(),
                        id.key(),
                        old_timestamp,
                        &old_hash,
                    );
                }
                for (acc, add) in bytes.iter_mut().zip(delta.0) {
                    *acc ^= add;
                }
                tables
                    .namespace_fingerprint
                    .insert(id.namespace().as_bytes(), &bytes)?;
            }

            // Maintain the insertion sequence: bump it and record which key
            // this state-changing put touched. A missing sequence row means
            // the index has not been built yet; the first query builds it
            // from the records table, so maintenance skips here rather than
            // leaving a partial index.
            let ns = id.namespace().to_bytes();
            let current = tables.namespace_sequence.get(&ns)?.map(|v| v.value());
            if let Some(current) = current {
                let next = current + 1;
                let author = id.author();
                let seq_key = (&ns, id.key(), author.as_bytes());
                // Compact: a key keeps only its latest touch row. Rows from
                // before the key index existed have no entry here and are
                // deduped at read time by `entries_since`.
                if let Some(previous) = tables.records_by_seq_key.get(seq_key)? {
                    let previous = previous.value();
                    tables.records_by_seq.remove((&ns, previous))?;
                }
                tables
                    .records_by_seq
                    .insert((&ns, next), (&id.author().to_bytes(), id.key()))?;
                tables.records_by_seq_key.insert(seq_key, next)?;
                tables.namespace_sequence.insert(&ns, next)?;
            }
            Ok(())
        })
    }

    fn get_range(&mut self, range: Range<RecordIdentifier>) -> Result<Self::RangeIterator<'_>> {
        let tables = self.store.as_mut().tables()?;
        let iter = match range.x().cmp(range.y()) {
            // identity range: iter1 = all, iter2 = none
            Ordering::Equal => {
                // iterator for all entries in replica
                let bounds = RecordsBounds::namespace(self.namespace);
                let iter = RecordsRange::with_bounds(&tables.records, bounds)?;
                chain_none(iter)
            }
            // regular range: iter1 = x <= t < y, iter2 = none
            Ordering::Less => {
                // iterator for entries from range.x to range.y
                let start = Bound::Included(range.x().to_byte_tuple());
                let end = Bound::Excluded(range.y().to_byte_tuple());
                let bounds = RecordsBounds::untrusted(&self.namespace, start, end);
                let iter = RecordsRange::with_bounds(&tables.records, bounds)?;
                chain_none(iter)
            }
            // split range: iter1 = start <= t < y, iter2 = x <= t <= end
            Ordering::Greater => {
                // iterator for entries from start to range.y
                let end = Bound::Excluded(range.y().to_byte_tuple());
                let bounds = RecordsBounds::untrusted(&self.namespace, Bound::Unbounded, end);
                let iter = RecordsRange::with_bounds(&tables.records, bounds)?;

                // iterator for entries from range.x to end
                let start = Bound::Included(range.x().to_byte_tuple());
                let bounds = RecordsBounds::untrusted(&self.namespace, start, Bound::Unbounded);
                let iter2 = RecordsRange::with_bounds(&tables.records, bounds)?;

                iter.chain(Some(iter2).into_iter().flatten())
            }
        };
        Ok(iter)
    }

    #[cfg(test)]
    fn entry_remove(&mut self, id: &RecordIdentifier) -> Result<Option<SignedEntry>> {
        let namespace = id.namespace().to_bytes();
        self.store.as_mut().modify(|tables| {
            let entry = {
                let (namespace, author, key) = id.as_byte_tuple();
                let id = (namespace, key, author);
                tables.records_by_key.remove(id)?;
                let id = (namespace, author, key);
                let value = tables.records.remove(id)?;
                value.map(|value| into_entry(id, value.value()))
            };
            // Keep the maintained aggregate equal to a scan of the records
            // table: XOR the removed row out. A missing row means the
            // aggregate has not been built yet; the first query builds it.
            if let Some(entry) = entry.as_ref() {
                let stored = tables
                    .namespace_fingerprint
                    .get(&namespace)?
                    .map(|fp| *fp.value());
                if let Some(mut bytes) = stored {
                    let removed = entry.as_fingerprint();
                    for (acc, sub) in bytes.iter_mut().zip(removed.0) {
                        *acc ^= sub;
                    }
                    tables.namespace_fingerprint.insert(&namespace, &bytes)?;
                }
            }
            Ok(entry)
        })
    }

    #[cfg(test)]
    fn all(&mut self) -> Result<Self::RangeIterator<'_>> {
        let tables = self.store.as_mut().tables()?;
        let bounds = RecordsBounds::namespace(self.namespace);
        let iter = RecordsRange::with_bounds(&tables.records, bounds)?;
        Ok(chain_none(iter))
    }

    fn prefixes_of(
        &mut self,
        id: &RecordIdentifier,
    ) -> Result<Self::ParentIterator<'_>, Self::Error> {
        let tables = self.store.as_mut().tables()?;
        // Pinned to the session namespace, not `id.namespace()`, so that a caller-supplied
        // id cannot read another document even if entry validation is ever reordered.
        ParentIterator::new(tables, self.namespace, id.author(), id.key().to_vec())
    }

    #[cfg(test)]
    fn prefixed_by(&mut self, id: &RecordIdentifier) -> Result<Self::RangeIterator<'_>> {
        let tables = self.store.as_mut().tables()?;
        let bounds = RecordsBounds::author_prefix(self.namespace, id.author(), id.key_bytes());
        let iter = RecordsRange::with_bounds(&tables.records, bounds)?;
        Ok(chain_none(iter))
    }

    fn remove_prefix_filtered(
        &mut self,
        id: &RecordIdentifier,
        predicate: impl Fn(&Record) -> bool,
    ) -> Result<usize> {
        // Pinned to the session namespace, not `id.namespace()`: this deletes records, so a
        // caller-supplied id reaching another document would be worse than a read.
        let bounds = RecordsBounds::author_prefix(self.namespace, id.author(), id.key_bytes());
        let namespace = self.namespace.to_bytes();
        self.store.as_mut().modify(|tables| {
            let cb = |_k: RecordsId, v: RecordsValue| {
                let (timestamp, _namespace_sig, _author_sig, len, hash) = v;
                let record = Record::new(hash.into(), len, timestamp);

                predicate(&record)
            };
            let iter = tables.records.extract_from_if(bounds.as_ref(), cb)?;
            let mut count = 0usize;
            // XOR the removed entries out of the maintained namespace
            // fingerprint, so the aggregate stays equal to a scan of the
            // records table. A missing row means the aggregate has not been
            // built yet; the first query builds it, so skip here.
            //
            // The accumulator starts from all-zero bytes, not
            // `Fingerprint::empty()`: the stored aggregate carries the
            // ranger's empty-set base (`blake3("")`), and removing entries
            // must cancel exactly the rows that were XORed in, base
            // untouched.
            let mut removed_fp = Fingerprint([0u8; 32]);
            for removed in iter {
                let (compound_key, value) = removed?;
                let (ns, author, key) = compound_key.value();
                let (timestamp, _sig_ns, _sig_author, _len, hash) = value.value();
                removed_fp ^= crate::ranger::record_fingerprint(ns, author, key, timestamp, hash);
                count += 1;
            }
            if count > 0 {
                let stored = tables
                    .namespace_fingerprint
                    .get(&namespace)?
                    .map(|fp| *fp.value());
                if let Some(mut bytes) = stored {
                    for (acc, sub) in bytes.iter_mut().zip(removed_fp.0) {
                        *acc ^= sub;
                    }
                    tables.namespace_fingerprint.insert(&namespace, &bytes)?;
                }
            }
            Ok(count)
        })
    }
}

fn chain_none<'a, I: Iterator<Item = T> + 'a, T>(
    iter: I,
) -> Chain<I, Flatten<std::option::IntoIter<I>>> {
    iter.chain(None.into_iter().flatten())
}

/// Iterator over parent entries, i.e. entries with the same namespace and author, and a key which
/// is a prefix of the key passed to the iterator.
#[derive(Debug)]
pub struct ParentIterator {
    inner: std::vec::IntoIter<anyhow::Result<SignedEntry>>,
}

impl ParentIterator {
    fn new(
        tables: &Tables,
        namespace: NamespaceId,
        author: AuthorId,
        key: Vec<u8>,
    ) -> anyhow::Result<Self> {
        let parents = parents(&tables.records, namespace, author, key.clone());
        Ok(Self {
            inner: parents.into_iter(),
        })
    }
}

fn parents(
    table: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    namespace: NamespaceId,
    author: AuthorId,
    mut key: Vec<u8>,
) -> Vec<anyhow::Result<SignedEntry>> {
    let mut res = Vec::new();

    while !key.is_empty() {
        let entry = get_exact(table, namespace, author, &key, false);
        key.pop();
        match entry {
            Err(err) => res.push(Err(err)),
            Ok(Some(entry)) => res.push(Ok(entry)),
            Ok(None) => continue,
        }
    }
    res.reverse();
    res
}

impl Iterator for ParentIterator {
    type Item = Result<SignedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Iterator for all content hashes
///
/// Note that you might get duplicate hashes. Also, the iterator will keep
/// a database snapshot open until it is dropped.
///
/// Also, this represents a snapshot of the database at the time of creation.
/// It needs a copy of a redb::ReadOnlyTable to be self-contained.
#[derive(derive_more::Debug)]
pub struct ContentHashesIterator {
    #[debug(skip)]
    range: RecordsRange<'static>,
}

impl ContentHashesIterator {
    /// Create a new iterator over all content hashes.
    pub fn all(table: &RecordsTable) -> anyhow::Result<Self> {
        let range = RecordsRange::all_static(table)?;
        Ok(Self { range })
    }
}

impl Iterator for ContentHashesIterator {
    type Item = Result<Hash>;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.range.next()?;
        Some(v.map(|e| e.content_hash()))
    }
}

/// Iterator over the latest entry per author.
#[derive(derive_more::Debug)]
#[debug("LatestIterator")]
pub struct LatestIterator<'a>(
    redb::Range<'a, LatestPerAuthorKey<'static>, LatestPerAuthorValue<'static>>,
);

impl<'a> LatestIterator<'a> {
    fn new(
        latest_per_author: &'a impl ReadableTable<
            LatestPerAuthorKey<'static>,
            LatestPerAuthorValue<'static>,
        >,
        namespace: NamespaceId,
    ) -> anyhow::Result<Self> {
        let start = (namespace.as_bytes(), &[u8::MIN; 32]);
        let end = (namespace.as_bytes(), &[u8::MAX; 32]);
        let range = latest_per_author.range(start..=end)?;
        Ok(Self(range))
    }
}

impl Iterator for LatestIterator<'_> {
    type Item = Result<(AuthorId, u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_map(|key, value| {
            let (_namespace, author) = key;
            let (timestamp, key) = value;
            (author.into(), timestamp, key.to_vec())
        })
    }
}

fn into_entry(key: RecordsId, value: RecordsValue) -> SignedEntry {
    let (namespace, author, key) = key;
    let (timestamp, namespace_sig, author_sig, len, hash) = value;
    let id = RecordIdentifier::new(namespace, author, key);
    let record = Record::new(hash.into(), len, timestamp);
    let entry = Entry::new(id, record);
    let entry_signature = EntrySignature::from_parts(namespace_sig, author_sig);
    SignedEntry::new(entry_signature, entry)
}

#[cfg(all(test, feature = "fs-store"))]
mod tests {
    use super::*;
    use crate::ranger::{InsertOutcome, Store as _};

    /// Retiring write authority keeps the entries, commits before returning,
    /// and survives a process that never gets to shut down cleanly.
    ///
    /// The commit is the point. `modify` shares an open transaction and
    /// explicitly does not guarantee persistence, so reporting success before
    /// flushing would let a crash roll back a transition the caller has already
    /// acted on. `std::mem::forget` models exactly that: no `Drop`, no graceful
    /// flush, nothing but what was already committed.
    #[tokio::test]
    async fn retire_commits_before_returning_and_keeps_entries() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();

        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;
        let mut replica = store.new_replica(namespace.clone())?;
        replica.hash_and_insert(b"k", &author, b"v").await?;
        drop(replica);
        store.close_replica(id);

        assert_eq!(store.retire_write_authority(&id)?, CapabilityKind::Read);

        // Read the value that is actually *committed*, through an independent
        // read transaction — not through the store's own possibly-open write
        // transaction, which would show uncommitted data and prove nothing.
        // This is what a process that died here would find on the next boot.
        {
            use super::tables::NAMESPACES_TABLE;
            let tx = store.db.begin_read()?;
            let table = tx.open_table(NAMESPACES_TABLE)?;
            let entry = table
                .get(id.as_bytes())?
                .context("namespace missing from the committed database")?;
            let (raw_kind, raw_bytes) = entry.value();
            assert_eq!(
                Capability::from_raw(raw_kind, raw_bytes)?.kind(),
                CapabilityKind::Read,
                "the retirement must be committed before it is reported, or a \
                 crash rolls back a transition the caller already acted on",
            );
        }

        assert_eq!(
            store.namespace_capability_kind(&id)?,
            CapabilityKind::Read,
            "the retirement must have been committed before it was reported",
        );
        let entries = store
            .get_many(id, Query::author(author.id()))?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            entries.len(),
            1,
            "retiring write authority narrows access; it does not delete content",
        );
        assert_eq!(entries[0].key(), b"k");

        // No signing key remains, so nothing local can write.
        assert!(
            store.open_replica(&id)?.secret_key().is_err(),
            "a read-only replica must not yield a signing key",
        );
        store.close_replica(id);

        // Idempotent.
        assert_eq!(store.retire_write_authority(&id)?, CapabilityKind::Read);

        // And a later valid write capability upgrades again: this forgets the
        // secret, it does not blacklist the namespace.
        let outcome = store.import_namespace(Capability::Write(namespace))?;
        assert!(matches!(outcome, ImportNamespaceOutcome::Upgraded));
        assert_eq!(store.namespace_capability_kind(&id)?, CapabilityKind::Write);
        Ok(())
    }

    /// An open replica is refused, and the stored capability is untouched.
    #[tokio::test]
    async fn retire_refuses_an_open_replica() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        let _replica = store.new_replica(namespace)?;

        assert!(
            store.retire_write_authority(&id).is_err(),
            "fail closed rather than retire under a live writable handle",
        );
        assert_eq!(
            store.namespace_capability_kind(&id)?,
            CapabilityKind::Write,
            "and the stored capability is unchanged",
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_ranges() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;

        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let mut replica = store.new_replica(namespace.clone())?;

        // test author prefix relation for all-255 keys
        let key1 = vec![255, 255];
        let key2 = vec![255, 255, 255];
        replica.hash_and_insert(&key1, &author, b"v1").await?;
        replica.hash_and_insert(&key2, &author, b"v2").await?;
        let res = store
            .get_many(namespace.id(), Query::author(author.id()).key_prefix([255]))?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(res.len(), 2);
        assert_eq!(
            res.into_iter()
                .map(|entry| entry.key().to_vec())
                .collect::<Vec<_>>(),
            vec![key1, key2]
        );
        Ok(())
    }

    /// `get_range` must return exactly the entries of the namespace it is pinned to.
    ///
    /// All namespaces share one records table, keyed `(namespace, author, key)`, so the
    /// query bounds are the only thing keeping documents apart. The range in a `RangeItem`
    /// is remote-supplied and unvalidated, and the resulting diff is echoed straight back
    /// to the peer.
    #[test]
    fn test_get_range_stays_within_its_namespace() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;

        // A foreign namespace on either side of ours, so that both halves of a wrapping
        // range have something to leak.
        let ours = NamespaceSecret::new(&mut rand::rng());
        let (mut below, mut above) = (None, None);
        while below.is_none() || above.is_none() {
            let ns = NamespaceSecret::new(&mut rand::rng());
            if ns.id() < ours.id() {
                below = Some(ns);
            } else {
                above = Some(ns);
            }
        }
        for ns in [&ours, &below.unwrap(), &above.unwrap()] {
            store.new_replica(ns.clone())?;
            let mut wrapper = StoreInstance::new(ns.id(), &mut store);
            let id = RecordIdentifier::new(ns.id(), author.id(), b"key");
            let entry = Entry::new(id, Record::current_from_data(b"value"));
            wrapper.entry_put(SignedEntry::from_entry(entry, ns, &author))?;
        }

        let record_id = |namespace: [u8; 32], author: [u8; 32], key: &[u8]| {
            RecordIdentifier::new(NamespaceId::from(&namespace), AuthorId::from(&author), key)
        };
        let min = record_id([0; 32], [0; 32], b"");

        let mut wrapper = StoreInstance::new(ours.id(), &mut store);
        for (label, range) in [
            // identity range: the whole replica
            ("equal", Range::new(min.clone(), min.clone())),
            // regular range spanning the whole table
            (
                "less",
                Range::new(min.clone(), record_id([255; 32], [255; 32], b"")),
            ),
            // wrapping range whose `start .. y` half spans everything below `y`
            (
                "greater/high",
                Range::new(
                    record_id([255; 32], [255; 32], b"\xff"),
                    record_id([255; 32], [0; 32], b""),
                ),
            ),
            // wrapping range whose `x .. end` half spans everything above `x`
            (
                "greater/low",
                Range::new(record_id([0; 32], [0; 32], b"\x00"), min.clone()),
            ),
        ] {
            let returned = wrapper
                .get_range(range)?
                .map(|entry| entry.map(|entry| entry.namespace()))
                .collect::<Result<Vec<_>>>()?;
            // Exactly our own entry: nothing foreign leaked, and nothing legitimate was
            // clamped away either.
            assert_eq!(returned, vec![ours.id()], "{label} range");
        }
        Ok(())
    }

    #[test]
    fn test_basics() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;

        let authors: Vec<_> = store.list_authors()?.collect::<Result<_>>()?;
        assert!(authors.is_empty());

        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(namespace.id());
        let replica = store.load_replica_info(&namespace.id())?;
        assert_eq!(replica.capability.id(), namespace.id());

        let author_back = store.get_author(&author.id())?.unwrap();
        assert_eq!(author.to_bytes(), author_back.to_bytes(),);

        let mut wrapper = StoreInstance::new(namespace.id(), &mut store);
        for i in 0..5 {
            let id = RecordIdentifier::new(namespace.id(), author.id(), format!("hello-{i}"));
            let entry = Entry::new(id, Record::current_from_data(format!("world-{i}")));
            let entry = SignedEntry::from_entry(entry, &namespace, &author);
            wrapper.entry_put(entry)?;
        }

        // all
        let all: Vec<_> = wrapper.all()?.collect();
        assert_eq!(all.len(), 5);

        // add a second version
        let mut ids = Vec::new();
        for i in 0..5 {
            let id = RecordIdentifier::new(namespace.id(), author.id(), format!("hello-{i}"));
            let entry = Entry::new(
                id.clone(),
                Record::current_from_data(format!("world-{i}-2")),
            );
            let entry = SignedEntry::from_entry(entry, &namespace, &author);
            wrapper.entry_put(entry)?;
            ids.push(id);
        }

        // get all
        let entries = wrapper
            .store
            .get_many(namespace.id(), Query::all())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 5);

        // get all prefix
        let entries = wrapper
            .store
            .get_many(namespace.id(), Query::key_prefix("hello-"))?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 5);

        // delete and get
        for id in ids {
            let res = wrapper.get(&id)?;
            assert!(res.is_some());
            let out = wrapper.entry_remove(&id)?.unwrap();
            assert_eq!(out.entry().id(), &id);
            let res = wrapper.get(&id)?;
            assert!(res.is_none());
        }

        // get latest
        let entries = wrapper
            .store
            .get_many(namespace.id(), Query::all())?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 0);

        Ok(())
    }

    fn copy_and_modify(
        source: &std::path::Path,
        modify: impl Fn(&redb::WriteTransaction) -> Result<()>,
    ) -> Result<tempfile::NamedTempFile> {
        let dbfile = tempfile::NamedTempFile::new()?;
        std::fs::copy(source, dbfile.path())?;
        let db = Database::create(dbfile.path())?;
        let write_tx = db.begin_write()?;
        modify(&write_tx)?;
        write_tx.commit()?;
        drop(db);
        Ok(dbfile)
    }

    #[tokio::test]
    #[cfg(feature = "fs-store")]
    async fn test_migration_001_populate_latest_table() -> Result<()> {
        use super::tables::LATEST_PER_AUTHOR_TABLE;
        let dbfile = tempfile::NamedTempFile::new()?;
        let namespace = NamespaceSecret::new(&mut rand::rng());

        // create a store and add some data
        let expected = {
            let mut store = Store::persistent(dbfile.path())?;
            let author1 = store.new_author(&mut rand::rng())?;
            let author2 = store.new_author(&mut rand::rng())?;
            let mut replica = store.new_replica(namespace.clone())?;
            replica.hash_and_insert(b"k1", &author1, b"v1").await?;
            replica.hash_and_insert(b"k2", &author2, b"v1").await?;
            replica.hash_and_insert(b"k3", &author1, b"v1").await?;

            let expected = store
                .get_latest_for_each_author(namespace.id())?
                .collect::<Result<Vec<_>>>()?;
            // drop everything to clear file locks.
            store.close_replica(namespace.id());
            // flush the store to disk
            store.flush()?;
            drop(store);
            expected
        };
        assert_eq!(expected.len(), 2);

        // create a copy of our db file with the latest table deleted.
        let dbfile_before_migration = copy_and_modify(dbfile.path(), |tx| {
            tx.delete_table(LATEST_PER_AUTHOR_TABLE)?;
            Ok(())
        })?;

        // open the copied db file, which will run the migration.
        let mut store = Store::persistent(dbfile_before_migration.path())?;
        let actual = store
            .get_latest_for_each_author(namespace.id())?
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    #[cfg(feature = "fs-store")]
    fn test_migration_004_populate_by_key_index() -> Result<()> {
        use redb::ReadableTableMetadata;
        let dbfile = tempfile::NamedTempFile::new()?;

        let mut store = Store::persistent(dbfile.path())?;

        // check that the new table is there, even if empty
        {
            let tables = store.tables()?;
            assert_eq!(tables.records_by_key.len()?, 0);
        }

        // TODO: write test checking that the indexing is done correctly
        Ok(())
    }

    #[test]
    #[cfg(all(feature = "fs-store", feature = "redb-v2-migration"))]
    fn test_migration_redb_v2_tuples() -> Result<()> {
        use migrate_redb_v2_tuples::old;

        let dbfile = tempfile::NamedTempFile::new()?;
        let path = dbfile.path().to_path_buf();

        let ns = [1u8; 32];
        let author = [2u8; 32];
        let key: &[u8] = b"hello";
        let ns_sig = [3u8; 64];
        let auth_sig = [4u8; 64];
        let hash = [5u8; 32];

        {
            let db = redb_v3::Database::create(&path)?;
            let tx = db.begin_write()?;
            {
                let mut records = tx.open_table(old::RECORDS_TABLE)?;
                let mut latest = tx.open_table(old::LATEST_PER_AUTHOR_TABLE)?;
                let mut by_key = tx.open_table(old::RECORDS_BY_KEY_TABLE)?;
                records.insert(
                    (&ns, &author, key),
                    (42u64, &ns_sig, &auth_sig, 7u64, &hash),
                )?;
                latest.insert((&ns, &author), (42u64, key))?;
                by_key.insert((&ns, key, &author), ())?;
            }
            tx.commit()?;
        }

        // Confirm redb 4 rejects the file before migration.
        {
            let db = redb::Database::create(&path)?;
            let tx = db.begin_write()?;
            let err = Tables::new(&tx).unwrap_err();
            assert!(
                matches!(err, redb::TableError::TableTypeMismatch { .. }),
                "expected TableTypeMismatch, got {err:?}",
            );
        }

        let store = Store::persistent(&path)?;
        drop(store);

        let backup: std::path::PathBuf = {
            let mut p = path.clone().into_os_string();
            p.push(".backup-redb-v2-tuples");
            p.into()
        };
        assert!(
            backup.exists(),
            "missing backup file at {}",
            backup.display()
        );

        // After migration redb 4 can open the affected tables and the rows survive.
        {
            let db = redb::Database::create(&path)?;
            let tx = db.begin_read()?;
            let records = tx.open_table(tables::RECORDS_TABLE)?;
            let entries: Vec<_> = records.iter()?.collect::<std::result::Result<_, _>>()?;
            assert_eq!(entries.len(), 1);
            let (k, v) = &entries[0];
            assert_eq!(k.value(), (&ns, &author, key));
            assert_eq!(v.value(), (42u64, &ns_sig, &auth_sig, 7u64, &hash));

            let latest = tx.open_table(tables::LATEST_PER_AUTHOR_TABLE)?;
            let entries: Vec<_> = latest.iter()?.collect::<std::result::Result<_, _>>()?;
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0.value(), (&ns, &author));
            assert_eq!(entries[0].1.value(), (42u64, key));

            let by_key = tx.open_table(tables::RECORDS_BY_KEY_TABLE)?;
            let entries: Vec<_> = by_key.iter()?.collect::<std::result::Result<_, _>>()?;
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0.value(), (&ns, key, &author));
        }

        Ok(())
    }

    #[test]
    #[cfg(feature = "fs-store")]
    fn test_no_migration_on_fresh_store() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let path = dbfile.path().to_path_buf();
        let store = Store::persistent(&path)?;
        drop(store);

        let backup: std::path::PathBuf = {
            let mut p = path.clone().into_os_string();
            p.push(".backup-redb-v2-tuples");
            p.into()
        };
        assert!(
            !backup.exists(),
            "unexpected backup file at {}",
            backup.display()
        );

        let _store = Store::persistent(&path)?;
        assert!(!backup.exists());
        Ok(())
    }

    /// The full-namespace fingerprint computed independently of the
    /// maintained aggregate: decode and hash every record row directly.
    /// Going through the store's all-range `get_fingerprint` here would
    /// compare the maintained value to itself.
    fn scan_namespace_fingerprint(wrapper: &mut StoreInstance) -> Result<Fingerprint> {
        let mut fp = Fingerprint::empty();
        for entry in wrapper.all()? {
            fp ^= entry?.as_fingerprint();
        }
        Ok(fp)
    }

    /// A signed entry at `key` with fixed data and timestamp, so replicas
    /// built from it are byte-identical.
    fn fixed_entry(
        namespace: &NamespaceSecret,
        author: &Author,
        key: &str,
        data: &[u8],
        timestamp: u64,
    ) -> SignedEntry {
        let id = RecordIdentifier::new(namespace.id(), author.id(), key);
        let hash = Hash::new(data);
        let record = Record::new(hash, data.len() as u64, timestamp);
        SignedEntry::from_entry(Entry::new(id, record), namespace, author)
    }

    /// The maintained fingerprint equals a scan over every reachable state:
    /// entries written before the aggregate existed (the migration path),
    /// later inserts, overwrites, tombstones and duplicate re-inserts.
    #[test]
    fn namespace_fingerprint_migrates_and_matches_scan() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let path = dbfile.path().to_path_buf();
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();

        let author = {
            let mut store = Store::persistent(&path)?;
            store.new_author(&mut rand::rng())?
        };

        let mut store = Store::persistent(&path)?;
        store.import_author(author.clone())?;
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(id);

        let mut wrapper = StoreInstance::new(id, &mut store);
        // Entries land before any query: no maintained row exists, exactly
        // the state of a replica written before the fingerprint table.
        for i in 0..10u64 {
            wrapper.put(fixed_entry(
                &namespace,
                &author,
                &format!("k{i}"),
                b"v0",
                i + 1,
            ))?;
        }
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );

        // Maintained path: more inserts, an overwrite, a duplicate re-insert
        // and a tombstone all keep the aggregate equal to the scan.
        wrapper.put(fixed_entry(&namespace, &author, "k0", b"v1", 100))?;
        wrapper.put(fixed_entry(&namespace, &author, "k11", b"v0", 11))?;
        wrapper.put(fixed_entry(&namespace, &author, "k0", b"v1", 100))?;
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );
        // A tombstone at "k" removes k0, k1, k11 and is itself a record row.
        wrapper.put(fixed_entry(&namespace, &author, "k", &[], 200))?;
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );

        // The built aggregate survives a reopen: a second open reads it back
        // without rebuilding.
        {
            let _wrapper = wrapper;
        }
        store.close_replica(id);
        drop(store);
        let mut store = Store::persistent(&path)?;
        let mut wrapper = StoreInstance::new(id, &mut store);
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );
        Ok(())
    }

    /// A silently corrupted aggregate (a release build never compares it
    /// against a scan) is rebuilt from the records table once per replica
    /// open, bounding divergence to one process lifetime.
    #[test]
    fn load_replica_info_rebuilds_the_namespace_fingerprint() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let path = dbfile.path().to_path_buf();
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();

        let author = {
            let mut store = Store::persistent(&path)?;
            store.new_author(&mut rand::rng())?
        };

        // Write rows and remember what the aggregate must be.
        let expected = {
            let mut store = Store::persistent(&path)?;
            store.import_author(author.clone())?;
            let _replica = store.new_replica(namespace.clone())?;
            store.close_replica(id);
            let mut wrapper = StoreInstance::new(id, &mut store);
            for i in 0..5u64 {
                wrapper.put(fixed_entry(
                    &namespace,
                    &author,
                    &format!("k{i}"),
                    b"v",
                    i + 1,
                ))?;
            }
            scan_namespace_fingerprint(&mut wrapper)?
        };

        // Corrupt the stored aggregate row without touching the records and
        // without opening a replica (so nothing rebuilds): the corrupted
        // value is what the O(1) read would serve.
        let mut store = Store::persistent(&path)?;
        store.modify(|tables| {
            tables
                .namespace_fingerprint
                .insert(id.as_bytes(), &[0u8; 32])?;
            Ok(())
        })?;
        assert_eq!(
            StoreInstance::new(id, &mut store).namespace_fingerprint()?,
            Fingerprint([0u8; 32]),
            "the corruption is in place before the reopen"
        );

        // Opening the replica rebuilds the aggregate from the rows and the
        // rebuild persists: the next store open reads the healed value with
        // no rebuild of its own.
        let mut replica = store.open_replica(&id)?;
        assert_eq!(replica.namespace_fingerprint()?, expected);
        drop(replica);
        store.close_replica(id);
        drop(store);
        let mut store = Store::persistent(&path)?;
        assert_eq!(
            StoreInstance::new(id, &mut store).namespace_fingerprint()?,
            expected,
            "the rebuilt aggregate is durable"
        );
        Ok(())
    }

    /// Two replicas holding the same rows produce the same fingerprint,
    /// independent of insertion order and of consumer-side filtering.
    #[test]
    fn namespace_fingerprint_is_order_independent_and_view_independent() -> Result<()> {
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let author = {
            let dbfile = tempfile::NamedTempFile::new()?;
            let mut store = Store::persistent(dbfile.path())?;
            store.new_author(&mut rand::rng())?
        };
        let entries: Vec<_> = (0..20u64)
            .map(|i| fixed_entry(&namespace, &author, &format!("k{i}"), &[i as u8; 3], i + 1))
            .collect();

        let build = |order: &[usize]| -> Result<(Fingerprint, Fingerprint)> {
            let dbfile = tempfile::NamedTempFile::new()?;
            let mut store = Store::persistent(dbfile.path())?;
            store.import_author(author.clone())?;
            let _replica = store.new_replica(namespace.clone())?;
            store.close_replica(namespace.id());
            {
                let mut wrapper = StoreInstance::new(namespace.id(), &mut store);
                for &i in order {
                    wrapper.put(entries[i].clone())?;
                }
            }
            // A consumer-side read with an arbitrary filter must not move
            // the fingerprint: grants filter consumers, not the store.
            let _filtered: Vec<_> = store
                .get_many(namespace.id(), Query::author(author.id()).key_prefix(b"k1"))?
                .collect::<Result<_>>()?;
            let mut wrapper = StoreInstance::new(namespace.id(), &mut store);
            let maintained = wrapper.namespace_fingerprint()?;
            let scanned = scan_namespace_fingerprint(&mut wrapper)?;
            Ok((maintained, scanned))
        };

        let forward: Vec<_> = (0..20).collect();
        let mut reverse = forward.clone();
        reverse.reverse();
        let (fp_a, scan_a) = build(&forward)?;
        let (fp_b, scan_b) = build(&reverse)?;
        assert_eq!(fp_a, scan_a);
        assert_eq!(fp_b, scan_b);
        assert_eq!(fp_a, fp_b);
        Ok(())
    }

    /// Random inserts, overwrites, duplicate re-inserts and tombstones: the
    /// maintained fingerprint equals a scan after every operation.
    #[test]
    fn namespace_fingerprint_tracks_random_ops() -> Result<()> {
        use rand::{RngExt, SeedableRng};

        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(namespace.id());
        let mut wrapper = StoreInstance::new(namespace.id(), &mut store);

        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        let mut last = vec![0u8; 0];
        for op in 0..200u64 {
            let key = format!("k{}", rng.random_range(0..8usize));
            match rng.random_range(0..4usize) {
                // New version at the key.
                0 => {
                    let data = vec![op as u8; 1 + (op % 7) as usize];
                    wrapper.put(fixed_entry(&namespace, &author, &key, &data, op + 1))?;
                    last = data;
                }
                // Duplicate re-insert of the previous version.
                1 => {
                    let data = last.clone();
                    if !data.is_empty() {
                        wrapper.put(fixed_entry(&namespace, &author, &key, &data, op))?;
                    }
                }
                // Tombstone at a prefix, removing everything under it.
                _ => {
                    let prefix = key[..1].to_string();
                    wrapper.put(fixed_entry(&namespace, &author, &prefix, &[], op + 1))?;
                    last = Vec::new();
                }
            }
            assert_eq!(
                wrapper.namespace_fingerprint()?,
                scan_namespace_fingerprint(&mut wrapper)?,
                "after op {op}"
            );
        }
        Ok(())
    }

    /// The insertion sequence builds lazily over pre-existing rows, bumps
    /// exactly once per state-changing put (overwrites and tombstones
    /// included, byte-identical re-inserts not), and replays via
    /// `entries_since` — surviving a reopen.
    #[test]
    fn namespace_sequence_builds_bumps_and_replays() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let path = dbfile.path().to_path_buf();
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        let author = {
            let mut store = Store::persistent(&path)?;
            store.new_author(&mut rand::rng())?
        };

        let mut store = Store::persistent(&path)?;
        store.import_author(author.clone())?;
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(id);
        let mut wrapper = StoreInstance::new(id, &mut store);

        // Rows written before any sequence query: the first query builds
        // the index from the records table.
        for i in 0..10u64 {
            wrapper.put(fixed_entry(
                &namespace,
                &author,
                &format!("k{i}"),
                b"v0",
                i + 1,
            ))?;
        }
        assert_eq!(wrapper.namespace_sequence()?, 10);
        let replay = wrapper.entries_since(0, 0)?;
        assert_eq!(replay.len(), 10);
        for (i, (touch, entry)) in replay.iter().enumerate() {
            assert_eq!(*touch, i as u64 + 1);
            assert_eq!(entry.key(), format!("k{i}").as_bytes());
        }

        // A state-changing overwrite bumps once and replays after the old
        // sequence.
        wrapper.put(fixed_entry(&namespace, &author, "k0", b"v1", 100))?;
        assert_eq!(wrapper.namespace_sequence()?, 11);
        let replay = wrapper.entries_since(10, 0)?;
        assert_eq!(replay.len(), 1);
        let (touch, entry) = &replay[0];
        assert_eq!(*touch, 11);
        assert_eq!(entry.key(), b"k0");
        assert_eq!(entry.content_len(), 2);

        // A byte-identical re-insert is a no-op: no bump, no replay row.
        let outcome = wrapper.put(fixed_entry(&namespace, &author, "k0", b"v1", 100))?;
        assert!(matches!(outcome, InsertOutcome::NotInserted));
        assert_eq!(wrapper.namespace_sequence()?, 11);
        assert_eq!(wrapper.entries_since(10, 0)?.len(), 1);

        // A tombstone removes the covered keys and is itself a record row,
        // so it bumps and replays as an empty record.
        wrapper.put(fixed_entry(&namespace, &author, "k", &[], 200))?;
        assert_eq!(wrapper.namespace_sequence()?, 12);
        let replay = wrapper.entries_since(11, 0)?;
        assert_eq!(replay.len(), 1);
        let (touch, entry) = &replay[0];
        assert_eq!(*touch, 12);
        assert_eq!(entry.key(), b"k");
        assert_eq!(entry.content_len(), 0);

        // The sequence and index survive a reopen.
        {
            let _wrapper = wrapper;
        }
        store.close_replica(id);
        drop(store);
        let mut store = Store::persistent(&path)?;
        let mut wrapper = StoreInstance::new(id, &mut store);
        assert_eq!(wrapper.namespace_sequence()?, 12);
        let replay = wrapper.entries_since(11, 0)?;
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].1.key(), b"k");
        Ok(())
    }

    /// A key keeps only its latest touch row in the sequence index, so the
    /// index stays bounded by the record count no matter how often a key is
    /// touched, and replaying still yields each key once.
    #[test]
    fn sequence_index_compacts_superseded_touches() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(namespace.id());
        let mut wrapper = StoreInstance::new(namespace.id(), &mut store);

        // Three keys, then build the index (a query), so later puts hit the
        // maintenance path instead of the lazy build.
        wrapper.put(fixed_entry(&namespace, &author, "hot", b"v0", 1))?;
        wrapper.put(fixed_entry(&namespace, &author, "a", b"v0", 2))?;
        wrapper.put(fixed_entry(&namespace, &author, "b", b"v0", 3))?;
        assert_eq!(wrapper.namespace_sequence()?, 3);

        // Touch the hot key twice more: each state-changing put bumps the
        // sequence, but compaction keeps one touch row per record key.
        wrapper.put(fixed_entry(&namespace, &author, "hot", b"v1", 4))?;
        wrapper.put(fixed_entry(&namespace, &author, "hot", b"v2", 5))?;
        assert_eq!(wrapper.namespace_sequence()?, 5);

        // Three record keys, so at most three touch rows: the hot key's
        // superseded touches are gone.
        let row_count = {
            let tables = wrapper.store.as_mut().tables()?;
            let ns_id = namespace.id();
            let ns = ns_id.as_bytes();
            tables
                .records_by_seq
                .range((ns, 0u64)..=(ns, u64::MAX))?
                .count()
        };
        assert_eq!(row_count, 3, "superseded touches are compacted away");

        // Replay still yields each key once, with its latest touch.
        let replay = wrapper.entries_since(0, 0)?;
        assert_eq!(replay.len(), 3);
        let by_key: std::collections::BTreeMap<_, _> = replay
            .iter()
            .map(|(touch, entry)| (entry.key().to_vec(), *touch))
            .collect();
        // The lazy build assigns sequence in records-table order (a, b, hot),
        // so migrated rows are a=1, b=2; the later touches keep hot at 5.
        assert_eq!(by_key.get(b"hot".as_slice()), Some(&5));
        assert_eq!(by_key.get(b"a".as_slice()), Some(&1));
        assert_eq!(by_key.get(b"b".as_slice()), Some(&2));

        // Paging: bounded pages, and the concatenation equals the tail.
        let page1 = wrapper.entries_since(0, 2)?;
        assert_eq!(page1.len(), 2);
        let next = page1[1].0;
        let page2 = wrapper.entries_since(next, 2)?;
        assert_eq!(page2.len(), 1);
        let mut paged: Vec<_> = page1;
        paged.extend(page2);
        assert_eq!(paged, replay);
        Ok(())
    }

    /// A direct `entry_put` overwrite (no preceding `remove_prefix_filtered`)
    /// still keeps the fingerprint equal to a scan, and a byte-identical
    /// direct re-insert neither moves the fingerprint nor bumps the
    /// sequence.
    #[test]
    fn direct_entry_put_overwrite_keeps_fingerprint_and_bumps_once() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(namespace.id());
        let mut wrapper = StoreInstance::new(namespace.id(), &mut store);

        wrapper.put(fixed_entry(&namespace, &author, "k0", b"v0", 1))?;
        // Build both aggregates up front, as a consumer would.
        assert_eq!(wrapper.namespace_sequence()?, 1);
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );

        // Overwrite through the low-level path, skipping prefix removal.
        wrapper.entry_put(fixed_entry(&namespace, &author, "k0", b"v1", 2))?;
        assert_eq!(wrapper.namespace_sequence()?, 2);
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );
        let replay = wrapper.entries_since(1, 0)?;
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].0, 2);
        assert_eq!(replay[0].1.key(), b"k0");
        assert_eq!(replay[0].1.content_len(), 2);

        // Byte-identical direct re-insert: nothing changes.
        wrapper.entry_put(fixed_entry(&namespace, &author, "k0", b"v1", 2))?;
        assert_eq!(wrapper.namespace_sequence()?, 2);
        assert_eq!(
            wrapper.namespace_fingerprint()?,
            scan_namespace_fingerprint(&mut wrapper)?
        );
        assert_eq!(wrapper.entries_since(1, 0)?.len(), 1);
        Ok(())
    }

    /// Random inserts, overwrites, duplicate re-inserts and tombstones: the
    /// sequence tracks exactly the state-changing puts, and replaying from
    /// zero yields the current records with each key's latest touch.
    #[test]
    fn namespace_sequence_tracks_random_ops() -> Result<()> {
        use rand::{RngExt, SeedableRng};
        use std::collections::BTreeMap;

        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(namespace.id());
        let mut wrapper = StoreInstance::new(namespace.id(), &mut store);

        let mut rng = rand::rngs::StdRng::seed_from_u64(11);
        let mut expected_seq = 0u64;
        let mut last_touch: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut last = vec![0u8; 0];
        for op in 0..200u64 {
            let key = format!("k{}", rng.random_range(0..8usize));
            let mut changed = false;
            let mut put_key = key.clone();
            match rng.random_range(0..4usize) {
                // New version at the key.
                0 => {
                    let data = vec![op as u8; 1 + (op % 7) as usize];
                    wrapper.put(fixed_entry(&namespace, &author, &key, &data, op + 1))?;
                    last = data;
                    changed = true;
                }
                // Duplicate re-insert of the previous version.
                1 => {
                    let data = last.clone();
                    if !data.is_empty() {
                        let outcome =
                            wrapper.put(fixed_entry(&namespace, &author, &key, &data, op))?;
                        changed = !matches!(outcome, InsertOutcome::NotInserted);
                    }
                }
                // Tombstone at a prefix, removing everything under it.
                _ => {
                    let prefix = key[..1].to_string();
                    wrapper.put(fixed_entry(&namespace, &author, &prefix, &[], op + 1))?;
                    last = Vec::new();
                    put_key = prefix;
                    changed = true;
                }
            }
            if changed {
                expected_seq += 1;
                last_touch.insert(put_key.into_bytes(), expected_seq);
            }
            assert_eq!(wrapper.namespace_sequence()?, expected_seq, "after op {op}");
            // Replay from zero converges on the current records, each key
            // carrying its latest touch.
            let replay = wrapper.entries_since(0, 0)?;
            let first = wrapper.get_first()?;
            let current: Vec<_> = wrapper
                .get_range(crate::ranger::Range::new(first.clone(), first))?
                .collect::<Result<_>>()?;
            assert_eq!(replay.len(), current.len(), "after op {op}");
            let mut by_key: BTreeMap<Vec<u8>, (u64, SignedEntry)> = replay
                .into_iter()
                .map(|(touch, entry)| (entry.key().to_vec(), (touch, entry)))
                .collect();
            let mut current_by_key: BTreeMap<Vec<u8>, SignedEntry> = current
                .into_iter()
                .map(|entry| (entry.key().to_vec(), entry))
                .collect();
            for (key, current_entry) in &current_by_key {
                let (touch, replay_entry) = by_key.remove(key).expect("replayed key missing");
                let tracked = last_touch.get(key);
                assert_eq!(
                    touch,
                    tracked.copied().unwrap_or(0),
                    "touch for {key:?} after op {op}"
                );
                assert_eq!(
                    &replay_entry, current_entry,
                    "entry for {key:?} after op {op}"
                );
            }
            assert!(by_key.is_empty(), "replay has keys absent from the store");
            let _ = &mut current_by_key;
        }
        Ok(())
    }

    /// Removing a replica clears the sequence tables with it.
    #[test]
    fn remove_replica_clears_sequence_tables() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let mut store = Store::persistent(dbfile.path())?;
        let author = store.new_author(&mut rand::rng())?;
        let namespace = NamespaceSecret::new(&mut rand::rng());
        let id = namespace.id();
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(id);
        {
            let mut wrapper = StoreInstance::new(id, &mut store);
            wrapper.put(fixed_entry(&namespace, &author, "k0", b"v0", 1))?;
            assert_eq!(wrapper.namespace_sequence()?, 1);
        }
        store.remove_replica(&id)?;
        let _replica = store.new_replica(namespace.clone())?;
        store.close_replica(id);
        let mut wrapper = StoreInstance::new(id, &mut store);
        assert_eq!(wrapper.namespace_sequence()?, 0);
        assert!(wrapper.entries_since(0, 0)?.is_empty());
        Ok(())
    }
}

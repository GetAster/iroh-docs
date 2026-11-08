//! [`ProtocolHandler`] implementation for the docs [`Engine`].

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use iroh::{endpoint::Connection, protocol::ProtocolHandler, Endpoint};
use iroh_blobs::api::Store as BlobsStore;
use iroh_gossip::net::Gossip;

use crate::{
    api::DocsApi,
    engine::{DefaultAuthorStorage, Engine, ProtectCallbackHandler},
    store::Store,
    Clock,
};

/// Default size threshold for inlining entry values in sync and gossip
/// messages: entries up to 2 KiB of content ride the message with their value.
pub const DEFAULT_MAX_INLINE_VALUE_SIZE: usize = 2048;

#[derive(Default, Debug)]
enum Storage {
    #[default]
    Memory,
    #[cfg(feature = "fs-store")]
    Persistent(std::path::PathBuf),
}

/// Docs protocol.
#[derive(Debug, Clone)]
pub struct Docs {
    engine: Arc<Engine>,
    api: DocsApi,
}

impl Docs {
    /// Create a new [`Builder`] for the docs protocol, using in memory replica and author storage.
    pub fn memory() -> Builder {
        Builder::default()
    }

    /// Create a new [`Builder`] for the docs protocol, using a persistent replica and author storage
    /// in the given directory.
    #[cfg(feature = "fs-store")]
    pub fn persistent(path: std::path::PathBuf) -> Builder {
        Builder {
            storage: Storage::Persistent(path),
            protect_cb: None,
            clock: None,
            sync_inactivity_timeout: None,
            max_future_shift: None,
            max_inline_value_size: None,
            redb_cache_size: None,
        }
    }

    /// Creates a new [`Docs`] from an [`Engine`].
    pub fn new(engine: Engine) -> Self {
        let engine = Arc::new(engine);
        let api = DocsApi::spawn(engine.clone());
        Self { engine, api }
    }

    /// Returns the API for this docs instance.
    pub fn api(&self) -> &DocsApi {
        &self.api
    }

    /// Returns the sync handle, for direct store operations such as
    /// enumerating the content the live actors currently reference.
    pub fn sync(&self) -> &crate::actor::SyncHandle {
        &self.engine.sync
    }
}

impl std::ops::Deref for Docs {
    type Target = DocsApi;

    fn deref(&self) -> &Self::Target {
        &self.api
    }
}

impl ProtocolHandler for Docs {
    async fn accept(&self, connection: Connection) -> Result<(), iroh::protocol::AcceptError> {
        self.engine
            .handle_connection(connection)
            .await
            .map_err(|err| iroh::protocol::AcceptError::from_err(n0_error::anyerr!(err)))?;
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(err) = self.engine.shutdown().await {
            tracing::warn!("shutdown error: {:?}", err);
        }
    }
}

/// Builder for the docs protocol.
#[derive(derive_more::Debug, Default)]
pub struct Builder {
    storage: Storage,
    protect_cb: Option<ProtectCallbackHandler>,
    #[debug("Clock")]
    clock: Option<Arc<dyn Clock>>,
    sync_inactivity_timeout: Option<Duration>,
    max_future_shift: Option<Duration>,
    max_inline_value_size: Option<usize>,
    redb_cache_size: Option<usize>,
}

impl Builder {
    /// Set the page cache size of the persistent replica store, in bytes.
    ///
    /// Without it the store uses redb's default (1 GiB). The cache grows up
    /// to this bound as pages are read, so it bounds the store's resident
    /// memory. Ignored for in-memory storage.
    pub fn redb_cache_size(mut self, bytes: usize) -> Self {
        self.redb_cache_size = Some(bytes);
        self
    }

    /// Set the garbage collection protection handler for blobs.
    ///
    /// See [`ProtectCallbackHandler::new`] for details.
    pub fn protect_handler(mut self, protect_handler: ProtectCallbackHandler) -> Self {
        self.protect_cb = Some(protect_handler);
        self
    }

    /// Set the [`Clock`] that stamps local entries and screens remote ones.
    ///
    /// Without one, local entries use system time and remote entries more than ten minutes
    /// ahead of it are refused.
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Set how long a sync exchange may go without progress before it is abandoned.
    ///
    /// The limit applies to every step that depends on the peer and restarts whenever the peer
    /// makes progress, so it does not bound the total duration of a large sync. The exchange
    /// fails with a timeout error and the connection is closed, which lets the same peer be
    /// synced again. Defaults to [`crate::net::DEFAULT_SYNC_INACTIVITY_TIMEOUT`] (30 seconds).
    pub fn sync_inactivity_timeout(mut self, timeout: Duration) -> Self {
        self.sync_inactivity_timeout = Some(timeout);
        self
    }

    /// Set how far ahead of the wall clock a remote entry may be stamped before it is refused.
    ///
    /// The default is [`crate::MAX_TIMESTAMP_FUTURE_SHIFT`] (ten minutes). Only applies without
    /// a [`Clock`]: a configured clock owns the timestamp rules, and this limit is then ignored.
    pub fn max_future_shift(mut self, shift: Duration) -> Self {
        self.max_future_shift = Some(shift);
        self
    }

    /// Set the size threshold for inlining entry values in sync and gossip
    /// messages.
    ///
    /// Entries at or below this many bytes of content carry their value inside
    /// the message itself; the receiver imports it directly instead of
    /// downloading the value blob, saving one round trip per small entry. The
    /// default is [`DEFAULT_MAX_INLINE_VALUE_SIZE`] (2048 bytes). Set to 0 to
    /// disable the feature.
    pub fn max_inline_value_size(mut self, size: usize) -> Self {
        self.max_inline_value_size = Some(size);
        self
    }

    /// Build a [`Docs`] protocol given a [`BlobsStore`] and [`Gossip`] protocol.
    pub async fn spawn(
        self,
        endpoint: Endpoint,
        blobs: BlobsStore,
        gossip: Gossip,
    ) -> anyhow::Result<Docs> {
        let replica_store = match &self.storage {
            Storage::Memory => Store::memory(),
            #[cfg(feature = "fs-store")]
            Storage::Persistent(path) => {
                Store::persistent_with_cache_size(path.join("docs.redb"), self.redb_cache_size)?
            }
        };
        let author_store = match &self.storage {
            Storage::Memory => DefaultAuthorStorage::Mem,
            #[cfg(feature = "fs-store")]
            Storage::Persistent(path) => {
                DefaultAuthorStorage::Persistent(path.join("default-author"))
            }
        };
        let downloader = blobs.downloader(&endpoint);
        let engine = Engine::spawn_with_options(
            endpoint,
            gossip,
            replica_store,
            blobs,
            downloader,
            author_store,
            self.protect_cb,
            self.clock,
            self.sync_inactivity_timeout
                .unwrap_or(crate::net::DEFAULT_SYNC_INACTIVITY_TIMEOUT),
            self.max_future_shift
                .unwrap_or(Duration::from_micros(crate::MAX_TIMESTAMP_FUTURE_SHIFT)),
            self.max_inline_value_size
                .unwrap_or(DEFAULT_MAX_INLINE_VALUE_SIZE),
        )
        .await?;
        Ok(Docs::new(engine))
    }
}

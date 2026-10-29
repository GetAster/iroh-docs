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
}

impl Builder {
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
            Storage::Persistent(path) => Store::persistent(path.join("docs.redb"))?,
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
        )
        .await?;
        Ok(Docs::new(engine))
    }
}

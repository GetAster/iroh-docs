//! Network implementation of the iroh-docs protocol

use std::future::Future;

use iroh::{Endpoint, EndpointAddr, PublicKey};
use n0_future::time::{timeout, Duration, Instant};
use serde::{Deserialize, Serialize};
use tracing::{debug, error_span, trace, Instrument};

use crate::{
    actor::SyncHandle,
    metrics::Metrics,
    net::codec::{run_alice, BobState},
    NamespaceId, SyncOutcome,
};

/// The ALPN identifier for the iroh-docs protocol
pub const ALPN: &[u8] = b"/iroh-sync/1";

mod codec;
mod idle;

use idle::{is_idle_timeout, IdleTimeout};

/// How long a sync exchange may go without any progress before it is abandoned.
///
/// This is an inactivity limit, not a limit on the total duration: it restarts whenever the peer
/// makes progress, so a large exchange that keeps moving is never cut off.
pub const DEFAULT_SYNC_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Application error code the connection is closed with when an exchange times out.
const TIMEOUT_CLOSE_CODE: u32 = 0x74_69_6d_65; // "time"

fn close_timed_out(connection: &iroh::endpoint::Connection) {
    connection.close(TIMEOUT_CLOSE_CODE.into(), b"sync inactivity timeout");
}

/// Connect to a peer and sync a replica
///
/// The exchange is abandoned after [`DEFAULT_SYNC_INACTIVITY_TIMEOUT`] without progress.
pub async fn connect_and_sync(
    endpoint: &Endpoint,
    sync: &SyncHandle,
    namespace: NamespaceId,
    peer: EndpointAddr,
    metrics: Option<&Metrics>,
) -> Result<SyncFinished, ConnectError> {
    connect_and_sync_with_timeout(
        endpoint,
        sync,
        namespace,
        peer,
        metrics,
        DEFAULT_SYNC_INACTIVITY_TIMEOUT,
    )
    .await
}

/// Like [`connect_and_sync`], with an explicit inactivity limit.
///
/// Every step that depends on the peer (connecting, opening the stream, each read and write,
/// closing) fails with [`ConnectError::Timeout`] if it makes no progress within `inactivity`.
/// The connection is then closed with an error code.
pub async fn connect_and_sync_with_timeout(
    endpoint: &Endpoint,
    sync: &SyncHandle,
    namespace: NamespaceId,
    peer: EndpointAddr,
    metrics: Option<&Metrics>,
    inactivity: Duration,
) -> Result<SyncFinished, ConnectError> {
    let t_start = Instant::now();
    let peer_id = peer.id;
    trace!("connect");
    let connection = timeout(inactivity, endpoint.connect(peer, crate::ALPN))
        .await
        .map_err(|_| ConnectError::timeout(inactivity))?
        .map_err(ConnectError::connect)?;

    let (mut send_stream, mut recv_stream) = match timeout(inactivity, connection.open_bi()).await {
        Ok(res) => res.map_err(ConnectError::connect)?,
        Err(_) => {
            close_timed_out(&connection);
            return Err(ConnectError::timeout(inactivity));
        }
    };

    let t_connect = t_start.elapsed();
    debug!(?t_connect, "connected");

    let res = {
        let mut writer = IdleTimeout::new(&mut send_stream, inactivity);
        let mut reader = IdleTimeout::new(&mut recv_stream, inactivity);
        run_alice(&mut writer, &mut reader, sync, namespace, peer_id).await
    };

    let closed = if matches!(res, Err(ConnectError::Timeout { .. })) {
        // The peer is not answering, so the closing handshake would not complete either.
        close_timed_out(&connection);
        Err(ConnectError::timeout(inactivity))
    } else {
        close_connect(&connection, &mut send_stream, &mut recv_stream, inactivity).await
    };

    if let Some(metrics) = metrics {
        if res.is_ok() {
            metrics.sync_via_connect_success.inc();
        } else {
            metrics.sync_via_connect_failure.inc();
        }
    }

    let t_process = t_start.elapsed() - t_connect;
    match &res {
        Ok(res) => {
            debug!(
                ?t_connect,
                ?t_process,
                sent = %res.num_sent,
                recv = %res.num_recv,
                "done, ok"
            );
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
        }
    }

    closed?;
    let outcome = res?;

    let timings = Timings {
        connect: t_connect,
        process: t_process,
    };

    let res = SyncFinished {
        namespace,
        peer: peer_id,
        outcome,
        timings,
    };

    Ok(res)
}

/// Finishes the dialer's streams and waits for the peer to acknowledge, bounded by `inactivity`.
async fn close_connect(
    connection: &iroh::endpoint::Connection,
    send_stream: &mut iroh::endpoint::SendStream,
    recv_stream: &mut iroh::endpoint::RecvStream,
    inactivity: Duration,
) -> Result<(), ConnectError> {
    let closing = async {
        send_stream.finish().map_err(ConnectError::close)?;
        send_stream.stopped().await.map_err(ConnectError::close)?;
        recv_stream
            .read_to_end(0)
            .await
            .map_err(ConnectError::close)?;
        Ok(())
    };
    match timeout(inactivity, closing).await {
        Ok(res) => res,
        Err(_) => {
            close_timed_out(connection);
            Err(ConnectError::timeout(inactivity))
        }
    }
}

/// Whether we want to accept or reject an incoming sync request.
#[derive(Debug, Clone)]
pub enum AcceptOutcome {
    /// Accept the sync request.
    Allow,
    /// Decline the sync request
    Reject(AbortReason),
}

/// Handle an iroh-docs connection and sync all shared documents in the replica store.
///
/// The exchange is abandoned after [`DEFAULT_SYNC_INACTIVITY_TIMEOUT`] without progress.
pub async fn handle_connection<F, Fut>(
    sync: SyncHandle,
    connection: iroh::endpoint::Connection,
    accept_cb: F,
    metrics: Option<&Metrics>,
) -> Result<SyncFinished, AcceptError>
where
    F: Fn(NamespaceId, PublicKey) -> Fut,
    Fut: Future<Output = AcceptOutcome>,
{
    handle_connection_with_timeout(
        sync,
        connection,
        accept_cb,
        metrics,
        DEFAULT_SYNC_INACTIVITY_TIMEOUT,
    )
    .await
}

/// Like [`handle_connection`], with an explicit inactivity limit.
///
/// Every step that depends on the peer (accepting the stream, each read and write, closing)
/// fails with [`AcceptError::Timeout`] if it makes no progress within `inactivity`. The
/// connection is then closed with an error code.
pub async fn handle_connection_with_timeout<F, Fut>(
    sync: SyncHandle,
    connection: iroh::endpoint::Connection,
    accept_cb: F,
    metrics: Option<&Metrics>,
    inactivity: Duration,
) -> Result<SyncFinished, AcceptError>
where
    F: Fn(NamespaceId, PublicKey) -> Fut,
    Fut: Future<Output = AcceptOutcome>,
{
    let t_start = Instant::now();
    let peer = connection.remote_id();
    let (mut send_stream, mut recv_stream) = match timeout(inactivity, connection.accept_bi()).await
    {
        Ok(res) => res.map_err(|e| AcceptError::open(peer, e))?,
        Err(_) => {
            close_timed_out(&connection);
            return Err(AcceptError::timeout(peer, None, inactivity));
        }
    };

    let t_connect = t_start.elapsed();
    let span = error_span!("accept", peer = %peer.fmt_short(), namespace = tracing::field::Empty);
    span.in_scope(|| {
        debug!(?t_connect, "connection established");
    });

    let mut state = BobState::new(peer);
    let res = {
        let mut writer = IdleTimeout::new(&mut send_stream, inactivity);
        let mut reader = IdleTimeout::new(&mut recv_stream, inactivity);
        state
            .run(&mut writer, &mut reader, sync, accept_cb)
            .instrument(span.clone())
            .await
    };

    if let Some(metrics) = metrics {
        if res.is_ok() {
            metrics.sync_via_accept_success.inc();
        } else {
            metrics.sync_via_accept_failure.inc();
        }
    }

    let namespace = state.namespace();
    let outcome = state.into_outcome();

    if matches!(res, Err(AcceptError::Timeout { .. })) {
        // The peer is not answering, so the closing handshake would not complete either.
        close_timed_out(&connection);
    } else {
        let closing = async {
            send_stream
                .finish()
                .map_err(|error| AcceptError::close(peer, namespace, error))?;
            send_stream
                .stopped()
                .await
                .map_err(|error| AcceptError::close(peer, namespace, error))?;
            recv_stream
                .read_to_end(0)
                .await
                .map_err(|error| AcceptError::close(peer, namespace, error))?;
            Ok(())
        };
        match timeout(inactivity, closing).await {
            Ok(closed) => closed?,
            Err(_) => {
                close_timed_out(&connection);
                return Err(AcceptError::timeout(peer, namespace, inactivity));
            }
        }
    }

    let t_process = t_start.elapsed() - t_connect;
    span.in_scope(|| match &res {
        Ok(_res) => {
            debug!(
                ?t_connect,
                ?t_process,
                sent = %outcome.num_sent,
                recv = %outcome.num_recv,
                "done, ok"
            );
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
        }
    });

    let namespace = res?;

    let timings = Timings {
        connect: t_connect,
        process: t_process,
    };
    let res = SyncFinished {
        namespace,
        outcome,
        peer,
        timings,
    };

    Ok(res)
}

/// Details of a finished sync operation.
#[derive(Debug, Clone)]
pub struct SyncFinished {
    /// The namespace that was synced.
    pub namespace: NamespaceId,
    /// The peer we syned with.
    pub peer: PublicKey,
    /// The outcome of the sync operation
    pub outcome: SyncOutcome,
    /// The time this operation took
    pub timings: Timings,
}

/// Time a sync operation took
#[derive(Debug, Default, Clone)]
pub struct Timings {
    /// Time to establish connection
    pub connect: Duration,
    /// Time to run sync exchange
    pub process: Duration,
}

/// Errors that may occur on handling incoming sync connections.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum AcceptError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to open replica
    #[error("Failed to open replica with {peer:?}")]
    Open {
        peer: PublicKey,
        #[source]
        error: anyhow::Error,
    },
    /// We aborted the sync request.
    #[error("Aborted sync of {namespace:?} with {peer:?}: {reason:?}")]
    Abort {
        peer: PublicKey,
        namespace: NamespaceId,
        reason: AbortReason,
    },
    /// Failed to run sync
    #[error("Failed to sync {namespace:?} with {peer:?}")]
    Sync {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close {namespace:?} with {peer:?}")]
    Close {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
    /// The peer made no progress within the inactivity limit.
    #[error("Sync of {namespace:?} with {peer:?} timed out: no progress for {limit:?}")]
    Timeout {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        limit: Duration,
    },
}

/// Errors that may occur on outgoing sync requests.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum ConnectError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// The remote peer aborted the sync request.
    #[error("Remote peer aborted sync: {0:?}")]
    RemoteAbort(AbortReason),
    /// Failed to run sync
    #[error("Failed to sync")]
    Sync {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close connection1")]
    Close {
        #[source]
        error: anyhow::Error,
    },
    /// The peer made no progress within the inactivity limit.
    #[error("Sync timed out: no progress for {limit:?}")]
    Timeout { limit: Duration },
}

/// Reason why we aborted an incoming sync request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AbortReason {
    /// Namespace is not available.
    NotFound,
    /// We are already syncing this namespace.
    AlreadySyncing,
    /// We experienced an error while trying to provide the requested resource
    InternalServerError,
}

impl AcceptError {
    fn open(peer: PublicKey, error: impl Into<anyhow::Error>) -> Self {
        Self::Open {
            peer,
            error: error.into(),
        }
    }
    fn timeout(peer: PublicKey, namespace: Option<NamespaceId>, limit: Duration) -> Self {
        Self::Timeout {
            peer,
            namespace,
            limit,
        }
    }
    /// A failure while exchanging messages; an idle-timeout failure becomes [`Self::Timeout`].
    pub(crate) fn sync(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        let error = error.into();
        if let Some(limit) = is_idle_timeout(&error) {
            return Self::timeout(peer, namespace, limit);
        }
        Self::Sync {
            peer,
            namespace,
            error,
        }
    }
    fn close(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Close {
            peer,
            namespace,
            error: error.into(),
        }
    }
    /// Get the peer's node ID (if available)
    pub fn peer(&self) -> Option<PublicKey> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { peer, .. } => Some(*peer),
            AcceptError::Sync { peer, .. } => Some(*peer),
            AcceptError::Close { peer, .. } => Some(*peer),
            AcceptError::Abort { peer, .. } => Some(*peer),
            AcceptError::Timeout { peer, .. } => Some(*peer),
        }
    }

    /// Get the namespace (if available)
    pub fn namespace(&self) -> Option<NamespaceId> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { .. } => None,
            AcceptError::Sync { namespace, .. } => namespace.to_owned(),
            AcceptError::Close { namespace, .. } => namespace.to_owned(),
            AcceptError::Abort { namespace, .. } => Some(*namespace),
            AcceptError::Timeout { namespace, .. } => namespace.to_owned(),
        }
    }
}

impl ConnectError {
    fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect {
            error: error.into(),
        }
    }
    fn close(error: impl Into<anyhow::Error>) -> Self {
        Self::Close {
            error: error.into(),
        }
    }
    fn timeout(limit: Duration) -> Self {
        Self::Timeout { limit }
    }
    /// A failure while exchanging messages; an idle-timeout failure becomes [`Self::Timeout`].
    pub(crate) fn sync(error: impl Into<anyhow::Error>) -> Self {
        let error = error.into();
        if let Some(limit) = is_idle_timeout(&error) {
            return Self::timeout(limit);
        }
        Self::Sync { error }
    }
    pub(crate) fn remote_abort(reason: AbortReason) -> Self {
        Self::RemoteAbort(reason)
    }
}

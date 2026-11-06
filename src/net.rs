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

/// The ALPN identifier for the iroh-docs protocol with inline values.
///
/// Nodes that enable inline values accept both ALPNs; the dialer picks
/// which wire format the connection uses by dialing [`ALPN_INLINE`] first
/// and falling back to [`ALPN`]. Peers that predate inline values accept
/// only [`ALPN`], so a mixed-version pair negotiates the version-0 wire
/// format and keeps syncing across a rolling upgrade instead of being
/// partitioned by a version bump.
pub const ALPN_INLINE: &[u8] = b"/iroh-sync/2";

/// The wire format of a sync connection, negotiated via ALPN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFormat {
    /// The frozen version-0 format: entries carry no value inline.
    Legacy,
    /// Entries at or below the configured threshold carry their value
    /// inline in the reconciliation messages.
    Inline,
}

impl WireFormat {
    /// The wire format for a connection, from its negotiated ALPN.
    pub fn from_alpn(alpn: &[u8]) -> Self {
        match alpn {
            alpn if alpn == ALPN_INLINE => Self::Inline,
            _ => Self::Legacy,
        }
    }
}

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

/// Whether the peer closed the connection gracefully (application code 0,
/// what dropping its last connection handle sends).
///
/// Each side ends an exchange by finishing its stream, waiting for the peer
/// to acknowledge it, and reading the peer's stream to its end; the side that
/// gets there first drops the connection. The other may still be waiting for
/// the acknowledgement of its final frame, which the close can overtake (or a
/// lost packet can carry, and acknowledgements are not retransmitted). Once
/// the exchange itself succeeded, a graceful close by the peer during this
/// closing phase means the peer is done, not that the sync failed.
fn closed_gracefully_by_peer(error: &iroh::endpoint::ConnectionError) -> bool {
    matches!(
        error,
        iroh::endpoint::ConnectionError::ApplicationClosed(close)
            if close.error_code == iroh::endpoint::VarInt::from_u32(0)
    )
}

/// Finish our stream, wait for the peer to take it, and read the peer's
/// stream to its end; a graceful close by the peer at any point counts as
/// done (see [`closed_gracefully_by_peer`]).
async fn close_streams(
    send_stream: &mut iroh::endpoint::SendStream,
    recv_stream: &mut iroh::endpoint::RecvStream,
) -> anyhow::Result<()> {
    use iroh::endpoint::{ReadError, ReadToEndError, StoppedError};
    send_stream.finish()?;
    match send_stream.stopped().await {
        Ok(_) => {}
        Err(StoppedError::ConnectionLost(error)) if closed_gracefully_by_peer(&error) => {
            return Ok(())
        }
        Err(error) => return Err(error.into()),
    }
    match recv_stream.read_to_end(0).await {
        Ok(_) => Ok(()),
        Err(ReadToEndError::Read(ReadError::ConnectionLost(error)))
            if closed_gracefully_by_peer(&error) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
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
    // Dial the inline-values ALPN first; a peer that predates the feature
    // refuses it, and the version-0 ALPN still gets through. The wire
    // format follows whichever ALPN was negotiated.
    let (connection, wire) =
        match timeout(inactivity, endpoint.connect(peer.clone(), ALPN_INLINE)).await {
            Ok(Ok(connection)) => {
                let wire = WireFormat::from_alpn(connection.alpn());
                (connection, wire)
            }
            // The peer refused the inline ALPN (it predates the feature) or
            // the attempt made no progress: fall back to the frozen
            // version-0 wire on the original ALPN.
            _ => {
                let connection = timeout(inactivity, endpoint.connect(peer, ALPN))
                    .await
                    .map_err(|_| ConnectError::timeout(inactivity))?
                    .map_err(ConnectError::connect)?;
                (connection, WireFormat::Legacy)
            }
        };

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
        run_alice(&mut writer, &mut reader, sync, namespace, peer_id, wire).await
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
            res.stats.log_summary();
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
    let closing = close_streams(send_stream, recv_stream);
    match timeout(inactivity, closing).await {
        Ok(res) => res.map_err(ConnectError::close),
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
    let wire = WireFormat::from_alpn(connection.alpn());
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
            .run(&mut writer, &mut reader, sync, accept_cb, wire)
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
        let closing = close_streams(&mut send_stream, &mut recv_stream);
        match timeout(inactivity, closing).await {
            Ok(closed) => closed.map_err(|error| AcceptError::close(peer, namespace, error))?,
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
            outcome.stats.log_summary();
        }
        Err(err) => {
            debug!(?t_connect, ?t_process, ?err, "done, failed");
            outcome.stats.log_summary();
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

#[cfg(test)]
mod close_tests {
    use iroh::endpoint::presets;

    use super::*;

    const TEST_ALPN: &[u8] = b"iroh-docs/close-test";

    /// A peer that accepts the stream and then closes the connection with
    /// `code`, without ever reading or acknowledging what we sent.
    async fn closed_by_peer(code: u32) -> anyhow::Result<anyhow::Result<()>> {
        let server = Endpoint::builder(presets::Minimal)
            .alpns(vec![TEST_ALPN.to_vec()])
            .bind()
            .await?;
        let client = Endpoint::builder(presets::Minimal).bind().await?;
        let addr = server.addr();
        let peer = tokio::spawn(async move {
            let conn = server.accept().await.expect("incoming").await?;
            let (mut send, _recv) = conn.accept_bi().await?;
            send.finish()?;
            conn.close(code.into(), b"done");
            conn.closed().await;
            anyhow::Ok(server)
        });
        let conn = client.connect(addr, TEST_ALPN).await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        // A byte the peer never reads, so our stream stays unacknowledged
        // until the peer's close ends the connection.
        send.write_all(b"x").await?;
        let res = timeout(Duration::from_secs(10), close_streams(&mut send, &mut recv))
            .await
            .expect("closing finished");
        let server = peer.await??;
        client.close().await;
        server.close().await;
        Ok(res)
    }

    #[tokio::test]
    async fn a_graceful_close_by_the_peer_completes_the_closing_phase() -> anyhow::Result<()> {
        closed_by_peer(0)
            .await?
            .expect("a code-0 close by the peer is a finished exchange");
        Ok(())
    }

    #[tokio::test]
    async fn an_error_close_by_the_peer_still_fails_the_closing_phase() -> anyhow::Result<()> {
        assert!(closed_by_peer(TIMEOUT_CLOSE_CODE).await?.is_err());
        Ok(())
    }
}

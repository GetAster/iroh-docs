//! A sync exchange that stops making progress must end, return the peer to idle and let the
//! next sync with that peer run.
//!
//! Without a limit, an exchange whose peer stops answering stays `Running` for as long as the
//! QUIC connection lives (keep-alives hold it open), and `Running` refuses every other sync with
//! that peer, as dialer and as acceptor.
//!
//! The stalls are staged with peers that hold the connection open and say nothing, and every
//! step is ordered by events (the stalled peer signalling that it holds the exchange, the
//! engine's `SyncFinished`), not by sleeps. The inactivity limit is the only elapsed time the
//! tests wait for.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use iroh::{
    endpoint::{presets, Connection, ConnectionError},
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, EndpointAddr,
};
use iroh_docs::{
    actor::{OpenOpts, SyncHandle},
    api::{
        protocol::{AddrInfoOptions, ShareMode},
        Doc,
    },
    engine::{LiveEvent, Origin, SyncEvent},
    net::AbortReason,
    protocol::Docs,
    store::Store,
    Author, NamespaceId, ProtocolMessage,
};
use iroh_gossip::net::Gossip;
use n0_future::StreamExt;
use rand::SeedableRng;
use serde::Serialize;
use tokio::sync::Notify;

/// Inactivity limit of the engine under test.
const LIMIT: Duration = Duration::from_secs(1);
/// Bound on waiting for an event that must happen; never the thing being measured.
const GUARD: Duration = Duration::from_secs(20);

/// Nodes do not accept the gossip ALPN, so the only syncs are the ones a test starts: gossip
/// neighbours would otherwise start their own.
///
/// Docs ALPN handler that can be told to hold the exchange without answering.
#[derive(Debug, Clone)]
struct Staller {
    docs: Docs,
    stalling: Arc<AtomicBool>,
    /// Signalled once a stalled exchange has received the dialer's first bytes.
    holding: Arc<Notify>,
    /// Receives how the dialer ended the connection it was stalled on.
    closed: Arc<tokio::sync::Mutex<Option<ConnectionError>>>,
}

impl ProtocolHandler for Staller {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        if !self.stalling.load(Ordering::SeqCst) {
            return self.docs.accept(connection).await;
        }
        let (_send, mut recv) = connection.accept_bi().await?;
        let mut first = [0u8; 1];
        recv.read_exact(&mut first).await.ok();
        self.holding.notify_one();
        // Say nothing and keep the connection open until the dialer closes it.
        let reason = connection.closed().await;
        *self.closed.lock().await = Some(reason);
        Ok(())
    }
}

struct TestNode {
    endpoint: Endpoint,
    docs: Docs,
    router: Router,
    staller: Staller,
}

impl TestNode {
    async fn spawn(inactivity: Option<Duration>) -> Result<Self> {
        let endpoint = Endpoint::bind(presets::Minimal).await?;
        let blobs = iroh_blobs::store::mem::MemStore::new();
        let gossip = Gossip::builder().spawn(endpoint.clone());
        let mut builder = Docs::memory();
        if let Some(inactivity) = inactivity {
            builder = builder.sync_inactivity_timeout(inactivity);
        }
        let docs = builder
            .spawn(endpoint.clone(), (*blobs).clone(), gossip.clone())
            .await?;
        let staller = Staller {
            docs: docs.clone(),
            stalling: Default::default(),
            holding: Default::default(),
            closed: Default::default(),
        };
        let router = Router::builder(endpoint.clone())
            .accept(
                iroh_blobs::ALPN,
                iroh_blobs::BlobsProtocol::new(&blobs, None),
            )
            .accept(iroh_docs::ALPN, staller.clone())
            .spawn();
        Ok(Self {
            endpoint,
            docs,
            router,
            staller,
        })
    }

    /// An address other nodes can dial this node on.
    async fn addr(&self, doc: &Doc) -> Result<EndpointAddr> {
        let ticket = doc
            .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
            .await?;
        ticket
            .nodes
            .into_iter()
            .next()
            .context("ticket names no node")
    }

    async fn shutdown(self) -> Result<()> {
        self.router.shutdown().await?;
        Ok(())
    }
}

/// How the connection ended, within the guard.
async fn closed_by(connection: &Connection) -> Result<ConnectionError> {
    tokio::time::timeout(GUARD, connection.closed())
        .await
        .context("the stalled connection was never closed")
}

/// The next `SyncFinished` the document reports.
async fn next_sync_finished(
    events: &mut (impl n0_future::Stream<Item = Result<LiveEvent>> + Unpin),
) -> Result<SyncEvent> {
    tokio::time::timeout(GUARD, async {
        while let Some(event) = events.next().await {
            if let LiveEvent::SyncFinished(event) = event? {
                return Ok(event);
            }
        }
        anyhow::bail!("event stream ended")
    })
    .await
    .context("no sync finished: the exchange never ended and its peer state never went idle")?
}

fn assert_timed_out(event: &SyncEvent) {
    let err = event.result.as_ref().expect_err("the exchange must fail");
    assert!(err.contains("timed out"), "not a timeout error: {err}");
}

/// Two nodes sharing one namespace; `a` holds an entry. Neither syncs yet.
async fn shared_doc(a: &TestNode, b: &TestNode) -> Result<(Doc, Doc, iroh_docs::Capability)> {
    let doc_a = a.docs.api().create().await?;
    let author = a.docs.api().author_create().await?;
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
    Ok((doc_a, doc_b, ticket.capability))
}

#[tokio::test]
async fn dialer_gives_up_on_a_peer_that_stops_answering() -> Result<()> {
    let a = TestNode::spawn(Some(LIMIT)).await?;
    let b = TestNode::spawn(None).await?;
    let (doc_a, doc_b, _) = shared_doc(&a, &b).await?;
    let b_addr = b.addr(&doc_b).await?;
    let mut events = doc_a.subscribe().await?;

    // `b` takes the stream and the first bytes of the request and never answers.
    b.staller.stalling.store(true, Ordering::SeqCst);
    doc_a.start_sync(vec![b_addr.clone()]).await?;
    b.staller.holding.notified().await;

    // From now on `b` answers.
    b.staller.stalling.store(false, Ordering::SeqCst);

    let first = next_sync_finished(&mut events).await?;
    assert_timed_out(&first);

    // The peer went back to idle: a new sync with it runs and reconciles.
    doc_a.start_sync(vec![b_addr]).await?;
    let second = next_sync_finished(&mut events).await?;
    let details = second.result.map_err(anyhow::Error::msg)?;
    assert_eq!(details.entries_sent, 1);

    // The stalled connection was closed with an application error code, not left to idle out.
    let closed = b.staller.closed.lock().await.take();
    assert!(
        matches!(closed, Some(ConnectionError::ApplicationClosed(_))),
        "stalled connection ended with {closed:?}"
    );

    a.shutdown().await?;
    b.shutdown().await
}

/// What a dialer puts on the wire first; mirrors the engine's private message type.
#[derive(Serialize)]
#[allow(dead_code)]
enum WireMessage {
    Init {
        namespace: NamespaceId,
        message: ProtocolMessage,
    },
    Sync(ProtocolMessage),
    Abort {
        reason: AbortReason,
    },
}

fn frame(message: &WireMessage) -> Result<Vec<u8>> {
    let body = postcard::to_stdvec(message)?;
    let mut framed = (body.len() as u32).to_be_bytes().to_vec();
    framed.extend(body);
    Ok(framed)
}

/// The first message of a sync of `namespace` from a replica holding one entry that `a` lacks.
async fn init_frame(capability: iroh_docs::Capability) -> Result<Vec<u8>> {
    let handle = SyncHandle::spawn(Store::memory(), None, "raw-dialer".into());
    let namespace = handle.import_namespace(capability).await?;
    handle.open(namespace, OpenOpts::default().sync()).await?;
    let author = handle
        .import_author(Author::new(&mut rand::rngs::ChaCha12Rng::seed_from_u64(7)))
        .await?;
    handle
        .insert_local(
            namespace,
            author,
            Bytes::from_static(b"other"),
            iroh_blobs::Hash::new(b"other"),
            5,
        )
        .await?;
    let message = handle.sync_initial_message(namespace).await?;
    frame(&WireMessage::Init { namespace, message })
}

#[tokio::test]
async fn acceptor_gives_up_on_a_dialer_that_stops_mid_exchange() -> Result<()> {
    let a = TestNode::spawn(Some(LIMIT)).await?;
    let b = TestNode::spawn(None).await?;
    let (doc_a, doc_b, capability) = shared_doc(&a, &b).await?;
    let a_addr = a.addr(&doc_a).await?;
    let b_addr = b.addr(&doc_b).await?;
    // `a` takes part in the namespace's live sync, so it accepts requests for it.
    doc_a.start_sync(vec![]).await?;
    let mut events = doc_a.subscribe().await?;

    // `b`'s identity dials `a` by hand: a valid request, `a`'s reply read, then silence.
    let connection = b.endpoint.connect(a_addr, iroh_docs::ALPN).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&init_frame(capability).await?).await?;
    let mut len = [0u8; 4];
    recv.read_exact(&mut len).await?;
    assert!(
        u32::from_be_bytes(len) > 0,
        "`a` must reply and wait for more"
    );

    let accepted = next_sync_finished(&mut events).await?;
    assert_timed_out(&accepted);
    assert!(matches!(accepted.origin, Origin::Accept));
    assert!(
        matches!(
            closed_by(&connection).await?,
            ConnectionError::ApplicationClosed(_)
        ),
        "the acceptor must close the stalled connection with an error code"
    );

    // The peer went back to idle on the accept side: a sync with the same peer runs.
    doc_a.start_sync(vec![b_addr]).await?;
    let next = next_sync_finished(&mut events).await?;
    next.result.map_err(anyhow::Error::msg)?;

    a.shutdown().await?;
    b.shutdown().await
}

#[tokio::test]
async fn acceptor_gives_up_on_a_dialer_that_sends_a_partial_request() -> Result<()> {
    let a = TestNode::spawn(Some(LIMIT)).await?;
    let b = TestNode::spawn(None).await?;
    let (doc_a, doc_b, capability) = shared_doc(&a, &b).await?;
    let a_addr = a.addr(&doc_a).await?;
    let b_addr = b.addr(&doc_b).await?;
    doc_a.start_sync(vec![]).await?;
    let mut events = doc_a.subscribe().await?;

    // Opens the stream and sends part of the request header, then nothing.
    let connection = b.endpoint.connect(a_addr, iroh_docs::ALPN).await?;
    let (mut send, _recv) = connection.open_bi().await?;
    send.write_all(&init_frame(capability).await?[..2]).await?;
    assert!(matches!(
        closed_by(&connection).await?,
        ConnectionError::ApplicationClosed(_)
    ));

    doc_a.start_sync(vec![b_addr]).await?;
    let next = next_sync_finished(&mut events).await?;
    next.result.map_err(anyhow::Error::msg)?;

    a.shutdown().await?;
    b.shutdown().await
}

#[tokio::test]
async fn a_normal_sync_is_unaffected() -> Result<()> {
    let a = TestNode::spawn(Some(LIMIT)).await?;
    let b = TestNode::spawn(Some(LIMIT)).await?;
    let (doc_a, doc_b, _) = shared_doc(&a, &b).await?;
    let a_addr = a.addr(&doc_a).await?;
    let mut events = doc_b.subscribe().await?;

    doc_b.start_sync(vec![a_addr]).await?;
    let event = next_sync_finished(&mut events).await?;
    let details = event.result.map_err(anyhow::Error::msg)?;
    assert_eq!(details.entries_received, 1);

    a.shutdown().await?;
    b.shutdown().await
}

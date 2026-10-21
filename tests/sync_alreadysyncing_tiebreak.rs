//! A connect that loses the simultaneous-dial tie-break is not a failed sync.
//!
//! When both peers dial each other, the lower id rejects the higher id's request with
//! `AlreadySyncing` (`expected_sync_direction`), by design: the lower id's own connect goes
//! ahead and the higher id accepts it. The higher id's rejected connect must reset its peer
//! state (the wedge fix, row 22) but must not surface as `SyncFinished(Err(AlreadySyncing))`
//! to subscribers, which only ever see the exchange that actually ran.
//!
//! The race is forced, not hoped for:
//! - `first.id() > second.id()`, so `first` is the connect-loser;
//! - `first`'s docs handler is wrapped to hold every incoming connection for a while, so the
//!   connect `second` opens stays Running on `second` for the whole hold;
//! - `second` dials `first`, then `first` dials `second` inside that window; `second` is still
//!   Running{Connect}, so it rejects `first` with `AlreadySyncing`. `first` is the subscriber
//!   under test. When the hold ends, `first` accepts `second`'s connect and the exchange runs.
//!
//! Before the quiet reset, `first`'s subscriber saw `SyncFinished(Err("remote aborted sync:
//! AlreadySyncing"))`.

use std::time::Duration;

use anyhow::Result;
use iroh::{
    endpoint::{presets, Connection},
    protocol::{AcceptError, ProtocolHandler, Router},
    Endpoint, SecretKey,
};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    engine::LiveEvent,
    protocol::Docs,
};
use iroh_gossip::net::Gossip;
use n0_future::StreamExt;
use rand::{RngExt, SeedableRng};

/// Docs acceptor that holds each incoming connection before handing it to the engine.
#[derive(Debug, Clone)]
struct DelayedDocs {
    inner: Docs,
    delay: Duration,
}

impl ProtocolHandler for DelayedDocs {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        tokio::time::sleep(self.delay).await;
        self.inner.accept(connection).await
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await
    }
}

/// `first.public() > second.public()` as byte strings.
fn ordered_keys(seed: &[u8]) -> (SecretKey, SecretKey) {
    let mut rng = rand::rngs::ChaCha12Rng::from_seed(*iroh_blobs::Hash::new(seed).as_bytes());
    loop {
        let a = SecretKey::from_bytes(&rng.random());
        let b = SecretKey::from_bytes(&rng.random());
        if a.public().as_bytes() > b.public().as_bytes() {
            return (a, b);
        }
    }
}

struct TestNode {
    ep: Endpoint,
    docs: Docs,
    router: Router,
}

async fn spawn_node(key: SecretKey, accept_delay: Duration) -> Result<TestNode> {
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(key)
        .bind()
        .await?;
    let store = iroh_blobs::store::mem::MemStore::new();
    let gossip = Gossip::builder().spawn(ep.clone());
    let docs = Docs::memory()
        .spawn(ep.clone(), (*store).clone(), gossip.clone())
        .await?;
    let router = Router::builder(ep.clone())
        .accept(
            iroh_blobs::ALPN,
            iroh_blobs::BlobsProtocol::new(&store, None),
        )
        .accept(iroh_gossip::ALPN, gossip)
        .accept(
            iroh_docs::ALPN,
            DelayedDocs {
                inner: docs.clone(),
                delay: accept_delay,
            },
        )
        .spawn();
    Ok(TestNode { ep, docs, router })
}

#[tokio::test]
async fn connect_losing_the_tiebreak_is_not_reported_as_a_failed_sync() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iroh_docs=debug".into()),
        )
        .with_test_writer()
        .try_init();
    let (first_key, second_key) = ordered_keys(b"alreadysyncing-tiebreak");
    // `first` holds incoming connections for 1.5s, which keeps `second`'s connect Running
    // while `first` dials back.
    let first = spawn_node(first_key, Duration::from_millis(1500)).await?;
    let second = spawn_node(second_key, Duration::ZERO).await?;
    assert!(first.ep.id().as_bytes() > second.ep.id().as_bytes());

    // `first` is the connect-loser; `second` is the tie-break winner.
    let doc_second = second.docs.api().create().await?;
    let ticket = doc_second
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;
    let second_addr = ticket
        .nodes
        .first()
        .cloned()
        .expect("ticket names its node");
    let doc_first = first
        .docs
        .api()
        .import_namespace(ticket.capability.clone())
        .await?;
    let first_addr = first.ep.addr();

    let author_first = first.docs.api().author_create().await?;
    let author_second = second.docs.api().author_create().await?;
    doc_first
        .set_bytes(author_first, b"from-first".to_vec(), b"1".to_vec())
        .await?;
    doc_second
        .set_bytes(author_second, b"from-second".to_vec(), b"2".to_vec())
        .await?;

    let mut first_events = doc_first.subscribe().await?;

    // `second` dials `first` and sits Running{Connect} while `first` holds the connection…
    doc_second.start_sync(vec![first_addr]).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    // …and `first` dials `second` inside that window: rejected with AlreadySyncing.
    doc_first.start_sync(vec![second_addr]).await?;

    // Watch `first`'s subscriber until both replicas converge, plus a settling period.
    let converged = async {
        loop {
            let a = doc_first
                .get_exact(author_second, b"from-second".to_vec(), false)
                .await?;
            let b = doc_second
                .get_exact(author_first, b"from-first".to_vec(), false)
                .await?;
            if a.is_some() && b.is_some() {
                return anyhow::Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let mut failed_rounds = Vec::new();
    let mut ok_rounds = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(20));
    tokio::pin!(deadline);
    tokio::pin!(converged);
    let mut done = false;
    let mut settle: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            r = &mut converged, if !done => {
                r?;
                done = true;
                settle = Some(Box::pin(tokio::time::sleep(Duration::from_secs(2))));
            }
            _ = async { settle.as_mut().unwrap().await }, if settle.is_some() => break,
            ev = first_events.next() => match ev {
                Some(Ok(LiveEvent::SyncFinished(ev))) => match &ev.result {
                    Ok(_) => ok_rounds += 1,
                    Err(e) => failed_rounds.push(e.clone()),
                },
                Some(_) => {}
                None => break,
            },
        }
    }
    assert!(done, "the replicas never converged");
    assert!(
        ok_rounds >= 1,
        "the connect-loser never saw the exchange that ran"
    );
    assert!(
        failed_rounds.is_empty(),
        "the connect-loser reported its tie-break loss as a failed sync: {failed_rounds:?}",
    );

    first.router.shutdown().await?;
    second.router.shutdown().await?;
    Ok(())
}

//! The per-peer sync policy decides which peers sync which namespaces.

use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};
use iroh::{endpoint::presets, Endpoint, PublicKey, SecretKey};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    engine::{LiveEvent, SyncDirection},
    NamespaceId,
};
use n0_future::{time::Duration, Stream, StreamExt};
use tokio::sync::mpsc;
use tracing_test::traced_test;

mod util;
use util::Node;

const TIMEOUT: Duration = Duration::from_secs(60);

async fn spawn_node(seed: u8) -> Result<Node> {
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::from_bytes(&[seed; 32]))
        .bind()
        .await?;
    Node::memory(ep).spawn().await
}

/// The result of the next `SyncFinished` with `peer`.
async fn next_sync(
    events: &mut (impl Stream<Item = Result<LiveEvent>> + Unpin),
    peer: PublicKey,
) -> Result<std::result::Result<(), String>> {
    let wait = async {
        while let Some(event) = events.next().await {
            if let LiveEvent::SyncFinished(e) = event? {
                if e.peer == peer {
                    return Ok(e.result.map(|_| ()));
                }
            }
        }
        bail!("event stream ended")
    };
    match tokio::time::timeout(TIMEOUT, wait).await {
        Ok(res) => res,
        Err(_) => bail!("no SyncFinished within {TIMEOUT:?}"),
    }
}

type Calls = Arc<Mutex<Vec<(NamespaceId, PublicKey, SyncDirection)>>>;

#[tokio::test]
#[traced_test]
async fn accept_policy_refuses_and_then_admits_a_peer() -> Result<()> {
    let node0 = spawn_node(1).await?;
    let node1 = spawn_node(2).await?;
    let docs0 = node0.client().docs();
    let docs1 = node1.client().docs();
    let author = docs0.author_create().await?;
    let doc0 = docs0.create().await?;
    doc0.set_bytes(author, b"k".to_vec(), b"v".to_vec()).await?;
    let namespace = doc0.id();
    let ticket = doc0
        .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
        .await?;
    let peer0 = ticket.nodes[0].clone();

    let calls: Calls = Default::default();
    let recorded = calls.clone();
    let refused = node1.id();
    node0
        .docs_protocol()
        .set_sync_policy(move |ns, peer, direction| {
            recorded.lock().unwrap().push((ns, peer, direction));
            !(ns == namespace && peer == refused)
        });

    let doc1 = docs1.import_namespace(ticket.capability.clone()).await?;
    let mut events = doc1.subscribe().await?;
    doc1.start_sync(vec![peer0.clone()]).await?;
    let result = next_sync(&mut events, peer0.id).await?;
    assert!(result.is_err(), "refused peer synced: {result:?}");
    assert!(doc1
        .get_exact(author, b"k".to_vec(), false)
        .await?
        .is_none());
    assert!(calls
        .lock()
        .unwrap()
        .contains(&(namespace, node1.id(), SyncDirection::Accept)));

    node0.docs_protocol().clear_sync_policy();
    doc1.start_sync(vec![peer0.clone()]).await?;
    let result = next_sync(&mut events, peer0.id).await?;
    assert!(result.is_ok(), "admitted peer failed to sync: {result:?}");
    assert!(doc1
        .get_exact(author, b"k".to_vec(), false)
        .await?
        .is_some());

    node0.shutdown().await?;
    node1.shutdown().await?;
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn connect_policy_skips_the_dial() -> Result<()> {
    let node0 = spawn_node(3).await?;
    let node1 = spawn_node(4).await?;
    let docs0 = node0.client().docs();
    let docs1 = node1.client().docs();
    let author = docs0.author_create().await?;
    let doc0 = docs0.create().await?;
    doc0.set_bytes(author, b"k".to_vec(), b"v".to_vec()).await?;
    let namespace = doc0.id();
    let ticket = doc0
        .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
        .await?;
    let peer0 = ticket.nodes[0].clone();

    // node0 records every request it is asked to accept.
    let accepted: Calls = Default::default();
    let recorded = accepted.clone();
    node0
        .docs_protocol()
        .set_sync_policy(move |ns, peer, direction| {
            recorded.lock().unwrap().push((ns, peer, direction));
            true
        });

    // node1 refuses to dial node0 for this namespace and reports each refusal.
    let (refusals_tx, mut refusals) = mpsc::unbounded_channel();
    let target = node0.id();
    node1
        .docs_protocol()
        .set_sync_policy(move |ns, peer, direction| {
            let allow = !(ns == namespace && peer == target && direction == SyncDirection::Connect);
            if !allow {
                refusals_tx.send(()).ok();
            }
            allow
        });
    let doc1 = docs1.import_namespace(ticket.capability.clone()).await?;
    let mut events = doc1.subscribe().await?;
    doc1.start_sync(vec![peer0.clone()]).await?;
    tokio::time::timeout(TIMEOUT, refusals.recv())
        .await?
        .expect("policy dropped");
    // The refusal runs before the dial, so node0 was never asked.
    assert!(accepted
        .lock()
        .unwrap()
        .iter()
        .all(|(ns, _, _)| *ns != namespace));
    assert!(doc1
        .get_exact(author, b"k".to_vec(), false)
        .await?
        .is_none());

    node1.docs_protocol().clear_sync_policy();
    doc1.start_sync(vec![peer0.clone()]).await?;
    let result = next_sync(&mut events, peer0.id).await?;
    assert!(result.is_ok(), "sync failed after clearing: {result:?}");
    assert!(doc1
        .get_exact(author, b"k".to_vec(), false)
        .await?
        .is_some());

    node0.shutdown().await?;
    node1.shutdown().await?;
    Ok(())
}

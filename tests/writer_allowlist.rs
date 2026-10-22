//! Per-document writer allowlist over the public docs API, between two nodes.

use std::collections::BTreeSet;

use anyhow::{bail, Result};
use iroh::{endpoint::presets, Endpoint, EndpointAddr, PublicKey, SecretKey};
use iroh_docs::{
    api::{
        protocol::{AddrInfoOptions, ShareMode},
        Doc,
    },
    engine::LiveEvent,
    AuthorId,
};
use n0_future::{time::Duration, Stream, StreamExt};
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

/// Waits for a successful `SyncFinished` with `peer`, failing on a failed one.
async fn sync_finished(
    events: &mut (impl Stream<Item = Result<LiveEvent>> + Unpin),
    peer: PublicKey,
) -> Result<()> {
    let wait = async {
        while let Some(event) = events.next().await {
            if let LiveEvent::SyncFinished(e) = event? {
                if e.peer == peer {
                    return e.result.map(|_| ()).map_err(anyhow::Error::msg);
                }
            }
        }
        bail!("event stream ended")
    };
    match tokio::time::timeout(TIMEOUT, wait).await {
        Ok(res) => res,
        Err(_) => bail!(
            "no SyncFinished with {} within {TIMEOUT:?}",
            peer.fmt_short()
        ),
    }
}

/// Starts a sync round with `peer` and waits until it finished.
async fn sync_with(doc: &Doc, peer: &EndpointAddr) -> Result<()> {
    let mut events = doc.subscribe().await?;
    doc.start_sync(vec![peer.clone()]).await?;
    sync_finished(&mut events, peer.id).await?;
    doc.leave().await?;
    Ok(())
}

async fn has(doc: &Doc, author: AuthorId, key: &[u8]) -> Result<bool> {
    Ok(doc.get_exact(author, key.to_vec(), false).await?.is_some())
}

#[tokio::test]
#[traced_test]
async fn allowed_writers_restrict_local_and_synced_entries() -> Result<()> {
    let node0 = spawn_node(1).await?;
    let node1 = spawn_node(2).await?;
    let docs0 = node0.client().docs();
    let docs1 = node1.client().docs();

    let writer = docs0.author_create().await?;
    let other = docs0.author_create().await?;
    let doc0 = docs0.create().await?;
    doc0.set_bytes(writer, b"w1".to_vec(), b"v".to_vec())
        .await?;
    doc0.set_bytes(other, b"o1".to_vec(), b"v".to_vec()).await?;

    // node1 joins without syncing, restricts the writers, then syncs.
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;
    let peer0 = ticket.nodes[0].clone();
    let doc1 = docs1.import_namespace(ticket.capability.clone()).await?;
    assert_eq!(doc1.get_allowed_writers().await?, None);
    doc1.set_allowed_writers([writer]).await?;
    assert_eq!(
        doc1.get_allowed_writers().await?,
        Some(BTreeSet::from([writer]))
    );

    sync_with(&doc1, &peer0).await?;
    assert!(has(&doc1, writer, b"w1").await?);
    assert!(!has(&doc1, other, b"o1").await?);

    // A local write under a disallowed author fails, even though node1 holds its key.
    for author in [writer, other] {
        docs1
            .author_import(docs0.author_export(author).await?.expect("author"))
            .await?;
    }
    let err = doc1
        .set_bytes(other, b"o-local".to_vec(), b"v".to_vec())
        .await
        .expect_err("disallowed author must not write");
    assert!(
        format!("{err:#}").contains("not allowed to write"),
        "unexpected error: {err:#}"
    );
    doc1.set_bytes(writer, b"w-local".to_vec(), b"v".to_vec())
        .await?;

    // New entries from both authors: only the allowed one reaches node1.
    doc0.set_bytes(other, b"o2".to_vec(), b"v".to_vec()).await?;
    doc0.set_bytes(writer, b"w2".to_vec(), b"v".to_vec())
        .await?;
    sync_with(&doc1, &peer0).await?;
    assert!(has(&doc1, writer, b"w2").await?);
    assert!(!has(&doc1, other, b"o1").await?);
    assert!(!has(&doc1, other, b"o2").await?);
    // node0 has no restriction and received node1's local entry.
    assert!(has(&doc0, writer, b"w-local").await?);

    // Clearing the list lets the next sync bring the rest.
    doc1.clear_allowed_writers().await?;
    assert_eq!(doc1.get_allowed_writers().await?, None);
    sync_with(&doc1, &peer0).await?;
    assert!(has(&doc1, other, b"o1").await?);
    assert!(has(&doc1, other, b"o2").await?);
    doc1.set_bytes(other, b"o-local".to_vec(), b"v".to_vec())
        .await?;

    node0.shutdown().await?;
    node1.shutdown().await?;
    Ok(())
}

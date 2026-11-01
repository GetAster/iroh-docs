//! Namespace introspection queries (fingerprint, insertion sequence, replay)
//! over the public docs API.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::Result;
use iroh::{endpoint::presets, Endpoint, SecretKey};
use iroh_docs::sync::{Clock, ClockUnavailable};
use tracing_test::traced_test;

mod util;
use util::Node;

/// Deterministic timestamp source: local records are stamped from an
/// explicitly advanced counter, so back-to-back puts never collide.
#[derive(Debug, Default)]
struct TestClock(AtomicU64);

impl TestClock {
    fn advance(&self) {
        self.0.fetch_add(1_000, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now(&self) -> Result<u64, ClockUnavailable> {
        Ok(self.0.load(Ordering::SeqCst))
    }

    fn observe(&self, _remote: u64) -> bool {
        true
    }
}

async fn spawn_node(seed: u8, clock: Arc<TestClock>) -> Result<Node> {
    let ep = Endpoint::builder(presets::Minimal)
        .secret_key(SecretKey::from_bytes(&[seed; 32]))
        .bind()
        .await?;
    Node::memory(ep).clock(clock).spawn().await
}

#[tokio::test]
#[traced_test]
async fn fingerprint_sequence_and_replay_over_the_api() -> Result<()> {
    let clock = Arc::new(TestClock::default());
    let node = spawn_node(1, clock.clone()).await?;
    let docs = node.client().docs().clone();
    let author = docs.author_default().await?;
    let doc = docs.create().await?;

    // A fresh document carries the empty-set fingerprint and sequence 0.
    let empty: [u8; 32] = *blake3::hash(&[]).as_bytes();
    assert_eq!(doc.namespace_fingerprint().await?, empty);
    assert_eq!(doc.namespace_sequence().await?, 0);
    assert!(doc.entries_since(0, 0).await?.is_empty());

    // Puts bump the sequence and move the fingerprint.
    clock.advance();
    doc.set_bytes(author, b"a".to_vec(), b"1".to_vec()).await?;
    let fp1 = doc.namespace_fingerprint().await?;
    assert_ne!(fp1, empty);
    assert_eq!(doc.namespace_sequence().await?, 1);

    clock.advance();
    doc.set_bytes(author, b"b".to_vec(), b"2".to_vec()).await?;
    assert_eq!(doc.namespace_sequence().await?, 2);
    let fp2 = doc.namespace_fingerprint().await?;
    assert_ne!(fp2, fp1);

    // An overwrite is a state-changing put.
    clock.advance();
    doc.set_bytes(author, b"a".to_vec(), b"3".to_vec()).await?;
    assert_eq!(doc.namespace_sequence().await?, 3);

    // Replay since seq 1: the b touch at 2 and the a touch at 3, each with
    // its latest entry, ordered by touch sequence.
    let entries = doc.entries_since(1, 0).await?;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].0, 2);
    assert_eq!(entries[0].1.key(), b"b");
    assert_eq!(entries[1].0, 3);
    assert_eq!(entries[1].1.key(), b"a");
    assert_eq!(entries[1].1.content_hash(), iroh_blobs::Hash::new(b"3"));

    // Replay since the latest sequence is empty.
    assert!(doc.entries_since(3, 0).await?.is_empty());

    // A delete is a state-changing put too. It leaves a tombstone row, so
    // the replay emits it with the empty-content hash.
    let fp_before_delete = doc.namespace_fingerprint().await?;
    clock.advance();
    doc.del(author, b"b".to_vec()).await?;
    assert_eq!(doc.namespace_sequence().await?, 4);
    assert_ne!(doc.namespace_fingerprint().await?, fp_before_delete);
    let tombstones = doc.entries_since(3, 0).await?;
    assert_eq!(tombstones.len(), 1);
    assert_eq!(tombstones[0].0, 4);
    assert_eq!(tombstones[0].1.key(), b"b");
    assert_eq!(tombstones[0].1.content_hash(), iroh_blobs::Hash::new([]));

    // Re-setting the key supersedes the tombstone: the replay resolves the
    // key to its latest row.
    clock.advance();
    doc.set_bytes(author, b"b".to_vec(), b"2".to_vec()).await?;
    assert_eq!(doc.namespace_sequence().await?, 5);
    assert_ne!(doc.namespace_fingerprint().await?, fp_before_delete);
    let replayed = doc.entries_since(3, 0).await?;
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].0, 5);
    assert_eq!(replayed[0].1.content_hash(), iroh_blobs::Hash::new(b"2"));
    assert!(doc.get_exact(author, b"b", false).await?.is_some());

    Ok(())
}

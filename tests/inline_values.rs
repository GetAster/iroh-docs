//! Inline values: entries at or below the inline threshold carry their value
//! inside gossip and sync messages, so the receiver's content is complete
//! without a value-blob download round trip.
//!
//! The download is impossible in these tests by construction: the author
//! node's router does not accept the blobs ALPN, so any download attempt by
//! the receiver fails. Content that ends up complete on the receiver can
//! therefore only have arrived inlined in a docs message.

use anyhow::{bail, Result};
use iroh::endpoint::presets;
use iroh_blobs::api::blobs::BlobStatus;
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    engine::LiveEvent,
    store::DownloadPolicy,
};
use n0_future::TryStreamExt;
use tracing_test::traced_test;
use util::Node;

mod util;

/// `iroh_docs::protocol::DEFAULT_MAX_INLINE_VALUE_SIZE`.
const MAX_INLINE: usize = 1024;

async fn node(accept_blobs: bool) -> Result<Node> {
    let ep = iroh::Endpoint::bind(presets::Minimal).await?;
    let builder = Node::memory(ep);
    let builder = if accept_blobs {
        builder
    } else {
        builder.without_blobs_protocol()
    };
    builder.spawn().await
}

/// A small value rides the gossip message: the receiver gets complete content
/// with no download, even under a download policy that forbids downloads.
#[tokio::test]
#[traced_test]
async fn gossip_inlines_small_value() -> Result<()> {
    let author = node(false).await?;
    let reader = node(true).await?;
    let doc0 = author.client().docs().create().await?;
    let author_id = author.client().docs().author_create().await?;
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;

    let doc1 = reader.client().docs().import(ticket).await?;
    doc1.set_download_policy(DownloadPolicy::NothingExcept(vec![]))
        .await?;
    let mut events1 = doc1.subscribe().await?;

    // Let the (empty) join sync finish so the write below flows via gossip.
    loop {
        match events1.try_next().await? {
            Some(LiveEvent::SyncFinished(event)) => {
                if let Err(err) = event.result {
                    bail!("join sync failed: {err}");
                }
                break;
            }
            Some(_) => {}
            None => bail!("event stream closed before the join sync finished"),
        }
    }

    let small: Vec<u8> = vec![0xAB; 512];
    let hash_small = doc0
        .set_bytes(author_id, b"small".to_vec(), small.clone())
        .await?;

    // The inline value arrives as a complete insert; nothing is downloaded.
    loop {
        match events1.try_next().await? {
            Some(LiveEvent::InsertRemote {
                entry,
                content_status,
                ..
            }) if entry.content_hash() == hash_small => {
                assert_eq!(content_status, iroh_docs::ContentStatus::Complete);
                break;
            }
            Some(_) => {}
            None => bail!("event stream closed before InsertRemote"),
        }
    }

    let blobs1 = reader.client().blobs();
    assert!(matches!(
        blobs1.status(hash_small).await?,
        BlobStatus::Complete { size } if size == 512
    ));
    assert_eq!(blobs1.get_bytes(hash_small).await?, small);

    author.shutdown().await?;
    reader.shutdown().await?;
    Ok(())
}

/// The join sync inlines a value exactly at the threshold; one byte over it
/// does not ride the message, and its download fails (author refuses blobs),
/// so it stays incomplete.
#[tokio::test]
#[traced_test]
async fn sync_exchange_inlines_only_below_threshold() -> Result<()> {
    let author = node(false).await?;
    let reader = node(true).await?;
    let doc0 = author.client().docs().create().await?;
    let author_id = author.client().docs().author_create().await?;

    let at_threshold: Vec<u8> = vec![0x42; MAX_INLINE];
    let over_threshold: Vec<u8> = vec![0x43; MAX_INLINE + 1];
    let hash_at = doc0
        .set_bytes(author_id, b"at".to_vec(), at_threshold.clone())
        .await?;
    let hash_over = doc0
        .set_bytes(author_id, b"over".to_vec(), over_threshold.clone())
        .await?;

    // Share only after the writes: the entries travel via the join sync.
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;
    let doc1 = reader.client().docs().import(ticket).await?;
    let mut events1 = doc1.subscribe().await?;

    let mut seen_at = None;
    let mut seen_over = None;
    loop {
        match events1.try_next().await? {
            Some(LiveEvent::InsertRemote {
                entry,
                content_status,
                ..
            }) => {
                if entry.content_hash() == hash_at {
                    seen_at = Some(content_status);
                }
                if entry.content_hash() == hash_over {
                    seen_over = Some(content_status);
                }
                if seen_at.is_some() && seen_over.is_some() {
                    break;
                }
            }
            Some(_) => {}
            None => bail!("event stream closed before both InsertRemotes"),
        }
    }

    // The at-threshold value arrived inline: reported complete, bytes match.
    assert_eq!(seen_at, Some(iroh_docs::ContentStatus::Complete));
    let blobs1 = reader.client().blobs();
    assert!(matches!(
        blobs1.status(hash_at).await?,
        BlobStatus::Complete { size } if size == MAX_INLINE as u64
    ));
    assert_eq!(blobs1.get_bytes(hash_at).await?, at_threshold);

    // The over-threshold value did not ride the message. Its download may
    // have been attempted (and will fail: the author refuses blobs), so the
    // reported status is Missing or Incomplete — never Complete.
    assert_ne!(seen_over, Some(iroh_docs::ContentStatus::Complete));
    assert!(!matches!(
        blobs1.status(hash_over).await?,
        BlobStatus::Complete { .. }
    ));

    author.shutdown().await?;
    reader.shutdown().await?;
    Ok(())
}

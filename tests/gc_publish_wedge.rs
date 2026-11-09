//! Witness for the publish/GC wedge (ledger row 51): a GC sweep can collect
//! a freshly published blob when its enumeration ran before the document
//! record naming the blob was inserted, its tag traversal ran after the
//! publish's temp tag was dropped, and its candidate list (read at sweep
//! time) contains the blob. The publish paths now hold the temp tag until a
//! protect enumeration has observed the record, which closes the window:
//! the sweep whose enumeration missed the record still sees the tag, and
//! the sweep whose enumeration saw the record is protected by the live set.
//!
//! The assertions are deterministic, not a timing repro, and witness both
//! halves of the fix: the tag must still exist after the publish returns
//! (it is held), a sweep whose live set was enumerated before the publish
//! must not collect the blob (only the held tag keeps it alive), and the
//! tag must survive the first enumeration after the publish, releasing only
//! when the next one retires its batch (two-phase rotation: every tag
//! survives at least one full enumeration cycle).
#![cfg(feature = "fs-store")]

use std::{collections::HashSet, time::Duration};

use iroh::endpoint::presets;
use iroh_blobs::store::GcConfig;
use iroh_docs::{engine::ProtectCallbackHandler, protocol::Docs};
use iroh_gossip::net::Gossip;
use n0_error::Result;
use n0_future::StreamExt;
use testdir::testdir;

async fn temp_hashes(store: &iroh_blobs::api::Store) -> Result<HashSet<iroh_blobs::Hash>> {
    let mut temps = store.tags().list_temp_tags().await?;
    let mut hashes = HashSet::new();
    while let Some(hf) = temps.next().await {
        hashes.insert(hf.hash);
    }
    Ok(hashes)
}

#[tokio::test]
#[cfg(feature = "fs-store")]
async fn blob_published_before_a_sweep_enumerates_survives_it() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let dir = testdir!();
    let endpoint = iroh::Endpoint::bind(presets::Minimal).await?;

    let (handler, cb) = ProtectCallbackHandler::new();
    let mut opts = iroh_blobs::store::fs::options::Options::new(&dir.join("blobs"));
    opts.gc = Some(GcConfig {
        interval: Duration::from_secs(3600),
        add_protected: Some(cb.clone()),
    });
    let fs = iroh_blobs::store::fs::FsStore::load_with_opts(dir.join("blobs.db"), opts).await?;
    let blobs = (*fs).clone();
    let gossip = Gossip::builder().spawn(endpoint.clone());
    tokio::fs::create_dir_all(dir.join("docs")).await?;
    let docs = Docs::persistent(dir.join("docs"))
        .protect_handler(handler)
        .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
        .await?;
    let api = docs.api().clone();
    let doc = api.create().await?;
    let author = api.author_create().await?;

    let plain = doc.set_bytes(author, "plain", "plain-value").await?;
    let durable = doc
        .set_bytes_durable(author, "durable", "durable-value")
        .await?;

    // Give an unfixed tree time to flush the tag drops it performs at
    // publish return; a fixed tree still holds both tags here.
    n0_future::time::sleep(Duration::from_millis(250)).await;
    let held = temp_hashes(&blobs).await?;
    assert!(
        held.contains(&plain),
        "the publish temp tag must be held until an enumeration observes the record"
    );
    assert!(
        held.contains(&durable),
        "the durable publish temp tag must be held until an enumeration observes the record"
    );

    // The wedge itself, deterministically: the sweep below runs with the
    // live set from an enumeration taken before the publish, so only the
    // held temp tag keeps the blob alive. A tree that drops the tag at
    // publish return (the original wedge) loses both blobs here.
    let mut stale = HashSet::new();
    iroh_blobs::store::gc_run_once(&blobs, &mut stale, None).await?;
    assert!(
        blobs.has(plain).await?,
        "a sweep with a stale live set collected the plain blob"
    );
    assert!(
        blobs.has(durable).await?,
        "a sweep with a stale live set collected the durable blob"
    );

    // Enumeration #1 sees both records: the tags move to the retiring
    // batch and are still held — every tag survives at least one full
    // enumeration cycle after the publish.
    let mut live = HashSet::new();
    (cb)(&mut live).await;
    assert!(
        live.contains(&plain) && live.contains(&durable),
        "the protect enumeration must see the published records"
    );
    let still_held = temp_hashes(&blobs).await?;
    assert!(
        still_held.contains(&plain),
        "the publish temp tag must survive the first enumeration after the publish"
    );

    // Enumeration #2 retires the batch: the tags release.
    let mut live = HashSet::new();
    (cb)(&mut live).await;
    let released = n0_future::time::timeout(Duration::from_secs(10), async {
        loop {
            if !temp_hashes(&blobs)
                .await
                .unwrap()
                .iter()
                .any(|h| *h == plain || *h == durable)
            {
                break;
            }
            n0_future::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        released.is_ok(),
        "the held tags must release after the enumeration that follows the publish"
    );

    // A sweep with the live set that observed the records leaves the
    // published state intact.
    iroh_blobs::store::gc_run_once(&blobs, &mut live, None).await?;
    assert!(
        blobs.has(plain).await?,
        "the sweep collected the plain blob"
    );
    assert!(
        blobs.has(durable).await?,
        "the sweep collected the durable blob"
    );
    assert!(doc.get_exact(author, "plain", false).await?.is_some());
    assert!(doc.get_exact(author, "durable", false).await?.is_some());
    Ok(())
}

/// Without a protect handler there is no enumeration to ever release a
/// held temp tag, so the publish paths must not hold one at all: the tags
/// drop when the publish returns and the blobs stay in the store (GC is
/// off by default, nothing collects them).
#[tokio::test]
#[cfg(feature = "fs-store")]
async fn without_a_protect_handler_publish_tags_are_not_held() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let dir = testdir!();
    let endpoint = iroh::Endpoint::bind(presets::Minimal).await?;

    // No GcConfig and no protect handler, mirroring the default config.
    let opts = iroh_blobs::store::fs::options::Options::new(&dir.join("blobs"));
    let fs = iroh_blobs::store::fs::FsStore::load_with_opts(dir.join("blobs.db"), opts).await?;
    let blobs = (*fs).clone();
    let gossip = Gossip::builder().spawn(endpoint.clone());
    tokio::fs::create_dir_all(dir.join("docs")).await?;
    let docs = Docs::persistent(dir.join("docs"))
        .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
        .await?;
    let api = docs.api().clone();
    let doc = api.create().await?;
    let author = api.author_create().await?;

    let plain = doc.set_bytes(author, "plain", "plain-value").await?;
    let durable = doc
        .set_bytes_durable(author, "durable", "durable-value")
        .await?;

    // Give an eager implementation time to drop any tag it holds; none may
    // remain: there is no enumeration to release a held tag, so holding
    // would leak it forever.
    n0_future::time::sleep(Duration::from_millis(250)).await;
    let held = temp_hashes(&blobs).await?;
    assert!(
        !held.contains(&plain) && !held.contains(&durable),
        "publish temp tags must not be held without a protect handler"
    );

    // The blobs themselves stay: GC is off by default.
    assert!(blobs.has(plain).await?);
    assert!(blobs.has(durable).await?);
    assert!(doc.get_exact(author, "plain", false).await?.is_some());
    assert!(doc.get_exact(author, "durable", false).await?.is_some());
    Ok(())
}

/// A durable batch that fails partway keeps the entries it inserted before
/// the failure; their blobs must stay protected like a successful batch's.
/// The second entry is empty, so the insert fails after the first one.
#[tokio::test]
#[cfg(feature = "fs-store")]
async fn a_failed_durable_batch_keeps_its_inserted_prefix_protected() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    let dir = testdir!();
    let endpoint = iroh::Endpoint::bind(presets::Minimal).await?;

    let (handler, cb) = ProtectCallbackHandler::new();
    let mut opts = iroh_blobs::store::fs::options::Options::new(&dir.join("blobs"));
    opts.gc = Some(GcConfig {
        interval: Duration::from_secs(3600),
        add_protected: Some(cb.clone()),
    });
    let fs = iroh_blobs::store::fs::FsStore::load_with_opts(dir.join("blobs.db"), opts).await?;
    let blobs = (*fs).clone();
    let gossip = Gossip::builder().spawn(endpoint.clone());
    tokio::fs::create_dir_all(dir.join("docs")).await?;
    let docs = Docs::persistent(dir.join("docs"))
        .protect_handler(handler)
        .spawn(endpoint.clone(), blobs.clone(), gossip.clone())
        .await?;
    let api = docs.api().clone();
    let doc = api.create().await?;
    let author = api.author_create().await?;

    let entries = vec![
        (bytes::Bytes::from("kept"), bytes::Bytes::from("kept-value")),
        (bytes::Bytes::from("empty"), bytes::Bytes::new()),
    ];
    assert!(doc.set_bytes_many_durable(author, entries).await.is_err());
    let kept = iroh_blobs::Hash::new(b"kept-value");
    assert!(
        doc.get_exact(author, "kept", false).await?.is_some(),
        "the entry inserted before the failure stays in the document"
    );

    n0_future::time::sleep(Duration::from_millis(250)).await;
    assert!(
        temp_hashes(&blobs).await?.contains(&kept),
        "the inserted prefix's temp tag must be held after a failed batch"
    );
    let mut stale = HashSet::new();
    iroh_blobs::store::gc_run_once(&blobs, &mut stale, None).await?;
    assert!(
        blobs.has(kept).await?,
        "a sweep with a stale live set collected the inserted prefix's blob"
    );
    Ok(())
}

//! Witness for the publish/GC wedge (ledger row 51): a GC sweep can collect
//! a freshly published blob when its enumeration ran before the document
//! record naming the blob was inserted, its tag traversal ran after the
//! publish's temp tag was dropped, and its candidate list (read at sweep
//! time) contains the blob. The publish paths now hold the temp tag until a
//! protect enumeration has observed the record, which closes the window:
//! the sweep whose enumeration missed the record still sees the tag, and
//! the sweep whose enumeration saw the record is protected by the live set.
//!
//! The assertions are deterministic, not a timing repro: the tag must still
//! exist after the publish returns (it is held), and must be released once
//! an enumeration has run — the two halves of the fix.
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

    // One full GC run, exactly as `run_gc` drives it: the protect
    // enumeration first, then the mark and sweep with that live set.
    let mut live = HashSet::new();
    (cb)(&mut live).await;
    iroh_blobs::store::gc_run_once(&blobs, &mut live).await?;

    // The enumeration observed both records: the held tags release.
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
        "the held tags must release after the enumeration"
    );

    // The sweep must not have collected either blob, and a second sweep
    // (enumeration + mark + sweep) must leave the published state intact.
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

    let mut live = HashSet::new();
    (cb)(&mut live).await;
    iroh_blobs::store::gc_run_once(&blobs, &mut live).await?;
    assert!(blobs.has(plain).await?);
    assert!(blobs.has(durable).await?);
    Ok(())
}

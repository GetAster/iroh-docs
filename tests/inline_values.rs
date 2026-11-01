//! Inline values: entries at or below the inline threshold carry their value
//! inside gossip and sync messages, so the receiver's content is complete
//! without a value-blob download round trip.
//!
//! The download is impossible in these tests by construction: the author
//! node's router does not accept the blobs ALPN, so any download attempt by
//! the receiver fails. Content that ends up complete on the receiver can
//! therefore only have arrived inlined in a docs message.

use anyhow::{bail, Result};
use iroh::{endpoint::presets, EndpointId};
use iroh_blobs::{api::blobs::BlobStatus, Hash};
use iroh_docs::{
    api::protocol::{AddrInfoOptions, ShareMode},
    engine::{LiveEvent, Op},
    store::DownloadPolicy,
    AuthorId, NamespaceId,
};
use iroh_gossip::{api::GossipTopic, TopicId};
use n0_future::{Stream, TryStreamExt};
use std::time::Duration;
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
    wait_sync_finished(&mut events1).await?;

    let small: Vec<u8> = vec![0xAB; 512];
    let hash_small = doc0
        .set_bytes(author_id, b"small".to_vec(), small.clone())
        .await?;

    // The entry and the inline value arrive on different gossip topics in
    // either order, and the events ride different channels: wait for both
    // InsertRemote and ContentReady, without assuming an order.
    let mut saw_insert = false;
    let mut saw_ready = false;
    loop {
        match events1.try_next().await? {
            Some(LiveEvent::InsertRemote { entry, .. }) if entry.content_hash() == hash_small => {
                saw_insert = true;
            }
            Some(LiveEvent::ContentReady { hash }) if hash == hash_small => saw_ready = true,
            Some(_) => {}
            None => bail!("event stream closed before the content was ready"),
        }
        if saw_insert && saw_ready {
            break;
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

// ---- forged-message tests ------------------------------------------------

/// The derived topic a namespace's inline values ride on; mirrors the
/// fork's `values_topic` derivation, a frozen protocol constant (the hash
/// of the namespace id with this suffix), so it must never change.
const VALUES_TOPIC_SUFFIX: &[u8] = b"|inline-v1";

fn values_topic(namespace: NamespaceId) -> TopicId {
    let mut bytes = [0u8; 32 + VALUES_TOPIC_SUFFIX.len()];
    bytes[..32].copy_from_slice(namespace.as_ref());
    bytes[32..].copy_from_slice(VALUES_TOPIC_SUFFIX);
    TopicId::from_bytes(*Hash::new(bytes).as_bytes())
}

/// Waits for the join sync that follows a ticket import to finish.
async fn wait_sync_finished(
    events: &mut (impl Stream<Item = Result<LiveEvent>> + Unpin),
) -> Result<()> {
    loop {
        match events.try_next().await? {
            Some(LiveEvent::SyncFinished(event)) => {
                if let Err(err) = event.result {
                    bail!("join sync failed: {err}");
                }
                return Ok(());
            }
            Some(_) => {}
            None => bail!("event stream closed before the join sync finished"),
        }
    }
}

/// Waits until `topic`'s direct neighbors include `peer`.
///
/// A topic's neighbor set only updates while its event stream is polled:
/// each subscription tracks the `NeighborUp` events it has consumed, so this
/// drains events as it waits.
async fn wait_for_neighbor(
    topic: &mut GossipTopic,
    peer: EndpointId,
    limit: Duration,
) -> Result<()> {
    n0_future::time::timeout(limit, async {
        loop {
            if topic.neighbors().any(|neighbor| neighbor == peer) {
                return Ok(());
            }
            // Consuming any event updates the neighbor set.
            match n0_future::time::timeout(Duration::from_millis(100), topic.try_next()).await {
                Ok(Ok(Some(_))) => {}
                Ok(Ok(None)) => bail!("topic stream closed"),
                Ok(Err(err)) => bail!("topic stream errored: {err}"),
                Err(_) => {}
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("{peer} never became a direct gossip neighbor"))?
}

/// Waits until `hash` has settled at not-complete: either a failed download
/// left a partial entry, or the hash is entirely absent. Used where a
/// download cannot succeed by construction (the author refuses blobs).
async fn wait_not_complete(store: &iroh_blobs::api::Store, hash: Hash) -> Result<()> {
    n0_future::time::timeout(Duration::from_secs(30), async {
        loop {
            match store.status(hash).await {
                Ok(status) if !matches!(status, BlobStatus::Complete { .. }) => return Ok(()),
                Ok(_) => {}
                Err(_) => return Ok(()),
            }
            n0_future::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("blob {hash} never settled at not-complete"))?
}

/// Waits until the blob store reports the hash as never present. Used where
/// a download cannot succeed by construction (the author refuses blobs), so
/// NotFound is the only stable outcome — a download that is merely slow
/// would still have to settle there.
async fn wait_not_found(store: &iroh_blobs::api::Store, hash: Hash) -> Result<()> {
    let mut last: Option<BlobStatus> = None;
    n0_future::time::timeout(Duration::from_secs(30), async {
        loop {
            match store.status(hash).await {
                Ok(BlobStatus::NotFound) => return Ok(()),
                Ok(status) => last = Some(status),
                Err(_) => {}
            }
            n0_future::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("blob {hash} never settled at NotFound (last status: {last:?})"))?
}

/// Writes a small value on `doc0` and waits for the reader's insert plus
/// content-ready, returning the content hash.
async fn wait_for_inline_write(
    events: &mut (impl Stream<Item = Result<LiveEvent>> + Unpin),
    doc0: &iroh_docs::api::Doc,
    author: AuthorId,
    key: &'static [u8],
    value: Vec<u8>,
) -> Result<Hash> {
    let hash = doc0.set_bytes(author, key.to_vec(), value).await?;
    // InsertRemote and ContentReady ride different channels and either may
    // be observed first — a cached inline value completes before the
    // InsertRemote event has run its content-status lookup.
    let mut saw_insert = false;
    let mut saw_ready = false;
    loop {
        match n0_future::time::timeout(Duration::from_secs(30), events.try_next()).await {
            Ok(Ok(Some(LiveEvent::InsertRemote { entry, .. }))) if entry.content_hash() == hash => {
                saw_insert = true;
            }
            Ok(Ok(Some(LiveEvent::ContentReady { hash: ready }))) if ready == hash => {
                saw_ready = true;
            }
            Ok(Ok(Some(_))) => {}
            other => bail!("the legitimate write never became ready: {other:?}"),
        }
        if saw_insert && saw_ready {
            return Ok(hash);
        }
    }
}

/// A forged `Op::Put` carrying an entry from a foreign namespace, a forged
/// `Op::PutValue` for its hash, and one undecodable frame all leave the
/// reader's store and event stream untouched, and live gossip for the
/// namespace survives: a legitimate write still arrives afterwards.
#[tokio::test]
#[traced_test]
async fn forged_gossip_ops_write_nothing_and_gossip_survives() -> Result<()> {
    let author = node(false).await?;
    let reader = node(true).await?;
    let attacker = node(true).await?;
    let doc0 = author.client().docs().create().await?;
    let author_id = author.client().docs().author_create().await?;
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;

    let doc1 = reader.client().docs().import(ticket.clone()).await?;
    let mut events1 = doc1.subscribe().await?;
    wait_sync_finished(&mut events1).await?;

    // The attacker joins the swarm through its own import...
    let doc_atk = attacker.client().docs().import(ticket).await?;
    let mut events_atk = doc_atk.subscribe().await?;
    wait_sync_finished(&mut events_atk).await?;

    // ...and forges with an entry that is fully valid in a *different*
    // namespace: it decodes, its signatures are real, and validation must
    // refuse it on the namespace check alone.
    let evil_namespace = iroh_docs::NamespaceSecret::new(&mut rand::rng());
    let evil_author = iroh_docs::Author::new(&mut rand::rng());
    // Non-empty content: an empty record hashes to the empty blob, which the
    // mem store always reports as Complete { size: 0 }, so NotFound could
    // never be observed for it even with no write at all.
    let forged_content = b"forged content";
    let evil_signed = iroh_docs::SignedEntry::from_parts(
        &evil_namespace,
        &evil_author,
        b"evil",
        iroh_docs::Record::new(Hash::new(forged_content), forged_content.len() as u64, 0),
    );
    let evil_hash = evil_signed.content_hash();

    let topic: TopicId = doc1.id().into();
    let mut sub = attacker.gossip().subscribe_and_join(topic, vec![]).await?;
    wait_for_neighbor(&mut sub, reader.id(), Duration::from_secs(30)).await?;
    sub.broadcast(postcard::to_stdvec(&Op::Put { entry: evil_signed })?.into())
        .await?;
    // One undecodable frame: skipped with a warn, never fatal to the loop.
    sub.broadcast(b"not a gossip op".to_vec().into()).await?;

    let mut values = attacker
        .gossip()
        .subscribe_and_join(values_topic(doc1.id()), vec![])
        .await?;
    wait_for_neighbor(&mut values, reader.id(), Duration::from_secs(30)).await?;
    values
        .broadcast(
            postcard::to_stdvec(&Op::PutValue {
                hash: evil_hash,
                value: b"evil-value".to_vec().into(),
            })?
            .into(),
        )
        .await?;

    // Give the forged messages time to arrive and be processed, then drain
    // the event stream: nothing for the forged hash may have happened.
    n0_future::time::sleep(Duration::from_secs(2)).await;
    let mut drained = Vec::new();
    while let Ok(Ok(Some(event))) =
        n0_future::time::timeout(Duration::from_millis(500), events1.try_next()).await
    {
        drained.push(event);
    }
    assert!(
        !drained.iter().any(
            |event| matches!(event, LiveEvent::InsertRemote { entry, .. }
                if entry.content_hash() == evil_hash)
        ),
        "a forged entry from a foreign namespace was inserted"
    );
    assert!(
        !drained
            .iter()
            .any(|event| matches!(event, LiveEvent::ContentReady { hash } if *hash == evil_hash)),
        "a forged value was announced as ready"
    );
    wait_not_found(reader.client().blobs(), evil_hash).await?;
    assert!(
        doc1.get_exact(evil_author.id(), b"evil".to_vec(), false)
            .await?
            .is_none(),
        "the forged entry must not be in the replica"
    );

    // Live gossip for the namespace survived the forgeries: a legitimate
    // write still reaches the reader complete, with no download.
    let legit: Vec<u8> = vec![0xCD; 512];
    let legit_hash =
        wait_for_inline_write(&mut events1, &doc0, author_id, b"legit", legit.clone()).await?;
    assert_eq!(reader.client().blobs().get_bytes(legit_hash).await?, legit);

    author.shutdown().await?;
    reader.shutdown().await?;
    attacker.shutdown().await?;
    Ok(())
}

/// A `PutValue` for a hash no entry references is dropped by the wanted
/// gate before any verification or write: the blob store stays clean.
#[tokio::test]
#[traced_test]
async fn put_value_for_an_unknown_hash_writes_no_blob() -> Result<()> {
    let author = node(false).await?;
    let reader = node(true).await?;
    let attacker = node(true).await?;
    let doc0 = author.client().docs().create().await?;
    let author_id = author.client().docs().author_create().await?;
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;

    let doc1 = reader.client().docs().import(ticket.clone()).await?;
    let mut events1 = doc1.subscribe().await?;
    wait_sync_finished(&mut events1).await?;
    let doc_atk = attacker.client().docs().import(ticket).await?;
    let mut events_atk = doc_atk.subscribe().await?;
    wait_sync_finished(&mut events_atk).await?;

    let unknown = Hash::new(b"no entry references this hash");
    let mut values = attacker
        .gossip()
        .subscribe_and_join(values_topic(doc1.id()), vec![])
        .await?;
    wait_for_neighbor(&mut values, reader.id(), Duration::from_secs(30)).await?;
    values
        .broadcast(
            postcard::to_stdvec(&Op::PutValue {
                hash: unknown,
                value: b"junk".to_vec().into(),
            })?
            .into(),
        )
        .await?;

    // The wanted gate must have dropped the orphan value: no blob appears.
    wait_not_found(reader.client().blobs(), unknown).await?;

    // Gossip is still alive: a legitimate write still arrives.
    let legit: Vec<u8> = vec![0xDE; 512];
    let legit_hash =
        wait_for_inline_write(&mut events1, &doc0, author_id, b"legit", legit.clone()).await?;
    assert_eq!(reader.client().blobs().get_bytes(legit_hash).await?, legit);

    author.shutdown().await?;
    reader.shutdown().await?;
    attacker.shutdown().await?;
    Ok(())
}

/// A `PutValue` whose bytes don't hash to the announced hash is rejected
/// (no blob written); the correct value is then imported and announced.
/// The author runs with inline values off and refuses blobs, so the
/// reader's content is genuinely missing — the values topic is its only
/// possible source.
#[tokio::test]
#[traced_test]
async fn put_value_hash_mismatch_is_rejected_and_the_correct_value_imports() -> Result<()> {
    let author = Node::memory(iroh::Endpoint::bind(presets::Minimal).await?)
        .without_blobs_protocol()
        .max_inline_value_size(0)
        .spawn()
        .await?;
    let reader = node(true).await?;
    let attacker = node(true).await?;
    let doc0 = author.client().docs().create().await?;
    let author_id = author.client().docs().author_create().await?;
    let small: Vec<u8> = vec![0x5A; 512];
    let hash = doc0
        .set_bytes(author_id, b"k".to_vec(), small.clone())
        .await?;
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;

    let doc1 = reader.client().docs().import(ticket.clone()).await?;
    let mut events1 = doc1.subscribe().await?;
    wait_sync_finished(&mut events1).await?;
    // The join sync delivered the entry but no inline value (the author
    // has inline values off), and its download cannot succeed (the author
    // refuses blobs): the content settles below complete.
    wait_not_complete(reader.client().blobs(), hash).await?;

    let doc_atk = attacker.client().docs().import(ticket).await?;
    let mut events_atk = doc_atk.subscribe().await?;
    wait_sync_finished(&mut events_atk).await?;

    // The values topic has no member with a bootstrap address (the author
    // is not on it), so the attacker waits until the namespace topic has
    // connected it to the reader directly, then bootstraps the values
    // topic from the reader's id.
    let mut sub = attacker
        .gossip()
        .subscribe_and_join(doc1.id().into(), vec![])
        .await?;
    wait_for_neighbor(&mut sub, reader.id(), Duration::from_secs(30)).await?;
    let mut values = attacker
        .gossip()
        .subscribe_and_join(values_topic(doc1.id()), vec![reader.id()])
        .await?;
    wait_for_neighbor(&mut values, reader.id(), Duration::from_secs(30)).await?;

    // Wrong bytes: the hash check rejects them and nothing is written.
    values
        .broadcast(
            postcard::to_stdvec(&Op::PutValue {
                hash,
                value: b"WRONG".to_vec().into(),
            })?
            .into(),
        )
        .await?;
    // The wrong bytes must have been rejected: still no complete blob.
    wait_not_complete(reader.client().blobs(), hash).await?;

    // The right bytes import and are announced.
    values
        .broadcast(
            postcard::to_stdvec(&Op::PutValue {
                hash,
                value: small.clone().into(),
            })?
            .into(),
        )
        .await?;
    loop {
        match n0_future::time::timeout(Duration::from_secs(30), events1.try_next()).await {
            Ok(Ok(Some(LiveEvent::ContentReady { hash: ready }))) if ready == hash => break,
            Ok(Ok(Some(_))) => {}
            other => bail!("the corrected value never became ready: {other:?}"),
        }
    }
    assert_eq!(reader.client().blobs().get_bytes(hash).await?, small);

    author.shutdown().await?;
    reader.shutdown().await?;
    attacker.shutdown().await?;
    Ok(())
}

/// A peer that speaks only the version-0 ALPN: the dialer tries
/// `ALPN_INLINE` first and falls back, so a mixed-version pair still
/// syncs on the frozen wire and the values arrive by download.
#[tokio::test]
#[traced_test]
async fn sync_falls_back_to_a_peer_that_speaks_only_the_legacy_alpn() -> Result<()> {
    // Legacy node: its router accepts only the version-0 docs ALPN.
    let ep = iroh::Endpoint::bind(presets::Minimal).await?;
    let blobs = (*iroh_blobs::store::mem::MemStore::new()).clone();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let legacy_docs = iroh_docs::protocol::Docs::memory()
        .spawn(ep.clone(), blobs.clone(), gossip.clone())
        .await?;
    let router = iroh::protocol::Router::builder(ep)
        .accept(
            iroh_blobs::ALPN,
            iroh_blobs::BlobsProtocol::new(&blobs, None),
        )
        .accept(iroh_gossip::ALPN, gossip)
        .accept(iroh_docs::ALPN, legacy_docs.clone())
        .spawn();

    // The legacy node authors one value below and one above the inline
    // threshold; on the legacy wire neither rides the sync message.
    let legacy_api = legacy_docs.api();
    let doc0 = legacy_api.create().await?;
    let legacy_author = legacy_api.author_create().await?;
    let small: Vec<u8> = vec![0x11; 512];
    let large: Vec<u8> = vec![0x22; 2048];
    let hash_small = doc0
        .set_bytes(legacy_author, b"small".to_vec(), small.clone())
        .await?;
    let hash_large = doc0
        .set_bytes(legacy_author, b"large".to_vec(), large.clone())
        .await?;
    let ticket = doc0
        .share(ShareMode::Write, AddrInfoOptions::RelayAndAddresses)
        .await?;

    // A current node imports the ticket: its dialer tries ALPN_INLINE
    // first, is refused, and must fall back to the legacy wire.
    let reader = node(true).await?;
    let doc1 = reader.client().docs().import(ticket).await?;
    let mut events1 = doc1.subscribe().await?;

    // The join sync delivers both entries. Their content completes by
    // download (the legacy wire inlines nothing), and the small value can
    // become ready before the SyncFinished event fires — so this loop must
    // not wait for SyncFinished first.
    let mut got_small = false;
    let mut got_large = false;
    loop {
        match n0_future::time::timeout(Duration::from_secs(30), events1.try_next()).await {
            Ok(Ok(Some(LiveEvent::ContentReady { hash }))) => {
                if hash == hash_small {
                    got_small = true;
                }
                if hash == hash_large {
                    got_large = true;
                }
                if got_small && got_large {
                    break;
                }
            }
            Ok(Ok(Some(LiveEvent::SyncFinished(event)))) => {
                if let Err(err) = event.result {
                    bail!("join sync failed: {err}");
                }
            }
            Ok(Ok(Some(_))) => {}
            other => bail!("content never became ready: {other:?}"),
        }
    }
    let blobs1 = reader.client().blobs();
    assert_eq!(blobs1.get_bytes(hash_small).await?, small);
    assert_eq!(blobs1.get_bytes(hash_large).await?, large);

    router.shutdown().await?;
    reader.shutdown().await?;
    Ok(())
}

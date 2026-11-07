//! Crash witness for [`Doc::set_bytes_many_durable`]: once the call returns,
//! a hard kill can never lose a record of the batch, and every record names
//! blob metadata the blob store committed. The child writes one batch and is
//! killed the moment the call reports success; the parent reopens the stores
//! and demands every value and every record.
#![cfg(feature = "fs-store")]

use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};

use bytes::Bytes;
use n0_error::Result;

mod util;

/// Enough entries that a per-entry commit would be visible, with values on
/// both sides of the blob store's inline threshold.
const ENTRIES: usize = 64;
const READY_PREFIX: &str = "DURABLE_MANY_READY ";

fn value(i: usize) -> Vec<u8> {
    let len = if i.is_multiple_of(8) { 64 * 1024 } else { 256 };
    vec![i as u8; len]
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "child role for the crash witness; spawned by the parent test"]
async fn crash_child_batch_writer() -> Result<()> {
    let dir = std::env::var("DURABLE_MANY_DIR").expect("parent sets DURABLE_MANY_DIR");
    let endpoint = util::empty_endpoint().await?;
    let node = util::Node::persistent(&dir, endpoint)
        .spawn()
        .await
        .unwrap();
    let author = node.docs().author_create().await?;
    let doc = node.docs().create().await?;

    let entries = (0..ENTRIES)
        .map(|i| (Bytes::from(format!("k{i:03}")), Bytes::from(value(i))))
        .collect();
    let hashes = doc.set_bytes_many_durable(author, entries).await?;
    assert_eq!(hashes.len(), ENTRIES);
    let hashes: Vec<String> = hashes.iter().map(|h| h.to_hex().to_string()).collect();

    println!("{READY_PREFIX}{} {} {}", doc.id(), author, hashes.join(","));
    use std::io::Write;
    std::io::stdout().flush().ok();
    std::future::pending::<()>().await;
    unreachable!()
}

#[test]
fn a_hard_kill_after_set_bytes_many_durable_keeps_the_whole_batch() {
    let exe = std::env::current_exe().expect("test exe");
    for round in 0..3 {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut child = Command::new(&exe)
            .args([
                "crash_child_batch_writer",
                "--exact",
                "--ignored",
                "--nocapture",
            ])
            .env("DURABLE_MANY_DIR", dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn child");

        let stdout = child.stdout.take().expect("child stdout");
        let ready = BufReader::new(stdout)
            .lines()
            .map(|line| line.expect("child stdout line"))
            .find_map(|line| line.strip_prefix(READY_PREFIX).map(str::to_string))
            .expect("child printed the READY line before exiting");
        child.kill().expect("SIGKILL child");
        child.wait().expect("reap child");

        let mut fields = ready.split_whitespace();
        let ns: iroh_docs::NamespaceId = fields.next().unwrap().parse().unwrap();
        let author: iroh_docs::AuthorId = fields.next().unwrap().parse().unwrap();
        let hashes: Vec<iroh_blobs::Hash> = fields
            .next()
            .unwrap()
            .split(',')
            .map(|h| h.parse().unwrap())
            .collect();
        assert_eq!(hashes.len(), ENTRIES);

        let dir_path = dir.path().to_path_buf();
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(async move {
                let endpoint = util::empty_endpoint().await.expect("endpoint");
                let node = util::Node::persistent(&dir_path, endpoint)
                    .spawn()
                    .await
                    .expect("reopen node after kill");
                let doc = node
                    .docs()
                    .open(ns)
                    .await
                    .expect("open document")
                    .expect("document exists");
                for (i, hash) in hashes.iter().enumerate() {
                    let entry = doc
                        .get_exact(author, format!("k{i:03}"), false)
                        .await
                        .expect("read record")
                        .unwrap_or_else(|| panic!("round {round}: record k{i:03} lost"));
                    assert_eq!(entry.content_hash(), *hash, "round {round}: k{i:03}");
                    let bytes = node
                        .blobs()
                        .get_bytes(*hash)
                        .await
                        .unwrap_or_else(|e| panic!("round {round}: blob of k{i:03}: {e}"));
                    assert_eq!(
                        bytes.as_ref(),
                        value(i).as_slice(),
                        "round {round}: k{i:03}"
                    );
                }
                node.shutdown().await.ok();
            });
    }
}

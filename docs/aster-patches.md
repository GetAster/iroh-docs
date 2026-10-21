# Aster patch ledger: iroh-docs

This repository is Aster's fork of [n0-computer/iroh-docs](https://github.com/n0-computer/iroh-docs). Aster carries the patches listed below on top of upstream releases. Upstream maintainers are welcome to incorporate any of them, in whole or in part, at their discretion.

## Branch and tag model

- `main` is upstream `main` plus one commit: the Aster README preamble and this ledger. Nothing is built from `main`. It is refreshed with `git merge upstream/main` and is never reset or force-pushed. Ledger edits are commits on `main`, never on `upgrade/*`.
- `upgrade/<repo>-v<upstream version>` (for example `upgrade/iroh-v1.4.0`, `upgrade/iroh-blobs-v0.103.1`) is the patched integration line: the upstream release plus the patches in this ledger. A new upstream base always gets a new branch, cut from the upstream release tag; Aster-only fixes and re-pins append to the current branch. Per-minor names (`upgrade/iroh-blobs-v0.103`, `upgrade/salvo-v1.0`, …) are historical. Consumers pin commits on it. Published `upgrade/*` branches and tags are never rewritten; a failed attempt is superseded by new commits and a new version.
- Topic branches carry one change each, are named for the change (for example `feat/read-into`, `security/plumtree-bounds`), are based on the upstream release tag (or, for a patch that builds on another, on that prerequisite patch's commit, which its row names), and are cherry-picked with `git cherry-pick -x` onto the upgrade branch, so every upgrade-branch commit names its source. A topic branch a row names is not rewritten; re-cutting it onto a new upstream base takes a versioned name, `<topic>-v<upstream version>`.
- Version: the upstream version if Aster's registry has never served that version or a higher one in the series; otherwise the next patch above Aster's highest published version. A re-pin without a code change keeps the version and takes the tag suffix `-p<n>`.
- Tags: `aster-<repo>-v<published version>` (re-pins `aster-<repo>-v<published version>-p<n>`), created only after the Aster consumer trial passes against those exact commits, because tags cannot be moved.
- The fork manifest is `iroh-fork-manifest.toml` in the Aster repository (the only copy).
- Aster does not submit patches upstream; upstream maintainers are welcome to take any patch here.

Historical and current tag schemes in this repository:

- `aster-iroh-docs-v<version>`, for example `aster-iroh-docs-v0.99.0`, `aster-iroh-docs-v0.100.0`, `aster-iroh-docs-v0.101.7`.
- `aster-iroh-docs-v<version>-p<n>`, a later commit at the same crate version: `aster-iroh-docs-v0.100.0-p1` (added the content-ready patch, row 10), `-v0.100.0-p2`, `-v0.101.0-p1`, `-v0.101.5-p1`, `-v0.101.5-p2` and `-v0.101.7-p1` (patch-block re-pins with no source change).

## Current release

| Field | Value |
|---|---|
| Upstream base | `v0.101.0` |
| Integration branch | `upgrade/iroh-docs-v0.101` (a historical per-minor name; the next upstream base gets `upgrade/iroh-docs-v<upstream version>`) |
| Tag | `aster-iroh-docs-v0.101.7-p1` |
| Pinned commit | `ebb13bc349045e53ec9a3c084863bdced71f8054` |
| Crate version | 0.101.7 (workspace spec `iroh-docs = "0.101.7"`) |
| Source repository | `https://forge.emrul.dev/Aster/iroh-docs.git` |

The fork's own `[patch.crates-io]` (row 9, re-pinned in 0.101.7-p1) pins iroh `261ad623` (1.4.0), noq `535bd884` (1.4.0), iroh-blobs `1ec44e44` (0.103.11, `upgrade/iroh-blobs-v0.103.1`), iroh-gossip `5fde770e` (`aster-iroh-gossip-v0.101.1-p2`) and iroh-util `a90ea88d` (`aster-iroh-util-v0.6.3-p2`), all matching the manifest; the gossip and util lag noted at 0.101.7 is closed.

Bootstrap pin line for source-tree builds (consumers install a versioned Aster SDK release instead and must not copy this):

```toml
iroh-docs = { git = "https://forge.emrul.dev/Aster/iroh-docs.git", rev = "ebb13bc349045e53ec9a3c084863bdced71f8054" }
```

iroh-docs is the fork that exercises all the others together (iroh, noq, iroh-blobs, iroh-gossip, iroh-util). Its suite runs whenever any of them is re-pinned; see "Porting to a new upstream release".

## Patches

Row numbers are stable patch ids shared across Aster's iroh-family forks (iroh, iroh-blobs, iroh-docs, iroh-gossip, iroh-util, noq), so a commit message that says "row 16" resolves here.

Most topic branches from the 0.100 line are stacked on each other rather than based on the upstream tag: `fix/large-namespace-initial-sync` carries rows 12 and 13 on top of row 11; `fix/bounded-sync-redrive` carries row 14 on top of that; `fix/batched-content-download-scheduling` carries row 15 on top of that.

| Id | Topic branch | What | Why / what in Aster depends on it | Commits (topic / upgrade) | Status | Conflict or porting notes |
|---|---|---|---|---|---|---|
| 9 | none recorded (original commit `81f3461`) | `[patch.crates-io]` block pointing the iroh/noq crates (and since 0.101.5-p2 also iroh-blobs, iroh-gossip, iroh-util) at the Aster forks. File: `Cargo.toml`. | Keeps the docs sync tests and standalone builds on the same endpoint/base/noq fork stack as Aster. | `81f3461` (original); rc.1 copy `7efdff0`; re-pinned since (see History), latest `ebb13bc3` (0.101.7-p1) | carried | Low conflict risk. Pin explicit fork revs or tags; drop stale `pre.6` wording. |
| 10 | none recorded | Emits `ContentReady` immediately when `start_download` sees `BlobStatus::Complete`, instead of silently returning. Files: `src/engine/live.rs`, `tests/sync.rs`. | portal-sync waits for `LiveEvent::ContentReady` to mark remotely advertised CAS content usable/retryable, even when the blob was preloaded through another path. | `4ae3eaf` (released in `aster-iroh-docs-v0.100.0-p1`) | carried | Medium conflict risk around `LiveActor::start_download`, `on_download_ready` and content-ready propagation. Preserve the regression: the receiver preloads the blob, imports the doc, receives `InsertRemote { content_status: Complete }`, and still gets an explicit `ContentReady` (`sync_emits_content_ready_for_already_local_content`). |
| 11 | `fix/sync-redrive-missing-content-after-resync` | After a successful sync round, scans local doc entries matching the download policy and re-queues missing content downloads from the synced peer before emitting `PendingContentReady`. Files: `src/engine/live.rs`, `tests/sync.rs`. | portal-sync recovers from "entry replicated, content blob missing" after a peer reconnects or stabilises; repeated `start_sync` calls fetch content that was unavailable at the original insert. | `2c8ab9b` | carried | Medium conflict risk around `LiveActor::on_sync_finished`, `start_download` and `PendingContentReady` ordering. Preserve the regression: an entry arrives with `ContentStatus::Missing`, the provider later stores the blob, a re-sync receives no new entries, and the receiver still gets `ContentReady` plus the blob (`sync_redrives_known_missing_content_after_resync`; see row 20). |
| 12 | `fix/large-namespace-initial-sync` (tip `14107686520394f1df0ffe08cb45c9ec7a229221`) | Adds a collected `Doc::get_many_vec` query path and protocol request, avoiding the streaming query channel drain path for large full-namespace snapshots. Files: `src/actor.rs`, `src/api.rs`, `src/api/actor.rs`, `src/api/protocol.rs`, `tests/sync.rs`. | Aster core uses `Doc::get_many_vec` for `query_key_exact`, `query_latest_exact`, `query_key_prefix` and `query_latest_prefix`; portal-sync uses those during Tree snapshots. | `e1f9340` | carried | Medium conflict risk around the docs query RPC plumbing. Preserve the large read-import, peer-initiated sync regression where a namespace with many content-bearing entries can be queried to completion after sync. |
| 13 | `fix/large-namespace-initial-sync` | Makes docs subscription channels non-backpressuring (unbounded event channels for replica/live subscriptions), so a slow subscriber cannot block the sync actor during large insert bursts. File: `src/engine.rs`. | portal-sync owns a dedicated manifest-event watcher, but event fan-out must still not backpressure iroh-docs sync/query progress for large namespaces. | `1410768` | carried | Medium conflict risk around subscription/channel plumbing. If upstream changes event fan-out, preserve the invariant: a stalled or slow subscriber cannot block sync actor progress. Row 18 completes it. |
| 14 | `fix/bounded-sync-redrive` (tip `027467daa2e29cf18b68e074ab07289b9f45ee22`) | Bounds `redrive_missing_content_for_peer` to hashes already tracked as missing for that namespace, instead of scanning every local entry after each successful sync. File: `src/engine/live.rs`. | Keeps row 11's reconnect recovery without O(namespace entries) CPU and blob-status work during portal-sync large-namespace progress. | `027467d` | carried | Medium conflict risk around missing-content tracking, download scheduling and `on_download_ready`. Preserve namespace-scoped missing state, clear it on successful content completion, and emit `ContentReady` for every namespace waiting on the completed hash. Test: `missing_hashes_are_tracked_per_namespace`. |
| 15 | `fix/batched-content-download-scheduling` (tip `de983790c850a4406416697d29d948f78b6210fa`) | Drains up to 1024 ready replica events per live-actor tick, groups content download candidates by hash, uses `Blobs::status_many` (iroh-blobs fork row 7), and starts at most one downloader per missing hash. Also separates per-hash `ContentReady` from namespace-wide `PendingContentReady` when downloads run concurrently, and fixes queued-hash bookkeeping. Files: `Cargo.toml`, `Cargo.lock`, `src/engine/live.rs`. | Reduces per-entry blob-status actor round trips during large RemoteInsert bursts without changing portal-cas data layout. | `de98379` | carried | Medium conflict risk around `on_replica_event`, redrive, neighbour content-ready handling, queued hashes and content-ready ordering. Preserve `test_download_policies`: concurrent downloads in one namespace emit `ContentReady` for each completed hash, while `PendingContentReady` waits for the namespace queue to drain. Requires the iroh-blobs fork's `status_many`. |
| 16 | `fix/syncfinish` (tip `cabe333b20`, based on `v0.101.0`) | Queues one follow-up reconciliation when a `NewNeighbor` sync request races an already-running sync, matching the existing `SyncReport` behaviour. File: `src/engine/state.rs`. | Prevents portal-sync's startup `DirectJoin` from consuming the only reconciliation opportunity before gossip is ready, which could leave a post-join publish absent indefinitely. | `cabe333b20` / `6d13844`; released as 0.101.1 at `6c011608` | carried | Low conflict risk in the per-peer sync state machine. Preserve the regression that `NewNeighbor` during `DirectJoin` sets `resync_requested`; keep it separate from any future `Event::Lagged` anti-entropy behaviour. Upstream audit 2026-08-14: `upstream/main` at `ad80e691`, `PeerState::start_connect` still queues only `SyncReport`, so not applied upstream; re-audit before each port. Row 16 changed the observable event sequence; port rows 20 and 22 with it. |
| 18 | `fix/replica-event-backpressure` (tip `2761364d`) | Makes the live actor's own replica-event channel unbounded. `Subscribers::send` delivers into it with a blocking `tx.send(..).await` executed inside the sync actor, so bounding it means a lagging subscriber stops the single actor servicing replica requests. File: `src/engine/live.rs`. | Completes row 13, which established this invariant for the subscription channels and missed this sibling. portal-sync's shape (one large granted Tree syncing beside a small policy namespace) is exactly the burst that overran the bound. Closes portal-sync's OI-015. | `2761364d` / `905d4bc`; released in 0.101.3 | carried | Low conflict risk (one channel constructor). Preserve the invariant, not the line: bound work per tick via `MAX_REPLICA_EVENT_BATCH`, never the channel's capacity. Regression: `sync_large_read_import_after_peer_initiated_race` stalls at exactly 1027 of 2000 events when the channel is bounded. |
| 19 | `fix/capability-downgrade` (tip `d47957a`) | Adds `DocsApi::retire_write_authority(doc_id)`, narrowing a namespace's stored capability from Write to Read without deleting entries, refusing with an outstanding-reference count while handles remain, and reporting the capability kind now stored rather than the one requested. Files: `src/actor.rs`, `src/api.rs`, `src/api/actor.rs`, `src/api/protocol.rs`, `src/store/fs.rs`, `src/sync.rs`, `tests/sync.rs`. | Aster exposes it as `Docs::retire_local_write_authority`; portal-sync needs a non-destructive Write-to-Read transition for its grant lifecycle, and neither importing Read (capability merge only upgrades) nor dropping the document (loses entries) provides one. | `d47957a` / `5976751`; released in 0.101.3 | carried | Medium conflict risk in the replica open/capability path. Preserve all three properties together: entries survive, the narrowed capability is durable across restart, and a later write import widens again. This is local authority retirement, not cryptographic revocation: a peer holding the namespace secret can still write. |
| 20 | none, deliberately (integration branch only) | Replaces the exact-completion multiset in `sync_redrives_known_missing_content_after_resync` with a semantic event collector. Phase 1 requires a neighbour, the missing-content entry, at least one successful completion and a drain, tolerating further completions in any position rather than consuming a single-use slot. Phase 2 is an ordered state machine (completion, then the expected content, then the drain) in which a drain seen before the content belongs to the previous cycle and resets the wait. Failed completions, completions from another peer and unrelated events remain errors. File: `tests/sync.rs`. | Row 16 changed the observable event sequence; this test, written two months earlier, asserted an exact event set with an origin-blind matcher. It failed about 1 run in 5, and 0.101.1 shipped it. | `6523891` on `upgrade/iroh-docs-v0.101`; released in 0.101.3 | carried | Low conflict risk. No upstream-tag-based topic branch exists by design: it needs both row 14's test (an Aster addition) and row 16's behaviour, which coexist only on the integration branch. If row 16 is ever ported, port this with it. **Do not reintroduce a partition of completions by origin or entry count**: no such partition exists, and two attempts at one shipped or nearly shipped defects (see History, 0.101.2 and 0.101.3). |
| 21 | none (integration branch only; see row 20) | **Public API addition.** Re-exports `SyncDetails`, the `Ok` type of the public `SyncEvent::result` field, which was previously unnameable outside the crate (a caller could read a completion's counts by inference but could not construct or annotate one). Also corrects `SyncEvent::finished`/`started`, whose doc comments were swapped, and documents what origin and entry counts do not establish. Files: `src/engine.rs`, `src/engine/live.rs`. | Row 20's unit tests construct synthetic completions and cannot be written without a nameable `SyncDetails`. | `6fbe973` on `upgrade/iroh-docs-v0.101`; released in 0.101.3 | carried | Low conflict risk. Adds no behaviour and changes no type; it makes an existing public API nameable. Recorded as a surface change rather than folded into the test patch that needed it. Drops out cleanly if upstream adds the same re-export. |
| 22 | `fix/alreadysyncing-wedge` (tip `dd71c384be`, stacked on `fix/syncfinish`; backfilled) | A connect rejected by the remote as `AlreadySyncing` left `PeerState` in `Running{Connect}` forever, refusing every later attempt as already running, so the peer wedged until restart. The fix lets the rejected connect's own abort reset the state, while a stale abort arriving after a reciprocal accept has taken ownership must not erase that live exchange. Files: `src/engine/live.rs`, `src/engine/state.rs`, `tests/sync_alreadysyncing_wedge.rs`. | portal-sync daemons dial trees at startup and on grant changes; two daemons connecting to each other simultaneously hit the `AlreadySyncing` rejection routinely. | `dd71c384be` / `287ef24` + `f38c2a5`; released as 0.101.4 | carried | Medium conflict risk in the per-peer sync state machine. Stacked on `fix/syncfinish` because its unit tests assert row 16's `resync_requested`; port rows 16 and 22 together. The topic branch was reconstructed after release, so its trailers point at the upgrade-branch commits (see History). Three deterministic unit tests in `engine::state` plus the two-node integration wedge test pin the behaviour. 0.101.4 skipped the fmt/clippy gates; `00d1a3b9` restores them. Row 39 later made the abort's reset silent (no `SyncFinished(Err)`/`PendingContentReady`). |
| 25 | `fix/durable-set-bytes` (tip `77385ea71b`) | Adds `Doc::set_bytes_durable`: `add_bytes` → `sync_db` → `insert_local`. The blob store acknowledges imports inside an open write batch and commits later, so plain `set_bytes` can leave a durable document record naming blob metadata that was never committed after a hard kill (measured: reopen reports `NotFound` for the published hash). The barrier is a top-level blob-store command, so it also commits every earlier import on the same store: one call gives whole-pipeline ordering for import-payloads-then-publish-record flows. `set_hash` deliberately unchanged (reference-without-content contract). New `SetDurable` protocol variant added at the enum tail (positional serialisation). Files: `src/api.rs`, `src/api/actor.rs`, `src/api/protocol.rs`, `tests/durable_set_crash.rs`. | portal-sync's publish pipeline (portal-cas manifest writes) needs the record-never-dangles direction for its OI-018; the reverse crash window self-heals by rescan. | `77385ea71b` / `a0192b7`; released as 0.101.5 at `0db8c5cc` | carried | Low conflict risk (additive API). Crash witness `tests/durable_set_crash.rs` SIGKILLs a child on the ack and reopens the blob store demanding `Complete`, five rounds; `DURABLE_CRASH_MODE=plain` reproduces the dangling state as a manual diagnostic. Not a power-loss guarantee for data files. |
| 36 | `fix/live-actor-task-panic` (tip `6bdff10`, based on `85ec38b4`, 0.101.5-p2) | The live actor applied `?` to every `join_next` result of its connect, accept and download task sets, so one task that panicked (or was cancelled) returned a `JoinError` from `run_inner`; `shutdown()` then stopped gossip and the sync actor, and every later docs call on the node failed with tokio's "channel closed". The actor now joins with `join_next_with_id` and records per task id what it is for: `(namespace, peer, reason)` for a connect, the `(namespace, peer)` an accept has read from its request (set by the accept callback), `(namespace, hash)` for a download. A failed task is logged with `warn!` and handled as an ordinary failure of that exchange: `on_sync_finished(.., Err)` releases the peer's `Running` state (so a later `start_sync` with that peer runs), a download goes back to `missing_hashes`; an accept that fails before reading its request has nothing to release. Mirrors how `GossipState::progress` already treats cancelled receive loops. File: `src/engine/live.rs`. | Aster review finding TM-2: the .NET docs parity test once failed with `aster_doc_start_sync: channel closed` under a full gate (runbook row 116), the signature of a stopped live actor; without this patch one panicking sync takes a node's docs down for the rest of the process. The panic itself was not reproduced then (Aster `core/tests/docs_join_leave_restart.rs`, join → leave → start_sync under load, 5 760 sequences, no panic); row 37 later found it. | `6bdff10` / same commit on `upgrade/iroh-docs-v0.101` (fast-forward); released as 0.101.6 at `573efc68` | carried | **Aster's own; no upstream equivalent (checked 2026-10-17): upstream `main` (`8cfeacb`, 2026-08-19) still has `res.context("running_sync_connect closed")?` and the `TODO: abort_all and join_next all JoinSets to catch panics` in `shutdown`; open issue #65 ("Iroh aborts under high load", 2024) reports the symptom without a fix; open PRs #106–#114 do not touch the task joins.** Drop when upstream stops propagating task `JoinError`s out of the live actor. Low–medium conflict risk in `run_inner`'s select arms and the three spawn sites (rows 15, 18 and 22 touch the same file). Test: `engine::live::task_failure_tests::panicked_sync_task_leaves_the_actor_alive` (a `cfg(test)` fault hook panics the next connect task for a namespace; the round fails with the panic, then `start_sync` with the same peer reconciles the entry); with upstream's `?` the event stream ends because the actor stopped. lib 83, client 5, durable_set_crash 1 (1 ignored), gc 1, sync 27 (3 ignored), sync_alreadysyncing_wedge 1, doc 1; clippy `--all-features --all-targets -D warnings`, fmt and `--no-default-features` check clean. |
| 37 | `fix/accept-outcome-after-error` (tip `a9ea085`, based on `573efc68`, 0.101.6) | `BobState::run` takes `progress` before each `sync_process_message` and puts it back only on success, so a step that fails (as when a leave closes the replica mid-sync) leaves it `None`. `handle_connection` then asks for `state.into_outcome()` before it looks at the result, and `into_outcome` unwrapped it: the accept task panicked with `Option::unwrap()` on a `None` at `codec.rs:290`. `into_outcome` now returns an empty `SyncOutcome` in that case; the outcome is only reported for a successful run. File: `src/net/codec.rs`. | Aster `core/tests/docs_join_leave_restart.rs` (join → leave → start_sync under load) caught it in release candidate 14 (2026-10-19): "1 panic(s), 0 failed pair(s)". Very likely the panic row 36 could not reproduce: before row 36 it stopped the live actor and every later docs call failed with "channel closed"; with row 36 it fails one exchange. | `a9ea085` / same commit (fast-forward); released as 0.101.7 at `df48e810` | carried | **Aster's own; no upstream equivalent:** the unwrap is upstream code unchanged since `f1c3cf9` (the 2024 rename). Drop when upstream stops unwrapping the progress on the error path. Low conflict risk (one function). Test: `net::codec::tests::into_outcome_after_a_failed_step_is_empty`. lib 84, client 5, durable_set_crash 1 (1 ignored), gc 1, sync 27 (3 ignored), sync_alreadysyncing_wedge 1, doc 1; clippy `--all-features --all-targets -D warnings`, fmt and `--no-default-features` check clean. |
| 39 | `fix/alreadysyncing-tiebreak-quiet` (tip `edaa1ec`, stacked on `fix/alreadysyncing-wedge`) | A connect that loses the simultaneous-dial tie-break (the remote rejects it with `AlreadySyncing`, by design: `expected_sync_direction` makes the lower id reject while its own connect runs) is reset to idle but no longer reported. Since row 22 the typed `ConnectError::RemoteAbort(AlreadySyncing)` was converted to an `anyhow` error and routed through `on_sync_finished`, which emitted `SyncFinished(Err)` plus an extra `PendingContentReady` to every subscriber. The abort is now matched while still typed: `NamespaceStates::finish_tiebreak_loss` finishes the `PeerState` (origin-conditional, so a stale abort cannot erase a live accept) and a queued `resync_requested` still re-drives, but no event is emitted and `register_useful_peer`, the sync report broadcast and the redrive are skipped. Every other connect error still goes through `on_sync_finished`. Files: `src/engine/live.rs`, `src/engine/state.rs`, `tests/sync_alreadysyncing_tiebreak.rs`. | portal-sync subscribers saw the spurious `Err` on every simultaneous connect; `tests/sync.rs` `sync_simple` bailed on it (about 1 run in 20, 2 of 17 on the 1.4.0 pins). Closes backlog item T6. | `94b8a5a` (test `ee2c896`) / `9fae81c` (test `434f505`); released in 0.101.8 (`18db38d`) | carried | Low conflict risk (one branch in `on_sync_via_connect_finished`). Stacked on row 22 because it changes how row 22 routes the abort; port rows 16, 22 and 39 together. Preserve the reset, not the silence alone: dropping the `finish` call brings back the wedge. Tests: four `engine::state` unit tests (`tiebreak_loss_*`, `only_the_alreadysyncing_abort_is_a_tiebreak_loss`) and `sync_alreadysyncing_tiebreak`, which holds the higher id's incoming docs connections so the collision is forced; it fails (two `Err` rounds) without the change. |

Bookkeeping and documentation commits (not needed at runtime):

| Commits | Files | Purpose | Status |
|---|---|---|---|
| `87c1c00` (rc.1) | `.gitignore` | Ignores local `docs/aster/` internal notes. | Recorded in the rc.1 stack; current status not recorded. Optional. |
| `5acb0f3` (rc.1) | `Cargo.lock` | Resolves the Aster iroh/noq fork crates. Regenerated each wave. | carried in spirit (regenerated). |
| `00d1a3b9` | various | rustfmt hygiene; restores the fmt/clippy gates 0.101.4 skipped. | carried (released in 0.101.5). |
| `be04181` | `README.md` | Aster fork overview and contribution table. | Replaced by the `main` README preamble under the current model; if reused, refresh PR statuses. |

### Upstreamed or dropped

No iroh-docs patch has been upstreamed or dropped. Superseded releases (not patches): 0.101.2 (`c1987427`) shipped a defective row 20 and is superseded by 0.101.3; it was never published to the registry, and its tag stays in place.

### Closed security-review branches

These topic branches came from an April 2026 security review. The fixes from that review that held up were merged upstream, so these branches are not carried on the upgrade branch. They are kept for reference.

| Branch |
|---|
| `fix/bound-entry-key-size` |
| `fix/bound-mempublickeystore` |
| `security/hardening` |
| `security/lock-concurrency-safety` |
| `security/protocol-robustness` |
| `security/resource-bounds` |

## Porting to a new upstream release

Order across the iroh family: noq, then iroh, then the satellites in the Aster publisher's order: iroh-util, iroh-gossip, iroh-blobs (its rows need iroh-util's `ConnectionPool::retire`), this repository. This fork's `[patch.crates-io]` names every other fork by revision, so it is re-pinned last, and iroh-blobs is published before iroh-docs.

Before touching branches, read the upstream changelog and range from the previous base (the rc.1 port reviewed `v0.99.0..v0.100.0`), and decide for each row: keep, drop because upstream absorbed it, or rewrite. In particular: re-audit upstream's `PeerState::start_connect` for row 16; check the task joins for row 36 and `into_outcome` for row 37. Also decide whether to base on the release tag or on upstream `main` when `main` carries a relevant fix past the tag (in the rc.1 port upstream `main` was `fc89461`, one commit past `v0.100.0`: `fix(store): migrate redb 2.x tuple tables on open`).

Reapply order on the new base:

1. Row 9 (patch block, re-pointed at the new fork revs of iroh, noq, iroh-blobs, iroh-gossip, iroh-util), then regenerate `Cargo.lock`.
2. Rows 10, 11, 12, 13, 14, 15 (the 0.100-line stack, in order).
3. Row 16, then row 22 (stacked on it), then rows 20 and 21 together with them.
4. Rows 18, 19, 25, 36, 37.

```bash
git fetch --all --tags --prune
git push origin <upstream-tag>          # mirror the upstream tag (e.g. v0.101.1) to the forge
git switch -c upgrade/iroh-docs-v<upstream version> <upstream-tag>
# cherry-pick -x each row's commits in the order above
# version bump, only if the version rule requires it (Aster's registry has already
# served <upstream version> or higher in the series, as it has 0.101.0..0.101.7):
# set the crate to the next patch above Aster's highest published, then
git commit -am "chore: release <published version>"
git push origin upgrade/iroh-docs-v<upstream version>
# do not tag yet: tag after the consumer trial (below)
```

Prefer `cherry-pick -x`; if a patch no longer applies, treat the "What" and "Conflict or porting notes" columns as the specification. Published `upgrade/*` branches and tags are never rewritten; a failed attempt is superseded by new commits and a new version (as 0.101.3 superseded 0.101.2).

An Aster-only fix on the current base is a topic branch, cherry-picked `-x` onto the current upgrade branch, followed by a version-bump commit to the next patch above Aster's highest published version (as 0.101.6 and 0.101.7 were cut). A re-pin without a code change (row 9 only) is a commit on the current upgrade branch with no version bump, tagged `aster-iroh-docs-v<current version>-p<n>` after the consumer trial (as `aster-iroh-docs-v0.101.5-p2` was).

Validate in this repository, and re-run it whenever iroh, noq, iroh-blobs, iroh-gossip or iroh-util is re-pinned (re-pin this fork's own `[patch.crates-io]` to the same revs first):

```bash
cargo fmt --check
cargo clippy --all-features --all-targets -- -D warnings
cargo check --no-default-features
cargo test --all-features     # 118 tests incl. tests/sync.rs
```

How to validate `sync_redrives_known_missing_content_after_resync` (row 20): run the whole `sync` suite, not the single test, because concurrency is the variable that matters, and rely primarily on the deterministic unit tests, which pin every ordering in-process without timing. Repeating the single test in isolation (`cargo test --test sync <name>`) suppresses the failing ordering and is not evidence.

Gotcha when re-pinning: `cargo update -p iroh` alone can keep crates.io iroh and report the patches as unused; use `cargo update -p iroh --precise <version>` (with two iroh entries in the lock, `cargo update -p iroh@<old> --precise <new>`), or temporarily require the exact version and run `cargo update -p <crate>`. Update only the crates you name, and revert any unrelated lock churn.

Aster validation after updating pins, from the Aster repository root:

```bash
cargo fmt --check
cargo clippy --manifest-path bindings/python/rust/Cargo.toml -- -D warnings
cargo test -p aster_transport_core
cargo test -p aster --all-features
uv run maturin develop -m bindings/python/rust/Cargo.toml
uv run pytest tests/python/ -v --timeout=30
```

Tag only after the Aster consumer trial passes against these exact commits (tags cannot be moved):

```bash
git tag -a aster-iroh-docs-v<published version> -m "Aster iroh-docs v<published version>" <release commit>
git push origin aster-iroh-docs-v<published version>
# re-pin only: git tag -a aster-iroh-docs-v<current version>-p<n> -m "..." <re-pin commit>
```

Update `iroh-fork-manifest.toml` in the Aster repository (the only copy), and this ledger's Current release section on `main` (merge upstream `main` first).

Local checkouts: downstream workspaces with `[patch.crates-io]` path overrides into a local checkout (for example portal-sync) build whatever is checked out. After publishing, switch the local checkout to the released tag (for development, the upgrade branch, which can sit past the tag), and run at least one path-override consumer check (for example `cargo check --locked -p portal-cas` in portal-sync).

Before the current branch model, fork `main` mirrored upstream `main` and was reset to it on each port (`git reset --hard upstream/main`, `git push origin main --force-with-lease`). Under the current model `main` is upstream `main` plus one commit (README preamble and this ledger), refreshed with `git merge upstream/main`, and is never reset or force-pushed.

## History

### 1.4.0 wave (2026-10-21), current

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.101.0` | `aster-iroh-docs-v0.101.8` | `18db38d62fbdce99754fa32448ddcbd967585b44` | `upgrade/iroh-docs-v0.101` |

- No upstream iroh-docs release after `v0.101.0`, so no port.
- **2026-10-21, 0.101.8** (`18db38d`, tag `aster-iroh-docs-v0.101.8`): `fix/alreadysyncing-tiebreak-quiet` cherry-picked `-x` as `434f505` (test) and `9fae81c` (fix), then the version bump to 0.101.8 (next patch above 0.101.7, the highest Aster has published). Closes backlog T6. Validation: `cargo test --locked --all-features` all green (lib 88, client 5, sync 27 with 3 ignored, sync_alreadysyncing_tiebreak 1, sync_alreadysyncing_wedge 1, and the others); clippy `--all-features --all-targets -D warnings`, patch-file fmt and `--no-default-features` check clean. The whole `sync` suite ran 30 times: 29 passed; the one failure was `test_sync_via_relay` (a third successful `SyncFinished` against a matcher allowing two), not `sync_simple` and not an `AlreadySyncing` event; `sync_simple` passed 30 of 30. A comment-only follow-up (`18db38d`, from topic `edaa1ec`) updates the wedge test's note that the aborted round surfaces as a failed `SyncFinished`. Baseline for the one `test_sync_via_relay` failure: 0 of 30 whole-suite runs on `ebb13bc3` without row 39 (2 of 30 `sync_simple`), so 1 in 30 with it is not distinguishable from noise at this count; watch it. Consumer trial in Aster: the fork's suite through `fork_compat.sh`, `aster_transport_core` (432 passed, three clean runs; one earlier run had a single unidentified failure), `aster --all-features`, workspace clippy.
- **2026-10-21, 0.101.7-p1** (`ebb13bc3`, tag `aster-iroh-docs-v0.101.7-p1`): re-pin only (row 9). iroh, iroh-base and iroh-relay → `261ad623` (1.4.0); noq, noq-udp and noq-proto → `535bd884` (1.4.0); iroh-blobs `bcd00bb2` → `1ec44e44` (0.103.11); iroh-gossip `aed725d1` → `5fde770e` (0.101.1-p2); iroh-util `6c42d238` → `a90ea88d` (0.6.3-p2). The commit touches only Cargo.toml's `[patch.crates-io]` and Cargo.lock. No source change; the crate stays 0.101.7 and is not republished. Validation: `cargo test --locked --all-features` 117 passed, 4 ignored; clippy `--all-features --all-targets -D warnings` and fmt clean.
- **Flake to watch (root cause found 2026-10-21, fixed by row 39):** `sync_simple` failed 2 of 17 full `tests/sync.rs` runs on these pins ("Remote peer aborted sync: AlreadySyncing"), against 0 of 8 on the 1.3.2 pins. This was not an iroh 1.4.0 regression: row 22 (0.101.4) routed the tie-break loser's `AlreadySyncing` abort into `on_sync_finished`, so the loser's subscribers got a spurious `SyncFinished(Err)` that the test bails on. The 1.3.2 pins carried the same code; 0 of 8 there is chance at roughly 1 run in 20. Row 39 resets the connect without reporting it. Measure with the whole `sync` suite, as for row 20.

### 1.3.0 wave (2026-10-03 onward)

No upstream iroh-docs release forced a re-port; base stays `v0.101.0`. Releases in this wave, newest first:

| Version | Commit | Tag | Date | Change |
|---|---|---|---|---|
| 0.101.8 | `18db38d` | `aster-iroh-docs-v0.101.8` | 2026-10-21 | Row 39 (quiet tie-break loss) on top of 0.101.7-p1. |
| 0.101.7 | `df48e810` | `aster-iroh-docs-v0.101.7` | 2026-10-19 | Row 37 on top of 0.101.6. Pin until 0.101.7-p1 (2026-10-21). |
| 0.101.6 | `573efc68` | `aster-iroh-docs-v0.101.6` | 2026-10-16 | Row 36 on top of 0.101.5-p2. |
| 0.101.5-p2 | `85ec38b4` | `aster-iroh-docs-v0.101.5-p2` | 2026-10-16 | Re-pins the fork's own `[patch.crates-io]` to the stack Aster links it against: iroh `a4a9a76b` (1.3.2), noq `e75f3a55` (1.3.2), iroh-blobs `bcd00bb2` (0.103.10), and adds iroh-gossip `aed725d1` (0.101.1) and iroh-util `6c42d238` (0.6.3), which its lock had taken from crates.io as 0.101.0 and 0.6.0. The other forks had been re-pinned (rows 29–35) without an iroh-docs run on the new stack (review finding TM-7). No source change; the crate stays 0.101.5. Validation: lib 82, client 5, durable_set_crash 1 (1 ignored), gc 1, sync 27 (3 ignored) three times, sync_alreadysyncing_wedge 1, doc 1 (118 in all), all passing; clippy `--all-features --all-targets -D warnings` and `cargo fmt --check` clean. From then on the suite runs whenever any fork it depends on is re-pinned. |
| 0.101.5 | `0db8c5cc` | `aster-iroh-docs-v0.101.5` | | Row 25 (`set_bytes_durable`) plus rustfmt hygiene `00d1a3b`. |

- Same-day follow-up after noq 1.3.1: the fork's own patch block was re-pinned to iroh `1974e958` and noq `867a3a9a` (it still named iroh-blobs `b535aa30`; harmless, because the Aster repository's root patch overrides it and the registry dependency is `0.103`). Lock and pin changes only; the publisher skipped the unchanged version as identical. This re-pin exposed the noq 1.3.0 regression from upstream #794: `tests/sync.rs` (`sync_simple`, `test_sync_via_relay`, `sync_redrives_known_missing_content_after_resync`) failed every sync's accept-side close with `Read(ClosedStream)`, because iroh-docs' close sequence drains an already-finished stream with `read_to_end(0)`. Fixed in noq 1.3.1. Validation on the new pins: iroh-docs 118 (sync 27/27 twice), clippy clean.

### 1.0.1 wave (2026-07-02)

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.101.0` | `aster-iroh-docs-v0.101.4` | `f38c2a5cb140a83a1c97db4ace1063a7f67e4dfd` | `upgrade/iroh-docs-v0.101` |

- No upstream release; the wave's iroh/noq bump only. Releases during the wave:
- **0.101.1** (`6c011608`, tag `aster-iroh-docs-v0.101.1`): row 16. `fix/syncfinish` is the upstream-based source commit; `6d13844` is its provenance-preserving cherry-pick.
- **2026-08-13 sweep (`-p1` tags):** the fork's own `[patch.crates-io]`, which had named the GitHub mirror at the 1.0.0-wave iroh (`3c329f0c`) and noq (`ddb8829d`) and a pre-multifetch iroh-blobs, was moved to Forgejo at this wave's revisions, in dependency order noq → iroh → iroh-blobs → iroh-docs / iroh-gossip. Aster's product graph was never affected (its root patch overrides it), but this fork's own build and tests compiled a stack Aster does not ship, which is where fork patches get validated. No `src` change.
- **2026-08-14 addition:** passed its 98-test release suite (3 ignored), the native-stack source/package audit, Aster package checks and clippy, and a clean Aster `v0.3.20` candidate run of 612 workspace tests (8 ignored). One aggregate run hit the pre-existing timing assertion in `swarm_converges_with_sealed_grant_distribution`; that test then passed six consecutive all-feature runs and the final aggregate run passed.
- **0.101.2** (`c1987427`, tag `aster-iroh-docs-v0.101.2`): tagged and pushed to both remotes with a defective row 20. The tag is left in place rather than force-moved (a moved public tag is worse than a superseded one), so 0.101.2 exists as a real but superseded release. Never published to the registry.
- **0.101.3** (`d80d7a6c`, tag `aster-iroh-docs-v0.101.3`): four patches over 0.101.1: row 19 (write-authority retirement), row 18 (replica-event backpressure), row 20 (redrive-test correction) and row 21 (public API addition, the only public-surface change). **Consume 0.101.3, not 0.101.2.** Two of these correct defects that 0.101.1 introduced and shipped, both found by taking that release's one known "flaky test" seriously instead of quarantining it; each bisects to a specific Aster patch in this release line.
  - Row 18 closes what portal-sync tracked as OI-015, recorded as a release-gate flake; it was not one. `sync_large_read_import_after_peer_initiated_race` stalled at exactly 1027 of 2000 events on three independent runs and then sat until the 60 s timeout, while a passing run finished in 2.7 s. An identical stall count across runs is the `bounded(1024)` channel filling, not a race. With the channel unbounded the test passes 6/6 at ~2.7 s. The test had been failing since it was written and reproduces on 0.101.1.
  - Row 20: `sync_redrives_known_missing_content_after_resync` failed about one run in five. It was a stale test, not a defect: row 16 added a second `SyncFinished` and the test asserted an exact event set with an origin-blind matcher. Bisected to row 16: 20/20 pass at `6d13844^`, 9 failures in 40 after it.
  - **Row 20's first implementation was wrong, and 0.101.2 shipped it.** Admitting the resync as an optional matcher is not enough: `assert_next_unordered_with_optionals` tries required matchers before optional ones, so a required `match_sync_finished` broad enough to accept `Connect(Resync)` consumes it whenever it arrives first and orphans the real sync's event.
  - **A second attempt, partitioning completions by origin, was also wrong** and never shipped. Excluding `Connect(Resync)` from the required matcher makes it unsatisfiable in runs where only a resync completes, converting a 27.5% mismatch rate into a 2.2% timeout rate: a rarer failure, not a fixed one.
  - **No such partition exists.** Origin identifies why a synchronisation started; it cannot establish ownership by an artificial test phase, nor which completion caused the redrive, and either origin may legitimately satisfy phase two. Entry counts do not separate them either: a `DirectJoin` and a `Resync` that each reconciled nothing differ in origin while sharing an entry-count shape, and a queued reconciliation may itself carry entries an earlier snapshot missed. The shipped form asserts the shape of what must happen and inspects neither field.
  - **The measurement that cleared the first attempt was invalid.** It repeated the single test in isolation, the worst available control: with nothing else running, the real sync almost always finishes first, so the triggering ordering barely arises. An earlier revision of the ledger claimed "120 clean settled-binary runs" and called 0.101.2 reliably green; both claims were false and were retracted.
  - Evidence for 0.101.3: 11 unit tests covering DirectJoin-only, Resync-only, both orderings, a stale drain, a failed completion, an unrelated event, a completion from another peer, and repeated completions in phase 1; plus 110 consecutive full-suite runs under concurrency with zero failures, against 11 failures in 40 runs without row 20.
- **2026-08-24..26, 0.101.4 (row 22), remediated 2026-08-27:** during the portal-sync cutover campaign the AlreadySyncing wedge fix was committed directly on `upgrade/iroh-docs-v0.101`, with no topic branch and no `-x` trailers. `fix/alreadysyncing-wedge` (`dd71c384be`, stacked on `fix/syncfinish`) was reconstructed afterwards; it compiled and passed its regression tests before push. Its trailers point at the upgrade-branch release commits, the opposite of the normal flow, because the release existed first. The released tag and registry artifact were not touched. A stray `repro/alreadysyncing-wedge` branch (a duplicate pointer at the 0.101.4 tip, not a topic branch) was deleted locally and on origin. 0.101.4 skipped the fmt/clippy gates; `00d1a3b9` restored them.
- **Row 25** (`fix/durable-set-bytes` `77385ea71b`) followed the procedure: topic branch first, then `-x` onto the release branch as `a0192b7`. Released later as 0.101.5.
- Wave validation: per-fork fmt/clippy (`-D warnings`, `--all-features --tests`)/tests, then downstream `aster_transport_core` (242) and `aster --all-features` (94), PyO3 clippy clean, `maturin develop` and 1117 Python tests.
- Local checkout at the end of the cycle: `aster-iroh-docs-v0.101.4` (released code); the upgrade branch (past the tag, with the then-unreleased row 25 and fmt hygiene) for development.

### 1.0 wave (2026-06-21)

| Upstream base | Tag | Pinned commit | Branch |
|---|---|---|---|
| `v0.101.0` | `aster-iroh-docs-v0.101.0-p1` | `d9c091511de89a3e1310b23072804118822cebb6` | `upgrade/iroh-docs-v0.101` |

- Patch stack carried forward unchanged from rc.1; everything cherry-picked clean except a `Cargo.lock` regeneration. Tag == pinned commit == validated code.
- Validation: per-fork fmt/clippy/tests, then `aster_transport_core` (229) and `aster` (68) Rust tests and `maturin develop` + 1117 Python tests.

### rc.1 port (2026-06-07)

- Before the port Aster pinned `de983790c8` (`fix/batched-content-download-scheduling`), upstream base `v0.100.0` (`bbb1981`), target the same; stack range `v0.100.0..fix/batched-content-download-scheduling`. Earlier upgrade branch name: `upgrade/iroh-docs-v0.99`.
- Ported and published through `aster-iroh-docs-v0.100.0-p2`: fork `main` mirrored upstream `main` (`fc89461`); branch `upgrade/iroh-docs-v0.100` and tags `aster-iroh-docs-v0.100.0`, `-p1` and `-p2` pushed to `aster-rpc/iroh-docs`. Row 11 was also published on `fix/sync-redrive-missing-content-after-resync`, and the stacked topic branches (rows 12–15) were pushed so Cargo could resolve the pinned rev from GitHub.

| Commit | Purpose |
|---:|---|
| `7efdff0` | Cherry-pick of `81f3461`: Aster iroh/noq `[patch.crates-io]` entries, updated to explicit rc1 revs. |
| `87c1c00` | Ignore local `docs/aster/` internal notes. |
| `5acb0f3` | `Cargo.lock` resolves the Aster iroh/noq fork crates. |
| `4ae3eaf` | Row 10. |
| `2c8ab9b` | Row 11. |
| `e1f9340` | Row 12. |
| `1410768` | Row 13. |
| `027467d` | Row 14. |
| `de98379` | Row 15. |

- Validation: `cargo fmt --check`; `cargo test sync_emits_content_ready_for_already_local_content -- --nocapture`; `cargo test --test sync sync_redrives_known_missing_content_after_resync -- --nocapture`; `N_ENTRIES=2000 cargo test -p iroh-docs sync_large_read_import_after_peer_initiated_race -- --nocapture`; `N_ENTRIES=2000 RUST_LOG=iroh_docs=info cargo test --test sync sync_gossip_bulk -- --nocapture`; `cargo test --all-features`; `cargo clippy --all-features --tests -- -D warnings`; `cargo test missing_hashes_are_tracked_per_namespace`; `cargo test sync_redrives_known_missing_content_after_resync`; `cargo test --test sync test_download_policies -- --nocapture`.
- Portal H2 local-path validation: `RUSTC_WRAPPER= BASE=500 HIGH=1 PORTAL_SYNC_BLOB_FETCH_TIMEOUT_MS=5000 RUST_LOG=info,portal_sync_session=debug,portal_syncd::sync_supervisor=debug,iroh_docs=info scripts/phase5/sync/h-content/many-small.sh` passed: 500 files converged on B after ~22 s, sampled content intact, and the 501-file probe converged. This validated correctness with the batching patches but did not materially improve H2 wall clock versus the prior ~22 s baseline; the remaining time is likely outside this scheduling round-trip path.
- The rc.1 command sequence was `git cherry-pick -x 81f3461` then `git cherry-pick -x 4ae3eaf` onto `upgrade/iroh-docs-v0.100` from `v0.100.0` (after reviewing `v0.99.0..v0.100.0`), then tag `aster-iroh-docs-v0.100.0-p1` ("content-ready patch"). The Aster root patch block then pointed at `https://github.com/aster-rpc/iroh-docs`; it now points at the Forgejo repository.
- Until replacement tags were cut, path-override consumers kept the local checkout on the `fix/batched-content-download-scheduling` tip; the end-of-cycle checkout tag was `aster-iroh-docs-v0.100.0-p1`.

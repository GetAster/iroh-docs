//! Maintained bucket aggregates over the records table.
//!
//! Each namespace's records, in records-table order, are partitioned into
//! contiguous buckets (see [`super::tables::NAMESPACE_BUCKETS_TABLE`]). A
//! bucket row holds its first record's id, the number of records it covers
//! and the XOR of their fingerprints, so a range count or fingerprint reads
//! the rows of the buckets the range covers and only the records of the (at
//! most two) buckets it cuts through: O(N/B + B) instead of O(range).
//!
//! Invariants, for every namespace with records:
//! - the buckets' ids are ids of existing records, in strictly increasing
//!   order, and the first one is the namespace's first record;
//! - a bucket covers the records from its id up to, excluding, the next
//!   bucket's id (the namespace end for the last bucket), and is never empty;
//! - a bucket's count and XOR equal a scan of the records it covers.
//!
//! Every write to the records table applies the matching change here in the
//! same transaction.

use std::ops::Bound;

use anyhow::{bail, Result};
use bytes::Bytes;
use redb::{ReadableTable, Table};

use super::{
    bounds::RecordsBounds,
    tables::{BucketValue, RecordsId, RecordsIdOwned, RecordsValue},
};
use crate::{ranger::record_fingerprint, NamespaceId};

/// Target number of records per bucket. A bucket splits when it exceeds
/// twice this and small neighbours merge up to this.
pub(crate) const BUCKET_SIZE: u64 = 512;

#[cfg(test)]
thread_local! {
    static BUCKET_SIZE_OVERRIDE: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Use a smaller bucket size on this thread until the guard drops, so tests
/// exercise splits, merges and partial buckets with few records.
#[cfg(test)]
pub(crate) struct BucketSizeGuard(Option<u64>);

#[cfg(test)]
impl BucketSizeGuard {
    pub(crate) fn new(size: u64) -> Self {
        Self(BUCKET_SIZE_OVERRIDE.with(|cell| cell.replace(Some(size))))
    }
}

#[cfg(test)]
impl Drop for BucketSizeGuard {
    fn drop(&mut self) {
        BUCKET_SIZE_OVERRIDE.with(|cell| cell.set(self.0));
    }
}

pub(super) fn bucket_size() -> u64 {
    #[cfg(test)]
    if let Some(size) = BUCKET_SIZE_OVERRIDE.with(|cell| cell.get()) {
        return size;
    }
    BUCKET_SIZE
}

static ZERO: [u8; 32] = [0u8; 32];

/// A bucket row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bucket {
    pub(crate) start: RecordsIdOwned,
    pub(crate) count: u64,
    pub(crate) xor: [u8; 32],
}

fn own(id: RecordsId<'_>) -> RecordsIdOwned {
    (*id.0, *id.1, Bytes::copy_from_slice(id.2))
}

fn borrow(id: &RecordsIdOwned) -> RecordsId<'_> {
    (&id.0, &id.1, &id.2[..])
}

fn bucket_of(key: RecordsId<'_>, value: BucketValue<'_>) -> Bucket {
    Bucket {
        start: own(key),
        count: value.0,
        xor: *value.1,
    }
}

fn xor_into(acc: &mut [u8; 32], other: &[u8; 32]) {
    for (a, b) in acc.iter_mut().zip(other) {
        *a ^= b;
    }
}

fn record_xor(key: RecordsId<'_>, value: RecordsValue<'_>) -> [u8; 32] {
    let (timestamp, _sig_ns, _sig_author, _len, hash) = value;
    record_fingerprint(key.0, key.1, key.2, timestamp, hash).0
}

fn put(
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    bucket: &Bucket,
) -> Result<()> {
    buckets.insert(borrow(&bucket.start), (bucket.count, &bucket.xor))?;
    Ok(())
}

fn floor(
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &[u8; 32],
    id: RecordsId<'_>,
) -> Result<Option<Bucket>> {
    let lower: RecordsId<'_> = (ns, &ZERO, &[]);
    let mut rows = buckets.range(lower..=id)?;
    rows.next_back()
        .map(|row| {
            let (key, value) = row?;
            Ok(bucket_of(key.value(), value.value()))
        })
        .transpose()
}

fn first(
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &[u8; 32],
) -> Result<Option<Bucket>> {
    let lower: RecordsId<'_> = (ns, &ZERO, &[]);
    let mut rows = buckets.range(lower..)?;
    next_in(&mut rows, ns)
}

fn after(
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &[u8; 32],
    id: RecordsId<'_>,
) -> Result<Option<Bucket>> {
    let mut rows = buckets.range::<RecordsId<'_>>((Bound::Excluded(id), Bound::Unbounded))?;
    next_in(&mut rows, ns)
}

fn before(
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &[u8; 32],
    id: RecordsId<'_>,
) -> Result<Option<Bucket>> {
    let lower: RecordsId<'_> = (ns, &ZERO, &[]);
    let mut rows = buckets.range(lower..id)?;
    rows.next_back()
        .map(|row| {
            let (key, value) = row?;
            Ok(bucket_of(key.value(), value.value()))
        })
        .transpose()
}

/// The next row of `rows` if it is in namespace `ns`.
fn next_in(
    rows: &mut redb::Range<'_, RecordsId<'static>, BucketValue<'static>>,
    ns: &[u8; 32],
) -> Result<Option<Bucket>> {
    match rows.next() {
        None => Ok(None),
        Some(row) => {
            let (key, value) = row?;
            let key = key.value();
            Ok((key.0 == ns).then(|| bucket_of(key, value.value())))
        }
    }
}

/// Whether `ns` has records but no bucket rows yet.
pub(super) fn is_unbuilt(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
) -> Result<bool> {
    let ns = ns.to_bytes();
    if first(buckets, &ns)?.is_some() {
        return Ok(false);
    }
    let bounds = RecordsBounds::namespace(NamespaceId::from(&ns));
    Ok(records.range(bounds.as_ref())?.next().is_some())
}

/// Replace `ns`'s bucket rows with a fresh partition into buckets of
/// [`BUCKET_SIZE`] records. Returns the XOR of all the namespace's record
/// fingerprints (no empty-set base).
pub(super) fn rebuild(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
) -> Result<[u8; 32]> {
    let bounds = RecordsBounds::namespace(*ns);
    buckets.retain_in(bounds.as_ref(), |_k, _v| false)?;
    let size = bucket_size();
    let mut total = [0u8; 32];
    let mut current: Option<Bucket> = None;
    for row in records.range(bounds.as_ref())? {
        let (key, value) = row?;
        let (key, value) = (key.value(), value.value());
        let fp = record_xor(key, value);
        xor_into(&mut total, &fp);
        let bucket = current.get_or_insert_with(|| Bucket {
            start: own(key),
            count: 0,
            xor: [0u8; 32],
        });
        bucket.count += 1;
        xor_into(&mut bucket.xor, &fp);
        if bucket.count == size {
            put(buckets, bucket)?;
            current = None;
        }
    }
    if let Some(bucket) = current {
        put(buckets, &bucket)?;
    }
    Ok(total)
}

/// Account for a record the caller has just written to the records table.
///
/// `delta` is the XOR of the new record's fingerprint and, for an overwrite
/// (`is_new == false`), the superseded record's. A namespace that has not
/// been built yet is left alone: the build covers the new record.
pub(super) fn insert(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    id: RecordsId<'_>,
    delta: [u8; 32],
    is_new: bool,
) -> Result<()> {
    let ns = id.0;
    let mut bucket = match floor(buckets, ns, id)? {
        Some(mut bucket) => {
            if is_new {
                bucket.count += 1;
            }
            xor_into(&mut bucket.xor, &delta);
            put(buckets, &bucket)?;
            bucket
        }
        None => {
            if !is_new {
                return Ok(());
            }
            match first(buckets, ns)? {
                // The record sorts before every bucket: the first bucket
                // now starts at it.
                Some(mut bucket) => {
                    buckets.remove(borrow(&bucket.start))?;
                    bucket.start = own(id);
                    bucket.count += 1;
                    xor_into(&mut bucket.xor, &delta);
                    put(buckets, &bucket)?;
                    bucket
                }
                None => {
                    let bounds = RecordsBounds::namespace(NamespaceId::from(ns));
                    let mut others = records.range(bounds.as_ref())?;
                    let unbuilt = others.any(|row| match row {
                        Ok((key, _)) => key.value() != id,
                        Err(_) => true,
                    });
                    if unbuilt {
                        return Ok(());
                    }
                    let bucket = Bucket {
                        start: own(id),
                        count: 1,
                        xor: delta,
                    };
                    put(buckets, &bucket)?;
                    bucket
                }
            }
        }
    };
    if bucket.count > 2 * bucket_size() {
        split(records, buckets, &mut bucket)?;
    }
    Ok(())
}

/// Split `bucket` into two halves at its middle record.
fn split(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    bucket: &mut Bucket,
) -> Result<()> {
    let half = bucket.count / 2;
    let mut lower = Bucket {
        start: bucket.start.clone(),
        count: half,
        xor: [0u8; 32],
    };
    let mut upper = Bucket {
        start: bucket.start.clone(),
        count: bucket.count - half,
        xor: [0u8; 32],
    };
    let mut seen = 0u64;
    for row in records
        .range(borrow(&bucket.start)..)?
        .take(bucket.count as usize)
    {
        let (key, value) = row?;
        let (key, value) = (key.value(), value.value());
        let fp = record_xor(key, value);
        if seen < half {
            xor_into(&mut lower.xor, &fp);
        } else {
            if seen == half {
                upper.start = own(key);
            }
            xor_into(&mut upper.xor, &fp);
        }
        seen += 1;
    }
    if seen != bucket.count {
        bail!(
            "bucket covers {} records but {} were found",
            bucket.count,
            seen
        );
    }
    put(buckets, &lower)?;
    put(buckets, &upper)?;
    Ok(())
}

/// Account for a record the caller has just removed from the records table,
/// and restore the bucket invariants.
#[cfg(test)]
pub(super) fn remove(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    id: RecordsId<'_>,
    fingerprint: [u8; 32],
) -> Result<()> {
    let mut touched = Vec::new();
    remove_counted(buckets, id, fingerprint, &mut touched)?;
    for start in touched {
        settle(records, buckets, &start)?;
    }
    Ok(())
}

/// Subtract a removed record from its bucket. The bucket's key may name the
/// removed record until [`settle`] runs for the key pushed to `touched`, so
/// a caller removing many records settles once at the end.
pub(super) fn remove_counted(
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    id: RecordsId<'_>,
    fingerprint: [u8; 32],
    touched: &mut Vec<RecordsIdOwned>,
) -> Result<()> {
    // No row means the namespace has not been built yet.
    let Some(mut bucket) = floor(buckets, id.0, id)? else {
        return Ok(());
    };
    if bucket.count == 0 {
        bail!("empty bucket");
    }
    bucket.count -= 1;
    xor_into(&mut bucket.xor, &fingerprint);
    if bucket.count == 0 {
        buckets.remove(borrow(&bucket.start))?;
    } else {
        put(buckets, &bucket)?;
        if touched.last() != Some(&bucket.start) {
            touched.push(bucket.start);
        }
    }
    Ok(())
}

/// Restore the invariants of the bucket keyed `start` after removals: key it
/// by its first remaining record, and merge it into a neighbour if it has
/// become small.
pub(super) fn settle(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    start: &RecordsIdOwned,
) -> Result<()> {
    let ns = &start.0;
    let Some(mut bucket) = buckets
        .get(borrow(start))?
        .map(|v| bucket_of(borrow(start), v.value()))
    else {
        return Ok(());
    };
    if records.get(borrow(start))?.is_none() {
        let bounds = (Bound::Excluded(borrow(start)), Bound::Unbounded);
        let next = match records.range::<RecordsId<'_>>(bounds)?.next() {
            Some(row) => own(row?.0.value()),
            None => bail!("bucket without records"),
        };
        if &next.0 != ns {
            bail!("bucket without records");
        }
        buckets.remove(borrow(start))?;
        bucket.start = next;
        put(buckets, &bucket)?;
    }
    let size = bucket_size();
    if bucket.count * 4 >= size {
        return Ok(());
    }
    if let Some(next) = after(buckets, ns, borrow(&bucket.start))? {
        if bucket.count + next.count <= size {
            buckets.remove(borrow(&next.start))?;
            bucket.count += next.count;
            xor_into(&mut bucket.xor, &next.xor);
            put(buckets, &bucket)?;
            return Ok(());
        }
    }
    if let Some(mut previous) = before(buckets, ns, borrow(&bucket.start))? {
        if previous.count + bucket.count <= size {
            buckets.remove(borrow(&bucket.start))?;
            previous.count += bucket.count;
            xor_into(&mut previous.xor, &bucket.xor);
            put(buckets, &previous)?;
        }
    }
    Ok(())
}

/// Drop all of `ns`'s bucket rows.
pub(super) fn clear(
    buckets: &mut Table<'_, RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
) -> Result<()> {
    let bounds = RecordsBounds::namespace(*ns);
    buckets.retain_in(bounds.as_ref(), |_k, _v| false)?;
    Ok(())
}

fn past_end(end: Bound<&RecordsIdOwned>, start: &RecordsIdOwned) -> bool {
    match end {
        Bound::Unbounded => false,
        Bound::Excluded(end) => start >= end,
        Bound::Included(end) => start > end,
    }
}

fn starts_within(from: Bound<&RecordsIdOwned>, start: &RecordsIdOwned) -> bool {
    match from {
        Bound::Unbounded => true,
        Bound::Included(from) => from <= start,
        Bound::Excluded(from) => from < start,
    }
}

/// Whether every record below `limit` is within `end`. `limit` is the next
/// bucket's id, or the namespace end for the last bucket.
fn ends_within(end: Bound<&RecordsIdOwned>, limit: Bound<&RecordsIdOwned>) -> bool {
    match (end, limit) {
        (Bound::Unbounded, _) => true,
        (_, Bound::Unbounded) => false,
        (
            Bound::Included(end) | Bound::Excluded(end),
            Bound::Included(limit) | Bound::Excluded(limit),
        ) => limit <= end,
    }
}

/// One bucket met by [`walk`].
struct Visit<'a> {
    bucket: &'a Bucket,
    /// Whether the range covers every record of the bucket.
    covered: bool,
    /// The records of the bucket inside the range, when it is not covered.
    partial: RecordsBounds,
    /// The next bucket's id, if any.
    end: Option<&'a RecordsIdOwned>,
}

/// Visit, in order, every bucket of `ns` that can hold records inside
/// `bounds`, until `visit` returns false.
fn walk(
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
    bounds: &RecordsBounds,
    mut visit: impl FnMut(Visit<'_>) -> Result<bool>,
) -> Result<()> {
    use std::ops::RangeBounds;
    let ns_bytes = ns.to_bytes();
    let namespace = RecordsBounds::namespace(*ns);
    let (from, to) = (bounds.start_bound(), bounds.end_bound());
    let begin = match from {
        Bound::Unbounded => None,
        Bound::Included(from) | Bound::Excluded(from) => floor(buckets, &ns_bytes, borrow(from))?,
    };
    let begin = match begin {
        Some(bucket) => bucket.start,
        None => (ns_bytes, [0u8; 32], Bytes::new()),
    };
    let mut rows = buckets.range(borrow(&begin)..)?;
    let mut current = next_in(&mut rows, &ns_bytes)?;
    while let Some(bucket) = current {
        let next = next_in(&mut rows, &ns_bytes)?;
        if past_end(to, &bucket.start) {
            break;
        }
        let end = next.as_ref().map(|next| &next.start);
        let limit = end.map_or(namespace.end_bound(), Bound::Excluded);
        let covered = starts_within(from, &bucket.start) && ends_within(to, limit);
        let lower = if starts_within(from, &bucket.start) {
            Bound::Included(bucket.start.clone())
        } else {
            clone_bound(from)
        };
        let partial = RecordsBounds::new(lower, clone_bound(to));
        if !visit(Visit {
            bucket: &bucket,
            covered,
            partial,
            end,
        })? {
            break;
        }
        current = next;
    }
    Ok(())
}

fn clone_bound(bound: Bound<&RecordsIdOwned>) -> Bound<RecordsIdOwned> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(id) => Bound::Included(id.clone()),
        Bound::Excluded(id) => Bound::Excluded(id.clone()),
    }
}

/// The records of a partially covered bucket that lie inside the range.
fn partial_records<'a>(
    records: &'a impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    visit: &Visit<'_>,
) -> Result<impl Iterator<Item = Result<(RecordsIdOwned, [u8; 32])>> + 'a> {
    let end = visit.end.cloned();
    let rows = records.range(visit.partial.as_ref())?;
    Ok(rows
        .map(|row| {
            let (key, value) = row?;
            let (key, value) = (key.value(), value.value());
            Ok((own(key), record_xor(key, value)))
        })
        .take_while(move |row| match (row, &end) {
            (Ok((id, _)), Some(end)) => id < end,
            _ => true,
        }))
}

/// The count and fingerprint XOR of a range.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Aggregate {
    pub(crate) count: u64,
    pub(crate) xor: [u8; 32],
    /// Records read from the records table to answer (those of partially
    /// covered buckets).
    pub(crate) scanned: u64,
}

/// Count the records inside `bounds` and XOR their fingerprints.
pub(super) fn aggregate(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
    bounds: &RecordsBounds,
    with_xor: bool,
) -> Result<Aggregate> {
    let mut out = Aggregate::default();
    walk(buckets, ns, bounds, |visit| {
        if visit.covered {
            out.count += visit.bucket.count;
            xor_into(&mut out.xor, &visit.bucket.xor);
        } else if with_xor {
            for row in partial_records(records, &visit)? {
                let (_, fp) = row?;
                out.count += 1;
                out.scanned += 1;
                xor_into(&mut out.xor, &fp);
            }
        } else {
            // Counting needs no hashing, only the keys.
            let end = visit.end.cloned();
            for row in records.range(visit.partial.as_ref())? {
                let (key, _) = row?;
                if end.as_ref().is_some_and(|end| key.value() >= borrow(end)) {
                    break;
                }
                out.count += 1;
                out.scanned += 1;
            }
        }
        Ok(true)
    })?;
    Ok(out)
}

/// The result of [`nth`].
#[derive(Debug)]
pub(crate) enum Nth {
    /// The record at the offset.
    Found { id: RecordsIdOwned, scanned: u64 },
    /// The range holds `total` records, not more than the offset.
    Short { total: u64, scanned: u64 },
}

/// The id of the record at `offset` within `bounds`, in records-table order.
pub(super) fn nth(
    records: &impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
    bounds: &RecordsBounds,
    offset: u64,
) -> Result<Nth> {
    let mut remaining = offset;
    let mut total = 0u64;
    let mut scanned = 0u64;
    let mut found = None;
    walk(buckets, ns, bounds, |visit| {
        if visit.covered {
            if remaining < visit.bucket.count {
                let mut rows = records.range(borrow(&visit.bucket.start)..)?;
                let row = rows.nth(remaining as usize);
                scanned += remaining + 1;
                found = match row {
                    Some(row) => Some(own(row?.0.value())),
                    None => bail!("bucket covers fewer records than counted"),
                };
                return Ok(false);
            }
            remaining -= visit.bucket.count;
            total += visit.bucket.count;
            return Ok(true);
        }
        let end = visit.end.cloned();
        for row in records.range(visit.partial.as_ref())? {
            let (key, _) = row?;
            let key = key.value();
            if end.as_ref().is_some_and(|end| key >= borrow(end)) {
                break;
            }
            scanned += 1;
            total += 1;
            if remaining == 0 {
                found = Some(own(key));
                return Ok(false);
            }
            remaining -= 1;
        }
        Ok(true)
    })?;
    Ok(match found {
        Some(id) => Nth::Found { id, scanned },
        None => Nth::Short { total, scanned },
    })
}

/// Every bucket row of `ns`, in order.
#[cfg(test)]
pub(super) fn all(
    buckets: &impl ReadableTable<RecordsId<'static>, BucketValue<'static>>,
    ns: &NamespaceId,
) -> Result<Vec<Bucket>> {
    let ns = ns.to_bytes();
    let mut out = Vec::new();
    let lower: RecordsId<'_> = (&ns, &ZERO, &[]);
    let mut rows = buckets.range(lower..)?;
    while let Some(bucket) = next_in(&mut rows, &ns)? {
        out.push(bucket);
    }
    Ok(out)
}

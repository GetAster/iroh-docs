//! Ranges and helpers for working with [`redb`] tables

use redb::{Key, Range, ReadOnlyTable, ReadableTable, Table, Value};

use super::{
    bounds::{ByKeyBounds, RecordsBounds},
    into_entry,
    tables::{RecordsByKeyId, RecordsId, RecordsValue},
};
use crate::{store::SortDirection, SignedEntry};

/// An extension trait for [`Range`] that provides methods for mapped retrieval.
pub trait RangeExt<K: Key, V: Value> {
    /// Get the next entry and map the item with a callback function.
    fn next_map<T>(
        &mut self,
        map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> T,
    ) -> Option<anyhow::Result<T>>;

    /// Get the next entry, but only if the callback function returns Some, otherwise continue.
    ///
    /// With `direction` the range can be either process in forward or backward direction.
    fn next_filter_map<T>(
        &mut self,
        direction: &SortDirection,
        filter_map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> Option<T>,
    ) -> Option<anyhow::Result<T>>;

    /// Like [`Self::next_filter_map`], but the callback returns a `Result`, and the result is
    /// flattened with the result from the range operation.
    fn next_try_filter_map<T>(
        &mut self,
        direction: &SortDirection,
        filter_map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> Option<anyhow::Result<T>>,
    ) -> Option<anyhow::Result<T>> {
        Some(self.next_filter_map(direction, filter_map)?.and_then(|r| r))
    }
}

impl<K: Key + 'static, V: Value + 'static> RangeExt<K, V> for Range<'_, K, V> {
    fn next_map<T>(
        &mut self,
        map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> T,
    ) -> Option<anyhow::Result<T>> {
        self.next()
            .map(|r| r.map_err(Into::into).map(|r| map(r.0.value(), r.1.value())))
    }

    fn next_filter_map<T>(
        &mut self,
        direction: &SortDirection,
        filter_map: impl for<'x> Fn(K::SelfType<'x>, V::SelfType<'x>) -> Option<T>,
    ) -> Option<anyhow::Result<T>> {
        loop {
            let next = match direction {
                SortDirection::Asc => self.next(),
                SortDirection::Desc => self.next_back(),
            };
            match next {
                None => break None,
                Some(Err(err)) => break Some(Err(err.into())),
                Some(Ok(res)) => match filter_map(res.0.value(), res.1.value()) {
                    None => continue,
                    Some(item) => break Some(Ok(item)),
                },
            }
        }
    }
}

/// An iterator over a range of entries from the records table.
#[derive(derive_more::Debug)]
#[debug("RecordsRange")]
pub struct RecordsRange<'a>(Range<'a, RecordsId<'static>, RecordsValue<'static>>);

// pub type RecordsRange<'a> = Range<'a, RecordsId<'static>, RecordsValue<'static>>;

impl<'a> RecordsRange<'a> {
    pub(super) fn with_bounds(
        records: &'a impl ReadableTable<RecordsId<'static>, RecordsValue<'static>>,
        bounds: RecordsBounds,
    ) -> anyhow::Result<Self> {
        let range = records.range(bounds.as_ref())?;
        Ok(Self(range))
    }

    //
    /// Get the next item in the range.
    ///
    /// Omit items for which the `matcher` function returns false.
    pub(super) fn next_filtered(
        &mut self,
        direction: &SortDirection,
        filter: impl for<'x> Fn(RecordsId<'x>, RecordsValue<'x>) -> bool,
    ) -> Option<anyhow::Result<SignedEntry>> {
        self.0
            .next_filter_map(direction, |k, v| filter(k, v).then(|| into_entry(k, v)))
    }
}

impl RecordsRange<'static> {
    pub(super) fn all_static(
        records: &ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>>,
    ) -> anyhow::Result<Self> {
        let range = records.range::<RecordsId<'static>>(..)?;
        Ok(Self(range))
    }
    pub(super) fn with_bounds_static(
        records: &ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>>,
        bounds: RecordsBounds,
    ) -> anyhow::Result<Self> {
        let range = records.range(bounds.as_ref())?;
        Ok(Self(range))
    }
}

impl Iterator for RecordsRange<'_> {
    type Item = anyhow::Result<SignedEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_map(into_entry)
    }
}

/// Point lookups into the records table, from a read snapshot or from the
/// open write transaction.
pub trait RecordsLookup {
    /// The entry stored under `id`, if any.
    fn lookup(&self, id: &RecordsId<'_>) -> anyhow::Result<Option<SignedEntry>>;
}

impl RecordsLookup for ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>> {
    fn lookup(&self, id: &RecordsId<'_>) -> anyhow::Result<Option<SignedEntry>> {
        Ok(self.get(id)?.map(|value| into_entry(*id, value.value())))
    }
}

impl RecordsLookup for &Table<'_, RecordsId<'static>, RecordsValue<'static>> {
    fn lookup(&self, id: &RecordsId<'_>) -> anyhow::Result<Option<SignedEntry>> {
        Ok(self.get(id)?.map(|value| into_entry(*id, value.value())))
    }
}

/// An iterator over a range of the by-key index, resolving each row in the
/// records table `R`.
#[derive(derive_more::Debug)]
#[debug("RecordsByKeyRange")]
pub struct RecordsByKeyRange<'a, R = ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>>> {
    records_table: R,
    by_key_range: Range<'a, RecordsByKeyId<'static>, ()>,
}

impl RecordsByKeyRange<'static> {
    pub fn with_bounds_static(
        records_by_key_table: ReadOnlyTable<RecordsByKeyId<'static>, ()>,
        records_table: ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>>,
        bounds: ByKeyBounds,
    ) -> anyhow::Result<Self> {
        let by_key_range = records_by_key_table.range(bounds.as_ref())?;
        Ok(Self {
            records_table,
            by_key_range,
        })
    }
}

impl<'a, R: RecordsLookup> RecordsByKeyRange<'a, R> {
    pub fn with_bounds(
        records_by_key_table: &'a impl ReadableTable<RecordsByKeyId<'static>, ()>,
        records_table: R,
        bounds: ByKeyBounds,
    ) -> anyhow::Result<Self> {
        let by_key_range = records_by_key_table.range(bounds.as_ref())?;
        Ok(Self {
            records_table,
            by_key_range,
        })
    }

    /// Get the next item in the range.
    ///
    /// Omit items for which the `filter` function returns false.
    pub fn next_filtered(
        &mut self,
        direction: &SortDirection,
        filter: impl for<'x> Fn(RecordsByKeyId<'x>) -> bool,
    ) -> Option<anyhow::Result<SignedEntry>> {
        self.by_key_range.next_try_filter_map(direction, |k, _v| {
            if !filter(k) {
                return None;
            };
            let (namespace, key, author) = k;
            let records_id = (namespace, author, key);
            self.records_table.lookup(&records_id).transpose()
        })
    }
}

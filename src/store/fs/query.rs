use anyhow::Result;
use iroh_blobs::Hash;
use redb::{ReadOnlyTable, Table};

use super::{
    bounds::{ByKeyBounds, RecordsBounds},
    ranges::{RecordsByKeyRange, RecordsLookup, RecordsRange},
    tables::{RecordsId, Tables},
    RecordsValue,
};
use crate::{
    store::{
        fs::tables::ReadOnlyTables,
        util::{IndexKind, LatestPerKeySelector, SelectorRes},
        AuthorFilter, KeyFilter, Query,
    },
    AuthorId, NamespaceId, SignedEntry,
};

/// A query iterator for entry queries.
///
/// Reads either a read snapshot it owns, or the open write transaction's
/// tables it borrows; the records table type `R` says which.
#[derive(Debug)]
pub struct QueryIterator<'a, R = ReadOnlyTable<RecordsId<'static>, RecordsValue<'static>>> {
    range: QueryRange<'a, R>,
    query: Query,
    offset: u64,
    count: u64,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum QueryRange<'a, R> {
    AuthorKey {
        range: RecordsRange<'a>,
        key_filter: KeyFilter,
    },
    KeyAuthor {
        range: RecordsByKeyRange<'a, R>,
        author_filter: AuthorFilter,
        selector: Option<LatestPerKeySelector>,
    },
}

/// Which index a query reads, with its bounds and residual filters.
#[allow(clippy::large_enum_variant)]
enum Plan {
    AuthorKey {
        bounds: RecordsBounds,
        key_filter: KeyFilter,
    },
    KeyAuthor {
        bounds: ByKeyBounds,
        author_filter: AuthorFilter,
        selector: Option<LatestPerKeySelector>,
    },
}

impl Plan {
    fn new(namespace: NamespaceId, query: &Query) -> Self {
        match IndexKind::from(query) {
            IndexKind::AuthorKey { range, key_filter } => {
                let (bounds, key_filter) = match range {
                    // single author: both author and key are selected via the range. therefore
                    // set `filter` to `Any`.
                    AuthorFilter::Exact(author) => (
                        RecordsBounds::author_key(namespace, author, key_filter),
                        KeyFilter::Any,
                    ),
                    // no author set => full table scan with the provided key filter
                    AuthorFilter::Any => (RecordsBounds::namespace(namespace), key_filter),
                };
                Plan::AuthorKey { bounds, key_filter }
            }
            IndexKind::KeyAuthor {
                range,
                author_filter,
                latest_per_key,
            } => Plan::KeyAuthor {
                bounds: ByKeyBounds::new(namespace, &range),
                author_filter,
                selector: latest_per_key.then(LatestPerKeySelector::default),
            },
        }
    }
}

impl QueryIterator<'static> {
    /// A query over an owned read snapshot.
    pub fn new(tables: ReadOnlyTables, namespace: NamespaceId, query: Query) -> Result<Self> {
        let range = match Plan::new(namespace, &query) {
            Plan::AuthorKey { bounds, key_filter } => QueryRange::AuthorKey {
                range: RecordsRange::with_bounds_static(&tables.records, bounds)?,
                key_filter,
            },
            Plan::KeyAuthor {
                bounds,
                author_filter,
                selector,
            } => QueryRange::KeyAuthor {
                range: RecordsByKeyRange::with_bounds_static(
                    tables.records_by_key,
                    tables.records,
                    bounds,
                )?,
                author_filter,
                selector,
            },
        };
        Ok(Self::with_range(range, query))
    }
}

impl<'a, 'tx> QueryIterator<'a, &'a Table<'tx, RecordsId<'static>, RecordsValue<'static>>> {
    /// A query over the open write transaction's tables, which sees its
    /// uncommitted writes.
    pub fn borrowed(tables: &'a Tables<'tx>, namespace: NamespaceId, query: Query) -> Result<Self> {
        let range = match Plan::new(namespace, &query) {
            Plan::AuthorKey { bounds, key_filter } => QueryRange::AuthorKey {
                range: RecordsRange::with_bounds(&tables.records, bounds)?,
                key_filter,
            },
            Plan::KeyAuthor {
                bounds,
                author_filter,
                selector,
            } => QueryRange::KeyAuthor {
                range: RecordsByKeyRange::with_bounds(
                    &tables.records_by_key,
                    &tables.records,
                    bounds,
                )?,
                author_filter,
                selector,
            },
        };
        Ok(Self::with_range(range, query))
    }
}

impl<'a, R> QueryIterator<'a, R> {
    fn with_range(range: QueryRange<'a, R>, query: Query) -> Self {
        Self {
            range,
            query,
            offset: 0,
            count: 0,
        }
    }
}

impl<R: RecordsLookup> Iterator for QueryIterator<'_, R> {
    type Item = Result<SignedEntry>;

    fn next(&mut self) -> Option<Result<SignedEntry>> {
        // early-return if we reached the query limit.
        if let Some(limit) = self.query.limit() {
            if self.count >= limit {
                return None;
            }
        }
        loop {
            let next = match &mut self.range {
                QueryRange::AuthorKey { range, key_filter } => {
                    // get the next entry from the query range, filtered by the key and empty filters
                    range.next_filtered(&self.query.sort_direction, |(_ns, _author, key), value| {
                        key_filter.matches(key)
                            && (self.query.include_empty || !value_is_empty(&value))
                    })
                }

                QueryRange::KeyAuthor {
                    range,
                    author_filter,
                    selector,
                } => loop {
                    // get the next entry from the query range, filtered by the author filter
                    let next = range
                        .next_filtered(&self.query.sort_direction, |(_ns, _key, author)| {
                            author_filter.matches(&(AuthorId::from(author)))
                        });

                    // early-break if next contains Err
                    let next = match next.transpose() {
                        Err(err) => break Some(Err(err)),
                        Ok(next) => next,
                    };

                    // push the entry into the selector. if active, only the latest entry
                    // for each key will be emitted.
                    let next = match selector {
                        None => next,
                        Some(selector) => match selector.push(next) {
                            SelectorRes::Continue => continue,
                            SelectorRes::Finished => None,
                            SelectorRes::Some(res) => Some(res),
                        },
                    };

                    // skip the entry if empty and no empty entries requested
                    if !self.query.include_empty && matches!(&next, Some(e) if e.is_empty()) {
                        continue;
                    }

                    break next.map(Result::Ok);
                },
            };

            // skip the entry if we didn't get past the requested offset yet.
            if self.offset < self.query.offset() && matches!(next, Some(Ok(_))) {
                self.offset += 1;
                continue;
            }

            self.count += 1;
            return next;
        }
    }
}

fn value_is_empty(value: &RecordsValue) -> bool {
    let (_timestamp, _namespace_sig, _author_sig, _len, hash) = value;
    *hash == Hash::EMPTY.as_bytes()
}

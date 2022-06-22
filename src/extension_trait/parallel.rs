use std::{cmp::Ordering, io};

use crate::{
    orderer::{FuncOrderer, KeyOrderer, OrdOrderer},
    sorter::{result_iter::ResultIterator, ExtsortConfig},
    ExtSortByExtension, ExtSortOrdExtension,
};

pub struct ParallelResultIterator<'a, T, O> {
    _inner: ResultIterator<'a, T, O>,
}

pub trait ParallelExtSortOrdExtension<'a>: ExtSortOrdExtension<'a>
where
    Self::Item: Send,
{
    fn par_external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, OrdOrderer>>;
}

pub trait ParallelExtSortExtension<'a>: ExtSortByExtension<'a>
where
    Self::Item: Send,
{
    fn par_external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;

    fn par_external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K,
        K: Ord;
}

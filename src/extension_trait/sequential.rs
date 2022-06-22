use std::{cmp::Ordering, io};

use crate::{
    orderer::{FuncOrderer, KeyOrderer, OrdOrderer},
    sorter::{self, result_iter::ResultIterator, ExtsortConfig},
};

pub trait ExtSortOrdExtension<'a>: Iterator {
    fn external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ResultIterator<'a, Self::Item, OrdOrderer>>;
}

impl<'a, I, T> ExtSortOrdExtension<'a> for I
where
    I: Iterator<Item = T>,
    T: Ord + 'a,
{
    fn external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ResultIterator<'a, Self::Item, OrdOrderer>> {
        sorter::ExtSorter::new(options).run(self, OrdOrderer::new())
    }
}

pub trait ExtSortByExtension<'a>: Iterator {
    fn external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ResultIterator<'a, Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;

    fn external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ResultIterator<'a, Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K,
        K: Ord;
}

impl<'a, I, T> ExtSortByExtension<'a> for I
where
    I: Iterator<Item = T>,
    T: 'a,
{
    fn external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ResultIterator<'a, Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering,
    {
        sorter::ExtSorter::new(options).run(self, FuncOrderer::new(comparator))
    }

    fn external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ResultIterator<'a, Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K,
        K: Ord,
    {
        sorter::ExtSorter::new(options).run(self, KeyOrderer::new(key_extractor))
    }
}
// impl<'a, I, T> ExtSortByExtension

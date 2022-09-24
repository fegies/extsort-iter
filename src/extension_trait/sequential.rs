use std::{cmp::Ordering, io};

use crate::{
    orderer::{FuncOrderer, KeyOrderer, OrdOrderer, Orderer},
    sorter::{self, result_iter::ResultIterator, ExtsortConfig},
};

pub trait ExtSortOrdExtension<'a>: Iterator {
    /// Sorts the provided Iterator according to the provided config
    /// using the native ordering on the type to sort
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
    fn external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ResultIterator<'a, Self::Item, OrdOrderer>>;
}

fn buffer_sort<T>(orderer: &impl Orderer<T>, buffer: &mut [T]) {
    buffer.sort_unstable_by(|a, b| orderer.compare(a, b));
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
        sorter::ExtSorter::new(options).run(self, OrdOrderer::new(), buffer_sort)
    }
}

pub trait ExtSortByExtension<'a>: Iterator {
    /// Sorts the provided Iterator according to the provided config
    /// using a custom comparator function
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
    fn external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ResultIterator<'a, Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering;

    /// Sorts the provided Iterator according to the provided config
    /// using a key extraction function.
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
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
        sorter::ExtSorter::new(options).run(self, FuncOrderer::new(comparator), buffer_sort)
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
        sorter::ExtSorter::new(options).run(self, KeyOrderer::new(key_extractor), buffer_sort)
    }
}
// impl<'a, I, T> ExtSortByExtension

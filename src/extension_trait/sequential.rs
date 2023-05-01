use std::{
    cmp::Ordering,
    io::{self, Read},
};

use crate::{
    orderer::{FuncOrderer, KeyOrderer, OrdOrderer, Orderer},
    run::{file_run::ExternalRun, Run},
    sorter::{self, result_iter::ResultIterator, ExtsortConfig},
};

pub trait ExtSortOrdExtension: Iterator {
    /// Sorts the provided Iterator according to the provided config
    /// using the native ordering on the type to sort
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
    fn external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ResultIterator<Self::Item, OrdOrderer>>;
}

fn buffer_sort<T>(orderer: &impl Orderer<T>, buffer: &mut [T]) {
    buffer.sort_unstable_by(|a, b| orderer.compare(a, b));
}

impl<I, T> ExtSortOrdExtension for I
where
    I: Iterator<Item = T>,
    T: Ord,
{
    fn external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ResultIterator<Self::Item, OrdOrderer>> {
        sorter::ExtSorter::new(options).run(self, OrdOrderer::new(), buffer_sort)
    }
}

pub trait ExtSortByExtension: Iterator {
    type Run: Run<Self::Item>;
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
    ) -> io::Result<ResultIterator<Self::Item, FuncOrderer<F>>>
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
    ) -> io::Result<ResultIterator<Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K,
        K: Ord;
}

impl<I, T> ExtSortByExtension for I
where
    I: Iterator<Item = T>,
{
    type Run = ExternalRun<T, Box<dyn Read>>;

    fn external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ResultIterator<Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering,
    {
        sorter::ExtSorter::new(options).run(self, FuncOrderer::new(comparator), buffer_sort)
    }

    fn external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ResultIterator<Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K,
        K: Ord,
    {
        sorter::ExtSorter::new(options).run(self, KeyOrderer::new(key_extractor), buffer_sort)
    }
}

use std::{cmp::Ordering, io};

use rayon::slice::ParallelSliceMut;

use crate::{
    orderer::{FuncOrderer, KeyOrderer, OrdOrderer, Orderer},
    sorter::{
        self, buffer_cleaner::threaded::MultithreadedBufferCleaner, result_iter::ResultIterator,
        ExtsortConfig,
    },
};

/// The specific iterator type returned by
/// the parallel sorting implementations.
pub struct ParallelResultIterator<T, O> {
    inner: ResultIterator<T, O>,
}

impl<T, O> Iterator for ParallelResultIterator<T, O>
where
    O: Orderer<T>,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
impl<T, O> ExactSizeIterator for ParallelResultIterator<T, O> where O: Orderer<T> {}

fn buffer_sort<T, O>(orderer: &O, buffer: &mut [T])
where
    T: Send,
    O: Orderer<T> + Sync,
{
    buffer.par_sort_unstable_by(|a, b| orderer.compare(a, b));
}

fn run<T, O>(
    source: impl Iterator<Item = T>,
    options: ExtsortConfig,
    orderer: O,
) -> io::Result<ParallelResultIterator<T, O>>
where
    O: Orderer<T> + Send + Sync,
    T: Send,
{
    let cleaner = MultithreadedBufferCleaner::new(options, orderer, buffer_sort);
    cleaner.run(move |cleaner_handle| {
        let inner = sorter::ExtSorter::new().run(source, cleaner_handle)?;
        Ok(ParallelResultIterator { inner })
    })
}

pub trait ParallelExtSortOrdExtension: Iterator
where
    Self::Item: Send,
{
    /// Sorts the provided Iterator according to the provided config
    /// the native ordering specified on the iterated type.
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
    fn par_external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ParallelResultIterator<Self::Item, OrdOrderer>>;
}

pub trait ParallelExtSortExtension: Iterator
where
    Self::Item: Send,
{
    /// Sorts the provided Iterator according to the provided config
    /// using a custom comparison function.
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
    fn par_external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ParallelResultIterator<Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering + Send + Sync;

    /// Sorts the provided Iterator according to the provided config
    /// using a key extraction function.
    /// # Errors
    /// This function may error if a sort file fails to be written.
    /// In this case the library will do its best to clean up the
    /// already written files, but no guarantee is made.
    fn par_external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ParallelResultIterator<Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K + Send + Sync,
        K: Ord;
}

impl<I, T> ParallelExtSortOrdExtension for I
where
    I: Iterator<Item = T>,
    T: Send + Ord,
{
    fn par_external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ParallelResultIterator<Self::Item, OrdOrderer>> {
        run(self, options, OrdOrderer::new())
    }
}

impl<I, T> ParallelExtSortExtension for I
where
    I: Iterator<Item = T>,
    T: Send,
{
    fn par_external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ParallelResultIterator<Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering + Send + Sync,
    {
        run(self, options, FuncOrderer::new(comparator))
    }

    fn par_external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ParallelResultIterator<Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K + Send + Sync,
        K: Ord,
    {
        run(self, options, KeyOrderer::new(key_extractor))
    }
}

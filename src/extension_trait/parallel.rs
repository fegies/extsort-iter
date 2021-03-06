use std::{cmp::Ordering, io};

use rayon::slice::ParallelSliceMut;

use crate::{
    orderer::{FuncOrderer, KeyOrderer, OrdOrderer, Orderer},
    sorter::{self, result_iter::ResultIterator, ExtsortConfig},
};

pub struct ParallelResultIterator<'a, T, O> {
    inner: ResultIterator<'a, T, O>,
}

impl<T, O> Iterator for ParallelResultIterator<'_, T, O>
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
impl<T, O> ExactSizeIterator for ParallelResultIterator<'_, T, O> where O: Orderer<T> {}

fn buffer_sort<T, O>(orderer: &O, buffer: &mut [T])
where
    T: Send,
    O: Orderer<T> + Sync,
{
    buffer.par_sort_unstable_by(|a, b| orderer.compare(a, b))
}

pub trait ParallelExtSortOrdExtension<'a>: Iterator
where
    Self::Item: Send,
{
    fn par_external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, OrdOrderer>>;
}

pub trait ParallelExtSortExtension<'a>: Iterator
where
    Self::Item: Send,
{
    fn par_external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering + Send + Sync;

    fn par_external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K + Send + Sync,
        K: Ord;
}

impl<'a, I, T> ParallelExtSortOrdExtension<'a> for I
where
    I: Iterator<Item = T>,
    T: Send + Ord + 'a,
{
    fn par_external_sort(
        self,
        options: ExtsortConfig,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, OrdOrderer>> {
        let inner = sorter::ExtSorter::new(options).run(self, OrdOrderer::new(), buffer_sort)?;
        Ok(ParallelResultIterator { inner })
    }
}

impl<'a, I, T> ParallelExtSortExtension<'a> for I
where
    I: Iterator<Item = T>,
    T: Send + 'a,
{
    fn par_external_sort_by<F>(
        self,
        options: ExtsortConfig,
        comparator: F,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, FuncOrderer<F>>>
    where
        F: Fn(&Self::Item, &Self::Item) -> Ordering + Send + Sync,
    {
        let inner =
            sorter::ExtSorter::new(options).run(self, FuncOrderer::new(comparator), buffer_sort)?;
        Ok(ParallelResultIterator { inner })
    }

    fn par_external_sort_by_key<F, K>(
        self,
        options: ExtsortConfig,
        key_extractor: F,
    ) -> io::Result<ParallelResultIterator<'a, Self::Item, KeyOrderer<F>>>
    where
        F: Fn(&Self::Item) -> K + Send + Sync,
        K: Ord,
    {
        let inner = sorter::ExtSorter::new(options).run(
            self,
            KeyOrderer::new(key_extractor),
            buffer_sort,
        )?;
        Ok(ParallelResultIterator { inner })
    }
}

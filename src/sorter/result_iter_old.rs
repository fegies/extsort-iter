use std::{
    cmp::Ordering,
    collections::{binary_heap::PeekMut, BinaryHeap},
    rc::Rc,
};

use crate::{orderer::Orderer, run::Run};

struct HeapEntry<R, O> {
    run: R,
    orderer: Rc<O>,
}

impl<R, T, O> PartialEq for HeapEntry<R, O>
where
    O: Orderer<T>,
    R: Run<T>,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl<R, T, O> Eq for HeapEntry<R, O> where O: Orderer<T> {}

impl<R, T, O> PartialOrd for HeapEntry<R, O>
where
    O: Orderer<T>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<R, T, O> Ord for HeapEntry<R, O>
where
    O: Orderer<T>,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left = if let Some(l) = self.run.peek() {
            l
        } else {
            return Ordering::Less;
        };
        let right = if let Some(r) = other.run.peek() {
            r
        } else {
            return Ordering::Greater;
        };

        self.orderer.compare(left, right).reverse()
    }
}

pub struct ResultIterator<R, O> {
    runs: BinaryHeap<HeapEntry<R, O>>,
}

impl<'a, R, T, O> ResultIterator<R, O>
where
    O: Orderer<T>,
{
    pub fn new(source: impl Iterator<Item = impl Run<T>>, orderer: O) -> Self {
        let orderer = Rc::new(orderer);
        let runs = source
            .map(|run| HeapEntry {
                run,
                orderer: orderer.clone(),
            })
            .collect();
        Self { runs }
    }
}

impl<R, T, O> Iterator for ResultIterator<R, O>
where
    O: Orderer<T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let mut first_run = self.runs.peek_mut()?;
            if let Some(next) = first_run.run.next() {
                break Some(next);
            } else {
                PeekMut::pop(first_run);
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.runs.iter().map(|r| r.run.size_hint()).sum();
        (size, Some(size))
    }
}

impl<T, R, O> ExactSizeIterator for ResultIterator<R, O> where O: Orderer<T> {}

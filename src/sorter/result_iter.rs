use std::{
    cmp::Ordering,
    collections::{binary_heap::PeekMut, BinaryHeap},
    rc::Rc,
};

use crate::{orderer::Orderer, run::Run};

struct HeapEntry<'a, T, O> {
    run: Box<dyn Run<T> + 'a>,
    orderer: Rc<O>,
}

impl<T, O> PartialEq for HeapEntry<'_, T, O>
where
    O: Orderer<T>,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl<T, O> Eq for HeapEntry<'_, T, O> where O: Orderer<T> {}

impl<T, O> PartialOrd for HeapEntry<'_, T, O>
where
    O: Orderer<T>,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T, O> Ord for HeapEntry<'_, T, O>
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

pub struct ResultIterator<'a, T, O> {
    runs: BinaryHeap<HeapEntry<'a, T, O>>,
}

impl<'a, T, O> ResultIterator<'a, T, O>
where
    O: Orderer<T>,
{
    pub fn new(source: impl Iterator<Item = Box<dyn Run<T> + 'a>>, orderer: O) -> Self {
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

impl<T, O> Iterator for ResultIterator<'_, T, O>
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

impl<T, O> ExactSizeIterator for ResultIterator<'_, T, O> where O: Orderer<T> {}

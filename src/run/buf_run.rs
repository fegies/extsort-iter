use std::vec::IntoIter;

use super::Run;

/// A run backed by the provided buffer.
pub struct BufRun<T> {
    source: IntoIter<T>,
}

impl<T> BufRun<T> {
    /// Creates a new instance of a BufRun
    pub fn new(source: Vec<T>) -> Self {
        Self {
            source: source.into_iter(),
        }
    }
}

impl<T> Run for BufRun<T> {
    type Item = T;

    fn peek(&self) -> Option<&Self::Item> {
        self.source.as_slice().get(0)
    }

    fn next(&mut self) -> Option<Self::Item> {
        self.source.next()
    }
}

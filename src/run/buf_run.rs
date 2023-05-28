use std::vec::IntoIter;

use super::Run;

/// A run backed by the provided buffer.
pub(crate) struct BufRun<T> {
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

impl<T> Run<T> for BufRun<T> {
    fn peek(&self) -> Option<&T> {
        self.source.as_slice().get(0)
    }

    fn next(&mut self) -> Option<T> {
        self.source.next()
    }

    fn remaining_items(&self) -> usize {
        self.source.len()
    }
}

#[cfg(test)]
mod test {
    use crate::run::Run;

    use super::BufRun;

    #[test]
    fn test_remaining() {
        let run: BufRun<()> = BufRun::new(vec![]);
        assert_eq!(0, run.remaining_items())
    }
}

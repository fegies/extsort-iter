use std::io::{self};

use crate::{orderer::Orderer, run::BoxedRun};

pub mod sequential;

#[cfg(any(feature = "parallel_sort", feature = "docsrs"))]
pub mod threaded;

/// A struct to get the finalization results
pub struct FinalizeContents<T, O, F> {
    /// the runs that were moved to disk during execution
    pub tapes: Vec<BoxedRun<T>>,
    /// the orderer supplied to the cleaner
    pub orderer: O,
    /// the sorting function supplied to the cleaner
    pub sort_func: F,
}

/// A strategy on how to move runs to disk
pub trait BufferCleaner<T, O, F>
where
    O: Orderer<T>,
    F: FnMut(&O, &mut [T]),
{
    /// sorts the provided run and moves it to disk.
    /// after this function returns successfully, the buffer will be empty
    /// and ready for reuse.
    fn clean_buffer(&mut self, buffer: &mut Vec<T>) -> io::Result<()>;

    /// constructs an initial buffer for sorting use, matching the configured size
    /// To avoid excessive resource consumption, only one buffer should be constructed
    /// using this method.
    fn get_buffer(&mut self) -> Vec<T>;

    /// stops the sorting process and returns the runs moved to disk.
    fn finalize(self) -> io::Result<FinalizeContents<T, O, F>>;
}

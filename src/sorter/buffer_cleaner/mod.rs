use std::io::{self, Read};

use crate::run::file_run::ExternalRun;

pub mod sequential;

#[cfg(any(feature = "parallel_sort", feature = "docsrs"))]
pub mod threaded;

pub trait BufferCleaner<T, O> {
    type Handle: BufferCleanerHandle<T, O>;
    fn run<F, R>(self, func: F) -> R
    where
        F: FnOnce(Self::Handle) -> R;
}

pub type BoxedRun<T> = ExternalRun<T, Box<dyn Read + Send>>;

pub trait BufferCleanerHandle<T, O> {
    fn sort_buffer(&mut self, buffer: &mut Vec<T>);

    fn clean_buffer(&mut self, buffer: &mut Vec<T>) -> io::Result<()>;

    fn get_buffer(&mut self) -> Vec<T>;

    fn finalize(self) -> io::Result<(Vec<BoxedRun<T>>, O)>;
}

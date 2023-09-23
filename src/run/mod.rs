use std::io::Read;

#[cfg(test)]
pub(crate) mod buf_run;
pub mod file_run;
pub mod split_backing;

pub type BoxedRun<T> = file_run::ExternalRun<T, Box<dyn Read + Send>>;

/// A run is a sequence of items in ascending order.
pub trait Run<T> {
    /// peeks at the next value in the sequence.
    fn peek(&self) -> Option<&T>;

    /// fetches the next item from the run.
    /// If the method returns None, we have reached the end.
    fn next(&mut self) -> Option<T>;

    /// returns the bounds on the remaining length of the run
    /// See https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.size_hint
    fn remaining_items(&self) -> usize;
}

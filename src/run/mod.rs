pub mod buf_run;
pub mod file_run;

/// A run is a sequence of items in ascending order.
pub trait Run<T> {
    /// peeks at the next value in the sequence.
    fn peek(&self) -> Option<&T>;

    /// fetches the next item from the run.
    /// If the method returns None, we have reached the end.
    fn next(&mut self) -> Option<T>;

    /// returns the bounds on the remaining length of the run
    /// See https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.size_hint
    fn size_hint(&self) -> (usize, Option<usize>);
}

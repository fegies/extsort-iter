pub mod buf_run;
pub mod file_run;

/// A run is a sequence of items in ascending order.
pub trait Run<T> {
    /// peeks at the next value in the sequence.
    fn peek(&self) -> Option<&T>;

    /// fetches the next item from the run.
    /// If the method returns None, we have reached the end.
    fn next(&mut self) -> Option<T>;
}

use std::{
    io::{self, ErrorKind, Read},
    mem::{self, MaybeUninit},
    num::NonZeroUsize,
};

use crate::tape::Tape;

use super::Run;

/// A run backed by a file on disk.
/// The file is deleted when the run is dropped.
/// Our TBacking type will be File for the real
/// usage in the library and Cursor for testing
pub struct ExternalRun<T, TBacking>
where
    TBacking: Read,
{
    /// the source file that is backing our run.
    source: TBacking,
    /// the data maintained in our sort buffer.
    /// we maintain the invariant that
    /// all the data entries from the read_idx
    /// to the end are actually initialized.
    buffer: Vec<MaybeUninit<T>>,
    /// a pointer marking the point from which on the entries are initialized.
    /// the entries in our vec are initialized.
    read_idx: usize,
    /// the remaining entries for this run.
    /// used for the size_hint and to be able to deal with zero sized types
    remaining_entries: usize,
}

impl<T, B> Drop for ExternalRun<T, B>
where
    B: Read,
{
    fn drop(&mut self) {
        // if the
        if mem::needs_drop::<T>() {
            // drop all elements by reading from the source until all items are exhausted
            while self.next().is_some() {}
        }
    }
}

/// Creates a new FileRun Object that uses the provided source as its
/// buffer, but is not actually backed by anything on disk
pub fn create_buffer_run<T>(source: Vec<T>) -> ExternalRun<T, Box<dyn Read + Send>> {
    let buffer: Vec<MaybeUninit<T>> = unsafe {
        // we are only transmuting our Vec<T> to Vec<MaybeUninit<T>>.
        // this is guaranteed to have the same binary representation.
        core::mem::transmute(source)
    };

    let remaining_entries = buffer.len();

    ExternalRun {
        source: Box::new(io::Cursor::new(&[])),
        buffer,
        read_idx: 0,
        remaining_entries,
    }
}

impl<T, TBacking> ExternalRun<T, TBacking>
where
    TBacking: Read,
{
    pub fn from_tape(tape: Tape<TBacking>, buffer_size: NonZeroUsize) -> Self {
        let num_entries = tape.num_entries();
        let source = tape.into_backing();

        let mut buffer = Vec::with_capacity(buffer_size.into());
        for _ in 0..buffer_size.into() {
            buffer.push(MaybeUninit::uninit());
        }
        let mut res = Self {
            buffer,
            read_idx: 0,
            remaining_entries: num_entries,
            source,
        };

        res.refill_buffer();

        res
    }
    /// refills the read buffer.
    /// this should only be called if the read_idx is at the end of the buffer
    ///
    /// This function may panic on IO errors
    fn refill_buffer(&mut self) {
        /// keep retrying the read if it returns with an interrupted error.
        fn read_with_retry(source: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
            loop {
                match source.read(buffer) {
                    Ok(size) => break Ok(size),
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    err => break err,
                }
            }
        }

        /// try to read exactly the requested number of bytes
        /// This function may only return less than the number of requested bytes
        /// when the end of the run is reached.
        fn try_read_exact(source: &mut impl Read, mut buffer: &mut [u8]) -> usize {
            let mut bytes_read = 0;
            while !buffer.is_empty() {
                let read = read_with_retry(source, buffer).expect("Unable to perform read on FileRun. This means that the file was modified from under us!");
                if read == 0 {
                    break;
                }
                buffer = &mut buffer[read..];
                bytes_read += read;
            }

            bytes_read
        }

        let item_size = std::mem::size_of::<T>();

        // for ZSTs it really does not make sense to try to read them back from our
        // io backing, so we just reset the read index
        if item_size == 0 {
            self.read_idx = 0;
            return;
        }

        let slice = unsafe {
            let start = self.buffer.as_mut_ptr() as *mut u8;
            std::slice::from_raw_parts_mut(start, self.buffer.len() * item_size)
        };

        let bytes_read = try_read_exact(&mut self.source, slice);
        assert_eq!(
            0,
            bytes_read % item_size,
            "The size of the file does not match anymore! was it modified?"
        );
        let remaining_size = bytes_read / item_size;
        self.buffer.truncate(remaining_size);

        self.read_idx = 0;
    }
}

impl<T, TBacking> Run<T> for ExternalRun<T, TBacking>
where
    TBacking: Read,
{
    /// Peek at the next entry in the run
    fn peek(&self) -> Option<&T> {
        if self.remaining_entries == 0 {
            None
        } else {
            // SAFETY:
            // we always ensure that everything from the read_idx to the
            // end of the buffer is properly initialized from the backing file.
            // so while the read_idx is inside the buffer bounds, it must be valid.
            unsafe { Some(self.buffer[self.read_idx].assume_init_ref()) }
        }
    }

    /// Get the next item from the run and advance its position
    fn next(&mut self) -> Option<T> {
        if self.remaining_entries == 0 {
            return None;
        }

        // when we have reached this point, we can be certain that we are inside the
        // buffer bounds.

        // SAFETY:
        // we always ensure that everything from the read_idx to the
        // end of the buffer is properly initialized from the backing file.
        // so while the read_idx is inside the buffer bounds, it must be valid.
        let result = unsafe { self.buffer[self.read_idx].assume_init_read() };

        // we consumed the value at the read_index so we need to make sure that we increment it
        // to maintain the buffer invariant
        // as well as decrement the remaining entries in our run.
        self.read_idx += 1;
        self.remaining_entries -= 1;

        // we check if we need to refill the buffer in case we have reached the end
        // we do this here to make sure that the peek is always inside
        // the buffer as long as there are still items
        if self.read_idx >= self.buffer.len() {
            self.refill_buffer();
        }

        Some(result)
    }

    fn remaining_items(&self) -> usize {
        self.remaining_entries
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use crate::tape::vec_to_tape;

    use super::*;

    fn test_file_run<T>(data: Vec<T>, buffer_size: NonZeroUsize)
    where
        T: Clone + Eq + Debug,
    {
        let tape = vec_to_tape(data.clone());
        let mut run = ExternalRun::from_tape(tape, buffer_size);

        assert_eq!(data.len(), run.remaining_items());
        let collected = std::iter::from_fn(|| run.next()).collect::<Vec<_>>();
        assert_eq!(data, collected);
    }

    #[test]
    fn test_drop() {
        let vec: Vec<i32> = (1..5).collect();
        let data: Vec<_> = core::iter::repeat(&vec).take(20).cloned().collect();
        let tape = vec_to_tape(data);
        let mut run: ExternalRun<Vec<i32>, _> =
            ExternalRun::from_tape(tape, NonZeroUsize::new(4096).unwrap());
        for _ in 0..10 {
            run.next();
        }
        drop(run);
    }

    #[test]
    fn works_with_vecs() {
        let d = (1..100).collect::<Vec<_>>();
        let data = vec![d; 10];

        test_file_run(data, NonZeroUsize::new(2).unwrap());
    }

    #[test]
    fn works_with_zst() {
        let data = vec![(); 10];
        test_file_run(data, NonZeroUsize::new(2).unwrap());
    }

    #[test]
    fn works_with_larger_buffer() {
        let size = NonZeroUsize::new(20).unwrap();
        let data = vec![(); 10];
        test_file_run(data, size);

        let data = vec![1337; 10];
        test_file_run(data, size);
    }

    #[test]
    fn works_with_empty_data() {
        let size = NonZeroUsize::new(10).unwrap();
        let data = vec![(); 0];
        test_file_run(data, size);

        let data = vec![1; 0];
        test_file_run(data, size);
    }
}

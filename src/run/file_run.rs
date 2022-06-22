use std::{
    fs::{self, File},
    io::{self, ErrorKind, Read, Seek, Write},
    mem::{self, MaybeUninit},
    num::NonZeroUsize,
    path::Path,
};

use super::Run;

/// Our ExternalRun specialised to the File backing type.
/// This is the only type that will be used outside of this module,
/// we mainly keep the type parameter in order to be able to
/// run our tests under miri
pub type FileRun<T> = ExternalRun<T, File>;

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

/// Fills the provided file with the values drained from source.
/// When the call completes successfully, source will be empty.
/// If it fails, source will remain untouched.
fn fill_backing<T, TBacking>(source: &mut Vec<T>, file: &mut TBacking) -> io::Result<()>
where
    TBacking: Write + Seek,
{
    // we create a byteslice view into the vec
    // SAFETY:
    // this is safe because the alignment restrictions of the byteslice are loose enough to allow this
    // and because if T is zero-sized, we will create an empty slice over T.
    let slice = unsafe {
        let num_bytes = source.len() * std::mem::size_of::<T>();
        std::slice::from_raw_parts(source.as_ptr() as *const u8, num_bytes)
    };

    // move the contents of the vec to the file.
    file.write_all(slice)?;

    // seek to the beginning of the file to ensure that we will actually read its contents
    file.seek(io::SeekFrom::Start(0))?;

    // we have conceptually moved all the data that our vec used to contain to disk.
    // in order to make sure that the drop functions are not called twice,
    // we will leak the content of the vec (this is conceptually the same calling mem::forget)
    // on every item in the vec.
    // SAFETY:
    // this is safe because the vec is now empty after this and we no longer refer to
    // any of the elements inside.
    unsafe {
        source.set_len(0);
    }

    Ok(())
}

/// Creates the file that we want to use for the run later.
fn create_file(filename: &Path) -> io::Result<File> {
    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(filename)?;

    // we immediately delete the file, but keep the handle open
    // this has 2 advantages:
    // - we make it much harder to modify the file outside our program
    //   because it is no longer accessible from the file system (just /proc)
    // - it will automatically be cleaned up for us when the handle is dropped (or when the program exits)
    //   eliminating the need for custom cleanup code in our program.
    fs::remove_file(filename)?;

    Ok(file)
}

impl<T> FileRun<T> {
    /// Creates a new FileRun Object filled with the contents from the provided vector.
    /// Might fail if there is not enough space available for the new file or the file already exists.
    /// When this call returns successfully, source will be empty.
    /// if it fails, source will remain untouched.
    pub fn new(
        source: &mut Vec<T>,
        filename: &Path,
        buffer_size: NonZeroUsize,
    ) -> io::Result<Self> {
        let file = create_file(filename)?;
        Self::from_backing(source, file, buffer_size)
    }
}

impl<T, TBacking> ExternalRun<T, TBacking>
where
    TBacking: Read + Write + Seek,
{
    /// Creates a new ExternalRun Object filled with the contents from the provided vector.
    /// Might fail if there is not enough space available on disk
    /// When this call returns successfully, source will be empty.
    /// if it fails, source will remain untouched.
    fn from_backing(
        source: &mut Vec<T>,
        mut backing: TBacking,
        buffer_size: NonZeroUsize,
    ) -> io::Result<Self> {
        let remaining_entries = source.len();
        // let backing = create_file(filename)?;
        fill_backing(source, &mut backing)?;
        let source = backing;

        let mut buffer = Vec::with_capacity(buffer_size.into());
        for _ in 0..buffer_size.into() {
            buffer.push(MaybeUninit::zeroed());
        }

        let mut res = Self {
            source,
            read_idx: buffer.len(),
            buffer,
            remaining_entries,
        };
        // in order for the peek calls to not have to take a mutable reference
        // we further maintain the invariant that the read_idx only reaches
        // the buffer end when the source is actually empty,
        // so we need to refill it once now.
        res.refill_buffer();

        Ok(res)
    }
}

impl<T, TBacking> ExternalRun<T, TBacking>
where
    TBacking: Read,
{
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

    fn size_hint(&self) -> usize {
        self.remaining_entries
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;
    use std::io::Cursor;

    use super::*;

    fn test_file_run<T>(data: Vec<T>, buffer_size: NonZeroUsize)
    where
        T: Clone + Eq + Debug,
    {
        let mut run =
            ExternalRun::from_backing(&mut data.clone(), Cursor::new(Vec::new()), buffer_size)
                .unwrap();

        assert_eq!(data.len(), run.size_hint());
        let collected = std::iter::from_fn(|| run.next()).collect::<Vec<_>>();
        assert_eq!(data, collected);
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

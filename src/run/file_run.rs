use std::{
    fs::{self, File},
    io::{self, Read, Write},
    mem::{self, MaybeUninit},
    path::Path,
};

use super::Run;

/// A run backed by a file on disk.
/// The file is deleted when the run is dropped.
pub struct FileRun<T> {
    source: File,
    buffer: Vec<MaybeUninit<T>>,
    read_idx: usize,
}

impl<T> Drop for FileRun<T> {
    fn drop(&mut self) {
        if mem::needs_drop::<T>() {
            // drop all elements by reading from the source until all items are exhausted
            while self.next().is_some() {}
        }
    }
}

/// Allocates a new file and fills it with the values drained from source.
/// When the call completes, source will be empty.
fn fill_file<T>(source: &mut Vec<T>, filename: &Path) -> io::Result<File> {
    let mut file = fs::OpenOptions::new().create_new(true).open(filename)?;

    // we immediately delete the file.
    // this has 2 advantages:
    // - we make it much harder to modify the file outside our program
    //   because it is no longer accessible from the file system (just /proc)
    // - it will automatically be cleaned up for us when the handle is dropped (or when the program exits)
    //   eliminating the need for custom cleanup code in our program.
    fs::remove_file(filename)?;

    // we create a byteslice view into the vec
    // SAFETY:
    // this is safe because the alignment restrictions of the byteslice are loose enough to allow this
    // and because
    let slice = unsafe {
        let num_bytes = source.len() * std::mem::size_of::<T>();
        std::slice::from_raw_parts(source.as_ptr() as *const u8, num_bytes)
    };

    // move the contents of the vec to the file.
    file.write_all(slice)?;

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

    Ok(file)
}

impl<T> FileRun<T> {
    /// Creates a new FileRun Object filled with the contents from the provided vector.
    /// Might fail if there is not enough space available for the new file or the file already exists.
    /// When this call returns, source will be empty.
    pub fn new(source: &mut Vec<T>, filename: &Path, buffer_size: usize) -> io::Result<Self> {
        assert!(
            mem::size_of::<T>() > 0,
            "It does not make sense to serialize zero sized types to a file!"
        );
        let source = fill_file(source, filename)?;
        let mut buffer = Vec::with_capacity(buffer_size);
        for _ in 0..buffer_size {
            buffer.push(MaybeUninit::uninit());
        }

        let mut res = Self {
            source,
            read_idx: buffer.len(),
            buffer,
        };
        res.refill_buffer();
        Ok(res)
    }

    // refills the read buffer.
    // this should only be called if the read_idx is at the end of the buffer
    fn refill_buffer(&mut self) {
        fn try_read_exact(source: &mut File, mut buffer: &mut [u8]) -> usize {
            let mut bytes_read = 0;
            while !buffer.is_empty() {
                let read = source.read(buffer).expect("Unable to perform read on FileRun. This means that the file was modified from under us!");
                if read == 0 {
                    break;
                }
                buffer = &mut buffer[read..];
                bytes_read += read;
            }

            bytes_read
        }

        let item_size = std::mem::size_of::<T>();

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

impl<T> Run<T> for FileRun<T> {
    fn peek(&self) -> Option<&T> {
        if self.read_idx >= self.buffer.len() {
            None
        } else {
            // SAFETY:
            // we always ensure that everything from the read_idx to the
            // end of the buffer is properly initialized from the backing file.
            // so while the read_idx is inside the buffer bounds, it must be valid.
            unsafe { Some(self.buffer[self.read_idx].assume_init_ref()) }
        }
    }

    fn next(&mut self) -> Option<T> {
        if self.read_idx >= self.buffer.len() {
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
        // to maintain the buffer invariant.
        self.read_idx += 1;

        // we check again to make sure that the buffer is filled for the
        // next peek call.
        if self.read_idx >= self.buffer.len() {
            self.refill_buffer();
        }

        Some(result)
    }
}

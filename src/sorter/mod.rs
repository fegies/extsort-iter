use std::{io, num::NonZeroUsize, path::PathBuf};

use crate::{orderer::Orderer, run::file_run::create_buffer_run, tape::TapeCollection};

use self::result_iter::ResultIterator;

pub mod result_iter;

/// The configuration for the external sorting.
#[derive(Clone)]
pub struct ExtsortConfig {
    /// the maximum size of the sort buffer
    pub(crate) sort_buffer_size: NonZeroUsize,
    /// the number of bytes to read ahead
    pub(crate) run_read_size: NonZeroUsize,
    pub temp_file_folder: PathBuf,
}

impl ExtsortConfig {
    /// Creates a configuration with a specified sort buffer size in bytes
    /// and a sort directory of /tmp
    pub fn create_with_buffer_size_for<T>(sort_buf_bytes: usize) -> Self {
        let t_size = std::mem::size_of::<T>();

        let one = NonZeroUsize::new(1).unwrap();
        let sort_count;
        let run_count;
        if t_size == 0 {
            sort_count = one;
            run_count = one;
        } else {
            sort_count = NonZeroUsize::new(sort_buf_bytes / t_size).unwrap_or(one);
            run_count = NonZeroUsize::new(4096 / t_size).unwrap_or(one);
        }

        ExtsortConfig {
            sort_buffer_size: sort_count,
            run_read_size: run_count,
            temp_file_folder: PathBuf::from("/tmp"),
        }
    }
    /// Creates a configuration with a sort buffer size of 10M
    /// and a sort directory of /tmp
    pub fn default_for<T>() -> Self {
        Self::create_with_buffer_size_for::<T>(10_000_000)
    }
    /// Updates the temp_file_folder attribute.
    /// Useful for fluent-style api usage.
    pub fn temp_file_folder(self, folder: impl Into<PathBuf>) -> Self {
        Self {
            temp_file_folder: folder.into(),
            ..self
        }
    }
}

pub struct ExtSorter {
    config: ExtsortConfig,
}

impl ExtSorter {
    pub fn new(options: ExtsortConfig) -> Self {
        Self { config: options }
    }

    pub fn run<'a, S, T, O, F>(
        &self,
        mut source: S,
        orderer: O,
        mut buffer_sort: F,
    ) -> io::Result<ResultIterator<T, O>>
    where
        S: Iterator<Item = T>,
        O: Orderer<T>,
        T: 'a,
        F: FnMut(&O, &mut [T]),
    {
        let max_buffer_size = self.config.sort_buffer_size.into();
        let mut sort_buffer = Vec::with_capacity(max_buffer_size);

        let mut tape_collection = TapeCollection::<T>::new(
            self.config.temp_file_folder.clone(),
            NonZeroUsize::new(256).unwrap(),
        );

        let source = &mut source;
        loop {
            sort_buffer.extend(source.take(max_buffer_size));
            buffer_sort(&orderer, &mut sort_buffer);
            if sort_buffer.len() < max_buffer_size {
                // we could not completely fill the buffer, so we know that this
                // is the last run that will be generated.

                if tape_collection.is_empty() {
                    // we did not acually move anything to disk.
                    // in this case we can just reuse the sort buffer
                    // as a sort of pseudo tape.
                    let buffer_run = create_buffer_run(sort_buffer);
                    return Ok(ResultIterator::new(vec![buffer_run], orderer));
                } else {
                    // since we moved runs to disk, we will need to use memory for the read buffers.
                    // to avoid going over budget, we move the final run to disk as well
                    tape_collection.add_run(&mut sort_buffer)?;
                }
                break;
            } else {
                tape_collection.add_run(&mut sort_buffer)?;
            }
        }

        // at this point, we must have moved runs to disk, including the final sort buffer.
        debug_assert!(sort_buffer.is_empty());
        // it should not be necessary to manually drop the buffer here, but it sure does
        // not hurt and this way we are guaranteed to have released the memory
        // before initializing the tapes, even on compiler versions that do not
        // implement NLL yet.
        drop(sort_buffer);

        let tapes = tape_collection.into_tapes(self.config.run_read_size);

        Ok(ResultIterator::new(tapes, orderer))
    }
}

use std::{
    io::{self},
    num::NonZeroUsize,
    path::PathBuf,
};

use crate::{
    orderer::Orderer, run::file_run::create_buffer_run,
    sorter::buffer_cleaner::BufferCleanerHandle, tape::compressor::CompressionCodec,
};

use self::{buffer_cleaner::BufferCleaner, result_iter::ResultIterator};

pub mod buffer_cleaner;
pub mod result_iter;

/// The configuration for the external sorting.
#[non_exhaustive]
pub struct ExtsortConfig {
    /// the maximum size of the sort buffer
    pub(crate) sort_buffer_size_bytes: usize,
    pub temp_file_folder: PathBuf,
    #[cfg(feature = "compression")]
    pub compress_with: CompressionCodec,
}

impl Default for ExtsortConfig {
    fn default() -> Self {
        Self {
            sort_buffer_size_bytes: 10_000_000,
            temp_file_folder: PathBuf::from("/tmp"),
            #[cfg(feature = "compression")]
            compress_with: Default::default(),
        }
    }
}

impl ExtsortConfig {
    fn get_num_items_for<T>(&self) -> NonZeroUsize {
        let t_size = std::mem::size_of::<T>();

        let one = NonZeroUsize::new(1).unwrap();

        if t_size == 0 {
            one
        } else {
            NonZeroUsize::new(self.sort_buffer_size_bytes / t_size).unwrap_or(one)
        }
    }

    /// Creates a configuration with a sort buffer size of 10M
    /// and a sort directory of /tmp
    ///
    /// It is recommended to increase the sort buffer size
    /// for improved performance.
    pub fn new() -> Self {
        Default::default()
    }

    /// Creates a configuration with a specified sort buffer size in bytes
    /// and a sort directory of /tmp
    pub fn with_buffer_size(sort_buf_bytes: usize) -> Self {
        ExtsortConfig {
            sort_buffer_size_bytes: sort_buf_bytes,
            ..Default::default()
        }
    }

    /// Creates a configuration with a specified sort buffer size in bytes
    /// and a sort directory of /tmp
    #[deprecated = "Use new() or the Default impl instead. These do not require a type annotation"]
    pub fn create_with_buffer_size_for<T>(sort_buf_bytes: usize) -> Self {
        ExtsortConfig {
            sort_buffer_size_bytes: sort_buf_bytes,
            ..Default::default()
        }
    }
    /// Creates a configuration with a sort buffer size of 10M
    /// and a sort directory of /tmp
    #[deprecated = "Use new() or the Default impl instead. These do not require a type annotation"]
    pub fn default_for<T>() -> Self {
        Default::default()
    }
    /// Updates the temp_file_folder attribute.
    /// Useful for fluent-style api usage.
    pub fn temp_file_folder(self, folder: impl Into<PathBuf>) -> Self {
        Self {
            temp_file_folder: folder.into(),
            ..self
        }
    }
    #[cfg(feature = "compression_lz4_flex")]
    pub fn compress_lz4_flex(mut self) -> Self {
        self.compress_with = CompressionCodec::Lz4Flex;
        self
    }

    /// sets the sort buffer size in bytes
    pub fn sort_buffer_size(mut self, new_size: usize) -> Self {
        self.sort_buffer_size_bytes = new_size;
        self
    }

    fn compression_choice(&self) -> CompressionCodec {
        #[cfg(feature = "compression")]
        {
            self.compress_with
        }
        #[cfg(not(feature = "compression"))]
        {
            CompressionCodec::NoCompression
        }
    }
}

pub struct ExtSorter {}

impl ExtSorter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn run<'a, S, T, C, O>(
        self,
        mut source: S,
        buffer_cleaner: C,
    ) -> io::Result<ResultIterator<T, O>>
    where
        S: Iterator<Item = T>,
        T: 'a,
        C: BufferCleaner<T, O>,
        O: Orderer<T>,
    {
        buffer_cleaner.run(move |mut buffer_cleaner| {
            let mut sort_buffer = buffer_cleaner.get_buffer();

            let source = &mut source;
            let mut any_buffer_was_flushed = false;
            loop {
                debug_assert!(sort_buffer.is_empty());
                let capacity = sort_buffer.capacity();

                sort_buffer.extend(source.take(capacity));
                if sort_buffer.len() < capacity {
                    // we could not completely fill the buffer, so we know that this
                    // is the last run that will be generated.

                    if !any_buffer_was_flushed {
                        // we did not acually move anything to disk.
                        // in this case we can just reuse the sort buffer
                        // as a sort of pseudo tape.
                        buffer_cleaner.sort_buffer(&mut sort_buffer);
                        let (_tapes, orderer) = buffer_cleaner.finalize()?;
                        let buffer_run = create_buffer_run(sort_buffer);
                        return Ok(ResultIterator::new(vec![buffer_run], orderer));
                    } else if !sort_buffer.is_empty() {
                        // since we moved runs to disk, we will need to use memory for the read buffers.
                        // to avoid going over budget, we move the final run to disk as well
                        buffer_cleaner.clean_buffer(&mut sort_buffer)?;
                    }
                    break;
                } else {
                    buffer_cleaner.clean_buffer(&mut sort_buffer)?;
                    any_buffer_was_flushed = true;
                }
            }

            // at this point, we must have moved runs to disk, including the final sort buffer.
            debug_assert!(sort_buffer.is_empty());
            // it should not be necessary to manually drop the buffer here, but it sure does
            // not hurt and this way we are guaranteed to have released the memory
            // before initializing the tapes, even on compiler versions that do not
            // implement NLL yet.
            drop(sort_buffer);

            // wait for the io thread to be done writing and get the file handles back to the main thread
            let (tapes, orderer) = buffer_cleaner.finalize()?;
            Ok(ResultIterator::new(tapes, orderer))
        })
    }
}

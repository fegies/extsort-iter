use std::{io, num::NonZeroUsize, path::PathBuf};

use crate::{
    orderer::Orderer,
    run::file_run::create_buffer_run,
    tape::{compressor::CompressionCodec, TapeCollection},
};

use self::result_iter::ResultIterator;

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

pub struct ExtSorter {
    config: ExtsortConfig,
}

impl ExtSorter {
    pub fn new(options: ExtsortConfig) -> Self {
        Self { config: options }
    }

    pub fn run<'a, S, T, O, F>(
        self,
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
        let max_buffer_size_nonzero = self.config.get_num_items_for::<T>();
        let max_buffer_size = max_buffer_size_nonzero.get();
        let mut sort_buffer = Vec::with_capacity(max_buffer_size);

        let compression_choice = self.config.compression_choice();
        let mut tape_collection = TapeCollection::<T>::new(
            self.config.temp_file_folder,
            NonZeroUsize::new(256).unwrap(),
            compression_choice,
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
                } else if !sort_buffer.is_empty() {
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

        let tapes = tape_collection.into_tapes(max_buffer_size_nonzero);

        Ok(ResultIterator::new(tapes, orderer))
    }
}

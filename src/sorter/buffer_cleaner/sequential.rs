use std::num::NonZeroUsize;

use crate::{orderer::Orderer, tape::TapeCollection, ExtsortConfig};

use super::*;

pub struct SingleThreadedBufferCleaner<T, O, F> {
    tape_collection: TapeCollection<T>,
    buffer_sort: F,
    orderer: O,
    buffer_cap: NonZeroUsize,
}

impl<T, O, F> BufferCleaner<T, O> for SingleThreadedBufferCleaner<T, O, F>
where
    O: Orderer<T>,
    F: FnMut(&O, &mut [T]),
{
    type Handle = Self;

    fn run<Fo, R>(self, func: Fo) -> R
    where
        Fo: FnOnce(Self::Handle) -> R,
    {
        func(self)
    }
}

impl<T, O, F> BufferCleanerHandle<T, O> for SingleThreadedBufferCleaner<T, O, F>
where
    O: Orderer<T>,
    F: FnMut(&O, &mut [T]),
{
    fn sort_buffer(&mut self, buffer: &mut Vec<T>) {
        (self.buffer_sort)(&self.orderer, buffer)
    }

    fn clean_buffer(&mut self, buffer: &mut Vec<T>) -> io::Result<()> {
        self.sort_buffer(buffer);
        self.tape_collection.add_run(buffer)
    }

    fn get_buffer(&mut self) -> Vec<T> {
        Vec::with_capacity(self.buffer_cap.get())
    }

    fn finalize(self) -> io::Result<(Vec<BoxedRun<T>>, O)> {
        let runs = self.tape_collection.into_tapes(self.buffer_cap);
        Ok((runs, self.orderer))
    }
}

impl<T, O, F> SingleThreadedBufferCleaner<T, O, F> {
    pub fn new(config: ExtsortConfig, orderer: O, buffer_sort: F) -> Self {
        let max_buffer_size_nonzero = config.get_num_items_for::<T>();

        let compression_choice = config.compression_choice();
        let tape_collection = TapeCollection::<T>::new(
            config.temp_file_folder,
            NonZeroUsize::new(256).unwrap(),
            compression_choice,
        );

        Self {
            tape_collection,
            buffer_sort,
            orderer,
            buffer_cap: max_buffer_size_nonzero,
        }
    }
}

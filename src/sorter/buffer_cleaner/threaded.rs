use std::{
    num::NonZeroUsize,
    sync::mpsc::{Receiver, SyncSender},
};

use crate::{orderer::Orderer, tape::TapeCollection, ExtsortConfig};

use super::*;

enum BufferCleanerResponse<T, O> {
    SortedBuffer(Vec<T>),
    CleanedBuffer(Vec<T>),
    Finalize((TapeCollection<T>, O)),
}
enum BufferCleanerCommand<T> {
    SortBuffer(Vec<T>),
    CleanBuffer(Vec<T>),
    Finalize,
}

pub struct MultithreadedBufferCleanerHandle<T, O> {
    rx: Receiver<io::Result<BufferCleanerResponse<T, O>>>,
    tx: SyncSender<BufferCleanerCommand<T>>,
    buffer_capacity: NonZeroUsize,
}

pub struct MultithreadedBufferCleaner<O, F> {
    config: ExtsortConfig,
    orderer: O,
    buffer_sort: F,
}

impl<O, F> MultithreadedBufferCleaner<O, F> {
    pub fn new(config: ExtsortConfig, orderer: O, buffer_sort: F) -> Self {
        Self {
            config,
            orderer,
            buffer_sort,
        }
    }
}
impl<T, O, F> BufferCleaner<T, O> for MultithreadedBufferCleaner<O, F>
where
    O: Orderer<T> + Send,
    F: FnMut(&O, &mut [T]) + Send,
    T: Send,
{
    type Handle = MultithreadedBufferCleanerHandle<T, O>;

    fn run<Fo, R>(self, func: Fo) -> R
    where
        Fo: FnOnce(Self::Handle) -> R,
    {
        std::thread::scope(move |scope| {
            let config = self.config;

            let max_buffer_size_nonzero = config.get_num_items_for::<T>();
            let max_buffer_size = max_buffer_size_nonzero.get();

            let compression_choice = config.compression_choice();
            let tape_collection = TapeCollection::<T>::new(
                config.temp_file_folder,
                NonZeroUsize::new(256).unwrap(),
                compression_choice,
            );

            let (worker_tx, rx) = std::sync::mpsc::sync_channel(0);
            let (tx, worker_rx) = std::sync::mpsc::sync_channel(0);

            std::thread::Builder::new()
                .name("Sort-Buffer-Writer".to_owned())
                .spawn_scoped(scope, move || {
                    let mut cleaned_buffer = Vec::with_capacity(max_buffer_size / 2);
                    let orderer = self.orderer;
                    let mut tape_collection = tape_collection;
                    let mut buffer_sort = self.buffer_sort;
                    loop {
                        match worker_rx.recv().unwrap() {
                            BufferCleanerCommand::SortBuffer(mut buf) => {
                                (buffer_sort)(&orderer, &mut buf);
                                worker_tx
                                    .send(Ok(BufferCleanerResponse::SortedBuffer(buf)))
                                    .ok();
                            }
                            BufferCleanerCommand::CleanBuffer(mut buf) => {
                                worker_tx
                                    .send(Ok(BufferCleanerResponse::CleanedBuffer(cleaned_buffer)))
                                    .ok();
                                (buffer_sort)(&orderer, &mut buf);
                                if let Err(e) = tape_collection.add_run(&mut buf) {
                                    worker_tx.send(Err(e)).ok();
                                    break;
                                }
                                cleaned_buffer = buf;
                            }
                            BufferCleanerCommand::Finalize => {
                                drop(cleaned_buffer);
                                worker_tx
                                    .send(Ok(BufferCleanerResponse::Finalize((
                                        tape_collection,
                                        orderer,
                                    ))))
                                    .ok();
                                break;
                            }
                        };
                    }
                })
                .unwrap();

            let handle = MultithreadedBufferCleanerHandle {
                rx,
                tx,
                buffer_capacity: max_buffer_size_nonzero,
            };

            func(handle)
        })
    }
}

impl<T, O> MultithreadedBufferCleanerHandle<T, O>
where
    O: Orderer<T> + Send,
    T: Send,
{
    fn recv_next(&mut self) -> io::Result<BufferCleanerResponse<T, O>> {
        self.rx
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?
    }
}

impl<T, O> BufferCleanerHandle<T, O> for MultithreadedBufferCleanerHandle<T, O>
where
    O: Orderer<T> + Send,
    T: Send,
{
    fn sort_buffer(&mut self, buffer: &mut Vec<T>) {
        let buf = core::mem::take(buffer);
        self.tx.send(BufferCleanerCommand::SortBuffer(buf)).ok();

        loop {
            match self.recv_next().unwrap() {
                BufferCleanerResponse::SortedBuffer(buf) => {
                    *buffer = buf;
                    break;
                }
                BufferCleanerResponse::CleanedBuffer(_) => {}
                BufferCleanerResponse::Finalize(_) => panic!("unexpected finalize"),
            }
        }
    }

    fn clean_buffer(&mut self, buffer: &mut Vec<T>) -> io::Result<()> {
        let buf = core::mem::take(buffer);
        self.tx.send(BufferCleanerCommand::CleanBuffer(buf)).ok();

        match self.recv_next()? {
            BufferCleanerResponse::CleanedBuffer(buf) => {
                *buffer = buf;
                Ok(())
            }
            BufferCleanerResponse::Finalize(_) => panic!("unexpected response: finalize"),
            BufferCleanerResponse::SortedBuffer(_) => panic!("unexpected response: sorted buffer"),
        }
    }

    fn get_buffer(&mut self) -> Vec<T> {
        Vec::with_capacity(self.buffer_capacity.get() / 2)
    }

    fn finalize(mut self) -> io::Result<(Vec<BoxedRun<T>>, O)> {
        self.tx.send(BufferCleanerCommand::Finalize).ok();

        loop {
            if let BufferCleanerResponse::Finalize((tape_collection, orderer)) = self.recv_next()? {
                let tapes = tape_collection.into_tapes(self.buffer_capacity);
                return Ok((tapes, orderer));
            }
        }
    }
}

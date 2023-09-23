use std::{
    num::NonZeroUsize,
    sync::mpsc::{Receiver, SyncSender},
    thread::ScopedJoinHandle,
};

use crate::{orderer::Orderer, tape::TapeCollection, ExtsortConfig};

use super::*;

/// A multithreaded buffer cleaner.
/// The idea here is that we split our available sort buffer into 2 equal parts,
/// and flush one buffer using a background thread while the main thread fills the
/// other buffer.
/// On every clean call, the buffers are swapped.

/// the cleaner object
pub struct MultithreadedBufferCleaner<O, F> {
    config: ExtsortConfig,
    orderer: O,
    buffer_sort: F,
}

/// A handle object to send commands to the background thread
/// and receive responses from it.
///
/// Because our type T or the orderer might have a lifetime on it,
/// the background thread needs to be scoped to only this sort call.
///
/// On the receive side, we can either receive a cleaned buffer or
/// an IO error.
pub struct MultithreadedBufferCleanerHandle<'scope, T, O, F> {
    rx: Receiver<io::Result<Vec<T>>>,
    tx: SyncSender<BufferCleanerCommand<T>>,
    finalize_handle: ScopedJoinHandle<'scope, FinalizeContents<T, O, F>>,
    buffer_capacity: NonZeroUsize,
}

/// the commands that may be sent to the background thread.
enum BufferCleanerCommand<T> {
    /// Instruct the background thread to write the provided buffer to disk
    CleanBuffer(Vec<T>),
    /// Instruct the background thread to finalize their runs and exit.
    Finalize,
}

impl<O, F> MultithreadedBufferCleaner<O, F>
where
    O: Send,
{
    pub fn new(config: ExtsortConfig, orderer: O, buffer_sort: F) -> Self {
        Self {
            config,
            orderer,
            buffer_sort,
        }
    }

    /// spawns the io thread and runs the provided closure with a command handle to that thread.
    pub fn run<Fo, T, R>(self, func: Fo) -> R
    where
        Fo: FnOnce(MultithreadedBufferCleanerHandle<T, O, F>) -> R,
        F: FnMut(&O, &mut [T]) + Send,
        T: Send,
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

            let (worker_tx, rx) = std::sync::mpsc::sync_channel(1);
            let (tx, worker_rx) = std::sync::mpsc::sync_channel(1);

            let finalize_handle = std::thread::Builder::new()
                .name("Sort-Buffer-Writer".to_owned())
                .spawn_scoped(scope, move || {
                    // we hold a second empty buffer ready to exchange with the main thread.
                    let mut cleaned_buffer = Vec::with_capacity(max_buffer_size / 2);
                    let orderer = self.orderer;
                    let mut tape_collection = tape_collection;
                    let mut buffer_sort = self.buffer_sort;
                    loop {
                        match worker_rx.recv().unwrap() {
                            BufferCleanerCommand::CleanBuffer(mut buf) => {
                                // first send the previously cleaned buffer so that the main thread can continue
                                worker_tx.send(Ok(cleaned_buffer)).ok();
                                // sort the buffer
                                (buffer_sort)(&orderer, &mut buf);
                                // move it to disk
                                if let Err(e) = tape_collection.add_run(&mut buf) {
                                    worker_tx.send(Err(e)).ok();
                                    break;
                                }
                                // and mark it as cleaned for the next iteration
                                cleaned_buffer = buf;
                            }
                            BufferCleanerCommand::Finalize => {
                                // drop our half of the cleaned buffer before the tap finalization call
                                // to avoid double memory consumption.
                                drop(cleaned_buffer);
                                // exit the loop to terminate the thread.
                                break;
                            }
                        };
                    }
                    // rewind all tapes and prefill read buffers
                    let tapes = tape_collection.into_tapes(max_buffer_size_nonzero);
                    FinalizeContents {
                        tapes,
                        orderer,
                        sort_func: buffer_sort,
                    }
                })
                .unwrap();

            let handle = MultithreadedBufferCleanerHandle {
                rx,
                tx,
                finalize_handle,
                buffer_capacity: max_buffer_size_nonzero,
            };

            // run our processing function and pass the handle to it.
            func(handle)
        })
    }
}

impl<T, O, F> MultithreadedBufferCleanerHandle<'_, T, O, F> {
    /// convenience function that converts send errors to io errs
    fn send(&mut self, command: BufferCleanerCommand<T>) -> io::Result<()> {
        self.tx.send(command).map_err(|_buf| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the writer thread exited unexpectedly",
            )
        })
    }
}

impl<T, O, F> BufferCleaner<T, O, F> for MultithreadedBufferCleanerHandle<'_, T, O, F>
where
    O: Orderer<T> + Send,
    T: Send,
    F: FnMut(&O, &mut [T]),
{
    /// clean the provided buffer by handing it over to the background thread
    /// and swapping it with a newly cleaned buffer.
    fn clean_buffer(&mut self, buffer: &mut Vec<T>) -> io::Result<()> {
        let buf = core::mem::take(buffer);
        self.send(BufferCleanerCommand::CleanBuffer(buf))?;

        let buf = self
            .rx
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))??;
        *buffer = buf;
        Ok(())
    }

    // we can only hand out a buffer of half the allocated size because
    // there is another, equally sized buffer in use by the background thread.
    fn get_buffer(&mut self) -> Vec<T> {
        Vec::with_capacity(self.buffer_capacity.get() / 2)
    }

    fn finalize(mut self) -> io::Result<FinalizeContents<T, O, F>> {
        // send the io command
        self.send(BufferCleanerCommand::Finalize)?;

        // ensure that we get notified about all errors (if any)
        while let Ok(msg) = self.rx.recv() {
            drop(msg?);
        }

        // and collect the final result
        let res = self.finalize_handle.join().unwrap();
        Ok(res)
    }
}

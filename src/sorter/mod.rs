use std::{io, num::NonZeroUsize, path::PathBuf, process};

use crate::{
    orderer::Orderer,
    run::{buf_run::BufRun, file_run::FileRun, Run},
};

use self::result_iter::ResultIterator;

pub mod result_iter;

pub struct ExtsortConfig {
    pub sort_buffer_size: NonZeroUsize,
    pub run_read_size: NonZeroUsize,
    pub temp_file_folder: PathBuf,
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
        source: S,
        orderer: O,
        mut buffer_sort: F,
    ) -> io::Result<ResultIterator<'a, T, O>>
    where
        S: Iterator<Item = T>,
        O: Orderer<T>,
        T: 'a,
        F: FnMut(&O, &mut [T]),
    {
        let pid = process::id();
        let self_addr = self as *const Self as usize;

        let max_buffer_size = self.config.sort_buffer_size.into();
        let mut sort_buffer = Vec::with_capacity(max_buffer_size);
        let mut sort_folder = self.config.temp_file_folder.clone();
        sort_folder.push("dummy");
        let mut file_runs = Vec::new();
        for item in source {
            sort_buffer.push(item);
            if sort_buffer.len() == max_buffer_size {
                buffer_sort(&orderer, &mut sort_buffer);
                sort_folder.set_file_name(format!(
                    "{}_{}_sort_file_{}",
                    pid,
                    self_addr,
                    file_runs.len()
                ));
                file_runs.push(FileRun::new(
                    &mut sort_buffer,
                    &sort_folder,
                    self.config.run_read_size,
                )?);
            }
        }

        let runs = file_runs
            .into_iter()
            .map(|a| Box::new(a) as Box<dyn Run<T> + '_>)
            .chain(Some(
                Box::new(BufRun::new(sort_buffer)) as Box<dyn Run<T> + '_>
            ));

        Ok(ResultIterator::new(runs, orderer))
    }
}

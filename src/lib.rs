// struct SequentialSorter {}

//! This crate implements an external sort for arbitrary
//! iterators of any size, assuming they fit on disk.
//! ```
//! # #[cfg(not(miri))]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use extsort_iter::*;
//!
//! let sequence = [3,21,42,9,5];
//!
//! // the default configuration will sort with up to 10M in buffered in Memory
//! // and place the files under /tmp
//! //
//! // you will most likely want to change at least the location.
//! let config = ExtsortConfig::default_for::<i32>();
//!
//! let data = sequence
//!     .iter()
//!     .cloned()
//!     .external_sort(config)?
//!     .collect::<Vec<_>>();
//! assert_eq!(&data, &[3,5,9,21,42]);
//! # Ok(())
//! # }
//! # #[cfg(miri)]
//! # fn main() {}
//! ```
//!
//! ## When not to use this crate
//!
//! When your source iterator is big because each item owns large amounts of heap memory.
//! That means the following case will result in memory exhaustion:
//! ```no_run
//! # use extsort_iter::*;
//! let data = "somestring".to_owned();
//! let iterator = std::iter::from_fn(|| Some(data.clone())).take(1_000_000);
//! let sorted  = iterator.external_sort(ExtsortConfig::default_for::<String>());
//! ```
//!
//! The reason for that is that we are not dropping the values from the source iterator until they are
//! yielded by the result iterator.
//!
//! You can think of it as buffering the entire input iterator, with the values
//! themselves living on disk but all memory the values point to still living on the heap.

#[warn(clippy::all, clippy::pedantic)]
#[allow(clippy::module_name_repetitions)]
pub mod extension_trait;
mod orderer;
mod run;
mod sorter;

pub use extension_trait::*;
pub use sorter::ExtsortConfig;

#[cfg(not(miri))]
#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, path::PathBuf};

    use crate::{extension_trait::ExtSortOrdExtension, sorter::ExtsortConfig};

    #[test]
    fn integration() {
        let sequence = [
            2, 82, 29, 86, 100, 67, 44, 19, 25, 10, 84, 47, 65, 42, 11, 24, 53, 92, 69, 49, 70, 36,
            8, 48, 16, 91, 62, 58, 55, 18, 27, 79, 76, 40, 22, 95, 99, 28, 17, 7, 59, 30, 97, 80,
            34, 33, 54, 45, 31, 52, 56, 1, 57, 38, 61, 6, 23, 94, 85, 51, 35, 68, 41, 15, 90, 14,
            74, 75, 32, 73, 83, 64, 77, 89, 4, 72, 71, 21, 63, 5, 39, 12, 20, 3, 43, 88, 26, 78,
            93, 60, 50, 13, 37, 87, 46, 96, 66, 98, 81, 9,
        ];

        let data = sequence
            .iter()
            .external_sort(ExtsortConfig {
                sort_buffer_size: NonZeroUsize::new(10).unwrap(),
                run_read_size: NonZeroUsize::new(2).unwrap(),
                temp_file_folder: PathBuf::from("/tmp"),
            })
            .unwrap();

        let is_sorted = data.zip(1..).all(|(l, r)| *l == r);

        assert!(is_sorted);
    }
}

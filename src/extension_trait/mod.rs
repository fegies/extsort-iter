#[cfg(any(feature = "parallel_sort", feature = "docsrs"))]
/// parallel ordering extension traits.
/// This module in only available when the `parallel_sort` feature is enabled
pub mod parallel;
/// sequential ordering extension traits
pub mod sequential;

#[cfg(any(feature = "parallel_sort", feature = "docsrs"))]
pub use parallel::*;
pub use sequential::*;

#[cfg(feature = "parallel_sort")]
/// parallel ordering extension traits.
/// This module in only available when the `parallel_sort` feature is enabled
pub mod parallel;
/// sequential ordering extension traits
pub mod sequential;

#[cfg(feature = "parallel_sort")]
pub use parallel::*;
pub use sequential::*;

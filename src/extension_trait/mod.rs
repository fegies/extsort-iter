pub mod parallel;
pub mod sequential;

// #[cfg(parallel_sort)]
pub use parallel::*;
pub use sequential::*;

use std::cmp::Ordering;

/// A generialisation of the Ord trait.
/// The main difference is that the Orderer is able to
/// reference some internal state as it is passed by ref
/// and compares some other type.
pub trait Orderer<T> {
    /// the type to compare
    fn compare(&self, left: &T, right: &T) -> Ordering;
}

/// An orderer that just delegates to the Ord implementation on the type itself
#[derive(Default)]
pub struct OrdOrderer {}
impl OrdOrderer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<T: Ord> Orderer<T> for OrdOrderer {
    fn compare(&self, left: &T, right: &T) -> Ordering {
        left.cmp(right)
    }
}

/// an orderer that compares values based on a key extracted from then.
pub struct KeyOrderer<F> {
    key_extractor: F,
}
impl<F> KeyOrderer<F> {
    pub fn new<T, K>(key_extractor: F) -> Self
    where
        F: Fn(&T) -> K,
        K: Ord,
    {
        Self { key_extractor }
    }
}

impl<F, T, K> Orderer<T> for KeyOrderer<F>
where
    F: Fn(&T) -> K,
    K: Ord,
{
    fn compare(&self, left: &T, right: &T) -> Ordering {
        let left = (self.key_extractor)(left);
        let right = (self.key_extractor)(right);
        left.cmp(&right)
    }
}

/// an orderer that compares values by delegating to a comparison function
pub struct FuncOrderer<F> {
    comparator: F,
}

impl<F> FuncOrderer<F> {
    pub fn new<T>(comparator: F) -> Self
    where
        F: Fn(&T, &T) -> Ordering,
    {
        Self { comparator }
    }
}

impl<F, T> Orderer<T> for FuncOrderer<F>
where
    F: Fn(&T, &T) -> Ordering,
{
    fn compare(&self, left: &T, right: &T) -> Ordering {
        (self.comparator)(left, right)
    }
}

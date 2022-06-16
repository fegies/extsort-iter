// struct SequentialSorter {}

mod orderer;
mod run;
mod sorter;

#[cfg(test)]
mod tests {
    #[test]
    fn integration() {
        let sequence = [
            2, 82, 29, 86, 100, 67, 44, 19, 25, 10, 84, 47, 65, 42, 11, 24, 53, 92, 69, 49, 70, 36,
            8, 48, 16, 91, 62, 58, 55, 18, 27, 79, 76, 40, 22, 95, 99, 28, 17, 7, 59, 30, 97, 80,
            34, 33, 54, 45, 31, 52, 56, 1, 57, 38, 61, 6, 23, 94, 85, 51, 35, 68, 41, 15, 90, 14,
            74, 75, 32, 73, 83, 64, 77, 89, 4, 72, 71, 21, 63, 5, 39, 12, 20, 3, 43, 88, 26, 78,
            93, 60, 50, 13, 37, 87, 46, 96, 66, 98, 81, 9,
        ];

        let data = sequence.iter();

        fn is_sorted(mut source: impl Iterator<Item = impl Ord>) -> Option<bool> {
            let mut prev = source.next()?;
            for next in source {
                if next < prev {
                    return Some(false);
                }
                prev = next;
            }

            Some(true)
        }
        assert!(is_sorted(data).unwrap_or(true));
    }
}

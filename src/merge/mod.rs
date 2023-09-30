mod array_node;
mod treebuilder;

use std::{cmp::Ordering, marker::PhantomData};

use crate::{orderer::Orderer, run::Run};

use self::{
    array_node::{TreeNode, Winner},
    treebuilder::LoserTreeBuilder,
};

/// invariants:
/// the winner must always point to the tape whose
/// head is the smallest element.
pub struct LoserTree<T, R, O> {
    loser_indices: Vec<u32>,
    tapes: Vec<R>,
    orderer: O,
    phantom: PhantomData<T>,
    winner: Winner,
    remaining_tapes: usize,
}

/// returns the largest power of 2 less or equal to the provided number
fn previous_power_of_two(number: usize) -> usize {
    let leading_zeros = number.leading_zeros();
    const SHIFT_TO_HIGHEST_BIT: usize = core::mem::size_of::<usize>() * 8 - 1;
    let shift = SHIFT_TO_HIGHEST_BIT - leading_zeros as usize;
    1 << shift
}

fn get_candidate<T>(runs: &[impl Run<T>], candidate: Winner) -> Option<&T> {
    runs[candidate.idx as usize].peek()
}

fn compare_winners<T>(
    runs: &[impl Run<T>],
    orderer: &impl Orderer<T>,
    left: Winner,
    right: Winner,
) -> Ordering {
    match (get_candidate(runs, left), get_candidate(runs, right)) {
        (Some(l), Some(r)) => orderer.compare(l, r),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

impl<T, R, O> LoserTree<T, R, O>
where
    R: Run<T>,
    O: Orderer<T>,
{
    /// Constructs a new loser tree from the given tapes
    /// and a provided ordering instruction.
    pub fn new(tapes: Vec<R>, orderer: O) -> Self {
        let remaining_tapes = tapes.len();
        let mut result = Self {
            loser_indices: Vec::new(),
            remaining_tapes,
            tapes,
            orderer,
            winner: Winner { idx: u32::MAX },
            phantom: PhantomData,
        };

        result.winner = result.rebuild_tree();

        result
    }

    /// returns the remaining items of all merged runs.
    pub fn remaining_items(&self) -> usize {
        self.tapes.iter().map(|t| t.remaining_items()).sum()
    }

    /// advances the internal state
    /// Once this method returns None, it will never yield any elements again.
    pub fn next(&mut self) -> Option<T> {
        if self.tapes.len() <= 1 {
            return self.tapes.first_mut()?.next();
        }

        let winning_tape = &mut self.tapes[self.winner.idx as usize];
        let winning_value = winning_tape.next()?;
        let tape_exhausted = winning_tape.peek().is_none();

        self.winner = if tape_exhausted {
            // while we surely know that the next result must be a None
            // because the peek call did not return anything,
            // reading the tape past the end will allow it to release
            // backing resources already.
            let none = winning_tape.next();
            debug_assert!(none.is_none());

            self.remove_winner(self.winner)
        } else {
            self.replay_matches(self.winner)
        };

        Some(winning_value)
    }

    /// rebuilds the loser tree, returning the new winner leaf
    /// after reconstruction.
    fn rebuild_tree(&mut self) -> Winner {
        self.tapes.retain(|t| t.peek().is_some());
        self.remaining_tapes = self.tapes.len();

        if self.tapes.len() > 1 {
            LoserTreeBuilder::new(
                |left, right| compare_winners(&self.tapes, &self.orderer, left, right),
                &mut self.loser_indices,
            )
            .build(self.tapes.len())
        } else {
            Winner { idx: 0 }
        }
    }

    fn compare_winners(&self, left: Winner, right: Winner) -> Ordering {
        compare_winners(&self.tapes, &self.orderer, left, right)
    }

    /// replay the matches from the previous winner back up to the root.
    /// this must be applied to the tree after the winner was modified.
    fn replay_matches(&mut self, previous_winner: Winner) -> Winner {
        let mut winner = previous_winner;
        let mut current_node = self.get_leaf_node(previous_winner).parent();
        loop {
            let challenger = Winner {
                idx: self.loser_indices[current_node.idx],
            };

            if self.compare_winners(challenger, winner).is_lt() {
                // the challenger won, note the previous winner in the tree and continue with the challenger
                self.loser_indices[current_node.idx] = winner.idx;
                winner = challenger;
            }

            if current_node.is_root() {
                return winner;
            }
            current_node = current_node.parent();
        }
    }

    /// removes the previous winner node from the tree, shrinking it in the process.
    /// this involves a recomputation of the tree as a new node must be the winner after.
    fn remove_winner(&mut self, previous_winner: Winner) -> Winner {
        // we must have at least two runs remaining for it to make sense to remove one.
        debug_assert!(!self.loser_indices.is_empty());

        self.remaining_tapes -= 1;

        let number_of_tapes = self.tapes.len();
        let rebuild_threshold = previous_power_of_two(number_of_tapes - 1);

        if self.remaining_tapes <= rebuild_threshold {
            // we have exhausted enough tapes that the tree will be one level less deep.
            // we can take this opportunity to drop runs as well as rebuild the tree
            self.rebuild_tree()
        } else {
            // we have not exhausted enough tapes for it to actually make
            // sense to rebuild the tree. Instead we rely on the fact that
            // an exhausted tape is always compared to be greater than anything
            // else and just replay to the root as usual.
            self.replay_matches(previous_winner)
        }
    }

    /// computes the tree node the leaf _would_ occuppy if we did store the leaves
    fn get_leaf_node(&self, leaf: Winner) -> TreeNode {
        let tree_size = self.tapes.len();
        TreeNode::leaf_for_winner(leaf, tree_size)
    }
}

impl<T, R, O> Iterator for LoserTree<T, R, O>
where
    R: Run<T>,
    O: Orderer<T>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining_items();
        (remaining, Some(remaining))
    }
}
impl<T, R, O> ExactSizeIterator for LoserTree<T, R, O>
where
    R: Run<T>,
    O: Orderer<T>,
{
    fn len(&self) -> usize {
        self.remaining_items()
    }
}

#[cfg(test)]
mod test {

    use crate::{orderer::OrdOrderer, run::buf_run::BufRun};

    use super::LoserTree;

    fn run_merge_test(runs: Vec<Vec<u32>>) {
        let buf_runs = runs.iter().cloned().map(BufRun::new).collect();
        let mut merger = LoserTree::new(buf_runs, OrdOrderer::new());

        let mut result = Vec::new();
        while let Some(next) = merger.next() {
            result.push(next);
        }

        let mut expected: Vec<_> = runs.iter().flatten().cloned().collect();
        expected.sort();

        if expected != result {
            for run in &runs {
                println!("run: {run:?}");
            }
        }
        assert_eq!(expected, result);
    }

    #[test]
    fn test_merge_runs() {
        let run_1 = vec![1, 3, 5, 7];
        let run_4 = vec![0, 2, 4, 6];
        let run_3 = vec![8, 10, 12, 14];
        let run_2 = vec![9, 11, 13, 15];

        run_merge_test(vec![run_1, run_2, run_3, run_4]);
    }

    #[test]
    fn test_merge_unbalanced() {
        let run_1 = vec![1, 4];
        let run_2 = vec![5, 6, 7];
        let run_3 = vec![2, 3];

        run_merge_test(vec![run_1, run_3, run_2]);
    }

    #[test]
    fn test_merge_five() {
        let runs = vec![
            vec![20, 73],
            vec![29, 73],
            vec![3, 84],
            vec![33, 70],
            vec![63, 95],
        ];
        run_merge_test(runs);
    }

    #[cfg(not(miri))]
    // the only reason this is disabled on miri is that it would run too slowly
    mod random {
        use std::sync::{Arc, Mutex};

        use rand::{rngs::ThreadRng, RngCore};

        use super::run_merge_test;

        fn generate_run(rng: &mut ThreadRng, len: usize) -> Vec<u32> {
            let mut run = Vec::with_capacity(len);
            for _ in 0..len {
                run.push(rng.next_u32());
            }
            run.sort();
            run
        }

        #[test]
        fn test_merge_runs_random() {
            let params = (1..100).flat_map(move |runs| {
                (1..20).flat_map(move |items| (1..5).map(move |_| (runs, items)))
            });

            let params = Arc::new(Mutex::new(params));

            let threads: Vec<_> = (0..num_cpus::get())
                .map(|_| {
                    let params = params.clone();
                    std::thread::spawn(move || {
                        let mut rng = rand::thread_rng();
                        loop {
                            let next = params.lock().unwrap().next();
                            if let Some((num_runs, num_items)) = next {
                                let runs: Vec<_> =
                                    core::iter::repeat_with(|| generate_run(&mut rng, num_items))
                                        .take(num_runs)
                                        .collect();
                                run_merge_test(runs);
                            } else {
                                break;
                            }
                        }
                    })
                })
                .collect();

            threads.into_iter().for_each(|t| t.join().unwrap());
        }
    }
}

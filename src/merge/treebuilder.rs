use std::{cmp::Ordering, ops::Range};

use super::{TreeNode, Winner};

/// This module contains the code to construct a complete loser tree
/// in an implicit array representation.

/// This is a convenience struct to move the tree construction code out from the main merge
/// code
pub(super) struct LoserTreeBuilder<'a, C> {
    winner_comparer: C,
    loser_tree: &'a mut Vec<u32>,
}

/// split a range at the specified point
/// the split point shall denote the size of the left
/// range piece after the split.
fn split_range(range: Range<u32>, split_point: usize) -> (Range<u32>, Range<u32>) {
    let midpoint = range.start + split_point as u32;
    let mut left = range.clone();
    let mut right = range;
    left.end = midpoint;
    right.start = midpoint;
    (left, right)
}

impl<'a, C> LoserTreeBuilder<'a, C>
where
    C: FnMut(Winner, Winner) -> Ordering,
{
    pub fn new(winner_comparer: C, loser_tree: &'a mut Vec<u32>) -> Self {
        Self {
            winner_comparer,
            loser_tree,
        }
    }

    pub(super) fn build(mut self, number_of_tapes: usize) -> Winner {
        let num_internal_nodes = number_of_tapes - 1;
        if self.loser_tree.len() < num_internal_nodes {
            *self.loser_tree = vec![0; num_internal_nodes];
        }
        self.build_tree_complete(0..number_of_tapes as u32, TreeNode::root())
    }

    fn build_tree_perfect(&mut self, range_todo: Range<u32>, root: TreeNode) -> Winner {
        if range_todo.len() == 1 {
            Winner {
                idx: range_todo.start,
            }
        } else {
            let subtree_size = range_todo.len() / 2;
            let (left, right) = split_range(range_todo, subtree_size);
            let winner_left = self.build_tree_perfect(left, root.left());
            let winner_right = self.build_tree_perfect(right, root.right());
            self.commit_winner(winner_left, winner_right, root)
        }
    }

    fn build_tree_complete(&mut self, range_todo: Range<u32>, root: TreeNode) -> Winner {
        let total_nodes = range_todo.len();
        if total_nodes.is_power_of_two() {
            self.build_tree_perfect(range_todo, root)
        } else {
            let nodes_if_lowest_level_was_full = total_nodes.next_power_of_two();
            let nodes_in_lower_level = (total_nodes - nodes_if_lowest_level_was_full / 2) * 2;

            let perfect_tree_left = nodes_in_lower_level >= nodes_if_lowest_level_was_full / 2;

            if perfect_tree_left {
                let nodes_in_left_tree = nodes_if_lowest_level_was_full / 2;
                let (left, right) = split_range(range_todo, nodes_in_left_tree);
                let w_left = self.build_tree_perfect(left, root.left());
                let w_right = self.build_tree_complete(right, root.right());
                self.commit_winner(w_left, w_right, root)
            } else {
                // there are _not_ enough nodes to fill left side of the tree completely.
                // therefore the perfect tree must be on the right and have the size of half the upper level
                let nodes_in_right_tree = nodes_if_lowest_level_was_full / 2 / 2;
                let nodes_in_left_tree = total_nodes - nodes_in_right_tree;
                let (left, right) = split_range(range_todo, nodes_in_left_tree);
                let w_left = self.build_tree_complete(left, root.left());
                let w_right = self.build_tree_perfect(right, root.right());
                self.commit_winner(w_left, w_right, root)
            }
        }
    }

    fn commit_winner(
        &mut self,
        candidate_a: Winner,
        candidate_b: Winner,
        root: TreeNode,
    ) -> Winner {
        let (winner, loser) = if (self.winner_comparer)(candidate_a, candidate_b).is_le() {
            // left side wins !
            (candidate_a, candidate_b)
        } else {
            // right side wins
            (candidate_b, candidate_a)
        };
        self.loser_tree[root.idx] = loser.idx;
        winner
    }
}

#[cfg(test)]
mod test {
    use crate::orderer::{OrdOrderer, Orderer};

    use super::LoserTreeBuilder;

    fn assert_winner(tape: &[i64]) {
        let orderer = OrdOrderer {};
        let min_value = *tape.iter().min().unwrap();
        let ref_tape = tape.iter().map(|i| i).collect::<Vec<_>>();
        let mut tree = Vec::new();
        let winner = LoserTreeBuilder::new(
            |a, b| orderer.compare(ref_tape[a.idx as usize], ref_tape[b.idx as usize]),
            &mut tree,
        )
        .build(ref_tape.len());
        assert_eq!(min_value, tape[winner.idx as usize]);
        if tape.len() > 1 {
            assert!(min_value <= tape[tree[0] as usize])
        }
    }

    #[test]
    fn test_power_two() {
        assert!(1usize.is_power_of_two())
    }

    fn run_tree_construction_test(max_size: usize) {
        for r in 1..max_size {
            println!("constructing zero tree with {r} tapes");
            assert_winner(&vec![0; r]);
            println!("constructing ordered tree with {r} tapes");
            let mut tape = (0..r as i64).into_iter().collect::<Vec<_>>();
            assert_winner(&tape);
            println!("constructing reversed tree wtih {r} tapes");
            let mut reversed_tape = tape.clone();
            reversed_tape.reverse();
            assert_winner(&reversed_tape);
            println!("constructing tree with min in the middle");
            reversed_tape.append(&mut tape);
            assert_winner(&reversed_tape);
        }
    }

    #[test]
    fn test_construct_tree_small() {
        run_tree_construction_test(10);
    }

    // this would be too slow to execute with miri.
    // instead, there is a smaller version with less cases.
    #[test]
    #[cfg(not(miri))]
    fn test_construct_tree() {
        run_tree_construction_test(100);
    }
}

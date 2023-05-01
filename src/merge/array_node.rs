use super::previous_power_of_two;

#[derive(Clone, Copy, Debug)]
pub struct Winner {
    pub idx: u32,
}

/// This is a wrapper around the tree indices in
/// the classic implicit array tree.
/// it mainly exists for clarity, as well as to make
/// implementing the tree navigation easier.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct TreeNode {
    pub idx: usize,
}
impl TreeNode {
    pub fn left(self) -> Self {
        Self {
            idx: self.idx * 2 + 1,
        }
    }
    pub fn right(self) -> Self {
        Self {
            idx: self.idx * 2 + 2,
        }
    }
    pub fn parent(self) -> Self {
        Self {
            idx: self.idx.saturating_sub(1) / 2,
        }
    }
    pub fn root() -> Self {
        Self { idx: 0 }
    }
    pub fn is_root(&self) -> bool {
        self.idx == 0
    }

    /// computes the tree node the leaf _would_ occuppy if we did store the leaves
    /// tree_size should be the total number of leaves in the tree.
    pub fn leaf_for_winner(leaf: Winner, tree_size: usize) -> Self {
        let full_level_size = previous_power_of_two(tree_size);
        let overhang = (tree_size - full_level_size) * 2;

        let leaf_idx = leaf.idx as usize;

        let tree_idx = if leaf_idx < overhang {
            // we are in the lowest level of the tree... the indexes here start at
            // 2*the filled level - 1
            full_level_size * 2 - 1 + leaf_idx
        } else {
            // we are in the filled part of the tree. the indexes of this one start at
            // the filled level - 1

            // for every two leaves in the overhang, there is an internal node
            // that can not be used.
            // because we would be counting them twice, we need to subtract half again.
            let overhang_compensation = overhang / 2;

            full_level_size - 1 - overhang_compensation + leaf_idx
        };

        Self { idx: tree_idx }
    }
}

#[cfg(test)]
mod test {
    use crate::merge::array_node::{TreeNode, Winner};

    #[test]
    fn test_leaf_calculation() {
        fn run_test(expected_values: &[usize]) {
            let tree_size = expected_values.len() as u32;
            let values: Vec<_> = (0..tree_size)
                .map(|leaf| TreeNode::leaf_for_winner(Winner { idx: leaf }, tree_size as usize).idx)
                .collect();
            assert_eq!(expected_values, values);
        }
        run_test(&[1, 2]);
        run_test(&[3, 4, 2]);
        run_test(&[3, 4, 5, 6]);
        run_test(&[7, 8, 4, 5, 6]);
        run_test(&[7, 8, 9, 10, 5, 6]);
        run_test(&[7, 8, 9, 10, 11, 12, 6]);
        run_test(&[7, 8, 9, 10, 11, 12, 13, 14]);
        run_test(&[15, 16, 8, 9, 10, 11, 12, 13, 14]);
    }
}

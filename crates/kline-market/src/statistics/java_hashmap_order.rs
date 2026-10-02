//! Java 21 default `HashMap<String, _>` iteration after `put` operations.
//!
//! Statistic sorts are stable: equal float volumes keep this order, so even
//! collision-triggered resizes and tree bins can affect the selected top N.
//! This module returns positions, owns no statistical values, and handles only
//! the insert/replace operations used by the Java statistic calculation.
use std::cmp::Ordering;

#[derive(Clone, Copy, Default)]
struct Node {
    parent: Option<usize>,
    children: [Option<usize>; 2],
    red: bool,
}
#[derive(Default)]
struct Bucket {
    members: Vec<usize>,
    root: Option<usize>,
}
struct Order<'a> {
    keys: &'a [&'a str],
    hashes: Vec<u32>,
    nodes: Vec<Node>,
    buckets: Vec<Bucket>,
}

/// Each unique key appears once, at its last assigned input position. Replacing
/// a value keeps the key's original bucket/tree position and does not resize.
pub(super) fn indices(keys: &[&str]) -> Vec<usize> {
    if keys.is_empty() {
        return Vec::new();
    }
    let hashes = keys
        .iter()
        .map(|key| {
            let hash = key.encode_utf16().fold(0_u32, |hash, c| {
                hash.wrapping_mul(31).wrapping_add(u32::from(c))
            });
            hash ^ (hash >> 16)
        })
        .collect();
    let mut order = Order {
        keys,
        hashes,
        nodes: vec![Node::default(); keys.len()],
        buckets: (0..16).map(|_| Bucket::default()).collect(),
    };
    let mut latest: Vec<_> = (0..keys.len()).collect();
    let mut size = 0;
    for index in 0..keys.len() {
        let slot = order.hashes[index] as usize & (order.buckets.len() - 1);
        let mut bucket = std::mem::take(&mut order.buckets[slot]);
        if let Some(existing) = order.find(&bucket, index) {
            latest[existing] = index;
            order.buckets[slot] = bucket;
            continue;
        }
        let grow = if let Some(mut root) = bucket.root {
            let parent = order.insert(&mut root, index);
            let position = bucket.members.iter().position(|i| *i == parent).unwrap();
            bucket.members.insert(position + 1, index);
            bucket.root = Some(root);
            move_root(&mut bucket);
            false
        } else {
            bucket.members.push(index);
            if bucket.members.len() >= 9 {
                if order.buckets.len() < 64 {
                    true
                } else {
                    order.treeify(&mut bucket);
                    false
                }
            } else {
                false
            }
        };
        order.buckets[slot] = bucket;
        if grow {
            order.resize();
        }
        size += 1;
        if size > order.buckets.len() * 3 / 4 {
            order.resize();
        }
    }
    order
        .buckets
        .into_iter()
        .flat_map(|b| b.members)
        .map(|index| latest[index])
        .collect()
}
fn move_root(bucket: &mut Bucket) {
    let root = bucket.root.unwrap();
    let position = bucket.members.iter().position(|i| *i == root).unwrap();
    bucket.members.remove(position);
    bucket.members.insert(0, root);
}
impl Order<'_> {
    fn find(&self, bucket: &Bucket, index: usize) -> Option<usize> {
        if let Some(mut node) = bucket.root {
            loop {
                let comparison = self.compare(index, node);
                if comparison == Ordering::Equal {
                    return Some(node);
                }
                let side = usize::from(comparison == Ordering::Greater);
                node = self.nodes[node].children[side]?;
            }
        }
        bucket.members.iter().copied().find(|old| {
            self.hashes[index] == self.hashes[*old] && self.keys[index] == self.keys[*old]
        })
    }
    fn compare(&self, a: usize, b: usize) -> Ordering {
        (self.hashes[a] as i32)
            .cmp(&(self.hashes[b] as i32))
            .then_with(|| self.keys[a].encode_utf16().cmp(self.keys[b].encode_utf16()))
    }
    fn treeify(&mut self, bucket: &mut Bucket) {
        let mut root = bucket.members[0];
        self.nodes[root] = Node::default();
        for index in bucket.members.iter().skip(1).copied() {
            self.insert(&mut root, index);
        }
        bucket.root = Some(root);
        move_root(bucket);
    }
    fn resize(&mut self) {
        let capacity = self.buckets.len();
        let previous = std::mem::replace(
            &mut self.buckets,
            (0..2 * capacity).map(|_| Bucket::default()).collect(),
        );
        for (slot, bucket) in previous.into_iter().enumerate() {
            let mut low = Bucket::default();
            let mut high = Bucket::default();
            for index in bucket.members {
                if self.hashes[index] as usize & capacity == 0 {
                    low.members.push(index);
                } else {
                    high.members.push(index);
                }
            }
            if bucket.root.is_some() {
                let split = !low.members.is_empty() && !high.members.is_empty();
                for part in [&mut low, &mut high] {
                    if part.members.len() > 6 {
                        if split {
                            self.treeify(part);
                        } else {
                            part.root = bucket.root;
                        }
                    }
                }
            }
            self.buckets[slot] = low;
            self.buckets[slot + capacity] = high;
        }
    }
    fn insert(&mut self, root: &mut usize, index: usize) -> usize {
        self.nodes[index] = Node {
            red: true,
            ..Node::default()
        };
        let mut parent = *root;
        loop {
            let side = usize::from(self.compare(index, parent) == Ordering::Greater);
            if let Some(next) = self.nodes[parent].children[side] {
                parent = next;
            } else {
                self.nodes[parent].children[side] = Some(index);
                self.nodes[index].parent = Some(parent);
                break;
            }
        }
        self.balance(root, index);
        parent
    }
    // Rotate toward side 0 (left) or 1 (right).
    fn rotate(&mut self, root: &mut usize, pivot: usize, side: usize) {
        let child = self.nodes[pivot].children[1 - side].unwrap();
        let inner = self.nodes[child].children[side];
        self.nodes[pivot].children[1 - side] = inner;
        if let Some(inner) = inner {
            self.nodes[inner].parent = Some(pivot);
        }
        let parent = self.nodes[pivot].parent;
        self.nodes[child].parent = parent;
        if let Some(parent) = parent {
            let branch = usize::from(self.nodes[parent].children[1] == Some(pivot));
            self.nodes[parent].children[branch] = Some(child);
        } else {
            *root = child;
            self.nodes[child].red = false;
        }
        self.nodes[child].children[side] = Some(pivot);
        self.nodes[pivot].parent = Some(child);
    }
    fn balance(&mut self, root: &mut usize, mut index: usize) {
        loop {
            let Some(mut parent) = self.nodes[index].parent else {
                self.nodes[index].red = false;
                *root = index;
                break;
            };
            let Some(mut grandparent) = self.nodes[parent].parent else {
                break;
            };
            if !self.nodes[parent].red {
                break;
            }
            let side = usize::from(self.nodes[grandparent].children[1] == Some(parent));
            let uncle = self.nodes[grandparent].children[1 - side];
            if let Some(uncle) = uncle.filter(|u| self.nodes[*u].red) {
                self.nodes[uncle].red = false;
                self.nodes[parent].red = false;
                self.nodes[grandparent].red = true;
                index = grandparent;
                continue;
            }
            if self.nodes[parent].children[1 - side] == Some(index) {
                index = parent;
                self.rotate(root, index, side);
                parent = self.nodes[index].parent.unwrap();
                grandparent = self.nodes[parent].parent.unwrap();
            }
            self.nodes[parent].red = false;
            self.nodes[grandparent].red = true;
            self.rotate(root, grandparent, 1 - side);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::indices;
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Case {
        name: String,
        keys: Vec<String>,
        expected_indices: Vec<usize>,
    }
    #[test]
    fn order_matches_java_oracle_for_trees_resizes_unicode_and_replacements() {
        let cases: Vec<Case> = serde_json::from_str(include_str!(
            "../../tests/fixtures/java_hashmap_order_20260928.json"
        ))
        .unwrap();
        for case in cases {
            let keys: Vec<_> = case.keys.iter().map(String::as_str).collect();
            assert_eq!(indices(&keys), case.expected_indices, "{}", case.name);
        }
    }
}

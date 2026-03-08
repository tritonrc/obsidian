//! Sorted posting list for inverted index operations.
//!
//! `PostingList` maintains a sorted `Vec<u64>` of IDs, supporting efficient
//! set intersection and union via sorted merge algorithms.

use serde::{Deserialize, Serialize};

/// A sorted list of IDs used as an inverted index entry.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostingList {
    ids: Vec<u64>,
}

impl PostingList {
    /// Create an empty posting list.
    pub fn new() -> Self {
        Self { ids: Vec::new() }
    }

    /// Insert an ID, maintaining sorted order. No-op if already present.
    pub fn insert(&mut self, id: u64) {
        match self.ids.binary_search(&id) {
            Ok(_) => {} // already present
            Err(pos) => self.ids.insert(pos, id),
        }
    }

    /// Remove an ID. Returns true if it was present.
    pub fn remove(&mut self, id: u64) -> bool {
        match self.ids.binary_search(&id) {
            Ok(pos) => {
                self.ids.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    /// Check if an ID is present.
    pub fn contains(&self, id: u64) -> bool {
        self.ids.binary_search(&id).is_ok()
    }

    /// Number of IDs in the list.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Get the sorted IDs as a slice.
    pub fn ids(&self) -> &[u64] {
        &self.ids
    }
}

/// Intersect multiple posting lists, starting with the smallest.
/// Returns a sorted Vec of IDs present in ALL lists.
pub fn intersect(lists: &[&PostingList]) -> Vec<u64> {
    if lists.is_empty() {
        return Vec::new();
    }
    if lists.len() == 1 {
        return lists[0].ids.clone();
    }

    // Sort by length so we start with the smallest
    let mut sorted: Vec<&PostingList> = lists.to_vec();
    sorted.sort_by_key(|l| l.len());

    let mut result = sorted[0].ids.clone();
    for list in &sorted[1..] {
        result = intersect_two(&result, &list.ids);
        if result.is_empty() {
            break;
        }
    }
    result
}

/// Intersect two sorted slices using merge algorithm. O(n+m).
fn intersect_two(a: &[u64], b: &[u64]) -> Vec<u64> {
    let mut result = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                result.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    result
}

/// Union two posting lists into a new one.
pub fn union(a: &PostingList, b: &PostingList) -> PostingList {
    let mut ids = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.ids.len() && j < b.ids.len() {
        match a.ids[i].cmp(&b.ids[j]) {
            std::cmp::Ordering::Less => {
                ids.push(a.ids[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                ids.push(b.ids[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                ids.push(a.ids[i]);
                i += 1;
                j += 1;
            }
        }
    }
    ids.extend_from_slice(&a.ids[i..]);
    ids.extend_from_slice(&b.ids[j..]);
    PostingList { ids }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_maintains_sorted_order() {
        let mut pl = PostingList::new();
        pl.insert(5);
        pl.insert(1);
        pl.insert(3);
        pl.insert(1); // duplicate
        assert_eq!(pl.ids(), &[1, 3, 5]);
        assert_eq!(pl.len(), 3);
    }

    #[test]
    fn test_remove() {
        let mut pl = PostingList::new();
        pl.insert(1);
        pl.insert(2);
        pl.insert(3);
        assert!(pl.remove(2));
        assert!(!pl.remove(4));
        assert_eq!(pl.ids(), &[1, 3]);
    }

    #[test]
    fn test_contains() {
        let mut pl = PostingList::new();
        pl.insert(10);
        pl.insert(20);
        assert!(pl.contains(10));
        assert!(!pl.contains(15));
    }

    #[test]
    fn test_intersect_two_lists() {
        let mut a = PostingList::new();
        let mut b = PostingList::new();
        for id in [1, 3, 5, 7, 9] {
            a.insert(id);
        }
        for id in [2, 3, 5, 8, 9] {
            b.insert(id);
        }
        let result = intersect(&[&a, &b]);
        assert_eq!(result, vec![3, 5, 9]);
    }

    #[test]
    fn test_intersect_three_lists() {
        let mut a = PostingList::new();
        let mut b = PostingList::new();
        let mut c = PostingList::new();
        for id in [1, 2, 3, 4, 5] {
            a.insert(id);
        }
        for id in [2, 3, 5, 7] {
            b.insert(id);
        }
        for id in [3, 5, 8] {
            c.insert(id);
        }
        let result = intersect(&[&a, &b, &c]);
        assert_eq!(result, vec![3, 5]);
    }

    #[test]
    fn test_intersect_empty() {
        let a = PostingList::new();
        let mut b = PostingList::new();
        b.insert(1);
        assert_eq!(intersect(&[&a, &b]), Vec::<u64>::new());
        assert_eq!(intersect(&[]), Vec::<u64>::new());
    }

    #[test]
    fn test_intersect_single() {
        let mut a = PostingList::new();
        a.insert(1);
        a.insert(2);
        assert_eq!(intersect(&[&a]), vec![1, 2]);
    }

    #[test]
    fn test_union() {
        let mut a = PostingList::new();
        let mut b = PostingList::new();
        for id in [1, 3, 5] {
            a.insert(id);
        }
        for id in [2, 3, 6] {
            b.insert(id);
        }
        let result = union(&a, &b);
        assert_eq!(result.ids(), &[1, 2, 3, 5, 6]);
    }

    #[test]
    fn test_union_empty() {
        let a = PostingList::new();
        let b = PostingList::new();
        assert!(union(&a, &b).is_empty());
    }

    #[test]
    fn test_is_empty() {
        let pl = PostingList::new();
        assert!(pl.is_empty());
    }
}

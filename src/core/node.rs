use core::borrow::Borrow;
use std::ops::Deref;

pub trait NodeLike<T: Ord> {
    #[allow(dead_code)]
    fn with_capacity(capacity: usize) -> Self;
    #[allow(dead_code)]
    fn get_ith(&self, index: usize) -> Option<&T>;
    #[allow(dead_code)]
    fn halve(&mut self) -> Self;
    #[allow(dead_code)]
    fn need_to_split(&self, border: usize, value: &T) -> bool;
    #[allow(dead_code)]
    fn len(&self) -> usize;
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[allow(dead_code)]
    fn capacity(&self) -> usize;
    #[allow(dead_code)]
    fn insert(&mut self, value: T) -> (bool, usize);
    #[allow(dead_code)]
    fn contains<Q: Ord + ?Sized>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>;
    #[allow(dead_code)]
    /// Must return `Some(i)` exactly when [`Self::contains`] is true, with
    /// `get_ith(i)` equal to the requested value.
    fn try_select<Q: Ord + ?Sized>(&self, value: &Q) -> Option<usize>
    where
        T: Borrow<Q>;
    #[allow(dead_code)]
    fn rank<Q: Ord + ?Sized>(&self, bound: std::ops::Bound<&Q>, from_start: bool) -> Option<usize>
    where
        T: Borrow<Q>;
    #[allow(dead_code)]
    fn delete<Q: Ord + ?Sized>(&mut self, value: &Q) -> Option<(T, usize)>
    where
        T: Borrow<Q>;
    // Positional deletion is only used by the multimap concurrent-removal
    // recovery path. Gate it so enabling this trait method is not an
    // unconditional source-compatibility break for custom NodeLike impls that
    // do not use multimap.
    #[cfg(feature = "multimap")]
    #[allow(dead_code)]
    fn delete_at(&mut self, index: usize) -> Option<T>;
    #[allow(dead_code)]
    fn replace(&mut self, idx: usize, value: T) -> Option<T>;
    #[allow(dead_code)]
    fn max(&self) -> Option<&T>;
    #[allow(dead_code)]
    fn min(&self) -> Option<&T>;
    #[allow(dead_code)]
    fn iter<'a>(&'a self) -> std::slice::Iter<'a, T>
    where
        T: 'a;
}

#[cfg(all(feature = "std-binary-search", not(feature = "custom-binary-search")))]
mod search_backend {
    use core::borrow::Borrow;

    #[cfg(test)]
    pub(super) const NAME: &str = "std";

    #[inline]
    pub(super) fn search<Q, T>(haystack: &[T], needle: &Q) -> Result<usize, usize>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        haystack.binary_search_by(|candidate| candidate.borrow().cmp(needle))
    }
}

#[cfg(all(
    feature = "superslice-binary-search",
    not(any(
        feature = "custom-binary-search",
        feature = "std-binary-search",
        feature = "wt-slice-binary-search"
    ))
))]
mod search_backend {
    use core::borrow::Borrow;
    use superslice::Ext;

    #[cfg(test)]
    pub(super) const NAME: &str = "superslice";

    #[inline]
    pub(super) fn search<Q, T>(haystack: &[T], needle: &Q) -> Result<usize, usize>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let index = haystack.lower_bound_by(|candidate| candidate.borrow().cmp(needle));
        match haystack.get(index) {
            Some(candidate) if candidate.borrow().cmp(needle).is_eq() => Ok(index),
            _ => Err(index),
        }
    }
}

#[cfg(all(
    feature = "wt-slice-binary-search",
    not(any(feature = "custom-binary-search", feature = "std-binary-search"))
))]
mod search_backend {
    use core::borrow::Borrow;
    use wt_slice::ExactSearch;

    #[cfg(test)]
    pub(super) const NAME: &str = "wt-slice";

    #[inline]
    pub(super) fn search<Q, T>(haystack: &[T], needle: &Q) -> Result<usize, usize>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        haystack.exact_search_by(|candidate| candidate.borrow().cmp(needle))
    }
}

#[cfg(any(
    feature = "custom-binary-search",
    not(any(
        feature = "std-binary-search",
        feature = "superslice-binary-search",
        feature = "wt-slice-binary-search"
    ))
))]
mod search_backend {
    use core::borrow::Borrow;
    use core::cmp::Ordering;

    #[cfg(test)]
    pub(super) const NAME: &str = "custom";

    #[inline]
    pub(super) fn search<Q, T>(haystack: &[T], needle: &Q) -> Result<usize, usize>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        let mut j = haystack.len();
        let mut i = 0;
        let mut m = j >> 1;

        while i != j {
            debug_assert!(i <= m && m < j && j <= haystack.len());
            // SAFETY: initialization establishes `i <= m < j <= haystack.len()`
            // for a non-empty range, and both branches preserve that invariant.
            let candidate = unsafe { haystack.get_unchecked(m) };
            match candidate.borrow().cmp(needle) {
                Ordering::Equal => return Ok(m),
                Ordering::Less => {
                    i = m + 1;
                    m = (i + j) >> 1;
                }
                Ordering::Greater => {
                    j = m;
                    m = (i + j) >> 1;
                }
            }
        }

        Err(i)
    }
}

// Search backend precedence is deterministic when features are composed:
// custom > std > wt-slice > superslice. With no search feature selected, the
// custom implementation is the compatibility fallback. Each backend's cfg
// selects its implementation and test name together, preventing drift.
use search_backend::search;

/// Returns the first comparator-equal entry, or its insertion position.
///
/// This helper deliberately has one implementation across configured search
/// backends: callers that compare only a prefix (such as map key without
/// value) must not observe backend-dependent positions among duplicates.
#[inline]
pub(crate) fn search_by<T>(haystack: &[T], mut compare: impl FnMut(&T) -> core::cmp::Ordering) -> Result<usize, usize> {
    let index = haystack.partition_point(|candidate| compare(candidate).is_lt());
    match haystack.get(index) {
        Some(candidate) if compare(candidate).is_eq() => Ok(index),
        _ => Err(index),
    }
}

#[inline]
fn compute_positions_to_skip<Q, T: Ord>(haystack: &[T], bound: std::ops::Bound<&Q>, forward: bool) -> Option<usize>
where
    T: Borrow<Q> + Ord,
    Q: Ord + ?Sized,
{
    let skipped = match (bound, forward) {
        // A forward iterator skips values before the start bound.
        (std::ops::Bound::Included(value), true) => haystack.partition_point(|item| item.borrow().cmp(value).is_lt()),
        (std::ops::Bound::Excluded(value), true) => haystack.partition_point(|item| item.borrow().cmp(value).is_le()),

        // A backward iterator skips values after the end bound.
        (std::ops::Bound::Included(value), false) => {
            let first_greater = haystack.partition_point(|item| item.borrow().cmp(value).is_le());
            haystack.len() - first_greater
        }
        (std::ops::Bound::Excluded(value), false) => {
            let first_equal = haystack.partition_point(|item| item.borrow().cmp(value).is_lt());
            haystack.len() - first_equal
        }
        (std::ops::Bound::Unbounded, _) => return None,
    };

    // Callers use this as the index of the last value to skip. No skipped
    // values is represented by `None`.
    skipped.checked_sub(1)
}

impl<T: Ord> NodeLike<T> for Vec<T> {
    #[inline]
    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }
    #[inline]
    fn get_ith(&self, index: usize) -> Option<&T> {
        self.get(index)
    }
    #[inline]
    fn halve(&mut self) -> Self {
        self.split_off(self.len() / 2)
    }
    #[inline]
    fn need_to_split(&self, border: usize, _: &T) -> bool {
        self.len() >= border
    }
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
    #[inline]
    fn capacity(&self) -> usize {
        self.capacity()
    }
    #[inline]
    fn insert(&mut self, value: T) -> (bool, usize) {
        match search(self, &value) {
            Ok(idx) => (false, idx),
            Err(idx) => {
                self.insert(idx, value);
                (true, idx)
            }
        }
    }
    #[inline]
    fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        search(self, value).is_ok()
    }
    #[inline]
    fn try_select<Q>(&self, value: &Q) -> Option<usize>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        search(self, value).ok()
    }
    #[inline]
    fn rank<Q>(&self, bound: std::ops::Bound<&Q>, from_start: bool) -> Option<usize>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        compute_positions_to_skip(self, bound, from_start)
    }
    #[inline]
    fn delete<Q>(&mut self, value: &Q) -> Option<(T, usize)>
    where
        T: Borrow<Q> + Ord,
        Q: Ord + ?Sized,
    {
        match search(self, value) {
            Ok(idx) => Some((self.remove(idx), idx)),
            Err(_) => None,
        }
    }
    #[cfg(feature = "multimap")]
    #[inline]
    fn delete_at(&mut self, index: usize) -> Option<T> {
        if index < self.len() {
            Some(self.remove(index))
        } else {
            None
        }
    }
    #[inline]
    fn replace(&mut self, idx: usize, value: T) -> Option<T> {
        if let Some(old) = self.get_mut(idx) {
            let old = std::mem::replace(old, value);
            return Some(old);
        }

        None
    }
    #[inline]
    fn max(&self) -> Option<&T> {
        self.last()
    }
    #[inline]
    fn min(&self) -> Option<&T> {
        self.first()
    }
    #[inline]
    fn iter<'a>(&'a self) -> std::slice::Iter<'a, T>
    where
        T: 'a,
    {
        self.deref().iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Eq, Ord, PartialEq, PartialOrd)]
    struct KeyThenValue(usize, &'static str);

    impl Borrow<usize> for KeyThenValue {
        fn borrow(&self) -> &usize {
            &self.0
        }
    }

    #[test]
    fn configured_search_implementation_matches_precedence() {
        #[cfg(feature = "custom-binary-search")]
        assert_eq!(search_backend::NAME, "custom");

        #[cfg(all(feature = "std-binary-search", not(feature = "custom-binary-search")))]
        assert_eq!(search_backend::NAME, "std");

        #[cfg(all(
            feature = "wt-slice-binary-search",
            not(any(feature = "custom-binary-search", feature = "std-binary-search"))
        ))]
        assert_eq!(search_backend::NAME, "wt-slice");

        #[cfg(all(
            feature = "superslice-binary-search",
            not(any(
                feature = "custom-binary-search",
                feature = "std-binary-search",
                feature = "wt-slice-binary-search"
            ))
        ))]
        assert_eq!(search_backend::NAME, "superslice");

        #[cfg(not(any(
            feature = "custom-binary-search",
            feature = "std-binary-search",
            feature = "superslice-binary-search",
            feature = "wt-slice-binary-search"
        )))]
        assert_eq!(search_backend::NAME, "custom");
    }

    #[test]
    fn comparator_search_returns_first_duplicate() {
        let values = [(1, "a"), (1, "b"), (1, "c"), (2, "d")];
        assert_eq!(search_by(&values, |candidate| candidate.0.cmp(&1)), Ok(0));
        assert_eq!(search_by(&values, |candidate| candidate.0.cmp(&2)), Ok(3));
        assert_eq!(search_by(&values, |candidate| candidate.0.cmp(&0)), Err(0));
    }

    #[test]
    fn point_search_returns_match_or_insertion_position() {
        for values in [vec![], vec![2], vec![2, 4, 8, 16, 32]] {
            for needle in 0..=34 {
                let insertion = values.partition_point(|candidate| candidate < &needle);
                let expected = match values.get(insertion) {
                    Some(candidate) if candidate == &needle => Ok(insertion),
                    _ => Err(insertion),
                };

                assert_eq!(search(&values, &needle), expected, "values={values:?}, needle={needle}");
            }
        }
    }

    #[test]
    fn test_search_bound() {
        let vec = vec![1, 3, 5, 7, 9];

        assert_eq!(compute_positions_to_skip(&vec, std::ops::Bound::Unbounded, true), None);
        assert_eq!(compute_positions_to_skip(&vec, std::ops::Bound::Unbounded, false), None);

        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&1), true),
            None
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&5), true),
            Some(1)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&9), true),
            Some(3)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&0), true),
            None
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&10), true),
            Some(4)
        );

        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&1), true),
            Some(0)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&5), true),
            Some(2)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&9), true),
            Some(4)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&0), true),
            None
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&10), true),
            Some(4)
        );

        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&1), false),
            Some(3)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&5), false),
            Some(1)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&9), false),
            None
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&0), false),
            Some(4)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Included(&10), false),
            None
        );

        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&1), false),
            Some(4)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&5), false),
            Some(2)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&9), false),
            Some(0)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&0), false),
            Some(4)
        );
        assert_eq!(
            compute_positions_to_skip(&vec, std::ops::Bound::Excluded(&10), false),
            None
        );

        let empty: Vec<i32> = vec![];
        assert_eq!(
            compute_positions_to_skip(&empty, std::ops::Bound::Included(&1), true),
            None
        );
        assert_eq!(
            compute_positions_to_skip(&empty, std::ops::Bound::Excluded(&1), false),
            None
        );
    }
    #[test]
    fn excluded_borrowed_bound_skips_all_equal_keys() {
        let node = vec![KeyThenValue(1, "a"), KeyThenValue(1, "b"), KeyThenValue(2, "a")];

        assert_eq!(
            compute_positions_to_skip(&node, std::ops::Bound::Excluded(&1), true),
            Some(1),
        );
    }

    #[test]
    fn excluded_borrowed_end_bound_skips_all_equal_keys() {
        let node = vec![
            KeyThenValue(1, "a"),
            KeyThenValue(2, "a"),
            KeyThenValue(2, "b"),
            KeyThenValue(3, "a"),
        ];

        assert_eq!(
            compute_positions_to_skip(&node, std::ops::Bound::Excluded(&2), false),
            Some(2),
        );
    }

    #[test]
    fn binary_bound_search_matches_linear_bound_semantics() {
        fn expected(values: &[i32], bound: std::ops::Bound<&i32>, forward: bool) -> Option<usize> {
            let skipped = match (bound, forward) {
                (std::ops::Bound::Included(value), true) => values.iter().take_while(|item| *item < value).count(),
                (std::ops::Bound::Excluded(value), true) => values.iter().take_while(|item| *item <= value).count(),
                (std::ops::Bound::Included(value), false) => {
                    values.iter().rev().take_while(|item| *item > value).count()
                }
                (std::ops::Bound::Excluded(value), false) => {
                    values.iter().rev().take_while(|item| *item >= value).count()
                }
                (std::ops::Bound::Unbounded, _) => return None,
            };

            skipped.checked_sub(1)
        }

        let cases = [vec![], vec![1], vec![1, 3, 5, 7, 9], vec![1, 1, 1, 2, 2, 4, 7, 7, 9]];

        for values in cases {
            for probe in 0..=10 {
                for forward in [true, false] {
                    for bound in [std::ops::Bound::Included(&probe), std::ops::Bound::Excluded(&probe)] {
                        assert_eq!(
                            compute_positions_to_skip(&values, bound, forward),
                            expected(&values, bound, forward),
                            "values={values:?}, probe={probe}, forward={forward}, bound={bound:?}",
                        );
                    }
                }
            }
        }
    }
}

use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::Bound;

use core::cmp::Ordering;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::cdc::change::ChangeEvent;
use crate::concurrent::set::BTreeSet;
use crate::core::node::NodeLike;
use crate::core::pair::Pair;

use super::{MultiPairInsertHelper, MultiPairLike, MultiPairRemoveHelper};

/// A multimap pair whose stored identity and total order are the
/// `(key, discriminator)` tuple.
///
/// The discriminator is drawn at random on construction and refines the order
/// of equal-key entries, so every stored pair is strictly ordered and every
/// index entry key is unique. The value does not participate in `Ord`, `Eq`,
/// or `Hash` at all: an order that consulted value equality was not a lawful
/// total order (it violated transitivity, broke binary search, and let index
/// entry keys compare `Equal`, corrupting routing once equal-key nodes
/// split). Logical `(key, value)` operations are explicit scans over the
/// key's range with a separate value-equality bound; see
/// [`MultiPairInsertHelper`] and [`MultiPairRemoveHelper`].
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Default, Clone)]
pub struct RandomMultiPair<K, V> {
    pub key: K,
    pub value: V,
    pub discriminator: u64,
}

impl<K, V> RandomMultiPair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            discriminator: fastrand::u64(..),
        }
    }
}

impl<K: Ord, V> Eq for RandomMultiPair<K, V> {}

impl<K: Ord, V> PartialEq<Self> for RandomMultiPair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key) && self.discriminator.eq(&other.discriminator)
    }
}

impl<K: Ord, V> Ord for RandomMultiPair<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then(self.discriminator.cmp(&other.discriminator))
    }
}

impl<K: Ord, V> PartialOrd for RandomMultiPair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> std::hash::Hash for RandomMultiPair<K, V>
where
    K: std::hash::Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        self.discriminator.hash(state);
    }
}

impl<K, V> Borrow<K> for RandomMultiPair<K, V> {
    fn borrow(&self) -> &K {
        &self.key
    }
}

impl<K: Ord, V> MultiPairLike<K, V> for RandomMultiPair<K, V> {
    fn new(key: K, value: V) -> Self {
        Self::new(key, value)
    }

    fn key(&self) -> &K {
        &self.key
    }

    fn value(&self) -> &V {
        &self.value
    }

    // The stored pair's position among equal-key neighbors is determined by
    // its discriminator; keeping it on replace keeps the node sorted. With
    // identity being (key, discriminator), an Ord-equal replace already
    // carries the stored discriminator, so this is defensive.
    fn adopt_stored_identity(stored: &Self, incoming: &mut Self) {
        incoming.discriminator = stored.discriminator;
    }
}

impl<K, V> From<Pair<K, V>> for RandomMultiPair<K, V> {
    fn from(pair: Pair<K, V>) -> Self {
        RandomMultiPair {
            key: pair.key,
            value: pair.value,
            discriminator: fastrand::u64(..),
        }
    }
}

impl<K, V> From<RandomMultiPair<K, V>> for Pair<K, V> {
    fn from(pair: RandomMultiPair<K, V>) -> Self {
        Pair {
            key: pair.key,
            value: pair.value,
        }
    }
}

impl<K, V> From<RandomMultiPair<K, V>> for (K, V) {
    fn from(pair: RandomMultiPair<K, V>) -> Self {
        (pair.key, pair.value)
    }
}

impl<K, V> MultiPairInsertHelper<K, V> for RandomMultiPair<K, V>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + PartialEq + 'static,
{
    fn insert_into<Node>(set: &BTreeSet<Self, Node>, key: K, value: V) -> Option<(K, V)>
    where
        Self: Debug + Ord + Clone + Send + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        loop {
            // Logical identity is (key, value): locate a stored pair with an
            // equal value in the key's range and replace it in place. The
            // incoming pair carries the stored discriminator, so the put
            // finds the exact stored entry (Ord-equal) and the replacement
            // keeps its position among equal-key neighbors.
            let stored = set
                .range::<K, _>((Bound::Included(&key), Bound::Included(&key)))
                .find(|pair| pair.value == value);
            if let Some(stored) = stored {
                let incoming = Self {
                    key: key.clone(),
                    value: value.clone(),
                    discriminator: stored.discriminator,
                };
                if let Some(replaced) = set.put_with(incoming, Self::adopt_stored_identity) {
                    return Some(replaced.into());
                }
                // The located pair was removed concurrently before the
                // replace landed and the put inserted the pair fresh, which
                // completes the logical insert.
                return None;
            }

            // No logical match: insert a fresh entry under a random
            // discriminator. `put_checked` fails instead of replacing when
            // the (key, discriminator) identity is already taken by another
            // pair, so a random collision re-rolls by restarting (which also
            // re-runs the logical-match scan). A collision that lands inside
            // a concurrent split commit is replaced there instead of failing;
            // surface it as the replace it was.
            let candidate = Self::new(key.clone(), value.clone());
            match set.put_checked(candidate) {
                Ok((replaced, _)) => return replaced.map(Into::into),
                Err((node_guard, _, _)) => {
                    drop(node_guard);
                    continue;
                }
            }
        }
    }

    #[cfg(feature = "cdc")]
    fn insert_cdc_into<Node>(set: &BTreeSet<Self, Node>, key: K, value: V) -> (Option<(K, V)>, Vec<ChangeEvent<Self>>)
    where
        Self: Debug + Ord + Clone + Send + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        // See `insert_into`; this is the event-emitting twin.
        loop {
            let stored = set
                .range::<K, _>((Bound::Included(&key), Bound::Included(&key)))
                .find(|pair| pair.value == value);
            if let Some(stored) = stored {
                let incoming = Self {
                    key: key.clone(),
                    value: value.clone(),
                    discriminator: stored.discriminator,
                };
                let (replaced, events) = set.put_cdc_with(incoming, Self::adopt_stored_identity);
                return (replaced.map(Into::into), events);
            }

            let candidate = Self::new(key.clone(), value.clone());
            match set.put_cdc_checked(candidate) {
                Ok((replaced, events)) => return (replaced.map(Into::into), events),
                Err((node_guard, _, _)) => {
                    drop(node_guard);
                    continue;
                }
            }
        }
    }
}

impl<K, V> MultiPairRemoveHelper<K, V> for RandomMultiPair<K, V>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + PartialEq + 'static,
{
    fn remove_from<Node>(set: &BTreeSet<Self, Node>, key: &K, value: &V) -> Option<(K, V)>
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        let pair_to_remove = set
            .range::<K, _>((Bound::Included(key), Bound::Included(key)))
            .find(|pair| pair.key == *key && pair.value == *value);

        if let Some(pair_to_remove) = pair_to_remove {
            if let Some(removed) = set.remove(&pair_to_remove) {
                return Some(removed.into());
            }

            // A concurrent remove/reinsert can replace the located pair with a
            // logically equal pair that has a new discriminator. Revalidate
            // the logical pair under the structural write lock in that case.
            return set
                .remove_where(|pair| pair.key == *key && pair.value == *value)
                .map(Into::into);
        }

        None
    }

    fn remove_cdc_from<Node>(set: &BTreeSet<Self, Node>, key: &K, value: &V) -> (Option<(K, V)>, Vec<ChangeEvent<Self>>)
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        let pair_to_remove = set
            .range::<K, _>((Bound::Included(key), Bound::Included(key)))
            .find(|pair| pair.key == *key && pair.value == *value);

        if let Some(pair_to_remove) = pair_to_remove {
            let (res, evs) = set.remove_cdc(&pair_to_remove);
            if res.is_some() {
                return (res.map(Into::into), evs);
            }

            // See `remove_from`: the locator can become stale when an equal
            // logical pair is concurrently removed and reinserted.
            let (res, evs) = set.remove_where_cdc(|pair| pair.key == *key && pair.value == *value);
            return (res.map(Into::into), evs);
        }

        (None, vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::node::NodeLike;
    use std::ops::Bound::*;

    #[test]
    fn borrow_test() {
        let pair = RandomMultiPair::new(1usize, 2usize);
        assert_eq!(<RandomMultiPair<usize, usize> as Borrow<usize>>::borrow(&pair), &1usize);
    }

    #[test]
    fn eq_test() {
        // Identity is (key, discriminator); the value does not participate.
        let pair_one = RandomMultiPair {
            key: 1usize,
            value: 2usize,
            discriminator: 7,
        };
        let pair_two = RandomMultiPair {
            key: 1usize,
            value: 3usize,
            discriminator: 8,
        };
        assert_ne!(pair_one, pair_two);

        let same_slot = RandomMultiPair {
            key: 1usize,
            value: 3usize,
            discriminator: 7,
        };
        assert_eq!(pair_one, same_slot);
    }

    #[test]
    fn equal_pairs_have_equal_hashes() {
        use std::hash::{DefaultHasher, Hash, Hasher};

        // Hash follows the (key, discriminator) identity, so Eq-equal pairs
        // hash equal even when their values differ.
        let pair_one = RandomMultiPair {
            key: 1usize,
            value: 2usize,
            discriminator: 3,
        };
        let pair_two = RandomMultiPair {
            key: 1usize,
            value: 9usize,
            discriminator: 3,
        };

        assert_eq!(pair_one, pair_two);

        let mut hash_one = DefaultHasher::new();
        pair_one.hash(&mut hash_one);
        let mut hash_two = DefaultHasher::new();
        pair_two.hash(&mut hash_two);

        assert_eq!(hash_one.finish(), hash_two.finish());
    }

    // Exhaustive Ord-laws check over a small domain dense in collisions on
    // every axis. The previous Ord consulted value equality before the
    // discriminator, which made it non-transitive (a == b and b == c with
    // a < c was reachable), broke binary search, and let index entry keys
    // compare Equal.
    #[test]
    fn ord_is_a_lawful_total_order() {
        use core::cmp::Ordering;
        use std::hash::{DefaultHasher, Hash, Hasher};

        let mut pairs = Vec::new();
        for key in 0usize..4 {
            for value in 0usize..4 {
                for discriminator in 0u64..4 {
                    pairs.push(RandomMultiPair {
                        key,
                        value,
                        discriminator,
                    });
                }
            }
        }

        let hash_of = |pair: &RandomMultiPair<usize, usize>| {
            let mut hasher = DefaultHasher::new();
            pair.hash(&mut hasher);
            hasher.finish()
        };

        for a in &pairs {
            assert_eq!(a.cmp(a), Ordering::Equal, "reflexivity: {a:?}");
            for b in &pairs {
                let ab = a.cmp(b);
                let ba = b.cmp(a);
                // Antisymmetry / duality.
                assert_eq!(ab, ba.reverse(), "antisymmetry: {a:?} vs {b:?}");
                // Eq consistency with Ord, and Hash consistency with Eq.
                assert_eq!(ab == Ordering::Equal, a == b, "Eq/Ord consistency: {a:?} vs {b:?}");
                assert_eq!(a.partial_cmp(b), Some(ab), "PartialOrd/Ord consistency");
                if a == b {
                    assert_eq!(hash_of(a), hash_of(b), "Hash/Eq consistency: {a:?} vs {b:?}");
                }
                for c in &pairs {
                    let bc = b.cmp(c);
                    if ab == bc {
                        assert_eq!(a.cmp(c), ab, "transitivity: {a:?}, {b:?}, {c:?}");
                    }
                    if ab != Ordering::Greater && bc != Ordering::Greater {
                        assert_ne!(a.cmp(c), Ordering::Greater, "transitivity of <=: {a:?}, {b:?}, {c:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn replace_of_logically_equal_pair_preserves_discriminator_and_position() {
        let set = BTreeSet::<RandomMultiPair<usize, &'static str>>::new();
        set.attach_node(vec![
            RandomMultiPair {
                key: 1,
                value: "a",
                discriminator: 10,
            },
            RandomMultiPair {
                key: 1,
                value: "b",
                discriminator: 20,
            },
            RandomMultiPair {
                key: 1,
                value: "c",
                discriminator: 30,
            },
        ]);

        let replaced = RandomMultiPair::insert_into(&set, 1, "b");
        assert_eq!(replaced, Some((1, "b")), "logical duplicate must replace in place");

        let stored = set
            .range::<usize, _>((Bound::Included(&1), Bound::Included(&1)))
            .collect::<Vec<_>>();
        assert_eq!(stored.len(), 3);
        assert_eq!(
            stored.iter().map(|pair| pair.discriminator).collect::<Vec<_>>(),
            vec![10, 20, 30],
            "replace must preserve the stored discriminator and position"
        );
        assert_eq!(stored[1].value, "b");
    }

    #[test]
    fn node_like() {
        let mut vec = Vec::new();
        let p1 = RandomMultiPair::new(1, "a");
        let p2 = RandomMultiPair::new(1, "b");
        let p3 = RandomMultiPair::new(2, "c");

        NodeLike::insert(&mut vec, p1.clone());
        NodeLike::insert(&mut vec, p2.clone());
        assert_eq!(vec.len(), 2);

        NodeLike::insert(&mut vec, p1.clone());
        assert_eq!(vec.len(), 2);

        NodeLike::insert(&mut vec, p3.clone());
        assert_eq!(vec.len(), 3);
    }

    #[test]
    fn range_bounds() {
        let mut vec = Vec::new();

        let p1a = RandomMultiPair::new(1, "a");
        let p1b = RandomMultiPair::new(1, "b");
        let p1c = RandomMultiPair::new(1, "c");
        let p2a = RandomMultiPair::new(2, "a");
        let p2b = RandomMultiPair::new(2, "b");
        let p3a = RandomMultiPair::new(3, "a");
        let p3b = RandomMultiPair::new(3, "b");
        let p3c = RandomMultiPair::new(3, "c");
        let p3d = RandomMultiPair::new(3, "d");
        let p4a = RandomMultiPair::new(4, "a");

        NodeLike::insert(&mut vec, p4a.clone());
        NodeLike::insert(&mut vec, p1a.clone());
        NodeLike::insert(&mut vec, p1c.clone());
        NodeLike::insert(&mut vec, p1b.clone());
        NodeLike::insert(&mut vec, p2b.clone());
        NodeLike::insert(&mut vec, p2a.clone());
        NodeLike::insert(&mut vec, p3a.clone());
        NodeLike::insert(&mut vec, p3b.clone());
        NodeLike::insert(&mut vec, p3d.clone());
        NodeLike::insert(&mut vec, p3c.clone());
        assert_eq!(vec.len(), 10);

        let start_1 = vec.rank(Included(&1), true).map_or(0, |rank| rank + 1);
        let end_1 = vec.rank(Excluded(&1), true).unwrap();
        let range_1 = &vec[start_1..=end_1];
        assert_eq!(range_1.len(), 3);
        assert!(range_1.contains(&p1a));
        assert!(range_1.contains(&p1b));
        assert!(range_1.contains(&p1c));

        let end_2 = vec.rank(Excluded(&2), true).unwrap();
        let range_2 = &vec[start_1..=end_2];
        assert_eq!(range_2.len(), 5);
        assert!(range_2.contains(&p1a));
        assert!(range_2.contains(&p1b));
        assert!(range_2.contains(&p1c));
        assert!(range_2.contains(&p2a));
        assert!(range_2.contains(&p2b));
        assert_ne!(range_2.contains(&p3a), true);

        let start_3 = vec.rank(Included(&3), true).unwrap() + 1;
        let end_3 = vec.rank(Excluded(&3), true).unwrap();
        let range_3 = &vec[start_3..=end_3];
        assert_eq!(range_3.len(), 4);
        assert!(range_3.contains(&p3a));
        assert!(range_3.contains(&p3b));
        assert!(range_3.contains(&p3c));
        assert!(range_3.contains(&p3d));

        let start_4 = vec.rank(Included(&4), true).unwrap() + 1;
        let end_4 = vec.rank(Excluded(&4), true).unwrap();
        let range_4 = &vec[start_4..=end_4];
        assert_eq!(range_4.len(), 1);
        assert!(range_4.contains(&p4a));
    }
}

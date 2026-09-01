use crate::cdc::change::ChangeEvent;
use crate::concurrent::set::BTreeSet;
use crate::core::node::NodeLike;
use std::fmt::Debug;

pub mod ord;
pub use ord::OrdMultiPair;

/// The multimap entry representation.
///
/// Identity is the `(key, value)` pair itself, lexicographically ordered, so a stored
/// entry is located by binary search.
///
/// A `RandomMultiPair` variant used to exist alongside this one, ordering by
/// `(key, random discriminator)` so that a `V` that was only `PartialEq` could still be
/// stored. It was removed in 0.0.9 and should not be reintroduced. Two things were wrong
/// with it and both were inherent, not bugs to be fixed:
///
/// - Its `Ord` consulted value equality before the discriminator, which is not
///   transitive, so binary search over nodes was unreliable and same-key node splits
///   could corrupt routing and livelock the split retry loop.
/// - Making the order lawful meant the value no longer participated in it, so a stored
///   entry could not be found by its value. `insert` had to scan every entry sharing the
///   key, which is O(n) per insert and quadratic to fill one key: 356 us per insert at
///   16,000 values against 358 ns here, and it never won at any size, not even two
///   values per key.
///
/// The niche it served, a `V` that is `PartialEq` but not `Ord`, is not worth a second
/// representation. Wrap such a value in a newtype with a total order.
pub type MultiPair<K, V> = OrdMultiPair<K, V>;

/// Common contract for key-value pairs stored by `BTreeMultiMap`.
///
/// Implementations must order entries by key first: entries with the same key
/// must form one contiguous range, while their representation may refine the
/// order within that range.
pub trait MultiPairLike<K, V>: Into<(K, V)> + Ord {
    fn new(key: K, value: V) -> Self;
    fn key(&self) -> &K;
    fn value(&self) -> &V;
    /// Called before an incoming pair replaces a stored, logically equal pair
    /// in place. Implementations whose `Ord` refines equal keys with hidden
    /// state (e.g. a random discriminator) must copy that state from the
    /// stored pair onto the incoming one: the replacement position was
    /// determined by the stored pair's state, so replacing it with fresh
    /// state would break the node's sort invariant.
    fn adopt_stored_identity(_stored: &Self, _incoming: &mut Self) {}
}

/// Pair-specific insert-or-replace strategy used by `BTreeMultiMap`.
///
/// The logical identity of a multimap entry is the `(key, value)` pair, while
/// the stored representation's `Ord` identity may be refined by hidden state
/// (`RandomMultiPair`'s discriminator). Insertion therefore locates a
/// logically equal stored pair by scanning the key's range with value
/// equality, replaces it in place preserving its stored ordering identity,
/// and inserts a fresh entry only when no logical match exists.
pub trait MultiPairInsertHelper<K, V>: MultiPairLike<K, V> {
    fn insert_into<Node>(set: &BTreeSet<Self, Node>, key: K, value: V) -> Option<(K, V)>
    where
        Self: Debug + Ord + Clone + Send + 'static,
        Node: NodeLike<Self> + Send + 'static;

    #[cfg(feature = "cdc")]
    fn insert_cdc_into<Node>(set: &BTreeSet<Self, Node>, key: K, value: V) -> (Option<(K, V)>, Vec<ChangeEvent<Self>>)
    where
        Self: Debug + Ord + Clone + Send + 'static,
        Node: NodeLike<Self> + Send + 'static;
}

/// Pair-specific exact-removal strategy used by `BTreeMultiMap`.
pub trait MultiPairRemoveHelper<K, V> {
    fn remove_from<Node>(set: &BTreeSet<Self, Node>, key: &K, value: &V) -> Option<(K, V)>
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        Self::remove_cdc_from(set, key, value).0
    }

    fn remove_cdc_from<Node>(
        set: &BTreeSet<Self, Node>,
        key: &K,
        value: &V,
    ) -> (Option<(K, V)>, Vec<ChangeEvent<Self>>)
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static;
}

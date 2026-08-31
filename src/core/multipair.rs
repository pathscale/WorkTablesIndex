use crate::cdc::change::ChangeEvent;
use crate::concurrent::set::BTreeSet;
use crate::core::node::NodeLike;

pub mod ord;
pub mod random;

pub use ord::OrdMultiPair;
pub use random::RandomMultiPair;

pub type MultiPair<K, V> = RandomMultiPair<K, V>;

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

use core::cmp::Ordering;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Default, Clone)]
pub struct Pair<K, V>
{
    pub key: K,
    pub value: V,
}

impl<K, V> Eq for Pair<K, V> where K: Ord {}

impl<K, V> PartialEq<Self> for Pair<K, V>
where
    K: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl<K, V> PartialOrd<Self> for Pair<K, V>
where
    K: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.key.partial_cmp(&other.key)
    }
}

impl<K, V> Ord for Pair<K, V>
where
    K: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl<K, V> std::hash::Hash for Pair<K, V>
where
    K: std::hash::Hash,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

impl<K: Ord, V> Borrow<K> for Pair<K, V> {
    fn borrow(&self) -> &K {
        &self.key
    }
}

impl<V> Borrow<str> for Pair<String, V> {
    fn borrow(&self) -> &str {
        self.key.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::Pair;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn equal_pairs_hash_identically() {
        let first = Pair { key: 7, value: "first" };
        let second = Pair { key: 7, value: "second" };
        assert_eq!(first, second);

        let mut first_hasher = DefaultHasher::new();
        first.hash(&mut first_hasher);
        let mut second_hasher = DefaultHasher::new();
        second.hash(&mut second_hasher);

        assert_eq!(first_hasher.finish(), second_hasher.finish());
    }
}

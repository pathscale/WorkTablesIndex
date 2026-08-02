pub mod random;

pub use random::RandomMultiPair;

pub type MultiPair<K, V> = RandomMultiPair<K, V>;

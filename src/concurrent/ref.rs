use crate::core::node::NodeLike;
use parking_lot::{ArcRwLockReadGuard, RawRwLock};
use std::marker::PhantomData;

/// A point reference that keeps its node read-locked.
///
/// Drop this value before calling another operation on the same map from the
/// same thread. Like every non-recursive `RwLock`, reacquiring a read lock while
/// a writer is queued can deadlock even when the thread already holds a read
/// guard.
pub struct Ref<T: Ord + Clone + Send, Node: NodeLike<T> + Send> {
    pub(super) node_guard: ArcRwLockReadGuard<RawRwLock, Node>,
    pub(super) position: usize,
    pub(super) phantom_data: PhantomData<T>,
}

impl<T: Ord + Clone + Send, Node: NodeLike<T> + Send> Ref<T, Node> {
    pub fn get(&self) -> &T {
        self.node_guard.get_ith(self.position).unwrap()
    }
}

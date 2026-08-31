use crossbeam_skiplist::SkipMap;
use crossbeam_utils::sync::ShardedLock;
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use std::fmt::Debug;
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
#[cfg(feature = "cdc")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{borrow::Borrow, sync::Arc};

use crate::cdc::change::ChangeEvent;
use crate::concurrent::operation::*;
use crate::core::constants::DEFAULT_INNER_SIZE;
use crate::core::node::*;

use super::r#ref::Ref;

// Most point reads acquire the node mutex immediately while the structural
// mapping is pinned. If a node is genuinely contended, wait without holding a
// structural shard and retry. A bounded fallback preserves progress if the
// node keeps being reacquired between the wait and the next stable attempt.
const STABLE_READ_BLOCKING_FALLBACK_AFTER: usize = 2;
const ROOT_PUBLICATION_SPIN_LIMIT: usize = 16;

/// A **persistent** concurrent ordered set based on a B-Tree.
///
/// See [`BTreeMap`]'s documentation for a detailed discussion of this collection's performance
/// benefits and drawbacks.
///
/// It is a logic error for an item to be modified in such a way that the item's ordering relative
/// to any other item, as determined by the [`Ord`] trait, changes while it is in the set. This is
/// normally only possible through [`Cell`], [`RefCell`], global state, I/O, or unsafe code.
/// The behavior resulting from such a logic error is not specified, but will be encapsulated to the
/// `BTreeSet` that observed the logic error and not result in undefined behavior. This could
/// include panics, incorrect results, aborts, memory leaks, and non-termination.
///
/// Iterators returned by [`crate::BTreeSet::iter`] produce their items in order, and take worst-case
/// logarithmic and amortized constant time per item returned.
///
/// [`Cell`]: crate::core::cell::Cell
/// [`RefCell`]: crate::core::cell::RefCell
///
/// # Examples
///
/// ```
/// use indexset::concurrent::set::BTreeSet;
///
/// // Type inference lets us omit an explicit type signature (which
/// // would be `BTreeSet<&str>` in this example).
/// let mut books = BTreeSet::<&str>::new();
///
/// // Add some books.
/// books.insert("A Dance With Dragons");
/// books.insert("To Kill a Mockingbird");
/// books.insert("The Odyssey");
/// books.insert("The Great Gatsby");
///
/// // Check for a specific one.
/// if !books.contains("The Winds of Winter") {
///     println!("We have {} books, but The Winds of Winter ain't one.",
///              books.len());
/// }
///
/// // Remove a book.
/// books.remove("The Odyssey");
///
/// // Iterate over everything.
/// for book in &books {
///     println!("{book}");
/// }
/// ```
///
/// A `BTreeSet` with a known list of items can be initialized from an array:
///
/// ```
/// use indexset::concurrent::set::BTreeSet;
///
/// let set = BTreeSet::from_iter([1, 2, 3]);
/// ```
#[derive(Debug)]
pub struct BTreeSet<T, Node = Vec<T>>
where
    T: Ord + Clone + 'static,
    Node: NodeLike<T>,
{
    pub(crate) index: SkipMap<T, Arc<Mutex<Node>>>,
    // Lock order: whenever both locks overlap, acquire `index_lock` before a
    // node mutex. A path that starts with a node mutex must release it before
    // requesting `index_lock`. Definitive point reads try the node lock while
    // holding a structural read guard; on contention they release that guard
    // before waiting and retry the mapping.
    index_lock: ShardedLock<()>,
    node_capacity: usize,
    #[cfg(feature = "cdc")]
    // The counter provides unique sequence numbers only. Node/global locks
    // order conflicting mutations, and the persistence queue publishes event
    // payloads, so the counter itself does not carry memory visibility.
    event_id: AtomicU64,
}
impl<T: Ord + Clone + 'static, Node: NodeLike<T>> Default for BTreeSet<T, Node> {
    fn default() -> Self {
        let index = SkipMap::new();

        Self {
            index,
            index_lock: ShardedLock::new(()),
            node_capacity: DEFAULT_INNER_SIZE,
            #[cfg(feature = "cdc")]
            event_id: AtomicU64::new(0),
        }
    }
}

impl<T, Node> BTreeSet<T, Node>
where
    T: Debug + Ord + Clone + Send,
    Node: NodeLike<T> + Send + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }
    /// Makes a new, empty `BTreeSet` with the given maximum node size. Allocates one vec with
    /// the capacity set to be the specified node size.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let set: BTreeSet<i32> = BTreeSet::with_maximum_node_size(128);
    pub fn with_maximum_node_size(node_capacity: usize) -> Self {
        Self {
            index: SkipMap::new(),
            index_lock: ShardedLock::new(()),
            node_capacity,
            #[cfg(feature = "cdc")]
            event_id: AtomicU64::new(0),
        }
    }
    pub fn attach_node(&self, node: Node) {
        let node_id = node
            .max()
            .cloned()
            .expect("node should contain at least one value to be correct node");
        let _global_guard = self.index_lock.write();
        self.index.insert(node_id, Arc::new(Mutex::new(node)));
    }

    #[cfg(feature = "cdc")]
    pub(crate) fn export_topology(&self) -> (usize, Vec<Vec<T>>) {
        let _structural_guard = self.index_lock.read();
        let nodes = self
            .index
            .iter()
            .map(|entry| entry.value().lock().iter().cloned().collect())
            .collect();
        (self.node_capacity, nodes)
    }

    #[allow(clippy::type_complexity)]
    // Const specialization keeps ordinary writes free of CDC event construction
    // even when the crate is compiled with the `cdc` feature.
    fn put_checked_inner<const EMIT_CDC: bool>(
        &self,
        value: T,
    ) -> Result<(Option<T>, Vec<ChangeEvent<T>>), (ArcMutexGuard<RawMutex, Node>, usize, T)> {
        loop {
            let mut cdc = vec![];
            let _global_guard = self.index_lock.read();
            let target_node_entry = match self.index.lower_bound(std::ops::Bound::Included(&value)) {
                Some(entry) => entry,
                None => {
                    if let Some(last) = self.index.back() {
                        last
                    } else {
                        drop(_global_guard);
                        let mut spins = 0;
                        let _global_guard = loop {
                            if let Ok(guard) = self.index_lock.try_write() {
                                break guard;
                            }
                            if spins >= ROOT_PUBLICATION_SPIN_LIMIT {
                                // A bounded block gives root publication a
                                // deterministic progress path under reader
                                // contention instead of livelocking.
                                break self.index_lock.write().unwrap_or_else(|poisoned| poisoned.into_inner());
                            }
                            spins += 1;
                            std::hint::spin_loop();
                        };
                        // Another first writer may have published while this
                        // caller was acquiring the exclusive structural guard.
                        if !self.index.is_empty() {
                            continue;
                        }

                        let mut first_node = Node::with_capacity(self.node_capacity);
                        first_node.insert(value.clone());

                        #[cfg(feature = "cdc")]
                        if EMIT_CDC {
                            let node_insertion = ChangeEvent::CreateNode {
                                // is correct as index is locked and current thread is the only that can
                                // fetch event_id.
                                event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                                max_value: value.clone(),
                            };
                            cdc.push(node_insertion);
                        }

                        self.index.insert(value, Arc::new(Mutex::new(first_node)));

                        return Ok((None, cdc));
                    }
                }
            };

            let mut node_guard = target_node_entry.value().lock_arc();

            #[allow(unused_assignments)]
            let mut operation = None;
            if !node_guard.need_to_split(self.node_capacity, &value) {
                let old_max = node_guard.max().cloned();
                let (inserted, idx) = NodeLike::insert(&mut *node_guard, value.clone());
                if inserted {
                    #[cfg(feature = "cdc")]
                    if EMIT_CDC {
                        let node_element_insertion = ChangeEvent::InsertAt {
                            // is correct as node is locked and current thread is the only that can
                            // fetch event_id, so events for this node will have monotonic id's.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: old_max.clone().unwrap_or(value.clone()),
                            value: value.clone(),
                            index: idx,
                        };
                        cdc.push(node_element_insertion);
                    }

                    if node_guard.max().cloned() == old_max {
                        return Ok((None, cdc));
                    }

                    // The node's maximum changed, so its index entry must be
                    // re-keyed. Address the repair by the entry's CURRENT key,
                    // not the observed maximum: during a stale-key window (a
                    // concurrent writer changed the maximum but its own repair
                    // has not committed, or a remove emptied the node before
                    // this insert refilled it, leaving `old_max` as `None`)
                    // the two differ, and a repair addressed by the maximum
                    // misses the entry at commit time and is silently dropped,
                    // leaving the entry permanently stale (unreachable values,
                    // and a pending `MakeUnreachable` could unlink the node
                    // containing this acknowledged insert).
                    operation = Some(Operation::UpdateMax(
                        target_node_entry.value().clone(),
                        target_node_entry.key().clone(),
                    ));
                } else {
                    return Err((node_guard, idx, old_max.unwrap()));
                }
            } else {
                operation = Some(Operation::Split(
                    target_node_entry.value().clone(),
                    target_node_entry.key().clone(),
                    value.clone(),
                ));
            }

            drop(node_guard);
            drop(_global_guard);

            let _global_guard = self.index_lock.write();

            let op = operation.unwrap();
            match &op {
                Operation::Split(_, _, _) => {
                    if let Ok((value, value_cdc)) = op.commit::<EMIT_CDC>(&self.index) {
                        #[cfg(feature = "cdc")]
                        if EMIT_CDC {
                            for unassigned_event in value_cdc {
                                let event_id = self.event_id.fetch_add(1, Ordering::Relaxed).into();
                                cdc.push(unassigned_event.assign_id(event_id));
                            }
                        }
                        return Ok((value, cdc));
                    } else {
                        continue;
                    }
                }
                Operation::UpdateMax(_, _) => {
                    return if let Ok((value, value_cdc)) = op.commit::<EMIT_CDC>(&self.index) {
                        #[cfg(feature = "cdc")]
                        if EMIT_CDC {
                            for unassigned_event in value_cdc {
                                let event_id = self.event_id.fetch_add(1, Ordering::Relaxed).into();
                                cdc.push(unassigned_event.assign_id(event_id));
                            }
                        }
                        Ok((value, cdc))
                    } else {
                        Ok((None, cdc))
                    }
                }
                Operation::MakeUnreachable(_, _) => unreachable!(),
            }
        }
    }
    fn put_inner<const EMIT_CDC: bool>(&self, value: T) -> (Option<T>, Vec<ChangeEvent<T>>) {
        match self.put_checked_inner::<EMIT_CDC>(value.clone()) {
            Ok(res) => res,
            Err((mut node_guard, idx, max)) => {
                let mut cdc = vec![];
                #[cfg(feature = "cdc")]
                if EMIT_CDC {
                    if node_guard.len() == 1 {
                        let node_removal = ChangeEvent::RemoveNode {
                            // is correct as node is locked and current thread is the only that can
                            // fetch event_id, so events for this node will have monotonic id's.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: max.clone(),
                        };
                        let node_insertion = ChangeEvent::CreateNode {
                            // same as for previous.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: value.clone(),
                        };
                        cdc.push(node_removal);
                        cdc.push(node_insertion);
                    } else if idx == node_guard.len() - 1 {
                        let new_max = if node_guard.len() <= 1 {
                            None
                        } else {
                            node_guard.get_ith(node_guard.len() - 2)
                        };
                        let node_element_removal = ChangeEvent::RemoveAt {
                            // is correct as node is locked and current thread is the only that can
                            // fetch event_id, so events for this node will have monotonic id's.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: max.clone(),
                            value: value.clone(),
                            index: idx,
                        };
                        let node_element_insertion = ChangeEvent::InsertAt {
                            // same as for previous.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: new_max.expect("length was checked so should be ok").clone(),
                            value: value.clone(),
                            index: idx,
                        };
                        cdc.push(node_element_removal);
                        cdc.push(node_element_insertion);
                    } else {
                        let node_element_removal = ChangeEvent::RemoveAt {
                            // is correct as node is locked and current thread is the only that can
                            // fetch event_id, so events for this node will have monotonic id's.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: max.clone(),
                            value: value.clone(),
                            index: idx,
                        };
                        let node_element_insertion = ChangeEvent::InsertAt {
                            // same as for previous.
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: max.clone(),
                            value: value.clone(),
                            index: idx,
                        };
                        cdc.push(node_element_removal);
                        cdc.push(node_element_insertion);
                    }
                }

                (NodeLike::replace(&mut *node_guard, idx, value.clone()), cdc)
            }
        }
    }

    pub(crate) fn put(&self, value: T) -> Option<T> {
        self.put_inner::<false>(value).0
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn put_checked(
        &self,
        value: T,
    ) -> Result<(Option<T>, Vec<ChangeEvent<T>>), (ArcMutexGuard<RawMutex, Node>, usize, T)> {
        self.put_checked_inner::<false>(value)
    }

    pub(crate) fn put_cdc(&self, value: T) -> (Option<T>, Vec<ChangeEvent<T>>) {
        self.put_inner::<true>(value)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn put_cdc_checked(
        &self,
        value: T,
    ) -> Result<(Option<T>, Vec<ChangeEvent<T>>), (ArcMutexGuard<RawMutex, Node>, usize, T)> {
        self.put_checked_inner::<true>(value)
    }

    /// Adds a value to the set.
    ///
    /// Returns whether the value was newly inserted. That is:
    ///
    /// - If the set did not previously contain an equal value, `true` is
    ///   returned.
    /// - If the set already contained an equal value, `false` is returned, and
    ///   the entry is not updated.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let mut set = BTreeSet::<usize>::new();
    ///
    /// assert_eq!(set.insert(2), true);
    /// assert_eq!(set.insert(2), false);
    /// assert_eq!(set.len(), 1);
    /// ```
    pub fn insert(&self, value: T) -> bool {
        self.put(value).is_none()
    }
    // See `put_checked_inner`: this is const-specialized to avoid paying for
    // discarded events in the ordinary `remove` path.
    fn remove_inner<const EMIT_CDC: bool, Q>(&self, value: &Q) -> (Option<T>, Vec<ChangeEvent<T>>)
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut cdc = vec![];
        let _global_guard = self.index_lock.read();
        // Fall back to the last node when the value sorts above every index
        // key, exactly like `put` and `lock_node_for_value`: during a stale-key
        // window (a node whose maximum grew before its UpdateMax repair
        // committed) the value lives in the last node even though no index key
        // covers it. Without the fallback such a value is un-removable while
        // `contains` still finds it.
        if let Some(target_node_entry) = self
            .index
            .lower_bound(std::ops::Bound::Included(value))
            .or_else(|| self.index.back())
        {
            let mut node_guard = target_node_entry.value().lock_arc();
            let old_max = node_guard.max().cloned();
            let deleted = NodeLike::delete(&mut *node_guard, value);
            if deleted.is_none() {
                return (None, cdc);
            }
            let (deleted, idx) = deleted.expect("should be ok as checked before");

            let operation = if node_guard.len() > 0 {
                #[cfg(feature = "cdc")]
                if EMIT_CDC {
                    let node_element_removal = ChangeEvent::RemoveAt {
                        // is correct as node is locked and current thread is the only that can
                        // fetch event_id, so events for this node will have monotonic id's.
                        event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                        max_value: old_max.clone().expect("Max value should exist as Node is not empty"),
                        value: deleted.clone(),
                        index: idx,
                    };
                    cdc.push(node_element_removal);
                }

                if old_max.as_ref() == node_guard.max() {
                    return (Some(deleted), cdc);
                }

                // Address the repair by the entry's current key, not by the
                // observed old maximum: see `put_checked_inner`. In a
                // stale-key window they differ, and a repair addressed by the
                // maximum is dropped at commit time, leaving the entry stale.
                Some(Operation::UpdateMax(
                    target_node_entry.value().clone(),
                    target_node_entry.key().clone(),
                ))
            } else {
                Some(Operation::MakeUnreachable(
                    target_node_entry.value().clone(),
                    target_node_entry.key().clone(),
                ))
            };

            drop(node_guard);
            drop(_global_guard);

            let _global_guard = self.index_lock.write();

            return if let Ok((_, value_cdc)) = operation.unwrap().commit::<EMIT_CDC>(&self.index) {
                #[cfg(feature = "cdc")]
                if EMIT_CDC {
                    for unassigned_event in value_cdc {
                        let event_id = self.event_id.fetch_add(1, Ordering::Relaxed).into();
                        cdc.push(unassigned_event.assign_id(event_id));
                    }
                }
                (Some(deleted), cdc)
            } else {
                (Some(deleted), cdc)
            };
        }

        (None, vec![])
    }

    pub fn remove_cdc<Q>(&self, value: &Q) -> (Option<T>, Vec<ChangeEvent<T>>)
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_inner::<true, Q>(value)
    }
    /// If the set contains an element equal to the value, removes it from the
    /// set and drops it. Returns whether such an element was present.
    ///
    /// The value may be any borrowed form of the set's element type,
    /// but the ordering on the borrowed form *must* match the
    /// ordering on the element type.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let mut set = BTreeSet::<usize>::new();
    ///
    /// set.insert(2);
    /// assert_eq!(set.remove(&2).is_some(), true);
    /// assert_eq!(set.remove(&2).is_some(), false);
    /// ```
    pub fn remove<Q>(&self, value: &Q) -> Option<T>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.remove_inner::<false, Q>(value).0
    }

    // Slow-path recovery for multimap exact removal. Holding the structural
    // write guard makes predicate lookup, deletion, and node reindexing one
    // critical section after the ordinary point-removal path has missed. Only
    // the multimap paths use this, and it relies on NodeLike::delete_at (also
    // multimap-gated), so gate the whole family to avoid an unconditional break.
    #[cfg(feature = "multimap")]
    fn remove_where_inner<const EMIT_CDC: bool, F>(&self, predicate: F) -> (Option<T>, Vec<ChangeEvent<T>>)
    where
        F: Fn(&T) -> bool,
    {
        let mut cdc = vec![];
        let _global_guard = self.index_lock.write();

        for target_node_entry in self.index.iter() {
            let mut node_guard = target_node_entry.value().lock_arc();
            let Some(target_index) = node_guard.iter().position(&predicate) else {
                continue;
            };
            let old_max = node_guard.max().cloned().expect("target node must have a maximum");
            let deleted = NodeLike::delete_at(&mut *node_guard, target_index)
                .expect("target position was found while the node was locked");

            #[cfg(feature = "cdc")]
            if EMIT_CDC {
                let node_element_removal = ChangeEvent::RemoveAt {
                    event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                    max_value: old_max.clone(),
                    value: deleted.clone(),
                    index: target_index,
                };
                cdc.push(node_element_removal);
            }

            if node_guard.max() != Some(&old_max) {
                let node = target_node_entry.value().clone();
                target_node_entry.remove();

                if let Some(new_max) = node_guard.max().cloned() {
                    self.index.insert(new_max, node);
                } else {
                    #[cfg(feature = "cdc")]
                    if EMIT_CDC {
                        let node_removal = ChangeEvent::RemoveNode {
                            event_id: self.event_id.fetch_add(1, Ordering::Relaxed).into(),
                            max_value: old_max,
                        };
                        cdc.push(node_removal);
                    }
                }
            }

            return (Some(deleted), cdc);
        }

        (None, cdc)
    }

    #[cfg(feature = "multimap")]
    pub(crate) fn remove_where<F>(&self, predicate: F) -> Option<T>
    where
        F: Fn(&T) -> bool,
    {
        self.remove_where_inner::<false, F>(predicate).0
    }

    #[cfg(feature = "multimap")]
    pub(crate) fn remove_where_cdc<F>(&self, predicate: F) -> (Option<T>, Vec<ChangeEvent<T>>)
    where
        F: Fn(&T) -> bool,
    {
        self.remove_where_inner::<true, F>(predicate)
    }

    #[inline(always)]
    fn lock_node_for_value_optimistic<Q>(&self, value: &Q) -> Option<ArcMutexGuard<RawMutex, Node>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let node = {
            let _global_guard = self.index_lock.read();
            match self.index.lower_bound(std::ops::Bound::Included(value)) {
                Some(entry) => Some(entry.value().clone()),
                None => self
                    .index
                    .back()
                    .map(|last| last.value().clone())
                    .or_else(|| self.index.front().map(|first| first.value().clone())),
            }
        }?;
        Some(node.lock_arc())
    }

    /// Locates and locks the node whose structural range owns `value`.
    ///
    /// The common uncontended path acquires the node with `try_lock_arc` while
    /// holding `index_lock.read()`, making both hits and misses definitive with
    /// one structural lookup. On contention, the structural guard is released
    /// before waiting so a long-lived node reference cannot convoy unrelated
    /// structural writers. After repeated contention, the documented
    /// `index_lock -> node` order is used as a bounded progress fallback.
    #[inline(always)]
    fn lock_node_for_value<Q>(&self, value: &Q) -> Option<ArcMutexGuard<RawMutex, Node>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut contentions = 0;

        loop {
            let global_guard = self.index_lock.read();
            let node = match self.index.lower_bound(std::ops::Bound::Included(value)) {
                Some(entry) => entry.value().clone(),
                None => self
                    .index
                    .back()
                    .map(|last| last.value().clone())
                    .or_else(|| self.index.front().map(|first| first.value().clone()))?,
            };

            if let Some(node_guard) = node.try_lock_arc() {
                drop(global_guard);
                return Some(node_guard);
            }

            contentions += 1;
            if contentions >= STABLE_READ_BLOCKING_FALLBACK_AFTER {
                let node_guard = node.lock_arc();
                drop(global_guard);
                return Some(node_guard);
            }

            drop(global_guard);
            // Wait for the observed holder without pinning the structural
            // mapping, then retry so the returned guard always corresponds to
            // a mapping observed under `index_lock`.
            drop(node.lock_arc());
        }
    }

    #[inline(always)]
    fn get_with_guard<Q, R>(
        node_guard: ArcMutexGuard<RawMutex, Node>,
        value: &Q,
        read: impl FnOnce(&T) -> R,
    ) -> Option<R>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let position = node_guard.try_select(value)?;
        node_guard.get_ith(position).map(read)
    }

    #[inline(always)]
    pub(crate) fn get_with<Q, R>(&self, value: &Q, read: impl FnOnce(&T) -> R) -> Option<R>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Self::get_with_guard(self.lock_node_for_value(value)?, value, read)
    }

    #[inline(always)]
    pub(crate) fn get_with_optimistic<Q, R>(&self, value: &Q, read: impl FnOnce(&T) -> R) -> Option<R>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        Self::get_with_guard(self.lock_node_for_value_optimistic(value)?, value, read)
    }

    /// Returns `true` if the set contains an element equal to the value.
    ///
    /// The value may be any borrowed form of the set's element type,
    /// but the ordering on the borrowed form *must* match the
    /// ordering on the element type.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let set = BTreeSet::from_iter([1, 2, 3]);
    /// assert_eq!(set.contains(&1), true);
    /// assert_eq!(set.contains(&4), false);
    /// ```
    pub fn contains<Q>(&self, value: &Q) -> bool
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.lock_node_for_value(value)
            .is_some_and(|node_guard| node_guard.contains(value))
    }
    pub fn get<'a, Q>(&'a self, value: &'a Q) -> Option<Ref<T, Node>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        if let Some(node_guard) = self.lock_node_for_value(value) {
            let potential_position = node_guard.try_select(value);

            if let Some(position) = potential_position {
                return Some(Ref {
                    node_guard,
                    position,
                    phantom_data: PhantomData,
                });
            }
        }

        None
    }

    pub fn len(&self) -> usize {
        self.index.iter().map(|node| node.value().lock().len()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.index.iter().all(|node| node.value().lock().is_empty())
    }
    pub fn capacity(&self) -> usize {
        self.index
            .iter()
            .map(|entry| {
                let guard = entry.value().lock();
                guard.capacity()
            })
            .sum()
    }
    pub fn node_count(&self) -> usize {
        self.index.len()
    }
}

impl<T> FromIterator<T> for BTreeSet<T>
where
    T: Debug + Ord + Clone + Send,
{
    fn from_iter<K: IntoIterator<Item = T>>(iter: K) -> Self {
        let btree = BTreeSet::new();
        iter.into_iter().for_each(|item| {
            btree.insert(item);
        });

        btree
    }
}

impl<T, const N: usize> From<[T; N]> for BTreeSet<T>
where
    T: Debug + Ord + Clone + Send,
{
    fn from(value: [T; N]) -> Self {
        let btree: BTreeSet<T> = Default::default();

        value.into_iter().for_each(|item| {
            btree.insert(item);
        });

        btree
    }
}

pub struct Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    tree: &'a BTreeSet<T, Node>,
    current_front_node: Option<Arc<Mutex<Node>>>,
    current_front_node_guard: Option<ArcMutexGuard<RawMutex, Node>>,
    current_front_node_iter: Option<std::slice::Iter<'a, T>>,
    current_back_node: Option<Arc<Mutex<Node>>>,
    current_back_node_guard: Option<ArcMutexGuard<RawMutex, Node>>,
    current_back_node_iter: Option<std::slice::Iter<'a, T>>,
    current_front_value: Option<T>,
    current_back_value: Option<T>,
    met: bool,
}

impl<'a, T, Node> Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    pub fn new(btree: &'a BTreeSet<T, Node>) -> Self {
        let current_front_node = btree.index.front().map(|e| e.value().clone());
        let current_back_node = btree.index.back().map(|e| e.value().clone());
        Self {
            tree: btree,
            current_front_node,
            current_front_node_guard: None,
            current_front_node_iter: None,
            current_back_node,
            current_back_node_guard: None,
            current_back_node_iter: None,
            current_front_value: None,
            current_back_value: None,
            met: false,
        }
    }
}

impl<'a, T, Node> Iterator for Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.met {
                return None;
            }

            if self.current_front_node.is_none() {
                match self.tree.index.front() {
                    Some(e) => {
                        self.current_front_node = Some(e.value().clone());

                        if let Some(back_entry) = self.current_back_node.as_ref() {
                            if Arc::ptr_eq(e.value(), back_entry) {
                                self.current_front_node_guard = self.current_back_node_guard.take();
                                self.current_front_node_iter = self.current_back_node_iter.take();
                            }
                            continue;
                        }

                        self.current_front_node_guard = Some(
                            self.current_front_node
                                .as_ref()
                                .expect("was just set before")
                                .lock_arc(),
                        );
                        self.current_front_node_iter = Some(unsafe {
                            std::mem::transmute::<std::slice::Iter<'_, T>, std::slice::Iter<'a, T>>(
                                self.current_front_node_guard
                                    .as_ref()
                                    .expect("was just set before")
                                    .iter(),
                            )
                        });
                    }
                    None => {
                        return None;
                    }
                }
            }

            if self.current_back_node_guard.is_some() {
                self.current_back_node_guard = None;
                self.current_back_node_iter = None;
            }

            if let Some(iter) = self.current_front_node_iter.as_mut() {
                if let Some(value) = iter.next() {
                    // A node installed after advancing or repositioning can
                    // re-expose elements at or below the last yielded value
                    // (a split re-distributes the just-finished node, a
                    // repositioned node covers part of the scanned range).
                    // Skip them instead of yielding duplicates.
                    if let Some(current_front_value) = self.current_front_value.as_ref() {
                        if value.le(current_front_value) {
                            continue;
                        }
                    }
                    if let Some(current_back_value) = self.current_back_value.as_ref() {
                        if value.ge(current_back_value) {
                            self.met = true;
                            return None;
                        }
                    }
                    self.current_front_value = Some(value.clone());
                    return Some(value);
                } else {
                    self.current_front_node_iter = None;
                    self.current_front_node_guard = None;

                    if let Some(current_node_entry) = self.tree.index.iter().find(|e| {
                        Arc::ptr_eq(
                            e.value(),
                            self.current_front_node.as_ref().expect("was just set before"),
                        )
                    }) {
                        if let Some(next_node_entry) = current_node_entry.next() {
                            self.current_front_node = Some(next_node_entry.value().clone());

                            if let Some(back_entry) = self.current_back_node.as_ref() {
                                if Arc::ptr_eq(next_node_entry.value(), back_entry) {
                                    self.current_front_node_guard = self.current_back_node_guard.take();
                                    self.current_front_node_iter = self.current_back_node_iter.take();
                                }
                                continue;
                            }

                            self.current_front_node_guard = Some(
                                self.current_front_node
                                    .as_ref()
                                    .expect("was just set before")
                                    .lock_arc(),
                            );
                            self.current_front_node_iter = Some(unsafe {
                                std::mem::transmute::<std::slice::Iter<'_, T>, std::slice::Iter<'a, T>>(
                                    self.current_front_node_guard
                                        .as_ref()
                                        .expect("was just set before")
                                        .iter(),
                                )
                            });
                            continue;
                        } else {
                            self.current_front_node = None;
                            self.current_front_node_guard = None;
                            self.current_front_node_iter = None;
                            return None;
                        }
                    } else {
                        // The just-finished node is no longer in the index (it
                        // was re-keyed by an UpdateMax repair or removed).
                        // Ending the scan here would silently truncate it;
                        // reposition after the last yielded value instead. The
                        // resume path below skips anything already yielded.
                        let repositioned = match self.current_front_value.as_ref() {
                            Some(last_yielded) => self.tree.index.lower_bound(Bound::Excluded(last_yielded)),
                            None => self.tree.index.front(),
                        };
                        if let Some(entry) = repositioned {
                            self.current_front_node = Some(entry.value().clone());
                            self.current_front_node_guard = None;
                            self.current_front_node_iter = None;
                            continue;
                        }

                        self.current_front_node = None;
                        self.current_front_node_guard = None;
                        self.current_front_node_iter = None;
                        return None;
                    }
                }
            } else {
                self.current_front_node_guard = Some(
                    self.current_front_node
                        .as_ref()
                        .expect("was just set before")
                        .lock_arc(),
                );
                self.current_front_node_iter = Some(unsafe {
                    std::mem::transmute::<std::slice::Iter<'_, T>, std::slice::Iter<'a, T>>(
                        self.current_front_node_guard
                            .as_ref()
                            .expect("was just set before")
                            .iter(),
                    )
                });

                if let Some(current_front_value) = self.current_front_value.as_ref() {
                    let g = self.current_front_node_guard.as_mut().expect("was just set before");
                    if let Some(rank) = g.rank(Bound::Excluded(current_front_value), true) {
                        let i = self.current_front_node_iter.as_mut().expect("was just set before");
                        if let Some(v) = i.nth(rank + 1) {
                            if let Some(current_back_value) = self.current_back_value.as_ref() {
                                if v.ge(current_back_value) {
                                    self.met = true;
                                    return None;
                                }
                            }
                            self.current_front_value = Some(v.clone());
                            return Some(v);
                        }
                    }
                    // else iter is exhausted, will continue in next loop iteration.
                    continue;
                }
            }
        }
    }
}

impl<'a, T, Node> DoubleEndedIterator for Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            if self.met {
                return None;
            }

            if self.current_back_node.is_none() {
                match self.tree.index.back() {
                    Some(e) => {
                        self.current_back_node = Some(e.value().clone());

                        if let Some(front_entry) = self.current_front_node.as_ref() {
                            if Arc::ptr_eq(e.value(), front_entry) {
                                self.current_back_node_guard = self.current_front_node_guard.take();
                                self.current_back_node_iter = self.current_front_node_iter.take();
                            }
                            continue;
                        }

                        self.current_back_node_guard =
                            Some(self.current_back_node.as_ref().expect("was just set before").lock_arc());
                        self.current_back_node_iter = Some(unsafe {
                            std::mem::transmute::<std::slice::Iter<'_, T>, std::slice::Iter<'a, T>>(
                                self.current_back_node_guard
                                    .as_ref()
                                    .expect("was just set before")
                                    .iter(),
                            )
                        });
                    }
                    None => {
                        return None;
                    }
                }
            }

            if self.current_front_node_guard.is_some() {
                self.current_front_node_guard = None;
                self.current_front_node_iter = None;
            }

            if let Some(iter) = self.current_back_node_iter.as_mut() {
                if let Some(value) = iter.next_back() {
                    // Mirror of the forward path: skip elements at or above
                    // the last value yielded from the back, which a freshly
                    // installed node iterator can re-expose after churn.
                    if let Some(current_back_value) = self.current_back_value.as_ref() {
                        if value.ge(current_back_value) {
                            continue;
                        }
                    }
                    if let Some(current_front_value) = self.current_front_value.as_ref() {
                        if value.le(current_front_value) {
                            self.met = true;
                            return None;
                        }
                    }
                    self.current_back_value = Some(value.clone());
                    return Some(value);
                } else {
                    self.current_back_node_iter = None;
                    self.current_back_node_guard = None;

                    if let Some(current_node_entry) =
                        self.tree.index.iter().find(|e| {
                            Arc::ptr_eq(e.value(), self.current_back_node.as_ref().expect("was just set before"))
                        })
                    {
                        if let Some(prev_node_entry) = current_node_entry.prev() {
                            self.current_back_node = Some(prev_node_entry.value().clone());

                            if let Some(front_entry) = self.current_front_node.as_ref() {
                                if Arc::ptr_eq(prev_node_entry.value(), front_entry) {
                                    self.current_back_node_guard = self.current_front_node_guard.take();
                                    self.current_back_node_iter = self.current_front_node_iter.take();
                                }
                                continue;
                            }

                            self.current_back_node_guard =
                                Some(self.current_back_node.as_ref().expect("was just set before").lock_arc());
                            self.current_back_node_iter = Some(unsafe {
                                std::mem::transmute::<std::slice::Iter<'_, T>, std::slice::Iter<'a, T>>(
                                    self.current_back_node_guard
                                        .as_ref()
                                        .expect("was just set before")
                                        .iter(),
                                )
                            });
                            continue;
                        } else {
                            self.current_back_node = None;
                            self.current_back_node_guard = None;
                            self.current_back_node_iter = None;
                            return None;
                        }
                    } else {
                        // Mirror of the forward path: the just-finished node
                        // vanished from the index, so reposition on the node
                        // covering the last value yielded from the back
                        // instead of truncating the scan.
                        let repositioned = match self.current_back_value.as_ref() {
                            Some(last_yielded) => self
                                .tree
                                .index
                                .lower_bound(Bound::Included(last_yielded))
                                .or_else(|| self.tree.index.back()),
                            None => self.tree.index.back(),
                        };
                        if let Some(entry) = repositioned {
                            self.current_back_node = Some(entry.value().clone());
                            self.current_back_node_guard = None;
                            self.current_back_node_iter = None;
                            continue;
                        }

                        self.current_back_node = None;
                        self.current_back_node_guard = None;
                        self.current_back_node_iter = None;
                        return None;
                    }
                }
            } else {
                self.current_back_node_guard =
                    Some(self.current_back_node.as_ref().expect("was just set before").lock_arc());
                self.current_back_node_iter = Some(unsafe {
                    std::mem::transmute::<std::slice::Iter<'_, T>, std::slice::Iter<'a, T>>(
                        self.current_back_node_guard
                            .as_ref()
                            .expect("was just set before")
                            .iter(),
                    )
                });

                if let Some(current_back_value) = self.current_back_value.as_ref() {
                    let g = self.current_back_node_guard.as_mut().expect("was just set before");
                    if let Some(rank) = g.rank(Bound::Excluded(current_back_value), false) {
                        let i = self.current_back_node_iter.as_mut().expect("was just set before");
                        if let Some(v) = i.nth_back(rank + 1) {
                            if let Some(current_front_value) = self.current_front_value.as_ref() {
                                if v.le(current_front_value) {
                                    self.met = true;
                                    return None;
                                }
                            }
                            self.current_back_value = Some(v.clone());
                            return Some(v);
                        }
                    }
                    // else iter is exhausted, will continue in next loop iteration.
                    continue;
                }
            }
        }
    }
}

impl<'a, T: Debug + Ord + Clone + Send, Node: NodeLike<T> + Send + 'static> FusedIterator for Iter<'a, T, Node> {}

impl<'a, T, Node> IntoIterator for &'a BTreeSet<T, Node>
where
    T: Debug + Ord + Send + Clone,
    Node: NodeLike<T> + Send + 'static,
{
    type Item = &'a T;

    type IntoIter = Iter<'a, T, Node>;

    fn into_iter(self) -> Self::IntoIter {
        Iter::new(self)
    }
}

pub struct Range<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    iter: Iter<'a, T, Node>,
}

impl<'a, T, Node> Range<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    pub fn new<Q, R>(btree: &'a BTreeSet<T, Node>, range: R) -> Self
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
        R: RangeBounds<Q>,
    {
        let _global_guard = btree.index_lock.read();

        let mut met = false;

        let start_bound = range.start_bound();
        let current_front_entry = btree.index.lower_bound(start_bound);

        let front_value = if let Some(front_entry) = current_front_entry.as_ref() {
            let front_guard = front_entry.value().lock_arc();
            if let Some(rank) = match start_bound {
                Bound::Included(v) => front_guard.rank(Bound::Included(v), true),
                Bound::Excluded(v) => front_guard.rank(Bound::Excluded(v), true),
                Bound::Unbounded => None,
            } {
                let mut front_iter = front_guard.iter();
                let front_value = front_iter.nth(rank).cloned();
                drop(front_guard);

                front_value
            } else if let Some(pre_front_entry) = front_entry.prev() {
                let pre_front_guard = pre_front_entry.value().lock_arc();
                let front_value = pre_front_guard.iter().last().cloned();
                drop(pre_front_guard);

                front_value
            } else {
                None
            }
        } else {
            None
        };

        let end_bound = range.end_bound();
        let current_back_entry = btree
            .index
            .upper_bound(end_bound)
            .and_then(|e| e.next().or_else(|| btree.index.back()))
            .or_else(|| btree.index.front());

        let back_value = if let Some(back_entry) = current_back_entry.as_ref() {
            let back_guard = back_entry.value().lock_arc();
            if let Some(rank) = match end_bound {
                Bound::Included(v) => back_guard.rank(Bound::Included(v), false),
                Bound::Excluded(v) => back_guard.rank(Bound::Excluded(v), false),
                Bound::Unbounded => None,
            } {
                let mut back_iter = back_guard.iter();
                let back_value = back_iter.nth_back(rank).cloned();
                drop(back_guard);

                back_value
            } else if let Some(prev_back_entry) = back_entry.next() {
                let prev_back_guard = prev_back_entry.value().lock_arc();
                let back_value = prev_back_guard.iter().next().cloned();
                drop(prev_back_guard);

                back_value
            } else {
                None
            }
        } else {
            None
        };

        if front_value.is_none() && back_value.is_none() {
            // in this case we iter full or no iter at all
            if start_bound != Bound::Unbounded || end_bound != Bound::Unbounded {
                if let Some(max) = btree.index.back().and_then(|e| e.value().lock_arc().max().cloned()) {
                    if let Bound::Included(v) = start_bound {
                        if v > max.borrow() {
                            met = true;
                        }
                    } else if let Bound::Excluded(v) = start_bound {
                        if v >= max.borrow() {
                            met = true;
                        }
                    }
                }

                if let Some(min) = btree.index.front().and_then(|e| e.value().lock_arc().min().cloned()) {
                    if let Bound::Included(v) = end_bound {
                        if v < min.borrow() {
                            met = true;
                        }
                    } else if let Bound::Excluded(v) = end_bound {
                        if v <= min.borrow() {
                            met = true;
                        }
                    }
                }
            }
        }

        Self {
            iter: Iter {
                tree: btree,
                current_front_node: current_front_entry.map(|e| e.value().clone()),
                current_front_node_guard: None,
                current_front_node_iter: None,
                current_back_node: current_back_entry.map(|e| e.value().clone()),
                current_back_node_guard: None,
                current_back_node_iter: None,
                current_front_value: front_value,
                current_back_value: back_value,
                met,
            },
        }
    }
}

impl<'a, T, Node> Iterator for Range<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, T, Node> DoubleEndedIterator for Range<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

impl<'a, T, Node> FusedIterator for Range<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
}

impl<'a, T, Node> BTreeSet<T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    /// Gets an iterator that visits the elements in the `BTreeSet` in ascending
    /// order.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let set = BTreeSet::from_iter([1, 2, 3]);
    /// let mut set_iter = set.iter();
    /// assert_eq!(set_iter.next(), Some(&1));
    /// assert_eq!(set_iter.next(), Some(&2));
    /// assert_eq!(set_iter.next(), Some(&3));
    /// assert_eq!(set_iter.next(), None);
    /// ```
    ///
    /// Values returned by the iterator are returned in ascending order:
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let set = BTreeSet::from_iter([3, 1, 2]);
    /// let mut set_iter = set.iter();
    /// assert_eq!(set_iter.next(), Some(&1));
    /// assert_eq!(set_iter.next(), Some(&2));
    /// assert_eq!(set_iter.next(), Some(&3));
    /// assert_eq!(set_iter.next(), None);
    /// ```
    pub fn iter(&'a self) -> Iter<'a, T, Node> {
        Iter::new(self)
    }
    /// Constructs a double-ended iterator over a sub-range of elements in the set.
    /// The simplest way is to use the range syntax `min..max`, thus `range(min..max)` will
    /// yield elements from min (inclusive) to max (exclusive).
    /// The range may also be entered as `(Bound<T>, Bound<T>)`, so for example
    /// `range((Excluded(4), Included(10)))` will yield a left-exclusive, right-inclusive
    /// range from 4 to 10.
    ///
    /// # Panics
    ///
    /// Panics if range `start > end`.
    /// Panics if range `start == end` and both bounds are `Excluded`.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    /// use std::ops::Bound::Included;
    ///
    /// let mut set = BTreeSet::<usize>::new();
    /// set.insert(3);
    /// set.insert(5);
    /// set.insert(8);
    /// for &elem in set.range((Included(&4), Included(&8))) {
    ///     println!("{elem}");
    /// }
    /// assert_eq!(Some(&5), set.range(4..).next());
    /// ```
    pub fn range<Q, R>(&'a self, range: R) -> Range<'a, T, Node>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
        R: RangeBounds<Q>,
    {
        Range::new(self, range)
    }
}

impl<T> BTreeSet<T>
where
    T: Debug + Ord + Clone + Send + 'static,
{
    pub fn remove_range<R, Q>(&self, range: R)
    where
        Q: Ord + ?Sized,
        T: Borrow<Q>,
        R: RangeBounds<Q>,
    {
        // Declare detached storage before the structural guard so element
        // destructors run only after that guard is released.
        let mut detached_nodes = Vec::new();
        let _global_guard = self.index_lock.write();

        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        // First node that can contain an element within the start bound. If
        // no index key reaches the start bound, nothing qualifies.
        let Some(front_entry) = self.index.lower_bound(start_bound) else {
            return;
        };

        // Last node that can contain an element within the end bound. Both an
        // inclusive and an exclusive end resolve to the first node whose key
        // is >= the bound value: a node keyed exactly at an exclusive bound
        // still holds elements below it. Past the last key, the last node is
        // the only candidate.
        let back_entry = match end_bound {
            Bound::Included(end) | Bound::Excluded(end) => self
                .index
                .lower_bound(Bound::Included(end))
                .or_else(|| self.index.back()),
            Bound::Unbounded => self.index.back(),
        };
        let Some(back_entry) = back_entry else {
            return;
        };
        if back_entry.key() < front_entry.key() {
            // The end bound resolves before the start bound: empty range.
            return;
        }

        // Number of leading elements of the back node that fall within the
        // end bound (inclusive end: elements <= bound; exclusive: < bound).
        let removed_prefix_len = |guard: &Vec<T>| -> usize {
            match end_bound {
                Bound::Included(end) => guard.rank(Bound::Excluded(end), true).map_or(0, |last| last + 1),
                Bound::Excluded(end) => guard.rank(Bound::Included(end), true).map_or(0, |last| last + 1),
                Bound::Unbounded => guard.len(),
            }
        };

        if Arc::ptr_eq(front_entry.value(), back_entry.value()) {
            // The whole range lives in one node.
            let mut guard = front_entry.value().lock_arc();
            let front_position = guard.rank(start_bound, true).map_or(0, |last| last + 1);
            let back_position = removed_prefix_len(&guard);
            if back_position <= front_position {
                return;
            }

            let original_len = guard.len();
            guard.drain(front_position..back_position);
            if back_position == original_len {
                // The node's maximum was removed: re-key the entry, or drop
                // it when the node was fully drained.
                let node = front_entry.value().clone();
                front_entry.remove();
                if let Some(new_max) = guard.last().cloned() {
                    self.index.insert(new_max, node);
                }
            }
            return;
        }

        let mut front_guard = front_entry.value().lock_arc();
        let mut back_guard = back_entry.value().lock_arc();
        let front_position = front_guard.rank(start_bound, true).map_or(0, |last| last + 1);
        let back_position = removed_prefix_len(&back_guard);

        // Remove every node strictly between the front and the back one.
        while let Some(next_entry) = front_entry.next() {
            if next_entry.key() >= back_entry.key() {
                break;
            }

            let mut removed_node = next_entry.value().lock_arc();
            // `ArcMutexGuard` owns an Arc to the node, so removing the
            // skip-list entry cannot invalidate the guarded storage.
            next_entry.remove();
            detached_nodes.push(std::mem::take(&mut *removed_node));
        }

        // Trim the front node from the start position: its maximum goes away,
        // so its entry must be re-keyed (or dropped when the node empties).
        front_entry.remove();
        front_guard.drain(front_position..);
        if !front_guard.is_empty() {
            let new_front_max = front_guard.last().unwrap().clone();
            self.index.insert(new_front_max, front_entry.value().clone());
        }

        // Trim the back node's prefix: its maximum survives unless the whole
        // node drains, so the entry only changes when the node empties.
        if back_position >= back_guard.len() {
            back_entry.remove();
            back_guard.drain(..);
        } else if back_position > 0 {
            back_guard.drain(..back_position);
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "cdc")]
    use crate::cdc::change::ChangeEvent;
    use crate::concurrent::operation::Operation;
    use crate::concurrent::set::{BTreeSet, Iter, DEFAULT_INNER_SIZE};
    #[cfg(feature = "multimap")]
    use crate::core::multipair::RandomMultiPair;
    use crate::core::node::NodeLike;
    use rand::Rng;
    use std::collections::HashSet;
    use std::ops::Bound::Included;
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    // Regression for https://github.com/lucidarium-systems/indexset/issues/57.
    #[test]
    fn test_node_size_two_preserves_all_u64_values() {
        let set = BTreeSet::<u64>::with_maximum_node_size(2);

        for value in 0..10_u64 {
            set.insert(value);
        }

        assert_eq!(set.iter().copied().collect::<Vec<_>>(), (0..10).collect::<Vec<_>>());
    }

    // Regression for https://github.com/lucidarium-systems/indexset/issues/57.
    #[test]
    fn test_node_size_three_preserves_all_u8_values() {
        let set = BTreeSet::<u8>::with_maximum_node_size(3);

        for value in 0..20_u8 {
            set.insert(value);
        }

        assert_eq!(set.iter().copied().collect::<Vec<_>>(), (0..20).collect::<Vec<_>>());
    }

    #[test]
    fn concurrent_first_writers_preserve_disjoint_ranges() {
        const WRITERS: u64 = 8;
        const VALUES_PER_WRITER: u64 = 1_000;

        let set = Arc::new(BTreeSet::<u64>::new());
        let start = Arc::new(Barrier::new(WRITERS as usize));
        let handles = (0..WRITERS)
            .map(|writer| {
                let set = Arc::clone(&set);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    let first = writer * VALUES_PER_WRITER;
                    for value in first..first + VALUES_PER_WRITER {
                        assert!(set.insert(value));
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }

        let expected = (0..WRITERS * VALUES_PER_WRITER).collect::<Vec<_>>();
        assert_eq!(set.len(), expected.len());
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn mixed_structural_and_node_lock_paths_complete_without_deadlock() {
        const THREADS: usize = 8;
        const OPERATIONS: usize = 1_000;

        let set = Arc::new(BTreeSet::<usize>::with_maximum_node_size(8));
        for value in 0..256 {
            set.insert(value);
        }

        let start = Arc::new(Barrier::new(THREADS));
        let (done_tx, done_rx) = mpsc::channel();
        let handles = (0..THREADS)
            .map(|worker| {
                let set = Arc::clone(&set);
                let start = Arc::clone(&start);
                let done_tx = done_tx.clone();
                thread::spawn(move || {
                    start.wait();
                    for operation in 0..OPERATIONS {
                        let value = (operation * 17 + worker * 31) % 512;
                        match (operation + worker) % 5 {
                            0 => {
                                set.insert(value);
                            }
                            1 => {
                                set.remove(&value);
                            }
                            2 => {
                                let _ = set.contains(&value);
                            }
                            3 => {
                                let _ = set.get_with(&value, Clone::clone);
                            }
                            _ => {
                                set.remove_range(value..=value);
                                set.insert(value);
                            }
                        }
                    }
                    done_tx.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        drop(done_tx);

        for _ in 0..THREADS {
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("mixed structural/node-lock workload did not complete");
        }
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_insert() {
        let set = Arc::new(BTreeSet::<i32>::new());
        let num_threads = 128;
        let operations_per_thread = 10000;
        let mut handles = vec![];

        let test_data: Vec<Vec<(i32, i32)>> = (0..num_threads)
            .map(|_| {
                let mut rng = rand::rng();
                (0..operations_per_thread)
                    .map(|_| {
                        let value = rng.random_range(0..100000);
                        let operation = rng.random_range(0..2);
                        (operation, value)
                    })
                    .collect()
            })
            .collect();

        let expected_values = Arc::new(Mutex::new(HashSet::new()));

        for thread_idx in 0..num_threads {
            let set_clone = Arc::clone(&set);
            let expected_values = Arc::clone(&expected_values);
            let thread_data = test_data[thread_idx].clone();

            let handle = thread::spawn(move || {
                for (operation, value) in thread_data {
                    if operation == 0 {
                        let _a = set_clone.insert(value);
                        expected_values.lock().unwrap().insert(value);
                    }
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let expected_values = expected_values.lock().unwrap();
        assert_eq!(set.len(), expected_values.len());

        for value in expected_values.iter() {
            assert!(set.contains(value));
        }
    }

    #[test]
    fn test_insert_desc() {
        let set = Arc::new(BTreeSet::<i32>::new());

        assert!(set.insert(2));
        assert!(set.insert(1));
    }

    #[test]
    fn test_insert_st() {
        let set = Arc::new(BTreeSet::<i32>::new());
        let mut rng = rand::rng();

        let n = 2048 * 100;
        let range = 0..n;
        let mut inserted_values = HashSet::new();
        for _ in range {
            let value = rng.random_range(0..n);
            if inserted_values.insert(value) {
                set.insert(value);
            }
        }

        assert_eq!(
            set.len(),
            inserted_values.len(),
            "Length did not match, missing: {:?}",
            set.index
                .iter()
                .flat_map(|entry| entry.value().lock().iter().cloned().collect::<Vec<_>>())
                .collect::<HashSet<_>>()
                .symmetric_difference(&inserted_values)
                .collect::<Vec<_>>()
        );
        for i in inserted_values {
            assert!(
                set.contains(&i),
                "Did not find: {} with index: {:?}",
                i,
                set.index.iter().collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn test_single_element() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        let mut iter = set.into_iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn test_multiple_elements() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        set.insert(2);
        set.insert(3);
        let mut iter = set.into_iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next_back(), Some(&3));
        assert_eq!(iter.next(), Some(&2));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn test_bidirectional_iteration() {
        let set = BTreeSet::<i32>::with_maximum_node_size(3);
        for i in 1..=20 {
            set.insert(i);
        }
        let mut iter = set.into_iter();
        for i in 0..10 {
            // (1, 20), (2, 19), (3, 18), (4, 17), (5, 16), (6, 15), (7, 14), (8, 13), (9, 12), (10, 11)
            let tree = set.index.iter().collect::<Vec<_>>();

            let expected_next = i + 1;
            let actual_next = iter.next();
            assert_eq!(actual_next, Some(&expected_next), "Tree: {:?}", tree);

            let expected_next_back = 20 - i;
            let actual_next_back = iter.next_back();
            assert_eq!(actual_next_back, Some(&expected_next_back), "Tree: {:?}", tree);
        }
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn test_fused_iterator() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        let mut iter = set.into_iter();
        assert_eq!(iter.next(), Some(&1));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_fused_iterator_back() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        let mut iter = set.into_iter();
        assert_eq!(iter.next_back(), Some(&1));
        assert_eq!(iter.next_back(), None);
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn test_out_of_bounds_range() {
        let btree: BTreeSet<usize> = BTreeSet::from_iter(0..10);
        assert_eq!(btree.range((Included(5), Included(10))).count(), 5);
        assert_eq!(btree.range((Included(5), Included(11))).count(), 5);
        assert_eq!(btree.range((Included(5), Included(10 + DEFAULT_INNER_SIZE))).count(), 5);
        assert_eq!(btree.range((Included(0), Included(11))).count(), 10);
    }

    #[test]
    fn test_iterating_over_blocks() {
        let btree = BTreeSet::from_iter((0..(DEFAULT_INNER_SIZE + 10)).into_iter());
        assert_eq!(btree.iter().count(), (0..(DEFAULT_INNER_SIZE + 10)).count());
        let start = btree
            .range(0..DEFAULT_INNER_SIZE)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>();

        assert_eq!(start, (0..DEFAULT_INNER_SIZE).collect::<Vec<_>>());
        assert_eq!(
            btree
                .range(0..=DEFAULT_INNER_SIZE)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>(),
            (0..=DEFAULT_INNER_SIZE).collect::<Vec<_>>()
        );
        assert_eq!(
            btree.range(0..=DEFAULT_INNER_SIZE + 1).count(),
            (0..=DEFAULT_INNER_SIZE + 1).count()
        );
        assert_eq!(btree.iter().rev().count(), (0..(DEFAULT_INNER_SIZE + 10)).count());
        assert_eq!(
            btree.range(0..DEFAULT_INNER_SIZE).rev().count(),
            (0..DEFAULT_INNER_SIZE).count()
        );
        assert_eq!(
            btree.range(0..=DEFAULT_INNER_SIZE).rev().count(),
            (0..=DEFAULT_INNER_SIZE).count()
        );
        assert_eq!(
            btree.range(0..=DEFAULT_INNER_SIZE + 1).rev().count(),
            (0..=DEFAULT_INNER_SIZE + 1).count()
        );
    }

    #[test]
    fn test_empty_set() {
        let btree: BTreeSet<usize> = BTreeSet::new();
        assert_eq!(btree.iter().count(), 0);
        assert_eq!(btree.range(0..0).count(), 0);
        assert_eq!(btree.range(0..).count(), 0);
        assert_eq!(btree.range(..0).count(), 0);
        assert_eq!(btree.range(..).count(), 0);
        assert_eq!(btree.range(0..=0).count(), 0);
        assert_eq!(btree.range(..1).count(), 0);

        assert_eq!(btree.iter().rev().count(), 0);
        assert_eq!(btree.range(0..0).rev().count(), 0);
        assert_eq!(btree.range(..).rev().count(), 0);
        assert_eq!(btree.range(..1).rev().count(), 0);

        assert_eq!(btree.range(..DEFAULT_INNER_SIZE).count(), 0);
        assert_eq!(btree.range(DEFAULT_INNER_SIZE..DEFAULT_INNER_SIZE * 2).count(), 0);
    }

    #[test]
    fn test_remove_range() {
        // We have DEFAULT_INNER_SIZE * 2 elements
        let btree = BTreeSet::from_iter(0..(DEFAULT_INNER_SIZE * 2));
        let expected_len = DEFAULT_INNER_SIZE * 2;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // We remove 10 elements from the beginning, 5 included up to 15 excluded.
        btree.remove_range(5..15);
        let expected_len = expected_len - 10;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // Then take more 10 from the middle
        btree.remove_range(DEFAULT_INNER_SIZE - 5..DEFAULT_INNER_SIZE + 5);
        let expected_len = expected_len - 10;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // And then remove 512
        btree.remove_range(..DEFAULT_INNER_SIZE / 2);
        // We add +10 here because we are removing everything up to 512, but we already removed 5..15.
        let expected_len = expected_len - (DEFAULT_INNER_SIZE / 2) + 10;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // And then everything from (512 * 3) / 2 to the end, which is
        // exactly the upper 512 values.
        let from = (DEFAULT_INNER_SIZE * 3) / 2;
        btree.remove_range(from..);
        let expected_len = expected_len - DEFAULT_INNER_SIZE / 2;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // We now clear the tree
        btree.remove_range(..);
        assert_eq!(btree.len(), 0);

        // Re-insert everything
        for i in 0..(DEFAULT_INNER_SIZE * 2) {
            btree.insert(i);
        }
        let expected_len = DEFAULT_INNER_SIZE * 2;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        btree.remove_range((std::ops::Bound::Excluded(5), std::ops::Bound::Excluded(15)));
        let expected_len = expected_len - 9;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        btree.remove_range((
            std::ops::Bound::Included(DEFAULT_INNER_SIZE),
            std::ops::Bound::Excluded(DEFAULT_INNER_SIZE + 10),
        ));
        let expected_len = expected_len - 10;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // This range exceeds the size of the tree
        btree.remove_range(DEFAULT_INNER_SIZE * 3..DEFAULT_INNER_SIZE * 4);
        let expected_len = expected_len;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);

        // This range starts at the very end of the tree, and exceeds it
        btree.remove_range(DEFAULT_INNER_SIZE * 2 - 5..DEFAULT_INNER_SIZE * 3);
        let expected_len = expected_len - 5;
        let actual_len = btree.len();
        assert_eq!(expected_len, actual_len);
    }

    #[test]
    fn remove_range_end_bound_regressions() {
        // `x..` must remove only the suffix, not also drain the first node.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in 0..10 {
            set.insert(value);
        }
        set.remove_range(7..);
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), (0..7).collect::<Vec<_>>());

        // `..` must clear every node, not only the first one.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in 0..10 {
            set.insert(value);
        }
        set.remove_range(..);
        assert_eq!(set.len(), 0);
        assert!(set.is_empty());

        // An inclusive end must remove every element up to and including it.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in 0..10 {
            set.insert(value);
        }
        set.remove_range(3..=5);
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![0, 1, 2, 6, 7, 8, 9]);

        // `x..=x` must remove exactly x, not drain to the node end.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in 0..10 {
            set.insert(value);
        }
        set.remove_range(2..=2);
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![0, 1, 3, 4, 5, 6, 7, 8, 9]);

        // An exclusive end equal to a node maximum must not drain the
        // following node.
        let set = BTreeSet::<u64>::with_maximum_node_size(3);
        for value in 0..9 {
            set.insert(value);
        }
        let boundary = *set.index.front().expect("node must exist").key();
        set.remove_range(0..boundary);
        let expected = (0..9).filter(|value| *value >= boundary).collect::<Vec<_>>();
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn remove_range_matches_btreeset_oracle() {
        use std::ops::Bound;

        fn oracle_case(node_size: usize, values: &[u64], start: Bound<u64>, end: Bound<u64>) {
            let set = BTreeSet::<u64>::with_maximum_node_size(node_size);
            for &value in values {
                set.insert(value);
            }
            let mut oracle = values.iter().copied().collect::<std::collections::BTreeSet<_>>();

            let range = (start, end);
            oracle.retain(|value| !std::ops::RangeBounds::contains(&range, value));
            set.remove_range(range);

            assert_eq!(
                set.iter().copied().collect::<Vec<_>>(),
                oracle.iter().copied().collect::<Vec<_>>(),
                "node_size={node_size}, start={start:?}, end={end:?}"
            );
            assert_eq!(
                set.len(),
                oracle.len(),
                "node_size={node_size}, start={start:?}, end={end:?}"
            );
        }

        // Even values only, so probes hit present values, absent values, and
        // both sides of every node boundary.
        let values = (0..15u64).map(|value| value * 2).collect::<Vec<_>>();
        let mut bounds = vec![Bound::Unbounded];
        for probe in 0..=30u64 {
            bounds.push(Bound::Included(probe));
            bounds.push(Bound::Excluded(probe));
        }

        // Single-node and multi-node geometries, on and off node boundaries.
        for node_size in [4usize, 7, 64] {
            for &start in &bounds {
                for &end in &bounds {
                    oracle_case(node_size, &values, start, end);
                }
            }
        }
    }

    #[test]
    fn remove_range_clears_detached_nodes() {
        // White-box geometry fixture: a failure after split/merge tuning may
        // mean node boundaries changed rather than detached-node clearing
        // regressed. WorkTable's persisted-index fixtures have the same
        // geometry coupling.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in 0..32 {
            set.insert(value);
        }

        let front = set.index.lower_bound(Included(&2)).unwrap();
        let detached = front.next().unwrap().value().clone();
        let detached_values = detached.lock().iter().copied().collect::<Vec<_>>();
        assert!(detached_values.iter().all(|value| (2..30).contains(value)));

        set.remove_range(2..30);

        assert!(detached.lock().is_empty());
        assert!(detached_values.iter().all(|value| !set.contains(value)));
    }

    #[test]
    fn remove_reaches_value_above_every_index_key() {
        let set = BTreeSet::<u64>::new();
        for value in [1u64, 2, 3] {
            set.insert(value);
        }

        // Simulate a stale-key window: the last node's maximum grows past its
        // index key before the UpdateMax repair commits. `contains` already
        // reaches such a value through the back-node fallback; `remove` must
        // reach it the same way.
        {
            let node = set.index.back().expect("node must exist").value().clone();
            let mut guard = node.lock();
            NodeLike::insert(&mut *guard, 5u64);
        }

        assert!(set.contains(&5));
        assert_eq!(set.remove(&5), Some(5), "value above every index key must be removable");
        assert!(!set.contains(&5));
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    // Simulates the first phase of a remove that empties a node: the elements
    // are deleted under the node lock, leaving the index entry with a stale
    // key, and the caller receives the not-yet-committed MakeUnreachable.
    fn drain_node_with_pending_unlink(set: &BTreeSet<u64>, values: &[u64], stale_key: u64) -> Operation<u64, Vec<u64>> {
        let node = set.index.back().expect("node must exist").value().clone();
        {
            let mut guard = node.lock();
            for value in values {
                NodeLike::delete(&mut *guard, value).expect("seeded value must be present");
            }
        }
        Operation::MakeUnreachable(node, stale_key)
    }

    #[test]
    fn split_commit_against_drained_node_fails_instead_of_dropping_insert() {
        let set = BTreeSet::<u64>::new();
        for seeded in [10u64, 20, 30] {
            set.insert(seeded);
        }
        let node = set.index.back().expect("node must exist").value().clone();
        // A split is scheduled with a pending insert riding on it...
        let pending_split = Operation::Split(node.clone(), 30u64, 15u64);
        // ...then a concurrent remove drains the node before the commit.
        {
            let mut guard = node.lock();
            for seeded in [10u64, 20, 30] {
                NodeLike::delete(&mut *guard, &seeded).expect("seeded value must be present");
            }
        }

        // The commit must fail so the insert retries; it must neither drop
        // the pending value silently nor unlink the still-indexed node.
        assert!(pending_split.commit::<false>(&set.index).is_err());
        assert!(
            set.index.get(&30).is_some(),
            "drained node must stay linked for the retry"
        );

        // The retried insert lands and repairs the index.
        assert!(set.insert(15));
        assert!(set.contains(&15));
        assert_eq!(set.remove(&15), Some(15));
        assert!(set.is_empty());
    }

    #[cfg(feature = "cdc")]
    #[test]
    fn split_commit_against_drained_node_does_not_panic_in_cdc_build() {
        let set = BTreeSet::<u64>::new();
        for seeded in [10u64, 20, 30] {
            set.insert(seeded);
        }
        let node = set.index.back().expect("node must exist").value().clone();
        let pending_split = Operation::Split(node.clone(), 30u64, 15u64);
        {
            let mut guard = node.lock();
            for seeded in [10u64, 20, 30] {
                NodeLike::delete(&mut *guard, &seeded).expect("seeded value must be present");
            }
        }

        // The cdc-emitting commit used to panic reading the drained node's
        // maximum while holding the structural write lock.
        assert!(pending_split.commit::<true>(&set.index).is_err());
        assert!(set.index.get(&30).is_some());

        let (old, _events) = set.put_cdc(15);
        assert!(old.is_none());
        assert!(set.contains(&15));
    }

    #[test]
    fn insert_into_emptied_node_survives_stale_make_unreachable() {
        // One value below and one above the stale index key.
        for value in [5u64, 40u64] {
            let set = BTreeSet::<u64>::new();
            for seeded in [10u64, 20, 30] {
                set.insert(seeded);
            }
            let pending_unlink = drain_node_with_pending_unlink(&set, &[10, 20, 30], 30);

            // The insert lands in the emptied node and must repair the stale
            // index key immediately.
            assert!(set.insert(value));

            // The stale unlink then commits: it must not remove the node that
            // now contains the acknowledged insert.
            let _ = pending_unlink.commit::<false>(&set.index);

            assert!(set.contains(&value), "value {value} lost after stale unlink");
            assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![value]);
            assert_eq!(set.remove(&value), Some(value));
            assert!(!set.contains(&value));
            assert_eq!(set.len(), 0);
        }
    }

    #[test]
    fn stale_make_unreachable_rekeys_refilled_node_instead_of_unlinking() {
        for value in [5u64, 40u64] {
            let set = BTreeSet::<u64>::new();
            for seeded in [10u64, 20, 30] {
                set.insert(seeded);
            }
            let node = set.index.back().expect("node must exist").value().clone();
            let pending_unlink = drain_node_with_pending_unlink(&set, &[10, 20, 30], 30);

            // First phase of a concurrent insert: the value lands in the
            // routed (empty) node under the node lock; the UpdateMax repair
            // has not committed yet.
            {
                let mut guard = node.lock();
                NodeLike::insert(&mut *guard, value);
            }
            let pending_repair = Operation::UpdateMax(node.clone(), 30u64);

            // The remove's stale unlink commits first: it must observe the
            // refilled node and re-key it rather than unlink it.
            assert!(pending_unlink.commit::<false>(&set.index).is_ok());
            // The insert's repair then finds the entry already re-keyed.
            let _ = pending_repair.commit::<false>(&set.index);

            assert!(set.contains(&value), "value {value} lost to stale unlink");
            assert_eq!(set.remove(&value), Some(value));
            assert!(set.is_empty());
        }
    }

    #[test]
    fn concurrent_remove_reinsert_over_emptying_nodes_preserves_all_keys() {
        const THREADS: u64 = 4;
        const ITERATIONS: u64 = 1_000;

        // Tiny nodes over adjacent keys: removes empty nodes constantly, so
        // inserts keep racing pending MakeUnreachable repairs.
        let set = Arc::new(BTreeSet::<u64>::with_maximum_node_size(2));
        for key in 0..THREADS {
            set.insert(key);
        }

        let start = Arc::new(Barrier::new(THREADS as usize));
        let (done_tx, done_rx) = mpsc::channel();
        let handles = (0..THREADS)
            .map(|key| {
                let set = Arc::clone(&set);
                let start = Arc::clone(&start);
                let done_tx = done_tx.clone();
                thread::spawn(move || {
                    start.wait();
                    for _ in 0..ITERATIONS {
                        // A point remove may transiently miss while another
                        // writer's index repair is still in flight, but the
                        // acknowledged insert must never be LOST: under a
                        // stable snapshot the key must still be somewhere, and
                        // the self-healing repairs must make it removable
                        // again promptly.
                        let mut attempts = 0;
                        while set.remove(&key).is_none() {
                            let stable_guard = set.index_lock.write();
                            let present = set.index.iter().any(|e| e.value().lock().contains(&key));
                            drop(stable_guard);
                            assert!(present, "acknowledged insert of {key} was lost");
                            attempts += 1;
                            assert!(attempts < 10_000, "key {key} present but never became removable");
                            std::hint::spin_loop();
                        }
                        assert!(set.insert(key), "{key} still present after acknowledged remove");
                    }
                    done_tx.send(()).unwrap();
                })
            })
            .collect::<Vec<_>>();
        drop(done_tx);

        for _ in 0..THREADS {
            done_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("remove/reinsert workload did not complete in time");
        }
        for handle in handles {
            handle.join().unwrap();
        }

        for key in 0..THREADS {
            assert!(set.contains(&key), "key {key} lost after churn");
            assert_eq!(set.remove(&key), Some(key));
        }
        assert!(set.is_empty());
    }

    #[test]
    fn test_remove_single_element() {
        let set = BTreeSet::<i32>::new();
        set.insert(5);
        assert!(set.contains(&5));
        assert!(set.remove(&5).is_some());
        assert!(!set.contains(&5));
        assert!(!set.remove(&5).is_some());
    }

    #[cfg(feature = "multimap")]
    #[test]
    fn test_remove_where_reindexes_changed_node_maximum() {
        let set = BTreeSet::<usize>::with_maximum_node_size(4);
        for value in 0..4 {
            set.insert(value);
        }

        assert_eq!(set.remove_where(|value| *value == 3), Some(3));
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![0, 1, 2]);

        assert!(set.insert(4));
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    }

    #[cfg(feature = "multimap")]
    #[test]
    fn test_remove_where_deletes_the_position_found_under_the_node_lock() {
        let set = BTreeSet::<RandomMultiPair<usize, &'static str>>::new();
        set.attach_node(vec![
            RandomMultiPair {
                key: 1,
                value: "target",
                discriminator: 100,
            },
            RandomMultiPair {
                key: 1,
                value: "middle",
                discriminator: 20,
            },
            RandomMultiPair {
                key: 1,
                value: "last",
                discriminator: 30,
            },
        ]);

        let removed = set.remove_where(|pair| pair.value == "target");

        assert_eq!(removed.map(Into::<(usize, &'static str)>::into), Some((1, "target")));
        assert_eq!(
            set.iter().map(|pair| pair.value).collect::<Vec<_>>(),
            vec!["middle", "last"]
        );
    }

    #[cfg(all(feature = "cdc", feature = "multimap"))]
    #[test]
    fn test_remove_where_cdc_removes_empty_node() {
        let set = BTreeSet::<usize>::new();
        set.insert(7);

        let (removed, events) = set.remove_where_cdc(|value| *value == 7);

        assert_eq!(removed, Some(7));
        assert!(set.is_empty());
        assert!(matches!(
            events.as_slice(),
            [
                ChangeEvent::RemoveAt {
                    max_value: 7,
                    value: 7,
                    ..
                },
                ChangeEvent::RemoveNode { max_value: 7, .. }
            ]
        ));
    }

    #[test]
    fn test_remove_multiple_elements() {
        let set = BTreeSet::<i32>::new();
        for i in 0..2048 {
            set.insert(i);
        }
        for i in 0..2048 {
            assert!(set.remove(&i).is_some());
            assert!(!set.contains(&i));
        }
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_remove_non_existent() {
        let set = BTreeSet::<i32>::new();
        set.insert(5);
        assert!(!set.remove(&10).is_some());
        assert!(set.contains(&5));
    }

    #[test]
    fn test_remove_stress() {
        let set = Arc::new(BTreeSet::<i32>::new());
        const NUM_ELEMENTS: i32 = 10000;

        for i in 0..NUM_ELEMENTS {
            set.insert(i);
        }
        assert_eq!(set.len(), NUM_ELEMENTS as usize, "Incorrect size after insertion");

        let num_threads = 8;
        let elements_per_thread = NUM_ELEMENTS / num_threads;
        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let set = Arc::clone(&set);
                thread::spawn(move || {
                    for i in (t * elements_per_thread)..((t + 1) * elements_per_thread) {
                        if i % 2 == 1 {
                            assert!(set.remove(&i).is_some(), "Failed to remove {}", i);
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(set.len(), NUM_ELEMENTS as usize / 2, "Incorrect size after removal");

        for i in 0..NUM_ELEMENTS {
            if i % 2 == 0 {
                assert!(set.contains(&i), "Even number {} should be in the set", i);
            } else {
                assert!(!set.contains(&i), "Odd number {} should not be in the set", i);
            }
        }
    }

    #[test]
    fn test_remove_all_elements() {
        let set = BTreeSet::<i32>::new();
        let n = 2048;

        for i in 0..n {
            set.insert(i);
        }

        for i in 0..n {
            assert!(set.remove(&i).is_some(), "Failed to remove {}", i);
        }

        assert_eq!(set.len(), 0, "Set should be empty");

        for i in 0..n {
            assert!(!set.contains(&i), "Element {} should not be in the set", i);
        }
    }

    #[test]
    fn test_range_edge_cases() {
        let set = BTreeSet::<i32>::with_maximum_node_size(10);
        for i in 0..20 {
            set.insert(i);
        }
        // Nodes are:
        // [0, 1, 2, 3, 4]
        // [5, 6, 7, 8, 9]
        // [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]

        // First value of the node only
        assert_eq!(set.range(0..=0).collect::<Vec<_>>(), vec![&0]);
        assert_eq!(set.range(0..1).collect::<Vec<_>>(), vec![&0]);

        assert_eq!(set.range(5..=5).collect::<Vec<_>>(), vec![&5]);
        assert_eq!(set.range(5..6).collect::<Vec<_>>(), vec![&5]);

        assert_eq!(set.range(10..=10).collect::<Vec<_>>(), vec![&10]);
        assert_eq!(set.range(10..11).collect::<Vec<_>>(), vec![&10]);

        // From first value to middle
        assert_eq!(set.range(0..=3).collect::<Vec<_>>(), vec![&0, &1, &2, &3]);
        assert_eq!(set.range(0..3).collect::<Vec<_>>(), vec![&0, &1, &2]);

        assert_eq!(set.range(5..=8).collect::<Vec<_>>(), vec![&5, &6, &7, &8]);
        assert_eq!(set.range(5..8).collect::<Vec<_>>(), vec![&5, &6, &7]);

        assert_eq!(set.range(10..=13).collect::<Vec<_>>(), vec![&10, &11, &12, &13]);
        assert_eq!(set.range(10..13).collect::<Vec<_>>(), vec![&10, &11, &12]);

        // Last value of the node
        assert_eq!(set.range(4..=4).collect::<Vec<_>>(), vec![&4]);
        assert_eq!(set.range(4..5).collect::<Vec<_>>(), vec![&4]);

        assert_eq!(set.range(9..=9).collect::<Vec<_>>(), vec![&9]);
        assert_eq!(set.range(9..10).collect::<Vec<_>>(), vec![&9]);

        assert_eq!(set.range(19..=19).collect::<Vec<_>>(), vec![&19]);
        assert_eq!(set.range(19..20).collect::<Vec<_>>(), vec![&19]);

        // From middle to last value of the node
        assert_eq!(set.range(17..=19).collect::<Vec<_>>(), vec![&17, &18, &19]);
        assert_eq!(set.range(17..20).collect::<Vec<_>>(), vec![&17, &18, &19]);

        assert_eq!(set.range(7..=9).collect::<Vec<_>>(), vec![&7, &8, &9]);
        assert_eq!(set.range(7..10).collect::<Vec<_>>(), vec![&7, &8, &9]);

        assert_eq!(set.range(2..=4).collect::<Vec<_>>(), vec![&2, &3, &4]);
        assert_eq!(set.range(2..5).collect::<Vec<_>>(), vec![&2, &3, &4]);

        // Full node
        assert_eq!(set.range(0..=4).collect::<Vec<_>>(), vec![&0, &1, &2, &3, &4]);
        assert_eq!(set.range(0..5).collect::<Vec<_>>(), vec![&0, &1, &2, &3, &4]);

        assert_eq!(set.range(5..=9).collect::<Vec<_>>(), vec![&5, &6, &7, &8, &9]);
        assert_eq!(set.range(5..10).collect::<Vec<_>>(), vec![&5, &6, &7, &8, &9]);

        assert_eq!(
            set.range(10..=19).collect::<Vec<_>>(),
            vec![&10, &11, &12, &13, &14, &15, &16, &17, &18, &19]
        );
        assert_eq!(
            set.range(10..20).collect::<Vec<_>>(),
            vec![&10, &11, &12, &13, &14, &15, &16, &17, &18, &19]
        );

        // Node intersection
        assert_eq!(set.range(3..=6).collect::<Vec<_>>(), vec![&3, &4, &5, &6]);
        assert_eq!(set.range(3..7).collect::<Vec<_>>(), vec![&3, &4, &5, &6]);

        assert_eq!(set.range(8..=11).collect::<Vec<_>>(), vec![&8, &9, &10, &11]);
        assert_eq!(set.range(8..12).collect::<Vec<_>>(), vec![&8, &9, &10, &11]);

        // REVERSED

        // First value of the node only
        assert_eq!(set.range(0..=0).rev().collect::<Vec<_>>(), vec![&0]);
        assert_eq!(set.range(0..1).rev().collect::<Vec<_>>(), vec![&0]);

        assert_eq!(set.range(5..=5).rev().collect::<Vec<_>>(), vec![&5]);
        assert_eq!(set.range(5..6).rev().collect::<Vec<_>>(), vec![&5]);

        assert_eq!(set.range(10..=10).rev().collect::<Vec<_>>(), vec![&10]);
        assert_eq!(set.range(10..11).rev().collect::<Vec<_>>(), vec![&10]);

        // From first value to middle
        assert_eq!(set.range(0..=3).rev().collect::<Vec<_>>(), vec![&3, &2, &1, &0]);
        assert_eq!(set.range(0..3).rev().collect::<Vec<_>>(), vec![&2, &1, &0]);

        assert_eq!(set.range(5..=8).rev().collect::<Vec<_>>(), vec![&8, &7, &6, &5]);
        assert_eq!(set.range(5..8).rev().collect::<Vec<_>>(), vec![&7, &6, &5]);

        assert_eq!(set.range(10..=13).rev().collect::<Vec<_>>(), vec![&13, &12, &11, &10]);
        assert_eq!(set.range(10..13).rev().collect::<Vec<_>>(), vec![&12, &11, &10]);

        // Last value of the node
        assert_eq!(set.range(4..=4).rev().collect::<Vec<_>>(), vec![&4]);
        assert_eq!(set.range(4..5).rev().collect::<Vec<_>>(), vec![&4]);

        assert_eq!(set.range(9..=9).rev().collect::<Vec<_>>(), vec![&9]);
        assert_eq!(set.range(9..10).rev().collect::<Vec<_>>(), vec![&9]);

        assert_eq!(set.range(19..=19).rev().collect::<Vec<_>>(), vec![&19]);
        assert_eq!(set.range(19..20).rev().collect::<Vec<_>>(), vec![&19]);

        // From middle to last value of the node
        assert_eq!(set.range(17..=19).rev().collect::<Vec<_>>(), vec![&19, &18, &17]);
        assert_eq!(set.range(17..20).rev().collect::<Vec<_>>(), vec![&19, &18, &17]);

        assert_eq!(set.range(7..=9).rev().collect::<Vec<_>>(), vec![&9, &8, &7]);
        assert_eq!(set.range(7..10).rev().collect::<Vec<_>>(), vec![&9, &8, &7]);

        assert_eq!(set.range(2..=4).rev().collect::<Vec<_>>(), vec![&4, &3, &2]);
        assert_eq!(set.range(2..5).rev().collect::<Vec<_>>(), vec![&4, &3, &2]);

        // Full node
        assert_eq!(set.range(0..=4).rev().collect::<Vec<_>>(), vec![&4, &3, &2, &1, &0]);
        assert_eq!(set.range(0..5).rev().collect::<Vec<_>>(), vec![&4, &3, &2, &1, &0]);

        assert_eq!(set.range(5..=9).rev().collect::<Vec<_>>(), vec![&9, &8, &7, &6, &5]);
        assert_eq!(set.range(5..10).rev().collect::<Vec<_>>(), vec![&9, &8, &7, &6, &5]);

        assert_eq!(
            set.range(10..=19).rev().collect::<Vec<_>>(),
            vec![&19, &18, &17, &16, &15, &14, &13, &12, &11, &10]
        );
        assert_eq!(
            set.range(10..20).rev().collect::<Vec<_>>(),
            vec![&19, &18, &17, &16, &15, &14, &13, &12, &11, &10]
        );

        // Node intersection
        assert_eq!(set.range(3..=6).rev().collect::<Vec<_>>(), vec![&6, &5, &4, &3]);
        assert_eq!(set.range(3..7).rev().collect::<Vec<_>>(), vec![&6, &5, &4, &3]);

        assert_eq!(set.range(8..=11).rev().collect::<Vec<_>>(), vec![&11, &10, &9, &8]);
        assert_eq!(set.range(8..12).rev().collect::<Vec<_>>(), vec![&11, &10, &9, &8]);

        // Non-existent range
        assert!(set.range(20..).collect::<Vec<_>>().is_empty());
        assert!(set.range(..0).collect::<Vec<_>>().is_empty());
        assert!(set.range(20..).rev().collect::<Vec<_>>().is_empty());
        assert!(set.range(..0).rev().collect::<Vec<_>>().is_empty());
    }

    // Builds nodes [0, 10] (key 10), [20, 30] (key 30), [40, 50, 60] (key 60).
    fn three_node_set() -> BTreeSet<u64> {
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in [0u64, 10, 20, 30, 40, 50, 60] {
            set.insert(value);
        }
        assert_eq!(
            set.index.iter().map(|e| *e.key()).collect::<Vec<_>>(),
            vec![10, 30, 60],
            "fixture geometry changed"
        );
        set
    }

    #[test]
    fn forward_scan_repositions_when_current_node_vanishes() {
        let set = three_node_set();

        let mut iter = set.iter();
        assert_eq!(iter.next(), Some(&0));
        assert_eq!(iter.next(), Some(&10));
        assert_eq!(iter.next(), Some(&20));

        // The node the iterator is parked in vanishes from the index, as
        // UpdateMax's remove-then-insert re-key does on every monotonic
        // insert. The scan must reposition, not end.
        set.index.get(&30).expect("fixture entry").remove();

        assert_eq!(iter.next(), Some(&30));
        assert_eq!(iter.next(), Some(&40));
        assert_eq!(iter.next(), Some(&50));
        assert_eq!(iter.next(), Some(&60));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn backward_scan_repositions_when_current_node_vanishes() {
        let set = three_node_set();

        let mut iter = set.iter();
        assert_eq!(iter.next_back(), Some(&60));
        assert_eq!(iter.next_back(), Some(&50));

        set.index.get(&60).expect("fixture entry").remove();

        assert_eq!(iter.next_back(), Some(&40));
        assert_eq!(iter.next_back(), Some(&30));
        assert_eq!(iter.next_back(), Some(&20));
        assert_eq!(iter.next_back(), Some(&10));
        assert_eq!(iter.next_back(), Some(&0));
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn forward_scan_does_not_re_yield_after_split_of_finished_node() {
        // Nodes [0, 10] (key 10) and [20, 30, 40] (key 40), as left behind by
        // a split of [0, 10, 20, 30].
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in [0u64, 10, 20, 30, 40] {
            set.insert(value);
        }

        // An iterator that had already yielded through 20 from the pre-split
        // node and resumes positioned on the lower half: advancing into the
        // upper half must not re-yield 20.
        let iter = Iter {
            tree: &set,
            current_front_node: Some(set.index.front().expect("fixture node").value().clone()),
            current_front_node_guard: None,
            current_front_node_iter: None,
            current_back_node: None,
            current_back_node_guard: None,
            current_back_node_iter: None,
            current_front_value: Some(20),
            current_back_value: None,
            met: false,
        };

        assert_eq!(iter.copied().collect::<Vec<_>>(), vec![30, 40]);
    }

    #[test]
    fn backward_scan_does_not_re_yield_values_from_scanned_range() {
        // A stale-key window: the front node's maximum (35) grew past its
        // index key (10) while the backward scan had already advanced below
        // 30. Advancing into that node must not yield 35 again.
        let set = BTreeSet::<u64>::new();
        set.attach_node(vec![0u64, 10]);
        set.attach_node(vec![30u64, 40]);
        {
            let node = set.index.front().expect("fixture node").value().clone();
            let mut guard = node.lock();
            NodeLike::insert(&mut *guard, 35u64);
        }

        let mut iter = Iter {
            tree: &set,
            current_front_node: None,
            current_front_node_guard: None,
            current_front_node_iter: None,
            current_back_node: Some(set.index.back().expect("fixture node").value().clone()),
            current_back_node_guard: None,
            current_back_node_iter: None,
            current_front_value: None,
            current_back_value: Some(30),
            met: false,
        };

        let mut collected = vec![];
        while let Some(value) = iter.next_back() {
            collected.push(*value);
        }
        assert_eq!(collected, vec![10, 0]);
    }

    #[test]
    fn scans_stay_sorted_and_complete_under_monotonic_insert_churn() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        const BASELINE: u64 = 400;
        const EXTRA: u64 = 2_000;
        const SCAN_BOUND: usize = 10_000;

        // Small nodes: every monotonic insert re-keys the last node and
        // regularly splits it, exercising the reposition paths constantly.
        let set = Arc::new(BTreeSet::<u64>::with_maximum_node_size(8));
        for value in 0..BASELINE {
            set.insert(value);
        }

        let done = Arc::new(AtomicBool::new(false));
        let writer = {
            let set = Arc::clone(&set);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                for value in BASELINE..BASELINE + EXTRA {
                    assert!(set.insert(value));
                }
                done.store(true, AtomicOrdering::Release);
            })
        };

        let mut scans = 0usize;
        loop {
            let forward = set.iter().copied().collect::<Vec<_>>();
            assert!(
                forward.windows(2).all(|pair| pair[0] < pair[1]),
                "forward scan not strictly increasing (duplicate or unordered yield)"
            );
            assert_eq!(
                forward.iter().filter(|value| **value < BASELINE).count() as u64,
                BASELINE,
                "forward scan truncated: baseline keys missing"
            );

            let backward = set.iter().rev().copied().collect::<Vec<_>>();
            assert!(
                backward.windows(2).all(|pair| pair[0] > pair[1]),
                "backward scan not strictly decreasing (duplicate or unordered yield)"
            );
            assert_eq!(
                backward.iter().filter(|value| **value < BASELINE).count() as u64,
                BASELINE,
                "backward scan truncated: baseline keys missing"
            );

            scans += 1;
            if done.load(AtomicOrdering::Acquire) || scans >= SCAN_BOUND {
                break;
            }
        }

        writer.join().unwrap();

        let expected = (0..BASELINE + EXTRA).collect::<Vec<_>>();
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), expected);
        assert_eq!(set.iter().rev().copied().collect::<Vec<_>>(), {
            let mut reversed = expected;
            reversed.reverse();
            reversed
        });
    }

    #[test]
    fn parallel_iter_and_mut() {
        let set = Arc::new(BTreeSet::<i32>::new());
        for i in 0..10_000 {
            set.insert(i);
        }

        let set_clone = Arc::clone(&set);
        let handle = thread::spawn(move || {
            for _ in 0..1000 {
                let mut _sum = 0;
                for &value in set_clone.iter() {
                    _sum += value;
                }
            }
        });

        for i in 10_000..20_000 {
            set.insert(i);
        }
        handle.join().unwrap();
    }
}

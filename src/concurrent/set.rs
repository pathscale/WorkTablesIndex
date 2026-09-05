use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::{borrow::Borrow, sync::Arc};

use crate::cdc::change::ChangeEvent;
use crate::concurrent::operation::*;
use crate::core::constants::DEFAULT_INNER_SIZE;
use crate::core::node::*;

use super::r#ref::Ref;

const ROOT_PUBLICATION_SPIN_LIMIT: usize = 16;

type NodeIndex<T, Node> = BTreeMap<T, Arc<RwLock<Node>>>;

struct RetiredIndex<T, Node>(*mut NodeIndex<T, Node>);

// SAFETY: the pointer is uniquely owned after it has been swapped out of the
// publication slot, and this wrapper exposes no access to the map. Its only
// operation is destruction after the grace period. Dropping the keys and, for
// the final Arc, moving each node into its destructor on that thread is valid
// when both stored types are `Send`; sharing `Node` there is not required.
unsafe impl<T: Send, Node: Send> Send for RetiredIndex<T, Node> {}

impl<T, Node> Drop for RetiredIndex<T, Node> {
    fn drop(&mut self) {
        // SAFETY: this wrapper is created exactly once for a pointer returned
        // by `Box::into_raw`, after that pointer has been atomically unlinked.
        unsafe { drop(Box::from_raw(self.0)) }
    }
}

#[derive(Debug)]
struct PublishedIndex<T, Node> {
    current: AtomicPtr<NodeIndex<T, Node>>,
    domain: ps_reclaim::Domain,
}

impl<T, Node> PublishedIndex<T, Node> {
    fn new() -> Self {
        Self {
            current: AtomicPtr::new(Box::into_raw(Box::new(BTreeMap::new()))),
            domain: ps_reclaim::Domain::new(),
        }
    }
}

impl<T, Node> PublishedIndex<T, Node>
where
    T: Ord + Clone + Send + 'static,
    Node: Send + 'static,
{
    fn publish(&self, index: &NodeIndex<T, Node>) {
        let replacement = Box::into_raw(Box::new(index.clone()));
        let retired = self.current.swap(replacement, Ordering::AcqRel);
        // Keep the pointer's provenance intact while transferring its unique
        // ownership to the retirement callback.
        let retired = RetiredIndex(retired);
        self.domain.retire(move || drop(retired));
        // Topology publication is infrequent (normally one node boundary per
        // 1,024 inserts), so reclaim one expired snapshot on the writer path.
        self.domain.advance_up_to(1);
    }
}

impl<T, Node> Drop for PublishedIndex<T, Node> {
    fn drop(&mut self) {
        let current = *self.current.get_mut();
        // SAFETY: exclusive access proves no reader can load `current`, and it
        // is the one still-linked allocation created by `Box::into_raw`.
        unsafe { drop(Box::from_raw(current)) }
    }
}

#[derive(Debug)]
pub(crate) struct Topology<T, Node> {
    index: RwLock<NodeIndex<T, Node>>,
    published: PublishedIndex<T, Node>,
    // Even values are stable publications; odd values mean a writer may have
    // changed node contents or routing but has not published the new route.
    generation: AtomicU64,
}

impl<T, Node> Topology<T, Node> {
    fn new() -> Self {
        Self {
            index: RwLock::new(BTreeMap::new()),
            published: PublishedIndex::new(),
            generation: AtomicU64::new(0),
        }
    }

    #[inline]
    pub(crate) fn read(&self) -> RwLockReadGuard<'_, NodeIndex<T, Node>> {
        self.index.read()
    }
}

impl<T, Node> Topology<T, Node>
where
    T: Ord + Clone + Send + 'static,
    Node: Send + 'static,
{
    #[inline]
    fn write(&self) -> TopologyWriteGuard<'_, T, Node> {
        let index = self.index.write();
        self.generation.fetch_add(1, Ordering::AcqRel);
        TopologyWriteGuard { topology: self, index }
    }

    #[inline]
    fn try_write(&self) -> Option<TopologyWriteGuard<'_, T, Node>> {
        let index = self.index.try_write()?;
        self.generation.fetch_add(1, Ordering::AcqRel);
        Some(TopologyWriteGuard { topology: self, index })
    }
}

struct TopologyWriteGuard<'a, T, Node>
where
    T: Ord + Clone + Send + 'static,
    Node: Send + 'static,
{
    topology: &'a Topology<T, Node>,
    index: RwLockWriteGuard<'a, NodeIndex<T, Node>>,
}

impl<T, Node> std::ops::Deref for TopologyWriteGuard<'_, T, Node>
where
    T: Ord + Clone + Send + 'static,
    Node: Send + 'static,
{
    type Target = NodeIndex<T, Node>;

    fn deref(&self) -> &Self::Target {
        &self.index
    }
}

impl<T, Node> std::ops::DerefMut for TopologyWriteGuard<'_, T, Node>
where
    T: Ord + Clone + Send + 'static,
    Node: Send + 'static,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.index
    }
}

impl<T, Node> Drop for TopologyWriteGuard<'_, T, Node>
where
    T: Ord + Clone + Send + 'static,
    Node: Send + 'static,
{
    fn drop(&mut self) {
        self.topology.published.publish(&self.index);
        self.topology.generation.fetch_add(1, Ordering::Release);
    }
}

// Default identity-adoption hook for replace-on-equality: plain sets and maps
// have no hidden ordering state to carry over. See
// `MultiPairLike::adopt_stored_identity`.
pub(crate) fn no_identity_adoption<T>(_stored: &T, _incoming: &mut T) {}

// `BTreeMap::range::<Q>` requires the borrowed ordering to be identical to
// the stored-key ordering. That is true for ordinary borrowed keys such as
// `String`/`str`, but deliberately false for multimap entries: many distinct
// `(key, value)` entries borrow as the same `key`. Route those lookups by the
// borrowed view explicitly so node maxima sharing one logical key remain
// reachable.
fn first_for_borrowed_bound<'a, T, Q, V>(
    index: &'a BTreeMap<T, V>,
    bound: Bound<&Q>,
    borrow_order_matches: bool,
) -> Option<(&'a T, &'a V)>
where
    T: Ord + Borrow<Q>,
    Q: Ord + ?Sized,
{
    if borrow_order_matches {
        return index.range::<Q, _>((bound, Bound::Unbounded)).next();
    }

    index.iter().find(|(key, _)| match bound {
        Bound::Included(value) => <T as Borrow<Q>>::borrow(key) >= value,
        Bound::Excluded(value) => <T as Borrow<Q>>::borrow(key) > value,
        Bound::Unbounded => true,
    })
}

fn node_for_borrowed_end<'a, T, Q, V>(
    index: &'a BTreeMap<T, V>,
    end: &Q,
    borrow_order_matches: bool,
) -> Option<(&'a T, &'a V)>
where
    T: Ord + Borrow<Q>,
    Q: Ord + ?Sized,
{
    if borrow_order_matches {
        return index
            .range::<Q, _>((Bound::Included(end), Bound::Unbounded))
            .next()
            .or_else(|| index.last_key_value());
    }

    let mut last_equal = None;
    for entry @ (key, _) in index {
        match <T as Borrow<Q>>::borrow(key).cmp(end) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => last_equal = Some(entry),
            // The first node whose maximum is above the end may still start
            // with values inside the range. `Range::new` ranks within that
            // node to obtain the first out-of-range sentinel.
            std::cmp::Ordering::Greater => return Some(entry),
        }
    }
    last_equal.or_else(|| index.last_key_value())
}

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
    // Writers maintain the canonical ordered topology under one structural
    // lock and publish immutable snapshots for point reads. The read path is
    // therefore free of a shared reader-count cache line. Node contents use
    // independent read/write locks, so readers routed to one node may proceed
    // concurrently while mutations retain exclusive node access.
    pub(crate) index: Topology<T, Node>,
    node_capacity: usize,
    // Ordinary set/map keys satisfy Borrow's ordering contract and retain a
    // logarithmic BTreeMap route. Multimap entries intentionally borrow only
    // their leading key, so equal borrowed-key groups need an explicit scan.
    borrow_order_matches: bool,
    #[cfg(feature = "cdc")]
    // The counter provides unique sequence numbers only. Node/global locks
    // order conflicting mutations, and the persistence queue publishes event
    // payloads, so the counter itself does not carry memory visibility.
    event_id: AtomicU64,
}
impl<T: Ord + Clone + 'static, Node: NodeLike<T>> Default for BTreeSet<T, Node> {
    fn default() -> Self {
        Self {
            index: Topology::new(),
            node_capacity: DEFAULT_INNER_SIZE,
            borrow_order_matches: true,
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
            index: Topology::new(),
            node_capacity,
            borrow_order_matches: true,
            #[cfg(feature = "cdc")]
            event_id: AtomicU64::new(0),
        }
    }
    pub(crate) fn with_grouped_borrow_routing(mut self) -> Self {
        self.borrow_order_matches = false;
        self
    }
    pub fn attach_node(&self, node: Node) {
        let node_id = node
            .max()
            .cloned()
            .expect("node should contain at least one value to be correct node");
        self.index.write().insert(node_id, Arc::new(RwLock::new(node)));
    }

    #[cfg(feature = "cdc")]
    pub(crate) fn export_topology(&self) -> (usize, Vec<Vec<T>>) {
        let index = self.index.read();
        let nodes = index
            .values()
            .map(|node| node.read().iter().cloned().collect())
            .collect();
        (self.node_capacity, nodes)
    }

    #[allow(clippy::type_complexity)]
    // Const specialization keeps ordinary writes free of CDC event construction
    // even when the crate is compiled with the `cdc` feature.
    fn put_checked_inner<const EMIT_CDC: bool>(
        &self,
        value: T,
        adopt: fn(&T, &mut T),
    ) -> Result<(Option<T>, Vec<ChangeEvent<T>>), (ArcRwLockWriteGuard<RawRwLock, Node>, usize, T)> {
        loop {
            let mut cdc = vec![];
            let index = self.index.read();
            let target_node_entry = match index.range(value.clone()..).next() {
                Some(entry) => entry,
                None => {
                    if let Some(last) = index.last_key_value() {
                        last
                    } else {
                        drop(index);
                        let mut spins = 0;
                        let mut index = loop {
                            if let Some(guard) = self.index.try_write() {
                                break guard;
                            }
                            if spins >= ROOT_PUBLICATION_SPIN_LIMIT {
                                // A bounded block gives root publication a
                                // deterministic progress path under reader
                                // contention instead of livelocking.
                                break self.index.write();
                            }
                            spins += 1;
                            std::hint::spin_loop();
                        };
                        // Another first writer may have published while this
                        // caller was acquiring the exclusive structural guard.
                        if !index.is_empty() {
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

                        index.insert(value, Arc::new(RwLock::new(first_node)));

                        return Ok((None, cdc));
                    }
                }
            };

            let mut node_guard = target_node_entry.1.clone().write_arc();

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
                        target_node_entry.1.clone(),
                        target_node_entry.0.clone(),
                    ));
                } else {
                    return Err((node_guard, idx, old_max.unwrap()));
                }
            } else {
                operation = Some(Operation::Split(
                    target_node_entry.1.clone(),
                    target_node_entry.0.clone(),
                    value.clone(),
                ));
            }

            drop(node_guard);
            drop(index);

            let mut index = self.index.write();

            let op = operation.unwrap();
            match &op {
                Operation::Split(_, _, _) => {
                    if let Ok((value, value_cdc)) = op.commit::<EMIT_CDC>(&mut index, adopt) {
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
                    return if let Ok((value, value_cdc)) = op.commit::<EMIT_CDC>(&mut index, adopt) {
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
    fn put_inner<const EMIT_CDC: bool>(&self, value: T, adopt: fn(&T, &mut T)) -> (Option<T>, Vec<ChangeEvent<T>>) {
        match self.put_checked_inner::<EMIT_CDC>(value.clone(), adopt) {
            Ok(res) => res,
            Err((mut node_guard, idx, max)) => {
                // Replace-on-logical-equality: let the incoming value adopt
                // the stored value's hidden ordering state before it takes
                // the stored position (see MultiPairLike::adopt_stored_identity).
                let mut value = value;
                if let Some(stored) = node_guard.get_ith(idx) {
                    adopt(stored, &mut value);
                }
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
        self.put_inner::<false>(value, no_identity_adoption).0
    }

    #[cfg(feature = "multimap")]
    pub(crate) fn put_with(&self, value: T, adopt: fn(&T, &mut T)) -> Option<T> {
        self.put_inner::<false>(value, adopt).0
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn put_checked(
        &self,
        value: T,
    ) -> Result<(Option<T>, Vec<ChangeEvent<T>>), (ArcRwLockWriteGuard<RawRwLock, Node>, usize, T)> {
        self.put_checked_inner::<false>(value, no_identity_adoption)
    }

    pub(crate) fn put_cdc(&self, value: T) -> (Option<T>, Vec<ChangeEvent<T>>) {
        self.put_inner::<true>(value, no_identity_adoption)
    }

    #[cfg(all(feature = "multimap", feature = "cdc"))]
    pub(crate) fn put_cdc_with(&self, value: T, adopt: fn(&T, &mut T)) -> (Option<T>, Vec<ChangeEvent<T>>) {
        self.put_inner::<true>(value, adopt)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn put_cdc_checked(
        &self,
        value: T,
    ) -> Result<(Option<T>, Vec<ChangeEvent<T>>), (ArcRwLockWriteGuard<RawRwLock, Node>, usize, T)> {
        self.put_checked_inner::<true>(value, no_identity_adoption)
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
        let index = self.index.read();
        // Fall back to the last node when the value sorts above every index
        // key, exactly like `put` and `lock_node_for_value`: during a stale-key
        // window (a node whose maximum grew before its UpdateMax repair
        // committed) the value lives in the last node even though no index key
        // covers it. Without the fallback such a value is un-removable while
        // `contains` still finds it.
        if let Some(target_node_entry) =
            first_for_borrowed_bound(&index, Bound::Included(value), self.borrow_order_matches)
                .or_else(|| index.last_key_value())
        {
            let mut node_guard = target_node_entry.1.clone().write_arc();
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
                    target_node_entry.1.clone(),
                    target_node_entry.0.clone(),
                ))
            } else {
                Some(Operation::MakeUnreachable(
                    target_node_entry.1.clone(),
                    target_node_entry.0.clone(),
                ))
            };

            drop(node_guard);
            drop(index);

            let mut index = self.index.write();

            return if let Ok((_, value_cdc)) = operation.unwrap().commit::<EMIT_CDC>(&mut index, no_identity_adoption) {
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
    #[inline(always)]
    fn lock_node_for_value_optimistic<Q>(&self, value: &Q) -> Option<ArcRwLockReadGuard<RawRwLock, Node>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let node = {
            let index = self.index.read();
            match first_for_borrowed_bound(&index, Bound::Included(value), self.borrow_order_matches) {
                Some((_, node)) => Some(node.clone()),
                None => index
                    .last_key_value()
                    .map(|(_, node)| node.clone())
                    .or_else(|| index.first_key_value().map(|(_, node)| node.clone())),
            }
        }?;
        Some(node.read_arc())
    }

    /// Locates and locks the node whose structural range owns `value`.
    ///
    /// Readers route through an immutable published topology and validate its
    /// generation after locking the node. A concurrent structural change makes
    /// the read retry, so hits and misses remain definitive without updating a
    /// shared reader-count cache line.
    #[inline(always)]
    fn lock_node_for_value<Q>(&self, value: &Q) -> Option<ArcRwLockReadGuard<RawRwLock, Node>>
    where
        T: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        loop {
            let generation = self.index.generation.load(Ordering::Acquire);
            if !generation.is_multiple_of(2) {
                std::hint::spin_loop();
                continue;
            }

            let pin = self.index.published.domain.pin();
            let snapshot = self.index.published.current.load(Ordering::Acquire);
            // SAFETY: `snapshot` was loaded after `pin`, and the publication
            // domain cannot reclaim it until `pin` is dropped.
            let index = unsafe { &*snapshot };
            let node = match first_for_borrowed_bound(index, Bound::Included(value), self.borrow_order_matches) {
                Some((_, node)) => Some(node.clone()),
                None => index
                    .last_key_value()
                    .map(|(_, node)| node.clone())
                    .or_else(|| index.first_key_value().map(|(_, node)| node.clone())),
            };
            let Some(node) = node else {
                if self.index.generation.load(Ordering::Acquire) == generation {
                    return None;
                }
                continue;
            };

            let node_guard = node.read_arc();
            if self.index.generation.load(Ordering::Acquire) == generation {
                drop(pin);
                return Some(node_guard);
            }
        }
    }

    #[inline(always)]
    fn get_with_guard<Q, R>(
        node_guard: ArcRwLockReadGuard<RawRwLock, Node>,
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
        loop {
            let generation = self.index.generation.load(Ordering::Acquire);
            if !generation.is_multiple_of(2) {
                std::hint::spin_loop();
                continue;
            }

            let _pin = self.index.published.domain.pin();
            let snapshot = self.index.published.current.load(Ordering::Acquire);
            // SAFETY: `snapshot` was loaded after `pin`, and the publication
            // domain cannot reclaim it until `pin` is dropped.
            let index = unsafe { &*snapshot };
            let node = first_for_borrowed_bound(index, Bound::Included(value), self.borrow_order_matches)
                .or_else(|| index.last_key_value())
                .or_else(|| index.first_key_value())
                .map(|(_, node)| node);
            let Some(node) = node else {
                if self.index.generation.load(Ordering::Acquire) == generation {
                    return None;
                }
                continue;
            };

            // Borrow the Arc from the protected snapshot: unlike
            // `lock_node_for_value`, this owned-result path does not need an
            // Arc clone or its shared refcount update.
            let node_guard = node.read();
            if self.index.generation.load(Ordering::Acquire) != generation {
                continue;
            }
            let position = node_guard.try_select(value)?;
            return node_guard.get_ith(position).map(read);
        }
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
        self.get_with(value, |_| ()).is_some()
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
        self.index.read().values().map(|node| node.read().len()).sum()
    }
    pub fn is_empty(&self) -> bool {
        self.index.read().values().all(|node| node.read().is_empty())
    }
    pub fn capacity(&self) -> usize {
        self.index
            .read()
            .values()
            .map(|node| {
                let guard = node.read();
                guard.capacity()
            })
            .sum()
    }
    pub fn node_count(&self) -> usize {
        self.index.read().len()
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

/// An owned-yield iterator over a concurrent `BTreeSet`.
///
/// The iterator clones one node's remaining elements into an owned batch
/// while holding that node's mutex, releases the mutex, and then yields the
/// clones. No node lock and no structural lock is ever held between calls to
/// `next`/`next_back`, and every yielded `T` is an independent clone: items
/// collected from this iterator stay valid under arbitrary concurrent
/// mutation of the set.
///
/// The scan is weakly consistent, exactly like iterating any concurrent
/// collection: elements inserted or removed while the scan is in flight may
/// or may not be observed, but elements present for the whole scan are
/// yielded exactly once, in order.
pub struct Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    tree: &'a BTreeSet<T, Node>,
    current_front_batch: Option<std::vec::IntoIter<T>>,
    current_back_batch: Option<std::vec::IntoIter<T>>,
    // Identity of the node the last batch in each direction was cloned from,
    // so the next install can step past it when the cursor lookup lands on
    // it again (its entry key can sit past every element it still holds).
    exhausted_front_node: Option<Arc<RwLock<Node>>>,
    exhausted_back_node: Option<Arc<RwLock<Node>>>,
    // The node a direction is partway through, and how many of its elements it
    // has taken. A batch that stops short of a node's end must resume inside
    // that node, and must never take less than it already has: the rank-based
    // skip alone cannot guarantee that, because a repositioned node can leave
    // the cursor ranking below elements already yielded. Recording the count
    // makes forward progress structural rather than incidental.
    front_partial: Option<(Arc<RwLock<Node>>, usize)>,
    back_partial: Option<(Arc<RwLock<Node>>, usize)>,
    // How many elements the next batch may clone, doubling per install.
    front_batch_limit: usize,
    back_batch_limit: usize,
    current_front_value: Option<T>,
    current_back_value: Option<T>,
    met: bool,
}

/// Elements the first batch of a scan clones.
///
/// A one-element range is the common case and it used to clone a whole node to
/// yield one value. Starting small makes that cost proportional to what is
/// asked for; doubling means a real scan reaches whole-node batches after a
/// handful of installs and pays the same total clone count it always did.
const INITIAL_BATCH: usize = 4;

/// Ceiling on the growth. `available` bounds a batch to what the node holds, so
/// this only stops the doubling running away on a very long scan.
const MAX_BATCH: usize = 4096;

impl<'a, T, Node> Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    pub fn new(btree: &'a BTreeSet<T, Node>) -> Self {
        // No node is chosen here: each direction positions itself from its
        // cursor when it installs a batch, atomically with reading the node.
        // Choosing a node ahead of time and locking it later reintroduces
        // the split-migration window closed in `install_front_batch`.
        Self {
            tree: btree,
            current_front_batch: None,
            current_back_batch: None,
            exhausted_front_node: None,
            exhausted_back_node: None,
            front_partial: None,
            back_partial: None,
            front_batch_limit: INITIAL_BATCH,
            back_batch_limit: INITIAL_BATCH,
            current_front_value: None,
            current_back_value: None,
            met: false,
        }
    }

    // Select the node covering the forward cursor and clone its remaining
    // elements (strictly above the cursor) into an owned batch. Returns
    // false when no node remains and the scan is complete.
    //
    // Selection and the content read are ONE atomic step: the node is
    // locked while the structural read guard is still held. Both split
    // commits and re-keys take the structural write lock and the node lock,
    // so the chosen node cannot change contents or move between being
    // chosen and being read. Choosing under one guard and locking later
    // allowed a split to migrate not-yet-yielded elements into a new node
    // past the resume point, silently truncating the scan (deterministically
    // reproduced by `backward_scan_does_not_skip_values_split_away_after_positioning`).
    //
    // Lock order is topology then node, the same order every writer
    // uses, so this cannot deadlock ABBA against committers; holding the
    // node mutex alone pins its contents, so the structural guard is
    // released before the clone. No lock is held once this returns.
    //
    // One batched clone per node replaces the old per-item guard-holding
    // scheme: the previous design kept the node mutex alive inside the
    // iterator and transmuted the guard's slice iterator to the iterator's
    // lifetime, which let callers hold `&T` into node storage after the
    // guard was released (a use-after-free under concurrent mutation).
    // Owned batches make that impossible by construction.
    fn install_front_batch(&mut self) -> bool {
        let index = self.tree.index.read();
        let candidate = match self.current_front_value.as_ref() {
            Some(last_yielded) => index.range((Bound::Excluded(last_yielded), Bound::Unbounded)).next(),
            None => index.first_key_value(),
        };
        // Advance by the scan cursor with one logarithmic lookup: it lands
        // on whichever node now covers the cursor even after re-keys or
        // removals, and the yield-path filters skip anything already
        // yielded. Step past the just-exhausted node by identity so the
        // scan always makes progress.
        let entry = match (candidate, self.exhausted_front_node.as_ref()) {
            (Some((key, node)), Some(exhausted)) if Arc::ptr_eq(node, exhausted) => {
                index.range((Bound::Excluded(key), Bound::Unbounded)).next()
            }
            (candidate, _) => candidate,
        };
        let Some((_, entry)) = entry else {
            return false;
        };
        let node = entry.clone();
        let guard = node.read_arc();
        drop(index);

        let rank_skip = self
            .current_front_value
            .as_ref()
            .and_then(|value| guard.rank(Bound::Excluded(value), true))
            .map_or(0, |rank| rank + 1);
        // Resuming a node this scan is partway through. `front_partial` counts
        // *positions*, and a position is not a stable cursor: deleting an
        // element this scan already yielded shifts the unyielded tail left
        // while the count stays put. Letting it win a `max` against the value
        // rank steps over an element that was present for the whole scan,
        // which is the one thing this iterator promises not to do. See
        // `deleting_a_yielded_element_does_not_skip_a_live_one`.
        //
        // The value rank is authoritative wherever there is one: it counts the
        // elements at or below the last yielded value, so the batch resumes
        // strictly above the cursor. No duplicates, and progress every time.
        // That is also what makes dropping the `max` safe -- the
        // non-termination it guarded against was a batch that came back all
        // duplicates and advanced nothing, and a value-ranked resume cannot
        // produce one.
        //
        // The position still matters before anything has been yielded, where
        // there is no value to rank against and it is the only record that this
        // node was already drawn from.
        let partial_skip = match self.front_partial.as_ref() {
            Some((partial, taken)) if Arc::ptr_eq(partial, &node) => *taken,
            _ => 0,
        };
        let skip = if self.current_front_value.is_some() {
            rank_skip
        } else {
            partial_skip
        };

        // Clone what was asked for rather than the rest of the node. A range
        // that yields one value used to clone every remaining element of the
        // node it landed in, which is where the 2.6x cost of this path came
        // from; the owned batch is what makes the iterator sound, but nothing
        // about that soundness required cloning eagerly.
        let available = guard.len().saturating_sub(skip);
        let take = available.min(self.front_batch_limit);
        let batch = guard.iter().skip(skip).take(take).cloned().collect::<Vec<_>>();
        drop(guard);

        if take == available {
            // The node is finished, so the next install must step past it.
            self.exhausted_front_node = Some(node);
            self.front_partial = None;
        } else {
            // More of this node remains: resume inside it rather than stepping
            // past, and remember how far in.
            self.exhausted_front_node = None;
            self.front_partial = Some((node, skip + take));
        }
        self.front_batch_limit = self.front_batch_limit.saturating_mul(2).min(MAX_BATCH);
        self.current_front_batch = Some(batch.into_iter());
        true
    }

    // Mirror of `install_front_batch` for the backward cursor: select the
    // node covering the cursor (the last node when every entry key sits
    // below it) and clone the elements strictly below the cursor.
    fn install_back_batch(&mut self) -> bool {
        let index = self.tree.index.read();
        let candidate = match self.current_back_value.as_ref() {
            Some(last_yielded) => index
                .range((Bound::Included(last_yielded), Bound::Unbounded))
                .next()
                .or_else(|| index.last_key_value()),
            None => index.last_key_value(),
        };
        let entry = match (candidate, self.exhausted_back_node.as_ref()) {
            (Some((key, node)), Some(exhausted)) if Arc::ptr_eq(node, exhausted) => index.range(..key).next_back(),
            (candidate, _) => candidate,
        };
        let Some((_, entry)) = entry else {
            return false;
        };
        let node = entry.clone();
        let guard = node.read_arc();
        drop(index);

        let truncate = self
            .current_back_value
            .as_ref()
            .and_then(|value| guard.rank(Bound::Excluded(value), false))
            .map_or(0, |rank| rank + 1);
        // Mirror of the forward cursor's partial resume, defect included:
        // walking backwards the count already taken is trimmed from the end
        // rather than skipped at the start, and removing an already-yielded
        // high element shifts the unyielded head right while the count stays
        // put. The value rank wins here for the same reason it wins there. See
        // `deleting_a_yielded_element_backwards_does_not_skip_a_live_one`.
        let partial_truncate = match self.back_partial.as_ref() {
            Some((partial, taken)) if Arc::ptr_eq(partial, &node) => *taken,
            _ => 0,
        };
        let truncate = if self.current_back_value.is_some() {
            truncate
        } else {
            partial_truncate
        };
        let available = guard.len().saturating_sub(truncate);
        let take = available.min(self.back_batch_limit);
        // The backward batch is the last `take` of what remains, so the skip is
        // whatever sits below it.
        let skip = available - take;
        let batch = guard.iter().skip(skip).take(take).cloned().collect::<Vec<_>>();
        drop(guard);

        if take == available {
            self.exhausted_back_node = Some(node);
            self.back_partial = None;
        } else {
            self.exhausted_back_node = None;
            self.back_partial = Some((node, truncate + take));
        }
        self.back_batch_limit = self.back_batch_limit.saturating_mul(2).min(MAX_BATCH);
        self.current_back_batch = Some(batch.into_iter());
        true
    }
}

impl<'a, T, Node> Iterator for Iter<'a, T, Node>
where
    T: Debug + Ord + Clone + Send + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.met {
                return None;
            }

            if self.current_front_batch.is_none() && !self.install_front_batch() {
                return None;
            }

            let batch = self.current_front_batch.as_mut().expect("installed above");
            if let Some(value) = batch.next() {
                // A batch installed after repositioning can re-expose
                // elements at or below the last yielded value (a split
                // re-distributes the just-finished node, a repositioned node
                // covers part of the scanned range). Skip them instead of
                // yielding duplicates.
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
                self.current_front_batch = None;
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

            if self.current_back_batch.is_none() && !self.install_back_batch() {
                return None;
            }

            let batch = self.current_back_batch.as_mut().expect("installed above");
            if let Some(value) = batch.next_back() {
                // Mirror of the forward path: skip elements at or above the
                // last value yielded from the back, which a freshly
                // installed batch can re-expose after churn.
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
                self.current_back_batch = None;
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
    type Item = T;

    type IntoIter = Iter<'a, T, Node>;

    fn into_iter(self) -> Self::IntoIter {
        Iter::new(self)
    }
}

/// An owned-yield double-ended range iterator; see [`Iter`] for the
/// consistency and cloning semantics.
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
        let index = btree.index.read();

        let start_bound = range.start_bound();
        let end_bound = range.end_bound();
        let mut met = match (start_bound, end_bound) {
            (Bound::Included(start), Bound::Included(end)) => start > end,
            (Bound::Included(start), Bound::Excluded(end))
            | (Bound::Excluded(start), Bound::Included(end))
            | (Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
            _ => false,
        };

        let current_front_entry = first_for_borrowed_bound(&index, start_bound, btree.borrow_order_matches);

        let front_value = if let Some((front_key, front_node)) = current_front_entry {
            let front_guard = front_node.clone().read_arc();
            let rank = match start_bound {
                Bound::Included(v) => front_guard.rank(Bound::Included(v), true),
                Bound::Excluded(v) => front_guard.rank(Bound::Excluded(v), true),
                Bound::Unbounded => None,
            };
            if let Some(rank) = rank {
                let value = front_guard.iter().nth(rank).cloned();
                drop(front_guard);

                value
            } else {
                // Release the current node before locking its neighbor: this
                // branch used to hold front then prev (descending) while the
                // back branch below held back then next (ascending), an
                // ABBA deadlock between two concurrent Range constructions.
                // Never hold two node locks here.
                drop(front_guard);
                if let Some((_, pre_front_node)) = index.range::<T, _>(..front_key).next_back() {
                    let pre_front_guard = pre_front_node.clone().read_arc();
                    pre_front_guard.iter().last().cloned()
                } else {
                    None
                }
            }
        } else {
            None
        };

        let current_back_entry = match end_bound {
            Bound::Included(end) | Bound::Excluded(end) => {
                node_for_borrowed_end(&index, end, btree.borrow_order_matches)
            }
            Bound::Unbounded => index.last_key_value(),
        };

        let back_value = if let Some((back_key, back_node)) = current_back_entry {
            let back_guard = back_node.clone().read_arc();
            let rank = match end_bound {
                Bound::Included(v) => back_guard.rank(Bound::Included(v), false),
                Bound::Excluded(v) => back_guard.rank(Bound::Excluded(v), false),
                Bound::Unbounded => None,
            };
            if let Some(rank) = rank {
                let value = back_guard.iter().nth_back(rank).cloned();
                drop(back_guard);

                value
            } else {
                // See the front branch: release before locking the neighbor.
                drop(back_guard);
                if let Some((_, next_back_node)) = index
                    .range::<T, _>((Bound::Excluded(back_key), Bound::Unbounded))
                    .next()
                {
                    let next_back_guard = next_back_node.clone().read_arc();
                    next_back_guard.iter().next().cloned()
                } else {
                    None
                }
            }
        } else {
            None
        };

        if front_value.is_none() && back_value.is_none() {
            // in this case we iter full or no iter at all
            if start_bound != Bound::Unbounded || end_bound != Bound::Unbounded {
                if let Some(max) = index
                    .last_key_value()
                    .and_then(|(_, node)| node.clone().read_arc().max().cloned())
                {
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

                if let Some(min) = index
                    .first_key_value()
                    .and_then(|(_, node)| node.clone().read_arc().min().cloned())
                {
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

        // Only the cursor sentinels position the iterator: each direction
        // selects and reads its node atomically at install time. Prewiring
        // the entries' node Arcs here would reintroduce the choose-then-lock
        // split-migration window (see `Iter::install_front_batch`).
        Self {
            iter: Iter {
                tree: btree,
                current_front_batch: None,
                current_back_batch: None,
                exhausted_front_node: None,
                exhausted_back_node: None,
                front_partial: None,
                back_partial: None,
                front_batch_limit: INITIAL_BATCH,
                back_batch_limit: INITIAL_BATCH,
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
    type Item = T;

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
    /// The iterator yields owned clones of the stored elements (see [`Iter`]):
    /// collected values remain valid under arbitrary concurrent mutation of
    /// the set.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::set::BTreeSet;
    ///
    /// let set = BTreeSet::from_iter([1, 2, 3]);
    /// let mut set_iter = set.iter();
    /// assert_eq!(set_iter.next(), Some(1));
    /// assert_eq!(set_iter.next(), Some(2));
    /// assert_eq!(set_iter.next(), Some(3));
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
    /// assert_eq!(set_iter.next(), Some(1));
    /// assert_eq!(set_iter.next(), Some(2));
    /// assert_eq!(set_iter.next(), Some(3));
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
    /// for elem in set.range((Included(&4), Included(&8))) {
    ///     println!("{elem}");
    /// }
    /// assert_eq!(Some(5), set.range(4..).next());
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
        let mut index = self.index.write();

        let start_bound = range.start_bound();
        let end_bound = range.end_bound();

        // First node that can contain an element within the start bound. If
        // no index key reaches the start bound, nothing qualifies.
        let Some((front_key, front_node)) = first_for_borrowed_bound(&index, start_bound, self.borrow_order_matches)
        else {
            return;
        };
        let front_key = front_key.clone();
        let front_node = front_node.clone();

        // Last node that can contain an element within the end bound. Both an
        // inclusive and an exclusive end resolve to the first node whose key
        // is >= the bound value: a node keyed exactly at an exclusive bound
        // still holds elements below it. Past the last key, the last node is
        // the only candidate.
        let back_entry = match end_bound {
            Bound::Included(end) | Bound::Excluded(end) => {
                node_for_borrowed_end(&index, end, self.borrow_order_matches)
            }
            Bound::Unbounded => index.last_key_value(),
        };
        let Some((back_key, back_node)) = back_entry else {
            return;
        };
        let back_key = back_key.clone();
        let back_node = back_node.clone();
        if back_key < front_key {
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

        if Arc::ptr_eq(&front_node, &back_node) {
            // The whole range lives in one node.
            let mut guard = front_node.clone().write_arc();
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
                index.remove::<T>(&front_key);
                if let Some(new_max) = guard.last().cloned() {
                    index.insert(new_max, front_node);
                }
            }
            return;
        }

        let mut front_guard = front_node.clone().write_arc();
        let mut back_guard = back_node.clone().write_arc();
        let front_position = front_guard.rank(start_bound, true).map_or(0, |last| last + 1);
        let back_position = removed_prefix_len(&back_guard);

        // Remove every node strictly between the front and the back one.
        let middle_keys = index
            .range::<T, _>((Bound::Excluded(&front_key), Bound::Excluded(&back_key)))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in middle_keys {
            let node = index
                .remove::<T>(&key)
                .expect("middle key was collected under the write lock");
            let mut removed_node = node.write_arc();
            detached_nodes.push(std::mem::take(&mut *removed_node));
        }

        // Trim the front node from the start position: its maximum goes away,
        // so its entry must be re-keyed (or dropped when the node empties).
        index.remove::<T>(&front_key);
        front_guard.drain(front_position..);
        if !front_guard.is_empty() {
            let new_front_max = front_guard.last().unwrap().clone();
            index.insert(new_front_max, front_node);
        }

        // Trim the back node's prefix: its maximum survives unless the whole
        // node drains, so the entry only changes when the node empties.
        if back_position >= back_guard.len() {
            index.remove::<T>(&back_key);
            back_guard.drain(..);
        } else if back_position > 0 {
            back_guard.drain(..back_position);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::concurrent::operation::Operation;
    use crate::concurrent::set::{BTreeSet, Iter, DEFAULT_INNER_SIZE, INITIAL_BATCH};
    use crate::core::node::NodeLike;
    use rand::Rng;
    use std::collections::HashSet;
    use std::ops::Bound::{self, Included};
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

        assert_eq!(set.iter().collect::<Vec<_>>(), (0..10).collect::<Vec<_>>());
    }

    // Regression for https://github.com/lucidarium-systems/indexset/issues/57.
    #[test]
    fn test_node_size_three_preserves_all_u8_values() {
        let set = BTreeSet::<u8>::with_maximum_node_size(3);

        for value in 0..20_u8 {
            set.insert(value);
        }

        assert_eq!(set.iter().collect::<Vec<_>>(), (0..20).collect::<Vec<_>>());
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
        assert_eq!(set.iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn published_point_reads_remain_definitive_across_splits() {
        use std::sync::atomic::{AtomicBool, Ordering};

        const STABLE_KEYS: usize = 256;
        const FINAL_KEYS: usize = 4_096;
        const READERS: usize = 4;

        let set = Arc::new(BTreeSet::<usize>::with_maximum_node_size(8));
        for key in 0..STABLE_KEYS {
            set.insert(key);
        }

        let start = Arc::new(Barrier::new(READERS + 1));
        let done = Arc::new(AtomicBool::new(false));
        let readers = (0..READERS)
            .map(|reader| {
                let set = Arc::clone(&set);
                let start = Arc::clone(&start);
                let done = Arc::clone(&done);
                thread::spawn(move || {
                    start.wait();
                    let mut probe = reader;
                    while !done.load(Ordering::Acquire) {
                        let key = probe % STABLE_KEYS;
                        assert_eq!(set.get_with(&key, |value| *value), Some(key));
                        probe += READERS;
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        for key in STABLE_KEYS..FINAL_KEYS {
            set.insert(key);
        }
        done.store(true, Ordering::Release);

        for reader in readers {
            reader.join().unwrap();
        }
        assert_eq!(set.len(), FINAL_KEYS);
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
                .read()
                .values()
                .flat_map(|node| node.read().iter().cloned().collect::<Vec<_>>())
                .collect::<HashSet<_>>()
                .symmetric_difference(&inserted_values)
                .collect::<Vec<_>>()
        );
        for i in inserted_values {
            assert!(
                set.contains(&i),
                "Did not find: {} with index: {:?}",
                i,
                set.index.read().keys().cloned().collect::<Vec<_>>(),
            );
        }
    }

    #[test]
    fn test_single_element() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        let mut iter = set.into_iter();
        assert_eq!(iter.next(), Some(1));
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
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next_back(), Some(3));
        assert_eq!(iter.next(), Some(2));
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
            let tree = set.index.read().keys().cloned().collect::<Vec<_>>();

            let expected_next = i + 1;
            let actual_next = iter.next();
            assert_eq!(actual_next, Some(expected_next), "Tree: {:?}", tree);

            let expected_next_back = 20 - i;
            let actual_next_back = iter.next_back();
            assert_eq!(actual_next_back, Some(expected_next_back), "Tree: {:?}", tree);
        }
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next_back(), None);
    }

    #[test]
    fn test_fused_iterator() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        let mut iter = set.into_iter();
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_fused_iterator_back() {
        let set = BTreeSet::<i32>::new();
        set.insert(1);
        let mut iter = set.into_iter();
        assert_eq!(iter.next_back(), Some(1));
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
        let start = btree.range(0..DEFAULT_INNER_SIZE).into_iter().collect::<Vec<_>>();

        assert_eq!(start, (0..DEFAULT_INNER_SIZE).collect::<Vec<_>>());
        assert_eq!(
            btree.range(0..=DEFAULT_INNER_SIZE).into_iter().collect::<Vec<_>>(),
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
        assert_eq!(set.iter().collect::<Vec<_>>(), (0..7).collect::<Vec<_>>());

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
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![0, 1, 2, 6, 7, 8, 9]);

        // `x..=x` must remove exactly x, not drain to the node end.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in 0..10 {
            set.insert(value);
        }
        set.remove_range(2..=2);
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![0, 1, 3, 4, 5, 6, 7, 8, 9]);

        // An exclusive end equal to a node maximum must not drain the
        // following node.
        let set = BTreeSet::<u64>::with_maximum_node_size(3);
        for value in 0..9 {
            set.insert(value);
        }
        let boundary = *set.index.read().first_key_value().expect("node must exist").0;
        set.remove_range(0..boundary);
        let expected = (0..9).filter(|value| *value >= boundary).collect::<Vec<_>>();
        assert_eq!(set.iter().collect::<Vec<_>>(), expected);
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
                set.iter().collect::<Vec<_>>(),
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

        let detached = set
            .index
            .read()
            .range((Included(&2), Bound::Unbounded))
            .nth(1)
            .unwrap()
            .1
            .clone();
        let detached_values = detached.read().iter().copied().collect::<Vec<_>>();
        assert!(detached_values.iter().all(|value| (2..30).contains(value)));

        set.remove_range(2..30);

        assert!(detached.read().is_empty());
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
            let node = set.index.read().last_key_value().expect("node must exist").1.clone();
            let mut guard = node.write();
            NodeLike::insert(&mut *guard, 5u64);
        }

        assert!(set.contains(&5));
        assert_eq!(set.remove(&5), Some(5), "value above every index key must be removable");
        assert!(!set.contains(&5));
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    // Simulates the first phase of a remove that empties a node: the elements
    // are deleted under the node lock, leaving the index entry with a stale
    // key, and the caller receives the not-yet-committed MakeUnreachable.
    fn drain_node_with_pending_unlink(set: &BTreeSet<u64>, values: &[u64], stale_key: u64) -> Operation<u64, Vec<u64>> {
        let node = set.index.read().last_key_value().expect("node must exist").1.clone();
        {
            let mut guard = node.write();
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
        let node = set.index.read().last_key_value().expect("node must exist").1.clone();
        // A split is scheduled with a pending insert riding on it...
        let pending_split = Operation::Split(node.clone(), 30u64, 15u64);
        // ...then a concurrent remove drains the node before the commit.
        {
            let mut guard = node.write();
            for seeded in [10u64, 20, 30] {
                NodeLike::delete(&mut *guard, &seeded).expect("seeded value must be present");
            }
        }

        // The commit must fail so the insert retries; it must neither drop
        // the pending value silently nor unlink the still-indexed node.
        assert!(pending_split
            .commit::<false>(&mut set.index.write(), super::no_identity_adoption)
            .is_err());
        assert!(
            set.index.read().get(&30).is_some(),
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
        let node = set.index.read().last_key_value().expect("node must exist").1.clone();
        let pending_split = Operation::Split(node.clone(), 30u64, 15u64);
        {
            let mut guard = node.write();
            for seeded in [10u64, 20, 30] {
                NodeLike::delete(&mut *guard, &seeded).expect("seeded value must be present");
            }
        }

        // The cdc-emitting commit used to panic reading the drained node's
        // maximum while holding the structural write lock.
        assert!(pending_split
            .commit::<true>(&mut set.index.write(), super::no_identity_adoption)
            .is_err());
        assert!(set.index.read().get(&30).is_some());

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
            let _ = pending_unlink.commit::<false>(&mut set.index.write(), super::no_identity_adoption);

            assert!(set.contains(&value), "value {value} lost after stale unlink");
            assert_eq!(set.iter().collect::<Vec<_>>(), vec![value]);
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
            let node = set.index.read().last_key_value().expect("node must exist").1.clone();
            let pending_unlink = drain_node_with_pending_unlink(&set, &[10, 20, 30], 30);

            // First phase of a concurrent insert: the value lands in the
            // routed (empty) node under the node lock; the UpdateMax repair
            // has not committed yet.
            {
                let mut guard = node.write();
                NodeLike::insert(&mut *guard, value);
            }
            let pending_repair = Operation::UpdateMax(node.clone(), 30u64);

            // The remove's stale unlink commits first: it must observe the
            // refilled node and re-key it rather than unlink it.
            assert!(pending_unlink
                .commit::<false>(&mut set.index.write(), super::no_identity_adoption)
                .is_ok());
            // The insert's repair then finds the entry already re-keyed.
            let _ = pending_repair.commit::<false>(&mut set.index.write(), super::no_identity_adoption);

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
                            let index = set.index.read();
                            let present = index.values().any(|node| node.read().contains(&key));
                            drop(index);
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
        assert_eq!(set.range(0..=0).collect::<Vec<_>>(), vec![0]);
        assert_eq!(set.range(0..1).collect::<Vec<_>>(), vec![0]);

        assert_eq!(set.range(5..=5).collect::<Vec<_>>(), vec![5]);
        assert_eq!(set.range(5..6).collect::<Vec<_>>(), vec![5]);

        assert_eq!(set.range(10..=10).collect::<Vec<_>>(), vec![10]);
        assert_eq!(set.range(10..11).collect::<Vec<_>>(), vec![10]);

        // From first value to middle
        assert_eq!(set.range(0..=3).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        assert_eq!(set.range(0..3).collect::<Vec<_>>(), vec![0, 1, 2]);

        assert_eq!(set.range(5..=8).collect::<Vec<_>>(), vec![5, 6, 7, 8]);
        assert_eq!(set.range(5..8).collect::<Vec<_>>(), vec![5, 6, 7]);

        assert_eq!(set.range(10..=13).collect::<Vec<_>>(), vec![10, 11, 12, 13]);
        assert_eq!(set.range(10..13).collect::<Vec<_>>(), vec![10, 11, 12]);

        // Last value of the node
        assert_eq!(set.range(4..=4).collect::<Vec<_>>(), vec![4]);
        assert_eq!(set.range(4..5).collect::<Vec<_>>(), vec![4]);

        assert_eq!(set.range(9..=9).collect::<Vec<_>>(), vec![9]);
        assert_eq!(set.range(9..10).collect::<Vec<_>>(), vec![9]);

        assert_eq!(set.range(19..=19).collect::<Vec<_>>(), vec![19]);
        assert_eq!(set.range(19..20).collect::<Vec<_>>(), vec![19]);

        // From middle to last value of the node
        assert_eq!(set.range(17..=19).collect::<Vec<_>>(), vec![17, 18, 19]);
        assert_eq!(set.range(17..20).collect::<Vec<_>>(), vec![17, 18, 19]);

        assert_eq!(set.range(7..=9).collect::<Vec<_>>(), vec![7, 8, 9]);
        assert_eq!(set.range(7..10).collect::<Vec<_>>(), vec![7, 8, 9]);

        assert_eq!(set.range(2..=4).collect::<Vec<_>>(), vec![2, 3, 4]);
        assert_eq!(set.range(2..5).collect::<Vec<_>>(), vec![2, 3, 4]);

        // Full node
        assert_eq!(set.range(0..=4).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
        assert_eq!(set.range(0..5).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);

        assert_eq!(set.range(5..=9).collect::<Vec<_>>(), vec![5, 6, 7, 8, 9]);
        assert_eq!(set.range(5..10).collect::<Vec<_>>(), vec![5, 6, 7, 8, 9]);

        assert_eq!(
            set.range(10..=19).collect::<Vec<_>>(),
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );
        assert_eq!(
            set.range(10..20).collect::<Vec<_>>(),
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
        );

        // Node intersection
        assert_eq!(set.range(3..=6).collect::<Vec<_>>(), vec![3, 4, 5, 6]);
        assert_eq!(set.range(3..7).collect::<Vec<_>>(), vec![3, 4, 5, 6]);

        assert_eq!(set.range(8..=11).collect::<Vec<_>>(), vec![8, 9, 10, 11]);
        assert_eq!(set.range(8..12).collect::<Vec<_>>(), vec![8, 9, 10, 11]);

        // REVERSED

        // First value of the node only
        assert_eq!(set.range(0..=0).rev().collect::<Vec<_>>(), vec![0]);
        assert_eq!(set.range(0..1).rev().collect::<Vec<_>>(), vec![0]);

        assert_eq!(set.range(5..=5).rev().collect::<Vec<_>>(), vec![5]);
        assert_eq!(set.range(5..6).rev().collect::<Vec<_>>(), vec![5]);

        assert_eq!(set.range(10..=10).rev().collect::<Vec<_>>(), vec![10]);
        assert_eq!(set.range(10..11).rev().collect::<Vec<_>>(), vec![10]);

        // From first value to middle
        assert_eq!(set.range(0..=3).rev().collect::<Vec<_>>(), vec![3, 2, 1, 0]);
        assert_eq!(set.range(0..3).rev().collect::<Vec<_>>(), vec![2, 1, 0]);

        assert_eq!(set.range(5..=8).rev().collect::<Vec<_>>(), vec![8, 7, 6, 5]);
        assert_eq!(set.range(5..8).rev().collect::<Vec<_>>(), vec![7, 6, 5]);

        assert_eq!(set.range(10..=13).rev().collect::<Vec<_>>(), vec![13, 12, 11, 10]);
        assert_eq!(set.range(10..13).rev().collect::<Vec<_>>(), vec![12, 11, 10]);

        // Last value of the node
        assert_eq!(set.range(4..=4).rev().collect::<Vec<_>>(), vec![4]);
        assert_eq!(set.range(4..5).rev().collect::<Vec<_>>(), vec![4]);

        assert_eq!(set.range(9..=9).rev().collect::<Vec<_>>(), vec![9]);
        assert_eq!(set.range(9..10).rev().collect::<Vec<_>>(), vec![9]);

        assert_eq!(set.range(19..=19).rev().collect::<Vec<_>>(), vec![19]);
        assert_eq!(set.range(19..20).rev().collect::<Vec<_>>(), vec![19]);

        // From middle to last value of the node
        assert_eq!(set.range(17..=19).rev().collect::<Vec<_>>(), vec![19, 18, 17]);
        assert_eq!(set.range(17..20).rev().collect::<Vec<_>>(), vec![19, 18, 17]);

        assert_eq!(set.range(7..=9).rev().collect::<Vec<_>>(), vec![9, 8, 7]);
        assert_eq!(set.range(7..10).rev().collect::<Vec<_>>(), vec![9, 8, 7]);

        assert_eq!(set.range(2..=4).rev().collect::<Vec<_>>(), vec![4, 3, 2]);
        assert_eq!(set.range(2..5).rev().collect::<Vec<_>>(), vec![4, 3, 2]);

        // Full node
        assert_eq!(set.range(0..=4).rev().collect::<Vec<_>>(), vec![4, 3, 2, 1, 0]);
        assert_eq!(set.range(0..5).rev().collect::<Vec<_>>(), vec![4, 3, 2, 1, 0]);

        assert_eq!(set.range(5..=9).rev().collect::<Vec<_>>(), vec![9, 8, 7, 6, 5]);
        assert_eq!(set.range(5..10).rev().collect::<Vec<_>>(), vec![9, 8, 7, 6, 5]);

        assert_eq!(
            set.range(10..=19).rev().collect::<Vec<_>>(),
            vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]
        );
        assert_eq!(
            set.range(10..20).rev().collect::<Vec<_>>(),
            vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10]
        );

        // Node intersection
        assert_eq!(set.range(3..=6).rev().collect::<Vec<_>>(), vec![6, 5, 4, 3]);
        assert_eq!(set.range(3..7).rev().collect::<Vec<_>>(), vec![6, 5, 4, 3]);

        assert_eq!(set.range(8..=11).rev().collect::<Vec<_>>(), vec![11, 10, 9, 8]);
        assert_eq!(set.range(8..12).rev().collect::<Vec<_>>(), vec![11, 10, 9, 8]);

        // Non-existent range
        assert!(set.range(20..).collect::<Vec<_>>().is_empty());
        assert!(set.range(..0).collect::<Vec<_>>().is_empty());
        assert!(set.range(20..).rev().collect::<Vec<_>>().is_empty());
        assert!(set.range(..0).rev().collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn concurrent_range_constructions_at_node_boundaries_do_not_deadlock() {
        const THREAD_ITERATIONS: usize = 20_000;

        let set = Arc::new(BTreeSet::<u64>::with_maximum_node_size(4));
        for value in 0..64 {
            set.insert(value);
        }

        // One thread constructs ranges whose start sits at node minima
        // (locking a node, then its predecessor); the other constructs
        // ranges whose end sits at node maxima (locking a node, then its
        // successor). Pre-fix these acquisitions ran in opposite orders
        // while both locks were held, an ABBA deadlock.
        let (done_tx, done_rx) = mpsc::channel();
        let forward = {
            let set = Arc::clone(&set);
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                for iteration in 0..THREAD_ITERATIONS {
                    let start = (iteration % 64) as u64;
                    assert_eq!(set.range(start..).next(), Some(start));
                }
                done_tx.send(()).unwrap();
            })
        };
        let backward = {
            let set = Arc::clone(&set);
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                for iteration in 0..THREAD_ITERATIONS {
                    let end = (iteration % 64) as u64;
                    assert_eq!(set.range(..=end).next_back(), Some(end));
                }
                done_tx.send(()).unwrap();
            })
        };
        drop(done_tx);

        for _ in 0..2 {
            done_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("concurrent range constructions deadlocked");
        }
        forward.join().unwrap();
        backward.join().unwrap();
    }

    // Builds nodes [0, 10] (key 10), [20, 30] (key 30), [40, 50, 60] (key 60).
    fn three_node_set() -> BTreeSet<u64> {
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in [0u64, 10, 20, 30, 40, 50, 60] {
            set.insert(value);
        }
        assert_eq!(
            set.index.read().keys().copied().collect::<Vec<_>>(),
            vec![10, 30, 60],
            "fixture geometry changed"
        );
        set
    }

    #[test]
    fn forward_scan_repositions_when_current_node_vanishes() {
        let set = three_node_set();

        let mut iter = set.iter();
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), Some(10));
        assert_eq!(iter.next(), Some(20));

        // The node the iterator is parked in vanishes from the index, as
        // UpdateMax's remove-then-insert re-key does on every monotonic
        // insert. The scan must reposition, not end.
        set.index.write().remove(&30).expect("fixture entry");

        assert_eq!(iter.next(), Some(30));
        assert_eq!(iter.next(), Some(40));
        assert_eq!(iter.next(), Some(50));
        assert_eq!(iter.next(), Some(60));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn backward_scan_repositions_when_current_node_vanishes() {
        let set = three_node_set();

        let mut iter = set.iter();
        assert_eq!(iter.next_back(), Some(60));
        assert_eq!(iter.next_back(), Some(50));

        set.index.write().remove(&60).expect("fixture entry");

        assert_eq!(iter.next_back(), Some(40));
        assert_eq!(iter.next_back(), Some(30));
        assert_eq!(iter.next_back(), Some(20));
        assert_eq!(iter.next_back(), Some(10));
        assert_eq!(iter.next_back(), Some(0));
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
        // node and just exhausted the lower half: advancing into the upper
        // half must not re-yield 20.
        let iter = Iter {
            tree: &set,
            current_front_batch: None,
            current_back_batch: None,
            exhausted_front_node: Some(set.index.read().first_key_value().expect("fixture node").1.clone()),
            exhausted_back_node: None,
            front_partial: None,
            back_partial: None,
            front_batch_limit: INITIAL_BATCH,
            back_batch_limit: INITIAL_BATCH,
            current_front_value: Some(20),
            current_back_value: None,
            met: false,
        };

        assert_eq!(iter.collect::<Vec<_>>(), vec![30, 40]);
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
            let node = set.index.read().first_key_value().expect("fixture node").1.clone();
            let mut guard = node.write();
            NodeLike::insert(&mut *guard, 35u64);
        }

        let mut iter = Iter {
            tree: &set,
            current_front_batch: None,
            current_back_batch: None,
            exhausted_front_node: None,
            exhausted_back_node: Some(set.index.read().last_key_value().expect("fixture node").1.clone()),
            front_partial: None,
            back_partial: None,
            front_batch_limit: INITIAL_BATCH,
            back_batch_limit: INITIAL_BATCH,
            current_front_value: None,
            current_back_value: Some(30),
            met: false,
        };

        let mut collected = vec![];
        while let Some(value) = iter.next_back() {
            collected.push(value);
        }
        assert_eq!(collected, vec![10, 0]);
    }

    #[test]
    fn backward_scan_does_not_skip_values_split_away_after_positioning() {
        // The heavy-tier churn failure in deterministic form. A backward
        // scan chooses its node (at construction or when advancing) and only
        // later locks it to read. A split committed in that window keeps the
        // node's LOWER half in the chosen Arc and moves the upper half to a
        // new node: values the scan has not yielded yet migrate above its
        // resume point and are silently skipped. Node selection and the
        // content read must be one atomic step under the structural guard.
        let set = BTreeSet::<u64>::with_maximum_node_size(4);
        for value in [0u64, 10, 20, 30] {
            set.insert(value);
        }

        // Position the scan on the (single) node...
        let mut iter = set.iter();
        // ...then let a writer split it before the scan reads anything:
        // [0, 10] stays in the original Arc, [20, 30, 40] moves to a new
        // node above it.
        set.insert(40);
        assert!(set.node_count() > 1, "fixture must split");

        let mut collected = vec![];
        while let Some(value) = iter.next_back() {
            collected.push(value);
        }

        // 40 was inserted mid-scan, so a weakly consistent scan may or may
        // not observe it; every baseline value must be yielded. (Linear
        // scan on purpose: with NodeLike in scope, Vec::contains resolves
        // to NodeLike's binary search, which is wrong on this descending
        // vector.)
        for baseline in [30u64, 20, 10, 0] {
            assert!(
                collected.iter().any(|value| *value == baseline),
                "baseline value {baseline} skipped by backward scan (yielded: {collected:?})"
            );
        }
        assert!(
            collected.windows(2).all(|pair| pair[0] > pair[1]),
            "backward scan not strictly decreasing: {collected:?}"
        );
    }

    #[test]
    fn bidirectional_meet_into_opposite_held_node_does_not_self_deadlock() {
        // Nodes [0, 10] (key 10) and [20, 30, 40] (key 40).
        let set = Arc::new(BTreeSet::<u64>::with_maximum_node_size(4));
        for value in [0u64, 10, 20, 30, 40] {
            set.insert(value);
        }

        let (done_tx, done_rx) = mpsc::channel();
        let handle = {
            let set = Arc::clone(&set);
            thread::spawn(move || {
                // The back cursor enters the final node, then the forward
                // end exhausts the first node and must enter the node the
                // back cursor is positioned in to yield the middle. Under
                // the guard-holding design this double-locked the
                // non-reentrant node mutex; owned batches must keep this
                // lock-free.
                let mut finished = set.iter();
                assert_eq!(finished.next_back(), Some(40));
                assert_eq!(finished.next(), Some(0));
                assert_eq!(finished.next(), Some(10));
                assert_eq!(finished.next(), Some(20));
                assert_eq!(finished.next(), Some(30));
                assert_eq!(finished.next(), None);
                assert_eq!(finished.next_back(), None);

                // `finished` met in the middle and stays alive: a finished
                // iterator must hold no node locks, or the next lock of its
                // final node (here by a second iterator on the same thread)
                // self-deadlocks.
                let mut iter = set.iter();
                assert_eq!(iter.next(), Some(0));
                assert_eq!(iter.next_back(), Some(40));
                assert_eq!(iter.next_back(), Some(30));
                assert_eq!(iter.next_back(), Some(20));
                assert_eq!(iter.next_back(), Some(10));
                assert_eq!(iter.next_back(), None);
                assert_eq!(iter.next(), None);
                drop(finished);

                done_tx.send(()).unwrap();
            })
        };

        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("bidirectional meet-in-the-middle deadlocked");
        handle.join().unwrap();
    }

    #[test]
    fn structural_commits_complete_against_paused_scan() {
        // Several tiny nodes; the scan will pin the first one.
        let set = Arc::new(BTreeSet::<u64>::with_maximum_node_size(2));
        for value in 0..8 {
            set.insert(value);
        }

        let scan_holds_guard = Arc::new(Barrier::new(3));
        let (done_tx, done_rx) = mpsc::channel();

        let scanner = {
            let set = Arc::clone(&set);
            let scan_holds_guard = Arc::clone(&scan_holds_guard);
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                let mut iter = set.iter();
                // A paused scan must hold no node lock between calls;
                // under the guard-holding design the first node's mutex
                // stayed pinned here.
                assert_eq!(iter.next(), Some(0));
                scan_holds_guard.wait();
                // Give the writer time to take the structural write lock
                // and the first node's mutex while the scan is parked.
                thread::sleep(Duration::from_millis(100));
                // Resuming in the opposite direction acquires the
                // structural read guard; if the scan still held a node
                // mutex here it would deadlock ABBA against the writer
                // (writer: topology -> node).
                let mut collected = vec![];
                while let Some(value) = iter.next_back() {
                    collected.push(value);
                }
                assert_eq!(collected, vec![7, 6, 5, 4, 3, 2, 1]);
                done_tx.send(()).unwrap();
            })
        };

        let writer = {
            let set = Arc::clone(&set);
            let scan_holds_guard = Arc::clone(&scan_holds_guard);
            let done_tx = done_tx.clone();
            thread::spawn(move || {
                scan_holds_guard.wait();
                // remove_range acquires the structural write lock and then
                // locks the node pinned by the scanner.
                set.remove_range(0..=0);
                done_tx.send(()).unwrap();
            })
        };
        drop(done_tx);
        scan_holds_guard.wait();

        for _ in 0..2 {
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("scan or structural commit deadlocked");
        }
        scanner.join().unwrap();
        writer.join().unwrap();
        assert!(!set.contains(&0));
    }

    #[test]
    fn full_scans_do_not_degrade_quadratically_with_node_count() {
        use std::time::Instant;

        // Many tiny nodes: the node-advance cost dominates the scan.
        const VALUES: u64 = 30_000;
        let set = BTreeSet::<u64>::with_maximum_node_size(2);
        for value in 0..VALUES {
            set.insert(value);
        }
        assert!(
            set.node_count() >= (VALUES / 4) as usize,
            "fixture must be a many-node tree, got {} nodes",
            set.node_count()
        );

        let started = Instant::now();
        assert_eq!(set.iter().count(), VALUES as usize);
        let forward = started.elapsed();

        let started = Instant::now();
        assert_eq!(set.iter().rev().count(), VALUES as usize);
        let backward = started.elapsed();

        // Advancing between nodes costs one logarithmic index lookup, so both
        // scans finish in milliseconds even in a debug build. The removed
        // linear identity relocation made each advance walk the index from
        // the front (~N^2/2 entry visits per scan, well over a minute at this
        // node count), so the generous budget still fails it decisively.
        let budget = Duration::from_secs(10);
        assert!(
            forward < budget,
            "forward scan took {forward:?}, node advance is not logarithmic"
        );
        assert!(
            backward < budget,
            "backward scan took {backward:?}, node advance is not logarithmic"
        );
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
            let forward = set.iter().collect::<Vec<_>>();
            assert!(
                forward.windows(2).all(|pair| pair[0] < pair[1]),
                "forward scan not strictly increasing (duplicate or unordered yield)"
            );
            assert_eq!(
                forward.iter().filter(|value| **value < BASELINE).count() as u64,
                BASELINE,
                "forward scan truncated: baseline keys missing"
            );

            let backward = set.iter().rev().collect::<Vec<_>>();
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
        assert_eq!(set.iter().collect::<Vec<_>>(), expected);
        assert_eq!(set.iter().rev().collect::<Vec<_>>(), {
            let mut reversed = expected;
            reversed.reverse();
            reversed
        });
    }

    #[test]
    fn collected_owned_values_survive_arbitrary_concurrent_mutation() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        const BASELINE: u64 = 512;
        const CHURN: u64 = 4_000;

        // Small nodes so the churn constantly splits, re-keys, and unlinks
        // the nodes the scans are walking.
        let set = Arc::new(BTreeSet::<u64>::with_maximum_node_size(8));
        for value in 0..BASELINE {
            set.insert(value);
        }

        let done = Arc::new(AtomicBool::new(false));
        let writer = {
            let set = Arc::clone(&set);
            let done = Arc::clone(&done);
            thread::spawn(move || {
                for value in BASELINE..BASELINE + CHURN {
                    assert!(set.insert(value));
                    assert_eq!(set.remove(&value), Some(value));
                }
                done.store(true, AtomicOrdering::Release);
            })
        };

        // The type annotation is the point: collect() yields owned values,
        // not references tied to node storage. Under the previous borrowed
        // design this collected Vec<&u64> whose referents were unlocked node
        // slots, a use-after-free under exactly this churn.
        let mut snapshots: Vec<Vec<u64>> = Vec::new();
        loop {
            let snapshot: Vec<u64> = set.iter().collect();
            snapshots.push(snapshot);
            if done.load(AtomicOrdering::Acquire) {
                break;
            }
        }
        writer.join().unwrap();

        // Mutate the set arbitrarily after the snapshots were taken; the
        // snapshots must remain fully usable because they own their values.
        set.remove_range(..);
        assert!(set.is_empty());

        for snapshot in snapshots {
            assert!(
                snapshot.windows(2).all(|pair| pair[0] < pair[1]),
                "snapshot not strictly increasing"
            );
            assert_eq!(
                snapshot.iter().filter(|value| **value < BASELINE).count() as u64,
                BASELINE,
                "snapshot lost baseline keys"
            );
        }
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
                for value in set_clone.iter() {
                    _sum += value;
                }
            }
        });

        for i in 10_000..20_000 {
            set.insert(i);
        }
        handle.join().unwrap();
    }

    /// A scan that spans several batch installs inside one node yields every
    /// element, once, in order.
    ///
    /// The batch is bounded and grows, so a node larger than `INITIAL_BATCH` is
    /// consumed over several installs rather than one. Each install re-selects
    /// the node by cursor and skips what has already been taken, which is where
    /// a partial batch can silently drop or repeat elements. A whole-node batch
    /// could not get this wrong because it never resumed inside a node.
    #[test]
    fn a_scan_across_several_batch_installs_is_complete_and_ordered() {
        let set: BTreeSet<u64> = BTreeSet::new();
        // Comfortably more than INITIAL_BATCH, and more than the first few
        // doublings, so the scan resumes inside a node repeatedly.
        let count = (INITIAL_BATCH * 20) as u64;
        for i in 0..count {
            set.insert(i);
        }

        let seen: Vec<u64> = set.iter().collect();
        let expected: Vec<u64> = (0..count).collect();
        assert_eq!(seen, expected, "a partial-batch scan lost or repeated elements");
    }

    /// The same property backwards.
    #[test]
    fn a_backward_scan_across_several_installs_is_complete_and_ordered() {
        let set: BTreeSet<u64> = BTreeSet::new();
        let count = (INITIAL_BATCH * 20) as u64;
        for i in 0..count {
            set.insert(i);
        }

        let seen: Vec<u64> = set.iter().rev().collect();
        let expected: Vec<u64> = (0..count).rev().collect();
        assert_eq!(
            seen, expected,
            "a partial-batch backward scan lost or repeated elements"
        );
    }

    /// A scan over a node big enough to hold everything, so every install after
    /// the first resumes inside the same node.
    #[test]
    fn a_scan_within_a_single_node_resumes_correctly() {
        let set: BTreeSet<u64> = BTreeSet::with_maximum_node_size(DEFAULT_INNER_SIZE);
        let count = 200u64;
        for i in 0..count {
            set.insert(i);
        }
        assert_eq!(set.node_count(), 1, "fixture wants one node");

        let seen: Vec<u64> = set.iter().collect();
        assert_eq!(seen, (0..count).collect::<Vec<_>>());
    }

    /// A one-element range yields exactly that element.
    ///
    /// The case the bounded batch exists for: this used to clone every
    /// remaining element of the node it landed in to produce one value.
    #[test]
    fn a_single_element_range_yields_one_element() {
        let set: BTreeSet<u64> = BTreeSet::new();
        for i in 0..1_000u64 {
            set.insert(i);
        }

        for probe in [0u64, 1, 499, 998, 999] {
            let got: Vec<u64> = set.range(probe..=probe).collect();
            assert_eq!(got, vec![probe], "range({probe}..={probe})");
        }
        assert!(set.range(1_000..=1_000).next().is_none(), "absent key");
    }

    /// Ranges of every width across a batch boundary.
    ///
    /// Widths either side of `INITIAL_BATCH` and its first doublings are where
    /// an off-by-one in the resume arithmetic shows up, and nowhere else.
    #[test]
    fn ranges_spanning_batch_boundaries_are_exact() {
        let set: BTreeSet<u64> = BTreeSet::new();
        for i in 0..500u64 {
            set.insert(i);
        }

        for width in 1..=(INITIAL_BATCH * 8) as u64 {
            let start = 100u64;
            let got: Vec<u64> = set.range(start..start + width).collect();
            let expected: Vec<u64> = (start..start + width).collect();
            assert_eq!(got, expected, "range width {width}");
        }
    }

    /// Meeting in the middle still terminates and yields each element once.
    ///
    /// Both cursors now resume inside nodes, so the point at which they meet is
    /// reached through a different sequence of installs than before.
    #[test]
    fn a_double_ended_scan_meets_without_repeating() {
        let set: BTreeSet<u64> = BTreeSet::new();
        let count = (INITIAL_BATCH * 10) as u64;
        for i in 0..count {
            set.insert(i);
        }

        let mut iter = set.iter();
        let mut front = Vec::new();
        let mut back = Vec::new();
        loop {
            match iter.next() {
                Some(v) => front.push(v),
                None => break,
            }
            match iter.next_back() {
                Some(v) => back.push(v),
                None => break,
            }
        }
        back.reverse();
        front.extend(back);
        front.sort_unstable();
        assert_eq!(
            front,
            (0..count).collect::<Vec<_>>(),
            "double-ended scan is not a partition"
        );
    }

    /// A scan under concurrent mutation terminates and does not stream
    /// duplicates forever.
    ///
    /// This is the case the partial-skip guard exists for, and it cannot be
    /// reached from one thread. A batch that stops short of a node's end
    /// resumes inside that node by cursor rank; a concurrent split or re-key
    /// can leave that rank *below* elements already yielded, the yield path
    /// then drops the whole batch as duplicates, and the next install computes
    /// the same skip again. Without the recorded take count that is a scan
    /// which never advances.
    ///
    /// A stall is asserted as a bound rather than by waiting: the scan is
    /// capped, and a run that reaches the cap is one that was not making
    /// progress. Removing the guard makes this fail rather than hang, which is
    /// the difference between a test and a timeout.
    #[test]
    fn a_scan_under_concurrent_mutation_terminates() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        const SIZE: u64 = 4_000;
        // Generous: any honest scan yields at most SIZE plus whatever is
        // inserted while it runs. Reaching this many means it is looping.
        const CAP: usize = (SIZE * 20) as usize;

        for _ in 0..8 {
            let set: Arc<BTreeSet<u64>> = Arc::new(BTreeSet::new());
            for i in 0..SIZE {
                set.insert(i);
            }
            let stop = Arc::new(AtomicBool::new(false));

            // Churn that forces splits and re-keys under the scan.
            let writers: Vec<_> = (0..3)
                .map(|w| {
                    let (set, stop) = (Arc::clone(&set), Arc::clone(&stop));
                    std::thread::spawn(move || {
                        let mut i = SIZE + w * 100_000;
                        while !stop.load(Ordering::Relaxed) {
                            set.insert(i);
                            set.remove(&i);
                            i += 1;
                        }
                    })
                })
                .collect();

            let mut yielded = 0usize;
            for _ in set.iter() {
                yielded += 1;
                if yielded >= CAP {
                    break;
                }
            }

            stop.store(true, Ordering::Relaxed);
            for w in writers {
                w.join().expect("writer did not panic");
            }

            assert!(
                yielded < CAP,
                "scan did not make progress under concurrent mutation: {yielded} yields"
            );
        }
    }

    /// `Vec::contains` is not usable in this module: `NodeLike` is in scope and
    /// its `contains` for `Vec<T>` is a *binary search*, which silently answers
    /// nonsense for any sequence that is not sorted ascending. A scan's output
    /// is exactly such a sequence when it runs backwards.
    // Clippy suggests `seen.contains(&value)` here. Taking that suggestion
    // reintroduces the exact bug this helper exists to avoid, which is why the
    // lint is silenced rather than followed.
    #[allow(clippy::manual_contains)]
    fn yielded(seen: &[u64], value: u64) -> bool {
        seen.iter().any(|item| *item == value)
    }

    /// WTI-1: `front_partial` counts *positions*, and a position is not a
    /// stable cursor under deletion.
    ///
    /// Deleting an element the scan already yielded shifts the unyielded tail
    /// left while the recorded count stays put, so the stale position wins the
    /// `max` and steps over a live element. Key `0` is removed after it has
    /// been yielded; every key above it was present for the whole scan and must
    /// still appear, which is what the iterator promises.
    ///
    /// The prefix is swept because the defect only bites when the deletion
    /// lands while the scan is partway through a node, and where that boundary
    /// falls depends on the doubling batch limit.
    #[test]
    fn deleting_a_yielded_element_does_not_skip_a_live_one() {
        for prefix in 1..12usize {
            let set: BTreeSet<u64> = BTreeSet::new();
            for i in 0..256u64 {
                set.insert(i);
            }

            let mut seen = Vec::new();
            for value in set.iter() {
                seen.push(value);
                if seen.len() == prefix {
                    set.remove(&0);
                }
            }

            for expected in 1..256u64 {
                assert!(
                    yielded(&seen, expected),
                    "prefix {prefix}: {expected} was present for the whole scan but was never yielded"
                );
            }
        }
    }

    /// The backward mirror. `back_partial` trims from the end rather than
    /// skipping from the start, so the same staleness would drop an element off
    /// the low end of the scan.
    #[test]
    fn deleting_a_yielded_element_backwards_does_not_skip_a_live_one() {
        for prefix in 1..12usize {
            let set: BTreeSet<u64> = BTreeSet::new();
            for i in 0..256u64 {
                set.insert(i);
            }

            let mut seen = Vec::new();
            for value in set.iter().rev() {
                seen.push(value);
                if seen.len() == prefix {
                    set.remove(&255);
                }
            }

            for expected in 0..255u64 {
                assert!(
                    yielded(&seen, expected),
                    "prefix {prefix}: {expected} was present for the whole scan but was never yielded"
                );
            }
        }
    }
}

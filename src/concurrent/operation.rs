use std::fmt::Debug;
use std::sync::Arc;

use parking_lot::Mutex;
use std::collections::BTreeMap;

use crate::cdc::change::ChangeEventUnassigned;
use crate::core::node::NodeLike;

type OldVersion<Node> = Arc<Mutex<Node>>;
type CurrentVersion<Node> = Arc<Mutex<Node>>;

pub enum Operation<T: Send + Ord, Node: NodeLike<T>> {
    Split(OldVersion<Node>, T, T),
    UpdateMax(CurrentVersion<Node>, T),
    MakeUnreachable(CurrentVersion<Node>, T),
}

impl<T, Node> Operation<T, Node>
where
    T: Debug + Ord + Send + Clone + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    // `EMIT_CDC` is a compile-time switch so event construction disappears
    // from ordinary insert and remove monomorphizations.
    // `adopt` runs when the riding insert replaces a stored, logically equal
    // value in place: see `MultiPairLike::adopt_stored_identity`.
    pub fn commit<const EMIT_CDC: bool>(
        self,
        index: &mut BTreeMap<T, Arc<Mutex<Node>>>,
        adopt: fn(&T, &mut T),
    ) -> Result<(Option<T>, Vec<ChangeEventUnassigned<T>>), ()> {
        match self {
            Operation::Split(old_node, old_max, value) => {
                let mut guard = old_node.lock_arc();
                if let Some(entry) = index.get(&old_max) {
                    if Arc::ptr_eq(entry, &old_node) {
                        // The node was drained by a concurrent remove after
                        // this split was scheduled. Halving an empty node
                        // would unlink it while silently dropping the pending
                        // insert (and the cdc build would panic reading the
                        // vanished maximum). Fail the commit instead so the
                        // insert retries against the current topology.
                        if guard.max().is_none() {
                            return Err(());
                        }

                        let mut cdc = vec![];
                        #[cfg(feature = "cdc")]
                        let max_value =
                            EMIT_CDC.then(|| guard.max().expect("node should be non empty if split").clone());
                        index.remove(&old_max);
                        let mut new_vec = guard.halve();

                        #[cfg(feature = "cdc")]
                        if EMIT_CDC {
                            let node_split = ChangeEventUnassigned::SplitNode {
                                max_value: max_value.expect("captured when CDC emission is enabled"),
                                split_index: guard.len(),
                            };
                            cdc.push(node_split);
                        }

                        let mut old_value: Option<T> = None;
                        let mut insert_attempted = false;
                        if let Some(max) = guard.max().cloned() {
                            if max >= value {
                                let (inserted, idx) = NodeLike::insert(&mut *guard, value.clone());
                                insert_attempted = true;
                                let mut committed_value = value.clone();
                                if !inserted {
                                    if let Some(stored) = guard.get_ith(idx) {
                                        adopt(stored, &mut committed_value);
                                    }
                                    old_value = NodeLike::replace(&mut *guard, idx, committed_value.clone());
                                    #[cfg(feature = "cdc")]
                                    if EMIT_CDC {
                                        let value_insertion = ChangeEventUnassigned::RemoveAt {
                                            max_value: max.clone(),
                                            index: idx,
                                            value: old_value.clone().unwrap(),
                                        };
                                        cdc.push(value_insertion);
                                    }
                                }
                                #[cfg(feature = "cdc")]
                                if EMIT_CDC {
                                    let value_insertion = ChangeEventUnassigned::InsertAt {
                                        max_value: max.clone(),
                                        index: idx,
                                        value: committed_value,
                                    };
                                    cdc.push(value_insertion);
                                }
                            }

                            index.insert(max, old_node.clone());
                        }

                        if let Some(mut max) = new_vec.max().cloned() {
                            if !insert_attempted {
                                let (inserted, idx) = NodeLike::insert(&mut new_vec, value.clone());
                                let old_max = max.clone();
                                let mut committed_value = value.clone();
                                if inserted {
                                    if value > max {
                                        max = value.clone()
                                    }
                                } else {
                                    if let Some(stored) = new_vec.get_ith(idx) {
                                        adopt(stored, &mut committed_value);
                                    }
                                    old_value = NodeLike::replace(&mut new_vec, idx, committed_value.clone());
                                    #[cfg(feature = "cdc")]
                                    if EMIT_CDC {
                                        let value_insertion = ChangeEventUnassigned::RemoveAt {
                                            max_value: old_max.clone(),
                                            index: idx,
                                            value: old_value.clone().unwrap(),
                                        };
                                        cdc.push(value_insertion);
                                    }
                                }
                                #[cfg(feature = "cdc")]
                                if EMIT_CDC {
                                    let value_insertion = ChangeEventUnassigned::InsertAt {
                                        max_value: old_max.clone(),
                                        index: idx,
                                        value: committed_value,
                                    };
                                    cdc.push(value_insertion);
                                }
                            }
                            let new_node = Arc::new(Mutex::new(new_vec));

                            index.insert(max, new_node);
                        }

                        return Ok((old_value, cdc));
                    }
                }

                Err(())
            }
            Operation::UpdateMax(node, old_max) => {
                let guard = node.lock_arc();
                if let Some(entry) = index.get(&old_max) {
                    if Arc::ptr_eq(entry, &node) {
                        let mut cdc = vec![];
                        return Ok(match guard.max() {
                            // The node was drained by a concurrent remove
                            // after this repair was scheduled. Returning
                            // without unlinking would leave a stale entry
                            // pointing at an empty node: a routing black hole
                            // whose pending `MakeUnreachable` (addressed by
                            // the node's drained maximum, not this entry key)
                            // can never remove it. Unlink it here; a racing
                            // refill either completed before this commit (the
                            // maximum is visible under the node lock) or will
                            // route after it and no longer see this entry.
                            None => {
                                #[cfg(feature = "cdc")]
                                if EMIT_CDC {
                                    let node_removal = ChangeEventUnassigned::RemoveNode {
                                        max_value: old_max.clone(),
                                    };
                                    cdc.push(node_removal);
                                }
                                index.remove(&old_max);

                                (None, cdc)
                            }
                            // Re-key the entry to the node's current maximum,
                            // in either direction.
                            Some(new_max) if *new_max != old_max => {
                                let new_max = new_max.clone();
                                index.remove(&old_max);
                                index.insert(new_max, node.clone());

                                (None, cdc)
                            }
                            // The entry key already matches the maximum.
                            _ => (None, cdc),
                        });
                    }
                }

                Err(())
            }
            Operation::MakeUnreachable(node, old_max) => {
                let guard = node.lock_arc();
                if let Some(entry) = index.get(&old_max) {
                    if Arc::ptr_eq(entry, &node) {
                        return match guard.max() {
                            // Still empty: unlink the node as requested.
                            None => {
                                let mut cdc = vec![];

                                #[cfg(feature = "cdc")]
                                if EMIT_CDC {
                                    let node_removal = ChangeEventUnassigned::RemoveNode {
                                        max_value: old_max.clone(),
                                    };
                                    cdc.push(node_removal);
                                }
                                index.remove(&old_max);

                                Ok((None, cdc))
                            }
                            // The node was refilled under the stale key by a
                            // concurrent insert before this unlink committed.
                            // Unlinking would silently drop the acknowledged
                            // insert, so re-key the entry to the fresh maximum
                            // instead (in either direction).
                            Some(new_max) if *new_max != old_max => {
                                let new_max = new_max.clone();
                                index.remove(&old_max);
                                index.insert(new_max, node.clone());

                                Ok((None, vec![]))
                            }
                            // Refilled and the maximum matches the entry key:
                            // nothing to repair.
                            _ => Err(()),
                        };
                    }
                }

                Err(())
            }
        }
    }
}

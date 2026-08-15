# WorkTablesIndex Review: full

**Date:** 2026-07-27
**Scope:** the whole crate: `src/lib.rs`, `src/core/{node,pair,multipair,constants}.rs`, `src/concurrent/{set,map,multimap,operation,ref}.rs`, `src/cdc/change.rs`, `benches/`, `Cargo.toml`, `.github/workflows/`, `README.md`, `CHANGELOG.md`, `AGENTS.md`, `CLAUDE.md`. Consumer usage cross-checked read-only in `/Users/revenge/code/WorkTable/src/index/**` and `/Users/revenge/code/WorkTable/src/persistence/space/index/**`.
**Commit:** `8c7465d` (clean tree, no uncommitted work)
**Reviewer slice:** full (sole reviewer for this repo)

## Summary

- **This crate is the halve fix, not a victim of it.** `src/core/node.rs:145-157` carries the pathscale `split_off(self.len() / 2)` change, and it matches `/Users/revenge/code/indexset-halve-fix.patch` byte for byte. There is no other `NodeLike` impl in the tree, and no dependency pulls a second copy of upstream `indexset` (`Cargo.lock` has exactly one). **Exposure to the upstream halve bug: none.** The residual risk is different and described in `WTI-full-06`: `halve()` is still called on nodes that may have shrunk to 0 elements, where the crate panics (cdc on) or silently drops the value (cdc off).
- Three **Critical** defects, all reproduced with running code against this exact commit, all reachable from 100% safe API calls: an iterator that hands out references outliving the lock that protects them (use after free, demonstrated), a split path that accepts a duplicate key and physically stores it twice (demonstrated, and it is exactly what `WorkTable::insert_checked` relies on for primary key uniqueness), and a multimap that fabricates an uninitialised value with `MaybeUninit::assume_init` (demonstrated `SIGABRT`).
- The two patterns flagged from the sibling WorkTable review are **absent as written**: there is no `unsafe impl Send`/`Sync`, no `UnsafeCell`, no `static mut` anywhere in the crate (`rg 'unsafe impl|UnsafeCell|static mut' src/` is empty), and the single `if let Ok(_) = ...try_write()` at `src/concurrent/set.rs:156` **does** hold the guard for the body (I verified this empirically on both edition 2021 and 2024; the Rust 2024 if-let rescoping affects the `else` branch, not the `then` branch). The lock is held; what is missing is a re-check of the condition after upgrading read to write, which is a real race (`WTI-full-05`).
- The moral equivalent of pattern (a) is present in a different disguise: six `std::mem::transmute` calls in `src/concurrent/set.rs` launder a slice iterator's lifetime from "borrowed from this guard" to "borrowed from the tree". The write path takes the node mutex; the reader keeps using the data after that mutex is released. Same failure mode, achieved with `transmute` instead of `unsafe impl Sync`.
- **Top three things to do, in order:** (1) fix the split path duplicate hole (`WTI-full-02`), because it silently violates primary key uniqueness in WorkTable and corrupts `len()`; (2) tie iterator items to `&mut self` or make them owned (`WTI-full-01`); (3) delete `MultiPair::with_infimum`/`with_supremum` in favour of a real sentinel (`WTI-full-03`).
- **Test posture:** 62 fast-tier tests pass in 0.30s, `cargo check --all-features --all-targets` is clean. But there is no property test against a reference `BTreeMap`, no `loom`/`shuttle` model, no Miri run, and, most pointedly, **the halve fix that this fork exists for has no regression test**: `test_halve` (`src/lib.rs:3840`) only exercises `len == capacity`, which is the case that never failed. Concrete sketches in `WTI-full-07` and the cross-cutting section.

## Findings

### [SEV-1] Iterator items outlive the mutex guard that protects them: use after free from safe code

- **ID:** `wti-full-01`
- **Severity:** Critical
- **Category:** Correctness (soundness)
- **Confidence:** High (reproduced, output below)
- **Location:** `src/concurrent/set.rs:614` (`type Item = &'a T`), transmutes at `src/concurrent/set.rs:641-648, 702-709, 731-738, 799-806, 860-867, 889-896`; propagated by `src/concurrent/map.rs:49`, `src/concurrent/multimap.rs:50,99`
- **What:** `Iter` stores an `ArcMutexGuard` for the node it is currently walking and a `std::slice::Iter` over that node's contents, produced by `std::mem::transmute` to erase the borrow of the guard. `Iterator::Item` is `&'a T` where `'a` is the borrow of the *tree*, not of the guard and not of `&mut self`. As soon as the iterator advances past a node boundary (or is dropped), the guard is released, other threads may mutate that node, and every reference the caller is still holding dangles. Because the concurrent `BTreeSet`/`BTreeMap` take `&self` for `insert`/`remove`, the borrow checker cannot catch this even single threaded.
- **Why it matters:** `set.iter().collect::<Vec<&T>>()` followed by any mutation is a use after free. This is not theoretical; the crate's own tests do `set.range(0..=4).collect::<Vec<_>>()` (`src/concurrent/set.rs:1806`), which is `Vec<&i32>`. Reproduction against commit `8c7465d`:

  ```rust
  let set = BTreeSet::<String>::with_maximum_node_size(64);
  for i in 0..8 { set.insert(format!("value-number-{:03}-{}", i, "x".repeat(64))); }
  let refs: Vec<&String> = set.iter().collect();     // safe
  for i in 0..8 { set.remove(&format!("value-number-{:03}-{}", i, "x".repeat(64))); } // safe, &self
  for i in 0..64 { set.insert(format!("REUSED-{:03}-{}", i, "y".repeat(64))); }
  println!("{}", refs[0]);                            // reads freed memory
  ```

  Output: `before: value-number-000-xxxx...` then `after: \xef\xbf\xbdL\xef\xbf\xbdGo...` with `len: 81`. That is a freed `String` buffer being read back after reuse. In a database indexing layer this is an arbitrary memory disclosure and a crash source.
- **Fix:** Two options, both breaking. (a) Bind items to the iterator borrow: `fn next<'s>(&'s mut self) -> Option<&'s T>`, which requires abandoning `Iterator` (lending iterator) or `Item = T` with a clone. Given `T: Clone` is already a bound on the whole structure, **`Item = T` with a clone per element is the pragmatic fix** and matches how every current caller in WorkTable and in this crate's own tests uses it (they clone or copy immediately). (b) Keep `&'a T` but hold *all* node guards for the iterator's lifetime, which serialises the tree against writers and is worse. Until this is fixed, document `Iter` as unsound rather than leaving it looking safe.
- **Effort:** M for option (a) mechanically, plus a sweep of WorkTable call sites.
- **Blast radius:** `concurrent::set::{Iter,Range}`, `concurrent::map::{Iter,Range}`, `concurrent::multimap::{Iter,RawRange,Range}`, every consumer that iterates. Breaking API change, requires a coordinated version bump per the AGENTS.md pinning rule.

### [SEV-2] Split path skips the duplicate check and misroutes the middle key: duplicate entries, wrong `len()`, primary key uniqueness bypassed

- **ID:** `wti-full-02`
- **Severity:** Critical
- **Category:** Correctness
- **Confidence:** High (reproduced)
- **Location:** `src/concurrent/set.rs:182,214-220` (the `need_to_split` branch), `src/concurrent/operation.rs:53-116` (`Operation::Split::commit`, specifically the `if max > value` at line 54), `src/concurrent/map.rs:261-264` (`checked_insert`)
- **What:** `put_cdc_checked` returns `Err(..)` (the "value already present" signal) only on the non-splitting path. When the target node is full it schedules `Operation::Split` and never checks whether the value is already in the node. `commit` then halves the node and decides which half gets the value with `if max > value`, where `max` is the max of the *lower* half. When the incoming value is exactly equal to that max, which is precisely the case where it is a duplicate of the middle element, the strict `>` sends it to the **upper** half, where `NodeLike::insert` succeeds because that half does not contain it. The result is the same key stored in two nodes, with the second copy sitting below its node's key, violating the "every value in a node is `<=` the node's index key" invariant.
- **Why it matters:** `WorkTable/src/index/primary_index.rs:66-70` implements `insert_checked` as `self.pk_map.checked_insert(value.clone(), offset_link)?`, that is, primary key uniqueness is exactly this `Option`. Reproduction against commit `8c7465d`:

  ```rust
  let map = BTreeMap::<usize, &str>::with_maximum_node_size(4);
  for (k,v) in [(1,"a"),(2,"b"),(3,"c"),(4,"d")] { map.insert(k,v); }  // node is now full
  map.checked_insert(2, "DUPLICATE");   // -> Some(())  : accepted!
  map.len();                            // -> 5         : four distinct keys
  map.get(&2);                          // -> Some("b") : old value kept
  map.iter().count();                   // -> 4         : len() and iter() disagree
  map.insert(2, "DUP");                 // -> None      : claims "no previous value"
  ```

  So a duplicate primary key insert is accepted, WorkTable proceeds to write the row and to insert into `reverse_pk_map`, the old row's link stays in `pk_map`, and the tree now permanently reports a length that no traversal can produce. With `cdc` on, an `InsertAt` event for the duplicate is emitted, so the persisted space index inherits the duplicate too. The trigger is narrow but deterministic: for each full node, the key at index `len/2 - 1` reproduces it every time, and nodes stay full until something splits them, so this is a persistent state, not a transient window.
- **Fix:** Two changes, both small, plus one design decision.
  1. In `put_cdc_checked`, before scheduling the split, do the same presence check the other branch does:
     ```rust
     } else {
         if let Some(idx) = node_guard.try_select(&value) {
             let max = node_guard.max().cloned().expect("non-empty: it needs to split");
             return Err((node_guard, idx, max));
         }
         operation = Some(Operation::Split(...));
     }
     ```
  2. Defence in depth in `commit`: `if max >= value` at `src/concurrent/operation.rs:54`, so an equal value can never be routed to the upper half.
  3. Design decision: `checked_insert`/`checked_insert_cdc` currently map any `Ok` to `Some(())`, so even after fix 2 a `Ok((Some(old), _))` (a replacement) reads as success. They should return `None` when the result carries an old value. That changes what WorkTable's `insert_checked` sees in the replace case, so agree it with the WorkTable side before shipping.
- **Effort:** S for the code, M including the WorkTable-side agreement and a regression test at `with_maximum_node_size(4)`.
- **Blast radius:** `concurrent::set::put_cdc_checked`, `concurrent::operation`, `concurrent::map::{checked_insert,checked_insert_cdc}`. Behavioural change visible to WorkTable's primary and secondary index insert paths. Node geometry does not change for non-duplicate inserts, so the space index golden fixtures should be unaffected, but re-run them.

### [SEV-3] `MultiPair::with_infimum`/`with_supremum` fabricate an uninitialised value: instant UB, aborts for any `V` with a destructor

- **ID:** `wti-full-03`
- **Severity:** Critical
- **Category:** Correctness (soundness)
- **Confidence:** High (reproduced, `SIGABRT`)
- **Location:** `src/core/multipair.rs:23-28`; called from `src/concurrent/multimap.rs:270-271, 294-295, 563, 566, 573, 576`
- **What:**
  ```rust
  pub fn with_infimum(key: K) -> Self {
      Self { key, value: unsafe { MaybeUninit::uninit().assume_init() }, discriminator: INFIMUM }
  }
  ```
  These are safe public functions that produce a `V` out of uninitialised memory. Producing an uninitialised value of any type is immediate UB in Rust even for integers, and when the sentinel `MultiPair` is dropped (which happens at the end of `raw_get`, `get` and `range`, since the sentinels are moved into a temporary `Range` and dropped there) the fabricated `V` is dropped too.
- **Why it matters:** every `BTreeMultiMap::get`, `range` and `remove` builds two of these. For `V: Drop` the process dies. Reproduction against commit `8c7465d`:

  ```rust
  let m = BTreeMultiMap::<usize, String>::new();
  m.insert(1, "same-value".to_string());
  m.get(&1).count();                                 // survived (uninit happened to be zeroed)
  m.remove(&1, &"same-value".to_string());           // process aborts, exit code 134 (SIGABRT)
  ```

  WorkTable's instantiations use `V = Link` (`WorkTable/src/index/unsized_node.rs:277`, `WorkTable/src/in_memory/empty_link_registry.rs`), which has no destructor, so today this "only" reads uninitialised memory rather than crashing. That makes it a landmine, not a live outage: the first non-`Copy` multimap value type in WorkTable turns every `get` into a crash. It is also UB the optimiser is entitled to exploit today.
- **Fix:** stop fabricating `V`. Make the discriminator carry the whole sentinel and never touch `value`:
  ```rust
  pub enum MultiKeyBound<K> { Infimum(K), Supremum(K) }
  ```
  with `Borrow`/`Ord` bridging to `MultiPair`, so `range` can be expressed with a bound type that has no `V` at all. If that is too invasive for the minimal-diff policy, the cheap intermediate is `V: Default` plus `V::default()`, or wrapping the field as `Option<V>`. Either way `MaybeUninit::assume_init` must go. This is worth an upstream patch to brurucy/indexset, since it is upstream's bug.
- **Effort:** M (the `Ord`/`Borrow` plumbing is the work, not the sentinel itself).
- **Blast radius:** `core::multipair`, `concurrent::multimap`, WorkTable's `src/index/multipair.rs` and any custom `NodeLike` over `MultiPair`. Breaking if the enum route is taken.

### [SEV-4] `MultiPair`'s `Ord` is not transitive, so the multimap stores duplicate `(key, value)` pairs and leaves stale entries behind

- **ID:** `wti-full-04`
- **Severity:** High
- **Category:** Correctness
- **Confidence:** High (reproduced)
- **Location:** `src/core/multipair.rs:42-58`
- **What:** for equal keys, `cmp` returns `Equal` when the *values* compare equal, and otherwise orders by a random `discriminator`. That is not a total order. With `A = (k, "x", d=1)`, `B = (k, "x", d=9)`, `C = (k, "y", d=5)`: `A == B` (values equal), `A < C` (1 < 5), `B > C` (9 > 5). Transitivity is violated, so the binary search in `core::node::search` can walk past an element that compares `Equal` to the needle and report "not present".
- **Why it matters:** deduplication of `(key, value)` pairs silently fails once a key has more than one value. Reproduction against commit `8c7465d`:

  ```rust
  let m = BTreeMultiMap::<usize, usize>::new();
  for v in 0..50 { m.insert(1, v); }   // 50 distinct values under key 1
  for v in 0..50 { m.insert(1, v); }   // insert every one a second time
  m.len();                             // -> 91, expected 50
  m.remove(&1, &7);                    // -> Some((1,7))
  m.get(&1).filter(|(_,v)| **v == 7).count();  // -> 1, the duplicate survives
  ```

  For WorkTable's non-unique indexes (`key -> Link`) this means re-inserting an existing `(key, link)` pair, which an in-place update does, creates a second entry, and the subsequent single `remove` leaves a stale link pointing at a row that no longer exists there. Also note `Ord::cmp` disagreeing with `PartialEq` is itself a documented logic error for anything stored in an ordered container.
- **Fix:** make the order total and consistent with `Eq`: order by `(key, discriminator)` only, and assign the discriminator deterministically from the value (for example a 64-bit hash of `V`, requiring `V: Hash`) so that equal values collide into the same slot and dedupe correctly, with a linear probe over the equal-key run for genuine hash collisions. If dedupe of `(key, value)` is not actually wanted by WorkTable, then drop the value-equality shortcut entirely and let random discriminators do their job, but then `remove(&k, &v)` semantics need re-stating. This needs a short design discussion, not just an edit.
- **Effort:** M
- **Blast radius:** `core::multipair`, `concurrent::multimap`, WorkTable's secondary indexes and `empty_link_registry`. Changes the physical order of multimap nodes, therefore changes persisted geometry: regenerate WorkTable fixtures.

### [SEV-5] Empty-tree insert: the condition is checked under the read lock and acted on under the write lock without re-checking

- **ID:** `wti-full-05`
- **Severity:** High
- **Category:** Concurrency
- **Confidence:** Medium (read from code, timing dependent, not reproduced)
- **Location:** `src/concurrent/set.rs:141-174`
- **What:** the "index is empty" branch does `lower_bound` and `index.back()` under `_global_guard = self.index_lock.read()`, drops that guard at line 155, then takes `if let Ok(_) = self.index_lock.try_write()` and inserts unconditionally. The emptiness check is never revalidated under the write lock. (For the record, the guard *is* held for the body: I checked `if let Ok(_) = m.try_lock() { m.try_lock() }` on both edition 2021 and edition 2024 and the inner attempt fails in both, so the Rust 2024 if-let rescoping does not bite here. The bug is the missing re-check, not the lock scope.)
- **Why it matters:** thread T2 can observe an empty index, be preempted before `try_write`, and meanwhile T1 creates the first node and T3 inserts a second value into it. When T2 resumes and wins `try_write`, `self.index.insert(value, first_node)` replaces the whole node with its own single-element node, and T3's value is gone even though T3 already returned "inserted". With `cdc` on, this also emits a second `CreateNode` for a max the consumer already has, with no intervening `RemoveNode`, so the persisted replica diverges from memory. The window is short (an `Arc`/`Mutex`/`Vec` allocation happens before the drop, not after), but it exists on every fresh table and every table drained to empty.
- **Fix:** re-validate under the write guard. Simplest correct form:
  ```rust
  drop(_global_guard);
  let write_guard = self.index_lock.write();          // block instead of try_write
  if self.index.is_empty() {
      // emit CreateNode, insert, return
  }
  continue;                                            // someone beat us: retry the normal path
  ```
  The `try_write` plus `continue` spin is only needed because the code does not want to hold the write lock while it re-derives state; taking the write lock unconditionally here is fine, this branch runs once per empty tree.
- **Effort:** S
- **Blast radius:** one function. No API change.

### [SEV-6] `Operation::Split` committed against a node that shrank to zero: panic with `cdc`, silent value loss without

- **ID:** `wti-full-06`
- **Severity:** Medium
- **Category:** Correctness
- **Confidence:** High on the code path, Low on real-world reachability
- **Location:** `src/concurrent/operation.rs:35-38` (`expect("node should be non empty if split")`), `src/concurrent/operation.rs:40-116`
- **What:** this is the residual half of the halve story. `halve()` no longer panics (the `len()/2` fix), but the caller still assumes the node is non-empty when the split finally commits. Between `put_cdc_checked` dropping the node guard at `src/concurrent/set.rs:222` and `commit` re-locking it at `src/concurrent/operation.rs:30`, other threads can remove entries. At `len == 0`: with `cdc` enabled the `expect` at line 35 panics; with `cdc` disabled, `entry.remove()` has already happened, both `guard.max()` and `new_vec.max()` are `None`, neither half is re-inserted into the index, and the value being inserted is dropped on the floor while `insert()` returns `true`. At `len == 1` the code is correct (the lower half ends up empty and is intentionally not re-indexed).
- **Why it matters:** requires all `node_capacity` (default 1024) entries to be removed inside a very short window, so it is unlikely in production. But the failure modes are a panic inside a lock-holding path and a silent lost write, both of which are unacceptable in an index, and the same shape is what produced the original halve panic. Note WorkTable's own `UnsizedNode::halve` (`WorkTable/src/index/unsized_node.rs:80-82`) starts with `self.max().unwrap()`, so it panics on the same empty-node condition.
- **Fix:** make `commit` handle the degenerate case explicitly rather than by `expect`: if `guard.len() < 2`, skip the split, re-insert the node under its current max (or drop it if empty), insert `value` into it, and return the corresponding events. Roughly ten lines, and it removes both the panic and the lost write.
- **Effort:** S
- **Blast radius:** `Operation::Split::commit` only.

### [SEV-7] The fix this fork exists for has no regression test

- **ID:** `wti-full-07`
- **Severity:** Medium
- **Category:** AI-smell / Maintainability
- **Confidence:** High
- **Location:** `src/lib.rs:3839-3860` (`test_halve`), `src/core/node.rs:145-157`
- **What:** `test_halve` fills a node to exactly `DEFAULT_INNER_SIZE`, so `len == capacity` and `len/2 == capacity/2 == DEFAULT_CUTOFF`. It passes identically before and after the fix. Nothing in the suite exercises `capacity() > len()`, which is the entire regression. `git log` confirms `d08f06a "Fix halve() panicking when node capacity exceeds len"` touched `src/core/node.rs` and `Cargo.toml` and no test file.
- **Why it matters:** an upstream rebase that reverts to `capacity()/2` will be green in CI. The fork's whole reason for existing is unprotected.
- **Fix:** add to the `core::node` test module:
  ```rust
  #[test]
  fn halve_splits_on_len_not_capacity() {
      let mut node: Vec<usize> = Vec::with_capacity(1024);
      for i in 0..1024 { NodeLike::insert(&mut node, i); }
      for i in 0..1000 { NodeLike::delete(&mut node, &i); }   // len 24, capacity 1024
      let upper = node.halve();                                // upstream panics here
      assert_eq!(node.len(), 12);
      assert_eq!(upper.len(), 12);
      assert!(node.last().unwrap() < upper.first().unwrap());
  }
  ```
- **Effort:** S
- **Blast radius:** test only.

### [SEV-8] Insert into a transiently empty node never fixes the index key and emits a `CreateNode`-less `InsertAt`

- **ID:** `wti-full-08`
- **Severity:** Medium
- **Category:** Correctness (cdc)
- **Confidence:** Medium (read from code, timing dependent)
- **Location:** `src/concurrent/set.rs:182-210` (the `old_max.is_some()` gate), `src/concurrent/set.rs:403-415` (`MakeUnreachable` scheduling)
- **What:** when `remove_cdc` empties a node it drops both guards before taking the write lock, so there is a window in which an empty node is still reachable through the index. An insert landing there takes `old_max = None`, emits `InsertAt { max_value: value }` (via `old_max.clone().unwrap_or(value.clone())` at line 192) and then returns early at line 209 without scheduling `UpdateMax`, so the index key is never corrected. `MakeUnreachable` subsequently fails its `new_max < Some(old_max)` test and returns `Err`, which the caller ignores.
- **Why it matters:** the in-memory tree survives, because a node key that is *greater* than the node's max is still lookup-correct, and the `index.back()` fallback in `locate_node` covers the last-node case. The cdc stream does not survive: the consumer receives an `InsertAt` naming a node max it has never seen a `CreateNode` for, right after having applied the `RemoveAt` that emptied the node it does know about. `WorkTable/src/persistence/space/index/mod.rs` replays these into the persisted space index, so this is a replica divergence, in the same family as the duplicate-key reload bug fixed in WorkTable PR #175.
- **Fix:** do not leave an empty node reachable. Either hold the node guard across the `MakeUnreachable` commit (changing the lock order to node-then-index, which needs care), or mark the node as tombstoned so inserts skip it and retry, or, minimally, in the `old_max.is_none()` case schedule an operation that re-keys the node to its new max and emits `CreateNode` before the `InsertAt`.
- **Effort:** M (needs a decision on the locking shape)
- **Blast radius:** `put_cdc_checked`, `remove_cdc`, `Operation::MakeUnreachable`, cdc consumers.

### [SEV-9] `Ref` holds the node mutex, so a `get` still in scope deadlocks the next write to that node

- **ID:** `wti-full-09`
- **Severity:** Medium
- **Category:** API design
- **Confidence:** High
- **Location:** `src/concurrent/ref.rs:5-14`, `src/concurrent/set.rs:500-519`, `src/concurrent/map.rs:223-229`
- **What:** `BTreeMap::get` returns a `Ref` that owns an `ArcMutexGuard` over the node. `parking_lot::Mutex` is not reentrant, so `let r = map.get(&k); map.insert(k2, v);` self-deadlocks whenever `k2` routes to the same node, which for a 1024-wide node is common. Nothing in the doc comment on `get` mentions that the return value is a lock guard; the doc example (`map.get(&1).and_then(|e| Some(e.get().value))`) drops it immediately and hides the hazard.
- **Why it matters:** a single-threaded deadlock reachable by an obvious usage pattern, with no compile-time or runtime signal. It also means a `Ref` held across an `.await` in async consumer code blocks every writer to that node.
- **Fix:** at minimum document it loudly on `get`, `Ref` and `Ref::get`. Better: since `T: Clone` is already required, offer `get_cloned(&self, k) -> Option<T>` as the recommended API and keep `Ref` for the rare zero-copy case.
- **Effort:** S for docs plus the cloning accessor.
- **Blast radius:** additive.

### [SEV-10] Full iteration is quadratic in node count: a linear scan of the index at every node boundary

- **ID:** `wti-full-10`
- **Severity:** Medium
- **Category:** Performance
- **Confidence:** High
- **Location:** `src/concurrent/set.rs:675-682` (forward), `src/concurrent/set.rs:833-840` (backward)
- **What:** to advance to the next node, `Iter` does `self.tree.index.iter().find(|e| Arc::ptr_eq(e.value(), current))`, a full scan of the skiplist from the front, then `.next()`. So walking `m` nodes costs `O(m^2)` skiplist steps plus `m^2` `Arc` pointer comparisons, on top of one mutex acquire/release per node.
- **Why it matters:** at the default `DEFAULT_INNER_SIZE = 1024`, a 10M-row index has ~10k nodes, so a full scan performs ~50M skiplist hops purely to find where it already is. The README claims iteration is "As fast to iterate as a vec" and "twice as fast" as stdlib; those numbers come from `benches/stdlib.rs` on the single-threaded tree, and they do not describe this iterator at all.
- **Fix:** keep the `crossbeam_skiplist::map::Entry` for the current node instead of the `Arc`, and advance with `entry.next()`. The entry is already in hand at every point where the code stores the `Arc` (`src/concurrent/set.rs:592-593, 684, 842`). The only reason to re-find it is that the node may have been removed from the index concurrently, and `Entry::next()` already handles removed entries. This turns iteration into `O(m log m)` at worst and removes the `Arc::ptr_eq` scan entirely.
- **Effort:** M (the iterator state machine is fiddly, 6 transmute sites interact with it; do it together with `wti-full-01`)
- **Blast radius:** `concurrent::set::Iter` internals only, no API change.

### [SEV-11] `len()` and `capacity()` lock every node in the tree

- **ID:** `wti-full-11`
- **Severity:** Medium
- **Category:** Performance
- **Confidence:** High
- **Location:** `src/concurrent/set.rs:520-534`, exposed as `concurrent::map::len/capacity` and `concurrent::multimap::len/capacity`
- **What:** `len()` iterates the index and acquires each node's mutex to sum lengths. It is `O(nodes)` mutex acquisitions, it blocks writers to every node it touches, and it is not a snapshot (concurrent mutation makes the sum arbitrary). It is also not taken under `index_lock`, so nodes can be split under it and be counted twice or zero times.
- **Why it matters:** a 10M-row table means 10k lock acquisitions per `len()` call. Anything that calls `len()` per request, or in an assertion in a hot path, serialises against the whole tree. `wti-full-02` also demonstrates that `len()` can disagree with `iter().count()` permanently once the tree is corrupted, so callers cannot even trust it as an approximation.
- **Fix:** maintain an `AtomicUsize` updated on the insert/remove commit paths (they already run under `index_lock.write()` for the interesting cases and under the node lock otherwise), and have `len()` read it `Relaxed`. Keep the current traversal as a debug-only `len_exact()`.
- **Effort:** M
- **Blast radius:** additive plus one field; `len()` becomes approximate under concurrency, which it already is.

### [SEV-12] `attach_node` panics on an empty node and silently replaces a node with an equal max

- **ID:** `wti-full-12`
- **Severity:** Medium
- **Category:** Correctness (reload path)
- **Confidence:** Medium
- **Location:** `src/concurrent/set.rs:126-133`
- **What:**
  ```rust
  let node_id = node.max().cloned().expect("node should contain at least one value to be correct node");
  let _global_guard = self.index_lock.write();
  self.index.insert(node_id, Arc::new(Mutex::new(node)));
  ```
  There is no validation that the node is sorted, no check that its max does not collide with an existing key, and `SkipMap::insert` replaces on collision, so a colliding node is dropped without a trace. An empty node panics.
- **Why it matters:** this is the reload entry point. `WorkTable/src/persistence/space/index/mod.rs:293` and `unsized_.rs:250` call it in a loop over persisted pages. If persistence ever produced two nodes with the same max, which `wti-full-02` and `wti-full-04` can both cause, reload silently loses one whole page of index entries and no error surfaces. That is exactly the failure class of the duplicate-key reload bug WorkTable just fixed.
- **Fix:** return `Result` instead of panicking, reject empty nodes, reject a node whose max already exists (or merge), and debug-assert sortedness. `attach_node` is not on a hot path, so the checks are free.
- **Effort:** S (plus a small change at the two WorkTable call sites if it becomes fallible)
- **Blast radius:** `attach_node`, `attach_multi_node`, two WorkTable call sites.

### [SEV-13] `Operation::UpdateMax` has two identical arms and an always-empty cdc vector

- **ID:** `wti-full-13`
- **Severity:** Low
- **Category:** AI-smell
- **Confidence:** High
- **Location:** `src/concurrent/operation.rs:124-152`
- **What:** the `Greater` and `Less` arms of `new_max.cmp(&old_max)` contain byte-identical bodies (`index.remove(&old_max); index.insert(new_max.clone(), node.clone()); (None, cdc)`), and `let cdc = vec![]` is constructed, never pushed to, and returned. The `Ok(...)` return type therefore promises events that this operation can never produce.
- **Why it matters:** it reads as if `UpdateMax` might emit cdc and might treat growth and shrinkage differently, so the next reader spends time working out why it does not. The re-keying is intentionally invisible to cdc (the consumer re-derives the max from the `InsertAt`/`RemoveAt` it already applied), and that reasoning is nowhere in the code.
- **Fix:** collapse to `if new_max != old_max { remove; insert; }`, drop the `cdc` local, and write the one-sentence comment explaining why re-keying needs no event.
- **Effort:** S
- **Blast radius:** one function. Note the minimal-diff-to-upstream policy in `AGENTS.md`: this is inherited code, so weigh the churn against the clarity. The comment alone would be enough.

### [SEV-14] README describes upstream `indexset`, not this fork, and overstates concurrent iteration

- **ID:** `wti-full-14`
- **Severity:** Low
- **Category:** Docs
- **Confidence:** High
- **Location:** `README.md:1-7` (title `# indexset`, crates.io and docs.rs badges pointing at `indexset`), `README.md:47` ("As fast to iterate as a vec"), `README.md:65-67` (complexity claims)
- **What:** the README is upstream's, unmodified. It is the file crates.io renders for the published `WorkTablesIndex` package (`Cargo.toml:11`), so the published page carries badges for a different crate and never mentions that this is pathscale's fork or what the fork changes. `AGENTS.md` documents all of that; the README, which is the artefact users actually see, does not. Separately, "As fast to iterate as a vec" is false for the concurrent iterator (see `wti-full-10`), and the complexity list omits the `FenwickTree::from_iter` rebuild that every split and every node removal performs (`src/lib.rs:354, 360, 478`), which is `O(nodes)` per event.
- **Why it matters:** the next person to pick this up reads the README first and gets a wrong mental model of both provenance and performance.
- **Fix:** add a short "This is a fork" header block with the halve fix, the package alias, and the exact-pin rule, fix the badges, and qualify the iteration claims as single-threaded only.
- **Effort:** S
- **Blast radius:** docs.

### [SEV-15] No property or fuzz testing against a reference implementation

- **ID:** `wti-full-15`
- **Severity:** Medium
- **Category:** Maintainability
- **Confidence:** High
- **Location:** whole test suite (`src/lib.rs:3812+`, `src/concurrent/set.rs:1321+`, `src/concurrent/map.rs:445+`, `src/concurrent/multimap.rs:585+`)
- **What:** every test is example-based with hand-written expected values. There is no `proptest`/`quickcheck` dependency, no `loom`, no fuzz target, no Miri in CI. The cdc tests do compare against a `PersistedBTreeMap` mock (`src/concurrent/map.rs:483-561`), which is the right idea, but only for cdc replay and only on fixed sequences. Note that all three Critical findings above would have been caught by the property test sketched below, and two of them by an hour of Miri.
- **Why it matters:** for an ordered index this is the single highest-value test type, because the oracle is free: `std::collections::BTreeMap` already defines every answer.
- **Fix:** add `proptest` as a dev-dependency and one test per structure:
  ```rust
  #[derive(Debug, Clone)]
  enum Op { Insert(u16, u16), CheckedInsert(u16, u16), Remove(u16), Get(u16), Len, Iter, Range(u16, u16) }

  proptest! {
      #[test]
      fn matches_std_btreemap(ops in prop::collection::vec(any_op(), 1..2000),
                              node_size in 2usize..8) {
          let subject = BTreeMap::<u16, u16>::with_maximum_node_size(node_size);
          let mut oracle = std::collections::BTreeMap::<u16, u16>::new();
          for op in ops {
              match op {
                  Op::Insert(k, v)  => prop_assert_eq!(subject.insert(k, v), oracle.insert(k, v)),
                  Op::CheckedInsert(k, v) => {
                      let got = subject.checked_insert(k, v).is_some();
                      let want = !oracle.contains_key(&k);
                      prop_assert_eq!(got, want);            // fails today: wti-full-02
                      if want { oracle.insert(k, v); }
                  }
                  Op::Remove(k)     => prop_assert_eq!(subject.remove(&k).map(|(_, v)| v), oracle.remove(&k)),
                  Op::Get(k)        => prop_assert_eq!(subject.get(&k).map(|r| r.get().value), oracle.get(&k).copied()),
                  Op::Len           => prop_assert_eq!(subject.len(), oracle.len()),
                  Op::Iter          => prop_assert_eq!(subject.iter().map(|(k,v)|(*k,*v)).collect::<Vec<_>>(),
                                                       oracle.iter().map(|(k,v)|(*k,*v)).collect::<Vec<_>>()),
                  Op::Range(a, b)   => { let (lo, hi) = (a.min(b), a.max(b));
                                         prop_assert_eq!(subject.range(lo..hi).map(|(k,_)|*k).collect::<Vec<_>>(),
                                                         oracle.range(lo..hi).map(|(k,_)|*k).collect::<Vec<_>>()); }
              }
              // structural invariants, checked after every op
              prop_assert_eq!(subject.len(), subject.iter().count());          // fails today: wti-full-02
              for e in subject.set.index.iter() {
                  let g = e.value().lock();
                  prop_assert!(g.len() > 0);
                  prop_assert_eq!(g.max().unwrap(), e.key());                   // node key == node max
                  prop_assert!(g.iter().is_sorted());
              }
          }
      }
  }
  ```
  `node_size in 2..8` is what makes this find split bugs in seconds instead of after 1024 inserts. Add the equivalent for `BTreeSet` (oracle `std::collections::BTreeSet`) and for `BTreeMultiMap` (oracle `BTreeMap<K, Vec<V>>`), and a cdc property: replaying the event stream into `PersistedBTreeMap` must equal the live index after an arbitrary op sequence.
- **Effort:** M for the three property tests, L including a `loom` model of the insert/remove/split interleavings.
- **Blast radius:** test-only, plus a dev-dependency.

## Appendix A: every `unsafe` block in the crate

`rg 'unsafe' src/` returns 9 blocks in 3 files. There are no `unsafe impl Send`/`Sync`, no `UnsafeCell`, no `static mut`, and no `unsafe fn` in the public API.

| # | Location | What it does | Stated invariant | Enforced? |
|---|----------|--------------|------------------|-----------|
| 1 | `src/core/node.rs:54-72` | Raw pointer binary search: `(*p.add(m)).borrow().cmp(needle)` | `m` must be in bounds | **Yes, by construction.** `m = (i+j)>>1` with `i < j <= len`, so `m <= j-1 < len`. Empty slice: `j = 0`, loop body never runs. Sound, but it buys nothing that `get_unchecked` would not, and nothing over safe indexing that LLVM cannot already prove. No comment justifies it. Recommend replacing with the safe form and measuring. |
| 2 | `src/core/multipair.rs:24` | `MaybeUninit::uninit().assume_init()` for `value: V` in `with_infimum` | None stated | **No. Unsound.** See `wti-full-03`. Immediate UB; aborts for `V: Drop`. |
| 3 | `src/core/multipair.rs:27` | Same, in `with_supremum` | None stated | **No. Unsound.** Same as above. |
| 4 | `src/concurrent/set.rs:641-648` | `transmute` a `slice::Iter<'guard, T>` to `slice::Iter<'a, T>` (front, first node) | Implicit: the guard stored in the same struct outlives the iterator | **Partially.** The guard is dropped before the iterator field in the struct's declared order, and no code dereferences the stale iterator between the two assignments, so *internally* it is fine. What is not enforced is the escape of `&'a T` to the caller: see `wti-full-01`. |
| 5 | `src/concurrent/set.rs:702-709` | Same (front, node advance) | Same | **Partially**, same reasoning. |
| 6 | `src/concurrent/set.rs:731-738` | Same (front, guard re-acquire) | Same | **Partially**, same reasoning. |
| 7 | `src/concurrent/set.rs:799-806` | Same (back, first node) | Same | **Partially**, same reasoning. |
| 8 | `src/concurrent/set.rs:860-867` | Same (back, node advance) | Same | **Partially**, same reasoning. |
| 9 | `src/concurrent/set.rs:889-896` | Same (back, guard re-acquire) | Same | **Partially**, same reasoning. |

None of the nine carries a `// SAFETY:` comment. Blocks 4 through 9 are six copies of the same five-line expression and should be one `fn iter_of(guard: &ArcMutexGuard<..>) -> slice::Iter<'a, T>` helper with a single SAFETY note, whatever is decided about `wti-full-01`.

## Appendix B: classic index bugs, checked one by one

| Question | Verdict |
|----------|---------|
| Off-by-one in split | **Found one.** `if max > value` at `src/concurrent/operation.rs:54` misroutes a value equal to the lower half's max. `wti-full-02`. |
| Off-by-one in merge/rebalance | Not applicable: neither tree merges or rebalances. Nodes shrink freely and are only unlinked at length 0 (`src/lib.rs:473-481`, `Operation::MakeUnreachable`). A delete-heavy workload permanently leaves sparse nodes; that is a documented design choice, not a bug, but it means node count never recovers and lookups keep paying for nodes that are nearly empty. |
| Duplicate key handling | **Two bugs.** `wti-full-02` (split path) and `wti-full-04` (multimap `Ord`). The non-split path is correct: `put_cdc_checked` returns `Err` and `put_cdc` replaces. |
| Does `Ord` match serialized byte order? | Not this crate's concern: it never serializes keys itself (`serde` is derive-only on `Pair`/`MultiPair`/single-threaded trees, `Cargo.toml:20,31`). The on-disk ordering contract lives in WorkTable's space index. Worth confirming there that `Pair::cmp`, which delegates to `K::cmp` (`src/core/pair.rs:38-40`), agrees with whatever byte order the index pages use. |
| NaN / partial-`Ord` keys | Cannot occur through the public API: every entry point requires `T: Ord`, and `f64` is not `Ord`. A caller wrapping a float in a type with a lying `Ord` gets the "logic error" behaviour documented at `src/lib.rs:30-35`. `MultiPair` however *ships* a lying `Ord` in-crate: `wti-full-04`. |
| Empty and single-element edge cases | Empty tree: covered by `test_empty_set` and correct. Single element: covered. Empty *node* reachable through the index: two real holes, `wti-full-06` (split) and `wti-full-08` (insert into an emptied node). `attach_node` panics on an empty node, `wti-full-12`. |
| Iterator invalidation during mutation | **Unsound**, `wti-full-01`. Beyond the memory safety issue, the iterator gives no consistency guarantee at all: it re-reads `tree.index` at every node boundary, so a concurrent split can cause elements to be visited twice or skipped. That is defensible for a lock-free-ish structure but it is undocumented; `iter()`'s doc comment shows only single-threaded examples. |
| Index and data disagreeing after crash or reload | The cdc stream is the reconciliation mechanism and it has two divergence sources: `wti-full-02` (duplicate replicated into the persisted node) and `wti-full-08` (`InsertAt` for an unknown node max). Reload itself, `attach_node`, validates nothing: `wti-full-12`. Note the split event carries `split_index: guard.len()` computed *after* `halve()` (`src/concurrent/operation.rs:40-47`), so the consumer reproduces whatever split point the producer chose. That is good design and it means the `len()/2` fix did **not** desync cdc replay. |
| Lock ordering | Consistent: `index_lock` then node mutex, everywhere it matters (`put_cdc_checked`, `remove_cdc`, `remove_range`, `Range::new`, `Operation::commit`). `Iter` takes node locks without `index_lock`, which is a weaker order, not an inverted one. I found no lock-order inversion. The reachable deadlock is re-entrancy, not ordering: `wti-full-09`. |
| Atomic orderings | Only one atomic, `event_id: AtomicU64`, always `fetch_add(1, AcqRel)` (`src/concurrent/set.rs:162,191,235,250,274,279,293,300,311,318,385,419`). `AcqRel` is stronger than needed (`Relaxed` would do, since every use is already inside a node or index lock that provides the ordering) but it is correct. The comments claiming "current thread is the only that can fetch event_id" (for example `src/concurrent/set.rs:161`) are **false as stated**: other threads holding other node locks fetch concurrently. The property that actually holds, and that the tests check, is per-node monotonicity plus global gap-freedom. Worth rewording, the current comment invites a wrong optimisation. |
| TOCTOU between lookup and access | `wti-full-05` (empty index), `wti-full-06` (split), `wti-full-08` (emptied node). All three are the same shape: state observed under the read guard, acted on after re-acquiring, without revalidation. |

## Cross-cutting recommendations

1. **Decide the iterator's contract, then fix `wti-full-01` and `wti-full-10` in one pass.** They live in the same 300 lines of `Iter`/`next_back` state machine, and both fixes touch the six transmute sites. Plan: change `Item` to `T` (owned, `T: Clone` is already required), replace the stored `Arc<Mutex<Node>>` with the skiplist `Entry` so advancement is `entry.next()` instead of a linear `find`, and delete all six transmutes. What breaks: every caller that binds `&T` from these iterators, including this crate's own tests and WorkTable's index scans. It is a breaking change, so it needs the coordinated version bump that `AGENTS.md` describes.

2. **Add the reference-oracle property tests before anything else (`wti-full-15`), with a tiny `node_size`.** They cost a few hours, they reproduce `wti-full-02` in seconds, and they are the only mechanism that will keep an upstream rebase from silently reintroducing any of this. Pair them with a Miri job over the non-threaded tests, which catches `wti-full-03` immediately, and gate both in `rust.yml` next to the existing clippy job.

3. **Establish the node invariant in one place and assert it.** The tree has exactly one structural invariant, "the index key of a node equals that node's max, and every value in the node is greater than the previous node's key", and four separate code paths can break it (`wti-full-02`, `wti-full-05`, `wti-full-08`, `wti-full-12`). A `#[cfg(debug_assertions)] fn check_invariants(&self)` called at the end of every mutating operation, plus the property test above, converts all four from "silent corruption in production" to "test failure in CI". This is the single highest-leverage change in the list.

4. **Split the fork's changes from upstream's code physically.** The `AGENTS.md` minimal-diff policy is sound, but it currently conflicts with fixing anything: every fix here is a diff against upstream. Suggestion: keep upstream files untouched where possible and put pathscale fixes in clearly-marked blocks with a `// pathscale:` prefix and the issue id, so a rebase conflict is obviously ours. Also send `wti-full-03` and `wti-full-04` upstream, since they are upstream bugs that pathscale does not want to carry forever.

5. **Reconcile the cdc comments and the cdc contract.** The "current thread is the only that can fetch event_id" comments are wrong, `UpdateMax` silently emits nothing by design, and the emptied-node case emits an event the consumer cannot apply. Write down the actual contract (what identifies a node, when a node's identity changes, which events the consumer must be able to receive) in `src/cdc/change.rs` next to `ChangeEvent`, then make `wti-full-08` a violation of a written rule rather than an undiscovered edge case.

6. **Make `len()` cheap and the README honest (`wti-full-11`, `wti-full-14`).** Both are small, both mislead consumers today, and the README is the published crates.io front page for a package whose name does not match the crate the badges point at.

## What I did not cover

- **The single-threaded `BTreeSet`/`BTreeMap` in `src/lib.rs` got a skim, not an audit.** I read the structure (`locate_node`, `locate_value`, `locate_ith`, `insert_at`, `delete_at`, `split_off`) and the split/delete arithmetic, which looked correct, and I confirmed the `FenwickTree` rebuild pattern. I did not read the ~1800 lines of `BTreeMap`, the `Entry` API, `IterMut`, or the set-algebra iterators (`Union`, `Difference`, `SymmetricDifference`, `Intersection`) line by line. WorkTable uses the concurrent variants exclusively, so I spent the budget there. The proptest in `wti-full-15` would cover this gap cheaply.
- **No `loom`/`shuttle` model and no thread sanitizer run.** Findings 5, 6 and 8 are reasoned from the code with the interleaving spelled out, not observed. They are marked Medium confidence for that reason. Someone should confirm them with a model checker before rewriting the locking.
- **I did not run the heavy tier** (`test_concurrent_insert` at 128 threads x 10k ops, `test_remove_stress`, `parallel_iter_and_mut`, `test_concurrent_cdc_no_gaps`). The fast tier's 62 tests pass in 0.30s. Note that `parallel_iter_and_mut` (`src/concurrent/set.rs:1957`) spawns a reader thread and never joins it, so it asserts nothing and can end before the reader starts; it is not evidence of concurrent-iteration safety.
- **I did not verify WorkTable's side of the contract**, only how it calls this crate. In particular I did not check whether WorkTable's cdc consumer would panic or corrupt on the divergent event streams in `wti-full-02` and `wti-full-08`. That is the natural follow-up and it decides whether those two are Critical or Medium *for the product*.
- **Benchmarks were read, not run.** `benches/concurrent.rs:20-36` carries a commented-out duplicate of `generate_operations`, dead code that should go.
- **Supply chain and security lenses are near-empty here by nature.** No network input, no I/O, no secrets, no shell-outs, no serialization of untrusted data (serde is derive-only and optional). Five direct dependencies, all pinned in `Cargo.lock`, all well-known (`crossbeam-*`, `parking_lot`, `ftree`, `fastrand`, `serde`). The security-relevant surface of this crate is exactly its memory safety, which is findings 1 and 3.

## Quick-start for the follow-up agent

Read in this order:

1. `AGENTS.md` (5 min). Explains why this fork exists, the exact-pin rule with `worktable`/`data_bucket`, the minimal-diff-to-upstream policy, and the two-tier test split. Judge nothing before reading it.
2. `src/core/node.rs:135-244`. The whole leaf abstraction, including the `halve()` fix that is the reason for the fork.
3. `src/concurrent/set.rs:135-262` (`put_cdc_checked`). The heart of the crate: the read-lock, node-lock, write-lock retry dance that every insert goes through. Findings 2, 5 and 8 all live in or start here.
4. `src/concurrent/operation.rs` (182 lines, whole file). The three structural mutations. Finding 2's off-by-one is at line 54, finding 6's panic at line 35.
5. `src/concurrent/set.rs:609-925` (`Iter`/`next_back`). The six transmutes, findings 1 and 10.
6. `src/core/multipair.rs` (whole file, 90 lines). Findings 3 and 4, both in the first 60 lines.
7. `/Users/revenge/code/WorkTable/src/index/primary_index.rs:54-110` (read-only). Shows exactly what `checked_insert` is being asked to guarantee.

Commands:

```bash
cd /Users/revenge/code/WorkTablesIndex
cargo check --all-features --all-targets                  # ~1s warm, clean at 8c7465d
cargo test --all-features --lib --tests -- \
  --skip test_concurrent_insert --skip test_remove_stress \
  --skip parallel_iter_and_mut --skip test_concurrent_cdc_no_gaps   # fast tier, 62 tests, 0.30s
cargo test --all-features --lib --tests                   # heavy tier, can livelock on <4 cores
cargo clippy --all-targets --all-features -- -D warnings  # allow-list is in Cargo.toml [lints]
cargo bench --bench stdlib --all-features                 # single-threaded numbers
```

Reproductions for findings 1, 2, 3 and 4 are in `/private/tmp/claude-501/-Users-revenge-code/4526fe76-867f-4d6a-a325-84ff907ebbb8/scratchpad/uaf/` (a throwaway crate with `indexset = { package = "WorkTablesIndex", path = "/Users/revenge/code/WorkTablesIndex", features = ["concurrent","cdc","multimap"] }`). That directory is scratch and outside the repo; re-create it from the snippets in this document if it has been cleaned up. Nothing in the repository was modified by this review other than this file.

Surprises about the layout and conventions:

- Doctests are excluded from CI on purpose and **must stay that way**: every doc example says `use indexset::...`, which is correct for consumers (they alias the package back to `indexset`) but unresolvable under this crate's own name. Do not "fix" them, `AGENTS.md` calls this out explicitly.
- The `[lints]` tables in `Cargo.toml:58-97` are a frozen snapshot of upstream's lint debt, not a style preference. Do not add allows for new code.
- `target/package/WorkTablesIndex-0.0.1/` contains a stale copy of every source file. `rg` without `src/` will match it twice; scope your searches.
- `git log` before `3e848e0` is upstream brurucy/indexset history. The only pathscale source change in the whole fork is `d08f06a` (the halve fix); everything else is CI, docs and the rename.
- Any change to split or merge behaviour changes WorkTable's persisted geometry and requires regenerating `tests/data/expected/space_index/indexset/...` in the WorkTable repo, shipped in the same change. Fixes 2 and 4 in this document both need that check.

## Nits

- `src/concurrent/operation.rs:132` `let cdc = vec![];` in `UpdateMax` is constructed, never written, returned. See `wti-full-13`.
- `src/core/constants.rs:2-5` `CUTOFF_RATIO` and `DEFAULT_CUTOFF` are `#[allow(dead_code)]` and used only by `test_halve`. After `wti-full-07` lands they are genuinely dead.
- `src/core/node.rs:6-43` every one of the 14 `NodeLike` trait methods carries its own `#[allow(dead_code)]`. One `#[allow(dead_code)]` on the trait would do; better still, work out which are actually unused under which feature set.
- `benches/concurrent.rs:20-36` a commented-out earlier version of `generate_operations` sits directly above the live one.
- `src/concurrent/map.rs:733-734` two commented-out assertions in `test_concurrent_insert_cdc` (`assert_eq!(mock_state.len(), expected_values.len())`). Either the invariant holds and should be asserted, or it does not and that deserves a comment.
- `src/concurrent/set.rs:180-181` `#[allow(unused_assignments)] let mut operation = None;` followed by `operation.unwrap()` at line 227. The `Option` is doing the job a two-arm `match` producing an `Operation` would do without the unwrap.
- `src/concurrent/multimap.rs:269-276` `raw_get` and `get` (lines 293-302) are the same six lines twice, differing only in the wrapper type. `get` could be `Range { inner: self.raw_get(key) }`.
- `src/concurrent/multimap.rs:486-497` the `node_count` doc example says `use indexset::concurrent::map::BTreeMap` and builds a `BTreeMap`, copy-pasted from `map.rs` into the multimap.
- `src/concurrent/set.rs:1637` `let expected_len = expected_len;` a self-assignment left in `test_remove_range`.
- `src/core/pair.rs:49-53` `impl<V> Borrow<str> for Pair<String, V>` is a one-off for `String` keys; `&[u8]`/`Vec<u8>` keys would need the same hand-written impl. Worth a note if WorkTable ever indexes byte-string keys.
- `Cargo.toml:8` the description ("A two-level BTree with fast iteration and indexing operations") is upstream's and does not mention the fork, matching `wti-full-14`.

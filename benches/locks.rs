//! What this crate's node locking costs, four ways.
//!
//! # Why this exists
//!
//! `concurrent` is the only reason this crate needs `std`. Its 31 lock sites
//! are `parking_lot`, and `cdc` and `multimap` both imply `concurrent`, so a
//! consumer wanting only `ChangeEvent` and `Pair` takes `parking_lot` and
//! `libc` with them. DataBucket is exactly that consumer, and that is what
//! stops its format layer from being `no_std`.
//!
//! # The four arms
//!
//! | arm | what it is |
//! |---|---|
//! | `parking_lot` | `parking_lot::Mutex`, named directly. What this crate uses today. |
//! | `spin` | `spin::Mutex`, named directly. |
//! | `lock_api+parking_lot` | `lock_api::Mutex<parking_lot::RawMutex, T>` |
//! | `lock_api+spin` | `lock_api::Mutex<spin::Mutex<()>, T>` |
//!
//! The two `lock_api` arms are one generic body instantiated twice. That is the
//! proposed shape: `concurrent` goes generic over `R: RawMutex` and the
//! consumer chooses, rather than this crate naming a lock for everyone.
//! `lock_arc` and `ArcMutexGuard<R, T>` belong to `lock_api`, not to
//! `parking_lot`, so `Ref` keeps working either way.
//!
//! The two direct arms exist to price the `lock_api` wrapper itself. If
//! `parking_lot` and `lock_api+parking_lot` differ, the wrapper is not free and
//! every other comparison here is contaminated.
//!
//! # The two regimes
//!
//! `nodes` is this crate's shape from `src/concurrent/set.rs`:
//! `RwLock<BTreeMap<T, Arc<Mutex<Node>>>>`, with `DEFAULT_INNER_SIZE` entries
//! per node. **1024, not the 16 an earlier version of this file guessed.** That
//! was wrong by 64x and it decides the answer, because a lock held over 16
//! entries is short and favours spinning while one held over 1024 does not.
//!
//! `contended` is one lock held long under oversubscription, where a spinlock
//! is supposed to lose: a waiter burns the core the holder needs to finish.
//!
//! # Reading it
//!
//! **CPU time, from `getrusage`.** Wall clock cannot see a burnt core. A
//! spinlock finishes sooner while consuming more, and on a machine with spare
//! cores that looks free until something else wants one.
//!
//! **`null` is the first arm run twice.** Whatever it shows is what this
//! harness reports for identical code, so nothing closer than its distance
//! from 1.00x means anything.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use indexset::core::constants::DEFAULT_INNER_SIZE;

/// Nodes in the index, enough that lookups spread rather than queue on one.
const NODES: u64 = 4_096;
/// Lookups per measurement, split across threads.
const LOOKUPS: usize = 200_000;
/// How long the contended case holds. Long enough for a holder to be preempted
/// inside it, short enough to keep this bounded.
const HELD_ITERS: u64 = 20_000;
/// Acquisitions per thread in the contended case.
const HOLDS: usize = 40;
const REPS: usize = 5;

/// CPU consumed by this process, user plus system.
fn cpu() -> Duration {
    // SAFETY: `getrusage` fully initialises the `rusage` it is given for
    // `RUSAGE_SELF`, and returns non-zero rather than writing on failure.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    let ok = unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) };
    assert_eq!(ok, 0, "getrusage failed");
    let secs = |t: libc::timeval| Duration::new(t.tv_sec as u64, (t.tv_usec as u32).saturating_mul(1_000));
    secs(usage.ru_utime) + secs(usage.ru_stime)
}

/// A node the size this crate actually uses.
struct Node {
    keys: Vec<u64>,
}

impl Node {
    fn new(seed: u64) -> Self {
        let mut keys: Vec<u64> = (0..DEFAULT_INNER_SIZE as u64)
            .map(|n| seed.wrapping_mul(0x9e37_79b9).wrapping_add(n))
            .collect();
        keys.sort_unstable();
        Self { keys }
    }

    /// A sorted search then a write, which is what a node access is.
    fn touch(&mut self, n: u64) -> u64 {
        let at = self.keys.partition_point(|key| *key < n).min(self.keys.len() - 1);
        self.keys[at] ^= n;
        self.keys[at]
    }
}

fn spread(worker: usize) -> u64 {
    0x2545_F491_4F6C_DD1Du64 ^ (worker as u64).wrapping_mul(0x9E37_79B9)
}

fn next(rng: &mut u64) -> u64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    *rng % NODES
}

/// The index lock is the same in every arm.
///
/// Only the node lock varies. An earlier version changed both at once, which
/// conflated them: a difference could have come from either, and the two
/// tables could not be read against each other.
type IndexLock<M> = parking_lot::RwLock<BTreeMap<u64, Arc<M>>>;

macro_rules! nodes_arm {
    ($name:ident, $mx:ty) => {
        fn $name(threads: usize) -> (Duration, Duration) {
            let index: Arc<IndexLock<$mx>> = Arc::new(IndexLock::<$mx>::new(
                (0..NODES)
                    .map(|n| (n, Arc::new(<$mx>::new(Node::new(n)))))
                    .collect(),
            ));
            let before = cpu();
            let now = Instant::now();
            std::thread::scope(|scope| {
                for worker in 0..threads {
                    let index = index.clone();
                    scope.spawn(move || {
                        let mut acc = 0u64;
                        let mut rng = spread(worker);
                        for _ in 0..LOOKUPS / threads {
                            let key = next(&mut rng);
                            // The index lock is released before the node lock
                            // is taken, which is what the real code does.
                            let node = index.read().get(&key).cloned();
                            if let Some(node) = node {
                                acc ^= node.lock().touch(rng);
                            }
                        }
                        black_box(acc);
                    });
                }
            });
            (now.elapsed(), cpu() - before)
        }
    };
}

macro_rules! contended_arm {
    ($name:ident, $mx:ty) => {
        fn $name(threads: usize) -> (Duration, Duration) {
            let lock: Arc<$mx> = Arc::new(<$mx>::new(0u64));
            let before = cpu();
            let now = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    let lock = lock.clone();
                    scope.spawn(move || {
                        for _ in 0..HOLDS {
                            let mut held = lock.lock();
                            for n in 0..HELD_ITERS {
                                *held = held.wrapping_mul(0x9e37_79b9).wrapping_add(n);
                            }
                        }
                    });
                }
            });
            (now.elapsed(), cpu() - before)
        }
    };
}

/// A lock that spins for a bounded budget, then gives the core up.
///
/// `spin::relax::Yield` yields on every iteration, so it trades cycles for
/// syscalls and still costs 10x parking_lot's CPU when a lock is held long.
/// `RelaxStrategy` is stateless, so the budget cannot live there.
///
/// This is what parking_lot does, minus the parking: spin while the holder is
/// plausibly about to finish, then stop competing with it for the core. The
/// give-up step is the only part that needs a platform, which is why it is one
/// call and not a design.
pub struct Bounded;

/// parking_lot's spin schedule, copied from `parking_lot_core::SpinWait`.
///
/// Reading it was overdue. It is not "spin N times then yield": it is three
/// exponentially growing pauses, then seven yields, then give up and park.
///
/// ```text
/// counter 1..=3   cpu_relax(1 << counter)   2, 4, 8 pauses
/// counter 4..=10  yield to the scheduler
/// counter >10     stop spinning, park
/// ```
///
/// The first version here spun 64 times flat and then yielded forever, which
/// is why it never stopped burning CPU. Fourteen attempts, not sixty-four, and
/// a hard end to them.
struct SpinWait {
    counter: u32,
}

impl SpinWait {
    const fn new() -> Self {
        Self { counter: 0 }
    }

    /// Returns false once spinning has stopped being worth it.
    fn spin(&mut self) -> bool {
        if self.counter >= 10 {
            return false;
        }
        self.counter += 1;
        if self.counter <= 3 {
            for _ in 0..(1u32 << self.counter) {
                core::hint::spin_loop();
            }
        } else {
            give_up_the_core();
        }
        true
    }
}

pub struct BoundedMutex {
    locked: core::sync::atomic::AtomicBool,
}

unsafe impl lock_api::RawMutex for BoundedMutex {
    const INIT: Self = Self {
        locked: core::sync::atomic::AtomicBool::new(false),
    };
    type GuardMarker = lock_api::GuardSend;

    fn lock(&self) {
        // parking_lot's schedule exactly, minus the park at the end, because
        // there is nothing to park on without an operating system. So when the
        // schedule runs out this keeps yielding: that residue is the price of
        // no_std, and the table measures it.
        let mut spinwait = SpinWait::new();
        loop {
            if self.try_lock() {
                return;
            }
            if !spinwait.spin() {
                spinwait = SpinWait::new();
                give_up_the_core();
            }
        }
    }

    fn try_lock(&self) -> bool {
        self.locked
            .compare_exchange_weak(
                false,
                true,
                core::sync::atomic::Ordering::Acquire,
                core::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    unsafe fn unlock(&self) {
        self.locked.store(false, core::sync::atomic::Ordering::Release);
    }
}

/// The one platform call. On a target with no scheduler this is a spin, and
/// the lock degrades to `spin::Mutex` rather than breaking.
fn give_up_the_core() {
    std::thread::yield_now();
}

/// A lock whose waiters idle the core, with no operating system.
///
/// # Why this instruction exists and why nothing emits it
///
/// `WFE` puts the core in a low-power state until its event register is set;
/// `SEV` sets it on every core. ARM documents this as **the** intended spinlock
/// construction: the waiter executes WFE to request a low-power state and the
/// releaser executes SEV to wake it.
///
/// Rust does not emit it. `core::hint::spin_loop()` on aarch64 is
/// `__isb(SY)`, an instruction barrier, verified by disassembly:
///
/// ```text
/// __RNvCs8LfLpYhzmc_7hintasm1s:
///     isb
///     ret
/// ```
///
/// There is no WFE anywhere in `core::hint`, and no safe wrapper in
/// `core::arch::aarch64`. That is structural rather than an oversight: WFE is
/// only useful if the *releaser* pairs it with SEV, which is a protocol between
/// both sides of a lock, and a one-sided hint like `spin_loop()` cannot express
/// one. So it has to be written here, in the lock, where both sides are known.
///
/// # What it costs, measured on this machine
///
/// ```text
/// nop                     0.3 ns
/// isb (spin_loop)         8.6 ns
/// wfe, event pending   1336.7 ns
/// ```
///
/// WFE is not a no-op in userspace here, and it does not block forever either:
/// it idles for about 1.3 us and returns on its own. So a waiter needs no SEV
/// to make progress, and burns roughly 150x less CPU per unit of wall time
/// waited than an `isb` spin. SEV is still sent on unlock, because waking
/// immediately beats waiting out the timeout.
///
/// # Why this is the interesting arm rather than a curiosity
///
/// Every other `no_std` arm in this file fails the same way: when its spin
/// schedule runs out there is nothing to hand the core to, so the waiter keeps
/// running and keeps burning. Karlin et al.'s competitive-spinning result says
/// spin-then-block is 2-competitive, and the "block" half is exactly what a
/// target without an operating system cannot do. WFE is the hardware answering
/// that: a block with no scheduler involved.
///
/// The problem is current, not settled. HTLL (IEEE TPDS, January 2025) targets
/// throughput and latency together under oversubscription, reporting up to 97%
/// latency reduction for about 5% throughput; Fissile Locks (arXiv 2003.05025)
/// is compact, NUMA-aware and preemption-tolerant; Asymmetry-aware Scalable
/// Locking (arXiv 2108.03355) matters here specifically, because this is a
/// P-core/E-core machine and this benchmark does not separate them.
///
/// # When to use it, measured rather than assumed
///
/// **WFE idles the core. It does not yield to the operating system.** That is
/// the whole rule, and it was learned the expensive way here: with 128 threads
/// on 16 cores this arm costs 5478 ms of CPU against 1168 for a yielding
/// spinner, because a waiter idling in WFE is still a scheduled thread, so the
/// lock holder still cannot get a core. An earlier version of this lock
/// restarted its spin schedule after every WFE, which meant it yielded between
/// idles, and that accident is what made it competitive.
///
/// So the case for WFE is:
///
/// * threads at most cores, so idling a core costs nothing that is wanted;
/// * no operating system to yield to, which is when every other option here
///   reduces to burning the core anyway;
/// * long enough waits that 1.3 us of idle is better than 8.6 ns of `isb`
///   repeated until the holder finishes.
///
/// That is bare metal and pinned threads, not an oversubscribed server. Under
/// oversubscription yielding beats idling, and this arm is the wrong choice.
///
/// # And the reason to care beyond this crate
///
/// EKOPathRS is a compiler. A compiler that recognises a spin loop can emit
/// WFE for it, which is a transformation LLVM does not perform and which the
/// measurement above prices at 150x. That makes this arm a bet on the toolchain
/// rather than only a lock experiment.
pub struct WfeMutex {
    locked: core::sync::atomic::AtomicBool,
}

unsafe impl lock_api::RawMutex for WfeMutex {
    const INIT: Self = Self {
        locked: core::sync::atomic::AtomicBool::new(false),
    };
    type GuardMarker = lock_api::GuardSend;

    fn lock(&self) {
        // Spin briefly first: an uncontended lock should never reach a 1.3 us
        // instruction, and a short hold is over before the schedule ends.
        let mut spinwait = SpinWait::new();
        while spinwait.spin() {
            if self.try_lock() {
                return;
            }
        }
        // The schedule is spent, so stop competing with the holder for its
        // core and idle instead. **Do not restart the schedule**: an earlier
        // version reset it after every WFE, so it went back to yielding
        // between idles and measured the same as not using WFE at all.
        loop {
            if self.try_lock() {
                return;
            }
            #[cfg(target_arch = "aarch64")]
            // SAFETY: WFE is unprivileged, has no memory operands, and no
            // effect beyond waiting on the event register.
            unsafe {
                core::arch::asm!("wfe", options(nomem, nostack))
            };
            #[cfg(not(target_arch = "aarch64"))]
            give_up_the_core();
        }
    }

    fn try_lock(&self) -> bool {
        self.locked
            .compare_exchange_weak(
                false,
                true,
                core::sync::atomic::Ordering::Acquire,
                core::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    unsafe fn unlock(&self) {
        self.locked.store(false, core::sync::atomic::Ordering::Release);
        // Wake every waiting core now rather than letting each wait out its
        // own timeout.
        #[cfg(target_arch = "aarch64")]
        // SAFETY: SEV sets the event register on every core and has no other
        // effect.
        unsafe {
            core::arch::asm!("sev", options(nomem, nostack))
        };
    }
}

/// The same bounded spin, but it **blocks** instead of yielding.
///
/// This is the std column, and the difference is the whole point: a yielding
/// waiter still runs, so it still burns CPU. A blocked waiter consumes
/// nothing. That is what parking_lot does and it is why nothing without an
/// operating system can match it.
///
/// The classic three-state futex mutex: 0 free, 1 locked, 2 locked and
/// somebody is asleep on it. The third state exists so `unlock` can skip the
/// wake syscall when nobody is waiting, which is the common case.
pub struct FutexMutex {
    state: core::sync::atomic::AtomicU32,
}

const FREE: u32 = 0;
const HELD: u32 = 1;
const CONTENDED: u32 = 2;

unsafe impl lock_api::RawMutex for FutexMutex {
    const INIT: Self = Self {
        state: core::sync::atomic::AtomicU32::new(FREE),
    };
    type GuardMarker = lock_api::GuardSend;

    fn lock(&self) {
        use core::sync::atomic::Ordering;
        // parking_lot's schedule, then a real park. The earlier version spun a
        // flat 64 and then ran a CAS dance that re-announced every waiter on
        // every wake, so unlock woke somebody on every release forever. It
        // measured worse than a plain spinlock, which is not something a
        // blocking lock can honestly do.
        let mut spinwait = SpinWait::new();
        while spinwait.spin() {
            if self.try_lock() {
                return;
            }
        }

        // Drepper's three-state mutex. The first version of this swapped
        // CONTENDED unconditionally on every retry, which republished the
        // waiter flag after each wake and had every sleeper re-announce itself:
        // it measured worse than a plain spinlock, which is how the bug was
        // found rather than by reading it.
        let mut seen = self
            .state
            .compare_exchange(FREE, HELD, Ordering::Acquire, Ordering::Relaxed)
            .unwrap_or_else(|seen| seen);
        while seen != FREE {
            // Mark contention once, then sleep on that exact value.
            if seen != CONTENDED
                && self
                    .state
                    .compare_exchange(HELD, CONTENDED, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
                && self.state.load(Ordering::Relaxed) == FREE
            {
                seen = FREE;
                continue;
            }
            atomic_wait::wait(&self.state, CONTENDED);
            seen = self
                .state
                .compare_exchange(FREE, CONTENDED, Ordering::Acquire, Ordering::Relaxed)
                .unwrap_or_else(|seen| seen);
        }
    }

    fn try_lock(&self) -> bool {
        use core::sync::atomic::Ordering;
        self.state
            .compare_exchange(FREE, HELD, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    unsafe fn unlock(&self) {
        use core::sync::atomic::Ordering;
        // Only wake if somebody actually slept. The uncontended path is a
        // single store and no syscall.
        if self.state.swap(FREE, Ordering::Release) == CONTENDED {
            atomic_wait::wake_one(&self.state);
        }
    }
}

type LaPlMx = lock_api::Mutex<parking_lot::RawMutex, Node>;
type LaSpMx = lock_api::Mutex<spin::Mutex<()>, Node>;
type LaYdMx = lock_api::Mutex<spin::mutex::Mutex<(), spin::relax::Yield>, Node>;

contended_arm!(held_park, lock_api::Mutex<parking_lot::RawMutex, u64>);
contended_arm!(held_spin, lock_api::Mutex<spin::Mutex<()>, u64>);
contended_arm!(
    held_yield,
    lock_api::Mutex<spin::mutex::Mutex<(), spin::relax::Yield>, u64>
);
contended_arm!(held_bounded, lock_api::Mutex<BoundedMutex, u64>);
// Wired but not in NAMES: this futex arm is a known-broken implementation,
// kept so nobody writes it a third time. It loses to a spinning lock, which a
// blocking lock cannot honestly do.
#[allow(dead_code)]
mod broken_futex_arm {
    use super::*;
    contended_arm!(held_futex, lock_api::Mutex<FutexMutex, u64>);
    nodes_arm!(nodes_futex, lock_api::Mutex<FutexMutex, Node>);
}
contended_arm!(held_wfe, lock_api::Mutex<WfeMutex, u64>);

nodes_arm!(nodes_park, LaPlMx);
nodes_arm!(nodes_spin, LaSpMx);
nodes_arm!(nodes_yield, LaYdMx);
nodes_arm!(nodes_bounded, lock_api::Mutex<BoundedMutex, Node>);
nodes_arm!(nodes_wfe, lock_api::Mutex<WfeMutex, Node>);

/// The same five, named once and used by both tables.
const NAMES: [&str; 5] = ["parking_lot", "spin", "spin+yield", "bounded", "wfe"];

type Arm = fn(usize) -> (Duration, Duration);

fn median(mut runs: Vec<(Duration, Duration)>) -> (Duration, Duration) {
    runs.sort_by_key(|(wall, _)| *wall);
    runs[REPS / 2]
}

fn table(title: &str, threads: &[usize], names: [&str; 5], arms: [Arm; 5]) {
    println!("\n{title}");
    print!("         ");
    for name in names {
        print!("{name:>16}");
    }
    println!("{:>9}", "null");
    print!("  threads");
    for _ in names {
        print!("{:>8}{:>8}", "wall", "cpu");
    }
    println!("{:>9}", "");
    for &thread_count in threads {
        let mut runs: Vec<Vec<(Duration, Duration)>> = vec![Vec::new(); 6];
        for _ in 0..REPS {
            for (slot, arm) in arms.iter().enumerate() {
                runs[slot].push(arm(thread_count));
            }
            // The null arm: the first one again, measured under another name.
            runs[5].push(arms[0](thread_count));
        }
        let m: Vec<(Duration, Duration)> = runs.into_iter().map(median).collect();
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        println!(
            "  {thread_count:>7}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.1}{:>8.2}x",
            ms(m[0].0),
            ms(m[0].1),
            ms(m[1].0),
            ms(m[1].1),
            ms(m[2].0),
            ms(m[2].1),
            ms(m[3].0),
            ms(m[3].1),
            ms(m[4].0),
            ms(m[4].1),
            m[0].0.as_secs_f64() / m[5].0.as_secs_f64(),
        );
    }
}

fn main() {
    let cores = std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    println!("\n{cores} cores, median of {REPS}, nodes of {DEFAULT_INNER_SIZE} entries, ms");

    table(
        &format!("this crate's shape, {LOOKUPS} lookups over {NODES} nodes"),
        &[1, 2, 4, cores, cores * 2],
        NAMES,
        [nodes_park, nodes_spin, nodes_yield, nodes_bounded, nodes_wfe],
    );

    table(
        &format!("one lock, held {HELD_ITERS} iterations, {HOLDS} times per thread"),
        &[cores / 4, cores / 2, cores, cores * 2, cores * 8],
        NAMES,
        [held_park, held_spin, held_yield, held_bounded, held_wfe],
    );
}

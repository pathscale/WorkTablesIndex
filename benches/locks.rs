//! What this crate's node locking costs, and what it would cost on a `no_std`
//! lock instead.
//!
//! # Why this exists
//!
//! `concurrent` is the only reason this crate needs `std`. Its 31 lock sites
//! are `parking_lot`, and `cdc` and `multimap` both imply `concurrent`, so a
//! consumer that wants only `ChangeEvent` and `Pair` takes `parking_lot` and
//! `libc` with them. DataBucket is exactly that consumer.
//!
//! Swapping to a spinlock is the obvious `no_std` answer and the obvious
//! objection is equally well known: a spinlock burns a core instead of
//! sleeping, which is why `parking_lot` exists. **Both are true, in different
//! regimes**, and this measures which regime this crate is actually in.
//!
//! # The two regimes
//!
//! `nodes` is this crate's own shape, from `src/concurrent/set.rs`:
//! `RwLock<BTreeMap<T, Arc<Mutex<Node>>>>`. A lookup takes the index read
//! lock, clones the node `Arc`, drops the index lock and locks the node. The
//! critical section is a few operations on a key array.
//!
//! `contended` is the case a spinlock is supposed to lose: one lock, a long
//! hold, and far more threads than cores, so a waiter can burn the core that
//! the lock holder needs in order to finish. That needs both a long critical
//! section and oversubscription, and a benchmark with neither will report that
//! spinning is free.
//!
//! # Reading it
//!
//! **CPU time, not just wall clock.** A spinlock finishes sooner while burning
//! a core that did no work, and on a machine with spare cores that trade looks
//! free until something else wants one. `getrusage` is the only arm of this
//! that can see it.
//!
//! **The null column is the floor.** It runs `parking_lot` a second time under
//! another name, so whatever it shows is what this harness reports for
//! identical code. Nothing smaller than that means anything.
//!
//! # The generic, which is the point
//!
//! Every measurement runs one body, generic over `R: RawMutex`, instantiated
//! once with `parking_lot`'s raw lock and once with `spin`'s. That is not a
//! benchmarking convenience: it is the proposed shape for this crate. Both
//! locks are `lock_api` underneath, `ArcMutexGuard<R, T>` is `lock_api`'s type
//! in both cases, and `lock_arc` is available from both. So `concurrent` can
//! be generic over `R` and let a consumer choose, rather than naming a
//! concrete lock and deciding for everyone.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lock_api::{Mutex, RawMutex, RawRwLock, RwLock};

/// Nodes in the index, enough that lookups spread rather than queue on one.
const NODES: u64 = 4_096;
/// Lookups per measurement, split across threads.
const LOOKUPS: usize = 200_000;
/// How long the contended case holds its lock. Long enough that a holder can
/// be preempted inside it, short enough to keep this bounded.
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

struct Node {
    keys: Vec<u64>,
}

impl Node {
    fn new(seed: u64) -> Self {
        Self {
            keys: (0..16).map(|n| seed.wrapping_mul(31).wrapping_add(n)).collect(),
        }
    }

    /// A few operations, which is what a real node access is.
    fn touch(&mut self, n: u64) -> u64 {
        let at = (n as usize) % self.keys.len();
        self.keys[at] ^= n;
        self.keys[at]
    }
}

/// This crate's shape: an index lock, then a node lock per access.
fn nodes<RM, RR>(threads: usize) -> (Duration, Duration)
where
    RM: RawMutex + Send + Sync + 'static,
    RR: RawRwLock + Send + Sync + 'static,
{
    let index: Arc<RwLock<RR, BTreeMap<u64, Arc<Mutex<RM, Node>>>>> = Arc::new(RwLock::new(
        (0..NODES).map(|n| (n, Arc::new(Mutex::new(Node::new(n))))).collect(),
    ));
    let before = cpu();
    let now = Instant::now();
    std::thread::scope(|scope| {
        for worker in 0..threads {
            let index = index.clone();
            scope.spawn(move || {
                let mut acc = 0u64;
                let mut rng = 0x2545_F491_4F6C_DD1Du64 ^ (worker as u64).wrapping_mul(0x9E37_79B9);
                for _ in 0..LOOKUPS / threads {
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    // The index lock is dropped before the node lock is taken,
                    // which is what the real code does.
                    let node = index.read().get(&(rng % NODES)).cloned();
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

/// One lock, held long, oversubscribed. Where spinning is supposed to fail.
fn contended<RM: RawMutex + Send + Sync + 'static>(threads: usize) -> (Duration, Duration) {
    let lock: Arc<Mutex<RM, u64>> = Arc::new(Mutex::new(0));
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

type SpinM = spin::Mutex<()>;
type SpinR = spin::RwLock<()>;

fn median(mut runs: Vec<(Duration, Duration)>) -> (Duration, Duration) {
    runs.sort_by_key(|(wall, _)| *wall);
    runs[REPS / 2]
}

fn row(threads: usize, park: fn(usize) -> (Duration, Duration), spin: fn(usize) -> (Duration, Duration)) {
    let mut parking = Vec::new();
    let mut spinning = Vec::new();
    let mut null = Vec::new();
    for _ in 0..REPS {
        parking.push(park(threads));
        spinning.push(spin(threads));
        null.push(park(threads));
    }
    let ((pw, pc), (sw, sc), (nw, _)) = (median(parking), median(spinning), median(null));
    println!(
        "  {threads:>7}   {:>7.1} {:>8.1}   {:>7.1} {:>8.1}   {:>6.2}x   {:>8}",
        pw.as_secs_f64() * 1e3,
        pc.as_secs_f64() * 1e3,
        sw.as_secs_f64() * 1e3,
        sc.as_secs_f64() * 1e3,
        pw.as_secs_f64() / nw.as_secs_f64(),
        format!("{:+.0}%", 100.0 * (sc.as_secs_f64() / pc.as_secs_f64() - 1.0)),
    );
}

fn main() {
    let cores = std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    println!("\n{cores} cores, median of {REPS}, one generic body over R: RawMutex\n");

    println!("this crate's shape: RwLock<BTreeMap<T, Arc<Mutex<Node>>>>, {LOOKUPS} lookups");
    println!("            parking_lot            spin                     spin CPU");
    println!("  threads    wall      cpu       wall      cpu       null      vs now");
    for threads in [1, 2, 4, cores, cores * 2, cores * 4] {
        row(
            threads,
            nodes::<parking_lot::RawMutex, parking_lot::RawRwLock>,
            nodes::<SpinM, SpinR>,
        );
    }

    println!("\none lock, held {HELD_ITERS} iterations, {HOLDS} times per thread");
    println!("            parking_lot            spin                     spin CPU");
    println!("  threads    wall      cpu       wall      cpu       null      vs now");
    for threads in [cores / 2, cores, cores * 2, cores * 8] {
        row(threads, contended::<parking_lot::RawMutex>, contended::<SpinM>);
    }

    println!(
        "\n  The two tables disagree on purpose. Short critical sections favour\n  \
         spinning; a long one under oversubscription does not, because a spinner\n  \
         holds the core the lock holder needs. Which row this crate is in is the\n  \
         question, and the answer is per consumer, which is why the body above is\n  \
         generic over R rather than naming a lock."
    );
}

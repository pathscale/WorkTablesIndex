//! What WFE is, what it costs, and the one rule for when to emit it.
//!
//! # Why this file exists
//!
//! `benches/locks.rs` prices five locks. This asserts the platform facts that
//! benchmark rests on, so a change in the hardware, the toolchain or the OS
//! surfaces here as a named failure rather than as a benchmark that quietly
//! means something different.
//!
//! Everything below was measured on aarch64-apple-darwin, 16 cores, and every
//! bound is deliberately loose: these tests exist to catch a property
//! disappearing, not to pin a number.
//!
//! # The instruction
//!
//! `WFE` puts a core in a low-power state until its event register is set.
//! `SEV` sets it on every core. ARM documents this as *the* intended spinlock
//! construction: a waiter executes WFE to request a low-power state, and the
//! releaser executes SEV to wake it.
//!
//! Available on every ARMv7-and-later core, so every ARM64 chip: Apple
//! Silicon, AWS Graviton 2/3/4 (Neoverse N1/V1/V2), Ampere Altra, Raspberry Pi
//! 4/5. x86 has the same idea split by privilege: `MONITOR`/`MWAIT` is ring 0,
//! `UMONITOR`/`UMWAIT` is ring 3 from Intel Tremont (2020) onward, and AMD has
//! `MONITORX`/`MWAITX`.
//!
//! # Rust never emits it
//!
//! `core::hint::spin_loop()` on aarch64 is `__isb(SY)`, an instruction
//! barrier. Disassembled:
//!
//! ```text
//! __RNvCs8LfLpYhzmc_7hintasm1s:
//!     isb
//!     ret
//! ```
//!
//! There is no WFE anywhere in `core::hint`, and no wrapper in
//! `core::arch::aarch64`. That is structural rather than an oversight: WFE is
//! only useful if the *releaser* pairs it with SEV, and that is a protocol
//! between both sides of a lock. A one-sided hint cannot express one, so it has
//! to be written in the lock, where both sides are known.
//!
//! # What it costs here
//!
//! ```text
//! nop                     0.3 ns
//! isb (spin_loop)         8.6 ns
//! wfe, event pending   1336.7 ns
//! ```
//!
//! So WFE is not a userspace no-op on this machine, and it does not block
//! forever either: it idles for roughly 1.3 us and returns unprompted. A
//! waiter therefore needs no SEV to make progress, and burns about 150x less
//! CPU per unit of wall time waited than an `isb` spin.
//!
//! # The rule, which is the useful part
//!
//! **WFE idles the core. It does not yield to the operating system.**
//!
//! That is why it lost here. With 128 threads on 16 cores a WFE-waiting lock
//! cost 5584 ms of CPU against 1241 ms for a yielding spinner: a waiter idling
//! in WFE is still a *scheduled* thread, so the lock holder still cannot get a
//! core. Below the core count it is level with plain spinning and no better.
//!
//! An earlier version of that lock restarted its spin schedule after every
//! WFE, so it yielded between idles. That accident is the only reason it ever
//! looked competitive, and finding it is why this rule is written down.
//!
//! So emit WFE when:
//!
//! * threads are at most cores, so idling one costs nothing that is wanted;
//! * there is no operating system to yield to, which is exactly when every
//!   other option reduces to burning the core;
//! * waits are long enough that 1.3 us of idle beats 8.6 ns of `isb` repeated
//!   until the holder finishes.
//!
//! Bare metal and pinned threads. Not an oversubscribed host.
//!
//! # The result above is macOS-specific, and probably inverts on AWS
//!
//! Under KVM, WFE is trapped by the hypervisor (the `TWE` bit) and KVM yields
//! the vCPU. The Linux commit is literally "arm64: KVM: Yield CPU when vcpu
//! executes a WFE", written for the same pathology measured here, where
//! spinning vCPUs hold the cores a lock holder needs and hackbench slows by
//! 40x. That trap supplies the missing half: on Graviton, WFE *does* reach a
//! scheduler.
//!
//! Graviton also maps one vCPU to one physical core with no SMT, so a guest is
//! not oversubscribed the way this machine was at 128 threads on 16 cores,
//! which is the regime WFE is for in the first place.
//!
//! **So the negative result here is not portable and must be re-measured on
//! Graviton before anyone concludes WFE is worthless.**
//!
//! # Two findings from the same work, kept so they are not re-derived
//!
//! **`lock_api` is free.** `parking_lot::Mutex` and
//! `lock_api::Mutex<parking_lot::RawMutex, _>` measure the same, so making
//! `concurrent` generic over `R: RawMutex` costs nothing and lets a consumer
//! choose. `lock_arc` and `ArcMutexGuard<R, T>` are `lock_api`'s, not
//! `parking_lot`'s, so `Ref` keeps working either way.
//!
//! **The node lock is not this crate's bottleneck.** With the index `RwLock`
//! held constant, every node lock lands inside the null. An earlier benchmark
//! varied both at once and reported the index lock's behaviour as a node-lock
//! result. The index `RwLock` is what deserves the next look.
//!
//! # Why a compiler should care
//!
//! EKOPathRS is a compiler. A compiler that recognises a spin loop can emit
//! WFE for it, which LLVM does not do. The measurement above prices that at
//! 150x less CPU per unit waited, and the rule above says when it would be
//! wrong, which is the half that makes it safe to automate.
//!
//! # Related work, recent
//!
//! * HTLL, "Latency-Aware Scalable Blocking Mutex", IEEE TPDS, January 2025:
//!   throughput and latency together under oversubscription, up to 97% latency
//!   reduction for about 5% throughput.
//! * Fissile Locks, arXiv 2003.05025: compact, NUMA-aware, preemption tolerant.
//! * Asymmetry-aware Scalable Locking, arXiv 2108.03355. Directly relevant
//!   here, because this is a P-core/E-core machine and neither the benchmark
//!   nor these tests separate them.

#![cfg(target_arch = "aarch64")]

use std::hint::black_box;
use std::time::Instant;

/// Iterations per timing loop. Large enough that a nanosecond-scale
/// instruction is measurable over timer noise.
const ITERS: u64 = 200_000;

fn ns_each(mut body: impl FnMut()) -> f64 {
    // Warm, so the first run's page faults and frequency ramp are not counted.
    for _ in 0..ITERS / 10 {
        body();
    }
    let mut runs = Vec::new();
    for _ in 0..5 {
        let now = Instant::now();
        for _ in 0..ITERS {
            body();
        }
        runs.push(now.elapsed());
    }
    runs.sort();
    runs[2].as_nanos() as f64 / ITERS as f64
}

fn nop_ns() -> f64 {
    ns_each(|| {
        // SAFETY: `nop` has no operands and no effects.
        unsafe { core::arch::asm!("nop", options(nomem, nostack)) };
    })
}

fn isb_ns() -> f64 {
    ns_each(|| black_box(core::hint::spin_loop()))
}

fn wfe_ns() -> f64 {
    ns_each(|| {
        // SAFETY: WFE is unprivileged, has no memory operands, and waits at
        // most for an implementation-defined period before returning.
        unsafe { core::arch::asm!("wfe", options(nomem, nostack)) };
    })
}

/// WFE must be reachable at all from userspace, or none of this applies.
///
/// If this ever traps or is emulated away, every other claim in this file is
/// about a different machine.
#[test]
fn wfe_and_sev_execute_in_userspace() {
    // SAFETY: both are unprivileged and have no operands.
    unsafe {
        core::arch::asm!("sev", options(nomem, nostack));
        core::arch::asm!("wfe", options(nomem, nostack));
    }
}

/// WFE is not a no-op here, which is the whole reason it is worth emitting.
///
/// An implementation is free to make WFE a `nop`, and on such a machine a
/// WFE-based lock is a plain spinlock wearing a costume. This separates the
/// two: `nop` measured 0.3 ns and WFE measured 1336.7 ns, so anything within
/// an order of magnitude of `nop` means WFE is not idling.
#[test]
#[ignore = "timing, run with --ignored"]
fn wfe_actually_idles_rather_than_being_a_nop() {
    let nop = nop_ns();
    let wfe = wfe_ns();
    assert!(
        wfe > nop * 20.0,
        "WFE at {wfe:.1} ns against nop at {nop:.1} ns: WFE is not idling on \
         this machine, so a WFE lock here is a spinlock with extra steps"
    );
}

/// `core::hint::spin_loop()` is not WFE, and the gap is why the lock has to
/// write the instruction itself.
///
/// `spin_loop()` is `__isb(SY)`, measured at 8.6 ns against WFE's 1336.7. If
/// these ever converge, either Rust started emitting WFE, in which case a lock
/// should stop hand-writing it, or WFE stopped idling.
#[test]
#[ignore = "timing, run with --ignored"]
fn the_standard_spin_hint_is_not_wfe() {
    let isb = isb_ns();
    let wfe = wfe_ns();
    assert!(
        wfe > isb * 10.0,
        "spin_loop() at {isb:.1} ns and WFE at {wfe:.1} ns are within 10x. \
         core::hint::spin_loop() is __isb(SY) and should be far cheaper; if it \
         is not, check whether Rust now emits WFE and delete the hand-written one"
    );
}

/// WFE returns on its own, so a waiter cannot deadlock if SEV is missed.
///
/// This is what makes a WFE lock safe to write: the event register may already
/// be clear, or a SEV may land before the waiter reaches its WFE, and neither
/// wedges. Measured at roughly 1.3 us per WFE, so a hundred of them is
/// bounded well under a second on any machine where WFE idles at all.
#[test]
fn a_waiter_is_never_stuck_when_no_sev_arrives() {
    // Drain any pending event so the WFEs below have nothing waiting for them.
    // SAFETY: unprivileged, no operands.
    unsafe {
        core::arch::asm!("sev", options(nomem, nostack));
        core::arch::asm!("wfe", options(nomem, nostack));
    }
    let now = Instant::now();
    for _ in 0..100 {
        // SAFETY: as above.
        unsafe { core::arch::asm!("wfe", options(nomem, nostack)) };
    }
    assert!(
        now.elapsed() < std::time::Duration::from_secs(5),
        "100 WFEs with no SEV took {:?}: WFE is blocking indefinitely here, so \
         a lock must pair every one with a SEV rather than relying on the timeout",
        now.elapsed()
    );
}

/// The rule, as an executable statement: WFE does not yield to the scheduler.
///
/// Two threads per core, one holding a lock for a long stretch. If WFE reached
/// the scheduler, waiters would stand aside and this would finish in about the
/// time the holders need. It does not, which is why the rule is "threads at
/// most cores".
///
/// **Expected to be different under KVM**, where WFE traps and the hypervisor
/// yields the vCPU. On Graviton this test is the one to watch: if it starts
/// passing comfortably there, WFE became viable for oversubscribed hosts and
/// the guidance in this file needs revisiting.
#[test]
#[ignore = "heavy and oversubscribed, run with --ignored"]
fn wfe_does_not_yield_to_the_scheduler() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;

    let cores = std::thread::available_parallelism().map_or(8, std::num::NonZeroUsize::get);
    let threads = cores * 8;
    let held = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicU64::new(0));

    let now = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..threads {
            let held = held.clone();
            let done = done.clone();
            scope.spawn(move || {
                for _ in 0..8 {
                    while held
                        .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                        .is_err()
                    {
                        // SAFETY: unprivileged, no operands.
                        unsafe { core::arch::asm!("wfe", options(nomem, nostack)) };
                    }
                    let mut acc = 0u64;
                    for n in 0..20_000u64 {
                        acc = acc.wrapping_mul(0x9e37_79b9).wrapping_add(n);
                    }
                    black_box(acc);
                    held.store(false, Ordering::Release);
                    // SAFETY: as above.
                    unsafe { core::arch::asm!("sev", options(nomem, nostack)) };
                    done.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
    });

    assert_eq!(done.load(Ordering::Relaxed), threads as u64 * 8);
    // Generous: this exists to prove the run terminates and to print the cost,
    // not to pin a number that varies by machine.
    assert!(
        now.elapsed() < std::time::Duration::from_secs(120),
        "{threads} threads on {cores} cores took {:?}",
        now.elapsed()
    );
}

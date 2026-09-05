use loom::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Publication {
    generation: AtomicUsize,
    current: AtomicUsize,
    snapshots: [AtomicUsize; 2],
}

/// Models the ordering contract used by `concurrent::set::Topology`.
///
/// The writer marks the generation odd, initializes a replacement, publishes
/// it, and finally releases an even generation. A reader may accept a route
/// only when the same even generation brackets its publication load. Seeing a
/// newer publication with an older generation is harmless; seeing an older or
/// uninitialized publication after acquiring a newer generation is not.
#[test]
fn stable_even_generation_never_accepts_an_older_publication() {
    loom::model(|| {
        let publication = Arc::new(Publication {
            generation: AtomicUsize::new(0),
            current: AtomicUsize::new(0),
            snapshots: [AtomicUsize::new(0), AtomicUsize::new(usize::MAX)],
        });

        let writer_publication = Arc::clone(&publication);
        let writer = loom::thread::spawn(move || {
            writer_publication.generation.fetch_add(1, Ordering::AcqRel);
            writer_publication.snapshots[1].store(2, Ordering::Relaxed);
            writer_publication.current.swap(1, Ordering::AcqRel);
            writer_publication.generation.fetch_add(1, Ordering::Release);
        });

        let reader_publication = Arc::clone(&publication);
        let reader = loom::thread::spawn(move || {
            let before = reader_publication.generation.load(Ordering::Acquire);
            if !before.is_multiple_of(2) {
                return;
            }

            let route = reader_publication.current.load(Ordering::Acquire);
            let snapshot_generation = reader_publication.snapshots[route].load(Ordering::Relaxed);
            let after = reader_publication.generation.load(Ordering::Acquire);

            if before == after {
                assert_ne!(
                    snapshot_generation,
                    usize::MAX,
                    "an accepted publication must be fully initialized"
                );
                assert!(
                    snapshot_generation >= before,
                    "an accepted publication must not predate its stable generation"
                );
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
    });
}

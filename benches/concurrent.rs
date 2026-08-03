use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use crossbeam_skiplist::SkipSet;
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use scc::TreeIndex;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use WorkTablesIndex::concurrent::multimap::{BTreeMultiMap, OrderedBTreeMultiMap};

#[derive(Clone)]
enum Op {
    Read(usize),
    Write(usize),
}

const NUM_READERS: usize = 30;
const NUM_WRITERS: usize = 10;
const NUM_THREADS: usize = NUM_READERS + NUM_WRITERS;
const OPERATIONS_PER_THREAD: usize = 100_000;
const TOTAL_OPERATIONS: usize = NUM_THREADS * OPERATIONS_PER_THREAD;
const MULTIMAP_REMOVE_ENTRIES: usize = 20_000;
const MULTIMAP_REMOVE_SEED: u64 = 42;
const IS_EMPTY_ENTRIES: usize = 100_000;
const POINT_LOOKUP_QUERIES: usize = 65_536;
const POINT_LOOKUP_SEED: u64 = 0x9e37_79b9_7f4a_7c15;
static RUNTIME_CONCURRENT_MODE: AtomicBool = AtomicBool::new(false);
static ACTIVE_LOOKUPS: AtomicUsize = AtomicUsize::new(0);

// fn generate_operations(write_ratio: f64) -> Vec<Vec<Op>> {
//     let mut rng = thread_rng();
//     let mut all_operations: Vec<Vec<Op>> =
//         vec![Vec::with_capacity(OPERATIONS_PER_THREAD); NUM_THREADS];

//     for i in 0..TOTAL_OPERATIONS {
//         let thread_index = i % NUM_THREADS;
//         let value = rng.gen_range(0..TOTAL_OPERATIONS);
//         let operation = if thread_index == NUM_READERS || rng.gen::<f64>() < write_ratio {
//             Op::Write(value)
//         } else {
//             Op::Read(value)
//         };
//         all_operations[thread_index].push(operation);
//     }

//     all_operations
// }

fn generate_operations(write_ratio: f64) -> Vec<Vec<Op>> {
    let mut rng = thread_rng();
    let mut all_operations = vec![Vec::with_capacity(OPERATIONS_PER_THREAD); NUM_THREADS];

    for thread_idx in 0..NUM_THREADS {
        let range_start = thread_idx * (TOTAL_OPERATIONS / NUM_THREADS);
        let range_end = (thread_idx + 1) * (TOTAL_OPERATIONS / NUM_THREADS);

        for _ in 0..OPERATIONS_PER_THREAD {
            let value = rng.gen_range(range_start..range_end);
            let operation = if thread_idx < NUM_WRITERS || rng.gen::<f64>() < write_ratio {
                Op::Write(value)
            } else {
                Op::Read(value)
            };
            all_operations[thread_idx].push(operation);
        }
    }
    all_operations
}

fn concurrent_operations<T: Send + Sync + 'static>(
    set: Arc<T>,
    operations: Vec<Op>,
    read_op: impl Fn(&T, usize) + Send + Sync + 'static,
    write_op: impl Fn(&T, usize) + Send + Sync + 'static,
) {
    for op in operations {
        match op {
            Op::Read(value) => read_op(&set, value),
            Op::Write(value) => write_op(&set, value),
        }
    }
}

fn bench_btreeset_with_ratio(c: &mut Criterion, write_ratio: f64) {
    let operations = Arc::new(generate_operations(write_ratio));

    let mut group = c.benchmark_group(format!("Write Ratio: {:.2}", write_ratio));
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function(BenchmarkId::new("scc::TreeIndex", write_ratio), |b| {
        b.iter(|| {
            let set = Arc::new(TreeIndex::new());
            let mut handles = vec![];

            for thread_ops in operations.iter() {
                let set = Arc::clone(&set);
                let thread_ops = thread_ops.clone();
                let handle = thread::spawn(move || {
                    concurrent_operations(
                        set,
                        thread_ops,
                        |set, item| {
                            black_box(set.contains(&item));
                        },
                        |set, item| {
                            black_box({
                                let _ = set.insert(item, ());
                            });
                        },
                    );
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.bench_function(BenchmarkId::new("ConcurrentBTreeSet", write_ratio), |b| {
        b.iter(|| {
            let set: Arc<WorkTablesIndex::concurrent::set::BTreeSet<usize>> =
                Arc::new(WorkTablesIndex::concurrent::set::BTreeSet::new());
            let mut handles = vec![];

            for thread_ops in operations.iter() {
                let set = Arc::clone(&set);
                let thread_ops = thread_ops.clone();
                let handle = thread::spawn(move || {
                    concurrent_operations(
                        set,
                        thread_ops,
                        |set, item| {
                            set.contains(&item);
                        },
                        |set, item| {
                            set.insert(item);
                        },
                    );
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.bench_function(BenchmarkId::new("SkipSet", write_ratio), |b| {
        b.iter(|| {
            let set = Arc::new(SkipSet::new());
            let mut handles = vec![];

            for thread_ops in operations.iter() {
                let set = Arc::clone(&set);
                let thread_ops = thread_ops.clone();
                let handle = thread::spawn(move || {
                    concurrent_operations(
                        set,
                        thread_ops,
                        |set, item| {
                            set.contains(&item);
                        },
                        |set, item| {
                            set.insert(item);
                        },
                    );
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.finish();
}

fn bench_concurrent_btreeset(c: &mut Criterion) {
    let ratios = vec![0.01, 0.1, 0.3, 0.5];
    for ratio in ratios {
        bench_btreeset_with_ratio(c, ratio);
    }
}

fn bench_is_empty(c: &mut Criterion) {
    let set = WorkTablesIndex::concurrent::set::BTreeSet::<usize>::new();
    for value in 0..IS_EMPTY_ENTRIES {
        set.insert(value);
    }

    c.bench_function("ConcurrentBTreeSet is_empty/non-empty", |b| {
        b.iter(|| black_box(set.is_empty()))
    });
}

fn bench_contains(c: &mut Criterion) {
    let set = WorkTablesIndex::concurrent::set::BTreeSet::<usize>::new();
    for value in 0..IS_EMPTY_ENTRIES {
        set.insert(value);
    }

    let present = IS_EMPTY_ENTRIES / 2;
    let absent = IS_EMPTY_ENTRIES;
    let mut group = c.benchmark_group("ConcurrentBTreeSet contains");
    group.bench_function("present", |b| b.iter(|| black_box(set.contains(black_box(&present)))));
    group.bench_function("absent", |b| b.iter(|| black_box(set.contains(black_box(&absent)))));
    group.finish();
}

fn bench_map_select(c: &mut Criterion) {
    let map = WorkTablesIndex::concurrent::map::BTreeMap::<usize, usize>::new();
    for value in 0..IS_EMPTY_ENTRIES {
        map.insert(value, value);
    }

    let present = IS_EMPTY_ENTRIES / 2;
    let absent = IS_EMPTY_ENTRIES;
    let mut group = c.benchmark_group("ConcurrentBTreeMap lookup_for_select");
    group.bench_function("present", |b| {
        b.iter(|| black_box(map.lookup_for_select(black_box(&present))))
    });
    group.bench_function("absent", |b| {
        b.iter(|| black_box(map.lookup_for_select(black_box(&absent))))
    });
    group.finish();
}

fn bench_runtime_read_policy(c: &mut Criterion) {
    let map = WorkTablesIndex::concurrent::map::BTreeMap::<usize, usize>::new();
    for value in 0..IS_EMPTY_ENTRIES {
        map.insert(value, value);
    }
    let present = IS_EMPTY_ENTRIES / 2;
    RUNTIME_CONCURRENT_MODE.store(false, Ordering::Relaxed);

    let mut group = c.benchmark_group("ConcurrentBTreeMap runtime read policy/present");
    group.bench_function("compile-time optimistic", |b| {
        b.iter(|| black_box(map.lookup_for_select_optimistic(black_box(&present))))
    });
    group.bench_function("predictable atomic branch", |b| {
        b.iter(|| {
            let value = if RUNTIME_CONCURRENT_MODE.load(Ordering::Relaxed) {
                map.lookup_for_select(black_box(&present))
            } else {
                map.lookup_for_select_optimistic(black_box(&present))
            };
            black_box(value)
        })
    });
    group.bench_function("active-operation counter", |b| {
        b.iter(|| {
            ACTIVE_LOOKUPS.fetch_add(1, Ordering::Relaxed);
            let value = map.lookup_for_select_optimistic(black_box(&present));
            ACTIVE_LOOKUPS.fetch_sub(1, Ordering::Relaxed);
            black_box(value)
        })
    });
    group.finish();
}

fn randomized_point_lookups(hit_ratio: usize) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(POINT_LOOKUP_SEED ^ hit_ratio as u64);
    (0..POINT_LOOKUP_QUERIES)
        .map(|_| {
            let position = rng.random_range(0..IS_EMPTY_ENTRIES);
            if rng.random_range(0..100) < hit_ratio {
                position * 2
            } else {
                position * 2 + 1
            }
        })
        .collect()
}

fn point_lookup_string(value: usize) -> String {
    format!("{value:016x}")
}

fn bench_randomized_contains(c: &mut Criterion) {
    let numeric = WorkTablesIndex::concurrent::set::BTreeSet::<usize>::new();
    let strings = WorkTablesIndex::concurrent::set::BTreeSet::<String>::new();
    for value in 0..IS_EMPTY_ENTRIES {
        numeric.insert(value * 2);
        strings.insert(point_lookup_string(value * 2));
    }

    let queries = randomized_point_lookups(99);
    let string_queries: Vec<String> = queries.iter().map(|query| point_lookup_string(*query)).collect();
    let mut group = c.benchmark_group("ConcurrentBTreeSet randomized contains/99% hits");
    group.sample_size(40);
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(POINT_LOOKUP_QUERIES as u64));

    group.bench_function("usize", |b| {
        b.iter(|| {
            let mut checksum = false;
            for query in &queries {
                checksum ^= numeric.contains(black_box(query));
            }
            black_box(checksum)
        })
    });
    group.bench_function("16-byte string", |b| {
        b.iter(|| {
            let mut checksum = false;
            for query in &string_queries {
                checksum ^= strings.contains(black_box(query.as_str()));
            }
            black_box(checksum)
        })
    });

    group.finish();
}

fn bench_map_update(c: &mut Criterion) {
    let normal = WorkTablesIndex::concurrent::map::BTreeMap::<usize, usize>::new();
    normal.insert(1, 1);
    let with_cdc = WorkTablesIndex::concurrent::map::BTreeMap::<usize, usize>::new();
    with_cdc.insert_cdc(1, 1);

    let mut group = c.benchmark_group("ConcurrentBTreeMap update");
    group.bench_function("normal", |b| b.iter(|| black_box(normal.insert(1, black_box(2)))));
    group.bench_function("cdc", |b| b.iter(|| black_box(with_cdc.insert_cdc(1, black_box(2)))));
    group.finish();
}

fn removal_entries(values_per_key: RangeInclusive<usize>) -> Vec<(usize, usize)> {
    let mut rng = StdRng::seed_from_u64(MULTIMAP_REMOVE_SEED);
    let mut entries = Vec::with_capacity(MULTIMAP_REMOVE_ENTRIES);
    let mut key = 0;

    while entries.len() < MULTIMAP_REMOVE_ENTRIES {
        let value_count = rng.random_range(values_per_key.clone());
        entries.extend((0..value_count).map(|value| (key, value)));
        key += 1;
    }

    entries
}

fn build_random_multimap(entries: &[(usize, usize)]) -> BTreeMultiMap<usize, usize> {
    let map = BTreeMultiMap::<usize, usize>::new();
    for (key, value) in entries {
        map.insert(*key, *value);
    }

    map
}

fn build_ord_multimap(entries: &[(usize, usize)]) -> OrderedBTreeMultiMap<usize, usize> {
    let map = OrderedBTreeMultiMap::<usize, usize>::new();
    for (key, value) in entries {
        map.insert(*key, *value);
    }

    map
}

fn bench_multimap_removal(c: &mut Criterion) {
    let usual_entries = removal_entries(1..=3);
    let many_entries = removal_entries(1_000..=2_000);
    let usual_target = *usual_entries.last().unwrap();
    let many_target = *many_entries.last().unwrap();

    let mut group = c.benchmark_group("BTreeMultiMap remove pair");
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function(BenchmarkId::new("random", "1-3 values per key"), |b| {
        b.iter_batched_ref(
            || build_random_multimap(&usual_entries),
            |map| black_box(map.remove(&usual_target.0, &usual_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("ord", "1-3 values per key"), |b| {
        b.iter_batched_ref(
            || build_ord_multimap(&usual_entries),
            |map| black_box(map.remove(&usual_target.0, &usual_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("random", "1000-2000 values per key"), |b| {
        b.iter_batched_ref(
            || build_random_multimap(&many_entries),
            |map| black_box(map.remove(&many_target.0, &many_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("ord", "1000-2000 values per key"), |b| {
        b.iter_batched_ref(
            || build_ord_multimap(&many_entries),
            |map| black_box(map.remove(&many_target.0, &many_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_concurrent_btreeset,
    bench_is_empty,
    bench_contains,
    bench_map_select,
    bench_runtime_read_policy,
    bench_randomized_contains,
    bench_map_update,
    bench_multimap_removal
);
criterion_main!(benches);

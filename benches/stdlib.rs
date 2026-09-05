use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use rand::seq::SliceRandom;
use rand::thread_rng;
use scc::TreeIndex;
use std::hint::black_box;
use std::time::Duration;

fn criterion_benchmark(c: &mut Criterion) {
    let n = 100000;
    let mut input: Vec<usize> = (0..n).collect();
    input.shuffle(&mut thread_rng());

    c.bench_function("stdlib insert 100k", |b| {
        b.iter(|| {
            let mut btreeset = std::collections::BTreeSet::new();

            input.iter().for_each(|item| {
                black_box(btreeset.insert(item));
            });

            assert_eq!(btreeset.len(), n);
        })
    });
    c.bench_function("indexset insert 100k", |b| {
        b.iter(|| {
            let mut indexset = indexset::BTreeSet::new();

            input.iter().for_each(|item| {
                black_box(indexset.insert(*item));
            });

            assert_eq!(indexset.len(), n);
        })
    });
    c.bench_function("concurrent indexset insert 100k", |b| {
        b.iter(|| {
            let indexset: indexset::concurrent::set::BTreeSet<usize> = indexset::concurrent::set::BTreeSet::new();

            input.iter().for_each(|item| {
                black_box(indexset.insert(*item));
            });

            assert_eq!(indexset.len(), n);
        })
    });
    c.bench_function("concurrent indexset insert sorted 100k", |b| {
        b.iter(|| {
            let indexset: indexset::concurrent::set::BTreeSet<usize> = indexset::concurrent::set::BTreeSet::new();

            for item in 0..n {
                black_box(indexset.insert(item));
            }

            assert_eq!(indexset.len(), n);
        })
    });
    c.bench_function("treeindex insert 100k", |b| {
        b.iter(|| {
            let treeindex = TreeIndex::new();

            input.iter().for_each(|item| {
                black_box(treeindex.insert(*item, ()).unwrap());
            });

            assert_eq!(treeindex.len(), n);
        })
    });

    let mut restore = c.benchmark_group("concurrent indexset restore nodes");
    restore.sample_size(20);
    restore.warm_up_time(Duration::from_millis(500));
    restore.measurement_time(Duration::from_secs(2));
    for node_count in [1_000usize, 2_000, 4_000, 8_000] {
        let nodes = (0..node_count)
            .map(|node| (node * 8..node * 8 + 8).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        restore.throughput(Throughput::Elements(node_count as u64));
        restore.bench_with_input(BenchmarkId::new("bulk", node_count), &nodes, |b, nodes| {
            b.iter_batched(
                || nodes.clone(),
                |nodes| {
                    let set = indexset::concurrent::set::BTreeSet::<usize>::with_maximum_node_size(8);
                    set.attach_nodes(nodes);
                    black_box(set.node_count());
                },
                BatchSize::SmallInput,
            )
        });
    }
    restore.finish();

    let stdlib = std::collections::BTreeSet::from_iter(input.iter());
    let indexset = indexset::BTreeSet::from_iter(input.iter());
    let concurrent_indexset: indexset::concurrent::set::BTreeSet<usize> = indexset::concurrent::set::BTreeSet::new();
    for i in &input {
        concurrent_indexset.insert(*i);
    }
    let treeindex = TreeIndex::new();
    for i in &input {
        let _ = treeindex.insert(*i, ());
    }

    c.bench_function("stdlib contains 100k", |b| {
        b.iter(|| {
            input.iter().for_each(|item| {
                stdlib.contains(black_box(item));
            })
        })
    });
    c.bench_function("indexset contains 100k", |b| {
        b.iter(|| {
            input.iter().for_each(|item| {
                indexset.contains(black_box(item));
            })
        })
    });
    c.bench_function("concurrent indexset contains 100k", |b| {
        b.iter(|| {
            input.iter().for_each(|item| {
                indexset.contains(black_box(item));
            })
        })
    });
    c.bench_function("treeindex contains 100k", |b| {
        b.iter(|| {
            input.iter().for_each(|item| {
                treeindex.contains(black_box(item));
            })
        })
    });

    // c.bench_function("stdlib get i-th 100k", |b| {
    //     b.iter(|| {
    //         input.iter().for_each(|item| {
    //             stdlib.iter().nth(black_box(*item));
    //         })
    //     })
    // });
    c.bench_function("indexset get i-th 100k", |b| {
        b.iter(|| {
            input.iter().for_each(|item| {
                black_box(indexset.get_index(black_box(*item)));
            })
        })
    });

    c.bench_function("stdlib collect 100k into vec", |b| {
        b.iter(|| std::hint::black_box(stdlib.iter().collect::<Vec<&&usize>>()))
    });

    c.bench_function("indexset collect 100k into vec", |b| {
        b.iter(|| std::hint::black_box(indexset.iter().collect::<Vec<&&usize>>()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

use std::time::Instant;
use indexset::concurrent::map::BTreeMap as CMap;

fn main() {
    for nodes in [500usize, 1000, 2000, 4000, 8000] {
        let map: CMap<u64, u64> = CMap::with_maximum_node_size(16);
        let t = Instant::now();
        for n in 0..nodes as u64 {
            // one node per attach: 16 values, max = base+15
            let base = n * 16;
            let node: Vec<indexset::core::pair::Pair<u64, u64>> =
                (0..16).map(|i| indexset::core::pair::Pair { key: base + i, value: base + i }).collect();
            map.attach_node(node);
        }
        let d = t.elapsed();
        println!("{nodes:5} nodes: {:>10.3?}  ({:>8.1} us/node)", d, d.as_secs_f64() * 1e6 / nodes as f64);
    }
}

#![cfg(feature = "serde")]

use indexset::{BTreeMap, BTreeSet};
use serde_json::{from_str, to_string, Value};

#[test]
fn serde_snapshot_accepts_empty_zero_capacity_and_allows_insertion() {
    let snapshot = r#"{"inner":[[]],"index":{"inner":[0]},"node_capacity":0,"len":0}"#;
    let mut set: BTreeSet<u8> = from_str(snapshot).unwrap();
    assert!(set.is_empty());

    for value in [2, 1, 3] {
        assert!(set.insert(value));
    }
    assert_eq!(set.iter().copied().collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(from_str::<BTreeSet<u8>>(&to_string(&set).unwrap()).unwrap(), set);
}

#[test]
fn serde_snapshot_repairs_legacy_empty_nodes() {
    let snapshots = [
        r#"{"inner":[[],[1,2]],"index":{"inner":[0,2]},"node_capacity":1,"len":2}"#,
        r#"{"inner":[[],[1,2]],"index":{"inner":[0,2]},"node_capacity":1024,"len":2}"#,
    ];
    for snapshot in snapshots {
        let mut set: BTreeSet<u8> = from_str(snapshot).unwrap();
        assert_eq!(set.get_index(0), Some(&1));
        assert_eq!(set.get_index(1), Some(&2));
        assert!(set.contains(&1));
        assert!(set.contains(&2));
        assert!(set.insert(0));
        assert!(set.insert(3));
        assert!(!set.insert(2));
        assert_eq!(set.iter().copied().collect::<Vec<_>>(), [0, 1, 2, 3]);
        assert_eq!(from_str::<BTreeSet<u8>>(&to_string(&set).unwrap()).unwrap(), set);
    }

    let empty = r#"{"inner":[[],[]],"index":{"inner":[0,0]},"node_capacity":1,"len":0}"#;
    let set: BTreeSet<u8> = from_str(empty).unwrap();
    assert_eq!(set, BTreeSet::with_maximum_node_size(1));
}

#[test]
fn serde_snapshot_rejects_invalid_original_metadata_and_order() {
    let valid = r#"{"inner":[[1,2],[4,5]],"index":{"inner":[2,4]},"node_capacity":2,"len":4}"#;
    let set: BTreeSet<u8> = from_str(valid).unwrap();
    assert_eq!(set.get(&2), Some(&2));
    assert_eq!(set.get_index(2), Some(&4));

    let invalid = [
        r#"{"inner":[[1,4],[2,5]],"index":{"inner":[2,4]},"node_capacity":2,"len":4}"#,
        r#"{"inner":[[],[1,2]],"index":{"inner":[0,2]},"node_capacity":1,"len":3}"#,
        r#"{"inner":[[],[1,2]],"index":{"inner":[2]},"node_capacity":1,"len":2}"#,
    ];
    for snapshot in invalid {
        assert!(from_str::<BTreeSet<u8>>(snapshot).is_err(), "{snapshot}");
    }
}

#[test]
fn serde_snapshot_validates_map_keys() {
    let valid = r#"{"set":{"inner":[[{"key":1,"value":"a"},{"key":2,"value":"b"}]],
        "index":{"inner":[2]},"node_capacity":2,"len":2}}"#;
    let map: BTreeMap<u8, Value> = from_str(valid).unwrap();
    assert_eq!(map.get(&2).and_then(Value::as_str), Some("b"));

    let duplicate = r#"{"set":{"inner":[[{"key":1,"value":"a"}],[{"key":1,"value":"b"}]],
        "index":{"inner":[1,2]},"node_capacity":2,"len":2}}"#;
    assert!(from_str::<BTreeMap<u8, Value>>(duplicate).is_err());
}

#[cfg(target_pointer_width = "64")]
#[test]
fn serde_snapshot_can_clear_a_large_threshold() {
    let snapshot = r#"{"inner":[[]],"index":{"inner":[0]},"node_capacity":4611686018427387904,"len":0}"#;
    let mut set: BTreeSet<u8> = from_str(snapshot).unwrap();
    set.clear();
    assert!(set.is_empty());
    assert!(set.insert(7));
    assert_eq!(set.get_index(0), Some(&7));
}

use aether::storage::{RocksStorage, StorageEngine, WriteOp};
use tempfile::tempdir;

#[test]
fn test_put_get_roundtrip() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    storage.put(b"key1", b"value1").unwrap();
    let result = storage.get(b"key1").unwrap();
    assert_eq!(result, Some(b"value1".to_vec()));
}

#[test]
fn test_get_missing_key() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    let result = storage.get(b"missing").unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_put_overwrite() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    storage.put(b"key1", b"value1").unwrap();
    storage.put(b"key1", b"value2").unwrap();
    let result = storage.get(b"key1").unwrap();
    assert_eq!(result, Some(b"value2".to_vec()));
}

#[test]
fn test_delete() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    storage.put(b"key1", b"value1").unwrap();
    storage.delete(b"key1").unwrap();
    let result = storage.get(b"key1").unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_delete_missing_key() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    // Deleting a non-existent key should not error
    storage.delete(b"missing").unwrap();
}

#[test]
fn test_scan_prefix() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    storage.put(b"prefix:1", b"v1").unwrap();
    storage.put(b"prefix:2", b"v2").unwrap();
    storage.put(b"prefix:3", b"v3").unwrap();
    storage.put(b"other:1", b"v4").unwrap();

    let results = storage.scan(b"prefix:", 10).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].key, b"prefix:1");
    assert_eq!(results[1].key, b"prefix:2");
    assert_eq!(results[2].key, b"prefix:3");
}

#[test]
fn test_scan_with_limit() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    storage.put(b"key:1", b"v1").unwrap();
    storage.put(b"key:2", b"v2").unwrap();
    storage.put(b"key:3", b"v3").unwrap();

    let results = storage.scan(b"key:", 2).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_scan_empty_prefix() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    let results = storage.scan(b"nonexistent:", 10).unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_batch_write() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    let ops = vec![
        WriteOp::Put {
            key: b"batch:1".to_vec(),
            value: b"v1".to_vec(),
        },
        WriteOp::Put {
            key: b"batch:2".to_vec(),
            value: b"v2".to_vec(),
        },
        WriteOp::Put {
            key: b"batch:3".to_vec(),
            value: b"v3".to_vec(),
        },
    ];

    storage.batch_write(ops).unwrap();

    assert_eq!(storage.get(b"batch:1").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(storage.get(b"batch:2").unwrap(), Some(b"v2".to_vec()));
    assert_eq!(storage.get(b"batch:3").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn test_batch_write_mixed_ops() {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();

    storage.put(b"existing", b"value").unwrap();

    let ops = vec![
        WriteOp::Put {
            key: b"new_key".to_vec(),
            value: b"new_value".to_vec(),
        },
        WriteOp::Delete {
            key: b"existing".to_vec(),
        },
    ];

    storage.batch_write(ops).unwrap();

    assert_eq!(
        storage.get(b"new_key").unwrap(),
        Some(b"new_value".to_vec())
    );
    assert_eq!(storage.get(b"existing").unwrap(), None);
}

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use tempfile::tempdir;

use aether::storage::{RocksStorage, StorageEngine, WriteOp};

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage::put");
    for size in [256, 1024, 64 * 1024] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let dir = tempdir().unwrap();
            let storage = RocksStorage::open(dir.path()).unwrap();
            let value = vec![0u8; size];
            let mut i = 0u64;
            b.iter(|| {
                let key = format!("bench-key-{i:08}");
                storage.put(key.as_bytes(), &value).unwrap();
                i += 1;
            });
        });
    }
    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage::get");

    // get_hit
    group.bench_function("hit", |b| {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::open(dir.path()).unwrap();
        let value = vec![0u8; 1024];
        for i in 0..1000 {
            let key = format!("key-{i:08}");
            storage.put(key.as_bytes(), &value).unwrap();
        }
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("key-{i:08}");
            storage.get(key.as_bytes()).unwrap();
            i = (i + 1) % 1000;
        });
    });

    // get_miss
    group.bench_function("miss", |b| {
        let dir = tempdir().unwrap();
        let storage = RocksStorage::open(dir.path()).unwrap();
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("missing-{i:08}");
            storage.get(key.as_bytes()).unwrap();
            i += 1;
        });
    });

    group.finish();
}

fn bench_delete(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let storage = RocksStorage::open(dir.path()).unwrap();
    // Pre-fill
    for i in 0..10000 {
        let key = format!("del-{i:08}");
        storage.put(key.as_bytes(), b"v").unwrap();
    }
    let mut i = 0u64;
    c.bench_function("storage::delete", |b| {
        b.iter(|| {
            let key = format!("del-{i:08}");
            storage.delete(key.as_bytes()).unwrap();
            i += 1;
        });
    });
}

fn bench_batch_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage::batch_write");
    for count in [10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let dir = tempdir().unwrap();
            let storage = RocksStorage::open(dir.path()).unwrap();
            let value = vec![0u8; 256];
            let mut batch_id = 0u64;
            b.iter(|| {
                let ops: Vec<WriteOp> = (0..count)
                    .map(|j| {
                        let key = format!("batch-{batch_id:08}-{j:04}");
                        WriteOp::Put {
                            key: key.into_bytes(),
                            value: value.clone(),
                        }
                    })
                    .collect();
                storage.batch_write(ops).unwrap();
                batch_id += 1;
            });
        });
    }
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage::scan");
    for count in [100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let dir = tempdir().unwrap();
            let storage = RocksStorage::open(dir.path()).unwrap();
            let value = vec![0u8; 256];
            for i in 0..count {
                let key = format!("scan:bench:{i:08}");
                storage.put(key.as_bytes(), &value).unwrap();
            }
            b.iter(|| {
                storage.scan(b"scan:bench:", count).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put,
    bench_get,
    bench_delete,
    bench_batch_write,
    bench_scan
);
criterion_main!(benches);

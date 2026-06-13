use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use aether::storage::mvcc::{KeyIndex, MvccValue, decode_mvcc_key, encode_mvcc_key, mvcc_user_key};

fn bench_mvcc_key_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec::mvcc_key");
    for key_size in [8, 64, 256] {
        group.bench_with_input(
            BenchmarkId::new("encode", key_size),
            &key_size,
            |b, &key_size| {
                let key = vec![0u8; key_size];
                b.iter(|| encode_mvcc_key(&key, 42));
            },
        );
    }

    group.bench_function("decode", |b| {
        let encoded = encode_mvcc_key(b"bench-key", 42);
        b.iter(|| decode_mvcc_key(&encoded).unwrap());
    });

    group.bench_function("mvcc_user_key", |b| {
        let encoded = encode_mvcc_key(b"bench-key", 42);
        b.iter(|| mvcc_user_key(&encoded).unwrap());
    });

    group.finish();
}

fn bench_key_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec::key_index");

    group.bench_function("put", |b| {
        let mut ki = KeyIndex::new();
        let mut rev = 1u64;
        b.iter(|| {
            ki.put(rev);
            rev += 1;
        });
    });

    group.bench_function("get_latest", |b| {
        let mut ki = KeyIndex::new();
        for i in 1..=100 {
            ki.put(i);
        }
        b.iter(|| {
            ki.get(0);
        });
    });

    group.bench_function("get_specific_rev", |b| {
        let mut ki = KeyIndex::new();
        for i in 1..=100 {
            ki.put(i);
        }
        b.iter(|| {
            ki.get(50);
        });
    });

    group.bench_function("encode", |b| {
        let mut ki = KeyIndex::new();
        for i in 1..=10 {
            ki.put(i);
        }
        b.iter(|| ki.encode());
    });

    group.bench_function("decode", |b| {
        let mut ki = KeyIndex::new();
        for i in 1..=10 {
            ki.put(i);
        }
        let encoded = ki.encode();
        b.iter(|| KeyIndex::decode(&encoded).unwrap());
    });

    group.finish();
}

fn bench_mvcc_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec::mvcc_value");

    for val_size in [64, 1024, 64 * 1024] {
        group.bench_with_input(
            BenchmarkId::new("serialize", val_size),
            &val_size,
            |b, &val_size| {
                let mv = MvccValue {
                    create_revision: 1,
                    mod_revision: 2,
                    version: 1,
                    lease: 0,
                    value: vec![0u8; val_size],
                };
                b.iter(|| rkyv::to_bytes::<rkyv::rancor::BoxedError>(&mv).unwrap());
            },
        );

        group.bench_with_input(
            BenchmarkId::new("deserialize", val_size),
            &val_size,
            |b, &val_size| {
                let mv = MvccValue {
                    create_revision: 1,
                    mod_revision: 2,
                    version: 1,
                    lease: 0,
                    value: vec![0u8; val_size],
                };
                let bytes = rkyv::to_bytes::<rkyv::rancor::BoxedError>(&mv).unwrap();
                b.iter(|| rkyv::from_bytes::<MvccValue, rkyv::rancor::BoxedError>(&bytes).unwrap());
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_mvcc_key_encode,
    bench_key_index,
    bench_mvcc_value
);
criterion_main!(benches);

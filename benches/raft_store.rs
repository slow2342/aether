use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use raft::eraftpb::{ConfState, Entry, HardState};
use raft::storage::Storage;
use tempfile::tempdir;

use aether::raft::raftrs_store::RaftRsStore;

fn open_store() -> (tempfile::TempDir, RaftRsStore) {
    let dir = tempdir().unwrap();
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db =
        rocksdb::DB::open_cf(&opts, dir.path(), ["default", "raft_log", "raft_state"]).unwrap();
    (dir, RaftRsStore::new(Arc::new(db)))
}

fn make_entries(start: u64, count: u64, term: u64, data_size: usize) -> Vec<Entry> {
    (start..start + count)
        .map(|index| Entry {
            index,
            term,
            data: vec![0u8; data_size],
            ..Default::default()
        })
        .collect()
}

fn bench_append_entries(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_store::append_entries");
    for count in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let (_dir, store) = open_store();
            let mut index = 1u64;
            b.iter(|| {
                let entries = make_entries(index, count, 1, 128);
                store.append_entries(&entries).unwrap();
                index += count;
            });
        });
    }
    group.finish();
}

fn bench_entries_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_store::entries_read");
    for count in [100, 1000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            let (_dir, store) = open_store();
            let entries = make_entries(1, count, 1, 128);
            store.append_entries(&entries).unwrap();
            b.iter(|| {
                let _ = store.entries(
                    1,
                    count + 1,
                    None,
                    raft::storage::GetEntriesContext::empty(false),
                );
            });
        });
    }
    group.finish();
}

fn bench_save_hard_state(c: &mut Criterion) {
    let (_dir, store) = open_store();
    let hs = HardState {
        term: 5,
        vote: 2,
        commit: 10,
    };
    c.bench_function("raft_store::save_hard_state", |b| {
        b.iter(|| {
            store.save_hard_state(&hs).unwrap();
        });
    });
}

fn bench_compact(c: &mut Criterion) {
    let (_dir, store) = open_store();
    let entries = make_entries(1, 1000, 1, 128);
    store.append_entries(&entries).unwrap();
    let mut compact_index = 100u64;
    c.bench_function("raft_store::compact_100", |b| {
        b.iter(|| {
            store.compact(compact_index).unwrap();
            compact_index += 100;
        });
    });
}

fn bench_save_conf_state(c: &mut Criterion) {
    let (_dir, store) = open_store();
    let mut cs = ConfState::default();
    cs.set_voters(vec![1, 2, 3]);
    c.bench_function("raft_store::save_conf_state", |b| {
        b.iter(|| {
            store.save_conf_state(&cs).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_append_entries,
    bench_entries_read,
    bench_save_hard_state,
    bench_compact,
    bench_save_conf_state,
);
criterion_main!(benches);

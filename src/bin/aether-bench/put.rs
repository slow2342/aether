use std::time::Instant;

use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::common::{generate_key, random_bytes};
use crate::report::{BenchResult, Report};
use aether::proto::PutRequest;
use aether::proto::aether_kv_client::AetherKvClient;

pub struct PutConfig {
    pub endpoint: String,
    pub total: usize,
    pub clients: usize,
    pub key_size: usize,
    pub val_size: usize,
    pub sequential: bool,
}

pub async fn run(config: PutConfig) {
    let PutConfig {
        endpoint,
        total,
        clients,
        key_size,
        val_size,
        sequential,
    } = config;

    println!("Benchmark: PUT");
    println!("  Endpoint:   {endpoint}");
    println!("  Total:      {total}");
    println!("  Clients:    {clients}");
    println!("  Key size:   {key_size}");
    println!("  Value size: {val_size}");
    println!("  Sequential: {sequential}");
    println!();

    // Create per-worker channels (no shared lock, no backpressure)
    let mut worker_txs: Vec<mpsc::UnboundedSender<(Vec<u8>, Vec<u8>)>> =
        Vec::with_capacity(clients);
    let (report_tx, mut report_rx) = mpsc::channel::<BenchResult>(total);

    for _ in 0..clients {
        let (tx, mut rx) = mpsc::unbounded_channel::<(Vec<u8>, Vec<u8>)>();
        worker_txs.push(tx);

        let endpoint = endpoint.clone();
        let report_tx = report_tx.clone();

        tokio::spawn(async move {
            let channel = Channel::from_shared(format!("http://{endpoint}"))
                .unwrap()
                .connect()
                .await
                .expect("failed to connect");
            let mut client = AetherKvClient::new(channel);

            while let Some((key, value)) = rx.recv().await {
                let start = Instant::now();
                let result = client
                    .put(PutRequest {
                        key,
                        value,
                        lease: 0,
                        prev_kv: false,
                    })
                    .await;
                let end = Instant::now();
                let err = result.err().map(|e| {
                    let msg = format!("{}: {}", e.code(), e.message());
                    eprintln!("ERROR: {msg}");
                    msg
                });
                let _ = report_tx.send(BenchResult { start, end, err }).await;
            }
        });
    }

    drop(report_tx);

    // Generate and distribute requests round-robin
    let wall_start = Instant::now();
    for i in 0..total {
        let key = generate_key(i as u64, key_size, sequential);
        let value = random_bytes(val_size);
        let worker = i % clients;
        if worker_txs[worker].send((key, value)).is_err() {
            break;
        }
    }
    // Close all worker channels
    drop(worker_txs);

    // Collect results
    let mut report = Report::new();
    while let Some(result) = report_rx.recv().await {
        report.push(result);
    }

    let wall_elapsed = wall_start.elapsed();

    println!("{}", report.format(wall_elapsed));
}

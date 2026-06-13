use std::time::Instant;

use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::common::{generate_key, random_bytes};
use crate::report::{BenchResult, Report};
use aether::proto::aether_kv_client::AetherKvClient;
use aether::proto::{GetRequest, PutRequest};

pub struct RangeConfig {
    pub endpoint: String,
    pub total: usize,
    pub clients: usize,
    pub key_size: usize,
    pub val_size: usize,
}

pub async fn run(config: RangeConfig) {
    let RangeConfig {
        endpoint,
        total,
        clients,
        key_size,
        val_size,
    } = config;

    println!("Benchmark: RANGE (single key get)");
    println!("  Endpoint:   {endpoint}");
    println!("  Total:      {total}");
    println!("  Clients:    {clients}");
    println!("  Key size:   {key_size}");
    println!("  Value size: {val_size}");
    println!();

    // Step 1: Pre-fill data
    println!("Pre-filling {total} keys...");
    let fill_start = Instant::now();
    {
        let channel = Channel::from_shared(format!("http://{endpoint}"))
            .unwrap()
            .connect()
            .await
            .expect("failed to connect");
        let mut client = AetherKvClient::new(channel);

        for i in 0..total {
            let key = generate_key(i as u64, key_size, true);
            let value = random_bytes(val_size);
            client
                .put(PutRequest {
                    key,
                    value,
                    lease: 0,
                    prev_kv: false,
                })
                .await
                .expect("pre-fill put failed");
        }
    }
    println!(
        "Pre-fill done in {:.2} ms\n",
        fill_start.elapsed().as_secs_f64() * 1000.0
    );

    // Step 2: Benchmark reads with per-worker channels (no backpressure)
    let mut worker_txs: Vec<mpsc::UnboundedSender<Vec<u8>>> = Vec::with_capacity(clients);
    let (report_tx, mut report_rx) = mpsc::channel::<BenchResult>(total);

    for _ in 0..clients {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
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

            while let Some(key) = rx.recv().await {
                let start = Instant::now();
                let result = client
                    .get(GetRequest {
                        key,
                        range_end: vec![],
                        limit: 0,
                        revision: 0,
                        serializable: false,
                        sort_order: 0,
                        sort_target: 0,
                    })
                    .await;
                let end = Instant::now();
                let err = result.err().map(|e| e.message().to_string());
                let _ = report_tx.send(BenchResult { start, end, err }).await;
            }
        });
    }

    drop(report_tx);

    // Generate and distribute read requests round-robin
    let wall_start = Instant::now();
    for i in 0..total {
        let key = generate_key(i as u64, key_size, true);
        let worker = i % clients;
        if worker_txs[worker].send(key).is_err() {
            break;
        }
    }
    drop(worker_txs);

    // Collect results
    let mut report = Report::new();
    while let Some(result) = report_rx.recv().await {
        report.push(result);
    }

    let wall_elapsed = wall_start.elapsed();

    println!("{}", report.format(wall_elapsed));
}

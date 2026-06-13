use std::time::{Duration, Instant};

/// A single benchmark result.
#[derive(Debug)]
pub struct BenchResult {
    pub start: Instant,
    pub end: Instant,
    pub err: Option<String>,
}

impl BenchResult {
    pub fn latency(&self) -> Duration {
        self.end.duration_since(self.start)
    }
}

/// Collects benchmark results and generates reports.
#[derive(Debug, Default)]
pub struct Report {
    results: Vec<BenchResult>,
}

impl Report {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, result: BenchResult) {
        self.results.push(result);
    }

    /// Generate the full report string.
    /// `wall_duration` is the total wall-clock time for the benchmark run.
    pub fn format(&self, wall_duration: Duration) -> String {
        if self.results.is_empty() {
            return "No results collected.".to_string();
        }

        let mut lats: Vec<f64> = self
            .results
            .iter()
            .filter(|r| r.err.is_none())
            .map(|r| r.latency().as_secs_f64() * 1000.0) // ms
            .collect();

        if lats.is_empty() {
            return "All requests failed.".to_string();
        }

        lats.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let wall_ms = wall_duration.as_secs_f64() * 1000.0;
        let fastest = lats[0];
        let slowest = lats[lats.len() - 1];
        let average = lats.iter().sum::<f64>() / lats.len() as f64;
        let variance = lats.iter().map(|x| (x - average).powi(2)).sum::<f64>() / lats.len() as f64;
        let stddev = variance.sqrt();
        let rps = lats.len() as f64 / wall_duration.as_secs_f64();
        let error_count = self.results.iter().filter(|r| r.err.is_some()).count();

        let mut out = String::new();

        out.push_str("\nSummary:\n");
        out.push_str(&format!("  Wall time:    {:.2} ms\n", wall_ms));
        out.push_str(&format!("  Requests:     {}\n", lats.len()));
        if error_count > 0 {
            out.push_str(&format!("  Errors:       {}\n", error_count));
        }
        out.push_str(&format!("  Slowest:      {:.2} ms\n", slowest));
        out.push_str(&format!("  Fastest:      {:.2} ms\n", fastest));
        out.push_str(&format!("  Average:      {:.2} ms\n", average));
        out.push_str(&format!("  Stddev:       {:.2} ms\n", stddev));
        out.push_str(&format!("  RPS:          {:.2}\n", rps));

        out.push_str("\nLatency Distribution:\n");
        for p in [10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 99.0, 99.9] {
            let val = percentile(&lats, p);
            out.push_str(&format!("  {:>5.1}%:  {:.2} ms\n", p, val));
        }

        // Error distribution
        if error_count > 0 {
            out.push_str("\nError Distribution:\n");
            let mut err_counts: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for r in &self.results {
                if let Some(e) = &r.err {
                    *err_counts.entry(e.clone()).or_insert(0) += 1;
                }
            }
            for (err, count) in &err_counts {
                out.push_str(&format!("  {count}\t{err}\n"));
            }
        }

        out
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0 * sorted.len() as f64) as usize;
    let idx = idx.min(sorted.len() - 1);
    sorted[idx]
}

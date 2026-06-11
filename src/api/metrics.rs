use std::sync::Arc;
use std::time::Instant;

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts, Registry,
    TextEncoder,
};
use tower::{Layer, Service};

/// Central registry for all Prometheus metrics.
#[derive(Clone)]
pub struct MetricsRegistry {
    pub registry: Registry,

    // gRPC
    pub grpc_requests_total: IntCounterVec,
    pub grpc_request_duration_seconds: HistogramVec,

    // Raft
    pub raft_proposals_total: IntCounterVec,
    pub raft_leader_changes_total: IntCounter,

    // Watch
    pub active_watchers: Gauge,

    // Lease
    pub active_leases: Gauge,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Registry::new();

        let grpc_requests_total = IntCounterVec::new(
            Opts::new("aether_grpc_requests_total", "Total gRPC requests"),
            &["method", "code"],
        )
        .unwrap();

        let grpc_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "aether_grpc_request_duration_seconds",
                "gRPC request duration in seconds",
            ),
            &["method"],
        )
        .unwrap();

        let raft_proposals_total = IntCounterVec::new(
            Opts::new("aether_raft_proposals_total", "Total Raft proposals"),
            &["result"],
        )
        .unwrap();

        let raft_leader_changes_total = IntCounter::new(
            "aether_raft_leader_changes_total",
            "Total Raft leader changes",
        )
        .unwrap();

        let active_watchers =
            Gauge::new("aether_active_watchers", "Number of active watchers").unwrap();

        let active_leases = Gauge::new("aether_active_leases", "Number of active leases").unwrap();

        registry
            .register(Box::new(grpc_requests_total.clone()))
            .unwrap();
        registry
            .register(Box::new(grpc_request_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(raft_proposals_total.clone()))
            .unwrap();
        registry
            .register(Box::new(raft_leader_changes_total.clone()))
            .unwrap();
        registry
            .register(Box::new(active_watchers.clone()))
            .unwrap();
        registry.register(Box::new(active_leases.clone())).unwrap();

        Self {
            registry,
            grpc_requests_total,
            grpc_request_duration_seconds,
            raft_proposals_total,
            raft_leader_changes_total,
            active_watchers,
            active_leases,
        }
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn gather(&self) -> String {
        let metric_families = self.registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// --- Tower Layer / Service for automatic gRPC request metrics ---

/// Tower layer that records metrics for every gRPC request.
#[derive(Clone)]
pub struct MetricsLayer {
    registry: Arc<MetricsRegistry>,
}

impl MetricsLayer {
    pub fn new(registry: Arc<MetricsRegistry>) -> Self {
        Self { registry }
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService {
            inner,
            registry: self.registry.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
    registry: Arc<MetricsRegistry>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for MetricsService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let method = req
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or("unknown")
            .to_string();

        let registry = self.registry.clone();
        let start = Instant::now();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let result = fut.await;
            let elapsed = start.elapsed().as_secs_f64();

            registry
                .grpc_request_duration_seconds
                .with_label_values(&[&method])
                .observe(elapsed);

            let code = match &result {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() { "OK" } else { "ERROR" }
                }
                Err(_) => "ERROR",
            };

            registry
                .grpc_requests_total
                .with_label_values(&[&method, code])
                .inc();

            result
        })
    }
}

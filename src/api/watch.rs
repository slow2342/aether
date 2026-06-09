use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::proto::aether_watch_server::AetherWatch;
use crate::proto::{
    WatchCancelRequest, WatchCreateRequest, WatchRequest, WatchResponse, watch_request,
};
use crate::watch::WatchManager;

pub struct WatchService {
    manager: Arc<WatchManager>,
}

impl WatchService {
    pub fn new(manager: Arc<WatchManager>) -> Self {
        Self { manager }
    }
}

type WatchStream = Pin<Box<dyn Stream<Item = Result<WatchResponse, Status>> + Send>>;

#[tonic::async_trait]
impl AetherWatch for WatchService {
    type WatchStream = WatchStream;

    async fn watch(
        &self,
        request: Request<Streaming<WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut inbound = request.into_inner();
        let manager = self.manager.clone();

        let (tx, rx) = mpsc::channel::<Result<WatchResponse, Status>>(256);

        tokio::spawn(async move {
            // Track active watch_id -> cancel sender for this stream.
            let mut active_watches: std::collections::HashMap<i64, mpsc::Sender<()>> =
                std::collections::HashMap::new();

            loop {
                match inbound.message().await {
                    Ok(Some(watch_req)) => {
                        match watch_req.request {
                            Some(watch_request::Request::Create(create)) => {
                                handle_create(&manager, &tx, &mut active_watches, create).await;
                            }
                            Some(watch_request::Request::Cancel(cancel)) => {
                                handle_cancel(&manager, &tx, &mut active_watches, cancel).await;
                            }
                            None => {
                                // Empty request, ignore.
                            }
                        }
                    }
                    Ok(None) => {
                        // Client closed the stream. Cancel all active watches.
                        for (watch_id, _) in active_watches.drain() {
                            manager.cancel(watch_id, "stream closed".to_string()).await;
                        }
                        break;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "watch stream error");
                        for (watch_id, _) in active_watches.drain() {
                            manager.cancel(watch_id, format!("stream error: {e}")).await;
                        }
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as WatchStream))
    }
}

async fn handle_create(
    manager: &Arc<WatchManager>,
    tx: &mpsc::Sender<Result<WatchResponse, Status>>,
    active_watches: &mut std::collections::HashMap<i64, mpsc::Sender<()>>,
    create: WatchCreateRequest,
) {
    let filters: Vec<crate::raft::WatchEventType> = create
        .filters
        .into_iter()
        .map(|f| match f {
            0 => crate::raft::WatchEventType::Put,
            1 => crate::raft::WatchEventType::Delete,
            _ => crate::raft::WatchEventType::Put,
        })
        .collect();

    let (watch_id, mut rx) = manager
        .create(create.key, create.range_end, filters, create.prev_kv)
        .await;

    // Send the "created" acknowledgment.
    let created_resp = WatchResponse {
        watch_id,
        created: true,
        events: vec![],
        canceled: false,
        cancel_reason: String::new(),
        header: Some(crate::proto::ResponseHeader {
            cluster_id: 0,
            member_id: 0,
            revision: 0,
            raft_term: 0,
        }),
    };
    if tx.send(Ok(created_resp)).await.is_err() {
        manager.cancel(watch_id, "stream closed".to_string()).await;
        return;
    }

    // Spawn a task to forward events from this watch to the gRPC stream.
    let manager_clone = manager.clone();
    let tx_clone = tx.clone();
    let (cancel_tx, mut cancel_rx) = mpsc::channel::<()>(1);

    active_watches.insert(watch_id, cancel_tx);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                resp = rx.recv() => {
                    match resp {
                        Some(internal_resp) => {
                            let proto_events = internal_resp
                                .events
                                .into_iter()
                                .map(|e| crate::proto::WatchEvent {
                                    event_type: e.event_type as i32,
                                    kv: Some(crate::proto::KeyValue {
                                        key: e.kv.key,
                                        value: e.kv.value,
                                        create_revision: e.kv.create_revision,
                                        mod_revision: e.kv.mod_revision,
                                        version: e.kv.version,
                                        lease: e.kv.lease,
                                    }),
                                    prev_kv: e.prev_kv.map(|kv| crate::proto::KeyValue {
                                        key: kv.key,
                                        value: kv.value,
                                        create_revision: kv.create_revision,
                                        mod_revision: kv.mod_revision,
                                        version: kv.version,
                                        lease: kv.lease,
                                    }),
                                })
                                .collect();

                            let resp = WatchResponse {
                                watch_id: internal_resp.watch_id,
                                created: internal_resp.created,
                                events: proto_events,
                                canceled: internal_resp.canceled,
                                cancel_reason: internal_resp.cancel_reason,
                                header: None,
                            };
                            if tx_clone.send(Ok(resp)).await.is_err() {
                                // Stream closed.
                                manager_clone.cancel(watch_id, "stream closed".to_string()).await;
                                return;
                            }
                            if internal_resp.canceled {
                                return;
                            }
                        }
                        None => {
                            // Channel closed, watch was canceled.
                            return;
                        }
                    }
                }
                _ = cancel_rx.recv() => {
                    // Explicit cancel requested.
                    manager_clone.cancel(watch_id, "client cancel".to_string()).await;
                    return;
                }
            }
        }
    });
}

async fn handle_cancel(
    manager: &Arc<WatchManager>,
    _tx: &mpsc::Sender<Result<WatchResponse, Status>>,
    active_watches: &mut std::collections::HashMap<i64, mpsc::Sender<()>>,
    cancel: WatchCancelRequest,
) {
    if let Some(cancel_tx) = active_watches.remove(&cancel.watch_id) {
        // Signal the forwarding task to stop.
        let _ = cancel_tx.send(()).await;
    }
    manager
        .cancel(cancel.watch_id, "client cancel".to_string())
        .await;
}

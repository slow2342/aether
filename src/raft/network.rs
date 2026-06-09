use std::sync::Arc;
use std::time::Duration;

use prost011::Message as ProstMessage;
use raft::eraftpb::{Message, MessageType};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::proto::raft_rpc as pb;
use crate::proto::raft_rpc::raft_rpc_client::RaftRpcClient;

const APPEND_ENTRIES_TIMEOUT: Duration = Duration::from_secs(5);
const VOTE_TIMEOUT: Duration = Duration::from_secs(2);
const INSTALL_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(60);

/// Shared client pool for concurrent message sending.
type ClientPool = Arc<Mutex<std::collections::HashMap<u64, RaftRpcClient<Channel>>>>;

pub struct NetworkSender {
    msg_rx: tokio::sync::mpsc::Receiver<Vec<Message>>,
    node_id: u64,
    clients: ClientPool,
    addrs: Arc<std::collections::HashMap<u64, String>>,
}

impl NetworkSender {
    pub fn new(
        msg_rx: tokio::sync::mpsc::Receiver<Vec<Message>>,
        node_id: u64,
        addrs: std::collections::HashMap<u64, String>,
    ) -> Self {
        Self {
            msg_rx,
            node_id,
            clients: Arc::new(Mutex::new(std::collections::HashMap::new())),
            addrs: Arc::new(addrs),
        }
    }

    pub async fn run(&mut self) {
        tracing::info!(node_id = self.node_id, "network sender started");
        while let Some(messages) = self.msg_rx.recv().await {
            let futs: Vec<_> = messages
                .into_iter()
                .map(|msg| {
                    let clients = self.clients.clone();
                    let addrs = self.addrs.clone();
                    tokio::spawn(async move { send_message(clients, addrs, msg).await })
                })
                .collect();

            for fut in futs {
                if let Err(e) = fut.await {
                    tracing::warn!(node_id = self.node_id, error = %e, "failed to send raft message");
                }
            }
        }
    }
}

async fn get_or_connect(
    clients: &ClientPool,
    addrs: &std::collections::HashMap<u64, String>,
    to: u64,
) -> Result<RaftRpcClient<Channel>, Box<dyn std::error::Error + Send + Sync>> {
    let mut pool = clients.lock().await;
    if let std::collections::hash_map::Entry::Vacant(e) = pool.entry(to) {
        let addr = addrs
            .get(&to)
            .ok_or_else(|| format!("unknown target node {to}"))?;
        let uri = format!("http://{}", addr);
        let client = RaftRpcClient::connect(uri).await?;
        e.insert(client);
    }
    pool.get(&to)
        .cloned()
        .ok_or_else(|| "client not found after insert".into())
}

async fn send_message(
    clients: ClientPool,
    addrs: Arc<std::collections::HashMap<u64, String>>,
    msg: Message,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let to = msg.to;
    if to == 0 {
        return Ok(());
    }

    let payload = msg.encode_to_vec();
    let msg_type = MessageType::from_i32(msg.msg_type).unwrap_or(MessageType::MsgHup);

    let mut client = get_or_connect(&clients, &addrs, to).await?;

    match msg_type {
        MessageType::MsgRequestVote | MessageType::MsgRequestVoteResponse => {
            let request = tonic::Request::new(pb::VoteRequest { payload });
            tokio::time::timeout(VOTE_TIMEOUT, client.vote(request))
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }
        MessageType::MsgSnapshot => {
            let request = tonic::Request::new(pb::InstallSnapshotRequest { payload });
            tokio::time::timeout(INSTALL_SNAPSHOT_TIMEOUT, client.install_snapshot(request))
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }
        _ => {
            let request = tonic::Request::new(pb::AppendEntriesRequest { payload });
            tokio::time::timeout(APPEND_ENTRIES_TIMEOUT, client.append_entries(request))
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        }
    }

    Ok(())
}

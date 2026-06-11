use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{self as cc, Receiver as CcReceiver};
use prost011::Message as ProstMessage;
use raft::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType, Message};
use raft::storage::Storage;
use raft::{RawNode, StateRole};
use tokio::sync::{mpsc, oneshot};

use super::handle::RaftError;
use super::raftrs_store::RaftRsStore;
use super::state_machine::AetherStateMachine;

/// Proposal from a client, with a oneshot channel for the response.
pub struct ProposeRequest {
    pub data: Vec<u8>,
    pub tx: oneshot::Sender<Result<Vec<u8>, RaftError>>,
}

/// Raft node configuration passed to the event loop.
struct RaftNodeConfig {
    config: raft::Config,
    store: RaftRsStore,
    state_machine: Arc<Mutex<AetherStateMachine>>,
    initial_peers: Vec<(u64, String)>,
}

/// Channels used by the event loop to communicate with the outside world.
struct EventLoopChannels {
    msg_in_rx: CcReceiver<Message>,
    propose_rx: CcReceiver<ProposeRequest>,
    conf_change_rx: CcReceiver<ConfChangeRequest>,
    msg_out_tx: mpsc::Sender<Vec<Message>>,
}

/// A ConfChange proposal with a oneshot channel for the result.
pub struct ConfChangeRequest {
    pub cc: ConfChange,
    pub tx: oneshot::Sender<Result<(), RaftError>>,
}

/// Shared state between the event loop and RaftHandleImpl.
#[derive(Default)]
pub struct RaftSharedState {
    pub leader_id: AtomicU64,
    /// Last committed index seen by the event loop (for ReadIndex).
    pub commit_index: AtomicU64,
    /// Last applied index (updated after state machine apply, for ReadIndex wait).
    pub applied_index: AtomicU64,
}

/// Handle to a running raft node, returned by [`start_raft_node`].
pub struct RaftNodeHandle {
    /// Join handle for the event loop thread.
    pub thread_handle: JoinHandle<()>,
    /// Sender for incoming raft messages (from rpc.rs).
    pub msg_tx: mpsc::Sender<Message>,
    /// Sender for client proposals (from RaftHandleImpl).
    pub propose_tx: mpsc::Sender<ProposeRequest>,
    /// Sender for ConfChange proposals (add_learner, change_membership).
    pub conf_change_tx: mpsc::Sender<ConfChangeRequest>,
    /// Shared state for leader tracking.
    pub shared_state: Arc<RaftSharedState>,
}

/// Start the raft event loop on a dedicated thread.
pub fn start_raft_node(
    config: raft::Config,
    store: RaftRsStore,
    state_machine: Arc<Mutex<AetherStateMachine>>,
    msg_out_tx: mpsc::Sender<Vec<Message>>,
    initial_peers: Vec<(u64, String)>,
) -> anyhow::Result<RaftNodeHandle> {
    // Tokio channels for the RPC server and handle to send into.
    let (msg_in_tx, mut msg_in_rx) = mpsc::channel::<Message>(1024);
    let (propose_tx, mut propose_rx) = mpsc::channel::<ProposeRequest>(1024);
    let (conf_change_tx, mut conf_change_rx) = mpsc::channel::<ConfChangeRequest>(1024);

    // Crossbeam channels for the event loop's blocking select!.
    let (cc_msg_tx, cc_msg_rx) = cc::bounded::<Message>(1024);
    let (cc_propose_tx, cc_propose_rx) = cc::bounded::<ProposeRequest>(1024);
    let (cc_conf_tx, cc_conf_rx) = cc::bounded::<ConfChangeRequest>(1024);

    // Bridge: forward from tokio channels to crossbeam channels.
    tokio::spawn(async move {
        while let Some(msg) = msg_in_rx.recv().await {
            if cc_msg_tx.send(msg).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(req) = propose_rx.recv().await {
            if cc_propose_tx.send(req).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        while let Some(req) = conf_change_rx.recv().await {
            if cc_conf_tx.send(req).is_err() {
                break;
            }
        }
    });

    let shared_state = Arc::new(RaftSharedState::default());
    let shared = shared_state.clone();

    let node_config = RaftNodeConfig {
        config,
        store,
        state_machine,
        initial_peers,
    };
    let channels = EventLoopChannels {
        msg_in_rx: cc_msg_rx,
        propose_rx: cc_propose_rx,
        conf_change_rx: cc_conf_rx,
        msg_out_tx,
    };

    let handle = std::thread::spawn(move || {
        raft_event_loop(node_config, channels, shared);
    });

    Ok(RaftNodeHandle {
        thread_handle: handle,
        msg_tx: msg_in_tx,
        propose_tx,
        conf_change_tx,
        shared_state,
    })
}

/// Each tick is 100ms (matching raft-rs convention: election_tick=10 → 1s election timeout).
const TICK_MS: u64 = 100;

fn raft_event_loop(
    node_config: RaftNodeConfig,
    channels: EventLoopChannels,
    shared_state: Arc<RaftSharedState>,
) {
    let RaftNodeConfig {
        config,
        store,
        state_machine,
        initial_peers,
    } = node_config;
    let EventLoopChannels {
        msg_in_rx,
        propose_rx,
        conf_change_rx,
        msg_out_tx,
    } = channels;

    // Bootstrap: if store is empty, save initial ConfState before creating RawNode.
    // raft-rs reads ConfState during Raft::new() — without voters, become_leader panics.
    let needs_bootstrap =
        store.first_index().unwrap_or(1) == 1 && store.last_index().unwrap_or(0) == 0;
    if needs_bootstrap {
        let voter_ids: Vec<u64> = initial_peers.iter().map(|(id, _)| *id).collect();
        let cs = raft::eraftpb::ConfState {
            voters: voter_ids,
            ..Default::default()
        };
        if let Err(e) = store.save_conf_state(&cs) {
            tracing::error!(error = %e, "failed to save initial ConfState");
            return;
        }
        tracing::info!(node_id = config.id, voters = ?cs.voters, "bootstrapped ConfState");
    }

    let mut node = match RawNode::with_default_logger(&config, store) {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "failed to create raft node");
            return;
        }
    };

    // Normal proposals: request_id → response sender
    let mut pending: HashMap<u64, oneshot::Sender<Result<Vec<u8>, RaftError>>> = HashMap::new();
    // ConfChange proposals: (node_id, change_type) → response sender.
    // Keyed by identity rather than index so each commit resolves only its matching sender.
    let mut pending_conf: HashMap<(u64, i32), oneshot::Sender<Result<(), RaftError>>> =
        HashMap::new();
    let mut request_id_counter: u64 = 0;
    let mut was_leader = false;
    let tick_interval = Duration::from_millis(TICK_MS);
    let mut bootstrapped = false;

    if needs_bootstrap && initial_peers.len() == 1 && initial_peers[0].0 == config.id {
        tracing::info!(node_id = config.id, "bootstrapping single-node cluster");
        if let Err(e) = node.campaign() {
            tracing::error!(error = %e, "campaign failed during bootstrap");
        }
        bootstrapped = true;
    }

    // Maximum iterations before re-blocking on select. Prevents starvation when one
    // channel is always ready (e.g. continuous proposals).
    const MAX_SPIN: usize = 64;

    loop {
        // Drain all pending channel messages first, then tick.
        let mut msgs = Vec::new();
        let mut props = Vec::new();
        let mut ccs = Vec::new();

        // Collect all immediately available messages (non-blocking).
        while let Ok(msg) = msg_in_rx.try_recv() {
            msgs.push(msg);
        }
        while let Ok(prop) = propose_rx.try_recv() {
            props.push(prop);
        }
        while let Ok(cc_req) = conf_change_rx.try_recv() {
            ccs.push(cc_req);
        }

        // If nothing was immediately available, block until something arrives or tick fires.
        if msgs.is_empty() && props.is_empty() && ccs.is_empty() {
            cc::select! {
                recv(msg_in_rx) -> msg => {
                    if let Ok(m) = msg { msgs.push(m); }
                    while let Ok(m) = msg_in_rx.try_recv() { msgs.push(m); }
                }
                recv(propose_rx) -> prop => {
                    if let Ok(p) = prop { props.push(p); }
                    while let Ok(p) = propose_rx.try_recv() { props.push(p); }
                }
                recv(conf_change_rx) -> cc_req => {
                    if let Ok(c) = cc_req { ccs.push(c); }
                    while let Ok(c) = conf_change_rx.try_recv() { ccs.push(c); }
                }
                recv(cc::after(tick_interval)) -> _ => {}
            }
        }

        // Step incoming raft messages.
        for msg in msgs {
            if let Err(e) = node.step(msg) {
                tracing::warn!(error = %e, "step failed");
            }
        }

        // Process ConfChange proposals.
        for req in ccs {
            if node.raft.state != StateRole::Leader {
                let _ = req.tx.send(Err(RaftError::NotLeader {
                    leader: Some(node.raft.leader_id),
                }));
                continue;
            }
            if let Err(e) = node.propose_conf_change(vec![], req.cc.clone()) {
                let _ = req.tx.send(Err(RaftError::Internal(format!(
                    "propose_conf_change failed: {e}"
                ))));
                continue;
            }
            let key = (req.cc.node_id, req.cc.change_type);
            pending_conf.insert(key, req.tx);
        }

        // Process normal proposals.
        for prop in props {
            if node.raft.state != StateRole::Leader {
                let _ = prop.tx.send(Err(RaftError::NotLeader {
                    leader: Some(node.raft.leader_id),
                }));
                continue;
            }

            request_id_counter += 1;
            let id = request_id_counter;
            pending.insert(id, prop.tx);

            let id_bytes = id.to_be_bytes();
            let mut data = Vec::with_capacity(8 + prop.data.len());
            data.extend_from_slice(&id_bytes);
            data.extend_from_slice(&prop.data);

            if let Err(e) = node.propose(vec![], data) {
                tracing::error!(error = %e, "propose failed");
                if let Some(tx) = pending.remove(&id) {
                    let _ = tx.send(Err(RaftError::Internal(format!("propose failed: {e}"))));
                }
            }
        }

        if needs_bootstrap && !bootstrapped && node.raft.state == StateRole::Leader {
            let mut cc = ConfChange::default();
            cc.set_change_type(ConfChangeType::AddNode);
            cc.node_id = config.id;
            if let Err(e) = node.propose_conf_change(vec![], cc) {
                tracing::error!(error = %e, "bootstrap conf change failed");
            }
            bootstrapped = true;
            tracing::info!(node_id = config.id, "proposed initial ConfChange");
        }

        // Tick the raft state machine.
        node.tick();

        if !node.has_ready() {
            shared_state
                .leader_id
                .store(node.raft.leader_id, Ordering::Relaxed);
            continue;
        }

        // Process all available ready states before blocking again.
        for _ in 0..MAX_SPIN {
            if !node.has_ready() {
                break;
            }

            let mut ready = node.ready();

            if !ready.entries().is_empty()
                && let Err(e) = node.store().append_entries(ready.entries())
            {
                tracing::error!(error = %e, "failed to persist entries");
                return;
            }

            if let Some(hs) = ready.hs()
                && let Err(e) = node.store().save_hard_state(hs)
            {
                tracing::error!(error = %e, "failed to persist hard state");
                return;
            }

            let messages = ready.take_messages();
            if !messages.is_empty() {
                let _ = msg_out_tx.blocking_send(messages);
            }

            let snapshot = ready.snapshot();
            if !is_empty_snapshot(snapshot) {
                tracing::info!("received snapshot, not yet applied");
            }

            let committed = ready.take_committed_entries();
            if !committed.is_empty() {
                // Track the highest committed index for ReadIndex
                let max_committed = committed.last().map(|e| e.index).unwrap_or(0);
                shared_state
                    .commit_index
                    .store(max_committed, Ordering::Release);

                let mut sm = state_machine.lock().expect("state machine mutex poisoned");
                for entry in &committed {
                    apply_entry(&mut sm, entry, &mut pending, &mut pending_conf);
                }
                // Update applied_index after all entries are applied
                shared_state
                    .applied_index
                    .store(max_committed, Ordering::Release);
                drop(sm);

                // Persist ConfState for any conf change entries.
                for entry in &committed {
                    if entry.get_entry_type() == EntryType::EntryConfChange
                        && !entry.data.is_empty()
                        && let Ok(cc) = ConfChange::decode(entry.data.as_slice())
                    {
                        match node.apply_conf_change(&cc) {
                            Ok(cs) => {
                                if let Err(e) = node.store().save_conf_state(&cs) {
                                    tracing::error!(error = %e, "failed to persist conf state");
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    error = %e,
                                    index = entry.index,
                                    "failed to apply conf change to raft"
                                );
                            }
                        }
                    }
                }
            }

            let is_leader = node.raft.state == StateRole::Leader;
            if was_leader && !is_leader {
                for (_, tx) in pending.drain() {
                    let _ = tx.send(Err(RaftError::NotLeader { leader: None }));
                }
                for (_, tx) in pending_conf.drain() {
                    let _ = tx.send(Err(RaftError::NotLeader { leader: None }));
                }
            }
            was_leader = is_leader;

            shared_state
                .leader_id
                .store(node.raft.leader_id, Ordering::Relaxed);

            node.advance(ready);
        }
    }
}

fn apply_entry(
    sm: &mut AetherStateMachine,
    entry: &Entry,
    pending: &mut HashMap<u64, oneshot::Sender<Result<Vec<u8>, RaftError>>>,
    pending_conf: &mut HashMap<(u64, i32), oneshot::Sender<Result<(), RaftError>>>,
) {
    match EntryType::from_i32(entry.entry_type) {
        Some(EntryType::EntryNormal) => {
            if entry.data.is_empty() {
                return;
            }

            if entry.data.len() < 8 {
                tracing::warn!(index = entry.index, "entry data too short");
                return;
            }
            let request_id = u64::from_be_bytes(
                entry.data[..8]
                    .try_into()
                    .expect("entry data length already checked"),
            );

            let resp = sm.apply_normal_entry(&entry.data[8..], entry.index);

            if let Some(tx) = pending.remove(&request_id) {
                let _ = tx.send(resp.map_err(RaftError::Internal));
            }
        }
        Some(EntryType::EntryConfChange) => {
            match ConfChange::decode(entry.data.as_slice()) {
                Ok(cc) => {
                    sm.apply_conf_change(&cc, entry.index);
                    tracing::info!(
                        change_type = ?cc.change_type,
                        node_id = cc.node_id,
                        index = entry.index,
                        "applied conf change"
                    );
                    // Resolve only the sender that matches this specific ConfChange.
                    let key = (cc.node_id, cc.change_type);
                    if let Some(tx) = pending_conf.remove(&key) {
                        let _ = tx.send(Ok(()));
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, index = entry.index, "failed to decode conf change");
                }
            }
        }
        _ => {
            tracing::warn!(
                index = entry.index,
                entry_type = entry.entry_type,
                "unknown entry type"
            );
        }
    }
}

fn is_empty_snapshot(snap: &raft::eraftpb::Snapshot) -> bool {
    snap.get_data().is_empty() && snap.get_metadata().index == 0
}

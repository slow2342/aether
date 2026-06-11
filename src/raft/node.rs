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
use tokio::sync::{Notify, mpsc, oneshot};

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
    /// Number of committed log entries that triggers a snapshot.
    snapshot_trigger: u64,
}

/// Channels used by the event loop to communicate with the outside world.
struct EventLoopChannels {
    msg_in_rx: CcReceiver<Message>,
    propose_rx: CcReceiver<ProposeRequest>,
    conf_change_rx: CcReceiver<ConfChangeRequest>,
    read_index_rx: CcReceiver<ReadIndexRequest>,
    msg_out_tx: mpsc::Sender<Vec<Message>>,
}

/// A ConfChange proposal with a oneshot channel for the result.
pub struct ConfChangeRequest {
    pub cc: ConfChange,
    pub tx: oneshot::Sender<Result<(), RaftError>>,
}

/// A ReadIndex request with a unique context and a oneshot channel for the result.
/// The event loop calls `node.read_index(context)` which triggers a heartbeat-based
/// leader confirmation. Once confirmed, the commit index is returned.
pub struct ReadIndexRequest {
    pub context: Vec<u8>,
    pub tx: oneshot::Sender<Result<u64, RaftError>>,
}

/// Shared state between the event loop and RaftHandleImpl.
pub struct RaftSharedState {
    pub leader_id: AtomicU64,
    /// Last committed index seen by the event loop (for ReadIndex).
    pub commit_index: AtomicU64,
    /// Last applied index (updated after state machine apply, for ReadIndex wait).
    pub applied_index: AtomicU64,
    /// Notified after each apply batch so waiters can check applied_index.
    pub applied_notify: Notify,
}

impl Default for RaftSharedState {
    fn default() -> Self {
        Self {
            leader_id: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
            applied_notify: Notify::new(),
        }
    }
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
    /// Sender for ReadIndex requests (linearizable reads).
    pub read_index_tx: mpsc::Sender<ReadIndexRequest>,
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
    snapshot_trigger: u64,
) -> anyhow::Result<RaftNodeHandle> {
    // Tokio channels for the RPC server and handle to send into.
    let (msg_in_tx, mut msg_in_rx) = mpsc::channel::<Message>(1024);
    let (propose_tx, mut propose_rx) = mpsc::channel::<ProposeRequest>(1024);
    let (conf_change_tx, mut conf_change_rx) = mpsc::channel::<ConfChangeRequest>(1024);
    let (read_index_tx, mut read_index_rx) = mpsc::channel::<ReadIndexRequest>(1024);

    // Crossbeam channels for the event loop's blocking select!.
    let (cc_msg_tx, cc_msg_rx) = cc::bounded::<Message>(1024);
    let (cc_propose_tx, cc_propose_rx) = cc::bounded::<ProposeRequest>(1024);
    let (cc_conf_tx, cc_conf_rx) = cc::bounded::<ConfChangeRequest>(1024);
    let (cc_ri_tx, cc_ri_rx) = cc::bounded::<ReadIndexRequest>(1024);

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
    tokio::spawn(async move {
        while let Some(req) = read_index_rx.recv().await {
            if cc_ri_tx.send(req).is_err() {
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
        snapshot_trigger,
    };
    let channels = EventLoopChannels {
        msg_in_rx: cc_msg_rx,
        propose_rx: cc_propose_rx,
        conf_change_rx: cc_conf_rx,
        read_index_rx: cc_ri_rx,
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
        read_index_tx,
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
        snapshot_trigger,
    } = node_config;
    let EventLoopChannels {
        msg_in_rx,
        propose_rx,
        conf_change_rx,
        read_index_rx,
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
    // ReadIndex requests: context bytes → response sender.
    // Resolved when read_states in a Ready matches the context.
    let mut pending_read_index: HashMap<Vec<u8>, oneshot::Sender<Result<u64, RaftError>>> =
        HashMap::new();
    let mut request_id_counter: u64 = 0;
    let mut was_leader = false;
    let tick_interval = Duration::from_millis(TICK_MS);
    let mut bootstrapped = false;
    // Initialize from persisted snapshot index to avoid creating a redundant
    // snapshot immediately after restart.
    let mut last_snapshot_applied: u64 = node.store().snapshot_last_index();

    // Restore applied index from persistent storage so we skip entries
    // that were applied before the last shutdown.
    let persisted_applied = node.store().load_applied_index().unwrap_or_else(|e| {
        tracing::warn!(error = %e, "failed to load applied index, starting from 0");
        0
    });
    if persisted_applied > 0 {
        shared_state
            .applied_index
            .store(persisted_applied, Ordering::Release);
        tracing::info!(applied_index = persisted_applied, "restored applied index");
    }

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
        let mut ris = Vec::new();

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
        while let Ok(ri_req) = read_index_rx.try_recv() {
            ris.push(ri_req);
        }

        // If nothing was immediately available, block until something arrives or tick fires.
        if msgs.is_empty() && props.is_empty() && ccs.is_empty() && ris.is_empty() {
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
                recv(read_index_rx) -> ri_req => {
                    if let Ok(r) = ri_req { ris.push(r); }
                    while let Ok(r) = read_index_rx.try_recv() { ris.push(r); }
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

        // Process ReadIndex requests: call read_index on the RawNode which
        // triggers a heartbeat to confirm leadership. The commit index at
        // request time will be returned via read_states in a future Ready.
        for req in ris {
            if node.raft.state != StateRole::Leader {
                let _ = req.tx.send(Err(RaftError::NotLeader {
                    leader: Some(node.raft.leader_id),
                }));
                continue;
            }
            node.read_index(req.context.clone());
            pending_read_index.insert(req.context, req.tx);
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
                // Follower received a snapshot from the leader.
                // If snapshot application fails, the node is in an inconsistent
                // state (store updated but state machine not restored).  We MUST
                // stop the event loop to prevent applying subsequent entries on
                // top of stale state machine data.
                if let Err(e) =
                    apply_snapshot_to_state(snapshot, node.store(), &state_machine, &shared_state)
                {
                    tracing::error!(error = %e, "FATAL: snapshot application failed, stopping node");
                    return;
                }
                // Update tracking so that if this node later becomes leader,
                // it won't immediately create a redundant snapshot.
                last_snapshot_applied = snapshot.get_metadata().index;
            }

            let committed = ready.take_committed_entries();
            if !committed.is_empty() {
                // Track the highest committed index for ReadIndex
                let max_committed = committed.last().map(|e| e.index).unwrap_or(0);
                shared_state
                    .commit_index
                    .store(max_committed, Ordering::Release);

                // Skip entries that were already applied before the last shutdown.
                let already_applied = shared_state.applied_index.load(Ordering::Acquire);
                let to_apply: Vec<_> = committed
                    .into_iter()
                    .filter(|e| e.index > already_applied)
                    .collect();

                if !to_apply.is_empty() {
                    let new_applied = to_apply.last().map(|e| e.index).unwrap_or(0);
                    let mut sm = state_machine.lock().expect("state machine mutex poisoned");
                    for entry in &to_apply {
                        apply_entry(&mut sm, entry, &mut pending, &mut pending_conf);
                    }
                    drop(sm);

                    // Persist applied index so we don't re-apply on restart.
                    // Update shared_state regardless of persistence result — the
                    // entries are already applied to the state machine, so the
                    // in-memory index must track that. On crash+restart, we fall
                    // back to the persisted value and re-apply (idempotent).
                    if let Err(e) = node.store().save_applied_index(new_applied) {
                        tracing::error!(error = %e, "failed to persist applied index");
                    }
                    shared_state
                        .applied_index
                        .store(new_applied, Ordering::Release);

                    // Persist ConfState for any conf change entries.
                    for entry in &to_apply {
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

                    // Leader-side snapshot trigger: create snapshot when enough
                    // entries have been applied since the last snapshot.
                    if snapshot_trigger > 0
                        && node.raft.state == StateRole::Leader
                        && new_applied - last_snapshot_applied >= snapshot_trigger
                    {
                        match create_leader_snapshot(node.store(), &state_machine, new_applied) {
                            Ok(()) => last_snapshot_applied = new_applied,
                            Err(e) => {
                                tracing::error!(error = %e, "failed to create snapshot");
                                // Do NOT update last_snapshot_applied — the next
                                // apply will retry the snapshot creation.
                            }
                        }
                    }
                }

                // Notify waiters that applied_index has advanced.
                shared_state.applied_notify.notify_waiters();
            }

            // Process ReadIndex responses: match each read state's context
            // against pending read index requests and resolve them.
            for rs in ready.take_read_states() {
                if let Some(tx) = pending_read_index.remove(&rs.request_ctx) {
                    let _ = tx.send(Ok(rs.index));
                } else {
                    tracing::debug!(
                        index = rs.index,
                        ctx_len = rs.request_ctx.len(),
                        "read state context not found in pending (likely timed out)"
                    );
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
                for (_, tx) in pending_read_index.drain() {
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

/// Create a snapshot on the leader: serialize state machine, persist to store,
/// and compact old log entries.
fn create_leader_snapshot(
    store: &RaftRsStore,
    state_machine: &Arc<Mutex<AetherStateMachine>>,
    applied_index: u64,
) -> Result<(), String> {
    // Clone the Arc<RocksStorage> while briefly holding the lock, then release
    // immediately.  The expensive CF iteration happens without the lock.
    let storage = {
        let sm = state_machine.lock().expect("state machine mutex poisoned");
        sm.storage.clone()
    };

    let snapshot_data = AetherStateMachine::create_snapshot(&storage)?;

    // Determine the term for the snapshot index.
    let term = store.term(applied_index).unwrap_or(0);

    // Get current conf state.
    let conf_state = store
        .initial_state()
        .map(|s| s.conf_state)
        .unwrap_or_default();

    // Persist snapshot data to the store.
    store
        .save_snapshot_data(&snapshot_data, applied_index, term, &conf_state)
        .map_err(|e| format!("save snapshot data: {e}"))?;

    // Compact log entries up to and including the snapshot index.
    // The snapshot data covers all state up to applied_index, so log entries
    // at or below that index are redundant.
    if applied_index > 0 {
        let compact_index = applied_index + 1;
        if let Err(e) = store.compact(compact_index) {
            tracing::warn!(error = %e, compact_index, "failed to compact after snapshot");
        }
    }

    tracing::info!(
        index = applied_index,
        term,
        data_len = snapshot_data.len(),
        "created snapshot"
    );
    Ok(())
}

/// Apply an incoming snapshot from the leader to the state machine and store.
fn apply_snapshot_to_state(
    snapshot: &raft::eraftpb::Snapshot,
    store: &RaftRsStore,
    state_machine: &Arc<Mutex<AetherStateMachine>>,
    shared_state: &Arc<RaftSharedState>,
) -> Result<(), String> {
    let meta = snapshot.get_metadata();
    let data = snapshot.get_data();
    let index = meta.index;
    let term = meta.term;

    // Apply to the store (persists snapshot data, purges old log).
    store
        .apply_snapshot(meta, data)
        .map_err(|e| format!("store apply_snapshot: {e}"))?;

    // Restore the state machine from snapshot data.
    if !data.is_empty() {
        let mut sm = state_machine.lock().expect("state machine mutex poisoned");
        sm.restore_snapshot(data, index)?;
    }

    // Update shared state so the API layer sees the new applied index.
    shared_state.applied_index.store(index, Ordering::Release);
    shared_state.commit_index.store(index, Ordering::Release);

    // Persist applied index.
    if let Err(e) = store.save_applied_index(index) {
        tracing::error!(error = %e, "failed to persist applied index after snapshot");
    }

    // Wake any linearizable-read waiters so they can observe the new index.
    shared_state.applied_notify.notify_waiters();

    tracing::info!(index, term, "applied snapshot from leader");
    Ok(())
}

fn is_empty_snapshot(snap: &raft::eraftpb::Snapshot) -> bool {
    snap.get_data().is_empty() && snap.get_metadata().index == 0
}

#![allow(missing_docs)]

tonic::include_proto!("aether");

pub mod raft_rpc {
    tonic::include_proto!("raft");
}

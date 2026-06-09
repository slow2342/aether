fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(
        &["proto/kv.proto", "proto/cluster.proto", "proto/raft.proto"],
        &["proto/"],
    )?;
    Ok(())
}

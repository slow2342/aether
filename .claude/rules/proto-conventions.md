# Protobuf Conventions

## File Organization

- `proto/aether.proto` — Client-facing service definitions (KV, Watch, Lease, Auth, Cluster, Maintenance, Lock, Election)
- `proto/raft.proto` — Internal Raft RPC definitions (AppendEntries, Vote, InstallSnapshot)
- One file per concern, do not mix client-facing and internal RPCs

## Naming

- Service names: `PascalCase`, noun-based — `AetherKV`, `AetherWatch`, `AetherLease`
- RPC names: `PascalCase`, verb-based — `Put`, `Get`, `Range`, `Watch`
- Message names: `PascalCase`, noun-based — `PutRequest`, `PutResponse`, `KeyValue`
- Field names: `snake_case` — `key`, `range_end`, `start_revision`
- Enum values: `SCREAMING_SNAKE_CASE` with prefix — `EVENT_TYPE_PUT`, `EVENT_TYPE_DELETE`

## Request/Response Pattern

Every RPC follows request/response pattern:

```protobuf
service AetherKV {
    rpc Put(PutRequest) returns (PutResponse);
    rpc Get(GetRequest) returns (GetResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Range(RangeRequest) returns (RangeResponse);
    rpc Txn(TxnRequest) returns (TxnResponse);
}
```

## Common Fields

Request messages should include:

```protobuf
message PutRequest {
    bytes key = 1;
    bytes value = 2;
    int64 lease = 3;          // optional, 0 = no lease
    bool prev_kv = 4;         // return previous value
}

message GetRequest {
    bytes key = 1;
    bytes range_end = 2;      // empty = single key, "\0" = all keys to end of keyspace
    int64 limit = 3;          // 0 = no limit
    int64 revision = 4;       // 0 = latest
    bool serializable = 5;    // false = linearizable read
    SortOrder sort_order = 6;
    SortTarget sort_target = 7;
}

message DeleteRequest {
    bytes key = 1;
    bytes range_end = 2;
    bool prev_kv = 3;
}
```

Response messages should include:

```protobuf
message PutResponse {
    ResponseHeader header = 1;
    KeyValue prev_kv = 2;     // only if prev_kv was set in request
}

message GetResponse {
    ResponseHeader header = 1;
    repeated KeyValue kvs = 2;
    int64 count = 3;          // total count (when count_only is set)
}
```

## ResponseHeader

Every response must include a header with cluster metadata:

```protobuf
message ResponseHeader {
    uint64 cluster_id = 1;
    uint64 member_id = 2;
    int64 revision = 3;       // current store revision when response was created
    uint64 raft_term = 4;     // current Raft term
}
```

## Error Handling

Use gRPC status codes:

| Code | When to use |
|------|-------------|
| `OK` | Success |
| `INVALID_ARGUMENT` | Malformed request (empty key, invalid range) |
| `NOT_FOUND` | Key/lease/user not found |
| `ALREADY_EXISTS` | Creating resource that exists |
| `PERMISSION_DENIED` | Auth failure |
| `FAILED_PRECONDITION` | Transaction compare failed |
| `UNAVAILABLE` | Node not leader, cluster not ready |
| `DEADLINE_EXCEEDED` | Request timeout |

For application-level errors, use `google.rpc.Status` with detail messages:

```protobuf
import "google/rpc/status.proto";

message TxnResponse {
    ResponseHeader header = 1;
    bool succeeded = 2;
    repeated ResponseOp responses = 3;
}
```

## Field Numbers

- Use field numbers 1-15 for common fields (1-byte encoding)
- Reserve field numbers for future use: `reserved 10, 11, 12;`
- Never reuse a field number that was previously assigned

## Comments

- Add comments for non-obvious fields
- Document valid ranges and default values
- Reference related messages or RPCs

```protobuf
// Range gets the keys in the range [key, range_end).
// If range_end is "\0", it returns all keys from key to end of keyspace.
// If range_end is empty, it returns only the key.
// For prefix scan of key "foo", use range_end = "fop".
rpc Range(RangeRequest) returns (RangeResponse);
```

## See Also

- [api-design.md](api-design.md) — gRPC service implementation patterns
- [storage-conventions.md](storage-conventions.md) — Key encoding and range semantics

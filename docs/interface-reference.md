# Interface Reference

The canonical reference for everything outside the binary: the gRPC wire surface, the command-line interface, and the published Rust crates. This chapter is descriptive rather than tutorial — for *behavior and rationale* see [Client API and Usage](client-api-and-usage.md), [Consensus Integration](consensus-integration.md), and [The Allocator](the-allocator.md). For *deployment shapes and operational tuning* see [Operations](operations.md) and [Deployment](deployment.md).

The wire source of truth lives in the [`tsoracle-proto`](../crates/tsoracle-proto/proto/tsoracle/v1/tso.proto) and [`tsoracle-standalone`](../crates/tsoracle-standalone/proto/admin.proto) `.proto` files; the CLI source of truth lives in [`crates/tsoracle-bin/src/cli.rs`](../crates/tsoracle-bin/src/cli.rs); the Rust crate reference lives on [docs.rs](https://docs.rs/tsoracle). Anything stated here that contradicts those sources is a documentation bug — please [file an issue](https://github.com/prisma-risk/tsoracle/issues).

## External interfaces at a glance

| Surface | Audience | Source of truth | Reference |
|---|---|---|---|
| gRPC over HTTP/2 — `tsoracle.v1.TsoService` | Applications using `tsoracle-client` or any gRPC client | `crates/tsoracle-proto/proto/tsoracle/v1/tso.proto` | [§ gRPC wire reference](#grpc-wire-reference) |
| gRPC over HTTP/2 — `tsoracle.admin.v1.MembershipAdmin` | Cluster operators (openraft only) | `crates/tsoracle-standalone/proto/admin.proto` | [§ Admin gRPC reference](#admin-grpc-reference) |
| CLI — `tsoracle` binary | Operators running the standalone server | `crates/tsoracle-bin/src/cli.rs` | [§ CLI reference](#cli-reference) |
| Rust crates on crates.io | Embedders and driver authors | the per-crate `lib.rs` | [§ Rust crate API](#rust-crate-api) and [docs.rs/tsoracle](https://docs.rs/tsoracle) |

## gRPC wire reference

### Service: `tsoracle.v1.TsoService`

Two RPCs. Default port is **`50551`** (loopback by default; configurable via `--listen`). The server speaks gRPC over HTTP/2 in either plaintext (`h2c`) or TLS, controlled by `--tls-cert`/`--tls-key`/`--tls-client-ca`.

#### RPC: `GetTs`

Allocate `count` consecutive timestamps. Only the current leader serves this RPC.

**Request — `GetTsRequest`:**

| Field | Type | Constraint | Meaning |
|---|---|---|---|
| `count` | `uint32` (1) | `>= 1`; upper bound is the 18-bit logical capacity, `LOGICAL_MAX + 1 = 262_144` | Number of consecutive timestamps to allocate. |

**Response — `GetTsResponse` (on success):**

| Field | Type | Meaning |
|---|---|---|
| `physical_ms` | `uint64` (1) | 46-bit milliseconds-since-Unix-epoch component of the grant. |
| `logical_start` | `uint32` (2) | 18-bit logical counter. The response covers logicals `[logical_start, logical_start + count - 1]` at this `physical_ms`. |
| `count` | `uint32` (3) | Equals the request's `count`. The server never returns a partial grant. |
| `epoch_hi` | `uint64` (4) | Upper 64 bits of the 128-bit leader epoch that issued this grant. |
| `epoch_lo` | `uint64` (5) | Lower 64 bits of the leader epoch. Lexicographic `(epoch_hi, epoch_lo)` order equals numeric order; per-node strictly monotone across leader terms. |

**Success contract:**

- Each issued timestamp packs as `physical_ms << 18 | logical`, where `logical ∈ [logical_start, logical_start + count - 1]`.
- Across all successful `GetTs` responses from any node since cluster genesis, the packed integer values are strictly increasing and unique — no duplicates and no regression, even across leader transitions.
- See [Monotonicity proof](the-allocator.md#monotonicity-proof) for the argument.

**Error statuses returned from `GetTs`:**

| `tonic::Code` | When | Trailer |
|---|---|---|
| `FAILED_PRECONDITION` | This node is not currently the leader. | [`tsoracle-leader-hint-bin`](#trailer-tsoracle-leader-hint-bin) |
| `INVALID_ARGUMENT` | `count == 0` or `count > LOGICAL_MAX + 1` (i.e. `count > 262_144`). | — |
| `OUT_OF_RANGE` | The requested grant would exceed the 46-bit `physical_ms` field or the 18-bit logical field. | — |
| `UNAVAILABLE` | The leader's persist path returned a transient driver error (the window advance did not commit). Clients may retry; `tsoracle-client` does. | — |
| `INTERNAL` | A bug-class error: window-exhausted with no extension available, persist hardware failure, or a violated allocator invariant. Clients should not retry without inspection. | — |

`tsoracle-client` filters the `tsoracle-leader-hint-bin` trailer out of `FAILED_PRECONDITION` errors before surfacing the `Status` to caller code (it is consumed as a routing hint, not propagated). Other gRPC clients see the trailer as standard binary metadata.

#### RPC: `GetCurrentMaxSafe`

Read the current safe-point — the highest `physical_ms` at which any timestamp issued at or before that millisecond is durably backed by the leader's persisted window. External coordinators poll this to fence reads/writes that depend on a durable timestamp watermark.

**Request — `GetCurrentMaxSafeRequest`:** empty.

**Response — `GetCurrentMaxSafeResponse`:**

| Field | Type | Meaning |
|---|---|---|
| `max_safe_physical_ms` | `uint64` (1) | Safe-point in physical-millisecond units (not the 64-bit packed form). Equal to `persisted_high_water - window_ahead_ms - 1` on the leader when a window has been admitted; **zero** otherwise. |
| `epoch_hi` | `uint64` (2) | Upper 64 bits of the leader epoch attached to this value. |
| `epoch_lo` | `uint64` (3) | Lower 64 bits of the leader epoch. |

**Response contract:**

- **Leaders** return the authoritative current value.
- **Followers** return `max_safe_physical_ms = 0` and the zero epoch. Target the leader for an authoritative value; followers do not redirect this RPC.
- A leader that has not yet admitted its first window (cold start before the failover fence completes) also returns `0`.
- Long-lived pollers should fence stale-leader values by tracking `(epoch_hi, epoch_lo)` and discarding values from a smaller epoch after a larger one.

This RPC does not return `FAILED_PRECONDITION`, and the response carries no trailer.

### Service: `tsoracle.admin.v1.MembershipAdmin`

See [§ Admin gRPC reference](#admin-grpc-reference) below.

### Trailer: `tsoracle-leader-hint-bin`

When `GetTs` returns `FAILED_PRECONDITION`, the server attaches a binary metadata trailer named `tsoracle-leader-hint-bin` whose body is the postcard encoding of a `LeaderHint` message.

| `LeaderHint` field | Type | Semantics |
|---|---|---|
| `leader_endpoint` | `optional string` (1) | Scheme-less `host:port` of the leader's `TsoService` endpoint as known to this node. Absent on cold start or right after a leadership change. **The string must not carry a `http://` or `https://` scheme** — the client rejects a scheme-bearing hint to defend against TLS downgrade. |
| `leader_epoch` | `optional EpochWire` (2) | The leader's 128-bit epoch, bundled as a nested message so a half-populated epoch is unrepresentable on the wire. Either both halves are present (`epoch.hi`, `epoch.lo`) or neither is. |

```
EpochWire {
    uint64 hi = 1;   // more significant half
    uint64 lo = 2;   // less significant half
}
```

**Contract:**

- The hint is **advisory**, not authoritative. A stale hint costs an extra client round-trip; it does not affect correctness.
- A hint whose `leader_epoch` is strictly smaller than the client's cached leader epoch is dropped (the client treats it as evidence of a delayed response from an old term).
- The metadata key is suffixed `-bin` per gRPC convention; consumers must request binary metadata and base64-decode if their gRPC stack does not handle that automatically.

See [The leader-hint trailer](key-subsystems.md#the-leader-hint-trailer) for the consumption protocol on the client side.

## Admin gRPC reference

The membership-admin surface is bound to a separate listen address (`--admin-listen`), separate from the public `TsoService` port, and is **implemented only by the `openraft` driver**. The `file` and `paxos` drivers return `UNSUPPORTED` for every mutating RPC; `ListMembers` works on all drivers and reports a fixed view for `paxos`/`file`.

All mutating RPCs are **idempotent on the target id**: re-issuing a successful change is a no-op. The server serializes them on an internal lock — two reconfigurations cannot race. Mutating RPCs are **leader-only**: a non-leader returns `ok=false`, `error=NOT_LEADER`, and (when known) `leader_admin_endpoint` set to the leader's admin port. The `tsoracle admin` CLI uses this for one-step re-routing.

### Service: `tsoracle.admin.v1.MembershipAdmin`

| RPC | Request | Response | Leader-only |
|---|---|---|---|
| `ListMembers` | `ListMembersRequest{}` | `MembershipView` | No |
| `AddLearner` | `AddLearnerRequest{ id, raft_addr, service_endpoint, admin_endpoint }` | `ChangeResponse` | Yes |
| `Promote` | `PromoteRequest{ id }` | `ChangeResponse` | Yes |
| `RemoveNode` | `RemoveNodeRequest{ id }` | `ChangeResponse` | Yes |
| `ActivateFormat` | `ActivateFormatRequest{ target }` | `ChangeResponse` | Yes |

### Message: `MembershipView` (returned by `ListMembers`)

| Field | Type | Meaning |
|---|---|---|
| `members` | `repeated MemberEntry` (1) | The membership as this node currently sees it. |
| `has_leader` | `bool` (2) | True iff `leader` is meaningful. |
| `leader` | `uint64` (3) | Cluster-current leader's node id. **Meaningful only when `has_leader == true`** — consumers must not interpret `leader == 0` as "no leader" (proto3 has no `optional uint64`, so this pair encodes `Option<u64>`). |

### Message: `MemberEntry`

| Field | Type | Meaning |
|---|---|---|
| `id` | `uint64` (1) | Cluster-unique node id, stable across address changes. |
| `role` | `MemberRole` (2) | `VOTER` (counted in quorum) or `LEARNER` (replicates, not in quorum). |
| `raft_addr` | `string` (3) | `host:port` of the node's openraft peer transport. Consensus traffic. |
| `service_endpoint` | `string` (4) | Scheme-less `host:port` of the node's public `TsoService` endpoint. What clients use; what leader hints carry. |
| `admin_endpoint` | `string` (5) | `host:port` of the node's admin gRPC endpoint. What `NOT_LEADER` responses on the admin port redirect to. |

The three address fields are required at `AddLearner` time and stored verbatim in the membership log; the server does not parse or canonicalize them.

### Message: `ChangeResponse` (returned by every mutating RPC)

| Field | Type | Meaning |
|---|---|---|
| `ok` | `bool` (1) | **Authoritative success flag.** Always check this before reading `error`. |
| `error` | `AdminErrorKind` (2) | Typed error kind. Meaningful only when `ok == false`. |
| `leader_admin_endpoint` | `string` (3) | Set when `error == NOT_LEADER` and a leader admin endpoint is known. Empty string means no hint available (cold start or no leader yet). |
| `message` | `string` (4) | Human-readable detail — for example, the driver's error string when `error == DRIVER`. Empty on success. Suitable for operator display; programmatic dispatch should be on `error`, not on this string. |

### Enum: `AdminErrorKind`

| Value | When |
|---|---|
| `ADMIN_ERROR_KIND_UNSPECIFIED` (0) | proto3 default. The server never emits this with `ok == false`; the wire never carries `error=UNSPECIFIED` paired with `ok=false`. |
| `NOT_LEADER` (1) | Mutating RPC reached a follower. `leader_admin_endpoint` carries the leader's admin port when known. Note that `ActivateFormat`'s `NOT_LEADER` always leaves `leader_admin_endpoint` empty (the underlying error is a unit variant). |
| `UNSUPPORTED` (2) | This driver does not implement runtime membership changes. Returned by `paxos` and `file` for every mutating RPC. |
| `NOT_MEMBER` (3) | `Promote` against an id not currently in the membership. |
| `NOT_CAUGHT_UP` (4) | Reserved. The current openraft impl folds learner-not-caught-up into `DRIVER` with the upstream error text; this variant is reserved for a future explicit catch-up check. |
| `WOULD_LOSE_QUORUM` (5) | `RemoveNode` against the last remaining voter. Other quorum-loss shapes (e.g. removing two of three) are the operator's responsibility. |
| `TIMEOUT` (6) | Reserved. Not currently produced — the openraft impl folds timeouts into `DRIVER`. Reserved for future explicit-deadline plumbing. |
| `DRIVER` (7) | Catch-all for an upstream driver error not captured by a typed variant. `message` carries the driver's error string. |
| `MEMBERS_BELOW_TARGET` (8) | `ActivateFormat` rejected by the all-members gate: at least one member's `max_readable_version` is below the requested `target`. |
| `TARGET_OUT_OF_RANGE` (9) | `ActivateFormat` apply-arm defense-in-depth: `target` is outside the local binary's readable range. |
| `MEMBERSHIP_CHANGED` (10) | `ActivateFormat` apply-keyed no-op: the gated set drifted out of the membership by the entry's own log position. Operator re-gates and re-issues. |

Adding new variants is backwards compatible: clients that don't recognize a value can fall back to the `message` field.

### RPC details

- **`AddLearner`** — Idempotent on `id`: a node already in the membership is a no-op even if the addresses differ. Blocks until the learner is registered. Errors: `NOT_LEADER`, `UNSUPPORTED`, `DRIVER`.
- **`Promote`** — Idempotent on `id` (already-voter is a no-op). Errors: `NOT_LEADER`, `NOT_MEMBER`, `UNSUPPORTED`, `DRIVER` (which today subsumes the upstream "learner not caught up" case).
- **`RemoveNode`** — Idempotent on `id` (unknown ids return `ok`). Refuses to remove the last remaining voter (`WOULD_LOSE_QUORUM`). Removing the current leader is allowed; openraft commits the removal while still leader, then steps down. Errors: `NOT_LEADER`, `WOULD_LOSE_QUORUM`, `UNSUPPORTED`, `DRIVER`.
- **`ActivateFormat`** — Initiate a format-version activation to `target`. Runs the all-members capability gate, proposes the bump via raft, and reports the apply-keyed outcome. Errors: `NOT_LEADER`, `MEMBERS_BELOW_TARGET`, `TARGET_OUT_OF_RANGE`, `MEMBERSHIP_CHANGED`, `UNSUPPORTED`, `DRIVER`. See [Format-migration upgrade](format-migration-upgrade.md) for the broader story.

## CLI reference

Source of truth: [`crates/tsoracle-bin/src/cli.rs`](../crates/tsoracle-bin/src/cli.rs). The crate is published as `tsoracle` and produces a single binary, also named `tsoracle`.

### Top-level synopsis

```
tsoracle [SERVE-FILE-FLAGS]
tsoracle serve file     [SERVE-FILE-FLAGS]
tsoracle serve openraft [SERVE-COMMON-FLAGS] [OPENRAFT-FLAGS]
tsoracle serve paxos    [SERVE-COMMON-FLAGS] [PAXOS-FLAGS]
tsoracle init           --seed-physical-ms <N> [--state-dir <PATH>]
tsoracle admin <SUBCOMMAND> ...      # openraft feature only
```

Bare `tsoracle` (no subcommand) is exactly equivalent to `tsoracle serve file` with all defaults. `tsoracle --help` and `tsoracle <SUBCOMMAND> --help` print clap-generated usage; this section is the long-form reference.

### Flags shared by every `serve` subcommand

These come from `CommonServeArgs`. Every `serve` mode accepts them with the same defaults.

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--listen` | `SocketAddr` | `127.0.0.1:50551` | Client-facing gRPC listen address (the `TsoService` port). |
| `--window-ahead` | duration (humantime) | `3s` | How far ahead of wall-clock the leader advances its persisted high-water. Higher values reduce extension frequency at the cost of a larger time-to-live for a stale leader's window. See [Sizing window_ahead](operations.md#sizing-window_ahead). |
| `--failover-advance` | duration | `1s` | How far past the recovered high-water the failover fence advances on leadership gain. Must exceed the worst-case wall-clock skew between leaders. See [Sizing failover_advance](operations.md#sizing-failover_advance). |
| `--log` | string | `info` | `tracing` env-filter directive (`info`, `debug`, `tsoracle_server=debug,info`, etc.). |
| `--tls-cert` | path | — | PEM server certificate chain for the client gRPC API. Enables TLS on `--listen`. |
| `--tls-key` | path | — | PEM private key paired with `--tls-cert`. |
| `--tls-client-ca` | path | — | PEM CA used to verify *client* certificates. Enables client mTLS on the API port. |

`--tls-cert` and `--tls-key` must be supplied together; supplying `--tls-client-ca` without them is an error.

### `tsoracle serve file`

Single-node, fsync-durable. The default mode. No replication — losing the node loses availability.

Common flags (above), plus:

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--state-dir` | path | `./tsoracle-data` | Where to persist window state. Created on first start; first run against an empty directory begins at high-water 0. |

### `tsoracle serve openraft`

HA via openraft. Three or more nodes. Compiled in when the binary is built with the `openraft` feature (enabled by default on crates.io releases).

Common flags (above), plus:

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--id` | `u64` | *required* | This node's numeric raft id. Must be unique across the cluster. |
| `--raft-addr` | `SocketAddr` | *required* | Bind address for the raft peer transport (consensus traffic). Plaintext on loopback; routable bind requires the `--peer-tls-*` triple or `--allow-insecure-peer`. See [Peer-port trust boundary](operations.md#peer-port-trust-boundary). |
| `--raft-dir` | path | *required* | Directory for raft log + state-machine data. Persisted across restarts. |
| `--bootstrap` | bool | `false` | Initialize the cluster on this node, first boot only. Used exactly once per cluster lifetime. |
| `--members` | string | — | Initial membership, **only valid with `--bootstrap`**. Format: `id=raft_host:port/service_host:port/admin_host:port,...`. |
| `--heartbeat-ms` | `u64` | `250` | Raft heartbeat interval. |
| `--election-min-ms` | `u64` | `1000` | Lower bound on the randomized election timeout. |
| `--election-max-ms` | `u64` | `2000` | Upper bound on the randomized election timeout. |
| `--admin-listen` | `SocketAddr` | — | Bind address for the membership-admin gRPC server. Omitting the flag serves no admin surface. Plaintext on loopback; non-loopback bind requires the `--admin-tls-*` triple. |
| `--admin-tls-cert` | path | — | PEM server certificate for the admin gRPC server. Enables admin mTLS — needs all three `--admin-tls-*` flags. |
| `--admin-tls-key` | path | — | PEM private key paired with `--admin-tls-cert`. |
| `--admin-tls-ca` | path | — | PEM CA used to verify connecting admin clients. Operator-dedicated; **not** the peer CA. |
| `--peer-tls-cert` | path | — | PEM node certificate for the peer transport. Enables peer mTLS — needs all three `--peer-tls-*` flags. |
| `--peer-tls-key` | path | — | PEM private key paired with `--peer-tls-cert`. |
| `--peer-tls-ca` | path | — | PEM CA used to verify connecting peers. Cluster-dedicated. |
| `--allow-insecure-peer` | bool | `false` | Opt out of the peer-listener secure-by-default guard. Allows routable bind without `--peer-tls-*`. Matches the helm chart's `tls.allowInsecurePeer`. Intended for single-host dev or service-mesh-terminated mTLS only. |

### `tsoracle serve paxos`

HA via OmniPaxos. Three or more nodes. Compiled in when the binary is built with the `paxos` feature (enabled by default on crates.io releases).

Common flags (above), plus:

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--node-id` | `u64` | *required* | This node's OmniPaxos pid. Must be unique across the cluster. |
| `--peer-listen` | `SocketAddr` | *required* | Bind address for the paxos peer transport. Plaintext on loopback; routable bind requires the `--peer-tls-*` triple or `--allow-insecure-peer`. See [Peer-port trust boundary](operations.md#peer-port-trust-boundary). |
| `--peers` | string | *required* | Comma-separated `id=host:port` paxos peer addresses. Required at every start, not just bootstrap. |
| `--tso-peers` | string | *required* | Comma-separated `id=host:port` tsoracle service addresses (per-node `TsoService` ports), used to populate `LeaderHint` for follower redirects. |
| `--data-dir` | path | *required* | Directory for the paxos log + meta. |
| `--tick-interval` | duration | `20ms` | OmniPaxos tick interval. |
| `--peer-tls-cert` | path | — | PEM node certificate for the peer transport. Enables peer mTLS — needs all three `--peer-tls-*` flags. |
| `--peer-tls-key` | path | — | PEM private key paired with `--peer-tls-cert`. |
| `--peer-tls-ca` | path | — | PEM CA used to verify connecting peers. |
| `--allow-insecure-peer` | bool | `false` | Opt out of the peer-listener secure-by-default guard. Allows routable bind without `--peer-tls-*`. Matches the helm chart's `tls.allowInsecurePeer`. Intended for single-host dev or service-mesh-terminated mTLS only. |

### `tsoracle init`

Initialize a fresh `file`-driver state directory at a seeded high-water. Useful when migrating from a prior timestamp source — see [Migration](getting-started.md#migrating-from-an-existing-timestamp-source).

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--state-dir` | path | `./tsoracle-data` | Directory to initialize. Must not already contain `file`-driver state. |
| `--seed-physical-ms` | `u64` | *required* | High-water to seed. Every subsequent issued timestamp's `physical_ms` will be strictly greater than this value. |

### `tsoracle admin`

Administer cluster membership over the admin gRPC port. **Available only when the binary is built with the `openraft` feature.** Backed by the [`tsoracle.admin.v1.MembershipAdmin`](#service-tsoracleadminv1membershipadmin) RPCs above.

Each subcommand accepts `--endpoint` (any node's admin endpoint — e.g. `https://127.0.0.1:50561`) plus the optional `--client-tls-*` triple for connecting to an mTLS-enabled admin server.

Common admin-client flags (`AdminClientTlsArgs`):

| Flag | Type | Meaning |
|---|---|---|
| `--client-tls-cert` | path | PEM client certificate to present to the admin gRPC server. |
| `--client-tls-key` | path | PEM private key for `--client-tls-cert`. |
| `--client-tls-ca` | path | PEM CA used to verify the admin server's certificate. |

#### `tsoracle admin members`

List current members. Answerable by any node.

| Flag | Type | Meaning |
|---|---|---|
| `--endpoint` | string | Admin endpoint of any node. |
| `--client-tls-*` | path | See above. |

#### `tsoracle admin add-learner`

Add a non-voting learner.

| Flag | Type | Meaning |
|---|---|---|
| `--endpoint` | string | Admin endpoint to address. The CLI follows a `NOT_LEADER` redirect to `leader_admin_endpoint`. |
| `--id` | `u64` | New node's raft id. Cluster-unique. |
| `--raft-addr` | string | `host:port` of the new node's raft peer transport. |
| `--service-endpoint` | string | Scheme-less `host:port` of the new node's public `TsoService` endpoint. |
| `--admin-endpoint` | string | `host:port` of the new node's admin gRPC endpoint. |
| `--client-tls-*` | path | See above. |

#### `tsoracle admin promote`

Promote a learner to voter.

| Flag | Type | Meaning |
|---|---|---|
| `--endpoint` | string | Admin endpoint to address. Follows `NOT_LEADER` redirect. |
| `--id` | `u64` | Id of the learner to promote. |
| `--client-tls-*` | path | See above. |

#### `tsoracle admin remove`

Remove a node by id (voter or learner).

| Flag | Type | Meaning |
|---|---|---|
| `--endpoint` | string | Admin endpoint to address. Follows `NOT_LEADER` redirect. |
| `--id` | `u64` | Id of the node to remove. |
| `--client-tls-*` | path | See above. |

#### `tsoracle admin activate-format`

Initiate a format-version activation. Runs the all-members capability gate, then proposes the bump via raft.

| Flag | Type | Meaning |
|---|---|---|
| `--endpoint` | string | Admin endpoint of any cluster member. **The CLI does not auto-redirect on `NOT_LEADER` for this RPC** — the underlying error variant carries no endpoint hint, so re-issue against a known leader. |
| `--target` | `u8` | Target format version. Must be within the local binary's readable range and supported by every cluster member. |
| `--client-tls-*` | path | See above. |

**Exit codes specific to `activate-format`:**

| Code | Meaning |
|---|---|
| `0` | Success. |
| `2` | Gate rejected the activation — at least one member's `max_readable_version` is below `--target` (`MEMBERS_BELOW_TARGET`). |
| `3` | Receiving node is a follower (`NOT_LEADER`). |
| `4` | Receiving node's local readable range does not include `--target` (`TARGET_OUT_OF_RANGE`). |
| `1` | Any other failure. |

Other `admin` subcommands use only `0` (success) and `1` (failure).

## Rust crate API

Published on crates.io. Generated reference: <https://docs.rs/tsoracle> (and analogously for each crate below). Where prose chapters complement the generated docs, the third column points at them.

| Crate | Role | Companion chapter |
|---|---|---|
| [`tsoracle`](https://docs.rs/tsoracle) | The CLI binary (this section above). Depends on the crates below. | [§ CLI reference](#cli-reference) |
| [`tsoracle-client`](https://docs.rs/tsoracle-client) | Async Rust client. The recommended way to call `TsoService` from Rust code. | [Client API and Usage](client-api-and-usage.md), [The Client Driver](the-client-driver.md) |
| [`tsoracle-server`](https://docs.rs/tsoracle-server) | The server library. Embed `Server::builder()...build()?.serve_with_shutdown(...)` to host the gRPC service in your own binary. | [Architecture Deep Dive](architecture-deep-dive.md), [Key Subsystems](key-subsystems.md) |
| [`tsoracle-core`](https://docs.rs/tsoracle-core) | Sync algorithm core: the window allocator. No I/O, no async. | [The Allocator](the-allocator.md) |
| [`tsoracle-consensus`](https://docs.rs/tsoracle-consensus) | The `ConsensusDriver` trait. Implement this to back tsoracle with your own replicated log. | [Consensus Integration](consensus-integration.md) |
| [`tsoracle-driver-file`](https://docs.rs/tsoracle-driver-file) | Single-node fsync-durable driver. The default. | [Driver Comparison](driver-comparison.md) |
| [`tsoracle-driver-openraft`](https://docs.rs/tsoracle-driver-openraft) | Driver wrapping an openraft cluster. | [Driver Comparison](driver-comparison.md) |
| [`tsoracle-driver-paxos`](https://docs.rs/tsoracle-driver-paxos) | Driver wrapping an OmniPaxos cluster. | [Driver Comparison](driver-comparison.md) |
| [`tsoracle-standalone`](https://docs.rs/tsoracle-standalone) | Per-driver bootstrap helpers + peer transport. Used by the CLI binary; also embeddable. | — |
| [`tsoracle-proto`](https://docs.rs/tsoracle-proto) | Generated protobuf types for `tsoracle.v1.TsoService`. Use directly only when bypassing `tsoracle-client`. | [§ gRPC wire reference](#grpc-wire-reference) |
| [`tsoracle-codec`](https://docs.rs/tsoracle-codec) | Versioned postcard codec for on-disk and in-meta records. | [Format-migration upgrade](format-migration-upgrade.md) |
| [`tsoracle-openraft-toolkit`](https://docs.rs/tsoracle-openraft-toolkit) | Lower-level openraft glue, including `LogStoreCodec<C>`. For driver authors and embedders who already host their own openraft. | [Consensus Integration](consensus-integration.md) |
| [`tsoracle-paxos-toolkit`](https://docs.rs/tsoracle-paxos-toolkit) | Lower-level OmniPaxos glue. | [Consensus Integration](consensus-integration.md) |
| [`tsoracle-failpoint`](https://docs.rs/tsoracle-failpoint), [`tsoracle-yieldpoint`](https://docs.rs/tsoracle-yieldpoint) | Test-only fault-injection points. Compiled in only when the `failpoints` / `yieldpoints` feature is enabled. | [Failpoint Testing](failpoint-testing.md), [Yield-point Testing](yieldpoint-testing.md) |

## Stability and versioning

- **The gRPC public service is versioned by package**: `tsoracle.v1.TsoService`. Field numbers are stable; new optional fields may be added within `v1`. A future breaking wire change would land as `tsoracle.v2`.
- **The admin gRPC service is `tsoracle.admin.v1.MembershipAdmin`**. New `AdminErrorKind` variants may be added within `v1` — consumers must fall back to the `message` field when they encounter an unrecognized enum value, exactly as required by the proto contract.
- **The Rust crates follow Cargo semver.** Each crate is versioned independently; consult the crate's `CHANGELOG.md` (or the [release-plz](https://release-plz.dev) entries on the GitHub Releases page) for migration notes.
- **The CLI surface is stable across patch and minor releases** of the same crates.io major version of the `tsoracle` binary crate. Flag removals or rename without alias are major-version events.
- **On-disk format compatibility** is governed by the per-record `format_version` byte (currently `4`). The binary supports a documented `[MIN_READABLE_VERSION, MAX_READABLE_VERSION]` range so a rolling upgrade can convert in place. See [Format-migration upgrade](format-migration-upgrade.md) for the full story, including the `tsoracle admin activate-format` flow that flips the cluster-wide active write version.

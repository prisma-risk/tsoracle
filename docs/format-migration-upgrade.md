# Zero-downtime format-migration upgrade runbook (openraft driver)

This runbook describes how to roll a tsoracle openraft cluster from one persisted-and-wire format version to the next (`v_n` to `v_n+1`) with zero downtime, and the one-way finalization contract that follows. It applies only to the openraft driver; the paxos and file drivers keep stop-the-world version bumps and are out of scope. The format version governs both the on-disk RocksDB layout (log entries, snapshots, meta) and the peer-RPC payloads, because those share the same encoded types.

## Concepts

Each node tracks four separate version values. `min_readable_version` and `max_readable_version` are compile-time constants of the running binary: the oldest and newest formats it has a parser for. The active write version is durable runtime state: the single version this node currently emits, both when persisting and when sending peer RPCs. It lags `max_readable_version` and only ever advances through a successful, committed activation. `BASELINE_WRITE_VERSION` is the active write version in effect at the release that introduced this feature; an unframed legacy peer payload is read as `BASELINE_WRITE_VERSION`.

A node decodes any version in `[min_readable_version, max_readable_version]` and fails loud (refuses to boot or rejects the RPC) on a version outside that range. The core safety invariant the rollout preserves is: a node must never write or send a format that any current peer — voter or learner — cannot already read.

## The four-stage rollout (v_n to v_n+1)

### Stage 1 — Deploy read-capability (rolling restart, safe)

Roll out the new binary one node at a time. The new binary raises `max_readable_version` to `v_n+1` on both the disk and wire boundaries but still writes `v_n`; the new format's fields and behaviors are feature-gated off until activation lifts the gate. This stage is purely additive: every node still emits `v_n`, the additive `format_version` protobuf field keeps the mixed-binary window wire-compatible, and an absent field is read as `BASELINE_WRITE_VERSION`. Restart nodes one at a time and wait for each to rejoin and catch up before proceeding. Watch `tsoracle.schema.max_readable_version` climb to `v_n+1` on each node and confirm `tsoracle.schema.active_write_version` stays at `v_n` everywhere.

Do not proceed to Stage 2 until every member — every voter and every learner — is running the read-capable binary. A learner left on the old binary will block activation (Stage 2 gate) by design.

### Stage 2 — Activate (operator-initiated, gated)

Once the read-capable binary is on every node, initiate activation against the leader. The leader live-queries every current member (voters and learners) via the `Capabilities` peer RPC for its `{min_readable_version, max_readable_version, active_write_version}`, and requires every member's `max_readable_version` to be at least `v_n+1`. The lowest member capability observed is reported as `tsoracle.schema.min_member_read_capability`.

- If the gate passes, the leader proposes a `SetFormatVersion(v_n+1)` entry carrying the exact gated member set, increments `tsoracle.schema.format_version.proposed.total`, and on commit increments `tsoracle.schema.format_version.committed.total`.
- If the gate fails (some member cannot read `v_n+1`), the attempt is rejected before any proposal and increments `tsoracle.schema.format_version.rejected_by_gate.total`. Remediate the lagging member — upgrade it to the read-capable binary, or remove it from membership — then re-issue.

The activation trigger is currently an interim library method on `StandaloneHost::initiate_format_activation`; the `tsoracle admin` CLI surface for it is reconciled with the dynamic-membership admin work once a CLI consumer is wired through it.

The bump self-validates at apply time. The `SetFormatVersion` entry takes effect only if the membership committed as of the entry's own log position is a subset of the gated set it carried. If a membership change raced between the live query and the commit, the entry applies as a no-op (incrementing `tsoracle.schema.format_version.noop_membership_subset.total`), the active write version does not advance, and you simply re-gate and re-issue. A successful apply increments `tsoracle.schema.format_version.applied.total` and the leader flips its active write version to `v_n+1`, lifting the feature gate. Confirm `tsoracle.schema.active_write_version` steps to `v_n+1` cluster-wide before considering activation complete.

### Stage 3 — Steady state

New records and peer payloads are now `v_n+1`; pre-existing `v_n` records remain on disk until they are organically purged. No operator action is required. The cluster is fully on the new format for everything it writes from this point.

### Stage 4 — Garbage collection (organic, never forced)

Snapshots and meta migrate to `v_n+1` on their next write. Once snapshot install plus log purge advances `LastPurged` past every `v_n` log entry, no `v_n` bytes remain on disk. This happens organically; never force it. The `v_n` decoder is retained in the binary indefinitely regardless (decoders are never auto-removed) so that an operator upgrading on any schedule, skipping versions, can still read whatever any prior release wrote.

## Finalization and the no-downgrade contract

Activation is a one-way door. Before the `SetFormatVersion` entry applies successfully, the window is freely abortable — nothing `v_n+1` has been written or sent, so simply not issuing (or re-issuing) the activation leaves the cluster on `v_n`. After the entry applies successfully and `v_n+1` records exist on disk and on the wire, the migration is finalized.

Contract: do not downgrade any node below the cluster's active write version. A binary whose `max_readable_version` is below the cluster's active write version will correctly refuse to boot on a `v_n+1` log tail (it fails loud with a foreign-version decode error) and will reject `v_n+1` peer RPCs. There is no fenced-downgrade machinery and none is planned; the safety floor is fail-loud refusal, not silent misdecode. If you must run an older binary, you must do so before activation finalizes, never after.

## Rollback within a stage

Stage 1 is fully reversible: because every node still writes `v_n`, you can roll any node back to the prior binary at any time during Stage 1. Stage 2 is reversible only up to the successful apply: an aborted or gate-rejected or no-op'd activation leaves the active write version at `v_n`, and the durable recovery rule derives the active version from fsync-durable written evidence — the latest snapshot's leading version byte and the highest version byte among durable log records (there is NO meta key; the shared in-memory cell is re-established by deterministic raft-log replay) — never from the mere presence of a `SetFormatVersion` entry, so a non-applied bump is never resurrected on restart. Once the apply succeeds and `v_n+1` bytes exist, the no-downgrade contract above governs.

## What the operator watches

The catalog in [`crates/tsoracle-server/src/docs/operations.md`](../crates/tsoracle-server/src/docs/operations.md) lists every metric. The activation-specific ones to watch are:

- During Stage 1: `tsoracle.schema.max_readable_version` should climb to `v_n+1` on every node before Stage 2 begins; `tsoracle.schema.active_write_version` stays at `v_n` everywhere.
- During Stage 2: `tsoracle.schema.min_member_read_capability` should be `>= v_n+1` once Stage 1 is complete (this is the gate's binding constraint); `tsoracle.schema.format_version.proposed.total` ticks once when the gate passes, then `committed.total` ticks, then `applied.total`. A non-zero `rejected_by_gate.total` means an under-read-capable member remains; a non-zero `noop_membership_subset.total` means membership raced the bump.
- After Stage 2: `tsoracle.schema.active_write_version` steps from `v_n` to `v_n+1` on every node. Until every node reports the new value, treat the activation as in progress.

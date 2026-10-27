# Stopped file-driver state export

`tsoracle export file --state-dir /var/lib/tsoracle --compact` emits schema `tsoracle.stopped-file-state`, version 1. The command requires both `single-node` and `file` build features. Default builds have no export command. The additive library API is available without either CLI feature. The export API first ships in `tsoracle-driver-file` 2.3.0. The hidden `--json` alias remains accepted. Without `--compact` it emits the same complete receipt as indented JSON. Errors use a nonzero exit status and produce no receipt on stdout. An output I/O error can leave partial stdout, so consumers must require successful exit, complete JSON and a supported schema. Redirect the receipt outside the oracle directory. Protect it like the recovery set because it includes opaque holder identities.

The export opens an existing `LOCK` read-only without create or truncate and takes the file driver's nonblocking exclusive native lock. It retains ownership through validation, serialization and stdout flush. It never invokes `open_or_init`, samples a clock, advances a floor, removes temporary files or repairs a record. Reading may update filesystem access times. State contents and directory entries stay unchanged. The sentinel is read through the guard's own descriptor, and its identity is checked against the current directory entry before and after capture. This detects replacement before certification; maintenance ownership must still prevent later replacement during copying.

## Capture and ownership

Close mutation ingress, drain work, stop the oracle and all timestamp or dense-sequence holders, disable supervisor restarts and verify exit. Native lock acquisition only excludes cooperating file-driver owners. It cannot prove child exit or fence another copy on a different disk. Keep the directory and its ancestor paths under trusted maintenance ownership. Do not rename the directory, replace `LOCK`, or allow uncooperative writers during capture. The supported durable filesystem must provide native advisory exclusion.

A library consumer uses `tsoracle_driver_file::StoppedFileState::open(dir)`, then `guard.receipt()`, and keeps the guard alive through copying the complete directory and verifying destination sizes and SHA-256 digests against `receipt.files`. Any error aborts certification. Publish and sync the external backup manifest only after the complete recovery set is durable. Drop the guard before ordinary driver restart. Create a distinct empty `LOCK` at the destination instead of copying the locked sentinel through another handle. This also supports Windows exclusive locks. A hard link to the source would couple ownership across recovery sets. Copy only inventory entries with `present: true` and verify that entries with `present: false` remain absent.

The CLI releases ownership on exit. A shell command followed by an unlocked copy cannot establish a consistent capture when another launcher can start. Use the library guard across the copy, or supply separately enforced exclusion spanning the entire operation. A consumer already holding native `LOCK` must adopt this producer guard as its ownership boundary. Starting the CLI while holding a separately opened native lock refuses. There is no inherited-descriptor or lock-bypass flag. The library guard supplies the supported export-and-copy boundary without an exclusion gap.

The external backup manifest binds the receipt to the oracle executable digest, source/version and build features, stable namespace, source deployment identity and the same stopped recovery set. The driver has no persisted deployment identity and cannot authenticate these bindings. A file digest detects mismatched bytes; it cannot establish receipt provenance or prove an allocation history is complete.

## Inventory and refusal

Version 1 requires existing regular `LOCK` and `state` files. `LOCK` must be empty. `dense` and `leases` are optional because the driver initializes them lazily. The exporter rejects symlinks, nonregular entries and, on Unix, hard links. It refuses a symlink at the state directory itself. Parent paths remain subject to trusted maintenance ownership. Extra files, subdirectories and temporary files refuse because interrupted publication needs recovery before capture. No file larger than 64 MiB is accepted.

Absent `dense` means an empty map with the driver's default cardinality cap. Absent `leases` means an empty retained set. The inventory explicitly records absence with `present: false`, `size_bytes: null` and `sha256: null`; no empty file or synthetic file digest is created. A present empty record retains its actual size and digest. Absence has the same meaning as driver startup and cannot distinguish lazy initialization from deletion. Recovery must preserve the captured presence state and rely on the trusted recovery set for completeness.

A timestamp-only oracle exports once it has persisted its reservation. With `single-node`, `init_seeded` creates and holds the native lock while writing the seed, so a freshly seeded directory is also exportable. Without the feature, seeding retains the existing behavior and does not create `LOCK`. An entirely uninitialized directory without a persisted reservation refuses export.

The producer decoders validate lengths, magics, versions, checksums and ordering. Export and `single-node` driver startup use one shared semantic validator for physical range, dense keys and cardinality cap, nonempty bounded holders, nonzero TTLs and `lease_id <= ts_upper_bound <= high_water`. Renewing a lease can raise its upper bound while keeping its original ID. Runtime TTL limits and current-clock expiry are excluded from stored-record validation. Default driver startup retains its existing semantic acceptance behavior.

Library errors retain typed state, dense and lease decoder sources. I/O and decoder errors carry the affected path. Shared semantic errors identify the violated invariant.

## Receipt fields and units

| Field | Meaning |
| --- | --- |
| `schema`, `schema_version` | Exact document identity and version. Consumers must explicitly accept version 1. |
| `producer` | Producer driver crate name and compiled package version. Artifact identity is bound separately by the recovery manifest. |
| `driver`, `epoch` | `file`, epoch zero. The epoch is a driver property; it supplies no new recovery identity. |
| `files` | Sorted relative paths, explicit `present` flags, nullable sizes in bytes, nullable lowercase SHA-256 digests, record magic and format version. The native lock sentinel has a null format version. Present-file digests cover the captured bytes under ownership; absent entries carry no digest. |
| `committed_reservation_high_water_physical_ms` | Inclusive physical milliseconds since the Unix epoch. Every logical timestamp in that millisecond is reserved. This value is separate from packed timestamps. |
| `leases` | Complete retained set ordered by lease ID, including expired and superseded entries. Holder bytes use lowercase hexadecimal; holder epoch is an opaque ordering value. TTL is in milliseconds and expiry is absolute Unix milliseconds on the oracle server's clock. |
| `maximum_lease_upper_bound_physical_ms` | Inclusive maximum physical reservation over every retained lease. Null means the validated lease set is empty. |
| `maximum_lease_expires_at_unix_ms` | Absolute server-clock expiry maximum over every retained lease. Null means the validated lease set is empty. No consumer-clock subtraction or filtering occurs. |
| `dense_next_unallocated` | Per-key exclusive reservation ceilings. A value N reserves ordinals below N. Restart allocates at N or later, subject to exhaustion; it does not need to skip N itself. |
| `dense_cardinality_cap` | Persisted maximum number of dense keys, or the driver default when the dense file is absent. |
| `units` | Explicit semantic unit strings. All integer values must be read losslessly as unsigned 64-bit values where applicable. JavaScript floating-point numbers are insufficient for large counters and expiry values. |

## Consumer inventory contract

A backup or migration tool must require schema `tsoracle.stopped-file-state` and `schema_version: 1`. The `files` array contains exactly four entries in this order: `LOCK`, `dense`, `leases`, `state`. Each path occurs once and is relative to the state directory. A missing inventory entry is an incomplete receipt and must refuse; it never means an empty subsystem.

| Path | Required presence | `format` | `format_version` |
| --- | --- | --- | --- |
| `LOCK` | `true` | `native-lock-sentinel` | `null` |
| `dense` | `true` or `false` | `TSOD` | `1` |
| `leases` | `true` or `false` | `TSOO` | `1` |
| `state` | `true` | `TSOR` | `1` |

For every entry, `present` is a required JSON boolean. When it is `true`, `size_bytes` is a required unsigned 64-bit integer and `sha256` is a required string of exactly 64 lowercase hexadecimal characters. Verify both against the captured file bytes. An empty sentinel has size zero and the SHA-256 digest of zero bytes. A present dense or lease record containing an empty logical set still has its encoded record's nonzero size and digest.

When `present` is `false`, both `size_bytes` and `sha256` are required JSON null values. Null metadata records that no file was captured. It must never be converted to a zero size, the digest of empty bytes or an empty encoded file. `format` and `format_version` still identify the expected producer record format for that path; they do not assert that absent bytes were decoded. For example, an absent dense file has this inventory entry:

```json
{
  "path": "dense",
  "present": false,
  "size_bytes": null,
  "sha256": null,
  "format": "TSOD",
  "format_version": 1
}
```

For an absent dense file, `dense_next_unallocated` is an empty object and `dense_cardinality_cap` is the driver's version-1 default, 10000. For an absent lease file, `leases` is an empty array and both lease maxima are null. A present record can also describe an empty logical set; its `present` flag remains true. Consumers must reject contradictory fields or missing required fields rather than infer defaults from incomplete JSON. Parse integers losslessly; floating-point conversion can lose reservation and counter values.

Capture or restore into an empty destination directory. Create a distinct empty `LOCK`, copy the other entries marked present, and require every entry marked absent to remain absent. Refuse a destination containing an unexpected entry or a stale file at an absent path. Verify the complete presence map, sizes and digests before publishing the recovery set. Do not synthesize dense or lease files to replace recorded absence. Source ownership must span capture and verification as described above.

The reservation survives lease pruning and bounds prior timestamp grants. Retained leases supply the full currently persisted set. They are not a historical grant ledger. A receipt alone cannot authorize rollback to an older oracle state or migration to a cluster.

## Restore and growth

[The proposed anti-reuse procedure](2026-10-26-oracle-anti-reuse-import.md) specifies trust, holder fencing, floor import, durable publication and allocation evidence. No receipt import command is implemented. A complete exact restore can use existing startup fencing only when the recovery receipt demonstrably bounds all prior allocations and the previous deployment and holders are fenced. An older backup without later trusted bounds must keep mutation readiness withdrawn. Cluster growth and floor-raising rollback remain gated on a qualified destination import.

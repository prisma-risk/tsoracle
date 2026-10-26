# Stopped file-driver state export

`tsoracle export file --state-dir /var/lib/tsoracle --json` emits schema `tsoracle.stopped-file-state`, version 1. The command requires the `file` build feature. Without `--json` it emits the same complete receipt as indented JSON. Errors use a nonzero exit status and produce no receipt on stdout. An output I/O error can leave partial stdout, so consumers must require successful exit, complete JSON and a supported schema. Redirect the receipt outside the oracle directory. Protect it like the recovery set because it includes opaque holder identities.

The export opens an existing `LOCK` without create or truncate and takes the file driver's nonblocking exclusive native lock. It retains ownership through validation, serialization and stdout flush. It never invokes `open_or_init`, samples a clock, advances a floor, removes temporary files or repairs a record. Reading may update filesystem access times. State contents and directory entries stay unchanged.

## Capture and ownership

Close mutation ingress, drain work, stop the oracle and all timestamp or dense-sequence holders, disable supervisor restarts and verify exit. Native lock acquisition only excludes cooperating file-driver owners. It cannot prove child exit or fence another copy on a different disk. Keep the directory and its ancestor paths under trusted maintenance ownership. Do not rename the directory, replace `LOCK`, or allow uncooperative writers during capture. The supported durable filesystem must provide native advisory exclusion.

A library consumer uses `tsoracle_driver_file::StoppedFileState::open(dir)`, then `guard.receipt()`, and keeps the guard alive through copying the complete directory and verifying destination sizes and SHA-256 digests against `receipt.files`. Any error aborts certification. Publish and sync the external backup manifest only after the complete recovery set is durable. Drop the guard before ordinary driver restart. Each copied `LOCK` must be a distinct regular inode; a hard link to the source would couple ownership across recovery sets.

The CLI releases ownership on exit. A shell command followed by an unlocked copy cannot establish a consistent capture when another launcher can start. Use the library guard across the copy, or supply separately enforced exclusion spanning the entire operation. A consumer already holding native `LOCK` must adopt this producer guard as its ownership boundary. Starting the CLI while holding a separately opened native lock refuses. There is no inherited-descriptor or lock-bypass flag. The library guard supplies the supported export-and-copy boundary without an exclusion gap.

The external backup manifest binds the receipt to the oracle executable digest, source/version and build features, stable namespace, graph cluster identity and the same stopped recovery set. The driver has no persisted cluster identity and cannot authenticate these bindings. A file digest detects mismatched bytes; it cannot establish receipt provenance or prove an allocation history is complete.

## Inventory and refusal

Version 1 requires exactly four existing regular files: `LOCK`, `state`, `dense` and `leases`. `LOCK` must be empty. The exporter rejects symlinks, nonregular entries and, on Unix, hard links. It refuses a symlink at the state directory itself. Parent paths remain subject to trusted maintenance ownership. Extra files, subdirectories and all temporary files refuse because an interrupted publication needs explicit recovery before capture. No file larger than 64 MiB is accepted. The limits bound capture and decoding; an oversized valid store requires a reviewed exporter revision.

Existing initialization is lazy. `open_or_init` may leave `state`, `dense` or `leases` absent until the corresponding first durable write. Export refuses those directories too. Absence has no durable provenance that can distinguish an unused subsystem from a deleted recovery file. The exporter never fabricates an empty set. Before a directory is eligible, its producer must have durably written all three record files through normal operation or an independently reviewed initialization procedure. A future producer-owned initialization marker could support safe empty-state export. Do not create placeholder files or delete a subsystem's file to satisfy capture.

The producer decoders validate exact lengths, magic, version, checksum and record ordering. Export additionally validates the physical range, dense keys and cardinality cap, lease ID equality with its upper bound, nonempty bounded holder, nonzero TTL and every lease upper bound against the committed reservation. Configurable runtime TTL limits are deliberately excluded from stored-record validation. No current-clock expiry rule is applied. Invalid or missing input fails the entire export.

## Receipt fields and units

| Field | Meaning |
| --- | --- |
| `schema`, `schema_version` | Exact document identity and version. Consumers must explicitly accept version 1. |
| `producer` | Producer driver crate name and compiled package version. Artifact identity is bound separately by the recovery manifest. |
| `driver`, `epoch` | `file`, epoch zero. The epoch is a driver property; it supplies no new recovery identity. |
| `files` | Sorted relative paths, sizes in bytes, lowercase SHA-256 digests, record magic and format version. The native lock sentinel has a null format version. All decoded fields derive from those captured bytes under ownership. |
| `committed_reservation_high_water_physical_ms` | Inclusive physical milliseconds since the Unix epoch. Every logical timestamp in that millisecond is reserved. This value is separate from packed timestamps. |
| `leases` | Complete retained set ordered by lease ID, including expired and superseded entries. Holder bytes use lowercase hexadecimal; holder epoch is an opaque ordering value. TTL is in milliseconds and expiry is absolute Unix milliseconds on the oracle server's clock. |
| `maximum_lease_upper_bound_physical_ms` | Inclusive maximum physical reservation over every retained lease. Null means the validated lease set is empty. |
| `maximum_lease_expires_at_unix_ms` | Absolute server-clock expiry maximum over every retained lease. Null means the validated lease set is empty. No consumer-clock subtraction or filtering occurs. |
| `dense_next_unallocated` | Per-key exclusive reservation ceilings. A value N reserves ordinals below N. Restart allocates at N or later, subject to exhaustion; it does not need to skip N itself. |
| `dense_cardinality_cap` | Persisted maximum number of dense keys. |
| `units` | Explicit semantic unit strings. All integer values must be read losslessly as unsigned 64-bit values where applicable. JavaScript floating-point numbers are insufficient for large counters and expiry values. |

The reservation survives lease pruning and bounds prior timestamp grants. Retained leases supply the full currently persisted set. They are not a historical grant ledger. A receipt alone cannot authorize rollback to an older oracle state or migration to a cluster.

## Restore and growth

[The proposed anti-reuse procedure](2026-10-26-oracle-anti-reuse-import.md) specifies trust, holder fencing, floor import, durable publication and allocation evidence. No receipt import command is implemented. A complete exact restore can use existing startup fencing only when the recovery receipt demonstrably bounds all prior allocations and the previous deployment and holders are fenced. An older backup without later trusted bounds must keep mutation readiness withdrawn. Cluster growth and floor-raising rollback remain gated on a qualified destination import.

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.3.0...tsoracle-driver-paxos-v0.3.1) - 2026-05-26

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.3.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.2.3...tsoracle-driver-paxos-v0.3.0) - 2026-05-25

### Fixed

- *(driver-paxos)* never mint the 0 lease sentinel on generation-counter wrap ([#442](https://github.com/prisma-risk/tsoracle/pull/442))
- *(paxos)* seed barrier-nonce recovery from the durable log, not the non-synced decided_idx ([#427](https://github.com/prisma-risk/tsoracle/pull/427))
- *(driver-paxos)* classify append rejections by variant, not opaque string ([#425](https://github.com/prisma-risk/tsoracle/pull/425))
- *(driver-paxos)* make the single-active leadership lease observable and ABA-proof ([#416](https://github.com/prisma-risk/tsoracle/pull/416))
- *(paxos-toolkit)* [**breaking**] enforce PaxosRunner::start guard in release builds ([#412](https://github.com/prisma-risk/tsoracle/pull/412))
- *(driver-paxos)* enforce single active leadership stream via Drop-released lease ([#403](https://github.com/prisma-risk/tsoracle/pull/403))

### Other

- raise library coverage to 95.5% with targeted tests ([#414](https://github.com/prisma-risk/tsoracle/pull/414))

## [0.2.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.2.2...tsoracle-driver-paxos-v0.2.3) - 2026-05-25

### Added

- *(consensus)* give the high-water advance rule a single home via AdvancePayload::merge ([#369](https://github.com/prisma-risk/tsoracle/pull/369))
- *(paxos)* surface snapshot and async-persistence failures as health metrics ([#367](https://github.com/prisma-risk/tsoracle/pull/367))
- *(core)* lift a shared TsoPeer type into tsoracle-core ([#266](https://github.com/prisma-risk/tsoracle/pull/266)) ([#325](https://github.com/prisma-risk/tsoracle/pull/325))
- *(consensus)* unify HighWaterCommand advance naming across backends ([#323](https://github.com/prisma-risk/tsoracle/pull/323))

### Fixed

- *(driver-paxos)* re-subscribe leadership_events instead of take-once ([#262](https://github.com/prisma-risk/tsoracle/pull/262)) ([#396](https://github.com/prisma-risk/tsoracle/pull/396))
- *(driver-paxos)* reject double-start with AlreadyRunning instead of a debug-only assert ([#380](https://github.com/prisma-risk/tsoracle/pull/380))
- *(driver-paxos)* rebase snapshot baseline at recovery to avoid spurious post-restart snapshot ([#375](https://github.com/prisma-risk/tsoracle/pull/375))
- *(driver-paxos)* bound StandaloneHost barrier waits with a deadline and apply-task liveness ([#364](https://github.com/prisma-risk/tsoracle/pull/364))
- *(consensus)* reject out-of-range high-water advance before persisting ([#360](https://github.com/prisma-risk/tsoracle/pull/360))
- *(driver-paxos)* seed the apply cursor from the recovery fold instead of re-draining from 0 ([#330](https://github.com/prisma-risk/tsoracle/pull/330))

### Other

- *(driver-paxos)* extract ApplyEngine + ApplyTask from StandaloneHost ([#327](https://github.com/prisma-risk/tsoracle/pull/327))
- make the remaining real-time consensus-harness tests deterministic ([#326](https://github.com/prisma-risk/tsoracle/pull/326))
- *(driver-paxos)* convert the blocking-driver harness tests to deterministic stepping ([#318](https://github.com/prisma-risk/tsoracle/pull/318))
- *(driver-paxos)* deterministic step-driver for the paxos harness (+ convert the steppable tests) ([#312](https://github.com/prisma-risk/tsoracle/pull/312))

## [0.2.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.2.1...tsoracle-driver-paxos-v0.2.2) - 2026-05-24

### Added

- pin an on-disk schema version for snapshots, log entries, and meta records ([#291](https://github.com/prisma-risk/tsoracle/pull/291))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.2.0...tsoracle-driver-paxos-v0.2.1) - 2026-05-24

### Fixed

- *(driver-paxos)* gate submit_advance on a per-call barrier nonce ([#256](https://github.com/prisma-risk/tsoracle/pull/256)) ([#278](https://github.com/prisma-risk/tsoracle/pull/278))
- *(driver-paxos)* mint a fresh apply-shutdown Notify per start ([#232](https://github.com/prisma-risk/tsoracle/pull/232))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.1.4...tsoracle-driver-paxos-v0.2.0) - 2026-05-23

### Added

- *(driver-paxos)* generic entry type + paxos-piggyback example ([#191](https://github.com/prisma-risk/tsoracle/pull/191))
- *(yieldpoint)* extract yield-point registry into `tsoracle-yieldpoint`, wire `tsoracle-server::fence` ([#198](https://github.com/prisma-risk/tsoracle/pull/198))

### Fixed

- *(driver-paxos)* seed barrier_seq above the recovered ledger on restart ([#224](https://github.com/prisma-risk/tsoracle/pull/224))
- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))
- *(driver-paxos)* linearize current_high_water via per-node barrier nonces ([#209](https://github.com/prisma-risk/tsoracle/pull/209))
- *(driver-paxos)* qualify yieldpoint! macro path on the new sites ([#199](https://github.com/prisma-risk/tsoracle/pull/199))
- *(driver-paxos)* blocking reads observe drains via Notified::enable() ([#196](https://github.com/prisma-risk/tsoracle/pull/196))
- *(driver-paxos)* apply-task shutdown uses notify_one (stored permit) ([#194](https://github.com/prisma-risk/tsoracle/pull/194))

### Other

- *(paxos)* add fuzz targets and seed corpora for the paxos decoders ([#225](https://github.com/prisma-risk/tsoracle/pull/225))
- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- *(driver-paxos)* widen standalone shutdown liveness bound to 10s ([#220](https://github.com/prisma-risk/tsoracle/pull/220))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))
- *(driver-paxos)* wait for follower promise sync before sampling fence epoch ([#197](https://github.com/prisma-risk/tsoracle/pull/197))
- *(driver-paxos)* integration test suite ([#185](https://github.com/prisma-risk/tsoracle/pull/185))

## [0.1.4](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-paxos-v0.1.3...tsoracle-driver-paxos-v0.1.4) - 2026-05-23

### Added

- *(driver-paxos)* StandaloneHost and PaxosDriver public façade ([#182](https://github.com/prisma-risk/tsoracle/pull/182))
- *(driver-paxos)* host trait, apply task state machine, snapshot policy ([#180](https://github.com/prisma-risk/tsoracle/pull/180))

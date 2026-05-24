# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

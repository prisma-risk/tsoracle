# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v1.0.2...tsoracle-paxos-toolkit-v1.0.3) - 2026-05-30

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [1.0.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v1.0.1...tsoracle-paxos-toolkit-v1.0.2) - 2026-05-30

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [1.0.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v1.0.0...tsoracle-paxos-toolkit-v1.0.1) - 2026-05-27

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.3.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.3.2...tsoracle-paxos-toolkit-v0.3.3) - 2026-05-26

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.3.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.3.1...tsoracle-paxos-toolkit-v0.3.2) - 2026-05-26

### Other

- updated the following local packages: tsoracle-codec

## [0.3.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.3.0...tsoracle-paxos-toolkit-v0.3.1) - 2026-05-26

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.3.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.2.3...tsoracle-paxos-toolkit-v0.3.0) - 2026-05-25

### Added

- *(paxos-toolkit)* [**breaking**] drop unused declare_omnipaxos_types_ext! macro and pastey dep ([#429](https://github.com/prisma-risk/tsoracle/pull/429))

### Fixed

- *(server)* enforce LeaderState::Follower driver contracts with a debug guard ([#439](https://github.com/prisma-risk/tsoracle/pull/439))
- *(paxos-toolkit)* [**breaking**] enforce PaxosRunner::start guard in release builds ([#412](https://github.com/prisma-risk/tsoracle/pull/412))
- *(paxos-toolkit)* keep RocksdbStorage cursor on out-of-range empty truncate ([#401](https://github.com/prisma-risk/tsoracle/pull/401))

## [0.2.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.2.2...tsoracle-paxos-toolkit-v0.2.3) - 2026-05-25

### Added

- *(paxos)* surface snapshot and async-persistence failures as health metrics ([#367](https://github.com/prisma-risk/tsoracle/pull/367))
- *(core)* lift a shared TsoPeer type into tsoracle-core ([#266](https://github.com/prisma-risk/tsoracle/pull/266)) ([#325](https://github.com/prisma-risk/tsoracle/pull/325))
- extract the version-prefixed postcard codec into a shared tsoracle-codec crate ([#324](https://github.com/prisma-risk/tsoracle/pull/324))
- extract shared tsoracle-failpoint crate ([#306](https://github.com/prisma-risk/tsoracle/pull/306))

### Fixed

- *(driver-paxos)* re-subscribe leadership_events instead of take-once ([#262](https://github.com/prisma-risk/tsoracle/pull/262)) ([#396](https://github.com/prisma-risk/tsoracle/pull/396))

### Other

- *(paxos-toolkit)* cache next/compacted log indices instead of scanning per append ([#391](https://github.com/prisma-risk/tsoracle/pull/391))
- reflect paxos stress topology in README and stress-testing guide ([#388](https://github.com/prisma-risk/tsoracle/pull/388))
- *(paxos-toolkit)* require Send + Sync on box_err input ([#387](https://github.com/prisma-risk/tsoracle/pull/387))
- *(paxos)* isolate async_write failpoint tests into the integration binary ([#377](https://github.com/prisma-risk/tsoracle/pull/377))
- make the remaining real-time consensus-harness tests deterministic ([#326](https://github.com/prisma-risk/tsoracle/pull/326))
- *(driver-paxos)* deterministic step-driver for the paxos harness (+ convert the steppable tests) ([#312](https://github.com/prisma-risk/tsoracle/pull/312))

## [0.2.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.2.1...tsoracle-paxos-toolkit-v0.2.2) - 2026-05-24

### Added

- *(stress)* detect non-overlapping cross-client real-time monotonicity ([#135](https://github.com/prisma-risk/tsoracle/pull/135)) ([#297](https://github.com/prisma-risk/tsoracle/pull/297))
- pin an on-disk schema version for snapshots, log entries, and meta records ([#291](https://github.com/prisma-risk/tsoracle/pull/291))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.2.0...tsoracle-paxos-toolkit-v0.2.1) - 2026-05-24

### Added

- populate NOT_LEADER hints with leader endpoint and epoch (#88, #125) ([#234](https://github.com/prisma-risk/tsoracle/pull/234))

### Fixed

- *(paxos-toolkit)* floor MemStorage append at compacted_idx after full trim ([#277](https://github.com/prisma-risk/tsoracle/pull/277))

### Other

- *(paxos-toolkit)* gate test-fakes integration tests behind required-features ([#231](https://github.com/prisma-risk/tsoracle/pull/231))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.1.4...tsoracle-paxos-toolkit-v0.2.0) - 2026-05-23

### Fixed

- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))
- *(paxos-toolkit)* decouple PaxosRunner tick loop from outbound send completion ([#218](https://github.com/prisma-risk/tsoracle/pull/218))
- *(paxos-toolkit)* preserve absolute log index after full RocksDB compaction ([#188](https://github.com/prisma-risk/tsoracle/pull/188))

### Other

- *(paxos)* add fuzz targets and seed corpora for the paxos decoders ([#225](https://github.com/prisma-risk/tsoracle/pull/225))
- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))

## [0.1.4](https://github.com/prisma-risk/tsoracle/compare/tsoracle-paxos-toolkit-v0.1.3...tsoracle-paxos-toolkit-v0.1.4) - 2026-05-23

### Added

- *(paxos-toolkit)* in-memory test fakes (storage, network, partition controller) ([#169](https://github.com/prisma-risk/tsoracle/pull/169))
- *(paxos-toolkit)* declare_omnipaxos_types_ext! macro and lifecycle runner ([#167](https://github.com/prisma-risk/tsoracle/pull/167))
- *(paxos-toolkit)* RocksDB-backed omnipaxos Storage trait implementation ([#166](https://github.com/prisma-risk/tsoracle/pull/166))
- *(paxos-toolkit)* postcard codec, RocksDB key space, and meta serializers ([#165](https://github.com/prisma-risk/tsoracle/pull/165))

### Other

- *(paxos-toolkit)* integration tests and conformance harness ([#170](https://github.com/prisma-risk/tsoracle/pull/170))
- update README.md badges ([#164](https://github.com/prisma-risk/tsoracle/pull/164))

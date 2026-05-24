# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

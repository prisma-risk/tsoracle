# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v1.2.0...tsoracle-proto-v1.3.0) - 2026-07-07

### Added

- add lease API and safe frontier ([#660](https://github.com/prisma-risk/tsoracle/pull/660))

## [1.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v1.1.0...tsoracle-proto-v1.2.0) - 2026-05-31

### Added

- atomic multi-key GetSeqBatch RPC ([#601](https://github.com/prisma-risk/tsoracle/pull/601))

## [1.1.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v1.0.0...tsoracle-proto-v1.1.0) - 2026-05-30

### Added

- keyed dense sequence service (GetSeq) with file driver consensus support ([#579](https://github.com/prisma-risk/tsoracle/pull/579))

## [0.2.4](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v0.2.3...tsoracle-proto-v0.2.4) - 2026-05-26

### Added

- GetCurrentMaxSafe RPC ([#493](https://github.com/prisma-risk/tsoracle/pull/493))

### Other

- *(proto)* expand and correct service/RPC/field comments ([#492](https://github.com/prisma-risk/tsoracle/pull/492))

## [0.2.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v0.2.2...tsoracle-proto-v0.2.3) - 2026-05-26

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v0.2.0...tsoracle-proto-v0.2.1) - 2026-05-24

### Fixed

- *(proto)* bundle LeaderHint epoch into a single nested EpochWire ([#252](https://github.com/prisma-risk/tsoracle/pull/252)) ([#273](https://github.com/prisma-risk/tsoracle/pull/273))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v0.1.4...tsoracle-proto-v0.2.0) - 2026-05-23

### Fixed

- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))

### Other

- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- add READMEs for the remaining published crates ([#213](https://github.com/prisma-risk/tsoracle/pull/213))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v0.1.2...tsoracle-proto-v0.1.3) - 2026-05-22

### Added

- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-proto-v0.1.0...tsoracle-proto-v0.1.1) - 2026-05-21

### Added

- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))
- *(fuzz)* add coverage-guided fuzz testing ([#16](https://github.com/prisma-risk/tsoracle/pull/16))
- *(server)* add optional gRPC reflection ([#2](https://github.com/prisma-risk/tsoracle/pull/2))

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))
- address final-review findings ([#21](https://github.com/prisma-risk/tsoracle/pull/21))
- correct contrib.rocks attribution in README.md
- add contributors section in README.md
- update badges in README.md

## [0.1.0] - 2026-05-21

Initial release.

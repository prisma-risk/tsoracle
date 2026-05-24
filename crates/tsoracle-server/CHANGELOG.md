# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-server-v0.2.1...tsoracle-server-v0.2.2) - 2026-05-24

### Added

- populate NOT_LEADER hints with leader endpoint and epoch (#88, #125) ([#234](https://github.com/prisma-risk/tsoracle/pull/234))

### Fixed

- *(server)* carry the fencing epoch into the NOT_LEADER hint on stepdown ([#275](https://github.com/prisma-risk/tsoracle/pull/275))
- *(proto)* bundle LeaderHint epoch into a single nested EpochWire ([#252](https://github.com/prisma-risk/tsoracle/pull/252)) ([#273](https://github.com/prisma-risk/tsoracle/pull/273))
- *(server)* retry fence transient errors while racing the leadership stream ([#229](https://github.com/prisma-risk/tsoracle/pull/229)) ([#235](https://github.com/prisma-risk/tsoracle/pull/235))
- *(server)* require test-fakes for fence_yieldpoint integration test ([#230](https://github.com/prisma-risk/tsoracle/pull/230))

### Other

- *(server)* drive serve_shutdown tests through serve_with_listener ([#248](https://github.com/prisma-risk/tsoracle/pull/248)) ([#282](https://github.com/prisma-risk/tsoracle/pull/282))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-server-v0.2.0...tsoracle-server-v0.2.1) - 2026-05-23

### Fixed

- *(server)* recover from transient consensus errors during the fence ([#227](https://github.com/prisma-risk/tsoracle/pull/227))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-server-v0.1.4...tsoracle-server-v0.2.0) - 2026-05-23

### Added

- *(yieldpoint)* extract yield-point registry into `tsoracle-yieldpoint`, wire `tsoracle-server::fence` ([#198](https://github.com/prisma-risk/tsoracle/pull/198))

### Fixed

- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))

### Other

- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- add READMEs for the remaining published crates ([#213](https://github.com/prisma-risk/tsoracle/pull/213))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-server-v0.1.2...tsoracle-server-v0.1.3) - 2026-05-22

### Added

- add opt-in bt cargo feature for error backtraces ([#120](https://github.com/prisma-risk/tsoracle/pull/120))
- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

### Fixed

- *(server)* poison serving state on leader-watch stream EOF ([#124](https://github.com/prisma-risk/tsoracle/pull/124))
- *(client)* bound coalescing driver waiters and stream chunk delivery ([#115](https://github.com/prisma-risk/tsoracle/pull/115))

### Other

- *(critical-path)* mark per-request and consensus files; drop shell lib.rs markers ([#113](https://github.com/prisma-risk/tsoracle/pull/113))
- *(client,server)* close coverage gaps in TLS plumbing ([#83](https://github.com/prisma-risk/tsoracle/pull/83))

## [0.1.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-server-v0.1.1...tsoracle-server-v0.1.2) - 2026-05-22

### Added

- *(client,server)* TLS and mTLS transport configuration ([#81](https://github.com/prisma-risk/tsoracle/pull/81))

### Other

- *(contract)* clarify monotonicity guarantee, drop "gap-free" overclaim ([#73](https://github.com/prisma-risk/tsoracle/pull/73))
- *(brand)* update description ([#71](https://github.com/prisma-risk/tsoracle/pull/71))
- *(headers)* enforce canonical copyright header on .rs files ([#70](https://github.com/prisma-risk/tsoracle/pull/70))
- *(readme)* replace title heading with light/dark logo ([#69](https://github.com/prisma-risk/tsoracle/pull/69))
- *(readme)* expand examples list with HA and metrics bullets ([#68](https://github.com/prisma-risk/tsoracle/pull/68))
- *(tests)* move cross-crate e2e tests to tsoracle-tests crate ([#60](https://github.com/prisma-risk/tsoracle/pull/60))
- raise workspace coverage ([#57](https://github.com/prisma-risk/tsoracle/pull/57))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-server-v0.1.0...tsoracle-server-v0.1.1) - 2026-05-21

### Added

- *(server)* emit operational metrics via the metrics crate facade ([#41](https://github.com/prisma-risk/tsoracle/pull/41))
- add failpoint testing for driver-file and server paths ([#22](https://github.com/prisma-risk/tsoracle/pull/22))
- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))
- *(server)* add optional gRPC reflection ([#2](https://github.com/prisma-risk/tsoracle/pull/2))
- *(benchmarks)* add minimal setup ([#1](https://github.com/prisma-risk/tsoracle/pull/1))

### Fixed

- *(server)* replace leader-hint metadata-key expect with startup validation ([#38](https://github.com/prisma-risk/tsoracle/pull/38))
- *(server)* poison serving state when leader-watch panics in into_router ([#29](https://github.com/prisma-risk/tsoracle/pull/29))
- *(tests)* fix flaky tests related to bind race ([#15](https://github.com/prisma-risk/tsoracle/pull/15))

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))
- *(test)* introduce shared bootstrap helper for integration tests ([#39](https://github.com/prisma-risk/tsoracle/pull/39))
- *(lints)* warn on unwrap/expect in non-test code ([#28](https://github.com/prisma-risk/tsoracle/pull/28))
- address final-review findings ([#21](https://github.com/prisma-risk/tsoracle/pull/21))
- correct contrib.rocks attribution in README.md
- rename single-letter bindings to descriptive nouns
- add contributors section in README.md
- use descriptive parameter names and drop stale doc links
- update badges in README.md

## [0.1.0] - 2026-05-21

Initial release.

# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.1.4...tsoracle-client-v0.2.0) - 2026-05-23

### Fixed

- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))

### Other

- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- add READMEs for the remaining published crates ([#213](https://github.com/prisma-risk/tsoracle/pull/213))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.1.2...tsoracle-client-v0.1.3) - 2026-05-22

### Added

- *(client)* instrument retry, driver, and connect signals ([#116](https://github.com/prisma-risk/tsoracle/pull/116))
- add opt-in bt cargo feature for error backtraces ([#120](https://github.com/prisma-risk/tsoracle/pull/120))
- *(client)* add RetryPolicy with deadlines, keepalive, and jittered backoff ([#114](https://github.com/prisma-risk/tsoracle/pull/114))
- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

### Fixed

- *(client)* honor LeaderHint.leader_epoch and TTL the cached leader ([#126](https://github.com/prisma-risk/tsoracle/pull/126))
- *(client)* surface driver-task death as DriverGone ([#118](https://github.com/prisma-risk/tsoracle/pull/118))
- *(client)* bound coalescing driver waiters and stream chunk delivery ([#115](https://github.com/prisma-risk/tsoracle/pull/115))
- *(client)* reject plaintext leader-hint under tls_config ([#108](https://github.com/prisma-risk/tsoracle/pull/108))

### Other

- *(client)* dedupe MAX_TIMESTAMPS_PER_RPC into lib.rs ([#122](https://github.com/prisma-risk/tsoracle/pull/122))
- *(client,server)* close coverage gaps in TLS plumbing ([#83](https://github.com/prisma-risk/tsoracle/pull/83))

## [0.1.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.1.1...tsoracle-client-v0.1.2) - 2026-05-22

### Added

- *(client,server)* TLS and mTLS transport configuration ([#81](https://github.com/prisma-risk/tsoracle/pull/81))

### Other

- *(contract)* clarify monotonicity guarantee, drop "gap-free" overclaim ([#73](https://github.com/prisma-risk/tsoracle/pull/73))
- *(brand)* update description ([#71](https://github.com/prisma-risk/tsoracle/pull/71))
- *(headers)* enforce canonical copyright header on .rs files ([#70](https://github.com/prisma-risk/tsoracle/pull/70))
- *(readme)* replace title heading with light/dark logo ([#69](https://github.com/prisma-risk/tsoracle/pull/69))
- *(readme)* expand examples list with HA and metrics bullets ([#68](https://github.com/prisma-risk/tsoracle/pull/68))
- *(client)* drop expect() from flush-deadline path in driver_task ([#64](https://github.com/prisma-risk/tsoracle/pull/64))
- *(tests)* move cross-crate e2e tests to tsoracle-tests crate ([#60](https://github.com/prisma-risk/tsoracle/pull/60))
- raise workspace coverage ([#57](https://github.com/prisma-risk/tsoracle/pull/57))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.1.0...tsoracle-client-v0.1.1) - 2026-05-21

### Added

- add failpoint testing for driver-file and server paths ([#22](https://github.com/prisma-risk/tsoracle/pull/22))
- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))

### Fixed

- *(tests)* fix flaky tests related to bind race ([#15](https://github.com/prisma-risk/tsoracle/pull/15))

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))
- *(test)* introduce shared bootstrap helper for integration tests ([#39](https://github.com/prisma-risk/tsoracle/pull/39))
- *(perf)* add performance-critical-path guard ([#30](https://github.com/prisma-risk/tsoracle/pull/30))
- *(lints)* warn on unwrap/expect in non-test code ([#28](https://github.com/prisma-risk/tsoracle/pull/28))
- address final-review findings ([#21](https://github.com/prisma-risk/tsoracle/pull/21))
- correct contrib.rocks attribution in README.md
- rename single-letter bindings to descriptive nouns
- add contributors section in README.md
- update badges in README.md

## [0.1.0] - 2026-05-21

Initial release.

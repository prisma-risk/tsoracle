# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.6](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.2.5...tsoracle-client-v0.2.6) - 2026-05-26

### Added

- GetCurrentMaxSafe RPC ([#493](https://github.com/prisma-risk/tsoracle/pull/493))

## [0.2.5](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.2.4...tsoracle-client-v0.2.5) - 2026-05-26

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.2.4](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.2.3...tsoracle-client-v0.2.4) - 2026-05-25

### Added

- *(client)* count waiters abandoned before delivery ([#437](https://github.com/prisma-risk/tsoracle/pull/437))

### Fixed

- *(client)* bound leader-hint redirects with an absolute cross-pass cap ([#441](https://github.com/prisma-risk/tsoracle/pull/441))
- *(client)* keep the election signal sticky so a final timeout can't bury NOT_LEADER ([#432](https://github.com/prisma-risk/tsoracle/pull/432))
- *(client)* ride out a leader election in issue_rpc ([#417](https://github.com/prisma-risk/tsoracle/pull/417))
- *(client)* defend epoch-less leader cache from backward flap ([#413](https://github.com/prisma-risk/tsoracle/pull/413))
- *(client)* floor the failed-attempt budget at the worklist size ([#404](https://github.com/prisma-risk/tsoracle/pull/404))

### Other

- *(client)* sweep the hint-channel cap only on insert, not every lookup ([#435](https://github.com/prisma-risk/tsoracle/pull/435))
- *(client)* split retry.rs god-module into retry/attempt/leader_hint ([#433](https://github.com/prisma-risk/tsoracle/pull/433))
- *(client)* rename leader_resolved module to channel_pool ([#434](https://github.com/prisma-risk/tsoracle/pull/434))
- cover remaining reachable branches (95.5% -> 95.8%) ([#418](https://github.com/prisma-risk/tsoracle/pull/418))
- raise library coverage to 95.5% with targeted tests ([#414](https://github.com/prisma-risk/tsoracle/pull/414))

## [0.2.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.2.2...tsoracle-client-v0.2.3) - 2026-05-25

### Added

- *(client)* expose Client::cached_leader() and drop the dead pool field ([#329](https://github.com/prisma-risk/tsoracle/pull/329))

### Fixed

- *(client)* rotate iter_round_robin worklist over configured endpoints ([#395](https://github.com/prisma-risk/tsoracle/pull/395))
- *(client)* bound leader-hint redirect chains with an absolute cap ([#386](https://github.com/prisma-risk/tsoracle/pull/386))
- *(client)* bound channel pool against wire-supplied leader-hint endpoints ([#383](https://github.com/prisma-risk/tsoracle/pull/383))
- *(client)* reject off-list epoch-less leader hint over a fresh known-epoch leader ([#381](https://github.com/prisma-risk/tsoracle/pull/381))
- *(client)* decompose retry loop and stop redirects consuming the attempt budget ([#376](https://github.com/prisma-risk/tsoracle/pull/376))
- *(client)* preserve Status metadata when fanning RPC errors to siblings ([#366](https://github.com/prisma-risk/tsoracle/pull/366))
- *(client)* make record_success epoch-monotone ([#362](https://github.com/prisma-risk/tsoracle/pull/362))
- *(client)* drop unused async-trait dependency ([#314](https://github.com/prisma-risk/tsoracle/pull/314))

### Other

- reflect paxos stress topology in README and stress-testing guide ([#388](https://github.com/prisma-risk/tsoracle/pull/388))
- *(client)* thread compact TimestampRange through decode and delivery ([#370](https://github.com/prisma-risk/tsoracle/pull/370))

## [0.2.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.2.1...tsoracle-client-v0.2.2) - 2026-05-24

### Fixed

- *(client)* distinguish fanned-out transport failures from NoReachableEndpoints ([#241](https://github.com/prisma-risk/tsoracle/pull/241)) ([#300](https://github.com/prisma-risk/tsoracle/pull/300))
- *(client)* evict cached channel on transport-class RPC failure ([#239](https://github.com/prisma-risk/tsoracle/pull/239)) ([#292](https://github.com/prisma-risk/tsoracle/pull/292))
- *(client)* evict failed-dial ChannelPool entries to bound the cache ([#290](https://github.com/prisma-risk/tsoracle/pull/290))
- *(client)* single-flight ChannelPool dials via per-endpoint OnceCell ([#286](https://github.com/prisma-risk/tsoracle/pull/286))

### Other

- lift the leader-hint trailer key and decoder into tsoracle-proto ([#91](https://github.com/prisma-risk/tsoracle/pull/91)) ([#295](https://github.com/prisma-risk/tsoracle/pull/295))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.2.0...tsoracle-client-v0.2.1) - 2026-05-24

### Added

- populate NOT_LEADER hints with leader endpoint and epoch (#88, #125) ([#234](https://github.com/prisma-risk/tsoracle/pull/234))

### Fixed

- *(client)* seat leader hints atomically under the monotone-forward check ([#240](https://github.com/prisma-risk/tsoracle/pull/240)) ([#274](https://github.com/prisma-risk/tsoracle/pull/274))
- *(proto)* bundle LeaderHint epoch into a single nested EpochWire ([#252](https://github.com/prisma-risk/tsoracle/pull/252)) ([#273](https://github.com/prisma-risk/tsoracle/pull/273))
- *(client)* bound the connect+RPC pair by one per-attempt deadline ([#238](https://github.com/prisma-risk/tsoracle/pull/238)) ([#271](https://github.com/prisma-risk/tsoracle/pull/271))
- *(client)* preserve status on stale-epoch hint so it surfaces NOT_LEADER ([#237](https://github.com/prisma-risk/tsoracle/pull/237)) ([#270](https://github.com/prisma-risk/tsoracle/pull/270))
- *(client)* stop clearing leader cache on unactionable NOT_LEADER ([#236](https://github.com/prisma-risk/tsoracle/pull/236)) ([#268](https://github.com/prisma-risk/tsoracle/pull/268))

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

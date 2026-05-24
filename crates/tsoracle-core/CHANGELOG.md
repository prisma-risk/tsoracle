# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-core-v0.2.0...tsoracle-core-v0.2.1) - 2026-05-24

### Fixed

- *(core)* return NotLeader from try_prepare_window_extension off-leader ([#251](https://github.com/prisma-risk/tsoracle/pull/251)) ([#280](https://github.com/prisma-risk/tsoracle/pull/280))
- *(core)* saturate SystemClock::now_ms instead of truncating ([#249](https://github.com/prisma-risk/tsoracle/pull/249)) ([#272](https://github.com/prisma-risk/tsoracle/pull/272))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-core-v0.1.4...tsoracle-core-v0.2.0) - 2026-05-23

### Fixed

- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))

### Other

- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- add READMEs for the remaining published crates ([#213](https://github.com/prisma-risk/tsoracle/pull/213))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-core-v0.1.2...tsoracle-core-v0.1.3) - 2026-05-22

### Added

- add opt-in bt cargo feature for error backtraces ([#120](https://github.com/prisma-risk/tsoracle/pull/120))
- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

### Other

- *(critical-path)* mark per-request and consensus files; drop shell lib.rs markers ([#113](https://github.com/prisma-risk/tsoracle/pull/113))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-core-v0.1.0...tsoracle-core-v0.1.1) - 2026-05-21

### Added

- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))
- *(hooks)* auto-install pre-commit hook via husky-rs ([#35](https://github.com/prisma-risk/tsoracle/pull/35))
- *(core)* drop panicking convenience wrappers from Allocator ([#34](https://github.com/prisma-risk/tsoracle/pull/34))
- *(perf)* add performance-critical-path guard ([#30](https://github.com/prisma-risk/tsoracle/pull/30))
- *(lints)* warn on unwrap/expect in non-test code ([#28](https://github.com/prisma-risk/tsoracle/pull/28))
- address final-review findings ([#21](https://github.com/prisma-risk/tsoracle/pull/21))
- correct contrib.rocks attribution in README.md
- add contributors section in README.md
- update badges in README.md

## [0.1.0] - 2026-05-21

Initial release.

# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v1.0.1...tsoracle-consensus-v2.0.0) - 2026-05-30

### Added

- keyed dense sequences on openraft + format-activation rollout gate ([#585](https://github.com/prisma-risk/tsoracle/pull/585))
- keyed dense sequence service (GetSeq) with file driver consensus support ([#579](https://github.com/prisma-risk/tsoracle/pull/579))

### Other

- *(consensus)* [**breaking**] typed AdvanceOutOfRange variant end-to-end ([#569](https://github.com/prisma-risk/tsoracle/pull/569))

## [1.0.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v1.0.0...tsoracle-consensus-v1.0.1) - 2026-05-27

### Other

- updated the following local packages: tsoracle-core

## [0.1.10](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.9...tsoracle-consensus-v0.1.10) - 2026-05-26

### Other

- updated the following local packages: tsoracle-core

## [0.1.9](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.8...tsoracle-consensus-v0.1.9) - 2026-05-26

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.1.8](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.7...tsoracle-consensus-v0.1.8) - 2026-05-25

### Fixed

- *(server)* enforce LeaderState::Follower driver contracts with a debug guard ([#439](https://github.com/prisma-risk/tsoracle/pull/439))
- *(server)* bound graceful shutdown so a hung driver call can't stall exit ([#420](https://github.com/prisma-risk/tsoracle/pull/420))

## [0.1.7](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.6...tsoracle-consensus-v0.1.7) - 2026-05-25

### Added

- *(consensus)* give the high-water advance rule a single home via AdvancePayload::merge ([#369](https://github.com/prisma-risk/tsoracle/pull/369))
- *(consensus)* unify HighWaterCommand advance naming across backends ([#323](https://github.com/prisma-risk/tsoracle/pull/323))

### Fixed

- *(consensus)* reject out-of-range high-water advance before persisting ([#360](https://github.com/prisma-risk/tsoracle/pull/360))

### Other

- *(consensus)* specify persist_high_water epoch-fencing contract ([#389](https://github.com/prisma-risk/tsoracle/pull/389))
- reflect paxos stress topology in README and stress-testing guide ([#388](https://github.com/prisma-risk/tsoracle/pull/388))
- *(consensus)* split lib.rs into per-concern modules ([#361](https://github.com/prisma-risk/tsoracle/pull/361))
- *(consensus)* make leadership_events first-item contract normative ([#254](https://github.com/prisma-risk/tsoracle/pull/254)) ([#321](https://github.com/prisma-risk/tsoracle/pull/321))

## [0.1.6](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.5...tsoracle-consensus-v0.1.6) - 2026-05-24

### Added

- populate NOT_LEADER hints with leader endpoint and epoch (#88, #125) ([#234](https://github.com/prisma-risk/tsoracle/pull/234))

## [0.1.5](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.4...tsoracle-consensus-v0.1.5) - 2026-05-23

### Other

- updated the following local packages: tsoracle-core

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.2...tsoracle-consensus-v0.1.3) - 2026-05-22

### Added

- add opt-in bt cargo feature for error backtraces ([#120](https://github.com/prisma-risk/tsoracle/pull/120))
- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-consensus-v0.1.0...tsoracle-consensus-v0.1.1) - 2026-05-21

### Added

- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))
- *(lints)* warn on unwrap/expect in non-test code ([#28](https://github.com/prisma-risk/tsoracle/pull/28))
- address final-review findings ([#21](https://github.com/prisma-risk/tsoracle/pull/21))
- correct contrib.rocks attribution in README.md
- add contributors section in README.md
- update badges in README.md

## [0.1.0] - 2026-05-21

Initial release.

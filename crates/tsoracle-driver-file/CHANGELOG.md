# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v1.0.1...tsoracle-driver-file-v2.0.0) - 2026-05-30

### Added

- keyed dense sequence service (GetSeq) with file driver consensus support ([#579](https://github.com/prisma-risk/tsoracle/pull/579))

### Other

- *(consensus)* [**breaking**] typed AdvanceOutOfRange variant end-to-end ([#569](https://github.com/prisma-risk/tsoracle/pull/569))

## [1.0.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v1.0.0...tsoracle-driver-file-v1.0.1) - 2026-05-27

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.1.10](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.9...tsoracle-driver-file-v0.1.10) - 2026-05-26

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.1.9](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.8...tsoracle-driver-file-v0.1.9) - 2026-05-26

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.1.8](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.7...tsoracle-driver-file-v0.1.8) - 2026-05-25

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.1.7](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.6...tsoracle-driver-file-v0.1.7) - 2026-05-25

### Added

- extract shared tsoracle-failpoint crate ([#306](https://github.com/prisma-risk/tsoracle/pull/306))

### Fixed

- *(deps)* remove unused production dependencies ([#378](https://github.com/prisma-risk/tsoracle/pull/378))
- *(consensus)* reject out-of-range high-water advance before persisting ([#360](https://github.com/prisma-risk/tsoracle/pull/360))

### Other

- reflect paxos stress topology in README and stress-testing guide ([#388](https://github.com/prisma-risk/tsoracle/pull/388))
- *(driver-file)* document leader_tx via #[expect] instead of #[allow(dead_code)] ([#294](https://github.com/prisma-risk/tsoracle/pull/294))

## [0.1.6](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.5...tsoracle-driver-file-v0.1.6) - 2026-05-24

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.1.5](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.4...tsoracle-driver-file-v0.1.5) - 2026-05-23

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus

## [0.1.4](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.3...tsoracle-driver-file-v0.1.4) - 2026-05-23

### Fixed

- *(driver-file)* durably flush rename metadata on Windows ([#154](https://github.com/prisma-risk/tsoracle/pull/154))

### Other

- update README.md badges ([#164](https://github.com/prisma-risk/tsoracle/pull/164))
- update README.md and add downloads badge ([#143](https://github.com/prisma-risk/tsoracle/pull/143))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.2...tsoracle-driver-file-v0.1.3) - 2026-05-22

### Added

- add opt-in bt cargo feature for error backtraces ([#120](https://github.com/prisma-risk/tsoracle/pull/120))
- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

### Fixed

- *(driver-file)* acquire exclusive flock to reject concurrent opens ([#130](https://github.com/prisma-risk/tsoracle/pull/130))

### Other

- *(critical-path)* mark per-request and consensus files; drop shell lib.rs markers ([#113](https://github.com/prisma-risk/tsoracle/pull/113))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-file-v0.1.0...tsoracle-driver-file-v0.1.1) - 2026-05-21

### Added

- add failpoint testing for driver-file and server paths ([#22](https://github.com/prisma-risk/tsoracle/pull/22))
- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))
- *(fuzz)* add coverage-guided fuzz testing ([#16](https://github.com/prisma-risk/tsoracle/pull/16))

### Fixed

- *(driver-file)* avoid unwraps when decoding records ([#7](https://github.com/prisma-risk/tsoracle/pull/7))

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))
- *(perf)* add performance-critical-path guard ([#30](https://github.com/prisma-risk/tsoracle/pull/30))
- *(lints)* warn on unwrap/expect in non-test code ([#28](https://github.com/prisma-risk/tsoracle/pull/28))
- address final-review findings ([#21](https://github.com/prisma-risk/tsoracle/pull/21))
- correct contrib.rocks attribution in README.md
- add contributors section in README.md
- update badges in README.md

## [0.1.0] - 2026-05-21

Initial release.

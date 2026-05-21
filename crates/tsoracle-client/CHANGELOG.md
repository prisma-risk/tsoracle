# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-client-v0.1.0...tsoracle-client-v0.1.1) - 2026-05-21

### Added

- add failpoint testing for driver-file and server paths ([#22](https://github.com/prisma-risk/tsoracle/pull/22))
- *(examples)* openraft-standalone + openraft-piggyback ([#20](https://github.com/prisma-risk/tsoracle/pull/20))

### Fixed

- *(tests)* fix flaky tests related to bind race ([#15](https://github.com/prisma-risk/tsoracle/pull/15))

### Other

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

# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.7](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.6...tsoracle-openraft-toolkit-v0.1.7) - 2026-05-24

### Added

- pin an on-disk schema version for snapshots, log entries, and meta records ([#291](https://github.com/prisma-risk/tsoracle/pull/291))

### Other

- *(openraft-toolkit)* collapse BootstrapMode::Join into a dedicated join() fn ([#298](https://github.com/prisma-risk/tsoracle/pull/298))
- *(openraft-toolkit)* document the log-store fsync policy at every write site ([#285](https://github.com/prisma-risk/tsoracle/pull/285))

## [0.1.6](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.5...tsoracle-openraft-toolkit-v0.1.6) - 2026-05-24

### Fixed

- *(openraft-toolkit)* fsync truncate_after and purge writes ([#260](https://github.com/prisma-risk/tsoracle/pull/260)) ([#279](https://github.com/prisma-risk/tsoracle/pull/279))

## [0.1.5](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.4...tsoracle-openraft-toolkit-v0.1.5) - 2026-05-23

### Added

- *(test-fakes)* implement transfer_leader RPC on MemNetwork ([#226](https://github.com/prisma-risk/tsoracle/pull/226))

### Other

- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- *(paxos)* per-crate READMEs + driver-choice comparison ([#208](https://github.com/prisma-risk/tsoracle/pull/208))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.2...tsoracle-openraft-toolkit-v0.1.3) - 2026-05-22

### Added

- tsoracle.rs marketing site ([#111](https://github.com/prisma-risk/tsoracle/pull/111))

### Fixed

- *(openraft-toolkit)* dedup leadership_events by full value ([#117](https://github.com/prisma-risk/tsoracle/pull/117))

### Other

- *(openraft-toolkit)* use parking_lot::RwLock in PartitionController ([#123](https://github.com/prisma-risk/tsoracle/pull/123))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.0...tsoracle-openraft-toolkit-v0.1.1) - 2026-05-21

### Other

- *(readme)* refresh feature highlights for current capabilities ([#49](https://github.com/prisma-risk/tsoracle/pull/49))
- pre-seed per-crate CHANGELOG.md files ([#45](https://github.com/prisma-risk/tsoracle/pull/45))

## [0.1.0] - 2026-05-21

Initial release.

# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.2.1...tsoracle-openraft-toolkit-v0.2.2) - 2026-05-26

### Added

- *(openraft-toolkit)* version-neutral e2e-max-readable-next feature ([#468](https://github.com/prisma-risk/tsoracle/pull/468))
- *(driver-openraft)* joiner gate and migration-on-next-write tests ([#465](https://github.com/prisma-risk/tsoracle/pull/465))
- *(openraft-toolkit)* add ActiveWriteVersion cell and version constants ([#460](https://github.com/prisma-risk/tsoracle/pull/460))
- *(openraft-toolkit)* route RocksdbLogStore log/meta codec through LogStoreCodec provider seam ([#459](https://github.com/prisma-risk/tsoracle/pull/459))

### Fixed

- *(openraft-toolkit)* clamp recovered write version to readable range ([#478](https://github.com/prisma-risk/tsoracle/pull/478))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.2.0...tsoracle-openraft-toolkit-v0.2.1) - 2026-05-26

### Added

- runtime dynamic membership (openraft) — admin trait, gRPC AdminService, tsoracle admin CLI ([#453](https://github.com/prisma-risk/tsoracle/pull/453))

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.8...tsoracle-openraft-toolkit-v0.2.0) - 2026-05-25

### Added

- *(codec)* lift codec_io_error into tsoracle-codec ([#431](https://github.com/prisma-risk/tsoracle/pull/431))
- *(driver-openraft)* [**breaking**] membership-driven peer addressing ([#408](https://github.com/prisma-risk/tsoracle/pull/408))

### Other

- cover remaining reachable branches (95.5% -> 95.8%) ([#418](https://github.com/prisma-risk/tsoracle/pull/418))

## [0.1.8](https://github.com/prisma-risk/tsoracle/compare/tsoracle-openraft-toolkit-v0.1.7...tsoracle-openraft-toolkit-v0.1.8) - 2026-05-25

### Added

- extract the version-prefixed postcard codec into a shared tsoracle-codec crate ([#324](https://github.com/prisma-risk/tsoracle/pull/324))
- *(openraft-toolkit)* promote leadership stream by-value entry to stable API ([#310](https://github.com/prisma-risk/tsoracle/pull/310))
- extract shared tsoracle-failpoint crate ([#306](https://github.com/prisma-risk/tsoracle/pull/306))

### Fixed

- *(openraft-toolkit)* version-frame the log-store meta column ([#331](https://github.com/prisma-risk/tsoracle/pull/331)) ([#390](https://github.com/prisma-risk/tsoracle/pull/390))
- *(deps)* remove unused production dependencies ([#378](https://github.com/prisma-risk/tsoracle/pull/378))

### Other

- reflect paxos stress topology in README and stress-testing guide ([#388](https://github.com/prisma-risk/tsoracle/pull/388))
- *(openraft-toolkit)* range-delete truncate/purge instead of per-key loop ([#373](https://github.com/prisma-risk/tsoracle/pull/373))
- make the remaining real-time consensus-harness tests deterministic ([#326](https://github.com/prisma-risk/tsoracle/pull/326))
- *(openraft-toolkit)* remove unused MetaLabel::LastMembership ([#311](https://github.com/prisma-risk/tsoracle/pull/311))

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

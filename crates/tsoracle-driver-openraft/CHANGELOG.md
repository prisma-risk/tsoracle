# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.3.0...tsoracle-driver-openraft-v0.3.1) - 2026-05-26

### Added

- runtime dynamic membership (openraft) — admin trait, gRPC AdminService, tsoracle admin CLI ([#453](https://github.com/prisma-risk/tsoracle/pull/453))

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.3.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.2.3...tsoracle-driver-openraft-v0.3.0) - 2026-05-25

### Added

- *(codec)* lift codec_io_error into tsoracle-codec ([#431](https://github.com/prisma-risk/tsoracle/pull/431))
- *(driver-openraft)* [**breaking**] membership-driven peer addressing ([#408](https://github.com/prisma-risk/tsoracle/pull/408))

### Fixed

- *(server)* enforce LeaderState::Follower driver contracts with a debug guard ([#439](https://github.com/prisma-risk/tsoracle/pull/439))
- *(driver-openraft)* make snapshot publish monotone to close build/install TOCTOU ([#426](https://github.com/prisma-risk/tsoracle/pull/426))

### Other

- *(fuzz)* decode the openraft meta corpus through the version frame ([#415](https://github.com/prisma-risk/tsoracle/pull/415))

## [0.2.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.2.2...tsoracle-driver-openraft-v0.2.3) - 2026-05-25

### Added

- *(consensus)* give the high-water advance rule a single home via AdvancePayload::merge ([#369](https://github.com/prisma-risk/tsoracle/pull/369))
- *(core)* lift a shared TsoPeer type into tsoracle-core ([#266](https://github.com/prisma-risk/tsoracle/pull/266)) ([#325](https://github.com/prisma-risk/tsoracle/pull/325))
- *(consensus)* unify HighWaterCommand advance naming across backends ([#323](https://github.com/prisma-risk/tsoracle/pull/323))
- *(openraft-toolkit)* promote leadership stream by-value entry to stable API ([#310](https://github.com/prisma-risk/tsoracle/pull/310))

### Fixed

- *(openraft-toolkit)* version-frame the log-store meta column ([#331](https://github.com/prisma-risk/tsoracle/pull/331)) ([#390](https://github.com/prisma-risk/tsoracle/pull/390))
- *(deps)* remove unused production dependencies ([#378](https://github.com/prisma-risk/tsoracle/pull/378))
- *(consensus)* reject out-of-range high-water advance before persisting ([#360](https://github.com/prisma-risk/tsoracle/pull/360))

### Other

- *(fuzz)* fuzz the openraft meta-column bare-postcard decode ([#372](https://github.com/prisma-risk/tsoracle/pull/372))
- *(fuzz)* fuzz the full openraft Entry log record ([#368](https://github.com/prisma-risk/tsoracle/pull/368))
- *(driver-openraft)* run the harness tests under tokio virtual time ([#319](https://github.com/prisma-risk/tsoracle/pull/319))

## [0.2.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.2.1...tsoracle-driver-openraft-v0.2.2) - 2026-05-24

### Added

- pin an on-disk schema version for snapshots, log entries, and meta records ([#291](https://github.com/prisma-risk/tsoracle/pull/291))

### Other

- *(driver-openraft)* narrow OpenraftHighWaterHost to a metrics-only accessor ([#95](https://github.com/prisma-risk/tsoracle/pull/95)) ([#296](https://github.com/prisma-risk/tsoracle/pull/296))

## [0.2.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.2.0...tsoracle-driver-openraft-v0.2.1) - 2026-05-24

### Added

- populate NOT_LEADER hints with leader endpoint and epoch (#88, #125) ([#234](https://github.com/prisma-risk/tsoracle/pull/234))

### Fixed

- *(driver-openraft)* validate snapshot meta.last_log_id against payload.last_applied ([#276](https://github.com/prisma-risk/tsoracle/pull/276))

## [0.2.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.1.4...tsoracle-driver-openraft-v0.2.0) - 2026-05-23

### Fixed

- *(core)* [**breaking**] widen Epoch to u128 for lossless leader-epoch encoding ([#221](https://github.com/prisma-risk/tsoracle/pull/221))

### Other

- *(release)* version crates independently to fix release-plz resolution ([#223](https://github.com/prisma-risk/tsoracle/pull/223))
- add READMEs for the remaining published crates ([#213](https://github.com/prisma-risk/tsoracle/pull/213))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.1.2...tsoracle-driver-openraft-v0.1.3) - 2026-05-22

### Fixed

- *(driver-openraft)* map RaftError::Fatal to PermanentDriver ([#112](https://github.com/prisma-risk/tsoracle/pull/112))

### Other

- *(critical-path)* mark per-request and consensus files; drop shell lib.rs markers ([#113](https://github.com/prisma-risk/tsoracle/pull/113))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-driver-openraft-v0.1.0...tsoracle-driver-openraft-v0.1.1) - 2026-05-21

### Other

- release v0.1.1 ([#51](https://github.com/prisma-risk/tsoracle/pull/51))

## [0.1.0] - 2026-05-21

Initial release.

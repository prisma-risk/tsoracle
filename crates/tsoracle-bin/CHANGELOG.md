# Changelog

All notable changes to this crate are documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/spec/v1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v2.0.0...tsoracle-v2.0.1) - 2026-05-30

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus, tsoracle-server, tsoracle-driver-file, tsoracle-client, tsoracle-standalone

## [2.0.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v1.0.0...tsoracle-v2.0.0) - 2026-05-27

### Added

- *(server)* periodic heartbeat log + typed Reporter metric facade ([#567](https://github.com/prisma-risk/tsoracle/pull/567))
- peer-listener secure-by-default guard + chart pass-through (closes #481) ([#539](https://github.com/prisma-risk/tsoracle/pull/539))

### Other

- [**breaking**] relocate Bt to tsoracle-server, prune vestigial features ([#558](https://github.com/prisma-risk/tsoracle/pull/558))

## [0.1.14](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.13...tsoracle-v0.1.14) - 2026-05-26

### Other

- updated the following local packages: tsoracle-core, tsoracle-server, tsoracle-client, tsoracle-standalone, tsoracle-consensus, tsoracle-driver-file

## [0.1.13](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.12...tsoracle-v0.1.13) - 2026-05-26

### Fixed

- *(standalone)* require mTLS for non-loopback admin gRPC bind ([#462](https://github.com/prisma-risk/tsoracle/pull/462))

### Other

- *(smoke)* retry binary spawn on EADDRINUSE port-probe race ([#474](https://github.com/prisma-risk/tsoracle/pull/474))

## [0.1.12](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.11...tsoracle-v0.1.12) - 2026-05-26

### Added

- runtime dynamic membership (openraft) — admin trait, gRPC AdminService, tsoracle admin CLI ([#453](https://github.com/prisma-risk/tsoracle/pull/453))

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.1.11](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.10...tsoracle-v0.1.11) - 2026-05-25

### Added

- *(standalone)* TLS/mTLS for peer transport + client API ([#445](https://github.com/prisma-risk/tsoracle/pull/445))
- *(standalone)* multi-driver tsoracle-standalone crate + serve file|openraft|paxos CLI ([#438](https://github.com/prisma-risk/tsoracle/pull/438))
- *(server)* add public shutdown_signal() and wire the cluster examples to it ([#406](https://github.com/prisma-risk/tsoracle/pull/406))

## [0.1.10](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.9...tsoracle-v0.1.10) - 2026-05-25

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus, tsoracle-server, tsoracle-client, tsoracle-driver-file

## [0.1.9](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.8...tsoracle-v0.1.9) - 2026-05-24

### Other

- updated the following local packages: tsoracle-server

## [0.1.8](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.7...tsoracle-v0.1.8) - 2026-05-24

### Other

- updated the following local packages: tsoracle-server, tsoracle-client

## [0.1.7](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.6...tsoracle-v0.1.7) - 2026-05-24

### Fixed

- *(bin)* handle SIGTERM as a graceful-shutdown trigger ([#245](https://github.com/prisma-risk/tsoracle/pull/245)) ([#269](https://github.com/prisma-risk/tsoracle/pull/269))

## [0.1.6](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.5...tsoracle-v0.1.6) - 2026-05-23

### Other

- updated the following local packages: tsoracle-server

## [0.1.5](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.4...tsoracle-v0.1.5) - 2026-05-23

### Other

- updated the following local packages: tsoracle-core, tsoracle-server, tsoracle-client, tsoracle-consensus, tsoracle-driver-file

## [0.1.4](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.3...tsoracle-v0.1.4) - 2026-05-23

### Other

- updated the following local packages: tsoracle-driver-file

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.2...tsoracle-v0.1.3) - 2026-05-22

### Added

- add opt-in bt cargo feature for error backtraces ([#120](https://github.com/prisma-risk/tsoracle/pull/120))

## [0.1.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-v0.1.1...tsoracle-v0.1.2) - 2026-05-22

### Other

- updated the following local packages: tsoracle-server, tsoracle-client

## [0.1.0] - 2026-05-21

Initial release.

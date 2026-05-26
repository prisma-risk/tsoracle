# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v0.1.1...tsoracle-standalone-v0.1.2) - 2026-05-26

### Added

- *(openraft-toolkit)* version-neutral e2e-max-readable-next feature ([#468](https://github.com/prisma-risk/tsoracle/pull/468))
- *(driver-openraft)* add format-activation gate and Capabilities RPC ([#463](https://github.com/prisma-risk/tsoracle/pull/463))
- *(standalone)* frame openraft peer RPC bodies with format_version ([#461](https://github.com/prisma-risk/tsoracle/pull/461))
- *(openraft-toolkit)* add ActiveWriteVersion cell and version constants ([#460](https://github.com/prisma-risk/tsoracle/pull/460))
- *(openraft-toolkit)* route RocksdbLogStore log/meta codec through LogStoreCodec provider seam ([#459](https://github.com/prisma-risk/tsoracle/pull/459))

### Fixed

- *(standalone)* require mTLS for non-loopback admin gRPC bind ([#462](https://github.com/prisma-risk/tsoracle/pull/462))

## [0.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v0.1.0...tsoracle-standalone-v0.1.1) - 2026-05-26

### Added

- runtime dynamic membership (openraft) — admin trait, gRPC AdminService, tsoracle admin CLI ([#453](https://github.com/prisma-risk/tsoracle/pull/453))

### Fixed

- ship per-crate READMEs to crates.io ([#451](https://github.com/prisma-risk/tsoracle/pull/451))
- *(standalone)* bound openraft peer unary RPCs with the RPCOption deadline ([#443](https://github.com/prisma-risk/tsoracle/pull/443))

### Other

- expand copyright header to full Apache 2.0 block and share it via scripts/header.txt ([#449](https://github.com/prisma-risk/tsoracle/pull/449))

## [0.1.0](https://github.com/prisma-risk/tsoracle/releases/tag/tsoracle-standalone-v0.1.0) - 2026-05-25

### Added

- *(standalone)* TLS/mTLS for peer transport + client API ([#445](https://github.com/prisma-risk/tsoracle/pull/445))
- *(standalone)* multi-driver tsoracle-standalone crate + serve file|openraft|paxos CLI ([#438](https://github.com/prisma-risk/tsoracle/pull/438))

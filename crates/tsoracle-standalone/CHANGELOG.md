# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v1.1.2...tsoracle-standalone-v1.1.3) - 2026-05-31

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus, tsoracle-server, tsoracle-driver-file, tsoracle-openraft-toolkit, tsoracle-driver-openraft, tsoracle-paxos-toolkit, tsoracle-driver-paxos

## [1.1.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v1.1.1...tsoracle-standalone-v1.1.2) - 2026-05-30

### Other

- updated the following local packages: tsoracle-core, tsoracle-driver-openraft, tsoracle-consensus, tsoracle-server, tsoracle-driver-file, tsoracle-paxos-toolkit, tsoracle-driver-paxos

## [1.1.1](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v1.1.0...tsoracle-standalone-v1.1.1) - 2026-05-30

### Other

- updated the following local packages: tsoracle-core, tsoracle-consensus, tsoracle-server, tsoracle-driver-file, tsoracle-openraft-toolkit, tsoracle-driver-openraft, tsoracle-driver-paxos, tsoracle-paxos-toolkit

## [1.1.0](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v1.0.0...tsoracle-standalone-v1.1.0) - 2026-05-27

### Added

- peer-listener secure-by-default guard + chart pass-through (closes #481) ([#539](https://github.com/prisma-risk/tsoracle/pull/539))

### Other

- *(core)* PeerEndpoint newtype hoists scheme-less contract to type system ([#560](https://github.com/prisma-risk/tsoracle/pull/560))
- *(standalone)* eliminate close/rebind port-bind race in openraft_membership tests ([#530](https://github.com/prisma-risk/tsoracle/pull/530))

## [0.1.3](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v0.1.2...tsoracle-standalone-v0.1.3) - 2026-05-26

### Fixed

- *(standalone/tests)* hold listener until bind to close port-lease TOCTOU ([#490](https://github.com/prisma-risk/tsoracle/pull/490))

### Other

- *(proto)* expand and correct service/RPC/field comments ([#492](https://github.com/prisma-risk/tsoracle/pull/492))

## [0.1.2](https://github.com/prisma-risk/tsoracle/compare/tsoracle-standalone-v0.1.1...tsoracle-standalone-v0.1.2) - 2026-05-26

### Added

- *(openraft-toolkit)* version-neutral e2e-max-readable-next feature ([#468](https://github.com/prisma-risk/tsoracle/pull/468))
- *(driver-openraft)* add format-activation gate and Capabilities RPC ([#463](https://github.com/prisma-risk/tsoracle/pull/463))
- *(standalone)* frame openraft peer RPC bodies with format_version ([#461](https://github.com/prisma-risk/tsoracle/pull/461))
- *(openraft-toolkit)* add ActiveWriteVersion cell and version constants ([#460](https://github.com/prisma-risk/tsoracle/pull/460))
- *(openraft-toolkit)* route RocksdbLogStore log/meta codec through LogStoreCodec provider seam ([#459](https://github.com/prisma-risk/tsoracle/pull/459))

### Fixed

- *(openraft-toolkit)* clamp recovered write version to readable range ([#478](https://github.com/prisma-risk/tsoracle/pull/478))
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

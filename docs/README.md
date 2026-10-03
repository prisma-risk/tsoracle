# tsoracle documentation

The long-form prose guide to tsoracle — strictly monotonic timestamps over gRPC.

This directory is the **deep dive** covering everything from getting started through the allocator's monotonicity proof, consensus integration patterns, deployment topologies, and per-example walkthroughs. Browsable on GitHub and indexed by [DeepWiki](https://deepwiki.com/prisma-risk/tsoracle).

For the **reference documentation of the external interface** — the gRPC wire surface, the CLI, and the list of published Rust crates — see [Interface Reference](interface-reference.md). The full **Rust API reference** is generated on [docs.rs/tsoracle-server](https://docs.rs/tsoracle-server) (and the other crates linked from the Interface Reference). A docs.rs-rendered **subset of the most critical chapters** (algorithm, `ConsensusDriver` contract, operations) lives inside the published crates — see [CONTRIBUTING.md](../CONTRIBUTING.md#documentation) for the policy on which tree owns what.

## Chapters

- **[Overview](overview.md)** — what tsoracle is, what it is not, where to go next.
- **[Getting Started](getting-started.md)** — vocabulary, install & run, calling from Rust, embedding, migration.
- **[Architecture Deep Dive](architecture-deep-dive.md)** — crate layering, the clock contract, the epoch type, timestamp packing.
- **[The Allocator](the-allocator.md)** — allocation model, prepare-commit split, monotonic persistence, monotonicity proof.
- **[Key Subsystems](key-subsystems.md)** — leader-watch pipeline, failover fence, leader-hint trailer, steady-state window extension.
- **[Client API and Usage](client-api-and-usage.md)** — `Client` type, `GetTs`/`GetTsBatch`, leader discovery, configuration.
- **[The Client Driver](the-client-driver.md)** — coalescing vs. pre-fetching, external monotonicity across clients, auto-batching dynamics, `flush_interval` correctly understood.
- **[Consensus Integration](consensus-integration.md)** — the `ConsensusDriver` trait, per-method recipes, worked openraft example, single-leader requirement.
- **[Driver Comparison](driver-comparison.md)** — capability matrix and per-feature deep dive across `file`, `openraft`, and `paxos`; operator decision guidance and contributor-facing internals.
- **[Operations](operations.md)** — sizing `window_ahead`/`failover_advance`, monitoring hooks, deployment topologies, client retry behavior.
- **[Deployment](deployment.md)** — container images (fat vs lean, multi-arch), Helm chart quick start, values reference, TLS/mTLS setup, and topology notes (file vs openraft vs paxos).
- **[Testing and Examples](testing-and-examples.md)** — walkthroughs of the runnable example crates plus the workspace testing strategy.
- **[Failpoint Testing](failpoint-testing.md)** — fault-injection points for crash-recovery, fence, and service-path tests; the feature-gating model and contributor guidance.
- **[Yield-point Testing](yieldpoint-testing.md)** — async counterpart of failpoints, for tests that need to park production code in an async path without blocking a tokio worker.
- **[Interface Reference](interface-reference.md)** — reference documentation for the project's external interfaces: the `tsoracle.v1.TsoService` gRPC wire surface, the `tsoracle.admin.v1.MembershipAdmin` admin surface, the `tsoracle` CLI, and the published Rust crates.

## Where to start

- New to TSOs or to tsoracle — [Overview](overview.md), then [Getting Started](getting-started.md).
- Reading the code — pair each crate's `lib.rs` `//!` header with the corresponding section of [Architecture Deep Dive](architecture-deep-dive.md).
- Plugging tsoracle into your own consensus — [Consensus Integration](consensus-integration.md).
- Running it in production — [Operations](operations.md).
- Looking up a specific RPC, CLI flag, or published crate — [Interface Reference](interface-reference.md).

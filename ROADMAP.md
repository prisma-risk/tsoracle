<!-- SPDX-License-Identifier: Apache-2.0 -->
# Roadmap

Last updated: 2026-05-29.

This roadmap captures direction at three horizons. Specific issues track in <https://github.com/prisma-risk/tsoracle/issues>; this document is the narrative arc.

## Near term (0–3 months)

### OpenSSF Best Practices — closing Silver fixable gaps

- Land governance documentation (this `ROADMAP.md`, [`GOVERNANCE.md`](GOVERNANCE.md), [`docs/assurance-case.md`](docs/assurance-case.md)).
- Enable DCO sign-off and refresh CONTRIBUTING.md.
- Sign release tags via Sigstore gitsign (keyless), matching the existing SLSA provenance signing model.
- Triage existing issues for `good first issue` / `help wanted` labels.
- Refresh [`.bestpractices.json`](.bestpractices.json) to flip 9 criteria to Met and refine the remaining 3 organizational criteria with concrete action plans.

### Stability and operations

- Transport-crate extraction: carve per-driver transport out of `tsoracle-standalone` into `tsoracle-transport-openraft` and `tsoracle-transport-paxos`.
- Wire the mixed-version-soak Job into the kube-e2e orchestration so format-evolution soak runs alongside the existing acceptance lane.
- Peer-listener secure-by-default in the binary layer (issue [#481](https://github.com/prisma-risk/tsoracle/issues/481)) — mirror the Helm chart's `tls.allowInsecurePeer` guard at the openraft `raft_addr` and paxos `peer_listen` call sites in the binary.

### Contributor onboarding

- Triage existing issues for the `good first issue` / `help wanted` labels.
- Publish this roadmap on the documentation site (`tsoracle.rs`).

## Mid term (3–12 months)

### OpenSSF Best Practices — Gold

- Branch coverage measurement (`cargo-llvm-cov --branch`) and an ≥80% branch coverage CI gate.
- Reproducible-build CI lane (diffoscope-driven), gated against Rust toolchain reproducibility maturity upstream.
- Third-party security review ahead of 1.0.

### Project maturity

- ~~Designate a second active maintainer (not just continuity contact) and update [`GOVERNANCE.md`](GOVERNANCE.md) §6.1 accordingly.~~ **Done** — Charles Merill and Idriss Maoui are active co-maintainers ([`GOVERNANCE.md`](GOVERNANCE.md) §3.1); `bus_factor` is now Met.
- ~~Enable branch protection so changes require review.~~ **Done** — `main` requires at least one approving review from a non-author and blocks self-merge; with multiple active maintainers, two-person review is enforced for all changes.
- Continue growing the contributor base; aim for the `contributors_unassociated` criterion.

### Consensus and storage

- File-driver hardening: more failpoint coverage, durability invariants, snapshot-publish correctness audit.
- Continued zero-downtime format evolution under the framework shipped in PRs #454–#479.
- Dense-sequence (`GetSeq`) support for the OmniPaxos driver — bring `paxos` to parity with `file` and `openraft`, which already serve gapless sequences. Paxos returns `UNIMPLEMENTED` today via the inherited `DenseUnsupported` default; adding it safely requires porting the per-version codec discipline and an all-members activation gate to paxos.

## Long term (12+ months)

- Additional consensus backends (currently file, openraft, paxos — additional backends evaluated on demand).
- Public conformance suite that downstream operators can run against any tsoracle deployment.

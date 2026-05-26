<!-- SPDX-License-Identifier: Apache-2.0 -->
# Governance

This document describes how the tsoracle project is governed. It is the canonical reference for project roles, decision-making, and continuity of access. It supplements (and does not replace) [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md), [`CONTRIBUTING.md`](CONTRIBUTING.md), and [`SECURITY.md`](SECURITY.md).

## 1. Mission

tsoracle is an Apache-2.0 timestamp oracle providing strictly monotonic, linearizable timestamps backed by pluggable consensus drivers (openraft, paxos). See [`README.md`](README.md) for the full value proposition.

## 2. Project structure

- **Source repository**: <https://github.com/prisma-risk/tsoracle> — monorepo, Cargo workspace.
- **Crate registry**: each workspace crate publishes independently to <https://crates.io>.
- **Container registry**: images are published to `ghcr.io/prisma-risk/tsoracle*`.
- **Documentation site**: <https://www.tsoracle.rs> (deployed by the `pages.yml` workflow on every push to `main`).

## 3. Roles and responsibilities

### 3.1 Maintainer

The maintainer holds final design, merge, and release authority.

- Currently: **Sebastian Thiebaud** (`@sebastianthiebaud`, <sebastian@prismarisk.com>).
- Authorities and responsibilities:
  - Final design and merge authority on all PRs.
  - `crates.io` release authority (per workspace crate).
  - `ghcr.io` image release authority.
  - Security advisory authority (CVE coordination via the GitHub CNA, per [`SECURITY.md`](SECURITY.md)).
  - Code-of-Conduct enforcement, except where the maintainer is involved in the report — see §6.

### 3.2 Reviewer

Anyone listed in [`.github/CODEOWNERS`](.github/CODEOWNERS). The maintainer is currently the only code owner. As outside contributors land sustained substantive PRs, the maintainer will invite them to `CODEOWNERS` for the area they contribute to.

Reviewer authority: approve PRs, request changes, request additional review.

### 3.3 Contributor

Anyone opening a PR. Contributor responsibilities:

- Follow [`CONTRIBUTING.md`](CONTRIBUTING.md).
- Sign off on commits per the project's Developer Certificate of Origin (see CONTRIBUTING.md "Developer Certificate of Origin" section).
- Accept Apache-2.0 inbound = outbound licensing.

## 4. Decision-making

### 4.1 Day-to-day

PRs are decided by the maintainer with CI required-checks gating merge. Substantive design changes (new crate, protocol change, breaking change) should be initiated by opening an issue at <https://github.com/prisma-risk/tsoracle/issues> to discuss the approach **before** submitting a pull request, so the design conversation happens up front rather than as back-and-forth on a PR whose implementation is already done.

### 4.2 Contentious decisions

Lazy consensus on the issue or PR thread. If consensus cannot be reached, the maintainer breaks the tie and records the rationale on the thread.

### 4.3 Security decisions

Follow [`SECURITY.md`](SECURITY.md): 24h acknowledgment, 72h triage, ≤30d coordinated disclosure, CVE via the GitHub CNA when impact warrants. The maintainer is the single point of decision; the secondary contact (§6) is informed.

### 4.4 Releases

Releases are automated via release-plz on every `feat:`/`fix:`/`perf:` merge to `main`. The maintainer is the trust root for the `crates.io` owner and `ghcr.io` push credentials. Release tags are annotated and signed via Sigstore gitsign (keyless, transparency-log anchored); see [`docs/release-signatures.md`](docs/release-signatures.md).

## 5. Becoming a maintainer

Path: sustained substantive contributions (typically ≥6 months, ≥10 non-trivial PRs in code, docs, or test infrastructure) plus alignment with project direction. The current maintainer invites by issue or PR discussion. Once designated, the new maintainer is added to:

- [`.github/CODEOWNERS`](.github/CODEOWNERS) for the relevant areas (or `*` for full).
- The GitHub repository admin team.
- `crates.io` owners for the crates they will publish.
- The continuity-of-access section (§6) below.

## 6. Continuity of access

This section is the canonical list of credential surfaces. Each row names who holds it today and who can recover it if the holder becomes unavailable.

| Credential surface | Holder | Backup / recovery path |
| --- | --- | --- |
| `github.com/prisma-risk` organization admin | Sebastian Thiebaud (<sebastian@prismarisk.com>) | Charles Merill (<charles@prismarisk.com>) |
| `crates.io` owners — `tsoracle` and all workspace crates | Sebastian Thiebaud | Owner-add by the holder; team-owner addition planned. Recovery via `crates.io` support if the holder is unreachable. |
| `ghcr.io/prisma-risk` package maintainers | Members of the `prisma-risk` org with `write` permission | GitHub org admin can add members. |
| Sigstore signing identity (release SLSA provenance and tag signing) | Per-workflow OIDC token (no long-lived key) | No recovery needed — keyless. |
| GitHub Pages deployment / DNS for `tsoracle.rs` | GitHub Pages + the DNS provider account | DNS recovery via the domain registrar (account held by maintainer). |

### 6.1 Secondary contact

**Charles Merill** (<charles@prismarisk.com>) is the designated secondary contact. The secondary contact:

- Holds read-only access to the credential inventory above (the specific mechanism — shared password manager vault — is recorded outside the repository).
- Acts as the Code-of-Conduct escalation point for conflicts involving the maintainer.
- Is not currently an active maintainer (does not review PRs or cut releases). The OpenSSF `bus_factor` criterion is about active maintainership, not access continuity, and is therefore not considered met by this designation alone.

### 6.2 If the maintainer becomes unavailable

1. The secondary contact (§6.1) opens an issue at <https://github.com/prisma-risk/tsoracle/issues> titled "Maintainer transition" referencing this document.
2. GitHub Support is asked to transfer organization admin per the inactive-account-recovery policy (<https://docs.github.com/en/site-policy/other-site-policies/github-inactive-account-policy>) if the maintainer is unreachable for more than 90 days.
3. `crates.io` owners are added via the existing owner's `cargo owner --add`, or via `crates.io` support if no existing owner can act.
4. `ghcr.io` packages are migrated via GitHub Container Registry org admin.
5. A new release with the transition announcement is published; the project README and SECURITY.md are updated with the new primary contact.

## 7. Document maintenance

- Roles and continuity details are reviewed at every major release (≥0.X feature releases) and when any credential surface changes.
- Changes to this document are made via PR like any other code change.

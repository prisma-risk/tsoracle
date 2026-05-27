# Security Policy

Thank you for helping keep `tsoracle` and its users safe. We treat security reports as high-priority work and ask that you follow the disclosure process below.

## Supported versions

`tsoracle` is pre-1.0 software. Only the latest `0.x` release line receives security patches. Users on older `0.x` lines should upgrade to the current line before reporting an issue.

| Version       | Supported          |
| ------------- | ------------------ |
| latest `0.x`  | :white_check_mark: |
| older `0.x`   | :x:                |

## Reporting a vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.** Public issues are indexed immediately and put users at risk before a fix is available.

Instead, open a private draft security advisory: https://github.com/prisma-risk/tsoracle/security/advisories/new

A useful report includes:

- The affected version(s) (commit SHA preferred, or a release tag).
- A clear description of the issue and its security impact (confidentiality, integrity, availability, scope).
- A minimal reproduction: code snippet, command, or a step-by-step procedure. PoC exploit code is welcome but not required.
- Your assessment of severity and any known workarounds.

If you require an encrypted channel before filing the advisory, note that in a follow-up comment after the advisory is created; we will coordinate from there.

## Our commitments

- **Acknowledgment:** within 24 hours of the advisory being filed.
- **Initial triage:** within 72 hours, including a severity estimate and a preliminary remediation plan.
- **Coordinated disclosure:** within 30 days from the report, or on release of a fix, whichever is sooner. Reporters are credited in the advisory unless they request anonymity.

If a fix requires more time (for example, a protocol-level change that needs a coordinated rolling upgrade), we will say so explicitly in the advisory and propose a revised timeline.

## Scope

In scope:

- The `tsoracle` server binary and the Rust client crate.
- Toolkit and driver crates published from this repository.
- The Helm chart and container images published to `ghcr.io/prisma-risk/tsoracle-*` and `ghcr.io/prisma-risk/charts`.

Out of scope:

- Issues in third-party dependencies that are tracked upstream. Report those to the upstream project; we will track the advisory and bump our pin once a fix is released.
- Vulnerabilities that require a pre-compromised host, root on the same machine as the server, or physical access.
- Denial-of-service vectors that require unbounded resources beyond what a well-provisioned operator would allow.

## Supply chain integrity

Releases ship with [SLSA v1.0 build provenance](docs/release-signatures.md) signed transparently through Sigstore (Fulcio + Rekor). The `.intoto.jsonl` attestation attached to each GitHub Release proves that the `.crate` came from this repository's CI, at the commit pointed at by the release tag, on GitHub-hosted runners.

Every GitHub Action and reusable workflow in `.github/workflows/` is pinned to a full commit SHA, with the human-readable version in a trailing comment — with one documented exception.

### Documented exception: `slsa-framework/slsa-github-generator`

The SLSA generic generator's reusable workflow (referenced from `.github/workflows/release-plz.yml`) is referenced by tag (`@v2.1.0`) rather than by commit SHA. This is intentional and unavoidable:

- The generator's `generate-builder.sh` script validates the calling ref against `refs/tags/vX.Y.Z` and exits 2 with `Invalid ref: <sha>. Expected ref of the form refs/tags/vX.Y.Z` when SHA-pinned. The builder binary is fetched from the GitHub release matching the tag — there is no equivalent operation keyed on a commit SHA.
- Upstream tracks this constraint in [slsa-github-generator#722](https://github.com/slsa-framework/slsa-github-generator/issues/722) and [slsa-verifier#12](https://github.com/slsa-framework/slsa-verifier/issues/12); the [SLSA README](https://github.com/slsa-framework/slsa-github-generator#referencing-slsa-builders-and-generators) acknowledges this departs from GitHub's general best practice.
- OpenSSF Scorecard's `Pinned-Dependencies` check ([#2174](https://github.com/ossf/scorecard/issues/2174), closed-completed) flags this single line. There is no in-line suppression mechanism; a policy-file allowlist is tracked as [scorecard#1406](https://github.com/ossf/scorecard/issues/1406).

We accept the resulting ~1-point deduction on Pinned-Dependencies. The Scorecard project itself uses the same tag-ref pattern in its own [`goreleaser.yaml`](https://github.com/ossf/scorecard/blob/main/.github/workflows/goreleaser.yaml) for the same reason.

Integrity for the SLSA generator is anchored differently from typical actions: the SHA-256 of the generator binary is hardcoded into the reusable workflow at each tagged release, so the tag itself is the security boundary. Dependabot is configured to bump the tag when a new SLSA generator release lands.

## Public disclosure

Once a fix is released, we will publish the advisory with credit to the reporter and a CVE identifier (requested via GitHub's CNA when the impact warrants one).

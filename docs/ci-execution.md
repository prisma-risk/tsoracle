# Reuse green PR checks after merge

**Date:** 2026-10-08
**Status:** Proposed

**Decision:** Main pushes to `ci.yml` reuse the latest successful run of the same workflow for the merged PR's exact head, branch and same-repository origin. A small read-only policy job records the PR source and run URL and skips repeated validation. Daily and manual runs execute full source checks. Merged fork PRs validate normally because their privileged PR proof jobs can be skipped. Missing, failed, pending, cancelled or unreadable evidence keeps validation enabled.

**Reasoning:** These workflows repeat checks already performed before merge. Their policy shares `.github/scripts/ci-execution-policy.cjs` with primary CI, so API failures and workflow identity have one implementation. Repository policy loads only from the main-push checkout; PRs request validation without loading PR-controlled policy source.

**Alternatives considered:** Removing main-push triggers loses the fallback for direct pushes and unavailable PR evidence. Repeating every suite immediately after a green merge duplicates the PR work. Independent inline policies can drift. Requiring identical PR and merge trees repeats source checks for unrelated concurrent changes.

**Consequences:** PR validation and path scopes remain in place. Daily and manual runs validate the merged branch even when the PR was green. A reused receipt identifies tested PR source, not newly executed checks on the merged SHA; exact merged-source acceptance still requires an executed daily or manual run on that SHA. Publication and deployment retain their independent evidence requirements.

**Testing decisions:** Node policy tests cover event, workflow, PR, head and branch identity, latest-run selection, cleared GitHub associations and unavailable evidence. Workflow boundary tests execute actual wrapper scripts and job conditions, checking trusted policy loading, skipped duplicates, validation after policy failure, daily/manual triggers and independent jobs retained outside the reuse gate. Primary CI runs both suites with Node 24.

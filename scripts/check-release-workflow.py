#!/usr/bin/env python3
"""Pin the trusted automatic release trigger and manual release-PR lane."""

import sys
from pathlib import Path


DEFAULT_WORKFLOW = Path(__file__).parents[1] / ".github/workflows/release-plz.yml"


def require(haystack: str, needle: str, description: str) -> None:
    if needle not in haystack:
        raise SystemExit(f"release workflow guard: missing {description}")


if len(sys.argv) > 2:
    raise SystemExit("usage: check-release-workflow.py [WORKFLOW]")
workflow_path = Path(sys.argv[1]) if len(sys.argv) == 2 else DEFAULT_WORKFLOW
workflow = workflow_path.read_text(encoding="utf-8")
trigger, jobs = workflow.split("\npermissions:\n", maxsplit=1)

require(
    trigger,
    """  pull_request:
    types: [closed]
    branches:
      - main
    paths:
      - '**/CHANGELOG.md'
""",
    "closed pull-request trigger for main release changelogs",
)
if "\n  push:" in trigger:
    raise SystemExit("release workflow guard: push must not trigger publication")

release, remainder = jobs.split("\n  prepare-attestation:", maxsplit=1)
expected_release_condition = """    if: >-
      github.event_name == 'pull_request' &&
      github.event.action == 'closed' &&
      github.event.pull_request.merged == true &&
      github.event.pull_request.base.ref == 'main' &&
      github.event.pull_request.base.ref == github.event.repository.default_branch &&
      github.ref == 'refs/heads/main' &&
      github.event.pull_request.head.repo.full_name == github.repository &&
      startsWith(github.event.pull_request.head.ref, 'release-plz-') &&
      github.event.pull_request.user.id == 286791072 &&
      github.event.pull_request.user.login == 'prismarisk-public-release[bot]' &&
      github.event.pull_request.user.type == 'Bot'
"""
require(release, expected_release_condition, "exact trusted release-PR condition")
require(release, "          command: release\n", "release-plz publication command")

release_pr = remainder.split("\n  release-pr:", maxsplit=1)[1]
require(
    release_pr,
    "    if: github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main'\n",
    "manual main-branch release-PR condition",
)
require(release_pr, "          command: release-pr\n", "manual release-PR command")

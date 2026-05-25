#!/usr/bin/env bash
#
# Enforces the rules in docs/performance-critical-path.md against every file
# that carries the `#[PerformanceCriticalPath]` marker (found by a full-file
# scan), and additionally checks that each marker sits near the top of its file.
#
# Modes (controlled by env var CRITICAL_PATH_STRICT):
#   - unset or "0": warn-only. Prints violations, exits 0.
#   - "1":          strict. Prints violations, exits 1.
#
# CI sets CRITICAL_PATH_STRICT=1; warn-only is for local iteration while a
# new marker candidate is being prepared for compliance.

set -euo pipefail

# Banned patterns. Extended regex via `grep -E`. Both the fully-qualified
# `tracing::info!` form and the bare `info!(...)` / `warn!(...)` /
# `error!(...)` shortcut (via `use tracing::info;`) are banned — otherwise
# a file that imports `tracing::warn` would silently pass the guard while
# violating the no-info-or-higher-logging rule. Info-or-higher spans are
# banned for the same reason.
#
# Not in this list (intentionally):
#   - `std::sync::Mutex` across `.await`: enforced by `clippy::await_holding_lock`
#     at the workspace level (see root `Cargo.toml`). The clippy lint is precise;
#     a grep substitute would false-positive on legitimate non-async uses.
#   - `.unwrap()` / `.expect()`: enforced by the workspace panic policy
#     (`clippy::unwrap_used` and `clippy::expect_used` — see CONTRIBUTING.md).
declare -a BANNED=(
  'tracing::info!'
  'tracing::warn!'
  'tracing::error!'
  '(^|[^:[:alnum:]_])info!\('
  '(^|[^:[:alnum:]_])warn!\('
  '(^|[^:[:alnum:]_])error!\('
  'tracing::info_span!'
  'tracing::warn_span!'
  'tracing::error_span!'
  '(^|[^:[:alnum:]_])info_span!\('
  '(^|[^:[:alnum:]_])warn_span!\('
  '(^|[^:[:alnum:]_])error_span!\('
  'println!'
)

# Locate repo root so the guard works from CI and from subdirectories.
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$REPO_ROOT"

# The placement window for the marker: how many lines from the top the marker
# is allowed to sit. A marker found below this line is reported as misplaced
# (not silently ignored) — see docs/performance-critical-path.md for the rule.
#
# Derived from the canonical copyright header (scripts/header.txt — the single
# source of truth shared with scripts/check-ts-header.py) so the window tracks
# the header automatically: when the header grows or shrinks, this follows. The
# marker sits just below the header (a blank separator line, then the marker),
# so the allowance is only a couple of lines past the header's end — enough for
# that separator plus a little slack. Keeping it tight is the point: the marker
# must sit *above* the module's own contents, not hidden under a long `//!`
# module doc or a stack of inner attributes.
HEADER_FILE="scripts/header.txt"
POST_HEADER_ALLOWANCE=3
if [[ ! -f "$HEADER_FILE" ]]; then
  echo "error: $HEADER_FILE not found (needed to size the marker scan window)" >&2
  exit 1
fi
HEADER_LINES=$(awk 'END { print NR }' "$HEADER_FILE")
SCAN_WINDOW=$((HEADER_LINES + POST_HEADER_ALLOWANCE))

# Matches the marker *as a marker*: a line whose only content is the marker
# comment. Anchoring to the whole line means prose that merely mentions the
# marker — e.g. a doc comment containing `#[PerformanceCriticalPath]` in
# backticks — is not mistaken for an actual marker. Used by `grep -E`.
MARKER_LINE_RE='^[[:space:]]*//[[:space:]]*#\[PerformanceCriticalPath\][[:space:]]*$'

# Find every file carrying the marker, scanning the *whole* file rather than
# just the top: a marker that has drifted below the placement window must still
# be found (and flagged as misplaced) instead of silently escaping the guard.
# The marker is an opt-in signal, so we honor it wherever it lands across the
# repo rather than trusting directory layout. `mapfile` is not available on
# macOS bash 3.2, so we use a newline-terminated while-read loop — no path in
# this repo contains whitespace or newlines.
MARKED=()
while IFS= read -r f; do
  [[ -n "$f" ]] && MARKED+=("$f")
done < <(
  find . -name '*.rs' \
       -not -path './target/*' \
       -not -path './.git/*' \
       -not -path '*/node_modules/*' \
       -not -path '*/.claude/worktrees/*' -print \
    | while IFS= read -r candidate; do
        if grep -Eq "$MARKER_LINE_RE" "$candidate"; then
          printf '%s\n' "$candidate"
        fi
      done
)

if [[ ${#MARKED[@]} -eq 0 ]]; then
  echo "No files carry the #[PerformanceCriticalPath] marker." >&2
  exit 0
fi

FAIL=0
for file in "${MARKED[@]}"; do
  # Placement check: the marker must sit near the top, above the module's own
  # contents. Flag (rather than ignore) a marker that sits below the window.
  marker_line=$(grep -nE "$MARKER_LINE_RE" "$file" | head -1 | cut -d: -f1)
  if (( marker_line > SCAN_WINDOW )); then
    echo "MISPLACED MARKER: $file has #[PerformanceCriticalPath] on line $marker_line"
    echo "    (must sit within the first $SCAN_WINDOW lines, directly below the copyright header)"
    FAIL=1
  fi

  # Banned-pattern enforcement. Applies to every marked file regardless of where
  # the marker sits — logging discipline is independent of marker placement.
  for pat in "${BANNED[@]}"; do
    if grep -nE "$pat" "$file" >/dev/null 2>&1; then
      echo "VIOLATION: $file contains banned pattern '$pat'"
      grep -nE "$pat" "$file" | sed 's/^/    /'
      FAIL=1
    fi
  done
done

if [[ $FAIL -eq 1 ]]; then
  echo ""
  echo "See docs/performance-critical-path.md for the rules."
  if [[ "${CRITICAL_PATH_STRICT:-0}" == "1" ]]; then
    exit 1
  fi
  echo "(warn-only mode; exit 0 — set CRITICAL_PATH_STRICT=1 to enforce)"
  exit 0
fi

echo "Critical-path guard: all ${#MARKED[@]} marked files clean."

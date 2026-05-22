#!/usr/bin/env bash
#
# Enforces the rules in docs/performance-critical-path.md against every file
# whose first 10 lines contain the `#[PerformanceCriticalPath]` marker.
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

# The scan window for the marker. Files where the marker sits below this many
# lines silently fall out of enforcement — see docs/performance-critical-path.md
# for the placement rule.
#
# Sized to accommodate the 11-line canonical copyright header (enforced by
# scripts/check-ts-header.py) plus a blank separator line plus the marker
# itself, with slack for a short `#![cfg_attr(...)]` inner attribute. Bumping
# this too high would let the marker hide under a long `//!` module doc; the
# whole point is that the marker must sit *above* the module's own contents.
SCAN_WINDOW=25

# Locate repo root so the guard works from CI and from subdirectories.
REPO_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || pwd)
cd "$REPO_ROOT"

# Find candidate files. We only scan under `crates/` since production code lives
# there. `mapfile` is not available on macOS bash 3.2, so we use a
# newline-terminated while-read loop — no path in this repo contains whitespace
# or newlines.
MARKED=()
while IFS= read -r f; do
  [[ -n "$f" ]] && MARKED+=("$f")
done < <(
  find crates -name '*.rs' -print \
    | while IFS= read -r candidate; do
        if head -n "$SCAN_WINDOW" "$candidate" | grep -q '#\[PerformanceCriticalPath\]'; then
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

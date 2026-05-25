#!/usr/bin/env python3
"""Verify (and optionally fix) the tsoracle copyright header on first-party
Rust source files.

Covers `.rs` files anywhere in the repo (excluding `target/`, `.git/`,
`.venv/`, `node_modules/`, and `.claude/worktrees/`). The header uses `//`
line comments and is placed at the very top of the file — above any
`#![...]` inner attribute, since those are part of compilation, not a shebang.

Usage:
    python3 scripts/check-ts-header.py                   # check the whole repo
    python3 scripts/check-ts-header.py --fix             # repair missing/stale headers
    python3 scripts/check-ts-header.py path/to/file.rs   # check specific files / dirs

Exit code is 0 when every checked file matches the canonical header and 1 when
any file is missing or has a stale header (check mode only). With --fix, missing
headers are inserted and stale tsoracle headers are replaced; files whose
leading comment block looks unrelated (e.g. crate-level `//!` docs) get the
canonical header prepended above the existing content.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

COMMENT_PREFIX = "//"
RUST_EXTENSION = ".rs"

# Directory names that exclude any file underneath them, anywhere in the tree.
EXCLUDED_DIR_NAMES = frozenset({"target", ".git", ".venv", "node_modules"})

# Path prefixes (posix-style, relative to repo root) excluded from the scan.
EXCLUDED_PATH_PREFIXES: tuple[str, ...] = (".claude/worktrees",)

# Body content of each header line, without comment marker. An empty entry
# renders as a bare `//` (used for the visual top/bottom borders and the
# blank rows between sections).
_HEADER_BODY_LINES: tuple[str, ...] = (
    "",
    "  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀",
    "  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀",
    "  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀",
    "",
    "  tsoracle — Distributed Timestamp Oracle",
    "  https://www.tsoracle.rs",
    "",
    "  Copyright (c) 2026 Prisma Risk",
    "",
    '  Licensed under the Apache License, Version 2.0 (the "License");',
    "  you may not use this file except in compliance with the License.",
    "  You may obtain a copy of the License at",
    "",
    "      https://www.apache.org/licenses/LICENSE-2.0",
    "",
    "  Unless required by applicable law or agreed to in writing, software",
    '  distributed under the License is distributed on an "AS IS" BASIS,',
    "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
    "  See the License for the specific language governing permissions and",
    "  limitations under the License.",
    "",
)

# Substring used to recognize an *existing* (possibly stale) tsoracle header
# in the leading comment block so --fix can replace it cleanly instead of
# stacking a fresh header on top of an old one. "Prisma Risk" appears in the
# canonical header and is unlikely to occur in unrelated leading comments.
EXISTING_HEADER_SIGNATURE = "Prisma Risk"


def render_header() -> str:
    """Produce the canonical header text."""
    lines = [
        f"{COMMENT_PREFIX}{body}" if body else COMMENT_PREFIX
        for body in _HEADER_BODY_LINES
    ]
    return "\n".join(lines) + "\n"


def display_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path)


def is_excluded(path: Path) -> bool:
    try:
        rel = path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return False
    parts = rel.split("/")
    if EXCLUDED_DIR_NAMES.intersection(parts):
        return True
    return any(rel == p or rel.startswith(p + "/") for p in EXCLUDED_PATH_PREFIXES)


def header_matches(text: str) -> bool:
    """Whitespace-tolerant prefix comparison: each line is rstrip'd before
    comparing, so editor auto-trimming of trailing whitespace doesn't flag
    the file. Only the first N lines need to match; the file may contain
    anything after the header."""
    expected = render_header().splitlines()
    actual = text.splitlines()
    if len(actual) < len(expected):
        return False
    for a, e in zip(actual[: len(expected)], expected):
        if a.rstrip() != e.rstrip():
            return False
    return True


def find_existing_header_span(text: str) -> tuple[int, int] | None:
    """If the leading comment block contains the tsoracle signature, return
    the character range to delete before inserting the canonical header.
    Returns None when the leading block looks unrelated (e.g. `//!` crate
    docs, a vendored license header), so --fix prepends without destroying
    it."""
    lines = text.splitlines(keepends=True)
    end = 0
    while end < len(lines) and lines[end].lstrip().startswith(COMMENT_PREFIX):
        end += 1
    if end == 0:
        return None
    if EXISTING_HEADER_SIGNATURE not in "".join(lines[:end]):
        return None
    while end < len(lines) and lines[end].strip() == "":
        end += 1
    return (0, sum(len(line) for line in lines[:end]))


def apply_fix(path: Path) -> None:
    original = path.read_text()

    span = find_existing_header_span(original)
    body = original[span[1] :] if span is not None else original

    canonical = render_header()
    if not body:
        new_text = canonical
    elif body.startswith("\n"):
        new_text = canonical + body
    else:
        new_text = canonical + "\n" + body
    path.write_text(new_text)


def _walk_extension(root: Path) -> list[Path]:
    return [p for p in root.rglob(f"*{RUST_EXTENSION}") if not is_excluded(p)]


def iter_source_files(targets: list[Path]) -> list[Path]:
    """Resolve `targets` to a deduplicated sorted list of `.rs` paths.

    With no targets, walk the whole repo. With explicit targets, recurse
    into directories and resolve individual files by extension."""
    paths: set[Path] = set()

    def add(path: Path) -> None:
        if path.suffix != RUST_EXTENSION or is_excluded(path):
            return
        paths.add(path.resolve())

    if not targets:
        for path in _walk_extension(REPO_ROOT):
            add(path)
        return sorted(paths)

    for target in targets:
        if target.is_dir():
            for path in _walk_extension(target):
                add(path)
        elif target.is_file():
            add(target)
    return sorted(paths)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify (or repair with --fix) the tsoracle header on .rs files."
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="repair files that are missing or have a stale header",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="files or directories to check (default: whole repo)",
    )
    args = parser.parse_args()

    paths = iter_source_files(args.paths)
    if not paths:
        print("no source files to check", file=sys.stderr)
        return 0

    missing: list[Path] = []
    fixed: list[Path] = []
    for path in paths:
        text = path.read_text()
        if header_matches(text):
            continue
        if args.fix:
            apply_fix(path)
            fixed.append(path)
        else:
            missing.append(path)

    if args.fix:
        for path in fixed:
            print(f"fixed: {display_path(path)}")
        if fixed:
            print(f"\nrepaired {len(fixed)} file(s).")
        else:
            print("all files already have the canonical header.")
        return 0

    if missing:
        for path in missing:
            print(
                f"missing/stale header: {display_path(path)}",
                file=sys.stderr,
            )
        print(
            f"\n{len(missing)} file(s) need the tsoracle header. Run with --fix to repair.",
            file=sys.stderr,
        )
        return 1

    print(f"checked {len(paths)} file(s): all headers OK.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

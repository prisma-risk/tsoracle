---
name: Bug report
about: A defect with a concrete reproduction and evidence in tree
title: ''
labels: bug
---

## Summary

<!-- One paragraph: what's wrong, what the contract was supposed to be, and severity. Severity reflects observable impact, not theoretical worst case. -->

## Reproduction trace

<!-- Numbered steps that walk the reader from a known starting state to the broken outcome. Quote the calls and the resulting state at each step. End with the observable damage (return values, persisted state, log output). -->

1.

## Affected code

<!-- `path/to/file.rs:start-end` bullets with a one-sentence note per location. The file:line references carry the reader; no need to inline the source. -->

-

## Proposed fix

<!-- A code block showing the smallest change that restores the invariant, plus one or two sentences on why. Omit this section if the fix is not yet known. -->

## Regression tests

<!-- Numbered tests that would fail today and pass once the fix lands. Each test should be runnable with `cargo test` and live next to the affected module. -->

1.

## Existing tests that would have caught this

<!-- Either: a list of tests that should have covered this case with a note on why they didn't, or "None." plus a short explanation of the coverage gap. -->

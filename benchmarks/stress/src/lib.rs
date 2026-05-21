#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

//! tsoracle stress + chaos harness.
//!
//! See `docs/superpowers/specs/2026-05-21-stress-harness-design.md` for the
//! design. See `benchmarks/stress/README.md` for usage.

pub mod chaos;
pub mod config;
pub mod event;
pub mod git;
pub mod loadgen;
pub mod nemesis;
pub mod report;
pub mod sample;
pub mod schedule;
pub mod supervisor;
pub mod topology;
pub mod types;
pub mod violation;

/// MIRRORS `bench-minimal::parse_count` — kept in sync manually.
///
/// Accepts underscore digit separators and a single trailing lowercase
/// `k`/`m`/`g`: `1k` → 1_000, `2m` → 2_000_000, `1g` → 1_000_000_000.
pub fn parse_count(input: &str) -> Result<u64, String> {
    if input.is_empty() {
        return Err("empty input".into());
    }
    let (digits, multiplier) = match input.as_bytes().last().copied() {
        Some(b'k') => (&input[..input.len() - 1], 1_000u64),
        Some(b'm') => (&input[..input.len() - 1], 1_000_000u64),
        Some(b'g') => (&input[..input.len() - 1], 1_000_000_000u64),
        _ => (input, 1u64),
    };
    let cleaned: String = digits.chars().filter(|c| *c != '_').collect();
    if cleaned.is_empty() {
        return Err(format!("no digits in {input:?}"));
    }
    let base: u64 = cleaned
        .parse()
        .map_err(|e| format!("invalid number {input:?}: {e}"))?;
    base.checked_mul(multiplier)
        .ok_or_else(|| format!("overflow parsing {input:?}"))
}

#[cfg(test)]
mod parse_count_tests {
    use super::parse_count;

    #[test]
    fn plain_number() { assert_eq!(parse_count("1").unwrap(), 1); }
    #[test]
    fn k_suffix() { assert_eq!(parse_count("1k").unwrap(), 1_000); }
    #[test]
    fn underscores() { assert_eq!(parse_count("1_500k").unwrap(), 1_500_000); }
    #[test]
    fn empty_rejected() { assert!(parse_count("").is_err()); }
    #[test]
    fn uppercase_rejected() { assert!(parse_count("1K").is_err()); }
}

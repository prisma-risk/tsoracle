//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

//! Candidate-endpoint worklist for the client retry loop.
//!
//! Owns the worklist queue, the visited-set dedup, and the
//! push-front-on-hint steering so `crate::retry::issue_rpc` reads as a
//! sequence of policy decisions rather than queue bookkeeping. The queue
//! starts with the cached leader (if fresh) followed by the configured
//! endpoints — see [`ChannelPool::iter_round_robin`](crate::leader_resolved::ChannelPool::iter_round_robin).
//!
//! A NOT_LEADER response carrying a usable [`LeaderHint`](tsoracle_proto::v1::LeaderHint)
//! redirects the loop via [`Worklist::redirect_to`], which pushes the
//! hinted endpoint to the FRONT so the hinted leader is retried
//! immediately rather than at the end of the round-robin pass. The
//! visited-set guarantees each endpoint is dialed at most once even when
//! a hint points back at one already tried.

use std::collections::{HashSet, VecDeque};

/// Ordered set of endpoints the retry loop will dial, with single-visit
/// dedup and front-insertion for leader-hint redirects.
pub(crate) struct Worklist {
    queue: VecDeque<String>,
    visited: HashSet<String>,
}

impl Worklist {
    /// Seed the worklist from the pool's preferred dial order.
    pub(crate) fn new(endpoints: Vec<String>) -> Self {
        Worklist {
            queue: endpoints.into(),
            visited: HashSet::new(),
        }
    }

    /// Pop the next endpoint that has not yet been visited, marking it
    /// visited. Entries that were already visited (a duplicate in the
    /// seed order, or a redirect back to a tried endpoint) are skipped.
    pub(crate) fn next(&mut self) -> Option<String> {
        while let Some(endpoint) = self.queue.pop_front() {
            if self.visited.insert(endpoint.clone()) {
                return Some(endpoint);
            }
        }
        None
    }

    /// Steer the loop at a hinted leader by pushing it to the front of
    /// the queue. A hint pointing at an already-visited endpoint is still
    /// pushed; [`Worklist::next`] skips it on pop.
    pub(crate) fn redirect_to(&mut self, endpoint: String) {
        self.queue.push_front(endpoint);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Duplicate seed entries are dialed once: the second occurrence hits
    /// the visited-set short-circuit and is skipped, so a pool seeded with
    /// repeats does not burn an extra attempt on the same endpoint.
    #[test]
    fn visits_each_endpoint_once_despite_duplicates() {
        let mut worklist = Worklist::new(vec!["a:1".into(), "b:1".into(), "a:1".into()]);
        assert_eq!(worklist.next().as_deref(), Some("a:1"));
        assert_eq!(worklist.next().as_deref(), Some("b:1"));
        assert_eq!(worklist.next(), None, "the duplicate a:1 must be skipped");
    }

    /// A redirect inserts the hinted leader at the front, so it is the
    /// very next endpoint dialed — not appended after the remaining
    /// round-robin candidates.
    #[test]
    fn redirect_to_dials_the_hinted_endpoint_next() {
        let mut worklist = Worklist::new(vec!["a:1".into(), "b:1".into()]);
        assert_eq!(worklist.next().as_deref(), Some("a:1"));
        worklist.redirect_to("c:1".into());
        assert_eq!(
            worklist.next().as_deref(),
            Some("c:1"),
            "the hinted leader must jump ahead of the queued b:1",
        );
        assert_eq!(worklist.next().as_deref(), Some("b:1"));
    }

    /// A redirect back to an already-visited endpoint is skipped rather
    /// than re-dialed, so a peer that hints at the endpoint we just tried
    /// cannot trap the loop in a cycle.
    #[test]
    fn redirect_to_visited_endpoint_is_skipped() {
        let mut worklist = Worklist::new(vec!["a:1".into(), "b:1".into()]);
        assert_eq!(worklist.next().as_deref(), Some("a:1"));
        worklist.redirect_to("a:1".into());
        assert_eq!(
            worklist.next().as_deref(),
            Some("b:1"),
            "a redirect to the already-visited a:1 must be skipped",
        );
        assert_eq!(worklist.next(), None);
    }
}

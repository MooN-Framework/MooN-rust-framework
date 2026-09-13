//! The exclusion vote.
//!
//! This is the only path that permanently removes a node from the
//! fabric, so it carries two hard rules:
//!
//! - **No unilateral exclusion.** A single reporter is never
//!   authoritative, because that reporter could be the faulty node
//!   itself. With fewer than two reporters nothing is confirmed and
//!   error management runs into its timeout, which routes to failsafe.
//! - **Rule 1a.** A target does not vote in its own tally, otherwise a
//!   faulty node could keep itself in the fabric by abstaining.
//!
//! Proposal masks use the sender's peer order, same as
//! [`super::observation`].

use crate::framework::config::{MAX_PEERS, MAX_TOTAL_NODES};
use crate::framework::state::peers::{PeerHealth, PeerRoster};
use crate::framework::types::PeerMask;
use heapless::Vec;

/// Per-cycle exclusion-vote tracker.
///
/// One slot per peer holds the last proposal received from that peer this
/// vote round. The local proposal comes from the peer roster's fault
/// counters and is combined with these when aggregating.
pub struct ExclusionVotes {
    /// Last proposal received from each peer slot this vote round.
    pub proposals: Vec<Option<PeerMask>, MAX_PEERS>,
}

impl ExclusionVotes {
    /// Empty tracker with no slots. Call [`ExclusionVotes::resize`] once
    /// the peer count is known.
    pub const fn empty() -> Self {
        Self {
            proposals: Vec::new(),
        }
    }

    /// Set the per-peer slot count. Called once at discovery finalize.
    pub fn resize(&mut self, n: usize) {
        for _ in 0..n {
            let _ = self.proposals.push(None);
        }
    }

    /// Drop all recorded proposals for a new vote round.
    pub fn reset(&mut self) {
        for slot in self.proposals.iter_mut() {
            *slot = None;
        }
    }

    /// Return the mask of peers whose exclusion is confirmed by a strict
    /// majority of reporters. Rule 1a: the target's own vote is excluded
    /// from its own tally.
    pub fn aggregate(&self, roster: &PeerRoster, own_id: u8, own_proposal: PeerMask) -> PeerMask {
        let mut confirmed = PeerMask::EMPTY;
        for idx in 0..roster.peers().len() {
            let target = &roster.peers()[idx];
            if target.health == PeerHealth::Lost {
                continue;
            }
            let target_id = target.id;
            let mut yes = 0usize;
            let mut reporters = 1usize;
            if own_proposal.contains(idx) {
                yes += 1;
            }
            for (i, other) in roster.peers().iter().enumerate() {
                if i == idx || other.health == PeerHealth::Lost {
                    continue;
                }
                if let Some(mask) = self.proposals[i] {
                    reporters += 1;
                    if sender_marked_target(roster, own_id, other.id, mask, target_id) {
                        yes += 1;
                    }
                }
            }
            let threshold = reporters / 2 + 1;
            // SIL 2: no unilateral exclusion. A single reporter (self alone) is
            // not authoritative — we could be the one that's wrong. If we can't
            // hear anyone, EM should time out and route to Failsafe.
            if reporters < 2 {
                continue;
            }
            if yes >= threshold {
                confirmed.set(idx);
            }
        }
        confirmed
    }

    /// True when a strict majority of at least two reporting peers
    /// named this node for exclusion. The node then moves to isolation
    /// on its own rather than waiting to be cut off.
    pub fn self_excluded_by_peers(&self, roster: &PeerRoster, own_id: u8) -> bool {
        let mut yes = 0usize;
        let mut reporters = 0usize;
        for (i, other) in roster.peers().iter().enumerate() {
            if other.health == PeerHealth::Lost {
                continue;
            }
            if let Some(mask) = self.proposals[i] {
                reporters += 1;
                if sender_marked_target(roster, own_id, other.id, mask, own_id) {
                    yes += 1;
                }
            }
        }
        if reporters < 2 {
            return false;
        }
        yes > reporters / 2
    }
}

/// Decode whether `sender_id` marked `target_id` in its proposal mask.
/// See `observation::sender_observed` for the bit-ordering rule; this is
/// the same logic used against exclusion masks.
fn sender_marked_target(
    roster: &PeerRoster,
    own_id: u8,
    sender_id: u8,
    mask: PeerMask,
    target_id: u8,
) -> bool {
    if sender_id == target_id {
        return false;
    }
    let mut all_ids = [0u8; MAX_TOTAL_NODES];
    let mut n = 0usize;
    all_ids[n] = own_id;
    n += 1;
    for p in roster.peers().iter() {
        if n >= MAX_TOTAL_NODES {
            break;
        }
        all_ids[n] = p.id;
        n += 1;
    }
    all_ids[..n].sort_unstable();
    let mut position = 0usize;
    for &id in &all_ids[..n] {
        if id == sender_id {
            continue;
        }
        if id == target_id {
            return mask.contains(position);
        }
        position += 1;
    }
    false
}

#[cfg(test)]
mod exclusion_vote_tests {
    //! The exclusion vote is the only path that permanently removes a
    //! node from the fabric, so the two safety properties it rests on
    //! are pinned here: no unilateral exclusion (a lone reporter is
    //! never authoritative, SIL 2), and a target never votes in its own
    //! tally (Rule 1a). The mask-position decoding is covered too,
    //! because it is indexed against the *sender's* peer order and
    //! silently produces wrong accusations when node ids are not
    //! contiguous.
    use super::*;

    /// Roster as seen by `own_id`, containing `peer_ids`. Peers are
    /// sorted by id, so slot k corresponds to the k-th smallest id.
    fn roster_for(own_id: u8, peer_ids: &[u8]) -> PeerRoster {
        let mut r = PeerRoster::new();
        for &id in peer_ids {
            r.discover(id, own_id, peer_ids.len()).expect("discover");
        }
        r.finalize(own_id, peer_ids.len() as u8 + 1)
            .expect("finalize");
        r
    }

    fn votes_for(n: usize) -> ExclusionVotes {
        let mut v = ExclusionVotes::empty();
        v.resize(n);
        v
    }

    fn mask(bits: &[usize]) -> PeerMask {
        let mut m = PeerMask::EMPTY;
        for &b in bits {
            m.set(b);
        }
        m
    }

    #[test]
    fn two_reporters_confirm_the_exclusion() {
        // own=0, peers 1 and 2. Both healthy reporters accuse node 2.
        // From node 1's view the peer order is [0, 2], so node 2 sits
        // at bit 1 of node 1's proposal mask.
        let roster = roster_for(0, &[1, 2]);
        let mut votes = votes_for(2);
        votes.proposals[0] = Some(mask(&[1]));

        let confirmed = votes.aggregate(&roster, 0, mask(&[1]));

        assert!(confirmed.contains(1), "node 2 should be excluded");
        assert!(!confirmed.contains(0), "node 1 was never accused");
    }

    #[test]
    fn a_single_reporter_can_never_exclude() {
        // Regression guard for the SIL 2 rule: if we are the only
        // reporter we could be the faulty one ourselves. Excluding
        // unilaterally would let one broken node shrink the fabric.
        let roster = roster_for(0, &[1, 2]);
        let votes = votes_for(2);

        let confirmed = votes.aggregate(&roster, 0, mask(&[0, 1]));

        assert_eq!(confirmed, PeerMask::EMPTY);
    }

    #[test]
    fn target_vote_does_not_count_in_its_own_tally() {
        // Rule 1a. own=0, peers 1, 2, 3, target is node 3.
        // Reporters for node 3 are own + node 1 + node 2, not node 3
        // itself, so the threshold is 2 and two accusations suffice.
        // Were the target counted, the threshold would rise to 3 and
        // the accused node could save itself by voting.
        let roster = roster_for(0, &[1, 2, 3]);
        let mut votes = votes_for(3);
        // Node 1 sees peers [0, 2, 3]; node 3 is at bit 2.
        votes.proposals[0] = Some(mask(&[2]));
        // Node 2 abstains, node 3 submits an empty accusation.
        votes.proposals[1] = Some(PeerMask::EMPTY);
        votes.proposals[2] = Some(PeerMask::EMPTY);

        let confirmed = votes.aggregate(&roster, 0, mask(&[2]));

        assert!(confirmed.contains(2), "node 3 should be excluded");
    }

    #[test]
    fn lost_peers_are_neither_targets_nor_reporters() {
        let mut roster = roster_for(0, &[1, 2, 3]);
        assert_eq!(roster.exclude(mask(&[2])), 1); // node 3 is Lost
        let mut votes = votes_for(3);
        votes.proposals[2] = Some(mask(&[0, 1])); // Lost node accuses everyone
        votes.proposals[0] = Some(PeerMask::EMPTY);

        let confirmed = votes.aggregate(&roster, 0, PeerMask::EMPTY);

        assert_eq!(
            confirmed,
            PeerMask::EMPTY,
            "a Lost node's accusations must not count"
        );
    }

    #[test]
    fn mask_positions_follow_the_senders_order_not_the_node_id() {
        // Non-contiguous ids. own=0, peers 3 and 7. Node 3 sees the
        // peer order [0, 7], so it marks node 7 at bit 1. Indexing by
        // node id would put it at bit 7 and lose the accusation.
        let roster = roster_for(0, &[3, 7]);
        let mut votes = votes_for(2);
        votes.proposals[0] = Some(mask(&[1]));

        let confirmed = votes.aggregate(&roster, 0, mask(&[1]));
        assert!(confirmed.contains(1), "node 7 should be excluded");

        // The naive reading (bit 7 = node 7) must not confirm anything.
        let mut naive = votes_for(2);
        naive.proposals[0] = Some(PeerMask::from_u8(0b1000_0000));
        assert_eq!(naive.aggregate(&roster, 0, mask(&[1])), PeerMask::EMPTY);
    }

    #[test]
    fn self_exclusion_needs_a_majority_of_at_least_two_reporters() {
        let roster = roster_for(0, &[1, 2]);

        // Both peers mark node 0. From either sender's view node 0 is
        // the lowest remaining id, so it sits at bit 0.
        let mut both = votes_for(2);
        both.proposals[0] = Some(mask(&[0]));
        both.proposals[1] = Some(mask(&[0]));
        assert!(both.self_excluded_by_peers(&roster, 0));

        // One accuser out of two reporters is not a strict majority.
        let mut one = votes_for(2);
        one.proposals[0] = Some(mask(&[0]));
        one.proposals[1] = Some(PeerMask::EMPTY);
        assert!(!one.self_excluded_by_peers(&roster, 0));

        // A lone reporter is never authoritative, same rule as above.
        let mut lone = votes_for(2);
        lone.proposals[0] = Some(mask(&[0]));
        assert!(!lone.self_excluded_by_peers(&roster, 0));
    }

    #[test]
    fn reset_clears_every_slot() {
        let mut votes = votes_for(3);
        votes.proposals[0] = Some(mask(&[1]));
        votes.proposals[2] = Some(PeerMask::EMPTY);
        votes.reset();
        assert!(votes.proposals.iter().all(|s| s.is_none()));
    }
}

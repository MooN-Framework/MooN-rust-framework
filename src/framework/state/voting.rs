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
    pub proposals: Vec<Option<PeerMask>, MAX_PEERS>,
}

impl ExclusionVotes {
    pub const fn empty() -> Self {
        Self {
            proposals: Vec::new(),
        }
    }

    pub fn resize(&mut self, n: usize) {
        for _ in 0..n {
            let _ = self.proposals.push(None);
        }
    }

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
            if yes >= threshold {
                confirmed.set(idx);
            }
        }
        confirmed
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

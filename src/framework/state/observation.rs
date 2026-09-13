//! Cross-observation: who saw whom this phase.
//!
//! A node cannot decide on its own that a peer is absent, because its
//! own receive path may be the broken component. Instead every node
//! attests what it received, and a peer is only attributed as missing
//! when a strict majority of reporters failed to see it.
//!
//! Attested masks are indexed against the *sender's* peer order (all
//! node ids except the sender, sorted ascending), so every mask has to
//! be position-translated before it can be read. That translation is
//! what `sender_observed` does, and getting it wrong produces wrong
//! accusations only when node ids are non-contiguous, which is exactly
//! what happens after a rejoin.

use crate::framework::config::{MAX_PEERS, MAX_TOTAL_NODES};
use crate::framework::state::cycle::AckInfo;
use crate::framework::state::peers::{PeerHealth, PeerRoster};
use crate::framework::traits::CyclePayload;
use crate::framework::types::PeerMask;
use heapless::Vec;

/// Cross-observation buffers used across CycleSync and Result phases.
///
/// - `own_seen`: our own attested observation this phase.
/// - `peer_seen`: last seen attested mask from each peer this phase.
pub struct ObservationKind {
    /// What this node saw itself this phase.
    pub own_seen: PeerMask,
    /// Last attested mask from each peer slot this phase.
    pub peer_seen: Vec<Option<PeerMask>, MAX_PEERS>,
}

impl ObservationKind {
    /// Empty buffers with no slots. Call [`ObservationKind::resize`]
    /// once the peer count is known.
    pub const fn empty() -> Self {
        Self {
            own_seen: PeerMask::EMPTY,
            peer_seen: Vec::new(),
        }
    }

    /// Set the per-peer slot count. Called once at discovery finalize.
    pub fn resize(&mut self, n: usize) {
        for _ in 0..n {
            let _ = self.peer_seen.push(None);
        }
    }

    /// Drop all evidence, own and attested, for a new phase.
    pub fn reset_seen(&mut self) {
        self.own_seen = PeerMask::EMPTY;
        for slot in self.peer_seen.iter_mut() {
            *slot = None;
        }
    }
}

/// Aggregation input: which per-phase evidence to consult.
pub enum View<'a, P: CyclePayload> {
    CycleSync {
        own_seen: PeerMask,
        peer_seen: &'a [Option<PeerMask>],
    },
    Result {
        peer_results: &'a [Option<P>],
        peer_acks: &'a [Option<AckInfo>],
    },
    Input {
        peer_inputs_present: &'a [bool],
    }, // NEU
}

/// Compute the mask of peers this node currently attributes as missing
/// from the phase, using the strict-majority rule across reporters.
///
/// For each non-Lost peer target, count observers (own node + non-Lost
/// non-target peers) that reported seeing the target. If observers fall
/// below the strict majority of reporters, the target is attributed.
///
/// Returns `None` when no evidence was collected — the local inbound may
/// be at fault, so we refuse to attribute anyone.
pub fn attribute_missing<P: CyclePayload>(
    roster: &PeerRoster,
    view: View<'_, P>,
    own_id: u8,
) -> Option<PeerMask> {
    if !any_evidence(&view) {
        return None;
    }
    let mut mask = PeerMask::EMPTY;
    for idx in 0..roster.peers().len() {
        let target = &roster.peers()[idx];
        if target.health == PeerHealth::Lost {
            continue;
        }
        let (observers, reporters) = count_observations(roster, own_id, idx, target.id, &view);
        let threshold = reporters / 2 + 1;
        if observers < threshold {
            mask.set(idx);
        }
    }
    Some(mask)
}

impl<'a, P: CyclePayload> View<'a, P> {
    fn own_observed(&self, target_idx: usize) -> bool {
        match self {
            View::CycleSync { own_seen, .. } => own_seen.contains(target_idx),
            View::Result { peer_results, .. } => peer_results
                .get(target_idx)
                .map(|r| r.is_some())
                .unwrap_or(false),
            View::Input {
                peer_inputs_present,
            } => peer_inputs_present
                .get(target_idx)
                .copied()
                .unwrap_or(false),
        }
    }

    fn reporter_mask(&self, reporter_idx: usize) -> Option<PeerMask> {
        match self {
            View::CycleSync { peer_seen, .. } => peer_seen.get(reporter_idx).and_then(|m| *m),
            View::Result { peer_acks, .. } => peer_acks
                .get(reporter_idx)
                .and_then(|a| a.map(|ack| PeerMask::from_u8(ack.received_from))),
            View::Input { .. } => None, // Inputs carry no attested observation mask
        }
    }
}

/// True when at least one piece of evidence was recorded this phase.
fn any_evidence<P: CyclePayload>(view: &View<'_, P>) -> bool {
    match view {
        View::CycleSync {
            own_seen,
            peer_seen,
        } => !own_seen.is_empty() || peer_seen.iter().any(|m| m.is_some()),
        View::Result {
            peer_results,
            peer_acks,
        } => peer_results.iter().any(|r| r.is_some()) || peer_acks.iter().any(|a| a.is_some()),
        View::Input {
            peer_inputs_present,
        } => peer_inputs_present.iter().any(|p| *p),
    }
}

/// Count observers/reporters for one target. Own node is always a reporter.
/// Peer reporters count only if they submitted a mask this phase.
fn count_observations<P: CyclePayload>(
    roster: &PeerRoster,
    own_id: u8,
    target_idx: usize,
    target_id: u8,
    view: &View<'_, P>,
) -> (usize, usize) {
    let mut observers = 0usize;
    let mut reporters = 1usize;
    if view.own_observed(target_idx) {
        observers += 1;
    }
    for (i, other) in roster.peers().iter().enumerate() {
        if i == target_idx || other.health == PeerHealth::Lost {
            continue;
        }
        if let Some(mask) = view.reporter_mask(i) {
            reporters += 1;
            if sender_observed(roster, own_id, other.id, mask, target_id) {
                observers += 1;
            }
        }
    }
    (observers, reporters)
}

/// Decode whether `sender_id` marked `target_id` in its attested `mask`.
/// Mask bits are indexed against the sender's peer order (all node ids
/// except sender, sorted).
fn sender_observed(
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
mod attribution_tests {
    //! `attribute_missing` decides who gets accused of being absent
    //! from a phase, which is the input to the exclusion vote. Two
    //! properties matter: with no evidence at all nobody is accused
    //! (our own inbound path may be the broken one), and the three
    //! `View` variants aggregate different evidence and must not be
    //! assumed to behave alike.
    use super::*;
    use crate::brake::braking_curve::BrakeResult;

    fn roster_for(own_id: u8, peer_ids: &[u8]) -> PeerRoster {
        let mut r = PeerRoster::new();
        for &id in peer_ids {
            r.discover(id, own_id, peer_ids.len()).expect("discover");
        }
        r.finalize(own_id, peer_ids.len() as u8 + 1)
            .expect("finalize");
        r
    }

    fn mask(bits: &[usize]) -> PeerMask {
        let mut m = PeerMask::EMPTY;
        for &b in bits {
            m.set(b);
        }
        m
    }

    fn result(distance: f64) -> BrakeResult {
        BrakeResult {
            total_distance: distance,
            emergency_brake: false,
            valid_entry: true,
        }
    }

    fn ack(received_from: PeerMask) -> AckInfo {
        AckInfo {
            received_from: received_from.as_u8(),
            publisher_candidate: 0,
        }
    }

    #[test]
    fn no_evidence_accuses_nobody() {
        // Our own receive path may be the faulty component. Returning
        // an empty mask instead of None would have every isolated node
        // propose the whole fabric for exclusion.
        let roster = roster_for(0, &[1, 2]);
        let peer_seen = [None, None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: PeerMask::EMPTY,
                peer_seen: &peer_seen,
            },
            0,
        );
        assert_eq!(out, None);

        let peer_results: [Option<BrakeResult>; 2] = [None, None];
        let peer_acks = [None, None];
        assert_eq!(
            attribute_missing(
                &roster,
                View::Result {
                    peer_results: &peer_results,
                    peer_acks: &peer_acks,
                },
                0,
            ),
            None
        );

        let present = [false, false];
        assert_eq!(
            attribute_missing::<BrakeResult>(
                &roster,
                View::Input {
                    peer_inputs_present: &present,
                },
                0,
            ),
            None
        );
    }

    #[test]
    fn cycle_sync_attributes_the_peer_nobody_saw() {
        // own=0 saw node 1 only. Node 1 attests to seeing node 0 (bit
        // 0 in its own peer order [0, 2]) and not node 2. Two reporters
        // agree that node 2 is absent.
        let roster = roster_for(0, &[1, 2]);
        let peer_seen = [Some(mask(&[0])), None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: mask(&[0]),
                peer_seen: &peer_seen,
            },
            0,
        )
        .expect("evidence present");

        assert!(out.contains(1), "node 2 is missing");
        assert!(!out.contains(0), "node 1 was seen by us");
    }

    #[test]
    fn one_vouching_peer_is_not_enough_two_are() {
        // We did not see node 2 ourselves, node 1 did. One observer out
        // of two reporters stays below the strict majority, so node 2
        // is still attributed: our local view alone does not clear a
        // peer, and that is what makes the split-view case converge.
        let roster = roster_for(0, &[1, 2]);
        let peer_seen = [Some(mask(&[0, 1])), None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: mask(&[0]),
                peer_seen: &peer_seen,
            },
            0,
        )
        .expect("evidence present");
        assert!(out.contains(1));

        // Same situation with a fourth node: target is node 3, and both
        // node 1 and node 2 attest to having seen it. Two observers out
        // of three reporters clears it.
        let roster = roster_for(0, &[1, 2, 3]);
        // Node 1 orders its peers [0, 2, 3], node 2 orders them
        // [0, 1, 3], so node 3 sits at bit 2 for both of them.
        let peer_seen = [Some(mask(&[2])), Some(mask(&[2])), None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: mask(&[0, 1]),
                peer_seen: &peer_seen,
            },
            0,
        )
        .expect("evidence present");
        assert!(!out.contains(2), "two peers vouched for node 3");
    }

    #[test]
    fn lost_peers_are_skipped_entirely() {
        let mut roster = roster_for(0, &[1, 2]);
        assert_eq!(roster.exclude(mask(&[1])), 1); // node 2 is Lost
        let peer_seen = [Some(mask(&[0])), None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: mask(&[0]),
                peer_seen: &peer_seen,
            },
            0,
        )
        .expect("evidence present");
        assert!(!out.contains(1), "a Lost peer is not accused again");
    }

    #[test]
    fn result_view_uses_results_and_acks_together() {
        // Node 1 delivered a result and attested (via its ack) that it
        // received node 2's result as well. Node 2 delivered nothing to
        // us, so we have one observer out of two reporters for it.
        let roster = roster_for(0, &[1, 2]);
        let peer_results = [Some(result(10.0)), None];
        let peer_acks = [Some(ack(mask(&[0, 1]))), None];
        let out = attribute_missing(
            &roster,
            View::Result {
                peer_results: &peer_results,
                peer_acks: &peer_acks,
            },
            0,
        )
        .expect("evidence present");

        assert!(!out.contains(0), "node 1 sent its result");
        assert!(out.contains(1), "node 2 stayed silent towards us");
    }

    #[test]
    fn input_view_has_no_cross_attestation() {
        // Input frames carry no attested observation mask, so the own
        // view is the only evidence and the threshold is always 1.
        // Anything we did not receive ourselves is attributed, with no
        // chance for a peer to vouch for it.
        let roster = roster_for(0, &[1, 2]);
        let present = [true, false];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::Input {
                peer_inputs_present: &present,
            },
            0,
        )
        .expect("evidence present");

        assert!(!out.contains(0));
        assert!(out.contains(1));
    }

    #[test]
    fn attestation_positions_follow_the_senders_peer_order() {
        // own=0, peers 3 and 7. Node 3's peer order is [0, 7], so it
        // attests node 7 at bit 1. Reading the mask by node id would
        // look at bit 7 and wrongly accuse a node that was seen.
        let roster = roster_for(0, &[3, 7]);
        let peer_seen = [Some(mask(&[0, 1])), None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: mask(&[0, 1]),
                peer_seen: &peer_seen,
            },
            0,
        )
        .expect("evidence present");
        assert!(!out.contains(1), "node 7 was attested at bit 1");

        let wrong_bit = [Some(PeerMask::from_u8(0b1000_0000)), None];
        let out = attribute_missing::<BrakeResult>(
            &roster,
            View::CycleSync {
                own_seen: mask(&[0, 1]),
                peer_seen: &wrong_bit,
            },
            0,
        )
        .expect("evidence present");
        assert!(out.contains(1), "bit 7 must not read as node 7");
    }

    #[test]
    fn reset_seen_clears_own_and_peer_evidence() {
        let mut obs = ObservationKind::empty();
        obs.resize(2);
        obs.own_seen = mask(&[0, 1]);
        obs.peer_seen[0] = Some(mask(&[1]));
        obs.reset_seen();
        assert!(obs.own_seen.is_empty());
        assert!(obs.peer_seen.iter().all(|s| s.is_none()));
    }
}

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
    pub own_seen: PeerMask,
    pub peer_seen: Vec<Option<PeerMask>, MAX_PEERS>,
}

impl ObservationKind {
    pub const fn empty() -> Self {
        Self {
            own_seen: PeerMask::EMPTY,
            peer_seen: Vec::new(),
        }
    }

    pub fn resize(&mut self, n: usize) {
        for _ in 0..n {
            let _ = self.peer_seen.push(None);
        }
    }

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

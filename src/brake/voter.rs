use crate::brake::braking_curve::BrakeResult;
use crate::framework::traits::{CyclePayload, Voter, VotingOutcome};
use crate::framework::wire::{PayloadError, WireReader, WireWriter};
use heapless::Vec;
use tracing::{debug, error, info, warn};

// -------------------------------------------------------------
// CyclePayload-Impl fuer BrakeResult
// -------------------------------------------------------------
//
// Layout auf der Leitung (10 Byte, little-endian):
//   0..8   total_distance   (f64)
//   8      emergency_brake  (u8: 0x00 | 0x01, strikt)
//   9      valid_entry      (u8: 0x00 | 0x01, strikt)

impl CyclePayload for BrakeResult {
    const WIRE_SIZE: usize = 10;

    fn to_wire(&self, w: &mut WireWriter<'_>) {
        w.push_f64(self.total_distance);
        w.push_bool(self.emergency_brake);
        w.push_bool(self.valid_entry);
        debug_assert_eq!(w.written(), Self::WIRE_SIZE);
    }

    fn from_wire(r: &mut WireReader<'_>) -> Result<Self, PayloadError> {
        Ok(BrakeResult {
            total_distance: r.read_f64()?,
            emergency_brake: r.read_bool()?,
            valid_entry: r.read_bool()?,
        })
    }
}

// -------------------------------------------------------------
// Voter
// -------------------------------------------------------------

const MAX_PARTICIPANTS: usize = 2;

/// K-oo-M Voter fuer BrakeResult.
///
/// Uebereinstimmung ist definiert als:
///   - `emergency_brake` exakt gleich,
///   - `valid_entry` exakt gleich,
///   - `|a.total_distance - b.total_distance| <= distance_tolerance`.
#[derive(Debug, Clone, Copy)]
pub struct BrakeVoter {
    pub required: u8,
    pub distance_tolerance: f64,
}

impl BrakeVoter {
    pub fn new(required: u8, distance_tolerance: f64) -> Self {
        assert!(required >= 1, "required muss >= 1 sein");
        assert!(
            distance_tolerance >= 0.0 && distance_tolerance.is_finite(),
            "distance_tolerance muss endlich und nicht-negativ sein"
        );
        Self {
            required,
            distance_tolerance,
        }
    }

    fn agree(&self, a: &BrakeResult, b: &BrakeResult) -> bool {
        if a.emergency_brake != b.emergency_brake || a.valid_entry != b.valid_entry {
            return false;
        }
        if a.total_distance.is_nan() || b.total_distance.is_nan() {
            return false;
        }
        (a.total_distance - b.total_distance).abs() <= self.distance_tolerance
    }

    fn representative(&self, group: &[BrakeResult]) -> BrakeResult {
        let mut distances: Vec<f64, MAX_PARTICIPANTS> = Vec::new();
        for r in group {
            let _ = distances.push(r.total_distance);
        }
        distances.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let median = distances[distances.len() / 2];

        BrakeResult {
            total_distance: median,
            emergency_brake: group[0].emergency_brake,
            valid_entry: group[0].valid_entry,
        }
    }
}

impl Voter for BrakeVoter {
    type Payload = BrakeResult;
    type Decision = BrakeResult;

    fn required_participants(&self) -> u8 {
        self.required
    }

fn decide(
    &self,
    own: &BrakeResult,
    peers: &[Option<BrakeResult>],
) -> VotingOutcome<BrakeResult> {
    // n_expected: alle Nodes von denen wir in diesem Zyklus einen Wert
    // erwarten (eigener + alle Peer-Slots). Lost-Peers sind bereits
    // durch RunState::run_vote rausgefiltert, tauchen hier nicht auf.
    let n_expected = 1 + peers.len();

    // Strikte Mehrheit schliesst Ties strukturell aus:
    //   n=2 -> 2, n=3 -> 2, n=4 -> 3, n=5 -> 3, n=8 -> 5
    let strict_majority = n_expected / 2 + 1;

    // Kombination mit Systemintegrator-Vorgabe: `required` als
    // Untergrenze, falls der Systemintegrator ein strengeres Kriterium
    // als die reine Mehrheit gesetzt hat (z.B. 6oo8).
    let min_agreement = strict_majority.max(self.required as usize);

    // Vorhandene Werte sammeln
    let mut all: Vec<BrakeResult, MAX_PARTICIPANTS> = Vec::new();
    let _ = all.push(*own);
    for p in peers.iter().flatten() {
        let _ = all.push(*p);
    }

    // Reichen die Antworten ueberhaupt aus, um min_agreement zu erreichen?
    if all.len() < min_agreement {
        error!(
            "insufficient responses: got {} of {} expected, need {} to agree",
            all.len(),
            n_expected,
            min_agreement
        );
        return VotingOutcome::InsufficientQuorum;
    }

    // Suche eine Gruppe uebereinstimmender Werte
    for candidate in all.iter() {
        let mut group: Vec<BrakeResult, MAX_PARTICIPANTS> = Vec::new();
        for other in all.iter() {
            if self.agree(candidate, other) {
                let _ = group.push(*other);
            }
        }
        if group.len() >= min_agreement {
            return VotingOutcome::Consensus(self.representative(&group));
        }
    }

    VotingOutcome::Disagreement
}

    fn find_dissenters(
        &self,
        own: &BrakeResult,
        peers: &[Option<BrakeResult>],
        decision: &BrakeResult,
    ) -> (bool, heapless::Vec<u8, 16>) {
        let own_dissented = !self.agree(own, decision);

        let mut peer_dissenter_indices = heapless::Vec::<u8, 16>::new();

        for (idx, peer) in peers.iter().enumerate() {
            if let Some(peer_value) = peer {
                if !self.agree(peer_value, decision) {
                    let _ = peer_dissenter_indices.push(idx as u8);
                }
            }
        }

        (own_dissented, peer_dissenter_indices)
    }
}

//! This module implements the voter trait of the framework.
//! It defines the `BrakeVoter` struct, which implements the `Voter` trait for the brake example.

use crate::brake::braking_curve::BrakeResult;
use crate::framework::config::{MAX_DISSENTERS, MAX_TOTAL_NODES};
use crate::framework::traits::{CyclePayload, Voter, VotingOutcome};
use crate::framework::wire::{PayloadError, WireReader, WireWriter};
use heapless::Vec;
use tracing::error;

/// Cycle Payload implementation for `BrakeResult`.
/// This allows the framework to serialize and deserialize `BrakeResult` instances for communication between nodes in the distributed system.
/// Wire layout (10 bytes, little-endian):
/// ```text
///   0..8   total_distance   (f64)
///   8      emergency_brake  (u8: 0x00 | 0x01)
///   9      valid_entry      (u8: 0x00 | 0x01)
/// ```
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

/// M-oo-N voter for `BrakeResult`. Two values agree iff booleans match
/// exactly and total distances are within `distance_tolerance`.
#[derive(Debug, Clone, Copy)]
pub struct BrakeVoter {
    pub min_participants: u8,
    pub distance_tolerance: f64,
}

impl BrakeVoter {
    /// Create a new `BrakeVoter` with the specified minimum number of participants and distance tolerance.
    /// Checks are performed to ensure that `min_participants` is at least 1 and `distance_tolerance` is non-negative and finite.
    pub fn new(min_participants: u8, distance_tolerance: f64) -> Self {
        assert!(min_participants >= 1, "min_participants must be >= 1");
        assert!(
            distance_tolerance >= 0.0 && distance_tolerance.is_finite(),
            "distance_tolerance must be finite and non-negative"
        );
        Self {
            min_participants,
            distance_tolerance,
        }
    }

    /// True when both values match on both flags and their distances are
    /// within tolerance.
    fn agree(&self, a: &BrakeResult, b: &BrakeResult) -> bool {
        if a.emergency_brake != b.emergency_brake || a.valid_entry != b.valid_entry {
            return false;
        }
        if a.total_distance.is_nan() || b.total_distance.is_nan() {
            return false;
        }
        (a.total_distance - b.total_distance).abs() <= self.distance_tolerance
    }

    /// Median-distance representative of a group. Flags are taken from the
    /// first element (they are equal by construction).
    fn representative(&self, group: &[BrakeResult]) -> BrakeResult {
        let mut distances: Vec<f64, MAX_TOTAL_NODES> = Vec::new();
        for r in group {
            let _ = distances.push(r.total_distance);
        }
        distances.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        BrakeResult {
            total_distance: distances[distances.len() / 2],
            emergency_brake: group[0].emergency_brake,
            valid_entry: group[0].valid_entry,
        }
    }
}

/// Actual implementation of the `Voter` trait for `BrakeVoter`.
impl Voter for BrakeVoter {
    type Payload = BrakeResult;
    type Decision = BrakeResult;

    /// Return the minimum number of participants required for a valid vote.
    fn required_participants(&self) -> u8 {
        self.min_participants
    }

    /// Decide on a `BrakeResult` based on the own value and the peer values.
    /// Returns a `VotingOutcome` indicating whether a consensus was reached, there was a disagreement, or there were insufficient responses.
    /// The decision is based on a strict majority of agreeing values, with the representative value being the median of the agreeing group.
    fn decide(
        &self,
        own: &BrakeResult,
        peers: &[Option<BrakeResult>],
    ) -> VotingOutcome<BrakeResult> {
        // Count the number of expected participants and calculate the strict majority and minimum agreement required.
        let n_expected = 1 + peers.len();
        let strict_majority = n_expected / 2 + 1;
        let min_agreement = strict_majority.max(self.min_participants as usize);

        // Collect all valid responses (own and peers) into a single vector for processing.
        let mut all: Vec<BrakeResult, MAX_TOTAL_NODES> = Vec::new();
        let _ = all.push(*own);
        for p in peers.iter().flatten() {
            let _ = all.push(*p);
        }

        // If the number of valid responses is less than the minimum agreement required, return an InsufficientQuorum outcome.
        if all.len() < min_agreement {
            error!(
                got = all.len(),
                expected = n_expected,
                need = min_agreement,
                "insufficient responses"
            );
            return VotingOutcome::InsufficientQuorum;
        }

        // Iterate through each candidate value and group all agreeing values together.
        // If a group meets or exceeds the minimum agreement threshold,
        // return a Consensus outcome with the representative value of that group.
        for candidate in all.iter() {
            let mut group: Vec<BrakeResult, MAX_TOTAL_NODES> = Vec::new();
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

    /// Find dissenters among the peers based on the own value, peer values, and the final decision.
    /// A decenter is a peer whose value does not agree with the final decision.
    /// The function returns a tuple containing a boolean indicating whether the own value dissented and
    /// a vector of peer indices that dissented.
    fn find_dissenters(
        &self,
        own: &BrakeResult,
        peers: &[Option<BrakeResult>],
        decision: &BrakeResult,
    ) -> (bool, Vec<u8, MAX_DISSENTERS>) {
        // Determine if the own value dissented from the final decision and collect the indices of dissenting peers.
        let own_dissented = !self.agree(own, decision);
        // Iterate through the peers and check if their values agree with the final decision.
        let mut dissenters: Vec<u8, MAX_DISSENTERS> = Vec::new();
        for (idx, peer) in peers.iter().enumerate() {
            if let Some(peer_value) = peer {
                if !self.agree(peer_value, decision) {
                    let _ = dissenters.push(idx as u8);
                }
            }
        }
        // Return a tuple containing whether the own value dissented and the list of dissenting peer indices.
        (own_dissented, dissenters)
    }
}

#[cfg(feature = "diagnostic")]
impl crate::framework::traits::Corruptible for BrakeResult {
    fn corrupt(&mut self) {
        // 10 km Verschiebung liegt garantiert ausserhalb jeder
        // sinnvollen distance_tolerance. Emergency-Flag zusaetzlich
        // flippen, damit auch bei tolerance=inf der Boolean-Gate greift.
        self.total_distance += 10_000.0;
        self.emergency_brake = !self.emergency_brake;
    }
}
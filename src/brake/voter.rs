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
    /// Safety floor on agreeing values, independent of how many nodes
    /// are still active.
    pub min_participants: u8,
    /// Maximum accepted difference between two braking distances, in
    /// metres, for them to count as the same value.
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
#[cfg(test)]
mod brake_voter_tests {
    //! The voter turns N candidate results into one decision, so its
    //! failure modes are the interesting part: an even split must never
    //! resolve to one of the halves, a missing majority must surface as
    //! Disagreement rather than as a decision, and NaN must never
    //! compare equal to anything.
    use super::*;
    use crate::framework::traits::{Voter, VotingOutcome};

    const TOLERANCE: f64 = 0.5;

    fn voter() -> BrakeVoter {
        BrakeVoter::new(2, TOLERANCE)
    }

    fn result(distance: f64) -> BrakeResult {
        BrakeResult {
            total_distance: distance,
            emergency_brake: false,
            valid_entry: true,
        }
    }

    fn consensus(outcome: VotingOutcome<BrakeResult>) -> BrakeResult {
        match outcome {
            VotingOutcome::Consensus(d) => d,
            other => panic!("expected consensus, got {other:?}"),
        }
    }

    #[test]
    fn three_agreeing_nodes_reach_consensus() {
        let out = voter().decide(&result(100.0), &[Some(result(100.1)), Some(result(99.9))]);
        assert!((consensus(out).total_distance - 100.0).abs() < 1e-9);
    }

    #[test]
    fn the_representative_is_the_median_not_the_first_value() {
        // Picking the first element would let the node that happens to
        // be lowest-numbered steer the published value.
        let out = voter().decide(&result(10.0), &[Some(result(10.4)), Some(result(9.7))]);
        assert!((consensus(out).total_distance - 10.0).abs() < 1e-9);

        let out = voter().decide(&result(9.7), &[Some(result(10.4)), Some(result(10.0))]);
        assert!((consensus(out).total_distance - 10.0).abs() < 1e-9);
    }

    #[test]
    fn a_two_against_two_split_never_decides() {
        // Four nodes, two values. Neither group reaches the strict
        // majority of three, so the outcome has to be Disagreement and
        // the caller routes to error management. Returning either half
        // would publish a value half the fabric rejects.
        let out = voter().decide(
            &result(100.0),
            &[Some(result(100.0)), Some(result(200.0)), Some(result(200.0))],
        );
        assert_eq!(out, VotingOutcome::Disagreement);
    }

    #[test]
    fn three_of_four_agreeing_still_decides() {
        let out = voter().decide(
            &result(100.0),
            &[Some(result(100.0)), Some(result(100.0)), Some(result(200.0))],
        );
        assert!((consensus(out).total_distance - 100.0).abs() < 1e-9);
    }

    #[test]
    fn too_few_responses_report_insufficient_quorum() {
        let out = voter().decide(&result(100.0), &[None, None]);
        assert_eq!(out, VotingOutcome::InsufficientQuorum);
    }

    #[test]
    fn the_tolerance_boundary_is_inclusive() {
        let v = voter();
        // Exactly at the tolerance the two values still count as one
        // measurement, one ulp beyond it they do not.
        let at = v.decide(&result(1.0), &[Some(result(1.0 + TOLERANCE)), None]);
        assert!(matches!(at, VotingOutcome::Consensus(_)));

        let beyond = v.decide(&result(1.0), &[Some(result(1.0 + TOLERANCE * 2.0)), None]);
        assert_eq!(beyond, VotingOutcome::Disagreement);
    }

    #[test]
    fn differing_flags_break_agreement_regardless_of_distance() {
        // The booleans are the actual safety decision, so a matching
        // distance must not paper over a diverging emergency flag.
        let mut flipped = result(100.0);
        flipped.emergency_brake = true;
        let out = voter().decide(&result(100.0), &[Some(flipped), None]);
        assert_eq!(out, VotingOutcome::Disagreement);

        let mut invalid = result(100.0);
        invalid.valid_entry = false;
        let out = voter().decide(&result(100.0), &[Some(invalid), None]);
        assert_eq!(out, VotingOutcome::Disagreement);
    }

    #[test]
    fn nan_agrees_with_nothing_including_itself() {
        // Two healthy nodes must still reach consensus while the third
        // delivers NaN, and two NaN values must not form a group.
        let out = voter().decide(
            &result(f64::NAN),
            &[Some(result(50.0)), Some(result(50.0))],
        );
        assert!((consensus(out).total_distance - 50.0).abs() < 1e-9);

        let out = voter().decide(&result(f64::NAN), &[Some(result(f64::NAN)), None]);
        assert_eq!(out, VotingOutcome::Disagreement);
    }

    #[test]
    fn dissenters_are_reported_by_roster_slot() {
        let decision = result(100.0);
        let peers = [Some(result(100.2)), None, Some(result(500.0))];
        let (own_dissented, dissenters) = voter().find_dissenters(&result(100.0), &peers, &decision);

        assert!(!own_dissented);
        assert_eq!(dissenters.as_slice(), &[2]);
    }

    #[test]
    fn own_dissent_is_reported_separately() {
        // The node has to notice that it is the outlier itself,
        // otherwise a locally corrupted result never reaches the
        // exclusion vote.
        let decision = result(100.0);
        let peers = [Some(result(100.0)), Some(result(100.0))];
        let (own_dissented, dissenters) = voter().find_dissenters(&result(900.0), &peers, &decision);

        assert!(own_dissented);
        assert!(dissenters.is_empty());
    }

    #[test]
    fn a_silent_peer_is_not_a_dissenter() {
        let decision = result(100.0);
        let peers = [None, None];
        let (_, dissenters) = voter().find_dissenters(&result(100.0), &peers, &decision);
        assert!(dissenters.is_empty());
    }

    #[test]
    fn required_participants_is_reported_unchanged() {
        assert_eq!(BrakeVoter::new(3, 1.0).required_participants(), 3);
    }

    #[test]
    #[should_panic(expected = "min_participants")]
    fn zero_participants_is_rejected_at_construction() {
        BrakeVoter::new(0, 1.0);
    }

    #[test]
    #[should_panic(expected = "distance_tolerance")]
    fn a_nan_tolerance_is_rejected_at_construction() {
        // A NaN tolerance would make every comparison false and turn
        // the voter into a permanent Disagreement source.
        BrakeVoter::new(2, f64::NAN);
    }

    #[test]
    fn brake_result_round_trips_on_the_wire() {
        let value = BrakeResult {
            total_distance: 1234.5,
            emergency_brake: true,
            valid_entry: false,
        };
        let mut buf = [0u8; BrakeResult::WIRE_SIZE];
        let mut w = WireWriter::new(&mut buf);
        value.to_wire(&mut w);
        assert_eq!(w.written(), BrakeResult::WIRE_SIZE);

        let mut r = WireReader::new(&buf);
        assert_eq!(BrakeResult::from_wire(&mut r), Ok(value));
    }

    #[test]
    fn a_non_boolean_flag_byte_is_rejected() {
        // Guards the strict boolean codec end to end: a corrupted flag
        // byte must fail the decode instead of being read as true.
        let mut buf = [0u8; BrakeResult::WIRE_SIZE];
        buf[8] = 0x02;
        let mut r = WireReader::new(&buf);
        assert_eq!(BrakeResult::from_wire(&mut r), Err(PayloadError::Invalid));
    }
}

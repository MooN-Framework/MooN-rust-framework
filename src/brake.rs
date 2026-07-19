use heapless::Vec;

use crate::input::braking_curve::BrakeResult;
use crate::sys_state::traits::{CyclePayload, Voter, VotingOutcome};
use crate::sys_state::wire::{PayloadError, WireReader, WireWriter};

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

/// Compile-time Obergrenze fuer die Anzahl gleichzeitig votender Teilnehmer.
/// Deckt bis zu 8 Nodes (eigener + 7 Peers). Deckt sich mit der Kapazitaet
/// der PeerMask (u8, 8 Bit).
const MAX_PARTICIPANTS: usize = 8;

/// K-oo-M Voter fuer `BrakeResult`.
///
/// Uebereinstimmung ist definiert als:
///   - `emergency_brake` exakt gleich,
///   - `valid_entry` exakt gleich,
///   - `|a.total_distance - b.total_distance| <= distance_tolerance`.
///
/// Anmerkung fuer SIL-2: Toleranz-Aequivalenz ist nicht transitiv.
/// `distance_tolerance` muss deutlich unter der zu erwartenden Divergenz
/// zwischen fehlerfreien Nodes liegen — sonst ist ein Consensus-Ergebnis
/// nicht eindeutig einer Fehlerklasse zuzuordnen. Fuer den finalen Nachweis
/// ggf. auf `0.0` (exakte Gleichheit) setzen.
#[derive(Debug, Clone, Copy)]
pub struct BrakeVoter {
    /// K in K-oo-M: minimale Anzahl uebereinstimmender Ergebnisse.
    pub required: u8,
    /// Erlaubte Abweichung in Metern.
    pub distance_tolerance: f64,
}

impl BrakeVoter {
    pub fn new(required: u8, distance_tolerance: f64) -> Self {
        assert!(required >= 1, "required muss >= 1 sein");
        assert!(
            distance_tolerance >= 0.0 && distance_tolerance.is_finite(),
            "distance_tolerance muss endlich und nicht-negativ sein"
        );
        Self { required, distance_tolerance }
    }

    /// Vergleicht zwei Ergebnisse gemaess der Toleranz-Regel.
    fn agree(&self, a: &BrakeResult, b: &BrakeResult) -> bool {
        if a.emergency_brake != b.emergency_brake || a.valid_entry != b.valid_entry {
            return false;
        }
        // NaN in `total_distance` bricht Consensus: `NaN != NaN`, damit wird
        // ein Node mit NaN-Ergebnis nie in eine Uebereinstimmungsgruppe
        // aufgenommen. Das ist gewuenscht — NaN heisst "Berechnung kaputt".
        if a.total_distance.is_nan() || b.total_distance.is_nan() {
            return false;
        }
        (a.total_distance - b.total_distance).abs() <= self.distance_tolerance
    }

    /// Wahl des Consensus-Vertreters aus einer uebereinstimmenden Gruppe.
    ///
    /// Bei Toleranz > 0 liegen die Werte einer Gruppe nicht exakt gleich; wir
    /// waehlen den Median. Deterministisch, gegen einzelne Ausreisser robust,
    /// und fuer die Sicherheitsargumentation transparent.
    fn representative(&self, group: &[BrakeResult]) -> BrakeResult {
        let mut distances: Vec<f64, MAX_PARTICIPANTS> = Vec::new();
        for r in group {
            let _ = distances.push(r.total_distance);
        }
        // partial_cmp reicht: NaN wurde in `agree` bereits ausgeschlossen.
        distances.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        let median = distances[distances.len() / 2];

        BrakeResult {
            total_distance: median,
            // Bool-Flags sind innerhalb einer Uebereinstimmungsgruppe
            // per Definition (agree()) identisch — einfach uebernehmen.
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
        // Alle vorliegenden Ergebnisse in einen stack-Puffer sammeln.
        let mut all: Vec<BrakeResult, MAX_PARTICIPANTS> = Vec::new();
        let _ = all.push(*own);
        for p in peers.iter().flatten() {
            // Bei Fehlkonfiguration (peers > MAX_PARTICIPANTS-1) laeuft der
            // Puffer voll; das ignorierte Ergebnis fuehrt hoechstens zu
            // Disagreement, nie zu falschem Consensus.
            let _ = all.push(*p);
        }

        if (all.len() as u8) < self.required {
            return VotingOutcome::InsufficientQuorum;
        }

        // Fuer jeden Kandidaten pruefen, wie viele Ergebnisse mit ihm
        // uebereinstimmen. Erste Gruppe, die die Schwelle erreicht, gewinnt.
        for candidate in all.iter() {
            let mut group: Vec<BrakeResult, MAX_PARTICIPANTS> = Vec::new();
            for other in all.iter() {
                if self.agree(candidate, other) {
                    let _ = group.push(*other);
                }
            }
            if (group.len() as u8) >= self.required {
                return VotingOutcome::Consensus(self.representative(&group));
            }
        }

        VotingOutcome::Disagreement
    }
}

// -------------------------------------------------------------
// Tests
// -------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn r(d: f64, eb: bool, ve: bool) -> BrakeResult {
        BrakeResult { total_distance: d, emergency_brake: eb, valid_entry: ve }
    }

    #[test]
    fn wire_roundtrip() {
        let mut buf = [0u8; 10];
        let orig = r(123.456, true, false);

        let mut w = WireWriter::new(&mut buf);
        orig.to_wire(&mut w);
        assert_eq!(w.written(), BrakeResult::WIRE_SIZE);

        let mut rd = WireReader::new(&buf);
        let back = BrakeResult::from_wire(&mut rd).unwrap();
        assert_eq!(orig, back);
    }

    #[test]
    fn wire_rejects_invalid_bool() {
        let mut buf = [0u8; 10];
        buf[8] = 0x02;
        let mut rd = WireReader::new(&buf);
        assert_eq!(BrakeResult::from_wire(&mut rd), Err(PayloadError::Invalid));
    }

    #[test]
    fn wire_rejects_too_short() {
        let buf = [0u8; 5];
        let mut rd = WireReader::new(&buf);
        assert_eq!(BrakeResult::from_wire(&mut rd), Err(PayloadError::TooShort));
    }

    fn voter_2oo3() -> BrakeVoter {
        BrakeVoter::new(2, 0.5)
    }

    #[test]
    fn two_of_three_agree_within_tolerance() {
        let v = voter_2oo3();
        let own = r(100.0, true, true);
        let peers = [Some(r(100.3, true, true)), Some(r(200.0, true, true))];
        match v.decide(&own, &peers) {
            VotingOutcome::Consensus(d) => {
                assert_eq!(d.emergency_brake, true);
                assert_eq!(d.valid_entry, true);
                assert!((d.total_distance - 100.3).abs() < 1e-9);
            }
            other => panic!("expected Consensus, got {:?}", other),
        }
    }

    #[test]
    fn peers_agree_even_when_own_disagrees() {
        let v = voter_2oo3();
        let own = r(100.0, true, true);
        let peers = [Some(r(100.0, false, true)), Some(r(100.0, false, true))];
        match v.decide(&own, &peers) {
            VotingOutcome::Consensus(d) => assert_eq!(d.emergency_brake, false),
            other => panic!("expected Consensus, got {:?}", other),
        }
    }

    #[test]
    fn all_disagree() {
        let v = voter_2oo3();
        let own = r(100.0, true, true);
        let peers = [Some(r(200.0, true, true)), Some(r(300.0, true, true))];
        assert!(matches!(v.decide(&own, &peers), VotingOutcome::Disagreement));
    }

    #[test]
    fn insufficient_quorum_when_peers_missing() {
        let v = voter_2oo3();
        let own = r(100.0, true, true);
        let peers = [None, None];
        assert!(matches!(
            v.decide(&own, &peers),
            VotingOutcome::InsufficientQuorum
        ));
    }

    #[test]
    fn nan_never_agrees() {
        let v = voter_2oo3();
        let own = r(f64::NAN, true, true);
        let peers = [Some(r(100.0, true, true)), Some(r(100.1, true, true))];
        match v.decide(&own, &peers) {
            VotingOutcome::Consensus(d) => assert!(!d.total_distance.is_nan()),
            other => panic!("expected Consensus without NaN node, got {:?}", other),
        }
    }
}
use core::fmt;

use crate::framework::wire::{PayloadError, WireReader, WireWriter};

/// Nutzwert, den jeder Node pro Zyklus berechnet und mit den Peers teilt.
/// Fixed-size Wire-Repraesentation ist Pflicht: deterministischer Speicher
/// im sicherheitsrelevanten Pfad.
pub trait CyclePayload: Copy + PartialEq {
    const WIRE_SIZE: usize;

    fn to_wire(&self, w: &mut WireWriter<'_>);
    fn from_wire(r: &mut WireReader<'_>) -> Result<Self, PayloadError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VotingOutcome<D> {
    Consensus(D),
    Disagreement,
    InsufficientQuorum,
}

/// Voting-Regel: aus eigenem Ergebnis + Peer-Ergebnissen eine Entscheidung.
pub trait Voter {
    type Payload: CyclePayload;
    type Decision: Copy;

    /// `peers[i] == None`: Peer i hat rechtzeitig nichts geliefert.
    fn decide(
        &self,
        own: &Self::Payload,
        peers: &[Option<Self::Payload>],
    ) -> VotingOutcome<Self::Decision>;

    /// Mindestanzahl vorliegender Antworten (inkl. eigener), damit `decide`
    /// nicht sofort `InsufficientQuorum` liefert. Bei 2oo3: 2.
    fn required_participants(&self) -> u8;
}

/// Anwendungsspezifische Berechnung: aus Rohdaten (Input) den Wert
/// erzeugen, der im Zyklus mit den Peers geteilt wird.
///
/// Zustandsbehaftet erlaubt (Kalibrierung, Caches, Historie), aber
/// deterministisch: gleicher Zustand + gleicher Input -> gleicher Output.
pub trait Computation {
    type Input: Copy;
    type Payload: CyclePayload;
    type Error: fmt::Debug;

    fn compute(&mut self, input: Self::Input) -> Result<Self::Payload, Self::Error>;
}

/// Empfaenger der vom Voter beschlossenen Entscheidung.
/// Wird pro Zyklus einmal aufgerufen, wenn Consensus erreicht ist.
pub trait DecisionSink {
    type Decision;

    fn publish(&mut self, decision: &Self::Decision);
}
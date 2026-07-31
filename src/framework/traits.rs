use core::fmt;

use crate::framework::wire::{PayloadError, WireReader, WireWriter};

/// Nutzwert, den jeder Node pro Zyklus berechnet und mit den Peers teilt.
/// Fixed-size Wire-Repraesentation ist Pflicht: deterministischer Speicher
/// im sicherheitsrelevanten Pfad.
pub trait CyclePayload: Copy + PartialEq + core::fmt::Debug {
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

    /// Nach `decide` mit `Consensus` aufzurufen. Identifiziert Werte, die
    /// nicht mit der Consensus-Entscheidung uebereinstimmen.
    ///
    /// Rueckgabe:
    /// - `own_dissented`: true wenn der eigene Wert von der Consensus-
    ///   Entscheidung abwich. In diesem Fall ist der eigene Node der
    ///   Ausreisser und sollte sich isolieren.
    /// - `peer_dissenter_indices`: Indizes in den urspruenglichen
    ///   `peers`-Slice, deren Wert von der Consensus-Entscheidung abwich.
    ///
    /// Wird ausschliesslich nach `Consensus` aufgerufen — bei
    /// `Disagreement` oder `InsufficientQuorum` gibt es keine Referenz
    /// zum Vergleichen.
    fn find_dissenters(
        &self,
        own: &Self::Payload,
        peers: &[Option<Self::Payload>],
        decision: &Self::Decision,
    ) -> (bool, heapless::Vec<u8, 16>);
}

/// Anwendungsspezifische Berechnung: aus Rohdaten (Input) den Wert
/// erzeugen, der im Zyklus mit den Peers geteilt wird.
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

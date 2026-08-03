use crate::framework::config::MAX_DISSENTERS;
use crate::framework::wire::{PayloadError, WireReader, WireWriter};
use core::fmt;
use heapless::Vec;

/// Value each node computes and shares per cycle. Wire size is fixed for
/// deterministic memory use on safety-critical paths.
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

/// Voting rule: reduce own + peer values to a decision.
pub trait Voter {
    type Payload: CyclePayload;
    type Decision: Copy;

    /// `peers[i] == None` means peer i sent nothing in time.
    fn decide(
        &self,
        own: &Self::Payload,
        peers: &[Option<Self::Payload>],
    ) -> VotingOutcome<Self::Decision>;

    /// Minimum responses (including own) before `decide` short-circuits to
    /// `InsufficientQuorum`.
    fn required_participants(&self) -> u8;

    /// Called after `decide` returned `Consensus`. Returns:
    /// - `own_dissented`: our value disagreed with the consensus.
    /// - dissenting peer indices into the original `peers` slice.
    fn find_dissenters(
        &self,
        own: &Self::Payload,
        peers: &[Option<Self::Payload>],
        decision: &Self::Decision,
    ) -> (bool, Vec<u8, MAX_DISSENTERS>);
}

/// Domain-specific computation: raw inputs -> shareable payload.
pub trait Computation {
    type Input: Copy;
    type Payload: CyclePayload;
    type Error: fmt::Debug;

    fn compute(&mut self, input: Self::Input) -> Result<Self::Payload, Self::Error>;
}

/// Recipient of the voting decision. Called once per cycle on consensus.
pub trait DecisionSink {
    type Decision;

    fn publish(&mut self, decision: &Self::Decision);
}

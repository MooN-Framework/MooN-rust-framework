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

pub trait Corruptible: CyclePayload {
    fn corrupt(&mut self);
}

#[cfg(not(feature = "diagnostic"))]
impl<T: CyclePayload> Corruptible for T {
    fn corrupt(&mut self) {}
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
    type Input: Copy + CyclePayload;
    type Payload: CyclePayload;
    type Error: fmt::Debug;

    fn compute(&mut self, input: Self::Input) -> Result<Self::Payload, Self::Error>;

    /// Divergence gate on sensor inputs. Returns true when `own` and
    /// `peer` are close enough to be treated as the same physical
    /// measurement. Called during the ShareInputs phase; a returned
    /// `false` for any non-Lost peer routes the cycle through
    /// ErrorManagement so the divergent sensor gets excluded.
    fn inputs_agree(&self, own: &Self::Input, peer: &Self::Input) -> bool;
}

// ... alles davor unverändert ...

/// Verdict returned by the sink for each voted decision, evaluated on
/// every node (not only the publisher). `Failsafe` routes the whole
/// system to fail-stop; the publisher still publishes the current
/// decision so the physical actuator receives the safe reaction (e.g.
/// emergency brake) before the runner terminates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkVerdict {
    Deliver,
    Failsafe,
}

/// Recipient of the voting decision. Called once per cycle on consensus.
///
/// Lifecycle hooks separate the two terminal-ish events:
///
/// - `on_isolation`: this node is out of consensus (e.g. wrong CRC,
///   state-sync minority) but the *fabric* keeps operating with the
///   remaining nodes. The local actuator wiring should freeze on the
///   last delivered value or hand off to a peer's publisher.
/// - `on_failsafe`: system-wide fail-stop. The actuator must be driven
///   into its safe state. Called exactly once, right before the runner
///   exits.
pub trait DecisionSink {
    type Decision;

    /// Domain-specific safety gate. Every node calls this after voting
    /// consensus to decide whether the decision is safe to deliver. The
    /// default assumes the decision is always deliverable; override for
    /// safety-critical domains.
    fn evaluate(&self, _decision: &Self::Decision) -> SinkVerdict {
        SinkVerdict::Deliver
    }

    /// Push the decision to the actuator/log/diagnostic wiring. Only the
    /// designated publisher calls this in the normal path.
    fn publish(&mut self, decision: &Self::Decision);

    /// This node is entering `Isolation`. System operation continues on
    /// the remaining nodes. Default is no-op.
    fn on_isolation(&mut self) {}

    /// System-wide fail-stop is being triggered. Default is no-op.
    fn on_failsafe(&mut self) {}
}

/// Power-on self-test. Called once from the `Startup` phase before the
/// node joins the fabric. Any `Err` routes the node straight to Failsafe.
///
/// Kept trivially small on purpose — a domain implementation can wire
/// arbitrary checks (deterministic-compute vectors, sensor sanity,
/// memory patterns, watchdog probes) behind `run`.
pub trait SelfTest {
    type Error: fmt::Debug;

    fn run(&mut self) -> Result<(), Self::Error>;
}

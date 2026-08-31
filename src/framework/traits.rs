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

/// Source of per-cycle sensor input. Called once per cycle from
/// `handle_read_inputs`. Implementations may block on hardware, poll a
/// ring buffer written by a background thread, or return a latched
/// value — the runner treats the returned value as the cycle's input
/// and forwards it to `Computation::compute` after `inputs_agree`.
///
/// `read` is fallible so a broken sensor path never gets silently
/// papered over. Any `Err` routes the cycle through the state
/// machine's `InputSourceFailed` transition, which lands in Failsafe
/// — the sink's `on_failsafe` fires so the actuator gets driven into
/// its safe state. Domains that want softer semantics (e.g. re-use the
/// last-known-good value on a transient miss) must express that
/// explicitly inside their `read` impl by returning `Ok(last_known)`.
/// The framework never guesses.
///
/// `set` is a hook for external overrides (diagnostic input injection,
/// externally-driven simulations). Infallible on purpose — a diagnostic
/// push either lands or it doesn't, there's no domain-level failure
/// mode to propagate. Default is a no-op; impls that only pull from
/// hardware can ignore it.
pub trait InputSource {
    type Input: CyclePayload;
    type Error: fmt::Debug;

    /// Fetch the current sensor value for this cycle. `Err` triggers
    /// Failsafe on this node.
    fn read(&mut self) -> Result<Self::Input, Self::Error>;

    /// Override the current input from outside (diagnostic staged
    /// input, external push). Default is a no-op — impls that only
    /// pull from hardware can ignore this.
    fn set(&mut self, _input: Self::Input) {}
}

/// Trivial `InputSource` that just latches a value and returns it every
/// cycle. Matches the original runner behaviour where a single
/// `C::Input` field was polled each cycle. Infallible: a purely
/// in-memory latch has no way to fail, so `Error = Infallible` and
/// `read` always returns `Ok`.
#[derive(Debug, Clone, Copy)]
pub struct LatchedInput<T: CyclePayload> {
    current: T,
}

impl<T: CyclePayload> LatchedInput<T> {
    pub fn new(initial: T) -> Self {
        Self { current: initial }
    }

    pub fn current(&self) -> T {
        self.current
    }
}

impl<T: CyclePayload> InputSource for LatchedInput<T> {
    type Input = T;
    type Error = core::convert::Infallible;

    fn read(&mut self) -> Result<T, Self::Error> {
        Ok(self.current)
    }

    fn set(&mut self, input: T) {
        self.current = input;
    }
}

/// Domain-specific application state that participates in the
/// SystemStateCrc and SystemStateSync exchanges.
///
/// Any bytes returned by `to_wire` are folded into the system-state CRC
/// on every node, so a divergence in application state raises the same
/// `CrcDivergent` verdict as a divergence in roster or sequence
/// number. During SystemStateSync the same bytes are shipped inside the
/// snapshot payload, so a rejoining or newly promoted node can adopt
/// the sender's state via `ApplicationStateProvider::apply`.
///
/// `WIRE_SIZE` is checked at compile time against
/// `MAX_APPLICATION_DATA_SIZE` in `framework::config`.
pub trait ApplicationData: Copy + PartialEq + fmt::Debug {
    const WIRE_SIZE: usize;

    fn to_wire(&self, w: &mut WireWriter<'_>);
    fn from_wire(r: &mut WireReader<'_>) -> Result<Self, PayloadError>;
}

/// Owner of the application state that gets CRC'd and synced.
///
/// `snapshot` is called at CrcExchange (to hash into the CRC) and, on
/// the sender side, at SystemStateSync (to broadcast). `apply` is
/// called on the receiver side of SystemStateSync once the majority
/// snapshot has been picked, so the local application state adopts the
/// fabric-agreed values.
///
/// Use `NoApplicationData` + `NoAppState` for use cases without any
/// per-node domain state that needs syncing.
pub trait ApplicationStateProvider {
    type Data: ApplicationData;

    fn snapshot(&self) -> Self::Data;
    fn apply(&mut self, data: &Self::Data);
}

/// Zero-sized application data. Use when the domain has no state that
/// needs to participate in CRC / state sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NoApplicationData;

impl ApplicationData for NoApplicationData {
    const WIRE_SIZE: usize = 0;

    fn to_wire(&self, _w: &mut WireWriter<'_>) {}

    fn from_wire(_r: &mut WireReader<'_>) -> Result<Self, PayloadError> {
        Ok(Self)
    }
}

/// Provider that carries no application state — `snapshot` always
/// returns the unit value, `apply` is a no-op.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoAppState;

impl ApplicationStateProvider for NoAppState {
    type Data = NoApplicationData;

    fn snapshot(&self) -> NoApplicationData {
        NoApplicationData
    }

    fn apply(&mut self, _data: &NoApplicationData) {}
}

#[cfg(test)]
mod input_source_tests {
    //! Tests for the fallible `InputSource` contract. Covers:
    //! - `LatchedInput` (infallible impl) never returns Err.
    //! - A failing `InputSource` produces `Err` on read that the
    //!   caller (runner) can match on.
    //! - The state machine has the `(ReadInputs, InputSourceFailed)
    //!   => Failsafe` transition wired up.
    use super::*;
    use crate::framework::state_machine::{NodeState, StateEvent};
    use crate::framework::wire::{PayloadError, WireReader, WireWriter};

    /// Minimal CyclePayload for isolating InputSource behaviour from
    /// the domain types. One u32 field so the wire round-trip is
    /// non-trivial but doesn't pull in the whole brake module.
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct DummyInput(u32);

    impl CyclePayload for DummyInput {
        const WIRE_SIZE: usize = 4;

        fn to_wire(&self, w: &mut WireWriter<'_>) {
            w.push_u32(self.0);
        }

        fn from_wire(r: &mut WireReader<'_>) -> Result<Self, PayloadError> {
            Ok(Self(r.read_u32()?))
        }
    }

    /// Impl that always fails — models a permanently dead sensor.
    struct AlwaysFailInput;

    #[derive(Debug)]
    struct SensorGone;

    impl InputSource for AlwaysFailInput {
        type Input = DummyInput;
        type Error = SensorGone;

        fn read(&mut self) -> Result<DummyInput, SensorGone> {
            Err(SensorGone)
        }
    }

    #[test]
    fn latched_input_never_errs() {
        let mut src = LatchedInput::new(DummyInput(42));
        // Ok is guaranteed at the type level via Infallible, but a
        // runtime check reads well and documents the contract.
        assert_eq!(src.read().expect("infallible"), DummyInput(42));
        // set() overrides — the next read returns the new value.
        src.set(DummyInput(99));
        assert_eq!(src.read().expect("infallible"), DummyInput(99));
    }

    #[test]
    fn always_fail_input_returns_err() {
        // The runner match arm in handle_read_inputs pattern-matches
        // this Err and produces StateEvent::InputSourceFailed. Here
        // we just verify the trait side: the Err surfaces.
        let mut src = AlwaysFailInput;
        assert!(src.read().is_err());
    }

    #[test]
    fn read_inputs_transitions_to_failsafe_on_source_error() {
        // The critical safety guarantee: the state machine must route
        // InputSourceFailed to Failsafe. Verified against the actual
        // transition table so a future refactor can't quietly redirect
        // this event somewhere softer (e.g. Isolation).
        assert_eq!(
            NodeState::ReadInputs.next(StateEvent::InputSourceFailed),
            NodeState::Failsafe,
        );
    }

    #[test]
    fn read_inputs_transitions_to_share_inputs_on_ok() {
        // Companion regression guard for the happy path — ensures
        // we didn't accidentally break the normal flow while wiring
        // up the error path.
        assert_eq!(
            NodeState::ReadInputs.next(StateEvent::InputsRead),
            NodeState::ShareInputs,
        );
    }
}

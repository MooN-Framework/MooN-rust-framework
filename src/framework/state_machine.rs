/// System-wide operational mode. Set by the runner as the fault-tolerance
/// buffer of the fabric changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemState {
    Startup,
    Operational,
    Degraded,
    Failsafe,
}

/// Per-node lifecycle state. Wire values are stable — protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeState {
    Startup = 0x01,
    InitSync = 0x02,
    CycleSync = 0x03,
    ReadInputs = 0x04,
    ShareInputs = 0x0E,
    ShareResult = 0x05,
    SendAck = 0x06,
    PublishResult = 0x07,
    ErrorManagement = 0x08,
    Isolation = 0x09,
    ClockSync = 0x0A,
    ResyncLostPeer = 0x0B,
    SystemStateCrcExchange = 0x0C,
    SystemStateSync = 0x0D,
    Failsafe = 0xFF,
}

/// Events emitted by the phase handlers, consumed by `NodeState::next`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateEvent {
    SelfTestOk,
    SelfTestErr,
    InitialSyncOk,
    InitialSyncTimeout,
    ClockSyncOk,
    ClockSyncTimeout,
    CycleSyncOk,
    CycleSyncTimeout,
    InputsRead,
    /// All non-Lost peer inputs arrived and passed the divergence gate.
    InputsShared,
    /// ShareInputs timed out (at least one non-Lost peer silent).
    ShareInputsTimeout,
    /// At least one peer input failed `Computation::inputs_agree`.
    InputsDivergent,
    ResultShared,
    ShareResultTimeout,
    AckReceived,
    AckTimeout,
    ResultPublished,
    ResyncDue,

    GoResyncLostPeer,
    ResyncLostPeerTimeout,
    ResyncLostPeerOk,
    /// Consensus reached but at least one peer diverged. Routed through
    /// ErrorManagement so the divergent peer can be reconfigured centrally.
    DissenterDetected,
    StateOk,
    StateDiverged,

    CrcOk,
    CrcDivergent,

    SystemStateSyncOk,
    SystemStateSyncTimeout,
    SystemStateSyncMinority,
    /// Exclusion vote timed out: at least one peer we did NOT propose to
    /// exclude failed to reply (Rule 2b) — failsafe.
    StateTimeout,
    TooFewNodes,
    Fault,
    SelfExcluded,
    /// `InputSource::read` returned `Err`. The runner routes this to
    /// Failsafe so the sink drives the actuator into its safe state —
    /// domains that want softer semantics for transient sensor faults
    /// must return `Ok(last_known)` from their `read` impl instead.
    InputSourceFailed,
    /// A healthy peer's frame carried a `node_state` of `ErrorManagement`
    /// while we were still in an earlier in-cycle phase. Rendezvous rule:
    /// follow the peer forward so both healthy nodes reach the exclusion
    /// vote in the same cycle, instead of both timing out in different
    /// phases and going into Failsafe independently.
    PeerInError,
}

impl NodeState {
    /// Pure transition function. Unmapped `(state, event)` pairs fall
    /// through to `Failsafe` (fail-stop on unexpected events).
    pub fn next(self, event: StateEvent) -> NodeState {
        use NodeState::*;
        use StateEvent::*;

        match (self, event) {
            (Startup, SelfTestOk) => InitSync,
            (Startup, SelfTestErr) => Failsafe,

            (InitSync, InitialSyncOk) => ClockSync,
            (InitSync, GoResyncLostPeer) => ResyncLostPeer,
            (InitSync, InitialSyncTimeout) => Failsafe,

            (ClockSync, ClockSyncOk) => ReadInputs,
            (ClockSync, ClockSyncTimeout) => Failsafe,

            (CycleSync, CycleSyncOk) => ReadInputs,
            (CycleSync, CycleSyncTimeout) => ErrorManagement,

            (ReadInputs, InputsRead) => ShareInputs,
            (ReadInputs, InputSourceFailed) => Failsafe,

            (ShareInputs, InputsShared) => ShareResult,
            (ShareInputs, ShareInputsTimeout) => ErrorManagement,
            (ShareInputs, InputsDivergent) => ErrorManagement,
            (ShareInputs, PeerInError) => ErrorManagement,
            (ShareInputs, SelfExcluded) => Isolation,

            (ShareResult, ResultShared) => SendAck,
            (ShareResult, ShareResultTimeout) => ErrorManagement,
            (ShareResult, PeerInError) => ErrorManagement,

            (SendAck, AckReceived) => SystemStateCrcExchange,
            (SendAck, AckTimeout) => ErrorManagement,
            (SendAck, PeerInError) => ErrorManagement,

            (SystemStateCrcExchange, CrcOk) => PublishResult,
            (SystemStateCrcExchange, CrcDivergent) => ErrorManagement,
            (SystemStateCrcExchange, PeerInError) => ErrorManagement,
            (SystemStateCrcExchange,  SelfExcluded) => Isolation,

            (PublishResult, ResultPublished) => CycleSync,
            (PublishResult, ResyncDue) => ClockSync,
            (PublishResult, GoResyncLostPeer) => ResyncLostPeer,
            (PublishResult, DissenterDetected) => ErrorManagement,
            (PublishResult, StateDiverged) => Failsafe,
            (PublishResult, SelfExcluded) => Isolation,
            
            (ResyncLostPeer, ResyncLostPeerTimeout) => ErrorManagement,
            (ResyncLostPeer, ResyncLostPeerOk) => SystemStateSync,

            (SystemStateSync, SystemStateSyncOk) => ClockSync,
            // SystemStateSync failures go to Failsafe rather than
            // Isolation: Isolation is a silent state that stops
            // broadcast but never triggers the sink's emergency-brake
            // hook, which is dangerous in a safety context. If a
            // rejoin fails — whether because peers are unreachable
            // (Timeout) or because our own snapshot disagreed with
            // the majority (Minority) — we can't safely keep running,
            // so we go hard-stop instead of quietly parking.
            (SystemStateSync, SystemStateSyncTimeout) => Failsafe,
            (SystemStateSync, SystemStateSyncMinority) => Failsafe,

            (ErrorManagement, StateOk) => CycleSync,
            (ErrorManagement, StateDiverged) => Failsafe,
            (ErrorManagement, StateTimeout) => Failsafe,
            (ErrorManagement, TooFewNodes) => Failsafe,
            (ErrorManagement, SelfExcluded) => Isolation,

            (Failsafe, _) => Failsafe,
            (_, Fault) => Failsafe,
            _ => Failsafe,
        }
    }

    #[inline]
    pub fn to_wire(self) -> u8 {
        self as u8
    }

    /// Decode from wire byte. Rejects unknown discriminants.
    #[inline]
    pub fn from_wire(v: u8) -> Result<Self, InvalidNodeState> {
        use NodeState::*;
        Ok(match v {
            0x01 => Startup,
            0x02 => InitSync,
            0x03 => CycleSync,
            0x04 => ReadInputs,
            0x0E => ShareInputs,
            0x05 => ShareResult,
            0x06 => SendAck,
            0x07 => PublishResult,
            0x08 => ErrorManagement,
            0x09 => Isolation,
            0x0A => ClockSync,
            0x0B => ResyncLostPeer,
            0x0C => SystemStateCrcExchange,
            0x0D => SystemStateSync,
            0xFF => Failsafe,
            other => return Err(InvalidNodeState(other)),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidNodeState(pub u8);

impl core::fmt::Display for InvalidNodeState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "invalid NodeState wire value: 0x{:02X}", self.0)
    }
}

impl std::error::Error for InvalidNodeState {}

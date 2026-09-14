//! Node lifecycle and the transition function that drives it.
//!
//! [`NodeState::next`] is the single place where control flow is
//! decided. It is a pure function with no side effects, and every pair
//! it does not name explicitly falls through to
//! [`NodeState::Failsafe`]: an unexpected event in a safety context is
//! a reason to stop, not to improvise.
//!
//! A healthy cycle runs CycleSync, ReadInputs, ShareInputs,
//! ShareResult, SendAck, SystemStateCrcExchange, PublishResult and back
//! to CycleSync. Everything else is startup, resynchronisation, or a
//! fault path.
//!
//! [`NodeState::Isolation`] and [`NodeState::Failsafe`] are terminal by
//! design. Isolation has no outgoing edge at all: a node that has been
//! voted out stays out until an external restart brings it back through
//! the normal rejoin path.
//!
//! Wire values are part of the protocol version and must not be
//! renumbered.

/// Per-node lifecycle state. Wire values are stable — protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeState {
    /// Power-on self-test.
    Startup = 0x01,
    /// Peer discovery until the configured node count is present.
    InitSync = 0x02,
    /// Cycle barrier: wait for every non-lost peer's beacon.
    CycleSync = 0x03,
    /// Read the local sensor input for this cycle.
    ReadInputs = 0x04,
    /// Share the input and gate for input divergence.
    ShareInputs = 0x0E,
    /// Share the computation result.
    ShareResult = 0x05,
    /// Exchange acks, which attest what was received and nominate a
    /// publisher.
    SendAck = 0x06,
    /// Exchange system-state CRCs before anything is published.
    SystemStateCrcExchange = 0x0C,
    /// Vote, then publish on the elected publisher.
    PublishResult = 0x07,
    /// Exchange exclusion proposals and apply the confirmed ones.
    ErrorManagement = 0x08,
    /// Excluded from consensus but not fail-stop: stop broadcasting and
    /// let the remaining fabric drive the actuator. Terminal, an
    /// external restart is required to return to service.
    Isolation = 0x09,
    /// Cristian's-algorithm clock synchronisation round.
    ClockSync = 0x0A,
    /// Readmission handshake with a peer that is coming back.
    ResyncLostPeer = 0x0B,
    /// Snapshot exchange that brings a readmitted node back in step.
    SystemStateSync = 0x0D,
    /// Fail-stop. Terminal, absorbs every event.
    Failsafe = 0xFF,
}

/// Events emitted by the phase handlers, consumed by `NodeState::next`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateEvent {
    /// Power-on self-test passed.
    SelfTestOk,
    /// Power-on self-test failed. Only ever raised from `Startup`.
    SelfTestErr,
    /// Discovery closed with a roster the configuration does not allow,
    /// e.g. the wrong node count. Distinct from a timeout: peers were
    /// reachable, they just do not add up to a valid fabric, so there is
    /// nothing to wait for and nothing to vote on.
    DiscoveryInconsistent,
    /// All configured nodes discovered.
    InitialSyncOk,
    /// Discovery did not complete before its timeout.
    InitialSyncTimeout,
    /// Clock offsets established for every reachable peer.
    ClockSyncOk,
    /// No usable clock offset within the timeout.
    ClockSyncTimeout,
    /// Every non-lost peer reached the cycle barrier.
    CycleSyncOk,
    /// At least one non-lost peer missed the barrier.
    CycleSyncTimeout,
    /// Local sensor input read successfully.
    InputsRead,
    /// All non-Lost peer inputs arrived and passed the divergence gate.
    InputsShared,
    /// ShareInputs timed out (at least one non-Lost peer silent).
    ShareInputsTimeout,
    /// At least one peer input failed `Computation::inputs_agree`.
    InputsDivergent,
    /// All non-lost peer results arrived.
    ResultShared,
    /// At least one non-lost peer's result did not arrive.
    ShareResultTimeout,
    /// All non-lost peer acks arrived.
    AckReceived,
    /// At least one non-lost peer's ack did not arrive.
    AckTimeout,
    /// The cycle completed and the publisher delivered the decision.
    ResultPublished,
    /// The configured resync interval elapsed; run a clock sync before
    /// the next cycle.
    ResyncDue,

    /// A peer is rejoining and needs the readmission handshake.
    GoResyncLostPeer,
    /// The rejoining peer did not complete the handshake in time.
    ResyncLostPeerTimeout,
    /// The readmission handshake completed.
    ResyncLostPeerOk,
    /// Consensus reached but at least one peer diverged. Routed through
    /// ErrorManagement so the divergent peer can be reconfigured centrally.
    DissenterDetected,
    /// Error management finished and the fabric is consistent again.
    StateOk,
    /// System state diverged beyond repair.
    StateDiverged,

    /// Every node reported the same system-state CRC.
    CrcOk,
    /// At least one node reported a different system-state CRC.
    CrcDivergent,

    /// Snapshot exchange completed and the local state was adopted.
    SystemStateSyncOk,
    /// Snapshot exchange did not complete in time.
    SystemStateSyncTimeout,
    /// Our own snapshot was outvoted by the majority.
    SystemStateSyncMinority,
    /// Exclusion vote timed out: at least one peer we did NOT propose to
    /// exclude failed to reply (Rule 2b) — failsafe.
    StateTimeout,
    /// Applying the exclusions would drop the fabric below the safety
    /// floor.
    TooFewNodes,
    /// Unrecoverable local error, e.g. a failed send.
    Fault,
    /// A majority of peers named this node for exclusion.
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
            (InitSync, DiscoveryInconsistent) => Failsafe,
            (InitSync, GoResyncLostPeer) => ResyncLostPeer,
            (InitSync, InitialSyncTimeout) => Failsafe,

            (ClockSync, ClockSyncOk) => CycleSync,
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
            (ResyncLostPeer, DiscoveryInconsistent) => Failsafe,

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
            (ErrorManagement, StateTimeout) => Failsafe,
            (ErrorManagement, TooFewNodes) => Failsafe,
            (ErrorManagement, SelfExcluded) => Isolation,

            (Failsafe, _) => Failsafe,
            (_, Fault) => Failsafe,
            _ => Failsafe,
        }
    }

    /// Wire byte for this state.
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

/// A wire byte that does not name any known [`NodeState`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidNodeState(
    /// The offending byte.
    pub u8,
);

impl core::fmt::Display for InvalidNodeState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "invalid NodeState wire value: 0x{:02X}", self.0)
    }
}

impl std::error::Error for InvalidNodeState {}

#[cfg(test)]
mod transition_tests {
    //! `NodeState::next` is the single place where the protocol's
    //! control flow is decided, so it is pinned exhaustively here:
    //! every mapped edge as a table, the fail-stop default for every
    //! unmapped pair, and the wire codec in both directions.
    use super::NodeState::*;
    use super::StateEvent::*;
    use super::*;

    /// All states that exist on the wire, in discriminant order.
    const ALL_STATES: &[NodeState] = &[
        Startup,
        InitSync,
        CycleSync,
        ReadInputs,
        ShareInputs,
        ShareResult,
        SendAck,
        PublishResult,
        ErrorManagement,
        Isolation,
        ClockSync,
        ResyncLostPeer,
        SystemStateCrcExchange,
        SystemStateSync,
        Failsafe,
    ];

    /// Every mapped `(state, event) -> state` edge. Kept as data so a
    /// change to the transition table has to be made here too.
    const MAPPED_EDGES: &[(NodeState, StateEvent, NodeState)] = &[
        (Startup, SelfTestOk, InitSync),
        (Startup, SelfTestErr, Failsafe),
        (InitSync, InitialSyncOk, ClockSync),
        (InitSync, DiscoveryInconsistent, Failsafe),
        (InitSync, GoResyncLostPeer, ResyncLostPeer),
        (InitSync, InitialSyncTimeout, Failsafe),
        (ClockSync, ClockSyncOk, CycleSync),
        (ClockSync, ClockSyncTimeout, Failsafe),
        (CycleSync, CycleSyncOk, ReadInputs),
        (CycleSync, CycleSyncTimeout, ErrorManagement),
        (ReadInputs, InputsRead, ShareInputs),
        (ReadInputs, InputSourceFailed, Failsafe),
        (ShareInputs, InputsShared, ShareResult),
        (ShareInputs, ShareInputsTimeout, ErrorManagement),
        (ShareInputs, InputsDivergent, ErrorManagement),
        (ShareInputs, PeerInError, ErrorManagement),
        (ShareInputs, SelfExcluded, Isolation),
        (ShareResult, ResultShared, SendAck),
        (ShareResult, ShareResultTimeout, ErrorManagement),
        (ShareResult, PeerInError, ErrorManagement),
        (SendAck, AckReceived, SystemStateCrcExchange),
        (SendAck, AckTimeout, ErrorManagement),
        (SendAck, PeerInError, ErrorManagement),
        (SystemStateCrcExchange, CrcOk, PublishResult),
        (SystemStateCrcExchange, CrcDivergent, ErrorManagement),
        (SystemStateCrcExchange, PeerInError, ErrorManagement),
        (SystemStateCrcExchange, SelfExcluded, Isolation),
        (PublishResult, ResultPublished, CycleSync),
        (PublishResult, ResyncDue, ClockSync),
        (PublishResult, GoResyncLostPeer, ResyncLostPeer),
        (PublishResult, DissenterDetected, ErrorManagement),
        (PublishResult, StateDiverged, Failsafe),
        (PublishResult, SelfExcluded, Isolation),
        (ResyncLostPeer, ResyncLostPeerTimeout, ErrorManagement),
        (ResyncLostPeer, ResyncLostPeerOk, SystemStateSync),
        (ResyncLostPeer, DiscoveryInconsistent, Failsafe),
        (SystemStateSync, SystemStateSyncOk, ClockSync),
        (SystemStateSync, SystemStateSyncTimeout, Failsafe),
        (SystemStateSync, SystemStateSyncMinority, Failsafe),
        (ErrorManagement, StateOk, CycleSync),
        (ErrorManagement, StateTimeout, Failsafe),
        (ErrorManagement, TooFewNodes, Failsafe),
        (ErrorManagement, SelfExcluded, Isolation),
    ];

    /// Every event variant, used to probe the unmapped default.
    const ALL_EVENTS: &[StateEvent] = &[
        SelfTestOk,
        SelfTestErr,
        DiscoveryInconsistent,
        InitialSyncOk,
        InitialSyncTimeout,
        ClockSyncOk,
        ClockSyncTimeout,
        CycleSyncOk,
        CycleSyncTimeout,
        InputsRead,
        InputsShared,
        ShareInputsTimeout,
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
        DissenterDetected,
        StateOk,
        StateDiverged,
        CrcOk,
        CrcDivergent,
        SystemStateSyncOk,
        SystemStateSyncTimeout,
        SystemStateSyncMinority,
        StateTimeout,
        TooFewNodes,
        Fault,
        SelfExcluded,
        InputSourceFailed,
        PeerInError,
    ];

    #[test]
    fn every_mapped_edge_lands_where_the_table_says() {
        for &(from, event, expected) in MAPPED_EDGES {
            assert_eq!(
                from.next(event),
                expected,
                "edge ({from:?}, {event:?}) changed"
            );
        }
    }

    #[test]
    fn unmapped_pairs_fall_through_to_failsafe() {
        // Fail-stop default: anything the table does not name is an
        // unexpected event and must stop the node, never silently keep
        // the current state or pick a neighbouring one.
        for &state in ALL_STATES {
            for &event in ALL_EVENTS {
                let is_mapped = MAPPED_EDGES
                    .iter()
                    .any(|&(f, e, _)| f == state && e == event);
                if is_mapped {
                    continue;
                }
                assert_eq!(
                    state.next(event),
                    Failsafe,
                    "unmapped ({state:?}, {event:?}) must fail stop"
                );
            }
        }
    }

    #[test]
    fn fault_from_any_state_is_failsafe() {
        for &state in ALL_STATES {
            assert_eq!(state.next(Fault), Failsafe);
        }
    }

    #[test]
    fn failsafe_absorbs_every_event() {
        for &event in ALL_EVENTS {
            assert_eq!(Failsafe.next(event), Failsafe);
        }
    }

    #[test]
    fn isolation_is_a_dead_end_in_the_transition_table() {
        // Isolation has no outgoing edge at all. If one is ever added,
        // this test is the reminder that the rejoin path and the
        // diagnostic view need to be updated with it.
        assert!(!MAPPED_EDGES.iter().any(|&(f, _, _)| f == Isolation));
        for &event in ALL_EVENTS {
            assert_eq!(Isolation.next(event), Failsafe);
        }
    }

    #[test]
    fn wire_round_trip_for_every_state() {
        for &state in ALL_STATES {
            let byte = state.to_wire();
            assert_eq!(NodeState::from_wire(byte), Ok(state));
        }
    }

    #[test]
    fn wire_values_are_unique() {
        for (i, a) in ALL_STATES.iter().enumerate() {
            for b in ALL_STATES.iter().skip(i + 1) {
                assert_ne!(a.to_wire(), b.to_wire(), "{a:?} and {b:?} share a byte");
            }
        }
    }

    #[test]
    fn unknown_wire_values_are_rejected() {
        // The protocol version is the byte set, so every value outside
        // it has to be reported rather than mapped to a near neighbour.
        for v in 0u8..=255u8 {
            let known = ALL_STATES.iter().any(|s| s.to_wire() == v);
            match (known, NodeState::from_wire(v)) {
                (true, Ok(_)) => {}
                (false, Err(InvalidNodeState(got))) => assert_eq!(got, v),
                (_, other) => panic!("wire value {v:#04x} decoded as {other:?}"),
            }
        }
    }
}

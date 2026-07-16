// -------------------------------------------------------------
// state_machine.rs
// -------------------------------------------------------------

/// Wie viele erfolgreiche Zyklen ein Knoten in Probation
/// absolvieren muss, bevor er voll teilnimmt.
pub const PROBATION_CYCLES: u8 = 5;

/// Der äußere Zustand eines Voting-Knotens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Startup,
    Sync,
    Probation { cycles_remaining: u8 },
    Operational { phase: CyclePhase },
    Degraded { phase: CyclePhase, missing_peer: u8 },
    Failsafe,
}

/// Sub-Zustand innerhalb Operational und Degraded:
/// die aktuelle Phase des Voting-Zyklus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CyclePhase {
    CycleWait,
    ReadInputs,
    CalcCritical,
    SendResult,
    VoteResult,
    SendAck,
    VotePublisher,
    Publish,
}

/// Ereignisse, die einen Zustandsübergang auslösen.
#[derive(Debug, Clone, Copy)]
pub enum Event {
    SelfTestOk,
    SelfTestFailed,
    PeersFound { rejoining: bool },
    PeersTimeout,
    ProbationCycleOk,
    PhaseDone,
    PhaseTimeout { peer_id: u8 },
    ConsensusFailed,
    PeerReturned,
    SecondPeerLost,
    UnrecoverableError,
}

impl NodeState {
    /// Gib zum aktuellen Zustand + Ereignis den Folgezustand zurück.
    /// Failsafe ist eine Trap-State: keine Transition führt heraus.
    pub fn next(self, event: Event) -> Self {
        use Event::*;
        use NodeState::*;

        match (self, event) {
            // -------- Startup --------
            (Startup, SelfTestOk) => Sync,
            (Startup, SelfTestFailed) => Failsafe,

            // -------- Sync --------
            (Sync, PeersFound { rejoining: false }) => Operational {
                phase: CyclePhase::CycleWait,
            },
            (Sync, PeersFound { rejoining: true }) => Probation {
                cycles_remaining: PROBATION_CYCLES,
            },
            (Sync, PeersTimeout) => Failsafe,

            // -------- Probation --------
            (
                Probation {
                    cycles_remaining: 1,
                },
                ProbationCycleOk,
            ) => Operational {
                phase: CyclePhase::CycleWait,
            },
            (
                Probation {
                    cycles_remaining: n,
                },
                ProbationCycleOk,
            ) => Probation {
                cycles_remaining: n - 1,
            },
            (Probation { .. }, UnrecoverableError) => Failsafe,

            // -------- Operational (2oo3, alles läuft) --------
            (Operational { phase }, PhaseDone) => Operational {
                phase: phase.next(),
            },
            (Operational { phase }, PhaseTimeout { peer_id }) => Degraded {
                phase,
                missing_peer: peer_id,
            },
            (Operational { .. }, ConsensusFailed) => Failsafe,
            (Operational { .. }, UnrecoverableError) => Failsafe,

            // -------- Degraded (2oo2, ein Peer weg) --------
            (
                Degraded {
                    phase,
                    missing_peer,
                },
                PhaseDone,
            ) => Degraded {
                phase: phase.next(),
                missing_peer,
            },
            (Degraded { .. }, PeerReturned) => Operational {
                phase: CyclePhase::CycleWait,
            },
            (Degraded { .. }, SecondPeerLost) => Failsafe,
            (Degraded { .. }, ConsensusFailed) => Failsafe,
            (Degraded { .. }, UnrecoverableError) => Failsafe,

            // -------- Failsafe ist terminal --------
            (Failsafe, _) => Failsafe,

            // -------- Alles andere: Zustand halten --------
            (state, _) => state,
        }
    }

    pub fn category(&self) -> u8 {
        match self {
            NodeState::Startup => 0,
            NodeState::Sync => 1,
            NodeState::Probation { .. } => 2,
            NodeState::Operational { .. } => 3,
            NodeState::Degraded { .. } => 4,
            NodeState::Failsafe => 5,
        }
    }
}

impl CyclePhase {
    /// Nächste Phase im Zyklus. Publish → CycleWait ist der Loop.
    pub fn next(self) -> Self {
        use CyclePhase::*;
        match self {
            CycleWait => ReadInputs,
            ReadInputs => CalcCritical,
            CalcCritical => SendResult,
            SendResult => VoteResult,
            VoteResult => SendAck,
            SendAck => VotePublisher,
            VotePublisher => Publish,
            Publish => CycleWait,
        }
    }
}

// -------------------------------------------------------------
// Tests
// -------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_cluster_boots_into_operational() {
        let s = NodeState::Startup
            .next(Event::SelfTestOk)
            .next(Event::PeersFound { rejoining: false });
        assert_eq!(
            s,
            NodeState::Operational {
                phase: CyclePhase::CycleWait
            }
        );
    }

    #[test]
    fn rejoining_node_goes_through_probation() {
        let s = NodeState::Sync.next(Event::PeersFound { rejoining: true });
        assert!(matches!(s, NodeState::Probation { .. }));
    }

    #[test]
    fn probation_advances_and_promotes() {
        let mut s = NodeState::Probation {
            cycles_remaining: 2,
        };
        s = s.next(Event::ProbationCycleOk);
        assert_eq!(
            s,
            NodeState::Probation {
                cycles_remaining: 1
            }
        );
        s = s.next(Event::ProbationCycleOk);
        assert_eq!(
            s,
            NodeState::Operational {
                phase: CyclePhase::CycleWait
            }
        );
    }

    #[test]
    fn one_peer_lost_moves_to_degraded() {
        let op = NodeState::Operational {
            phase: CyclePhase::SendResult,
        };
        let s = op.next(Event::PhaseTimeout { peer_id: 2 });
        assert_eq!(
            s,
            NodeState::Degraded {
                phase: CyclePhase::SendResult,
                missing_peer: 2
            }
        );
    }

    #[test]
    fn failsafe_is_terminal() {
        let s = NodeState::Failsafe.next(Event::PeersFound { rejoining: false });
        assert_eq!(s, NodeState::Failsafe);
    }
}

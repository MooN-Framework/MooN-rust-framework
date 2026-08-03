#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemState {
    Startup,
    Operational,
    Degraded,
    Failsafe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeState {
    Startup = 0x01,
    InitSync = 0x02,
    CycleSync = 0x03,
    ReadInputs = 0x04,
    ShareResult = 0x05,
    SendACK = 0x06,
    PublishResult = 0x07,
    ErrorManagement = 0x08,
    Isolation = 0x09,
    PeerSync = 0x0A,
    Failsafe = 0xFF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateEvent {
    /* SelfTest */
    SelfTestOK,
    SelfTestErr,
    /* InitSync */
    InitialSyncOk,
    InitialSyncTimeout,
    /* PeerSync */
    PeerSyncOk,
    PeerSyncTimeout,
    /* CycleSync */
    CycleSyncOk,
    CycleSyncTimeout,
    /* Operational cycle */
    InputsRead,
    /* Result Events */
    ResultShared,
    ShareResultTimeout,
    /* Ack Events */
    AckReceived,
    AckTimeout,
    /* Publish Events */
    ResultPublished,
    ResyncDue,
    /// Consensus erreicht, aber mindestens ein Peer wich vom Consensus ab.
    /// Fuehrt nach ErrorManagement, damit die Health-Transition zentral
    /// angestossen und Peer ggf. isoliert werden kann.
    DissenterDetected,
    /* Error Management */
    StateOk,
    StateDiverged,
    /// Voting-Timeout in ErrorManagement: mindestens ein Peer, den WIR
    /// nicht ausschliessen wollen (also fuer uns "gesund"), hat seinen
    /// ExclusionProposal-Vote nicht rechtzeitig geliefert. Regel 2 (b):
    /// wenn gesunde Nodes untereinander die Kommunikation verlieren,
    /// ist das System nicht mehr verlaesslich → Failsafe.
    ///
    /// Silence eines Peers, den WIR selbst ausschliessen wollen, triggert
    /// diesen Event NICHT — die Aggregation im RunState behandelt das
    /// als bestaetigende Evidenz.
    StateTimeout,
    TooFewNodes,
    /* Generisch */
    Fault,
}

impl NodeState {
    pub fn next(self, event: StateEvent) -> NodeState {
        use NodeState::*;
        use StateEvent::*;

        match (self, event) {
            // --- Startup ---
            (Startup, SelfTestOK) => InitSync,
            (Startup, SelfTestErr) => Failsafe,

            // --- InitSync -> PeerSync ---
            (InitSync, InitialSyncOk) => PeerSync,
            (InitSync, InitialSyncTimeout) => Failsafe,

            // --- PeerSync -> ReadInputs ---
            (PeerSync, PeerSyncOk) => ReadInputs,
            (PeerSync, PeerSyncTimeout) => Failsafe,

            // --- CycleSync ---
            (CycleSync, CycleSyncOk) => ReadInputs,
            (CycleSync, CycleSyncTimeout) => ErrorManagement,

            // --- Operational cycle ---
            (ReadInputs, InputsRead) => ShareResult,
            (ShareResult, ShareResultTimeout) => ErrorManagement,
            (ShareResult, ResultShared) => SendACK,
            (SendACK, AckTimeout) => ErrorManagement,
            (SendACK, AckReceived) => PublishResult,

            // --- Publish: normal weiter, oder periodischer Resync, oder
            //     Dissenter erkannt (Rekonfiguration ueber ErrorManagement),
            //     oder keine Mehrheit moeglich (direkt Failsafe) ---
            (PublishResult, ResultPublished) => CycleSync,
            (PublishResult, ResyncDue) => PeerSync,
            (PublishResult, DissenterDetected) => ErrorManagement,
            (PublishResult, StateDiverged) => Failsafe,

            // --- Error management ---
            // Nach jeder Rekonfiguration zurueck ueber CycleSync — Barrier,
            // damit alle Nodes wieder am selben Zyklus-Tick aufsetzen.
            //
            // StateTimeout: Voting-Phase konnte nicht mit Konsens
            // abgeschlossen werden, weil ein von uns fuer gesund gehaltener
            // Peer nicht geantwortet hat → Failsafe (Regel 2b).
            (ErrorManagement, StateOk) => CycleSync,
            (ErrorManagement, StateDiverged) => Failsafe,
            (ErrorManagement, StateTimeout) => Failsafe,
            (ErrorManagement, TooFewNodes) => Failsafe,

            // --- Failsafe ist terminal ---
            (Failsafe, _) => Failsafe,

            // --- Fail-stop: alles Unerwartete ---
            (_, Fault) => Failsafe,
            _ => Failsafe,
        }
    }

    #[inline]
    pub fn to_wire(self) -> u8 {
        self as u8
    }

    #[inline]
    pub fn from_wire(v: u8) -> Result<Self, InvalidNodeState> {
        use NodeState::*;
        Ok(match v {
            0x01 => Startup,
            0x02 => InitSync,
            0x03 => CycleSync,
            0x04 => ReadInputs,
            0x05 => ShareResult,
            0x06 => SendACK,
            0x07 => PublishResult,
            0x08 => ErrorManagement,
            0x09 => Isolation,
            0x0A => PeerSync,
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
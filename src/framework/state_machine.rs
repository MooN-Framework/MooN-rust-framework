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
    Sync = 0x02,
    ReadInputs = 0x03,
    ShareResult = 0x04,
    SendACK = 0x05,
    PublishResult = 0x06,
    StateManagement = 0x07,
    Probation = 0x08,
    Failsafe = 0xFF,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateEvent {
    /* SelfTest */
    SelfTestOK,
    SelfTestErr,
    /* Sync */
    SyncDetectedReentry,
    InitialSyncOk,
    InitialSyncPeerTimeout,
    CycleSyncOk,
    CycleSyncTimeout,
    /* Operational cycle — Platzhalter, bitte final benennen */
    InputsRead,
    /* Result Events */
    ResultShared,
    ShareResultTimeout,
    /* Ack Events */
    AckReceived,
    AckTimeout,
    /* Publish Events */
    ResultPublished,
    /* State management / probation */
    StateOk,
    StateDiverged,
    ProbationPassed,
    ProbationFailed,
    /* Generisch */
    Fault,
}

impl NodeState {
    pub fn next(self, event: StateEvent) -> NodeState {
        use NodeState::*;
        use StateEvent::*;

        match (self, event) {
            // --- Startup ---
            (Startup, SelfTestOK) => Sync,
            (Startup, SelfTestErr) => Failsafe,

            // --- Sync ---
            (Sync, InitialSyncOk) | (Sync, CycleSyncOk) => ReadInputs,
            (Sync, InitialSyncPeerTimeout) | (Sync, CycleSyncTimeout) => Failsafe,
            (Sync, SyncDetectedReentry) => Sync,

            // --- Operational cycle ---
            (ReadInputs, InputsRead) => ShareResult,
            (ShareResult, ShareResultTimeout) => StateManagement,
            (ShareResult, ResultShared) => SendACK,
            (SendACK, AckReceived) => PublishResult,
            (PublishResult, ResultPublished) => Sync,

            // --- State management ---
            (StateManagement, StateOk) => ReadInputs,
            (StateManagement, StateDiverged) => Probation,

            // --- Probation ---
            (Probation, ProbationPassed) => Sync,
            (Probation, ProbationFailed) => Failsafe,

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
            0x02 => Sync,
            0x03 => ReadInputs,
            0x04 => ShareResult,
            0x05 => SendACK,
            0x06 => PublishResult,
            0x07 => StateManagement,
            0x08 => Probation,
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

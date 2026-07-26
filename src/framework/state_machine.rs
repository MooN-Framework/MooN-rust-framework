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
    StateManagement = 0x08,
    Probation = 0x09,
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
    /* CycleSync */
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
            (Startup, SelfTestOK) => InitSync,
            (Startup, SelfTestErr) => Failsafe,

            // --- InitSync ---
            (InitSync, InitialSyncOk) => ReadInputs,
            (InitSync, InitialSyncTimeout) => Failsafe,

            // CycleSync
            (CycleSync, CycleSyncOk) => ReadInputs,
            (CycleSync, CycleSyncTimeout) => StateManagement,

            // --- Operational cycle ---
            (ReadInputs, InputsRead) => ShareResult,
            (ShareResult, ShareResultTimeout) => StateManagement,
            (ShareResult, ResultShared) => SendACK,
            (SendACK, AckTimeout) => StateManagement,
            (SendACK, AckReceived) => PublishResult,
            (PublishResult, ResultPublished) => CycleSync,

            // --- State management ---
            (StateManagement, StateOk) => ReadInputs,
            (StateManagement, StateDiverged) => Probation,

            // --- Probation ---
            (Probation, ProbationPassed) => CycleSync,
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
            0x02 => InitSync,
            0x03 => CycleSync,
            0x04 => ReadInputs,
            0x05 => ShareResult,
            0x06 => SendACK,
            0x07 => PublishResult,
            0x08 => StateManagement,
            0x09 => Probation,
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

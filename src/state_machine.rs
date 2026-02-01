use std::fmt;

#[derive(PartialEq, Copy, Clone)]
pub enum StateMachine {
    InitialSync,
    CycleSync,
    CalcCritical,
    CalcCrc,
    VoterFetch,
    VoteLocally,
    PublishVote,
    FailSafe,
}

impl StateMachine {
    pub fn parse_state(s: &str) -> Option<Self> {
        match s {
            "InitialSync" => Some(StateMachine::InitialSync),
            "CycleSync" => Some(StateMachine::CycleSync),
            "CalcCritical" => Some(StateMachine::CalcCritical),
            "CalcCrc" => Some(StateMachine::CalcCrc),
            "VoterFetch" => Some(StateMachine::VoterFetch),
            "VoteLocally" => Some(StateMachine::VoteLocally),
            "PublishVote" => Some(StateMachine::PublishVote),
            "FailSafe" => Some(StateMachine::FailSafe),
            _ => None, // Return None if input string doesn't match any variant
        }
    }
}

impl fmt::Display for StateMachine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_str = match self {
            StateMachine::InitialSync => "InitialSync",
            StateMachine::CycleSync => "CycleSync",
            StateMachine::CalcCritical => "CalcCritical",
            StateMachine::CalcCrc => "CalcCrc",
            StateMachine::VoterFetch => "VoterFetch",
            StateMachine::VoteLocally => "VoteLocally",
            StateMachine::PublishVote => "PublishVote",
            StateMachine::FailSafe => "FailSafe",
        };
        write!(f, "{}", state_str)
    }
}

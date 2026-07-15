use std::fmt;

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum NodeState {
    Startup,
    InitialSync,
    CycleSync,
    CalcCritical,
    ExchangeCRC,
    Vote,
    ExchangeVote,
    PublishVote,
    ErrorHandling,
    Failsafe,
    Reset,
}

impl NodeState {
    pub fn parse_state(s: &str) -> Option<Self> {
        match s {
            "Startup" => Some(NodeState::Startup),
            "InitialSync" => Some(NodeState::InitialSync),
            "CycleSync" => Some(NodeState::CycleSync),
            "CalcCritical" => Some(NodeState::CalcCritical),
            "ExchangeCRC" => Some(NodeState::ExchangeCRC),
            "Vote" => Some(NodeState::Vote),
            "ExchangeVote" => Some(NodeState::ExchangeVote),
            "PublishVote" => Some(NodeState::PublishVote),
            "ErrorHandling" => Some(NodeState::ErrorHandling),
            "Failsafe" => Some(NodeState::Failsafe),
            "Reset" => Some(NodeState::Reset),
            _ => None, // Return None if input string doesn't match any variant
        }
    }

    pub fn get_next_state(self, last_state: NodeState, ok: bool) -> Self {
        use NodeState::*;

        match self {
            Startup => {
                if ok {
                    InitialSync
                } else {
                    Failsafe
                }
            }
            InitialSync => {
                if ok {
                    CalcCritical
                } else {
                    Failsafe
                }
            }
            CycleSync => {
                if ok {
                    CalcCritical
                } else {
                    ErrorHandling
                }
            }
            CalcCritical => {
                if ok {
                    ExchangeCRC
                } else {
                    ErrorHandling
                }
            }
            ExchangeCRC => {
                if ok {
                    Vote
                } else {
                    ErrorHandling
                }
            }
            Vote => {
                if ok {
                    ExchangeVote
                } else {
                    ErrorHandling
                }
            }
            ExchangeVote => {
                if ok {
                    PublishVote
                } else {
                    ErrorHandling
                }
            }
            PublishVote => {
                if ok {
                    Reset
                } else {
                    ErrorHandling
                }
            }
            Reset => CycleSync,
            ErrorHandling => {
                if ok {
                    last_state.get_next_state(self, true)
                } else {
                    Failsafe
                }
            }
            Failsafe => Failsafe,
        }
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_str = match self {
            NodeState::Startup => "Startup",
            NodeState::InitialSync => "InitialSync",
            NodeState::CycleSync => "CycleSync",
            NodeState::CalcCritical => "CalcCritical",
            NodeState::ExchangeCRC => "ExchangeCRC",
            NodeState::Vote => "Vote",
            NodeState::ExchangeVote => "ExchangeVote",
            NodeState::PublishVote => "PublishVote",
            NodeState::Reset => "Reset",
            NodeState::ErrorHandling => "ErrorHandling",
            NodeState::Failsafe => "Failsafe",
        };
        write!(f, "{}", state_str)
    }
}

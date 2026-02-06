use std::fmt;

#[derive(PartialEq, Copy, Clone)]
pub enum StateMachine {
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

impl StateMachine {
    pub fn parse_state(s: &str) -> Option<Self> {
        match s {
            "Startup" => Some(StateMachine::Startup),
            "InitialSync" => Some(StateMachine::InitialSync),
            "CycleSync" => Some(StateMachine::CycleSync),
            "CalcCritical" => Some(StateMachine::CalcCritical),
            "ExchangeCRC" => Some(StateMachine::ExchangeCRC),
            "Vote" => Some(StateMachine::Vote),
            "ExchangeVote" => Some(StateMachine::ExchangeVote),
            "PublishVote" => Some(StateMachine::PublishVote),
            "ErrorHandling" => Some(StateMachine::ErrorHandling),
            "Failsafe" => Some(StateMachine::Failsafe),
            "Reset" => Some(StateMachine::Reset),
            _ => None, // Return None if input string doesn't match any variant
        }
    }

    pub fn get_next_state(self, last_state: StateMachine, ok: bool) -> Self {
        use StateMachine::*;

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

impl fmt::Display for StateMachine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state_str = match self {
            StateMachine::Startup => "Startup",
            StateMachine::InitialSync => "InitialSync",
            StateMachine::CycleSync => "CycleSync",
            StateMachine::CalcCritical => "CalcCritical",
            StateMachine::ExchangeCRC => "ExchangeCRC",
            StateMachine::Vote => "Vote",
            StateMachine::ExchangeVote => "ExchangeVote",
            StateMachine::PublishVote => "PublishVote",
            StateMachine::Reset => "Reset",
            StateMachine::ErrorHandling => "ErrorHandling",
            StateMachine::Failsafe => "Failsafe",
        };
        write!(f, "{}", state_str)
    }
}

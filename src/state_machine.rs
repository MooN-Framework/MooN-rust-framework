use std::fmt;

#[derive(PartialEq, Copy, Clone)]
pub enum StateMachine {
    Startup,
    InitialSync,
    CycleSync,
    CalcCritical,
    ExchangeCRC,
    Vote,
    PublishVote,
    ErrorHandling,
    Failsafe,
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
            "PublishVote" => Some(StateMachine::PublishVote),
            "ErrorHandling" => Some(StateMachine::ErrorHandling),
            "Failsafe" => Some(StateMachine::Failsafe),
            _ => None, // Return None if input string doesn't match any variant
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
            StateMachine::PublishVote => "PublishVote",
            StateMachine::ErrorHandling => "ErrorHandling",
            StateMachine::Failsafe => "Failsafe",
        };
        write!(f, "{}", state_str)
    }
}


struct EvalState{
    state : StateMachine,
    result : bool
}

impl EvalState
{
    fn new(&self, state : StateMachine, result : bool)
    {
        self{state, result}
    }


}
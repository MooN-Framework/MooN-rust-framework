
enum DiagCmds {
     PrintReceivMessage = 0,
     InduceResultError = 1,
     InducePublisherError = 2,
     Reset = 3,
}

impl for DiagCmds {
    
    fn to_wire(self) -> u8 {
        self as u8
    }

    fn from_wire(v: u8) -> Result<Self, InvalidDiagCmd> {
        use DiagCmds::*;
        Ok(match v {
            0 => PrintReceivMessage,
            1 => InduceResultError,
            2 => InducePublisherError,
            3 => Reset,
            other => return Err(InvalidDiagCmd(other)),
        })
    }
}
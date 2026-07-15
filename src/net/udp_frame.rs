use crate::braking_curve::BrakeResult;
use crate::node_state::NodeState;
use crc32fast::Hasher;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UdpFrame {
    node_id: u8,
    epoch: u32,
    seq_num: u32,
    result: Option<BrakeResult>,
    node_state: NodeState,
    crc32: u32,
}

impl UdpFrame {
    pub fn new_state_frame(node_id: u8, epoch: u32, seq_num: u32, node_state: NodeState) -> Self {
        let mut frame = Self {
            node_id,
            epoch,
            seq_num,
            result: None,
            node_state,
            crc32: 0,
        };
        frame.crc32 = frame.compute_crc();
        frame
    }

    pub fn new_result_frame(
        node_id: u8,
        epoch: u32,
        seq_num: u32,
        result: BrakeResult,
        node_state: NodeState,
    ) -> Self {
        let mut frame = Self {
            node_id,
            epoch,
            seq_num,
            result: Some(result),
            node_state,
            crc32: 0,
        };
        frame.crc32 = frame.compute_crc();
        frame
    }

    fn compute_crc(&self) -> u32 {
        let mut h = Hasher::new();
        h.update(&[self.node_id]);
        h.update(&self.epoch.to_le_bytes());
        h.update(&self.seq_num.to_le_bytes());

        // Result mit reinhashen, wenn vorhanden
        match &self.result {
            Some(r) => {
                h.update(&[1]);
                h.update(&r.total_distance.to_le_bytes());
                h.update(&[r.emergency_brake as u8]);
                h.update(&[r.valid_entry as u8]);
            }
            None => {
                h.update(&[0]);
            }
        }

        h.update(&[self.node_state as u8]);
        h.finalize()
    }

    pub fn verify_frame(&self) -> bool {
        self.crc32 == self.compute_crc()
    }
}

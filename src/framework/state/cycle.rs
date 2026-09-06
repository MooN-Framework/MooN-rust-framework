use crate::framework::config::MAX_PEERS;
use crate::framework::traits::CyclePayload;
use heapless::Vec;

/// Peer ack payload: what the peer received this round + who it thinks
/// should publish.
#[derive(Debug, Clone, Copy)]
pub struct AckInfo {
    pub received_from: u8,
    pub publisher_candidate: u8,
}

/// Buffers cleared and reused each cycle.
///
/// Sizes are set once at discovery finalize via `resize`; slots stay
/// allocated for the process lifetime.
///
/// `I` is the sensor input payload (shared during ShareInputs). `P` is
/// the computation result payload (shared during ShareResult).
pub struct CycleState<P: CyclePayload, I: CyclePayload> {
    pub own_input: Option<I>,
    /// Result of `Computation::consolidate` over own + validated peer
    /// inputs. This is the value `compute` actually ran on, kept for
    /// logging and post-mortem traceability. `own_input` stays the raw
    /// local sensor value, since that is what went on the wire.
    pub consolidated_input: Option<I>,
    pub peer_inputs: Vec<Option<I>, MAX_PEERS>,
    pub own_result: Option<P>,
    pub peer_results: Vec<Option<P>, MAX_PEERS>,
    pub peer_acks: Vec<Option<AckInfo>, MAX_PEERS>,
    pub phase_deadline: u64,
}

impl<P: CyclePayload, I: CyclePayload> CycleState<P, I> {
    pub const fn empty() -> Self {
        Self {
            own_input: None,
            consolidated_input: None,
            peer_inputs: Vec::new(),
            own_result: None,
            peer_results: Vec::new(),
            peer_acks: Vec::new(),
            phase_deadline: 0,
        }
    }

    /// Set the per-peer slot count. Called once at discovery finalize.
    pub fn resize(&mut self, n: usize) {
        for _ in 0..n {
            let _ = self.peer_inputs.push(None);
            let _ = self.peer_results.push(None);
            let _ = self.peer_acks.push(None);
        }
    }

    /// Clear all per-peer slots for a new cycle.
    pub fn reset(&mut self, deadline: u64) {
        self.own_input = None;
        self.consolidated_input = None;
        self.own_result = None;
        for slot in self.peer_inputs.iter_mut() {
            *slot = None;
        }
        for slot in self.peer_results.iter_mut() {
            *slot = None;
        }
        for slot in self.peer_acks.iter_mut() {
            *slot = None;
        }
        self.phase_deadline = deadline;
    }
}

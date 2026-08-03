//! Cristian time synchronisation with multi-peer aggregation.
//!
//! Per round:
//! 1. Requester sends `TimeSyncReq { t1 }` where t1 is the local send time.
//! 2. Responder receives at t2, sends `TimeSyncResp { t1, t2, t3 }`.
//! 3. Requester receives at t4 and computes:
//!    ```text
//!    delay  = (t4 - t1) - (t3 - t2)
//!    offset = ((t2 - t1) + (t3 - t4)) / 2
//!    bound  = max(0, (delay - 2 * DT_MIN_NS) / 2)
//!    ```
//!
//! The sample with the smallest delay per peer is kept (NTP filtering).
//! Group convergence uses the median across all peers.

use crate::framework::traits::CyclePayload;
use crate::framework::wire::{Payload, UdpFrame};

/// Minimum physical one-way transmission time (ns). Conservative default
/// for gigabit ethernet with one switch hop; validate against target
/// hardware.
pub const DT_MIN_NS: u64 = 20_000;

/// Samples per peer. The best (smallest-delay) sample is retained.
pub const SAMPLES_PER_PEER: usize = 8;

/// A single Cristian measurement with all four timestamps.
#[derive(Debug, Clone, Copy)]
pub struct SyncSample {
    pub t1: u64,
    pub t2: u64,
    pub t3: u64,
    pub t4: u64,
}

impl SyncSample {
    /// Network round-trip time in ns, responder processing time removed.
    pub fn delay(&self) -> i64 {
        (self.t4 as i64 - self.t1 as i64) - (self.t3 as i64 - self.t2 as i64)
    }

    /// Estimated clock offset (peer_clock - self_clock) in ns.
    pub fn offset(&self) -> i64 {
        ((self.t2 as i64 - self.t1 as i64) + (self.t3 as i64 - self.t4 as i64)) / 2
    }

    /// Upper bound on the magnitude of the offset estimation error (ns).
    /// Zero on perfectly symmetric latency.
    pub fn error_bound(&self) -> i64 {
        let raw = (self.delay() - 2 * DT_MIN_NS as i64) / 2;
        if raw < 0 { 0 } else { raw }
    }
}

/// Aggregated per-peer sync result after the phase completes.
#[derive(Debug, Clone, Copy)]
pub struct PeerClock {
    pub peer_id: u8,
    pub offset_ns: i64,
    pub error_bound_ns: i64,
    pub samples_used: u32,
}

struct PeerSyncState {
    peer_id: u8,
    pending_t1: Option<u64>,
    samples_taken: u32,
    best: Option<SyncSample>,
}

impl PeerSyncState {
    fn new(peer_id: u8) -> Self {
        Self { peer_id, pending_t1: None, samples_taken: 0, best: None }
    }

    fn record_sample(&mut self, sample: SyncSample) {
        self.samples_taken = self.samples_taken.saturating_add(1);
        let keep = match self.best {
            None => true,
            Some(prev) => sample.delay() < prev.delay(),
        };
        if keep {
            self.best = Some(sample);
        }
        self.pending_t1 = None;
    }

    fn is_complete(&self) -> bool {
        self.samples_taken >= SAMPLES_PER_PEER as u32
    }

    fn as_peer_clock(&self) -> Option<PeerClock> {
        self.best.map(|s| PeerClock {
            peer_id: self.peer_id,
            offset_ns: s.offset(),
            error_bound_ns: s.error_bound(),
            samples_used: self.samples_taken,
        })
    }
}

/// Coordinator for time synchronisation with all known peers.
pub struct PeerSync {
    peers: Vec<PeerSyncState>,
}

impl PeerSync {
    pub fn new(peer_ids: &[u8]) -> Self {
        Self {
            peers: peer_ids.iter().map(|&id| PeerSyncState::new(id)).collect(),
        }
    }

    /// Note a request just sent so future responses can be matched.
    pub fn record_outgoing_request(&mut self, peer_id: u8, t1: u64) {
        if let Some(s) = self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            s.pending_t1 = Some(t1);
        }
    }

    /// True while a request to this peer is outstanding.
    pub fn has_pending(&self, peer_id: u8) -> bool {
        self.peers
            .iter()
            .find(|p| p.peer_id == peer_id)
            .and_then(|p| p.pending_t1)
            .is_some()
    }

    /// Clear the outstanding request for this peer (e.g. on response timeout).
    pub fn mark_timeout(&mut self, peer_id: u8) {
        if let Some(s) = self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            s.pending_t1 = None;
        }
    }

    /// Ingest a response. Rejects unknown peers, unmatched `t1`, and
    /// samples with implausible delay.
    pub fn on_response(&mut self, peer_id: u8, t1: u64, t2: u64, t3: u64, t4_local: u64) {
        let state = match self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            Some(s) => s,
            None => return,
        };
        match state.pending_t1 {
            Some(expected) if expected == t1 => {}
            _ => return,
        }
        let sample = SyncSample { t1, t2, t3, t4: t4_local };
        if sample.delay() < 0 {
            state.pending_t1 = None;
            return;
        }
        state.record_sample(sample);
    }

    /// True when every peer has reached the configured sample count.
    pub fn is_complete(&self) -> bool {
        !self.peers.is_empty() && self.peers.iter().all(|p| p.is_complete())
    }

    /// Extract the aggregated per-peer clocks. Meaningful only when
    /// `is_complete()`.
    pub fn finalize(&self) -> Vec<PeerClock> {
        self.peers.iter().filter_map(|p| p.as_peer_clock()).collect()
    }

    /// Median of the peer offsets plus our own clock (0). Correction to
    /// apply to the local clock to follow the group median.
    pub fn convergence_correction(&self) -> Option<i64> {
        let mut offsets: Vec<i64> = self
            .peers
            .iter()
            .filter_map(|p| p.as_peer_clock().map(|c| c.offset_ns))
            .collect();
        if offsets.is_empty() {
            return None;
        }
        offsets.push(0);
        offsets.sort_unstable();
        Some(offsets[offsets.len() / 2])
    }

    /// Largest error bound across all peers. Feeds the voter timeout.
    pub fn max_error_bound(&self) -> Option<i64> {
        self.peers
            .iter()
            .filter_map(|p| p.as_peer_clock())
            .map(|c| c.error_bound_ns)
            .max()
    }
}

/// Discriminated view over the time-sync payload variants extracted from
/// a received frame.
pub enum SyncFields {
    Request { peer_id: u8, t1: u64, t2_local: u64 },
    Response { peer_id: u8, t1: u64, t2: u64, t3: u64, t4_local: u64 },
}

/// Project a received frame down to the sync fields the coordinator uses.
pub fn extract_sync_fields<P: CyclePayload>(
    frame: &UdpFrame<P>,
    local_recv_ns: u64,
) -> Option<SyncFields> {
    match frame.payload() {
        Payload::TimeSyncReq { t1 } => Some(SyncFields::Request {
            peer_id: frame.node_id(),
            t1,
            t2_local: local_recv_ns,
        }),
        Payload::TimeSyncResp { t1, t2, t3 } => Some(SyncFields::Response {
            peer_id: frame.node_id(),
            t1,
            t2,
            t3,
            t4_local: local_recv_ns,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_delay_and_offset_symmetric() {
        let t1 = 1_000_000;
        let peer_offset = 1_000_000;
        let one_way = 100_000;
        let proc_time = 50_000;
        let t2 = t1 + one_way + peer_offset;
        let t3 = t2 + proc_time;
        let t4 = t3 - peer_offset + one_way;
        let s = SyncSample { t1, t2, t3, t4 };
        assert_eq!(s.delay(), 2 * one_way as i64);
        assert_eq!(s.offset(), peer_offset as i64);
    }

    #[test]
    fn error_bound_zero_at_two_dt_min() {
        let t1 = 0;
        let t2 = DT_MIN_NS;
        let t3 = t2 + 1000;
        let t4 = t3 + DT_MIN_NS;
        let s = SyncSample { t1, t2, t3, t4 };
        assert_eq!(s.error_bound(), 0);
    }

    #[test]
    fn on_response_rejects_unmatched_t1() {
        let mut ps = PeerSync::new(&[1]);
        ps.record_outgoing_request(1, 12345);
        ps.on_response(1, 99999, 100, 200, 300);
        assert_eq!(ps.peers[0].samples_taken, 0);
        assert!(ps.peers[0].best.is_none());
    }
}

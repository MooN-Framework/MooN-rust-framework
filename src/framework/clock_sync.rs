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
        if raw < 0 {
            0
        } else {
            raw
        }
    }
}

/// Aggregated per-peer clock sync result after the phase completes.
#[derive(Debug, Clone, Copy)]
pub struct PeerClock {
    pub peer_id: u8,
    pub offset_ns: i64,
    pub error_bound_ns: i64,
    pub samples_used: u32,
}

struct ClockSyncState {
    peer_id: u8,
    pending_t1: Option<u64>,
    samples_taken: u32,
    best: Option<SyncSample>,
    /// Nanosecond timestamp of the last activity for this peer — either
    /// an outgoing request we sent to them (via
    /// `record_outgoing_request`) or an incoming response we accepted
    /// (via `on_response`). Seeded to the phase-start timestamp so
    /// staleness has a well-defined origin for peers that never
    /// respond.
    last_activity_ns: u64,
    /// Set by `mark_unreachable_if_stale` when this peer has gone
    /// quiet for longer than the caller-supplied threshold. Once set,
    /// `is_complete` treats this peer as if it weren't in the required
    /// set — the framework will fall back on the normal missed-frame
    /// path to mark it Lost in a following cycle.
    unreachable: bool,
}

impl ClockSyncState {
    fn new(peer_id: u8, phase_start_ns: u64) -> Self {
        Self {
            peer_id,
            pending_t1: None,
            samples_taken: 0,
            best: None,
            last_activity_ns: phase_start_ns,
            unreachable: false,
        }
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
pub struct ClockSync {
    peers: Vec<ClockSyncState>,
}

impl ClockSync {
    /// `phase_start_ns` seeds every peer's `last_activity_ns` so
    /// staleness for peers that never respond has a well-defined
    /// origin. Pass the same monotonic-clock reading that the caller
    /// uses for the very first `record_outgoing_request`.
    pub fn new(peer_ids: &[u8], phase_start_ns: u64) -> Self {
        Self {
            peers: peer_ids
                .iter()
                .map(|&id| ClockSyncState::new(id, phase_start_ns))
                .collect(),
        }
    }

    /// Note a request just sent so future responses can be matched.
    /// `t1` doubles as the last-activity timestamp for the peer.
    pub fn record_outgoing_request(&mut self, peer_id: u8, t1: u64) {
        if let Some(s) = self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            s.pending_t1 = Some(t1);
            s.last_activity_ns = t1;
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

    /// Mark any peer that has been silent for longer than `threshold_ns`
    /// as unreachable so `is_complete` no longer waits on them. Idempotent
    /// — safe to call every loop iteration; once a peer is flagged we
    /// leave it flagged (a late response could still record a sample,
    /// which is harmless).
    ///
    /// A peer that has already gathered `SAMPLES_PER_PEER` samples is
    /// never flagged: it's already done, staleness after completion is
    /// expected.
    pub fn mark_unreachable_if_stale(&mut self, now_ns: u64, threshold_ns: u64) {
        for peer in self.peers.iter_mut() {
            if peer.unreachable || peer.is_complete() {
                continue;
            }
            let elapsed = now_ns.saturating_sub(peer.last_activity_ns);
            if elapsed > threshold_ns {
                peer.unreachable = true;
            }
        }
    }

    /// True when every peer we still expect to hear from has enough
    /// samples. Peers flagged `unreachable` are excluded from the
    /// required set. If ALL peers are unreachable, returns false — the
    /// caller's deadline path handles that as a normal timeout.
    pub fn is_complete(&self) -> bool {
        let mut has_responsive = false;
        for p in self.peers.iter() {
            if p.unreachable {
                continue;
            }
            has_responsive = true;
            if !p.is_complete() {
                return false;
            }
        }
        has_responsive
    }

    /// Peer IDs flagged unreachable during this phase. Diagnostic use —
    /// callers may want to log which peers were excluded from the sync.
    pub fn unreachable_peers(&self) -> Vec<u8> {
        self.peers
            .iter()
            .filter(|p| p.unreachable)
            .map(|p| p.peer_id)
            .collect()
    }

    /// Ingest a response. Rejects unknown peers, unmatched `t1`, and
    /// samples with implausible delay. `t4_local` also updates the
    /// peer's last-activity timestamp so `mark_unreachable_if_stale`
    /// sees the peer as fresh again.
    pub fn on_response(&mut self, peer_id: u8, t1: u64, t2: u64, t3: u64, t4_local: u64) {
        let state = match self.peers.iter_mut().find(|p| p.peer_id == peer_id) {
            Some(s) => s,
            None => return,
        };
        match state.pending_t1 {
            Some(expected) if expected == t1 => {}
            _ => return,
        }
        // Any well-formed response counts as activity, even one we
        // ultimately reject for negative delay — the peer clearly
        // wasn't silent.
        state.last_activity_ns = t4_local;
        let sample = SyncSample {
            t1,
            t2,
            t3,
            t4: t4_local,
        };
        if sample.delay() < 0 {
            state.pending_t1 = None;
            return;
        }
        state.record_sample(sample);
    }

    /// Extract the aggregated per-peer clocks. Meaningful only when
    /// `is_complete()`.
    pub fn finalize(&self) -> Vec<PeerClock> {
        self.peers
            .iter()
            .filter_map(|p| p.as_peer_clock())
            .collect()
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
    Request {
        peer_id: u8,
        t1: u64,
        t2_local: u64,
    },
    Response {
        peer_id: u8,
        t1: u64,
        t2: u64,
        t3: u64,
        t4_local: u64,
    },
}

/// Project a received frame down to the sync fields the coordinator uses.
pub fn extract_sync_fields<I: CyclePayload, R: CyclePayload>(
    frame: &UdpFrame<I, R>,
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
        let mut ps = ClockSync::new(&[1], 0);
        ps.record_outgoing_request(1, 12345);
        ps.on_response(1, 99999, 100, 200, 300);
        assert_eq!(ps.peers[0].samples_taken, 0);
        assert!(ps.peers[0].best.is_none());
    }

    // --- Liveness / unreachable-peer tests -------------------------
    // These cover the T21 fix: `ClockSync` must complete with a subset
    // of peers when one has been silent past `unreachable_threshold`,
    // rather than blocking until the phase deadline expires.

    /// Helper: push a full round-trip so a peer collects one sample
    /// with valid delay and a fresh `last_activity_ns = t4`.
    fn round_trip(ps: &mut ClockSync, peer_id: u8, t1: u64, one_way: u64, proc_time: u64) {
        ps.record_outgoing_request(peer_id, t1);
        let t2 = t1 + one_way;
        let t3 = t2 + proc_time;
        let t4 = t3 + one_way;
        ps.on_response(peer_id, t1, t2, t3, t4);
    }

    #[test]
    fn unreachable_peer_still_lets_sync_complete() {
        // Two peers: peer 1 gets all 8 samples, peer 2 goes silent.
        // Once we call `mark_unreachable_if_stale` past the threshold,
        // `is_complete` must return true instead of blocking on peer 2.
        let mut ps = ClockSync::new(&[1, 2], 0);
        for i in 0..SAMPLES_PER_PEER {
            let t1 = 1_000 + i as u64 * 1_000_000;
            round_trip(&mut ps, 1, t1, 100_000, 50_000);
        }
        // Peer 2 got nothing — still has last_activity_ns = 0.
        assert!(!ps.is_complete(), "before marking stale, peer 2 blocks completion");

        // Advance to well past the threshold. Peer 2 flagged; peer 1
        // is already complete so untouched.
        ps.mark_unreachable_if_stale(10_000_000_000, 100_000_000);
        assert!(ps.is_complete(), "peer 2 is now unreachable and skipped");
        assert_eq!(ps.unreachable_peers(), vec![2]);
    }

    #[test]
    fn responsive_peer_never_flagged_unreachable() {
        // Sanity: a peer that keeps responding within the threshold
        // must never be flagged, even after many iterations.
        let mut ps = ClockSync::new(&[1], 0);
        for i in 0..SAMPLES_PER_PEER {
            let t1 = 1_000 + i as u64 * 1_000_000;
            round_trip(&mut ps, 1, t1, 100_000, 50_000);
            // Call staleness check with a `now` that's only a few
            // hundred µs past the last t4 — well under the 100 ms
            // threshold.
            ps.mark_unreachable_if_stale(t1 + 500_000, 100_000_000);
        }
        assert!(ps.unreachable_peers().is_empty());
        assert!(ps.is_complete());
    }

    #[test]
    fn all_peers_unreachable_means_not_complete() {
        // Defensive: if every peer is silent, is_complete must NOT
        // return true (that would let us "sync" with nobody). The
        // caller's deadline path then handles this as a timeout.
        let mut ps = ClockSync::new(&[1, 2], 0);
        ps.mark_unreachable_if_stale(10_000_000_000, 100_000_000);
        assert_eq!(ps.unreachable_peers(), vec![1, 2]);
        assert!(!ps.is_complete(), "no responsive peer left → not complete");
    }

    #[test]
    fn completed_peer_immune_to_staleness() {
        // Regression: once a peer has 8 samples we're done with them.
        // A subsequent staleness sweep must not flip their state to
        // "unreachable" — that would confuse downstream consumers
        // reading `unreachable_peers()`.
        let mut ps = ClockSync::new(&[1], 0);
        for i in 0..SAMPLES_PER_PEER {
            let t1 = 1_000 + i as u64 * 1_000_000;
            round_trip(&mut ps, 1, t1, 100_000, 50_000);
        }
        assert!(ps.is_complete());
        // Massive gap — peer would be stale, but is_complete already
        // returned true.
        ps.mark_unreachable_if_stale(u64::MAX / 2, 100_000_000);
        assert!(ps.unreachable_peers().is_empty());
        assert!(ps.is_complete());
    }

    #[test]
    fn recorded_response_refreshes_activity() {
        // A response arriving *after* a staleness window is no longer
        // enough to un-flag the peer (unreachable is sticky) — this
        // test just verifies the timestamp bookkeeping: once a peer
        // responds, its last_activity_ns catches up so a *subsequent*
        // stale check with the same `now` no longer flags it.
        let mut ps = ClockSync::new(&[1], 0);
        // At t=200ms, peer would look stale (last_activity=0, gap=200ms).
        // But we send + receive at t1=150ms, t4=150.3ms → refresh.
        round_trip(&mut ps, 1, 150_000_000, 100_000, 50_000);
        // Now at t=200ms, gap since last_activity (~150.3ms) is ~50ms,
        // under a 100ms threshold → not flagged.
        ps.mark_unreachable_if_stale(200_000_000, 100_000_000);
        assert!(ps.unreachable_peers().is_empty());
    }
}

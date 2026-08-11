mod cycle;
mod observation;
mod peers;
mod voting;

use crate::framework::config::{ParticipantConfig, MAX_PEERS, MAX_TOTAL_NODES};
use crate::framework::peer_sync::PeerClock;
use crate::framework::state_machine::{NodeState, SystemState};
use crate::framework::traits::{CyclePayload, Voter, VotingOutcome};
use crate::framework::types::PeerMask;
use crate::framework::wire::SnapshotEntry;
use crc32fast::Hasher;
pub use cycle::{AckInfo, CycleState};
use heapless::Vec;
pub use observation::ObservationKind;
pub use peers::{DiscoveryError, PeerHealth, PeerInfo, PeerRoster};
use tracing::{info, warn};
pub use voting::ExclusionVotes;

fn health_wire(h: PeerHealth) -> u8 {
    match h {
        PeerHealth::Alive => 0,
        PeerHealth::Probation => 1,
        PeerHealth::Lost => 2,
    }
}

fn health_from_wire(w: u8) -> Result<PeerHealth, SnapshotApplyError> {
    match w {
        0 => Ok(PeerHealth::Alive),
        1 => Ok(PeerHealth::Probation),
        2 => Ok(PeerHealth::Lost),
        _ => Err(SnapshotApplyError::InvalidHealthWire),
    }
}

/// Update an existing `(peer_id, value)` entry in place or append a new one.
/// Used for `sync_snapshots` and `sync_acks` which stay unsorted and small.
fn upsert_by_id<T, const N: usize>(vec: &mut Vec<(u8, T), N>, peer_id: u8, value: T) {
    for entry in vec.iter_mut() {
        if entry.0 == peer_id {
            entry.1 = value;
            return;
        }
    }
    let _ = vec.push((peer_id, value));
}

pub fn crc_from_snapshot_fields(
    nominal: u8,
    min: u8,
    probation_cycles: u32,
    current_seq: u32,
    entries: &[SnapshotEntry],
) -> u32 {
    let mut h = Hasher::new();
    h.update(&[nominal, min]);
    h.update(&probation_cycles.to_le_bytes());
    h.update(&current_seq.to_le_bytes());
    let mut sorted: Vec<(u8, u8, u32), MAX_TOTAL_NODES> = Vec::new();
    for e in entries.iter().filter(|e| e.valid) {
        let _ = sorted.push((e.id, e.health, e.probation_cycles_ok));
    }
    sorted.sort_unstable_by_key(|(id, _, _)| *id);
    for (id, hw, cok) in sorted.iter() {
        h.update(&[*id, *hw]);
        h.update(&cok.to_le_bytes());
    }
    h.finalize()
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StoredSnapshot {
    pub nominal: u8,
    pub min: u8,
    pub probation_cycles: u32,
    pub current_seq: u32,
    pub entries: [SnapshotEntry; MAX_TOTAL_NODES],
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SnapshotApplyError {
    ConfigMismatch,
    SelfMarkedLost,
    UnknownPeerInSnapshot,
    InvalidHealthWire,
}

/// Full runtime state of one node. Composed of a peer roster, per-cycle
/// buffers, a distributed-observation tracker, exclusion-vote tracker,
/// clock offsets, and a small set of scalar fields.
pub struct RunState<V: Voter, I: CyclePayload> {
    own_id: u8,
    session_id: u64,
    voter: V,
    participants: ParticipantConfig,

    node_state: NodeState,
    system_state: SystemState,
    current_seq: u32,

    roster: PeerRoster,
    cycle: CycleState<V::Payload, I>,
    obs: ObservationKind,
    votes: ExclusionVotes,

    pending_exclusion_proposal: PeerMask,

    peer_clocks: Vec<PeerClock, MAX_PEERS>,
    sync_epsilon_ns: i64,
    sync_valid: bool,

    was_lost: bool,
    rejoin_seen: PeerMask,
    peer_rejoin_votes: Vec<Option<PeerMask>, MAX_PEERS>,
    pending_rejoin: PeerMask,
    self_probation_remaining: u32,

    peer_crcs: Vec<Option<u32>, MAX_PEERS>,

    needs_state_sync: bool,

    /// Snapshots collected during SystemStateSync, keyed by sender.
    sync_snapshots: Vec<(u8, StoredSnapshot), MAX_TOTAL_NODES>,

    /// Snapshot-Acks collected during SystemStateSync, keyed by receiver.
    sync_acks: Vec<(u8, u32), MAX_TOTAL_NODES>,

    last_decision: Option<VotingOutcome<V::Decision>>,
}

impl<V: Voter, I: CyclePayload> RunState<V, I> {
    pub fn new(own_id: u8, session_id: u64, voter: V, participants: ParticipantConfig) -> Self {
        Self {
            own_id,
            session_id,
            voter,
            participants,
            node_state: NodeState::Startup,
            system_state: SystemState::Startup,
            current_seq: 0,
            roster: PeerRoster::new(),
            cycle: CycleState::empty(),
            obs: ObservationKind::empty(),
            votes: ExclusionVotes::empty(),
            pending_exclusion_proposal: PeerMask::EMPTY,
            peer_clocks: Vec::new(),
            sync_epsilon_ns: 0,
            sync_valid: false,
            was_lost: false,
            rejoin_seen: PeerMask::EMPTY,
            peer_rejoin_votes: Vec::new(),
            pending_rejoin: PeerMask::EMPTY,
            self_probation_remaining: 0,
            needs_state_sync: false,
            sync_snapshots: Vec::new(),
            sync_acks: Vec::new(),
            last_decision: None,
            peer_crcs: Vec::new(),
        }
    }

    /// Look up a peer's slot index if we should record data for it.
    /// Returns `Ok(None)` when the peer is known but Lost — callers skip
    /// the recording silently.
    fn resolve_peer_slot(&self, peer_id: u8) -> Result<Option<usize>, DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        if self.roster.peers()[idx].health == PeerHealth::Lost {
            return Ok(None);
        }
        Ok(Some(idx))
    }

    /// Collect ids of non-Lost peers matching `predicate`. Used by the
    /// various `*_missing_*` methods to walk the roster once with a
    /// per-peer test.
    fn collect_peers_where<F>(&self, mut predicate: F) -> Vec<u8, MAX_PEERS>
    where
        F: FnMut(usize, &PeerInfo) -> bool,
    {
        let mut out: Vec<u8, MAX_PEERS> = Vec::new();
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if predicate(idx, peer) {
                let _ = out.push(peer.id);
            }
        }
        out
    }

    pub fn needs_state_sync(&self) -> bool {
        self.needs_state_sync
    }
    pub fn set_needs_state_sync(&mut self, v: bool) {
        self.needs_state_sync = v;
    }

    pub fn reset_state_sync_evidence(&mut self) {
        self.sync_snapshots.clear();
        self.sync_acks.clear();
    }

    pub fn record_sync_snapshot(&mut self, peer_id: u8, snap: StoredSnapshot) {
        upsert_by_id(&mut self.sync_snapshots, peer_id, snap);
    }

    pub fn record_sync_ack(&mut self, peer_id: u8, adopted_crc: u32) {
        upsert_by_id(&mut self.sync_acks, peer_id, adopted_crc);
    }

    pub fn compute_system_state_crc(&self) -> u32 {
        let (nom, min, pc, cs, entries) = self.build_snapshot();
        crc_from_snapshot_fields(nom, min, pc, cs, &entries)
    }

    pub fn sync_snapshots(&self) -> &[(u8, StoredSnapshot)] {
        &self.sync_snapshots
    }
    pub fn sync_acks(&self) -> &[(u8, u32)] {
        &self.sync_acks
    }

    /// Pick the majority snapshot from what we've collected.
    /// Returns (chosen_snapshot, minority_sender_ids) or None if empty.
    pub fn majority_snapshot(&self) -> Option<(StoredSnapshot, Vec<u8, MAX_TOTAL_NODES>)> {
        if self.sync_snapshots.is_empty() {
            return None;
        }
        let mut best_count = 0u8;
        let mut best_snap: Option<StoredSnapshot> = None;
        for (_, snap) in self.sync_snapshots.iter() {
            let count = self
                .sync_snapshots
                .iter()
                .filter(|(_, s)| s == snap)
                .count() as u8;
            if count > best_count {
                best_count = count;
                best_snap = Some(*snap);
            }
        }
        let winner = best_snap?;
        let mut minority: Vec<u8, MAX_TOTAL_NODES> = Vec::new();
        for (sender_id, snap) in self.sync_snapshots.iter() {
            if *snap != winner {
                let _ = minority.push(*sender_id);
            }
        }
        Some((winner, minority))
    }

    pub fn apply_snapshot(
        &mut self,
        nominal: u8,
        min: u8,
        probation_cycles: u32,
        current_seq: u32,
        entries: &[SnapshotEntry; MAX_TOTAL_NODES],
    ) -> Result<(), SnapshotApplyError> {
        if nominal != self.participants.nominal_participants
            || min != self.participants.min_participants
            || probation_cycles != self.participants.probation_cycles
        {
            return Err(SnapshotApplyError::ConfigMismatch);
        }
        self.current_seq = current_seq;
        for entry in entries.iter().filter(|e| e.valid) {
            let health = health_from_wire(entry.health)?;
            if entry.id == self.own_id {
                match health {
                    PeerHealth::Probation => {
                        self.self_probation_remaining =
                            probation_cycles.saturating_sub(entry.probation_cycles_ok);
                    }
                    PeerHealth::Alive => {
                        self.self_probation_remaining = 0;
                    }
                    PeerHealth::Lost => return Err(SnapshotApplyError::SelfMarkedLost),
                }
            } else {
                if !self
                    .roster
                    .set_peer_from_snapshot(entry.id, health, entry.probation_cycles_ok)
                {
                    return Err(SnapshotApplyError::UnknownPeerInSnapshot);
                }
            }
        }
        Ok(())
    }

    pub fn build_snapshot(&self) -> (u8, u8, u32, u32, [SnapshotEntry; MAX_TOTAL_NODES]) {
        let mut entries = [SnapshotEntry::default(); MAX_TOTAL_NODES];
        let own_health = if self.self_probation_remaining > 0 {
            PeerHealth::Probation
        } else {
            PeerHealth::Alive
        };
        let own_cycles_ok = if self.self_probation_remaining > 0 {
            self.participants
                .probation_cycles
                .saturating_sub(self.self_probation_remaining)
        } else {
            0
        };

        let mut collected: Vec<(u8, PeerHealth, u32), MAX_TOTAL_NODES> = Vec::new();
        let _ = collected.push((self.own_id, own_health, own_cycles_ok));
        for peer in self.roster.peers().iter() {
            let cok = if peer.health == PeerHealth::Probation {
                peer.probation_cycles_ok
            } else {
                0
            };
            let _ = collected.push((peer.id, peer.health, cok));
        }
        collected.sort_unstable_by_key(|(id, _, _)| *id);

        for (slot, (id, health, cok)) in entries.iter_mut().zip(collected.iter()) {
            slot.valid = true;
            slot.id = *id;
            slot.health = health_wire(*health);
            slot.probation_cycles_ok = *cok;
        }

        (
            self.participants.nominal_participants,
            self.participants.min_participants,
            self.participants.probation_cycles,
            self.current_seq,
            entries,
        )
    }

    /// For senders during SystemStateSync: check that every non-Lost
    /// receiver has ack'd with our own CRC. Returns true when done.
    pub fn all_receivers_acked_with(&self, own_crc: u32, receivers: &[u8]) -> bool {
        for rid in receivers.iter() {
            let matched = self
                .sync_acks
                .iter()
                .any(|(pid, crc)| pid == rid && *crc == own_crc);
            if !matched {
                return false;
            }
        }
        !receivers.is_empty()
    }

    /// Peers who have not yet ack'd during SystemStateSync (all non-Lost peers, filtered).
    pub fn peers_missing_sync_ack(&self) -> Vec<u8, MAX_PEERS> {
        self.collect_peers_where(|_, peer| !self.sync_acks.iter().any(|(pid, _)| *pid == peer.id))
    }

    /// Peers whose snapshot we haven't received (for receivers).
    pub fn peers_missing_snapshot(&self) -> Vec<u8, MAX_PEERS> {
        self.collect_peers_where(|_, peer| {
            !self.sync_snapshots.iter().any(|(pid, _)| *pid == peer.id)
        })
    }

    /// Log all fields that go into the system state CRC. For debugging
    /// CRC divergence — call on both nodes and diff the output.
    ///
    /// Draws its data from `build_snapshot` so the log stays byte-for-byte
    /// aligned with what actually flows into the CRC hash. Health is
    /// reported as the wire byte (0=Alive, 1=Probation, 2=Lost) so a diff
    /// across nodes shows exactly what the hasher saw.
    pub fn log_system_state_crc_contents(&self) {
        let (nominal, min, probation_cycles, current_seq, entries) = self.build_snapshot();
        let mut nodes: Vec<(u8, u8, u32), MAX_TOTAL_NODES> = Vec::new();
        for e in entries.iter().filter(|e| e.valid) {
            let _ = nodes.push((e.id, e.health, e.probation_cycles_ok));
        }
        info!(
            crc = crc_from_snapshot_fields(nominal, min, probation_cycles, current_seq, &entries),
            nominal,
            min,
            probation_cycles,
            current_seq,
            nodes = ?nodes.as_slice(),
            "system state crc contents"
        );
    }

    pub fn participants(&self) -> &ParticipantConfig {
        &self.participants
    }

    pub fn discovery_locked(&self) -> bool {
        self.roster.discovery_locked()
    }

    /// Register a newly discovered peer during InitSync.
    pub fn on_peer_discovered(&mut self, id: u8) -> Result<(), DiscoveryError> {
        self.roster
            .discover(id, self.own_id, self.participants.max_peers())
    }

    /// Close discovery, sort peers by id, size all per-peer buffers.
    pub fn finalize_discovery(&mut self) -> Result<(), DiscoveryError> {
        self.roster
            .finalize(self.own_id, self.participants.nominal_participants)?;
        let n = self.roster.peers().len();
        self.cycle.resize(n);
        self.obs.resize(n);
        self.votes.resize(n);
        // NEU: peer_rejoin_votes auf n füllen
        self.peer_rejoin_votes.clear();
        for _ in 0..n {
            let _ = self.peer_rejoin_votes.push(None);
        }
        self.peer_crcs.clear();
        for _ in 0..n {
            let _ = self.peer_crcs.push(None);
        }
        Ok(())
    }

    pub fn reset_crc_evidence(&mut self) {
        for slot in self.peer_crcs.iter_mut() {
            *slot = None;
        }
    }

    pub fn record_peer_crc(&mut self, peer_id: u8, crc: u32) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.peer_crcs[idx] = Some(crc);
        }
        Ok(())
    }

    pub fn healthy_peers_missing_crc(&self) -> Vec<u8, MAX_PEERS> {
        self.collect_peers_where(|idx, _| self.peer_crcs[idx].is_none())
    }
    pub fn cycle_peer_crc(&self, idx: usize) -> Option<u32> {
        self.peer_crcs.get(idx).and_then(|s| *s)
    }
    /// True iff own CRC matches every non-Lost peer's attestation.
    pub fn crc_unanimous(&self, own_crc: u32) -> bool {
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            match self.peer_crcs[idx] {
                Some(c) if c == own_crc => continue,
                _ => return false,
            }
        }
        true
    }

    /// Sammelt eigene + gesunde peer CRCs, ermittelt strikte Mehrheit.
    /// Wird von handle_system_state_crc genutzt: bei ermittelbarer
    /// Mehrheit werden alle Minority-Nodes exkludiert, sonst Failsafe.
    pub fn identify_crc_majority(&self, own_crc: u32) -> Option<u32> {
        let mut values: heapless::Vec<u32, MAX_TOTAL_NODES> = heapless::Vec::new();
        let _ = values.push(own_crc);
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if let Some(c) = self.peer_crcs[idx] {
                let _ = values.push(c);
            }
        }
        strict_majority(&values)
    }

    /// Peer-Ids deren gemeldete CRC nicht der uebergebenen Mehrheits-CRC
    /// entspricht. Nutzung: nach identify_crc_majority alle mit
    /// abweichender CRC in die eigene Exclusion-Proposal aufnehmen.
    pub fn peers_with_crc_other_than(&self, majority: u32) -> heapless::Vec<u8, MAX_PEERS> {
        let mut out: heapless::Vec<u8, MAX_PEERS> = heapless::Vec::new();
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if let Some(c) = self.peer_crcs[idx] {
                if c != majority {
                    let _ = out.push(peer.id);
                }
            }
        }
        out
    }

    pub fn peer_index(&self, id: u8) -> Option<usize> {
        self.roster.peer_index(id)
    }

    pub fn record_own_input(&mut self, input: I) {
        self.cycle.own_input = Some(input);
    }

    pub fn record_peer_input(&mut self, peer_id: u8, input: I) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.cycle.peer_inputs[idx] = Some(input);
        }
        Ok(())
    }

    pub fn own_input(&self) -> Option<I> {
        self.cycle.own_input
    }

    pub fn peer_inputs(&self) -> &[Option<I>] {
        &self.cycle.peer_inputs
    }

    /// Peers whose input we haven't received during ShareInputs.
    pub fn peers_missing_input(&self) -> Vec<u8, MAX_PEERS> {
        self.collect_peers_where(|idx, _| self.cycle.peer_inputs[idx].is_none())
    }

    pub fn record_own_result(&mut self, payload: V::Payload) {
        self.cycle.own_result = Some(payload);
    }

    pub fn record_peer_result(
        &mut self,
        peer_id: u8,
        payload: V::Payload,
    ) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.cycle.peer_results[idx] = Some(payload);
        }
        Ok(())
    }

    pub fn record_peer_ack(&mut self, peer_id: u8, ack: AckInfo) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.cycle.peer_acks[idx] = Some(ack);
        }
        Ok(())
    }

    /// Run the voter over own + non-Lost peer values.
    pub fn run_vote(&mut self) -> VotingOutcome<V::Decision> {
        let own = match self.cycle.own_result {
            Some(v) => v,
            None => return VotingOutcome::InsufficientQuorum,
        };

        // Collect Alive peer values only. Probation peer values don't count.
        let mut peer_values: Vec<Option<V::Payload>, MAX_PEERS> = Vec::new();
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Alive {
                let _ = peer_values.push(self.cycle.peer_results[idx]);
            }
        }

        let outcome = if self.self_in_probation() {
            // Self is in probation: own value must not influence the vote.
            // Pick the first present Alive peer value as the "own" anchor
            // that the voter operates on; contribute nothing extra.
            let anchor = peer_values.iter().flatten().copied().next();
            match anchor {
                Some(a) => {
                    // Remove the anchor from the peer set so it isn't counted
                    // twice, then hand the remainder to the voter as peers.
                    let mut anchor_removed = false;
                    let mut remainder: Vec<Option<V::Payload>, MAX_PEERS> = Vec::new();
                    for slot in peer_values.iter() {
                        if !anchor_removed {
                            if let Some(v) = slot {
                                if *v == a {
                                    anchor_removed = true;
                                    continue;
                                }
                            }
                        }
                        let _ = remainder.push(*slot);
                    }
                    self.voter.decide(&a, &remainder)
                }
                None => {
                    // No peer value at all — we can't vote without ourselves.
                    return VotingOutcome::InsufficientQuorum;
                }
            }
        } else {
            self.voter.decide(&own, &peer_values)
        };

        self.last_decision = Some(outcome);
        outcome
    }

    /// Reset all per-cycle buffers to start a fresh cycle. Also clears the
    /// pending exclusion proposal.
    pub fn start_new_cycle(&mut self, deadline: u64) {
        self.current_seq = self.current_seq.wrapping_add(1);
        self.cycle.reset(deadline);
        self.pending_exclusion_proposal = PeerMask::EMPTY;
        self.last_decision = None;
        self.rejoin_seen = PeerMask::EMPTY;
        for slot in self.peer_rejoin_votes.iter_mut() {
            *slot = None;
        }
    }

    /// Bitmask of peers currently expected to attend CycleSync (non-Lost).
    pub fn expected_sync_mask(&self) -> u8 {
        let mut mask = 0u8;
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health != PeerHealth::Lost {
                mask |= 1 << idx;
            }
        }
        mask
    }

    pub fn reset_cycle_sync_evidence(&mut self) {
        self.obs.reset_seen();
    }

    pub fn set_own_seen_bit(&mut self, peer_idx: usize) {
        let peers = self.roster.peers();
        if peer_idx < peers.len() && peers[peer_idx].health != PeerHealth::Lost {
            self.obs.own_seen.set(peer_idx);
        }
    }

    pub fn own_seen_mask(&self) -> PeerMask {
        self.obs.own_seen
    }

    pub fn record_peer_seen_mask(
        &mut self,
        peer_id: u8,
        mask: PeerMask,
    ) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.obs.peer_seen[idx] = Some(mask);
        }
        Ok(())
    }

    /// Update the pending exclusion proposal with peers attributed as
    /// missing this CycleSync phase. Majority rule; the proposal is the
    /// union of all attributions collected during the cycle.
    pub fn attribute_cycle_sync_missing(&mut self) {
        if let Some(mask) = observation::attribute_missing::<V::Payload>(
            &self.roster,
            observation::View::CycleSync {
                own_seen: self.obs.own_seen,
                peer_seen: &self.obs.peer_seen,
            },
            self.own_id,
        ) {
            self.pending_exclusion_proposal.0 |= mask.0;
        }
    }

    /// Update the pending exclusion proposal with peers attributed as
    /// missing this Result phase.
    pub fn attribute_result_missing(&mut self) {
        if let Some(mask) = observation::attribute_missing(
            &self.roster,
            observation::View::Result {
                peer_results: &self.cycle.peer_results,
                peer_acks: &self.cycle.peer_acks,
            },
            self.own_id,
        ) {
            self.pending_exclusion_proposal.0 |= mask.0;
        }
    }

    pub fn attribute_input_missing(&mut self) {
        let mut present: Vec<bool, MAX_PEERS> = Vec::new();
        for slot in self.cycle.peer_inputs.iter() {
            let _ = present.push(slot.is_some());
        }
        if let Some(mask) = observation::attribute_missing::<V::Payload>(
            &self.roster,
            observation::View::Input {
                peer_inputs_present: &present,
            },
            self.own_id,
        ) {
            self.pending_exclusion_proposal.0 |= mask.0;
        }
    }

    /// Add a peer to the local exclusion proposal explicitly (e.g. after
    /// a value-divergence detection in Publish).
    pub fn propose_exclude(&mut self, peer_id: u8) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        self.pending_exclusion_proposal.set(idx);
        Ok(())
    }

    pub fn reset_exclusion_proposals(&mut self) {
        self.votes.reset();
    }

    pub fn record_peer_exclusion_proposal(
        &mut self,
        peer_id: u8,
        mask: PeerMask,
    ) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.votes.proposals[idx] = Some(mask);
        }
        Ok(())
    }

    /// The local exclusion proposal accumulated over this cycle.
    pub fn proposed_exclusions(&self) -> PeerMask {
        self.pending_exclusion_proposal
    }

    /// Note that we've observed a ResyncLostPeer frame from `peer_id` —
    /// this contributes a bit to our own rejoin vote for this cycle.
    pub fn set_rejoin_seen(&mut self, peer_id: u8) {
        if (peer_id as usize) < 8 {
            self.rejoin_seen.set(peer_id as usize);
        }
    }

    /// The rejoin mask we'll attest to peers in send_ack.
    pub fn own_rejoin_vote(&self) -> PeerMask {
        self.rejoin_seen
    }

    pub fn set_pending_rejoin(&mut self, mask: PeerMask) {
        self.pending_rejoin = mask;
    }

    pub fn pending_rejoin(&self) -> PeerMask {
        self.pending_rejoin
    }

    pub fn clear_pending_rejoin(&mut self) {
        self.pending_rejoin = PeerMask::EMPTY;
    }

    /// Record a peer's rejoin-vote mask received in an ack frame.
    pub fn record_peer_rejoin_vote(
        &mut self,
        peer_id: u8,
        vote: PeerMask,
    ) -> Result<(), DiscoveryError> {
        if let Some(idx) = self.resolve_peer_slot(peer_id)? {
            self.peer_rejoin_votes[idx] = Some(vote);
        }
        Ok(())
    }

    /// AND-reduce own vote with every healthy peer's vote. Any healthy
    /// peer that didn't attest → return EMPTY (no rejoin this cycle).
    /// Called after send_ack completes; a missing attestation means the
    /// unanimity requirement isn't met.
    pub fn aggregate_rejoin_votes(&self) -> PeerMask {
        let mut agg = self.rejoin_seen.as_u8();
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            match self.peer_rejoin_votes[idx] {
                Some(m) => agg &= m.as_u8(),
                None => return PeerMask::EMPTY,
            }
        }
        PeerMask::from_u8(agg)
    }

    /// Peers whose vote is expected this round but has not arrived. A peer
    /// that is being proposed for exclusion, or that has been silent all
    /// cycle, is not expected to reply.
    pub fn healthy_peers_missing_vote(&self) -> Vec<u8, MAX_PEERS> {
        let own_proposal = self.proposed_exclusions();
        self.collect_peers_where(|idx, _| {
            if own_proposal.contains(idx) {
                return false;
            }
            let sent_result = self.cycle.peer_results[idx].is_some();
            let sent_ack = self.cycle.peer_acks[idx].is_some();
            // A peer that sent a CycleSync State frame this cycle recorded
            // its seen-mask into the buffer. Without this, EM entered from
            // CycleSyncTimeout completes with no votes exchanged because
            // neither result nor ack slots have been touched yet.
            let sent_cycle_sync = self.obs.peer_seen[idx].is_some();
            if !sent_result && !sent_ack && !sent_cycle_sync {
                return false;
            }
            self.votes.proposals[idx].is_none()
        })
    }

    /// Aggregate peer exclusion proposals + own vote into a confirmed mask.
    /// Rule 1a: target's own vote is ignored.
    pub fn aggregate_exclusion_votes(&self) -> PeerMask {
        self.votes
            .aggregate(&self.roster, self.own_id, self.proposed_exclusions())
    }

    /// Apply confirmed exclusions. Returns number of transitions applied.
    pub fn apply_confirmed_exclusions(&mut self, confirmed: PeerMask) -> usize {
        self.roster.exclude(confirmed)
    }

    pub fn active_peer_count(&self) -> usize {
        self.roster.active_count()
    }

    pub fn active_peer_count_alive_only(&self) -> usize {
        self.roster.voting_peer_count()
    }

    pub fn lowest_alive_id(&self) -> u8 {
        self.roster.lowest_alive_id(self.own_id)
    }

    /// True as long as the active node count stays at or above the safety
    /// floor.
    pub fn quorum_available(&self) -> bool {
        let voting_total = 1 + self.roster.voting_peer_count();
        voting_total >= self.participants.min_participants as usize
    }

    /// Number of agreeing values needed for a decision this cycle.
    /// `max(floor(N_active/2)+1, min_participants)`.
    pub fn required_agreement(&self) -> usize {
        let voting_total = 1 + self.roster.voting_peer_count();
        let strict_majority = voting_total / 2 + 1;
        strict_majority.max(self.participants.min_participants as usize)
    }

    /// How many further node failures the fabric can tolerate without
    /// losing majority voting. Zero means the next fault is unrecoverable.
    pub fn tolerable_failures_remaining(&self) -> usize {
        let voting_total = 1 + self.roster.voting_peer_count();
        voting_total.saturating_sub(self.participants.min_participants as usize)
    }

    pub fn set_peer_clocks(&mut self, clocks: &[PeerClock]) {
        self.peer_clocks.clear();
        for c in clocks {
            if self.peer_clocks.push(*c).is_err() {
                warn!(count = clocks.len(), "peer_clocks capacity exceeded");
                break;
            }
        }
    }

    pub fn set_sync_epsilon(&mut self, epsilon_ns: i64) {
        self.sync_epsilon_ns = epsilon_ns;
    }

    pub fn peer_clocks(&self) -> &[PeerClock] {
        &self.peer_clocks
    }

    pub fn sync_epsilon_ns(&self) -> i64 {
        self.sync_epsilon_ns
    }

    pub fn mark_sync_valid(&mut self) {
        self.sync_valid = true;
        info!("time sync valid");
    }

    pub fn invalidate_sync(&mut self) {
        self.sync_valid = false;
        warn!("time sync invalidated");
    }

    pub fn sync_valid(&self) -> bool {
        self.sync_valid
    }

    /// Translate a peer-clock timestamp into our local monotonic ns.
    pub fn peer_ts_to_local(&self, peer_id: u8, peer_ts: u64) -> Option<u64> {
        if !self.sync_valid {
            return None;
        }
        let offset = self
            .peer_clocks
            .iter()
            .find(|c| c.peer_id == peer_id)?
            .offset_ns;
        let local = (peer_ts as i128) - (offset as i128);
        if local < 0 {
            None
        } else {
            Some(local as u64)
        }
    }

    /// Readmit a peer that came back via resync. Returns true on transition.
    pub fn readmit_peer(&mut self, peer_id: u8) -> bool {
        self.roster.readmit(peer_id)
    }

    pub fn active_count_including_self(&self) -> u8 {
        (1 + self.active_peer_count()) as u8
    }

    pub fn tick_probation(&mut self) -> usize {
        if self.self_probation_remaining > 0 {
            self.self_probation_remaining -= 1;
            if self.self_probation_remaining == 0 {
                info!("self promoted from Probation to Alive");
            }
        }
        self.roster
            .tick_probation(self.participants.probation_cycles)
    }

    pub fn enter_self_probation(&mut self) {
        self.self_probation_remaining = self.participants.probation_cycles;
    }

    pub fn self_in_probation(&self) -> bool {
        self.self_probation_remaining > 0
    }

    pub fn lowest_alive_peer_id(&self) -> Option<u8> {
        self.roster
            .peers()
            .iter()
            .filter(|p| p.health == PeerHealth::Alive)
            .map(|p| p.id)
            .min()
    }

    pub fn publisher_consensus(&self, own_pick: u8) -> Option<u8> {
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            let ack = self.cycle.peer_acks[idx]?;

            if ack.publisher_candidate != own_pick {
                return None;
            }
        }
        Some(own_pick)
    }

    pub fn no_peer_evidence_this_cycle(&self) -> bool {
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if self.cycle.peer_inputs[idx].is_some()
                || self.cycle.peer_results[idx].is_some()
                || self.cycle.peer_acks[idx].is_some()
                || self.votes.proposals[idx].is_some()
            {
                return false;
            }
        }
        true
    }

    pub fn self_excluded_by_peers(&self) -> bool {
        self.votes.self_excluded_by_peers(&self.roster, self.own_id)
    }

    pub fn own_id(&self) -> u8 {
        self.own_id
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn node_state(&self) -> NodeState {
        self.node_state
    }
    pub fn set_node_state(&mut self, s: NodeState) {
        self.node_state = s;
    }
    pub fn system_state(&self) -> SystemState {
        self.system_state
    }
    pub fn set_system_state(&mut self, s: SystemState) {
        self.system_state = s;
    }
    pub fn current_seq(&self) -> u32 {
        self.current_seq
    }
    pub fn peers(&self) -> &[PeerInfo] {
        self.roster.peers()
    }
    pub fn cycle(&self) -> &CycleState<V::Payload, I> {
        &self.cycle
    }
    pub fn last_decision(&self) -> Option<VotingOutcome<V::Decision>> {
        self.last_decision
    }
    pub fn voter(&self) -> &V {
        &self.voter
    }
    pub fn was_lost(&self) -> bool {
        self.was_lost
    }
    pub fn set_was_lost(&mut self, lost: bool) {
        self.was_lost = lost;
    }
}

pub(crate) fn strict_majority<T: Eq + Copy>(values: &[T]) -> Option<T> {
    let n = values.len();
    if n == 0 {
        return None;
    }
    let threshold = n / 2 + 1;
    let mut best: Option<(T, usize)> = None;
    for &v in values.iter() {
        let count = values.iter().filter(|&&x| x == v).count();
        if count >= threshold {
            return Some(v);
        }
        match best {
            Some((_, c)) if count <= c => {}
            _ => best = Some((v, count)),
        }
    }
    // Kein Wert erreicht threshold.
    let _ = best;
    None
}

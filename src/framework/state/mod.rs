mod cycle;
mod observation;
mod peers;
mod voting;

pub use cycle::{AckInfo, CycleState};
pub use observation::ObservationKind;
pub use peers::{DiscoveryError, PeerHealth, PeerInfo, PeerRoster};
pub use voting::ExclusionVotes;

use crate::framework::config::{MAX_PEERS, ParticipantConfig};
use crate::framework::peer_sync::PeerClock;
use crate::framework::state_machine::{NodeState, SystemState};
use crate::framework::traits::{Voter, VotingOutcome};
use crate::framework::types::PeerMask;
use heapless::Vec;
use tracing::{info, warn};

/// Full runtime state of one node. Composed of a peer roster, per-cycle
/// buffers, a distributed-observation tracker, exclusion-vote tracker,
/// clock offsets, and a small set of scalar fields.
pub struct RunState<V: Voter> {
    own_id: u8,
    session_id: u64,
    voter: V,
    participants: ParticipantConfig,

    node_state: NodeState,
    system_state: SystemState,
    current_seq: u32,

    roster: PeerRoster,
    cycle: CycleState<V::Payload>,
    obs: ObservationKind,
    votes: ExclusionVotes,

    pending_exclusion_proposal: PeerMask,

    peer_clocks: Vec<PeerClock, MAX_PEERS>,
    sync_epsilon_ns: i64,
    sync_valid: bool,

    last_decision: Option<VotingOutcome<V::Decision>>,
}

impl<V: Voter> RunState<V> {
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
            last_decision: None,
        }
    }

    pub fn participants(&self) -> &ParticipantConfig {
        &self.participants
    }

    pub fn discovery_locked(&self) -> bool {
        self.roster.discovery_locked()
    }

    /// Register a newly discovered peer during InitSync.
    pub fn on_peer_discovered(&mut self, id: u8) -> Result<(), DiscoveryError> {
        self.roster.discover(id, self.own_id, self.participants.max_peers())
    }

    /// Close discovery, sort peers by id, size all per-peer buffers.
    pub fn finalize_discovery(&mut self) -> Result<(), DiscoveryError> {
        self.roster.finalize(self.own_id, self.participants.nominal_participants)?;
        let n = self.roster.peers().len();
        self.cycle.resize(n);
        self.obs.resize(n);
        self.votes.resize(n);
        Ok(())
    }

    pub fn peer_index(&self, id: u8) -> Option<usize> {
        self.roster.peer_index(id)
    }

    pub fn record_own_result(&mut self, payload: V::Payload) {
        self.cycle.own_result = Some(payload);
    }

    pub fn record_peer_result(&mut self, peer_id: u8, payload: V::Payload) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        if self.roster.peers()[idx].health == PeerHealth::Lost {
            return Ok(());
        }
        self.cycle.peer_results[idx] = Some(payload);
        Ok(())
    }

    pub fn record_peer_ack(&mut self, peer_id: u8, ack: AckInfo) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        if self.roster.peers()[idx].health == PeerHealth::Lost {
            return Ok(());
        }
        self.cycle.peer_acks[idx] = Some(ack);
        Ok(())
    }

    /// Run the voter over own + non-Lost peer values.
    pub fn run_vote(&mut self) -> VotingOutcome<V::Decision> {
        let own = match self.cycle.own_result {
            Some(v) => v,
            None => return VotingOutcome::InsufficientQuorum,
        };
        let mut active: Vec<Option<V::Payload>, MAX_PEERS> = Vec::new();
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health != PeerHealth::Lost {
                let _ = active.push(self.cycle.peer_results[idx]);
            }
        }
        let outcome = self.voter.decide(&own, &active);
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

    pub fn record_peer_seen_mask(&mut self, peer_id: u8, mask: PeerMask) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        if self.roster.peers()[idx].health == PeerHealth::Lost {
            return Ok(());
        }
        self.obs.peer_seen[idx] = Some(mask);
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

    /// Add a peer to the local exclusion proposal explicitly (e.g. after
    /// a value-divergence detection in Publish).
    pub fn propose_exclude(&mut self, peer_id: u8) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        self.pending_exclusion_proposal.set(idx);
        Ok(())
    }

    pub fn reset_exclusion_proposals(&mut self) {
        self.votes.reset();
    }

    pub fn record_peer_exclusion_proposal(&mut self, peer_id: u8, mask: PeerMask) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        if self.roster.peers()[idx].health == PeerHealth::Lost {
            return Ok(());
        }
        self.votes.proposals[idx] = Some(mask);
        Ok(())
    }

    /// The local exclusion proposal accumulated over this cycle.
    pub fn proposed_exclusions(&self) -> PeerMask {
        self.pending_exclusion_proposal
    }

    /// Peers whose vote is expected this round but has not arrived. A peer
    /// that is being proposed for exclusion, or that has been silent all
    /// cycle, is not expected to reply.
    pub fn healthy_peers_missing_vote(&self) -> Vec<u8, MAX_PEERS> {
        let own_proposal = self.proposed_exclusions();
        let mut missing: Vec<u8, MAX_PEERS> = Vec::new();
        for (idx, peer) in self.roster.peers().iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if own_proposal.contains(idx) {
                continue;
            }
            let sent_result = self.cycle.peer_results[idx].is_some();
            let sent_ack = self.cycle.peer_acks[idx].is_some();
            if !sent_result && !sent_ack {
                continue;
            }
            if self.votes.proposals[idx].is_none() {
                let _ = missing.push(peer.id);
            }
        }
        missing
    }

    /// Aggregate peer exclusion proposals + own vote into a confirmed mask.
    /// Rule 1a: target's own vote is ignored.
    pub fn aggregate_exclusion_votes(&self) -> PeerMask {
        self.votes.aggregate(&self.roster, self.own_id, self.proposed_exclusions())
    }

    /// Apply confirmed exclusions. Returns number of transitions applied.
    pub fn apply_confirmed_exclusions(&mut self, confirmed: PeerMask) -> usize {
        self.roster.exclude(confirmed)
    }

    pub fn active_peer_count(&self) -> usize {
        self.roster.active_count()
    }

    pub fn lowest_alive_id(&self) -> u8 {
        self.roster.lowest_alive_id(self.own_id)
    }

    /// True as long as the active node count stays at or above the safety
    /// floor.
    pub fn quorum_available(&self) -> bool {
        let active_total = 1 + self.active_peer_count();
        active_total >= self.participants.min_participants as usize
    }

    /// Number of agreeing values needed for a decision this cycle.
    /// `max(floor(N_active/2)+1, min_participants)`.
    pub fn required_agreement(&self) -> usize {
        let active_total = 1 + self.active_peer_count();
        let strict_majority = active_total / 2 + 1;
        strict_majority.max(self.participants.min_participants as usize)
    }

    /// How many further node failures the fabric can tolerate without
    /// losing majority voting. Zero means the next fault is unrecoverable.
    pub fn tolerable_failures_remaining(&self) -> usize {
        let active_total = 1 + self.active_peer_count();
        active_total.saturating_sub(self.participants.min_participants as usize)
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
        if local < 0 { None } else { Some(local as u64) }
    }

    pub fn own_id(&self) -> u8 { self.own_id }
    pub fn session_id(&self) -> u64 { self.session_id }
    pub fn node_state(&self) -> NodeState { self.node_state }
    pub fn set_node_state(&mut self, s: NodeState) { self.node_state = s; }
    pub fn system_state(&self) -> SystemState { self.system_state }
    pub fn set_system_state(&mut self, s: SystemState) { self.system_state = s; }
    pub fn current_seq(&self) -> u32 { self.current_seq }
    pub fn peers(&self) -> &[PeerInfo] { self.roster.peers() }
    pub fn cycle(&self) -> &CycleState<V::Payload> { &self.cycle }
    pub fn last_decision(&self) -> Option<VotingOutcome<V::Decision>> { self.last_decision }
    pub fn voter(&self) -> &V { &self.voter }
}
use crate::framework::config::{MAX_PEERS, ParticipantConfig};
use crate::framework::peer_sync::PeerClock;
use crate::framework::state_machine::{NodeState, SystemState};
use crate::framework::traits::{CyclePayload, Voter, VotingOutcome};
use heapless::Vec;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealth {
    Alive,
    Suspect,
    Lost,
}

impl PeerHealth {
    /// Reine Uebergangsfunktion. Wird ausschliesslich von
    /// `RunState::apply_health_transitions` in ErrorManagement aufgerufen.
    pub fn transition(
        current: PeerHealth,
        consecutive_faults: u32,
        consecutive_healthy: u32,
        cfg: &HealthConfig,
    ) -> PeerHealth {
        match current {
            PeerHealth::Lost => PeerHealth::Lost,

            PeerHealth::Alive => {
                if consecutive_faults >= cfg.suspect_threshold {
                    PeerHealth::Suspect
                } else {
                    PeerHealth::Alive
                }
            }

            PeerHealth::Suspect => {
                if consecutive_healthy >= cfg.recovery_threshold {
                    PeerHealth::Alive
                } else if consecutive_faults >= cfg.lost_threshold {
                    PeerHealth::Lost
                } else {
                    PeerHealth::Suspect
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    MissedShareResult,
    MissedAck,
    MissedCycleSync,
    ValueDivergence,
    StaleFrame,
}

#[derive(Debug, Clone, Copy)]
pub struct HealthConfig {
    pub suspect_threshold: u32,
    pub lost_threshold: u32,
    pub recovery_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            suspect_threshold: 3,
            lost_threshold: 10,
            recovery_threshold: 20,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PeerInfo {
    pub id: u8,
    pub mask_bit: u8,
    pub last_seq: u32,
    pub last_state_wire: u8,
    pub last_seen_cycle: u32,
    pub health: PeerHealth,
    pub consecutive_faults: u32,
    pub consecutive_healthy_cycles: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct AckInfo {
    pub received_from: u8,
    pub publisher_candidate: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct ProbationInfo {
    pub entered_cycle: u32,
    pub reason: ProbationReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbationReason {
    StateDiverged,
    LatePeer,
}

/// Fehler bei Peer-Discovery und -Verwaltung.
///
/// `WrongNodeCount` wird zum Ende der Discovery zurueckgegeben, wenn
/// die Anzahl gefundener Nodes nicht mit `nominal_participants`
/// uebereinstimmt (weder zuwenig noch zuviel — striktes ==).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    WrongNodeCount { found: u8, expected: u8 },
    UnknownPeer,
}

pub struct CycleState<P: CyclePayload> {
    pub own_result: Option<P>,
    pub peer_results: Vec<Option<P>, MAX_PEERS>,
    pub peer_acks: Vec<Option<AckInfo>, MAX_PEERS>,
    pub phase_deadline: u64,
}

impl<P: CyclePayload> CycleState<P> {
    pub const fn empty() -> Self {
        Self {
            own_result: None,
            peer_results: Vec::new(),
            peer_acks: Vec::new(),
            phase_deadline: 0,
        }
    }

    pub fn reset_for_new_cycle(&mut self, deadline: u64) {
        self.own_result = None;
        for slot in self.peer_results.iter_mut() {
            *slot = None;
        }
        for slot in self.peer_acks.iter_mut() {
            *slot = None;
        }
        self.phase_deadline = deadline;
    }
}

pub struct RunState<V: Voter> {
    own_id: u8,
    session_id: u64,
    voter: V,
    participants: ParticipantConfig,

    node_state: NodeState,
    system_state: SystemState,
    current_seq: u32,
    probation: Option<ProbationInfo>,

    health_config: HealthConfig,

    peers: Vec<PeerInfo, MAX_PEERS>,
    /// True sobald Discovery mit korrekter Node-Anzahl abgeschlossen ist.
    /// Danach werden keine neuen Peers mehr aufgenommen — Frames von
    /// unbekannten Peers werden vom Runner verworfen.
    discovery_locked: bool,

    peer_clocks: Vec<PeerClock, MAX_PEERS>,
    sync_epsilon_ns: i64,
    sync_valid: bool,

    cycle: CycleState<V::Payload>,

    last_decision: Option<VotingOutcome<V::Decision>>,
}

impl<V: Voter> RunState<V> {
    pub fn new(
        own_id: u8,
        session_id: u64,
        voter: V,
        participants: ParticipantConfig,
    ) -> Self {
        Self {
            own_id,
            session_id,
            voter,
            participants,
            node_state: NodeState::Startup,
            system_state: SystemState::Startup,
            current_seq: 0,
            probation: None,
            health_config: HealthConfig::default(),
            peers: Vec::new(),
            discovery_locked: false,
            peer_clocks: Vec::new(),
            sync_epsilon_ns: 0,
            sync_valid: false,
            cycle: CycleState::empty(),
            last_decision: None,
        }
    }

    pub fn set_health_config(&mut self, cfg: HealthConfig) {
        self.health_config = cfg;
    }

    pub fn health_config(&self) -> &HealthConfig {
        &self.health_config
    }

    pub fn participants(&self) -> &ParticipantConfig {
        &self.participants
    }

    pub fn discovery_locked(&self) -> bool {
        self.discovery_locked
    }

    // ---- Discovery ----

    /// Neuen Peer aufnehmen. Ignoriert wenn:
    /// - id == own_id
    /// - Peer bereits bekannt
    /// - Discovery abgeschlossen (Lock)
    /// - Nominale Anzahl bereits erreicht
    ///
    /// Kein Err in diesen Faellen — nur Log, weil das kein Fehler ist,
    /// den der Discovery-Loop verarbeiten muesste.
    pub fn on_peer_discovered(&mut self, id: u8) -> Result<(), DiscoveryError> {
        if id == self.own_id {
            return Ok(());
        }
        if self.peers.iter().any(|p| p.id == id) {
            return Ok(());
        }
        if self.discovery_locked {
            warn!(id, "discovery locked, ignoring unknown node");
            return Ok(());
        }
        let peers_limit = self.participants.max_peers();
        if self.peers.len() >= peers_limit {
            warn!(
                id,
                current = self.peers.len(),
                limit = peers_limit,
                "already have nominal peer count, ignoring additional node"
            );
            return Ok(());
        }

        let mask_bit = self.peers.len() as u8;
        // push kann nur fehlschlagen wenn peers.len() >= MAX_PEERS — was
        // durch peers_limit-Check oben ausgeschlossen ist. Falls doch:
        // struktureller Fehler, Fail-Stop.
        self.peers
            .push(PeerInfo {
                id,
                mask_bit,
                last_seq: 0,
                last_state_wire: 0,
                last_seen_cycle: 0,
                health: PeerHealth::Alive,
                consecutive_faults: 0,
                consecutive_healthy_cycles: 0,
            })
            .expect("peers.push failed despite limit check");
        Ok(())
    }

    /// Schliesst die Discovery ab. Verlangt striktes `==` zwischen
    /// gefundenen Nodes und `nominal_participants`. Weder mehr noch
    /// weniger. Bei Erfolg wird das Peer-Set gesperrt.
    pub fn finalize_discovery(&mut self) -> Result<(), DiscoveryError> {
        let found = self.peers.len() as u8 + 1;
        let expected = self.participants.nominal_participants;
        if found != expected {
            return Err(DiscoveryError::WrongNodeCount { found, expected });
        }
        self.peers.sort_unstable_by_key(|p| p.id);
        for (i, p) in self.peers.iter_mut().enumerate() {
            p.mask_bit = i as u8;
        }
        for _ in 0..self.peers.len() {
            let _ = self.cycle.peer_results.push(None);
            let _ = self.cycle.peer_acks.push(None);
        }
        self.discovery_locked = true;
        info!(nodes = found, "discovery finalized and locked");
        Ok(())
    }

    // ---- Zyklus-Betrieb ----

    pub fn peer_index(&self, id: u8) -> Option<usize> {
        self.peers.iter().position(|p| p.id == id)
    }

    pub fn record_own_result(&mut self, payload: V::Payload) {
        self.cycle.own_result = Some(payload);
    }

    pub fn record_peer_result(
        &mut self,
        peer_id: u8,
        payload: V::Payload,
    ) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        self.cycle.peer_results[idx] = Some(payload);
        debug!(peer_id, seq = self.current_seq, "recorded peer result");
        Ok(())
    }

    pub fn record_peer_ack(&mut self, peer_id: u8, ack: AckInfo) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or_else(|| {
            warn!(peer_id, "received ack from unknown peer");
            DiscoveryError::UnknownPeer
        })?;
        self.cycle.peer_acks[idx] = Some(ack);
        debug!(peer_id, seq = self.current_seq, "recorded peer ack");
        Ok(())
    }

    /// Fuehrt das Voting mit dem konfigurierten Voter durch.
    /// Filtert Lost-Peers raus, damit der Voter die strikte Mehrheit
    /// aus der Anzahl aktuell vertrauenswuerdiger Nodes berechnet.
    pub fn run_vote(&mut self) -> VotingOutcome<V::Decision> {
        let own = match self.cycle.own_result {
            Some(v) => v,
            None => return VotingOutcome::InsufficientQuorum,
        };

        let mut active_slots: Vec<Option<V::Payload>, MAX_PEERS> = Vec::new();
        for (idx, peer) in self.peers.iter().enumerate() {
            if peer.health != PeerHealth::Lost {
                let _ = active_slots.push(self.cycle.peer_results[idx]);
            }
        }

        let outcome = self.voter.decide(&own, &active_slots);
        self.last_decision = Some(outcome);
        outcome
    }

    pub fn start_new_cycle(&mut self, deadline: u64) {
        self.current_seq = self.current_seq.wrapping_add(1);
        self.cycle.reset_for_new_cycle(deadline);
        self.last_decision = None;
    }

    pub fn expected_sync_mask(&self) -> u8 {
        let mut mask = 0u8;
        for (idx, peer) in self.peers.iter().enumerate() {
            if peer.health != PeerHealth::Lost {
                mask |= 1 << idx;
            }
        }
        mask
    }

    // ---- Fehler-Buchhaltung ----

    pub fn record_peer_fault(
        &mut self,
        peer_id: u8,
        kind: FaultKind,
    ) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        let peer = &mut self.peers[idx];
        peer.consecutive_faults = peer.consecutive_faults.saturating_add(1);
        peer.consecutive_healthy_cycles = 0;
        debug!(
            peer_id,
            fault_kind = ?kind,
            consecutive_faults = peer.consecutive_faults,
            "peer fault counter incremented"
        );
        Ok(())
    }

    pub fn record_peer_healthy_cycle(
        &mut self,
        peer_id: u8,
    ) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        let peer = &mut self.peers[idx];
        peer.consecutive_healthy_cycles =
            peer.consecutive_healthy_cycles.saturating_add(1);
        peer.consecutive_faults = 0;
        Ok(())
    }

    pub fn apply_health_transitions(&mut self) -> usize {
        let cfg = self.health_config;
        let mut transitions = 0usize;
        for peer in self.peers.iter_mut() {
            let old = peer.health;
            let new = PeerHealth::transition(
                peer.health,
                peer.consecutive_faults,
                peer.consecutive_healthy_cycles,
                &cfg,
            );
            if new != old {
                warn!(
                    peer_id = peer.id,
                    from = ?old,
                    to = ?new,
                    consecutive_faults = peer.consecutive_faults,
                    consecutive_healthy = peer.consecutive_healthy_cycles,
                    "peer health transitioned in ErrorManagement"
                );
                peer.health = new;
                transitions += 1;
            }
        }
        transitions
    }

    pub fn peers_with_health(&self, health: PeerHealth) -> usize {
        self.peers.iter().filter(|p| p.health == health).count()
    }

    pub fn active_peer_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| p.health != PeerHealth::Lost)
            .count()
    }

    pub fn lowest_alive_id(&self) -> u8 {
        let mut min_id = self.own_id;
        for peer in self.peers.iter() {
            if peer.health == PeerHealth::Alive && peer.id < min_id {
                min_id = peer.id;
            }
        }
        min_id
    }

    pub fn has_alive_peer(&self) -> bool {
        self.peers.iter().any(|p| p.health == PeerHealth::Alive)
    }

    pub fn quorum_available(&self) -> bool {
        let active_total = 1 + self.active_peer_count();
        active_total >= self.participants.min_participants as usize
    }

    /// Mindestanzahl uebereinstimmender Ergebnisse fuer eine belastbare
    /// Entscheidung.
    ///
    /// Formel: `max(floor(N_aktiv/2)+1, min_participants)`
    ///
    /// Kombiniert strikte Mehrheit der aktuellen Situation mit dem
    /// Systemintegrator-Minimum aus ParticipantConfig.
    pub fn required_agreement(&self) -> usize {
        let active_total = 1 + self.active_peer_count();
        let strict_majority = active_total / 2 + 1;
        strict_majority.max(self.participants.min_participants as usize)
    }

    pub fn in_fail_safe_mode(&self) -> bool {
        let active_total = 1 + self.active_peer_count();
        active_total <= self.required_agreement()
    }

    // ---- PeerSync-Ergebnis ----

    pub fn set_peer_clocks(&mut self, clocks: &[PeerClock]) {
        self.peer_clocks.clear();
        for c in clocks {
            if self.peer_clocks.push(*c).is_err() {
                warn!("peer_clocks capacity exceeded, dropping clock entry");
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
        info!("time sync marked valid");
    }

    pub fn invalidate_sync(&mut self) {
        self.sync_valid = false;
        warn!("time sync invalidated");
    }

    pub fn sync_valid(&self) -> bool {
        self.sync_valid
    }

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

    // ---- Accessors ----

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
        &self.peers
    }
    pub fn cycle(&self) -> &CycleState<V::Payload> {
        &self.cycle
    }
    pub fn last_decision(&self) -> Option<VotingOutcome<V::Decision>> {
        self.last_decision
    }
    pub fn voter(&self) -> &V {
        &self.voter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> HealthConfig {
        HealthConfig {
            suspect_threshold: 3,
            lost_threshold: 10,
            recovery_threshold: 20,
        }
    }

    #[test]
    fn alive_stays_alive_below_threshold() {
        assert_eq!(
            PeerHealth::transition(PeerHealth::Alive, 2, 0, &cfg()),
            PeerHealth::Alive
        );
    }

    #[test]
    fn alive_becomes_suspect_at_threshold() {
        assert_eq!(
            PeerHealth::transition(PeerHealth::Alive, 3, 0, &cfg()),
            PeerHealth::Suspect
        );
    }

    #[test]
    fn suspect_recovers_at_threshold() {
        assert_eq!(
            PeerHealth::transition(PeerHealth::Suspect, 0, 20, &cfg()),
            PeerHealth::Alive
        );
    }

    #[test]
    fn suspect_becomes_lost_at_threshold() {
        assert_eq!(
            PeerHealth::transition(PeerHealth::Suspect, 10, 0, &cfg()),
            PeerHealth::Lost
        );
    }

    #[test]
    fn lost_is_terminal() {
        assert_eq!(
            PeerHealth::transition(PeerHealth::Lost, 0, 1000, &cfg()),
            PeerHealth::Lost
        );
    }
}
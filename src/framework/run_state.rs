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
    /// `RunState::apply_health_transitions` in ErrorManagement aufgerufen —
    /// zaehlerbasierte Updates in den Handlern lassen dieses Feld
    /// unangetastet, damit die Rekonfiguration zentral bleibt.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    TooManyPeers,
    NotEnoughPeers,
    UnknownPeer,
}

pub struct CycleState<P: CyclePayload, const N: usize> {
    pub own_result: Option<P>,
    pub peer_results: Vec<Option<P>, N>,
    pub peer_acks: Vec<Option<AckInfo>, N>,
    pub phase_deadline: u64,
}

impl<P: CyclePayload, const N: usize> CycleState<P, N> {
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

pub struct RunState<V: Voter, const N: usize> {
    own_id: u8,
    session_id: u64,
    voter: V,

    node_state: NodeState,
    system_state: SystemState,
    current_seq: u32,
    probation: Option<ProbationInfo>,

    health_config: HealthConfig,

    peers: Vec<PeerInfo, N>,

    peer_clocks: Vec<PeerClock, N>,
    sync_epsilon_ns: i64,
    sync_valid: bool,

    cycle: CycleState<V::Payload, N>,

    last_decision: Option<VotingOutcome<V::Decision>>,
}

impl<V: Voter, const N: usize> RunState<V, N> {
    pub fn new(own_id: u8, session_id: u64, voter: V) -> Self {
        Self {
            own_id,
            session_id,
            voter,
            node_state: NodeState::Startup,
            system_state: SystemState::Startup,
            current_seq: 0,
            probation: None,
            health_config: HealthConfig::default(),
            peers: Vec::new(),
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

    // ---- Discovery ----

    pub fn on_peer_discovered(&mut self, id: u8) -> Result<(), DiscoveryError> {
        if id == self.own_id {
            return Ok(());
        }
        if self.peers.iter().any(|p| p.id == id) {
            return Ok(());
        }
        let mask_bit = self.peers.len() as u8;
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
            .map_err(|_| DiscoveryError::TooManyPeers)
    }

    pub fn finalize_discovery(&mut self) -> Result<(), DiscoveryError> {
        let participants = self.peers.len() as u8 + 1;
        if participants < self.voter.required_participants() {
            return Err(DiscoveryError::NotEnoughPeers);
        }
        self.peers.sort_unstable_by_key(|p| p.id);
        for (i, p) in self.peers.iter_mut().enumerate() {
            p.mask_bit = i as u8;
        }
        for _ in 0..self.peers.len() {
            let _ = self.cycle.peer_results.push(None);
            let _ = self.cycle.peer_acks.push(None);
        }
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

    pub fn run_vote(&mut self) -> VotingOutcome<V::Decision> {
        let own = match self.cycle.own_result {
            Some(v) => v,
            None => return VotingOutcome::InsufficientQuorum,
        };

        // Lost-Peers vor dem Voter-Aufruf rausfiltern. Der Voter berechnet
        // die strikte Mehrheit aus `1 + peers.len()`; wenn Lost-Peers noch
        // im Slice waeren, wuerden sie in dieser Zaehlung mitwirken und
        // das Kriterium unangemessen verschaerfen.
        //
        // find_dissenters wird spaeter separat mit dem ungefilterten Slice
        // aufgerufen, damit die zurueckgegebenen Indizes in
        // self.peers passen (Runner uebersetzt Index -> peer_id).
        let mut active_slots: Vec<Option<V::Payload>, N> = Vec::new();
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

    // ---- Fehler-Buchhaltung (nur Zaehler) ----

    /// Erhoeht `consecutive_faults`, resettet `consecutive_healthy_cycles`.
    /// Aendert das `health`-Feld NICHT — Transitions passieren
    /// ausschliesslich in `apply_health_transitions`, aufgerufen von
    /// ErrorManagement.
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

    /// Erhoeht `consecutive_healthy_cycles`, resettet `consecutive_faults`.
    /// Aendert das `health`-Feld NICHT.
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

    /// Wendet fuer alle Peers die Transition an, basierend auf ihren
    /// aktuellen Zaehlerstaenden. Der einzige Ort, an dem sich das
    /// `health`-Feld eines Peers aendert.
    ///
    /// Wird von `handle_error_management` am Anfang jedes Durchlaufs
    /// aufgerufen. Rueckgabe: Anzahl der Transitions, die tatsaechlich
    /// stattgefunden haben (fuer Diagnose / Logging in ErrorManagement).
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

    /// Kleinste ID unter allen Nodes, die aktuell als Publisher taugen
    /// (nur Alive). Eigener Node wird immer als Alive betrachtet.
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
        active_total >= self.voter.required_participants() as usize
    }

    /// Mindestanzahl uebereinstimmender Ergebnisse, die aktuell fuer
    /// eine belastbare Entscheidung noetig sind. Kombiniert die
    /// mathematische strikte Mehrheit der aktuell aktiven Nodes mit
    /// dem Systemintegrator-Minimum aus dem Voter.
    ///
    /// Formel: `max(floor(N_aktiv / 2) + 1, required_participants)`
    ///
    /// - Strikte Mehrheit `floor(N/2)+1` schliesst Ties strukturell aus
    ///   (bei geraden N wuerde `ceil(N/2)` einen 2:2-Tie noch als
    ///   Mehrheit werten — deshalb `floor(N/2)+1`).
    /// - `required_participants` erlaubt dem Systemintegrator, ein
    ///   strengeres Kriterium als reine Mehrheit vorzugeben (z.B. 6oo8).
    ///
    /// Aktive Nodes = eigener Node + Peers mit Health != Lost.
    pub fn required_agreement(&self) -> usize {
        let active_total = 1 + self.active_peer_count();
        let strict_majority = active_total / 2 + 1;
        strict_majority.max(self.voter.required_participants() as usize)
    }

    /// True, wenn die aktive Node-Anzahl gerade so die aktuell
    /// benoetigte Uebereinstimmung erreicht — kein Puffer mehr fuer
    /// weitere Ausfaelle. Weitere Faults sind fatal, weil das System
    /// unter die Mindestanzahl fallen wuerde.
    ///
    /// Beispiele:
    /// - 2oo3 alle Alive: active=3, required_agreement=2 → 3>2 → false (Puffer)
    /// - 2oo3 mit einem Lost: active=2, required_agreement=2 → 2<=2 → true
    /// - 2oo2 alle Alive: active=2, required_agreement=2 → true
    /// - 6oo8 alle Alive: active=8, required_agreement=6 → false (Puffer 2)
    /// - 6oo8 mit 2 Lost: active=6, required_agreement=6 → true
    /// - 4oo8 alle Alive: active=8, required_agreement=5 → false
    ///   (weil floor(8/2)+1 = 5 > required=4)
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
    pub fn cycle(&self) -> &CycleState<V::Payload, N> {
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
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

#[derive(Debug, Clone, Copy)]
pub struct PeerInfo {
    pub id: u8,
    pub mask_bit: u8,
    pub last_seq: u32,
    pub last_state_wire: u8,
    pub last_seen_cycle: u32,
    pub health: PeerHealth,
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

/// Zyklus-lokaler Zustand. Wird bei jedem Zyklusstart zurueckgesetzt.
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

/// Kompletter Laufzeitzustand eines Nodes.
///
/// Generisch ueber:
///   V: die anwendungsspezifische Voting-Logik (bestimmt auch den Payload-Typ)
///   N: die maximale Anzahl Peers (compile-time Obergrenze; tatsaechliche
///      Anzahl wird beim Discovery befuellt).
pub struct RunState<V: Voter, const N: usize> {
    // session-persistent
    own_id: u8,
    session_id: u64,
    voter: V,

    // node-level
    node_state: NodeState,
    system_state: SystemState,
    current_seq: u32,
    probation: Option<ProbationInfo>,

    // peers: nach Discovery befuellt, Laenge <= N
    peers: Vec<PeerInfo, N>,

    // Ergebnis der PeerSync-Phase: Uhrenoffsets zu jedem Peer, jeweils mit
    // bewiesener Fehlerschranke. Wird nach `handle_peer_sync` befuellt und
    // im weiteren Betrieb nur gelesen.
    peer_clocks: Vec<PeerClock, N>,

    // Maximale Fehlerschranke ueber alle Peers in ns.
    // Geht als epsilon in die Voter-Timeout-Auslegung ein.
    sync_epsilon_ns: i64,

    // Ist die Zeitsynchronisation erfolgreich abgeschlossen? Vor dem
    // ersten erfolgreichen PeerSync sind Timestamps in eingehenden Frames
    // nur Rohdaten und duerfen NICHT fuer Stale-Frame-Erkennung, Cycle-
    // Alignment oder aehnliches ausgewertet werden. Nach dem Setzen sind
    // die Offsets in `peer_clocks` gueltig und Timestamps koennen ueber
    // `peer_ts_to_local` in die eigene Uhr umgerechnet werden.
    sync_valid: bool,

    // zyklus-lokal
    cycle: CycleState<V::Payload, N>,

    // letzte Voting-Entscheidung fuer Publikation und State-Management
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
            peers: Vec::new(),
            peer_clocks: Vec::new(),
            sync_epsilon_ns: 0,
            sync_valid: false,
            cycle: CycleState::empty(),
            last_decision: None,
        }
    }

    // ---- Discovery ----

    pub fn on_peer_discovered(&mut self, id: u8) -> Result<(), DiscoveryError> {
        if id == self.own_id {
            return Ok(());
        }
        if self.peers.iter().any(|p| p.id == id) {
            return Ok(()); // idempotent
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
            })
            .map_err(|_| DiscoveryError::TooManyPeers)
    }

    /// Peer-Set einfrieren. Sortierung nach node_id -> deterministische
    /// Mask-Bit-Vergabe ohne Verhandlung zwischen den Nodes.
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
        let outcome = self.voter.decide(&own, &self.cycle.peer_results);
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

    // ---- PeerSync-Ergebnis ----

    /// Uebernimmt die vom PeerSync-Modul ermittelten Uhrenoffsets.
    /// Uebersteigende Eintraege (theoretisch nicht moeglich, da |peer_clocks|
    /// <= |peers| <= N) werden verworfen und geloggt.
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

    /// Als gueltig markieren nach erfolgreichem PeerSync.
    pub fn mark_sync_valid(&mut self) {
        self.sync_valid = true;
        info!("time sync marked valid");
    }

    /// Als ungueltig markieren, z.B. bei Peer-Rejoin oder erkannter
    /// Uhrendrift, die eine erneute Synchronisation erfordert.
    pub fn invalidate_sync(&mut self) {
        self.sync_valid = false;
        warn!("time sync invalidated");
    }

    pub fn sync_valid(&self) -> bool {
        self.sync_valid
    }

    /// Rechnet einen Zeitstempel des angegebenen Peers auf die eigene Uhr um.
    /// Liefert None, wenn fuer diesen Peer kein Offset bekannt ist ODER
    /// die Zeitsynchronisation noch nicht gueltig ist. Der zweite Check
    /// verhindert, dass Aufrufer versehentlich mit Rohdaten arbeiten, die
    /// vor PeerSync auf der Leitung waren.
    pub fn peer_ts_to_local(&self, peer_id: u8, peer_ts: u64) -> Option<u64> {
        if !self.sync_valid {
            return None;
        }
        let offset = self
            .peer_clocks
            .iter()
            .find(|c| c.peer_id == peer_id)?
            .offset_ns;
        // local = peer_ts - offset, defensiv gegen Underflow.
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
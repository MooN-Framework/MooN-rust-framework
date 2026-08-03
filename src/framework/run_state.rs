use crate::framework::config::{MAX_PEERS, MAX_TOTAL_NODES, ParticipantConfig};
use crate::framework::peer_sync::PeerClock;
use crate::framework::state_machine::{NodeState, SystemState};
use crate::framework::traits::{CyclePayload, Voter, VotingOutcome};
use crate::framework::types::PeerMask;
use heapless::Vec;
use tracing::{debug, info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealth {
    Alive,
    Suspect,
    Lost,
}

impl PeerHealth {
    /// Reine Uebergangsfunktion. Wird nur noch fuer Tests und interne
    /// Konsistenzchecks genutzt — die produktive Anwendung geht ueber
    /// `RunState::apply_confirmed_exclusions`, weil dort die Voting-
    /// Bestaetigung mit einfliesst.
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

    // -------------------------------------------------------------
    // Distributed Observation State
    // -------------------------------------------------------------
    //
    // Diese Felder tragen Cross-Observed-Evidenz durch die jeweilige Phase.
    // Sie sind KEIN Teil von CycleState, weil sie phasenlokal sind
    // (CycleSync-Masken haben ausserhalb von CycleSync keine Bedeutung,
    // Exclusion-Proposals nur waehrend ErrorManagement). Explizite
    // Reset-Methoden am Phasen-Eingang halten die Lebensdauer sauber.
    // -------------------------------------------------------------
    /// Was ICH in der aktuellen CycleSync-Phase gesehen habe.
    /// Bit-Index = mein `peers[]`-Index. Wird bei jedem eintreffenden
    /// Peer-State-Frame gesetzt und in JEDEN eigenen State-Frame
    /// piggyback-versendet.
    cycle_own_seen: PeerMask,
    /// Zuletzt beobachtete `seen_mask` jedes Peers in der aktuellen
    /// CycleSync-Phase (indexiert nach unserem peers[]). `None` = keine
    /// State-Frame in dieser Phase erhalten. Wird von
    /// `aggregate_cycle_sync_evidence` mit `cycle_own_seen` kombiniert.
    cycle_peer_seen: Vec<Option<PeerMask>, MAX_PEERS>,

    /// Vote-Frames der Peers in der aktuellen ErrorManagement-Voting-Runde.
    /// Bit-Interpretation: Sender-Peer-Ordnung (siehe `sender_observed`).
    cycle_peer_exclusion_proposals: Vec<Option<PeerMask>, MAX_PEERS>,
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
            cycle_own_seen: PeerMask::EMPTY,
            cycle_peer_seen: Vec::new(),
            cycle_peer_exclusion_proposals: Vec::new(),
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
            let _ = self.cycle_peer_seen.push(None);
            let _ = self.cycle_peer_exclusion_proposals.push(None);
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

    // -------------------------------------------------------------
    // Distributed Observation: CycleSync
    // -------------------------------------------------------------

    /// Aufzurufen am Eingang von handle_cycle_sync. Loescht die eigene
    /// Sicht + alle Peer-berichteten Masks der letzten Runde.
    pub fn reset_cycle_sync_evidence(&mut self) {
        self.cycle_own_seen = PeerMask::EMPTY;
        for slot in self.cycle_peer_seen.iter_mut() {
            *slot = None;
        }
    }

    /// Setzt Bit fuer Peer bei Index `peer_idx` in unserer eigenen
    /// Beobachtungs-Mask.
    pub fn set_own_seen_bit(&mut self, peer_idx: usize) {
        if peer_idx < self.peers.len() {
            self.cycle_own_seen.set(peer_idx);
        }
    }

    /// Aktuelle eigene Beobachtungs-Mask. Wird in send_state piggyback
    /// mitgeschickt.
    pub fn own_seen_mask(&self) -> PeerMask {
        self.cycle_own_seen
    }

    /// Speichert die zuletzt von `peer_id` gemeldete `seen_mask`.
    /// Ueberschreibt aeltere Werte in derselben Phase — die letzte
    /// beobachtete Sicht gewinnt (Sequenznummern regeln Ordering im
    /// Transport-Layer).
    pub fn record_peer_seen_mask(
        &mut self,
        peer_id: u8,
        mask: PeerMask,
    ) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        self.cycle_peer_seen[idx] = Some(mask);
        Ok(())
    }

    /// Aggregiert Cross-Observed-Evidenz aus `cycle_own_seen` +
    /// `cycle_peer_seen`. Incrementiert MissedCycleSync-Zaehler nur fuer
    /// Peers, die von der Mehrheit der Reporter (ausser dem Peer selbst)
    /// nicht als anwesend beobachtet wurden.
    ///
    /// Am Ende von handle_cycle_sync aufzurufen — sowohl auf Complete-
    /// als auch auf Timeout-Pfad.
    ///
    /// **Self-Diagnose-Guard**: Wenn wir in dieser Phase NICHTS gehoert
    /// haben (weder eigene State-Frame-Ingests noch Peer-berichtete
    /// Masks), koennte unser eigener Inbound der Fehler sein. In dem
    /// Fall duerfen wir keine anderen Nodes beschuldigen — wir loggen
    /// und skippen die Fault-Attribution.
    pub fn aggregate_cycle_sync_evidence(&mut self) {
        // Self-Diagnose: haben wir ueberhaupt irgendetwas gesehen oder
        // gemeldet bekommen? Wenn nicht: mit hoher Wahrscheinlichkeit
        // eigener Empfangsfehler, keine Attribution.
        let any_own_seen = !self.cycle_own_seen.is_empty();
        let any_peer_reported = self
            .cycle_peer_seen
            .iter()
            .any(|m| m.is_some());
        if !any_own_seen && !any_peer_reported {
            warn!(
                "aggregate_cycle_sync_evidence: no evidence collected \
                 (own inbound may be broken), skipping fault attribution"
            );
            return;
        }

        let peer_count = self.peers.len();
        let mut fault_ids: Vec<u8, MAX_PEERS> = Vec::new();

        for idx in 0..peer_count {
            let target = &self.peers[idx];
            if target.health == PeerHealth::Lost {
                continue;
            }
            let target_id = target.id;

            let own_observed = self.cycle_own_seen.contains(idx);
            let (observers, reporters) = self.count_observations(
                idx,
                target_id,
                own_observed,
                |rs, i| rs.cycle_peer_seen.get(i).and_then(|m| *m),
            );

            let threshold = reporters / 2 + 1; // strikte Mehrheit
            if observers < threshold {
                let _ = fault_ids.push(target_id);
            }
        }

        for id in fault_ids {
            let _ = self.record_peer_fault(id, FaultKind::MissedCycleSync);
        }
    }

    // -------------------------------------------------------------
    // Distributed Observation: Results (via ACKs)
    // -------------------------------------------------------------

    /// Aggregiert Cross-Observed-Evidenz aus eigenen `peer_results` +
    /// den `received_from`-Masken der Peer-ACKs. Incrementiert
    /// MissedShareResult-Zaehler nach derselben Mehrheits-Regel wie
    /// `aggregate_cycle_sync_evidence`.
    ///
    /// Wichtige Randfaelle:
    /// - Wenn KEINE Peer-ACKs vorliegen (z.B. ShareResult-Timeout), zaehlt
    ///   nur die eigene Beobachtung → Threshold = 1. Peers, die wir selbst
    ///   nicht gesehen haben, werden dann per-target gefaultet — aber nur
    ///   die spezifisch fehlenden, nicht alle. Und nur wenn wir insgesamt
    ///   ueberhaupt was mitbekommen haben (siehe Self-Diagnose-Guard).
    /// - Peers, die selbst als Lost gelten, sind weder Reporter noch
    ///   Target.
    ///
    /// **Self-Diagnose-Guard**: Wenn wir in dieser Runde **weder** einen
    /// Result-Frame **noch** ein Peer-Ack empfangen haben, ist der
    /// wahrscheinlichste Grund unser eigener Inbound (Multicast-Filter,
    /// Socket-Puffer, physikalisch getrennter Link). In dem Fall duerfen
    /// wir keine anderen Nodes beschuldigen — wir loggen und skippen die
    /// Fault-Attribution komplett. Sonst wuerden wir auf ShareResult-
    /// Timeout automatisch alle anderen Nodes verdaechtigen, was das
    /// System aus einem eigenen Fehler heraus fehlleiten koennte.
    ///
    /// Am Ende von handle_send_ack aufzurufen (fuer volle Cross-Obs)
    /// oder auf handle_share_result-Timeout (fuer unilateralen Fallback).
    pub fn aggregate_result_evidence(&mut self) {
        // Self-Diagnose: nichts empfangen → kein Fault-Attribution.
        let any_own_result = self.cycle.peer_results.iter().any(|r| r.is_some());
        let any_peer_ack = self.cycle.peer_acks.iter().any(|a| a.is_some());
        if !any_own_result && !any_peer_ack {
            warn!(
                "aggregate_result_evidence: no evidence collected \
                 (own inbound may be broken), skipping fault attribution"
            );
            return;
        }

        let peer_count = self.peers.len();
        let mut fault_ids: Vec<u8, MAX_PEERS> = Vec::new();

        for idx in 0..peer_count {
            let target = &self.peers[idx];
            if target.health == PeerHealth::Lost {
                continue;
            }
            let target_id = target.id;

            let own_observed = self.cycle.peer_results[idx].is_some();
            let (observers, reporters) = self.count_observations(
                idx,
                target_id,
                own_observed,
                |rs, i| {
                    rs.cycle
                        .peer_acks
                        .get(i)
                        .and_then(|a| a.map(|ack| PeerMask::from_u8(ack.received_from)))
                },
            );

            let threshold = reporters / 2 + 1;
            if observers < threshold {
                let _ = fault_ids.push(target_id);
            }
        }

        for id in fault_ids {
            let _ = self.record_peer_fault(id, FaultKind::MissedShareResult);
        }
    }

    pub fn credit_majority_delivered_peers(&mut self) {
        let peer_count = self.peers.len();
        let mut credit_ids: Vec<u8, MAX_PEERS> = Vec::new();

        for idx in 0..peer_count {
            let target = &self.peers[idx];
            if target.health == PeerHealth::Lost {
                continue;
            }
            let target_id = target.id;

            let own_observed = self.cycle.peer_results[idx].is_some();
            let (observers, reporters) = self.count_observations(
                idx,
                target_id,
                own_observed,
                |rs, i| {
                    rs.cycle
                        .peer_acks
                        .get(i)
                        .and_then(|a| a.map(|ack| PeerMask::from_u8(ack.received_from)))
                },
            );

            let threshold = reporters / 2 + 1;
            if observers >= threshold {
                let _ = credit_ids.push(target_id);
            }
        }

        for id in credit_ids {
            let _ = self.record_peer_healthy_cycle(id);
        }
    }

    // -------------------------------------------------------------
    // Voting: Exclusion Proposals
    // -------------------------------------------------------------

    /// Aufzurufen am Eingang der Voting-Phase in handle_error_management.
    pub fn reset_exclusion_proposals(&mut self) {
        for slot in self.cycle_peer_exclusion_proposals.iter_mut() {
            *slot = None;
        }
    }

    pub fn record_peer_exclusion_proposal(
        &mut self,
        peer_id: u8,
        mask: PeerMask,
    ) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        self.cycle_peer_exclusion_proposals[idx] = Some(mask);
        Ok(())
    }

    /// Baut die eigene Exclusion-Proposal aus den aktuellen
    /// Fault-Countern: Peers, deren Uebergang eine Verschlechterung
    /// waere (Alive→Suspect oder Suspect→Lost), landen im Vorschlag.
    /// Erholungen (Suspect→Alive) werden NICHT vorgeschlagen — die laufen
    /// ohne Voting.
    pub fn proposed_exclusions(&self) -> PeerMask {
        let cfg = self.health_config;
        let mut mask = PeerMask::EMPTY;
        for (idx, peer) in self.peers.iter().enumerate() {
            match peer.health {
                PeerHealth::Alive => {
                    if peer.consecutive_faults >= cfg.suspect_threshold {
                        mask.set(idx);
                    }
                }
                PeerHealth::Suspect => {
                    if peer.consecutive_faults >= cfg.lost_threshold {
                        mask.set(idx);
                    }
                }
                PeerHealth::Lost => {}
            }
        }
        mask
    }

    /// Wer schweigt in dieser Runde, obwohl wir ihn NICHT ausschliessen
    /// wollen? Rueckgabe: Liste der Peer-IDs, deren Silence Rule 2 (b)
    /// triggert (StateTimeout → Failsafe).
    ///
    /// Silence eines Peers, den wir selbst vorschlagen auszuschliessen,
    /// wird von dieser Funktion NICHT gemeldet — das ist bestaetigende
    /// Evidenz, kein Kommunikationsproblem.
    pub fn healthy_peers_missing_vote(&self) -> Vec<u8, MAX_PEERS> {
        let own_proposal = self.proposed_exclusions();
        let mut missing: Vec<u8, MAX_PEERS> = Vec::new();
        for (idx, peer) in self.peers.iter().enumerate() {
            if peer.health == PeerHealth::Lost {
                continue;
            }
            if own_proposal.contains(idx) {
                continue; // wir wollen den eh ausschliessen → Silence okay
            }
            // NEU: Peer war in dieser Runde stumm bei Result UND Ack.
            // Ohne Teilnahme am Cycle erwarten wir auch kein Vote —
            // sein Silence ist Symptom des schon laufenden Ausfalls,
            // nicht ein separates Kommunikationsproblem unter Gesunden.
            let sent_result = self.cycle.peer_results[idx].is_some();
            let sent_ack = self.cycle.peer_acks[idx].is_some();
            if !sent_result && !sent_ack {
                continue;
            }
            if self.cycle_peer_exclusion_proposals[idx].is_none() {
                let _ = missing.push(peer.id);
            }
        }
        missing
    }

    /// Aggregiert die eingegangenen Exclusion-Vorschlaege + eigenen
    /// Vorschlag zu einer bestaetigten Ausschluss-Mask.
    ///
    /// Regeln:
    /// - Regel 1 (a): Fuer Ausschluss von X wird die Stimme von X selbst
    ///   ignoriert (die kann X nicht ueber sich selbst abstimmen).
    /// - Ausschluss bestaetigt, wenn Anzahl Ja-Stimmen ≥ strikte Mehrheit
    ///   der Nicht-X-Reporter.
    ///
    /// Rueckgabe: Mask der Peers (in unserem peers[]-Index), fuer die
    /// eine Health-Verschlechterung freigegeben ist.
    pub fn aggregate_exclusion_votes(&self) -> PeerMask {
        let mut confirmed = PeerMask::EMPTY;
        let own_proposal = self.proposed_exclusions();

        for idx in 0..self.peers.len() {
            let target = &self.peers[idx];
            if target.health == PeerHealth::Lost {
                continue;
            }
            let target_id = target.id;

            // Reporters = wir + alle Nicht-Lost-Peers ausser dem Target.
            // Ja-Stimme = Reporter hat Target-Bit in seinem Proposal gesetzt.
            let mut yes_votes = 0usize;
            let mut reporters = 0usize;

            // Eigene Stimme (wir sind nie das Target — Target ist ein Peer).
            reporters += 1;
            if own_proposal.contains(idx) {
                yes_votes += 1;
            }

            // Peer-Stimmen (Regel 1a: Target selbst wird uebersprungen).
            for (i, other) in self.peers.iter().enumerate() {
                if i == idx {
                    continue;
                }
                if other.health == PeerHealth::Lost {
                    continue;
                }
                if let Some(mask) = self.cycle_peer_exclusion_proposals[i] {
                    reporters += 1;
                    if self.sender_observed(other.id, mask, target_id) {
                        yes_votes += 1;
                    }
                }
            }

            let threshold = reporters / 2 + 1;
            if yes_votes >= threshold {
                confirmed.set(idx);
            }
        }

        confirmed
    }

    /// Wendet die durch Voting bestaetigten Ausschluss-Uebergaenge an
    /// (Verschlechterungen). Erholungen (Suspect→Alive) werden IMMER
    /// angewendet — die brauchen keine Bestaetigung.
    ///
    /// Ersetzt das bisherige `apply_health_transitions`.
    pub fn apply_confirmed_exclusions(&mut self, confirmed: PeerMask) -> usize {
        let cfg = self.health_config;
        let mut transitions = 0usize;

        for (idx, peer) in self.peers.iter_mut().enumerate() {
            let old = peer.health;
            let new = match peer.health {
                PeerHealth::Alive => {
                    if confirmed.contains(idx)
                        && peer.consecutive_faults >= cfg.suspect_threshold
                    {
                        PeerHealth::Suspect
                    } else {
                        PeerHealth::Alive
                    }
                }
                PeerHealth::Suspect => {
                    // Recovery hat Vorrang und laeuft unilateral.
                    if peer.consecutive_healthy_cycles >= cfg.recovery_threshold {
                        PeerHealth::Alive
                    } else if confirmed.contains(idx)
                        && peer.consecutive_faults >= cfg.lost_threshold
                    {
                        PeerHealth::Lost
                    } else {
                        PeerHealth::Suspect
                    }
                }
                PeerHealth::Lost => PeerHealth::Lost,
            };

            if new != old {
                warn!(
                    peer_id = peer.id,
                    from = ?old,
                    to = ?new,
                    consecutive_faults = peer.consecutive_faults,
                    consecutive_healthy = peer.consecutive_healthy_cycles,
                    vote_confirmed = confirmed.contains(idx),
                    "peer health transitioned"
                );
                peer.health = new;
                transitions += 1;
            }
        }
        transitions
    }

    // -------------------------------------------------------------
    // Fehler-Buchhaltung (Zaehler-Ebene)
    // -------------------------------------------------------------

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

    pub fn record_peer_healthy_cycle(&mut self, peer_id: u8) -> Result<(), DiscoveryError> {
        let idx = self
            .peer_index(peer_id)
            .ok_or(DiscoveryError::UnknownPeer)?;
        let peer = &mut self.peers[idx];
        peer.consecutive_healthy_cycles =
            peer.consecutive_healthy_cycles.saturating_add(1);
        peer.consecutive_faults = 0;
        Ok(())
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

    // -------------------------------------------------------------
    // Cross-Observation Helper
    // -------------------------------------------------------------

    /// Zaehlt fuer `target_id` (bei unserem Index `target_idx`) die
    /// Beobachter unter allen Reportern (uns selbst + Nicht-Lost-Peers
    /// ausser dem Target).
    ///
    /// - `own_observed`: hat der lokale Node den Target gesehen?
    ///   (Aggregator liefert das explizit, damit CycleSync
    ///   [cycle_own_seen] und Result [peer_results.is_some] mit
    ///   derselben Helper laufen koennen.)
    /// - `sender_mask_getter`: extrahiert die relevante Peer-Mask fuer
    ///   Reporter i (z.B. cycle_peer_seen[i] oder peer_acks[i].received_from).
    ///
    /// Rueckgabe: `(observers, reporters)`. Die eigene Sicht ist immer
    /// mitgezaehlt (reporters startet bei 1).
    fn count_observations<F>(
        &self,
        target_idx: usize,
        target_id: u8,
        own_observed: bool,
        sender_mask_getter: F,
    ) -> (usize, usize)
    where
        F: Fn(&Self, usize) -> Option<PeerMask>,
    {
        let mut observers = 0usize;
        let mut reporters = 1usize; // eigener Node zaehlt immer als Reporter

        if own_observed {
            observers += 1;
        }

        for (i, other) in self.peers.iter().enumerate() {
            if i == target_idx {
                continue;
            }
            if other.health == PeerHealth::Lost {
                continue;
            }
            if let Some(mask) = sender_mask_getter(self, i) {
                reporters += 1;
                if self.sender_observed(other.id, mask, target_id) {
                    observers += 1;
                }
            }
        }

        (observers, reporters)
    }

    /// Prueft, ob `sender_id` in seiner `sender_mask` die Node `target_id`
    /// als beobachtet markiert hat.
    ///
    /// Bit-Zuordnung: `sender_mask` ist ueber die Peer-Liste des SENDERS
    /// indiziert (= alle Nodes ausser `sender_id`, sortiert nach id). Der
    /// Empfaenger rekonstruiert diese Ordnung aus seinem Wissen ueber
    /// das komplette Nodeset.
    fn sender_observed(
        &self,
        sender_id: u8,
        sender_mask: PeerMask,
        target_id: u8,
    ) -> bool {
        if sender_id == target_id {
            return false; // Sender kann sich selbst nicht beobachten
        }

        // Alle bekannten Node-IDs sammeln (own + Peers) und sortieren.
        let mut all_ids = [0u8; MAX_TOTAL_NODES];
        let mut n = 0usize;
        all_ids[n] = self.own_id;
        n += 1;
        for p in self.peers.iter() {
            if n >= MAX_TOTAL_NODES {
                break;
            }
            all_ids[n] = p.id;
            n += 1;
        }
        all_ids[..n].sort_unstable();

        // Position von target_id in "all_ids ohne sender_id" bestimmen.
        let mut position = 0usize;
        for &id in &all_ids[..n] {
            if id == sender_id {
                continue;
            }
            if id == target_id {
                return sender_mask.contains(position);
            }
            position += 1;
        }
        false
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
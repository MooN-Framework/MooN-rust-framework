use crate::framework::config::{HealthConfig, MAX_PEERS};
use crate::framework::state::observation::FaultKind;
use crate::framework::types::PeerMask;
use heapless::Vec;
use tracing::{debug, warn};

/// Health of a peer as seen from this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealth {
    Alive,
    Suspect,
    Lost,
}

/// Discovery-time and runtime metadata for one peer.
#[derive(Debug, Clone, Copy)]
pub struct PeerInfo {
    pub id: u8,
    pub health: PeerHealth,
    pub consecutive_faults: u32,
    pub consecutive_healthy_cycles: u32,
}

/// Discovery and management errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    WrongNodeCount { found: u8, expected: u8 },
    UnknownPeer,
}

/// The set of known peers plus their per-peer health accounting.
///
/// Discovery is a two-step process: peers are added one by one via
/// `discover`, then `finalize` locks the set. After that the peer list is
/// frozen — foreign nodes are rejected upstream.
pub struct PeerRoster {
    peers: Vec<PeerInfo, MAX_PEERS>,
    health_cfg: HealthConfig,
    discovery_locked: bool,
}

impl PeerRoster {
    pub fn new(health_cfg: HealthConfig) -> Self {
        Self {
            peers: Vec::new(),
            health_cfg,
            discovery_locked: false,
        }
    }

    pub fn set_health_config(&mut self, cfg: HealthConfig) {
        self.health_cfg = cfg;
    }

    pub fn health_config(&self) -> &HealthConfig {
        &self.health_cfg
    }

    pub fn peers(&self) -> &[PeerInfo] {
        &self.peers
    }

    pub fn discovery_locked(&self) -> bool {
        self.discovery_locked
    }

    pub fn peer_index(&self, id: u8) -> Option<usize> {
        self.peers.iter().position(|p| p.id == id)
    }

    /// Register a discovered peer. Idempotent; no-ops on own id, duplicate
    /// id, locked roster, and full roster.
    pub fn discover(&mut self, id: u8, own_id: u8, max_peers: usize) -> Result<(), DiscoveryError> {
        if id == own_id || self.peer_index(id).is_some() {
            return Ok(());
        }
        if self.discovery_locked {
            warn!(peer_id = id, "discovery locked, ignoring new peer");
            return Ok(());
        }
        if self.peers.len() >= max_peers {
            warn!(peer_id = id, count = self.peers.len(), limit = max_peers, "peer roster full");
            return Ok(());
        }
        self.peers
            .push(PeerInfo {
                id,
                health: PeerHealth::Alive,
                consecutive_faults: 0,
                consecutive_healthy_cycles: 0,
            })
            .expect("push failed despite capacity check");
        Ok(())
    }

    /// Close discovery. Requires exactly `nominal` total nodes
    /// (own + peers). Sorts by id so peer_index maps to a stable ordering.
    pub fn finalize(&mut self, own_id: u8, nominal: u8) -> Result<(), DiscoveryError> {
        let found = self.peers.len() as u8 + 1;
        if found != nominal {
            return Err(DiscoveryError::WrongNodeCount { found, expected: nominal });
        }
        self.peers.sort_unstable_by_key(|p| p.id);
        self.discovery_locked = true;
        debug!(nodes = found, own_id, "discovery finalized");
        Ok(())
    }

    /// Increment the fault counter for a peer and reset its healthy streak.
    pub fn record_fault(&mut self, peer_id: u8, kind: FaultKind) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        let p = &mut self.peers[idx];
        p.consecutive_faults = p.consecutive_faults.saturating_add(1);
        p.consecutive_healthy_cycles = 0;
        debug!(
            peer_id,
            fault_kind = ?kind,
            consecutive_faults = p.consecutive_faults,
            "fault"
        );
        Ok(())
    }

    /// Increment the healthy streak and reset the fault counter.
    pub fn record_healthy(&mut self, peer_id: u8) -> Result<(), DiscoveryError> {
        let idx = self.peer_index(peer_id).ok_or(DiscoveryError::UnknownPeer)?;
        let p = &mut self.peers[idx];
        p.consecutive_healthy_cycles = p.consecutive_healthy_cycles.saturating_add(1);
        p.consecutive_faults = 0;
        Ok(())
    }

    pub fn active_count(&self) -> usize {
        self.peers.iter().filter(|p| p.health != PeerHealth::Lost).count()
    }

    /// Smallest id among the peers marked `Alive`, defaulting to `own_id`
    /// when none qualify.
    pub fn lowest_alive_id(&self, own_id: u8) -> u8 {
        let mut min_id = own_id;
        for p in self.peers.iter() {
            if p.health == PeerHealth::Alive && p.id < min_id {
                min_id = p.id;
            }
        }
        min_id
    }

    /// Build the local exclusion proposal: peers whose counters cross the
    /// next escalation threshold. Recoveries are unilateral and not
    /// proposed here.
    pub fn proposed_exclusions(&self) -> PeerMask {
        let cfg = self.health_cfg;
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

    /// Apply vote-confirmed escalations (Alive->Suspect, Suspect->Lost) and
    /// unconditional recoveries (Suspect->Alive). Returns the number of
    /// transitions applied.
    pub fn apply_transitions(&mut self, confirmed: PeerMask) -> usize {
        let cfg = self.health_cfg;
        let mut transitions = 0usize;
        for (idx, peer) in self.peers.iter_mut().enumerate() {
            let old = peer.health;
            let new = match old {
                PeerHealth::Alive => {
                    if confirmed.contains(idx) && peer.consecutive_faults >= cfg.suspect_threshold {
                        PeerHealth::Suspect
                    } else {
                        PeerHealth::Alive
                    }
                }
                PeerHealth::Suspect => {
                    if peer.consecutive_healthy_cycles >= cfg.recovery_threshold {
                        PeerHealth::Alive
                    } else if confirmed.contains(idx) && peer.consecutive_faults >= cfg.lost_threshold {
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
                    "health transition"
                );
                peer.health = new;
                transitions += 1;
            }
        }
        transitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roster_with_one_peer(faults: u32, healthy: u32, health: PeerHealth) -> PeerRoster {
        let mut r = PeerRoster::new(HealthConfig::default());
        r.peers.push(PeerInfo { id: 1, health, consecutive_faults: faults, consecutive_healthy_cycles: healthy }).unwrap();
        r
    }

    #[test]
    fn alive_stays_alive_without_confirmed_bit() {
        let mut r = roster_with_one_peer(5, 0, PeerHealth::Alive);
        assert_eq!(r.apply_transitions(PeerMask::EMPTY), 0);
        assert_eq!(r.peers()[0].health, PeerHealth::Alive);
    }

    #[test]
    fn alive_escalates_when_confirmed_and_over_threshold() {
        let mut r = roster_with_one_peer(3, 0, PeerHealth::Alive);
        let mut mask = PeerMask::EMPTY;
        mask.set(0);
        assert_eq!(r.apply_transitions(mask), 1);
        assert_eq!(r.peers()[0].health, PeerHealth::Suspect);
    }

    #[test]
    fn suspect_recovers_unconditionally() {
        let mut r = roster_with_one_peer(0, 20, PeerHealth::Suspect);
        assert_eq!(r.apply_transitions(PeerMask::EMPTY), 1);
        assert_eq!(r.peers()[0].health, PeerHealth::Alive);
    }

    #[test]
    fn suspect_escalates_when_confirmed_and_over_threshold() {
        let mut r = roster_with_one_peer(10, 0, PeerHealth::Suspect);
        let mut mask = PeerMask::EMPTY;
        mask.set(0);
        assert_eq!(r.apply_transitions(mask), 1);
        assert_eq!(r.peers()[0].health, PeerHealth::Lost);
    }

    #[test]
    fn lost_is_terminal() {
        let mut r = roster_with_one_peer(0, 100, PeerHealth::Lost);
        let mut mask = PeerMask::EMPTY;
        mask.set(0);
        assert_eq!(r.apply_transitions(mask), 0);
        assert_eq!(r.peers()[0].health, PeerHealth::Lost);
    }
}

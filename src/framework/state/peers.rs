use crate::framework::config::MAX_PEERS;
use crate::framework::types::PeerMask;
use heapless::Vec;
use tracing::{debug, warn};

/// Peer status. Peers start `Alive` and transition to `Lost` on the first
/// vote-confirmed exclusion. `Lost` is terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerHealth {
    Alive,
    Probation,
    Lost,
}

/// Discovery-time and runtime metadata for one peer.
#[derive(Debug, Clone, Copy)]
pub struct PeerInfo {
    pub id: u8,
    pub health: PeerHealth,
    pub probation_cycles_ok: u32,
}

/// Discovery and management errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryError {
    WrongNodeCount { found: u8, expected: u8 },
    UnknownPeer,
}

/// The set of known peers plus their exclusion status.
///
/// Discovery is a two-step process: peers are added one by one via
/// `discover`, then `finalize` locks the set. After that the peer list is
/// frozen — foreign nodes are rejected upstream.
pub struct PeerRoster {
    peers: Vec<PeerInfo, MAX_PEERS>,
    discovery_locked: bool,
}

impl PeerRoster {
    pub fn new() -> Self {
        Self { peers: Vec::new(), discovery_locked: false }
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
            .push(PeerInfo { id, health: PeerHealth::Alive, probation_cycles_ok: 0 })
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

    /// Mark the peers in `confirmed` as `Lost`. Returns the number of
    /// transitions applied.
    pub fn exclude(&mut self, confirmed: PeerMask) -> usize {
        let mut transitions = 0usize;
        for (idx, peer) in self.peers.iter_mut().enumerate() {
            if confirmed.contains(idx) && peer.health != PeerHealth::Lost {
                warn!(peer_id = peer.id, "peer excluded");
                peer.health = PeerHealth::Lost;
                transitions += 1;
            }
        }
        transitions
    }

pub fn readmit(&mut self, peer_id: u8) -> bool {
    for peer in self.peers.iter_mut() {
        if peer.id == peer_id && peer.health == PeerHealth::Lost {
            peer.health = PeerHealth::Probation;
            peer.probation_cycles_ok = 0;
            return true;
        }
    }
    false
}

    pub fn voting_peer_count(&self) -> usize {
        self.peers.iter().filter(|p| p.health == PeerHealth::Alive).count()
    }

    pub fn tick_probation(&mut self, threshold: u32) -> usize {
    let mut promoted = 0;
    for peer in self.peers.iter_mut() {
        if peer.health == PeerHealth::Probation {
            peer.probation_cycles_ok = peer.probation_cycles_ok.saturating_add(1);
            if peer.probation_cycles_ok >= threshold {
                peer.health = PeerHealth::Alive;
                peer.probation_cycles_ok = 0;
                promoted += 1;
            }
        }
    }
    promoted
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roster_with_one_peer(health: PeerHealth) -> PeerRoster {
        let mut r = PeerRoster::new();
        r.peers.push(PeerInfo { id: 1, health }).unwrap();
        r
    }

    #[test]
    fn exclude_ignores_empty_mask() {
        let mut r = roster_with_one_peer(PeerHealth::Alive);
        assert_eq!(r.exclude(PeerMask::EMPTY), 0);
        assert_eq!(r.peers()[0].health, PeerHealth::Alive);
    }

    #[test]
    fn exclude_sets_alive_to_lost() {
        let mut r = roster_with_one_peer(PeerHealth::Alive);
        let mut mask = PeerMask::EMPTY;
        mask.set(0);
        assert_eq!(r.exclude(mask), 1);
        assert_eq!(r.peers()[0].health, PeerHealth::Lost);
    }

    #[test]
    fn exclude_is_idempotent() {
        let mut r = roster_with_one_peer(PeerHealth::Lost);
        let mut mask = PeerMask::EMPTY;
        mask.set(0);
        assert_eq!(r.exclude(mask), 0);
    }
}
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
    /// Cycles completed under probation. Derived from `current_seq`
    /// in `refresh_probation`, never counted up locally.
    pub probation_cycles_ok: u32,
    /// Cycle counter value at which this peer entered probation.
    /// Local bookkeeping only, never on the wire: the wire carries
    /// `probation_cycles_ok`, from which a receiver reconstructs this.
    pub probation_start_seq: u32,
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

impl Default for PeerRoster {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerRoster {
    pub fn new() -> Self {
        Self {
            peers: Vec::new(),
            discovery_locked: false,
        }
    }

    pub fn set_peer_from_snapshot(
        &mut self,
        id: u8,
        health: PeerHealth,
        probation_cycles_ok: u32,
        current_seq: u32,
    ) -> bool {
        for peer in self.peers.iter_mut() {
            if peer.id == id {
                peer.health = health;
                peer.probation_cycles_ok = probation_cycles_ok;
                peer.probation_start_seq = current_seq.wrapping_sub(probation_cycles_ok);
                return true;
            }
        }
        false
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
            warn!(
                peer_id = id,
                count = self.peers.len(),
                limit = max_peers,
                "peer roster full"
            );
            return Ok(());
        }
        self.peers
            .push(PeerInfo {
                id,
                health: PeerHealth::Alive,
                probation_cycles_ok: 0,
                probation_start_seq: 0,
            })
            .expect("push failed despite capacity check");
        Ok(())
    }

    /// Close discovery. Requires exactly `nominal` total nodes
    /// (own + peers). Sorts by id so peer_index maps to a stable ordering.
    pub fn finalize(&mut self, own_id: u8, nominal: u8) -> Result<(), DiscoveryError> {
        let found = self.peers.len() as u8 + 1;
        if found != nominal {
            return Err(DiscoveryError::WrongNodeCount {
                found,
                expected: nominal,
            });
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

    pub fn readmit(&mut self, peer_id: u8, current_seq: u32) -> bool {
        for peer in self.peers.iter_mut() {
            if peer.id == peer_id && peer.health == PeerHealth::Lost {
                peer.health = PeerHealth::Probation;
                peer.probation_cycles_ok = 0;
                peer.probation_start_seq = current_seq;
                return true;
            }
        }
        false
    }

    pub fn voting_peer_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| p.health == PeerHealth::Alive)
            .count()
    }

    /// Re-derive probation progress from the fabric-agreed cycle
    /// counter and promote every peer that has served its term.
    ///
    /// The progress is a difference against `probation_start_seq`, not
    /// a locally incremented counter. `current_seq` is part of the
    /// system-state CRC and therefore identical on all nodes, so every
    /// node promotes in the same cycle even if one of them took a
    /// detour through ErrorManagement in between. A local counter would
    /// drift apart on exactly those paths and surface one phase later
    /// as an unexplained CRC divergence.
    pub fn refresh_probation(&mut self, current_seq: u32, threshold: u32) -> usize {
        let mut promoted = 0;
        for peer in self.peers.iter_mut() {
            if peer.health != PeerHealth::Probation {
                continue;
            }
            let elapsed = current_seq.wrapping_sub(peer.probation_start_seq);
            if elapsed >= threshold {
                peer.health = PeerHealth::Alive;
                peer.probation_cycles_ok = 0;
                peer.probation_start_seq = 0;
                promoted += 1;
            } else {
                peer.probation_cycles_ok = elapsed;
            }
        }
        promoted
    }

    pub fn active_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| p.health != PeerHealth::Lost)
            .count()
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
mod probation_tests {
    //! Probation progress is derived from the fabric-agreed cycle
    //! counter, not counted locally. These tests pin the property that
    //! two nodes which evaluate at the same `current_seq` reach the same
    //! verdict even if they called `refresh_probation` a different
    //! number of times in between.
    use super::*;

    const THRESHOLD: u32 = 10;

    fn roster_with_lost_peer() -> PeerRoster {
        let mut r = PeerRoster::new();
        r.discover(1, 0, 2).expect("discover");
        r.discover(2, 0, 2).expect("discover");
        r.finalize(0, 3).expect("finalize");
        let mut mask = PeerMask::EMPTY;
        mask.set(0); // peer id 1 sits in slot 0 after sorting
        assert_eq!(r.exclude(mask), 1);
        r
    }

    #[test]
    fn readmitted_peer_is_promoted_after_threshold_cycles() {
        let mut r = roster_with_lost_peer();
        assert!(r.readmit(1, 100));
        assert_eq!(r.peers()[0].health, PeerHealth::Probation);

        assert_eq!(r.refresh_probation(105, THRESHOLD), 0);
        assert_eq!(r.peers()[0].probation_cycles_ok, 5);

        assert_eq!(r.refresh_probation(110, THRESHOLD), 1);
        assert_eq!(r.peers()[0].health, PeerHealth::Alive);
    }

    #[test]
    fn skipped_refresh_calls_do_not_delay_promotion() {
        // The node that missed intermediate cycles (e.g. because it went
        // through ErrorManagement) must promote at the same seq as one
        // that refreshed every cycle. A local counter would lag here.
        let mut sparse = roster_with_lost_peer();
        let mut dense = roster_with_lost_peer();
        assert!(sparse.readmit(1, 100));
        assert!(dense.readmit(1, 100));

        for seq in 101..=110 {
            dense.refresh_probation(seq, THRESHOLD);
        }
        sparse.refresh_probation(110, THRESHOLD);

        assert_eq!(sparse.peers()[0].health, PeerHealth::Alive);
        assert_eq!(dense.peers()[0].health, PeerHealth::Alive);
    }

    #[test]
    fn snapshot_reconstructs_the_start_seq() {
        // A rejoining node learns only `probation_cycles_ok` from the
        // wire and has to derive the anchor from it, otherwise its
        // promotion cycle drifts against the senders'.
        let mut r = roster_with_lost_peer();
        assert!(r.set_peer_from_snapshot(1, PeerHealth::Probation, 4, 100));
        assert_eq!(r.peers()[0].probation_start_seq, 96);
        assert_eq!(r.refresh_probation(105, THRESHOLD), 0);
        assert_eq!(r.refresh_probation(106, THRESHOLD), 1);
    }

    #[test]
    fn alive_peers_are_untouched() {
        let mut r = roster_with_lost_peer();
        assert_eq!(r.refresh_probation(9_999, THRESHOLD), 0);
        assert_eq!(r.peers()[1].health, PeerHealth::Alive);
    }
}

use serde::Deserialize;
use std::net::Ipv4Addr;
use std::time::Duration;

/// Compile-time upper bound on total nodes. Limited by the 8-bit peer mask.
pub const MAX_TOTAL_NODES: usize = 8;

/// Peer slots per node (own node excluded).
pub const MAX_PEERS: usize = MAX_TOTAL_NODES - 1;

/// Maximum number of dissenter indices returned by `Voter::find_dissenters`.
pub const MAX_DISSENTERS: usize = 16;

/// M-oo-N participant configuration.
///
/// `nominal` is the full node count expected in normal operation and must
/// match the discovered set exactly. `minimum` is the safety floor; falling
/// below triggers failsafe.
#[derive(Debug, Clone, Copy)]
pub struct ParticipantConfig {
    pub nominal_participants: u8,
    pub min_participants: u8,
}

impl ParticipantConfig {
    /// Construct a config. Panics on invalid values (construction-time
    /// invariant, must fail loudly at startup).
    pub fn new(minimum: u8, nominal: u8) -> Self {
        assert!(minimum >= 1, "min_participants must be >= 1");
        assert!(minimum <= nominal, "min ({minimum}) must not exceed nominal ({nominal})");
        assert!(
            nominal as usize <= MAX_TOTAL_NODES,
            "nominal ({nominal}) exceeds MAX_TOTAL_NODES ({MAX_TOTAL_NODES})"
        );
        Self {
            nominal_participants: nominal,
            min_participants: minimum,
        }
    }

    /// Number of node failures tolerated before entering failsafe: `N - M`.
    pub fn tolerable_failures(&self) -> u8 {
        self.nominal_participants - self.min_participants
    }

    /// Peer slots per node (own node excluded).
    pub fn max_peers(&self) -> usize {
        (self.nominal_participants - 1) as usize
    }
}

/// Health-transition thresholds. Applied by `PeerRoster` on fault-counter
/// changes; actual transitions are gated by majority vote in ErrorManagement.
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

/// All timing parameters for the operational cycle and its subphases.
#[derive(Debug, Clone, Copy)]
pub struct CycleTiming {
    pub cycle_duration: Duration,
    pub init_sync_timeout: Duration,
    pub peer_sync_timeout: Duration,
    pub peer_sync_request_interval: Duration,
    pub cycle_sync_timeout: Duration,
    pub share_timeout: Duration,
    pub ack_timeout: Duration,
    pub error_management_vote_timeout: Duration,
    pub stale_threshold: Duration,
    pub resync_interval_cycles: u32,
}

/// UDP multicast transport binding for peer-to-peer traffic.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub interface_name: String,
    pub multicast_group: Ipv4Addr,
    pub port: u16,
    pub self_node_id: u8,
    pub self_session_id: u64,
    pub initial_seq_num: u32,
}

/// UDP multicast binding for the diagnostic side-channel.
#[derive(Debug, Clone)]
pub struct DiagnosticConfig {
    pub enabled: bool,
    pub interface_name: String,
    pub multicast_group: Ipv4Addr,
    pub port: u16,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interface_name: "lo".into(),
            multicast_group: Ipv4Addr::new(239, 10, 0, 2),
            port: 6666,
        }
    }
}

/// Root of a node's TOML configuration file. One file per node — `own_id`
/// distinguishes them; the other sections are typically identical across
/// the fabric.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub own_id: u8,
    pub participants: ParticipantSection,
    pub health: HealthSection,
    pub timing: TimingSection,
    pub transport: TransportSection,
    pub diagnostic: DiagnosticSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParticipantSection {
    pub nominal: u8,
    pub minimum: u8,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HealthSection {
    pub suspect_threshold: u32,
    pub lost_threshold: u32,
    pub recovery_threshold: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TimingSection {
    pub cycle_ms: u64,
    pub init_sync_ms: u64,
    pub peer_sync_ms: u64,
    pub peer_sync_request_interval_ms: u64,
    pub cycle_sync_ms: u64,
    pub share_ms: u64,
    pub ack_ms: u64,
    pub error_management_vote_ms: u64,
    pub stale_ms: u64,
    pub resync_interval_cycles: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TransportSection {
    pub interface: String,
    pub multicast_group: Ipv4Addr,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiagnosticSection {
    pub enabled: bool,
    pub interface: String,
    pub multicast_group: Ipv4Addr,
    pub port: u16,
}

impl NodeConfig {
    pub fn participants(&self) -> ParticipantConfig {
        ParticipantConfig::new(self.participants.minimum, self.participants.nominal)
    }

    pub fn health(&self) -> HealthConfig {
        HealthConfig {
            suspect_threshold: self.health.suspect_threshold,
            lost_threshold: self.health.lost_threshold,
            recovery_threshold: self.health.recovery_threshold,
        }
    }

    pub fn timing(&self) -> CycleTiming {
        let t = &self.timing;
        CycleTiming {
            cycle_duration: Duration::from_millis(t.cycle_ms),
            init_sync_timeout: Duration::from_millis(t.init_sync_ms),
            peer_sync_timeout: Duration::from_millis(t.peer_sync_ms),
            peer_sync_request_interval: Duration::from_millis(t.peer_sync_request_interval_ms),
            cycle_sync_timeout: Duration::from_millis(t.cycle_sync_ms),
            share_timeout: Duration::from_millis(t.share_ms),
            ack_timeout: Duration::from_millis(t.ack_ms),
            error_management_vote_timeout: Duration::from_millis(t.error_management_vote_ms),
            stale_threshold: Duration::from_millis(t.stale_ms),
            resync_interval_cycles: t.resync_interval_cycles,
        }
    }

    pub fn transport(&self, self_session_id: u64) -> TransportConfig {
        TransportConfig {
            interface_name: self.transport.interface.clone(),
            multicast_group: self.transport.multicast_group,
            port: self.transport.port,
            self_node_id: self.own_id,
            self_session_id,
            initial_seq_num: 0,
        }
    }

    pub fn diagnostic(&self) -> DiagnosticConfig {
        DiagnosticConfig {
            enabled: self.diagnostic.enabled,
            interface_name: self.diagnostic.interface.clone(),
            multicast_group: self.diagnostic.multicast_group,
            port: self.diagnostic.port,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn participants_valid() {
        let c = ParticipantConfig::new(2, 3);
        assert_eq!(c.tolerable_failures(), 1);
        assert_eq!(c.max_peers(), 2);
    }

    #[test]
    #[should_panic]
    fn participants_zero_minimum() {
        ParticipantConfig::new(0, 3);
    }

    #[test]
    #[should_panic]
    fn participants_minimum_over_nominal() {
        ParticipantConfig::new(3, 2);
    }

    #[test]
    #[should_panic]
    fn participants_nominal_over_max() {
        ParticipantConfig::new(5, 9);
    }
}

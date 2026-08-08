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
#[derive(Debug, Clone, Copy)]
pub struct ParticipantConfig {
    pub nominal_participants: u8,
    pub min_participants: u8,
    pub probation_cycles: u32,
}

impl ParticipantConfig {
    /// Construct a config. Panics on invalid values (construction-time
    /// invariant, must fail loudly at startup).
    pub fn new(minimum: u8, nominal: u8, probation_cycles: u32) -> Self {
        assert!(minimum >= 1, "min_participants must be >= 1");
        assert!(
            minimum <= nominal,
            "min ({minimum}) must not exceed nominal ({nominal})"
        );
        assert!(
            nominal as usize <= MAX_TOTAL_NODES,
            "nominal ({nominal}) exceeds MAX_TOTAL_NODES ({MAX_TOTAL_NODES})"
        );
        Self {
            nominal_participants: nominal,
            min_participants: minimum,
            probation_cycles,
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

/// All timing parameters for the operational cycle and its subphases.
///
/// Naming convention:
/// - `_duration`  — wall-clock length of a repeating window
/// - `_timeout`   — phase deadline before falling into ErrorManagement
/// - `_interval`  — inter-send spacing inside a collect_phase loop
/// - `_cycles`    — count of cycles, not a time
#[derive(Debug, Clone, Copy)]
pub struct CycleTiming {
    // Cycle scheduling.
    pub cycle_duration: Duration,

    // Phase timeouts.
    pub init_sync_timeout: Duration,
    pub peer_sync_timeout: Duration,
    pub cycle_sync_timeout: Duration,
    pub share_inputs_timeout: Duration,
    pub share_result_timeout: Duration,
    pub send_ack_timeout: Duration,
    pub error_management_timeout: Duration,
    pub system_state_crc_timeout: Duration,
    pub system_state_sync_timeout: Duration,
    pub resync_lost_peer_returning_timeout: Duration,
    pub resync_lost_peer_healthy_timeout: Duration,

    // Send intervals inside collect_phase loops.
    pub init_sync_send_interval: Duration,
    pub peer_sync_request_interval: Duration,
    pub cycle_sync_send_interval: Duration,
    pub share_inputs_send_interval: Duration,
    pub share_result_send_interval: Duration,
    pub send_ack_send_interval: Duration,
    pub error_management_send_interval: Duration,
    pub system_state_crc_send_interval: Duration,
    pub system_state_sync_send_interval: Duration,
    pub resync_send_interval: Duration,

    // Misc.
    pub stale_frame_threshold: Duration,
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
    pub timing: TimingSection,
    pub transport: TransportSection,
    pub diagnostic: DiagnosticSection,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParticipantSection {
    pub nominal: u8,
    pub minimum: u8,
    pub probation_cycles: u32,
}

/// TOML timing section.
///
/// All time values are milliseconds and end in `_timeout_ms`,
/// `_interval_ms`, `_duration_ms`, or `_threshold_ms` for clarity.
#[derive(Debug, Clone, Deserialize)]
pub struct TimingSection {
    pub cycle_duration_ms: u64,

    pub init_sync_timeout_ms: u64,
    pub peer_sync_timeout_ms: u64,
    pub cycle_sync_timeout_ms: u64,
    pub share_inputs_timeout_ms: u64,
    pub share_result_timeout_ms: u64,
    pub send_ack_timeout_ms: u64,
    pub error_management_timeout_ms: u64,
    pub system_state_crc_timeout_ms: u64,
    pub system_state_sync_timeout_ms: u64,
    pub resync_lost_peer_returning_timeout_ms: u64,
    pub resync_lost_peer_healthy_timeout_ms: u64,

    pub init_sync_send_interval_ms: u64,
    pub peer_sync_request_interval_ms: u64,
    pub cycle_sync_send_interval_ms: u64,
    pub share_inputs_send_interval_ms: u64,
    pub share_result_send_interval_ms: u64,
    pub send_ack_send_interval_ms: u64,
    pub error_management_send_interval_ms: u64,
    pub system_state_crc_send_interval_ms: u64,
    pub system_state_sync_send_interval_ms: u64,
    pub resync_send_interval_ms: u64,

    pub stale_frame_threshold_ms: u64,
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
        ParticipantConfig::new(
            self.participants.minimum,
            self.participants.nominal,
            self.participants.probation_cycles,
        )
    }

    pub fn timing(&self) -> CycleTiming {
        let t = &self.timing;
        let ms = Duration::from_millis;
        CycleTiming {
            cycle_duration: ms(t.cycle_duration_ms),

            init_sync_timeout: ms(t.init_sync_timeout_ms),
            peer_sync_timeout: ms(t.peer_sync_timeout_ms),
            cycle_sync_timeout: ms(t.cycle_sync_timeout_ms),
            share_inputs_timeout: ms(t.share_inputs_timeout_ms),
            share_result_timeout: ms(t.share_result_timeout_ms),
            send_ack_timeout: ms(t.send_ack_timeout_ms),
            error_management_timeout: ms(t.error_management_timeout_ms),
            system_state_crc_timeout: ms(t.system_state_crc_timeout_ms),
            system_state_sync_timeout: ms(t.system_state_sync_timeout_ms),
            resync_lost_peer_returning_timeout: ms(t.resync_lost_peer_returning_timeout_ms),
            resync_lost_peer_healthy_timeout: ms(t.resync_lost_peer_healthy_timeout_ms),

            init_sync_send_interval: ms(t.init_sync_send_interval_ms),
            peer_sync_request_interval: ms(t.peer_sync_request_interval_ms),
            cycle_sync_send_interval: ms(t.cycle_sync_send_interval_ms),
            share_inputs_send_interval: ms(t.share_inputs_send_interval_ms),
            share_result_send_interval: ms(t.share_result_send_interval_ms),
            send_ack_send_interval: ms(t.send_ack_send_interval_ms),
            error_management_send_interval: ms(t.error_management_send_interval_ms),
            system_state_crc_send_interval: ms(t.system_state_crc_send_interval_ms),
            system_state_sync_send_interval: ms(t.system_state_sync_send_interval_ms),
            resync_send_interval: ms(t.resync_send_interval_ms),

            stale_frame_threshold: ms(t.stale_frame_threshold_ms),
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
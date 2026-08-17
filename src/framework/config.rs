use serde::Deserialize;
use std::net::Ipv4Addr;
use std::time::Duration;
use sha2::{Digest, Sha256};

pub const MAX_TOTAL_NODES: usize = 8;
pub const MAX_PEERS: usize = MAX_TOTAL_NODES - 1;
pub const MAX_DISSENTERS: usize = 16;

#[derive(Debug, Clone, Copy)]
pub struct ParticipantConfig {
    pub nominal_participants: u8,
    pub min_participants: u8,
    pub probation_cycles: u32,
}

impl ParticipantConfig {
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

    pub fn tolerable_failures(&self) -> u8 {
        self.nominal_participants - self.min_participants
    }

    pub fn max_peers(&self) -> usize {
        (self.nominal_participants - 1) as usize
    }
}

/// Timing parameters. Organized into three groups:
///
/// - **Cycle scheduling**: one wall-clock period.
/// - **In-cycle phase deadlines**: offsets from the cycle anchor
///   (`last_cycle_start` in the runner), strictly monotonic. All healthy
///   nodes see the same deadline at the same wall-clock instant, which
///   keeps their phase transitions aligned regardless of intra-node jitter.
/// - **Non-cycle phase timeouts**: relative to phase entry. Used for
///   phases outside the cycle body (init, sync, error mgmt, resync).
///
/// The two resync timeouts are semantically distinct:
/// - `resync_returning_timeout` is for a node coming back from lost —
///   it must wait for the running fabric to acknowledge it, and needs
///   more headroom because cold-start latency dominates.
/// - `resync_healthy_timeout` is for an established node waiting on the
///   returning peer's state frame; can be shorter.
///
/// A single `send_interval` covers all `collect_phase` retransmit loops.
/// It must be smaller than every timeout. 1 ms is a safe default.
#[derive(Debug, Clone, Copy)]
pub struct CycleTiming {
    // Cycle scheduling.
    pub cycle_duration: Duration,

    // In-cycle phase deadlines (offsets from cycle anchor).
    pub share_inputs_offset: Duration,
    pub share_result_offset: Duration,
    pub send_ack_offset: Duration,
    pub crc_offset: Duration,

    // Non-cycle phase timeouts (relative to phase entry).
    pub init_sync_timeout: Duration,
    pub peer_sync_timeout: Duration,
    pub cycle_sync_timeout: Duration,
    pub error_mgmt_timeout: Duration,
    pub state_sync_timeout: Duration,
    pub resync_returning_timeout: Duration,
    pub resync_healthy_timeout: Duration,

    // Universal retransmit interval for collect_phase loops.
    pub send_interval: Duration,

    // Misc.
    pub stale_frame_threshold: Duration,
    pub resync_interval_cycles: u32,
}

impl CycleTiming {
    /// Validate timing at startup. Panics on inconsistency.
    pub fn validate(&self) {
        // In-cycle offsets strictly monotonic and fit into the cycle.
        assert!(
            self.share_inputs_offset < self.share_result_offset,
            "share_inputs_offset must precede share_result_offset"
        );
        assert!(
            self.share_result_offset < self.send_ack_offset,
            "share_result_offset must precede send_ack_offset"
        );
        assert!(
            self.send_ack_offset < self.crc_offset,
            "send_ack_offset must precede crc_offset"
        );
        assert!(
            self.crc_offset < self.cycle_duration,
            "crc_offset must fit into cycle_duration"
        );

        // send_interval must be strictly smaller than every timeout,
        // otherwise collect_phase would never retransmit before timing out.
        let all_timeouts = [
            self.cycle_sync_timeout,
            self.error_mgmt_timeout,
            self.peer_sync_timeout,
            self.state_sync_timeout,
            self.init_sync_timeout,
            self.resync_returning_timeout,
            self.resync_healthy_timeout,
            self.share_inputs_offset,
        ];
        let min_timeout = all_timeouts.iter().min().copied().unwrap();
        assert!(
            self.send_interval < min_timeout,
            "send_interval ({:?}) must be smaller than smallest timeout ({:?})",
            self.send_interval,
            min_timeout
        );
    }
}

#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub interface_name: String,
    pub multicast_group: Ipv4Addr,
    pub port: u16,
    pub self_node_id: u8,
    pub self_session_id: u64,
    pub initial_seq_num: u32,
}

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

#[derive(Debug, Clone, Deserialize)]
pub struct TimingSection {
    pub cycle_duration_ms: u64,

    pub share_inputs_offset_ms: u64,
    pub share_result_offset_ms: u64,
    pub send_ack_offset_ms: u64,
    pub crc_offset_ms: u64,

    pub init_sync_timeout_ms: u64,
    pub peer_sync_timeout_ms: u64,
    pub cycle_sync_timeout_ms: u64,
    pub error_mgmt_timeout_ms: u64,
    pub state_sync_timeout_ms: u64,
    pub resync_returning_timeout_ms: u64,
    pub resync_healthy_timeout_ms: u64,

    pub send_interval_ms: u64,

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

        pub fn verify_and_parse(text: &str) -> Result<Self, ConfigError> {
        // 1. Parse into a generic toml::Value so we can walk the tree
        //    for the canonical form BEFORE deserializing.
        let value: toml::Value = toml::from_str(text)
            .map_err(|e| ConfigError::Parse(e.to_string()))?;

        // 2. Read the integrity section.
        let integrity = value
            .get("integrity")
            .and_then(|v| v.as_table())
            .ok_or(ConfigError::MissingIntegrity)?;
        let algo = integrity
            .get("algo")
            .and_then(|v| v.as_str())
            .ok_or(ConfigError::MissingIntegrityField("algo"))?;
        if algo != "sha256" {
            return Err(ConfigError::UnsupportedAlgo(algo.to_string()));
        }
        let expected_digest = integrity
            .get("checksum")
            .and_then(|v| v.as_str())
            .ok_or(ConfigError::MissingIntegrityField("checksum"))?;

        // 3. Compute canonical form + digest, excluding [integrity].
        let canonical = canonical_bytes(&value);
        let mut hasher = Sha256::new();
        hasher.update(&canonical);
        let actual_digest = hex_lower(&hasher.finalize());

        if actual_digest != expected_digest {
            return Err(ConfigError::ChecksumMismatch {
                expected: expected_digest.to_string(),
                actual: actual_digest,
            });
        }

        // 4. Real deserialize into NodeConfig.
        //    NodeConfig doesn't include the integrity field so serde
        //    will happily ignore it (deny_unknown_fields would need to
        //    be off, which is the default).
        toml::from_str::<NodeConfig>(text)
            .map_err(|e| ConfigError::Parse(e.to_string()))
    }

    pub fn timing(&self) -> CycleTiming {
        let t = &self.timing;
        let ms = Duration::from_millis;
        let ct = CycleTiming {
            cycle_duration: ms(t.cycle_duration_ms),

            share_inputs_offset: ms(t.share_inputs_offset_ms),
            share_result_offset: ms(t.share_result_offset_ms),
            send_ack_offset: ms(t.send_ack_offset_ms),
            crc_offset: ms(t.crc_offset_ms),

            init_sync_timeout: ms(t.init_sync_timeout_ms),
            peer_sync_timeout: ms(t.peer_sync_timeout_ms),
            cycle_sync_timeout: ms(t.cycle_sync_timeout_ms),
            error_mgmt_timeout: ms(t.error_mgmt_timeout_ms),
            state_sync_timeout: ms(t.state_sync_timeout_ms),
            resync_returning_timeout: ms(t.resync_returning_timeout_ms),
            resync_healthy_timeout: ms(t.resync_healthy_timeout_ms),

            send_interval: ms(t.send_interval_ms),

            stale_frame_threshold: ms(t.stale_frame_threshold_ms),
            resync_interval_cycles: t.resync_interval_cycles,
        };
        ct.validate();
        ct
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

#[derive(Debug, Clone, Deserialize)]
pub struct IntegritySection {
    pub algo: String,
    pub checksum: String,
}

/// Walk the toml::Value tree, produce the canonical bytes.
/// Excludes the top-level `integrity` key.
fn canonical_bytes(root: &toml::Value) -> Vec<u8> {
    let mut leaves: Vec<(String, String)> = Vec::new();
    if let Some(t) = root.as_table() {
        for (k, v) in t.iter() {
            if k == "integrity" {
                continue;
            }
            flatten(k, v, &mut leaves);
        }
    }
    leaves.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out = String::new();
    for (i, (k, v)) in leaves.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(k);
        out.push('=');
        out.push_str(v);
    }
    out.into_bytes()
}

fn flatten(prefix: &str, v: &toml::Value, out: &mut Vec<(String, String)>) {
    match v {
        toml::Value::Table(t) => {
            for (k, sub) in t.iter() {
                let path = format!("{prefix}.{k}");
                flatten(&path, sub, out);
            }
        }
        _ => out.push((prefix.to_string(), value_to_json(v))),
    }
}

/// JSON-encode a scalar toml::Value the same way `json.dumps` would.
fn value_to_json(v: &toml::Value) -> String {
    match v {
        toml::Value::String(s) => {
            // JSON string with standard escaping.
            let mut out = String::with_capacity(s.len() + 2);
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => {
                        out.push_str(&format!("\\u{:04x}", c as u32));
                    }
                    c => out.push(c),
                }
            }
            out.push('"');
            out
        }
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Float(f) => {
            // Match Python json.dumps behaviour: no unnecessary trailing zeros,
            // but always include a decimal point / exponent.
            let s = format!("{}", f);
            if s.contains('.') || s.contains('e') || s.contains('E') {
                s
            } else {
                format!("{}.0", s)
            }
        }
        toml::Value::Datetime(dt) => format!("\"{}\"", dt),  // rare in configs
        toml::Value::Array(_) | toml::Value::Table(_) => {
            unreachable!("flatten already peeled tables; arrays not used in our schema")
        }
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[derive(Debug)]
pub enum ConfigError {
    Parse(String),
    MissingIntegrity,
    MissingIntegrityField(&'static str),
    UnsupportedAlgo(String),
    ChecksumMismatch { expected: String, actual: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Parse(msg) => write!(f, "config parse failed: {msg}"),
            Self::MissingIntegrity => write!(f, "missing [integrity] section"),
            Self::MissingIntegrityField(name) => {
                write!(f, "[integrity].{name} missing or wrong type")
            }
            Self::UnsupportedAlgo(a) => write!(f, "unsupported checksum algo: {a}"),
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "checksum mismatch: file says {expected}, computed {actual}"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

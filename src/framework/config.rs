//! Configuration: the file on disk and the typed values derived from it.
//!
//! A node is configured entirely by one TOML file. [`NodeConfig`] is the
//! literal file layout, and the `participants`, `timing`, `transport`
//! and `diagnostic` accessors project it onto the value types the
//! framework works with, validating as they go.
//!
//! The file carries its own SHA-256 in an `[integrity]` section, and
//! [`NodeConfig::verify_and_parse`] refuses to load a file whose digest
//! does not match. The digest is computed over a canonical form, so
//! comments, whitespace and key order do not affect it, while every
//! value does. Regenerate it with `python -m harness.config_gen` after
//! any edit.

use serde::Deserialize;
use std::net::Ipv4Addr;
use std::time::Duration;
use sha2::{Digest, Sha256};

/// Hard upper bound on nodes in one fabric. Sets the width of every
/// peer mask, so it cannot exceed 8 without changing the wire format.
pub const MAX_TOTAL_NODES: usize = 8;
/// Hard upper bound on peers, own node excluded. Sizes every per-peer
/// buffer.
pub const MAX_PEERS: usize = MAX_TOTAL_NODES - 1;
/// Capacity of the dissenter list a voter may report in one cycle.
pub const MAX_DISSENTERS: usize = 16;

/// Upper bound on the wire size of a single `ApplicationData` snapshot.
/// The framework carries application data inside the SystemStateSnapshot
/// payload as a fixed-size byte buffer plus a length prefix, so this
/// value caps how much domain state can ride along with the state-sync
/// exchange. Increase if the domain needs to sync larger blobs — the
/// only cost is a bigger `MAX_FRAME_SIZE` at the wire layer.
///
/// `ApplicationData::WIRE_SIZE` is asserted against this bound at
/// compile time inside `UdpFrame`.
pub const MAX_APPLICATION_DATA_SIZE: usize = 64;

/// Node counts and the probation term.
#[derive(Debug, Clone, Copy)]
pub struct ParticipantConfig {
    /// Nodes expected at startup. Discovery does not close below this.
    pub nominal_participants: u8,
    /// Safety floor on active nodes. Dropping below it is failsafe.
    pub min_participants: u8,
    /// Cycles a readmitted node serves before it votes again.
    pub probation_cycles: u32,
}

impl ParticipantConfig {
    /// Build a participant configuration.
    ///
    /// # Panics
    ///
    /// If `minimum` is zero, exceeds `nominal`, or `nominal` exceeds
    /// [`MAX_TOTAL_NODES`]. This runs at startup, before the node joins
    /// the fabric, so a misconfiguration stops the process rather than
    /// producing a silently degraded system.
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

    /// Node failures the configuration can absorb before the fabric
    /// falls below its floor.
    pub fn tolerable_failures(&self) -> u8 {
        self.nominal_participants - self.min_participants
    }

    /// Peers this node expects, own node excluded.
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
    /// Wall-clock length of one cycle.
    pub cycle_duration: Duration,

    /// Deadline for ShareInputs, as an offset from the cycle anchor.
    pub share_inputs_offset: Duration,
    /// Deadline for ShareResult, as an offset from the cycle anchor.
    pub share_result_offset: Duration,
    /// Deadline for SendAck, as an offset from the cycle anchor.
    pub send_ack_offset: Duration,
    /// Deadline for the CRC exchange, as an offset from the cycle
    /// anchor. Must still fit inside `cycle_duration`.
    pub crc_offset: Duration,

    /// How long discovery may take before the node gives up.
    pub init_sync_timeout: Duration,
    /// How long one clock-sync round may take.
    pub clock_sync_timeout: Duration,
    /// How long the cycle barrier waits for the last beacon.
    pub cycle_sync_timeout: Duration,
    /// How long the exclusion vote waits for the last proposal.
    pub error_mgmt_timeout: Duration,
    /// How long the snapshot exchange may take.
    pub state_sync_timeout: Duration,
    /// Patience of the node that is coming back. Needs the most
    /// headroom, since cold-start latency dominates here.
    pub resync_returning_timeout: Duration,
    /// Patience of an established node waiting on the returning peer.
    pub resync_healthy_timeout: Duration,

    /// Retransmit interval used by every waiting phase. Must be
    /// strictly smaller than the shortest timeout above.
    pub send_interval: Duration,

    /// Age past which a received frame is discarded as stale, measured
    /// against the sender's clock after offset translation.
    pub stale_frame_threshold: Duration,
    /// Cycles between two clock-sync rounds.
    pub resync_interval_cycles: u32,
}

impl CycleTiming {
    /// Validate timing at startup. Panics on inconsistency.
    /// Check the ordering invariants.
    ///
    /// # Panics
    ///
    /// If the in-cycle offsets are not strictly increasing, if the last
    /// offset does not fit inside the cycle, or if `send_interval` is
    /// not below every timeout. A phase whose retransmit interval
    /// reaches its timeout would expire before it ever retransmitted,
    /// which is indistinguishable from a silent peer in the logs.
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
            self.clock_sync_timeout,
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

/// Everything the operational multicast socket needs.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Interface to bind and join the multicast group on.
    pub interface_name: String,
    /// Operational multicast group.
    pub multicast_group: Ipv4Addr,
    /// Operational UDP port.
    pub port: u16,
    /// This node's id, written into every frame header.
    pub self_node_id: u8,
    /// Session id for this process run. Lets peers tell a restart apart
    /// from a sequence gap.
    pub self_session_id: u64,
    /// Sequence number the first outgoing frame carries.
    pub initial_seq_num: u32,
}

/// Everything the diagnostic socket needs. Only read in builds with the
/// `diagnostic` feature.
#[derive(Debug, Clone)]
pub struct DiagnosticConfig {
    /// Whether to open the diagnostic socket at all.
    pub enabled: bool,
    /// Interface to bind the diagnostic socket on.
    pub interface_name: String,
    /// Diagnostic multicast group, separate from the operational one.
    pub multicast_group: Ipv4Addr,
    /// Diagnostic UDP port.
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

/// The configuration file as it appears on disk.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    /// This node's id. Must be unique in the fabric.
    pub own_id: u8,
    /// `[participants]` section.
    pub participants: ParticipantSection,
    /// `[timing]` section.
    pub timing: TimingSection,
    /// `[transport]` section.
    pub transport: TransportSection,
    /// `[diagnostic]` section.
    pub diagnostic: DiagnosticSection,
}

/// `[participants]`: node counts and probation term.
#[derive(Debug, Clone, Deserialize)]
pub struct ParticipantSection {
    /// Nodes expected at startup.
    pub nominal: u8,
    /// Safety floor on active nodes.
    pub minimum: u8,
    /// Cycles a readmitted node serves before it votes again.
    pub probation_cycles: u32,
}

/// `[timing]`: all durations in milliseconds. See [`CycleTiming`] for
/// what each one governs and how they have to be ordered.
#[derive(Debug, Clone, Deserialize)]
pub struct TimingSection {
    /// Cycle length.
    pub cycle_duration_ms: u64,

    /// ShareInputs deadline, offset from the cycle anchor.
    pub share_inputs_offset_ms: u64,
    /// ShareResult deadline, offset from the cycle anchor.
    pub share_result_offset_ms: u64,
    /// SendAck deadline, offset from the cycle anchor.
    pub send_ack_offset_ms: u64,
    /// CRC exchange deadline, offset from the cycle anchor.
    pub crc_offset_ms: u64,

    /// Discovery timeout.
    pub init_sync_timeout_ms: u64,
    /// Clock-sync round timeout.
    pub clock_sync_timeout_ms: u64,
    /// Cycle barrier timeout.
    pub cycle_sync_timeout_ms: u64,
    /// Exclusion vote timeout.
    pub error_mgmt_timeout_ms: u64,
    /// Snapshot exchange timeout.
    pub state_sync_timeout_ms: u64,
    /// Rejoin timeout on the returning node.
    pub resync_returning_timeout_ms: u64,
    /// Rejoin timeout on an established node.
    pub resync_healthy_timeout_ms: u64,

    /// Retransmit interval for every waiting phase.
    pub send_interval_ms: u64,

    /// Age past which a received frame counts as stale.
    pub stale_frame_threshold_ms: u64,
    /// Cycles between two clock-sync rounds.
    pub resync_interval_cycles: u32,
}

/// `[transport]`: the operational multicast channel.
#[derive(Debug, Clone, Deserialize)]
pub struct TransportSection {
    /// Interface name, e.g. `eth0`. Needs an IPv4 address.
    pub interface: String,
    /// Multicast group, e.g. `239.10.0.1`.
    pub multicast_group: Ipv4Addr,
    /// UDP port.
    pub port: u16,
}

/// `[diagnostic]`: the diagnostic channel, ignored in production
/// builds.
#[derive(Debug, Clone, Deserialize)]
pub struct DiagnosticSection {
    /// Whether to open the diagnostic socket.
    pub enabled: bool,
    /// Interface name for the diagnostic socket.
    pub interface: String,
    /// Diagnostic multicast group, separate from the operational one.
    pub multicast_group: Ipv4Addr,
    /// Diagnostic UDP port.
    pub port: u16,
}

impl NodeConfig {
    /// Participant counts as a validated [`ParticipantConfig`].
    ///
    /// # Panics
    ///
    /// Via [`ParticipantConfig::new`] if the counts are inconsistent.
    pub fn participants(&self) -> ParticipantConfig {
        ParticipantConfig::new(
            self.participants.minimum,
            self.participants.nominal,
            self.participants.probation_cycles,
        )
    }

        /// Verify the `[integrity]` digest, then parse.
        ///
        /// The digest is computed over a canonical form of the document
        /// with the `[integrity]` section removed, so formatting is
        /// irrelevant and every value counts. A file that does not
        /// verify is rejected rather than loaded with a warning.
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

    /// Timing as a validated [`CycleTiming`].
    ///
    /// # Panics
    ///
    /// Via [`CycleTiming::validate`] if the deadlines are inconsistent.
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
            clock_sync_timeout: ms(t.clock_sync_timeout_ms),
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

    /// Transport settings for this run. `self_session_id` is generated
    /// per process start and is what lets peers detect a restart.
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

    /// Diagnostic socket settings.
    pub fn diagnostic(&self) -> DiagnosticConfig {
        DiagnosticConfig {
            enabled: self.diagnostic.enabled,
            interface_name: self.diagnostic.interface.clone(),
            multicast_group: self.diagnostic.multicast_group,
            port: self.diagnostic.port,
        }
    }
}

/// `[integrity]`: the self-check on the configuration file.
#[derive(Debug, Clone, Deserialize)]
pub struct IntegritySection {
    /// Digest algorithm. Only `sha256` is accepted.
    pub algo: String,
    /// Lower-case hex digest over the canonical form of the file.
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

/// Why a configuration file was rejected.
#[derive(Debug)]
pub enum ConfigError {
    /// The file is not valid TOML, or does not match the expected
    /// schema.
    Parse(String),
    /// No `[integrity]` section. Unsigned configurations are not
    /// accepted.
    MissingIntegrity,
    /// The named `[integrity]` field is missing or has the wrong type.
    MissingIntegrityField(&'static str),
    /// The file asks for a digest algorithm this build does not
    /// implement.
    UnsupportedAlgo(String),
    /// The digest does not match the file contents.
    ChecksumMismatch {
        /// Digest recorded in the file.
        expected: String,
        /// Digest computed from the file contents.
        actual: String,
    },
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
#[cfg(test)]
mod config_integrity_tests {
    //! `verify_and_parse` is the config-side integrity check. Two
    //! properties carry it: the digest must react to every value in the
    //! file, and it must not react to anything that is pure formatting,
    //! otherwise reformatting a config silently bricks a node.
    use super::*;

    /// A complete config without the `[integrity]` section.
    const BODY: &str = r#"
own_id = 0

[participants]
nominal          = 3
minimum          = 2
probation_cycles = 10

[timing]
cycle_duration_ms = 20

share_inputs_offset_ms = 5
share_result_offset_ms = 10
send_ack_offset_ms     = 14
crc_offset_ms          = 17

init_sync_timeout_ms        = 15000
clock_sync_timeout_ms       = 500
cycle_sync_timeout_ms       = 10
error_mgmt_timeout_ms       = 20
state_sync_timeout_ms       = 500
resync_returning_timeout_ms = 5000
resync_healthy_timeout_ms   = 500

send_interval_ms         = 1
stale_frame_threshold_ms = 100
resync_interval_cycles   = 500

[transport]
interface       = "lo"
multicast_group = "239.10.0.1"
port            = 5555

[diagnostic]
enabled         = true
interface       = "lo"
multicast_group = "239.10.0.2"
port            = 6666
"#;

    /// Digest over the canonical form of `body`, as the generator
    /// computes it.
    fn digest_of(body: &str) -> String {
        let value: toml::Value = toml::from_str(body).expect("body parses");
        let mut hasher = Sha256::new();
        hasher.update(canonical_bytes(&value));
        hex_lower(&hasher.finalize())
    }

    /// `body` plus a matching `[integrity]` section.
    fn signed(body: &str) -> String {
        format!(
            "{body}\n[integrity]\nalgo = \"sha256\"\nchecksum = \"{}\"\n",
            digest_of(body)
        )
    }

    #[test]
    fn a_correctly_signed_config_parses() {
        let cfg = NodeConfig::verify_and_parse(&signed(BODY)).expect("verifies");
        assert_eq!(cfg.own_id, 0);
        assert_eq!(cfg.participants.nominal, 3);
        assert_eq!(cfg.transport.port, 5555);
    }

    #[test]
    fn a_tampered_value_is_rejected() {
        let tampered = signed(BODY).replace("minimum          = 2", "minimum          = 1");
        match NodeConfig::verify_and_parse(&tampered) {
            Err(ConfigError::ChecksumMismatch { .. }) => {}
            other => panic!("tampering went undetected: {other:?}"),
        }
    }

    #[test]
    fn every_section_contributes_to_the_digest() {
        // Walked explicitly so a future refactor of `canonical_bytes`
        // cannot quietly drop a subtree from the hash.
        let mutations = [
            ("own_id = 0", "own_id = 1"),
            ("nominal          = 3", "nominal          = 4"),
            ("cycle_duration_ms = 20", "cycle_duration_ms = 21"),
            ("port            = 5555", "port            = 5556"),
            ("enabled         = true", "enabled         = false"),
        ];
        let baseline = digest_of(BODY);
        for (from, to) in mutations {
            let mutated = BODY.replace(from, to);
            assert_ne!(mutated, BODY, "mutation {from:?} did not apply");
            assert_ne!(digest_of(&mutated), baseline, "{from:?} left the digest untouched");
        }
    }

    #[test]
    fn formatting_does_not_change_the_digest() {
        // Comments, whitespace and key order are not part of the
        // canonical form. If they were, every hand-reformatted config
        // would fail the check at boot for no substantive reason.
        let commented = format!("# generated, do not edit\n{BODY}\n# trailing note\n");
        assert_eq!(digest_of(&commented), digest_of(BODY));

        let respaced = BODY.replace("          = ", " = ").replace("       = ", " = ");
        assert_eq!(digest_of(&respaced), digest_of(BODY));

        let reordered = BODY.replace(
            "nominal          = 3\nminimum          = 2",
            "minimum          = 2\nnominal          = 3",
        );
        assert_eq!(digest_of(&reordered), digest_of(BODY));
    }

    #[test]
    fn the_integrity_section_itself_is_excluded_from_the_digest() {
        // Otherwise the checksum would have to hash itself.
        let with_extra = format!(
            "{BODY}\n[integrity]\nalgo = \"sha256\"\nchecksum = \"{}\"\nnote = \"ignored\"\n",
            digest_of(BODY)
        );
        assert!(NodeConfig::verify_and_parse(&with_extra).is_ok());
    }

    #[test]
    fn a_missing_integrity_section_is_rejected() {
        match NodeConfig::verify_and_parse(BODY) {
            Err(ConfigError::MissingIntegrity) => {}
            other => panic!("unsigned config accepted: {other:?}"),
        }
    }

    #[test]
    fn missing_integrity_fields_are_named() {
        let no_checksum = format!("{BODY}\n[integrity]\nalgo = \"sha256\"\n");
        match NodeConfig::verify_and_parse(&no_checksum) {
            Err(ConfigError::MissingIntegrityField("checksum")) => {}
            other => panic!("unexpected: {other:?}"),
        }

        let no_algo = format!("{BODY}\n[integrity]\nchecksum = \"00\"\n");
        match NodeConfig::verify_and_parse(&no_algo) {
            Err(ConfigError::MissingIntegrityField("algo")) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn an_unsupported_algorithm_is_refused_not_skipped() {
        let md5 = format!(
            "{BODY}\n[integrity]\nalgo = \"md5\"\nchecksum = \"{}\"\n",
            digest_of(BODY)
        );
        match NodeConfig::verify_and_parse(&md5) {
            Err(ConfigError::UnsupportedAlgo(a)) => assert_eq!(a, "md5"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn malformed_toml_is_reported_as_a_parse_error() {
        match NodeConfig::verify_and_parse("own_id = ") {
            Err(ConfigError::Parse(_)) => {}
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn the_shipped_node_configs_verify() {
        // Guards against a config being edited without regenerating the
        // checksum, which a node would only report at boot time.
        for (name, text) in [
            ("node_0.toml", include_str!("../../configs/node_0.toml")),
            ("node_1.toml", include_str!("../../configs/node_1.toml")),
            ("node_2.toml", include_str!("../../configs/node_2.toml")),
        ] {
            NodeConfig::verify_and_parse(text)
                .unwrap_or_else(|e| panic!("{name} failed verification: {e}"));
        }
    }

    #[test]
    fn the_shipped_timing_passes_validation() {
        let cfg = NodeConfig::verify_and_parse(&signed(BODY)).expect("verifies");
        cfg.timing().validate();
    }
}

#[cfg(test)]
mod timing_validation_tests {
    //! `CycleTiming::validate` is the startup gate that keeps the
    //! in-cycle deadlines ordered. Each assertion is probed separately,
    //! because a panic message only tells you which one fired if there
    //! is a test that made it fire on purpose.
    use super::*;

    fn ms(v: u64) -> Duration {
        Duration::from_millis(v)
    }

    fn sane() -> CycleTiming {
        CycleTiming {
            cycle_duration: ms(20),
            share_inputs_offset: ms(5),
            share_result_offset: ms(10),
            send_ack_offset: ms(14),
            crc_offset: ms(17),
            init_sync_timeout: ms(15_000),
            clock_sync_timeout: ms(500),
            cycle_sync_timeout: ms(10),
            error_mgmt_timeout: ms(20),
            state_sync_timeout: ms(500),
            resync_returning_timeout: ms(5_000),
            resync_healthy_timeout: ms(500),
            send_interval: ms(1),
            stale_frame_threshold: ms(100),
            resync_interval_cycles: 500,
        }
    }

    #[test]
    fn a_sane_configuration_validates() {
        sane().validate();
    }

    #[test]
    #[should_panic(expected = "share_inputs_offset must precede share_result_offset")]
    fn input_and_result_offsets_must_be_ordered() {
        let mut t = sane();
        t.share_inputs_offset = t.share_result_offset;
        t.validate();
    }

    #[test]
    #[should_panic(expected = "share_result_offset must precede send_ack_offset")]
    fn result_and_ack_offsets_must_be_ordered() {
        let mut t = sane();
        t.share_result_offset = ms(15);
        t.validate();
    }

    #[test]
    #[should_panic(expected = "send_ack_offset must precede crc_offset")]
    fn ack_and_crc_offsets_must_be_ordered() {
        let mut t = sane();
        t.send_ack_offset = ms(18);
        t.validate();
    }

    #[test]
    #[should_panic(expected = "crc_offset must fit into cycle_duration")]
    fn the_last_offset_must_fit_into_the_cycle() {
        let mut t = sane();
        t.crc_offset = t.cycle_duration;
        t.validate();
    }

    #[test]
    #[should_panic(expected = "send_interval")]
    fn send_interval_must_be_below_every_timeout() {
        // If the retransmit interval reaches the shortest timeout, the
        // phase times out before it ever retransmits, which looks like
        // a silent peer in the logs.
        let mut t = sane();
        t.send_interval = t.cycle_sync_timeout;
        t.validate();
    }

    #[test]
    fn participant_config_derives_its_margins() {
        let p = ParticipantConfig::new(2, 3, 10);
        assert_eq!(p.tolerable_failures(), 1);
        assert_eq!(p.max_peers(), 2);

        let p = ParticipantConfig::new(2, 4, 10);
        assert_eq!(p.tolerable_failures(), 2);
        assert_eq!(p.max_peers(), 3);
    }

    #[test]
    #[should_panic(expected = "must not exceed nominal")]
    fn minimum_above_nominal_is_rejected() {
        ParticipantConfig::new(4, 3, 10);
    }

    #[test]
    #[should_panic(expected = "exceeds MAX_TOTAL_NODES")]
    fn more_nodes_than_the_buffers_hold_is_rejected() {
        ParticipantConfig::new(2, MAX_TOTAL_NODES as u8 + 1, 10);
    }
}

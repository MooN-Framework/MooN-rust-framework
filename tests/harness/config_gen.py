"""
Generiert pro Test eine Node-TOML aus einem Template.

Der Ansatz: das Template liegt einmal im Repo mit Platzhaltern
(`{OWN_ID}`, `{NOMINAL}`, `{MINIMUM}`, `{FABRIC_PORT}`, `{DIAG_PORT}`,
`{DIAG_GROUP}`, `{FABRIC_GROUP}`). Der Harness rendert es in ein temp-
Verzeichnis pro Testlauf.

Getrennte Ports pro Test wären ideal für Parallelisierung, aber
Multicast-Gruppen sind billiger — pytest laesst zunaechst seriell laufen.
"""
from pathlib import Path
from dataclasses import dataclass


@dataclass
class NodeSpec:
    own_id: int
    nominal: int
    minimum: int
    fabric_port: int = 5555
    diag_port: int = 6666
    fabric_group: str = "239.10.0.1"
    diag_group: str = "239.10.0.2"
    interface: str = "lo"
    # Timing — sinnvolle Defaults fuer schnelle Tests.
    cycle_duration_ms: int = 20
    share_inputs_offset_ms: int = 5
    share_result_offset_ms: int = 10
    send_ack_offset_ms: int = 14
    crc_offset_ms: int = 17
    init_sync_timeout_ms: int = 2000
    peer_sync_timeout_ms: int = 500
    cycle_sync_timeout_ms: int = 10
    error_mgmt_timeout_ms: int = 20
    state_sync_timeout_ms: int = 500
    resync_returning_timeout_ms: int = 5000
    resync_healthy_timeout_ms: int = 500
    send_interval_ms: int = 1
    stale_frame_threshold_ms: int = 100
    resync_interval_cycles: int = 500
    probation_cycles: int = 10


def render_config(spec: NodeSpec, out_path: Path) -> Path:
    """Schreibt eine TOML fuer diesen Node und gibt den Pfad zurueck."""
    toml = f"""own_id = {spec.own_id}

[participants]
nominal          = {spec.nominal}
minimum          = {spec.minimum}
probation_cycles = {spec.probation_cycles}

[timing]
cycle_duration_ms = {spec.cycle_duration_ms}

share_inputs_offset_ms  = {spec.share_inputs_offset_ms}
share_result_offset_ms  = {spec.share_result_offset_ms}
send_ack_offset_ms      = {spec.send_ack_offset_ms}
crc_offset_ms           = {spec.crc_offset_ms}

init_sync_timeout_ms        = {spec.init_sync_timeout_ms}
peer_sync_timeout_ms        = {spec.peer_sync_timeout_ms}
cycle_sync_timeout_ms       = {spec.cycle_sync_timeout_ms}
error_mgmt_timeout_ms       = {spec.error_mgmt_timeout_ms}
state_sync_timeout_ms       = {spec.state_sync_timeout_ms}
resync_returning_timeout_ms = {spec.resync_returning_timeout_ms}
resync_healthy_timeout_ms   = {spec.resync_healthy_timeout_ms}

send_interval_ms         = {spec.send_interval_ms}
stale_frame_threshold_ms = {spec.stale_frame_threshold_ms}
resync_interval_cycles   = {spec.resync_interval_cycles}

[transport]
interface       = "{spec.interface}"
multicast_group = "{spec.fabric_group}"
port            = {spec.fabric_port}

[diagnostic]
enabled         = true
interface       = "{spec.interface}"
multicast_group = "{spec.diag_group}"
port            = {spec.diag_port}
"""
    out_path.write_text(toml)
    return out_path

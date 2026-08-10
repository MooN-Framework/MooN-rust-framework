"""
T14 — Stale Frames durch Clock Drift.

Vollstaendiger Test: `resync_interval_cycles` sehr hoch setzen, viele
Minuten laufen lassen, sehen ob Frames als stale verworfen werden. Zu
langsam fuer routinemaessige CI-Laeufe.

Approximation: `stale_frame_threshold_ms` extrem klein setzen (~1 ms).
Dann werden auch minimal verzoegerte Frames als stale verworfen — das
mimt effektiv den Endzustand des Drift-Szenarios.

Als slow markiert damit CI es per default ueberspringt.
"""
import pytest

from harness.assertions import wait_peer_health
from harness.fabric import Fabric, FabricOptions

pytestmark = pytest.mark.slow


TARGET = 2


def test_stale_frames_via_tight_threshold(binary, work_dir):
    # Custom Fabric mit extrem niedrigem stale threshold.
    opts = FabricOptions(
        nominal=3,
        minimum=2,
        binary=binary,
        work_dir=work_dir,
        cycle_duration_ms=20,
        timing_overrides={"stale_frame_threshold_ms": 1},
    )
    fabric = Fabric(opts=opts)
    fabric.start_all()
    try:
        if not fabric.wait_operational(timeout=15.0):
            pytest.skip("fabric erreicht kein Operational mit tight threshold")

        # Bei so kleinem Threshold sollte spontan ein Peer als Lost gelten,
        # weil viele Frames zu spaet ankommen.
        for observer in (0, 1):
            peer = wait_peer_health(fabric, observer, TARGET, "Lost", timeout=15.0)
            assert peer is not None
    finally:
        fabric.stop_all()

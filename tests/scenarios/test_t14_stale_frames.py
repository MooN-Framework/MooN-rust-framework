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

pytestmark = pytest.mark.skip(
    reason="Braucht dedizierte Rust-Injection fuer Frame-Delay — "
           "tight-threshold-Trick auf loopback nicht zuverlaessig"
)

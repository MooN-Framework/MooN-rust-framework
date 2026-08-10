"""
T12 — Sequenzielle Ausfaelle in 2oo4.

Setup:     4 Nodes stabil.
Injection: shutdown 3, warten bis stabil (Cycle mit 3), dann shutdown 2.
Erwartet:  Zwei sequentielle Exklusionen. Fabric bleibt mit 2 Nodes am
           Leben (nominal=4, minimum=2 → 2 Ausfaelle OK).
"""
import time

from harness.assertions import (
    wait_cycles_advance,
    wait_node_died,
    wait_peer_health,
)


def test_sequential_faults_2oo4(fabric_4):
    # 1. Ausfall.
    assert fabric_4.diag.shutdown(3)
    assert wait_node_died(fabric_4, 3, timeout=5.0)
    for observer in (0, 1, 2):
        peer = wait_peer_health(fabric_4, observer, 3, "Lost", timeout=8.0)
        assert peer is not None

    # Stabilisieren.
    time.sleep(0.5)

    # 2. Ausfall.
    assert fabric_4.diag.shutdown(2)
    assert wait_node_died(fabric_4, 2, timeout=5.0)
    for observer in (0, 1):
        peer = wait_peer_health(fabric_4, observer, 2, "Lost", timeout=8.0)
        assert peer is not None

    # Die letzten zwei muessen weiter laufen.
    assert wait_cycles_advance(fabric_4, 0, n_cycles=5, timeout=8.0)

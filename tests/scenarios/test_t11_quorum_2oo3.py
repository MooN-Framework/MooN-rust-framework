"""
T11 — Quorum-Grenze in 2oo3.

Setup:     3 Nodes stabil.
Injection: shutdown Node 1, warten bis excluded (Cycle mit 2 laeuft),
           dann shutdown Node 2.
Erwartet:  Nach dem ersten Shutdown: exclude Node 1, Fabric mit Nodes 0
           und 2 weiter. Nach dem zweiten: einzig verbleibender Node
           kann nicht mehr alleine entscheiden (Fix B: reporters=1 →
           kein exclude) → EM timeoutet → StateTimeout → Failsafe.
"""
import time

from harness.assertions import (
    wait_node_died,
    wait_peer_health,
)


def test_quorum_limit_2oo3(fabric_3):
    # 1. Ausfall.
    assert fabric_3.diag.shutdown(1)
    assert wait_node_died(fabric_3, 1, timeout=5.0)
    for observer in (0, 2):
        peer = wait_peer_health(fabric_3, observer, 1, "Lost", timeout=8.0)
        assert peer is not None, f"node {observer} sieht 1 nicht als Lost"

    # Kurz warten damit die Fabric im 2-Node-Betrieb stabilisiert.
    time.sleep(0.5)

    # 2. Ausfall — der letzte verbleibende Node muss Failsafe.
    assert fabric_3.diag.shutdown(2)
    assert wait_node_died(fabric_3, 2, timeout=5.0)

    # Der einzige uebrige Node darf nicht alleine weiterlaufen.
    assert wait_node_died(fabric_3, 0, timeout=8.0), (
        "Alleinlaeufer haette Failsafe muessen (Fix B: no unilateral exclude)"
    )

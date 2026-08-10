"""
T2 — Shutdown (harter Ausfall).

Setup:     3 Nodes stabil.
Injection: shutdown <target>.
Erwartet:  Target-Prozess beendet sich; Peers erkennen den Ausfall und
           excluden den target.
"""
from harness.assertions import (
    wait_cycles_advance,
    wait_node_died,
    wait_peer_health,
)

TARGET = 2


def test_shutdown(fabric_3):
    assert fabric_3.diag.shutdown(TARGET), "shutdown injection nicht bestaetigt"

    # Der target-Prozess muss real weg sein.
    assert wait_node_died(fabric_3, TARGET, timeout=5.0), (
        f"node {TARGET} process not exited"
    )

    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None

    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)

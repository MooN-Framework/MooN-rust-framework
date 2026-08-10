"""
T9 — Doppelausfall gleichzeitig in 2oo4.

Setup:     4 Nodes stabil (fabric_4).
Injection: shutdown von 2 Nodes hintereinander (praktisch
           zeitgleich fuer das System).
Erwartet:  Beide werden excluded. Zwei verbleibende Nodes laufen mit
           reduziertem Quorum weiter (nominal=4, minimum=2 → OK).
"""
from harness.assertions import (
    wait_cycles_advance,
    wait_node_died,
    wait_peer_health,
)

TARGETS = [2, 3]


def test_double_fault_2oo4(fabric_4):
    # Beide Shutdowns hintereinander schicken.
    for t in TARGETS:
        assert fabric_4.diag.shutdown(t), f"shutdown fuer node {t} nicht bestaetigt"

    for t in TARGETS:
        assert wait_node_died(fabric_4, t, timeout=5.0), (
            f"node {t} nicht beendet"
        )

    survivors = [nid for nid in fabric_4.nodes if nid not in TARGETS]
    for survivor in survivors:
        for target in TARGETS:
            peer = wait_peer_health(fabric_4, survivor, target, "Lost", timeout=10.0)
            assert peer is not None, (
                f"node {survivor} sieht target {target} nicht als Lost"
            )

    # Zwei Nodes muessen weiter laufen.
    assert wait_cycles_advance(fabric_4, survivors[0], n_cycles=5, timeout=8.0)

"""
T7 - CRC Divergence (aktualisiert).

Setup:     3 Nodes stabil.
Injection: fake-crc <target> 1.
Erwartet:  Target sendet CRC = 0xDEADBEEF, Peers sehen Divergenz.
           Sie ermitteln per Mehrheit die korrekte CRC (die 2 gesunden
           Peers agreen) und proposen target zum Ausschluss. Target
           wird per self_excluded_by_peers-Detektor in EM zum
           SelfExcluded → Isolation.
           Peers laufen im 2-Node-Betrieb weiter.

Aenderung ggue frueherem Verhalten: es sterben NICHT mehr alle drei.
Isolation ist die richtige Reaktion auf einen einzelnen Divergenten
in 2oo3 solange (n - k) >= minimum.
"""
from harness.assertions import (
    wait_cycles_advance,
    wait_node_state,
    wait_peer_health,
)

TARGET = 2


def test_crc_divergence(fabric_3):
    assert fabric_3.diag.fake_crc(TARGET, 1), "injection nicht bestaetigt"

    # Target geht in Isolation (Prozess lebt weiter, aber isoliert).
    target_status = wait_node_state(fabric_3, TARGET, "Isolation", timeout=8.0)
    assert target_status is not None, (
        f"target {TARGET} sollte in Isolation sein"
    )

    # Peers sehen target als Lost und laufen weiter.
    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None, (
            f"node {survivor} sieht target {TARGET} nicht als Lost"
        )

    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0), (
        "peers laufen nach CRC-Divergenz nicht weiter"
    )
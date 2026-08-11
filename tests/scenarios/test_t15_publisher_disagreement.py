"""
T15 - Publisher Disagreement (aktualisiert).

Setup:     3 Nodes stabil.
Injection: divergent-publisher <target> 1.
Erwartet:  Target sendet Ack mit falschem publisher_candidate.
           Aus Peers-Sicht bucketen die drei picks: 2 gleiche + 1
           divergent. Majority ermittelbar → peers proposen target.
           Target selbst sieht Consensus (own_pick + peer picks aus
           seiner Sicht sind ok), landet ueber peer_in_error-
           Rendezvous in EM, wird per self_excluded_by_peers zum
           SelfExcluded → Isolation.
           Peers laufen im 2-Node-Betrieb weiter.

Aenderung ggue frueherem Verhalten: Publisher-Divergenz mit
identifizierbarer Mehrheit ist kein Byzantine-Fall mehr. Nur wenn
gar keine Mehrheit ermittelbar ist (z.B. 3 verschiedene Picks in
3-Node) bleibt es Failsafe.
"""
from harness.assertions import (
    wait_cycles_advance,
    wait_node_state,
    wait_peer_health,
)

TARGET = 2


def test_publisher_disagreement(fabric_3):
    assert fabric_3.diag.divergent_publisher(TARGET, 1), (
        "injection nicht bestaetigt"
    )

    target_status = wait_node_state(fabric_3, TARGET, "Isolation", timeout=8.0)
    assert target_status is not None, (
        f"target {TARGET} sollte in Isolation sein"
    )

    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None

    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)
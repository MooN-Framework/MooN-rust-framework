"""
T3 — Silent VOR Input.

Setup:     3 Nodes stabil.
Injection: drop-inputs <target> 1.
Erwartet:  Target sendet keinen Input. Peers timeouten in ShareInputs
           (Rendezvous ggf.), gehen in EM, excluden target.
"""
from harness.assertions import wait_cycles_advance, wait_peer_health

TARGET = 2


def test_silent_before_input(fabric_3):
    assert fabric_3.diag.drop_inputs(TARGET, 1), "injection nicht bestaetigt"

    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None, (
            f"node {survivor} sieht target {TARGET} nicht als Lost"
        )

    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)

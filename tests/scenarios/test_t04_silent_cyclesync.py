"""
T4 — Silent in CycleSync.

Setup:     3 Nodes stabil.
Injection: drop-cyclesync <target> 1.
Erwartet:  Target sendet keine State-Beacon im CycleSync. Peers timeouten
           in CycleSync, gehen in EM. Fix A stellt sicher, dass EM auf
           echte Votes wartet (nicht sofort completed). Nach EM: exclude.
"""
from harness.assertions import wait_cycles_advance, wait_peer_health

TARGET = 2


def test_silent_cyclesync(fabric_3):
    assert fabric_3.diag.drop_cyclesync(TARGET, 1), "injection nicht bestaetigt"

    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None

    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)

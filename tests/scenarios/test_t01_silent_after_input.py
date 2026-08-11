"""
T1 — Silent nach Input.

Setup:     3 Nodes stabil (fabric_3).
Injection: silent <target> 1  (drop-results + drop-acks kombiniert).
Erwartet:  Target sendet Input, dann still. Aus peers-Sicht ist target
           nicht mehr unterscheidbar von einem Crash oder byzantine
           silent — das ist kein identifizierbarer Divergenzfall.
           Peers timeouten in ShareResult, gehen in EM, excluden
           target und laufen kurz mit 2 weiter. Target selbst laeuft
           parallel durch (er hoert die peers noch), landet im
           naechsten Zyklus in CycleSync ohne peer beacons, geht
           ueber CycleSyncTimeout → EM → StateTimeout → Failsafe und
           broadcastet GoFailsafe. Damit sterben alle drei.

Semantik: silent-aber-nicht-crashed ist byzantine. Konsens laesst
sich zwischen "ich bin blind" und "peer ist byzantine" nicht mehr
unterscheiden. Konservative SIL-Reaktion ist Failsafe.
"""
from harness.assertions import wait_node_died

TARGET = 2


def test_silent_after_input(fabric_3):
    assert fabric_3.diag.silent(TARGET, 1), "silent injection nicht bestaetigt"

    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=15.0), (
            f"node {nid} sollte nach silent-Injection Failsafe erreichen"
        )
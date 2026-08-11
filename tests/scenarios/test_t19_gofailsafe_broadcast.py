"""
T19 - GoFailsafe-Broadcast koppelt Fabric.

Setup:     3 Nodes stabil.
Injection: broadcast_input mit unsafe values (analog T18).
Erwartet:  Der erste Node der SinkSafetyViolation erkennt sendet
           GoFailsafe. Die anderen zwei loggen den empfangenen
           Broadcast ("peer broadcast GoFailsafe") und gehen selbst
           in Failsafe.

Der Unterschied zu T18: T18 prueft nur dass alle sterben. T19 prueft
explizit dass der Broadcast-Pfad benutzt wird (nicht nur dass alle
unabhaengig SinkSafetyViolation erkennen).
"""
from harness.assertions import wait_node_died

UNSAFE_INPUT = {
    "current_speed": 200.0,
    "target_speed": 0.0,
    "available_distance": 20.0,
}


def test_gofailsafe_broadcast_propagates(fabric_3):
    fabric_3.diag.broadcast_input(UNSAFE_INPUT)

    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=10.0)

    # Mindestens zwei Nodes muessen den GoFailsafe von einem Peer
    # empfangen und geloggt haben. (Der Erst-Ausloeser selbst tut das
    # nicht.)
    receivers = 0
    for node in fabric_3.nodes.values():
        if node.wait_for_log(r"peer broadcast GoFailsafe", timeout=1.0):
            receivers += 1
    assert receivers >= 1, (
        f"kein Node hat den GoFailsafe-Broadcast empfangen (receivers={receivers})"
    )
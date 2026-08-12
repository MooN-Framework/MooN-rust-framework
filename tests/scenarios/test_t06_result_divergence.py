"""
T6 — Result Divergence.

Setup:     3 Nodes stabil.
Injection: Node 2 sendet fuer count Cycles einen manipulierten
           BrakeResult (grosse Distanzverschiebung + Emergency-Flip).
Erwartet:  Nodes 0 und 1 einigen sich auf ihren gemeinsamen Wert,
           erkennen Node 2 via find_dissenters, proposen ihn zum
           Ausschluss. Nach EM sehen sie ihn auf Lost.
"""
from harness.assertions import wait_peer_health

TARGET = 2


def test_result_divergence(fabric_3):
    assert fabric_3.diag.corrupt_result(TARGET, count=3) is not None

    for observer in (0, 1):
        peer = wait_peer_health(fabric_3, observer, TARGET, "Lost", timeout=8.0)
        assert peer is not None, (
            f"observer {observer} hat Node {TARGET} nicht als Lost markiert"
        )
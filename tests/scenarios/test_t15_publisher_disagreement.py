"""
T15 — Publisher Disagreement.

Setup:     3 Nodes stabil.
Injection: divergent-publisher <target> 1.
Erwartet:  Target sendet Ack mit falschem publisher_candidate.
           `publisher_consensus` divergiert → Fault → alle Failsafe.

Anmerkung: das Rust-Verhalten hier ist "harte Regel" — Publisher-
Divergenz ist keine Silent-Fault-Class, sie deutet auf ein
systematisches Voting-Problem hin und faellt sicher aus.
"""
from harness.assertions import wait_node_died

TARGET = 2


def test_publisher_disagreement(fabric_3):
    assert fabric_3.diag.divergent_publisher(TARGET, 1)

    # Alle drei Nodes muessen Failsafe (Process Exit).
    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=10.0), (
            f"node {nid} sollte Failsafe erreichen"
        )

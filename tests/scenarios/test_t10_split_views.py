"""
T10 — Split-Sichten (asymmetrische Beobachtung).

Setup:     3 Nodes stabil.
Injection: Node 2 verwirft alle eingehenden Frames von Node 0
           (`InjectDropFromPeer` mit Maske 0b0000_0001).
Erwartet:  Nodes 0 und 1 erkennen Node 2 als abweichend und schliessen
           ihn per EM-Konsens aus (`peer excluded peer_id=2`).

Nachlauf per Framework-Design (kein Bug, per Safety-Case gewollt):
    Der aus 0/1's Sicht ausgeschlossene Node 2 verliert Quorum und
    geht in Failsafe. Der resultierende GoFailsafe-Broadcast zieht
    0 und 1 nach — Fail-Stop ist bei SIL 2 die konservative
    Semantik. Das Zeitfenster zwischen Lost-Markierung und
    systemweiter Failsafe liegt bei ~50 ms, deshalb log-basierte
    Verifikation statt Status-Polling.
"""
TARGET = 2
BLOCKED_PEER = 0
EXCLUSION_TIMEOUT_S = 8.0


def test_split_views(fabric_3):
    peers_mask = 1 << BLOCKED_PEER
    resp = fabric_3.diag.drop_from_peer(TARGET, peers_mask)
    assert resp is not None, "drop_from_peer injection nicht staged"

    for observer in (0, 1):
        observer_node = fabric_3.nodes[observer]
        assert observer_node.wait_for_log(
            rf"peer excluded peer_id={TARGET}", timeout=EXCLUSION_TIMEOUT_S
        ), (
            f"observer {observer} hat Node {TARGET} nicht ausgeschlossen "
            f"innerhalb {EXCLUSION_TIMEOUT_S}s"
        )
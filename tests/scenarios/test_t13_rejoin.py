"""
T13 — Rejoin nach Isolation.
"""
from harness.assertions import wait_peer_health

TARGET = 2
OBSERVER = 0


def test_rejoin(fabric_3):
    # 1. Target sauber runterfahren.
    assert fabric_3.diag.shutdown(TARGET) is not None, (
        "shutdown injection nicht bestaetigt"
    )

    # 2. Peers muessen ihn als Lost sehen.
    peer = wait_peer_health(fabric_3, OBSERVER, TARGET, "Lost", timeout=8.0)
    assert peer is not None, f"observer {OBSERVER} sieht target {TARGET} nicht als Lost"

    # 3. Prozess neu starten.
    fabric_3.restart_node(TARGET)

    # 4a. Readmit-Ereignis im Observer-Log (Lost -> Probation).
    #     Log-basiert, weil das Probation-Fenster mit den Default-Timings
    #     (10 cycles * 20 ms = 200 ms) kuerzer ist als das Polling-
    #     Intervall von wait_peer_health und deshalb via GetStatus
    #     nicht zuverlaessig sichtbar wird.
    observer_node = fabric_3.nodes[OBSERVER]
    assert observer_node.wait_for_log(
        rf"peer readmitted.*peer_id={TARGET}", timeout=15.0
    ), f"observer {OBSERVER} hat target {TARGET} nicht readmitted (Lost->Probation)"

    # 4b. Promotion-Ereignis (Probation -> Alive).
    assert observer_node.wait_for_log(
        r"peers promoted from Probation to Alive", timeout=10.0
    ), f"target {TARGET} wurde nicht von Probation zu Alive promoted"

    # 5. Sanity: End-Zustand ist tatsaechlich Alive.
    peer = wait_peer_health(fabric_3, OBSERVER, TARGET, "Alive", timeout=5.0)
    assert peer is not None, f"target {TARGET} ist nicht Alive nach Rejoin"
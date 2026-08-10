"""
T8 — Cycle Skew.

Setup:     3 Nodes stabil.
Injection: cycle-delay <target> <ms> 1  (extra Sleep in ReadInputs).
Erwartet:  Target startet den Cycle spaeter. Wenn der Delay unter den
           in-cycle deadline-offsets bleibt, holt der Rendezvous-Pfad
           ihn nach — Fabric laeuft weiter, kein Failsafe. Wenn er
           deutlich groesser ist, muss er excluded werden.

Wir testen den 'nicht zu viel Skew'-Pfad: 3 ms extra bei
share_inputs_offset=5 ms. Sollte gerade eben klappen ohne Exklusion,
und der Node sollte Alive bleiben.
"""
from harness.assertions import wait_cycles_advance, wait_peer_health

TARGET = 2
DELAY_MS = 3


def test_cycle_skew_small_recovers(fabric_3):
    assert fabric_3.diag.cycle_delay(TARGET, DELAY_MS, 1), (
        "cycle-delay injection nicht bestaetigt"
    )

    # Nach dem einen verzoegerten Cycle sollte target weiterhin Alive sein.
    # Wir warten ein paar Cycles und pruefen dann.
    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)

    # Ziel: peer 2 aus sicht der anderen ist Alive.
    for survivor in survivors:
        # Peer sollte NICHT Lost sein.
        peer_lost = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=1.0)
        assert peer_lost is None, (
            f"target {TARGET} wurde nach kleinem Skew faelschlich excluded"
        )

"""
T5 — Input Divergence.

Setup:     3 Nodes stabil, alle mit gleichem Input.
Injection: set-input-single <target> mit stark abweichendem Wert.
Erwartet:  Target verwendet abweichenden Input. In ShareInputs feuert
           `computation.inputs_agree` und returnt InputsDivergent.
           Alle drei Nodes gehen in EM, target wird als Divergent
           identifiziert und excluded.

Anmerkung: welcher Wert "stark abweichend" ist, haengt von
`Computation::inputs_agree`. Fuer die ETCS-Bremskurve pruefen die
inputs_agree typischerweise Toleranzen auf current_speed, target_speed,
available_distance. Ein Wert weit ausserhalb sollte divergieren.
"""
from harness.assertions import wait_cycles_advance, wait_peer_health

TARGET = 2

DIVERGENT_INPUT = {
    "current_speed": 999.0,
    "target_speed": 0.0,
    "available_distance": 10.0,
}


def test_input_divergence(fabric_3):
    ack = fabric_3.diag.targeted_input(TARGET, DIVERGENT_INPUT)
    assert ack, "targeted input nicht bestaetigt"

    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None

    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)

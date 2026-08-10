"""
T16 — Frame mit falschem Phase-Header.

Braucht eine Rust-Injection die ein Frame mit einem `node_state_wire`
sendet der nicht zur tatsaechlichen Phase passt. Aktuell nicht im
Diagnostic-API — waere `InjectWrongPhaseHeader { fake_state }`.

Der Codepfad im Ingest verwirft solche Frames, das Verhalten degradiert
zu Silent-Fault. Deckt sich mit T1/T3, daher redundant fuer die
Baseline-Coverage.

Skip bis der Rust-Support da ist.
"""
import pytest

pytestmark = pytest.mark.skip(
    reason="Rust-Diagnostic hat keinen wrong-phase-header injector"
)


def test_wrong_phase_header(fabric_3):
    pass

"""
T17 — Startup Fault.

Ein Node muss mit fehlschlagendem `SelfTest::run` starten. Aktuell wird
der `SelfTest` als generischer Type im `main` gebaut — es gibt keinen
Konfig-Schalter der ihn zum Fehlschlagen bringt.

Zwei Optionen:
  a) `SelfTest` konfigurierbar machen ueber die TOML
     (`[selftest] fail_on_startup = true`) — invasiv, aber sauber.
  b) Env-Var im Node-Prozess: `FORCE_SELFTEST_FAIL=1` → self_test.rs
     liest env und returned Err.

Bis dahin: skip. Das Verhalten ist deterministisch (SelfTestErr →
Failsafe), also einmalig manuell getestet ausreichend.
"""
import pytest

pytestmark = pytest.mark.skip(
    reason="Braucht configurable SelfTest — future work"
)


def test_startup_fault(fabric_3):
    pass

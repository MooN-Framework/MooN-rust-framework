"""
T13 — Rejoin nach Isolation.

Der Test-Harness muss dafuer einen Node nach shutdown neu starten
koennen. Das ist mit der aktuellen Fabric-API noch nicht drin — der
Fabric-Kontext-Manager killt nur beim Verlassen.

Zu tun im Harness:
- Fabric.restart_node(node_id)  → spawn erneut mit gleicher config
- Assertion: wait_peer_health(observer, target, "Probation")
  → dann "Alive" nach probation_cycles.

Bis dahin: skip. Der Codepfad ist im Rust drin (SystemStateSync +
Probation), muss aber E2E getestet werden.
"""
import pytest

pytestmark = pytest.mark.skip(
    reason="Harness braucht restart_node() — future work"
)


def test_rejoin(fabric_3):
    pass

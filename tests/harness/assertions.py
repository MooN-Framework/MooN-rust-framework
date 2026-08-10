"""
Assertion-Helper fuer die Testszenarien.

Erfolg pruefen wir zweigleisig:
1. Status via GetStatus (primaer, strukturiert).
2. Log-Pattern-Matching (Fallback, sobald Node in Failsafe ist).

Beide sind hier in Wait-Funktionen gepackt weil das System asynchron ist —
wir wissen nicht genau wann eine Injection wirkt.
"""
from __future__ import annotations

import time
from typing import Optional

from .fabric import Fabric


def wait_peer_health(
    fabric: Fabric,
    observer_id: int,
    target_peer_id: int,
    expected_health: str,
    timeout: float = 5.0,
) -> Optional[dict]:
    """
    Wartet bis Node `observer_id` den Peer `target_peer_id` mit
    `expected_health` sieht. `expected_health` ist "Alive", "Lost",
    "Probation", etc. — String wie er in StatusResponse.peers steht.

    Returns die peer-dict wenn matched, None wenn timeout.
    """
    assert fabric.diag
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = fabric.diag.get_status(observer_id, timeout=1.0)
        if status:
            for peer in status.get("peers", []):
                if peer["id"] == target_peer_id and peer["health"] == expected_health:
                    return peer
        time.sleep(0.1)
    return None


def wait_node_state(
    fabric: Fabric,
    node_id: int,
    expected_state: str,
    timeout: float = 5.0,
) -> Optional[dict]:
    """Wartet bis ein Node in einem bestimmten NodeState laeuft."""
    assert fabric.diag
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = fabric.diag.get_status(node_id, timeout=1.0)
        if status and status.get("node_state") == expected_state:
            return status
        time.sleep(0.1)
    return None


def wait_node_died(fabric: Fabric, node_id: int, timeout: float = 5.0) -> bool:
    """Wartet bis der Prozess-Exit des Nodes."""
    deadline = time.monotonic() + timeout
    node = fabric.nodes[node_id]
    while time.monotonic() < deadline:
        if not node.is_running():
            return True
        time.sleep(0.1)
    return False


def wait_cycles_advance(
    fabric: Fabric,
    node_id: int,
    n_cycles: int,
    timeout: float = 10.0,
) -> bool:
    """
    Wartet bis `current_seq` des Nodes um mindestens n_cycles gewachsen ist.
    Nuetzlich um sicher zu stellen dass die Fabric nach einer Injection
    weiter laeuft (nicht in Failsafe hing).
    """
    assert fabric.diag
    initial = fabric.diag.get_status(node_id, timeout=2.0)
    if not initial:
        return False
    start_seq = initial["current_seq"]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = fabric.diag.get_status(node_id, timeout=1.0)
        if status and status["current_seq"] >= start_seq + n_cycles:
            return True
        time.sleep(0.1)
    return False


def any_node_reached_failsafe(fabric: Fabric, timeout: float = 5.0) -> Optional[int]:
    """
    Prueft ob irgendein Node in Failsafe gegangen ist (via Log-Pattern).
    Returns die node_id des ersten der Failsafe erreicht hat, oder None.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for nid, node in fabric.nodes.items():
            if node.wait_for_log(r"failsafe entered", timeout=0.2):
                return nid
        time.sleep(0.1)
    return None


def assert_exclusion_confirmed(fabric: Fabric, target_peer_id: int) -> None:
    """
    Harte Assertion: mindestens ein Node hat 'peer excluded peer_id=X'
    geloggt. Nutze `wait_peer_health` fuer die weiche Version.
    """
    pat = rf"peer excluded peer_id={target_peer_id}"
    found = False
    for node in fabric.nodes.values():
        if node.wait_for_log(pat, timeout=2.0):
            found = True
            break
    assert found, f"no node logged exclusion of peer {target_peer_id}"

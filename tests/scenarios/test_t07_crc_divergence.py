"""
T7 — CRC Divergence.

Setup:     3 Nodes stabil.
Injection: fake-crc <target> 1.
Erwartet:  Target sendet CRC = 0xDEADBEEF. CRC-Konsens divergiert.
           Alle drei Nodes gehen sofort in Failsafe (harte Regel:
           CRC-Divergenz ist kein recoverabler Zustand).
"""
import pytest

from harness.assertions import any_node_reached_failsafe, wait_node_died

TARGET = 2


def test_crc_divergence(fabric_3):
    assert fabric_3.diag.fake_crc(TARGET, 1), "injection nicht bestaetigt"

    # Alle drei Nodes muessen Failsafe erreichen.
    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=8.0), (
            f"node {nid} sollte in Failsafe / process exit sein"
        )

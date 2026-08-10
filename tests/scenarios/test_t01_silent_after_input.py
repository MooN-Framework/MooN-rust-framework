"""
T1 — Silent nach Input.

Setup:     3 Nodes stabil (fabric_3).
Injection: silent <target> 1  (drop-results + drop-acks kombiniert).
Erwartet:  Target sendet Input, dann still. Peers timeouten in ShareResult
           (Rendezvous ggf.), gehen in EM, excluden target, laufen zu zweit
           weiter.
"""
import pytest

from harness.assertions import (
    wait_cycles_advance,
    wait_peer_health,
)

TARGET = 2


def test_silent_after_input(fabric_3):
    assert fabric_3.diag.silent(TARGET, 1), "silent injection nicht bestaetigt"

    # Die beiden ueberlebenden Peers muessen target als Lost sehen.
    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None, (
            f"node {survivor} sieht target {TARGET} nicht als Lost"
        )

    # Fabric muss weiter laufen — mind. 5 Cycles nach der Exklusion.
    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0), (
        "fabric ist nach exklusion nicht weiter gelaufen"
    )

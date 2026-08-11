"""
T3 — Silent vor Input.

Wie T1, nur dass target seinen Input komplett verschluckt (statt
Results/Acks). Peers timeouten in ShareInputs, gehen in EM, excluden
target. Target selbst wird ueber CycleSync-Timeout blind → Failsafe
→ GoFailsafe → alle Failsafe.
"""
from harness.assertions import wait_node_died

TARGET = 2


def test_silent_before_input(fabric_3):
    assert fabric_3.diag.drop_inputs(TARGET, 1) is not None, (
        "drop_inputs injection nicht bestaetigt"
    )

    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=15.0), (
            f"node {nid} sollte nach silent-vor-Input Failsafe erreichen"
        )
"""
T4 — Silent in CycleSync.

Target sendet keine State-Beacons in CycleSync. Aus peers-Sicht
timeouted CycleSync → EM → excluden target. Target selbst geht
ueber blindness in Failsafe. GoFailsafe-Broadcast → alle Failsafe.
"""
from harness.assertions import wait_node_died

TARGET = 2


def test_silent_cyclesync(fabric_3):
    assert fabric_3.diag.drop_cyclesync(TARGET, 1) is not None, (
        "drop_cyclesync injection nicht bestaetigt"
    )

    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=15.0), (
            f"node {nid} sollte nach silent-cyclesync Failsafe erreichen"
        )
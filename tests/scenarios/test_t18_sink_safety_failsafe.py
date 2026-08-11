"""
T18 - Sink Safety Failsafe.

Setup:     3 Nodes stabil.
Injection: broadcast_input mit einem Wert der garantiert
           emergency_brake=true erzwingt (current_speed hoch,
           available_distance klein).
Erwartet:  Alle drei Nodes berechnen im Konsens emergency=true.
           Sink.evaluate returned Failsafe. Publisher published die
           Notbrems-Decision noch, alle drei Nodes gehen dann in
           Failsafe (via SinkSafetyViolation).
"""
from harness.assertions import wait_node_died

# 200 m/s aus dem Stand mit 20 m Restweg: total_distance ist so gross
# dass emergency_brake ganz sicher gesetzt wird — quer durch die ganze
# Deceleration-Tabelle nicht bremsbar.
UNSAFE_INPUT = {
    "current_speed": 200.0,
    "target_speed": 0.0,
    "available_distance": 20.0,
}


def test_sink_safety_failsafe(fabric_3):
    fabric_3.diag.broadcast_input(UNSAFE_INPUT)

    # Alle drei Nodes muessen Failsafe erreichen (Process Exit).
    for nid in fabric_3.nodes:
        assert wait_node_died(fabric_3, nid, timeout=10.0), (
            f"node {nid} sollte durch SinkSafetyViolation in Failsafe gehen"
        )

    # Mindestens ein Node muss die Sink-Ablehnung geloggt haben.
    found = False
    for node in fabric_3.nodes.values():
        if node.wait_for_log(r"sink rejected decision as unsafe", timeout=1.0):
            found = True
            break
    assert found, "kein Node hat die Sink-Ablehnung geloggt"
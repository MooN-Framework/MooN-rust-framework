"""
T20 — Byzantine 2/2 Split in 2oo4.

Setup:     4 Nodes stabil (fabric_4).
Injection: fake_crc_multi([2, 3], 1) — beide Targets ATOMAR in einem
           Broadcast-Telegramm scharf schalten, damit sie im selben
           Zyklus fake CRC senden.
Erwartet:  Zwei Nodes senden 0xDEADBEEF, zwei senden ihre echte CRC.
           Aus Sicht der beiden echten Nodes ergibt das einen 2/2
           Split der CRCs — strict_majority ist nicht mehr
           ermittelbar. Sie mark_failsafe(StateDivergence) → Failsafe
           → GoFailsafe-Broadcast. Die beiden fake-Sender folgen dem
           Broadcast. Ergebnis: alle vier gehen Failsafe.

Semantik: bei n=4 braucht strict_majority mindestens 3 gleiche.
2/2 ist genau der Byzantine-Fall aus der 2oo4-Analyse — nicht mehr
sicher entscheidbar wer die Wahrheit sagt → konservative
SIL-Reaktion ist Failsafe.

Aenderung ggue vorherigem Test: sequentielle fake_crc-Aufrufe
haben durch RPC-Latenz nie ueberlappt (Node 3 wurde erst 15 Zyklen
nach Node 2 scharf, Node 2 laengst isoliert). Der multi-target-
Aufruf schaltet beide Nodes atomar im selben Broadcast scharf.
"""
from harness.assertions import wait_node_died

TARGETS = [2, 3]


def test_byzantine_split_2oo4(fabric_4):
    assert fabric_4.diag.fake_crc_multi(TARGETS, count=1), (
        "fake_crc_multi injection nicht bestaetigt"
    )

    for nid in fabric_4.nodes:
        assert wait_node_died(fabric_4, nid, timeout=15.0), (
            f"node {nid} sollte bei 2/2 CRC-Split Failsafe erreichen"
        )
"""
T16 — Frame mit falschem Phase-Header.

Setup:     3 Nodes stabil.
Injection: Node 2 sendet fuer 20 Frames seinen node_state_wire mit
           dem Wert von `NodeState::ErrorManagement` (0x08) statt
           der echten Phase.

Beobachtetes Verhalten (positives Robustheits-Finding):
    Nodes 0 und 1 erkennen den ErrorManagement-Header und setzen
    ihren Rendezvous-Flag `peer_in_error_seen`. Am Ende der
    laufenden Phase transitionieren sie via `PeerInError` nach EM.
    Dort wird jedoch KEIN Exclusion-Proposal gegen Node 2 gemacht,
    weil Node 2 seine Payloads (Input, Result) trotz gespoofter
    Header korrekt geliefert hat und damit keine Attribute-Basis
    fuer einen Ausschluss vorliegt. Aggregat der Exclusion-Votes
    ist leer, EM schliesst mit `StateOk` ab und die Fabric kehrt
    nach CycleSync zurueck.

    Effekt: kurzer EM-Ausflug pro geladenem Fake-Frame, dann
    Recovery. Nach Erschoepfung der 20 Fakes laeuft die Fabric
    unveraendert weiter.

Finding fuer die Thesis:
    Der Rendezvous-Mechanismus ist absichtlich toleranter als der
    Ausschluss-Mechanismus: er zieht Peers *vorsorglich* nach EM,
    verlangt aber fuer einen Ausschluss weitere Belege
    (Attribution-Counter aus tatsaechlich fehlenden Frames). Damit
    ist ein isolierter Header-Spoof harmlos — ein fault-toleranter
    Peer wuerde durch einen einzigen fehlerhaften Rendezvous nicht
    dauerhaft ausgeschlossen. Das kostet einen Zyklus Latenz und
    ist der akzeptable Preis fuer die schnelle Rendezvous-
    Synchronisation.

Test-Assertion:
    Verifiziere, dass mindestens ein Beobachter den Rendezvous
    tatsaechlich getriggert hat (Log-Nachweis). Damit ist belegt,
    dass `InjectFakePhaseHeader` funktioniert und der Header
    ausgewertet wird. Das Recovery-Verhalten der Fabric ist der
    positive Nebenbefund.
"""
from harness.diag import NODE_STATE_WIRE

TARGET = 2
RENDEZVOUS_TIMEOUT_S = 6.0


def test_wrong_phase_header(fabric_3):
    fake = NODE_STATE_WIRE["ErrorManagement"]
    resp = fabric_3.diag.fake_phase_header(TARGET, count=20, wire_value=fake)
    assert resp is not None, "fake_phase_header injection nicht staged"

    # Mindestens ein Observer muss den Rendezvous ausgeloest haben.
    # Das ist der direkte Effekt des gefaelschten Phase-Headers und
    # der einzige beobachtbare Auswirkung im normalen Betrieb — die
    # Fabric erholt sich anschliessend per Design.
    triggered = False
    for observer in (0, 1):
        node = fabric_3.nodes[observer]
        if node.wait_for_log(
            r"peer already in ErrorManagement, rendezvous",
            timeout=RENDEZVOUS_TIMEOUT_S,
        ):
            triggered = True
            break

    assert triggered, (
        "Kein Rendezvous auf Nodes 0 oder 1 innerhalb "
        f"{RENDEZVOUS_TIMEOUT_S}s — Injection funktioniert nicht oder "
        "Header wird gar nicht ausgewertet"
    )
"""
T10 — Split-Sichten (asymmetrische Beobachtung).

Vollstaendiger Test-Case: nur ein bestimmter Peer verwirft Frames von
target, andere sehen alles. Multicast auf loopback laesst sich nicht
per-receiver drop-en ohne eine Test-Only-Route im Rust
(z.B. `InjectDropFromPeer { peer_id }` das im ingest_frame filtert).

Approximation: wir nutzen `divergent-publisher` + `drop-votes` in
Kombination um die asymmetrische Vote-Situation zu erzeugen — nach zwei
Cycles sollte die Fabric konvergieren.

Dieses Szenario ist noch grob; die exakte Split-Sicht braucht eine
weitere Rust-Injection. Markiert als xfail bis dahin.
"""
import pytest

pytestmark = pytest.mark.xfail(
    reason="Braucht InjectDropFromPeer im Rust — approximativer Test",
    strict=False,
)

from harness.assertions import wait_cycles_advance, wait_peer_health  # noqa: E402

TARGET = 2


def test_split_views_approximation(fabric_3):
    # Approximation: target sendet einen zufaelligen Publisher-Pick.
    # Erwartung: Divergenz wird erkannt und propagiert.
    assert fabric_3.diag.divergent_publisher(TARGET, 1)

    survivors = [nid for nid in fabric_3.nodes if nid != TARGET]
    # Nach 1-2 Cycles sollte target excluded sein (im schlechteren Fall
    # gehen alle Failsafe — das ist die konservative SIL-2-Antwort).
    for survivor in survivors:
        peer = wait_peer_health(fabric_3, survivor, TARGET, "Lost", timeout=8.0)
        assert peer is not None
    assert wait_cycles_advance(fabric_3, survivors[0], n_cycles=5, timeout=8.0)

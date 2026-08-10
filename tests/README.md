# Test-Harness

E2E-Tests fuer das Fault-Tolerance-Framework. Spawnt echte Node-Prozesse
ueber `cargo build --bin node`, wickelt die Diagnostic-Multicast-API,
prueft Verhalten via GetStatus und Log-Pattern-Matching.

## Setup

```bash
cd tests
pip install -r requirements.txt
```

Der Harness ruft `cargo build --bin node` einmal pro Session auf. Dauert
beim ersten Lauf ein paar Sekunden, danach cached.

## Ausfuehren

Alle Tests:
```bash
pytest
```

Einzelner Test:
```bash
pytest scenarios/test_t01_silent_after_input.py
```

Ohne die "slow"-markierten (T14):
```bash
pytest -m "not slow"
```

Nur die "slow":
```bash
pytest -m slow
```

Mit ausfuehrlichen Logs:
```bash
pytest -s
```

## Struktur

```
tests/
  conftest.py                 pytest fixtures (binary, fabric_3, fabric_4)
  pytest.ini                  Test-Konfig
  harness/
    config_gen.py             Rendert TOMLs pro Testlauf
    node.py                   Subprocess-Wrapper mit Log-Watcher
    fabric.py                 Orchestriert N Nodes zusammen
    diag.py                   Multicast-Client fuer Injections
    assertions.py             wait_peer_health, wait_cycles_advance, ...
  scenarios/
    test_t01_silent_after_input.py   T1
    test_t02_shutdown.py             T2
    ...
    test_t17_startup_fault.py        T17
```

## Fixtures

- `binary` (session): Path zum Node-Binary. Baut mit cargo einmal.
- `work_dir` (function): tmp-Verzeichnis pro Test.
- `fabric_3` (function): 3-Node-Fabric, wartet auf Operational.
- `fabric_4` (function): 4-Node-Fabric analog.

Jeder Fabric-Fixture stoppt am Ende alle Nodes (auch bei Fehlschlag),
raeumt tmp-Verzeichnisse via pytest's `tmp_path`.

## Status pro Test

| # | Test | Status | Bemerkung |
|---|---|---|---|
| T1 | silent_after_input | ok | drop-results + drop-acks kombiniert |
| T2 | shutdown | ok | Prozess-Exit |
| T3 | silent_before_input | ok | drop-inputs |
| T4 | silent_cyclesync | ok | testet Fix A |
| T5 | input_divergence | ok | targeted-input |
| T6 | result_divergence | SKIP | braucht Payload-Trait |
| T7 | crc_divergence | ok | fake-crc |
| T8 | cycle_skew | ok | approximativ, 3ms Delay |
| T9 | double_fault_2oo4 | ok | zwei Shutdowns hintereinander |
| T10 | split_views | XFAIL | braucht per-peer drop |
| T11 | quorum_2oo3 | ok | testet Fix B |
| T12 | sequential_2oo4 | ok | zwei sequentielle Exklusionen |
| T13 | rejoin | SKIP | braucht restart_node() im Harness |
| T14 | stale_frames | slow | approximativ via tight threshold |
| T15 | publisher_disagreement | ok | divergent-publisher |
| T16 | wrong_phase_header | SKIP | braucht Rust-Injection |
| T17 | startup_fault | SKIP | braucht configurable SelfTest |

## Debugging

Wenn ein Test fehlschlaegt, findest du die Node-Logs unter:
```
<pytest-tmp>/logs/node_<id>.log
```

Der Pfad steht im pytest-Output als `tmp_path`. Beispiel:
```bash
pytest scenarios/test_t01_silent_after_input.py -v
# ... FAIL ...
# tmp path: /tmp/pytest-of-you/pytest-42/test_silent_after_input0/logs/
```

Zusaetzlich kannst du den Testcode um einen `time.sleep(60)` erweitern
kurz vor dem Assert, dann per Hand `diag_test.py status <n>` gegen die
laufende Fabric feuern.

## Wo hier noch Arbeit reingesteckt werden koennte

- `Fabric.restart_node(node_id)` fuer T13.
- Rust-side `InjectDropFromPeer { peer_id }` fuer T10.
- Rust-side `InjectWrongPhaseHeader { fake_state }` fuer T16.
- `SelfTest` konfigurierbar via TOML fuer T17.
- Parallele Testausfuehrung: aktuell seriell weil alle Fabrics die
  gleichen Multicast-Gruppen benutzen. `pytest-xdist` mit rotierenden
  Gruppen-Adressen pro Worker moeglich, aber Aufwand.

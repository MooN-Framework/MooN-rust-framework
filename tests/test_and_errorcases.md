# Fehlerarten und Testszenarien

## Fehlerklassen der Software

| # | Fehlerklasse | Wo detektiert | Betroffene Phase(n) | Erwartetes Verhalten |
|---|---|---|---|---|
| 1 | Silent Fault (still) — Node sendet nichts mehr | `attribute_input_missing`, `attribute_result_missing`, `fault_peers_missing_ack_unilateral`, `attribute_cycle_sync_missing` | ShareInputs, ShareResult, SendAck, CycleSync, CrcExchange | Peer excluded (2oo3: 1 Ausfall OK); nach Fix A/B: mind. 2 Reporter nötig |
| 2 | Value Divergence Input — Sensor liefert abweichenden Wert | `computation.inputs_agree` gate | ShareInputs → EM | InputsDivergent, EM stimmt exclude ab |
| 3 | Value Divergence Result — Compute-Fehler, abweichendes Result | `voter.find_dissenters` | PublishResult → EM | Dissenter propose_exclude, cycle publiziert, dann EM |
| 4 | CRC Divergence — Roster-Zustand divergiert | `crc_unanimous` | SystemStateCrcExchange | Sofort Failsafe (harte Regel, kein Recovery) |
| 5 | Publisher Disagreement — Peers wählen verschiedene Publisher | `publisher_consensus` | PublishResult | Fault → Failsafe (harte Regel) |
| 6 | Stale Frame — Frame kommt zu spät | `frame_age_if_stale` in ingest | Alle | Frame verworfen, degradiert zu Silent-Fault |
| 7 | Phase Header Mismatch — Frame mit unerwartetem `node_state`-Header | Ingest-Match auf Payload | Alle | Verworfen (außer Rendezvous-Trigger für EM) |
| 8 | Cycle Skew — Node startet Cycle deutlich später (Sleep-Jitter) | Cycle-Anker-Barrier | ShareInputs, ShareResult, SendAck, Crc | Late-Node timeoutet, Rendezvous holt ihn nach; bei zu viel Skew Failsafe |
| 9 | Quorum Loss — zu viele Ausfälle | `quorum_available`, `no_buffer_before_vote` | Ende EM | TooFewNodes → Failsafe |
| 10 | Peer Clock Drift — Zeit-Sync veraltet | Über `stale_frame_threshold`, ab `resync_interval_cycles` | Alle | Fresh PeerSync; wenn nicht rechtzeitig → Fehlerkette 6 |
| 11 | Rejoin Fault — Node kehrt zurück, Snapshot inkonsistent | `apply_snapshot`, `majority_snapshot` | SystemStateSync | SystemStateSyncMinority → Isolation |
| 12 | Doppelter Silent Fault (simultan) — 2 Nodes fallen im selben Cycle aus | Kombination 1+9 | Alle | 2oo3: Failsafe; 2oo4: sollte OK sein bei simultan |
| 13 | Split-Sichten (asymmetrische Beobachtung) — A und B sehen unterschiedliche Frames | Rendezvous + aggregate mit ≥2 reporters | ShareInputs, ShareResult | 1 Cycle Extra-Latenz, dann exclude |
| 14 | Ingest-Overflow — Kernel-Puffer läuft voll | Implicit über verworfene Frames | Alle | Frames verloren → wie Silent |
| 15 | Startup Fault — Init self-test schlägt fehl | `SelfTest::run` | Startup | SelfTestErr → sofort Failsafe |
| 16 | Discovery Timeout — Peers finden sich nicht | `discovery_complete` | InitSync | InitialSyncTimeout → Failsafe |

## Testszenarien

| # | Test | Setup | Injection | Erwartete Kette |
|---|---|---|---|---|
| T1 | Silent nach Input | 3 Nodes stabil | `silent 3 1` | Node 3 sendet Input, dann still → A+B in EM → confirmed={3} → Cycle mit 2 weiter |
| T2 | Silent komplett | 3 Nodes stabil | `shutdown 3` | Node 3 weg → A+B EM, Rendezvous ggf. → confirmed={3} → Cycle mit 2 |
| T3 | Silent VOR Input | 3 Nodes stabil | `drop-inputs 3 1` (neu) | Node 3 sendet keinen Input → A+B ShareInputs timeout → EM → confirmed={3} |
| T4 | Silent in CycleSync | 3 Nodes stabil | `drop-cyclesync 3 1` (neu) | Node 3 sendet keine State-Beacon → A+B CycleSyncTimeout → EM → confirmed={3} (testet Fix A) |
| T5 | Input Divergence | 3 Nodes stabil | `set-input-single 3 speed=999` (neu) | Node 3 verwendet abweichenden Input → InputsDivergent → EM |
| T6 | Result Divergence | 3 Nodes stabil | `divergent-result 3 1` (neu) — bit-flip in eigenem Result | Node 3 sendet abweichendes Result → alle publizieren, dann Dissenter-EM → confirmed={3} |
| T7 | CRC Divergence | 3 Nodes stabil | `divergent-crc 3 1` (neu) | Node 3 sendet falschen CRC → alle Failsafe (harte Regel) |
| T8 | Cycle Skew testen | 3 Nodes stabil | `cycle-delay 3 100 1` (neu) — 100ms extra Sleep | Node 3 kommt spät in ShareInputs → sein Timeout überschritten → Rendezvous zieht ihn nach → EM → StateOk (kein Failsafe wenn Offsets groß genug) |
| T9 | Doppelausfall gleichzeitig 2oo4 | 4 Nodes stabil | `shutdown 3 && shutdown 4` | A+B in EM, beide proposed={3,4}, reporters=2, yes=2 → beide excluded → Cycle mit 2 |
| T10 | Split Sichten | 3 Nodes stabil | Rust-Injection: nur A drop einen Frame von C für 1 Cycle | A hat C's Input, B nicht → B in EM → A rendezvous mit proposed={} → confirmed={} → nächster Cycle: beide EM → confirmed={3} |
| T11 | Quorum-Grenze 2oo3 | Bereits 1 Node excluded | `shutdown 2` | A allein → aggregate mit reporters=1 blockt (Fix B) → EM timeout → Failsafe |
| T12 | Sequenzielle Ausfälle 2oo4 | 4 Nodes | `shutdown 4`, warten, `shutdown 3` | 1. → exclude 4; 2. → exclude 3; A+B laufen weiter |
| T13 | Rejoin nach Isolation | Nach T1, Node 3 neu gestartet | Node 3 kommt hoch, InitSync erkennt bestehende Fabric | ResyncLostPeer → SystemStateSync → adoption via majority_snapshot → laufender Cycle |
| T14 | Stale Frames durch Drift | Lange Laufzeit ohne Resync | `set-resync-interval 999999` (Config), warten 10 min | Frames als stale verworfen → wie Silent → EM → exclude |
| T15 | Publisher Disagreement | 3 Nodes stabil | `divergent-publisher 3 1` (neu) — Node 3 sendet Ack mit anderem publisher_candidate | publisher_consensus divergiert → Fault → Failsafe |
| T16 | Frame mit falschem Phase-Header | 3 Nodes stabil | `wrong-phase-header 3 SendAck 1` (neu) — Node 3 sendet Input mit Header=SendAck | A+B verwerfen Frame → wie Silent → EM |
| T17 | Startup fault | Fresh start | Node 3 mit failing self-test starten | Node 3 sofort Failsafe, A+B erreichen nicht Discovery-Nominal → InitialSyncTimeout |
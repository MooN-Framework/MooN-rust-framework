"""
T20 — Timing-Analyse: minimal erreichbare Zyklusdauer (Synchronisationsfrequenz).

Ziel:
    Bestimme die kleinste stabile ``cycle_duration_ms``, bei der die Fabric
    ueber ein Messfenster hinweg im Operational-Zustand bleibt, ohne
    dass ``cycle overrun``-Warnings ueber der Toleranzschwelle liegen.

Methodik:
    1. Sweep in absteigender Reihenfolge ueber eine Kandidatenliste.
    2. Fuer jeden Kandidaten die In-Cycle-Offsets proportional skalieren
       (Verhaeltnis 5 : 10 : 14 / 20 aus den Defaults beibehalten).
    3. Fabric hochfahren, MEASURE_S Sekunden laufen lassen, Logs auswerten.
    4. Kandidat gilt als "stabil", wenn:
         - wait_operational binnen SETUP_TIMEOUT_S erfolgreich,
         - kein Node gestorben ist,
         - Overrun-Anteil pro Node <= MAX_OVERRUN_FRACTION.
    5. Am Ende der kleinste stabile Wert; Report als Text-Artefakt
       im work_dir.

Der Test ist als eigenstaendige Studie gedacht und in ``pytest.ini`` als
``timing`` markiert, um ihn aus der normalen CI auszuschliessen
(``pytest -m timing``).
"""
from __future__ import annotations

import re
import time
from pathlib import Path

import pytest

from harness.fabric import Fabric, FabricOptions


# --------- Sweep-Parameter (zentral zum Tuning) ---------
CANDIDATES_MS = [20, 15, 12, 10, 8, 6, 5, 4, 3, 2]
NOMINAL = 3
MINIMUM = 2
MEASURE_S = 8.0
SETUP_TIMEOUT_S = 15.0
MAX_OVERRUN_FRACTION = 0.02  # <=2% der Zyklen duerfen ueberlaufen
# --------------------------------------------------------


_OVERRUN_RE = re.compile(r"cycle overrun.*overrun_us=(\d+)")
_CYCLE_RE = re.compile(r"cycle duration.*cycle_us=(\d+)")


def _scaled_timing(cycle_ms: int) -> dict:
    """
    Skaliert die In-Cycle-Offsets proportional zu den Defaults
    (cycle=20 -> SI=5, SR=10, SA=14, CRC=17). Rundet auf ganze ms.

    Fuer cycle_ms <= 4 wuerden die Offsets kollabieren; wir erzwingen
    dann send_interval < share_inputs_offset via floor bei 1 ms.
    Ein zu kurzer cycle_ms faellt so bereits an validate() aus, was
    genau das erwartete Verhalten ist (der Kandidat gilt als "instabil").
    """
    ratio = cycle_ms / 20
    si = max(1, round(5 * ratio))
    sr = max(si + 1, round(10 * ratio))
    sa = max(sr + 1, round(14 * ratio))
    crc = max(sa + 1, round(17 * ratio))

    return dict(
        cycle_duration_ms=cycle_ms,
        share_inputs_offset_ms=si,
        share_result_offset_ms=sr,
        send_ack_offset_ms=sa,
        crc_offset_ms=crc,
        # Nicht-Zyklus-Timeouts skalieren wir nicht: sie sind bereits
        # unabhaengig von der Zyklusdauer.
    )


def _analyze_node_log(log_path: Path) -> tuple[int, int, list[int]]:
    """
    Liest das Node-Log und zaehlt Zyklen sowie Overruns.
    Rueckgabe: (n_cycles, n_overruns, overrun_us_list).
    """
    n_cycles = 0
    overruns: list[int] = []
    try:
        for line in log_path.read_text(errors="replace").splitlines():
            if _CYCLE_RE.search(line):
                n_cycles += 1
            m = _OVERRUN_RE.search(line)
            if m:
                overruns.append(int(m.group(1)))
    except FileNotFoundError:
        pass
    return n_cycles, len(overruns), overruns


@pytest.mark.timing
def test_minimum_cycle_duration(binary: Path, work_dir: Path):
    """
    Sweep ueber CANDIDATES_MS, gibt am Ende die kleinste stabile
    Zyklusdauer aus. Der Test faellt NIE hart durch — er ist eine
    Messung. Assertion nur, dass ueberhaupt ein Kandidat stabil war.
    """
    report_lines: list[str] = []
    report_lines.append(
        f"# Timing-Sweep: {NOMINAL}oo{MINIMUM}, measure={MEASURE_S}s, "
        f"max_overrun={MAX_OVERRUN_FRACTION:.1%}"
    )
    report_lines.append(
        "cycle_ms | operational | mean_cycle_us | overrun_frac | worst_overrun_us | verdict"
    )
    report_lines.append("-" * 90)

    smallest_stable: int | None = None

    for cycle_ms in CANDIDATES_MS:
        cand_dir = work_dir / f"cycle_{cycle_ms}ms"
        cand_dir.mkdir(parents=True, exist_ok=True)

        opts = FabricOptions(
            nominal=NOMINAL,
            minimum=MINIMUM,
            binary=binary,
            work_dir=cand_dir,
            cycle_duration_ms=cycle_ms,
            timing_overrides=_scaled_timing(cycle_ms),
        )
        fab = Fabric(opts=opts)

        operational = False
        mean_us = 0
        overrun_frac = 1.0
        worst = 0
        verdict = "FAIL"

        try:
            fab.start_all()
            operational = fab.wait_operational(timeout=SETUP_TIMEOUT_S)
            if not operational:
                verdict = "NO_OPERATIONAL"
            else:
                time.sleep(MEASURE_S)

                any_died = any(not n.is_running() for n in fab.nodes.values())
                if any_died:
                    verdict = "NODE_DIED"

                # Logs pro Node zusammenfassen.
                total_cycles = 0
                total_overruns = 0
                worst_all = 0
                mean_accum = 0
                mean_count = 0
                for nid in fab.nodes:
                    log_path = cand_dir / "logs" / f"node_{nid}.log"
                    nc, no_, ovs = _analyze_node_log(log_path)
                    total_cycles += nc
                    total_overruns += no_
                    if ovs:
                        worst_all = max(worst_all, max(ovs))

                    # Mittlere cycle_us extrahieren.
                    try:
                        text = log_path.read_text(errors="replace")
                        for m in _CYCLE_RE.finditer(text):
                            mean_accum += int(m.group(1))
                            mean_count += 1
                    except FileNotFoundError:
                        pass

                mean_us = mean_accum // max(1, mean_count)
                overrun_frac = total_overruns / max(1, total_cycles)
                worst = worst_all

                if not any_died and overrun_frac <= MAX_OVERRUN_FRACTION:
                    verdict = "STABLE"
                    smallest_stable = cycle_ms
                elif not any_died:
                    verdict = "TOO_MANY_OVERRUNS"
        except Exception as e:
            verdict = f"EXC:{type(e).__name__}"
        finally:
            fab.stop_all()

        report_lines.append(
            f"{cycle_ms:>8} | {str(operational):>11} | {mean_us:>13} | "
            f"{overrun_frac:>12.3%} | {worst:>16} | {verdict}"
        )

        # Wenn wir schon "instabil" sind und weiter runtergehen, ist es
        # sehr wahrscheinlich weiter instabil. Aber wir laufen den Sweep
        # trotzdem zu Ende, denn manchmal wird der Overhead durch kuerzere
        # send_intervals paradoxerweise besser messbar. Keine Optimierung.

    report = "\n".join(report_lines)
    (work_dir / "timing_sweep_report.txt").write_text(report + "\n")
    print("\n" + report + "\n")

    assert smallest_stable is not None, (
        "Kein Kandidat stabil — Umgebung zu jittery oder Sweep-Bereich falsch"
    )
    print(f"\nKleinste stabile Zyklusdauer: {smallest_stable} ms "
          f"({1000 / smallest_stable:.1f} Hz)")
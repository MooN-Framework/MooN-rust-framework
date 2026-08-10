"""
Ein einzelner Node-Prozess plus Log-Watcher.

Design:
- Popen mit stdout=PIPE, stderr=STDOUT (kombiniert).
- Reader-Thread schiebt jede Zeile in einen thread-safe deque.
- API:
    node.start()
    node.wait_for_log(pattern, timeout=5.0) -> Match | None
    node.stop(timeout=2.0)         # SIGTERM, dann SIGKILL
    node.exit_code                 # nur nach stop() gueltig
    node.log_lines                 # list[str] aller bisher gesehenen Zeilen
- Der Log-Watcher speichert alle Zeilen, damit ein Test spaeter auch
  auf ein Pattern matchen kann das schon vorbei ist.
"""
from __future__ import annotations

import os
import re
import signal
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Pattern


@dataclass
class Node:
    node_id: int
    binary: Path
    config_path: Path
    log_dir: Path
    rust_log: str = "info"

    _proc: Optional[subprocess.Popen] = field(default=None, init=False, repr=False)
    _reader_thread: Optional[threading.Thread] = field(default=None, init=False, repr=False)
    _lines: deque = field(default_factory=lambda: deque(maxlen=100_000), init=False, repr=False)
    _lines_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _new_line_event: threading.Event = field(default_factory=threading.Event, init=False, repr=False)
    exit_code: Optional[int] = field(default=None, init=False)

    def start(self) -> None:
        env = os.environ.copy()
        env["RUST_LOG"] = self.rust_log
        # tracing schreibt farbig auf stdout wenn TTY. Wir wollen plaintext.
        env["NO_COLOR"] = "1"

        self.log_dir.mkdir(parents=True, exist_ok=True)

        cmd = [str(self.binary), "node", "--config", str(self.config_path)]
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
            bufsize=1,  # line-buffered
        )

        self._reader_thread = threading.Thread(
            target=self._read_loop,
            name=f"node-{self.node_id}-reader",
            daemon=True,
        )
        self._reader_thread.start()

    def _read_loop(self) -> None:
        assert self._proc and self._proc.stdout
        log_file = self.log_dir / f"node_{self.node_id}.log"
        with log_file.open("w") as f:
            for line in self._proc.stdout:
                line = line.rstrip("\n")
                f.write(line + "\n")
                f.flush()
                with self._lines_lock:
                    self._lines.append(line)
                self._new_line_event.set()

    def wait_for_log(self, pattern: str | Pattern, timeout: float = 5.0) -> Optional[re.Match]:
        """
        Warte bis eine Log-Zeile das Pattern matched, oder timeout.
        Prueft auch alle bereits gesehenen Zeilen — damit ein Test nicht
        an einer Race-Condition scheitert wenn die Zeile schon durch ist.
        """
        pat = re.compile(pattern) if isinstance(pattern, str) else pattern
        deadline = time.monotonic() + timeout

        # Zuerst: bestehende Zeilen scannen.
        with self._lines_lock:
            existing = list(self._lines)
        for ln in existing:
            m = pat.search(ln)
            if m:
                return m

        # Dann: warten auf neue Zeilen bis timeout.
        seen_count = len(existing)
        while time.monotonic() < deadline:
            self._new_line_event.wait(timeout=0.1)
            self._new_line_event.clear()
            with self._lines_lock:
                new = list(self._lines)[seen_count:]
                seen_count = len(self._lines)
            for ln in new:
                m = pat.search(ln)
                if m:
                    return m
        return None

    @property
    def log_lines(self) -> list[str]:
        with self._lines_lock:
            return list(self._lines)

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def stop(self, timeout: float = 2.0) -> None:
        if not self._proc:
            return
        if self._proc.poll() is None:
            self._proc.send_signal(signal.SIGTERM)
            try:
                self.exit_code = self._proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self.exit_code = self._proc.wait(timeout=1.0)
        else:
            self.exit_code = self._proc.returncode
        if self._reader_thread:
            self._reader_thread.join(timeout=1.0)

"""
Pytest-Fixtures. Sitzt in tests/ und wird von pytest automatisch geladen.

Wichtige Fixtures:
- `binary` (session-scope): pfad zum kompilierten Rust-Node-Binary.
  Baut mit `cargo build` beim ersten Test, danach cached.
- `work_dir` (function-scope): temp-Verzeichnis pro Test.
- `fabric_3` (function-scope): 3-Node-Fabric, laeuft und ist operational.
- `fabric_4` (function-scope): 4-Node-Fabric analog.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

import pytest

from harness.fabric import Fabric, FabricOptions


REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def binary() -> Path:
    """Baut den Rust-Node-Binary einmal pro Test-Session."""
    print("\n[fixture] cargo build...")
    t0 = time.monotonic()
    subprocess.run(
        ["cargo", "build", "--bin", "node", "--features", "diagnostic"],
        cwd=REPO_ROOT,
        check=True,
    )
    print(f"[fixture] cargo build fertig in {time.monotonic() - t0:.1f}s")
    return REPO_ROOT / "target" / "debug" / "node"


@pytest.fixture
def work_dir(tmp_path: Path) -> Path:
    return tmp_path


def _make_fabric(
    binary: Path,
    work_dir: Path,
    nominal: int,
    minimum: int,
) -> Fabric:
    opts = FabricOptions(
        nominal=nominal,
        minimum=minimum,
        binary=binary,
        work_dir=work_dir,
        cycle_duration_ms=20,
    )
    f = Fabric(opts=opts)
    f.start_all()
    if not f.wait_operational(timeout=15.0):
        f.stop_all()
        raise RuntimeError("fabric did not reach operational state")
    return f


@pytest.fixture
def fabric_3(binary: Path, work_dir: Path):
    f = _make_fabric(binary, work_dir, nominal=3, minimum=2)
    yield f
    f.stop_all()


@pytest.fixture
def fabric_4(binary: Path, work_dir: Path):
    f = _make_fabric(binary, work_dir, nominal=4, minimum=2)
    yield f
    f.stop_all()

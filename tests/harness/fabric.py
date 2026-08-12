"""
Fabric: eine Menge Nodes die zusammen laufen. Startet, wartet auf
Operational, stoppt beim Verlassen des Context-Managers.
"""
from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from .config_gen import NodeSpec, render_config
from .diag import DiagClient
from .node import Node


@dataclass
class FabricOptions:
    nominal: int
    minimum: int
    binary: Path
    work_dir: Path
    fabric_port: int = 5555
    diag_port: int = 6666
    fabric_group: str = "239.10.0.1"
    diag_group: str = "239.10.0.2"
    cycle_duration_ms: int = 20
    # Optional overrides zum Bauen des NodeSpec; leeres dict = Defaults.
    timing_overrides: dict = field(default_factory=dict)


@dataclass
class Fabric:
    opts: FabricOptions
    nodes: dict[int, Node] = field(default_factory=dict)
    diag: DiagClient | None = None

    def _make_spec(self, own_id: int) -> NodeSpec:
        base = NodeSpec(
            own_id=own_id,
            nominal=self.opts.nominal,
            minimum=self.opts.minimum,
            fabric_port=self.opts.fabric_port,
            diag_port=self.opts.diag_port,
            fabric_group=self.opts.fabric_group,
            diag_group=self.opts.diag_group,
            cycle_duration_ms=self.opts.cycle_duration_ms,
        )
        for k, v in self.opts.timing_overrides.items():
            setattr(base, k, v)
        return base

    def start_all(self) -> None:
        cfg_dir = self.opts.work_dir / "configs"
        cfg_dir.mkdir(parents=True, exist_ok=True)
        log_dir = self.opts.work_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        for own_id in range(self.opts.nominal):
            spec = self._make_spec(own_id)
            cfg_path = render_config(spec, cfg_dir / f"node_{own_id}.toml")
            node = Node(
                node_id=own_id,
                binary=self.opts.binary,
                config_path=cfg_path,
                log_dir=log_dir,
            )
            node.start()
            self.nodes[own_id] = node

        # Diagnostic client aufbauen NACH den Nodes damit die Multicast-
        # Gruppe schon existiert.
        self.diag = DiagClient(
            multicast_group=self.opts.diag_group,
            port=self.opts.diag_port,
        )

    def wait_operational(self, timeout: float = 15.0) -> bool:
        """
        Wartet bis jeder Node einmal 'Operational' erreicht hat.
        Erkennungspattern: 'transition from=CycleSync event=CycleSyncOk to=ReadInputs'
        signalisiert dass Discovery + Sync durch und der erste Cycle laeuft.
        """
        pat = r"transition from=CycleSync event=CycleSyncOk to=ReadInputs"
        for node in self.nodes.values():
            if not node.wait_for_log(pat, timeout=timeout):
                return False
        return True

    def stop_all(self) -> None:
        if self.diag is not None:
            self.diag.close()
            self.diag = None
        for node in self.nodes.values():
            node.stop(timeout=2.0)

    def alive_ids(self) -> list[int]:
        return [nid for nid, node in self.nodes.items() if node.is_running()]

    def restart_node(self, node_id: int, wait_operational: float = 8.0) -> None:
        """
        Killt (falls noch lebendig) und respawnt den Node mit derselben
        config. Der neue Prozess bindet die gleichen Multicast-Sockets;
        das appended Log geht in dieselbe Datei (Node._read_loop
        verwendet Append-Mode).
    
        Kein wait_operational-Check: der rejoin-Prozess laeuft nicht
        ueber CycleSyncOk, sondern ueber ResyncLostPeer. Der Test soll
        selbst mit wait_peer_health(target, "Probation") warten.
        """
        if node_id not in self.nodes:
            raise KeyError(f"unknown node_id {node_id}")
    
        old = self.nodes[node_id]
        if old.is_running():
            old.stop(timeout=2.0)
    
        from .node import Node  # local import um circular zu vermeiden
        new_node = Node(
            node_id=node_id,
            binary=old.binary,
            config_path=old.config_path,
            log_dir=old.log_dir,
            rust_log=old.rust_log,
        )
        new_node.start()
        self.nodes[node_id] = new_node
        _ = wait_operational  # reserviert fuer zukuenftigen "warte bis Alive"-Modus


@contextmanager
def fabric(opts: FabricOptions) -> Iterator[Fabric]:
    f = Fabric(opts=opts)
    f.start_all()
    try:
        if not f.wait_operational():
            raise RuntimeError("fabric did not reach operational state")
        yield f
    finally:
        f.stop_all()

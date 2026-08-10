"""
Diagnostic-Multicast-Client fuer den Test-Harness.

Klein und synchron gehalten: fuer jede Injection sendet der Client das
Telegramm, sammelt Antworten fuer ein kurzes Fenster, gibt sie als Liste
zurueck. Fuer GetStatus wird das Status-JSON pro Node zurueckgegeben.

Getrennt von diag_test.py weil das ein CLI-Tool ist und wir hier eine
programmatische API brauchen.
"""
from __future__ import annotations

import json
import select
import socket
import struct
import time
from typing import Any, Optional


class DiagClient:
    def __init__(
        self,
        multicast_group: str = "239.10.0.2",
        port: int = 6666,
        interface_ip: str = "127.0.0.1",
    ):
        self.group = multicast_group
        self.port = port
        self.interface_ip = interface_ip
        self._sock = self._make_socket()

    def _make_socket(self) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        s.bind(("", self.port))
        mreq = struct.pack(
            "4s4s",
            socket.inet_aton(self.group),
            socket.inet_aton(self.interface_ip),
        )
        s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        s.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_MULTICAST_IF,
            socket.inet_aton(self.interface_ip),
        )
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        s.setblocking(False)
        return s

    def close(self) -> None:
        try:
            self._sock.close()
        except OSError:
            pass

    # ---- Basic send/recv --------------------------------------------------

    def _send(self, telegram: dict) -> None:
        self._sock.sendto(json.dumps(telegram).encode(), (self.group, self.port))

    def _drain(self, duration: float = 0.2) -> list[dict]:
        """Alle Antworten fuer duration Sekunden einsammeln."""
        deadline = time.monotonic() + duration
        out = []
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            rlist, _, _ = select.select([self._sock], [], [], max(remaining, 0.01))
            if not rlist:
                continue
            try:
                data, _ = self._sock.recvfrom(8192)
            except BlockingIOError:
                continue
            try:
                parsed = json.loads(data)
            except json.JSONDecodeError:
                continue
            # Loopback (unsere eigenen Sends) rausfiltern.
            if parsed.get("type") in ("command", "input"):
                continue
            out.append(parsed)
        return out

    # ---- Injection commands ----------------------------------------------

    def _inject(
        self,
        node_id: int,
        cmd: str,
        wait_ack: bool = True,
        ack_timeout: float = 3.0,
        **kwargs,
    ) -> Optional[dict]:
        """
        Sendet ein gerichtetes Command und wartet auf das Staged-Ack.
        Wir senden ein paar Mal weil Multicast auf loopback rare packet
        loss haben kann.
        """
        telegram = {
            "type": "command",
            "targets": [node_id],
            "cmd": cmd,
            **kwargs,
        }
        if not wait_ack:
            self._send(telegram)
            return None

        deadline = time.monotonic() + ack_timeout
        while time.monotonic() < deadline:
            self._send(telegram)
            for resp in self._drain(0.3):
                if resp.get("type") == "staged" and resp.get("source_node_id") == node_id:
                    return resp
        return None

    # Frame drops
    def drop_inputs(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_drop_inputs", count=count)

    def drop_results(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_drop_results", count=count)

    def drop_acks(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_drop_acks", count=count)

    def drop_cyclesync(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_drop_cyclesync", count=count)

    def drop_crc(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_drop_crc", count=count)

    def drop_votes(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_drop_votes", count=count)

    # Value corruption
    def fake_crc(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_fake_crc", count=count)

    def divergent_publisher(self, node_id: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_divergent_publisher", count=count)

    # Whole-node
    def shutdown(self, node_id: int) -> Optional[dict]:
        return self._inject(node_id, "inject_shutdown")

    def mute(self, node_id: int, cycles: int) -> Optional[dict]:
        return self._inject(node_id, "inject_mute", cycles=cycles)

    def cycle_delay(self, node_id: int, ms: int, count: int) -> Optional[dict]:
        return self._inject(node_id, "inject_cycle_delay", ms=ms, count=count)

    def targeted_input(self, node_id: int, value: dict) -> Optional[dict]:
        return self._inject(node_id, "inject_targeted_input", value=value)

    def clear(self, node_id: int) -> Optional[dict]:
        return self._inject(node_id, "clear_injection")

    # Silent = drop_results + drop_acks
    def silent(self, node_id: int, count: int, ack_timeout: float = 3.0) -> bool:
        """
        Beide Kommandos hintereinander senden, dann auf beide Acks
        gleichzeitig warten. Wichtig: erst BEIDE senden, dann warten —
        sonst kann der Node zwischen den zwei Sends bereits Wirkung
        entfalten und das zweite Kommando verpassen.

        Multicast auf loopback verliert manchmal einzelne Pakete, wir
        wiederholen deshalb periodisch beide Kommandos zusammen bis
        BEIDE Acks eingetroffen sind.
        """
        needed = {"inject_drop_results", "inject_drop_acks"}
        received: set[str] = set()

        deadline = time.monotonic() + ack_timeout
        while time.monotonic() < deadline and received != needed:
            self._send({
                "type": "command",
                "targets": [node_id],
                "cmd": "inject_drop_results",
                "count": count,
            })
            self._send({
                "type": "command",
                "targets": [node_id],
                "cmd": "inject_drop_acks",
                "count": count,
            })

            for resp in self._drain(0.3):
                if (
                    resp.get("type") == "staged"
                    and resp.get("source_node_id") == node_id
                ):
                    kind = resp.get("staged_kind", "")
                    if kind in needed:
                        received.add(kind)

        return received == needed

    # Broadcast input
    def broadcast_input(self, value: dict) -> list[dict]:
        telegram = {"type": "input", "value": value}
        self._send(telegram)
        return self._drain(0.3)

    # ---- Status polling --------------------------------------------------

    def get_status(self, node_id: int, timeout: float = 2.0) -> Optional[dict]:
        """
        Status eines Nodes abfragen. Sendet get_status bis eine Antwort
        kommt oder timeout. Gibt das `data`-Objekt zurueck (StatusResponse).
        """
        telegram = {
            "type": "command",
            "targets": [node_id],
            "cmd": "get_status",
        }
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._send(telegram)
            for resp in self._drain(0.3):
                if (
                    resp.get("type") == "status"
                    and resp.get("source_node_id") == node_id
                ):
                    return resp.get("data")
        return None

    def get_all_statuses(
        self, node_ids: list[int], timeout: float = 2.0
    ) -> dict[int, Optional[dict]]:
        return {nid: self.get_status(nid, timeout=timeout) for nid in node_ids}

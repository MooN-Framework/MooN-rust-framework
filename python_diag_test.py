#!/usr/bin/env python3
"""
Kleines Diagnose-Testtool: sendet ein Telegramm an die Multicast-Gruppe
und zeigt die naechsten paar Antworten an.
"""

import json
import socket
import struct
import sys
import time

MULTICAST_GROUP = "239.10.0.2"
PORT = 6666
INTERFACE_IP = "127.0.0.1"  # anpassen wenn du nicht auf lo bist
RECV_TIMEOUT_SECONDS = 3.0


def make_socket():
    """Empfangs-Socket auf der Multicast-Gruppe."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)  # NEU
    s.bind(("", PORT))

    mreq = struct.pack(
        "4s4s",
        socket.inet_aton(MULTICAST_GROUP),
        socket.inet_aton(INTERFACE_IP),
    )
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    # Interface auch fuers Senden setzen (damit Multicast raus geht).
    s.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_MULTICAST_IF,
        socket.inet_aton(INTERFACE_IP),
    )
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 0)
    return s


def send(sock, telegram):
    payload = json.dumps(telegram).encode()
    sock.sendto(payload, (MULTICAST_GROUP, PORT))
    print(f"→ sent: {json.dumps(telegram)}")


def listen(sock, duration=RECV_TIMEOUT_SECONDS):
    sock.settimeout(0.2)
    end = time.time() + duration
    while time.time() < end:
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            continue
        try:
            parsed = json.loads(data)
            # Eigene Sendungen (loopback) ignorieren.
            # Nur Node-Antworten haben type: status, staged, error.
            if parsed.get("type") in ("command", "input"):
                continue
            print(f"← from {addr[0]}: {json.dumps(parsed, indent=2)}")
        except json.JSONDecodeError:
            print(f"← from {addr[0]} (raw): {data!r}")


def main():
    if len(sys.argv) < 2:
        print("Usage: diag_client.py <command> [args]")
        print("Commands:")
        print("  status <node_id>")
        print("  drop-results <node_id> <count>")
        print("  drop-acks <node_id> <count>")
        print("  clear <node_id>")
        print("  set-input <speed> <target_speed> <available_distance>")
        return

    cmd = sys.argv[1]
    sock = make_socket()

    if cmd == "status":
        node_id = int(sys.argv[2])
        send(sock, {
            "type": "command",
            "targets": [node_id],
            "cmd": "get_status",
        })

    elif cmd == "drop-results":
        node_id = int(sys.argv[2])
        count = int(sys.argv[3])
        send(sock, {
            "type": "command",
            "targets": [node_id],
            "cmd": "inject_drop_results",
            "count": count,
        })

    elif cmd == "drop-acks":
        node_id = int(sys.argv[2])
        count = int(sys.argv[3])
        send(sock, {
            "type": "command",
            "targets": [node_id],
            "cmd": "inject_drop_acks",
            "count": count,
        })

    elif cmd == "clear":
        node_id = int(sys.argv[2])
        send(sock, {
            "type": "command",
            "targets": [node_id],
            "cmd": "clear_injection",
        })

    elif cmd == "set-input":
        speed = float(sys.argv[2])
        target = float(sys.argv[3])
        dist = float(sys.argv[4])
        send(sock, {
            "type": "input",
            "value": {
                "current_speed": speed,
                "target_speed": target,
                "available_distance": dist,
            },
        })

    else:
        print(f"unknown command: {cmd}")
        return

    print("--- listening for responses ---")
    listen(sock)


if __name__ == "__main__":
    main()
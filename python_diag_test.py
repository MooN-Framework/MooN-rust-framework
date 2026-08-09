#!/usr/bin/env python3
"""
Kleines Diagnose-Testtool: sendet ein Telegramm an die Multicast-Gruppe
und wartet non-blocking auf Antworten. Sendet periodisch neu, bis die
erwartete Anzahl an Antworten eingetroffen ist oder das Gesamttimeout
ablaeuft.

Duplikate (Antwort desselben source_node_id mehrfach) werden gezaehlt
als eine Antwort, damit mehrfaches Neusenden nicht die Zaehlung
verfaelscht.
"""

import argparse
import json
import select
import socket
import struct
import sys
import time

MULTICAST_GROUP = "239.10.0.2"
PORT = 6666
INTERFACE_IP = "127.0.0.1"

RESEND_INTERVAL = 1.0     # sek. zwischen Sendungen
OVERALL_TIMEOUT = 10.0    # sek. bis endgueltig aufgegeben wird
POLL_INTERVAL   = 0.05    # sek. select-Poll


def make_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind(("", PORT))

    mreq = struct.pack(
        "4s4s",
        socket.inet_aton(MULTICAST_GROUP),
        socket.inet_aton(INTERFACE_IP),
    )
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    s.setsockopt(
        socket.IPPROTO_IP,
        socket.IP_MULTICAST_IF,
        socket.inet_aton(INTERFACE_IP),
    )
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    s.setblocking(False)
    return s


def send(sock, telegram):
    payload = json.dumps(telegram).encode()
    sock.sendto(payload, (MULTICAST_GROUP, PORT))
    print(f"→ sent: {json.dumps(telegram)}")


def is_response(parsed):
    """Loopback (eigene commands/inputs) rausfiltern."""
    return parsed.get("type") not in ("command", "input")


def send_and_wait(sock, telegram, expected_count,
                  resend_interval=RESEND_INTERVAL,
                  overall_timeout=OVERALL_TIMEOUT,
                  poll_interval=POLL_INTERVAL):
    """
    Sendet telegram periodisch und liest non-blocking bis:
      - expected_count verschiedene source_node_ids geantwortet haben
        (return True)
      - overall_timeout erreicht ist (return False)

    Wenn ein Node auf mehrfaches Neusenden mehrfach antwortet, zaehlt
    das trotzdem als eine Antwort (Deduplizierung per source_node_id).
    """
    start = time.monotonic()
    next_send = 0.0
    seen_nodes = set()

    while True:
        now = time.monotonic()

        if now - start >= overall_timeout:
            print(f"--- timeout, {len(seen_nodes)}/{expected_count} Antworten erhalten ---")
            return False

        if now >= next_send:
            send(sock, telegram)
            next_send = now + resend_interval

        rlist, _, _ = select.select([sock], [], [], poll_interval)
        if not rlist:
            continue

        try:
            data, addr = sock.recvfrom(4096)
        except BlockingIOError:
            continue

        try:
            parsed = json.loads(data)
        except json.JSONDecodeError:
            print(f"← from {addr[0]} (raw): {data!r}")
            continue

        if not is_response(parsed):
            continue

        src = parsed.get("source_node_id")
        if src in seen_nodes:
            continue  # Duplikat, still schlucken

        seen_nodes.add(src)
        print(f"← from {addr[0]} (node {src}): {json.dumps(parsed, indent=2)}")

        if len(seen_nodes) >= expected_count:
            print(f"--- {len(seen_nodes)}/{expected_count} Antworten, fertig ---")
            return True


def build_telegram(args):
    cmd = args.cmd

    if cmd == "status":
        return {
            "type": "command",
            "targets": [args.node_id],
            "cmd": "get_status",
        }
    if cmd == "drop-results":
        return {
            "type": "command",
            "targets": [args.node_id],
            "cmd": "inject_drop_results",
            "count": args.count,
        }
    if cmd == "drop-acks":
        return {
            "type": "command",
            "targets": [args.node_id],
            "cmd": "inject_drop_acks",
            "count": args.count,
        }
    if cmd == "clear":
        return {
            "type": "command",
            "targets": [args.node_id],
            "cmd": "clear_injection",
        }
    if cmd == "set-input":
        return {
            "type": "input",
            "value": {
                "current_speed": args.speed,
                "target_speed": args.target_speed,
                "available_distance": args.available_distance,
            },
        }
    raise ValueError(f"unknown command: {cmd}")


def build_silent_telegrams(node_id, count):
    """
    'Silent'-Injection: der Zielnode sendet weiterhin Input, danach aber
    weder Result noch Ack fuer `count` Cycles. Das simuliert einen stillen
    Ausfall mitten im Cycle und triggert den Rendezvous-Pfad (PeerInError)
    auf den beiden gesunden Peers.

    Zwei einzelne Telegramme; Rust-seitig staged Diagnostic beide beim
    naechsten Cycle-Boundary gemeinsam, sodass sie im selben Cycle
    aktiv werden.
    """
    return [
        {
            "type": "command",
            "targets": [node_id],
            "cmd": "inject_drop_results",
            "count": count,
        },
        {
            "type": "command",
            "targets": [node_id],
            "cmd": "inject_drop_acks",
            "count": count,
        },
    ]


def default_expected(cmd):
    """Sinnvolle Defaults, wenn --expect nicht gesetzt wurde."""
    if cmd in ("status", "drop-results", "drop-acks", "clear", "silent"):
        return 1  # gerichtet an einen Node
    if cmd == "set-input":
        return 3  # broadcast an alle
    return 1


def run_silent(sock, args):
    """
    Silent-Injection: zwei Telegramme nacheinander an denselben Node.
    Wartet nach jedem auf ein Staged-Ack, damit sichergestellt ist,
    dass beide Kommandos vom Rust-Node auch angenommen wurden.
    """
    telegrams = build_silent_telegrams(args.node_id, args.count)
    for i, tel in enumerate(telegrams, start=1):
        print(f"--- silent step {i}/{len(telegrams)} ---")
        ok = send_and_wait(
            sock, tel, expected_count=1,
            overall_timeout=args.timeout,
        )
        if not ok:
            print(f"--- silent step {i} nicht bestaetigt, abbruch ---")
            return False
    print(f"--- silent aktiviert fuer node {args.node_id}, {args.count} cycles ---")
    return True


def main():
    parser = argparse.ArgumentParser(description="Diagnose-Testtool")
    parser.add_argument(
        "--expect", type=int, default=None,
        help="Anzahl erwarteter Antworten (Default: 1 fuer gerichtete "
             "Kommandos, 3 fuer set-input)"
    )
    parser.add_argument(
        "--timeout", type=float, default=OVERALL_TIMEOUT,
        help=f"Gesamttimeout in Sekunden (Default: {OVERALL_TIMEOUT})"
    )

    subs = parser.add_subparsers(dest="cmd", required=True)

    p_status = subs.add_parser("status", help="Status eines Nodes abfragen")
    p_status.add_argument("node_id", type=int)

    p_drop_r = subs.add_parser("drop-results", help="Result-Sendungen droppen")
    p_drop_r.add_argument("node_id", type=int)
    p_drop_r.add_argument("count", type=int)

    p_drop_a = subs.add_parser("drop-acks", help="Ack-Sendungen droppen")
    p_drop_a.add_argument("node_id", type=int)
    p_drop_a.add_argument("count", type=int)

    p_silent = subs.add_parser(
        "silent",
        help="Stiller Ausfall mid-cycle: droppt Result UND Ack fuer N Cycles "
             "(Kombination aus drop-results + drop-acks). Testet den "
             "Rendezvous-Pfad in der 2oo3-Konfiguration.",
    )
    p_silent.add_argument("node_id", type=int)
    p_silent.add_argument("count", type=int)

    p_clear = subs.add_parser("clear", help="Injection loeschen")
    p_clear.add_argument("node_id", type=int)

    p_input = subs.add_parser("set-input", help="Input-Daten broadcasten")
    p_input.add_argument("speed", type=float)
    p_input.add_argument("target_speed", type=float)
    p_input.add_argument("available_distance", type=float)

    args = parser.parse_args()

    sock = make_socket()

    # Silent laeuft ueber zwei Telegramme, daher eigener Pfad.
    if args.cmd == "silent":
        ok = run_silent(sock, args)
        sys.exit(0 if ok else 1)

    try:
        telegram = build_telegram(args)
    except ValueError as e:
        print(e)
        sys.exit(2)

    expected = args.expect if args.expect is not None else default_expected(args.cmd)

    print(f"--- send loop, warte auf {expected} Antwort(en) ---")
    ok = send_and_wait(sock, telegram, expected_count=expected,
                       overall_timeout=args.timeout)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
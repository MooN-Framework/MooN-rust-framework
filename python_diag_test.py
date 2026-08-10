#!/usr/bin/env python3
"""
Diagnose-Testtool fuer die Fault-Injection-API.

Sendet ein Telegramm an die Multicast-Gruppe und wartet non-blocking auf
Antworten. Sendet periodisch neu, bis die erwartete Anzahl an Antworten
eingetroffen ist oder das Gesamttimeout ablaeuft.

Kommando-Katalog (siehe Fehlerarten-Tabelle):

Zustands-Abfrage:
  status <node>                       Status abfragen

Frame-Suppression (pro Cycle):
  drop-inputs   <node> <n>            Input-Frames droppen        (T3)
  drop-results  <node> <n>            Result-Frames droppen       (T1 Teil)
  drop-acks     <node> <n>            Ack-Frames droppen          (T1 Teil)
  drop-cyclesync <node> <n>           CycleSync-Frames droppen    (T4)
  drop-crc      <node> <n>            SystemStateCrc droppen
  drop-votes    <node> <n>            ExclusionProposal droppen   (T10)

Wert-Verfaelschung:
  fake-crc      <node> <n>            Falscher CRC-Wert           (T7)
  divergent-publisher <node> <n>      Ack mit falschem Publisher  (T15)

Node-Verhalten:
  silent   <node> <n>                 Result + Ack droppen        (T1 komplett)
  shutdown <node>                     Prozess exit (irreversibel) (T2)
  mute     <node> <cycles>            Alle Sends fuer N Cycles    (T2 soft)
  cycle-delay <node> <ms> <n>         Extra Sleep in ReadInputs   (T8)
  set-input-single <node> <sp> <ts> <ad>
                                      Nur an diesen Node          (T5)

Aufraeumen:
  clear <node>                        Alle Injections zuruecksetzen
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

RESEND_INTERVAL = 1.0
OVERALL_TIMEOUT = 10.0
POLL_INTERVAL   = 0.05


def make_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    s.bind(("", PORT))
    mreq = struct.pack("4s4s",
        socket.inet_aton(MULTICAST_GROUP),
        socket.inet_aton(INTERFACE_IP))
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF,
        socket.inet_aton(INTERFACE_IP))
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    s.setblocking(False)
    return s


def send(sock, telegram):
    sock.sendto(json.dumps(telegram).encode(), (MULTICAST_GROUP, PORT))
    print(f"→ sent: {json.dumps(telegram)}")


def is_response(parsed):
    return parsed.get("type") not in ("command", "input")


def send_and_wait(sock, telegram, expected_count,
                  overall_timeout=OVERALL_TIMEOUT,
                  resend_interval=RESEND_INTERVAL,
                  poll_interval=POLL_INTERVAL):
    start = time.monotonic()
    next_send = 0.0
    seen_nodes = set()
    while True:
        now = time.monotonic()
        if now - start >= overall_timeout:
            print(f"--- timeout, {len(seen_nodes)}/{expected_count} Antworten ---")
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
            continue
        seen_nodes.add(src)
        print(f"← from {addr[0]} (node {src}): {json.dumps(parsed, indent=2)}")
        if len(seen_nodes) >= expected_count:
            print(f"--- {len(seen_nodes)}/{expected_count} Antworten, fertig ---")
            return True


# ---- Command builders ------------------------------------------------------

def _targeted(node_id, cmd, **kwargs):
    return {"type": "command", "targets": [node_id], "cmd": cmd, **kwargs}


def build_telegram(args):
    c = args.cmd
    n = getattr(args, "node_id", None)

    if c == "status":
        return _targeted(n, "get_status")

    # Simple count-based drops.
    for name, rust_cmd in [
        ("drop-inputs",    "inject_drop_inputs"),
        ("drop-results",   "inject_drop_results"),
        ("drop-acks",      "inject_drop_acks"),
        ("drop-cyclesync", "inject_drop_cyclesync"),
        ("drop-crc",       "inject_drop_crc"),
        ("drop-votes",     "inject_drop_votes"),
        ("fake-crc",             "inject_fake_crc"),
        ("divergent-publisher",  "inject_divergent_publisher"),
    ]:
        if c == name:
            return _targeted(n, rust_cmd, count=args.count)

    if c == "shutdown":
        return _targeted(n, "inject_shutdown")
    if c == "clear":
        return _targeted(n, "clear_injection")
    if c == "mute":
        return _targeted(n, "inject_mute", cycles=args.cycles)
    if c == "cycle-delay":
        return _targeted(n, "inject_cycle_delay",
                         ms=args.ms, count=args.count)
    if c == "set-input-single":
        return _targeted(n, "inject_targeted_input", value={
            "current_speed": args.speed,
            "target_speed": args.target_speed,
            "available_distance": args.available_distance,
        })
    if c == "set-input":
        return {
            "type": "input",
            "value": {
                "current_speed": args.speed,
                "target_speed": args.target_speed,
                "available_distance": args.available_distance,
            },
        }
    raise ValueError(f"unknown command: {c}")


def build_silent_telegrams(node_id, count):
    """silent = drop-results + drop-acks kombiniert."""
    return [
        _targeted(node_id, "inject_drop_results", count=count),
        _targeted(node_id, "inject_drop_acks",    count=count),
    ]


def default_expected(cmd):
    if cmd == "set-input":
        return 3
    return 1


def run_silent(sock, args):
    tels = build_silent_telegrams(args.node_id, args.count)
    for i, tel in enumerate(tels, start=1):
        print(f"--- silent step {i}/{len(tels)} ---")
        if not send_and_wait(sock, tel, expected_count=1,
                             overall_timeout=args.timeout):
            print(f"--- silent step {i} nicht bestaetigt, abbruch ---")
            return False
    print(f"--- silent aktiviert node {args.node_id}, {args.count} cycles ---")
    return True


# ---- CLI plumbing ----------------------------------------------------------

def _add_node_count(sp):
    sp.add_argument("node_id", type=int)
    sp.add_argument("count", type=int)


def main():
    p = argparse.ArgumentParser(description="Diagnose-Testtool")
    p.add_argument("--expect", type=int, default=None,
                   help="Erwartete Antwortenzahl (default: 1, bei set-input: 3)")
    p.add_argument("--timeout", type=float, default=OVERALL_TIMEOUT,
                   help=f"Gesamttimeout in Sekunden (default {OVERALL_TIMEOUT})")

    sp = p.add_subparsers(dest="cmd", required=True)

    sp.add_parser("status", help="Node-Status abfragen").add_argument("node_id", type=int)

    # Frame drops.
    for name, help_ in [
        ("drop-inputs",    "Input-Frames droppen (T3)"),
        ("drop-results",   "Result-Frames droppen"),
        ("drop-acks",      "Ack-Frames droppen"),
        ("drop-cyclesync", "CycleSync-Frames droppen (T4)"),
        ("drop-crc",       "SystemStateCrc-Frames droppen"),
        ("drop-votes",     "ExclusionProposal-Frames droppen (T10)"),
        ("fake-crc",             "Falscher CRC-Wert (T7)"),
        ("divergent-publisher",  "Ack mit falschem Publisher (T15)"),
    ]:
        _add_node_count(sp.add_parser(name, help=help_))

    # Silent (T1 kombiniert).
    silent = sp.add_parser("silent",
        help="Result + Ack droppen fuer N Cycles (T1 kombiniert)")
    _add_node_count(silent)

    # Shutdown (T2 hart).
    sp.add_parser("shutdown", help="Prozess-Exit bei naechstem Cycle (T2)"
                  ).add_argument("node_id", type=int)

    # Mute (T2 soft).
    mute = sp.add_parser("mute",
        help="Alle Sends fuer N Cycles unterdruecken, reversibel via clear")
    mute.add_argument("node_id", type=int)
    mute.add_argument("cycles", type=int)

    # Cycle delay (T8).
    cd = sp.add_parser("cycle-delay",
        help="Extra Sleep in ReadInputs, testet Cycle-Skew-Rendezvous (T8)")
    cd.add_argument("node_id", type=int)
    cd.add_argument("ms", type=int)
    cd.add_argument("count", type=int)

    # Targeted input (T5).
    ti = sp.add_parser("set-input-single",
        help="Input nur an einen Node (Input-Divergenz, T5)")
    ti.add_argument("node_id", type=int)
    ti.add_argument("speed", type=float)
    ti.add_argument("target_speed", type=float)
    ti.add_argument("available_distance", type=float)

    # Broadcast input.
    bi = sp.add_parser("set-input", help="Input an alle Nodes broadcasten")
    bi.add_argument("speed", type=float)
    bi.add_argument("target_speed", type=float)
    bi.add_argument("available_distance", type=float)

    # Clear.
    sp.add_parser("clear", help="Alle Injections zuruecksetzen (ausser Shutdown)"
                  ).add_argument("node_id", type=int)

    args = p.parse_args()
    sock = make_socket()

    if args.cmd == "silent":
        sys.exit(0 if run_silent(sock, args) else 1)

    try:
        tel = build_telegram(args)
    except ValueError as e:
        print(e)
        sys.exit(2)

    expected = args.expect if args.expect is not None else default_expected(args.cmd)
    print(f"--- send loop, warte auf {expected} Antwort(en) ---")
    ok = send_and_wait(sock, tel, expected_count=expected,
                       overall_timeout=args.timeout)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
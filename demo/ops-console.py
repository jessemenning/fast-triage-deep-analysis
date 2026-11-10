#!/usr/bin/env python3
"""
ops-console.py — live terminal console for the FAA arrival-capacity demo.

Subscribes over MQTT (Solace Platform broker, localhost:1883, default/default)
to the data-product trigger stream and the agent decision stream and renders
each message as a colored card:

    fl/faa/arrival-capacity/#   Continuum breach + demand feeds  (.../breach/<airport>, .../demand/<airport>)
    solutions/#                 Agent Mesh decisions             solutions/arrival-capacity/decision/<airport>

For every decision it prints the trigger -> decision latency, measured from the
wall-clock arrival of the most recent trigger with the same key (and, when
available, the same product slug). Handles bare rows, Continuum's
{"insert": {...}} / {"delete": {...}} changelog envelopes, JSON arrays and
NDJSON batches.

MQTT wildcards: '#' is multi-level (== SMF '>'), '+' is single-level (== SMF '*').

    ./ops-console.py                              # defaults
    ./ops-console.py --topics 'fl/faa/arrival-capacity/#' 'solutions/#'
    ./ops-console.py --json | tee run.ndjson       # raw NDJSON, no colors

Requires paho-mqtt 2.x (host has 2.1.0).
"""
import argparse
import json
import os
import shutil
import signal
import sys
import time
from datetime import datetime

try:
    import paho.mqtt.client as mqtt
except ImportError:  # pragma: no cover
    sys.exit("paho-mqtt is required:  pip install 'paho-mqtt>=2'")

# ── colors ───────────────────────────────────────────────────────────────────
class C:
    RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
    RED = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"
    BLUE = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"
    BGREEN = "\033[1;32m"; BYELLOW = "\033[1;33m"; BCYAN = "\033[1;36m"; BMAGENTA = "\033[1;35m"; BRED = "\033[1;31m"

def _strip_colors():
    for k in list(vars(C)):
        if not k.startswith("_"):
            setattr(C, k, "")

# Fields shown first on a card, in this order, when present.
PRIORITY = [
    "airport", "airport_name", "tracon", "bin_start", "bin_end",
    "demand", "bin_capacity", "overage", "severity", "eta_quality",
    "runway_config", "wx_flight_category", "active_restriction_type",
    "action", "will_materialize", "impact", "case_priority", "confidence",
    "headline", "whats_happening", "why", "downstream_airports_at_risk",
    "options", "provenance", "reason", "summary", "rationale",
]
HIDE = {"_raw"}
INCLUDE_LIVE = False   # set from --include-live in main()

# ── topic classification ─────────────────────────────────────────────────────
def classify(topic: str):
    """Return (kind, slug, trigger, key)."""
    lv = topic.split("/")
    # fl/faa/arrival-capacity/breach/{airport}  — the agent trigger
    if topic.startswith("fl/faa/arrival-capacity/breach/"):
        return "trigger", "arrival-capacity", "breach", lv[-1]
    # fl/faa/arrival-capacity/demand/{airport}  — live demand feed (noisy)
    if topic.startswith("fl/faa/arrival-capacity/demand/"):
        return "live", "arrival-capacity", "demand", lv[-1]
    # fl/cp/<slug>/... — Continuum data products built live via the generation engine
    if len(lv) >= 5 and lv[0] == "fl" and lv[1] == "cp" and lv[3] == "live":
        return "live", lv[2], "live", lv[-1]
    if len(lv) >= 5 and lv[0] == "fl" and lv[1] == "cp":
        return "trigger", lv[2], lv[3], lv[-1]
    if lv[0] == "solutions" and "decision" in lv[1:]:
        i = lv.index("decision")
        slug = lv[i - 1] if i >= 1 else "?"
        return "decision", slug, "decision", lv[-1]
    if lv[0] == "solutions" and lv[-1] == "triage":
        return "context", "arrival-capacity", "triage", lv[-1]
    return "other", "-", "-", lv[-1]

KIND_STYLE = {
    "trigger":  (C.BYELLOW,  "TRIGGER"),
    "live":     (C.DIM,      "LIVE"),
    "decision": (C.BGREEN,   "DECISION"),
    "context":  (C.BCYAN,    "CONTEXT"),
    "other":    (C.WHITE,    "EVENT"),
}

# ── payload unwrapping ───────────────────────────────────────────────────────
def unwrap(payload: bytes):
    """Yield (op, row) pairs. op in {insert, delete, row, raw}."""
    text = payload.decode("utf-8", "replace").strip()
    if not text:
        return [("raw", {"_raw": ""})]
    chunks = [text] if text[0] in "[{" and "\n" not in text else text.splitlines()
    out = []
    for ch in chunks:
        ch = ch.strip()
        if not ch:
            continue
        try:
            obj = json.loads(ch)
        except json.JSONDecodeError:
            # maybe the whole text was one pretty-printed object
            try:
                obj = json.loads(text)
                chunks = []  # consumed everything
            except json.JSONDecodeError:
                out.append(("raw", {"_raw": ch}))
                continue
        items = obj if isinstance(obj, list) else [obj]
        for it in items:
            if isinstance(it, dict) and it and set(it) <= {"insert", "delete"}:
                for op in ("insert", "delete"):
                    if op in it and isinstance(it[op], dict):
                        out.append((op, it[op]))
            elif isinstance(it, dict):
                out.append(("row", it))
            else:
                out.append(("raw", {"_raw": json.dumps(it)}))
        if not chunks:
            break
    return out or [("raw", {"_raw": text})]

def fmt_val(v, width=60):
    if isinstance(v, (dict, list)):
        s = json.dumps(v, separators=(",", ":"))
    elif isinstance(v, float):
        s = f"{v:,.2f}"
    else:
        s = str(v)
    return s if len(s) <= width else s[: width - 1] + "…"

# ── console ──────────────────────────────────────────────────────────────────
class Console:
    def __init__(self, args):
        self.args = args
        self.raw = args.json
        self.triggers = {}          # (slug, key) -> (t_arrival, topic)
        self.triggers_by_key = {}   # key -> (t_arrival, topic)
        self.counts = {"trigger": 0, "decision": 0, "context": 0, "other": 0, "delete": 0}
        self.latencies = []
        self.width = max(60, min(shutil.get_terminal_size((100, 20)).columns, 140))
        self.t0 = time.time()

    # -- rendering ------------------------------------------------------------
    def card(self, kind, topic, op, row, ts, extra_lines):
        color, label = KIND_STYLE[kind]
        if op == "delete":
            color, label = C.DIM + C.RED, label + " (retraction)"
        stamp = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3]
        head = f"{color}{label:<10}{C.RESET} {C.BOLD}{topic}{C.RESET}"
        pad = self.width - len(label) - 1 - len(topic) - len(stamp) - 4
        line = f"{head}{' ' * max(1, pad)}{C.DIM}{stamp}{C.RESET}"
        print(line)
        keys = [k for k in PRIORITY if k in row] + [k for k in row if k not in PRIORITY and k not in HIDE]
        if "_raw" in row:
            print(f"  {C.DIM}raw:{C.RESET} {fmt_val(row['_raw'], self.width - 8)}")
        # two columns when the terminal allows
        colw = (self.width - 4) // 2
        pairs = [f"{C.CYAN}{k}{C.RESET}={fmt_val(row[k], colw - len(k) - 2)}" for k in keys]
        i = 0
        while i < len(pairs):
            a = pairs[i]; b = pairs[i + 1] if i + 1 < len(pairs) else ""
            visible_a = len(a) - len(C.CYAN) - len(C.RESET)
            if b and visible_a < colw:
                print(f"  {a}{' ' * (colw - visible_a)}{b}")
                i += 2
            else:
                print(f"  {a}")
                i += 1
        for ln in extra_lines:
            print(f"  {ln}")
        print(f"{C.DIM}{'─' * self.width}{C.RESET}")

    # -- message handling -----------------------------------------------------
    def handle(self, topic: str, payload: bytes):
        ts = time.time()
        kind, slug, trig, key = classify(topic)
        if kind == "live" and not INCLUDE_LIVE:
            return                                   # the live changelog is noisy; opt in with --include-live
        for op, row in unwrap(payload):
            if op == "delete":
                self.counts["delete"] += 1
            else:
                self.counts[kind] += 1
            if self.raw:
                print(json.dumps({"ts": datetime.fromtimestamp(ts).isoformat(timespec="milliseconds"),
                                  "topic": topic, "kind": kind, "op": op, "row": row}, default=str), flush=True)
                if kind == "trigger" and op != "delete":
                    self._remember(slug, key, ts, topic)
                elif kind == "decision":
                    lat = self._latency(slug, key, ts)
                    if lat is not None:
                        print(json.dumps({"ts": datetime.fromtimestamp(ts).isoformat(timespec="milliseconds"),
                                          "topic": topic, "kind": "latency", "key": key,
                                          "trigger_to_decision_s": round(lat, 3)}), flush=True)
                continue

            extra = []
            if kind == "trigger" and op != "delete":
                self._remember(slug, key, ts, topic)
                extra.append(f"{C.YELLOW}▶ trigger armed for key {key} (product {slug}, rule {trig}) — waiting for decision{C.RESET}")
            elif kind == "decision":
                lat = self._latency(slug, key, ts)
                if lat is None:
                    extra.append(f"{C.DIM}Δ trigger→decision: no matching trigger seen for key {key}{C.RESET}")
                else:
                    self.latencies.append(lat)
                    src = (self.triggers.get((slug, key)) or self.triggers_by_key.get(key))[1]
                    extra.append(f"{C.BMAGENTA}Δ trigger→decision: {lat:6.2f} s{C.RESET}  {C.DIM}(from {src}){C.RESET}")
            self.card(kind, topic, op, row, ts, extra)
        sys.stdout.flush()

    def _remember(self, slug, key, ts, topic):
        self.triggers[(slug, key)] = (ts, topic)
        self.triggers_by_key[key] = (ts, topic)

    def _latency(self, slug, key, ts):
        hit = self.triggers.get((slug, key)) or self.triggers_by_key.get(key)
        return None if hit is None else ts - hit[0]

    def summary(self):
        up = time.time() - self.t0
        print(f"\n{C.BOLD}session {up:,.0f}s{C.RESET}  triggers={self.counts['trigger']}  "
              f"decisions={self.counts['decision']}  context={self.counts['context']}  "
              f"other={self.counts['other']}  retractions={self.counts['delete']}")
        if self.latencies:
            ls = sorted(self.latencies)
            print(f"trigger→decision latency: n={len(ls)}  min={ls[0]:.2f}s  "
                  f"median={ls[len(ls)//2]:.2f}s  max={ls[-1]:.2f}s")

# ── MQTT wiring (paho 2.x callback API) ──────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.environ.get("MQTT_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    ap.add_argument("--user", default=os.environ.get("SOLACE_USER", "default"))
    ap.add_argument("--password", default=os.environ.get("SOLACE_PASSWORD", "default"))
    ap.add_argument("--topics", nargs="+", default=["fl/faa/arrival-capacity/#", "fl/cp/#", "solutions/#"],
                    help="MQTT subscriptions ('#' multi-level == SMF '>')")
    ap.add_argument("--qos", type=int, default=0, choices=[0, 1])
    ap.add_argument("--client-id", default=f"ops-console-{os.getpid()}")
    ap.add_argument("--json", action="store_true", help="raw NDJSON output (one line per row), no cards")
    ap.add_argument("--include-live", action="store_true",
                    help="also show the fl/cp/<slug>/live/* product changelog (very noisy; hidden by default)")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args()
    globals()["INCLUDE_LIVE"] = args.include_live

    if args.no_color or args.json or os.environ.get("NO_COLOR") or not sys.stdout.isatty():
        _strip_colors()
        # KIND_STYLE captured color codes at import; refresh them
        for k in KIND_STYLE:
            KIND_STYLE[k] = ("", KIND_STYLE[k][1])

    console = Console(args)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code != 0 and str(reason_code) != "Success":
            print(f"{C.BRED}connect failed: {reason_code}{C.RESET}", file=sys.stderr)
            return
        for t in args.topics:
            client.subscribe(t, qos=args.qos)
        if not args.json:
            print(f"{C.BOLD}ops-console{C.RESET} connected mqtt://{args.host}:{args.port}  "
                  f"subscribed: {', '.join(args.topics)}   (Ctrl-C for summary)")
            print(f"{C.DIM}{'─' * console.width}{C.RESET}")

    def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
        if not args.json:
            print(f"{C.YELLOW}disconnected ({reason_code}) — reconnecting{C.RESET}", file=sys.stderr)

    def on_message(client, userdata, msg):
        try:
            console.handle(msg.topic, msg.payload)
        except Exception as e:  # never let a bad payload kill the console on stage
            print(f"{C.BRED}render error on {msg.topic}: {e}{C.RESET}", file=sys.stderr)

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                         client_id=args.client_id, protocol=mqtt.MQTTv311, clean_session=True)
    client.username_pw_set(args.user, args.password)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=10)

    def stop(*_):
        client.disconnect()
        if not args.json:
            console.summary()
        sys.exit(0)
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        client.connect(args.host, args.port, keepalive=30)
    except OSError as e:
        sys.exit(f"cannot reach MQTT broker at {args.host}:{args.port}: {e} (is the stack up? MQTT is host port 1883)")
    client.loop_forever(retry_first_connection=True)

if __name__ == "__main__":
    main()

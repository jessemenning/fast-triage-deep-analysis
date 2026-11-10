#!/usr/bin/env python3
"""
verify-solution.py — headless end-to-end check for a generated solution (no UI).

Subscribes over MQTT to the generated solution's trigger + decision topics, optionally
drives the wind-shift stimulus, and asserts a decision lands — printing the
trigger→decision latency and whether a System-2 brief was produced.

Usage:
  python3 demo/verify-solution.py <slug> --trigger <trigger_name> [--no-drive] [--timeout 150]
Example:
  python3 demo/verify-solution.py wind-arrival-risk --trigger wind-risk
"""
import argparse
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST, MQTT_PORT = "localhost", 1883
REST = "http://localhost:9000/TOPIC/faa/scenario/windshift"


def drive():
    body = b'{"trigger":"windshift","airport":"KEWR"}'
    req = urllib.request.Request(REST, data=body, method="POST",
                                 headers={"Content-Type": "application/octet-stream"})
    import base64
    req.add_header("Authorization", "Basic " + base64.b64encode(b"default:default").decode())
    try:
        urllib.request.urlopen(req, timeout=10).read()
        print("  drove wind shift → faa/scenario/windshift")
    except Exception as e:
        print(f"  drive failed: {e}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--trigger", required=True)
    ap.add_argument("--no-drive", action="store_true")
    ap.add_argument("--timeout", type=int, default=150)
    args = ap.parse_args()
    slug, trig = args.slug, args.trigger

    trig_topic = f"fl/cp/{slug}/{trig}/#"
    dec_topic = f"solutions/{slug}/decision/#"
    tri_topic = f"solutions/{slug}/triage"
    state = {"trigger_ts": None, "decision": None, "decision_ts": None, "triage": None}

    def on_connect(c, *_):
        for t in (trig_topic, dec_topic, tri_topic):
            c.subscribe(t, qos=0)
        print(f"  subscribed: {trig_topic} · {dec_topic} · {tri_topic}")

    def on_message(c, u, msg):
        now = time.time()
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8", "replace"))
        except Exception:
            payload = {"_raw": msg.payload[:120].decode("utf-8", "replace")}
        if isinstance(payload, dict) and "insert" in payload:
            payload = payload["insert"]
        if topic.startswith(f"fl/cp/{slug}/{trig}/"):
            if state["trigger_ts"] is None:
                state["trigger_ts"] = now
                print(f"  TRIGGER  {topic}  airport={payload.get('airport')} demand={payload.get('demand')} cap={payload.get('bin_capacity')} wx={payload.get('flight_category')}")
        elif topic == tri_topic and state["triage"] is None:
            state["triage"] = payload
            print(f"  TRIAGE   action={payload.get('action')} impact={payload.get('impact')}")
        elif topic.startswith(f"solutions/{slug}/decision/") and state["decision"] is None:
            state["decision"] = payload
            state["decision_ts"] = now

    cl = mqtt.Client(client_id=f"verify-{slug}-{int(time.time())}")
    cl.username_pw_set("default", "default")
    cl.on_connect = on_connect
    cl.on_message = on_message
    cl.connect(MQTT_HOST, MQTT_PORT, 30)
    cl.loop_start()
    time.sleep(2)

    if not args.no_drive:
        drive()

    print(f"  waiting up to {args.timeout}s for a decision on solutions/{slug}/decision/… …")
    deadline = time.time() + args.timeout
    while time.time() < deadline and state["decision"] is None:
        time.sleep(1)
    cl.loop_stop()

    d = state["decision"]
    if not d:
        print(f"\n✗ NO decision on solutions/{slug}/decision/… within {args.timeout}s", file=sys.stderr)
        return 1
    lat = (state["decision_ts"] - state["trigger_ts"]) if state["trigger_ts"] else None
    brief = d.get("brief")
    print(f"\n✓ DECISION on solutions/{slug}/decision/…")
    print(f"    action        : {d.get('action')}")
    print(f"    impact        : {d.get('impact')}  will_materialize={d.get('will_materialize')}  self_resolves={d.get('self_resolves')}")
    print(f"    decided_by    : {d.get('decided_by')}")
    print(f"    brief         : {'present ('+str(brief.get('headline'))+')' if isinstance(brief,dict) else ('present' if brief else 'none (not escalated)')}")
    if lat is not None:
        print(f"    trigger→decision: {lat:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

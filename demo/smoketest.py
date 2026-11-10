#!/usr/bin/env python3
"""
smoketest.py — end-to-end readiness check for the FAA Fast/Slow demo.

Verifies the whole spine is live and record-ready before a demo recording:
  services healthy  ->  base pipeline Running  ->  wind-shift scenario fires
  ->  Continuum incident trigger (the collapsed one-per-airport row)
  ->  SAM fast triage (System 1)  ->  decision  ->  System 2 brief populated.

It drives the SAME path the hero UI's "Wind shift" button uses
(POST /api/faa/scenario/start?airport=KEWR), and listens on the broker for the
breach storm, the collapsed incident trigger, the triage, and the decision.

Hard-won gotchas this encodes (see CLAUDE.md):
  * The gateway subscribes the COLLAPSED incident trigger
    (fl/faa/arrival-capacity/incident/#), NOT the per-bin breach storm. The
    incident row is published once per airport on entry to a SEVERE incident
    (set-membership edge semantics) — so a re-fire while an airport is still
    latched in-incident does NOT re-trigger until it recovers. This script
    therefore RESETS the hero airport to green first by default, so the fire
    produces a clean new incident edge. Use --no-reset to skip.
  * MQTT creds are default/default (same as demo.html's MQTT-over-WS); MQTT
    wildcards are '#'/'+', NOT the SMF '>'/'*' used inside SAM.
  * The AI toggle (POST /api/faa/agent-state) must be ON or no decision fires.
  * After a broker restart, SAM must be restarted too or it won't consume the
    trigger (stale connection) — this script hints that if no decision lands.

Usage:
  python3 demo/smoketest.py                  # full check, KEWR wind shift
  python3 demo/smoketest.py --no-reset       # skip the pre-fire reset-to-green
  python3 demo/smoketest.py --airport KORD   # different hero airport
  python3 demo/smoketest.py --timeout 180    # wait longer for the slow brief
Env overrides: API_BASE, MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS.

Requires paho-mqtt (`pip install 'paho-mqtt>=2'`). Exit 0 = record-ready.
"""
import argparse, json, os, sys, time, urllib.request

API   = os.environ.get("API_BASE", "http://localhost:8080")
MHOST = os.environ.get("MQTT_HOST", "localhost")
MPORT = int(os.environ.get("MQTT_PORT", "1883"))
MUSER = os.environ.get("MQTT_USER", "default")
MPASS = os.environ.get("MQTT_PASS", "default")
FELDERA = os.environ.get("FELDERA_BASE", "http://localhost:8081")
PIPELINE = "arrival-capacity"

# MQTT topics (MQTT wildcards: '#' multi-level). The gateway trigger is the
# COLLAPSED incident row, not the per-bin breach storm.
BREACH_T   = "fl/faa/arrival-capacity/breach/#"      # per-bin alert storm (informational)
INCIDENT_T = "fl/faa/arrival-capacity/incident/#"    # the SAM escalation trigger
TRIAGE_T   = "solutions/arrival-capacity/triage"
DECISION_T = "solutions/arrival-capacity/decision/#"

GREEN, RED, YEL, DIM, RST = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"
def ok(m):   print(f"{GREEN}  PASS{RST} {m}")
def bad(m):  print(f"{RED}  FAIL{RST} {m}")
def warn(m): print(f"{YEL}  WARN{RST} {m}")
def step(m): print(f"\n{m}")

def http_code(url, timeout=5):
    try:
        return urllib.request.urlopen(url, timeout=timeout).status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0

def http_json(url, timeout=5):
    try:
        return json.loads(urllib.request.urlopen(url, timeout=timeout).read().decode())
    except Exception:
        return None

def post(path, payload=None, timeout=10):
    body = json.dumps(payload).encode() if payload is not None else b""
    req = urllib.request.Request(API + path, data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    return urllib.request.urlopen(req, timeout=timeout).read().decode()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--airport", default="KEWR", help="hero airport to hit with the wind shift (default KEWR)")
    ap.add_argument("--timeout", type=int, default=150, help="seconds to wait for the decision")
    ap.add_argument("--no-reset", action="store_true",
                    help="skip the pre-fire reset-to-green (incident may already be latched)")
    ap.add_argument("--reset-wait", type=int, default=15,
                    help="max seconds to wait for the incident-state CLEAR edge after reset")
    args = ap.parse_args()
    apt = args.airport
    failures = []

    # 1. service health -------------------------------------------------------
    step("1. Service health")
    for name, url in [("SAM :8800",       "http://localhost:8800/healthz"),
                      ("engine :8081",    f"{FELDERA}/"),
                      ("hero UI :8080",   f"{API}/demo.html"),
                      ("API :8080",       f"{API}/api/faa/health"),
                      ("broker SEMP :8089","http://localhost:8089/")]:
        c = http_code(url)
        (ok if c == 200 else bad)(f"{name} -> HTTP {c}")
        if c != 200:
            failures.append(name)

    # 1b. base pipeline Running ------------------------------------------------
    ps = http_json(f"{FELDERA}/v0/pipelines/{PIPELINE}")
    if ps and ps.get("deployment_status") == "Running" and ps.get("program_status") == "Success":
        ok(f"pipeline '{PIPELINE}' -> program Success / deployment Running")
    else:
        ds = (ps or {}).get("deployment_status"); prog = (ps or {}).get("program_status")
        bad(f"pipeline '{PIPELINE}' -> program {prog} / deployment {ds} (expected Success/Running)")
        failures.append("pipeline")

    # 2. paho + subscriber ----------------------------------------------------
    step("2. Broker subscription")
    try:
        import paho.mqtt.client as mqtt
    except ImportError:
        bad("paho-mqtt not installed -> pip install 'paho-mqtt>=2'")
        sys.exit(2)

    seen = {"breach": 0, "incident": None, "triage": None, "decision": None}
    def on_connect(c, u, flags, rc, props=None):
        c.subscribe([(BREACH_T, 0), (INCIDENT_T, 0), (TRIAGE_T, 0), (DECISION_T, 0)])
    def on_message(c, u, m):
        t = m.topic
        try: p = json.loads(m.payload.decode())
        except Exception: p = {}
        if isinstance(p, dict) and "insert" in p:   # Feldera change-stream envelope
            p = p["insert"]
        if "/breach/" in t:
            seen["breach"] += 1
        elif "/incident/" in t and seen["incident"] is None:
            seen["incident"] = (time.time(), p)
        elif t == TRIAGE_T and seen["triage"] is None:
            seen["triage"] = (time.time(), p)
        elif "/decision/" in t and seen["decision"] is None:
            seen["decision"] = (time.time(), p)
    cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    cli.username_pw_set(MUSER, MPASS)
    cli.on_connect = on_connect
    cli.on_message = on_message
    try:
        cli.connect(MHOST, MPORT, 30)
    except Exception as e:
        bad(f"MQTT connect failed ({e}) — is the broker up? creds {MUSER}/***")
        sys.exit(2)
    cli.loop_start()
    time.sleep(2)
    ok(f"subscribed on {MHOST}:{MPORT} (breach, incident, triage, decision)")

    # 3. AI toggle ------------------------------------------------------------
    step("3. AI agent toggle")
    try:
        st = json.loads(post("/api/faa/agent-state", {"enabled": True}))
        ok(f"AI agent {'ON' if st.get('enabled') else '??'}")
    except Exception as e:
        warn(f"could not set agent-state: {e}")

    # 4. reset hero airport to green (clean incident edge) --------------------
    if not args.no_reset:
        step(f"4. Reset {apt} to green (waits for the incident-state CLEAR edge)")
        j = {}
        try:
            # The API blocks until the data plane confirms CLEAR (via the incident-state
            # feed's retained status, seeded at startup AND updated by every edge), then
            # returns green:true. This is robust even when the airport is already green
            # (no new edge to observe) — unlike waiting for a live broker edge.
            resp = post(f"/api/faa/scenario/reset?airport={apt}&timeout={args.reset_wait}",
                        timeout=args.reset_wait + 8)
            j = json.loads(resp) if resp else {}
        except Exception as e:
            warn(f"reset request failed ({e}); continuing — incident may already be latched")
        if j.get("green"):
            ok(f"{apt} confirmed CLEAR/green in {j.get('waited_ms', '?')}ms before firing")
        else:
            warn(f"{apt} not confirmed green (status={j.get('status')}); firing anyway")

    # 5. fire the wind shift --------------------------------------------------
    step(f"5. Fire wind shift on {apt} (runway reconfig -> AAR degrade + GDP + arrival burst)")
    t0 = time.time()
    try:
        resp = json.loads(post(f"/api/faa/scenario/start?airport={apt}"))
        ok(f"scenario fired for {resp.get('airport', apt)}")
    except Exception as e:
        bad(f"scenario start failed: {e}"); failures.append("scenario")

    # 6. await the pipeline ---------------------------------------------------
    step(f"6. Awaiting incident -> triage -> decision (up to {args.timeout}s)")
    deadline = t0 + args.timeout
    reported = set()
    while time.time() < deadline and "decision" not in reported:
        if seen["incident"] and "incident" not in reported:
            ts, p = seen["incident"]
            ok(f"incident trigger (+{ts-t0:.1f}s)  {p.get('airport', apt)} / "
               f"severity {p.get('severity') or p.get('severity_tier')} "
               f"(after {seen['breach']} per-bin breach msgs — the storm collapsed to 1)")
            reported.add("incident")
        if seen["triage"] and "triage" not in reported:
            ts, p = seen["triage"]
            ok(f"triage / System 1 (+{ts-t0:.1f}s)  action={p.get('action')} "
               f"impact={p.get('impact')} priority={p.get('case_priority')}")
            reported.add("triage")
        if seen["decision"] and "decision" not in reported:
            ts, p = seen["decision"]
            brief = p.get("brief")
            ok(f"decision (+{ts-t0:.1f}s)  action={p.get('action')} "
               f"will_materialize={p.get('will_materialize')} decided_by={p.get('decided_by')}")
            if isinstance(brief, dict):
                ok(f"System 2 brief: \"{brief.get('headline')}\"")
            elif p.get("action") == "ESCALATE":
                warn("System 2 brief: null on an ESCALATE — briefer likely timed out; "
                     "warm up and retry a take before recording")
            else:
                print(f"{DIM}  (no brief — triage did not ESCALATE this run){RST}")
            reported.add("decision")
        time.sleep(1)
    cli.loop_stop()

    # 7. verdict --------------------------------------------------------------
    step("Result")
    if "incident" not in reported:
        bad(f"no Continuum incident trigger — {apt} not latched to SEVERE? "
            "check the feed/scenario, or it was already in-incident (run without --no-reset)")
        failures.append("incident")
    if "decision" not in reported:
        bad("no SAM decision — if the incident fired, SAM likely needs: "
            "docker compose -f docker-compose.yml -f docker-compose.cache.yml restart sam")
        failures.append("decision")

    if failures:
        print(f"\n{RED}NOT record-ready{RST} — issues: {', '.join(failures)}")
        sys.exit(1)
    print(f"\n{GREEN}RECORD-READY{RST} — full fast/slow path verified end to end.")
    if seen["decision"] and seen["decision"][1].get("action") == "ESCALATE" \
            and not isinstance(seen["decision"][1].get("brief"), dict):
        print(f"{DIM}(brief came back null this run — fire one more warm-up before recording){RST}")
    sys.exit(0)

if __name__ == "__main__":
    main()

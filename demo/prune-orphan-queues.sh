#!/usr/bin/env bash
# prune-orphan-queues.sh — list (and optionally delete) orphaned Solace Agent
# Mesh event-mesh queues on the local Solace Platform broker.
#
# Every SAM event-mesh entrypoint auto-provisions a queue named
#   solace-agent-mesh/q/eventmesh/<...>
# When SAM is restarted with a new instance id the old queue stays behind with
# zero bound consumers but its topic subscriptions intact, so trigger events
# fan out into a queue nobody reads (and it fills its spool). Before a demo,
# list them and delete the ones with no consumers.
#
# SEMP v2 (host port 8089 -> broker 8080). Credentials come from the
# environment — SOLACE_ADMIN_USER (default admin) / SOLACE_ADMIN_PASSWORD
# (default admin; the variable name matches the repo .env).
# Queue names contain '/', which must be URL-encoded as %2F in the path segment;
# the "where" filter is sent with --data-urlencode for the same reason.
#
#   monitor: GET /SEMP/v2/monitor/msgVpns/{vpn}/queues?where=queueName==<pattern>
#            GET /SEMP/v2/monitor/msgVpns/{vpn}/queues/{name%2Fencoded}/txFlows   (bound consumers)
#   config : DELETE /SEMP/v2/config/msgVpns/{vpn}/queues/{name%2Fencoded}
#
# Usage: prune-orphan-queues.sh [--dry-run] [--yes] [--pattern 'solace-agent-mesh/q/eventmesh/*']
#                               [--host localhost] [--port 8089] [--vpn default]
# Default is --dry-run (list only). --yes deletes queues with zero tx flows.
set -euo pipefail

HOST="${SEMP_HOST:-localhost}"
PORT="${SEMP_PORT:-8089}"
VPN="${SOLACE_VPN:-default}"
PATTERN="solace-agent-mesh/q/eventmesh/*"
DO_DELETE=0

usage() { sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DO_DELETE=0 ;;
    --yes)     DO_DELETE=1 ;;
    --pattern) PATTERN="$2"; shift ;;
    --host)    HOST="$2"; shift ;;
    --port)    PORT="$2"; shift ;;
    --vpn)     VPN="$2"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
  shift
done

export SEMP_BASE="http://${HOST}:${PORT}/SEMP/v2"
export SEMP_VPN="$VPN" QUEUE_PATTERN="$PATTERN" DO_DELETE
export SEMP_USER="${SOLACE_ADMIN_USER:-admin}"
export SEMP_PASS="${SOLACE_ADMIN_PASSWORD:-admin}"

python3 - <<'PY'
import base64, json, os, sys, urllib.parse, urllib.request, urllib.error

base, vpn = os.environ["SEMP_BASE"], os.environ["SEMP_VPN"]
pattern, do_delete = os.environ["QUEUE_PATTERN"], os.environ["DO_DELETE"] == "1"
auth = base64.b64encode(f"{os.environ['SEMP_USER']}:{os.environ['SEMP_PASS']}".encode()).decode()
hdr = {"Authorization": f"Basic {auth}", "Content-Type": "application/json"}

def call(method, url):
    req = urllib.request.Request(url, headers=hdr, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        sys.exit(f"SEMP {method} {url} -> HTTP {e.code}: {body[:400]}")
    except urllib.error.URLError as e:
        sys.exit(f"cannot reach SEMP at {base}: {e.reason} (host port 8089 -> broker 8080; is the stack up?)")

def enc(name):  # '/' -> %2F for path segments
    return urllib.parse.quote(name, safe="")

# 1. list queues matching the pattern (SEMP 'where' supports '*' wildcards), follow paging
where = urllib.parse.urlencode({"where": f"queueName=={pattern}", "count": "100"})
url = f"{base}/monitor/msgVpns/{vpn}/queues?{where}"
queues = []
while url:
    page = call("GET", url)
    queues.extend(page.get("data", []))
    url = (page.get("meta", {}).get("paging") or {}).get("nextPageUri")

queues = [q for q in queues if not q["queueName"].startswith("#")]
mode = "DELETE zero-consumer queues" if do_delete else "dry-run (list only; pass --yes to delete)"
print(f"VPN {vpn}  pattern {pattern}  -> {len(queues)} queue(s)   [{mode}]")
if not queues:
    sys.exit(0)

# 2. bound consumers = number of tx flows on the queue
rows = []
for q in queues:
    name = q["queueName"]
    flows = call("GET", f"{base}/monitor/msgVpns/{vpn}/queues/{enc(name)}/txFlows")
    n_flows = len(flows.get("data", []))
    rows.append((name, n_flows, q.get("spooledMsgCount", 0), q.get("msgSpoolUsage", 0)))

w = max(len(r[0]) for r in rows)
print(f"{'queue':<{w}}  txFlows  spooledMsgs  spoolMB")
for name, n, msgs, mb in rows:
    flag = "" if n > 0 else "   <- ORPHAN"
    print(f"{name:<{w}}  {n:>7}  {msgs:>11}  {mb:>7}{flag}")

orphans = [r for r in rows if r[1] == 0]
print(f"\n{len(orphans)} orphan(s) with zero bound consumers")
if not orphans:
    sys.exit(0)
if not do_delete:
    print("re-run with --yes to delete them")
    sys.exit(0)

# 3. delete
for name, *_ in orphans:
    call("DELETE", f"{base}/config/msgVpns/{vpn}/queues/{enc(name)}")
    print(f"deleted {name}")
print("done")
PY

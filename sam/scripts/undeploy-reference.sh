#!/usr/bin/env bash
# Undeploy the reference arrival-capacity decision loop from the running
# platform (e.g. to redeploy it fresh — `make reset` deletes the product and
# re-applies this solution).
#
# Removes ONLY the components listed in solutions/arrival-capacity/manifests/dev.yaml
# (entrypoint, workflow, the agents) — never the Orchestrator or other platform
# agents. Deletes in teardown order (entrypoint -> workflow -> agents) so the
# gateway stops consuming the trigger before its workflow/agents disappear. Idempotent:
# a component that is already gone is skipped.
#
# Delete is `DELETE /api/v1/platform/{agents|workflows|entrypoints}/{id}` (204); ids are
# assigned per deploy, so names are resolved to ids at run time.
#
# Usage:  scripts/undeploy-reference.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f ../.env ]]; then set -a; . ../.env; set +a; fi
if [[ -f .env ]]; then set -a; . ./.env; set +a; fi

URL="${SAM_WEBUI_URL:-http://127.0.0.1:8800}"
TOKEN="${SAM_AUTH_TOKEN:-${SAM_PLATFORM_TOKEN:-}}"
MANIFEST="${1:-solutions/arrival-capacity/manifests/dev.yaml}"

python3 - "$URL" "$TOKEN" "$MANIFEST" <<'PY'
import sys, json, urllib.request, urllib.error
import yaml

base, token, manifest = sys.argv[1], sys.argv[2], sys.argv[3]
res = (yaml.safe_load(open(manifest)) or {}).get("resources", {})
# teardown order: entrypoints first (stop consuming), then workflows, then agents
plan = [("entrypoints", res.get("entrypoints") or []),
        ("workflows",   res.get("workflows")   or []),
        ("agents",      res.get("agents")      or [])]

def api(method, path):
    req = urllib.request.Request(base + path, method=method)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode() or "[]"
            return r.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()

def ids_by_name(kind):
    status, body = api("GET", f"/api/v1/platform/{kind}")
    if status != 200:
        print(f"  !! list {kind} -> {status}"); return {}
    d = json.loads(body)
    items = d if isinstance(d, list) else (d.get("data") or d.get("items") or [])
    return {it.get("name"): it.get("id") for it in items if isinstance(it, dict)}

any_deleted = False
for kind, names in plan:
    if not names:
        continue
    live = ids_by_name(kind)
    for name in names:
        cid = live.get(name)
        if not cid:
            print(f"  - {kind}/{name}: already gone")
            continue
        status, _ = api("DELETE", f"/api/v1/platform/{kind}/{cid}")
        ok = status in (200, 202, 204)
        any_deleted = any_deleted or ok
        print(f"  {'x' if ok else '!!'} {kind}/{name} ({cid}) -> {status}")

print("reference loop cleared." if any_deleted else "nothing to clear (loop already absent).")
PY

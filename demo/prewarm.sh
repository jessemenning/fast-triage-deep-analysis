#!/usr/bin/env bash
# prewarm.sh — arm the "warm path" so a LIVE product create in the demo is a
# cache hit (seconds) instead of a cold Feldera compile (~7–8 min).
#
# For each demo product spec it: creates the product (compiles the SQL into the
# PERSISTENT Feldera compile cache), then deletes the product record — keeping the
# cached binary but clearing the record so the demo can "create it live" from a
# clean slate. The live create of the identical SQL then reuses the cached binary.
#
# The compile cache persists across down/up and host reboots (it lives in the
# feldera_home/feldera_cargo volumes, mounted by default — see docker-compose.cache.yml),
# so you pay the cold compile ONCE per machine. Re-run only after `make clean`.
# Fast (~15–40 s/product) when the cache is already warm — safe to run anytime.
#
# Measured (verified): cold first compile 468 s; recreate of identical spec 37 s
# (compile skipped — cache hit). See .claude/skills/faa-generate-solution/SKILL.md.
#
# Usage: demo/prewarm.sh [spec.json ...]   (default: all demo/products/*.json)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT"
# shellcheck disable=SC1091
set -a; [[ -f .env ]] && . ./.env; set +a
export CONTINUUM_MCP_TOKEN="${CONTINUUM_MCP_TOKEN:-continuum-dev-token-local}"
export CONTINUUM_MCP_LOCAL_URL="${CONTINUUM_MCP_LOCAL_URL:-http://localhost:8091/mcp}"
KEEP_RUNNING="${KEEP_RUNNING:-0}"   # set 1 to leave products running instead of deleting the record

specs=("$@")
if [[ ${#specs[@]} -eq 0 ]]; then
  shopt -s nullglob; specs=(demo/products/*.json); shopt -u nullglob
fi
[[ ${#specs[@]} -gt 0 ]] || { echo "!! no product specs found in demo/products/ (nothing to prewarm)"; exit 1; }

# continuum-mcp must be reachable — the server answering with ANY HTTP status (even 4xx)
# means it's up; only a failed TCP connect (code 000) is a real "not running".
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "$CONTINUUM_MCP_LOCAL_URL" \
         -H "Authorization: Bearer $CONTINUUM_MCP_TOKEN" -H 'Content-Type: application/json' \
         --data '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' 2>/dev/null || echo 000)"
[[ "$code" != "000" ]] || { echo "!! continuum-mcp not reachable on :8091 — bring the stack up first (make up)"; exit 1; }

delete_record () {  # $1 = product name
  python3 - "$1" <<'PY'
import json, os, sys, urllib.request
name = sys.argv[1]
body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                   "params": {"name": "delete_data_product",
                              "arguments": {"name": name, "drop_queues": True}}}).encode()
req = urllib.request.Request(os.environ["CONTINUUM_MCP_LOCAL_URL"], data=body, headers={
    "Authorization": f"Bearer {os.environ['CONTINUUM_MCP_TOKEN']}",
    "Accept": "application/json", "Content-Type": "application/json"})
urllib.request.urlopen(req, timeout=60).read()
print(f"    deleted product record '{name}' (compile cache kept)")
PY
}

echo "prewarm: arming warm path for ${#specs[@]} product(s) — $(date -u +%FT%TZ)"
for spec in "${specs[@]}"; do
  [[ -f "$spec" ]] || { echo "  skip (missing): $spec"; continue; }
  name="$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['name'])" "$spec")"
  echo "==> $name  ($spec)"
  t0=$(date +%s)
  # --recreate: clear any stale record first, then compile+run (populates the cache)
  python3 demo/continuum-create-product.py "$spec" --recreate
  echo "    ready in $(( $(date +%s) - t0 ))s"
  if [[ "$KEEP_RUNNING" = "1" ]]; then
    echo "    (KEEP_RUNNING=1 — leaving '$name' running)"
  else
    delete_record "$name"
  fi
done
echo "warm-path armed: $(date -u +%FT%TZ)" | tee .warm-path
echo "Live creates of these specs are now cache hits (seconds). Cache persists across down/up + reboot;"
echo "re-run 'make prewarm' only after 'make clean'."

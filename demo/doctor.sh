#!/usr/bin/env bash
# doctor.sh — pre-flight for the demo. Prints ✓/✗ for each prerequisite with the exact
# fix, so a colleague can get to `make up` fast. Read-only; never changes anything.
set -u
cd "$(dirname "$0")/.." || exit 1

SAM_IMAGE_DEFAULT="solace-agent-mesh:2.409.2"
SAM_CLI="${SAM:-sam-2.409.2}"
ok=0; warn=0; bad=0
pass() { printf '  \033[32m✓\033[0m %s\n' "$1"; ok=$((ok+1)); }
note() { printf '  \033[33m!\033[0m %s\n     \033[2m%s\033[0m\n' "$1" "$2"; warn=$((warn+1)); }
fail() { printf '  \033[31m✗\033[0m %s\n     \033[2m%s\033[0m\n' "$1" "$2"; bad=$((bad+1)); }

echo "demo pre-flight (make doctor)"
echo

# 1. Docker
if docker info >/dev/null 2>&1; then pass "Docker daemon is running"
else fail "Docker daemon not reachable" "start Docker Desktop / dockerd, then re-run"; fi

# 2. .env
if [ -f .env ]; then pass ".env present"
else fail ".env missing" "cp .env.example .env  (then set LLM_SERVICE_ENDPOINT + LLM_SERVICE_API_KEY)"; fi

# 3. SAM runtime image
IMG="$SAM_IMAGE_DEFAULT"
[ -f .env ] && IMG="$(grep -E '^SAM_IMAGE=' .env | cut -d= -f2- | tr -d ' ')"
IMG="${IMG:-$SAM_IMAGE_DEFAULT}"
if docker image inspect "$IMG" >/dev/null 2>&1; then pass "SAM image present ($IMG)"
else fail "SAM image '$IMG' not loaded" "make pull-image   (downloads the official prebuilt release)"; fi

# 4. host SAM CLI (needed for apply / apply-reference)
if command -v "$SAM_CLI" >/dev/null 2>&1; then pass "host SAM CLI on PATH ($SAM_CLI -> $($SAM_CLI --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1))"
else fail "host SAM CLI '$SAM_CLI' not on PATH" "install the release CLI asset to ~/.local/bin/$SAM_CLI (see CLAUDE.md External)"; fi

# 5. required / optional keys
if [ -f .env ]; then
  grep -qE '^LLM_SERVICE_API_KEY=.+' .env && pass "LLM_SERVICE_API_KEY set (fast decision node + System-2 investigation)" \
    || fail "LLM_SERVICE_API_KEY empty" "set it in .env — powers both the LiteLLM /v1/decisions fast triage and the System-2 agents"
fi

# 6. tooling
command -v python3 >/dev/null 2>&1 && pass "python3 present" || fail "python3 missing" "install Python 3"
python3 -c 'import paho.mqtt' 2>/dev/null && pass "paho-mqtt present (ops-console / create-product)" \
  || note "paho-mqtt not importable" "pip install paho-mqtt  (only for demo/ops tooling)"
command -v gh >/dev/null 2>&1 && pass "gh CLI present (make pull-image Path A)" \
  || note "gh CLI not found" "install gh + 'gh auth login' for make pull-image, or use the shared-registry path"

# 7. stack up? (informational)
if curl -fs -m 3 http://127.0.0.1:8800/healthz >/dev/null 2>&1; then
  pass "SAM platform is up on http://localhost:8800"
else
  note "SAM platform not up yet" "run: make up"
fi

echo
printf 'summary: \033[32m%d ok\033[0m, \033[33m%d warn\033[0m, \033[31m%d blocking\033[0m\n' "$ok" "$warn" "$bad"
[ "$bad" -eq 0 ] && echo "→ good to go: make up" || echo "→ fix the ✗ item(s) above, then: make up"
exit 0

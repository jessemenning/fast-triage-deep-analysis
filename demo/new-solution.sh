#!/usr/bin/env bash
# new-solution.sh — one-command deploy+wire+verify of an already-authored generated solution.
#
# Speedup #2 (+#3 overlap): given an authored product spec (demo/products/<slug>.json) and an
# authored SAM solution (sam/solutions/<slug>/), this:
#   1. starts the Continuum product compile in the BACKGROUND, and
#   2. applies the SAM solution in the FOREGROUND at the same time (the gateway can deploy
#      before the product is running — it just subscribes a topic), then
#   3. waits for the product to reach running, 4. registers Event Portal, 5. verifies headless.
# Authoring (the product SQL + the SAM detection prompt) is the creative step done by the
# faa-generate-solution skill; this script is the fast mechanical deploy path.
#
# Usage: demo/new-solution.sh <slug> <trigger> [key]
set -euo pipefail

SLUG="${1:?usage: new-solution.sh <slug> <trigger> [key]}"
TRIGGER="${2:?usage: new-solution.sh <slug> <trigger> [key]}"
KEY="${3:-airport}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SPEC="demo/products/${SLUG}.json"
SOLDIR="sam/solutions/${SLUG}"
[[ -f "$SPEC"   ]] || { echo "!! missing product spec $SPEC (author it first / via the skill)"; exit 1; }
[[ -d "$SOLDIR" ]] || { echo "!! missing SAM solution $SOLDIR (author it first / via the skill)"; exit 1; }

# shellcheck disable=SC1091
set -a; [[ -f .env ]] && . ./.env; set +a
export CONTINUUM_MCP_TOKEN="${CONTINUUM_MCP_TOKEN:-continuum-dev-token-local}"
export SAM_FEATURE_WORKFLOW_DECISION_NODE=true
SAM_BIN="${SAM:-sam-2.403.3}"

echo "==> [1/5] Continuum product compile (background) + [2/5] SAM apply (foreground) — overlapped"
python3 demo/continuum-create-product.py "$SPEC" > "/tmp/new-solution-${SLUG}-product.log" 2>&1 &
PROD_PID=$!

( cd sam && "$SAM_BIN" config apply --manifest "solutions/${SLUG}/manifests/dev.yaml" ) \
  2>&1 | tail -6
echo "   SAM solution applied."

echo "==> [3/5] waiting for the product pipeline to finish compiling + start ..."
wait "$PROD_PID" && echo "   product running." || { echo "!! product create failed:"; tail -15 "/tmp/new-solution-${SLUG}-product.log"; exit 1; }

echo "==> [4/5] registering Event Portal (SAM app + decision events) ..."
make register-ep-solution SLUG="$SLUG" TRIGGER="$TRIGGER" KEY="$KEY"

echo "==> [5/5] verifying end-to-end (headless) ..."
python3 demo/verify-solution.py "$SLUG" --trigger "$TRIGGER"

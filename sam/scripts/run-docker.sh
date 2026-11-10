#!/usr/bin/env bash
# Run the Solace-enabled SAM platform (2.360.11 via SAM_IMAGE; 2.215.9 fallback) from the
# container image, on the host network.
#
# Why a container: the host `sam` CLI binary is NOT a Solace-enabled build — it refuses to
# deploy an event-mesh entrypoint against a real broker ("can only run an event mesh
# entrypoint in dev mode"). The image (same version) is the Solace-enabled build the FAA
# demo uses. The host CLI stays the client: `sam config plan|apply`, `sam task send`.
#
# Host networking means the container reaches the data-plane broker (localhost:55555), the
# Continuum MCP (:8091) and the Event Portal MCP (:8092) exactly like a host process, and
# serves the web UI + platform API on :8800. Stop any host `sam run --embedded` first.
#
# Env comes from the root .env (KEY=VALUE, no substitution). LLM_SERVICE_* are mirrored into
# OPENAI_API_KEY / OPENAI_API_BASE because the model layer reads those directly.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMG="${SAM_IMAGE:-solace-agent-mesh:2.409.2}"
NAME="${SAM_CONTAINER:-rtai-sam}"
DATA="${SAM_DOCKER_DATA:-$ROOT/.sam-docker-data}"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"

[ -f "$ENV_FILE" ] || { echo "missing $ENV_FILE (copy sam/.env.template to .env)" >&2; exit 1; }
mkdir -p "$DATA" && chmod 0777 "$DATA"          # the image runs as uid 10001 (samuser)

envval() { grep -E "^$1=" "$ENV_FILE" | head -1 | cut -d= -f2-; }

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --name "$NAME" --network host \
  --env-file "$ENV_FILE" \
  -e HOME=/tmp/sam-data \
  -e SAM_DATA_DIR=/tmp/sam-data \
  -e PLATFORM_DATABASE_URL=sqlite:////tmp/sam-data/platform.db \
  -e OPENAI_API_KEY="$(envval LLM_SERVICE_API_KEY)" \
  -e OPENAI_API_BASE="$(envval LLM_SERVICE_ENDPOINT)" \
  -v "$DATA:/tmp/sam-data" \
  "$IMG" \
  run --embedded --system-env --no-onboarding -- \
    /etc/sam/configs/gwe/gwe.yaml \
    /etc/sam/configs/gwe/platform.yaml \
    /etc/sam/configs/str/str.yaml \
    /etc/sam/configs/awe/sam.yaml >/dev/null

echo "container $NAME started; waiting for http://127.0.0.1:8800/healthz ..."
for _ in $(seq 1 60); do
  if curl -fs -m 2 http://127.0.0.1:8800/healthz >/dev/null 2>&1; then
    echo "SAM ready: http://127.0.0.1:8800   (logs: docker logs -f $NAME)"
    exit 0
  fi
  sleep 2
done
echo "SAM did not become healthy — docker logs $NAME" >&2
exit 1

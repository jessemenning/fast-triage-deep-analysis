#!/usr/bin/env bash
# NOTE: the host `sam` binary is not a Solace-enabled build — it cannot run event-mesh entrypoints against a real
# broker (422 "dev mode only"). Use scripts/run-docker.sh for the full demo; this script is for agent-only dev.
# Start a dedicated embedded Solace Agent Mesh platform for the
# realtime-ai-platform demo.
#
# `sam run --embedded` boots the whole platform in-process (platform + agent
# runtime + gateway/entrypoint proxy on :8800, config REST API, in-memory dev
# broker). It does NOT take resource paths — the declarative models /
# connectors / skills / agent are pushed in afterwards with
# `scripts/apply.sh` (sam config apply).
#
# Environment: if sam/.env exists it is sourced and exported so the platform
# and later `sam config` calls see the same values. Copy .env.template to .env
# and fill it in first. We deliberately do NOT pass --system-env, so `sam`'s
# own .env auto-detection still works when this script is not used.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
else
  echo "warn: sam/.env not found — starting with whatever is in the environment (copy .env.template to .env)" >&2
fi

LISTEN="${SAM_LISTEN:-:8800}"
HEALTH="${SAM_HEALTH_ADDR:-:8810}"

exec sam run --embedded --no-onboarding \
  --listen "$LISTEN" --health-addr "$HEALTH" "$@"

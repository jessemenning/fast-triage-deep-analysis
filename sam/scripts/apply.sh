#!/usr/bin/env bash
# Plan, then apply, the dev manifest against the running embedded platform.
#
# Usage:  scripts/apply.sh                 # plan + apply manifests/dev.yaml
#         scripts/apply.sh --plan-only     # plan only
#         scripts/apply.sh -m manifests/other.yaml [--prune]
#
# Requires the platform from scripts/run-embedded.sh to be up on :8800.
# Never pass --prune unless the manifest is the COMPLETE desired state.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

MANIFEST="manifests/dev.yaml"
PLAN_ONLY=0
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--manifest) MANIFEST="$2"; shift 2 ;;
    --plan-only)   PLAN_ONLY=1; shift ;;
    *)             EXTRA+=("$1"); shift ;;
  esac
done

echo "==> sam config plan --manifest $MANIFEST"
sam config plan --manifest "$MANIFEST"

if [[ $PLAN_ONLY -eq 1 ]]; then
  exit 0
fi

echo
echo "==> sam config apply --manifest $MANIFEST ${EXTRA[*]:-}"
sam config apply --manifest "$MANIFEST" "${EXTRA[@]}"

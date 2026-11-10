#!/usr/bin/env bash
# Deploy a solution whose event-mesh gateway targets a workflow — resolving the
# workflow's generated runtime id automatically so the build "just works" with no
# manual id-chasing.
#
# WHY THIS EXISTS
#   On SAM 2.360.11 an event-mesh gateway resolves `targetWorkflowName` by the
#   workflow's generated A2A id (`workflow_<uuid>`), NOT its declarative name
#   (agents friendly-resolve; workflows do not). A gateway authored with the
#   declarative name silently creates no task and never publishes a decision.
#   See ~/sam-bug-eventmesh-gateway-workflow-target-name.md.
#
# WHAT IT DOES
#   Author the gateway with the DECLARATIVE workflow name in targetWorkflowName.
#   This script: (1) applies the solution so the workflow exists, (2) looks up the
#   workflow's runtime id via the platform API and derives workflow_<id>,
#   (3) re-applies the gateway with the resolved target — from a temp render dir,
#   so the checked-in source keeps the clean, portable declarative name.
#
# Usage: deploy-solution-resolved.sh <solution-name> [--restart]
#   <solution-name>  dir under sam/solutions/ (its manifests/dev.yaml is applied)
#   --restart        also `docker compose restart sam` at the end (usually not needed;
#                    the phase-2 apply rebinds the gateway on its own)
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

if [[ -f .env ]]; then set -a; . ./.env; set +a; fi
SAM="${SAM:-sam-2.403.3}"
command -v "$SAM" >/dev/null 2>&1 || SAM="$HOME/.local/bin/${SAM##*/}"
URL="${SAM_URL:-http://127.0.0.1:8800}"
# sam api refuses plain http:// without --insecure; add it only for http URLs.
INSECURE=(); [[ "$URL" == http://* ]] && INSECURE=(--insecure)

NAME="${1:?usage: deploy-solution-resolved.sh <solution-name> [--restart]}"
RESTART=0; [[ "${2:-}" == "--restart" ]] && RESTART=1
SRC="sam/solutions/$NAME"
MAN="$SRC/manifests/dev.yaml"
[[ -f "$MAN" ]] || { echo "error: no manifest at $MAN" >&2; exit 2; }

echo "==> apply (phase 1: create/update all resources)"
"$SAM" config apply --manifest "$MAN"

echo "==> resolving workflow runtime ids"
# name<TAB>id for every deployed workflow
WF_JSON="$("$SAM" api "${INSECURE[@]}" --url "$URL" /api/v1/platform/workflows \
  --jq '.data[] | [.name, .id] | @tsv' 2>/dev/null | grep -E $'\t' || true)"

render_dir="$(mktemp -d)"; trap 'rm -rf "$render_dir"' EXIT
cp -r "$SRC/." "$render_dir/"
changed=0
shopt -s nullglob
for gw in "$render_dir"/entrypoints/*.yaml "$render_dir"/entrypoints/*.yml; do
  tname="$(grep -E '^[[:space:]]*targetWorkflowName:' "$gw" | head -1 \
            | sed -E 's/.*targetWorkflowName:[[:space:]]*//; s/[[:space:]]*#.*//; s/["'\'']//g; s/[[:space:]]*$//')"
  [[ -n "$tname" ]] || continue
  [[ "$tname" == workflow_* ]] && continue   # already a runtime id
  wid="$(printf '%s\n' "$WF_JSON" | awk -F'\t' -v n="$tname" '$1==n{print $2; exit}')"
  if [[ -z "$wid" ]]; then
    echo "  WARN: no deployed workflow named '$tname' — leaving $(basename "$gw") unchanged" >&2
    continue
  fi
  a2a="workflow_$(echo "$wid" | tr '-' '_')"
  sed -i -E "s|^([[:space:]]*targetWorkflowName:).*|\1 $a2a|" "$gw"
  echo "  $(basename "$gw"): targetWorkflowName $tname -> $a2a"
  changed=1
done

if [[ $changed -eq 1 ]]; then
  echo "==> apply (phase 2: gateway with resolved workflow id)"
  "$SAM" config apply --manifest "$render_dir/manifests/dev.yaml"
else
  echo "==> nothing to resolve (no declarative workflow targets found)"
fi

if [[ $RESTART -eq 1 ]]; then
  echo "==> restart sam"
  docker compose restart sam >/dev/null
  for _ in $(seq 1 40); do curl -sf "$URL/healthz" >/dev/null 2>&1 && break; sleep 3; done
fi
echo "done: $NAME deployed with resolved workflow target(s)."

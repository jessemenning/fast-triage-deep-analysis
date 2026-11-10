#!/usr/bin/env bash
# burst.sh — fire the "wind shift" scenario at the hero airport on stage.
#
# Publishes one control message to faa/scenario/windshift; the scenario driver
# (stack/scenario/scenario_driver.py) reacts by (1) publishing a degraded TTP
# Airport Information rate for the hero airport (wind swings SW->NE, runways
# reconfigure to single-runway 4R, AAR drops), (2) publishing a gusty VFR METAR with
# the shifted wind + a TMR ground-delay restriction, and (3) bursting extra bunched
# inbound arrivals — so rolling-hour demand exceeds the reduced AAR and the
# arrival-capacity breach fires into the fast/slow SAM solution.
#
# Transport: the broker's REST messaging listener (host port 9000, enabled by
# config/solace/setup.sh). Content-Type application/octet-stream (raw bytes) so the
# payload is delivered clean.
#
# Usage: burst.sh [--airport KEWR] [--via rest|docker] [--dry-run]
set -euo pipefail

AIRPORT="${HERO_AIRPORT:-KEWR}"
VIA="rest"
DRY=0
REST_HOST="${SOLACE_REST_HOST:-localhost}"
REST_PORT="${SOLACE_REST_PORT:-9000}"
REST_USER="${SOLACE_USER:-default}"
REST_PASS="${SOLACE_PASSWORD:-default}"
TOPIC="faa/scenario/windshift"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --airport) AIRPORT="$2"; shift ;;
    --via)     VIA="$2"; shift ;;
    --dry-run) DRY=1 ;;
    -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

PAYLOAD="$(printf '{"trigger":"windshift","airport":"%s"}' "$AIRPORT")"
echo "wind shift -> $AIRPORT  (topic $TOPIC, via $VIA)"

if (( DRY )); then echo "  $PAYLOAD"; echo "(dry-run: nothing published)"; exit 0; fi

if [[ "$VIA" == "docker" ]]; then
  docker compose exec -T solace curl -s -o /dev/null -u "$REST_USER:$REST_PASS" \
    -H 'Content-Type: application/octet-stream' --data-binary "$PAYLOAD" \
    "http://localhost:9000/TOPIC/$TOPIC"
  echo "published via broker container"
else
  REST_BASE="http://${REST_HOST}:${REST_PORT}"
  curl -s -o /dev/null --connect-timeout 3 "$REST_BASE/" 2>/dev/null \
    || echo "note: $REST_BASE not answering yet — is the stack up? (make up)" >&2
  code="$(curl -s -o /dev/null -w '%{http_code}' -u "$REST_USER:$REST_PASS" \
    -H 'Content-Type: application/octet-stream' --data-binary "$PAYLOAD" \
    "$REST_BASE/TOPIC/$TOPIC")"
  echo "published -> HTTP $code (200 = accepted)"
fi

echo
echo "watch:  make ops   (fl/faa/arrival-capacity/breach/$AIRPORT -> solutions/arrival-capacity/decision/$AIRPORT)"
echo "ui:     http://localhost:8080/demo.html"

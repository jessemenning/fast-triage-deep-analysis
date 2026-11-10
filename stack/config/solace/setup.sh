#!/usr/bin/env bash
# =============================================================================
# config/solace/setup.sh — Configure Solace Platform for faa-fast-slow
#
# Fully Solace-native data plane. Creates in the default VPN one durable queue per
# SWIM/SCDS feed that the feldera-solace sidecar binds (one ingest bridge per queue):
#
#   DMQ:   dlq-fl-faa          (self-draining: maxTtl 300s, 500 MB cap)
#   Queue: fl-fdps-position    ← FDPS/position/*/ACTIVE/>   (active flights only)
#   Queue: fl-fdps-flightplan  ← FDPS/flightPlan/>
#   Queue: fl-ttp-rate         ← TTP/airportInfo/>          (TTP Airport Information: AAR/ADR)
#   Queue: fl-ttp-eta          ← TTP/flightData/>           (TTPFD: per-flight ETA)
#   Queue: fl-ttp-tmr          ← TTP/restriction/>          (TMR: GDP/APREQ/ground stop)
#   Queue: fl-aviation-metar   ← aviation/metar/>
#
# The replay feed publishes captured SWIM messages to:
#   FDPS/position/{flight_id}/{status}/{callsign}/{origin}/{destination}/{lat}/{lon}/{gs}/{alt}/{hdg}/{gufi}
#   FDPS/flightPlan/{flight_id}/{status}/{departure}/{arrival}/{gufi}
#   TTP/airportInfo/{airport}
#   TTP/flightData/{gufi}/{arrival}/{eta_source}
#   TTP/restriction/{airport}/{restriction_type}
#   aviation/metar/{station_id}
# and the scenario driver republishes TTP/airportInfo/{airport} to degrade the AAR.
#
# Usage: bash config/solace/setup.sh [SOLACE_HOST] [SEMP_PORT]
#   SOLACE_HOST defaults to "localhost"; SEMP_PORT defaults to "8080"
# =============================================================================
set -euo pipefail

SOLACE_HOST="${1:-localhost}"
SEMP_PORT="${2:-8080}"
SEMP_BASE="http://${SOLACE_HOST}:${SEMP_PORT}/SEMP/v2/config"
SEMP_AUTH="admin:${SOLACE_ADMIN_PASSWORD:-admin}"
VPN="${FL_SOLACE_VPN:-default}"

log()  { echo "[solace-setup] $(date '+%H:%M:%S')  $*"; }
fail() { echo "[solace-setup] FATAL: $*" >&2; exit 1; }

# PATCH helper — non-fatal; used for VPN attribute updates
semp_patch() {
  local url="$1"; local body="$2"
  curl -s -X PATCH -u "${SEMP_AUTH}" \
    -H "Content-Type: application/json" \
    --data "${body}" \
    "${SEMP_BASE}${url}" > /dev/null 2>&1 || true
}

# POST helper — treats ALREADY_EXISTS (400/409) as success
semp_post() {
  local url="$1"; local body="$2"; local resp
  resp=$(curl -s -X POST -u "${SEMP_AUTH}" \
    -H "Content-Type: application/json" \
    --data "${body}" \
    "${SEMP_BASE}${url}" 2>&1)
  local err_code
  err_code=$(echo "${resp}" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('meta',{}).get('error',{}).get('status',''))" \
    2>/dev/null || echo "")
  if [[ "${err_code}" == "ALREADY_EXISTS" ]]; then
    log "  (already exists — ok)"; return 0
  fi
  local code
  code=$(echo "${resp}" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(d.get('meta',{}).get('responseCode',0))" \
    2>/dev/null || echo "0")
  if [[ "${code}" != "200" ]]; then
    log "  WARN: POST ${url} → code=${code} err=${err_code}"
  fi
}

# Create one durable, non-exclusive, DMQ-linked queue and subscribe it to a topic.
create_queue() {
  local q="$1"; local topic="$2"; local ttl="${3:-180}"
  log "Creating durable queue: ${q} → ${topic}"
  semp_post "/msgVpns/${VPN}/queues" "{
    \"queueName\": \"${q}\",
    \"msgVpnName\": \"${VPN}\",
    \"accessType\": \"non-exclusive\",
    \"permission\": \"consume\",
    \"ingressEnabled\": true,
    \"egressEnabled\": true,
    \"deadMsgQueue\": \"dlq-fl-faa\",
    \"maxRedeliveryCount\": 3,
    \"maxTtl\": ${ttl},
    \"respectTtlEnabled\": true
  }"
  semp_post "/msgVpns/${VPN}/queues/${q}/subscriptions" "{
    \"msgVpnName\": \"${VPN}\",
    \"queueName\": \"${q}\",
    \"subscriptionTopic\": \"${topic}\"
  }"
}

log "Configuring Solace Platform at ${SOLACE_HOST}:${SEMP_PORT} (VPN ${VPN})"

# ── MQTT must be enabled BEFORE queue creation (a VPN PATCH can reset VPN state) ─
log "Enabling MQTT plain-text (1883) and WebSocket (8000) on VPN ${VPN}"
semp_patch "/msgVpns/${VPN}" '{
  "serviceMqttPlainTextEnabled": true,
  "serviceMqttPlainTextListenPort": 1883,
  "serviceMqttWebSocketEnabled": true,
  "serviceMqttWebSocketListenPort": 8000
}'
sleep 2

# ── Self-draining dead-message queue ──────────────────────────────────────────
# Unbounded DMQs eventually fill the spool quota and back-pressure their source
# queues to ~1 msg/s. Bound + self-drain it.
log "Creating self-draining DMQ: dlq-fl-faa"
semp_post "/msgVpns/${VPN}/queues" "{
  \"queueName\": \"dlq-fl-faa\",
  \"msgVpnName\": \"${VPN}\",
  \"accessType\": \"non-exclusive\",
  \"permission\": \"consume\",
  \"ingressEnabled\": true,
  \"egressEnabled\": true,
  \"maxMsgSpoolUsage\": 500,
  \"maxTtl\": 300,
  \"respectTtlEnabled\": true
}"
semp_patch "/msgVpns/${VPN}/queues/dlq-fl-faa" \
  '{"maxMsgSpoolUsage":500,"maxTtl":300,"respectTtlEnabled":true}'

# ── SWIM / SCDS ingest queues (one per sidecar ingest bridge → Feldera table) ──
# Position: only ACTIVE flights reach Feldera (status is topic level 4).
create_queue "fl-fdps-position"   "FDPS/position/*/ACTIVE/>"  180
create_queue "fl-fdps-flightplan" "FDPS/flightPlan/>"         300
# Rates change slowly and are the versioned side input — give them a long TTL so a
# late-binding Feldera connector still sees the current rate for each airport.
create_queue "fl-ttp-rate"        "TTP/airportInfo/>"         600
create_queue "fl-ttp-eta"         "TTP/flightData/>"          180
create_queue "fl-ttp-tmr"         "TTP/restriction/>"         600
create_queue "fl-aviation-metar"  "aviation/metar/>"          600
# Reset-epoch marker: the UI ↺ Reset / API publishes faa/scenario/reset; Feldera ingests
# it so capacity_assessment re-scores pre-reset (frozen-degraded) bins against the current
# restored rate — a reset clears stale SEVERE bins at once instead of waiting ~1h to age out.
create_queue "fl-faa-reset"       "faa/scenario/reset"        600

# ── REST messaging (9000) ─────────────────────────────────────────────────────
# Enabled so events can also be injected over HTTP (e.g. demo/burst.sh). Publishers
# MUST send application/octet-stream (raw bytes) — application/json makes the broker
# frame the payload and PARSE_JSON(data) returns NULL for every field.
log "Enabling REST messaging (9000) on VPN ${VPN}"
semp_patch "/msgVpns/${VPN}" \
  '{"serviceRestIncomingPlainTextEnabled": true, "serviceRestIncomingPlainTextListenPort": 9000}'
sleep 2

log "Solace Platform configuration complete."
log "  Queues: fl-fdps-position / fl-fdps-flightplan / fl-ttp-rate / fl-ttp-eta / fl-ttp-tmr / fl-aviation-metar / fl-faa-reset"
log "  DMQ:    dlq-fl-faa (self-draining, 300s TTL)"
log "  MQTT WS: VPN port 8000 (host-mapped to 8008 for the browser)"
log "  REST msg: VPN port 9000"

#!/bin/sh
# init.sh — Feldera pipeline setup for FAA Fast/Slow (API v0.306+).
# One-shot init container; exits 0 on success.
#
# Fully Solace-native data plane — NO Postgres, NO postgres_cdc_input, so NO
# reset_stale_cdc_state self-heal is needed (cf. the CDC sibling demos).
#
# Steps:
#   1. Wait for Feldera /healthz
#   2. Wait for Solace VPN (SEMP)
#   3. Substitute env vars into faa_program.sql
#   4. Create or update pipeline via PUT /v0/pipelines/{name}
#   5. Wait for program_status == "Success"
#   6. Start pipeline via POST /v0/pipelines/{name}/start
#   7. Wait for deployment_status == "Running"

FL_HOST="${FL_HOST:-feldera}"
FL_PORT="${FL_PORT:-8080}"
FL_BASE="http://${FL_HOST}:${FL_PORT}"

SEMP_HOST="${SEMP_HOST:-solace}"
SEMP_PORT="${SEMP_PORT:-8080}"
FL_SOLACE_VPN="${FL_SOLACE_VPN:-default}"
SEMP_URL="http://${SEMP_HOST}:${SEMP_PORT}/SEMP/v2/monitor/msgVpns/${FL_SOLACE_VPN}"
SEMP_USER="${SOLACE_ADMIN_USER:-admin}"
SEMP_PASS="${SOLACE_ADMIN_PASSWORD:-admin}"

PIPELINE_NAME="${FELDERA_PIPELINE:-arrival-capacity}"
WORKERS="${FL_WORKERS:-4}"
SQL_FILE="/config/feldera/faa_program.sql"

log()  { printf '[fl-init] %s  %s\n' "$(date '+%H:%M:%S')" "$*"; }
fail() { printf '[fl-init] FATAL: %s\n' "$*" >&2; exit 1; }

# ── 1. Wait for Feldera ────────────────────────────────────────────────────────
wait_for_feldera() {
    log "Waiting for Feldera at ${FL_BASE}/healthz ..."
    i=1
    while true; do
        if wget -q -O /dev/null --timeout=5 "${FL_BASE}/healthz" 2>/dev/null; then
            log "Feldera is healthy."
            return 0
        fi
        log "Feldera not ready — waiting 5s (attempt ${i})"
        i=$((i + 1))
        sleep 5
    done
}

# ── 2. Wait for Solace VPN ─────────────────────────────────────────────────────
wait_for_solace_vpn() {
    log "Waiting for Solace VPN ${FL_SOLACE_VPN} at ${SEMP_URL} ..."
    SEMP_AUTH=$(printf '%s:%s' "${SEMP_USER}" "${SEMP_PASS}" | base64)
    i=1
    while true; do
        if wget -q -O /dev/null \
               --header="Authorization: Basic ${SEMP_AUTH}" \
               --timeout=5 "${SEMP_URL}" 2>/dev/null; then
            log "Solace ${FL_SOLACE_VPN} VPN is ready."
            return 0
        fi
        log "Solace VPN not ready — waiting 10s (attempt ${i})"
        i=$((i + 1))
        sleep 10
    done
}

# ── 3. Substitute env vars into SQL program ────────────────────────────────────
# FL_SOLACE_HOSTNAME — hostname only (e.g. "solace"), no tcp:// scheme or port.
# FL_SOLACE_PORT     — SMF port as a bare integer (e.g. "55555"), used inline in
#                      the JSON "port" field which expects a number, not a string.
prepare_sql() {
    log "Substituting env vars into faa_program.sql ..."
    sed \
        -e "s|\${FL_SOLACE_HOSTNAME}|${FL_SOLACE_HOSTNAME:-solace}|g" \
        -e "s|\${FL_SOLACE_PORT}|${FL_SOLACE_PORT:-55555}|g" \
        -e "s|\${FL_SOLACE_VPN}|${FL_SOLACE_VPN:-default}|g" \
        -e "s|\${FL_SOLACE_USER}|${FL_SOLACE_USER:-default}|g" \
        -e "s|\${FL_SOLACE_PASSWORD}|${FL_SOLACE_PASSWORD:-default}|g" \
        "${SQL_FILE}" > /tmp/faa_program_applied.sql
    log "SQL prepared ($(wc -c < /tmp/faa_program_applied.sql) bytes)."
}

# ── 4. Create or update the pipeline (SQL in program_code) ────────────────────
# Skip if pipeline is already Running (idempotent restart).
upsert_pipeline() {
    CURRENT_STATUS=$(wget -q -O - "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}" 2>/dev/null \
        | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('deployment_status','none'))" 2>/dev/null \
        || echo "none")

    if [ "${CURRENT_STATUS}" = "Running" ]; then
        log "Pipeline '${PIPELINE_NAME}' is already Running — skipping upsert."
        return 0
    fi

    log "Uploading pipeline '${PIPELINE_NAME}' with SQL program (current status: ${CURRENT_STATUS}) ..."
    SQL_BODY=$(cat /tmp/faa_program_applied.sql)
    SQL_JSON=$(printf '%s' "${SQL_BODY}" \
        | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))')

    RUNTIME_CONFIG=$(printf '{"workers":%s}' "${WORKERS}")

    PAYLOAD=$(printf '{"name":"%s","description":"FAA Fast/Slow — arrival capacity vs published rate","program_code":%s,"runtime_config":%s}' \
        "${PIPELINE_NAME}" "${SQL_JSON}" "${RUNTIME_CONFIG}")

    HTTP_STATUS=$(wget -q -O /tmp/pipeline_resp.json \
        --method=PUT \
        --header='Content-Type: application/json' \
        --body-data="${PAYLOAD}" \
        --server-response \
        "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}" 2>&1 | grep "HTTP/" | tail -1 | awk '{print $2}')

    log "Pipeline upsert response: HTTP ${HTTP_STATUS}"
    if [ -z "${HTTP_STATUS}" ] || { [ "${HTTP_STATUS}" -ge 400 ] 2>/dev/null; }; then
        cat /tmp/pipeline_resp.json >&2
        fail "Pipeline upsert failed (HTTP ${HTTP_STATUS})"
    fi
    log "Pipeline '${PIPELINE_NAME}' created/updated."
}

# ── 5. Wait for program_status == "Success" ───────────────────────────────────
wait_for_compilation() {
    CURRENT_DEPLOY=$(wget -q -O - "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('deployment_status','none'))" 2>/dev/null \
        || echo "none")
    if [ "${CURRENT_DEPLOY}" = "Running" ]; then
        log "Pipeline already Running — skipping compilation wait."
        return 0
    fi
    log "Waiting for SQL compilation ..."
    # 360 iters x 5s = 30 min. A cold Rust compile of this program can exceed the
    # old 10-min bound (esp. under CPU contention / cold cargo cache), which would
    # kill init before the pipeline ever started. Override with COMPILE_WAIT_ITERS.
    i=1
    max_iters="${COMPILE_WAIT_ITERS:-360}"
    while [ "$i" -le "$max_iters" ]; do
        RESP=$(wget -q -O - "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}" 2>/dev/null)
        PROG_STATUS=$(printf '%s' "${RESP}" \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('program_status','unknown'))" 2>/dev/null \
            || echo "unknown")

        log "program_status: ${PROG_STATUS}"

        case "${PROG_STATUS}" in
            Success)
                log "Compilation succeeded."
                return 0
                ;;
            SqlError|RustError|SystemError)
                cat /tmp/pipeline_resp.json >&2 2>/dev/null || true
                fail "Compilation failed with status: ${PROG_STATUS}"
                ;;
        esac

        sleep 5
        i=$((i + 1))
    done
    fail "Compilation timed out after $((max_iters * 5 / 60)) minutes."
}

# ── 6. Start the pipeline ──────────────────────────────────────────────────────
start_pipeline() {
    CURRENT_DEPLOY=$(wget -q -O - "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}" 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('deployment_status','none'))" 2>/dev/null \
        || echo "none")
    if [ "${CURRENT_DEPLOY}" = "Running" ]; then
        log "Pipeline already Running — skipping start."
        return 0
    fi
    log "Starting pipeline '${PIPELINE_NAME}' ..."
    HTTP_STATUS=$(wget -q -O /tmp/start_resp.json \
        --method=POST \
        --header='Content-Type: application/json' \
        --body-data='{}' \
        --server-response \
        "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}/start" 2>&1 | grep "HTTP/" | tail -1 | awk '{print $2}')

    log "Pipeline start response: HTTP ${HTTP_STATUS}"
    if [ -z "${HTTP_STATUS}" ] || { [ "${HTTP_STATUS}" -ge 400 ] 2>/dev/null; }; then
        cat /tmp/start_resp.json >&2
        fail "Pipeline start failed (HTTP ${HTTP_STATUS})"
    fi
    log "Pipeline start command accepted."
}

# ── 7. Wait for deployment_status == "Running" ────────────────────────────────
wait_for_running() {
    log "Waiting for pipeline to reach Running state ..."
    TIMEOUT=120
    elapsed=0
    while [ "${elapsed}" -lt "${TIMEOUT}" ]; do
        DEPLOY_STATUS=$(wget -q -O - \
            "${FL_BASE}/v0/pipelines/${PIPELINE_NAME}" 2>/dev/null \
            | python3 -c "import sys,json; print(json.load(sys.stdin).get('deployment_status','unknown'))" 2>/dev/null \
            || echo "unknown")

        log "deployment_status: ${DEPLOY_STATUS}"

        case "${DEPLOY_STATUS}" in
            Running)
                log "Pipeline is Running. Initialization complete."
                return 0
                ;;
            Failed)
                fail "Pipeline entered Failed state."
                ;;
        esac

        sleep 5
        elapsed=$((elapsed + 5))
    done

    log "WARNING: Pipeline not in Running state after ${TIMEOUT}s (status=${DEPLOY_STATUS}) — proceeding anyway"
    return 0
}

# ── Main ───────────────────────────────────────────────────────────────────────
log "Starting Feldera FAA arrival-capacity pipeline initialization (API v0.306+)"

wait_for_feldera
wait_for_solace_vpn
prepare_sql
upsert_pipeline
wait_for_compilation
start_pipeline
wait_for_running

log "FAA Fast/Slow Feldera pipeline initialized successfully."
exit 0

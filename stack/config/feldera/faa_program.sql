-- =============================================================================
-- Feldera Streaming SQL — FAA Fast/Slow: Arrival Capacity Demand vs Published Rate
-- =============================================================================
--
-- Continuously maintains expected-arrival DEMAND per airport per rolling time bin
-- and raises an event whenever demand exceeds the published arrival rate (AAR).
--
-- Fully Solace-native data plane. Every feed is a Solace queue bridged into Feldera
-- by the external feldera-solace sidecar (stock upstream Feldera, no custom fork).
-- There is NO database and NO postgres_cdc_input — rates (TTP Airport Information)
-- arrive as events, exactly as they do over SWIM/SCDS, and "versioned side input"
-- is reconstructed in SQL by deriving each rate's validity interval
-- (valid_from .. valid_to) from the stream.
--
-- SCDS source → object mapping:
--   FDPS position/flightPlan  → flight state (status, arrival airport, GUFI)
--   TTPFD (TTP/flightData)    → per-flight ETA + eta_source quality
--   TTP Airport Information    → airport_rate (AAR/ADR, runway config), versioned
--   TMR (TTP/restriction)      → active restrictions (GDP/APREQ/ground stop) for context
--   METAR (aviation/metar)     → weather for the decision brief
--
-- Solace I/O lives in the sidecar, not the SQL: ingest tables are plain tables the
-- sidecar POSTs to over /ingress (payload_mode: wrap → raw body in `payload`, with
-- topic captures + solace_topic + broker_ts merged in as same-named JSON fields),
-- and output views are plain MATERIALIZED views the sidecar reads over /egress and
-- publishes to Solace. The queue/topic mapping lives in stack/sidecar/config.yaml.
-- Output views MUST be MATERIALIZED to be egressed / streamed to Solace.
-- =============================================================================


-- ═════════════════════════════════════════════════════════════════════════════
-- §1  INGEST TABLES — fed by the feldera-solace sidecar over HTTP (fl- queues)
-- ═════════════════════════════════════════════════════════════════════════════

-- Ingest tables are fed by the feldera-solace sidecar (stack/sidecar/config.yaml),
-- NOT by an in-SQL Solace connector. Each table maps to one sidecar ingest bridge
-- running `payload_mode: wrap`: the raw broker body arrives in the `payload` column,
-- and the sidecar merges topic-level captures, the full `solace_topic`, and the
-- broker receive `broker_ts` in as ordinary JSON fields (same names as the old
-- CONNECTOR_METADATA() columns). The sidecar binds the fl-* queues provisioned by
-- stack/config/solace/setup.sh. Columns map to JSON keys by name.

-- ── FDPS Position ─────────────────────────────────────────────────────────────
-- Topic: FDPS/position/{flight_id}/{status}/{callsign}/{origin}/{destination}/
--        {latitude}/{longitude}/{ground_speed}/{altitude_ft}/{heading}/{gufi}
CREATE TABLE faa_ingest_position_raw (
    payload      VARCHAR,
    solace_topic VARCHAR,
    flight_id    VARCHAR,
    status       VARCHAR,
    callsign     VARCHAR,
    origin       VARCHAR,
    destination  VARCHAR,
    latitude     VARCHAR,
    longitude    VARCHAR,
    ground_speed VARCHAR,
    altitude_ft  VARCHAR,
    heading      VARCHAR,
    gufi         VARCHAR,
    broker_ts    TIMESTAMP
);

-- ── FDPS Flight Plan ──────────────────────────────────────────────────────────
-- Topic: FDPS/flightPlan/{flight_id}/{status}/{departure}/{arrival}/{gufi}
CREATE TABLE faa_ingest_flightplan_raw (
    payload      VARCHAR,
    solace_topic VARCHAR,
    flight_id    VARCHAR,
    status       VARCHAR,
    departure    VARCHAR,
    arrival      VARCHAR,
    gufi         VARCHAR,
    broker_ts    TIMESTAMP
);

-- ── TTPFD per-flight ETA (TTP Flight Data) ───────────────────────────────────
-- Topic: TTP/flightData/{gufi}/{arrival}/{eta_source}
-- Payload JSON: { "gufi", "arrival", "eta" (ISO/space ts), "eta_source" }
CREATE TABLE faa_ingest_eta_raw (
    payload      VARCHAR,
    solace_topic VARCHAR,
    gufi         VARCHAR,
    arrival      VARCHAR,
    eta_source   VARCHAR,
    broker_ts    TIMESTAMP
);

-- ── TTP Airport Information — published arrival/departure rates (AAR/ADR) ─────
-- Topic: TTP/airportInfo/{airport}
-- Payload JSON: { "airport", "aar", "adr", "runway_config", "effective_time" }
-- This is the VERSIONED side input: each event is a new rate version for an airport.
CREATE TABLE faa_ingest_rate_raw (
    payload        VARCHAR,
    solace_topic   VARCHAR,
    airport        VARCHAR,
    broker_ts      TIMESTAMP
);

-- ── TMR — Traffic Management Restrictions (context only) ──────────────────────
-- Topic: TTP/restriction/{airport}/{restriction_type}
-- Payload JSON: { "airport", "restriction_type", "reason", "effective_time", "scope" }
CREATE TABLE faa_ingest_tmr_raw (
    payload          VARCHAR,
    solace_topic     VARCHAR,
    airport          VARCHAR,
    restriction_type VARCHAR,
    broker_ts        TIMESTAMP
);

-- ── METAR ─────────────────────────────────────────────────────────────────────
CREATE TABLE faa_ingest_metar_raw (
    payload      VARCHAR,
    solace_topic VARCHAR,
    station_id   VARCHAR,
    broker_ts    TIMESTAMP
);

-- ── Reset marker (↺ Reset button / API) ───────────────────────────────────────
-- Topic: faa/scenario/reset   Payload JSON: { "trigger":"reset", "airport": "KEWR" }
-- Each message's broker_ts is the moment the operator reset an airport. `reset_epoch`
-- (below §4) turns this into the latest reset time per airport, which capacity_assessment
-- uses to re-score pre-reset frozen-degraded bins against the current restored rate.
CREATE TABLE faa_ingest_reset_raw (
    payload      VARCHAR,
    solace_topic VARCHAR,
    broker_ts    TIMESTAMP
);


-- ═════════════════════════════════════════════════════════════════════════════
-- §2  DEDUP VIEWS — latest row per key (Feldera sources are append-only)
-- ═════════════════════════════════════════════════════════════════════════════
-- Both dedups carry a temporal filter INSIDE the ROW_NUMBER subquery. Without it
-- the TopK operator keeps every row ever seen per GUFI (verified 2026-10-29 from
-- the circuit profile: ~24k rows/hour, never collected; LATENESS does not bound a
-- TopK, only a NOW() filter does). One day is wide enough that a flight plan that
-- is older than its latest ETA stays current, which is the normal real-feed case.
-- The flight-plan subquery also lists its columns explicitly so the raw payload
-- column `payload` is projected away before the window instead of being retained.
-- LOCAL: consumed only by `flight`; not egress-watched or queried ad hoc.

CREATE LOCAL VIEW flightplan_latest AS
SELECT gufi, flight_id, status, departure, arrival, broker_ts
FROM (
    SELECT gufi, flight_id, status, departure, arrival, broker_ts,
           ROW_NUMBER() OVER (PARTITION BY gufi ORDER BY broker_ts DESC NULLS LAST) AS rn
    FROM faa_ingest_flightplan_raw
    WHERE gufi IS NOT NULL
      AND broker_ts >= NOW() - INTERVAL '1' DAY
) WHERE rn = 1;

-- Latest ETA per GUFI. eta is parsed from the JSON payload; eta_source from topic.
CREATE LOCAL VIEW eta_latest AS
SELECT gufi, arrival, eta_source, eta, broker_ts
FROM (
    SELECT gufi, arrival, eta_source, broker_ts,
           CAST(PARSE_JSON(payload)['eta'] AS TIMESTAMP) AS eta,
           ROW_NUMBER() OVER (PARTITION BY gufi ORDER BY broker_ts DESC NULLS LAST) AS rn
    FROM faa_ingest_eta_raw
    WHERE gufi IS NOT NULL
      AND broker_ts >= NOW() - INTERVAL '1' DAY
) WHERE rn = 1;


-- ═════════════════════════════════════════════════════════════════════════════
-- §3  STATIC AIRPORT REFERENCE (small, not event-sourced → inline VALUES)
-- ═════════════════════════════════════════════════════════════════════════════
-- The only durable, non-streaming reference. Kept tiny: the hero + downstream cast.
-- (If a deployment needs a large/edited airport list, publish it as a reference
--  feed over Solace and ingest it instead — this VALUES view is the laptop default.)
CREATE VIEW airports (airport, airport_name, tracon, metering_active) AS
SELECT * FROM (VALUES
    ('KMKG', 'Muskegon County',             'GRR',  TRUE),
    ('KEWR', 'Newark Liberty Intl',         'N90',  TRUE),
    ('KJFK', 'John F. Kennedy Intl',        'N90',  TRUE),
    ('KLGA', 'LaGuardia',                    'N90',  TRUE),
    ('KPHL', 'Philadelphia Intl',           'PHL',  TRUE),
    ('KBOS', 'Boston Logan Intl',           'A90',  TRUE),
    ('KORD', 'Chicago O''Hare Intl',        'C90',  TRUE),
    ('KATL', 'Hartsfield-Jackson Atlanta',  'A80',  TRUE),
    ('KDCA', 'Reagan Washington National',  'PCT',  TRUE),
    ('KIAD', 'Washington Dulles Intl',      'PCT',  TRUE),
    ('KBWI', 'Baltimore/Washington Intl',   'PCT',  TRUE)
) AS t(airport, airport_name, tracon, metering_active);


-- ═════════════════════════════════════════════════════════════════════════════
-- §4  AIRPORT RATE — versioned side input (valid_from .. valid_to) from the stream
-- ═════════════════════════════════════════════════════════════════════════════
-- Every TTP Airport Information event is a new rate version. Each version is valid
-- from its effective_time until the NEXT version's effective_time for that airport.
-- This gives the "rate in effect for a bin" temporal join without any database.
CREATE MATERIALIZED VIEW airport_rate AS
SELECT
    airport,
    aar,
    adr,
    runway_config,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY airport ORDER BY valid_from) AS valid_to
FROM (
    SELECT
        airport,
        CAST(PARSE_JSON(payload)['aar'] AS INTEGER)                         AS aar,
        CAST(PARSE_JSON(payload)['adr'] AS INTEGER)                         AS adr,
        CAST(PARSE_JSON(payload)['runway_config'] AS VARCHAR)              AS runway_config,
        CAST(PARSE_JSON(payload)['effective_time'] AS TIMESTAMP)          AS valid_from
    FROM faa_ingest_rate_raw
    WHERE airport IS NOT NULL
);

-- Latest reset time per airport (the ↺ Reset marker). capacity_assessment (§7) uses this
-- to re-score any bin whose rate-in-effect PREDATES the last reset against the CURRENT
-- restored rate — so a reset clears old frozen-degraded SEVERE bins immediately instead of
-- waiting for the event-time window to slide past them (~1h). A fresh wind shift AFTER a
-- reset publishes a newer rate version (valid_from > reset_ts), so its bins keep their own
-- degraded rate and a new incident still fires — the reset only neutralises STALE windows.
CREATE MATERIALIZED VIEW reset_epoch AS
SELECT airport, MAX(broker_ts) AS reset_ts
FROM (
    SELECT CAST(PARSE_JSON(payload)['airport'] AS VARCHAR) AS airport, broker_ts
    FROM faa_ingest_reset_raw
) WHERE airport IS NOT NULL
GROUP BY airport;


-- ═════════════════════════════════════════════════════════════════════════════
-- §5  FLIGHT — one keyed row per in-bound flight with best-available ETA
-- ═════════════════════════════════════════════════════════════════════════════
-- Arrival airport comes from the flight plan; ETA + source from TTPFD. Only ACTIVE
-- flights with a known arrival airport and a fresh ETA count toward demand. An
-- amended ETA / cancellation moves the flight between bins (retraction for free).
CREATE MATERIALIZED VIEW flight AS
SELECT
    fp.gufi,
    fp.flight_id,
    fp.status,
    COALESCE(e.arrival, fp.arrival)  AS arr_airport,
    fp.departure                     AS dep_airport,
    e.eta,
    COALESCE(e.eta_source, 'FILED')  AS eta_source,
    GREATEST(fp.broker_ts, e.broker_ts) AS updated_ts
FROM flightplan_latest fp
JOIN eta_latest e ON fp.gufi = e.gufi
WHERE fp.status = 'ACTIVE'
  AND COALESCE(e.arrival, fp.arrival) IS NOT NULL
  AND e.eta IS NOT NULL
  -- freshness: drop flights whose last update is stale (arrived/no longer tracked).
  -- Demo-tuned to 15 min: the replay loop refreshes baseline flights every ~90s, so
  -- this never sawtooths demand, but it shrinks the post-reset decay tail — a flight
  -- that silently stops updating leaves the demand window in 15 min, not 45.
  AND NOW() <= GREATEST(fp.broker_ts, e.broker_ts) + INTERVAL '15' MINUTE;


-- ═════════════════════════════════════════════════════════════════════════════
-- §6  ARRIVAL DEMAND — rolling 60-min window, sliding every 15 min, per airport
-- ═════════════════════════════════════════════════════════════════════════════
-- HOP rolling window avoids the bin-boundary artifacts of a fixed TUMBLE: a surge
-- straddling a boundary is still caught. window size 60m = one hour of AAR.
--
-- NOTE (Feldera windowing): if the HOP table-valued function is rejected at compile
-- time for lack of a watermark/LATENESS on `eta`, replace the FROM clause below with
-- a fixed 15-min tumbling bin computed by truncation, e.g.:
--     TIMESTAMP_TRUNC(eta, HOUR) + (EXTRACT(MINUTE FROM eta)/15)*INTERVAL '15' MINUTE
--   grouped as bin_start/bin_end — see CUSTOMIZE.md. The rolling HOP is preferred.
CREATE MATERIALIZED VIEW arrival_demand AS
SELECT
    arr_airport                                                   AS airport,
    window_start                                                   AS bin_start,
    window_end                                                     AS bin_end,
    COUNT(*)                                                       AS demand,
    SUM(CASE WHEN eta_source IN ('TTPFD', 'TBFM') THEN 1 ELSE 0 END) AS high_quality_eta_count
FROM TABLE(HOP(TABLE flight, DESCRIPTOR(eta), INTERVAL '15' MINUTE, INTERVAL '60' MINUTE))
GROUP BY arr_airport, window_start, window_end;


-- ═════════════════════════════════════════════════════════════════════════════
-- §7  CAPACITY ASSESSMENT — demand vs the rate in effect for the bin
-- ═════════════════════════════════════════════════════════════════════════════
-- bin is 60 min, AAR is arrivals/hour → bin_capacity = aar (no scaling). A new rate
-- version (a wind shift drops the AAR) re-evaluates every affected bin automatically.
--
-- Reset-epoch re-scoring: normally a bin is scored against the rate IN EFFECT when its
-- window opened (temporal join on bin_start). That freezes a degraded AAR onto old bins —
-- after a ↺ Reset restores the rate, those past bins stay scored against the degraded AAR
-- and, fed by ordinary baseline demand (which exceeds the degraded rate but not the real
-- one), stay SEVERE until they age out (~1h). So: a bin whose matched rate version PREDATES
-- the airport's last reset (reset_ts > r.valid_from) is re-scored against the CURRENT rate
-- (valid_to IS NULL) instead. A reset therefore clears stale frozen bins at once; a NEW
-- wind shift after the reset publishes a newer rate version, so its bins keep their own
-- degraded rate and still fire. `eff_aar` is computed once in the inner query.
CREATE MATERIALIZED VIEW capacity_assessment AS
SELECT
    airport,
    bin_start,
    bin_end,
    demand,
    eff_aar                                                     AS bin_capacity,
    runway_config,
    demand - eff_aar                                            AS overage,
    CASE WHEN demand > 0
         THEN CAST(high_quality_eta_count AS DOUBLE) / demand
         ELSE 0.0 END                                           AS eta_quality,
    (demand > eff_aar)                                          AS exceeded
FROM (
    SELECT
        d.airport,
        d.bin_start,
        d.bin_end,
        d.demand,
        d.high_quality_eta_count,
        r.runway_config,
        CASE WHEN re.reset_ts IS NOT NULL AND re.reset_ts > r.valid_from
             THEN cur.aar ELSE r.aar END                        AS eff_aar
    FROM arrival_demand d
    JOIN airport_rate r
      ON r.airport = d.airport
     AND d.bin_start >= r.valid_from
     AND (r.valid_to IS NULL OR d.bin_start < r.valid_to)
    LEFT JOIN (SELECT airport, aar FROM airport_rate WHERE valid_to IS NULL) cur
      ON cur.airport = d.airport
    LEFT JOIN reset_epoch re
      ON re.airport = d.airport
);


-- ═════════════════════════════════════════════════════════════════════════════
-- §8  CAPACITY BREACH EVENTS — hysteresis: margin + persistence (anti-flap)
-- ═════════════════════════════════════════════════════════════════════════════
-- Only alert when the overage is MATERIAL (≥10% of AAR, min 1) AND the overage is
-- sustained (the previous hop bin for this airport was also over). This debounces
-- the alert so a one-reading blip from shifting ETAs does not fire.
CREATE MATERIALIZED VIEW capacity_breach_events AS
SELECT
    airport,
    bin_start,
    bin_end,
    demand,
    bin_capacity,
    overage,
    runway_config,
    eta_quality,
    CASE WHEN overage >= GREATEST(CAST(0.25 * bin_capacity AS INTEGER), 5) THEN 'SEVERE'
         WHEN overage >= GREATEST(CAST(0.10 * bin_capacity AS INTEGER), 2) THEN 'MODERATE'
         ELSE 'MARGINAL' END                                            AS severity
FROM (
    SELECT a.*,
        LAG(a.overage) OVER (PARTITION BY a.airport ORDER BY a.bin_start) AS prev_overage
    FROM capacity_assessment a
    WHERE a.exceeded
)
WHERE overage >= GREATEST(CAST(0.10 * bin_capacity AS INTEGER), 1)   -- ≥10% over, material
  AND ( overage >= GREATEST(CAST(0.20 * bin_capacity AS INTEGER), 3) -- clearly significant → fire now
        OR COALESCE(prev_overage, 0) > 0 );                          -- or sustained across ≥2 bins


-- ═════════════════════════════════════════════════════════════════════════════
-- §9  WEATHER + RESTRICTION CONTEXT (for the decision brief / provenance)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW metar_latest AS
SELECT station_id, observed_at, flight_category, wind_dir_degrees, wind_speed_kt,
       visibility_sm, wx_string
FROM (
    SELECT
        CAST(PARSE_JSON(payload)['icaoId'] AS VARCHAR)      AS station_id,
        CAST(PARSE_JSON(payload)['reportTime'] AS TIMESTAMP) AS observed_at,
        CAST(PARSE_JSON(payload)['fltCat'] AS VARCHAR)      AS flight_category,
        CASE WHEN CAST(PARSE_JSON(payload)['wdir'] AS VARCHAR) RLIKE '^[0-9]+$'
             THEN CAST(PARSE_JSON(payload)['wdir'] AS INTEGER) END AS wind_dir_degrees,
        CAST(PARSE_JSON(payload)['wspd'] AS INTEGER)        AS wind_speed_kt,
        CAST(PARSE_JSON(payload)['visib'] AS VARCHAR)       AS visibility_sm,
        CAST(PARSE_JSON(payload)['rawOb'] AS VARCHAR)       AS wx_string,
        ROW_NUMBER() OVER (PARTITION BY CAST(PARSE_JSON(payload)['icaoId'] AS VARCHAR)
                           ORDER BY CAST(PARSE_JSON(payload)['reportTime'] AS TIMESTAMP) DESC) AS rn
    FROM faa_ingest_metar_raw
) WHERE rn = 1;

-- Active restrictions per airport (latest per airport+type, 2h freshness).
CREATE MATERIALIZED VIEW active_restriction AS
SELECT airport, restriction_type, reason, scope, effective_time
FROM (
    SELECT airport, restriction_type,
        CAST(PARSE_JSON(payload)['reason'] AS VARCHAR)         AS reason,
        CAST(PARSE_JSON(payload)['scope'] AS VARCHAR)          AS scope,
        CAST(PARSE_JSON(payload)['effective_time'] AS TIMESTAMP) AS effective_time,
        broker_ts,
        ROW_NUMBER() OVER (PARTITION BY airport, restriction_type ORDER BY broker_ts DESC) AS rn
    FROM faa_ingest_tmr_raw WHERE airport IS NOT NULL
) WHERE rn = 1
  AND NOW() <= broker_ts + INTERVAL '2' HOUR;

-- Breach enriched with airport descriptor + weather + active restrictions.
CREATE MATERIALIZED VIEW arrival_context AS
SELECT
    b.airport,
    ap.airport_name,
    ap.tracon,
    b.bin_start,
    b.bin_end,
    b.demand,
    b.bin_capacity,
    b.overage,
    b.severity,
    b.runway_config,
    b.eta_quality,
    w.flight_category  AS wx_flight_category,
    w.wind_dir_degrees AS wx_wind_dir,
    w.wind_speed_kt    AS wx_wind_kt,
    w.visibility_sm    AS wx_visibility_sm,
    w.wx_string        AS wx_raw,
    r.restriction_type AS active_restriction_type,
    r.reason           AS active_restriction_reason
FROM capacity_breach_events b
LEFT JOIN airports     ap ON b.airport = ap.airport
LEFT JOIN metar_latest w  ON b.airport = w.station_id
LEFT JOIN active_restriction r ON b.airport = r.airport;

-- Incident trigger — ONE row per airport, the SAM escalation signal. Collapses the
-- per-bin breach storm (many overlapping HOP bins, each re-publishing as demand
-- climbs) into a single membership edge: an airport is "in an incident" while it has
-- any SEVERE (material + sustained) breach bin. The payload carries ONLY fields that
-- are INVARIANT for the life of the incident — airport identity, the AAR + runway in
-- effect, the SEVERE tier, and any restriction — deliberately NOT the live
-- demand/overage/eta_quality/window, which climb as arrivals bunch and would re-publish
-- the row on every tick. Result: GROUP BY airport yields a row that does not change
-- while the incident persists, so (with skip_deletes on the sink) it is published
-- exactly once when the airport ENTERS a severe incident and never on recovery. The
-- live demand, overage, eta_quality and weather for the brief are fetched by the
-- System-2 specialists via continuum-mcp. Non-severe/transient breaches never reach
-- SAM — Continuum does the alert-fatigue reduction here, in the data plane.
CREATE MATERIALIZED VIEW arrival_incident AS
SELECT
    airport,
    MAX(airport_name)              AS airport_name,
    MAX(tracon)                    AS tracon,
    MAX(bin_capacity)              AS bin_capacity,      -- the AAR in effect (stable per incident)
    MAX(runway_config)             AS runway_config,
    'SEVERE'                       AS severity,
    MAX(active_restriction_type)   AS active_restriction_type,
    MAX(active_restriction_reason) AS active_restriction_reason
FROM arrival_context
WHERE severity = 'SEVERE'
GROUP BY airport;

-- Incident STATE — the observable recovery edge. arrival_incident (above) is insert-only
-- (skip_deletes), so it fires on ENTRY to SEVERE and is silent on recovery — which is why
-- the operator "is never sure when it's reset." This view keeps a row for EVERY known
-- airport (from the static airports reference), LEFT JOINed to the current SEVERE set, so
-- its `status` flips SEVERE↔CLEAR. Sunk WITHOUT skip_deletes (see §10), the flip emits a
-- real edge in BOTH directions — an explicit all-clear the UI / verified-reset handshake /
-- smoketest can wait on. The SAM escalation trigger stays on arrival_incident; this is for
-- observability only (it never drives the fast/slow workflow).
CREATE MATERIALIZED VIEW arrival_incident_state AS
SELECT
    ap.airport,
    ap.airport_name,
    CASE WHEN i.airport IS NOT NULL THEN 'SEVERE' ELSE 'CLEAR' END AS status,
    i.bin_capacity,                                   -- AAR in effect (NULL when CLEAR)
    i.runway_config
FROM airports ap
LEFT JOIN arrival_incident i ON ap.airport = i.airport;


-- ═════════════════════════════════════════════════════════════════════════════
-- §10  OUTPUT SINKS — published to Solace (fl/faa/… prefix)
-- ═════════════════════════════════════════════════════════════════════════════

-- The agent trigger: one enriched breach event per airport/bin. skip_deletes so a
-- bin recovering (retraction) does not publish a noisy delete — SAM acts on inserts.
-- Shared contract: subscribed by sam/solutions/arrival-capacity entrypoints gateway.
-- Published by the sidecar egress bridge → fl/faa/arrival-capacity/breach/{airport}
-- (payload_shape: envelope, publish_deletes: skip). Materialized so the sidecar can
-- stream its change set over Feldera's /egress.
CREATE MATERIALIZED VIEW fl_arrival_breach_sink
AS SELECT * FROM arrival_context;

-- The agent ESCALATION trigger: one invariant row per airport per incident (see
-- arrival_incident). skip_deletes so a recovery does not publish. This — NOT the
-- per-bin breach sink above — is what the SAM arrival_capacity_gateway subscribes to,
-- so the fast/slow workflow runs ONCE per incident instead of once per breach bin.
-- Published by the sidecar egress bridge → fl/faa/arrival-capacity/incident/{airport}
-- (payload_shape: envelope, publish_deletes: skip).
CREATE MATERIALIZED VIEW fl_arrival_incident_sink
AS SELECT * FROM arrival_incident;

-- Observable incident STATE feed — one row per airport, status SEVERE↔CLEAR. NO
-- skip_deletes (unlike the two sinks above): the status flip must publish in BOTH
-- directions so consumers get an explicit all-clear on recovery. Subscribed by the API
-- (verified-reset handshake + UI green signal) and the smoketest — NOT by the SAM
-- gateway, which stays on the insert-only incident sink above.
-- Published by the sidecar egress bridge → fl/faa/arrival-capacity/incident-state/{airport}
-- (payload_shape: envelope, publish_deletes: envelope — the SEVERE↔CLEAR flip must
-- publish in BOTH directions, so deletes are NOT skipped).
CREATE MATERIALIZED VIEW fl_arrival_incident_state_sink
AS SELECT * FROM arrival_incident_state;

-- Live assessment feed for the UI (every bin, incl. deletes so the dashboard clears).
-- Published by the sidecar egress bridge → fl/faa/arrival-capacity/demand/{airport}
-- (payload_shape: envelope, publish_deletes: envelope — deletes included so the
-- dashboard clears a bin on retraction).
CREATE MATERIALIZED VIEW fl_arrival_demand_sink
AS SELECT * FROM capacity_assessment;


-- ═════════════════════════════════════════════════════════════════════════════
-- §11  MCP TOOL VIEWS — queried by continuum-mcp / System-2 specialist agents
-- ═════════════════════════════════════════════════════════════════════════════

-- Current demand picture per airport (latest bin per airport).
CREATE MATERIALIZED VIEW mcp_arrival_demand AS
SELECT airport, bin_start, bin_end, demand, bin_capacity, overage, eta_quality, exceeded
FROM capacity_assessment;

-- Rate currently in effect per airport (open-ended version).
CREATE MATERIALIZED VIEW mcp_airport_rate AS
SELECT airport, aar, adr, runway_config, valid_from
FROM airport_rate WHERE valid_to IS NULL;

-- Active restrictions (TMR) for the brief.
CREATE MATERIALIZED VIEW mcp_active_restrictions AS
SELECT airport, restriction_type, reason, scope, effective_time FROM active_restriction;

-- Latest weather per station for the brief.
CREATE MATERIALIZED VIEW mcp_weather AS
SELECT station_id, observed_at, flight_category, wind_dir_degrees, wind_speed_kt,
       visibility_sm, wx_string FROM metar_latest;

-- Downstream exposure: other airports also breaching right now (cascade view).
CREATE MATERIALIZED VIEW mcp_downstream_risk AS
SELECT airport, bin_start, demand, bin_capacity, overage
FROM capacity_assessment WHERE exceeded;

-- Inbound flights the System-2 brief can REPOSITION (hold / delay / reroute) to fit a
-- reduced arrival rate after a runway reconfiguration. One row per upcoming inbound
-- flight: callsign (flight_id), origin (dep_airport), ETA, and eta_source (confidence:
-- TTPFD/TBFM = firm, FILED = estimate). The repositioning specialist queries this view
-- for the hero airport and orders by eta to pick which arrivals to space out. This is
-- deliberately an MCP view (pulled live during the slow path), NOT part of the invariant
-- one-per-incident trigger payload — the flight list is volatile and would re-fire it.
CREATE MATERIALIZED VIEW mcp_inbound_flights AS
SELECT
    arr_airport  AS airport,
    gufi,
    flight_id,
    dep_airport,
    eta,
    eta_source
FROM flight
WHERE eta >= NOW();

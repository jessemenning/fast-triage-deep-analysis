# CLAUDE.md — FAA Fast/Slow ground truth

Ground-truth architecture + operations for this repo. Audience: developers and Claude.
For the business story see [BUSINESS.md](BUSINESS.md); to adapt it see [CUSTOMIZE.md](CUSTOMIZE.md);
for a quick start see [README.md](README.md).

## What this is

A three-layer **Solace Platform** demo — **Event Mesh → Continuum (Feldera) → Agent Mesh (SAM)** —
that detects **airport arrival-capacity breaches** (expected-arrival demand exceeding the published
arrival rate) and runs a **fast/slow AI** over them: a sub-second triage that cuts alert fatigue, and
an ESCALATE-only System-2 investigation that produces an advisory **decision brief**.

## Design decisions (what makes this one different)

- **Fully Solace-native data plane. No Postgres, no CDC.** Every feed — flights (FDPS), published
  rates (TTP Airport Information), ETAs (TTPFD), weather (METAR), restrictions (TMR) — arrives over
  Solace as real SWIM/SCDS feeds do. A rate change *is* an event. There is therefore **no
  `postgres_cdc_input` and no `reset_stale_cdc_state` self-heal** (unlike CDC-based variants).
- **"Versioned side input" in SQL, not a database.** `airport_rate` derives each rate version's
  `valid_from`/`valid_to` from the rate stream (`LEAD()` per airport), so a temporal join picks the
  "rate in effect" for each demand bin. Drop the AAR via a new rate event and every affected bin
  re-evaluates.
- **The base pipeline publishes the trigger directly.** `faa_program.sql` sinks per-bin breaches to
  `fl/faa/arrival-capacity/breach/{airport}` (the UI alert-storm feed) **and** collapses them into one
  invariant incident row per airport on `fl/faa/arrival-capacity/incident/{airport}` — the SAM
  escalation trigger — so `make up` needs no runtime data-product. The continuum-mcp generation engine
  is an *additive* capability, not a dependency.
- **The incident trigger collapses the alert storm in the data plane (one fire per incident).** The
  per-bin breach stream re-publishes many times per storm (overlapping HOP bins × demand climbing), so
  subscribing SAM to it fired one System-2 workflow per bin (alert-fatigue pileup). `arrival_incident`
  (§9) groups breaches to ONE row per airport, filtered to SEVERE, carrying only fields that stay
  **invariant** for the life of an incident (airport identity, AAR/runway in effect, severity tier,
  restriction) — NOT the climbing demand/overage/eta_quality. With `skip_deletes` that gives
  set-membership edge semantics: published once when an airport enters a severe incident, never on
  recovery. The gateway subscribes `incident/>`; the System-2 specialists fetch live demand/overage/
  weather via MCP for the brief. (Consequence: a re-fire while the airport is still latched in-incident
  won't re-trigger until it recovers — correct behaviour, but note it when testing back-to-back.)
- **An observable recovery edge sits alongside the insert-only trigger.** The incident sink above is
  silent on recovery (`skip_deletes`), which is why the demo used to feel "never sure when it's reset."
  A companion view `arrival_incident_state` (§9) keeps a row per KNOWN airport (the static `airports`
  reference LEFT JOINed to the SEVERE set) whose `status` flips SEVERE↔CLEAR, sunk **without**
  `skip_deletes` to `fl/faa/arrival-capacity/incident-state/{airport}`. So recovery publishes a real
  CLEAR edge — the UI latches green off it, and `POST /api/faa/scenario/reset` blocks until it sees
  CLEAR before returning `green:true` (a confirmed reset, not an optimistic guess). The SAM trigger is
  unchanged — it stays on the insert-only `incident` sink; the state feed is observability only.
- **Advisory only.** The Lead Briefer presents options + tradeoffs + provenance; it never issues a
  traffic-management initiative. This is a hard product rule (FAA will not accept automated TMIs).
- **Real-but-canned data.** A captured/generated SWIM corpus is replayed deterministically; the
  scenario (an unexpected wind-direction change) is scripted.

## Architecture & services

`docker-compose.yml` (no `db` service):

| Service | Port(s) | Role |
|---|---|---|
| solace | 55555 SMF, 8089 SEMP, 1883 MQTT, 8008 MQTT-WS, 9000 REST | Event Mesh |
| feldera | 8081 | Continuum engine + console (**stock** upstream `ghcr.io/feldera/pipeline-manager:latest`) |
| sidecar | 8079 | feldera-solace sidecar — bridges Solace ⇄ Feldera over HTTP (built from `../continuum-solace-connector`, OSS/non-FIPS) |
| init | — | one-shot: broker queue setup (`setup.sh`) + load/start the base pipeline (`init.sh`) |
| feed | — | `replay.py` — replays the canned SWIM corpus on a timeline |
| scenario | — | `scenario_driver.py` — the wind-shift scenario |
| api | 8080 | FastAPI + demo UI (`/demo.html`, `/api/faa/*`) |
| continuum-mcp | 8091 | Continuum as data products + triggers (generation engine) |
| ep-mcp | 8092 | Event Portal MCP (optional; `SOLACE_API_TOKEN`) |
| sam | 8800 | Agent Mesh runtime (private ECR image `solace-agent-mesh:2.409.2`) |

## The data plane — `stack/config/feldera/faa_program.sql`

The load-bearing, net-new file. Sections:

1. **§1 Ingest tables** (`faa_ingest_*_raw`) — plain tables fed by the **sidecar** over HTTP `/ingress`
   (not an in-SQL connector). The sidecar runs `payload_mode: wrap`: raw JSON body in `payload`, with
   topic-level captures + `solace_topic` + `broker_ts` merged in as same-named JSON fields. Queue→table
   mapping lives in `stack/sidecar/config.yaml`; the `fl-*` queues are created by `setup.sh`.
2. **§2 Dedup** (`flightplan_latest`, `eta_latest`) — `LOCAL`; latest row per GUFI (`ROW_NUMBER`) over a
   1-day `NOW()` window so the TopK state is bounded (see *Rolling out a change* under Operating it).
3. **§3 `airports`** — tiny static reference as an inline `VALUES` view (the only non-streamed data).
4. **§4 `airport_rate`** — versioned rate with `valid_from`/`valid_to` derived via `LEAD()`; plus
   `reset_epoch` — latest `faa/scenario/reset` time per airport (ingested from the `fl-faa-reset` queue),
   the input to §7's reset-epoch re-scoring.
5. **§5 `flight`** — one keyed inbound flight with best ETA + `eta_source` (TTPFD>TFMS>FILED).
6. **§6 `arrival_demand`** — rolling **HOP** window (60-min size, 15-min slide) count per airport.
7. **§7 `capacity_assessment`** — demand vs the rate in effect (temporal join), `overage`, `eta_quality`.
   **Reset-epoch re-scoring**: a bin whose rate-in-effect predates the airport's last reset
   (`reset_ts > r.valid_from`) is re-scored against the CURRENT restored rate, so a ↺ Reset clears old
   frozen-degraded SEVERE bins at once instead of waiting ~1 h for the event-time window to slide past.
8. **§8 `capacity_breach_events`** — hysteresis: material (≥10% of AAR) **and** sustained (prev bin
   also over) → `severity`.
9. **§9 context** — `metar_latest`, `active_restriction`, `arrival_context` (the enriched breach),
   `arrival_incident` (one invariant row per SEVERE airport — the trigger) and `arrival_incident_state`
   (one row per KNOWN airport with a `status` that flips SEVERE↔CLEAR — the observable recovery edge).
10. **§10 sinks** — plain `MATERIALIZED VIEW`s the **sidecar** streams over `/egress` and publishes to
    Solace (`payload_shape: envelope` → the same `{"insert":…}`/`{"delete":…}` bytes consumers already
    parse). Mapping in `stack/sidecar/config.yaml`: `fl_arrival_breach_sink` →
    `fl/faa/arrival-capacity/breach/{airport}` (`publish_deletes: skip`, per-bin, UI alert storm);
    `fl_arrival_incident_sink` → `fl/faa/arrival-capacity/incident/{airport}` (`publish_deletes: skip`, one
    invariant row per airport — the SAM escalation trigger, see `arrival_incident` in §9);
    `fl_arrival_incident_state_sink` → `fl/faa/arrival-capacity/incident-state/{airport}`
    (`publish_deletes: envelope` — the SEVERE↔CLEAR edge the UI / verified reset / smoketest wait on);
    `fl_arrival_demand_sink` → `fl/faa/arrival-capacity/demand/{airport}` (`publish_deletes: envelope`, UI).
11. **§11 `mcp_*` views** — queried by the System-2 specialists / continuum-mcp.

**Feldera dialect notes (this is net-new; can't be compiled offline):**
- HOP follows the verified nexmark signature `HOP(TABLE t, DESCRIPTOR(ts), <slide>, <size>)`. LATENESS
  is only a state-GC optimization (omitted here; fine for a demo). If a build rejects windowing over
  the `flight` view, the documented fallback (tumbling floor-bin, or a `NOW()`-relative "next 60 min"
  demand) is in the `arrival_demand` comment.
- `airports` uses an inline `VALUES` view; if a build rejects it, publish the list as a reference feed.
- `capacity_assessment` uses a temporal (inequality) join for the rate-in-effect.
- Connector key is plural `'connectors'` as a JSON-array **string**; output views must be `MATERIALIZED`;
  env vars are `sed`-substituted by `init.sh`. Publishers must send **raw bytes** (not `str`) or
  `PARSE_JSON(data)` returns NULL for every field; timestamps are UTC `YYYY-MM-DD HH:MM:SS`.

Broker queues (one per feed, default VPN) are provisioned by `stack/config/solace/setup.sh` — including
`fl-faa-reset` (← `faa/scenario/reset`), which feeds the §7 reset-epoch re-scoring.

## Canned data & scenario

- `stack/data/sample-corpus.jsonl` — committed offline baseline (generated by `gen_sample_corpus.py`),
  tuned so every airport's rolling-hour demand stays **below** its AAR until the scenario fires.
- `stack/feed/replay.py` — replays the corpus; resolves now-relative tokens (`@now`, `@+N`) so canned
  ETAs/rates/weather always land in the live window. `REPLAY_SPEED`, `REPLAY_LOOP`.
- `stack/data/capture.py` — one-time tool to record a fresh corpus from a live SWIM broker.
- `stack/scenario/scenario_driver.py` — the wind shift: on a trigger to `faa/scenario/windshift`
  (fired by `demo/burst.sh`, i.e. `make demo`), it publishes a degraded TTP rate for `HERO_AIRPORT`
  (default **KEWR**, AAR 44→26 as the wind swings SW→NE and runways reconfigure `22L|22R`→single-runway
  `4R`), a gusty VFR METAR carrying the shifted wind (`wdir` ~210→040, 30G42) + a GDP restriction, and a
  burst of bunched arrivals; after a hold it restores the normal rate + SW wind. `SCENARIO_AUTOSTART=true`
  self-fires once after warmup. Also subscribes `faa/scenario/reset` (the UI **↺ Reset** button → `POST
  /api/faa/scenario/reset`): `reset()` restores the normal rate + SW wind / `22L|22R` flow, cancels the
  restriction, and evicts the burst's deterministic GUFIs (far-future ETA **and** non-`ACTIVE`
  flight-plan). **The deterministic clear, though, comes from reset-epoch re-scoring (§7), not the
  eviction**: old demand bins are frozen against the degraded AAR by the temporal join, and ordinary
  baseline demand exceeds that degraded rate (but not the real one), so without re-scoring a reset would
  take ~1 h to age the frozen bins out. The reset marker (`faa/scenario/reset` → `reset_epoch`) makes
  `capacity_assessment` re-score any bin whose rate-in-effect predates the reset against the CURRENT
  restored rate → the SEVERE incident clears in well under a second, and `arrival_incident_state`
  publishes the CLEAR edge. `POST /api/faa/scenario/reset` **blocks until it observes that CLEAR**
  (default 15 s) and returns `green:true` only then — so the UI button and the smoketest know the airport
  is actually green, not just that the POST returned (verified: reset → green in ~0–400 ms). `run()`
  acquires its lock with an **8 s blocking timeout** (not a non-blocking drop), and `reset()` sets the
  abort flag only for a run that was already in progress when the reset began (otherwise a Reset-then-Wind-shift
  fired within the same second had its fresh run killed by the late flag), so a Reset-then-Wind-shift
  in quick succession isn't dropped while the aborted hold unwinds — the demo is **repeatable**: fire →
  brief → Reset → green → fire a FRESH incident (verified 3× back-to-back, each firing at ~+3 s). A new
  wind shift after a reset publishes a newer rate version (valid_from > reset_ts), so its bins keep their
  own degraded rate and still fire — the reset only neutralises STALE windows. Flight freshness is
  demo-tuned to 15 min (was 45); the ~90 s replay loop keeps baseline flights fresh, so this never
  sawtooths demand.

## The AI — `sam/solutions/arrival-capacity/`

- **Gateway** (`entrypoints/arrival_capacity_gateway.yaml`) subscribes `fl/faa/arrival-capacity/incident/>`
  (the collapsed one-per-incident trigger, NOT the per-bin `breach/>`),
  runs `ArrivalCapacityWorkflow`, republishes to `solutions/arrival-capacity/decision/{airport}`.
- **Workflow** (`workflows/ArrivalCapacityWorkflow.yaml`):
  - **FAST** `triage` node — native OpenAI Decisions (`models/arrival_decider.yaml` → MaaS LiteLLM
    `/v1/decisions`, model `microsoft-decision-1`). Questions: `will_materialize`, `self_resolves`, `impact`, `is_upstream_symptom`,
    `action` (ESCALATE/GROUP/SUPPRESS), `case_priority`. Most breaches suppress/group here.
  - **`publish_triage`** tool node — interim verdict on ESCALATE (fires `solutions/arrival-capacity/triage`).
  - **SLOW** stage, `when`-gated on `action == "ESCALATE"` (the quoting is load-bearing): specialists
    `weather` / `downstream` / `trend` run in parallel (each one live `continuum_preview_query`), then
    `brief` (Lead Briefer, Sonnet) synthesises the advisory decision brief (headline, what's happening,
    why, options with tradeoffs, downstream airports at risk, provenance).
- **Models**: platform `general` (Haiku; triage + specialists) and `investigation` (Sonnet; briefer),
  kept on separate pools so the slow path can't starve the fast path (HTTP 429 → null decision).
- **Alternative**: `alternatives/ArrivalCapacityWorkflow.llm-decider.yaml` swaps the decision node for
  an agent (`arrival_triage_decider`) for A/B.
- Requires `SAM_FEATURE_WORKFLOW_DECISION_NODE=true` on the host CLI and the runtime.

Platform layer (`sam/manifests/dev.yaml`): models `general`/`investigation`; connectors
`continuum-mcp`, `event-portal-mcp`, `event-mesh-publish`; plus the generation-engine skills +
`SolutionBuilder` agent.

## The generation engine (build data objects + agents live)

The runtime generation engine:
- **`continuum-mcp/`** — renders Feldera SQL from Jinja templates and PUTs a new live pipeline at
  runtime (`create_data_product`/`create_trigger`), creating broker queues + Solace in/out and
  (optionally) Event Portal objects. Base pipeline alias: `arrival-capacity`; catalog under
  `continuum-mcp/catalog/` describes the FAA streams.
  - **KNOWN LIMITATION (sidecar migration):** the generated SQL still embeds `solace_input`/
    `solace_output` connectors (`continuum-mcp/continuum_mcp/templates/*.j2`), which **stock Feldera
    rejects** ("transport.name solace_input is not known"). The base demo was migrated to stock
    Feldera + the external sidecar, but this runtime generation path was **not** — it needs the old
    `feat-solace-connector` fork, or a follow-on that emits stock SQL and teaches the sidecar the new
    queues/views (reconfigure+restart, or a per-product sidecar). `make up` and the base fast/slow
    flow do not use generation, so they are unaffected. Querying existing views over HTTP (the System-2
    MCP tool views, §11) works on stock Feldera; only *creating new Solace-wired pipelines* is gated.
- **`sam/agents/SolutionBuilder.yaml` + `sam/skills/` + `sam/scripts/deploy-solution.sh`** — an LLM
  agent that, from one sentence, discovers Continuum data products and authors a new
  `data product → trigger → gateway → workflow → decision node → decision` solution as declarative
  YAML, then `deploy-solution.sh` applies it. See [CUSTOMIZE.md](CUSTOMIZE.md) §5.
- Caveat: `sam/builder/builder_agent.stock.yaml` is the demo image's stock Builder; refresh it from
  the running 2.409.2 image + re-run `builder/patch_builder.py` if you use the in-runtime `/builder`.

## API & UI — `stack/api/`

FastAPI (`/api/faa/*`): `demand` (capacity_assessment), `rates` (mcp_airport_rate), `breaches`
(arrival_context), `reference/{rates|weather|restrictions}` (side-input feeds for the continuum inspector),
`latest/{stream}`, `agent-state` (AI on/off), `scenario/start`, `scenario/reset`, `inject/*`,
`stream/{demand|breach|decision|triage}` (SSE). `solace_receiver.py` subscribes the live topics
(demand, incident-state, breach, decision, triage) and retains latest state; `scenario/reset` uses the
retained `incident_state` to block until the hero reads CLEAR (verified reset handshake). `static/demo.html` is the main dashboard (AAR vs demand per airport, the
wind shift, the alert-storm → one-incident collapse, the decision brief); Solace brand colors.

## Operating it

| Command | Does |
|---|---|
| `make up` | compose up + `apply` (platform) + `apply-reference` (solution) |
| `make demo-ready` | `up` + `prewarm` — the one-command cold-start→warm path (run once per machine / after `clean`) |
| `make prewarm` | compile every `demo/products/*.json` into the persistent cache, then clear the records (live creates → cache hits) |
| `make demo` | fire the wind shift (`faa/scenario/windshift`) |
| `make ops` | terminal console of `fl/faa/arrival-capacity/#` + `solutions/#` |
| `make create-product` | demo the generation engine (build a product + trigger via MCP) |
| `make reset` | redeploy the reference solution |
| `make pipeline-status` / `make fq SQL=…` | base pipeline status / ad-hoc query |
| `make down` / `make clean` | stop (keep / drop volumes); `clean` wipes the warm compile cache too |
| `make up-ephemeral` | bring up without the persistent cache (ephemeral engine; cold compiles — CI/true-cold) |

**Warm path (so a live product create is a cache hit, ~seconds, not a ~7–8 min cold compile):** the
Feldera compile cache persists by **default** (the `docker-compose.cache.yml` overlay is always on —
`COMPOSE` in the Makefile), in the `feldera_home`/`feldera_cargo` volumes. It survives `down`/`up`
**and host reboots** (verified: base pipeline returns `program: Success`, starts in ~0 s). `delete_data_product`
drops the pipeline + queues but **not** the compile cache, so recreating identical SQL reuses the cached
binary (verified: 468 s cold → 37 s recreate). `make prewarm` compiles the demo products once and clears
their records, so the demo's "build it live" create is a cache hit from a clean slate. You pay the cold
compile **once per machine** — only `make clean` (down -v) wipes it, after which re-run `make demo-ready`.

**Iterate loop:** base-SQL edit → `make clean && make up` (cold compile) — or `make down && make up`
to keep the warm cargo cache (init re-PUTs the changed SQL; it only *skips* the upload when the pipeline
is already Running, which a fresh `up` never is). SAM prompt/model edit → `make apply-reference`. SAM
node-type/topology change → `make apply-reference` + `docker compose restart sam`. UI edit → live
(bind-mounted). Offline MCP unit check → `cd continuum-mcp && uv run pytest -q`.

**Gotcha — changing a gateway's subscription topic:** the event-mesh gateway's durable queue
(`solace-agent-mesh/q/eventmesh/<gwID>/<handler>`) keeps old topic subscriptions when the config
changes — SAM *adds* the new one but never prunes the old, so on an already-deployed box the gateway
ends up consuming both. A clean `make down && make up` on Solace without a persisted queue recreates it
correctly; on a box that had the old topic, delete the stale sub via SEMP (`DELETE
/SEMP/v2/config/msgVpns/default/queues/<urlenc-queue>/subscriptions/<urlenc-topic>`) or it keeps firing.

**Gotcha — never run plain `docker compose`; always use `make` or `$(COMPOSE)`:** the stack runs with
the cache overlay (`docker compose -f docker-compose.yml -f docker-compose.cache.yml`, the Makefile's
`COMPOSE`). A bare `docker compose up -d --build <svc>` (no overlay) sees a *different* config than the
running stack and **recreates containers inconsistently** — it has silently dropped the Feldera pipeline
(feldera recreated, one-shot `init` doesn't re-run → `/api/faa/*` 404/500 until `make up`). To rebuild one
service, use the overlay: `docker compose -f docker-compose.yml -f docker-compose.cache.yml up -d --build <svc>`
(verified: rebuilds only that service, pipeline stays `Running`). Recovery from an inconsistent state is a
clean `make down && make up`.

**Gotcha — a "healthy" `sam` container can have a DEAD trigger (gateways=0):** the event-mesh
gateway connects to the broker once at reconcile and the reconciler does NOT retry a failed one. If `sam`
starts before `solace` is DNS-resolvable (the inconsistent-stack state above), the `arrival_capacity_gateway`
fails with `SESSION_HOST ... could not be resolved`, SAM logs `reconciliation complete ... gateways=0`, and
`docker compose ps` still shows `sam` healthy — but nothing consumes `fl/faa/arrival-capacity/incident/>`, so
the wind shift never reaches SAM. `depends_on: solace {service_healthy}` (already on the sam service) covers a
full `make up`, but NOT a targeted recreate of `solace` under an already-running `sam`. **Detect:**
`make check-gateway` (asserts a bound consumer flow on the eventmesh queue — the real readiness signal, not
container health); it also runs automatically at the end of `make up` and fails the target if the trigger is
down. **Fix:** `docker compose -f docker-compose.yml -f docker-compose.cache.yml restart sam` (re-reconciles
now that DNS resolves), or a clean `make down && make up`.

**Gotcha — `spooledMsgCount` is CUMULATIVE, not queue depth.** On this broker build
(`solace/solace-pubsub-standard`, SEMP 10.26) a queue's `spooledMsgCount` counts every message ever spooled
and never decrements on consume+ack, and `deletedMsgCount` only counts TTL/admin/DMQ removals (not consume-acks)
— so a fully drained, fully acked queue still reads `spooledMsgCount: N, deletedMsgCount: 0`. Do NOT read these
as "messages stuck on the queue." The only correct backlog gauge is
`q_backlog = lastSpooledMsgId - highestAckedMsgId` (0 = drained), corroborated by the flow's `ackedMsgCount`
and the queue's `txUnackedMsgCount`. This is the same trap documented in the Solace perf harness
(`~/feldera-solace/perf/solace/README.md` §"three lag surfaces" and `solace_perf/broker.py` `queue_stats`).

### Rolling out a change to `faa_program.sql`

The SQL is baked into the `init` image at build time (`stack/config/init.Dockerfile` copies
`feldera/`), and `init.sh` is idempotent: when the pipeline is already **Running** it skips the
upsert, the compile wait and the start. So `make up` / `docker compose up -d --build` on a live
stack does **not** deploy an edited program. Three steps instead (≈1 min with the warm compile cache):

1. **Stop the pipeline** — drops in-memory state and resets the scenario, so do it between demo runs:
   `curl -X POST 'http://localhost:8081/v0/pipelines/arrival-capacity/stop?force=true'`, then wait for
   `make pipeline-status` to show `Stopped`.
2. **Rebuild the init image** so it carries the new SQL: `docker compose build init`.
3. **Re-run init**: `docker compose up -d --no-deps --force-recreate init`, then `docker compose logs -f init`
   until `Pipeline is Running` (it does PUT → `CompilingRust` → start → `Running`).

Verify with `make pipeline-status` (program `Success`, deployment `Running`, `program_version` bumped),
`curl -s localhost:8081/v0/pipelines/arrival-capacity/dataflow_graph` when the change is structural, and
`make smoketest`. To validate SQL *before* stopping anything: `POST /v0/validate_program` with
`{"program_code": ..., "ir": true}` — substitute the `${FL_*}` tokens first (see `prepare_sql` in `init.sh`).

State notes (audited 2026-10-29 with `/feldera-audit`): the §2 dedups carry a 1-day `NOW()` filter
*inside* the `ROW_NUMBER` subquery because a TopK otherwise keeps every row ever seen per key
(`LATENESS` does not bound it), and the flight-plan subquery lists its columns so the raw `data` payload
is projected away before the window. After a restart the TopK trace in `/circuit_profile` grows for the
first 24 h, then plateaus (≈150 MB in the replay loop).

## Smoke test

`demo/smoketest.py` (`make smoketest`) is the end-to-end readiness check for the base
`arrival-capacity` reference solution: it asserts services + pipeline health, toggles the AI on,
**resets the primary airport to green first and waits for the broker's incident-state CLEAR edge**
(a confirmed clean incident edge, not a fixed sleep — see the latch semantics in the incident-trigger
design decision), fires the wind shift via `POST /api/faa/scenario/start`, and waits on the broker for
incident → triage → decision + System-2 brief. Exit 0 = record-ready. Knobs: `AIRPORT=` (default KEWR),
`TIMEOUT=` (default 150), `RESET_WAIT=` (max seconds for the CLEAR edge, default 15), `NORESET=1`.
Needs `paho-mqtt`.
(`demo/verify-solution.py` / `make verify-solution SLUG=… TRIGGER=…` is the sibling check for a
*generated* solution by slug.)

# CUSTOMIZE.md — change the use case

A map of the knobs for adapting this demo, from "tune the numbers" to "re-domain it entirely."
Read [CLAUDE.md](CLAUDE.md) first for how the pieces fit; this file is the *where do I edit* index.
Line numbers are approximate — grep the named symbol if they've drifted.

## The shape you're changing

```
SWIM/SCDS feeds ──Event Mesh──▶ Continuum (rolling demand vs published AAR) ──trigger──▶ SAM (triage + brief) ──▶ decision
 replay + scenario              base pipeline SQL (faa_program.sql)          event-mesh gateway → fast/slow workflow
```

Four editable surfaces, roughly in order of how often you'll touch them:

| Surface | Lives in | Change it to… |
|---|---|---|
| **Canned data** (airports, rates, flights, the scripted scenario) | `stack/data/`, `stack/scenario/scenario_driver.py` | retune who breaches and how the wind shift looks |
| **Replay feed** (what flows, how fast) | `stack/feed/replay.py`, `stack/data/*.jsonl` | change the traffic and its pacing |
| **Continuum computation** (demand, the rate join, the breach) | `stack/config/feldera/faa_program.sql` | change the detection logic itself |
| **AI decision** (triage questions, specialists, the brief) | `sam/solutions/arrival-capacity/` | change what the agent decides and how |

The **shared contract** tying Continuum → SAM together is the trigger topic root
`fl/faa/arrival-capacity/breach/` — published by the Feldera `fl_arrival_breach_sink`
(`faa_program.sql`) and subscribed by the gateway
(`sam/solutions/arrival-capacity/entrypoints/arrival_capacity_gateway.yaml`).
Change one, change both.

---

## 1. Retune without re-domaining (start here)

The quickest, lowest-risk changes — numbers only.

**The primary airport + its normal/degraded rate** → `stack/scenario/scenario_driver.py`:
- `HERO_AIRPORT` (default `KEWR`), `NORMAL_AAR` (44), `DEGRADED_AAR` (26), `SCENARIO_BURST`
  (16 extra arrivals), `SCENARIO_HOLD` (how long the degraded state holds). Also settable via
  `.env` (`HERO_AIRPORT`) / compose env.

**Baseline demand per airport** (kept below AAR so nothing breaches until the scenario) →
`stack/data/gen_sample_corpus.py`, the `AIRPORTS` table (`aar, adr, runway_config, inbound`).
Raise an airport's `inbound` toward its `aar` to make it breach more easily. Re-run
`python3 stack/data/gen_sample_corpus.py` to regenerate `sample-corpus.jsonl`, then
`docker compose restart feed`.

**The breach thresholds** (hysteresis margin, severity bands) → `faa_program.sql`,
`capacity_breach_events` (~§8): the `overage >= GREATEST(CAST(0.10 * bin_capacity AS INTEGER), 1)`
margin and the SEVERE/MODERATE/MARGINAL `severity` CASE.

**The rolling window** (60-min size, 15-min slide) → `faa_program.sql`, `arrival_demand`
HOP arguments (~§6).

---

## 2. Reshape the data

**`stack/feed/replay.py`** — the replay engine. Reads `stack/data/*.jsonl` and re-publishes on
the recorded timeline. The now-relative tokens (`@now`, `@+<seconds>`) are resolved here — see
`stack/data/README.md` for the record format. `REPLAY_SPEED` / `REPLAY_LOOP` env knobs.

**`stack/data/capture.py`** — capture a fresh corpus from a live SWIM broker (one-time
authoring tool). **`gen_sample_corpus.py`** — regenerate the offline baseline.

**`stack/scenario/scenario_driver.py`** — the wind-shift scenario. Triggered by a
message on `faa/scenario/windshift` (`demo/burst.sh`), or `SCENARIO_AUTOSTART=true`.

**Gotcha:** publish raw **bytes** (`bytearray(...)`), never a `str` — a str payload gets SDT
framing that makes Feldera's `PARSE_JSON(data)` return NULL for every field. Timestamps are
stamped UTC (`YYYY-MM-DD HH:MM:SS`) so they cast straight to SQL `TIMESTAMP`.

---

## 3. Change the Continuum computation

**`stack/config/feldera/faa_program.sql`**. The load-bearing objects:

| What | Object | Section |
|---|---|---|
| Solace ingest (FDPS/TTP/METAR) | `faa_ingest_*_raw` tables | §1 |
| Latest-per-key dedup | `flightplan_latest`, `eta_latest` | §2 |
| Static airport reference | `airports` (inline VALUES) | §3 |
| **Versioned rate (valid_from..valid_to)** | `airport_rate` (LEAD over the rate stream) | §4 |
| Keyed inbound flight with ETA | `flight` | §5 |
| **Rolling demand (HOP 60m/15m)** | `arrival_demand` | §6 |
| **Demand vs rate-in-effect** | `capacity_assessment` | §7 |
| **Breach + hysteresis** | `capacity_breach_events` | §8 |
| Weather/restriction context | `metar_latest`, `active_restriction`, `arrival_context` | §9 |
| Solace sinks | `fl_arrival_breach_sink`, `fl_arrival_demand_sink` | §10 |
| MCP tool views | `mcp_*` | §11 |

**Feldera caveats (this is net-new SQL):**
- The HOP table-valued function follows the verified nexmark signature
  `HOP(TABLE flight, DESCRIPTOR(eta), <slide>, <size>)`. If a build rejects windowing over
  the `flight` view for lack of a watermark, the documented fallback (a tumbling floor-bin
  GROUP BY, or a `NOW()`-relative "next 60 min" demand) is in the `arrival_demand` comment.
- `airports` uses an inline `VALUES` view. If a build rejects it, publish the airport list as
  a small reference feed over Solace and ingest it like the other feeds.
- `capacity_assessment` uses a temporal (inequality) join to pick the rate in effect for a bin.

**Deploy:** a base-SQL edit needs a pipeline recreate — the sure path is `make clean && make up`
(cold Feldera compile, a few minutes).

---

## 4. Change the AI decision

**`sam/solutions/arrival-capacity/`:**

```
workflows/ArrivalCapacityWorkflow.yaml  ← fast triage decision node + System-2 fan-out
models/arrival_decider.yaml             ← the OpenAI Decisions model (endpoint + key only)
agents/arrival_weather_context.yaml     ← slow specialist (parallel) — weather/restriction
agents/arrival_downstream_impact.yaml   ← slow specialist (parallel) — cascade risk
agents/arrival_capacity_trend.yaml      ← slow specialist (parallel) — trajectory
agents/arrival_lead_briefer.yaml        ← slow synthesis (depends_on all three) — the brief
agents/arrival_triage_decider.yaml      ← no-tools LLM decider (used by the alternative)
entrypoints/arrival_capacity_gateway.yaml ← event-mesh gateway (subscribes the trigger topic)
manifests/dev.yaml                      ← what deploys
alternatives/…llm-decider.yaml          ← A/B: plain-LLM decider instead of the Decisions node
```

- **The triage questions** live in `ArrivalCapacityWorkflow.yaml` under the `triage` node's
  `questions:` block: `will_materialize`, `self_resolves`, `impact`, `is_upstream_symptom`,
  `action` (ESCALATE/GROUP/SUPPRESS), `case_priority`. The `detection:` prompt defines the
  field semantics and the triage policy — this is where you change *how* the agent judges.
- **The escalation gate**: the slow System-2 stage (`weather`/`downstream`/`trend` → `brief`) is
  `when`-gated on `action == "ESCALATE"`. **The `when:` quoting is load-bearing** — the
  interpolated value must be quoted to become a string literal, or an unquoted gate nulls the
  whole decision. To also brief GROUP-ed breaches, widen each gate to
  `'"{{triage.output.answers.action.choice}}" in ["ESCALATE","GROUP"]'`.
- **Advisory only** is a hard rule: the Lead Briefer presents options with tradeoffs and
  provenance; it never issues a TMI. Keep that framing if you edit the prompts.

**Deploy:** prompt/question/model edits propagate with `make apply-reference` alone. A
node-type or node-topology change needs `docker compose restart sam` after the apply.

---

## 5. Build a NEW data object or agent live (the generation engine)

This demo ships the runtime generation engine (`continuum-mcp` + the `SolutionBuilder` SAM
agent). To build a brand-new Continuum data product + trigger + agent on stage from one
sentence, talk to the SolutionBuilder (see `sam/agents/SolutionBuilder.yaml`) or call
`make create-product` for a scripted example. The prebuilt arrival-capacity solution runs
independently of this — the base pipeline publishes its breach trigger directly.

---

## 6. Re-domain it entirely

The checklist, in
dependency order:
1. **Canned data** — rewrite `stack/data/gen_sample_corpus.py` (entities + the rate/limit each
   is judged against) and the scenario in `stack/scenario/scenario_driver.py`.
2. **Ingest** — change the `faa_ingest_*` tables + topic patterns in `faa_program.sql` to your
   feeds; keep the raw-bytes + UTC-timestamp conventions.
3. **Computation** — rewrite the demand/rate/breach logic; update the `mcp_*` tool views.
4. **Trigger** — re-point the `fl_arrival_breach_sink` topic and **the gateway's matching
   subscription** (§ shared contract above).
5. **Decision** — rewrite the `questions` + `detection:` prompt and the specialist instructions.
6. **Rename** `demo/burst.sh`, the UI, and the docs.

Keep the brand rules: "Solace Platform" never "PubSub+"; never name the stream engine in
user-facing strings (say "Continuum").

# FAA Fast/Slow — Arrival Capacity Demand vs Published Rate

A laptop-scale demo of the **Solace Platform** three-layer stack — **Event Mesh → Continuum
(stream processing) → Agent Mesh** — applied to air traffic management.

Continuum ingests flight plans and flight data and continuously maintains **expected-arrival
demand per airport**, raising an event whenever demand exceeds the airport's **published arrival
rate (AAR)**. A **two-speed AI** then turns the raw breach-alert storm into a triaged, explained
**decision brief** for a traffic manager — *advisory only*, with every claim traced to a source feed.

- **Why it matters:** [BUSINESS.md](BUSINESS.md)
- **How it fits together (ground truth):** [CLAUDE.md](CLAUDE.md)
- **Adapt / re-domain it:** [CUSTOMIZE.md](CUSTOMIZE.md)

## Architecture at a glance

```
Replayed SWIM/SCDS over Solace:
  FDPS position + flightPlan · TTP Airport Information (AAR) · TTPFD (ETA) · METAR · TMR
        │
        ▼  feldera-solace sidecar (Solace queues → Feldera /ingress; /egress → Solace topics)
        ▼
  Continuum (stock Feldera)  flight → arrival_demand (rolling 60m) → demand vs published AAR → breach (hysteresis)
        │  fl/faa/arrival-capacity/breach/{airport}
        ▼
  Agent Mesh  gateway → FAST triage (OpenAI Decisions: escalate / group / suppress)
                         └─ ESCALATE → System-2 specialists (weather · downstream · trend) → Lead Briefer
        │  solutions/arrival-capacity/decision/{airport}
        ▼
  Demo UI (http://localhost:8080/demo.html)
```

Fully **Solace-native**: flights, rates, weather and restrictions all flow as events — no database,
no CDC. Published rates (TTP Airport Information) are a versioned stream; Continuum reconstructs the
"rate in effect" for each bin in SQL.

## Quick start

Prerequisites: Docker; the `sam` CLI `sam-2.409.2` on PATH; a filled `.env`; `docker login` to the
SAM image registry (or `make pull-image`).

```bash
cp .env.example .env            # fill LLM_SERVICE_* (chat agents AND the LiteLLM /v1/decisions node)
make demo-ready                 # bring up the platform AND pre-warm the demo products
                                #   (run once per machine; `make up` alone also works)
# then, in two terminals:
make ops                        # watch triggers + decisions on the mesh
make demo                       # fire the wind shift at the primary airport (KEWR)
```

`make demo-ready` adds a one-time **pre-warm**: it compiles the demo products into Feldera's
persistent compile cache, so a *live* "build a solution" create (`make create-product`, or the
`faa-generate-solution` skill) is a **cache hit (~seconds)** instead of a ~7–8 min cold compile. The
cache survives `down`/`up` and reboots — you only pay the cold compile once (until `make clean`).

Open **http://localhost:8080/demo.html** to watch published AAR vs live demand per airport, the
wind shift degrade the primary airport, the alert storm collapse into one escalated incident, and the
decision brief with options and downstream airports at risk.

| Command | What it does |
|---|---|
| `make up` | compose up + deploy platform + arrival-capacity solution (warm cache on by default) |
| `make demo-ready` | `up` + `prewarm` — cold-start → fully warm in one command (live creates become cache hits) |
| `make prewarm` | pre-compile the demo products into the persistent cache, then clear the records |
| `make demo` | publish the `faa/scenario/windshift` trigger (runway reconfig → primary AAR drops + arrival burst → breach) |
| `make ops` | live terminal console of `fl/faa/arrival-capacity/#` + `solutions/#` |
| `make create-product` | demo the live generation engine: build a Continuum product + trigger via MCP |
| `make reset` | re-arm: redeploy the reference solution |
| `make down` / `make clean` | stop (keep / drop volumes) |

## Data streams

Ingest (replayed canned SWIM):

| Feed | Transport | Topic | Feldera table |
|---|---|---|---|
| FDPS position | Solace SMF | `FDPS/position/{…}/{gufi}` | `faa_ingest_position_raw` |
| FDPS flight plan | Solace SMF | `FDPS/flightPlan/{…}/{gufi}` | `faa_ingest_flightplan_raw` |
| TTPFD ETA | Solace SMF | `TTP/flightData/{gufi}/{arrival}/{eta_source}` | `faa_ingest_eta_raw` |
| TTP Airport Information (AAR) | Solace SMF | `TTP/airportInfo/{airport}` | `faa_ingest_rate_raw` |
| TMR restriction | Solace SMF | `TTP/restriction/{airport}/{type}` | `faa_ingest_tmr_raw` |
| METAR | **nisshi (Kafka)** | `aviation.metar` | `faa_ingest_metar_kafka` (native `kafka_input`) |

METAR rides the [nisshi](https://nisshi.io/) Kafka on-ramp (a Solace-backed, Kafka-compatible
broker): Feldera reads it with its native `kafka_input` connector and writes `aviation.metar.normalized`
back to nisshi via `kafka_output`, while the pipeline still sinks the incident/brief to Solace through
the sidecar. Peek either Kafka topic with `make nisshi-peek TOPIC=aviation.metar.normalized`.

Output (Continuum → mesh):

| Stream | Transport | Topic |
|---|---|---|
| Arrival-capacity breach (agent trigger) | Solace | `fl/faa/arrival-capacity/breach/{airport}` |
| Live demand vs capacity (UI) | Solace | `fl/faa/arrival-capacity/demand/{airport}` |
| Normalized weather (reverse Kafka leg) | nisshi (Kafka) | `aviation.metar.normalized` |
| Agent triage (interim) | Solace | `solutions/arrival-capacity/triage` |
| Agent decision + brief | Solace | `solutions/arrival-capacity/decision/{airport}` |

## Services & ports

| Service | Port | Role |
|---|---|---|
| solace | 55555 / 8089 / 1883 / 8008 / 9000 | Event Mesh (SMF / SEMP / MQTT / MQTT-WS / REST) |
| nisshi | 9092 | Kafka-compatible broker (Solace-backed, OSS) — the METAR on-ramp Feldera reads/writes over Kafka |
| feldera | 8081 | Continuum engine + console (stock upstream) |
| sidecar | 8079 | feldera-solace sidecar — bridges Solace ⇄ Feldera (built from `../continuum-solace-connector`) |
| api | 8080 | FastAPI + demo UI (`/demo.html`, `/api/faa/*`) |
| continuum-mcp | 8091 | Continuum as data products + triggers (generation engine) |
| ep-mcp | 8092 | Event Portal MCP (optional; set `SOLACE_API_TOKEN`) |
| sam | 8800 | Agent Mesh runtime |

## Real-but-canned data

The demo runs offline on a captured/generated SWIM corpus replayed on a timeline. See
[stack/data/README.md](stack/data/README.md) — including `capture.py` to record a fresh corpus from
a live SWIM broker, and the now-relative time tokens that keep canned ETAs landing in the live window.

## Event Portal (optional)

Leave `SOLACE_API_TOKEN` empty to run without it. Set a Solace Cloud API token to enable Continuum's
producer-side registration into the "FAA Arrival Capacity" domain.

## Build it live (generation engine)

Beyond the prebuilt solution, this demo ships the runtime generation engine: the `SolutionBuilder`
SAM agent (`sam/agents/SolutionBuilder.yaml`) authors a new Continuum data product + trigger +
gateway + workflow + agents from one sentence, and `continuum-mcp` deploys the new live pipeline.
See [CUSTOMIZE.md](CUSTOMIZE.md) §5.

## Authoring new solutions in Claude Code

The same thing from the IDE: describe a real-time AI use case in one sentence and the
`solace-solution-builder` / `faa-generate-solution` skills compose the whole spine — Continuum
data product → trigger → event-mesh entrypoint → workflow → native **OpenAI Decisions** node — then
deploy it. The repo-owned Continuum / Event Portal / method knowledge lives in `sam/skills/` +
`sam/builder/`; the SAM-spine authoring defers to the off-the-shelf SAM skills.

Those off-the-shelf skills are **gitignored** (a per-checkout install artifact), so after cloning,
install them once so the builder's references resolve:

```bash
sam ai-assistance skill install --target claude    # writes .claude/skills/sam-* at the CLI version
```

Deploy an authored solution with `make new-solution SLUG=<slug> TRIGGER=<trigger>` (deploy + wire +
verify; a plain declarative apply — `targetWorkflowName` resolves natively on 2.409.2, no resolver).

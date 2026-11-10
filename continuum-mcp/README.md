# continuum-mcp

MCP server that exposes **Solace Continuum** — the streaming-data layer of Solace Platform — to AI agents
(the Solace Agent Mesh System-2 investigation agents and any other MCP client), for the **FAA arrival-capacity** demo.

A Continuum **data product** is a SQL-defined live view kept **strongly consistent** per event by the
Continuum engine. It is built in four steps — *ingest a stream to a table → refine with SQL → join across
streams → trigger an event* — and consumed three ways: as **events** on the Solace Platform event mesh,
through **MCP**, or with **SQL**. This server lets an agent do all of that without touching the engine's
REST API, the broker's SEMP API or Event Portal directly.

## Vocabulary (applies to every user-facing string)

| Say | Never say |
|---|---|
| Solace Platform | the legacy broker product name |
| Continuum, data product, live view | — |
| the Continuum engine | the engine vendor's name |
| strongly consistent | eventually consistent |

## How it works

```
 agent ──MCP (streamable HTTP, bearer)──► continuum-mcp
                                            │  catalog/           hand-authored stream registry + product manifests
                                            │  templates/*.j2     program rendering (sources, view, live sink, triggers)
                                            ├─► Continuum engine  POST /v0/validate_program  (validate + derive schema)
                                            │                     PUT  /v0/pipelines/cp-<slug> (one pipeline per product)
                                            ├─► SEMP v2           queues fl-cp-<slug>-<stream> + topic subscriptions
                                            └─► Event Portal      domain / schemas / events / application (optional)
```

* The base pipeline (`arrival-capacity`) is **read-only**: its live views are listed as platform-owned
  products (`managed: false`) and serve `preview_query`.
* Every created product is its **own pipeline** `cp-<slug>` with its **own queues**, so products never steal
  each other's messages and can be deleted independently.
* `create_*` tools return in well under 30 s with `status: compiling|redeploying`; a background deployer polls
  the engine (compile → start → Running) and updates the manifest. First compile is about 2 minutes, warm
  compiles a few seconds. The deployer is **resumable**: on startup any manifest left in `compiling` or
  `redeploying` is re-driven.
* Program changes (add/remove trigger) follow the only sequence this engine build allows:
  `stop?force=true → Stopped → PATCH program_code → compile → start`. State is rebuilt from live traffic; the
  previous program is restored automatically if the new one fails to compile.

## Tools

| Tool | Purpose |
|---|---|
| `list_source_streams(kind?)` | Streams a product can be built from: `raw` broker streams and `derived` live views the base pipeline publishes. Gives the SQL `relation` name, columns, keys, changelog kind, ingestability. |
| `list_data_products(status?, tag?, include_base=true)` | Managed products + read-only base products + orphan pipelines tagged `continuum:data-product`. |
| `get_data_product(name, sample_rows=5)` | Manifest, schema (live when running), engine status, sample rows, lineage, triggers, `consume` block (topic, subscribe pattern, wire format, JSON Schema, SAM event-rule snippet), Event Portal ids. |
| `search_catalog(keywords, limit=10)` | Deterministic token-overlap search over products, streams and triggers with a `why[]` per hit. |
| `preview_query(sql, limit=20, timeout_secs=15)` | SELECT-only ad-hoc query against the pipeline that materializes all referenced relations. |
| `create_data_product(...)` | Validate SQL → derive schema → create queues → deploy pipeline → `status: compiling`. Optional triggers, tags, owner, freshness SLA, Event Portal registration. |
| `create_trigger(data_product, name, description, condition, output_topic_template, payload_columns?, distinct=true, recency_window?)` | Add an event output that fires when `condition` becomes true; pipeline is redeployed. |
| `delete_trigger(data_product, name)` | Remove a trigger; pipeline is redeployed. |
| `delete_data_product(name, drop_queues=true, unregister_event_portal=false)` | Stop, clear and delete the pipeline; drop queues; remove catalog entry. |
| `get_status(name?)` | Program/deployment status, last error, per-connector counters and health, queue backlog. |
| `sync_event_portal(data_product)` | Idempotent find-or-create of the Event Portal objects. |

### Rules the tools enforce

* `sql` must be a single `SELECT` (or `WITH … SELECT`): no `;`, no DDL/DML keywords.
* Every relation in `FROM`/`JOIN` must be provided by a declared, **ingestable** source stream. Errors list
  the missing relation and which stream provides it.
* Slug `^[a-z][a-z0-9-]{2,40}$` derived from `name`; unique across manifests and engine pipelines.
* Topic templates must start with `fl/cp/<slug>/`, contain **at least one `{column}` placeholder** (static
  topics batch several rows into one message), no wildcards/spaces/empty levels, ≤ 250 bytes. Placeholder
  columns are emitted as `COALESCE(CAST(x AS VARCHAR), 'unknown')`.
* The live sink publishes the **full changelog** (inserts and deletes) with `delivery_mode: direct`.
* Trigger sinks use `SELECT DISTINCT` + `skip_deletes`, and the default payload is the product's columns
  minus the source streams' `volatile_columns`, so a trigger fires **once per episode** rather than on every
  recompute. For the arrival-capacity use case this strips the churny demand/overage/eta-quality values,
  leaving the airport + bin key and stable descriptors. `recency_window` (e.g. `60 minutes`) adds
  `NOW() <= <event_time_column> + INTERVAL …`.
* SQL compiler messages are remapped to the user's SELECT (`your SELECT, line 4, column 9`) or to the trigger
  that caused them.

## Catalog model (`catalog/`)

| Path | Content |
|---|---|
| `streams/<name>.yaml` | Hand-authored stream registry. `relation` equals the base pipeline's materialized view name so the same SQL works in `preview_query` and in a product. Raw streams carry `ddl` + `decode_sql`; derived streams carry typed `columns` (the typed ingest table and the relation view are generated) and may set `snapshot: true`. |
| `base/arrival-capacity.yaml` | Base pipeline live views exposed as read-only products. |
| `products/<slug>/manifest.yaml`, `program.sql`, `program.prev.sql` | Created products (runtime state; git-ignored). Manifest is authoritative for metadata, the engine for `status` and the live schema. |

Registered streams: raw `airport_rate` (→ `airport_rate`, the versioned AAR/ADR side input), `weather`
(→ `metar_latest`) and `active_restrictions` (→ `active_restriction`); derived `capacity_assessment`
(→ `capacity_assessment`, the demand-vs-rate primary view, `snapshot: true`) and `arrival_context`
(→ `arrival_context`, the enriched breach brief, `inserts_only`); and the base-only relations `flight`,
`arrival_demand` and `downstream_risk` (→ `mcp_downstream_risk`), which are queryable but `ingestable: false`.

### How derived streams are ingested

A product pipeline reads a derived stream from the base pipeline's live topic with `format: json` and
`update_format: insert_delete`, into a typed table whose columns are the stream's `columns`. The engine applies
both `{"insert": …}` and `{"delete": …}` events, so for `changelog: full` streams the relation is a plain view
over that table and mirrors the base view. For `changelog: inserts_only` streams (base sink uses
`skip_deletes`) the table also records the broker receive time and the relation keeps the **latest event per
key**. At-least-once redelivery after a restart can leave a duplicate insert (a phantom row after the matching
delete); this is accepted.

**Initial state.** The base pipeline publishes a view's snapshot once when it starts and afterwards only
changes. A product created later subscribes after that snapshot went by, so slow-changing reference streams
would start empty (and a JOIN would produce nothing). Streams flagged `snapshot: true` — notably
`capacity_assessment` — therefore render the ingest table with **two connectors**: a one-shot `url_input`
that GETs the base pipeline's ad-hoc query endpoint from inside the docker network (`FELDERA_INTERNAL_URL` +
`/v0/pipelines/<base>/query?sql=<snapshot_sql>&format=json`, parsed as `update_format: raw` NDJSON rows), then
the live `solace_input`. A change that lands between the snapshot read and the subscription start can be
missed — a small race accepted. Raw windowed streams (rates, weather) need no snapshot.

## Wire envelope

Confirmed on the live stack: `solace_output` with `format: json` publishes the **insert_delete envelope** —
`{"insert": {…row…}}` and `{"delete": {…row…}}`. Trigger sinks keep `skip_deletes: true`, so only
`{"insert": …}` reaches SAM. `TRIGGER_ENVELOPE` stays as an escape hatch for builds that emit bare rows:

| `TRIGGER_ENVELOPE` | Message body | `consume.wire_format` | SAM `inputExpression` | `forwardContext` |
|---|---|---|---|---|
| `insert` (default) | `{"insert": {…row…}}` / `{"delete": {…}}` | `insert_delete_envelope` | `input.payload:insert` | `input.payload:insert.<key>` |
| `bare` | the row object | `bare_row` | `input.payload` | `input.payload:<key>` |

The switch drives the JSON Schemas returned to agents (an `insert` wrapper object in the default mode), the SAM
event-rule snippets, and the `update_format` (`insert_delete` vs `raw`) of derived-stream inputs.

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `FELDERA_URL` | `http://localhost:8081` | Continuum engine REST API (host side) |
| `FELDERA_INTERNAL_URL` | `http://feldera:8080` | Engine address as seen from inside the docker network; used by the snapshot connector of `snapshot: true` streams |
| `BASE_PIPELINE` | `arrival-capacity` | Read-only base pipeline |
| `COMPILE_TIMEOUT_SECS` / `POLL_INTERVAL_SECS` | `600` / `5` | Deployer polling |
| `DEFAULT_WORKERS` | `2` | Pipeline workers |
| `SEMP_URL` / `SEMP_USER` / `SEMP_PASS` / `SEMP_VPN` | `http://localhost:8089` / `admin` / `admin` / `default` | Broker management |
| `QUEUE_MAX_TTL_SECS` / `QUEUE_MAX_SPOOL_MB` | `300` / `500` | Bounds for product queues (self-draining) |
| `FL_SOLACE_HOSTNAME` / `FL_SOLACE_PORT` / `FL_SOLACE_VPN` / `FL_SOLACE_USER` / `FL_SOLACE_PASSWORD` | `solace` / `55555` / `default` / `default` / `default` | Broker coordinates **as the engine container sees them** (written into connector JSON) |
| `EP_BASE_URL` | `https://api.solace.cloud/api/v2/architecture` | Event Portal API |
| `SOLACE_API_TOKEN` | unset | Event Portal token; registration is skipped when unset |
| `CONTINUUM_EP_DOMAIN` | `FAA Arrival Capacity` | Event Portal application domain |
| `MCP_HOST` / `MCP_PORT` | `0.0.0.0` / `8091` | Listen address |
| `CONTINUUM_MCP_TOKEN` | unset | Bearer token; when unset the endpoint is open (a warning is logged) |
| `MCP_ALLOWED_HOSTS` | unset | Comma-separated `Host` values to accept (turns on the SDK's DNS-rebinding protection). Unset = any host, so docker-network clients (e.g. `continuum-mcp:8091`) can connect |
| `CATALOG_DIR` | `./catalog` | Catalog root |
| `TRIGGER_ENVELOPE` | `insert` | `insert` (confirmed) or `bare`, see above |

Queue backlog in `get_status` uses the broker's spooled count when the monitor API returns it and otherwise
`lastSpooledMsgId - highestAckedMsgId`, an approximation that ignores gaps from expired or dead-lettered
messages.

## Running

```bash
cp .env.example .env            # edit as needed
uv sync
uv run python -m continuum_mcp.server
```

Docker: `docker build -t continuum-mcp . && docker run --env-file .env -p 8091:8091 -v continuum-products:/app/catalog/products continuum-mcp`

Endpoints: `POST /mcp` (trailing slash optional) and `GET /healthz`.

### Smoke sequence

```bash
TOKEN=...   # value of CONTINUUM_MCP_TOKEN
H=(-H "Authorization: Bearer $TOKEN" -H 'Accept: application/json' -H 'Content-Type: application/json')

# 1. 11 tools
curl -s "${H[@]}" -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' localhost:8091/mcp | jq '.result.tools | length'

# 2. streams
curl -s "${H[@]}" -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_source_streams","arguments":{"kind":"raw"}}}' localhost:8091/mcp | jq -c '.result.structuredContent.result[].name'

# 3. preview on the base pipeline
curl -s "${H[@]}" -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"preview_query","arguments":{"sql":"SELECT airport, demand, bin_capacity, overage FROM capacity_assessment WHERE exceeded","limit":5}}}' localhost:8091/mcp | jq '.result.structuredContent'

# 4. create a product (returns status=compiling)
curl -s "${H[@]}" -d @- localhost:8091/mcp <<'EOF' | jq '.result.structuredContent'
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"create_data_product","arguments":{
  "name":"Rate Weather Watch","description":"Airports with an AAR of 40 or less, with the latest weather flight category",
  "sql":"SELECT r.airport, r.aar, r.runway_config, r.valid_from, w.flight_category FROM airport_rate r LEFT JOIN metar_latest w ON w.station_id = r.airport WHERE r.aar <= 40",
  "source_streams":["airport_rate","weather"],"key_columns":["airport"],"event_time_column":"valid_from",
  "triggers":[{"name":"low-vis","description":"Low published rate while weather is IFR","condition":"flight_category = 'IFR'",
               "output_topic_template":"fl/cp/rate-weather-watch/low-vis/{airport}","recency_window":"60 minutes"}]}}}
EOF

# 5. poll until running
curl -s "${H[@]}" -d '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_data_product","arguments":{"name":"Rate Weather Watch"}}}' localhost:8091/mcp | jq '.result.structuredContent.status'

# 6. watch the live topic and the trigger (messages are {"insert": {...}} / {"delete": {...}})
mosquitto_sub -h localhost -p 1883 -t 'fl/cp/rate-weather-watch/#' -v
```

## Development

```bash
uv run pytest            # 126 passed, offline: respx for HTTP, tmp dirs for the catalog
uv run ruff check
UPDATE_GOLDEN=1 uv run pytest tests/test_templates.py   # regenerate tests/golden/*.sql after a template change
```

Layout: `continuum_mcp/` — `server.py` (FastMCP + bearer middleware), `service.py` (tool logic),
`deployer.py` (background compile/start/rollback), `catalog.py`, `templates.py` + `templates/*.j2`,
`sql_guard.py`, `schema_map.py`, `feldera_client.py`, `semp_client.py`, `ep_client.py`, `models.py`, `config.py`.

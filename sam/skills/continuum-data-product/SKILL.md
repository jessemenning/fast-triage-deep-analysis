---
name: continuum-data-product
description: How to read, choose, preview, create and trigger Continuum data products (the SQL-defined business objects / live views of the Solace Platform's streaming-data layer) through the continuum_* MCP tools — SQL guardrails, trigger design, async compile/poll, and the full tool reference.
tags: [solace-platform, continuum, data-product, trigger, streaming-sql]
---

# Continuum data products and triggers

**Continuum** is the streaming-data layer of the Solace Platform. It turns raw streams into
**data products**: SQL-defined business objects / live views that are always current, built in
four steps — **Ingest** (source streams) → **Refine** (filter, cast, derive) → **Join** (assemble
the business object) → **Trigger** (publish an event when a row meets a condition). A data
product can be consumed three ways: as **events** on the Event Mesh, over **MCP** (these tools),
or by **SQL**.

| Term | Meaning | Where you see it |
|---|---|---|
| **source stream** | an ingestable stream: `kind: raw` (broker topic → table) or `kind: derived` (another product's live changelog, with a snapshot on start); CDC-only relations are `ingestable: false` | `continuum_list_source_streams` |
| **data product** | named SQL view with `key_columns`, `status`, `owner`, `tags`, `freshness_sla`, lineage | `list_data_products`, `get_data_product` |
| **base** data product | platform-owned, read-only product of the base pipeline (`managed: false`) — the catalog you choose from and may `SELECT` from | listed by default (`include_base=true`) |
| **trigger** | predicate on a data product + topic template + payload columns; publishes **one event per row** that newly satisfies the predicate | `get_data_product.triggers[]`, `create_trigger` |
| **consume** block | how SAM subscribes: `topic`, `subscribe_pattern`, `wire_format`, `payload_json_schema`, `sam_event_rule_snippet` | `get_data_product.consume` |
| **event_portal** block | ids of the event / event version / producer application Continuum registered | `get_data_product.event_portal.ids` |

## Reading the catalog

`continuum_list_data_products(include_base=true)` → list of
`{name, description, status, owner, tags, key_columns, freshness_sla, trigger_count}`.
Filter by `tag` for a domain (`tag="arrival-capacity"`).

`continuum_get_data_product(name, sample_rows=3)` → the full picture. Read in this order:

1. `status` — only `running` products can trigger now (`compiling` → wait; `failed` → read `error`).
2. `key_columns` — the stable business identity. **No key → cannot trigger one event per object.**
3. `schema` — column names + types. The trigger predicate and payload may only use these.
4. `sample_rows` — sanity-check value ranges before writing a predicate (`risk_band` is `'RED'`, not `'red'`).
5. `triggers[]` — reuse one whose `condition` already matches; never create a duplicate.
6. `consume.wire_format` — `flat_row` → gateway `inputExpression: input.payload`;
   `insert_delete_envelope` → `input.payload:insert` (retractions arrive as `{delete: {...}}`).
7. `consume.subscribe_pattern` — paste into the gateway subscription verbatim.
8. `lineage.source_streams` — what feeds it (join coverage for the rubric).

`continuum_search_catalog(keywords, limit)` searches names, descriptions, tags and column names —
use it with the nouns from the user's sentence to catch products with unexpected names.

## SQL guardrails (for `preview_query` and `create_data_product`)

1. **SELECT-only.** No DDL/DML; `preview_query` rejects anything else.
2. **No `SELECT *`.** Name every column; the schema is the contract.
3. **Expose the key.** The column(s) used in `key_columns` and in the trigger topic `{field}` must be in the SELECT list, un-aggregated.
4. **Join on business identifiers**, not on timestamps.
5. **Bound every window.** Aggregations over streams must be time-bounded or they grow forever:
   `WHERE NOW() <= p.event_at + INTERVAL '30' MINUTE` (keep rows for 30 minutes), or a
   `TUMBLE`/`HOP` window on `event_time_column`.
6. **Derive the decision inputs in SQL** (`risk_band`, `reasons`) so the agent reads, not computes.
7. **Keep volatile aggregates out of trigger payloads** (see trigger design) — they can live in the view.
8. **Cast explicitly** when mixing types; streaming SQL is strict.
9. **Entity attributes come from a context stream — join it, don't drop the requirement.** Airport
   name, published rate (AAR), TRACON and runway config and similar attributes are usually NOT on the
   raw event stream. They live on a `kind: derived` context/reference source stream (in the arrival-capacity
   catalog: `airport_rate`, keyed on `airport`). If the use case needs them — e.g.
   *"high-rate airports only"*, *"exclude satellite fields"*, *"single-runway configs"* — you **must** `JOIN` that context
   stream and filter/expose the attribute. Never silently drop a stated requirement (like the
   rate-gating filter) just because the column is missing from the event stream: check
   `continuum_list_source_streams` for a context stream that carries it and join on the entity key.

Always `continuum_preview_query(sql, limit=5)` first. Show one sample row to the user. Only then
propose `create_data_product`.

## Engine SQL dialect (know these before you write SQL)

Continuum's streaming SQL front-end is Apache Calcite compiling to incremental views, not a row
store — a few things differ from ordinary Postgres/warehouse SQL and fail *at compile time or
silently* if you get them wrong. (Engine internals: never surface the engine's name to users — say
"Continuum" / "strongly consistent".)

- **No correlated `LATERAL` / outer-reference subqueries.** A `CROSS JOIN LATERAL (SELECT c.x …)`
  that references an outer table errors with `Physical plan does not support … OuterReferenceColumn`.
  Use a plain `JOIN … ON` instead. (Proven live.)
- **`INTERVAL 'n' UNIT` form** — quoted magnitude, singular unit: `INTERVAL '1' MINUTE`,
  `INTERVAL '30' MINUTE`. Accepted by both the compiler and the ad-hoc `preview_query` path.
- **Only MATERIALIZED relations are queryable.** `preview_query` and every read path can only
  SELECT from materialized views; the base relations you read (`arrival_demand_mv`,
  `airport_rate_mv`, …) already are, and your product view is materialized for you.
- **A `NOW()`-bounded bare `GROUP BY` deletes empty groups** rather than zeroing them: an aggregate
  row (a fan-in count) *disappears* ~one window after the last event, and the sink emits a delete.
  That is exactly right for an episodic trigger (`skip_deletes` drops the delete → one insert per
  episode) — just don't expect an "all-clear" row.
- **Supported and safe:** `COUNT(DISTINCT …)`, window functions, `DISTINCT`, inner/outer joins,
  explicit `CAST` (guardrail #8). Prefer these over clever rewrites.
- **State-bounding DDL is NOT yours to write.** `LATENESS`, `append_only`, `expected_size` bound
  engine state, but they are table-DDL: the SQL guard blocks `lateness`, and the base pipeline plus
  the MCP's program rendering own them. Bound your own windows with the `NOW()` filter (guardrail
  #5); never try to add `LATENESS` in a SELECT.

Authoritative engine SQL reference (full dialect, incremental semantics, time-series guidance):
Feldera SQL docs — <https://docs.feldera.com/sql/> and <https://docs.feldera.com/sql/streaming>.
These describe the whole engine surface; much of it (DDL annotations, recursive views, connectors)
is handled by the MCP, not the SELECT author, so treat it as background, not a to-do list.

## Creating a data product

```text
continuum_create_data_product(
  name="arrival_capacity_view",                   # snake_case, unique
  description="Per-airport arrival-capacity view: demand + capacity + rate + weather + downstream risk",
  sql="SELECT ...",                                # previewed
  source_streams=["arrival_demand","capacity_assessment","airport_rate","mcp_weather","mcp_downstream_risk"],
  key_columns=["airport"],
  event_time_column="event_at",
  output_topic_template="fl/faa/arrival-capacity/live/{airport}",    # the LIVE full-changelog sink (see caution)
  owner="traffic-management", tags=["arrival-capacity","faa"], freshness_sla="5s",
  triggers=[{...}],                                # optional, same shape as create_trigger
  register_in_event_portal=true)                   # Continuum registers the PRODUCER side
→ {name, status: "compiling", ...}
```

> **CAUTION — the live sink and the triggers are DIFFERENT sinks on DIFFERENT topics.**
> The product's `output_topic_template` is the **unfiltered, full-changelog LIVE sink** — it
> publishes *every* row of the product (every airport, including ones a trigger would exclude,
> and every row change). Each trigger has its own `output_topic_template` carrying only the
> **filtered** rows that satisfy its condition.
> - Live sink → `fl/faa/<slug>/live/{key}` (or omit `output_topic_template` to get this default).
> - Each trigger → `fl/faa/<slug>/<trigger-name>/{key}`.
> **Never set the product's `output_topic_template` equal to a trigger's topic.** If you do, the
> live sink floods the trigger topic with unfiltered rows and downstream agents fire on data the
> trigger was meant to exclude (e.g. within-rate airports on a `fan-in` topic filtered to rate-exceeding ones).
> If you only want triggers, omit `output_topic_template` entirely.

Compilation is **asynchronous**: poll `continuum_get_data_product(name)` every ~5 s until
`status` is `running` (about 5 s when the engine is warm, up to ~2 minutes cold). If `failed`,
read `error`, fix the SQL, `continuum_delete_data_product` and recreate. Tell the user what you
are waiting for.

`source_streams` may name an existing data product to build a **thin view** on top of it
(the "extend" decision in the selection rubric).

## Triggers

A trigger fires when a row **newly** satisfies `condition`. It publishes one message per row to
`output_topic_template` with `payload_columns` as the JSON body. Full design guidance in
`references/trigger-design.md`; the short version:

- `output_topic_template` must start with `fl/faa/<slug>/` and contain ≥ 1 `{field}` → one row per message.
- `payload_columns` = stable identity + decision inputs; **exclude churning aggregates** so it fires once per episode.
- `distinct=true` (default) suppresses repeats of the same payload; `recency_window` (ISO-8601 duration, e.g. `PT15M`) bounds how long a payload is remembered.
- Prefer several narrow triggers (`breach`, `demand-exceeds-rate`) over one broad one — the topic then says *why*.

After `create_trigger`, re-read `get_data_product` for the new `consume.subscribe_pattern` and
`event_portal.ids`; `continuum_sync_event_portal(data_product)` if the ids are missing.

## Safety

- Never call `create_data_product`, `create_trigger`, `delete_trigger`, `delete_data_product`
  without explicit user approval in the conversation.
- `delete_*` is for Phase 4 refinement of things *you* created in this session.
- `continuum_get_status()` before blaming SQL when everything is `compiling`.

## References

- `references/tool-reference.md` — all 11 `continuum_*` tools, parameters and returns.
- `references/trigger-design.md` — condition, topic, payload, once-per-episode.

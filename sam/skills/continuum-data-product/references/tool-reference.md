# Continuum MCP — tool reference

All tools surface in Solace Agent Mesh with the connector prefix `continuum_`
(connector `continuum-mcp`, `allow_list` = exactly these 11 names).

| # | Tool | Mutates? | Purpose |
|---|---|---|---|
| 1 | `list_source_streams` | no | raw streams available for Ingest |
| 2 | `list_data_products` | no | catalog of data products |
| 3 | `get_data_product` | no | full detail of one data product (schema, triggers, consume, event_portal) |
| 4 | `search_catalog` | no | keyword search across products/columns/tags |
| 5 | `preview_query` | no | run a SELECT and return rows (dry-run for SQL) |
| 6 | `create_data_product` | **yes** | define a new data product (async compile) |
| 7 | `create_trigger` | **yes** | add a trigger to a data product |
| 8 | `delete_trigger` | **yes** | remove a trigger |
| 9 | `delete_data_product` | **yes** | remove a data product (and its triggers) |
| 10 | `get_status` | no | engine / product health |
| 11 | `sync_event_portal` | **yes** (EP only) | (re)register the producer side in Event Portal |

Mutating tools (**bold yes**) require explicit user approval first.

---

## 1. `list_source_streams(kind?)`

- `kind` (optional): `event` | `cdc` | `reference`.
- Returns: `[{name, kind, description, schema: [{column, type}], event_time_column?, sample_rate?}]`.
- Use: pick `source_streams` for a new data product; check which streams carry the entities the decision needs.

## 2. `list_data_products(status?, tag?, include_base=true)`

- `status` (optional): `running` | `compiling` | `failed` | `stopped`.
- `tag` (optional): filter by tag.
- `include_base` (default `true`): include the platform-owned base products (`managed: false`, read-only) — they are the catalog SolutionBuilder chooses from; created products are `managed: true`.
- Returns: `[{name, description, status, owner, tags[], key_columns[], freshness_sla, trigger_count, updated_at}]`.
- Use: Phase 0 step 1. Always call this first.

## 3. `get_data_product(name, sample_rows=0)`

- `name`: data product name. `sample_rows`: 0–20 rows to include.
- Returns:
  ```yaml
  manifest: {name, description, owner, tags, key_columns, event_time_column, freshness_sla, sql, source_streams}
  schema: [{column, type, nullable}]
  status: running | compiling | failed | stopped
  error: <string, when failed>
  sample_rows: [{...}]
  lineage: {source_streams: [...], upstream_products: [...]}
  triggers:
    - {name, description, condition, output_topic_template, payload_columns, distinct, recency_window, status}
  consume:
    topic: fl/faa/<slug>/<trigger>/{key}          # template as published
    subscribe_pattern: fl/faa/<slug>/<trigger>/>  # paste into the gateway subscription
    wire_format: flat_row | insert_delete_envelope
    payload_json_schema: {...}                   # JSON Schema of one message
    sam_event_rule_snippet: |                    # starter event_rules[] entry for the gateway
      ...
  event_portal:
    ids: {application_domain_id, application_id, application_version_id, event_id, event_version_id, schema_id, schema_version_id}
    names: {...}
  ```
- Use: scoring, `inputExpression` choice (`wire_format`), gateway subscription, EP consumer registration (`event_portal.ids.event_version_id`).

## 4. `search_catalog(keywords, limit=10)`

- `keywords`: string or list; matched against names, descriptions, tags, column names.
- Returns: `[{name, kind: data_product | source_stream, match: [<field>], score}]`.
- Use: Phase 0 step 2, with the nouns from the user's sentence.

## 5. `preview_query(sql, limit=10)`

- `sql`: SELECT-only; anything else is rejected. `limit`: 1–100.
- Returns: `{columns: [{name, type}], rows: [[...]], row_count, elapsed_ms}`.
- Use: **always** before proposing new SQL or a trigger condition; also the one read tool an autonomous agent may be given (via a read-only connector).

## 6. `create_data_product(...)`

| Param | Required | Notes |
|---|---|---|
| `name` | yes | snake_case, unique |
| `description` | yes | one sentence, business terms |
| `sql` | yes | previewed SELECT; explicit columns; key exposed |
| `source_streams` | yes | streams or existing data products the SQL reads |
| `key_columns` | yes | stable business identity |
| `event_time_column` | no | enables windows / freshness |
| `output_topic_template` | no | publish every row (rarely needed; triggers are usually enough) |
| `owner` | no | team/person |
| `tags` | no | list |
| `freshness_sla` | no | e.g. `"5s"` |
| `triggers` | no | list of trigger objects (same fields as `create_trigger` minus `data_product`) |
| `register_in_event_portal` | no, default `true` | Continuum registers domain/schema/event/producer app |

- Returns: `{name, status: "compiling", event_portal?: {...}}`. Poll `get_data_product` until `running`.

## 7. `create_trigger(...)`

| Param | Required | Notes |
|---|---|---|
| `data_product` | yes | must be `running` |
| `name` | yes | kebab-case episode name (`breach`) |
| `description` | yes | why it fires, in business terms |
| `condition` | yes | SQL predicate over the product's exposed columns |
| `output_topic_template` | yes | must start `fl/faa/<slug>/`, contain ≥ 1 `{field}` |
| `payload_columns` | no (default: all) | stable identity + decision inputs; exclude churning aggregates |
| `distinct` | no, default `true` | suppress identical repeat payloads |
| `recency_window` | no | ISO-8601 duration (`PT15M`) the distinct memory lasts |

- Returns: `{data_product, trigger: {...}, consume: {...}, event_portal: {...}}`.

## 8. `delete_trigger(data_product, name)`

- Returns: `{deleted: true}`. Use in Phase 4 to replace a trigger you created.

## 9. `delete_data_product(name, force=false)`

- `force`: also delete triggers and downstream thin views. Returns `{deleted: true}`.
- Only for products created in this session, after approval.

## 10. `get_status(name?)`

- Without `name`: `{engine: {healthy, version, uptime}, products: {running, compiling, failed}}`.
- With `name`: `{name, status, rows, last_update, lag_ms, error?}`.

## 11. `sync_event_portal(data_product)`

- Re-registers or refreshes the producer-side objects (domain, schema+version, event+version, application+version) for the product. Returns the `event_portal` block.
- Use when `get_data_product.event_portal.ids` is empty or stale before registering the consumer.

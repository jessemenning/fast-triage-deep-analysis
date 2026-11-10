# Trigger design — fire once per episode, say why in the topic

A trigger is the **Trigger** step of Ingest → Refine → Join → Trigger: a predicate over a running
data product that publishes **one event per row** that newly satisfies it. Good triggers are the
difference between an agent that acts once per business episode and one that is spammed by every
tick of a counter.

## 1. Condition

- Write it as a SQL predicate over **exposed columns only** (check `get_data_product.schema`).
- Use the sample rows to get literals right (`risk_band = 'RED'`, not `'red'`).
- Prefer categorical columns derived in the view (`risk_band`) over raw thresholds in the trigger
  (`falcon_score >= 70 AND (device_new OR geo_anomaly)`) — the *meaning* then lives in the data product,
  and the trigger reads like policy.
- One trigger per **kind of episode**. Two conditions that lead to different agent actions are two
  triggers with two names (`breach`, `demand-exceeds-rate`), not one `OR`.

## 2. Topic

`output_topic_template` rules:

| Rule | Why |
|---|---|
| starts with `fl/faa/<slug>/` | the FAA solution namespace; consumers can wildcard `fl/faa/<slug>/>` |
| then `<trigger-name>/` | the topic says *why* the event exists |
| then `{key}` … (≥ 1 `{field}`) | **forces one row per message**; without a `{field}` the publisher may batch rows |
| composite key → one level per column | `fl/faa/sector-briefer-ready/{lat_cell}/{lon_cell}` |
| no volatile fields in the topic | `{flight_count}` in the topic makes every count change a new topic |

The gateway subscribes with `consume.subscribe_pattern` (`fl/faa/<slug>/<trigger>/>`) and lifts the
key from the payload via `forwardContext`, then publishes the decision to `solutions/<slug>/decision/{key}`.

### The live sink topic and the trigger topic must differ

A data product has up to two kinds of sink:

| Sink | Topic | Content |
|---|---|---|
| **live** (the product's `output_topic_template`) | `fl/faa/<slug>/live/{key}` | the **full changelog** — every row, unfiltered |
| **trigger** (each trigger's `output_topic_template`) | `fl/faa/<slug>/<trigger>/{key}` | only rows matching the trigger `condition` |

**Never give the live sink the same topic as a trigger.** The live sink is unfiltered, so pointing
it at `fl/faa/<slug>/fan-in/{key}` publishes *every* airport (including the within-rate airports an
`overage > 0 AND eta_quality >= 0.5` trigger excludes) plus a message on every row change — the
downstream gateway then wakes the workflow for data the trigger was built to filter out. If the
product only needs triggers, **omit `output_topic_template`** (no live sink at all); otherwise keep
it on `.../live/`.

## 3. Payload columns

Include:
- the **key** (always — the gateway needs it in the payload, not only in the topic),
- the **decision inputs** the agent reads (`risk_band`, `reasons`, `overage`, `eta_quality`),
- stable descriptive fields (`origin_airport`, `airport_name`).

Exclude:
- **churning aggregates** (`flight_count`, running totals, `last_seen_at`) unless the value itself is
  the decision input *and* you have `distinct` + `recency_window` in place,
- anything the agent does not need (keeps the event small and the schema stable),
- free text over ~1 KB (put it in the view; let the agent query if truly needed).

Why exclude churn: with `distinct=true`, the trigger remembers the payloads it has published. If a
payload column changes every tick, every tick is a "new" payload and the trigger fires continuously.
Stable payload → fires once when the row first satisfies the condition → **once per episode**.

**This is the most common trigger mistake — get it right the first time.** The trigger payload is
**identity + stable descriptors ONLY**. A running count is the *condition*, not the payload.

- ✅ GOOD (fan-in trigger): `payload_columns = [airport, airport_name,
  published_rate, tracon, runway_config]` — all stable for
  the airport. Condition `distinct_flights_60s >= 6` uses the count; the count is **not** in the payload.
- ❌ BAD: adding `distinct_flights_60s`, `total_overage`, `inbound_60s`, `last_inbound_at` to the payload.
  Each new arrival bumps these, so every arrival re-fires the trigger — dozens of events per episode.
The agent that consumes the trigger can read the live counts itself (one `preview_query`/`get_data_product`)
if it needs them; keep them out of the trigger event.

**Enforced tool-side:** `create_trigger` / `create_data_product` automatically strip volatile columns
(rolling counts/sums, windowed metrics like `*_60s`, balances, most-recent timestamps) from
`payload_columns` — the key and topic placeholders are always kept. Stripped columns come back in the
response `warnings`. You do not have to get this perfect by hand, but do prefer an identity-only payload;
pass `allow_volatile_payload: true` only if you deliberately want many events per episode.

## 4. `distinct` and `recency_window`

| Setting | Effect | Use |
|---|---|---|
| `distinct: true` (default) | identical payloads are not re-published | almost always |
| `recency_window: PT15M` | a payload is forgotten after 15 min; the same episode may fire again later | recurring episodes (congestion clears and returns) |
| no `recency_window` | remembered for the product's retention | one-shot episodes (a breach is escalated once) |

## 5. Insert / delete envelopes

If `consume.wire_format` is `insert_delete_envelope`, the row arrives as `{"insert": {...}}` and a
retraction (row no longer satisfies the condition) as `{"delete": {...}}`. Gateways use
`inputExpression: input.payload:insert` so the agent receives a flat row and `{}` on retractions;
autonomous agents must be told to reply `{}` when the input has no key.

## 6. Checklist before `create_trigger`

- [ ] `continuum_preview_query("SELECT <key>, <payload cols> FROM <product> WHERE <condition>", 5)` returns the rows you expect (and not thousands).
- [ ] topic starts `fl/faa/<slug>/`, contains the key as `{field}`, no volatile fields.
- [ ] the trigger topic (`.../<trigger>/{key}`) is DIFFERENT from the product's live `output_topic_template` (`.../live/{key}`); if you only need triggers, the product has no `output_topic_template` at all.
- [ ] payload = key + decision inputs, no churn.
- [ ] `distinct` / `recency_window` chosen deliberately.
- [ ] name + description say why it fires, in business terms.
- [ ] user approved.

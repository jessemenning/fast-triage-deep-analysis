---
name: faa-generate-solution
description: >
  Turn a natural-language air-traffic SCENARIO into a deployed end-to-end FAA Fast/Slow
  solution — a Continuum data product + trigger, a SAM fast/slow decision solution
  (OpenAI Decisions triage + System-2 brief), and Event Portal registration — on top of
  the constant SWIM feeds, verified headlessly (no UI). Use when the user says "build a
  solution for <scenario>", "generate a solution that detects/alerts/briefs on <X>", or
  feeds a scenario to implement over the FAA arrival-capacity stack.
---

# faa-generate-solution

Given a scenario, I (Claude) design + implement + deploy + verify a complete solution on the
running `/home/jmenning/fast-triage-deep-analysis` stack. The **6 SWIM feeds and the base Feldera pipeline
`arrival-capacity` are constant — never edit `faa_program.sql` or the feeds.** Everything
generated is ADDITIVE: a new Continuum product, a new SAM solution, EP registration.

This is the FAA-specific companion to `solace-solution-builder`, with four fixed choices:
**I orchestrate** (via continuum-mcp + EP + the `sam` CLI); the decision node is **OpenAI
Decisions** (not TypeSafe); the SAM solution is **cloned from `sam/solutions/arrival-capacity/`**;
and generation is **additive only**.

## The expressible envelope (check first)

A product is ONE SELECT (CTEs ok; no `;`, no DDL, no `LATENESS`, no `SELECT *`) over the five
**ingestable** base streams, joins allowed:

| source_stream | FROM relation | key columns | key fields |
|---|---|---|---|
| `capacity_assessment` | `capacity_assessment` | airport, bin_start | demand, bin_capacity, overage, exceeded, eta_quality |
| `arrival_context` | `arrival_context` | airport, bin_start | + severity, wx_*, active_restriction_* |
| `airport_rate` | `airport_rate` | airport, valid_from | aar, adr, runway_config, valid_to |
| `active_restrictions` | `active_restriction` | airport, restriction_type | reason, scope, effective_time |
| `weather` | `metar_latest` | station_id | flight_category, visibility_sm, wind_speed_kt, wx_string |

(`flight`, `arrival_demand`, `mcp_downstream_risk` are preview-only — NOT product sources.)
If a scenario needs data outside these (e.g. departures, sectors), it needs a base-pipeline
change — **stop and tell the user** (out of the additive envelope).

## Steps

1. **Discover / preview.** Map the scenario to a SELECT over the envelope. Preview it before
   creating anything: `make fq SQL="SELECT … FROM capacity_assessment c JOIN metar_latest w ON c.airport=w.station_id …"`.
   Confirm non-empty, weather/join non-null. Pick a `<slug>` and `<trigger>` name.

2. **Create the Continuum product + trigger.** Write `demo/products/<slug>.json` (name,
   description, sql, source_streams, key_columns, event_time_column, triggers[]). Trigger:
   `condition` (boolean over product columns, NO subquery), `output_topic_template:
   fl/cp/<slug>/<trigger>/{airport}` (must start `fl/cp/<slug>/`, ≥1 `{column}` key), `distinct: true`,
   `recency_window` (needs event_time_column), and `allow_volatile_payload: true` IF a churning
   metric (demand/overage/eta_quality/counts) must ride the event. Create + wait:
   `CONTINUUM_MCP_TOKEN=… python3 demo/continuum-create-product.py demo/products/<slug>.json`.
   Note the printed trigger topic `fl/cp/<slug>/<trigger>/{airport}`.

3. **Generate the SAM solution** (clone the template):
   - `cp -r sam/solutions/arrival-capacity sam/solutions/<slug>`; rename files to the new slug;
     `sed` the identifiers (`arrival_capacity_gateway`→`<slug>_gateway`, `ArrivalCapacityWorkflow`→
     `<Pascal>Workflow`, `arrival_decider`→`<slug>_decider`, the 5 agent names, `solutions/arrival-capacity`→
     `solutions/<slug>`, and the gateway subscribe topic `fl/faa/arrival-capacity/breach`→
     `fl/cp/<slug>/<trigger>`).
   - **Tailor the workflow** (`workflows/<Pascal>Workflow.yaml`): rewrite the `triage` node's
     `detection:` prompt and `input_schema` to the product's actual payload fields, and adjust the
     question instructions/criteria to those fields. Keep the fast/slow shape and the ESCALATE-takes-
     precedence criteria. The decision node stays `type: decision`, `model_alias: <slug>_decider`
     (copy of `arrival_decider`, native OpenAI-Decisions via the MaaS LiteLLM proxy).
   - Deploy: `cd sam && SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply --manifest solutions/<slug>/manifests/dev.yaml`
     (platform layer already applied; 2.409.2 resolves the workflow target natively — no resolver, no restart).

4. **Register Event Portal.** The product self-registered its producer app + events. Add the SAM
   side + consumer and wire them: `make register-ep-solution SLUG=<slug> TRIGGER=<trigger>`.

5. **Verify headless (no UI).** `make verify-solution SLUG=<slug> TRIGGER=<trigger>` — drives the
   wind shift, asserts a decision lands on `solutions/<slug>/decision/…`, prints trigger→decision
   latency + whether a brief was produced. Cross-check product rows with `make fq`, and the EP chain
   via the EP API (producer → SAM → consumer).

## Load-bearing rules (silently break the build)

- Gateway→workflow `successOutput`: EXACTLY `responseType: custom` + `customExpression: "input.payload:data[0]"`.
- The gateway delivers TEXT: read `{{workflow.input.text}}` only; never `{{workflow.input.<field>}}`.
- `when:` gate value MUST be quoted (`'"{{triage.output.answers.action.choice}}" == "ESCALATE"'`).
- Product topics MUST start `fl/cp/<slug>/` with ≥1 `{column}` placeholder; trigger conditions are
  boolean over product columns, no subquery.
- Trigger payloads auto-prune volatile columns (counts/sums/rates/`demand`/`overage`/…) unless
  `allow_volatile_payload: true`; key columns + topic placeholders are always kept.
- Brand: "Solace Platform" (never "PubSub+"), "Continuum" (never name the stream engine) in any
  user-facing string.

## Speed (measured on this stack)

The dominant cost is the Feldera Rust compile for a **new** product pipeline — **~7–8 min** on a
modest host (WSL2), and the unoptimized compile profile (now the default, set in
`continuum_mcp/config.py`) did **not** meaningfully cut that here. So, in order of impact:

1. **Prefer extending an existing product (add a trigger) over minting a new pipeline.** If the
   scenario's detection is a boolean over columns an existing product already materializes (e.g. a
   new filter over `wind-arrival-risk` or `arrival-demand-watch`), use
   `demo/continuum-add-trigger.py <product> <trigger.json>` — measured **~10 s** (incremental,
   cache-assisted recompile) vs ~7.5 min for a fresh product. This is the big lever.
2. **`make new-solution SLUG=… TRIGGER=…`** — overlaps the product compile (background) with the SAM
   apply (foreground), then registers EP + verifies in one command. Saves the orchestration + ~30–60 s.
3. **Mint a new product only for a genuinely new data shape** (new join/aggregation not already
   materialized). Pre-warm its compile before a live demo if latency matters (cache hit → seconds).
   **Verified (persistent cache, `make up-cached`):** a cold first compile of `wind-arrival-risk`
   took **468 s**; a subsequent `--recreate` (delete the product + queues, then create the *identical*
   spec) reached running in **37 s** — the Rust compile was skipped entirely (`program: Success` within
   ~15 s). `delete_data_product` drops the pipeline + queues but NOT the compile cache, so recreating
   identical SQL is a cache hit. The persistent cache also survives a host reboot: the base pipeline
   came back as `program: Success` and started in ~0 s. So the pre-warm → delete → live-create pattern
   gives a seconds-scale "live" create for a demo. **Automated:** `make prewarm` does exactly this for
   every `demo/products/*.json` (compile → delete record, keep cache); `make demo-ready` = `make up` +
   `make prewarm` is the one-command cold-start→warm path. Run once per machine / after `make clean`;
   the cache persists otherwise. When you author a NEW product for a live demo, drop its spec in
   `demo/products/` and `make prewarm` before presenting so its live create is a cache hit too.

Rule of thumb: new product = minutes (compile-bound); new trigger on an existing product = seconds.

## Reusable assets
- `demo/continuum-create-product.py` + `demo/products/<slug>.json` — generic product creator (new pipeline).
- `demo/prewarm.sh` (`make prewarm` / `make demo-ready`) — pre-compile the demo products into the
  persistent cache so a live create is a cache hit; run once per machine / after `make clean`.
- `demo/continuum-add-trigger.py <product> <trigger.json>` — add a trigger to an EXISTING product (fast path, #4).
- `demo/new-solution.sh` (`make new-solution`) — one-command deploy+wire+verify (compile/apply overlapped).
- `sam/solutions/arrival-capacity/` — the SAM template to clone.
- `demo/register-ep-solution.py` (`make register-ep-solution`) — per-solution EP wiring.
- `demo/verify-solution.py` (`make verify-solution`) — headless end-to-end check.
- Worked example: the `wind-arrival-risk` solution (product `demo/products/wind-arrival-risk.json`,
  SAM `sam/solutions/wind-arrival-risk/`) was generated by this exact flow.

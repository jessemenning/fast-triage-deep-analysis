---
name: solution-design
description: End-to-end method for turning one sentence into a running real-time AI solution on the Solace Platform — Continuum data product -> trigger -> event-mesh gateway -> workflow -> decision node (native OpenAI Decisions node by default, LLM agent as fallback) -> decision event. Includes the data-product selection rubric, how to say "none fits", naming conventions, and the solution_manifest.yaml contract.
tags: [solace-platform, continuum, event-mesh, solution-design, builder]
---

# Solution Design — from one sentence to a running real-time AI solution

The Solace Platform is three layers. Every solution you design composes all three:

| Layer | Role | What you touch |
|---|---|---|
| **Event Mesh** | moves events between systems and to agents | topics, the gateway subscription, the decision event |
| **Continuum** | streaming data: SQL-defined **data products** (business objects / live views) built **Ingest -> Refine -> Join -> Trigger an event**; outputs events, MCP, SQL | pick or create a data product; add a **trigger** |
| **Solace Agent Mesh** | acts | event-mesh gateway -> workflow -> decision node (native OpenAI Decisions node, or an agent) -> publish-out |

The spine of every solution:

```
Continuum data product ──trigger──▶ fl/faa/<slug>/<trigger>/{key}
        │                                   │
        │ (producer app in Event Portal,     ▼
        │  registered by Continuum)   event-mesh gateway (kind: entrypoint)
        │                                   │ targetWorkflowName
        ▼                                   ▼
  consumer app in Event Portal        <Name>Workflow (kind: workflow)
  (registered by you)                       │ decision node
                                            ▼
                       decision node → OpenAI Decisions (default)
                       — or an autonomous agent (kind: agent) when the
                         step needs tools / open reasoning / prose
                                            │ structured JSON
                                            ▼
                              solutions/<name>/decision/{key}
```

The point of the pattern: **Continuum does the assembly, the decision node decides immediately.** A well-chosen
data product pushes a complete, pre-joined business object, so the decision is a typed policy call — the native
OpenAI Decisions node answers it in ~0.2 s (`references/decision-node-openai.md`). An LLM agent node is only needed when
the step must look something up, reason open-endedly, or explain in prose.

## Phase 0 — Discover & select (always before you ask the user anything)

1. `continuum_list_data_products(include_base=true)` — the whole catalog: platform-owned base products (`managed: false`, read-only) **and** products created so far (`managed: true`). Always show the user both groups.
2. `continuum_search_catalog(keywords=<nouns from the request>, limit=10)` — catches products whose name does not match.
3. `ep_getApplicationDomains` — which domains and applications already exist around this use case.
4. For the top 2–4 candidates: `continuum_get_data_product(name, sample_rows=3)` — read `schema`, `key_columns`,
   `freshness_sla`, `triggers[]`, `consume.subscribe_pattern`, `consume.wire_format`, `lineage.source_streams`.
5. Score with the rubric below; present the comparison; decide.

### Selection rubric

Score each criterion 0–3, multiply by its weight, sum, divide by 3 → 0–100.

| Criterion | Weight | 0 | 1 | 2 | 3 |
|---|---|---|---|---|---|
| **Freshness / latency** — does the product update fast enough for the decision? | 20 | batch / stale | minutes | seconds | streaming, `freshness_sla` meets the use case |
| **Join coverage** — are all entities the decision needs already joined in? | 30 | key entity missing | one required join missing | all entities, some fields missing | complete: agent needs no lookup |
| **Trigger-ability** — a filterable predicate on exposed columns + a **stable key column** | 25 | no key column | key but predicate needs columns not exposed | predicate possible but fires repeatedly per episode | clean predicate, stable key, fires once per episode |
| **Topic granularity** — one row per message when the topic has `{field}` | 15 | aggregate rows only | row per entity but key not in topic | key in topic, extra churny levels | `fl/faa/<slug>/<trigger>/{key}` exactly |
| **Ownership / SLA** — owner, tags, freshness_sla declared | 10 | none | owner only | owner + tags | owner + tags + SLA, status `running` |

**Hard disqualifier:** no stable key column → it cannot trigger one event per business object → score 0 regardless.

| Total | Decision | What you create |
|---|---|---|
| ≥ 75 | **Reuse** | nothing in Continuum, unless no existing trigger matches → one trigger |
| 50–74 | **Extend** | a new trigger on the existing product, or a thin new data product that `SELECT`s from it (`source_streams: [<existing product>]`) to add the missing join/column |
| < 50 (or disqualified) | **None fits** | a new data product + trigger (see below) |

Present the comparison as a short table: `name | score | reuse/extend/reject | one sentence WHY`. One sentence per candidate — the WHY is mandatory.

### Saying "none fits" and proposing a composition

Say it plainly: *"None of the existing data products fits: X lacks the customer join, Y is a 5-minute aggregate with no key."*
Then propose:

1. **Source streams** — `continuum_list_source_streams()`; name the 2–4 streams that carry the entities.
2. **SQL** — SELECT-only, explicit columns (no `SELECT *`), the key column exposed, joins keyed on business identifiers,
   any window bounded (`NOW() <= event_at + INTERVAL '10' MINUTE`). See skill `continuum-data-product`.
3. **Key** — `key_columns: [<stable business id>]`, `event_time_column` if there is a timestamp.
4. **Trigger** — `condition` on exposed columns; `output_topic_template: fl/faa/<slug>/<trigger>/{<key>}`;
   `payload_columns` = identity + decision inputs, never volatile aggregates. See `continuum-data-product/references/trigger-design.md`.
5. **Preview** — `continuum_preview_query(sql, limit=5)` and show one sample row **before** asking for approval.
6. **Approval** — explicit yes from the user before any `continuum_create_*` / `ep_create*`.

## Phase 1 — Clarify (max 2 rounds)

Only ask what changes the design: decision vocabulary (e.g. `ESCALATE | SUPPRESS | GROUP`), who consumes the decision topic,
latency expectation, the policy to apply, and whether the decision is a typed policy call (default → native OpenAI
Decisions node) or must look things up / reason open-endedly / write prose (→ LLM agent node). State the defaults you assumed.

## Phase 2 — Design

Present the spine with names filled in and a one-line contract per element. Save:

- **build manifest** artifact (Builder `components:` format — `name`, `type`, `filename`, `status`, `depends_on`, `connectors`, `skills`),
  validated with `validate_build_manifest(full_validation=true)`.
- **`solution_manifest.yaml`** artifact, MIME `application/vnd.sam-solution-manifest+yaml`, shape in `references/solution-manifest-format.md`.

## Phase 3 — Generate (order matters)

| # | Component | Load first | Notes |
|---|---|---|---|
| 1 | data product / **trigger** | `continuum-data-product` | create → poll `get_data_product` until `status: running` → read `consume.*` and `event_portal.ids` |
| 2 | **gateway** (`kind: entrypoint`, `type: event_mesh`) | `schema-refs` → entrypoint | subscribe `consume.subscribe_pattern` (qos 0); `inputExpression` per `consume.wire_format` (`input.payload` or `input.payload:insert`); `targetWorkflowName`; `forwardContext.<key>`; `successOutput` `responseType: custom`, `customExpression: "input.payload:data[0]"`, `topicType: dynamic`, `topic: solutions/<name>/decision/{{text://user_data.forward_context:<key>}}`; `errorOutput` to `solutions/<name>/error/...`; broker fields from the `EVENT_MESH_SOLACE_BROKER_URL/VPN/USERNAME/PASSWORD` placeholders; `deploy: true` |
| 3 | **workflow** (`kind: workflow`) | `schema-refs` → workflow, then `references/decision-node-openai.md` | `spec.appConfig` (snake_case inside): `input_schema` = trigger payload. **Default decision node: one native `type: decision` node bound by `decision: {model_alias: <slug>_decider}`** to an OpenAI Decisions model (`kind: model`: `provider: "openai"`, `modelType: decision`, `modelName: "microsoft-decision-1"`, `apiBase: "${LLM_SERVICE_ENDPOINT}/v1"`, `authConfig: {type: apikey, api_key: "${LLM_SERVICE_API_KEY}"}` — on the MaaS LiteLLM proxy at `/v1/decisions`), node `input` with `event_json: "{{workflow.input.text}}"` + `detection` and choice/boolean questions whose `criteria` encode the policy, `output_mapping` from `{{decide.output.answers.<q>.choice}}` / `.confidence` / boolean `.probability` plus `event: "{{workflow.input.text}}"` and a `concat` rationale. Needs `SAM_FEATURE_WORKFLOW_DECISION_NODE=true` on the CLI and runtime. LLM variant (FALLBACK): one `agent` node per role with `instruction: "{{workflow.input}}"`, `output_schema_override` with named properties, `output_mapping` from `{{node.output.field}}` — never `{{workflow.input.field}}` (the gateway delivers text input, so it is null) |
| 4 | **agents** (`kind: agent`) — LLM variant only | `schema-refs` → agent | autonomous: `additionalConfigurations: {supports_streaming: false, max_llm_calls_per_task: 4}`; `toolsets: []` or `[artifact_management]` — never `general_agent_tools`; `modelProvider: [general]`; `deploy: true`; instruction: input is a COMPLETE pushed event, apply policy, reply with ONLY a JSON object, reply `{}` for retractions/empty input. **Pure-policy/decision agents (no lookups): `toolsets: []` AND state in the prompt "YOU HAVE NO TOOLS — never emit a tool/function call; output only the JSON object"** — a light model otherwise occasionally emits a spurious tool call and fails the node. **Tool-using agents: tell them to never fail the node — on a tool error, return valid JSON with zeroed fields and a note** so the downstream step still runs. |
| 5 | **Event Portal consumer** | `solace-event-portal` | `ep_createApplication` + `ep_createApplicationVersion` with `declaredConsumedEventVersionIds` from `event_portal.ids`; `ep_getAsyncApiForApplicationVersion` → artifact |
| 6 | validate + hand off | — | every artifact `validate_component_config`; manifest `validate_build_manifest(full_validation=true)`; author `targetWorkflowName` as the DECLARATIVE workflow name (on SAM 2.409.2 the gateway resolves it natively — the old `workflow_<uuid>` resolver was needed only through 2.360.11, fixed in 2.373.10; no `sam` restart), then deploy the solution's own manifest with a plain apply from the repo's `sam/` dir: `cd sam && SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply --manifest solutions/<slug>/manifests/dev.yaml` — or `make new-solution SLUG=<slug> TRIGGER=<trigger> [KEY=airport]`. (`make apply-reference` redeploys the prebuilt arrival-capacity solution only.) |

Agents come *after* the workflow in authoring order here only because the workflow's contract defines them; the build manifest still lists agents before the workflow (`depends_on`) so deploy order is agents → workflow → gateway.

## Phase 4 — Refine

Change the smallest thing that fixes the feedback:

| Feedback | Change |
|---|---|
| fires too often / too rarely | trigger `condition`, `recency_window`, `distinct` → `continuum_delete_trigger` + `continuum_create_trigger` |
| wrong decision | decision node: the question `criteria` and `input.detection` policy text; LLM node: the agent instruction (policy section) |
| missing field in decision | decision node: add a question + an `output_mapping` entry; LLM node: `output_schema_override` + agent output list |
| the decision needs data it does not get | extend the data product (thin view) rather than giving an agent a query tool |

Never re-create a running data product just to change a trigger.

## Naming conventions

| Thing | Pattern | Example |
|---|---|---|
| solution slug | kebab-case noun phrase | `arrival-capacity`, `sector-briefer` |
| data product | snake_case business object | `arrival_demand`, `sector_density` |
| trigger | kebab-case episode name | `breach`, `demand-exceeds-rate`, `sector-briefer-ready` |
| trigger topic | `fl/faa/<slug>/<trigger>/{key}` (composite key → more levels) | `fl/faa/arrival-capacity/breach/{airport}` |
| gateway | `<slug_snake>_gateway_app` | `arrival_capacity_gateway_app` |
| workflow | `<PascalSlug>Workflow` | `ArrivalCapacityWorkflow` |
| decision model (OpenAI Decisions) | `<slug_snake>_decider` alias → `decision: {model_alias}` | `arrival_decider` |
| agent (LLM variant) | role in Title Case | `Capacity Briefer`, `Sector Briefer` |
| decision topic | `solutions/<slug>/decision/{key}` | `solutions/arrival-capacity/decision/{airport}` |
| error topic | `solutions/<slug>/error/{key}` | |
| EP consumer app | `<slug>-decision-loop` | `arrival-capacity-decision-loop` |

## Vocabulary rules

- Say **Solace Platform** (never "PubSub+"). Say **Continuum** / **the Continuum engine**; never name the engine underneath.
- Data products are "business objects" or "live views". Triggers "publish one event per row".
- One sentence of WHY for every data product you chose or rejected.

## References

- `references/decision-node-openai.md` — **the default decision node**: native OpenAI Decisions node shape, the bound decision model, rules, and when an LLM agent is the right FALLBACK instead.
- `references/solution-manifest-format.md` — the `solution_manifest.yaml` contract.
- `references/worked-example-arrival-capacity.md` — pushed-complete-event pattern, LLM-agent variant (agent has no tools); today that decision step would be the native OpenAI Decisions node.
- `references/worked-example-sector-briefer.md` — enrich-on-demand pattern (agent makes exactly one read-only query).

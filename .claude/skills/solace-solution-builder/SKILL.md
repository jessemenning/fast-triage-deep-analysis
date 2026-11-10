---
name: solace-solution-builder
description: Build a complete real-time AI solution on the Solace Platform from one sentence — discover/compose a Continuum data product, add a trigger, then author the SAM event-mesh gateway → workflow → decision-node (native OpenAI Decisions node by default) spine and deploy it. Triggers on "build a real-time AI solution / data product / trigger / gateway / workflow" over live streaming data.
---

# Solace Solution Builder (IDE surface)

This is the **IDE authoring surface** for the same job the in-runtime `/builder` Builder
does: turn one sentence ("build a real-time AI solution that catches X and decides what to
do") into the full spine —

    Continuum data product → trigger (fl/cp/<slug>/<trigger>/{key}) → event-mesh gateway →
    workflow → decision node (native OpenAI Decisions node by default; an LLM agent only when the
    step needs tools / open reasoning / prose) → decision event on solutions/<slug>/decision/{key}

The knowledge is **not duplicated here** — it lives in the files linked below and stays the
single source of truth. This skill is a thin orchestrator: it points you at them, names the
IDE-specific tool prefixes, and gives the deploy command.

## ABSOLUTE RULES (read before authoring anything)

Full text: [../../../sam/builder/critical_prepend.md](../../../sam/builder/critical_prepend.md).
The one that silently breaks builds most often:

- **A workflow-target gateway `successOutput` MUST be EXACTLY**
  `responseType: custom` + `customExpression: "input.payload:data[0]"`. The workflow result
  arrives as `{data:[<decision object>]}`, so `data[0]` is the decision.
  NEVER `input.payload:text` (that publishes the workflow status string), and NEVER
  `responseType: text | full | structured` for a workflow target (wrong slice or `null` on
  this runtime). `input.payload:text` is correct only for a gateway targeting a plain AGENT.
- **A fan-in / aggregate trigger payload fires once per EPISODE, never once per row.**
  `payload_columns` = the group-by entity keyed in the topic (e.g. `airport`) + its
  airport-stable descriptors (airport_name, facility_type, published_rate, class)
  ONLY. NEVER a per-row id (`gufi`, `flight_id`, `event_id`, a per-row timestamp) — and NEVER a
  per-flight attribute that merely reads like a descriptor (`origin` varies flight-to-flight).
  Each distinct value re-fires the trigger, so one burst publishes N duplicate decisions (verified:
  including `origin` turned an 8-flight burst into ~15 identical GROUND_DELAY_PROGRAM decisions). The volatile-column guard strips the rolling demand
  count/sums/windows but NOT a per-row id.
- **The decision node defaults to a NATIVE OpenAI Decisions node, not an LLM.** A `type: decision`
  node bound by `decision: {model_alias: <slug>_decider}` to an OpenAI Decisions model (`kind: model`:
  `provider: "openai"`, `modelType: decision`, `modelName: "microsoft-decision-1"`,
  `apiBase: "${LLM_SERVICE_ENDPOINT}/v1"`, `authConfig: {type: apikey, api_key: "${LLM_SERVICE_API_KEY}"}`),
  served natively by the MaaS LiteLLM proxy at `/v1/decisions`. `input.event_json: "{{workflow.input.text}}"`
  (the gateway delivers text input, so `{{workflow.input.<field>}}` is null); question `criteria` encode
  the policy; `output_mapping` from `{{decide.output.answers.<q>.choice}}` / `.confidence` / boolean
  `.probability` plus `event`. Shape + when an LLM agent is the right FALLBACK:
  [../../../sam/skills/solution-design/references/decision-node-openai.md](../../../sam/skills/solution-design/references/decision-node-openai.md).
  Reuses `LLM_SERVICE_ENDPOINT` + `LLM_SERVICE_API_KEY` in `.env` + `SAM_FEATURE_WORKFLOW_DECISION_NODE=true`
  on BOTH the host CLI and the runtime — `sam config plan/apply` refuse to deploy the node otherwise.
- **Never** call any Continuum/Event-Portal `create_*` / `delete_*` / `update*` tool without
  explicit user approval in the chat first. State exactly what will be created.
- **Compose every solution from first principles — never recover or copy a worked one.**
  Build the data product from `mcp__continuum__list_source_streams` + `preview_query`, and the
  SAM half (gateway/workflow/agents) from the generic rules in the depth files below. Do NOT
  read, `git show`, or copy any existing solution under `sam/solutions/` or `demo/` (including
  deleted files recoverable from git), and do NOT paste a stored SQL / trigger / YAML as a
  starting point. The depth files are generic constraints to satisfy, not a specific solution
  to reproduce.
- **Brand:** say "Solace Platform" (never "PubSub+"), say "Continuum", never name the engine
  underneath it. No marketing language.

## Read in order, then author

1. **Procedure** — [../../../sam/builder/method.md](../../../sam/builder/method.md):
   Phase 0 discover & select → clarify → design → trigger → gateway → workflow (OpenAI Decisions
   node by default) → agents (LLM variant only);
   the SAM workflow field contract; the exact gateway `entrypoint` shape; defect rules #1–#7.
2. **Rules** — [../../../sam/builder/critical_prepend.md](../../../sam/builder/critical_prepend.md)
   (summarized above; read the whole file).
3. **Depth for the artifact you're authoring** (the same Claude-skill files the runtime uses):
   - Overall method, selection rubric, "none fits", naming, `solution_manifest.yaml`:
     [../../../sam/skills/solution-design/SKILL.md](../../../sam/skills/solution-design/SKILL.md)
   - Reading/choosing/previewing/creating/triggering Continuum data products, SQL guardrails,
     trigger design, async compile/poll:
     [../../../sam/skills/continuum-data-product/SKILL.md](../../../sam/skills/continuum-data-product/SKILL.md)
   - Event Portal governance (domains/events/schemas/applications, creation order, AsyncAPI):
     [../../../sam/skills/solace-event-portal/SKILL.md](../../../sam/skills/solace-event-portal/SKILL.md)
   - Live SAM `kind:` schema snapshots for the kind you're writing (agent, entrypoint,
     workflow, connector, model, manifest):
     [../../../sam/skills/schema-refs/SKILL.md](../../../sam/skills/schema-refs/SKILL.md)

These files teach generic rules and constraints, not a specific solution. Compose the data
product and the SAM spine yourself from the live source streams and those rules — do NOT open,
copy, or mirror any worked solution under `sam/solutions/` or `demo/` (see the ABSOLUTE RULES
above).

## MCP tools in this IDE (note the prefixes — they differ from the runtime)

The two MCP servers are configured in the repo-root [.mcp.json](../../../.mcp.json) against
the **host-published** ports. Tool names use the **server-name prefix**, NOT the runtime's
`continuum_` / `ep_` prefixes:

| Server | URL (host) | Tool names here | Runtime equivalent |
|---|---|---|---|
| Continuum | `http://localhost:8091/mcp` | `mcp__continuum__<tool>` | `continuum_<tool>` |
| Event Portal | `http://localhost:8092/mcp` | `mcp__event-portal__<tool>` | `ep_<tool>` |

So where method.md says `continuum_list_data_products`, call
`mcp__continuum__list_data_products`; `continuum_preview_query` →
`mcp__continuum__preview_query`; `continuum_create_trigger` →
`mcp__continuum__create_trigger`; and Event Portal `ep_getDomains` →
`mcp__event-portal__getDomains`, etc. Phase 0 starts with
`mcp__continuum__list_data_products(include_base=true)` +
`mcp__continuum__search_catalog`.

Requires the stack up and both tokens exported — see the repo README's
"Authoring from the IDE" section.

## Speed — keep the Continuum compile off the critical path

Every Continuum program change is a Rust compile: ~4 s warm (engine checksum cache), up to
~2 min cold. Two wins, both detailed in
[../../../sam/builder/method.md](../../../sam/builder/method.md) step 6 — apply them, don't
restate them:
- **Inline the trigger** in `mcp__continuum__create_data_product(triggers=[...])` for a new
  product → the final program compiles once, not twice.
- **Overlap the compile with authoring** → `create_*` returns `status: compiling` with the
  trigger's `subscribe_pattern` / `sam_event_rule_snippet` already usable; author the
  gateway → workflow → agents while it compiles, and poll
  `mcp__continuum__get_data_product` only to confirm `running` just before deploy.

IDE-only extra (needs a shell, so the in-runtime Builder can't do it): when REBUILDING a
solution whose rendered `cp-<slug>` program is already staged as a file, warm the cache in
the background at the very start of Phase 0 so the one compile you pay collapses to ~4 s:

    make prewarm &                       # defaults to demo/prestage/build/cp-arrival-capacity.sql
    # or: demo/prewarm.sh --sql <path-to-rendered-cp-slug.sql> &

Caveat: prewarm caches on BYTE-IDENTICAL program code and warms the *rendered* program
(the SELECT wrapped with the mesh connectors), not your SELECT — so it only helps a rebuild
whose rendered file exists and matches what `create_*` will submit, never a brand-new SELECT.
If that file is absent `make prewarm` just errors out harmlessly; rely on the two wins above.

## Deploy

The deploy path uses the **host `sam` CLI as the config client** (make var `SAM = sam-2.409.2`; the
runtime image is `solace-agent-mesh:2.409.2` — CLI and runtime must match). Author your own `kind:`
YAML into the repo under `sam/solutions/<slug>/` (its own `manifests/dev.yaml` listing the decision
model + agents + workflow + entrypoint) plus any Continuum SQL, then deploy with a **plain
declarative apply**, run from the repo's `sam/` dir:

```bash
cd sam && SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply --manifest solutions/<slug>/manifests/dev.yaml
# or, for a full deploy + wire + verify of a generated solution:
make new-solution SLUG=<slug> TRIGGER=<trigger> [KEY=airport]
```

No resolver, no uuid rewrite, no `sam` restart: on SAM 2.409.2 the event-mesh gateway resolves
`targetWorkflowName` by the workflow's DECLARATIVE name natively (the `workflow_<uuid>` resolver was
needed only through 2.360.11; the defect was FIXED in 2.373.10). Author the clean declarative name —
never hand-paste a `workflow_<uuid>` into source. `SAM_FEATURE_WORKFLOW_DECISION_NODE=true` is required
on BOTH the host CLI (to parse `type: decision`) and the runtime (docker-compose passes it through),
and a decision node reuses `LLM_SERVICE_ENDPOINT` + `LLM_SERVICE_API_KEY` from `.env` (the MaaS LiteLLM
endpoint/key — the decision model POSTs to `<endpoint>/v1/decisions`) — the placeholders are substituted
into the model resource at apply time and an unset value fails the plan loudly. (`make apply` on its own deploys only the platform
agent/connectors/skills, not a solution.)

`make apply-reference` redeploys the prebuilt `arrival-capacity` solution as an operational
demo-recovery fallback ONLY — it is not an authoring template: do not open or copy it when composing a
new solution.
This IDE surface is **additive** — it does not touch the in-runtime `/builder` Builder
(`sam/builder/*`), which keeps working unchanged.

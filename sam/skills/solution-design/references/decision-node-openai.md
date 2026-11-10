# Decision node — native OpenAI Decisions `type: decision` node (default) vs LLM agent node

Default the decision step of every solution to a **native `type: decision` workflow node bound to
a Decisions model** (OpenAI-Decisions wire format, served by the MaaS LiteLLM proxy at
`/v1/decisions`). Use an **LLM agent node** only when the step must gather evidence with tools,
reason open-endedly, or write prose.

The worked ground-truth is the deployed solution `sam/solutions/arrival-capacity/`
(`models/arrival_decider.yaml` + `workflows/ArrivalCapacityWorkflow.yaml`) — mirror its shape.

| | OpenAI Decisions node | LLM agent node |
|---|---|---|
| Fit | a typed policy decision over a pushed-complete event | evidence gathering, tool use, free-text explanation |
| Latency (measured, SAM 2.409.2) | ~0.2 s trigger→decision (LiteLLM /v1/decisions ~0.3–0.4 s warm) | 2.4–3.6 s (model 1.1–1.5 s + A2A hop + artifacts) |
| Output | `choice` (option + per-option probabilities + `confidence`), `boolean` (calibrated `probability` leaf) | model-written JSON |
| Steering | question `instructions` + `criteria` text — **policy lives in the criteria** | system prompt |
| Config | a `kind: model` decision model + a `decision: {model_alias}` node, no inline OpenAPI tool | agent YAML + model slot + `output_schema_override` + `supports_streaming: false` |

## Requirements

- SAM **2.409.2** (CLI + runtime), with `SAM_FEATURE_WORKFLOW_DECISION_NODE=true` on **BOTH** the host
  CLI (to parse/apply `type: decision`) and the runtime (docker-compose passes it through). Off by
  default, the flag-off CLI rejects `type: decision` as an unknown node type.
- No separate decision key or in-stack proxy. The decision model REUSES the platform
  `LLM_SERVICE_ENDPOINT` + `LLM_SERVICE_API_KEY` (apiBase `${LLM_SERVICE_ENDPOINT}/v1`, api_key
  `${LLM_SERVICE_API_KEY}`), substituted into the model resource at `sam config apply` time.
- The MaaS LiteLLM proxy serves the native OpenAI-Decisions format at `/v1/decisions` directly, so
  SAM POSTs straight to it — no translator. `modelName` must be `microsoft-decision-1` (the only
  model wired to `/v1/decisions` on this proxy; OpenAI/Azure chat aliases like `gpt-6-luna` answer
  `/v1/chat/completions` but hang on `/v1/decisions`).

## Shape (mirror `sam/solutions/arrival-capacity/`)

The OpenAI Decisions API is bound as a **decision MODEL** and referenced by alias — no inline OpenAPI
tool, no bearer token in the workflow (the key lives on the model, rotatable without a redeploy).

Register the model in the **solution** manifest (`models: [arrival_decider]`) so it applies before the
workflow that binds it — `provider` is immutable after a model is created, so it is a standalone
decision model.

```yaml
# models/arrival_decider.yaml
kind: model
name: arrival_decider
description: "Structured-decision model (native OpenAI-Decisions wire format, served by the MaaS LiteLLM proxy at /v1/decisions) — the arrival-capacity decision node references this by alias. provider: openai + modelType: decision are required together (SAM's native decision client)."
spec:
  alias: arrival_decider
  provider: "openai"
  modelType: decision
  modelName: "microsoft-decision-1"         # the LiteLLM model serving /v1/decisions
  apiBase: "${LLM_SERVICE_ENDPOINT}/v1"     # SAM calls <apiBase − /v1>/v1/decisions on the LiteLLM proxy
  authConfig:
    type: apikey                            # a decision model REQUIRES the apikey shape (type + api_key);
    api_key: "${LLM_SERVICE_API_KEY}"       # the chat models' authConfig.apiKey is REJECTED here
```

```yaml
# workflows/ArrivalCapacityWorkflow.yaml  (decision-relevant shape only)
kind: workflow
name: ArrivalCapacityWorkflow
description: "Arrival-capacity decision workflow — a native OpenAI Decisions decision node triages every demand-exceeds-rate breach. ADVISORY ONLY."
spec:
  appConfig:
    version: "1.0.0"
    input_schema:
      type: object
      properties:                 # the trigger payload columns
        airport: { type: string }
        airport_name: { type: string }
        demand: { type: string }
        bin_capacity: { type: string }
        overage: { type: string }
        severity: { type: string }
      required: [airport]
    # Bind the decision model by alias (no inline tool, no token in the workflow).
    decision:
      model_alias: arrival_decider
    nodes:
      - id: triage
        type: decision
        # The event-mesh gateway delivers the trigger as the workflow's TEXT input
        # (workflow.input = {"text": "<trigger JSON>"}), so the fields are NOT addressable
        # as workflow.input.<field>. This `input` object is the decision "state"; the
        # sibling `questions` carries the typed policy questions.
        input:
          event_json: "{{workflow.input.text}}"     # the trigger event as a JSON string (see rule 2)
          detection: "An airport ARRIVAL-CAPACITY INCIDENT fired: rolling-hour DEMAND exceeded the airport's PUBLISHED arrival rate (AAR). Continuum has already debounced per-bin storms into ONE material/sustained/severe incident per airport. Your job is TRIAGE — decide whether a human traffic manager must look NOW. DECISION SUPPORT ONLY — never issue a traffic-management initiative."
        # Classify mode: no route_on/routes — this node produces answers, does not branch.
        questions:
          will_materialize:
            type: choice
            instructions: "How likely is this overage to actually occur, judging eta_quality and overage size together?"
            criteria:
              YES_LIKELY: "High eta_quality and/or large overage — firm demand, breach very likely."
              UNCERTAIN: "Mixed signals — moderate overage or mid eta_quality."
              UNLIKELY: "Marginal overage and/or low eta_quality — soft demand, may clear."
          self_resolves:
            type: choice
            instructions: "Absent intervention, is this breach likely to clear on its own shortly?"
            criteria:
              LIKELY: "Marginal overage, benign/improving weather, no restriction — a blip."
              UNLIKELY: "Sustained/large overage, degraded weather, or an active restriction."
          impact:
            type: choice
            instructions: "If nothing is done, how significant is the expected arrival delay/disruption?"
            criteria:
              SEVERE: "Large overage vs AAR, major hub, or degraded weather — cascading delays."
              MODERATE: "Clear but contained overage — noticeable delay, limited cascade."
              MINOR: "Marginal overage — little practical delay."
          is_upstream_symptom:
            type: boolean
            instructions: "Is this breach most likely a SYMPTOM of a wider event (weather, or an existing restriction/GDP) rather than an independent capacity problem?"
            criteria:
              "true": "Degraded weather (IFR/LIFR) or an active restriction present — a facet of that event."
              "false": "No obvious wider cause — the capacity pressure stands on its own."
          action:
            type: choice
            instructions: "What should happen to THIS alert right now? ESCALATE takes PRECEDENCE — decide ESCALATE first, only then GROUP or SUPPRESS."
            criteria:
              ESCALATE: "Raise for a decision brief NOW — impact SEVERE and self_resolves UNLIKELY (even if weather-driven/restricted), or impact MODERATE and will_materialize YES_LIKELY."
              GROUP: "Fold into a wider incident — ONLY when the ESCALATE test fails AND is_upstream_symptom is true."
              SUPPRESS: "Do not raise — transient/marginal: will_materialize UNLIKELY, or impact MINOR, or self_resolves LIKELY."
          case_priority:
            type: choice
            instructions: "If escalated, what priority should the brief carry?"
            criteria:
              HIGH: "Impact SEVERE and will_materialize YES_LIKELY — immediate attention."
              MEDIUM: "Impact MODERATE, or SEVERE but uncertain — attention soon."
              LOW: "Grouped or suppressed breaches — informational."
    output_mapping:
      action: "{{triage.output.answers.action.choice}}"
      will_materialize: "{{triage.output.answers.will_materialize.choice}}"
      self_resolves: "{{triage.output.answers.self_resolves.choice}}"
      impact: "{{triage.output.answers.impact.choice}}"
      # boolean question → calibrated probability leaf.
      is_upstream_symptom: "{{triage.output.answers.is_upstream_symptom.probability}}"
      case_priority: "{{triage.output.answers.case_priority.choice}}"
      confidence: "{{triage.output.answers.action.confidence}}"
      event: "{{workflow.input.text}}"                 # identity travels here and in the topic key
      decided_by: "Decisions API (microsoft-decision-1 via LiteLLM)"
      advisory_notice: "Decision support only — not an automated traffic-management initiative."
      rationale:
        concat:
          - "Arrival-capacity breach (see event): rolling-hour demand exceeded the published AAR. Triage action: "
          - "{{triage.output.answers.action.choice}}"
          - " (confidence "
          - "{{triage.output.answers.action.confidence}}"
          - "); will materialize: "
          - "{{triage.output.answers.will_materialize.choice}}"
          - "; impact: "
          - "{{triage.output.answers.impact.choice}}"
          - " — Decisions API (microsoft-decision-1 via LiteLLM). Advisory only."
```

The gateway is unchanged: `successOutput` stays `responseType: custom` + `customExpression: "input.payload:data[0]"`.

The deployed workflow also fans out a when-gated System-2 brief (specialist `type: agent` nodes +
a Lead Briefer) off `triage` when `action == ESCALATE`; that slow stage is orthogonal to the
decision-node mechanics above — see the full `workflows/ArrivalCapacityWorkflow.yaml`.

### LLM-decider A/B alternative (plain agent, NOT deployed)

A non-decision variant is kept for A/B only — it is **not** loaded by `sam config apply`:

- `sam/solutions/arrival-capacity/alternatives/ArrivalCapacityWorkflow.llm-decider.yaml` — swaps the
  `type: decision` node for one `type: agent` node (`agent_name: arrival_triage_decider`,
  `instruction: "{{workflow.input}}"`), with an `output_schema_override` and `output_mapping` that reads
  `{{triage.output.<field>}}` directly from the model-written JSON.
- `sam/solutions/arrival-capacity/agents/arrival_triage_decider.yaml` — a no-tool `type: standard`
  agent (`modelProvider: [general]`, `toolsets: []`, `connectors: []`). It **must** set
  `additionalConfigurations.supports_streaming: false` and the system prompt **must emit ONLY the JSON
  object** (no prose, no fences) — a streaming agent never emits the structured result, so the node
  gets `no result embed found` → a `null` decision.

To use it: `cp alternatives/ArrivalCapacityWorkflow.llm-decider.yaml workflows/ArrivalCapacityWorkflow.yaml`,
`make apply-reference`, then `docker compose restart sam`.

## Rules

1. **Policy lives in the criteria.** A neutral question produced a confident wrong answer until the
   criteria said which option policy prescribes and when the others must NOT be chosen. Also restate
   the policy in `input.detection`.
2. **The gateway hands the workflow TEXT input**: `workflow.input` is `{"text": "<trigger JSON>"}`, so
   `{{workflow.input.<field>}}` is null everywhere (node input and `output_mapping`). Pass
   `{{workflow.input.text}}` as `input.event_json` and echo it as `event` in the decision; the key
   (`airport`) is in the topic via the gateway's `forwardContext`. There is no JSON-parse template operator.
3. **Output references** are `{{triage.output.answers.<question>.choice | .confidence | .probability}}`;
   a `boolean` question reads back via `.probability`. `rationale` is a `concat` list, not model text.
   (`decided_by` is a literal string — there is no `decide.output.model` leaf on a decision node.)
4. **Templates**: a whole-string `{{…}}` keeps its native type; mixed strings interpolate;
   `concat` / `coalesce` operator maps build values.
5. **The endpoint + key are dollar-brace env placeholders** (`${LLM_SERVICE_ENDPOINT}`,
   `${LLM_SERVICE_API_KEY}`), substituted at `sam config apply` from the host `.env`. An unset
   placeholder fails `plan`/`apply` loudly — that is the intended behaviour; never default it and
   never paste a literal key.
6. **`provider` is immutable** after a model is created on the platform, so a decision model is a
   standalone resource — list it in the solution manifest `models:` so it applies before the workflow.
7. **A node-TYPE change needs a `sam` restart.** Swapping the `triage` node between `type: agent` and
   `type: decision` deploys cleanly but the already-running workflow instance does not hot-reload —
   `docker compose restart sam` clears it (deployed resources persist in `sam_data` across the restart).
8. **Deploy**: a plain declarative apply on SAM 2.409.2 (the workflow-target name resolves natively):
   `cd sam && SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply --manifest solutions/arrival-capacity/manifests/dev.yaml`.

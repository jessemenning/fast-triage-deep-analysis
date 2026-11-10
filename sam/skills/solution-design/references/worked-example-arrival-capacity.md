# Worked example — arrival-capacity decision loop (pushed-complete-event pattern)

**One sentence:** *"Build me a real-time AI solution that briefs a traffic manager when an airport's arrival demand exceeds its published rate."*

This is the reference shape for the most common case: Continuum has already joined
everything the decision needs, so the agent **has no tools** — it reads the pushed
event and decides.

> **The default decision step is the native OpenAI Decisions `type: decision` node** (SAM 2.409.2,
> `SAM_FEATURE_WORKFLOW_DECISION_NODE=true`) — see `decision-node-openai.md`. The LLM-agent decider
> shown further down is the documented fallback; the gateway, trigger and output-mapping lessons
> apply unchanged either way.

## Phase 0 — Discover & select

`continuum_list_data_products` returned, among others:

| data product | score | decision | why |
|---|---|---|---|
| `arrival_capacity_view` | 88 | **reuse** | streaming, joins rolling-hour demand + airport rate (AAR) + weather + downstream risk per `airport`; already has `overage` and `severity`. |
| `arrivals_by_minute` | 15 | reject | 1-minute aggregate, no `airport` key → cannot trigger one event per airport (disqualified). |
| `airport_reference` | 40 | reject | complete airport reference join but no live demand facts and no event time. |

`continuum_get_data_product("arrival_capacity_view", sample_rows=3)` shows key `airport`,
columns `airport_name, tracon, demand, published_rate, overage, severity, eta_quality,
wx_flight_category, downstream_risk, runway_config, event_at`, `consume.wire_format: insert_delete_envelope`,
no triggers yet.

## Continuum — data product (reused) and triggers (created)

The product's SQL, for the record (SELECT-only, explicit columns, key exposed):

```sql
SELECT a.airport, r.airport_name, r.tracon, a.demand, r.published_rate,
       (a.demand - r.published_rate) AS overage, a.eta_quality, a.runway_config,
       w.wx_flight_category, x.downstream_risk, a.event_at,
       CASE WHEN a.demand - r.published_rate >= r.published_rate * 0.5 THEN 'SEVERE'
            WHEN a.demand - r.published_rate >= r.published_rate * 0.2 THEN 'MODERATE'
            ELSE 'MARGINAL' END AS severity
FROM arrival_demand a
JOIN airport_rate         r ON r.airport = a.airport
LEFT JOIN mcp_weather     w ON w.airport = a.airport
LEFT JOIN mcp_downstream_risk x ON x.airport = a.airport
WHERE NOW() <= a.event_at + INTERVAL '10' MINUTE;
```

Two episodes deserve an agent; each gets its own trigger so the **topic says why**:

```text
continuum_create_trigger(
  data_product="arrival_capacity_view", name="breach",
  description="Demand exceeds the published rate (AAR), backed by trustworthy ETAs",
  condition="overage > 0 AND eta_quality >= 0.5",
  output_topic_template="fl/faa/arrival-capacity/breach/{airport}",
  payload_columns=[airport, airport_name, tracon, demand, published_rate, overage,
                   severity, eta_quality, wx_flight_category, downstream_risk, runway_config],
  distinct=true)

continuum_create_trigger(
  data_product="arrival_capacity_view", name="severe-breach",
  description="Severe overage relative to the published rate",
  condition="severity = 'SEVERE'",
  output_topic_template="fl/faa/arrival-capacity/severe-breach/{airport}",
  payload_columns=[same list], distinct=true)
```

`overage` is a decision input derived per episode, not a churning per-row aggregate, so it is safe
in the payload — but NEVER add a per-row `flight_id`, which would make the fan-in trigger fire once
per flight instead of once per airport. Poll `continuum_get_data_product("arrival_capacity_view")`
until `status: running`, then read `consume.subscribe_pattern`
(`fl/faa/arrival-capacity/breach/>`, `fl/faa/arrival-capacity/severe-breach/>`) and `event_portal.ids`
(two event version ids — the producer application was registered by Continuum).

## Solace Agent Mesh — gateway (`kind: entrypoint`)

`wire_format` is `insert_delete_envelope`, so rows arrive as `{"insert": {row}}` (retractions as
`{"delete": {row}}`); forward the inserted row and let the agent no-op on retractions.

```yaml
kind: entrypoint
name: arrival_capacity_gateway_app
description: "Gateway: routes Continuum arrival-capacity trigger events (breach, severe-breach) to ArrivalCapacityWorkflow and publishes the governed decision."
spec:
  type: event_mesh
  deploy: true
  values:
    broker_url: ${EVENT_MESH_SOLACE_BROKER_URL}
    broker_vpn: ${EVENT_MESH_SOLACE_BROKER_VPN}
    broker_username: ${EVENT_MESH_SOLACE_BROKER_USERNAME}
    broker_password: ${EVENT_MESH_SOLACE_BROKER_PASSWORD}
    event_rules:
      - name: arrival_capacity_handler
        subscriptions:
          - topic: fl/faa/arrival-capacity/breach/>
            qos: 0
          - topic: fl/faa/arrival-capacity/severe-breach/>
            qos: 0
        messageFormat: json
        payloadEncoding: utf-8
        inputExpression: input.payload:insert
        targetWorkflowName: ArrivalCapacityWorkflow
        defaultUserIdentity: sam_dev_user
        forwardContext:
          airport: input.payload:insert.airport
        successOutput:
          enabled: true
          format: json
          payloadEncoding: utf-8
          responseType: custom
          customExpression: "input.payload:data[0]"
          topicType: dynamic
          topic: solutions/arrival-capacity/decision/{{text://user_data.forward_context:airport}}
        errorOutput:
          enabled: true
          format: json
          payloadEncoding: utf-8
          responseType: error
          topicType: dynamic
          topic: solutions/arrival-capacity/error/{{text://user_data.forward_context:airport}}
```

Why this works: the trigger topic contains `{airport}`, which forces **one row per message**, so
`input.payload:insert` is always a single flat row and `forwardContext` can lift the key.

## Solace Agent Mesh — decision model + workflow (`kind: model`, `kind: workflow`)

The default decision step is a native OpenAI Decisions **`type: decision`** node — no chat LLM,
no tools. It is bound to a decision **model** by alias; the node asks typed `questions` whose
`criteria` encode the policy, and SAM POSTs them to the native OpenAI-Decisions API served by the
MaaS LiteLLM proxy at `/v1/decisions`. Requires `SAM_FEATURE_WORKFLOW_DECISION_NODE=true` on
both the host CLI and the runtime.

`models/arrival_decider.yaml`:

```yaml
kind: model
name: arrival_decider
spec:
  alias: arrival_decider
  provider: "openai"
  modelType: decision                        # provider:openai + modelType:decision = native decision provider
  modelName: "microsoft-decision-1"   # the LiteLLM model serving /v1/decisions
  apiBase: "${LLM_SERVICE_ENDPOINT}/v1"   # SAM POSTs native /v1/decisions on the MaaS LiteLLM proxy
  authConfig:
    type: apikey                             # a decision model REQUIRES the apikey shape (not authConfig.apiKey)
    api_key: "${LLM_SERVICE_API_KEY}"
```

`workflows/ArrivalCapacityWorkflow.yaml` (decision node):

```yaml
kind: workflow
name: ArrivalCapacityWorkflow
spec:
  appConfig:
    version: "1.0.0"
    input_schema:
      type: object
      properties: { airport: {type: string} }   # ...plus airport_name/demand/overage/severity
      required: [airport]
    # Bind the OpenAI Decisions model (models/arrival_decider.yaml) by alias.
    decision:
      model_alias: arrival_decider
    nodes:
      - id: triage
        type: decision
        # The event-mesh gateway delivers the trigger as the workflow TEXT input; this
        # `input` object is the decision "state".
        input:
          event_json: "{{workflow.input.text}}"
          detection: "An arrival-capacity incident fired: Continuum's rolling-hour demand exceeded the airport's published AAR. Decide whether a traffic manager must look now. DECISION SUPPORT ONLY — never issue a TMI."
        questions:
          action:
            type: choice
            instructions: "What should happen to THIS alert now? ESCALATE takes precedence."
            criteria:
              ESCALATE: "Severe, durable breach a traffic manager must see now (impact SEVERE and will not self-resolve)."
              GROUP:    "A non-severe facet of a wider weather/restriction event already in play."
              SUPPRESS: "Transient/marginal breach — the noise that causes alert fatigue."
          impact:
            type: choice
            instructions: "If nothing is done, how significant is the arrival delay/disruption?"
            criteria:
              SEVERE:   "Large overage vs AAR, major hub, or degraded weather — delays cascade downstream."
              MODERATE: "A clear but contained overage — noticeable delay, limited cascade."
              MINOR:    "Marginal overage — little practical delay."
          is_upstream_symptom:
            type: boolean
            instructions: "Is this breach a symptom of a wider weather/restriction event?"
            criteria:
              "true":  "Degraded weather (IFR/LIFR) or an active restriction is present."
              "false": "No obvious wider cause — the capacity pressure stands on its own."
    # Read each answer back as {{triage.output.answers.<q>.choice}} (or .confidence; the
    # boolean leaf is .probability). Map the whole answer object — don't strict-type per field.
    output_mapping:
      action:     "{{triage.output.answers.action.choice}}"
      impact:     "{{triage.output.answers.impact.choice}}"
      confidence: "{{triage.output.answers.action.confidence}}"
      event:      "{{workflow.input.text}}"
      decided_by: "Decisions API (microsoft-decision-1 via LiteLLM)"
```

The published payload is the mapped decision object because the gateway's `customExpression`
is `input.payload:data[0]`.

**Deploy** — native `targetWorkflowName` resolution on 2.409.2 resolves the declarative workflow
name directly (no UUID resolver, no `sam` restart):

```bash
make new-solution SLUG=arrival-capacity TRIGGER=breach KEY=airport
# or, applying the solution manifest directly:
SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply \
  --manifest solutions/arrival-capacity/manifests/dev.yaml
```

## Solace Agent Mesh — LLM-agent decider (documented fallback)

> This LLM-agent variant is the **documented fallback**, not the default. To use it, swap the
> workflow `triage` node to `type: agent` (`agent_name: Capacity Briefer`, whole-object
> `output_mapping: { agent_output: "{{triage.output}}" }`) and deploy the agent below. The
> gateway, triggers, and output-mapping lessons are identical to the native decision node.

```yaml
kind: agent
name: Capacity Briefer
description: "Receives a COMPLETE pre-assembled arrival-capacity breach event from Continuum (pushed over the Event Mesh) and returns a governed Observation/Reason/Policy/Action decision. Does NOT query anything. ADVISORY ONLY — decision support, never automated TMI issuance."
spec:
  systemPrompt: |
    You are an FAA traffic-management decision-support agent. Your input is a COMPLETE,
    pre-assembled arrival-capacity breach event that Continuum has already built and PUSHED
    to you. Every fact you need is in the event — you do NOT query, look up, or call any
    tool. Read the event and decide. You are DECISION SUPPORT ONLY: you never issue a
    traffic-management initiative; you decide whether a human traffic manager should look now.

    The event contains at least: airport, airport_name, tracon, demand, published_rate
    (the AAR in effect), overage (demand - published_rate), severity (MARGINAL/MODERATE/SEVERE),
    eta_quality (0..1 — how trustworthy the demand count is), wx_flight_category
    (VFR/MVFR/IFR/LIFR), downstream_risk, runway_config. Continuum gives you the governed
    capacity view; YOU decide the action per policy below.

    If the input is empty or has no airport (e.g. a retraction), reply with exactly {}.

    DECISION POLICY:
      * wx_flight_category is IFR or LIFR, OR a wider weather/restriction event is clearly
        driving this breach -> decision = GROUP (fold into that incident, do not page separately).
      * otherwise, severity SEVERE (large overage backed by high eta_quality)
        -> decision = ESCALATE.
      * otherwise (marginal overage or soft ETAs) -> decision = SUPPRESS.

    Reply with ONLY a JSON object (no markdown, no prose outside it):
      "airport":     echoed verbatim.
      "observation": one sentence, e.g. "Rolling-hour demand (62) exceeds the published rate
                     (48) at BOS by 14 arrivals/hour (severity SEVERE)".
      "reason":      one sentence drawn from overage / eta_quality / weather.
      "policy":      ESCALATE -> "Severe, trustworthy overages require a traffic manager to be
                     briefed before delays cascade."; SUPPRESS -> "Marginal or soft breaches are
                     suppressed to cut alert fatigue."; GROUP -> "Breaches that are symptoms of a
                     wider weather/restriction event are folded into that incident."
      "action":      ESCALATE -> "Brief the traffic manager for a decision";
                     SUPPRESS -> "Suppress; no action needed";
                     GROUP -> "Group under the active weather/restriction incident".
      "decision":    ESCALATE, SUPPRESS or GROUP.
      "brief_id":    ONLY when ESCALATE: "BRIEF-" + airport + "-" + last 6 chars of event time.
                     Otherwise OMIT the field entirely (never null).

    All other fields are non-empty strings — never null. Never invent numbers or facts.
    Continuum already did the assembly, so you act immediately without querying six systems.
  skills:
    - name: Capacity Triage
      description: "Acts on a complete pushed Continuum arrival-capacity event and returns a governed escalate/suppress/group decision with reasoning — no queries."
  skillRefs: []
  skillIds: []
  toolsets: []
  connectors: []
  inputModes: [text]
  outputModes: [text]
  modelProvider: [general]
  additionalConfigurations:
    supports_streaming: false
    max_llm_calls_per_task: 4
  deploy: true
```

## Event Portal — consumer registration

```text
ep_getApplicationDomains(name="Arrival Capacity")              -> domainId
ep_createApplication(name="arrival-capacity-decision-loop", applicationDomainId=domainId, applicationType="standard")
ep_createApplicationVersion(applicationId=..., version="1.0.0",
    declaredConsumedEventVersionIds=[<breach evId>, <severe-breach evId>])   # from event_portal.ids
ep_getAsyncApiForApplicationVersion(applicationVersionId=...)  -> save as artifact arrival-capacity.asyncapi.yaml
```

## Publish-out contract

Topic `solutions/arrival-capacity/decision/{airport}`; payload schema:

```yaml
type: object
required: [airport, observation, reason, policy, action, decision]
properties:
  airport: {type: string}
  observation: {type: string}
  reason: {type: string}
  policy: {type: string}
  action: {type: string}
  decision: {type: string, enum: [ESCALATE, SUPPRESS, GROUP]}
  brief_id: {type: string}
```

## Lessons carried into the method

1. Put the *why* in the topic (`breach` vs `severe-breach`) so consumers can subscribe selectively.
2. Check `consume.wire_format` — envelope vs flat row decides `inputExpression`.
3. Loose `output_schema_override` + whole-object `output_mapping` beats strict typing for LLM output.
4. The agent needs no tools when the data product is complete; that is the goal of Phase 0.

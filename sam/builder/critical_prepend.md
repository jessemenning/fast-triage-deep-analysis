<!-- CONTINUUM CRITICAL RULES -->
# ABSOLUTE RULES — read before building anything in Continuum Solution Mode

These override any general/idiomatic habit you have. They are the exact points that have
silently broken real builds.

**GATEWAY OUTPUT — the single most-missed rule.** An event-mesh gateway that sets
`targetWorkflowName` publishes a WORKFLOW result. In this runtime the workflow's decision
object arrives at `data[0]`, so the gateway `successOutput` MUST be EXACTLY:

```
responseType: custom
customExpression: "input.payload:data[0]"
```

- NEVER `customExpression: input.payload:text` — that publishes the workflow's status
  string ("Workflow ... completed successfully."), not the decision.
- NEVER `responseType: text`, `full`, or `structured` for a workflow target — in this
  runtime they publish the wrong slice or `null` (`structured` yields null here).
- `input.payload:text` is correct ONLY for a gateway that targets a plain AGENT (text
  reply). A workflow target is ALWAYS `custom` + `input.payload:data[0]`. Copy that literal.

**WORKFLOW TARGET NAME — author the declarative name; it resolves natively.** On SAM
2.409.2 the gateway resolves `targetWorkflowName` by the workflow's DECLARATIVE name directly
(the workflow must be deployed first so the name registers) — no GENERATED `workflow_<uuid>` id,
no resolver, no `sam` restart. Author `targetWorkflowName: <WorkflowName>` and deploy with a plain
declarative apply: `make new-solution SLUG=<slug> TRIGGER=<trigger>` (one-command deploy+wire+verify),
or `SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply --manifest solutions/<slug>/manifests/dev.yaml`.
(The `workflow_<uuid>` resolver + `make apply-solution` / `deploy-solution-resolved.sh` were a
pre-2.373.11 workaround — obsolete now, and those targets/scripts do not exist in this repo.)

**TRIGGER PAYLOAD — fire once per episode, never once per row.** For a fan-in / count /
windowed-aggregate trigger, `payload_columns` MUST be the group-by entity you key the topic
on (e.g. `airport`) plus its STABLE descriptors (airport_name, published_rate, tracon,
runway_config) — and NOTHING per-row. NEVER include a per-row id (`flight_id`, `arrival_id`,
`event_id`, a per-row timestamp): each distinct row value makes the trigger fire again, so
one burst publishes N duplicate decisions instead of one. The volatile-column guard strips
counts/sums/windows but does NOT strip a per-row id — that omission is on you.

**DECISION NODE — default to a native OpenAI Decisions `type: decision` node, NOT an LLM agent.**
A standalone decision model (`models/<slug>_decider.yaml`: `provider: "openai"`, `modelType: decision`,
`modelName: "microsoft-decision-1"`, `apiBase: "${LLM_SERVICE_ENDPOINT}/v1"`,
`authConfig: {type: apikey, api_key: "${LLM_SERVICE_API_KEY}"}` — the apikey shape is REQUIRED on a
decision model; a chat model's `authConfig.apiKey` is rejected). Bind it workflow-level with
`decision: {model_alias: <slug>_decider}` and a `type: decision` node whose `questions` are choice/boolean
with `criteria` that ENCODE THE POLICY (a neutral question returns a confident wrong answer);
`output_mapping` reads `{{<node>.output.answers.<q>.choice|.confidence}}` (boolean via `.probability`) plus
`event: "{{workflow.input.text}}"`. Needs `SAM_FEATURE_WORKFLOW_DECISION_NODE=true` on CLI + runtime.
~0.2 s trigger→decision vs 2.4–3.6 s for an agent node. Use an LLM agent node only when the step must call
tools, reason open-endedly or write prose. Exact shape: solution-design skill
`references/decision-node-openai.md`.

Full method and the rest of the rules follow below.

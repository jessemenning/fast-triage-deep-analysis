# `solution_manifest.yaml` — format

One file per solution, saved as an artifact with MIME type
`application/vnd.sam-solution-manifest+yaml`. It is the human-readable record of
*what* was built and *why*; the Builder's build manifest (`components:` list)
remains the machine record of *which files* to deploy. Keep both.

## Schema (YAML)

```yaml
apiVersion: solace.com/solution-manifest/v1
kind: SolutionManifest

solution:
  name: <kebab-case slug>                 # e.g. arrival-capacity — used in topics and file names
  use_case: <one sentence, the user's words>
  owner: <team or person>                 # optional
  created: <ISO-8601 timestamp>
  status: designed | generating | generated | deployed

data_product:
  name: <snake_case>
  decision: reused | extended | created   # what Phase 0 decided
  why: <one sentence>                     # mandatory — why this one (or why a new one)
  rubric_score: <0-100>                   # from the selection rubric (omit when created)
  source_streams: [<stream>, ...]          # lineage; for a thin view this may be [<existing product>]
  key_columns: [<column>, ...]
  event_time_column: <column>             # optional
  freshness_sla: <e.g. "5s">              # optional
  sql: |                                  # only when decision is extended|created
    SELECT ...

trigger:
  name: <kebab-case>
  decision: reused | created
  condition: <SQL predicate on exposed columns>
  topic: fl/faa/<slug>/<trigger>/{key}     # must contain >= 1 {field}
  payload_columns: [<column>, ...]         # identity + decision inputs, no volatile aggregates
  distinct: true
  recency_window: <e.g. "PT10M">          # optional
  subscribe_pattern: fl/faa/<slug>/<trigger>/>    # from get_data_product.consume
  wire_format: flat_row | insert_delete_envelope # from get_data_product.consume

gateway:
  name: <slug_snake>_gateway_app
  file: entrypoints/<name>.yaml
  subscriptions: [<topic pattern>, ...]
  input_expression: input.payload | input.payload:insert
  target_workflow: <Name>Workflow
  forward_context: { <key>: input.payload:<key> }
  ack: on_receive | on_completion

workflow:
  name: <Name>Workflow
  file: workflows/<name>.yaml
  version: "2.0.0"
  nodes: [<node id>, ...]

agents:
  - name: <Role Name>
    file: agents/<name>.yaml
    role: <one line>
    model: general
    tools: []                             # or [artifact_management]; never general_agent_tools
    connectors: []                        # read-only connector only if the design needs a lookup
    max_llm_calls_per_task: 4

publish_out:
  topic: solutions/<slug>/decision/{key}
  error_topic: solutions/<slug>/error/{key}
  payload_schema:                         # JSON Schema of the decision event
    type: object
    properties: { ... }
    required: [ ... ]

event_portal:
  domain: <application domain name>
  producer_app: <name registered by Continuum>   # from get_data_product.event_portal
  event_version_ids: [<id>, ...]                  # from get_data_product.event_portal.ids
  consumer_app:
    name: <slug>-decision-loop
    application_id: <id>
    version_id: <id>
    declared_consumed_event_version_ids: [<id>, ...]
    asyncapi_artifact: <artifact filename>        # from ep_getAsyncApiForApplicationVersion

build_manifest: build_manifest.yaml       # the Builder components: file
deploy_hint: "SAM_FEATURE_WORKFLOW_DECISION_NODE=true sam-2.409.2 config apply --manifest solutions/<slug>/manifests/dev.yaml"   # or: make new-solution SLUG=<slug> TRIGGER=<trigger>
```

## Rules

- `solution.name` is the single source for every `<slug>` in topics and file names.
- `data_product.why` and `trigger.condition` are required — a manifest without them is not a design.
- `trigger.topic` must start with `fl/faa/` and contain at least one `{field}` (one row per message).
- `publish_out.payload_schema` must match the workflow `output_schema` and the agent's output list.
- Update `solution.status` and the Event Portal ids as Phase 3 progresses; re-save the artifact (new version).

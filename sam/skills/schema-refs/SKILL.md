---
name: schema-refs
description: Live Solace Agent Mesh declarative-config schema snapshots (agent, event_mesh entrypoint, workflow, mcp connector, model, manifest) generated from the sam CLI. Load the reference for the kind you are about to author before writing any kind YAML.
tags: [sam, schema, declarative-config, generated]
---

# SAM Schema References (generated)

Snapshot of `sam config schema show|example` taken from `sam version 2.373.11` on 2026-10-05T17:08:04Z
by `scripts/gen-schema-refs.sh`. These are the field tables and canonical
examples the platform enforces at `sam config apply` time.

## How to use

**Load the reference for the kind you are about to author, then author.**
Do not write a `kind:` YAML from memory.

| You are about to write | Read |
|---|---|
| an agent (`kind: agent`) | `references/agent.md` |
| an event-mesh gateway (`kind: entrypoint`, `type: event_mesh`) | `references/entrypoint-event_mesh.md` |
| a workflow (`kind: workflow`) | `references/workflow.md` |
| an MCP connector (`kind: connector`, `type: mcp`, `subtype: remote`) | `references/connector-mcp-remote.md` |
| a model (`kind: model`) | `references/model.md` |
| a manifest (`kind: manifest`) | `references/manifest.md` |
| the directory layout of a config repo | `references/layout.md` |

## Hard rules that apply to every kind

1. Every file declares its own `kind:`. Only `kind`, `name`, `description` sit at the root; everything else is under `spec:`.
2. Field names are **camelCase** (`systemPrompt`, `modelName`, `skillRefs`, `targetWorkflowName`, `forwardContext`, `successOutput`). snake_case keys are silently dropped — **except** inside `workflow.spec.appConfig`, whose content is the workflow engine's own snake_case shape (`input_schema`, `output_mapping`, `agent_name`, `output_schema_override`).
3. Secrets are always environment-variable placeholders — a dollar sign followed by the variable name in braces (exact syntax in `references/manifest.md`); appending `, default` inside the braces supplies a fallback (defaults are expanded one level deep).
4. `systemPrompt` is 100–10,000 characters; agent `connectors` max 5; `description` 10–1,000 characters.
5. `additionalConfigurations` is for runtime knobs (`supports_streaming`, `max_llm_calls_per_task`) — never `tools`. Tools come from `spec.toolsets` (`artifact_management`, `general_agent_tools`, `builder_tools`, `web_tools`, `image_tools`, `data_analysis`, `hil_tools`).
6. The only MCP connector shape is `type: mcp`, `subtype: remote` (`server_url`, `connection_type: streamable-http`, `auth_type: http`, `auth_http_scheme: bearer`, `auth_http_bearer_token`, optional `tool_name_prefix`, `allow_list` CSV).

## Field notes from working event-mesh gateways (not in the generated tables)

Inside `spec.values.event_rules[]` of an `event_mesh` entrypoint that targets a workflow:

```yaml
- name: <rule_name>
  subscriptions:
    - topic: fl/faa/<slug>/<trigger>/>        # the data product's consume.subscribe_pattern
      qos: 0
  messageFormat: json
  payloadEncoding: utf-8
  inputExpression: input.payload              # or input.payload:insert for {insert:{row}} envelopes
  targetWorkflowName: <Name>Workflow
  defaultUserIdentity: sam_dev_user
  forwardContext:
    <key>: input.payload:<key>                # carried to the output topic
  successOutput:
    enabled: true
    format: json
    payloadEncoding: utf-8
    responseType: custom
    customExpression: "input.payload:data[0]" # the workflow's structured result
    topicType: dynamic
    topic: solutions/<name>/decision/{{text://user_data.forward_context:<key>}}
  errorOutput:
    enabled: true
    format: json
    payloadEncoding: utf-8
    responseType: error
    topicType: dynamic
    topic: solutions/<name>/error/{{text://user_data.forward_context:<key>}}
```

Broker fields (`broker_url`, `broker_vpn`, `broker_username`, `broker_password`) take
dollar-brace placeholders for `EVENT_MESH_SOLACE_BROKER_URL` / `_VPN` / `_USERNAME` / `_PASSWORD` (see `references/entrypoint-event_mesh.md`). Set `spec.deploy: true`.

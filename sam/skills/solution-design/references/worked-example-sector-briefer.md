# Worked example — airspace sector briefer (enrich-on-demand pattern)

**One sentence:** *"Build me a real-time AI solution that briefs a sector controller when an airspace cell gets congested."*

Contrast with the arrival-capacity breach example: the trigger carries a **compact aggregate** (cell + count) and
the agent needs the underlying rows to write a useful briefing. Rather than pushing 24 flights
through the trigger payload, the agent makes **exactly one read-only query**. Prefer the pushed-
complete-event pattern when the row is small; use this pattern when the decision needs detail
that would bloat the trigger payload.

## Phase 0 — Discover & select

| data product | score | decision | why |
|---|---|---|---|
| `sector_density` | 70 | **extend** | live 1° cell aggregate with `flight_count`; stable composite key `(lat_cell, lon_cell)`; no trigger yet → add one. |
| `active_flights` | 55 | keep as lookup | row per aircraft, fresh, but 3,000 rows/s of position updates — triggering on it would fire per position report. Used by the agent for the one query. |
| `airport_departures` | 20 | reject | airport-keyed; no cell geometry. |

`continuum_get_data_product("sector_density", sample_rows=3)`: key `[lat_cell, lon_cell]`,
columns `lat_cell, lon_cell, flight_count, window_end`, `consume.wire_format: flat_row`.

## Continuum — trigger (created)

```text
continuum_create_trigger(
  data_product="sector_density", name="sector-briefer-ready",
  description="A 1-degree airspace cell crossed the congestion threshold",
  condition="flight_count >= 12",
  output_topic_template="fl/faa/sector-briefer-ready/{lat_cell}/{lon_cell}",
  payload_columns=[lat_cell, lon_cell, flight_count],
  distinct=true, recency_window="PT15M")
```

`flight_count` changes every second above the threshold; `distinct=true` plus a 15-minute
`recency_window` makes the trigger fire **once per congestion episode per cell**, not once per
count change. The composite key becomes two topic levels.

## Gateway (`kind: entrypoint`)

```yaml
kind: entrypoint
name: sector_briefer_gateway_app
description: "Gateway: routes Continuum sector-congestion trigger events to SectorBrieferWorkflow and publishes the spoken briefing per cell."
spec:
  type: event_mesh
  deploy: true
  values:
    broker_url: ${EVENT_MESH_SOLACE_BROKER_URL}
    broker_vpn: ${EVENT_MESH_SOLACE_BROKER_VPN}
    broker_username: ${EVENT_MESH_SOLACE_BROKER_USERNAME}
    broker_password: ${EVENT_MESH_SOLACE_BROKER_PASSWORD}
    event_rules:
      - name: density_alert_handler
        subscriptions:
          - topic: fl/faa/sector-briefer-ready/>
            qos: 0
        messageFormat: json
        payloadEncoding: utf-8
        inputExpression: input.payload            # flat_row: no envelope
        targetWorkflowName: SectorBrieferWorkflow
        defaultUserIdentity: sam_dev_user
        forwardContext:
          lat: input.payload:lat_cell
          lon: input.payload:lon_cell
          flight_count: input.payload:flight_count
        successOutput:
          enabled: true
          format: json
          payloadEncoding: utf-8
          responseType: custom
          customExpression: "input.payload:data[0]"
          topicType: dynamic
          topic: solutions/sector-briefer/decision/{{text://user_data.forward_context:lat}}/{{text://user_data.forward_context:lon}}
        errorOutput:
          enabled: true
          format: json
          payloadEncoding: utf-8
          responseType: error
          topicType: dynamic
          topic: solutions/sector-briefer/error/{{text://user_data.forward_context:lat}}/{{text://user_data.forward_context:lon}}
```

## Workflow (`kind: workflow`)

```yaml
kind: workflow
name: SectorBrieferWorkflow
description: |
  Analyzes an airspace congestion trigger event and produces a sector-handoff briefing script.
  The Sector Briefer agent makes one live Continuum query for the flights in and around the
  congested cell, then writes the spoken briefing in a single step, returning {result, situation}.
spec:
  appConfig:
    description: Congestion event -> one live query -> spoken briefing.
    version: 2.0.0
    input_schema:
      type: object
      description: One sector_density row (lat_cell, lon_cell, flight_count)
    nodes:
      - id: briefing
        type: agent
        agent_name: Sector Briefer
        instruction: |
          {{workflow.input}}
        output_schema_override:
          type: object
          properties:
            result: {type: string, description: "Spoken sector-handoff briefing script"}
            situation: {type: string, description: "3-4 word plain-english summary"}
          required: [result]
    output_mapping:
      result: "{{briefing.output.result}}"
    output_schema:
      type: object
      properties:
        result: {type: string, description: "Spoken sector-handoff briefing script"}
      required: [result]
```

## Agent (`kind: agent`, autonomous, one read-only query)

The agent gets a **read-only** Continuum connector — a second connector instance whose
`allow_list` is `preview_query` only (e.g. `continuum-mcp-readonly`, `tool_name_prefix: continuum_`).
Never hand an autonomous agent the full connector with `create_*` / `delete_*` tools.

```yaml
kind: agent
name: Sector Briefer
description: "Queries live flight data from Continuum for a congested airspace cell and writes the sector-handoff briefing in one step. Emits {result, situation}."
spec:
  systemPrompt: |
    You are an airspace sector controller assistant. Input is a JSON event for a congested
    1-degree cell with fields lat_cell, lon_cell, flight_count.

    TOOL USE — you have exactly ONE tool: continuum_preview_query. Call it ONCE with the
    query below (substitute LAT / LON with lat_cell / lon_cell, limit=24), then reply.
    Do NOT call any other tool and do NOT query twice.

      SELECT callsign, origin, destination,
             ROUND(CAST(altitude_ft AS DOUBLE),0) AS altitude_ft,
             ROUND(CAST(heading AS DOUBLE),0)     AS heading
      FROM active_flights
      WHERE CAST(FLOOR(CAST(latitude AS DOUBLE)) AS INTEGER)  BETWEEN LAT-1 AND LAT+1
        AND CAST(FLOOR(CAST(longitude AS DOUBLE)) AS INTEGER) BETWEEN LON-1 AND LON+1
      ORDER BY altitude_ft DESC

    The result is a representative sample; use the event's flight_count for the headline
    count. Never invent flights. If the query fails, brief from the event alone and say so.

    Reply with ONLY a JSON object (no markdown, no prose outside it):
      "result":    a 3-4 sentence sector-handoff briefing (~70-90 words) read aloud —
                   opening (cell lat/lon + flight_count), body (altitude layering, crossing
                   conflicts, dominant flow, 2-3 named flights), closing (one action:
                   spacing / reroute / metering / altitude restriction). If no rows, say
                   the sector is clear.
      "situation": 3-4 word summary (e.g. "Fort Worth crossing conflicts").
  skills:
    - name: Sector Briefing
      description: "Queries live flight data from Continuum and writes the sector-handoff briefing in one step."
  skillRefs: []
  skillIds: []
  toolsets: []
  connectors: [continuum-mcp-readonly]
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
ep_getApplicationDomains(name="Airspace")                        -> domainId
ep_createApplication(name="sector-briefer-decision-loop", applicationDomainId=domainId, applicationType="standard")
ep_createApplicationVersion(applicationId=..., version="1.0.0",
    declaredConsumedEventVersionIds=[<sector-briefer-ready evId>])
ep_getAsyncApiForApplicationVersion(applicationVersionId=...)    -> artifact sector-briefer.asyncapi.yaml
```

## Publish-out contract

Topic `solutions/sector-briefer/decision/{lat_cell}/{lon_cell}`; payload
`{result: string, situation: string}`.

## Lessons carried into the method

1. A compact aggregate trigger + one bounded lookup is fine; N lookups is a design smell — extend the data product instead.
2. `distinct` + `recency_window` turn a churning aggregate into once-per-episode.
3. Composite keys map to multiple topic levels in both the trigger topic and the decision topic.
4. Autonomous agents get read-only connectors only.

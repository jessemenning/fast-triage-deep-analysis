---
name: solace-event-portal
description: How SolutionBuilder uses Solace Event Portal through the ep_* MCP tools — discover domains/events/schemas/applications, respect the Domain -> Schema -> Event -> Application creation order, let Continuum own the producer side, and register the generated gateway/workflow as the consumer application with an AsyncAPI export.
tags: [solace-platform, event-portal, asyncapi, governance]
---

# Solace Event Portal — discovery and consumer registration

Event Portal is the design-time catalog of the Event Mesh: **application domains** own
**schemas**, **events** and **applications**; each has **versions**; an application version
declares which event versions it **produces** and **consumes**. SolutionBuilder uses it for two
things only:

1. **Discovery** (Phase 0) — what already exists around the use case.
2. **Consumer registration** (Phase 3 step 5) — record the generated gateway/workflow as the
   application that consumes the trigger event, and export its AsyncAPI.

## Who creates what

| Object | Created by | Tool |
|---|---|---|
| Application domain (if missing) | you, with approval | `ep_createApplicationDomain` |
| Schema + schema version of the trigger payload | **Continuum** (`register_in_event_portal=true` / `continuum_sync_event_portal`) | — |
| Event + event version for the trigger topic | **Continuum** | — |
| **Producer** application + version | **Continuum** | — |
| **Consumer** application + version (the gateway/workflow) | **you** | `ep_createApplication`, `ep_createApplicationVersion` |
| Decision event (`solutions/<slug>/decision/{key}`) as a *produced* event | you, optional, with approval | `ep_createSchema`/`Version`, `ep_createEvent`/`Version` |
| AsyncAPI document for the consumer | you | `ep_getAsyncApiForApplicationVersion` |

Never recreate the producer side — read its ids from `continuum_get_data_product(...).event_portal.ids`.

## Creation order (when you do create)

```
Domain  →  Schema (+ SchemaVersion)  →  Event (+ EventVersion, referencing schemaVersionId)
        →  Application (+ ApplicationVersion, declaring consumed/produced eventVersionIds)
```

Each `create*Version` needs the parent id from the corresponding `create*`. A version references
other objects **by version id** (`schemaVersionId`, `declaredConsumedEventVersionIds`).

## Discovery recipe (Phase 0)

```text
ep_getApplicationDomains(pageSize=50)                       → pick the domain whose name matches the business area
ep_getApplications(applicationDomainId=<id>)                → existing producers/consumers
ep_getEvents(applicationDomainId=<id>, name=<contains>)     → events already flowing
ep_getEventVersions(eventIds=[...])                         → topic address + schema per version
ep_getSchemas(applicationDomainId=<id>)                     → payload contracts
```

Report in one line per hit: `domain / event@version → topic` and whether a data product already
publishes it (cross-check `get_data_product.event_portal.names`).

## Consumer registration recipe (Phase 3 step 5)

```text
ids = continuum_get_data_product(<product>).event_portal.ids     # after triggers exist and status=running
ep_getApplications(applicationDomainId=ids.application_domain_id, name="<slug>-decision-loop")
  → if none:
      ep_createApplication(name="<slug>-decision-loop",
                           applicationDomainId=ids.application_domain_id,
                           applicationType="standard",
                           brokerType="solace")
ep_createApplicationVersion(applicationId=<app id>,
                            version="1.0.0",
                            displayName="<slug> decision loop",
                            description="Solace Agent Mesh event-mesh gateway <gateway> -> <Workflow> -> agents",
                            declaredConsumedEventVersionIds=[ids.event_version_id, ...],   # one per trigger
                            declaredProducedEventVersionIds=[<decision event version id>])  # optional
ep_getAsyncApiForApplicationVersion(applicationVersionId=<version id>, format="yaml")
  → save as artifact <slug>.asyncapi.yaml
```

Record `application_id`, `version_id`, `declared_consumed_event_version_ids` and the AsyncAPI
artifact name in `solution_manifest.yaml` → `event_portal.consumer_app`.

To add a second consumed event later: `ep_updateApplicationVersion(versionId, declaredConsumedEventVersionIds=[...])`
(versions in `DRAFT` state are editable; released versions need a new version).

## Topic address conventions

| Direction | Pattern | Example |
|---|---|---|
| Continuum trigger (produced by the data product) | `fl/faa/<slug>/<trigger>/{key}` | `fl/faa/arrival-capacity/breach/{airport}` |
| Solution decision (produced by the gateway's successOutput) | `solutions/<slug>/decision/{key}` | `solutions/arrival-capacity/decision/{airport}` |
| Solution error | `solutions/<slug>/error/{key}` | |
| External / upstream systems (source streams) | `<system>/<object>/<verb>/v<N>` | `salesforce/case/created/v1` |

In Event Portal the `{key}` levels are **variable** topic address levels; every other level is literal.

## Guardrails

- Read before write: always `ep_get*` to find an existing object before `ep_create*`.
- Explicit user approval before any `ep_create*` / `ep_update*`.
- Delete and state-transition operations are intentionally not exposed on the connector.
- Schemas you create are JSON Schema draft-07 (`"$schema": "http://json-schema.org/draft-07/schema#"`).
- Say "Solace Platform" and "Event Portal"; never "PubSub+".

## References

- `references/ep-tool-reference.md` — every `ep_*` tool on the connector, its route and typical parameters.

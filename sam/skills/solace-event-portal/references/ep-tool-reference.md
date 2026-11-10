# Event Portal MCP — tool reference

Tool names are the Event Portal Designer API `operationId`s, surfaced with the connector prefix
`ep_` (connector `event-portal-mcp`). The connector `allow_list` exposes exactly the 24 tools
below; `delete*` and `update*State` operations are deliberately excluded.

Common list parameters: `pageSize` (1–100), `pageNumber`, `applicationDomainId`/`applicationDomainIds`,
`name` (exact), `ids`. Responses are `{data: [...], meta: {pagination}}`.

## Application domains

| Tool | Route | Use |
|---|---|---|
| `ep_getApplicationDomains` | `GET /applicationDomains` | list/filter domains (`name`, `include=stats`) |
| `ep_getApplicationDomain` | `GET /applicationDomains/{id}` | one domain |
| `ep_createApplicationDomain` | `POST /applicationDomains` | body `{name, description?, uniqueTopicAddressEnforcementEnabled?, topicDomainEnforcementEnabled?}` — only when no suitable domain exists |

## Applications

| Tool | Route | Use |
|---|---|---|
| `ep_getApplications` | `GET /applications` | list (`applicationDomainId`, `name`, `applicationType`) |
| `ep_getApplication` | `GET /applications/{id}` | one application |
| `ep_createApplication` | `POST /applications` | body `{name, applicationDomainId, applicationType: "standard", brokerType: "solace"}` — the **consumer** app `<slug>-decision-loop` |
| `ep_updateApplication` | `PATCH /applications/{id}` | rename / re-describe |

## Application versions

| Tool | Route | Use |
|---|---|---|
| `ep_getApplicationVersions` | `GET /applicationVersions` | list (`applicationIds`, `stateIds`) |
| `ep_getApplicationVersion` | `GET /applicationVersions/{versionId}` | one version (shows declared consumed/produced ids) |
| `ep_createApplicationVersion` | `POST /applicationVersions` | body `{applicationId, version: "1.0.0", displayName?, description?, declaredConsumedEventVersionIds: [...], declaredProducedEventVersionIds?: [...]}` |
| `ep_updateApplicationVersion` | `PATCH /applicationVersions/{versionId}` | change declared ids / description on a DRAFT version |
| `ep_getAsyncApiForApplicationVersion` | `GET /applicationVersions/{applicationVersionId}/asyncApi` | `format=yaml|json`, `asyncApiVersion=2.5.0` — save as artifact |

## Events

| Tool | Route | Use |
|---|---|---|
| `ep_getEvents` | `GET /events` | list (`applicationDomainId`, `name`, `shared`) |
| `ep_getEvent` | `GET /events/{id}` | one event |
| `ep_createEvent` | `POST /events` | body `{name, applicationDomainId, shared?, description?}` — only for a *decision* event you choose to register |

## Event versions

| Tool | Route | Use |
|---|---|---|
| `ep_getEventVersions` | `GET /eventVersions` | list (`eventIds`, `ids`) — topic address + schemaVersionId |
| `ep_getEventVersion` | `GET /eventVersions/{id}` | one version |
| `ep_createEventVersion` | `POST /eventVersions` | body `{eventId, version, displayName?, schemaVersionId, deliveryDescriptor: {brokerType: "solace", address: {addressLevels: [{name, addressLevelType: "literal" | "variable"}]}}}` |

## Schemas

| Tool | Route | Use |
|---|---|---|
| `ep_getSchemas` | `GET /schemas` | list (`applicationDomainId`, `name`, `schemaType`) |
| `ep_getSchema` | `GET /schemas/{id}` | one schema |
| `ep_createSchema` | `POST /schemas` | body `{name, applicationDomainId, schemaType: "jsonSchema", contentType: "json", shared?}` |

## Schema versions

| Tool | Route | Use |
|---|---|---|
| `ep_getSchemaVersions` | `GET /schemaVersions` | list (`schemaIds`, `ids`) |
| `ep_getSchemaVersion` | `GET /schemaVersions/{versionId}` | one version (contains `content`) |
| `ep_createSchemaVersion` | `POST /schemaVersions` | body `{schemaId, version, content: "<JSON Schema draft-07 as string>", displayName?}` |

## Not exposed (by design)

`deleteApplicationDomain`, `deleteApplication`, `deleteApplicationVersion`, `deleteEvent`,
`deleteEventVersion`, `deleteSchema`, `deleteSchemaVersion`, `update*State`,
`updateMsgSvcAssociation*`, `importApplicationDomains`, `exportApplicationDomains`,
custom-attribute operations. If a design genuinely needs one, ask the operator to run it in the
Event Portal Designer UI.

## Typical address levels for a trigger event

`fl/faa/arrival-capacity/breach/{airport}` →

```json
[{"name":"fl","addressLevelType":"literal"},
 {"name":"faa","addressLevelType":"literal"},
 {"name":"arrival-capacity","addressLevelType":"literal"},
 {"name":"breach","addressLevelType":"literal"},
 {"name":"airport","addressLevelType":"variable"}]
```

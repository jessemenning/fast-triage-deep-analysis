"""Pydantic v2 models for the catalog, tool inputs and tool outputs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

StreamKind = Literal["raw", "derived"]
Changelog = Literal["full", "inserts_only"]
ProductStatus = Literal["compiling", "redeploying", "running", "failed", "stopped", "unknown"]


def utcnow_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class ColumnDef(BaseModel):
    # extra="forbid" catches YAML flow mappings that split on an unquoted comma.
    model_config = ConfigDict(extra="forbid")

    name: str
    type: str
    description: str | None = None


class StreamDef(BaseModel):
    """A hand-authored registry entry for one ingestable (or reference) stream."""

    model_config = ConfigDict(extra="forbid")

    name: str
    kind: StreamKind
    description: str
    relation: str
    base_pipeline: str = "arrival-capacity"
    topic_subscriptions: list[str] = Field(default_factory=list)
    topic_pattern: str | None = None
    ddl: str | None = None
    decode_sql: str | None = None
    columns: list[ColumnDef] = Field(default_factory=list)
    key_columns: list[str] = Field(default_factory=list)
    event_time_column: str | None = None
    changelog: Changelog = "full"
    append_only: bool = False
    """Mesh feed is insert-only (no deletes): render the source table WITH ('append_only' = 'true')
    so the engine can bound state. Set only for streams whose broker topic carries inserts only
    (raw event/signal feeds), NEVER for full-changelog streams like capacity_assessment."""
    ingestable: bool = True
    volatile_columns: list[str] = Field(default_factory=list)
    include_topic: bool = False
    include_broker_timestamp: bool = False
    hint: str | None = None
    preview_cte: str | None = None
    # Derived streams only: also load the base pipeline's current rows once at start
    # (url_input against the base pipeline's ad-hoc query endpoint) so a product created
    # after the base pipeline published its snapshot does not start empty.
    snapshot: bool = False
    snapshot_sql: str | None = None
    notes: str | None = None

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def example_topic(self) -> str | None:
        if not self.topic_pattern:
            return None
        from continuum_mcp.sql_guard import sample_topic

        return sample_topic(self.topic_pattern)

    def summary(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "description": self.description,
            "relation": self.relation,
            "base_pipeline": self.base_pipeline,
            "topic_pattern": self.topic_pattern,
            "topic_subscriptions": self.topic_subscriptions,
            "example_topic": self.example_topic,
            "columns": [c.model_dump(exclude_none=True) for c in self.columns],
            "key_columns": self.key_columns,
            "event_time_column": self.event_time_column,
            "changelog": self.changelog,
            "ingestable": self.ingestable,
            "volatile_columns": self.volatile_columns,
            "hint": self.hint,
        }


class BaseProduct(BaseModel):
    """A materialized view of the read-only base pipeline, exposed as a product."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str
    owner: str = "platform"
    tags: list[str] = Field(default_factory=list)
    live_topic: str | None = None
    key_columns: list[str] = Field(default_factory=list)
    managed: bool = False


class BasePipeline(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline: str
    description: str = ""
    products: list[BaseProduct] = Field(default_factory=list)


class TriggerSpec(BaseModel):
    """User-facing trigger definition (input to create_data_product / create_trigger)."""

    name: str = Field(description="Short kebab-case trigger name, unique within the product.")
    description: str = Field(default="", description="What business condition this trigger detects.")
    condition: str = Field(description="SQL boolean expression over the product's columns.")
    output_topic_template: str = Field(
        description="Event topic, must start with fl/cp/<slug>/ and contain at least one {column} placeholder."
    )
    payload_columns: list[str] | None = Field(
        default=None,
        description=(
            "Columns carried on the event — identity/descriptor columns only. Default: all product "
            "columns minus volatile ones. Volatile columns (rolling counts/sums/amounts, most-recent "
            "timestamps, balances) are stripped automatically to preserve fire-once semantics; the "
            "key and any topic placeholders are always kept. Set allow_volatile_payload=true to keep them."
        ),
    )
    allow_volatile_payload: bool = Field(
        default=False,
        description=(
            "Escape hatch: keep volatile columns in the payload even though they make the trigger re-fire "
            "on every change (dozens of events per episode). Leave false unless you truly want that."
        ),
    )
    distinct: bool = Field(default=True, description="SELECT DISTINCT so each distinct payload fires once.")
    recency_window: str | None = Field(
        default=None,
        description="Only fire for rows whose event time is within this window, e.g. '5 minutes'.",
    )


class TriggerEventPortal(BaseModel):
    event_id: str | None = None
    event_version_id: str | None = None
    schema_id: str | None = None
    schema_version_id: str | None = None


class TriggerRecord(BaseModel):
    """Catalog representation of a deployed trigger."""

    name: str
    description: str = ""
    condition: str
    topic_template: str
    subscribe_pattern: str
    sample_topic: str
    payload_columns: list[str]
    distinct: bool = True
    recency_window: str | None = None
    payload_json_schema: dict[str, Any] = Field(default_factory=dict)
    view_name: str
    event_portal: TriggerEventPortal = Field(default_factory=TriggerEventPortal)
    sam_event_rule_snippet: str = ""
    created_at: str = Field(default_factory=utcnow_iso)


class EventPortalState(BaseModel):
    domain_id: str | None = None
    application_id: str | None = None
    application_version_id: str | None = None
    application_version: str | None = None
    schema_id: str | None = None
    schema_version_id: str | None = None
    event_ids: dict[str, str] = Field(default_factory=dict)
    event_version_ids: dict[str, str] = Field(default_factory=dict)
    status: Literal["pending", "registered", "skipped", "failed", "unregistered"] = "pending"
    error: str | None = None
    synced_at: str | None = None


class SchemaColumn(BaseModel):
    name: str
    type: str
    nullable: bool = True


class HistoryEntry(BaseModel):
    at: str = Field(default_factory=utcnow_iso)
    action: str
    detail: str | None = None


class ProductManifest(BaseModel):
    """Authoritative metadata for one created data product (catalog/products/<slug>/manifest.yaml)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    name: str
    slug: str
    description: str = ""
    owner: str = "unknown"
    tags: list[str] = Field(default_factory=list)
    freshness_sla: str | None = None
    source_streams: list[str]
    sql: str
    key_columns: list[str]
    event_time_column: str | None = None
    live_topic: str
    pipeline: str
    workers: int = 2
    queues: list[str] = Field(default_factory=list)
    schema_: list[SchemaColumn] = Field(default_factory=list, alias="schema")
    status: ProductStatus = "compiling"
    error: str | None = None
    created_at: str = Field(default_factory=utcnow_iso)
    updated_at: str = Field(default_factory=utcnow_iso)
    program_hash: str | None = None
    triggers: list[TriggerRecord] = Field(default_factory=list)
    event_portal: EventPortalState = Field(default_factory=EventPortalState)
    history: list[HistoryEntry] = Field(default_factory=list)
    managed: bool = True

    @property
    def view_name(self) -> str:
        return self.slug.replace("-", "_")

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.schema_]

    def touch(self, action: str, detail: str | None = None) -> None:
        self.updated_at = utcnow_iso()
        self.history.append(HistoryEntry(action=action, detail=detail))

    def trigger(self, name: str) -> TriggerRecord | None:
        return next((t for t in self.triggers if t.name == name), None)


class PipelineStatus(BaseModel):
    program_status: str | None = None
    deployment_status: str | None = None
    storage_status: str | None = None
    error: str | None = None
    exists: bool = True

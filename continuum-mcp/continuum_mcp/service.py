"""ContinuumService: the business logic behind every MCP tool.

Tools in server.py are thin wrappers; everything testable lives here.
User-facing strings say "Solace Platform", "Continuum" and "the Continuum
engine" — never the engine vendor.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import re
from typing import Any

import yaml

from continuum_mcp import schema_map
from continuum_mcp.catalog import Catalog, CatalogError
from continuum_mcp.config import Settings
from continuum_mcp.deployer import Deployer, derive_status
from continuum_mcp.ep_client import EventPortalClient, EventPortalError, pascal
from continuum_mcp.feldera_client import (
    FelderaClient,
    FelderaError,
    program_error_text,
    sql_error_messages,
)
from continuum_mcp.models import (
    EventPortalState,
    PipelineStatus,
    ProductManifest,
    SchemaColumn,
    StreamDef,
    TriggerEventPortal,
    TriggerRecord,
    TriggerSpec,
    utcnow_iso,
)
from continuum_mcp.semp_client import SempClient, SempError
from continuum_mcp.sql_guard import (
    SqlGuardError,
    check_condition,
    ensure_single_select,
    extract_relations,
    parse_recency_window,
    placeholders,
    sample_topic,
    slugify,
    subscribe_pattern,
    validate_identifiers,
    validate_topic_template,
    validate_trigger_name,
)
from continuum_mcp.templates import (
    PIPELINE_TAG,
    ProgramRenderer,
    RenderedProgram,
    RenderSpec,
    TriggerRender,
    pipeline_name,
    pipeline_tags,
    queue_name,
    trigger_view_name,
)

log = logging.getLogger("continuum-mcp.service")

COLD_COMPILE_SECS = 120
WARM_COMPILE_SECS = 4
NEXT_HINT = "poll get_data_product until status=running"

# --- Trigger payload guard --------------------------------------------------
# Volatile columns — rolling counts/sums/amounts, windowed metrics, most-recent
# timestamps, balances — change on every event within an episode. With SELECT
# DISTINCT + skip_deletes, a trigger re-fires whenever any payload column changes,
# so a volatile column in the payload makes the trigger fire dozens of times per
# episode instead of once. The guard keeps trigger payloads to identity/descriptor
# columns so triggers fire once per episode. Heuristic, with an escape hatch
# (TriggerSpec.allow_volatile_payload); the key and topic placeholders are never stripped.
# Aggregate/metric tokens that mark a column as a rolling value. Note: bare "demand"
# is deliberately NOT here — a single point value is stable per key; a *windowed*
# aggregate (e.g. demand_60m) is caught by the window suffix, and total_/sum_ by tokens.
# Ratio/utilization columns without a window suffix or aggregate token (e.g.
# utilization_pct) are NOT caught here and are declared per-stream in volatile_columns.
_VOLATILE_TOKENS = frozenset({
    "count", "cnt", "sum", "total", "avg", "mean", "stddev", "stdev",
    "var", "variance", "rate", "velocity", "volume", "balance", "window",
})
_WINDOW_SUFFIX = re.compile(r"_\d+(?:ms|s|m|h|d|min|sec|hr|hrs|day|days)$", re.IGNORECASE)
_RECENCY_PREFIX = ("last_", "latest_", "most_recent_", "recent_")


def is_volatile_column(name: str, event_time_column: str | None = None) -> bool:
    """Heuristic: does this column's value churn within a single episode?

    True for rolling aggregates (``trades_60s``, ``cumulative_usd_60s``,
    ``notional_usd_24h``), balances, and most-recent timestamps (``last_seen_at``,
    ``last_scored_at``) — the things that must stay OUT of a fire-once trigger
    payload. Stable identity/dimension columns and a per-row event-time column
    (``counterparty_account``, ``cp_type``, ``credit_rating``, ``value_date``, ``event_at``)
    return False. ``event_time_column`` is accepted for future use but a plain
    event-time column is treated as stable; aggregated recency columns are caught by
    the ``last_``/``latest_`` prefix or the window suffix instead.
    """
    _ = event_time_column
    n = name.strip().lower()
    if not n:
        return False
    if _WINDOW_SUFFIX.search(n):
        return True
    if n.startswith(_RECENCY_PREFIX):
        return True
    return bool(_VOLATILE_TOKENS & set(n.split("_")))


class ContinuumError(Exception):
    """User-facing failure: message + optional structured details."""

    def __init__(self, message: str, details: Any = None):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {"error": self.message}
        if self.details is not None:
            out["details"] = self.details
        return out


class ContinuumService:
    def __init__(
        self,
        settings: Settings,
        catalog: Catalog,
        feldera: FelderaClient,
        semp: SempClient,
        ep: EventPortalClient | None = None,
        deployer: Deployer | None = None,
        renderer: ProgramRenderer | None = None,
    ):
        self.settings = settings
        self.catalog = catalog
        self.feldera = feldera
        self.semp = semp
        self.ep = ep
        self.deployer = deployer or Deployer(feldera, catalog, settings)
        self.renderer = renderer or ProgramRenderer(settings)
        self._background: set[asyncio.Task[Any]] = set()

    # ------------------------------------------------------------------ lifecycle
    async def startup(self) -> None:
        self.catalog.load()
        resumed = await self.deployer.resume()
        if resumed:
            log.info("resumed deployments: %s", resumed)
        if not self.settings.ep_enabled:
            log.warning("SOLACE_API_TOKEN unset: Event Portal registration disabled")

    async def shutdown(self) -> None:
        await self.deployer.shutdown()
        for t in list(self._background):
            t.cancel()
        for t in list(self._background):
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await t
        await self.feldera.aclose()
        await self.semp.aclose()
        if self.ep:
            await self.ep.aclose()

    def _spawn(self, coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    # ------------------------------------------------------------------ streams
    def list_source_streams(self, kind: str | None = "all") -> list[dict[str, Any]]:
        if kind not in (None, "all", "raw", "derived"):
            raise ContinuumError("kind must be one of raw, derived, all")
        return [s.summary() for s in self.catalog.streams_by_kind(kind)]

    # ------------------------------------------------------------------ products
    async def list_data_products(
        self, status: str | None = None, tag: str | None = None, include_base: bool = True
    ) -> list[dict[str, Any]]:
        engine: dict[str, dict[str, Any]] = {}
        try:
            for p in await self.feldera.list_pipelines():
                engine[p["name"]] = p
        except (FelderaError, Exception) as exc:  # noqa: BLE001 - engine down must not hide the catalog
            log.warning("could not list pipelines from the Continuum engine: %s", exc)

        items: list[dict[str, Any]] = []
        for m in self.catalog.products.values():
            info = engine.get(m.pipeline)
            items.append(self._product_summary(m, info))
        known_pipelines = {m.pipeline for m in self.catalog.products.values()}
        for name, info in engine.items():
            if name in known_pipelines or PIPELINE_TAG not in (info.get("tags") or []):
                continue
            slug = next(
                (t.split("=", 1)[1] for t in info.get("tags", []) if t.startswith("continuum:slug=")),
                name.removeprefix("cp-"),
            )
            items.append(
                {
                    "name": slug,
                    "description": info.get("description", ""),
                    "owner": "unknown",
                    "tags": info.get("tags", []),
                    "status": derive_status(info),
                    "pipeline": name,
                    "live_topic": None,
                    "trigger_names": [],
                    "freshness_sla": None,
                    "managed": True,
                    "orphan": True,
                    "note": "pipeline exists in the Continuum engine but has no catalog manifest",
                }
            )
        if include_base:
            base_info = engine.get(self.catalog.base_pipeline_name() or "")
            base_status = derive_status(base_info) if base_info else "unknown"
            for bp in self.catalog.base_products():
                items.append(
                    {
                        "name": bp.name,
                        "description": bp.description,
                        "owner": bp.owner,
                        "tags": bp.tags,
                        "status": base_status,
                        "pipeline": self.catalog.base_pipeline_name(),
                        "live_topic": bp.live_topic,
                        "trigger_names": [],
                        "freshness_sla": "continuous (strongly consistent per event)",
                        "managed": False,
                    }
                )
        if status:
            items = [i for i in items if i["status"] == status]
        if tag:
            items = [i for i in items if tag in (i.get("tags") or [])]
        return items

    def _product_summary(self, m: ProductManifest, info: dict[str, Any] | None) -> dict[str, Any]:
        status = m.status if m.status in ("compiling", "redeploying") or info is None else derive_status(info)
        return {
            "name": m.name,
            "slug": m.slug,
            "description": m.description,
            "owner": m.owner,
            "tags": m.tags,
            "status": status,
            "pipeline": m.pipeline,
            "live_topic": m.live_topic,
            "trigger_names": [t.name for t in m.triggers],
            "freshness_sla": m.freshness_sla,
            "managed": True,
            "error": m.error,
        }

    async def get_data_product(self, name: str, sample_rows: int = 5) -> dict[str, Any]:
        m = self.catalog.product(name)
        if m is None:
            return await self._get_base_product(name, sample_rows)
        info = await self.feldera.get_pipeline(m.pipeline, selector="all")
        pstatus = self._pipeline_status(info)
        status = m.status if (m.status in ("compiling", "redeploying") or info is None) else derive_status(info)
        schema = m.schema_
        schema_source = "validated"
        if info and pstatus.deployment_status == "Running":
            rel = schema_map.find_relation(info.get("program_info") or {}, m.view_name)
            if rel:
                schema = schema_map.relation_columns(rel)
                schema_source = "live"
        rows: list[dict[str, Any]] = []
        sample_note = None
        if sample_rows > 0 and pstatus.deployment_status == "Running":
            try:
                rows = await self.feldera.query(
                    m.pipeline, f"SELECT * FROM {m.view_name} LIMIT {int(sample_rows)}", timeout=15.0
                )
            except (FelderaError, Exception) as exc:  # noqa: BLE001
                sample_note = f"sample query failed: {exc}"
        elif sample_rows > 0:
            sample_note = f"pipeline is not running (status={status}); no sample rows"
        streams = [self.catalog.streams[s] for s in m.source_streams if s in self.catalog.streams]
        out = m.model_dump(mode="json", by_alias=True)
        out.update(
            {
                "status": status,
                "schema": [c.model_dump() for c in schema],
                "schema_source": schema_source,
                "engine_status": {
                    "program_status": pstatus.program_status,
                    "deployment_status": pstatus.deployment_status,
                    "error": pstatus.error or m.error,
                },
                "sample_rows": rows,
                "sample_note": sample_note,
                "lineage": {
                    "source_streams": m.source_streams,
                    "relations": sorted({s.relation for s in streams}),
                    "base_pipeline": self.catalog.base_pipeline_name(),
                },
                "triggers": [t.model_dump(mode="json") for t in m.triggers],
                "consume": self._consume_block(m, schema),
                "event_portal": m.event_portal.model_dump(mode="json"),
            }
        )
        return out

    async def _get_base_product(self, name: str, sample_rows: int) -> dict[str, Any]:
        bp = next((p for p in self.catalog.base_products() if p.name.lower() == name.lower()), None)
        if bp is None:
            raise ContinuumError(
                f"unknown data product {name!r}",
                {"known_products": sorted(m.name for m in self.catalog.products.values()),
                 "base_products": sorted(p.name for p in self.catalog.base_products())},
            )
        pipeline = self.catalog.base_pipeline_name() or ""
        info = await self.feldera.get_pipeline(pipeline, selector="all")
        pstatus = self._pipeline_status(info)
        schema: list[SchemaColumn] = []
        if info:
            rel = schema_map.find_relation(info.get("program_info") or {}, bp.name)
            if rel:
                schema = schema_map.relation_columns(rel)
        rows: list[dict[str, Any]] = []
        if sample_rows > 0 and pstatus.deployment_status == "Running":
            with contextlib.suppress(Exception):
                rows = await self.feldera.query(pipeline, f"SELECT * FROM {bp.name} LIMIT {int(sample_rows)}")
        stream = next((s for s in self.catalog.streams.values() if s.relation.lower() == bp.name.lower()), None)
        return {
            "name": bp.name,
            "description": bp.description,
            "owner": bp.owner,
            "tags": bp.tags,
            "managed": False,
            "pipeline": pipeline,
            "status": derive_status(info),
            "engine_status": pstatus.model_dump(),
            "schema": [c.model_dump() for c in schema],
            "schema_source": "live" if schema else "unavailable",
            "key_columns": bp.key_columns,
            "live_topic": bp.live_topic,
            "sample_rows": rows,
            "lineage": {"source_streams": [stream.name] if stream else [], "relations": [bp.name]},
            "triggers": [],
            "consume": (
                {
                    "topic": bp.live_topic,
                    "subscribe_pattern": subscribe_pattern(bp.live_topic),
                    "wire_format": self._wire_format(),
                }
                if bp.live_topic
                else None
            ),
            "event_portal": None,
            "note": "read-only product of the base pipeline; use it as a source via list_source_streams",
        }

    def search_catalog(self, keywords: str, limit: int = 10) -> list[dict[str, Any]]:
        return self.catalog.search(keywords, limit)

    # ------------------------------------------------------------------ preview
    async def preview_query(self, sql: str, limit: int = 20, timeout_secs: int = 15) -> dict[str, Any]:
        try:
            sql = ensure_single_select(sql)
        except SqlGuardError as exc:
            raise ContinuumError(str(exc)) from exc
        limit = max(1, min(int(limit), 500))
        relations = extract_relations(sql)
        if not relations:
            raise ContinuumError("could not find any relation in FROM/JOIN; reference a source stream relation")
        pipeline, ctes, unknown = self._resolve_relations(relations)
        if unknown:
            raise ContinuumError(
                f"unknown relations {sorted(unknown)}; use list_source_streams (field 'relation') or list_data_products",
                {"known_relations": sorted(self._known_relations())},
            )
        if pipeline is None:
            raise ContinuumError("referenced relations are not materialized by a single pipeline")
        wrapped = f"SELECT * FROM (\n{sql}\n) AS q LIMIT {limit + 1}"
        if ctes:
            wrapped = "WITH " + ",\n".join(c.strip() for c in ctes) + "\n" + wrapped
        try:
            rows = await self.feldera.query(pipeline, wrapped, timeout=float(timeout_secs))
        except FelderaError as exc:
            hint = ""
            if exc.status in (400, 503) or "not running" in exc.message.lower():
                hint = f" (is pipeline {pipeline!r} running? check get_status)"
            raise ContinuumError(f"preview failed on pipeline {pipeline!r}: {exc.message}{hint}") from exc
        truncated = len(rows) > limit
        rows = rows[:limit]
        columns = list(rows[0].keys()) if rows else []
        return {"pipeline_used": pipeline, "columns": columns, "rows": rows, "truncated": truncated,
                "row_count": len(rows)}

    def _known_relations(self) -> set[str]:
        known = set(self.catalog.relation_index())
        known |= self.catalog.base_relations()
        for m in self.catalog.products.values():
            known.add(m.view_name)
            known.update(t.view_name for t in m.triggers)
        return known

    def _resolve_relations(self, relations: set[str]) -> tuple[str | None, list[str], set[str]]:
        """Map relation names to (single pipeline, CTEs to prepend, unknown names)."""
        rel_index = self.catalog.relation_index()
        base_pipeline = self.catalog.base_pipeline_name()
        pipelines: set[str] = set()
        ctes: list[str] = []
        unknown: set[str] = set()
        product_views = {m.view_name: m for m in self.catalog.products.values()}
        trigger_views = {t.view_name: m for m in self.catalog.products.values() for t in m.triggers}
        for rel in relations:
            if rel in rel_index:
                s = rel_index[rel]
                pipelines.add(s.base_pipeline)
                if s.preview_cte:
                    ctes.append(s.preview_cte)
            elif rel in self.catalog.base_relations() and base_pipeline:
                pipelines.add(base_pipeline)
            elif rel in product_views:
                pipelines.add(product_views[rel].pipeline)
            elif rel in trigger_views:
                pipelines.add(trigger_views[rel].pipeline)
            else:
                unknown.add(rel)
        if unknown:
            return None, [], unknown
        if len(pipelines) != 1:
            raise ContinuumError(
                "the referenced relations live in different pipelines and cannot be joined in one preview",
                {"pipelines": sorted(pipelines)},
            )
        return pipelines.pop(), ctes, set()

    # ------------------------------------------------------------------ create
    async def create_data_product(
        self,
        name: str,
        description: str,
        sql: str,
        source_streams: list[str],
        key_columns: list[str],
        event_time_column: str | None = None,
        output_topic_template: str | None = None,
        owner: str | None = None,
        tags: list[str] | None = None,
        freshness_sla: str | None = None,
        triggers: list[TriggerSpec] | None = None,
        register_in_event_portal: bool = True,
        workers: int = 2,
    ) -> dict[str, Any]:
        try:
            sql = ensure_single_select(sql)
            slug = slugify(name)
            validate_identifiers(list(key_columns), "key_columns")
            if event_time_column:
                validate_identifiers([event_time_column], "event_time_column")
        except SqlGuardError as exc:
            raise ContinuumError(str(exc)) from exc
        if not key_columns:
            raise ContinuumError("key_columns must name at least one output column")
        if not source_streams:
            raise ContinuumError("source_streams must list at least one stream (see list_source_streams)")
        workers = max(1, min(int(workers), 16))
        pipeline = pipeline_name(slug)
        await self._ensure_unique(slug, pipeline, name)

        streams = self._resolve_streams(source_streams)
        self._check_relations(sql, streams)
        view_name = slug.replace("-", "_")
        if view_name in {s.relation.lower() for s in streams}:
            raise ContinuumError(f"product name collides with a source relation {view_name!r}; choose another name")

        live_topic = output_topic_template or f"fl/cp/{slug}/live/{{{key_columns[0]}}}"
        self._check_topic(live_topic, slug, allowed=None)
        trigger_specs = list(triggers or [])
        self._check_trigger_specs(trigger_specs, slug, event_time_column)

        # Pass 1: compile sources + product view only, to learn the output schema.
        base_spec = RenderSpec(
            name=name, slug=slug, description=description, sql=sql, streams=streams,
            live_topic=live_topic, event_time_column=event_time_column, include_sinks=False,
        )
        program_info = await self._validate(self.renderer.render_program(base_spec))
        relation = schema_map.find_relation(program_info, view_name)
        if relation is None:
            raise ContinuumError("the Continuum engine did not report a schema for the product view")
        columns = [f["name"] for f in relation.get("fields", [])]
        col_set = set(columns)
        missing_keys = [k for k in key_columns if k not in col_set]
        if missing_keys:
            raise ContinuumError(f"key_columns {missing_keys} are not output columns", {"columns": columns})
        if event_time_column and event_time_column not in col_set:
            raise ContinuumError(f"event_time_column {event_time_column!r} is not an output column", {"columns": columns})
        self._check_topic(live_topic, slug, allowed=col_set)

        trigger_renders = [
            self._trigger_render(t, columns, streams, slug, list(key_columns), event_time_column)
            for t in trigger_specs
        ]
        trigger_warnings = [w for tr in trigger_renders for w in tr.warnings]

        # Pass 2: the definitive program including sinks and triggers.
        full_spec = RenderSpec(
            name=name, slug=slug, description=description, sql=sql, streams=streams,
            live_topic=live_topic, columns=columns, event_time_column=event_time_column,
            triggers=trigger_renders, include_sinks=True,
        )
        rendered = self.renderer.render_program(full_spec)
        program_info = await self._validate(rendered)
        relation = schema_map.find_relation(program_info, view_name) or relation
        schema = schema_map.relation_columns(relation)

        # Broker first: solace_input only binds pre-existing queues.
        queues = await self._ensure_queues(slug, streams)

        await self._put_pipeline(pipeline, rendered.sql, description, slug, workers)

        manifest = ProductManifest(
            name=name, slug=slug, description=description, owner=owner or "unknown", tags=tags or [],
            freshness_sla=freshness_sla, source_streams=[s.name for s in streams], sql=sql,
            key_columns=list(key_columns), event_time_column=event_time_column, live_topic=live_topic,
            pipeline=pipeline, workers=workers, queues=queues, schema=schema, status="compiling",
            program_hash=_hash(rendered.sql),
            triggers=[self._trigger_record(tr, slug, relation, streams) for tr in trigger_renders],
        )
        manifest.event_portal.status = "pending" if (register_in_event_portal and self.ep) else "skipped"
        manifest.touch("created", f"pipeline {pipeline} uploaded; compiling")
        self.catalog.save_manifest(manifest, program_sql=rendered.sql)

        self.deployer.schedule(slug)
        if register_in_event_portal and self.ep:
            self._spawn(self._register_in_background(slug))

        return {
            "status": "compiling",
            "name": name,
            "slug": slug,
            "pipeline": pipeline,
            "queues": queues,
            "live_topic": live_topic,
            "subscribe_pattern": subscribe_pattern(live_topic),
            "schema": [c.model_dump() for c in schema],
            "estimated_ready_secs": self._estimate_ready_secs(),
            "triggers": [t.model_dump(mode="json") for t in manifest.triggers],
            "event_portal": manifest.event_portal.model_dump(mode="json"),
            "warnings": trigger_warnings,
            "next": NEXT_HINT,
        }

    async def _ensure_unique(self, slug: str, pipeline: str, name: str) -> None:
        if self.catalog.product(slug) or self.catalog.product(name):
            raise ContinuumError(f"a data product with slug {slug!r} already exists; delete it or choose another name")
        if any(p.name.lower() == slug for p in self.catalog.base_products()):
            raise ContinuumError(f"{slug!r} is a read-only base product name; choose another name")
        existing = await self.feldera.get_pipeline(pipeline)
        if existing is not None:
            raise ContinuumError(
                f"pipeline {pipeline!r} already exists in the Continuum engine (orphan?); delete it first "
                f"with delete_data_product"
            )

    def _resolve_streams(self, names: list[str]) -> list[StreamDef]:
        streams: list[StreamDef] = []
        seen: set[str] = set()
        for n in names:
            try:
                s = self.catalog.stream(n)
            except CatalogError as exc:
                raise ContinuumError(str(exc)) from exc
            if not s.ingestable:
                raise ContinuumError(f"stream {n!r} is not ingestable. {s.hint or ''}".strip())
            if s.name not in seen:
                streams.append(s)
                seen.add(s.name)
        return streams

    def _check_relations(self, sql: str, streams: list[StreamDef]) -> None:
        allowed = {s.relation.lower() for s in streams}
        referenced = extract_relations(sql)
        missing = sorted(referenced - allowed)
        if missing:
            hints = {r: self._stream_for_relation(r) for r in missing}
            raise ContinuumError(
                f"sql references relations {missing} that are not provided by the declared source_streams; "
                f"declared streams provide {sorted(allowed)}",
                {"missing": hints},
            )

    def _stream_for_relation(self, relation: str) -> str:
        s = self.catalog.relation_index().get(relation)
        if s is None:
            return "no stream provides this relation"
        if not s.ingestable:
            return f"stream '{s.name}' is not ingestable: {s.hint}"
        return f"add stream '{s.name}' to source_streams"

    def _check_topic(self, topic: str, slug: str, allowed: set[str] | None) -> None:
        check = validate_topic_template(topic, required_prefix=f"fl/cp/{slug}/", allowed_fields=allowed)
        if not check.ok:
            raise ContinuumError(f"invalid topic template {topic!r}", check.errors)

    def _check_trigger_specs(self, specs: list[TriggerSpec], slug: str, event_time_column: str | None) -> None:
        names: set[str] = set()
        for t in specs:
            try:
                validate_trigger_name(t.name)
                check_condition(t.condition)
                if t.recency_window:
                    parse_recency_window(t.recency_window)
            except SqlGuardError as exc:
                raise ContinuumError(f"trigger {t.name!r}: {exc}") from exc
            if t.name in names:
                raise ContinuumError(f"duplicate trigger name {t.name!r}")
            names.add(t.name)
            if t.recency_window and not event_time_column:
                raise ContinuumError(f"trigger {t.name!r} uses recency_window but the product has no event_time_column")
            self._check_topic(t.output_topic_template, slug, allowed=None)

    def _trigger_render(
        self,
        spec: TriggerSpec,
        columns: list[str],
        streams: list[StreamDef],
        slug: str,
        key_columns: list[str],
        event_time_column: str | None,
    ) -> TriggerRender:
        col_set = set(columns)
        ph = placeholders(spec.output_topic_template)
        # Columns that must never be stripped: the product key and any topic placeholders.
        keep = {k for k in key_columns if k in col_set} | {p for p in ph if p in col_set}
        # Volatile = product-derived churny columns + any source-stream volatile columns.
        volatile = {c for c in columns if is_volatile_column(c, event_time_column)}
        volatile |= {c for s in streams for c in s.volatile_columns if c in col_set}
        volatile -= keep
        warnings: list[str] = []
        if spec.payload_columns:
            unknown = [c for c in spec.payload_columns if c not in col_set]
            if unknown:
                raise ContinuumError(f"trigger {spec.name!r}: payload_columns {unknown} are not output columns", {"columns": columns})
            requested = list(dict.fromkeys(spec.payload_columns))
            if spec.allow_volatile_payload:
                payload = requested
            else:
                removed = [c for c in requested if c in volatile]
                payload = [c for c in requested if c not in volatile]
                if removed:
                    warnings.append(
                        f"trigger {spec.name!r}: removed volatile column(s) {removed} from the payload so it "
                        f"fires once per episode — rolling counts/sums/amounts, windowed metrics, balances and "
                        f"most-recent timestamps re-fire the trigger on every change. The consuming agent can "
                        f"read live values with preview_query/get_data_product. Pass allow_volatile_payload=true "
                        f"to keep them (and accept many events per episode)."
                    )
                if not payload:
                    payload = [c for c in columns if c in keep] or list(requested)
                    warnings.append(
                        f"trigger {spec.name!r}: every requested payload column was volatile; kept the "
                        f"identity column(s) {payload} only."
                    )
        else:
            payload = [c for c in columns if c not in volatile] or list(columns)
        self._check_topic(spec.output_topic_template, slug, allowed=col_set)
        not_in_payload = [p for p in ph if p not in payload]
        if not_in_payload:
            raise ContinuumError(
                f"trigger {spec.name!r}: topic placeholders {not_in_payload} must be among payload_columns {payload}"
            )
        return TriggerRender(spec=spec, payload_columns=payload, warnings=warnings)

    def _trigger_record(
        self, tr: TriggerRender, slug: str, relation: dict[str, Any], streams: list[StreamDef]
    ) -> TriggerRecord:
        spec = tr.spec
        ph = set(placeholders(spec.output_topic_template))
        row_schema = schema_map.row_json_schema(
            relation, title=f"{pascal(slug)}{pascal(spec.name)}Trigger",
            description=spec.description, only=tr.payload_columns,
        )
        for p in ph:
            if p in row_schema["properties"]:
                row_schema["properties"][p] = {"type": "string", "description": "topic placeholder (stringified, 'unknown' when NULL)"}
        pattern = subscribe_pattern(spec.output_topic_template)
        return TriggerRecord(
            name=spec.name,
            description=spec.description,
            condition=spec.condition,
            topic_template=spec.output_topic_template,
            subscribe_pattern=pattern,
            sample_topic=sample_topic(spec.output_topic_template),
            payload_columns=tr.payload_columns,
            distinct=spec.distinct,
            recency_window=spec.recency_window,
            payload_json_schema=schema_map.envelope_json_schema(row_schema, self.settings.trigger_envelope),
            view_name=trigger_view_name(slug, spec.name),
            sam_event_rule_snippet=self.sam_event_rule_snippet(f"{slug}-{spec.name}", pattern, [p for p in ph] or tr.payload_columns[:1]),
        )

    async def _validate(self, rendered: RenderedProgram) -> dict[str, Any]:
        try:
            result = await self.feldera.validate_program(rendered.sql)
        except FelderaError as exc:
            raise ContinuumError(f"the Continuum engine could not validate the program: {exc.message}") from exc
        if "Success" in result:
            return (result["Success"] or {}).get("program_info") or {}
        if "SqlError" in result:
            msgs = [rendered.remap_message(m) for m in sql_error_messages(result)]
            errors = [m for m in msgs if not m["warning"]] or msgs
            raise ContinuumError(
                "SQL did not compile: " + "; ".join(f"{m['where']}: {m['message']}" for m in errors[:3]),
                {"messages": msgs},
            )
        err = (result.get("SystemError") or {}).get("error") or str(result)[:400]
        raise ContinuumError(f"the Continuum engine failed to validate the program: {err}")

    async def _ensure_queues(self, slug: str, streams: list[StreamDef]) -> list[str]:
        queues: list[str] = []
        for s in streams:
            q = queue_name(slug, s.name)
            try:
                await self.semp.ensure_queue(q, s.topic_subscriptions)
            except SempError as exc:
                raise ContinuumError(f"could not create broker queue {q!r} on Solace Platform: {exc.message}") from exc
            queues.append(q)
        return queues

    async def _put_pipeline(self, pipeline: str, program: str, description: str, slug: str, workers: int) -> None:
        try:
            await self.feldera.put_pipeline(
                pipeline, program_code=program, description=description, tags=pipeline_tags(slug), workers=workers,
                compilation_profile=self.settings.compilation_profile,
            )
        except FelderaError as exc:
            raise ContinuumError(f"the Continuum engine rejected pipeline {pipeline!r}: {exc.message}") from exc

    def _estimate_ready_secs(self) -> int:
        queued = max(0, self.deployer.queue_depth - 1)
        return COLD_COMPILE_SECS + queued * COLD_COMPILE_SECS

    # ------------------------------------------------------------------ triggers
    async def create_trigger(
        self,
        data_product: str,
        name: str,
        description: str,
        condition: str,
        output_topic_template: str,
        payload_columns: list[str] | None = None,
        distinct: bool = True,
        recency_window: str | None = None,
        allow_volatile_payload: bool = False,
    ) -> dict[str, Any]:
        m = self._managed_product(data_product)
        if m.trigger(name):
            raise ContinuumError(f"trigger {name!r} already exists on {m.name!r}; delete it first")
        spec = TriggerSpec(
            name=name, description=description, condition=condition, output_topic_template=output_topic_template,
            payload_columns=payload_columns, distinct=distinct, recency_window=recency_window,
            allow_volatile_payload=allow_volatile_payload,
        )
        self._check_trigger_specs([spec], m.slug, m.event_time_column)
        streams = self._resolve_streams(m.source_streams)
        existing = [self._render_from_record(t, m) for t in m.triggers]
        new_render = self._trigger_render(spec, m.column_names, streams, m.slug, m.key_columns, m.event_time_column)
        rendered = self._render_full(m, streams, existing + [new_render])
        program_info = await self._validate(rendered)
        relation = schema_map.find_relation(program_info, m.view_name) or {"fields": []}
        record = self._trigger_record(new_render, m.slug, relation, streams)
        m.triggers.append(record)
        await self._redeploy(m, rendered.sql, f"trigger {name!r} added")
        return {
            "status": "redeploying",
            "trigger": record.model_dump(mode="json"),
            "warnings": new_render.warnings,
            "warning": (
                "the product pipeline is being restarted to add the trigger: its state is rebuilt from live "
                "traffic, so rows only re-appear as new events arrive (queue backlog is replayed)"
            ),
            "next": NEXT_HINT,
        }

    async def delete_trigger(self, data_product: str, name: str) -> dict[str, Any]:
        m = self._managed_product(data_product)
        record = m.trigger(name)
        if record is None:
            raise ContinuumError(f"no trigger {name!r} on {m.name!r}", {"triggers": [t.name for t in m.triggers]})
        streams = self._resolve_streams(m.source_streams)
        remaining = [self._render_from_record(t, m) for t in m.triggers if t.name != name]
        rendered = self._render_full(m, streams, remaining)
        await self._validate(rendered)
        m.triggers = [t for t in m.triggers if t.name != name]
        await self._redeploy(m, rendered.sql, f"trigger {name!r} removed")
        return {
            "status": "redeploying",
            "trigger": record.model_dump(mode="json"),
            "warning": "the product pipeline is being restarted; state is rebuilt from live traffic",
            "next": NEXT_HINT,
        }

    def _managed_product(self, name: str) -> ProductManifest:
        m = self.catalog.product(name)
        if m is None:
            raise ContinuumError(f"unknown managed data product {name!r}", {"known": sorted(self.catalog.products)})
        if self.deployer.is_busy(m.slug) or m.status in ("compiling", "redeploying"):
            raise ContinuumError(f"data product {m.name!r} is {m.status}; wait until it is running")
        return m

    def _render_from_record(self, t: TriggerRecord, m: ProductManifest) -> TriggerRender:
        spec = TriggerSpec(
            name=t.name, description=t.description, condition=t.condition, output_topic_template=t.topic_template,
            payload_columns=t.payload_columns, distinct=t.distinct, recency_window=t.recency_window,
        )
        return TriggerRender(spec=spec, payload_columns=t.payload_columns)

    def _render_full(self, m: ProductManifest, streams: list[StreamDef], triggers: list[TriggerRender]) -> RenderedProgram:
        return self.renderer.render_program(
            RenderSpec(
                name=m.name, slug=m.slug, description=m.description, sql=m.sql, streams=streams,
                live_topic=m.live_topic, columns=m.column_names, event_time_column=m.event_time_column,
                triggers=triggers, include_sinks=True,
            )
        )

    async def _redeploy(self, m: ProductManifest, program: str, reason: str) -> None:
        prev = self.catalog.read_program(m.slug)
        m.status = "redeploying"
        m.error = None
        m.program_hash = _hash(program)
        m.touch("redeploy", reason)
        self.catalog.save_manifest(m, program_sql=program, prev_program_sql=prev)
        self.deployer.schedule_redeploy(m.slug, program, prev)
        if self.ep and m.event_portal.status not in ("skipped",):
            self._spawn(self._register_in_background(m.slug))

    # ------------------------------------------------------------------ delete
    async def delete_data_product(
        self, name: str, drop_queues: bool = True, unregister_event_portal: bool = False
    ) -> dict[str, Any]:
        m = self.catalog.product(name)
        steps: list[str] = []
        pipeline = m.pipeline if m else (name if name.startswith("cp-") else pipeline_name(slugify(name)))
        if m is None and await self.feldera.get_pipeline(pipeline) is None:
            raise ContinuumError(f"unknown data product {name!r}", {"known": sorted(self.catalog.products)})
        if m and self.deployer.is_busy(m.slug):
            await self.deployer.cancel(m.slug)
            steps.append("cancelled in-flight deployment")
        try:
            info = await self.feldera.get_pipeline(pipeline)
            if info is not None:
                if info.get("deployment_status") != "Stopped":
                    with contextlib.suppress(FelderaError):
                        await self.feldera.stop_force(pipeline)
                    await self.feldera.wait_for(
                        pipeline, lambda i: i.get("deployment_status") == "Stopped", timeout=20.0, interval=1.0
                    )
                    steps.append(f"stopped pipeline {pipeline}")
                info = await self.feldera.get_pipeline(pipeline) or {}
                if info.get("storage_status") == "InUse":
                    with contextlib.suppress(FelderaError):
                        await self.feldera.clear(pipeline)
                    await self.feldera.wait_for(
                        pipeline, lambda i: i.get("storage_status") == "Cleared", timeout=15.0, interval=1.0
                    )
                    steps.append(f"cleared storage of {pipeline}")
                await self.feldera.delete(pipeline)
                steps.append(f"deleted pipeline {pipeline}")
            else:
                steps.append(f"pipeline {pipeline} not present in the Continuum engine")
        except TimeoutError as exc:
            raise ContinuumError(f"pipeline {pipeline} is still stopping; retry delete_data_product shortly ({exc})", steps) from exc
        except FelderaError as exc:
            raise ContinuumError(f"could not delete pipeline {pipeline}: {exc.message}", steps) from exc

        if drop_queues and m:
            for q in m.queues:
                try:
                    removed = await self.semp.delete_queue(q)
                    steps.append(f"{'deleted' if removed else 'no such'} queue {q}")
                except SempError as exc:
                    steps.append(f"failed to delete queue {q}: {exc.message}")
        if unregister_event_portal and m and self.ep and m.event_portal.domain_id:
            try:
                steps += await self.ep.unregister_application(m.event_portal.domain_id, f"continuum-{m.slug}")
                for ev in [f"{m.slug}.live", *[f"{m.slug}.{t.name}" for t in m.triggers]]:
                    steps += await self.ep.unregister_event(m.event_portal.domain_id, ev)
            except EventPortalError as exc:
                steps.append(f"Event Portal cleanup failed: {exc.message}")
        if m:
            self.catalog.delete_product(m.slug)
            steps.append(f"removed catalog entry {m.slug}")
        return {"deleted": True, "name": m.name if m else name, "steps": steps}

    # ------------------------------------------------------------------ status
    async def get_status(self, name: str | None = None) -> dict[str, Any]:
        targets: list[ProductManifest]
        if name:
            m = self.catalog.product(name)
            if m is None:
                raise ContinuumError(f"unknown managed data product {name!r}", {"known": sorted(self.catalog.products)})
            targets = [m]
        else:
            targets = list(self.catalog.products.values())
        out = []
        for m in targets:
            out.append(await self._status_for(m))
        return {"products": out, "deployer_queue_depth": self.deployer.queue_depth}

    async def _status_for(self, m: ProductManifest) -> dict[str, Any]:
        info = await self.feldera.get_pipeline(m.pipeline)
        pstatus = self._pipeline_status(info)
        inputs: list[dict[str, Any]] = []
        outputs: list[dict[str, Any]] = []
        if pstatus.deployment_status == "Running":
            try:
                stats = await self.feldera.stats(m.pipeline)
                inputs = [_endpoint_summary(e, "input") for e in stats.get("inputs", [])]
                outputs = [_endpoint_summary(e, "output") for e in stats.get("outputs", [])]
            except (FelderaError, Exception) as exc:  # noqa: BLE001
                inputs = [{"error": f"stats unavailable: {exc}"}]
        backlog = []
        for q in m.queues:
            try:
                backlog.append(await self.semp.queue_backlog(q))
            except SempError as exc:
                backlog.append({"queue": q, "error": exc.message})
        return {
            "name": m.name,
            "pipeline": m.pipeline,
            "status": m.status if m.status in ("compiling", "redeploying") or info is None else derive_status(info),
            "program_status": pstatus.program_status,
            "deployment_status": pstatus.deployment_status,
            "error": pstatus.error or m.error,
            "inputs": inputs,
            "outputs": outputs,
            "queue_backlog": backlog,
            "queue_backlog_note": "backlog_msgs uses the broker's spooled count when available, else "
                                  "lastSpooledMsgId - highestAckedMsgId (an approximation)",
        }

    @staticmethod
    def _pipeline_status(info: dict[str, Any] | None) -> PipelineStatus:
        if info is None:
            return PipelineStatus(exists=False)
        return PipelineStatus(
            program_status=info.get("program_status"),
            deployment_status=info.get("deployment_status"),
            storage_status=info.get("storage_status"),
            error=program_error_text(info),
        )

    # ------------------------------------------------------------------ event portal
    async def sync_event_portal(self, data_product: str) -> dict[str, Any]:
        m = self.catalog.product(data_product)
        if m is None:
            raise ContinuumError(f"unknown managed data product {data_product!r}", {"known": sorted(self.catalog.products)})
        if not self.ep:
            m.event_portal.status = "skipped"
            m.event_portal.error = "SOLACE_API_TOKEN is not configured"
            self.catalog.save_manifest(m)
            return m.event_portal.model_dump(mode="json")
        state = await self.register_event_portal(m)
        return state.model_dump(mode="json")

    async def _register_in_background(self, slug: str) -> None:
        m = self.catalog.products.get(slug)
        if m is None:
            return
        try:
            await self.register_event_portal(m)
        except Exception as exc:  # noqa: BLE001 - never let EP break a product
            log.warning("Event Portal registration for %s failed: %s", slug, exc)

    async def register_event_portal(self, m: ProductManifest) -> EventPortalState:
        assert self.ep is not None
        state = m.event_portal
        try:
            domain_id = await self.ep.ensure_domain(
                self.settings.ep_domain,
                "Continuum data products: SQL-defined live views on Solace Platform, published as events.",
            )
            slug_p = pascal(m.slug)
            live_ph = set(placeholders(m.live_topic))
            row_schema = schema_map.json_schema_from_columns(
                m.schema_, title=f"{slug_p}Row", description=m.description, string_columns=live_ph
            )
            schema_id = await self.ep.ensure_schema(domain_id, f"{slug_p}Row")
            sv_id = await self.ep.ensure_schema_version(
                schema_id, "1.0.0", schema_map.envelope_json_schema(row_schema, self.settings.trigger_envelope)
            )
            live_ev = await self.ep.ensure_event(domain_id, f"{m.slug}.live")
            live_evv = await self.ep.ensure_event_version(
                live_ev, "1.0.0", schema_version_id=sv_id, topic_template=m.live_topic,
                description=f"Live changelog of data product {m.name}",
            )
            state.event_ids["live"] = live_ev
            state.event_version_ids["live"] = live_evv
            produced = [live_evv]
            for t in m.triggers:
                t_schema_id = await self.ep.ensure_schema(domain_id, f"{slug_p}{pascal(t.name)}Trigger")
                t_sv = await self.ep.ensure_schema_version(t_schema_id, "1.0.0", t.payload_json_schema)
                t_ev = await self.ep.ensure_event(domain_id, f"{m.slug}.{t.name}")
                t_evv = await self.ep.ensure_event_version(
                    t_ev, "1.0.0", schema_version_id=t_sv, topic_template=t.topic_template,
                    description=t.description or f"Trigger {t.name} of {m.name}",
                )
                t.event_portal = TriggerEventPortal(
                    event_id=t_ev, event_version_id=t_evv, schema_id=t_schema_id, schema_version_id=t_sv
                )
                state.event_ids[t.name] = t_ev
                state.event_version_ids[t.name] = t_evv
                produced.append(t_evv)
            app_id = await self.ep.ensure_application(domain_id, f"continuum-{m.slug}")
            app_ver_id, version = await self.ep.ensure_application_version(
                app_id, produced_event_version_ids=produced,
                description=f"Continuum data product {m.name} ({m.pipeline})",
            )
            state.domain_id = domain_id
            state.schema_id = schema_id
            state.schema_version_id = sv_id
            state.application_id = app_id
            state.application_version_id = app_ver_id
            state.application_version = version
            state.status = "registered"
            state.error = None
        except EventPortalError as exc:
            state.status = "failed"
            state.error = exc.message
        except Exception as exc:  # noqa: BLE001
            state.status = "failed"
            state.error = str(exc)
        state.synced_at = utcnow_iso()
        m.touch("event-portal-sync", state.status)
        self.catalog.save_manifest(m)
        return state

    # ------------------------------------------------------------------ consume
    def _wire_format(self) -> str:
        """Identifier of the sink's wire envelope: insert_delete_envelope ({"insert": row} / {"delete": row}) or bare_row."""
        return "insert_delete_envelope" if self.settings.trigger_envelope == "insert" else "bare_row"

    def _consume_block(self, m: ProductManifest, schema: list[SchemaColumn]) -> dict[str, Any]:
        live_ph = set(placeholders(m.live_topic))
        row_schema = schema_map.json_schema_from_columns(
            schema, title=f"{pascal(m.slug)}Row", description=m.description, string_columns=live_ph
        )
        pattern = subscribe_pattern(m.live_topic)
        return {
            "topic": m.live_topic,
            "subscribe_pattern": pattern,
            "wire_format": self._wire_format(),
            "payload_json_schema": schema_map.envelope_json_schema(row_schema, self.settings.trigger_envelope),
            "sam_event_rule_snippet": self.sam_event_rule_snippet(f"{m.slug}-live", pattern, m.key_columns),
            "note": (
                "the live topic carries the full changelog: one message per row change, {\"insert\": row} or "
                "{\"delete\": row}; trigger topics carry inserts only, so each trigger fires once per episode"
                if self.settings.trigger_envelope == "insert"
                else "the live topic carries the full changelog (one bare row per message); triggers fire once per episode"
            ),
        }

    def sam_event_rule_snippet(self, rule_name: str, pattern: str, keys: list[str]) -> str:
        insert = self.settings.trigger_envelope == "insert"
        rule = {
            "name": rule_name,
            "subscriptions": [{"topic": pattern, "qos": 0}],
            "messageFormat": "json",
            "inputExpression": "input.payload:insert" if insert else "input.payload",
            "forwardContext": {k: (f"input.payload:insert.{k}" if insert else f"input.payload:{k}") for k in keys},
            "targetWorkflowName": "TODO-set-target-workflow",
        }
        return yaml.safe_dump([rule], sort_keys=False, width=200)


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _endpoint_summary(e: dict[str, Any], kind: str) -> dict[str, Any]:
    metrics = e.get("metrics", {}) or {}
    out = {
        "name": e.get("endpoint_name"),
        "stream": (e.get("config") or {}).get("stream"),
        "health": (e.get("health") or {}).get("status"),
        "fatal_error": e.get("fatal_error"),
    }
    if kind == "input":
        out.update(
            total_records=metrics.get("total_records"),
            buffered_records=metrics.get("buffered_records"),
            parse_errors=metrics.get("num_parse_errors"),
            transport_errors=metrics.get("num_transport_errors"),
        )
    else:
        out.update(
            transmitted_records=metrics.get("transmitted_records"),
            queued_records=metrics.get("queued_records"),
            encode_errors=metrics.get("num_encode_errors"),
            transport_errors=metrics.get("num_transport_errors"),
        )
    return out

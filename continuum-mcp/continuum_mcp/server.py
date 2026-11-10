"""Continuum MCP server (streamable HTTP).

Exposes Solace Continuum — the streaming-data layer of Solace Platform — to AI
agents: discover source streams, preview SQL against live views, create
strongly consistent data products, attach event triggers, and get consumption
details (topics, schemas, SAM event-rule snippets).

Run:  uv run python -m continuum_mcp.server
Env:  see .env.example (CONTINUUM_MCP_TOKEN enables bearer auth; unset = open, with a warning)
"""

from __future__ import annotations

import contextlib
import logging
from collections.abc import AsyncIterator
from typing import Any, Literal

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from starlette.types import Receive, Scope, Send

from continuum_mcp.catalog import Catalog, CatalogError
from continuum_mcp.config import Settings, load_settings
from continuum_mcp.ep_client import EventPortalClient
from continuum_mcp.feldera_client import FelderaClient, FelderaError
from continuum_mcp.models import TriggerSpec
from continuum_mcp.semp_client import SempClient, SempError
from continuum_mcp.service import ContinuumError, ContinuumService
from continuum_mcp.sql_guard import SqlGuardError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("continuum-mcp")

INSTRUCTIONS = (
    "Solace Continuum is the streaming-data layer of Solace Platform. A data product is a "
    "SQL-defined live view kept strongly consistent per event by the Continuum engine, built in "
    "four steps: ingest a stream to a table, refine with SQL, join across streams, and trigger an "
    "event when a condition becomes true. Typical flow: list_source_streams -> preview_query -> "
    "create_data_product -> poll get_data_product until running -> create_trigger."
)

_service: ContinuumService | None = None


def service() -> ContinuumService:
    if _service is None:
        raise RuntimeError("ContinuumService not initialised; run via build_app()/main()")
    return _service


async def _run(coro: Any) -> Any:
    """Convert known failures into a structured error the agent can act on."""
    try:
        return await coro
    except ContinuumError as exc:
        return exc.to_dict()
    except (SqlGuardError, CatalogError) as exc:
        return {"error": str(exc)}
    except FelderaError as exc:
        return {"error": f"Continuum engine error: {exc.message}", "details": {"status": exc.status, "code": exc.error_code}}
    except SempError as exc:
        return {"error": f"Solace Platform broker error: {exc.message}", "details": {"status": exc.status, "code": exc.code}}


async def _value(value: Any) -> Any:
    return value


# ---------------------------------------------------------------------- tools
async def list_source_streams(kind: Literal["raw", "derived", "all"] = "all") -> list[dict[str, Any]]:
    """List the streams a data product can be built from.

    `raw` streams are business events arriving on the Solace Platform event mesh
    (TTP airport rates, METAR weather, TMR traffic-management restrictions);
    `derived` streams are live views the base Continuum pipeline already publishes
    back onto the mesh (the capacity assessment of demand vs rate, and the enriched
    arrival-context breach brief). Reference relations that live only inside the
    base pipeline (flight, arrival_demand, downstream risk) are listed too but are
    not ingestable. Each entry gives
    the `relation` name to use in SQL, its columns, key and event-time columns,
    changelog kind, and whether it is ingestable. Non-ingestable entries carry a
    hint pointing to the stream to use instead.
    """
    return await _run(_value(service().list_source_streams(kind)))


async def list_data_products(
    status: str | None = None, tag: str | None = None, include_base: bool = True
) -> list[dict[str, Any]]:
    """List Continuum data products: those created through this server (managed)
    plus, when include_base is true, the read-only live views of the base
    pipeline. Filter by status (compiling|redeploying|running|failed|stopped) or
    tag. Also surfaces pipelines tagged as data products that have no catalog
    entry (orphans) so they can be cleaned up.
    """
    return await _run(service().list_data_products(status=status, tag=tag, include_base=include_base))


async def get_data_product(name: str, sample_rows: int = 5) -> dict[str, Any]:
    """Everything about one data product: manifest, schema (live when running,
    validated otherwise), engine status, a few sample rows, lineage (source
    streams and relations), triggers, and a `consume` block with the live topic,
    subscription pattern, wire format, payload JSON Schema and a ready-to-paste
    SAM event-rule snippet. Poll this after create_data_product until
    status=running.
    """
    return await _run(service().get_data_product(name, sample_rows=sample_rows))


async def search_catalog(keywords: str, limit: int = 10) -> list[dict[str, Any]]:
    """Keyword search over data products, source streams and triggers (name,
    description, tags, columns, topics). Deterministic token-overlap ranking;
    each hit explains why it matched.
    """
    return await _run(_value(service().search_catalog(keywords, limit)))


async def preview_query(sql: str, limit: int = 20, timeout_secs: int = 15) -> dict[str, Any]:
    """Run a read-only SELECT against the live, strongly consistent views of a
    Continuum pipeline and return up to `limit` rows. Use the `relation` names
    from list_source_streams; all referenced relations must be materialized by
    the same pipeline. Ideal for trying the SQL of a data product before
    creating it — the same SELECT will produce the same relation shape.
    """
    return await _run(service().preview_query(sql, limit=limit, timeout_secs=timeout_secs))


async def create_data_product(
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
    """Create a Continuum data product: a strongly consistent live view defined
    by a single SELECT over one or more source streams, published as a full
    changelog on `output_topic_template` (default fl/cp/<slug>/live/{<first key>})
    and queryable with preview_query. Optional `triggers` add event outputs that
    fire once per episode when a condition becomes true.

    The SQL is validated and its schema derived before anything is provisioned;
    then queues are created on Solace Platform and a dedicated pipeline is
    deployed in the Continuum engine. Returns immediately with status=compiling
    (first compile is about two minutes); poll get_data_product until
    status=running. Topic templates must contain at least one {column}
    placeholder and start with fl/cp/<slug>/.
    """
    return await _run(
        service().create_data_product(
            name=name, description=description, sql=sql, source_streams=source_streams,
            key_columns=key_columns, event_time_column=event_time_column,
            output_topic_template=output_topic_template, owner=owner, tags=tags,
            freshness_sla=freshness_sla, triggers=triggers,
            register_in_event_portal=register_in_event_portal, workers=workers,
        )
    )


async def create_trigger(
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
    """Add an event trigger to a running data product: when `condition` (a SQL
    boolean over the product's columns) becomes true for a row, an event with
    `payload_columns` is published to `output_topic_template` (must start with
    fl/cp/<slug>/ and contain at least one {column} placeholder drawn from the
    payload). `recency_window` (e.g. '5 minutes') limits firing to recent rows
    by the product's event-time column.

    Fire-once guard: the payload should carry identity/descriptor columns only.
    Volatile columns — rolling counts/sums/amounts, windowed metrics (`*_60s`),
    balances, most-recent timestamps — are stripped from `payload_columns`
    automatically (the key and topic placeholders are always kept), because a
    volatile payload makes the trigger re-fire on every change instead of once per
    episode. Stripped columns are listed in the response `warnings`; the consuming
    agent can read live values with preview_query/get_data_product. Set
    `allow_volatile_payload=true` only if you truly want many events per episode.

    The pipeline is restarted to apply the change (status=redeploying); the
    previous program is restored automatically if the new one fails to compile.
    """
    return await _run(
        service().create_trigger(
            data_product=data_product, name=name, description=description, condition=condition,
            output_topic_template=output_topic_template, payload_columns=payload_columns,
            distinct=distinct, recency_window=recency_window,
            allow_volatile_payload=allow_volatile_payload,
        )
    )


async def delete_trigger(data_product: str, name: str) -> dict[str, Any]:
    """Remove a trigger from a data product. The pipeline is restarted without
    the trigger's event output (status=redeploying).
    """
    return await _run(service().delete_trigger(data_product, name))


async def delete_data_product(
    name: str, drop_queues: bool = True, unregister_event_portal: bool = False
) -> dict[str, Any]:
    """Delete a managed data product: stop and remove its pipeline from the
    Continuum engine, optionally drop its Solace Platform queues and its Event
    Portal application/events, and remove the catalog entry. Also accepts an
    orphan pipeline name (cp-<slug>).
    """
    return await _run(
        service().delete_data_product(name, drop_queues=drop_queues, unregister_event_portal=unregister_event_portal)
    )


async def get_status(name: str | None = None) -> dict[str, Any]:
    """Operational status of one or all managed data products: engine program and
    deployment status, last error, per-connector input/output counters and
    health, and the backlog of each ingest queue on Solace Platform.
    """
    return await _run(service().get_status(name))


async def sync_event_portal(data_product: str) -> dict[str, Any]:
    """Register (or re-sync) a data product in Solace Event Portal: application
    domain, row/trigger schemas, events with topic addresses, and an application
    version declaring the produced events. Idempotent; requires SOLACE_API_TOKEN.
    """
    return await _run(service().sync_event_portal(data_product))


TOOL_FUNCTIONS = (
    list_source_streams,
    list_data_products,
    get_data_product,
    search_catalog,
    preview_query,
    create_data_product,
    create_trigger,
    delete_trigger,
    delete_data_product,
    get_status,
    sync_event_portal,
)


def create_mcp(settings: Settings | None = None) -> FastMCP:
    """A fresh FastMCP with all 11 tools (each streamable-HTTP session manager runs once, so build_app needs a new one).

    The SDK's DNS-rebinding protection (Host allow-list) is off unless MCP_ALLOWED_HOSTS is set:
    clients inside the docker network address this server by container name, and the bearer
    token is the access control.
    """
    allowed = settings.allowed_hosts if settings else []
    security = TransportSecuritySettings(
        enable_dns_rebinding_protection=bool(allowed),
        allowed_hosts=allowed,
        allowed_origins=[],
    )
    instance = FastMCP(
        "continuum",
        instructions=INSTRUCTIONS,
        stateless_http=True,
        json_response=True,
        transport_security=security,
    )
    for fn in TOOL_FUNCTIONS:
        instance.add_tool(fn)
    return instance


mcp = create_mcp()


# ---------------------------------------------------------------------- app
def build_service(settings: Settings) -> ContinuumService:
    catalog = Catalog(settings.catalog_dir)
    feldera = FelderaClient(settings.feldera_url)
    semp = SempClient(
        settings.semp_url, settings.semp_user, settings.semp_pass, settings.semp_vpn,
        max_ttl_secs=settings.queue_max_ttl_secs, max_spool_mb=settings.queue_max_spool_mb,
    )
    ep = EventPortalClient(settings.ep_base_url, settings.solace_api_token) if settings.ep_enabled else None
    return ContinuumService(settings, catalog, feldera, semp, ep)


def build_app(settings: Settings | None = None) -> Any:
    """ASGI app: bearer auth + '/mcp' -> '/mcp/' rewrite around FastMCP's streamable HTTP app."""
    global _service
    settings = settings or load_settings()
    _service = build_service(settings)
    svc = _service
    token = settings.mcp_token
    if not token:
        log.warning("CONTINUUM_MCP_TOKEN unset: MCP endpoint is unauthenticated")

    instance = create_mcp(settings)
    inner = instance.streamable_http_app()

    @contextlib.asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        async with instance.session_manager.run():
            await svc.startup()
            log.info("continuum-mcp ready: %d tools, catalog %s", len(TOOL_FUNCTIONS), settings.catalog_dir)
            try:
                yield
            finally:
                await svc.shutdown()

    async def healthz(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "products": len(svc.catalog.products), "streams": len(svc.catalog.streams)})

    outer = Starlette(lifespan=lifespan, routes=[Route("/healthz", healthz), Mount("/", app=inner)])

    async def app(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            # FastMCP registers the streamable HTTP endpoint as an exact Route("/mcp"), so a
            # trailing slash would 307-redirect. Normalise so clients may use either form.
            if scope.get("path") == "/mcp/":
                scope = {**scope, "path": "/mcp", "raw_path": b"/mcp"}
            if token and scope.get("path") != "/healthz":
                headers = dict(scope.get("headers", []))
                auth = headers.get(b"authorization", b"").decode()
                if auth != f"Bearer {token}":
                    await send({"type": "http.response.start", "status": 401,
                                "headers": [[b"content-type", b"text/plain"]]})
                    await send({"type": "http.response.body", "body": b"Unauthorized"})
                    return
        await outer(scope, receive, send)

    app.starlette = outer  # type: ignore[attr-defined]  # lets tests drive the lifespan without uvicorn
    app.service = svc  # type: ignore[attr-defined]
    return app


def main() -> None:
    settings = load_settings()
    uvicorn.run(build_app(settings), host=settings.mcp_host, port=settings.mcp_port, log_level="info")


if __name__ == "__main__":
    main()

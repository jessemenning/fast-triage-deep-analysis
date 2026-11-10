"""FAA arrival-capacity API routes for the Feldera/Continuum data plane.

Point queries hit Feldera's ad-hoc SQL endpoint (/v0/pipelines/{p}/query);
live state is driven by the Continuum/Feldera Solace sinks + the SAM Agent Mesh
output topics via the Solace receiver, surfaced here as SSE streams and a simple
latest-state polling API.

Queryable materialized views (see arrival-capacity pipeline):
  capacity_assessment(airport, bin_start, bin_end, demand, bin_capacity,
                      overage, eta_quality, exceeded)
  arrival_context  — enriched breach (airport_name, severity, wx_*, restriction)
  airport_rate / mcp_airport_rate(airport, aar, adr, runway_config, valid_*)
"""

import asyncio
import json
import logging
import re
from pathlib import Path
from typing import Optional

import httpx
from fastapi import APIRouter, Query, Request
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from src.config import get_settings
from src.services.feldera_client import (
    get_feldera_client,
    get_watch_client,
    sql_quote,
)
from src.services.solace_receiver import (
    get_receiver,
    is_agent_enabled,
    set_agent_enabled,
    STREAM_NAMES,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/faa", tags=["FAA"])


# ── Coercion helpers (Feldera returns numbers, but DECIMAL may arrive as str) ──
def _f(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    # Tolerate METAR-style visibility tokens ("10+", "10SM", "P6SM"): take the
    # leading numeric portion, else None — never raise (used across wx_* fields).
    m = re.match(r"-?\d+(?:\.\d+)?", str(v).strip())
    return float(m.group()) if m else None


def _i(v) -> Optional[int]:
    return int(v) if v is not None else None


def _s(v) -> Optional[str]:
    return str(v) if v is not None else None


def _b(v) -> Optional[bool]:
    return bool(v) if v is not None else None


class CapacityRow(BaseModel):
    airport: str
    bin_start: Optional[str] = None
    bin_end: Optional[str] = None
    demand: Optional[int] = None
    bin_capacity: Optional[int] = None
    overage: Optional[int] = None
    eta_quality: Optional[str] = None
    exceeded: Optional[bool] = None


class AirportRateRow(BaseModel):
    airport: str
    aar: Optional[int] = None
    adr: Optional[int] = None
    runway_config: Optional[str] = None
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None


class BreachContextRow(BaseModel):
    airport: str
    airport_name: Optional[str] = None
    severity: Optional[str] = None
    bin_start: Optional[str] = None
    bin_end: Optional[str] = None
    demand: Optional[int] = None
    bin_capacity: Optional[int] = None
    overage: Optional[int] = None
    wx_condition: Optional[str] = None
    wx_ceiling_ft: Optional[int] = None
    wx_visibility_mi: Optional[float] = None
    wx_wind_kt: Optional[int] = None
    active_restriction_type: Optional[str] = None
    active_restriction_reason: Optional[str] = None


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/demand", response_model=list[CapacityRow])
async def get_demand(
    airport: Optional[str] = Query(default=None, description="Filter to one airport"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    """Current per-bin demand vs published capacity (capacity_assessment MV).

    Drives the per-airport AAR-vs-rolling-hour-demand gauges on the dashboard.
    """
    client = get_feldera_client()
    where = f"WHERE airport = '{sql_quote(airport)}'" if airport else ""
    rows = await client.query(
        f"""
        SELECT airport, bin_start, bin_end, demand, bin_capacity,
               overage, eta_quality, exceeded
        FROM capacity_assessment
        {where}
        ORDER BY bin_start DESC
        LIMIT {int(limit)}
        """
    )
    return [
        CapacityRow(
            airport=row["airport"],
            bin_start=_s(row.get("bin_start")),
            bin_end=_s(row.get("bin_end")),
            demand=_i(row.get("demand")),
            bin_capacity=_i(row.get("bin_capacity")),
            overage=_i(row.get("overage")),
            eta_quality=_s(row.get("eta_quality")),
            exceeded=_b(row.get("exceeded")),
        )
        for row in rows
    ]


@router.get("/rates", response_model=list[AirportRateRow])
async def get_rates():
    """Published arrival rates per airport (mcp_airport_rate MV).

    AAR = Airport Acceptance Rate, ADR = Airport Departure Rate.
    """
    client = get_feldera_client()
    # mcp_airport_rate exposes valid_from only (no valid_to) — selecting valid_to 500s.
    rows = await client.query(
        """
        SELECT airport, aar, adr, runway_config, valid_from
        FROM mcp_airport_rate
        ORDER BY airport
        """
    )
    return [
        AirportRateRow(
            airport=row["airport"],
            aar=_i(row.get("aar")),
            adr=_i(row.get("adr")),
            runway_config=_s(row.get("runway_config")),
            valid_from=_s(row.get("valid_from")),
            valid_to=_s(row.get("valid_to")),
        )
        for row in rows
    ]


@router.get("/breaches", response_model=list[BreachContextRow])
async def get_breaches(limit: int = Query(default=50, ge=1, le=200)):
    """Enriched capacity breaches (arrival_context MV) — the SAM triggers."""
    client = get_feldera_client()
    rows = await client.query(
        f"""
        SELECT airport, airport_name, severity, bin_start, bin_end,
               demand, bin_capacity, overage,
               wx_flight_category, wx_visibility_sm, wx_wind_kt,
               active_restriction_type, active_restriction_reason
        FROM arrival_context
        ORDER BY bin_start DESC
        LIMIT {int(limit)}
        """
    )
    return [
        BreachContextRow(
            airport=row["airport"],
            airport_name=_s(row.get("airport_name")),
            severity=_s(row.get("severity")),
            bin_start=_s(row.get("bin_start")),
            bin_end=_s(row.get("bin_end")),
            demand=_i(row.get("demand")),
            bin_capacity=_i(row.get("bin_capacity")),
            overage=_i(row.get("overage")),
            # arrival_context carries wx_flight_category / wx_visibility_sm (not the old
            # wx_condition / wx_ceiling_ft / wx_visibility_mi); map to the response model.
            wx_condition=_s(row.get("wx_flight_category")),
            wx_ceiling_ft=None,
            wx_visibility_mi=_f(row.get("wx_visibility_sm")),
            wx_wind_kt=_i(row.get("wx_wind_kt")),
            active_restriction_type=_s(row.get("active_restriction_type")),
            active_restriction_reason=_s(row.get("active_restriction_reason")),
        )
        for row in rows
    ]


# ── Reference / side-input feeds (rates, weather, restrictions) ───────────────
# The versioned "side inputs" Continuum joins into the live window. Whitelisted
# read-only queries over the mcp_* views, for the continuum.html inspector panels.
_REFERENCE_QUERIES = {
    "rates": "SELECT airport, aar, adr, runway_config FROM mcp_airport_rate ORDER BY airport",
    "weather": ("SELECT station_id, flight_category, wind_dir_degrees, wind_speed_kt, "
                "visibility_sm, wx_string FROM mcp_weather ORDER BY station_id"),
    "restrictions": ("SELECT airport, restriction_type, reason, scope "
                     "FROM mcp_active_restrictions ORDER BY airport"),
}


@router.get("/reference/{name}")
async def get_reference(name: str):
    """A reference/side-input feed Continuum joins (name = rates|weather|restrictions)."""
    sql = _REFERENCE_QUERIES.get(name)
    if sql is None:
        return []
    return await get_feldera_client().query(sql)


# ── Latest-state polling (fed by the Solace receiver) ─────────────────────────
@router.get("/latest/{stream}")
async def get_latest(stream: str):
    """Most recent payloads retained by the receiver for a live stream.

    stream ∈ {demand, breach, decision, triage}. Lets a client poll live state
    instead of holding an SSE/MQTT connection.
    """
    if stream not in STREAM_NAMES:
        return {"error": "unknown stream", "streams": STREAM_NAMES}
    return get_receiver().latest(stream)


# ── Agentic-analysis toggle ───────────────────────────────────────────────────
# Presenter switch for the SAM (LLM) triage layer. When OFF, the Continuum breach
# still goes red on the dashboard but the agent is not surfaced; turn it ON to let
# SAM produce the fast-triage verdict and the slow decision brief.
class AgentState(BaseModel):
    enabled: bool


@router.get("/agent-state", response_model=AgentState)
async def get_agent_state():
    return AgentState(enabled=is_agent_enabled())


@router.post("/agent-state", response_model=AgentState)
async def post_agent_state(state: AgentState):
    set_agent_enabled(state.enabled)
    logger.info("Agentic analysis %s", "ENABLED" if state.enabled else "DISABLED")
    return AgentState(enabled=is_agent_enabled())


@router.post("/scenario/start")
async def scenario_start(request: Request):
    """Trigger ONE hero-airport wind-shift scenario on the scenario driver
    (button-driven demo). An optional ?airport=... overrides the hero airport.

    The scenario driver (stack/scenario/scenario_driver.py) listens on the
    control topic faa/scenario/windshift and expects
    {"trigger":"windshift","airport":"KEWR"}. (The driver pins the hero from its
    HERO_AIRPORT env and ignores this airport field; it is kept for logging/trace.)"""
    import json as _json
    airport = request.query_params.get("airport") or "KEWR"
    get_receiver().publish(
        "faa/scenario/windshift",
        _json.dumps({"trigger": "windshift", "airport": airport}),
    )
    logger.info("Scenario wind shift requested for %s", airport)
    return {"airport": airport}


@router.post("/scenario/reset")
async def scenario_reset(request: Request):
    """Recover the hero airport to green so the demo is repeatable, and WAIT for the data
    plane to confirm it before returning. Tells the scenario driver to restore the rate +
    SW wind / 22L|22R flow, cancel the restriction, and evict the bunched arrivals; then
    polls the Continuum incident-state feed (fl/faa/arrival-capacity/incident-state/{airport})
    until the hero reads CLEAR. Returns green=True only on that confirmed CLEAR edge —
    never an optimistic guess. green=False on timeout, so the caller can honestly show
    'still clearing' instead of a false green. ?timeout= overrides the wait (default 15s)."""
    import json as _json
    import asyncio
    airport = request.query_params.get("airport") or "KEWR"
    recv = get_receiver()
    recv.publish(
        "faa/scenario/reset",
        _json.dumps({"trigger": "reset", "airport": airport}),
    )
    logger.info("Scenario reset requested for %s", airport)

    # Verified handshake: poll the retained incident-state until the hero reads CLEAR.
    # The incident-state sink publishes the SEVERE↔CLEAR edge (no skip_deletes), so a
    # confirmed CLEAR is a real recovery, not a fixed sleep.
    timeout_s = float(request.query_params.get("timeout") or 15.0)
    interval_s = 0.4
    waited = 0.0
    status = None
    while waited < timeout_s:
        row = next((r for r in recv.latest("incident_state") if r.get("airport") == airport), None)
        status = (row or {}).get("status")
        if status == "CLEAR":
            logger.info("Scenario reset confirmed green for %s in %.1fs", airport, waited)
            return {"airport": airport, "reset": True, "green": True, "waited_ms": int(waited * 1000)}
        await asyncio.sleep(interval_s)
        waited += interval_s
    logger.warning("Scenario reset for %s not confirmed green within %.0fs (status=%s)",
                   airport, timeout_s, status)
    return {"airport": airport, "reset": True, "green": False,
            "waited_ms": int(waited * 1000), "status": status}


# ── Demo console inject fallback (static/console.html) ────────────────────────
# The console publishes inject events straight from the browser over the
# /fx/mqtt WebSocket proxy; these endpoints are the break-glass path. The
# allowlist keeps this from being an open broker relay.
# fl/faa/… + solutions/… are the Continuum/SAM topics; FDPS/ TTP/ aviation/ are the
# SWIM ingest feeds the demo console injects (e.g. continuum.html "Add arrivals" needs
# to publish real flight-plan + ETA events so Feldera actually ingests them).
_INJECT_PREFIXES = ("fl/faa/", "solutions/arrival-capacity/", "signals/", "scenario/",
                    "faa/scenario/", "FDPS/", "TTP/", "aviation/")


class InjectMsg(BaseModel):
    topic: str
    payload: str


class InjectBatch(BaseModel):
    messages: list[InjectMsg]


def _inject_one(msg: InjectMsg) -> bool:
    if not msg.topic.startswith(_INJECT_PREFIXES):
        logger.warning("inject rejected for topic %s", msg.topic)
        return False
    get_receiver().publish(msg.topic, msg.payload)
    return True


@router.post("/inject/publish")
async def inject_publish(msg: InjectMsg):
    """Publish one console inject event over the API's SMF publisher."""
    receiver = get_receiver()
    if receiver._publisher is None:
        return {"ok": False, "error": "solace publisher not connected"}
    if not _inject_one(msg):
        return {"ok": False, "error": "topic not allowed"}
    return {"ok": True}


@router.post("/inject/publish-batch")
async def inject_publish_batch(batch: InjectBatch):
    """One round-trip for the burst button (many events at once)."""
    receiver = get_receiver()
    if receiver._publisher is None:
        return {"ok": False, "sent": 0, "error": "solace publisher not connected"}
    sent = sum(1 for m in batch.messages if _inject_one(m))
    return {"ok": True, "sent": sent, "rejected": len(batch.messages) - sent}


@router.get("/capacity-watch/status")
async def capacity_watch_status():
    """deployment_status of the demo-console pipeline, for the console's
    STOPPED/RUNNING chip. Feldera's own API has no CORS headers, so proxy it."""
    try:
        return {"deployment_status": await get_watch_client().deployment_status()}
    except Exception as exc:
        return {"deployment_status": "unknown", "detail": str(exc)}


# ── SSE streaming endpoints (driven by Continuum sinks + SAM via Solace) ──────

def _sse_stream(stream_name: str, event_name: str):
    """Build an SSE endpoint bound to a Solace receiver stream."""

    async def endpoint(request: Request):
        receiver = get_receiver()
        q = receiver.subscribe(stream_name)

        async def generator():
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        payload = await asyncio.wait_for(q.get(), timeout=30.0)
                        yield {"event": event_name, "data": payload}
                    except asyncio.TimeoutError:
                        yield {"event": "heartbeat", "data": "{}"}
            finally:
                receiver.unsubscribe(stream_name, q)

        return EventSourceResponse(generator())

    return endpoint


router.add_api_route("/stream/demand", _sse_stream("demand", "demand"), methods=["GET"])
router.add_api_route("/stream/breach", _sse_stream("breach", "breach"), methods=["GET"])
router.add_api_route("/stream/decision", _sse_stream("decision", "decision"), methods=["GET"])
router.add_api_route("/stream/triage", _sse_stream("triage", "triage"), methods=["GET"])


# ── Materialized-view change stream (server-side fan-in of Feldera egress watches) ──
# The browser can't hold 14 live engine connections (≈6-per-origin cap), so the API
# watches every MV's egress change feed here and multiplexes "which views changed" into
# ONE SSE the UI consumes to flash each box on any insert/delete (an in-place update
# arrives as delete+insert). Per-connection fan-in; fine for a demo's handful of viewers.
_MV_VIEWS = [
    "airport_rate", "flight", "arrival_demand", "capacity_assessment",
    "capacity_breach_events", "metar_latest", "active_restriction",
    "arrival_context", "arrival_incident", "mcp_arrival_demand",
    "mcp_airport_rate", "mcp_active_restrictions", "mcp_weather", "mcp_downstream_risk",
]


async def _watch_view(base_url: str, pipeline: str, view: str, changed: set) -> None:
    """Stream one MV's egress change feed; mark `view` changed on each real delta."""
    url = f"{base_url.rstrip('/')}/v0/pipelines/{pipeline}/egress/{view}"
    params = {"format": "json", "mode": "watch"}
    while True:
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("POST", url, params=params) as resp:
                    if resp.status_code >= 400:
                        logger.warning("egress watch %s -> HTTP %s (giving up)", view, resp.status_code)
                        return
                    async for line in resp.aiter_lines():
                        if not line.strip():
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        if obj.get("snapshot"):      # skip the initial state dump
                            continue
                        if obj.get("json_data"):     # a real insert/delete batch
                            changed.add(view)
        except asyncio.CancelledError:
            raise
        except Exception as exc:                     # a blip — reconnect after a beat
            logger.debug("egress watch %s blipped (%s); retrying", view, exc)
            await asyncio.sleep(2.0)


async def _mv_changes(request: Request):
    s = get_settings()
    changed: set[str] = set()
    tasks = [
        asyncio.create_task(_watch_view(s.feldera_url, s.feldera_pipeline, v, changed))
        for v in _MV_VIEWS
    ]

    async def generator():
        ticks = 0
        try:
            while True:
                if await request.is_disconnected():
                    break
                await asyncio.sleep(0.2)             # coalesce rapid changes into ~5 msgs/s
                ticks += 1
                if changed:
                    views = sorted(changed)
                    changed.clear()
                    yield {"event": "mv", "data": json.dumps({"views": views})}
                elif ticks % 100 == 0:              # ~20s keepalive
                    yield {"event": "heartbeat", "data": "{}"}
        finally:
            for t in tasks:
                t.cancel()

    return EventSourceResponse(generator())


router.add_api_route("/stream/mv-changes", _mv_changes, methods=["GET"])


# ── "Add data product" — same-origin proxy to the continuum-mcp generation engine ──
# The browser can't reach continuum-mcp directly (MCP JSON-RPC, Bearer-gated, no CORS),
# so the api forwards create/get calls server-side (keeping the token off the client),
# mirroring the demo scripts' mcp() helper (demo/create-product.py).
async def _mcp_call(tool: str, args: dict) -> dict:
    s = get_settings()
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": tool, "arguments": args}}
    headers = {"Authorization": f"Bearer {s.continuum_mcp_token}",
               "Accept": "application/json", "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=90) as client:
        resp = await client.post(s.continuum_mcp_url, json=body, headers=headers)
        resp.raise_for_status()
        d = resp.json()
    res = d.get("result", d)
    content = res.get("content", res) if isinstance(res, dict) else res
    if isinstance(content, list) and content and isinstance(content[0], dict) and "text" in content[0]:
        try:
            return json.loads(content[0]["text"])
        except Exception:
            return {"_raw": content[0]["text"]}
    return res if isinstance(res, dict) else {"_raw": res}


class DataProductSpec(BaseModel):
    name: str
    description: str = ""
    sql: str
    source_streams: list[str]
    key_columns: list[str]
    event_time_column: Optional[str] = None
    triggers: Optional[list[dict]] = None
    register_in_event_portal: bool = False   # off by default: avoids the EP token dependency


@router.get("/data-product/presets")
async def data_product_presets():
    """The prewarmed product specs (demo/products/*.json), to prefill the authoring form."""
    out = []
    pdir = Path(get_settings().products_dir)
    if pdir.is_dir():
        for f in sorted(pdir.glob("*.json")):
            try:
                out.append({"file": f.stem, "spec": json.loads(f.read_text())})
            except Exception as exc:
                logger.warning("preset %s unreadable: %s", f, exc)
    return out


@router.post("/data-product/create")
async def data_product_create(spec: DataProductSpec):
    """Create a Continuum data product via the generation engine (returns status=compiling)."""
    args = {k: v for k, v in spec.model_dump().items() if v is not None}
    try:
        return await _mcp_call("create_data_product", args)
    except Exception as exc:
        logger.warning("create_data_product failed: %s", exc)
        return {"error": f"continuum-mcp unreachable or errored: {exc}"}


@router.get("/data-product/status/{name}")
async def data_product_status(name: str):
    """Poll a product's lifecycle (compiling → running) by name."""
    try:
        return await _mcp_call("get_data_product", {"name": name})
    except Exception as exc:
        return {"error": str(exc)}

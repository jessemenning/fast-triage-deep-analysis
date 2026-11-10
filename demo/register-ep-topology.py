#!/usr/bin/env python3
"""
register-ep-topology.py — register the WHOLE arrival-capacity event mesh in Event Portal.

continuum-mcp registers only what a data PRODUCT produces. This one-shot script models
the full hero flow in the "FAA Arrival Capacity" domain so Designer draws it end to end:

    FAA SWIM / SCDS Feeds ─(6 feeds)→ Continuum ─(breach, demand)→ SAM ─(triage, decision)→ Traffic Manager Console

It registers every event (draft-07 schema + delivery descriptor on its real topic) and
each application with its produced/consumed events, idempotently (reusing continuum-mcp's
EventPortalClient). Re-runnable.

Run INSIDE the continuum-mcp container so it reuses the EP client + the token:
    docker compose cp demo/register-ep-topology.py continuum-mcp:/app/register-ep-topology.py
    docker compose exec -T continuum-mcp /app/.venv/bin/python /app/register-ep-topology.py
(or: make register-ep)
"""
import asyncio
import os

from continuum_mcp.config import load_settings
from continuum_mcp.ep_client import EventPortalClient

# Reuse the Continuum data-product app (from `make create-product`) as the Continuum
# node when present; otherwise a dedicated app is created.
CONTINUUM_APP = os.environ.get("CONTINUUM_APP", "continuum-arrival-demand-watch")

S = lambda d="": {"type": "string", "description": d}        # noqa: E731
I = lambda d="": {"type": "integer", "description": d}        # noqa: E731
N = lambda d="": {"type": "number", "description": d}         # noqa: E731
B = lambda d="": {"type": "boolean", "description": d}        # noqa: E731
O = lambda d="": {"type": "object", "description": d}         # noqa: E731


def schema(title, props, required, desc=""):
    return {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object",
            "title": title, "description": desc, "properties": props, "required": required}


# ── Every event in the mesh: (event name, schema name, topic, schema, description) ──
INPUTS = [
    ("swim.fdps.position", "FdpsPosition",
     "FDPS/position/{flight_id}/{status}/{callsign}/{origin}/{destination}/{latitude}/{longitude}/{ground_speed}/{altitude_ft}/{heading}/{gufi}",
     schema("FdpsPosition", {"flight_id": S(), "gufi": S()}, ["gufi"]),
     "FDPS surveillance position (most fields in the topic levels)."),
    ("swim.fdps.flightplan", "FdpsFlightPlan",
     "FDPS/flightPlan/{flight_id}/{status}/{departure}/{arrival}/{gufi}",
     schema("FdpsFlightPlan", {"flight_id": S(), "status": S(), "departure": S(), "arrival": S(), "gufi": S()}, ["gufi", "arrival"]),
     "FDPS flight plan: departure/arrival + status per flight."),
    ("swim.ttpfd.eta", "TtpfdEta",
     "TTP/flightData/{gufi}/{arrival}/{eta_source}",
     schema("TtpfdEta", {"gufi": S(), "arrival": S(), "eta": S("arrival ETA UTC"), "eta_source": S("TTPFD|TBFM|FILED")}, ["gufi", "arrival", "eta"]),
     "TTPFD: best-available arrival ETA per flight + source quality."),
    ("swim.ttp.airport-info", "TtpAirportInfo",
     "TTP/airportInfo/{airport}",
     schema("TtpAirportInfo", {"airport": S(), "aar": I("arrival rate/hr"), "adr": I("departure rate/hr"), "runway_config": S(), "effective_time": S("UTC")}, ["airport", "aar", "effective_time"]),
     "TTP Airport Information: published AAR/ADR + runway config (versioned)."),
    ("swim.tmr.restriction", "TmrRestriction",
     "TTP/restriction/{airport}/{restriction_type}",
     schema("TmrRestriction", {"airport": S(), "restriction_type": S("GDP|APREQ|GROUND_STOP"), "reason": S(), "scope": S(), "effective_time": S("UTC")}, ["airport", "restriction_type"]),
     "TMR: traffic-management restriction in effect."),
    ("swim.metar", "Metar",
     "aviation/metar/{station_id}",
     schema("Metar", {"icaoId": S(), "reportTime": S("UTC"), "fltCat": S(), "wdir": S(), "wspd": I(), "visib": S(), "rawOb": S()}, ["icaoId", "reportTime"]),
     "METAR aviation weather observation."),
]

_breach_props = {
    "airport": S(), "airport_name": S(), "tracon": S(), "bin_start": S("UTC"), "bin_end": S("UTC"),
    "demand": I("expected arrivals in the rolling hour"), "bin_capacity": I("AAR in effect"),
    "overage": I("demand - capacity"), "severity": S("MARGINAL|MODERATE|SEVERE"), "runway_config": S(),
    "eta_quality": N("0..1 share of TTPFD/TBFM ETAs"), "wx_flight_category": S(), "wx_wind_kt": I(),
    "wx_visibility_sm": S(), "wx_raw": S(), "active_restriction_type": S(), "active_restriction_reason": S(),
}
CONTINUUM_OUT = [
    ("arrival-capacity.breach", "ArrivalCapacityBreach",
     "fl/faa/arrival-capacity/breach/{airport}",
     schema("ArrivalCapacityBreach", _breach_props, ["airport"]),
     "Continuum: arrival demand exceeds the published rate (the agent trigger)."),
    ("arrival-capacity.demand", "ArrivalCapacityDemand",
     "fl/faa/arrival-capacity/demand/{airport}",
     schema("ArrivalCapacityDemand", {"airport": S(), "bin_start": S("UTC"), "bin_end": S("UTC"),
            "demand": I(), "bin_capacity": I(), "overage": I(), "eta_quality": N(), "exceeded": B()}, ["airport"]),
     "Continuum: live rolling-hour demand vs capacity per airport (UI feed)."),
]
SAM_OUT = [
    ("arrival-capacity.triage", "ArrivalCapacityTriage",
     "solutions/arrival-capacity/triage",
     schema("ArrivalCapacityTriage", {"stage": S(), "action": S("ESCALATE|GROUP|SUPPRESS"), "will_materialize": S(),
            "impact": S(), "case_priority": S(), "confidence": N(), "decided_by": S(), "event": S()}, ["action"]),
     "SAM: fast-triage interim verdict (sub-second)."),
    ("arrival-capacity.decision", "ArrivalCapacityDecision",
     "solutions/arrival-capacity/decision/{airport}",
     schema("ArrivalCapacityDecision", {"airport": S(), "action": S("ESCALATE|GROUP|SUPPRESS"), "will_materialize": S(),
            "self_resolves": S(), "impact": S(), "is_upstream_symptom": N(), "case_priority": S(), "confidence": N(),
            "decided_by": S(), "rationale": S(), "advisory_notice": S(), "brief": O("the System-2 decision brief"), "event": S()}, ["airport", "action"]),
     "SAM: final triage decision + advisory decision brief (options/provenance)."),
]


async def ensure_event(ep, domain_id, name, schema_name, topic, sch, desc):
    sid = await ep.ensure_schema(domain_id, schema_name)
    svid = await ep.ensure_schema_version(sid, "1.0.0", sch)
    eid = await ep.ensure_event(domain_id, name)
    return await ep.ensure_event_version(eid, "1.0.0", schema_version_id=svid, topic_template=topic, description=desc)


async def main():
    s = load_settings()
    if not s.solace_api_token:
        raise SystemExit("SOLACE_API_TOKEN is not set — Event Portal registration is disabled.")
    ep = EventPortalClient(s.ep_base_url, s.solace_api_token)
    try:
        domain_id = await ep.ensure_domain(
            s.ep_domain, "FAA arrival-capacity event mesh: SWIM/SCDS feeds → Continuum → SAM → operations.")
        print(f"domain '{s.ep_domain}' -> {domain_id}")

        ev = {}  # event name -> event version id
        for group in (INPUTS, CONTINUUM_OUT, SAM_OUT):
            for name, sname, topic, sch, desc in group:
                ev[name] = await ensure_event(ep, domain_id, name, sname, topic, sch, desc)
                print(f"  event {name:28s} {topic.split('/')[0]}/...")

        feed_ids   = [ev[n] for n, *_ in INPUTS]
        breach, demand = ev["arrival-capacity.breach"], ev["arrival-capacity.demand"]
        triage, decision = ev["arrival-capacity.triage"], ev["arrival-capacity.decision"]

        # 1) SWIM producer
        swim = await ep.ensure_application(domain_id, "FAA SWIM / SCDS Feeds")
        await ep.ensure_application_version(swim, produced_event_version_ids=feed_ids,
            description="Upstream FAA SWIM/SCDS source feeds.")
        print("app  FAA SWIM / SCDS Feeds        produces 6")

        # Keep the hero graph a clean linear chain: drop the generation-engine product
        # app + its dangling (unconsumed) leaf events so nothing hangs off the line.
        for step in await ep.unregister_application(domain_id, CONTINUUM_APP):
            print(f"  cleanup: {step}")
        for leaf in ("arrival-demand-watch.live", "arrival-demand-watch.demand-exceeds-rate"):
            for step in await ep.unregister_event(domain_id, leaf):
                print(f"  cleanup: {step}")

        # 2) Continuum (dedicated hero node): consumes the feeds, produces breach + demand.
        cont_id = await ep.ensure_application(domain_id, "Continuum — Arrival Capacity")
        await ep.ensure_application_version(cont_id, produced_event_version_ids=[breach, demand],
            consumed_event_version_ids=feed_ids,
            description="Continuum arrival-capacity: consumes the SWIM feeds; emits the breach + demand events.")
        print("app  Continuum — Arrival Capacity consumes 6 (feeds), produces 2 (breach, demand)")

        # 3) SAM: consumes the breach, produces triage + decision
        sam = await ep.ensure_application(domain_id, "SAM — Arrival Triage")
        await ep.ensure_application_version(sam, produced_event_version_ids=[triage, decision],
            consumed_event_version_ids=[breach],
            description="Solace Agent Mesh: fast triage + System-2 decision brief on each arrival-capacity breach.")
        print("app  SAM — Arrival Triage         consumes 1 (breach), produces 2 (triage, decision)")

        # 4) Consumer: the ops console / traffic-manager UI
        console = await ep.ensure_application(domain_id, "Traffic Manager Console")
        await ep.ensure_application_version(console, produced_event_version_ids=[],
            consumed_event_version_ids=[decision, triage, demand],
            description="Operations console: shows the demand picture and the triage decision + brief.")
        print("app  Traffic Manager Console      consumes 3 (decision, triage, demand)")
        print("\nmesh connected: SWIM → Continuum → SAM → Console")
    finally:
        await ep.aclose()


if __name__ == "__main__":
    asyncio.run(main())

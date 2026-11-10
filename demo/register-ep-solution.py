#!/usr/bin/env python3
"""
register-ep-solution.py — register a GENERATED solution's SAM side in Event Portal.

The Continuum product already self-registers its producer app (`continuum-<slug>`) and
its events (`<slug>.live`, `<slug>.<trigger>`) when created with register_in_event_portal.
This script completes the mesh for a generated solution by adding:
  • the SAM app  "SAM — <Title>"  — CONSUMES the product's trigger event, PRODUCES the
    decision + triage events (solutions/<slug>/decision/{key}, solutions/<slug>/triage);
  • a consumer app "<Title> Consumer" — CONSUMES the decision + triage events.
All in the same domain (CONTINUUM_EP_DOMAIN, default "FAA Arrival Capacity"), idempotently.

So Designer draws, for the generated solution:
  Continuum (product) ─<trigger>─▶ SAM — <Title> ─triage, decision─▶ <Title> Consumer

Run INSIDE the continuum-mcp container (reuses its EP client + token), e.g. via
`make register-ep-solution SLUG=<slug>`:
  docker compose cp demo/register-ep-solution.py continuum-mcp:/app/register-ep-solution.py
  docker compose exec -T continuum-mcp /app/.venv/bin/python /app/register-ep-solution.py <slug> \
     --trigger <trigger_name> --key <key_col>
"""
import argparse
import asyncio

from continuum_mcp.config import load_settings
from continuum_mcp.ep_client import EventPortalClient

S = lambda d="": {"type": "string", "description": d}       # noqa: E731
N = lambda d="": {"type": "number", "description": d}       # noqa: E731
O = lambda d="": {"type": "object", "description": d}       # noqa: E731


def schema(title, props, required, desc=""):
    return {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object",
            "title": title, "description": desc, "properties": props, "required": required}


def title_of(slug: str) -> str:
    return " ".join(w.capitalize() for w in slug.replace("-", " ").split())


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("slug")
    ap.add_argument("--trigger", required=True, help="the Continuum trigger name (e.g. wind-risk)")
    ap.add_argument("--key", default="airport", help="the decision topic key column (default airport)")
    args = ap.parse_args()
    slug, trig, key = args.slug, args.trigger, args.key
    title = title_of(slug)

    s = load_settings()
    if not s.solace_api_token:
        raise SystemExit("SOLACE_API_TOKEN is not set — Event Portal registration is disabled.")
    ep = EventPortalClient(s.ep_base_url, s.solace_api_token)
    try:
        domain_id = await ep.ensure_domain(s.ep_domain, "FAA arrival-capacity event mesh.")

        # Find the product's trigger event version (created by continuum-mcp as "<slug>.<trigger>").
        trig_event_name = f"{slug}.{trig}"
        ev = await ep.find_by_name("/events", trig_event_name, applicationDomainId=domain_id)
        if not ev:
            raise SystemExit(f"trigger event '{trig_event_name}' not found — create the product first "
                             f"(make create-product / continuum-create-product.py) with register_in_event_portal.")
        trig_vers = await ep.find_version("/eventVersions", "1.0.0", eventIds=ev["id"])
        trig_vv = trig_vers["id"]

        # Decision + triage events produced by SAM.
        dec_schema = schema(f"{title.replace(' ','')}Decision", {
            key: S(), "action": S("ESCALATE|GROUP|SUPPRESS"), "will_materialize": S(), "self_resolves": S(),
            "impact": S(), "case_priority": S(), "confidence": N(), "decided_by": S(), "rationale": S(),
            "advisory_notice": S(), "brief": O("the System-2 decision brief"), "event": S()},
            [key, "action"], f"SAM triage decision + brief for {title}.")
        tri_schema = schema(f"{title.replace(' ','')}Triage", {
            "stage": S(), "action": S(), "will_materialize": S(), "impact": S(), "case_priority": S(),
            "confidence": N(), "decided_by": S(), "event": S()}, ["action"], f"SAM interim triage for {title}.")

        async def ensure_event(name, sname, topic, sch, desc):
            sid = await ep.ensure_schema(domain_id, sname)
            svid = await ep.ensure_schema_version(sid, "1.0.0", sch)
            eid = await ep.ensure_event(domain_id, name)
            return await ep.ensure_event_version(eid, "1.0.0", schema_version_id=svid, topic_template=topic, description=desc)

        dec_vv = await ensure_event(f"{slug}.decision", dec_schema["title"],
                                    f"solutions/{slug}/decision/{{{key}}}", dec_schema, f"{title}: final decision + brief.")
        tri_vv = await ensure_event(f"{slug}.triage", tri_schema["title"],
                                    f"solutions/{slug}/triage", tri_schema, f"{title}: interim fast-triage verdict.")

        sam_app = await ep.ensure_application(domain_id, f"SAM — {title}")
        await ep.ensure_application_version(sam_app, produced_event_version_ids=[tri_vv, dec_vv],
            consumed_event_version_ids=[trig_vv],
            description=f"Solace Agent Mesh: fast triage + System-2 brief for the {title} scenario.")
        print(f"app  SAM — {title}: consumes 1 ({trig_event_name}), produces 2 (triage, decision)")

        cons = await ep.ensure_application(domain_id, f"{title} Consumer")
        await ep.ensure_application_version(cons, produced_event_version_ids=[],
            consumed_event_version_ids=[dec_vv, tri_vv],
            description=f"Downstream consumer of the {title} decision + triage events.")
        print(f"app  {title} Consumer: consumes 2 (decision, triage)")
        print(f"\nwired: Continuum ({slug}) → SAM — {title} → {title} Consumer")
    finally:
        await ep.aclose()


if __name__ == "__main__":
    asyncio.run(main())

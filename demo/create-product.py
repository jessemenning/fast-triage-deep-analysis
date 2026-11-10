#!/usr/bin/env python3
"""Ensure the Continuum `arrival-demand-watch` data product + demand-exceeds-rate trigger
exist and are running — idempotently, and healing the stale-catalog case.

Why this exists: `make demo` only publishes the FAA feeds; the custom trigger topic
`fl/cp/arrival-demand-watch/custom-breach/{airport}` is produced by this Continuum
product, so it must exist first. After a `docker compose down` the engine has no volume,
so the `cp-arrival-demand-watch` pipeline is gone while the Continuum catalog may still
report the product `running` (stale) — this script detects that and recreates.

This product is a worked demonstration of the generation engine: it takes the base
pipeline's live `capacity_assessment` view (arrival demand vs the published AAR per bin)
and raises its own event whenever a bin's demand exceeds the rate in effect.

Note on the trigger topic: the MCP enforces the `fl/cp/<slug>/` prefix for product-owned
topics (see continuum_mcp/sql_guard.validate_topic_template), so the custom breach lands on
`fl/cp/arrival-demand-watch/custom-breach/{airport}`. The platform-owned base breach sink is
the separate `fl/faa/arrival-capacity/breach/{airport}`.

Run from the repo root (reads CONTINUUM_MCP_TOKEN from the environment / .env). Safe to
re-run: if the product is genuinely running (engine pipeline present), it's a no-op.

    python3 demo/create-product.py            # ensure + wait for running
    python3 demo/create-product.py --no-wait  # kick off create, don't block on compile
"""
from __future__ import annotations
import json, os, sys, time, urllib.request, urllib.error

MCP_URL = os.environ.get("CONTINUUM_MCP_LOCAL_URL", "http://localhost:8091/mcp")
ENGINE_URL = os.environ.get("FELDERA_LOCAL_URL", "http://localhost:8081")
TOKEN = os.environ.get("CONTINUUM_MCP_TOKEN", "continuum-dev-token-local")
PRODUCT = "arrival-demand-watch"
PIPELINE = "cp-arrival-demand-watch"
WAIT = "--no-wait" not in sys.argv

# The product definition — a custom demand-vs-rate watch built on the base pipeline's
# capacity_assessment view, with a demand-exceeds-rate fan-in trigger per airport.
CREATE_ARGS = {
    "name": PRODUCT,
    "description": "FAA arrival-capacity watch: expected arrival demand per airport per bin vs the "
                   "published arrival rate (AAR) in effect, with a demand-exceeds-rate breach trigger.",
    "sql": "SELECT airport, bin_start, bin_end, demand, bin_capacity, runway_config, "
           "overage, eta_quality, exceeded "
           "FROM capacity_assessment",
    "source_streams": ["capacity_assessment"],
    "key_columns": ["airport"],
    "event_time_column": "bin_start",
    "triggers": [{
        "name": "demand-exceeds-rate",
        "description": "Expected arrival demand for a bin exceeded the published arrival rate (AAR) "
                       "in effect for that airport.",
        "condition": "exceeded = TRUE OR overage > 0",
        "output_topic_template": "fl/cp/arrival-demand-watch/custom-breach/{airport}",
        "payload_columns": ["airport", "bin_start", "bin_end", "demand", "bin_capacity",
                            "runway_config", "overage", "eta_quality"],
        "distinct": True,
        # demand/overage/eta_quality are declared volatile on the capacity_assessment stream
        # (they churn as ETAs shift within a bin); keep them on the payload anyway so the
        # breach event is self-contained for the agent. recency_window keeps firing to live bins.
        "allow_volatile_payload": True,
        "recency_window": "60 minutes",
    }],
}


def mcp(name: str, args: dict) -> dict:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": name, "arguments": args}}).encode()
    req = urllib.request.Request(MCP_URL, data=body, headers={
        "Authorization": f"Bearer {TOKEN}", "Accept": "application/json",
        "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read())
    res = d.get("result", d)
    c = res.get("content", res)
    txt = c[0]["text"] if isinstance(c, list) and c and "text" in c[0] else json.dumps(res)
    try:
        return json.loads(txt)
    except Exception:
        return {"_raw": txt}


def engine_has_pipeline() -> bool:
    try:
        with urllib.request.urlopen(f"{ENGINE_URL}/v0/pipelines", timeout=10) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("data", [])
        return any(p.get("name") == PIPELINE for p in rows)
    except Exception:
        return False


def status() -> str | None:
    # get_data_product returns the product with an "error": null key on success, so test the
    # value's truthiness, not key presence.
    d = mcp("get_data_product", {"name": PRODUCT})
    return d.get("status") if not d.get("error") else None


def start_pipeline() -> None:
    try:
        urllib.request.urlopen(urllib.request.Request(
            f"{ENGINE_URL}/v0/pipelines/{PIPELINE}/start", method="POST"), timeout=30).read()
    except urllib.error.HTTPError as e:
        print(f"    (start returned HTTP {e.code})")


def wait_running() -> int:
    if not WAIT:
        print("  (--no-wait: not blocking; poll with get_data_product)")
        return 0
    print("  waiting for the pipeline to start (warm ≈ seconds; cold compile ≈ 6–7 min) ...")
    deadline, last = time.time() + 900, None
    while time.time() < deadline:
        s = status()
        if s != last:
            print(f"    status: {s}"); last = s
        if s == "running" and engine_has_pipeline():
            print(f"✓ '{PRODUCT}' running — trigger topic fl/cp/{PRODUCT}/custom-breach/>")
            return 0
        time.sleep(8)
    print("!! product did not reach 'running' within 15 min — check the Continuum MCP / engine logs",
          file=sys.stderr)
    return 1


def create_and_wait() -> int:
    r = mcp("create_data_product", CREATE_ARGS)
    if r.get("error"):
        if "already exists" in str(r["error"]).lower():
            print("    (already in catalog — starting its pipeline instead)")
            start_pipeline(); return wait_running()
        print(f"!! create failed: {r['error']}", file=sys.stderr)
        return 1
    print(f"… create accepted (status {r.get('status')}, pipeline {r.get('pipeline')})")
    return wait_running()


def main() -> int:
    in_catalog = status()
    pipeline = engine_has_pipeline()

    if pipeline:
        if in_catalog == "running":
            print(f"✓ '{PRODUCT}' already running (engine pipeline {PIPELINE} present) — nothing to do")
            return 0
        print(f"… '{PRODUCT}' pipeline {PIPELINE} is present but not running (status {in_catalog}) — starting it")
        start_pipeline()
        return wait_running()

    # No engine pipeline.
    if in_catalog is not None:
        print(f"… catalog has '{PRODUCT}' (status {in_catalog}) but engine pipeline {PIPELINE} is gone "
              f"(stale after a down) — deleting and recreating")
        mcp("delete_data_product", {"name": PRODUCT})
    else:
        print(f"… '{PRODUCT}' not found — creating")
    return create_and_wait()



if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
continuum-create-product.py — generic Continuum data-product creator for the
AI-driven scenario→solution generator.

Reads a product spec (JSON) and creates it via the continuum-mcp server (the same
JSON-RPC path demo/create-product.py uses), then waits for the pipeline to run.
This is the reusable Continuum step the `faa-generate-solution` skill calls per
scenario — the spec is the only thing that changes between scenarios.

Spec JSON (one SELECT over the ingestable base streams; see the skill for the envelope):
  {
    "name": "wind-arrival-risk",
    "description": "...",
    "sql": "SELECT ... FROM capacity_assessment c JOIN metar_latest w ON ...",
    "source_streams": ["capacity_assessment", "weather"],
    "key_columns": ["airport", "bin_start"],
    "event_time_column": "bin_start",
    "triggers": [ { "name","description","condition","output_topic_template",
                    "payload_columns","distinct","allow_volatile_payload","recency_window" } ]
  }

Usage:  python3 demo/continuum-create-product.py demo/products/<slug>.json [--no-wait] [--recreate]
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

MCP_URL = os.environ.get("CONTINUUM_MCP_LOCAL_URL", "http://localhost:8091/mcp")
ENGINE_URL = os.environ.get("FELDERA_LOCAL_URL", "http://localhost:8081")
TOKEN = os.environ.get("CONTINUUM_MCP_TOKEN", "continuum-dev-token-local")


def mcp(name: str, args: dict) -> dict:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                       "params": {"name": name, "arguments": args}}).encode()
    req = urllib.request.Request(MCP_URL, data=body, headers={
        "Authorization": f"Bearer {TOKEN}", "Accept": "application/json",
        "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        d = json.loads(r.read())
    res = d.get("result", d)
    c = res.get("content", res)
    txt = c[0]["text"] if isinstance(c, list) and c and "text" in c[0] else json.dumps(res)
    try:
        return json.loads(txt)
    except Exception:
        return {"_raw": txt}


def engine_has_pipeline(pipeline: str) -> bool:
    try:
        with urllib.request.urlopen(f"{ENGINE_URL}/v0/pipelines", timeout=10) as r:
            data = json.loads(r.read())
        rows = data if isinstance(data, list) else data.get("data", [])
        return any(p.get("name") == pipeline for p in rows)
    except Exception:
        return False


def status(product: str) -> str | None:
    d = mcp("get_data_product", {"name": product})
    return d.get("status") if not d.get("error") else None


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    flags = {a for a in sys.argv[1:] if a.startswith("--")}
    if not args:
        print("usage: continuum-create-product.py <spec.json> [--no-wait] [--recreate]", file=sys.stderr)
        return 2
    spec = json.load(open(args[0]))
    product = spec["name"]
    pipeline = f"cp-{product}"
    wait = "--no-wait" not in flags

    if "--recreate" in flags:
        print(f"recreate: deleting '{product}' first ...")
        mcp("delete_data_product", {"name": product, "drop_queues": True})
        time.sleep(3)

    if not status(product):
        print(f"creating data product '{product}' (pipeline {pipeline}) ...")
        r = mcp("create_data_product", spec)
        print("  create ->", json.dumps(r)[:300])
        if r.get("error"):
            print("  FAILED:", r.get("error"), file=sys.stderr)
            return 1
        for w in r.get("warnings", []) or []:
            print("  warning:", w)
    else:
        print(f"'{product}' already exists — ensuring running")

    if not wait:
        print("  (--no-wait) poll with get_data_product")
        return 0

    print("  waiting for pipeline running (warm ~secs; cold compile ~2-7 min) ...")
    deadline, last = time.time() + 900, None
    while time.time() < deadline:
        s = status(product)
        if s != last:
            print(f"    status: {s}"); last = s
        if s == "running" and engine_has_pipeline(pipeline):
            # show the consume contract (trigger topic the SAM gateway must subscribe)
            d = mcp("get_data_product", {"name": product})
            trigs = d.get("triggers", [])
            print(f"✓ '{product}' running")
            for t in trigs:
                print(f"   trigger '{t.get('name')}' → {t.get('output_topic_template')}")
            return 0
        if s == "failed":
            print("  pipeline FAILED — see get_data_product", file=sys.stderr)
            return 1
        time.sleep(5)
    print("  timed out waiting for running", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

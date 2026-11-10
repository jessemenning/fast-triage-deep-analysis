#!/usr/bin/env python3
"""
continuum-add-trigger.py — add a trigger to an EXISTING Continuum product (speedup #4).

When a scenario's detection can be expressed as a boolean over the columns an EXISTING
product already materializes (e.g. another filter over `arrival-demand-watch` or
`wind-arrival-risk`), add a trigger to that product instead of creating a whole new
`cp-<slug>` pipeline. The product's compiled program is reused (only the new sink view
is added), so this is faster and lighter than a new product — especially combined with
the unoptimized compile profile.

Trigger spec JSON (same shape as a product's triggers[] entry):
  { "name","description","condition","output_topic_template",
    "payload_columns","distinct","allow_volatile_payload","recency_window" }
The output_topic_template must start with fl/cp/<product-slug>/ and contain >=1 {column}.

Usage:  python3 demo/continuum-add-trigger.py <product> <trigger-spec.json>
"""
import json
import os
import sys
import urllib.request

MCP_URL = os.environ.get("CONTINUUM_MCP_LOCAL_URL", "http://localhost:8091/mcp")
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


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: continuum-add-trigger.py <product> <trigger-spec.json>", file=sys.stderr)
        return 2
    product = sys.argv[1]
    spec = json.load(open(sys.argv[2]))
    args = {"data_product": product, **spec}
    print(f"adding trigger '{spec.get('name')}' to product '{product}' ...")
    r = mcp("create_trigger", args)
    if r.get("error"):
        print("  FAILED:", r.get("error"), file=sys.stderr)
        return 1
    for w in r.get("warnings", []) or []:
        print("  warning:", w)
    print("  ->", json.dumps(r)[:300])
    # show the live trigger topic
    d = mcp("get_data_product", {"name": product})
    for t in d.get("triggers", []):
        if t.get("name") == spec.get("name"):
            print(f"✓ trigger '{t['name']}' → {t.get('topic_template')}  (subscribe {t.get('subscribe_pattern')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

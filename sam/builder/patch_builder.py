#!/usr/bin/env python3
"""Make the built-in SAM Builder Continuum-aware.

The Builder agent that backs the web UI /builder Deploy canvas is loaded directly from
`/etc/sam/configs/awe/builder_agent.yaml` inside the SAM image (see the `sam` service
command in docker-compose.yml). This script starts from that stock file (pinned to the
image version) and applies two surgical patches:

  1. appends `method.md` (Continuum Solution Mode) to app_config.instruction, so the
     Builder knows to discover/compose a Continuum data product + trigger BEFORE building
     the SAM half of the solution;
  2. appends the MCP tool entries in `mcp_tools.yaml` to app_config.tools, giving the
     Builder the continuum_* and ep_* tools (bearer over streamable-http).

Output is a complete builder_agent.yaml that docker-compose.yml bind-mounts over the
container path. Idempotent: re-running is safe. Run it on the HOST (needs pyyaml; the
container image has no yaml module).

    python3 patch_builder.py [STOCK] [OUT]

Defaults: STOCK=builder_agent.stock.yaml  OUT=builder_agent.yaml (both beside this file).

Refresh the stock after a SAM image upgrade:
    docker exec <sam-container> cat /etc/sam/configs/awe/builder_agent.yaml \
        > sam/builder/builder_agent.stock.yaml
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
MARKER = "CONTINUUM SOLUTION MODE"
PREPEND_MARKER = "CONTINUUM CRITICAL RULES"


def main() -> int:
    stock = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE / "builder_agent.stock.yaml"
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else HERE / "builder_agent.yaml"

    doc = yaml.safe_load(stock.read_text())
    app_config = doc["apps"][0]["app_config"]

    # 1a) PREPEND the critical absolute rules to the TOP of the instruction (idempotent).
    #     Top-of-prompt placement gives the most-missed rules (esp. the gateway
    #     successOutput expression) primacy over the Builder's general habits; an appended
    #     rule alone was followed inconsistently.
    prepend = (HERE / "critical_prepend.md").read_text().rstrip()
    if PREPEND_MARKER not in app_config["instruction"]:
        app_config["instruction"] = prepend + "\n\n" + app_config["instruction"].lstrip()

    # 1b) append the full Continuum method to the Builder's instruction (idempotent)
    method = (HERE / "method.md").read_text().rstrip()
    if MARKER not in app_config["instruction"]:
        app_config["instruction"] = app_config["instruction"].rstrip() + "\n\n" + method + "\n"

    # 2) append the MCP tools (idempotent, keyed on tool_name_prefix)
    tools = app_config.setdefault("tools", [])
    have = {t.get("tool_name_prefix") for t in tools if isinstance(t, dict)}
    added = []
    for tool in yaml.safe_load((HERE / "mcp_tools.yaml").read_text()) or []:
        prefix = tool.get("tool_name_prefix")
        if prefix not in have:
            tools.append(tool)
            added.append(prefix)

    out.write_text(
        yaml.safe_dump(doc, sort_keys=False, width=100_000, allow_unicode=True, default_flow_style=False)
    )
    print(f"wrote {out} ({out.stat().st_size} bytes); tools added: {added or 'none (already present)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

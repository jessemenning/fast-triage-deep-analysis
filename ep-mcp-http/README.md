# ep-mcp-http — Event Portal MCP over streamable HTTP

Solace Agent Mesh connectors only reach **remote** MCP servers (`type: mcp`, `subtype: remote`,
streamable-http or SSE). The published Event Portal Designer MCP
(`solace-event-portal-designer-mcp`, PyPI) runs over stdio. This wrapper rebuilds the same
FastMCP server — identical OpenAPI spec, route whitelist and tool customisation — and serves it
on `http://localhost:8092/mcp` behind a static bearer token.

Tools exposed (35, same as upstream): application domains, applications (+versions, AsyncAPI
export), events (+versions), schemas (+versions) — `get*/create*/update*/delete*`.

## Run

```bash
cp .env.example .env            # set SOLACE_API_TOKEN
uv run ep-mcp-http              # or: uv run python -m ep_mcp_http.server
```

Smoke test:

```bash
curl -s -X POST localhost:8092/mcp \
  -H "Authorization: Bearer $EP_MCP_TOKEN" \
  -H 'Accept: application/json, text/event-stream' -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
```

SAM connector: see `../sam/connectors/event-portal-mcp.yaml`.

## Notes

- The Event Portal API token is used server-side; the agent never sees it.
- `EP_MCP_TOKEN` unset ⇒ no auth (local dev only).
- Known Event Portal limitations (logo/icon fields inert, no Modelled Event Mesh creation via API)
  are documented in `~/.claude/rules/event-portal.md`.

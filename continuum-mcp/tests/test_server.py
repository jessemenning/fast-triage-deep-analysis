"""ASGI-level tests of server.py: auth middleware, /mcp path handling, tool registration. No network."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

import httpx

from continuum_mcp import server
from tests.conftest import make_settings

EXPECTED_TOOLS = {
    "list_source_streams", "list_data_products", "get_data_product", "search_catalog", "preview_query",
    "create_data_product", "create_trigger", "delete_trigger", "delete_data_product", "get_status",
    "sync_event_portal",
}
HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
AUTH = {**HEADERS, "Authorization": "Bearer s3cret"}


@contextlib.asynccontextmanager
async def running_app(catalog_dir) -> AsyncIterator[httpx.AsyncClient]:
    """Build the ASGI app and drive its lifespan inside the test's own task (anyio task groups need that)."""
    settings = make_settings(catalog_dir, CONTINUUM_MCP_TOKEN="s3cret")
    asgi = server.build_app(settings)
    starlette = asgi.starlette
    async with starlette.router.lifespan_context(starlette):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=asgi), base_url="http://mcp") as client:
            yield client


def test_eleven_tools_registered():
    tools = server.mcp._tool_manager.list_tools()
    assert {t.name for t in tools} == EXPECTED_TOOLS
    for t in tools:
        text = (t.description or "").lower()
        assert "pubsub+" not in text and "feldera" not in text, t.name


async def test_bearer_auth_enforced(catalog_dir):
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    async with running_app(catalog_dir) as client:
        assert (await client.post("/mcp", json=body, headers=HEADERS)).status_code == 401
        assert (await client.post("/mcp", json=body, headers={**HEADERS, "Authorization": "Bearer nope"})).status_code == 401
        health = await client.get("/healthz")
        assert health.status_code == 200 and health.json()["streams"] == 8


async def test_tools_list_with_and_without_trailing_slash(catalog_dir):
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    async with running_app(catalog_dir) as client:
        for path in ("/mcp", "/mcp/"):
            resp = await client.post(path, json=body, headers=AUTH)
            assert resp.status_code == 200, (path, resp.status_code, resp.text[:200])
            tools = resp.json()["result"]["tools"]
            assert {t["name"] for t in tools} == EXPECTED_TOOLS
        create = next(t for t in tools if t["name"] == "create_data_product")
        assert set(create["inputSchema"]["required"]) == {"name", "description", "sql", "source_streams", "key_columns"}
        assert "TriggerSpec" in create["inputSchema"].get("$defs", {})


async def test_tool_call_returns_structured_result(catalog_dir):
    async with running_app(catalog_dir) as client:
        resp = await client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                  "params": {"name": "list_source_streams", "arguments": {"kind": "raw"}}},
            headers=AUTH,
        )
        assert resp.status_code == 200, resp.text
        result = resp.json()["result"]
        assert result.get("isError") is not True
        streams = result["structuredContent"]["result"]
        assert {"airport_rate", "weather", "active_restrictions"} <= {s["name"] for s in streams}
        assert all(s["kind"] == "raw" for s in streams)


async def test_tool_errors_are_structured_not_exceptions(catalog_dir):
    async with running_app(catalog_dir) as client:
        resp = await client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                  "params": {"name": "preview_query", "arguments": {"sql": "DROP TABLE x"}}},
            headers=AUTH,
        )
        assert resp.status_code == 200
        result = resp.json()["result"]
        assert result.get("isError") is not True
        assert "single SELECT" in result["structuredContent"]["error"]

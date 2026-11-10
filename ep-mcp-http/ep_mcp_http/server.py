"""Serve the Solace Event Portal Designer MCP server over streamable HTTP.

The PyPI package ``solace-event-portal-designer-mcp`` builds a FastMCP server
from the Event Portal Designer OpenAPI spec and runs it over stdio. Solace
Agent Mesh connectors only speak to *remote* MCP servers (streamable-http or
SSE), so this module rebuilds the identical server -- same spec, route maps
and component customisation -- and serves it over HTTP behind a static bearer
token.

Environment:
    SOLACE_API_TOKEN      Event Portal (Solace Cloud) API token  [required]
    SOLACE_API_BASE_URL   default https://api.solace.cloud
    EP_MCP_TOKEN          bearer token clients must present; unset = no auth
    EP_MCP_HOST / EP_MCP_PORT   default 0.0.0.0 / 8092
"""

from __future__ import annotations

import json
import logging
import os
import sys

import httpx
import uvicorn
from fastmcp import FastMCP
from fastmcp.client.auth import BearerAuth
from fastmcp.server.openapi import MCPType, RouteMap
from starlette.responses import JSONResponse

import solace_event_portal_designer_mcp.server as ep_server
from solace_event_portal_designer_mcp import __version__ as EP_MCP_VERSION

logger = logging.getLogger("ep-mcp-http")

# Same whitelist the upstream package uses: domains, applications (+versions,
# AsyncAPI export), events (+versions), schemas (+versions). Everything else
# in the 85-path spec is excluded.
ROUTE_MAPS = [
    RouteMap(pattern=r"^/api/v2/architecture/applicationDomains(/\{id\})?$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/applications(/\{id\})?$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/applicationVersions(/\{versionId\})?$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/applicationVersions/\{applicationVersionId\}/asyncApi$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/events(/\{id\})?$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/eventVersions(/\{id\})?$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/schemaVersions(/\{id\})?$", mcp_type=MCPType.TOOL),
    RouteMap(pattern=r"^/api/v2/architecture/schemas(/\{id\})?$", mcp_type=MCPType.TOOL),
    RouteMap(mcp_type=MCPType.EXCLUDE),
]


def _load_spec() -> dict:
    spec_path = os.path.join(os.path.dirname(ep_server.__file__), "data", "ep-designer.json")
    with open(spec_path) as f:
        spec = json.load(f)
    # Break the InvalidStateReference self-reference cycle, exactly as upstream does.
    props = spec["components"]["schemas"]["InvalidStateReference"]["properties"]
    props["inboundInvalidStateReferences"]["items"] = {"type": "object"}
    props["outboundInvalidStateReferences"]["items"] = {"type": "object"}
    return spec


def build_server() -> FastMCP:
    token = os.getenv("SOLACE_API_TOKEN")
    if not token:
        # Start anyway so the server is reachable and SAM's connector discovery
        # succeeds; Event Portal calls will 401 until a real token is provided.
        logger.warning(
            "SOLACE_API_TOKEN is not set — Event Portal calls will fail with 401. "
            "Set it in .env to enable Event Portal registration/discovery."
        )
        token = "ep-token-not-configured"
    base_url = os.getenv("SOLACE_API_BASE_URL", "https://api.solace.cloud")

    client = httpx.AsyncClient(base_url=base_url, auth=BearerAuth(token=token))
    client.headers.update(
        {
            "User-Agent": f"solace/event-portal-designer-mcp/{EP_MCP_VERSION} (+ep-mcp-http)",
            "x-issuer": f"solace/event-portal-designer-mcp/{EP_MCP_VERSION}",
        }
    )
    logger.info("Event Portal API base %s", base_url)

    return FastMCP.from_openapi(
        openapi_spec=_load_spec(),
        client=client,
        name="Solace Event Portal Designer",
        route_maps=ROUTE_MAPS,
        mcp_component_fn=getattr(ep_server, "customize_components", None),
    )


class BearerGate:
    """Minimal ASGI middleware: require ``Authorization: Bearer <token>`` on HTTP.

    Lifespan and non-HTTP scopes pass straight through so FastMCP's own
    session manager still starts. Also accepts ``/mcp/`` for ``/mcp``.
    """

    def __init__(self, app, token: str | None, path: str):
        self.app = app
        self.token = token
        self.path = path

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        if scope.get("path") == self.path + "/":
            scope = dict(scope, path=self.path, raw_path=self.path.encode())
        if self.token:
            headers = {k.lower(): v for k, v in scope.get("headers") or []}
            presented = headers.get(b"authorization", b"").decode(errors="replace")
            if presented != f"Bearer {self.token}":
                response = JSONResponse({"error": "unauthorized"}, status_code=401)
                return await response(scope, receive, send)
        return await self.app(scope, receive, send)


def build_app(path: str = "/mcp"):
    mcp = build_server()
    inner = mcp.http_app(path=path)
    gate_token = os.getenv("EP_MCP_TOKEN") or None
    if not gate_token:
        logger.warning("EP_MCP_TOKEN unset - serving without authentication")
    return BearerGate(inner, gate_token, path)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), stream=sys.stderr,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    host = os.getenv("EP_MCP_HOST", "0.0.0.0")
    port = int(os.getenv("EP_MCP_PORT", "8092"))
    logger.info("Solace Event Portal MCP (v%s) over streamable-http on %s:%d/mcp", EP_MCP_VERSION, host, port)
    uvicorn.run(build_app(), host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()

"""Thin async client for the Continuum engine's REST API (v0).

Only the calls the service needs, with errors normalised to FelderaError.
Endpoint shapes follow the engine's OpenAPI document:
  POST /v0/validate_program         {program_code}  -> {"Success":{program_info}} | {"SqlError":{info}} | {"SystemError":{error}}
  GET  /v0/pipelines[?selector=]    -> [PipelineSelectedInfo]
  GET  /v0/pipelines/{name}[?selector=all|status|status_with_connectors]
  PUT  /v0/pipelines/{name}         PostPutPipeline {name, description, tags, program_code, runtime_config}
  PATCH /v0/pipelines/{name}        PatchPipeline (only while Stopped)
  POST /v0/pipelines/{name}/start | /stop?force=true | /clear    -> 202
  DELETE /v0/pipelines/{name}       (must be fully stopped and cleared)
  GET  /v0/pipelines/{name}/query?sql=...&format=json  -> NDJSON rows
  GET  /v0/pipelines/{name}/stats   -> ControllerStatus {global_metrics, inputs[], outputs[]}
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

import httpx

log = logging.getLogger("continuum-mcp.engine")

TERMINAL_PROGRAM_ERRORS = {"SqlError", "RustError", "SystemError"}


class FelderaError(Exception):
    def __init__(self, status: int, message: str, error_code: str | None = None, details: Any = None):
        super().__init__(f"engine HTTP {status}: {message}" + (f" [{error_code}]" if error_code else ""))
        self.status = status
        self.message = message
        self.error_code = error_code
        self.details = details


class FelderaClient:
    def __init__(self, base_url: str, *, timeout: float = 30.0, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _raise(resp: httpx.Response) -> None:
        if resp.is_success:
            return
        message, code, details = resp.text, None, None
        try:
            body = resp.json()
            if isinstance(body, dict):
                message = body.get("message", message)
                code = body.get("error_code")
                details = body.get("details")
        except ValueError:
            pass
        raise FelderaError(resp.status_code, message, code, details)

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        resp = await self._client.request(method, path, **kwargs)
        self._raise(resp)
        return resp

    # ------------------------------------------------------------------ program
    async def validate_program(self, program_code: str) -> dict[str, Any]:
        resp = await self._request(
            "POST", "/v0/validate_program", json={"program_code": program_code}, timeout=120.0
        )
        return resp.json()

    # ------------------------------------------------------------------ pipelines
    async def list_pipelines(self, selector: str = "status") -> list[dict[str, Any]]:
        resp = await self._request("GET", "/v0/pipelines", params={"selector": selector})
        return resp.json()

    async def get_pipeline(self, name: str, selector: str = "status") -> dict[str, Any] | None:
        resp = await self._client.get(f"/v0/pipelines/{name}", params={"selector": selector})
        if resp.status_code == 404:
            return None
        self._raise(resp)
        return resp.json()

    async def put_pipeline(
        self,
        name: str,
        *,
        program_code: str,
        description: str = "",
        tags: list[str] | None = None,
        workers: int = 2,
        compilation_profile: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "name": name,
            "description": description,
            "tags": tags or [],
            "program_code": program_code,
            "runtime_config": {"workers": workers},
        }
        # Compile profile: "unoptimized" cuts the Rust compile from ~minutes to ~seconds
        # (demo-volume throughput is unaffected). Omitted → the engine's default profile.
        if compilation_profile:
            body["program_config"] = {"profile": compilation_profile}
        resp = await self._request("PUT", f"/v0/pipelines/{name}", json=body)
        return resp.json()

    async def patch_pipeline(self, name: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = await self._request("PATCH", f"/v0/pipelines/{name}", json=body)
        return resp.json()

    async def patch_program(self, name: str, program_code: str) -> dict[str, Any]:
        return await self.patch_pipeline(name, {"program_code": program_code})

    async def start(self, name: str) -> None:
        await self._request("POST", f"/v0/pipelines/{name}/start")

    async def stop_force(self, name: str) -> None:
        await self._request("POST", f"/v0/pipelines/{name}/stop", params={"force": "true"})

    async def clear(self, name: str) -> None:
        await self._request("POST", f"/v0/pipelines/{name}/clear")

    async def delete(self, name: str) -> None:
        resp = await self._client.delete(f"/v0/pipelines/{name}")
        if resp.status_code == 404:
            return
        self._raise(resp)

    # ------------------------------------------------------------------ data
    async def query(self, pipeline: str, sql: str, *, timeout: float = 15.0) -> list[dict[str, Any]]:
        resp = await self._request(
            "GET", f"/v0/pipelines/{pipeline}/query", params={"sql": sql, "format": "json"}, timeout=timeout
        )
        return [json.loads(line) for line in resp.text.splitlines() if line.strip()]

    async def stats(self, pipeline: str) -> dict[str, Any]:
        resp = await self._request("GET", f"/v0/pipelines/{pipeline}/stats")
        return resp.json()

    # ------------------------------------------------------------------ polling
    async def wait_for(
        self,
        name: str,
        predicate: Callable[[dict[str, Any]], bool],
        *,
        timeout: float,
        interval: float,
        on_poll: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        """Poll GET /pipelines/{name} until predicate(info) is true. Raises TimeoutError."""
        deadline = time.monotonic() + timeout
        while True:
            info = await self.get_pipeline(name)
            if info is None:
                raise FelderaError(404, f"pipeline {name!r} disappeared while waiting")
            if on_poll:
                await on_poll(info)
            if predicate(info):
                return info
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"timed out after {timeout:.0f}s waiting on pipeline {name!r} "
                    f"(program_status={info.get('program_status')}, deployment_status={info.get('deployment_status')})"
                )
            await asyncio.sleep(interval)


def program_error_text(info: dict[str, Any]) -> str | None:
    """Extract a readable compile/runtime error from a pipeline info document."""
    err = info.get("program_error") or {}
    if isinstance(err, dict):
        sql = err.get("sql_compilation") or {}
        msgs = [m for m in (sql.get("messages") or []) if not m.get("warning")]
        if msgs:
            return "; ".join(
                f"line {m.get('start_line_number')}: {m.get('message')}" for m in msgs[:5]
            )
        rust = err.get("rust_compilation") or {}
        if rust and rust.get("exit_code") not in (None, 0):
            return f"Rust compilation failed: {str(rust.get('stderr') or '')[-800:]}"
        if err.get("system_error"):
            return str(err["system_error"])
    dep = info.get("deployment_error")
    if isinstance(dep, dict) and dep.get("message"):
        return str(dep["message"])
    return None


def sql_error_messages(validation: dict[str, Any]) -> list[dict[str, Any]]:
    """Messages from a validate_program SqlError response (errors first)."""
    info = (validation.get("SqlError") or {}).get("info") or {}
    msgs = list(info.get("messages") or [])
    return sorted(msgs, key=lambda m: bool(m.get("warning")))

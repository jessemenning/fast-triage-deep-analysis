"""Feldera ad-hoc query client for the FAA arrival-capacity API.

Replaces the Materialize pgwire (asyncpg/SQLAlchemy) path. Feldera exposes an
ad-hoc SQL endpoint:

    GET /v0/pipelines/{pipeline}/query?sql=<SELECT ...>&format=json

The response is newline-delimited JSON — one JSON object per result row. This
client executes read-only SELECTs against the pipeline's materialized views and
returns a list of dicts (already typed by Feldera's JSON serialization).
"""

from __future__ import annotations

import json
import logging

import httpx

from src.config import get_settings

log = logging.getLogger(__name__)


def sql_quote(value: str) -> str:
    """Escape a string for safe inline use in a SQL literal ('' escaping)."""
    return value.replace("'", "''")


class FelderaClient:
    def __init__(self, base_url: str, pipeline: str) -> None:
        self._base = base_url.rstrip("/")
        self._pipeline = pipeline
        self._query_url = f"{self._base}/v0/pipelines/{pipeline}/query"
        self._status_url = f"{self._base}/v0/pipelines/{pipeline}"

    async def query(self, sql: str) -> list[dict]:
        """Run a read-only SELECT and return rows as dicts.

        Keys are lowercased so callers need not care whether Feldera/Calcite
        returns column names upper- or lower-cased.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(self._query_url, params={"sql": sql, "format": "json"})
            resp.raise_for_status()
        rows = []
        for line in resp.text.splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            rows.append({k.lower(): v for k, v in obj.items()})
        return rows

    async def deployment_status(self) -> str:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(self._status_url)
            resp.raise_for_status()
            return resp.json().get("deployment_status", "unknown")


_client: FelderaClient | None = None
_watch_client: FelderaClient | None = None


def get_feldera_client() -> FelderaClient:
    global _client
    if _client is None:
        s = get_settings()
        _client = FelderaClient(s.feldera_url, s.feldera_pipeline)
    return _client


def get_watch_client() -> FelderaClient:
    """Client for the demo-console pipeline (see Settings.watch_pipeline).

    A separate instance rather than a parameter on get_feldera_client(): the
    two pipelines have independent lifecycles, and this one is deliberately
    Stopped until the presenter starts it on stage.
    """
    global _watch_client
    if _watch_client is None:
        s = get_settings()
        _watch_client = FelderaClient(s.feldera_url, s.watch_pipeline)
    return _watch_client

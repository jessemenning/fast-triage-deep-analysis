"""Shared fixtures: a temp copy of the catalog, offline settings, respx-mocked clients."""

from __future__ import annotations

import shutil
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest
import respx

from continuum_mcp.catalog import Catalog
from continuum_mcp.config import Settings
from continuum_mcp.ep_client import EventPortalClient
from continuum_mcp.feldera_client import FelderaClient
from continuum_mcp.semp_client import SempClient
from continuum_mcp.service import ContinuumService
from continuum_mcp.templates import ProgramRenderer

REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_CATALOG = REPO_ROOT / "catalog"
GOLDEN_DIR = Path(__file__).resolve().parent / "golden"

ENGINE = "http://engine"
SEMP = "http://semp"
EP = "https://ep.test/api/v2/architecture"


@pytest.fixture
def catalog_dir(tmp_path: Path) -> Path:
    dst = tmp_path / "catalog"
    shutil.copytree(REPO_CATALOG / "streams", dst / "streams")
    shutil.copytree(REPO_CATALOG / "base", dst / "base")
    (dst / "products").mkdir()
    return dst


def make_settings(catalog_dir: Path, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "CATALOG_DIR": str(catalog_dir),
        "FELDERA_URL": ENGINE,
        "SEMP_URL": SEMP,
        "EP_BASE_URL": EP,
        "SOLACE_API_TOKEN": "ep-token",
        "POLL_INTERVAL_SECS": 0.01,
        "COMPILE_TIMEOUT_SECS": 5,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)  # type: ignore[call-arg]


@pytest.fixture
def settings(catalog_dir: Path) -> Settings:
    return make_settings(catalog_dir)


@pytest.fixture
def catalog(catalog_dir: Path) -> Catalog:
    return Catalog(catalog_dir).load()


@pytest.fixture
def renderer(settings: Settings) -> ProgramRenderer:
    return ProgramRenderer(settings)


@pytest.fixture
def api() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield router


@pytest.fixture
async def service(settings: Settings, catalog: Catalog, api: respx.MockRouter) -> AsyncIterator[ContinuumService]:
    feldera = FelderaClient(settings.feldera_url)
    semp = SempClient(settings.semp_url, "admin", "admin", "default")
    ep = EventPortalClient(settings.ep_base_url, "ep-token")
    svc = ContinuumService(settings, catalog, feldera, semp, ep)
    yield svc
    await svc.shutdown()


# ------------------------------------------------------------------ builders
def field(name: str, sql_type: str, nullable: bool = True, **extra: Any) -> dict[str, Any]:
    ct: dict[str, Any] = {"type": sql_type, "nullable": nullable}
    ct.update(extra)
    return {"name": name, "case_sensitive": False, "columntype": ct, "unused": False}


def relation(name: str, fields: list[dict[str, Any]], materialized: bool = True) -> dict[str, Any]:
    return {"name": name, "case_sensitive": False, "fields": fields, "materialized": materialized}


def validate_success(*outputs: dict[str, Any], inputs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "Success": {
            "program_info": {
                "schema": {"inputs": inputs or [], "outputs": list(outputs)},
                "input_connectors": {},
                "output_connectors": {},
            }
        }
    }


def validate_sql_error(*messages: dict[str, Any]) -> dict[str, Any]:
    return {"SqlError": {"info": {"exit_code": 1, "messages": list(messages)}}}


def pipeline_info(
    name: str,
    program_status: str = "Success",
    deployment_status: str = "Stopped",
    storage_status: str = "Cleared",
    **extra: Any,
) -> dict[str, Any]:
    info = {
        "id": "00000000-0000-0000-0000-000000000001",
        "name": name,
        "description": "",
        "tags": extra.pop("tags", []),
        "program_status": program_status,
        "deployment_status": deployment_status,
        "storage_status": storage_status,
        "program_error": extra.pop("program_error", {"sql_compilation": None, "rust_compilation": None, "system_error": None}),
        "deployment_error": None,
    }
    info.update(extra)
    return info


# The representative product used across the tests: airport rates joined to the latest
# weather, keyed by airport. A raw stream (airport_rate) LEFT JOIN another raw stream
# (weather -> metar_latest), mirroring a "facts LEFT JOIN signals" shape.
HIGH_VALUE_SQL = """SELECT r.airport, r.aar, r.runway_config, r.valid_from, w.flight_category
FROM airport_rate r
LEFT JOIN metar_latest w ON w.station_id = r.airport
WHERE r.aar <= 40"""

HIGH_VALUE_FIELDS = [
    field("airport", "VARCHAR", precision=-1),
    field("aar", "INTEGER"),
    field("runway_config", "VARCHAR", precision=-1),
    field("valid_from", "TIMESTAMP", precision=0),
    field("flight_category", "VARCHAR", precision=-1),
]

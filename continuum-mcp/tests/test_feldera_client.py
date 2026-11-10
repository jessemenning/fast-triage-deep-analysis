from __future__ import annotations

import json

import httpx
import pytest
import respx

from continuum_mcp.feldera_client import (
    FelderaClient,
    FelderaError,
    program_error_text,
    sql_error_messages,
)
from tests.conftest import (
    ENGINE,
    field,
    pipeline_info,
    relation,
    validate_sql_error,
    validate_success,
)


@pytest.fixture
async def client():
    c = FelderaClient(ENGINE)
    yield c
    await c.aclose()


async def test_validate_program_success(client: FelderaClient, api: respx.MockRouter):
    route = api.post(f"{ENGINE}/v0/validate_program").mock(
        return_value=httpx.Response(200, json=validate_success(relation("v", [field("a", "INTEGER")])))
    )
    result = await client.validate_program("CREATE VIEW v AS SELECT 1 AS a")
    assert route.called
    assert json.loads(route.calls.last.request.content) == {"program_code": "CREATE VIEW v AS SELECT 1 AS a"}
    assert result["Success"]["program_info"]["schema"]["outputs"][0]["name"] == "v"


async def test_validate_program_sql_error(client: FelderaClient, api: respx.MockRouter):
    warning = {"start_line_number": 1, "start_column": 1, "end_line_number": 1, "end_column": 2,
               "warning": True, "error_type": "w", "message": "unused"}
    error = {"start_line_number": 3, "start_column": 5, "end_line_number": 3, "end_column": 9,
             "warning": False, "error_type": "Error parsing SQL", "message": "Column 'amout' not found"}
    api.post(f"{ENGINE}/v0/validate_program").mock(return_value=httpx.Response(200, json=validate_sql_error(warning, error)))
    result = await client.validate_program("bad")
    msgs = sql_error_messages(result)
    assert [m["warning"] for m in msgs] == [False, True]
    assert msgs[0]["message"] == "Column 'amout' not found"


async def test_put_patch_start_stop_clear_delete(client: FelderaClient, api: respx.MockRouter):
    put = api.put(f"{ENGINE}/v0/pipelines/cp-x").mock(return_value=httpx.Response(201, json=pipeline_info("cp-x", "Pending")))
    patch = api.patch(f"{ENGINE}/v0/pipelines/cp-x").mock(return_value=httpx.Response(200, json=pipeline_info("cp-x", "Pending")))
    start = api.post(f"{ENGINE}/v0/pipelines/cp-x/start").mock(return_value=httpx.Response(202))
    stop = api.post(f"{ENGINE}/v0/pipelines/cp-x/stop").mock(return_value=httpx.Response(202))
    clear = api.post(f"{ENGINE}/v0/pipelines/cp-x/clear").mock(return_value=httpx.Response(202))
    delete = api.delete(f"{ENGINE}/v0/pipelines/cp-x").mock(return_value=httpx.Response(200))

    await client.put_pipeline("cp-x", program_code="SQL", description="d", tags=["continuum:data-product"], workers=3)
    body = json.loads(put.calls.last.request.content)
    assert body == {"name": "cp-x", "description": "d", "tags": ["continuum:data-product"], "program_code": "SQL",
                    "runtime_config": {"workers": 3}}

    await client.patch_program("cp-x", "SQL2")
    assert json.loads(patch.calls.last.request.content) == {"program_code": "SQL2"}

    await client.start("cp-x")
    await client.stop_force("cp-x")
    assert stop.calls.last.request.url.params["force"] == "true"
    await client.clear("cp-x")
    await client.delete("cp-x")
    assert start.called and clear.called and delete.called


async def test_get_pipeline_404_is_none_and_errors_are_typed(client: FelderaClient, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines/missing").mock(
        return_value=httpx.Response(404, json={"message": "Unknown pipeline name 'missing'", "error_code": "UnknownPipelineName", "details": None})
    )
    assert await client.get_pipeline("missing") is None
    api.patch(f"{ENGINE}/v0/pipelines/running").mock(
        return_value=httpx.Response(400, json={"message": "Pipeline can only be updated while stopped.", "error_code": "UpdateRestrictedToStopped", "details": None})
    )
    with pytest.raises(FelderaError) as exc:
        await client.patch_program("running", "x")
    assert exc.value.status == 400 and exc.value.error_code == "UpdateRestrictedToStopped"
    assert "updated while stopped" in exc.value.message


async def test_delete_missing_is_noop(client: FelderaClient, api: respx.MockRouter):
    api.delete(f"{ENGINE}/v0/pipelines/gone").mock(return_value=httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}))
    await client.delete("gone")


async def test_query_parses_ndjson(client: FelderaClient, api: respx.MockRouter):
    route = api.get(f"{ENGINE}/v0/pipelines/p/query").mock(
        return_value=httpx.Response(200, text='{"a": 1, "b": "x"}\n{"a": 2, "b": "y"}\n\n')
    )
    rows = await client.query("p", "SELECT * FROM v", timeout=5)
    assert rows == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    params = route.calls.last.request.url.params
    assert params["sql"] == "SELECT * FROM v" and params["format"] == "json"


async def test_wait_for_times_out(client: FelderaClient, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines/slow").mock(return_value=httpx.Response(200, json=pipeline_info("slow", "CompilingSql")))
    with pytest.raises(TimeoutError, match="program_status=CompilingSql"):
        await client.wait_for("slow", lambda i: i["program_status"] == "Success", timeout=0.05, interval=0.01)


def test_program_error_text():
    info = pipeline_info("p", "SqlError", program_error={
        "sql_compilation": {"exit_code": 1, "messages": [
            {"start_line_number": 4, "warning": True, "message": "w"},
            {"start_line_number": 7, "warning": False, "message": "Object 'nope' not found"},
        ]},
        "rust_compilation": None, "system_error": None,
    })
    assert program_error_text(info) == "line 7: Object 'nope' not found"
    assert program_error_text(pipeline_info("p", program_error={"system_error": "disk full"})) == "disk full"
    assert program_error_text(pipeline_info("p")) is None

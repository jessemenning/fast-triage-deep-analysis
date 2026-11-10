"""End-to-end service tests with every HTTP call mocked (respx)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable

import httpx
import pytest
import respx
import yaml

from continuum_mcp.catalog import Catalog
from continuum_mcp.models import TriggerSpec
from continuum_mcp.service import ContinuumError, ContinuumService
from tests.conftest import (
    ENGINE,
    EP,
    HIGH_VALUE_FIELDS,
    HIGH_VALUE_SQL,
    SEMP,
    field,
    pipeline_info,
    relation,
    validate_sql_error,
    validate_success,
)

CFG = f"{SEMP}/SEMP/v2/config/msgVpns/default"
MON = f"{SEMP}/SEMP/v2/monitor/msgVpns/default"
PIPE = "cp-rate-weather-watch"
SLUG = "rate-weather-watch"


def sequence(*responses: httpx.Response) -> Callable[[httpx.Request], httpx.Response]:
    """Return responses in order; repeat the last one forever."""
    it = iter(responses)
    last = responses[-1]

    def _side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal last
        try:
            last = next(it)
        except StopIteration:
            pass
        return last

    return _side_effect


def mock_engine_validate(api: respx.MockRouter, view: str = "rate_weather_watch") -> respx.Route:
    return api.post(f"{ENGINE}/v0/validate_program").mock(
        return_value=httpx.Response(200, json=validate_success(relation(view, HIGH_VALUE_FIELDS)))
    )


def mock_semp_create(api: respx.MockRouter) -> None:
    api.post(f"{CFG}/queues").mock(return_value=httpx.Response(200, json={"meta": {"responseCode": 200}}))
    api.post(url__regex=rf"{CFG}/queues/[^/]+/subscriptions").mock(
        return_value=httpx.Response(200, json={"meta": {"responseCode": 200}})
    )


def mock_ep_empty(api: respx.MockRouter) -> None:
    """Event Portal that finds nothing and accepts every create."""
    counter = {"n": 0}

    def _post(request: httpx.Request) -> httpx.Response:
        counter["n"] += 1
        body = json.loads(request.content)
        body["id"] = f"ep-{counter['n']}"
        return httpx.Response(201, json={"data": body})

    api.get(url__regex=rf"{EP}/.*").mock(return_value=httpx.Response(200, json={"data": []}))
    api.post(url__regex=rf"{EP}/.*").mock(side_effect=_post)


async def create_high_value(service: ContinuumService, **overrides):
    kwargs = dict(
        name="Rate Weather Watch",
        description="Airports with an AAR of 40 or less, with the latest weather category",
        sql=HIGH_VALUE_SQL,
        source_streams=["airport_rate", "weather"],
        key_columns=["airport"],
        event_time_column="valid_from",
        owner="traffic-ops",
        tags=["capacity"],
        triggers=[
            TriggerSpec(
                name="low-vis",
                description="Low published rate while weather is IFR",
                condition="flight_category = 'IFR'",
                output_topic_template=f"fl/cp/{SLUG}/low-vis/{{airport}}",
                recency_window="5 minutes",
            )
        ],
    )
    kwargs.update(overrides)
    return await service.create_data_product(**kwargs)


# ---------------------------------------------------------------------- create
async def test_create_flow_call_order_and_state_transitions(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    validate = mock_engine_validate(api)
    get_pipeline = api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(
        side_effect=sequence(
            httpx.Response(404, json={"message": "unknown", "error_code": "UnknownPipelineName", "details": None}),
            httpx.Response(200, json=pipeline_info(PIPE, "CompilingSql", "Stopped")),
            httpx.Response(200, json=pipeline_info(PIPE, "CompilingRust", "Stopped")),
            httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),
            httpx.Response(200, json=pipeline_info(PIPE, "Success", "Provisioning")),
            httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")),
        )
    )
    mock_semp_create(api)
    put = api.put(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(201, json=pipeline_info(PIPE, "Pending")))
    start = api.post(f"{ENGINE}/v0/pipelines/{PIPE}/start").mock(return_value=httpx.Response(202))
    mock_ep_empty(api)

    result = await create_high_value(service)

    assert result["status"] == "compiling" and result["slug"] == SLUG and result["pipeline"] == PIPE
    assert result["queues"] == [f"fl-cp-{SLUG}-airport_rate", f"fl-cp-{SLUG}-weather"]
    assert result["live_topic"] == f"fl/cp/{SLUG}/live/{{airport}}"
    assert result["subscribe_pattern"] == f"fl/cp/{SLUG}/live/>"
    assert [c["name"] for c in result["schema"]] == ["airport", "aar", "runway_config", "valid_from", "flight_category"]
    assert result["schema"][1]["type"] == "INTEGER"
    assert result["next"] == "poll get_data_product until status=running"
    assert result["estimated_ready_secs"] >= 120

    # Call order: validate (twice: schema pass, then definitive pass) -> SEMP -> PUT.
    order = [(c.request.method, c.request.url.host, c.request.url.path) for c in api.calls
             if c.request.url.host in ("engine", "semp")]
    validate_idx = [i for i, c in enumerate(order) if c[2] == "/v0/validate_program"]
    semp_idx = [i for i, c in enumerate(order) if c[1] == "semp" and c[0] == "POST"]
    put_idx = [i for i, c in enumerate(order) if c[0] == "PUT"]
    assert validate.call_count == 2 and len(semp_idx) == 4 and len(put_idx) == 1
    assert max(validate_idx) < min(semp_idx) < max(semp_idx) < put_idx[0]

    # The definitive program has sinks + trigger; the PUT carries tags and workers.
    put_body = json.loads(put.calls.last.request.content)
    assert put_body["tags"] == ["continuum:data-product", f"continuum:slug={SLUG}", "continuum:v1"]
    assert put_body["runtime_config"] == {"workers": 2}
    assert "trg_rate_weather_watch_low_vis" in put_body["program_code"]
    assert "rate_weather_watch_live_sink" in put_body["program_code"]
    first_validate_program = json.loads(validate.calls[0].request.content)["program_code"]
    assert "solace_output" not in first_validate_program

    # Manifest persisted as compiling, then the background deployer drives it to running.
    manifest_path = catalog.product_dir(SLUG) / "manifest.yaml"
    assert yaml.safe_load(manifest_path.read_text())["status"] == "compiling"
    assert (catalog.product_dir(SLUG) / "program.sql").read_text() == put_body["program_code"]
    await service.deployer.wait_idle(SLUG)
    assert start.call_count == 1
    assert get_pipeline.call_count >= 6
    saved = yaml.safe_load(manifest_path.read_text())
    assert saved["status"] == "running" and saved["error"] is None
    assert [h["action"] for h in saved["history"]][:1] == ["created"]
    assert saved["history"][-1]["action"] == "running"

    # Trigger catalog representation.
    trg = saved["triggers"][0]
    assert trg["view_name"] == "trg_rate_weather_watch_low_vis"
    assert trg["subscribe_pattern"] == f"fl/cp/{SLUG}/low-vis/>"
    assert trg["payload_columns"] == ["airport", "aar", "runway_config", "valid_from", "flight_category"]
    envelope = trg["payload_json_schema"]
    assert envelope["required"] == ["insert"] and envelope["title"] == "RateWeatherWatchLowVisTriggerEnvelope"
    row = envelope["properties"]["insert"]
    assert row["properties"]["airport"] == {"type": "string", "description": "topic placeholder (stringified, 'unknown' when NULL)"}
    assert row["properties"]["aar"]["type"] == ["integer", "null"]
    rule = yaml.safe_load(trg["sam_event_rule_snippet"])[0]
    assert rule["subscriptions"] == [{"topic": f"fl/cp/{SLUG}/low-vis/>", "qos": 0}]
    assert rule["inputExpression"] == "input.payload:insert"
    assert rule["forwardContext"] == {"airport": "input.payload:insert.airport"}
    assert rule["messageFormat"] == "json" and rule["targetWorkflowName"].startswith("TODO")

    # Event Portal registration ran in the background and recorded ids.
    for _ in range(50):
        if catalog.products[SLUG].event_portal.status == "registered":
            break
        await asyncio.sleep(0.01)
    ep_state = catalog.products[SLUG].event_portal
    assert ep_state.status == "registered"
    assert set(ep_state.event_version_ids) == {"live", "low-vis"}
    assert ep_state.application_version == "1.0.0"


async def test_create_reports_compile_errors_remapped_to_user_sql(service: ContinuumService, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}))
    from continuum_mcp.templates import RenderSpec

    rendered = service.renderer.render_program(
        RenderSpec(name="Rate Weather Watch", slug=SLUG, description="d", sql=HIGH_VALUE_SQL,
                   streams=[service.catalog.stream("airport_rate"), service.catalog.stream("weather")],
                   live_topic="x/{airport}", include_sinks=False)
    )
    bad_line = rendered.user_sql_start_line + 3  # the WHERE line of the user's SELECT
    api.post(f"{ENGINE}/v0/validate_program").mock(
        return_value=httpx.Response(200, json=validate_sql_error(
            {"start_line_number": bad_line, "start_column": 9, "end_line_number": bad_line, "end_column": 14,
             "warning": False, "error_type": "Error in SQL", "message": "Column 'aarr' not found in table 'r'"}
        ))
    )
    with pytest.raises(ContinuumError) as exc:
        await create_high_value(service, sql=HIGH_VALUE_SQL.replace("r.aar <= 40", "r.aarr <= 40"))
    assert "your SELECT, line 4, column 9" in exc.value.message
    assert exc.value.details["messages"][0]["where"] == "your SELECT, line 4, column 9"
    assert not any(c.request.method == "PUT" for c in api.calls)
    assert not service.catalog.products


async def test_create_rejects_bad_inputs_before_any_http(service: ContinuumService, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}))

    with pytest.raises(ContinuumError, match="single SELECT"):
        await create_high_value(service, sql="DROP TABLE x")
    with pytest.raises(ContinuumError, match="read-only.*DELETE"):
        await create_high_value(service, sql="SELECT * FROM airport_rate WHERE 1 = 1 DELETE")
    with pytest.raises(ContinuumError, match="not provided by the declared source_streams") as exc:
        await create_high_value(service, source_streams=["airport_rate"])
    assert exc.value.details["missing"]["metar_latest"] == "add stream 'weather' to source_streams"
    with pytest.raises(ContinuumError, match="not ingestable"):
        await create_high_value(service, source_streams=["airport_rate", "weather", "flight"])
    with pytest.raises(ContinuumError, match="invalid topic template"):
        await create_high_value(service, output_topic_template="fl/cp/rate-weather-watch/live/static")
    with pytest.raises(ContinuumError, match="unknown source stream"):
        await create_high_value(service, source_streams=["nope"])
    with pytest.raises(ContinuumError, match="recency_window but the product has no event_time_column"):
        await create_high_value(service, event_time_column=None)
    assert not any(c.request.url.path == "/v0/validate_program" for c in api.calls)


async def test_create_rejects_key_and_placeholder_not_in_output(service: ContinuumService, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}))
    mock_engine_validate(api)
    with pytest.raises(ContinuumError, match="key_columns \\['nope'\\] are not output columns"):
        await create_high_value(service, key_columns=["nope"], output_topic_template=f"fl/cp/{SLUG}/live/{{airport}}")
    with pytest.raises(ContinuumError, match="invalid topic template") as exc:
        await create_high_value(service, output_topic_template=f"fl/cp/{SLUG}/live/{{gufi}}")
    assert any("placeholders ['gufi'] are not output columns" in e for e in exc.value.details)
    with pytest.raises(ContinuumError, match="topic placeholders \\['valid_from'\\] must be among payload_columns"):
        await create_high_value(service, triggers=[TriggerSpec(
            name="late", condition="TRUE", output_topic_template=f"fl/cp/{SLUG}/late/{{valid_from}}", payload_columns=["airport"])])
    with pytest.raises(ContinuumError, match="trigger name 'Bad Name' must match"):
        await create_high_value(service, triggers=[TriggerSpec(
            name="Bad Name", condition="TRUE", output_topic_template=f"fl/cp/{SLUG}/x/{{airport}}")])


async def test_create_refuses_duplicate_pipeline(service: ContinuumService, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(200, json=pipeline_info(PIPE)))
    with pytest.raises(ContinuumError, match="already exists in the Continuum engine"):
        await create_high_value(service)


async def test_compile_failure_marks_failed_with_engine_message(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    mock_engine_validate(api)
    mock_semp_create(api)
    mock_ep_empty(api)
    api.put(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(201, json=pipeline_info(PIPE, "Pending")))
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}),
        httpx.Response(200, json=pipeline_info(PIPE, "RustError", "Stopped", program_error={
            "sql_compilation": None, "rust_compilation": {"exit_code": 101, "stdout": "", "stderr": "error[E0308]: mismatched types"}, "system_error": None})),
    ))
    await create_high_value(service, register_in_event_portal=False)
    await service.deployer.wait_idle(SLUG)
    m = catalog.products[SLUG]
    assert m.status == "failed" and "mismatched types" in (m.error or "")
    assert m.event_portal.status == "skipped"


# ---------------------------------------------------------------------- read side
async def _seed_running_product(service: ContinuumService, api: respx.MockRouter) -> None:
    mock_engine_validate(api)
    mock_semp_create(api)
    api.put(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(201, json=pipeline_info(PIPE, "Pending")))
    api.post(f"{ENGINE}/v0/pipelines/{PIPE}/start").mock(return_value=httpx.Response(202))
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running", program_info={
            "schema": {"inputs": [], "outputs": [relation("rate_weather_watch", HIGH_VALUE_FIELDS + [field("extra", "INTEGER", nullable=False)])]}})),
    ))
    await create_high_value(service, register_in_event_portal=False)
    await service.deployer.wait_idle(SLUG)
    assert service.catalog.products[SLUG].status == "running"


async def test_get_data_product_merges_live_schema_and_consume_block(service: ContinuumService, api: respx.MockRouter):
    await _seed_running_product(service, api)
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}/query").mock(return_value=httpx.Response(200, text='{"airport": "KEWR", "aar": 40}\n'))
    out = await service.get_data_product("Rate Weather Watch", sample_rows=3)
    assert out["status"] == "running" and out["schema_source"] == "live"
    assert [c["name"] for c in out["schema"]][-1] == "extra"
    assert out["sample_rows"] == [{"airport": "KEWR", "aar": 40}]
    assert out["lineage"] == {"source_streams": ["airport_rate", "weather"],
                              "relations": ["airport_rate", "metar_latest"], "base_pipeline": "arrival-capacity"}
    consume = out["consume"]
    assert consume["topic"] == f"fl/cp/{SLUG}/live/{{airport}}" and consume["subscribe_pattern"] == f"fl/cp/{SLUG}/live/>"
    assert consume["wire_format"] == "insert_delete_envelope"
    row = consume["payload_json_schema"]["properties"]["insert"]
    assert row["properties"]["airport"] == {"type": "string", "description": "SQL type VARCHAR"}
    assert row["required"] == ["airport", "extra"]
    assert "inputExpression: input.payload:insert" in consume["sam_event_rule_snippet"]
    assert "airport: input.payload:insert.airport" in consume["sam_event_rule_snippet"]
    assert out["engine_status"]["deployment_status"] == "Running"
    query_sql = api.calls.last.request.url.params["sql"]
    assert query_sql == "SELECT * FROM rate_weather_watch LIMIT 3"


async def test_get_data_product_unknown_and_base(service: ContinuumService, api: respx.MockRouter):
    with pytest.raises(ContinuumError, match="unknown data product"):
        await service.get_data_product("nope")
    api.get(f"{ENGINE}/v0/pipelines/arrival-capacity").mock(return_value=httpx.Response(200, json=pipeline_info(
        "arrival-capacity", "Success", "Running", program_info={"schema": {"inputs": [], "outputs": [relation("capacity_assessment", [field("airport", "VARCHAR")])]}})))
    api.get(f"{ENGINE}/v0/pipelines/arrival-capacity/query").mock(return_value=httpx.Response(200, text='{"airport": "KEWR"}\n'))
    out = await service.get_data_product("capacity_assessment", sample_rows=1)
    assert out["managed"] is False and out["status"] == "running"
    assert out["schema"] == [{"name": "airport", "type": "VARCHAR", "nullable": True}]
    assert out["consume"]["subscribe_pattern"] == "fl/faa/arrival-capacity/demand/>"
    assert out["lineage"]["source_streams"] == ["capacity_assessment"]


async def test_list_data_products_merges_manifests_base_and_orphans(service: ContinuumService, api: respx.MockRouter):
    await _seed_running_product(service, api)
    api.get(f"{ENGINE}/v0/pipelines").mock(return_value=httpx.Response(200, json=[
        pipeline_info(PIPE, "Success", "Running"),
        pipeline_info("arrival-capacity", "Success", "Running"),
        pipeline_info("cp-orphan", "Success", "Stopped", tags=["continuum:data-product", "continuum:slug=orphan"]),
        pipeline_info("unrelated-pipeline", "Success", "Stopped"),
    ]))
    items = await service.list_data_products()
    by_name = {i["name"]: i for i in items}
    assert by_name["Rate Weather Watch"]["status"] == "running" and by_name["Rate Weather Watch"]["trigger_names"] == ["low-vis"]
    assert by_name["orphan"]["orphan"] is True and by_name["orphan"]["status"] == "stopped"
    assert by_name["capacity_assessment"]["managed"] is False and by_name["capacity_assessment"]["status"] == "running"
    assert "unrelated-pipeline" not in by_name
    only_managed = await service.list_data_products(include_base=False)
    assert {i["name"] for i in only_managed} == {"Rate Weather Watch", "orphan"}
    assert [i["name"] for i in await service.list_data_products(tag="capacity", include_base=False)] == ["Rate Weather Watch"]
    assert [i["name"] for i in await service.list_data_products(status="stopped", include_base=False)] == ["orphan"]


async def test_list_data_products_survives_engine_outage(service: ContinuumService, api: respx.MockRouter):
    api.get(f"{ENGINE}/v0/pipelines").mock(side_effect=httpx.ConnectError("down"))
    items = await service.list_data_products()
    assert items and all(i["status"] == "unknown" for i in items)


# ---------------------------------------------------------------------- preview
async def test_preview_query_routes_to_base_pipeline_and_wraps(service: ContinuumService, api: respx.MockRouter):
    route = api.get(f"{ENGINE}/v0/pipelines/arrival-capacity/query").mock(
        return_value=httpx.Response(200, text="\n".join(json.dumps({"airport": f"A{i}", "demand": i}) for i in range(3)))
    )
    out = await service.preview_query("SELECT airport, demand FROM capacity_assessment t JOIN flight c ON c.arr_airport = t.airport", limit=2)
    assert out == {"pipeline_used": "arrival-capacity", "columns": ["airport", "demand"],
                   "rows": [{"airport": "A0", "demand": 0}, {"airport": "A1", "demand": 1}], "truncated": True, "row_count": 2}
    sql = route.calls.last.request.url.params["sql"]
    assert sql.startswith("SELECT * FROM (\nSELECT airport") and sql.endswith(") AS q LIMIT 3")


async def test_preview_query_on_derived_relation_hits_base_pipeline(service: ContinuumService, api: respx.MockRouter):
    route = api.get(f"{ENGINE}/v0/pipelines/arrival-capacity/query").mock(return_value=httpx.Response(200, text=""))
    out = await service.preview_query("SELECT * FROM capacity_assessment WHERE exceeded")
    assert out["rows"] == [] and out["columns"] == [] and out["truncated"] is False
    sql = route.calls.last.request.url.params["sql"]
    assert sql.startswith("SELECT * FROM (") and "WITH" not in sql


async def test_preview_query_prepends_cte_for_synthesized_stream(service: ContinuumService, api: respx.MockRouter, catalog_dir):
    (catalog_dir / "streams" / "airport_summary.yaml").write_text(
        "name: airport_summary\nkind: derived\ndescription: test\nrelation: airport_summary\n"
        "topic_subscriptions: [x/>]\ntopic_pattern: x/{airport}\n"
        "columns: [{name: airport, type: VARCHAR}, {name: n, type: BIGINT}]\nkey_columns: [airport]\n"
        "preview_cte: |\n  airport_summary AS (SELECT arr_airport AS airport, COUNT(*) AS n FROM flight GROUP BY arr_airport)\n"
    )
    service.catalog.load()
    route = api.get(f"{ENGINE}/v0/pipelines/arrival-capacity/query").mock(return_value=httpx.Response(200, text=""))
    await service.preview_query("SELECT * FROM airport_summary WHERE n > 1")
    sql = route.calls.last.request.url.params["sql"]
    assert sql.startswith("WITH airport_summary AS (SELECT arr_airport AS airport, COUNT(*) AS n FROM flight GROUP BY arr_airport)")
    assert "SELECT * FROM (\nSELECT * FROM airport_summary WHERE n > 1\n) AS q LIMIT 21" in sql


async def test_preview_query_errors(service: ContinuumService, api: respx.MockRouter):
    with pytest.raises(ContinuumError, match="unknown relations \\['nope'\\]"):
        await service.preview_query("SELECT * FROM nope")
    # Force two relations onto different base pipelines to exercise the cross-pipeline guard.
    service.catalog.streams["weather"].base_pipeline = "weather-pipeline"
    with pytest.raises(ContinuumError, match="different pipelines") as exc:
        await service.preview_query("SELECT airport FROM capacity_assessment t JOIN metar_latest w ON w.station_id = t.airport")
    assert exc.value.details == {"pipelines": ["arrival-capacity", "weather-pipeline"]}
    with pytest.raises(ContinuumError, match="single SELECT"):
        await service.preview_query("DELETE FROM capacity_assessment")
    api.get(f"{ENGINE}/v0/pipelines/weather-pipeline/query").mock(
        return_value=httpx.Response(400, json={"message": "Pipeline is not running", "error_code": "x", "details": None}))
    with pytest.raises(ContinuumError, match="is pipeline 'weather-pipeline' running"):
        await service.preview_query("SELECT * FROM metar_latest")


# ---------------------------------------------------------------------- triggers
async def test_create_trigger_stops_patches_and_redeploys(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    await _seed_running_product(service, api)
    prev_program = catalog.read_program(SLUG)
    stop = api.post(f"{ENGINE}/v0/pipelines/{PIPE}/stop").mock(return_value=httpx.Response(202))
    patch = api.patch(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(200, json=pipeline_info(PIPE, "Pending")))
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopping")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),
        httpx.Response(200, json=pipeline_info(PIPE, "CompilingSql", "Stopped")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")),
    ))
    result = await service.create_trigger(
        data_product="Rate Weather Watch", name="steep", description="rate under 10", condition="aar < 10",
        output_topic_template=f"fl/cp/{SLUG}/steep/{{runway_config}}/{{airport}}", payload_columns=["airport", "runway_config", "aar"],
        distinct=True,
    )
    assert result["status"] == "redeploying" and result["trigger"]["name"] == "steep"
    assert result["trigger"]["subscribe_pattern"] == f"fl/cp/{SLUG}/steep/*/>"
    assert catalog.products[SLUG].status == "redeploying"
    assert catalog.read_prev_program(SLUG) == prev_program
    await service.deployer.wait_idle(SLUG)
    assert stop.called and stop.calls.last.request.url.params["force"] == "true"
    new_program = json.loads(patch.calls.last.request.content)["program_code"]
    assert "trg_rate_weather_watch_steep" in new_program and "trg_rate_weather_watch_low_vis" in new_program
    assert catalog.read_program(SLUG) == new_program
    m = catalog.products[SLUG]
    assert m.status == "running" and [t.name for t in m.triggers] == ["low-vis", "steep"]
    # A busy/redeploying product refuses further changes until running.
    m.status = "redeploying"
    with pytest.raises(ContinuumError, match="wait until it is running"):
        await service.delete_trigger("Rate Weather Watch", "steep")


async def test_create_trigger_rolls_back_on_compile_failure(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    await _seed_running_product(service, api)
    prev_program = catalog.read_program(SLUG)
    api.post(f"{ENGINE}/v0/pipelines/{PIPE}/stop").mock(return_value=httpx.Response(202))
    patch = api.patch(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(200, json=pipeline_info(PIPE, "Pending")))
    bad = pipeline_info(PIPE, "SqlError", "Stopped", program_error={
        "sql_compilation": {"exit_code": 1, "messages": [{"start_line_number": 200, "warning": False, "message": "type mismatch in trigger"}]},
        "rust_compilation": None, "system_error": None})
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")),   # pre-check
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),   # stopped
        httpx.Response(200, json=bad),                                          # compile of new program fails
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),   # rollback: already stopped
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),   # rollback program compiled
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")),   # restarted
    ))
    await service.create_trigger(data_product=SLUG, name="bad", description="", condition="aar > 'x'",
                                 output_topic_template=f"fl/cp/{SLUG}/bad/{{airport}}")
    await service.deployer.wait_idle(SLUG)
    programs = [json.loads(c.request.content)["program_code"] for c in patch.calls]
    assert len(programs) == 2 and "trg_rate_weather_watch_bad" in programs[0] and programs[1] == prev_program
    m = catalog.products[SLUG]
    assert m.status == "running" and "rolled back" in (m.error or "") and "type mismatch" in m.error
    assert catalog.read_program(SLUG) == prev_program
    assert m.history[-1].action == "rolled-back"


async def test_delete_trigger_and_unknown(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    await _seed_running_product(service, api)
    api.post(f"{ENGINE}/v0/pipelines/{PIPE}/stop").mock(return_value=httpx.Response(202))
    patch = api.patch(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(200, json=pipeline_info(PIPE, "Pending")))
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")),
    ))
    with pytest.raises(ContinuumError, match="no trigger 'nope'"):
        await service.delete_trigger(SLUG, "nope")
    out = await service.delete_trigger(SLUG, "low-vis")
    assert out["status"] == "redeploying" and out["trigger"]["name"] == "low-vis"
    await service.deployer.wait_idle(SLUG)
    assert "trg_rate_weather_watch_low_vis" not in json.loads(patch.calls.last.request.content)["program_code"]
    assert catalog.products[SLUG].triggers == [] and catalog.products[SLUG].status == "running"


# ---------------------------------------------------------------------- delete / status / EP
async def test_delete_data_product_full_sequence(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    await _seed_running_product(service, api)
    stop = api.post(f"{ENGINE}/v0/pipelines/{PIPE}/stop").mock(return_value=httpx.Response(202))
    clear = api.post(f"{ENGINE}/v0/pipelines/{PIPE}/clear").mock(return_value=httpx.Response(202))
    delete = api.delete(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(200))
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running", "InUse")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped", "InUse")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped", "InUse")),
        httpx.Response(200, json=pipeline_info(PIPE, "Success", "Stopped", "Cleared")),
    ))
    q_delete = api.delete(url__regex=rf"{CFG}/queues/.*").mock(return_value=httpx.Response(200, json={"meta": {"responseCode": 200}}))
    out = await service.delete_data_product("Rate Weather Watch")
    assert out["deleted"] is True
    assert out["steps"] == [
        f"stopped pipeline {PIPE}", f"cleared storage of {PIPE}", f"deleted pipeline {PIPE}",
        f"deleted queue fl-cp-{SLUG}-airport_rate", f"deleted queue fl-cp-{SLUG}-weather", f"removed catalog entry {SLUG}",
    ]
    assert stop.called and clear.called and delete.called and q_delete.call_count == 2
    assert SLUG not in catalog.products and not catalog.product_dir(SLUG).exists()
    with pytest.raises(ContinuumError, match="unknown data product"):
        api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}))
        await service.delete_data_product("Rate Weather Watch")


async def test_get_status_reports_engine_counters_and_queue_backlog(service: ContinuumService, api: respx.MockRouter):
    await _seed_running_product(service, api)
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}/stats").mock(return_value=httpx.Response(200, json={
        "global_metrics": {},
        "inputs": [{"endpoint_name": f"cp-{SLUG}-airport_rate", "config": {"stream": "airport_rate_raw"},
                    "metrics": {"total_records": 120, "buffered_records": 0, "num_parse_errors": 1, "num_transport_errors": 0},
                    "health": {"status": "Healthy"}, "paused": False, "barrier": False}],
        "outputs": [{"endpoint_name": f"cp-{SLUG}-live", "config": {"stream": "rate_weather_watch_live_sink"},
                     "metrics": {"transmitted_records": 40, "queued_records": 0, "num_encode_errors": 0, "num_transport_errors": 2}}],
    }))
    api.get(url__regex=rf"{MON}/queues/.*").mock(return_value=httpx.Response(200, json={"data": {"lastSpooledMsgId": 10, "highestAckedMsgId": 4}}))
    out = await service.get_status("Rate Weather Watch")
    p = out["products"][0]
    assert p["status"] == "running" and p["deployment_status"] == "Running"
    assert p["inputs"][0] == {"name": f"cp-{SLUG}-airport_rate", "stream": "airport_rate_raw", "health": "Healthy", "fatal_error": None,
                              "total_records": 120, "buffered_records": 0, "parse_errors": 1, "transport_errors": 0}
    assert p["outputs"][0]["transmitted_records"] == 40 and p["outputs"][0]["transport_errors"] == 2
    assert [b["backlog_msgs"] for b in p["queue_backlog"]] == [6, 6]
    with pytest.raises(ContinuumError):
        await service.get_status("nope")


async def test_sync_event_portal_registers_and_is_idempotent(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    await _seed_running_product(service, api)
    store: dict[str, list[dict]] = {}
    counter = {"n": 0}

    def _get(request: httpx.Request) -> httpx.Response:
        kind = request.url.path.rsplit("/", 1)[-1]
        items = store.get(kind, [])
        if "name" in request.url.params:
            items = [i for i in items if i.get("name") == request.url.params["name"]]
        return httpx.Response(200, json={"data": items})

    def _post(request: httpx.Request) -> httpx.Response:
        kind = request.url.path.rsplit("/", 1)[-1]
        body = json.loads(request.content)
        counter["n"] += 1
        body["id"] = f"{kind}-{counter['n']}"
        store.setdefault(kind, []).append(body)
        return httpx.Response(201, json={"data": body})

    api.get(url__regex=rf"{EP}/.*").mock(side_effect=_get)
    api.post(url__regex=rf"{EP}/.*").mock(side_effect=_post)

    state = await service.sync_event_portal(SLUG)
    assert state["status"] == "registered" and state["application_version"] == "1.0.0"
    assert {s["name"] for s in store["schemas"]} == {"RateWeatherWatchRow", "RateWeatherWatchLowVisTrigger"}
    assert {e["name"] for e in store["events"]} == {f"{SLUG}.live", f"{SLUG}.low-vis"}
    assert store["applications"][0]["name"] == f"continuum-{SLUG}"
    assert sorted(store["applicationVersions"][0]["declaredProducedEventVersionIds"]) == sorted(state["event_version_ids"].values())
    live_schema = json.loads(store["schemaVersions"][0]["content"])
    assert live_schema["title"] == "RateWeatherWatchRowEnvelope" and live_schema["required"] == ["insert"]
    row = live_schema["properties"]["insert"]
    assert row["title"] == "RateWeatherWatchRow"
    assert row["properties"]["airport"] == {"type": "string", "description": "SQL type VARCHAR"}
    created = counter["n"]
    again = await service.sync_event_portal(SLUG)
    assert again["status"] == "registered" and counter["n"] == created
    assert catalog.products[SLUG].triggers[0].event_portal.event_version_id == state["event_version_ids"]["low-vis"]


async def test_sync_event_portal_without_token_is_skipped(catalog_dir, catalog, api):
    from continuum_mcp.feldera_client import FelderaClient
    from continuum_mcp.semp_client import SempClient
    from tests.conftest import make_settings

    settings = make_settings(catalog_dir, SOLACE_API_TOKEN=None)
    svc = ContinuumService(settings, catalog, FelderaClient(ENGINE), SempClient(SEMP, "a", "b", "default"), None)
    try:
        await _seed_running_product(svc, api)
        state = await svc.sync_event_portal(SLUG)
        assert state["status"] == "skipped" and "SOLACE_API_TOKEN" in state["error"]
    finally:
        await svc.shutdown()


async def test_resume_redrives_compiling_manifest_on_startup(service: ContinuumService, api: respx.MockRouter, catalog: Catalog):
    mock_engine_validate(api)
    mock_semp_create(api)
    api.put(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(201, json=pipeline_info(PIPE, "Pending")))
    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(side_effect=sequence(
        httpx.Response(404, json={"message": "x", "error_code": "y", "details": None}),
        httpx.Response(200, json=pipeline_info(PIPE, "CompilingSql", "Stopped")),
    ))
    await create_high_value(service, register_in_event_portal=False)
    await service.deployer.cancel(SLUG)  # simulate a server restart mid-compile
    assert yaml.safe_load((catalog.product_dir(SLUG) / "manifest.yaml").read_text())["status"] == "compiling"

    api.get(f"{ENGINE}/v0/pipelines/{PIPE}").mock(return_value=httpx.Response(200, json=pipeline_info(PIPE, "Success", "Running")))
    await service.startup()
    await service.deployer.wait_idle(SLUG)
    assert service.catalog.products[SLUG].status == "running"

"""Golden tests for rendered programs. Set UPDATE_GOLDEN=1 to regenerate fixtures."""

from __future__ import annotations

import json
import os
import re

import pytest

from continuum_mcp.catalog import Catalog
from continuum_mcp.models import TriggerSpec
from continuum_mcp.templates import (
    ProgramRenderer,
    RenderSpec,
    TriggerRender,
    pipeline_tags,
    queue_name,
    trigger_view_name,
)
from tests.conftest import GOLDEN_DIR, HIGH_VALUE_SQL, make_settings


def _golden(name: str, actual: str) -> None:
    path = GOLDEN_DIR / name
    if os.environ.get("UPDATE_GOLDEN") == "1":
        path.write_text(actual, encoding="utf-8")
    assert path.exists(), f"missing golden {path}; run with UPDATE_GOLDEN=1"
    assert actual == path.read_text(encoding="utf-8")


def _connectors(rendered_source: str) -> list[dict]:
    """Parse the connectors JSON back out of a rendered CREATE TABLE ... WITH ('connectors' = '...')."""
    m = re.search(r"'connectors' = '(\[.*?\])'\);", rendered_source, re.DOTALL)
    assert m, rendered_source
    return json.loads(m.group(1).replace("''", "'"))


def _spec(catalog: Catalog, include_sinks: bool = True) -> RenderSpec:
    trigger = TriggerSpec(
        name="low-vis",
        description="Low published rate while weather is IFR",
        condition="flight_category = 'IFR'",
        output_topic_template="fl/cp/rate-weather-watch/low-vis/{airport}",
        recency_window="5 minutes",
    )
    return RenderSpec(
        name="Rate Weather Watch",
        slug="rate-weather-watch",
        description="Airports with an AAR of 40 or less, with the latest weather category",
        sql=HIGH_VALUE_SQL,
        streams=[catalog.stream("airport_rate"), catalog.stream("weather")],
        live_topic="fl/cp/rate-weather-watch/live/{airport}",
        columns=["airport", "aar", "runway_config", "valid_from", "flight_category"],
        event_time_column="valid_from",
        triggers=[TriggerRender(trigger, ["airport", "aar", "runway_config", "flight_category"])],
        include_sinks=include_sinks,
    )


def test_full_program_golden(catalog: Catalog, renderer: ProgramRenderer):
    out = renderer.render_program(_spec(catalog))
    _golden("rate_weather_watch.sql", out.sql)


def test_validation_pass_has_no_sinks(catalog: Catalog, renderer: ProgramRenderer):
    out = renderer.render_program(_spec(catalog, include_sinks=False))
    assert "solace_output" not in out.sql
    assert "CREATE MATERIALIZED VIEW rate_weather_watch AS" in out.sql


def test_line_map_points_into_user_select(catalog: Catalog, renderer: ProgramRenderer):
    out = renderer.render_program(_spec(catalog))
    lines = out.sql.splitlines()
    assert lines[out.user_sql_start_line - 1].startswith("SELECT r.airport")
    assert out.user_sql_line_count == 4
    assert out.locate(out.user_sql_start_line + 2) == ("your SELECT", 3)
    label, rel = out.locate(out.sections[-1].start_line + 1)
    assert label.startswith("trigger view trg_rate_weather_watch_low_vis")
    msg = out.remap_message({"start_line_number": out.user_sql_start_line + 1, "start_column": 8, "message": "boom", "error_type": "x", "warning": False})
    assert msg["where"] == "your SELECT, line 2, column 8"


def test_raw_source_connector_shape(catalog: Catalog, renderer: ProgramRenderer):
    src = renderer.render_source(catalog.stream("airport_rate"), "rate-weather-watch")
    (conn,) = _connectors(src)
    assert conn["name"] == "cp-rate-weather-watch-airport_rate"
    assert conn["transport"]["name"] == "solace_input"
    cfg = conn["transport"]["config"]
    assert cfg == {
        "host": "solace", "port": 55555, "vpn": "default", "username": "default", "password": "default",
        "queue": "fl-cp-rate-weather-watch-airport_rate", "include_topic": True, "include_broker_timestamp": True,
        "topic_pattern": "TTP/airportInfo/{airport}",
    }
    assert conn["format"] == {"name": "raw", "config": {"column_name": "data"}}
    assert "CREATE TABLE airport_rate_raw (" in src and "CREATE MATERIALIZED VIEW airport_rate AS" in src
    assert "LATENESS airport_rate_events.valid_from INTERVAL '2' HOUR;" in src


def test_append_only_only_for_insert_only_feeds(catalog: Catalog, renderer: ProgramRenderer):
    # Insert-only mesh feed -> the source table is rendered WITH ('append_only' = 'true', ...)
    src = renderer.render_source(catalog.stream("airport_rate"), "rate-weather-watch")
    assert "'append_only' = 'true'" in src
    assert src.count("'append_only'") == 1  # exactly once, on the WITH clause
    # The extra property must not disturb connector parsing.
    (conn,) = _connectors(src)
    assert conn["transport"]["name"] == "solace_input"
    # A full-changelog stream (receives deletes) must NOT be marked append_only.
    ctx = renderer.render_source(catalog.stream("capacity_assessment"), "rate-weather-watch")
    assert "append_only" not in ctx


def test_live_sink_and_trigger_shapes(catalog: Catalog, renderer: ProgramRenderer):
    sql = renderer.render_program(_spec(catalog)).sql
    # live sink: full changelog (no skip_deletes), direct delivery, placeholder coalesced
    live = sql[sql.index("rate_weather_watch_live_sink"):sql.index("-- Trigger 'low-vis'")]
    assert '"format": {"name": "json"}' in live
    assert "skip_deletes" not in live
    assert "COALESCE(CAST(airport AS VARCHAR), 'unknown') AS airport" in live
    # trigger: DISTINCT + skip_deletes + recency window on the event time column
    trg = sql[sql.index("-- Trigger 'low-vis'"):]
    assert '"format": {"name": "json", "config": {"skip_deletes": true}}' in trg
    assert "AS SELECT DISTINCT" in trg
    assert "AND NOW() <= valid_from + INTERVAL '5' MINUTE" in trg
    assert "WHERE (flight_category = 'IFR')" in trg
    assert '"topic":    "fl/cp/rate-weather-watch/low-vis/{airport}"' in trg


def test_derived_snapshot_source_golden(catalog: Catalog, renderer: ProgramRenderer):
    """capacity_assessment: one-shot url_input snapshot + live insert_delete solace_input."""
    src = renderer.render_source(catalog.stream("capacity_assessment"), "demo")
    _golden("capacity_assessment_source.sql", src)
    snapshot, live = _connectors(src)
    assert snapshot["name"] == "cp-demo-capacity_assessment-snapshot"
    assert snapshot["transport"] == {"name": "url_input", "config": {
        "path": "http://feldera:8080/v0/pipelines/arrival-capacity/query?sql=SELECT%20%2A%20FROM%20capacity_assessment&format=json"}}
    assert snapshot["format"] == {"name": "json", "config": {"update_format": "raw", "array": False}}
    assert live["transport"]["name"] == "solace_input"
    assert live["transport"]["config"]["queue"] == "fl-cp-demo-capacity_assessment"
    assert "include_broker_timestamp" not in live["transport"]["config"]  # full changelog: deletes must match rows exactly
    assert live["format"] == {"name": "json", "config": {"update_format": "insert_delete", "array": False}}
    assert "CREATE TABLE capacity_assessment_raw (" in src
    assert "bin_capacity INTEGER" in src
    assert "CREATE VIEW capacity_assessment AS\nSELECT airport, bin_start, bin_end, demand, bin_capacity, runway_config, overage, eta_quality, exceeded FROM capacity_assessment_raw;" in src
    assert "PARSE_JSON" not in src


def test_derived_inserts_only_uses_broker_ts_latest_wins(catalog: Catalog, renderer: ProgramRenderer):
    src = renderer.render_source(catalog.stream("arrival_context"), "demo")
    (live,) = _connectors(src)
    assert live["transport"]["config"]["include_broker_timestamp"] is True
    assert live["format"]["config"]["update_format"] == "insert_delete"
    assert "broker_ts TIMESTAMP DEFAULT CAST(CONNECTOR_METADATA()['broker_ts'] AS TIMESTAMP)" in src
    assert "CREATE VIEW arrival_context AS" in src
    assert "PARTITION BY airport, bin_start" in src and "ORDER BY broker_ts DESC" in src


def test_bare_envelope_switches_input_update_format(catalog_dir):
    settings = make_settings(catalog_dir, TRIGGER_ENVELOPE="bare")
    catalog = Catalog(catalog_dir).load()
    # A full-changelog derived stream with the snapshot disabled renders a single solace_input.
    stream = catalog.stream("capacity_assessment").model_copy(update={"snapshot": False})
    src = ProgramRenderer(settings).render_source(stream, "demo")
    (live,) = _connectors(src)
    assert live["format"]["config"]["update_format"] == "raw"
    assert "CREATE VIEW capacity_assessment AS" in src


def test_snapshot_url_escapes_quotes_safely(catalog: Catalog, renderer: ProgramRenderer):
    stream = catalog.stream("capacity_assessment").model_copy(
        update={"snapshot_sql": "SELECT * FROM capacity_assessment WHERE runway_config = 'XYZ'"}
    )
    src = renderer.render_source(stream, "demo")
    snapshot, _ = _connectors(src)
    assert "runway_config%20%3D%20%27XYZ%27" in snapshot["transport"]["config"]["path"]
    assert "'XYZ'" not in src  # no raw single quote can break the SQL string literal


def test_every_ingestable_stream_renders(catalog: Catalog, renderer: ProgramRenderer):
    for s in catalog.streams.values():
        if not s.ingestable:
            with pytest.raises(ValueError):
                renderer.render_source(s, "demo")
            continue
        out = renderer.render_source(s, "demo")
        assert queue_name("demo", s.name) in out
        assert f"VIEW {s.relation} AS" in out, s.name
        conns = _connectors(out)
        assert conns[-1]["transport"]["name"] == "solace_input"
        if s.kind == "derived":
            assert conns[-1]["format"]["name"] == "json"
            assert len(conns) == (2 if s.snapshot else 1)


def test_naming_helpers():
    assert trigger_view_name("rate-weather-watch", "low-vis") == "trg_rate_weather_watch_low_vis"
    assert pipeline_tags("x-y") == ["continuum:data-product", "continuum:slug=x-y", "continuum:v1"]

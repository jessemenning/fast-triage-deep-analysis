from __future__ import annotations

import pytest
import yaml

from continuum_mcp.catalog import Catalog, CatalogError
from continuum_mcp.models import ProductManifest, SchemaColumn, TriggerRecord

EXPECTED_STREAMS = {
    "airport_rate": "airport_rate",
    "weather": "metar_latest",
    "active_restrictions": "active_restriction",
    "capacity_assessment": "capacity_assessment",
    "arrival_context": "arrival_context",
    "flight": "flight",
    "arrival_demand": "arrival_demand",
    "downstream_risk": "mcp_downstream_risk",
}


def test_streams_registry(catalog: Catalog):
    assert {n: s.relation for n, s in catalog.streams.items()} == EXPECTED_STREAMS
    raw = {s.name for s in catalog.streams_by_kind("raw")}
    derived = {s.name for s in catalog.streams_by_kind("derived")}
    assert "airport_rate" in raw and "active_restrictions" in raw
    assert {"capacity_assessment", "arrival_context", "flight", "arrival_demand"} <= derived
    assert not catalog.stream("flight").ingestable
    assert "capacity_assessment" in catalog.stream("flight").hint
    assert catalog.stream("arrival_context").changelog == "inserts_only"
    assert catalog.stream("capacity_assessment").snapshot is True
    assert [s.name for s in catalog.streams.values() if s.snapshot] == ["capacity_assessment"]
    for s in catalog.streams.values():
        assert set(s.key_columns) <= set(s.column_names), s.name
        assert set(s.volatile_columns) <= set(s.column_names), s.name
        if s.event_time_column:
            assert s.event_time_column in s.column_names, s.name
        if s.ingestable:
            assert s.topic_pattern and s.topic_subscriptions, s.name


def test_stream_summary_has_example_topic(catalog: Catalog):
    s = catalog.stream("capacity_assessment").summary()
    assert s["example_topic"] == "fl/faa/arrival-capacity/demand/airport-001"
    assert s["columns"][0] == {"name": "airport", "type": "VARCHAR", "description": "ICAO airport identifier (key)"}


def test_base_products(catalog: Catalog):
    assert catalog.base_pipeline_name() == "arrival-capacity"
    names = {p.name for p in catalog.base_products()}
    assert {"flight", "airport_rate", "capacity_assessment", "arrival_context", "metar_latest"} <= names
    by_name = {p.name: p for p in catalog.base_products()}
    assert by_name["capacity_assessment"].live_topic == "fl/faa/arrival-capacity/demand/{airport}"
    assert all(not p.managed and p.owner == "platform" for p in catalog.base_products())


def test_unknown_stream_error(catalog: Catalog):
    with pytest.raises(CatalogError, match="unknown source stream"):
        catalog.stream("nope")


def _manifest(slug: str = "rate-weather-watch") -> ProductManifest:
    return ProductManifest(
        name="Rate Weather Watch",
        slug=slug,
        description="low-rate airports",
        owner="traffic-ops",
        tags=["capacity", "faa"],
        source_streams=["airport_rate"],
        sql="SELECT airport, aar FROM airport_rate WHERE aar <= 40",
        key_columns=["airport"],
        live_topic=f"fl/cp/{slug}/live/{{airport}}",
        pipeline=f"cp-{slug}",
        queues=[f"fl-cp-{slug}-airport_rate"],
        schema=[SchemaColumn(name="airport", type="VARCHAR"), SchemaColumn(name="aar", type="INTEGER")],
        triggers=[
            TriggerRecord(
                name="very-low",
                description="rate under 20",
                condition="aar < 20",
                topic_template=f"fl/cp/{slug}/very-low/{{airport}}",
                subscribe_pattern=f"fl/cp/{slug}/very-low/>",
                sample_topic=f"fl/cp/{slug}/very-low/airport-001",
                payload_columns=["airport", "aar"],
                view_name="trg_rate_weather_watch_very_low",
            )
        ],
    )


def test_manifest_roundtrip(catalog: Catalog):
    m = _manifest()
    catalog.save_manifest(m, program_sql="-- program", prev_program_sql="-- prev")
    data = yaml.safe_load((catalog.product_dir(m.slug) / "manifest.yaml").read_text())
    assert data["schema"][0]["name"] == "airport"  # alias used on disk
    assert data["status"] == "compiling"
    reloaded = Catalog(catalog.root).load()
    got = reloaded.product("Rate Weather Watch")
    assert got is not None and got.slug == m.slug and got.view_name == "rate_weather_watch"
    assert reloaded.product("cp-rate-weather-watch") is got and reloaded.product("rate_weather_watch") is got
    assert reloaded.read_program(m.slug) == "-- program"
    assert reloaded.read_prev_program(m.slug) == "-- prev"
    assert reloaded.delete_product(m.slug) is True
    assert reloaded.product(m.slug) is None
    assert not catalog.product_dir(m.slug).exists()


def test_unreadable_manifest_is_skipped(catalog: Catalog):
    bad = catalog.products_dir / "broken"
    bad.mkdir()
    (bad / "manifest.yaml").write_text("name: only-a-name\n")
    reloaded = Catalog(catalog.root).load()
    assert "broken" not in reloaded.products


def test_search_ranking_is_deterministic(catalog: Catalog):
    catalog.save_manifest(_manifest())
    hits = catalog.search("wind visibility", limit=5)
    assert hits and hits[0]["type"] == "stream" and hits[0]["name"] == "weather"
    assert any("column" in w or "description" in w for w in hits[0]["why"])
    assert hits == catalog.search("wind visibility", limit=5)

    hits = catalog.search("very low", limit=3)
    assert hits[0]["type"] == "trigger" and hits[0]["product"] == "Rate Weather Watch"

    hits = catalog.search("rate weather watch")
    assert hits[0] == {**hits[0], "type": "product", "name": "Rate Weather Watch"}

    assert catalog.search("zzzz-nothing") == []
    assert catalog.search("") == []
    assert len(catalog.search("airport", limit=3)) == 3

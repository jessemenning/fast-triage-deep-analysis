"""The trigger payload guard: volatile columns are kept out of trigger payloads
so triggers fire once per episode instead of on every recompute."""

from __future__ import annotations

import pytest

from continuum_mcp.models import TriggerSpec
from continuum_mcp.service import is_volatile_column

# The arrival-demand-watch product's output columns (a rolling demand fan-in view
# joined to airport context) — the realistic mix of volatile and stable columns.
PRODUCT_COLUMNS = [
    "airport",
    "arrivals_60m",
    "overage_60m",
    "distinct_runways_60m",
    "last_breach_at",
    "airport_name",
    "runway_config",
    "severity",
    "tracon",
    "bin_capacity",
]
VOLATILE = {
    "arrivals_60m",
    "overage_60m",
    "distinct_runways_60m",
    "last_breach_at",
}
STABLE = {c for c in PRODUCT_COLUMNS if c not in VOLATILE}
TOPIC = "fl/cp/arrival-demand-watch/fan-in/{airport}"


@pytest.mark.parametrize("col", sorted(VOLATILE) + [
    "demand_24h", "total_overage", "arrival_count", "demand_rate",
    "window_start", "last_updated_at", "avg_delay",
])
def test_volatile_columns_flagged(col: str) -> None:
    assert is_volatile_column(col) is True


@pytest.mark.parametrize("col", sorted(STABLE) + [
    "airport", "runway_config", "severity", "tracon", "valid_from",
    "gufi", "status", "flight_id",
])
def test_stable_columns_not_flagged(col: str) -> None:
    assert is_volatile_column(col) is False


def test_per_row_event_time_is_stable_but_recency_is_volatile() -> None:
    # a per-row event-time column is stable (one value per key), even as event_time_column
    assert is_volatile_column("bin_start", event_time_column="bin_start") is False
    assert is_volatile_column("bin_start") is False
    # aggregated most-recent timestamps churn and are volatile
    assert is_volatile_column("last_breach_at") is True
    assert is_volatile_column("last_updated_at") is True


def _render(service, spec: TriggerSpec):
    return service._trigger_render(
        spec, PRODUCT_COLUMNS, [], "arrival-demand-watch", ["airport"], "last_breach_at"
    )


@pytest.mark.asyncio
async def test_guard_strips_volatile_from_caller_payload(service) -> None:
    spec = TriggerSpec(
        name="demand-breach", condition="arrivals_60m >= bin_capacity", output_topic_template=TOPIC,
        payload_columns=list(PRODUCT_COLUMNS),  # caller over-specifies (all columns)
    )
    render = _render(service, spec)
    assert set(render.payload_columns) == STABLE
    assert "airport" in render.payload_columns  # key/placeholder kept
    assert not (set(render.payload_columns) & VOLATILE)
    assert render.warnings and "volatile" in render.warnings[0].lower()


@pytest.mark.asyncio
async def test_guard_keeps_key_placeholder_even_if_named_volatile(service) -> None:
    # topic keyed on a volatile-looking column: it must still survive as a placeholder
    spec = TriggerSpec(
        name="by-demand", condition="arrivals_60m >= bin_capacity",
        output_topic_template="fl/cp/arrival-demand-watch/by-demand/{arrivals_60m}",
        payload_columns=["arrivals_60m", "overage_60m"],
    )
    render = _render(service, spec)
    assert "arrivals_60m" in render.payload_columns   # placeholder kept
    assert "overage_60m" not in render.payload_columns  # other volatile stripped


@pytest.mark.asyncio
async def test_allow_volatile_payload_overrides(service) -> None:
    spec = TriggerSpec(
        name="demand-breach", condition="arrivals_60m >= bin_capacity", output_topic_template=TOPIC,
        payload_columns=["airport", "arrivals_60m", "overage_60m"],
        allow_volatile_payload=True,
    )
    render = _render(service, spec)
    assert render.payload_columns == ["airport", "arrivals_60m", "overage_60m"]
    assert not render.warnings


@pytest.mark.asyncio
async def test_default_payload_excludes_volatile(service) -> None:
    spec = TriggerSpec(name="demand-breach", condition="arrivals_60m >= bin_capacity", output_topic_template=TOPIC)
    render = _render(service, spec)
    assert set(render.payload_columns) == STABLE

from __future__ import annotations

import json

import httpx
import pytest
import respx

from continuum_mcp.ep_client import (
    EventPortalClient,
    EventPortalError,
    address_levels,
    next_minor_version,
    pascal,
)
from tests.conftest import EP


@pytest.fixture
async def ep():
    c = EventPortalClient(EP, "ep-token")
    yield c
    await c.aclose()


def test_helpers():
    assert pascal("high-value-review") == "HighValueReview"
    assert address_levels("fl/cp/x/live/{payment_id}/{rail}") == [
        {"name": "fl", "addressLevelType": "literal"},
        {"name": "cp", "addressLevelType": "literal"},
        {"name": "x", "addressLevelType": "literal"},
        {"name": "live", "addressLevelType": "literal"},
        {"name": "payment_id", "addressLevelType": "variable"},
        {"name": "rail", "addressLevelType": "variable"},
    ]
    assert next_minor_version([]) == "1.0.0"
    assert next_minor_version(["1.0.0", "1.2.0", "0.9.0"]) == "1.3.0"


async def test_find_or_create_is_idempotent(ep: EventPortalClient, api: respx.MockRouter):
    """First pass creates every object; second pass finds them and POSTs nothing."""
    store: dict[str, list[dict]] = {k: [] for k in ("applicationDomains", "schemas", "schemaVersions", "events",
                                                     "eventVersions", "applications", "applicationVersions")}
    counter = {"n": 0}

    def lister(kind):
        def _get(request):
            params = request.url.params
            items = store[kind]
            if "name" in params:
                items = [i for i in items if i["name"] == params["name"]]
            return httpx.Response(200, json={"data": items})
        return _get

    def creator(kind):
        def _post(request):
            body = json.loads(request.content)
            counter["n"] += 1
            body["id"] = f"{kind}-{counter['n']}"
            store[kind].append(body)
            return httpx.Response(201, json={"data": body})
        return _post

    for kind in store:
        api.get(f"{EP}/{kind}").mock(side_effect=lister(kind))
        api.post(f"{EP}/{kind}").mock(side_effect=creator(kind))

    async def run() -> dict[str, str]:
        d = await ep.ensure_domain("Continuum Data Products", "desc")
        s = await ep.ensure_schema(d, "HighValueReviewRow")
        sv = await ep.ensure_schema_version(s, "1.0.0", {"type": "object"})
        e = await ep.ensure_event(d, "high-value-review.live")
        ev = await ep.ensure_event_version(e, "1.0.0", schema_version_id=sv, topic_template="fl/cp/x/live/{payment_id}")
        a = await ep.ensure_application(d, "continuum-high-value-review")
        av, ver = await ep.ensure_application_version(a, produced_event_version_ids=[ev], description="d")
        return {"d": d, "s": s, "sv": sv, "e": e, "ev": ev, "a": a, "av": av, "ver": ver}

    first = await run()
    creates_after_first = counter["n"]
    assert creates_after_first == 7
    assert first["ver"] == "1.0.0"
    second = await run()
    assert second == first
    assert counter["n"] == creates_after_first, "second pass must not create anything"

    # schema version content is posted as a JSON string, event version carries address levels
    sv_body = store["schemaVersions"][0]
    assert json.loads(sv_body["content"]) == {"type": "object"} and sv_body["schemaId"] == first["s"]
    ev_body = store["eventVersions"][0]
    assert ev_body["deliveryDescriptor"]["address"]["addressLevels"][-1] == {"name": "payment_id", "addressLevelType": "variable"}
    assert store["applications"][0]["applicationType"] == "standard"
    assert api.calls.last.request.headers["authorization"] == "Bearer ep-token"

    # a new produced set yields a new minor application version
    av2, ver2 = await ep.ensure_application_version(first["a"], produced_event_version_ids=[first["ev"], "evv-trigger"])
    assert ver2 == "1.1.0" and av2 != first["av"]


async def test_errors_are_typed(ep: EventPortalClient, api: respx.MockRouter):
    api.get(f"{EP}/applicationDomains").mock(return_value=httpx.Response(401, json={"message": "invalid token"}))
    with pytest.raises(EventPortalError, match="invalid token"):
        await ep.ensure_domain("x")


async def test_unregister_is_best_effort(ep: EventPortalClient, api: respx.MockRouter):
    api.get(f"{EP}/applications").mock(return_value=httpx.Response(200, json={"data": [{"id": "a1", "name": "continuum-x"}]}))
    api.get(f"{EP}/applicationVersions").mock(return_value=httpx.Response(200, json={"data": [{"id": "av1", "version": "1.0.0"}]}))
    api.delete(f"{EP}/applicationVersions/av1").mock(return_value=httpx.Response(204))
    api.delete(f"{EP}/applications/a1").mock(return_value=httpx.Response(204))
    steps = await ep.unregister_application("d1", "continuum-x")
    assert steps == ["deleted applicationVersion 1.0.0", "deleted application continuum-x"]
    api.get(f"{EP}/events").mock(return_value=httpx.Response(200, json={"data": []}))
    assert await ep.unregister_event("d1", "x.live") == []

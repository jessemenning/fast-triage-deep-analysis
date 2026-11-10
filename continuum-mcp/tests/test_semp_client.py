from __future__ import annotations

import json

import httpx
import pytest
import respx

from continuum_mcp.semp_client import SempClient, SempError
from tests.conftest import SEMP

CFG = f"{SEMP}/SEMP/v2/config/msgVpns/default"
MON = f"{SEMP}/SEMP/v2/monitor/msgVpns/default"


def _already_exists() -> httpx.Response:
    return httpx.Response(
        400,
        json={"meta": {"error": {"code": 6, "description": "Object already exists", "status": "ALREADY_EXISTS"},
                       "responseCode": 400}},
    )


@pytest.fixture
async def semp():
    c = SempClient(SEMP, "admin", "admin", "default", max_ttl_secs=300, max_spool_mb=500)
    yield c
    await c.aclose()


async def test_ensure_queue_creates_queue_and_subscriptions(semp: SempClient, api: respx.MockRouter):
    q = api.post(f"{CFG}/queues").mock(return_value=httpx.Response(200, json={"meta": {"responseCode": 200}}))
    sub = api.post(f"{CFG}/queues/fl-cp-demo-payments_review/subscriptions").mock(
        return_value=httpx.Response(200, json={"meta": {"responseCode": 200}})
    )
    result = await semp.ensure_queue("fl-cp-demo-payments_review", ["payments/*/*/*/*/REVIEW"])
    body = json.loads(q.calls.last.request.content)
    assert body == {
        "queueName": "fl-cp-demo-payments_review", "msgVpnName": "default", "accessType": "non-exclusive",
        "permission": "consume", "ingressEnabled": True, "egressEnabled": True, "maxTtl": 300,
        "respectTtlEnabled": True, "maxMsgSpoolUsage": 500,
    }
    assert json.loads(sub.calls.last.request.content)["subscriptionTopic"] == "payments/*/*/*/*/REVIEW"
    assert q.calls.last.request.headers["authorization"].startswith("Basic ")
    assert result == {"queue": "fl-cp-demo-payments_review", "created": True,
                      "subscriptions": ["payments/*/*/*/*/REVIEW"], "subscriptions_added": ["payments/*/*/*/*/REVIEW"]}


async def test_ensure_queue_tolerates_already_exists_and_patches(semp: SempClient, api: respx.MockRouter):
    api.post(f"{CFG}/queues").mock(return_value=_already_exists())
    patch = api.patch(f"{CFG}/queues/q1").mock(return_value=httpx.Response(200, json={"meta": {"responseCode": 200}}))
    api.post(f"{CFG}/queues/q1/subscriptions").mock(return_value=_already_exists())
    result = await semp.ensure_queue("q1", ["a/>", "b/>"])
    assert result["created"] is False and result["subscriptions_added"] == []
    assert json.loads(patch.calls.last.request.content)["maxTtl"] == 300


async def test_ensure_queue_raises_on_other_errors(semp: SempClient, api: respx.MockRouter):
    api.post(f"{CFG}/queues").mock(
        return_value=httpx.Response(401, json={"meta": {"error": {"status": "UNAUTHORIZED", "description": "bad creds"}}})
    )
    with pytest.raises(SempError, match="bad creds"):
        await semp.ensure_queue("q1", [])


async def test_delete_queue(semp: SempClient, api: respx.MockRouter):
    api.delete(f"{CFG}/queues/q1").mock(return_value=httpx.Response(200, json={"meta": {"responseCode": 200}}))
    api.delete(f"{CFG}/queues/q2").mock(
        return_value=httpx.Response(400, json={"meta": {"error": {"status": "NOT_FOUND", "description": "nope"}}})
    )
    assert await semp.delete_queue("q1") is True
    assert await semp.delete_queue("q2") is False


async def test_queue_backlog_approximation(semp: SempClient, api: respx.MockRouter):
    api.get(f"{MON}/queues/q1").mock(
        return_value=httpx.Response(200, json={"data": {"lastSpooledMsgId": 1200, "highestAckedMsgId": 1150,
                                                         "msgSpoolUsage": 4096, "bindCount": 1}})
    )
    api.get(f"{MON}/queues/q2").mock(return_value=httpx.Response(200, json={"data": {"spooledMsgCount": 7, "bindCount": 0}}))
    api.get(f"{MON}/queues/q3").mock(
        return_value=httpx.Response(400, json={"meta": {"error": {"status": "NOT_FOUND", "description": "x"}}})
    )
    b1 = await semp.queue_backlog("q1")
    assert b1["backlog_msgs"] == 50 and "approximation" in b1["backlog_method"] and b1["bind_count"] == 1
    b2 = await semp.queue_backlog("q2")
    assert b2["backlog_msgs"] == 7 and b2["backlog_method"] == "spooledMsgCount"
    assert await semp.queue_backlog("q3") == {"queue": "q3", "exists": False}

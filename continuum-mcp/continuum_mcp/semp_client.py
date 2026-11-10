"""SEMP v2 client: durable queues + topic subscriptions for data-product pipelines.

Mirrors the broker setup idiom: POST the queue,
treat ALREADY_EXISTS as success and PATCH the bounds so re-runs converge, then
POST each subscription (ALREADY_EXISTS tolerated).
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

log = logging.getLogger("continuum-mcp.semp")


class SempError(Exception):
    def __init__(self, status: int, message: str, code: str | None = None):
        super().__init__(f"SEMP HTTP {status}: {message}" + (f" [{code}]" if code else ""))
        self.status = status
        self.message = message
        self.code = code


class SempClient:
    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        vpn: str,
        *,
        max_ttl_secs: int = 300,
        max_spool_mb: int = 500,
        timeout: float = 15.0,
        client: httpx.AsyncClient | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.vpn = vpn
        self.max_ttl_secs = max_ttl_secs
        self.max_spool_mb = max_spool_mb
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url, auth=(user, password), timeout=timeout
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------ helpers
    @property
    def _cfg(self) -> str:
        return f"/SEMP/v2/config/msgVpns/{self.vpn}"

    @property
    def _mon(self) -> str:
        return f"/SEMP/v2/monitor/msgVpns/{self.vpn}"

    @staticmethod
    def _error(resp: httpx.Response) -> tuple[str | None, str]:
        try:
            meta = resp.json().get("meta", {})
            err = meta.get("error", {}) or {}
            return err.get("status"), err.get("description") or resp.text
        except ValueError:
            return None, resp.text

    async def _post_tolerant(self, path: str, body: dict[str, Any]) -> bool:
        """POST; True if created, False if it already existed. Raises otherwise."""
        resp = await self._client.post(path, json=body)
        if resp.is_success:
            return True
        code, desc = self._error(resp)
        if code == "ALREADY_EXISTS":
            return False
        raise SempError(resp.status_code, desc, code)

    def _queue_body(self, name: str) -> dict[str, Any]:
        return {
            "queueName": name,
            "msgVpnName": self.vpn,
            "accessType": "non-exclusive",
            "permission": "consume",
            "ingressEnabled": True,
            "egressEnabled": True,
            "maxTtl": self.max_ttl_secs,
            "respectTtlEnabled": True,
            "maxMsgSpoolUsage": self.max_spool_mb,
        }

    # ------------------------------------------------------------------ queues
    async def ensure_queue(self, name: str, subscriptions: list[str]) -> dict[str, Any]:
        created = await self._post_tolerant(f"{self._cfg}/queues", self._queue_body(name))
        if not created:
            # Converge bounds on re-run, same as setup.sh's semp_patch after ALREADY_EXISTS.
            patch = {
                "maxTtl": self.max_ttl_secs,
                "respectTtlEnabled": True,
                "maxMsgSpoolUsage": self.max_spool_mb,
                "ingressEnabled": True,
                "egressEnabled": True,
            }
            resp = await self._client.patch(f"{self._cfg}/queues/{name}", json=patch)
            if not resp.is_success:
                code, desc = self._error(resp)
                raise SempError(resp.status_code, desc, code)
        added: list[str] = []
        for topic in subscriptions:
            if await self._post_tolerant(
                f"{self._cfg}/queues/{name}/subscriptions",
                {"msgVpnName": self.vpn, "queueName": name, "subscriptionTopic": topic},
            ):
                added.append(topic)
        log.info("queue %s ensured (created=%s, new subscriptions=%s)", name, created, added)
        return {"queue": name, "created": created, "subscriptions": subscriptions, "subscriptions_added": added}

    async def delete_queue(self, name: str) -> bool:
        resp = await self._client.delete(f"{self._cfg}/queues/{name}")
        if resp.is_success:
            return True
        code, desc = self._error(resp)
        if resp.status_code == 400 and code == "NOT_FOUND" or resp.status_code == 404:
            return False
        raise SempError(resp.status_code, desc, code)

    # ------------------------------------------------------------------ monitor
    async def queue_backlog(self, name: str) -> dict[str, Any]:
        """Backlog snapshot from the monitor API.

        `backlog_msgs` prefers the broker's spooled count when present and otherwise
        falls back to lastSpooledMsgId - highestAckedMsgId, an approximation that
        ignores gaps from expired or dead-lettered messages.
        """
        resp = await self._client.get(f"{self._mon}/queues/{name}")
        if resp.status_code == 400 or resp.status_code == 404:
            code, _ = self._error(resp)
            if code == "NOT_FOUND" or resp.status_code == 404:
                return {"queue": name, "exists": False}
        if not resp.is_success:
            code, desc = self._error(resp)
            raise SempError(resp.status_code, desc, code)
        data = resp.json().get("data", {})
        approx = None
        method = "spooledMsgCount"
        if "spooledMsgCount" in data:
            approx = data.get("spooledMsgCount")
        else:
            last = data.get("lastSpooledMsgId")
            acked = data.get("highestAckedMsgId")
            if isinstance(last, int) and isinstance(acked, int):
                approx = max(0, last - acked)
                method = "lastSpooledMsgId - highestAckedMsgId (approximation)"
        return {
            "queue": name,
            "exists": True,
            "backlog_msgs": approx,
            "backlog_method": method,
            "msg_spool_usage_bytes": data.get("msgSpoolUsage"),
            "bind_count": data.get("bindCount"),
            "ingress_enabled": data.get("ingressEnabled"),
            "egress_enabled": data.get("egressEnabled"),
        }

"""Event Portal (Solace Cloud, architecture API v2) client with find-or-create semantics.

Object model we maintain per data product:
  application domain  CONTINUUM_EP_DOMAIN
    schema            <Slug>Row               + schemaVersion 1.0.0 (row/envelope JSON Schema)
    schema            <Slug><Trigger>Trigger  + schemaVersion 1.0.0 (per trigger)
    event             <slug>.live             + eventVersion 1.0.0 (address from live topic)
    event             <slug>.<trigger>        + eventVersion 1.0.0 (address from trigger topic)
    application       continuum-<slug>        + applicationVersion 1.<n>.0 declaring produced events
Payload shapes follow solace-streaming-arch/create_ep_objects.sh.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from continuum_mcp.sql_guard import PLACEHOLDER_RE

log = logging.getLogger("continuum-mcp.ep")


class EventPortalError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(f"Event Portal HTTP {status}: {message}")
        self.status = status
        self.message = message


def pascal(text: str) -> str:
    return "".join(p.capitalize() for p in re.split(r"[^A-Za-z0-9]+", text) if p)


def address_levels(topic_template: str) -> list[dict[str, str]]:
    levels: list[dict[str, str]] = []
    for level in topic_template.split("/"):
        m = PLACEHOLDER_RE.fullmatch(level)
        if m:
            levels.append({"name": m.group(1), "addressLevelType": "variable"})
        else:
            levels.append({"name": level, "addressLevelType": "literal"})
    return levels


def next_minor_version(existing: list[str]) -> str:
    minors = [int(m.group(1)) for v in existing if (m := re.fullmatch(r"1\.(\d+)\.0", v or ""))]
    return f"1.{(max(minors) + 1) if minors else 0}.0"


class EventPortalClient:
    def __init__(self, base_url: str, token: str, *, timeout: float = 20.0, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=timeout,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    # ------------------------------------------------------------------ http
    @staticmethod
    def _raise(resp: httpx.Response) -> None:
        if resp.is_success:
            return
        try:
            body = resp.json()
            msg = body.get("message") or json.dumps(body)[:400]
        except ValueError:
            msg = resp.text[:400]
        raise EventPortalError(resp.status_code, msg)

    async def _get_list(self, path: str, **params: Any) -> list[dict[str, Any]]:
        resp = await self._client.get(path, params={k: v for k, v in params.items() if v is not None})
        self._raise(resp)
        return list(resp.json().get("data", []))

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = await self._client.post(path, json=body)
        self._raise(resp)
        return dict(resp.json().get("data", {}))

    async def _delete(self, path: str) -> None:
        resp = await self._client.delete(path)
        if resp.status_code == 404:
            return
        self._raise(resp)

    async def find_by_name(self, path: str, name: str, **params: Any) -> dict[str, Any] | None:
        for item in await self._get_list(path, name=name, **params):
            if item.get("name") == name:
                return item
        return None

    async def find_version(self, path: str, version: str, **params: Any) -> dict[str, Any] | None:
        for item in await self._get_list(path, **params):
            if item.get("version") == version:
                return item
        return None

    # ------------------------------------------------------------------ objects
    async def ensure_domain(self, name: str, description: str = "") -> str:
        found = await self.find_by_name("/applicationDomains", name)
        if found:
            return found["id"]
        created = await self._post("/applicationDomains", {"name": name, "description": description})
        log.info("EP domain created: %s (%s)", name, created.get("id"))
        return created["id"]

    async def ensure_schema(self, domain_id: str, name: str) -> str:
        found = await self.find_by_name("/schemas", name, applicationDomainId=domain_id)
        if found:
            return found["id"]
        created = await self._post(
            "/schemas", {"applicationDomainId": domain_id, "name": name, "schemaType": "jsonSchema"}
        )
        return created["id"]

    async def ensure_schema_version(self, schema_id: str, version: str, content: dict[str, Any]) -> str:
        found = await self.find_version("/schemaVersions", version, schemaIds=schema_id)
        if found:
            return found["id"]
        created = await self._post(
            "/schemaVersions",
            {"schemaId": schema_id, "version": version, "content": json.dumps(content)},
        )
        return created["id"]

    async def ensure_event(self, domain_id: str, name: str) -> str:
        found = await self.find_by_name("/events", name, applicationDomainId=domain_id)
        if found:
            return found["id"]
        created = await self._post("/events", {"applicationDomainId": domain_id, "name": name})
        return created["id"]

    async def ensure_event_version(
        self,
        event_id: str,
        version: str,
        *,
        schema_version_id: str,
        topic_template: str,
        description: str = "",
    ) -> str:
        found = await self.find_version("/eventVersions", version, eventIds=event_id)
        if found:
            return found["id"]
        created = await self._post(
            "/eventVersions",
            {
                "eventId": event_id,
                "version": version,
                "description": description,
                "schemaVersionId": schema_version_id,
                "deliveryDescriptor": {
                    "brokerType": "solace",
                    "address": {"addressType": "topic", "addressLevels": address_levels(topic_template)},
                },
            },
        )
        return created["id"]

    async def ensure_application(self, domain_id: str, name: str) -> str:
        found = await self.find_by_name("/applications", name, applicationDomainId=domain_id)
        if found:
            return found["id"]
        created = await self._post(
            "/applications",
            {
                "applicationDomainId": domain_id,
                "name": name,
                "applicationType": "standard",
                "brokerType": "solace",
            },
        )
        return created["id"]

    async def list_application_versions(self, app_id: str) -> list[dict[str, Any]]:
        return await self._get_list("/applicationVersions", applicationIds=app_id)

    async def ensure_application_version(
        self,
        app_id: str,
        *,
        produced_event_version_ids: list[str],
        consumed_event_version_ids: list[str] | None = None,
        description: str = "",
    ) -> tuple[str, str]:
        """Reuse the latest version if it already declares exactly these events; else add 1.<n>.0.

        `consumed_event_version_ids` is optional and backward compatible: when omitted
        (the product-registration path), reuse matches on PRODUCED only — so a version
        that was later wired with consumed events is still reused, not reverted. When
        consumed events ARE supplied (the end-to-end wiring path), reuse matches on
        both produced AND consumed.
        """
        versions = await self.list_application_versions(app_id)
        wanted_p = sorted(produced_event_version_ids)
        wanted_c = sorted(consumed_event_version_ids or [])
        for v in versions:
            vp = sorted(v.get("declaredProducedEventVersionIds") or [])
            if vp != wanted_p:
                continue
            if consumed_event_version_ids is None:
                return v["id"], v.get("version", "")   # produced-only match (don't disturb consume wiring)
            if sorted(v.get("declaredConsumedEventVersionIds") or []) == wanted_c:
                return v["id"], v.get("version", "")
        version = next_minor_version([v.get("version", "") for v in versions])
        body: dict[str, Any] = {
            "applicationId": app_id,
            "version": version,
            "description": description,
            "declaredProducedEventVersionIds": produced_event_version_ids,
        }
        if consumed_event_version_ids:
            body["declaredConsumedEventVersionIds"] = consumed_event_version_ids
        created = await self._post("/applicationVersions", body)
        return created["id"], version

    # ------------------------------------------------------------------ teardown
    async def unregister_application(self, domain_id: str, app_name: str) -> list[str]:
        """Best-effort delete of application versions + application. Returns steps."""
        steps: list[str] = []
        app = await self.find_by_name("/applications", app_name, applicationDomainId=domain_id)
        if not app:
            return ["application not found in Event Portal"]
        for v in await self.list_application_versions(app["id"]):
            await self._delete(f"/applicationVersions/{v['id']}")
            steps.append(f"deleted applicationVersion {v.get('version')}")
        await self._delete(f"/applications/{app['id']}")
        steps.append(f"deleted application {app_name}")
        return steps

    async def unregister_event(self, domain_id: str, event_name: str) -> list[str]:
        steps: list[str] = []
        ev = await self.find_by_name("/events", event_name, applicationDomainId=domain_id)
        if not ev:
            return []
        for v in await self._get_list("/eventVersions", eventIds=ev["id"]):
            await self._delete(f"/eventVersions/{v['id']}")
            steps.append(f"deleted eventVersion {event_name} {v.get('version')}")
        await self._delete(f"/events/{ev['id']}")
        steps.append(f"deleted event {event_name}")
        return steps

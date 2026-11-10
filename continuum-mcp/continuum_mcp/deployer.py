"""Background deployer: takes a pipeline from 'program uploaded' to Running.

Compiles are serialized with an asyncio lock (the engine compiles one program
at a time anyway, and this keeps the 'estimated_ready_secs' honest). Every
transition is persisted to the product manifest so the server can resume
after a restart: any manifest left in `compiling`/`redeploying` is re-driven.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from continuum_mcp.catalog import Catalog
from continuum_mcp.config import Settings
from continuum_mcp.feldera_client import (
    TERMINAL_PROGRAM_ERRORS,
    FelderaClient,
    FelderaError,
    program_error_text,
)
from continuum_mcp.models import ProductManifest

log = logging.getLogger("continuum-mcp.deployer")

TRANSITIONAL_DEPLOYMENT = {
    "Provisioning", "Initializing", "Bootstrapping", "ConcurrentBootstrapping",
    "Synchronizing", "Replaying", "Coordination", "AwaitingApproval",
}


def derive_status(info: dict[str, Any] | None) -> str:
    """Map engine program/deployment statuses to a product status."""
    if not info:
        return "unknown"
    program = info.get("program_status")
    deployment = info.get("deployment_status")
    if program in TERMINAL_PROGRAM_ERRORS:
        return "failed"
    if deployment == "Running":
        return "running"
    if deployment in TRANSITIONAL_DEPLOYMENT or program not in (None, "Success"):
        return "compiling"
    if deployment in ("Stopped", "Stopping", "Suspended", "Paused", "Unavailable", "Standby"):
        return "stopped"
    return "unknown"


class Deployer:
    def __init__(self, feldera: FelderaClient, catalog: Catalog, settings: Settings):
        self.feldera = feldera
        self.catalog = catalog
        self.settings = settings
        self.compile_lock = asyncio.Lock()
        self._tasks: dict[str, asyncio.Task[None]] = {}

    # ------------------------------------------------------------------ public
    def is_busy(self, slug: str) -> bool:
        t = self._tasks.get(slug)
        return t is not None and not t.done()

    @property
    def queue_depth(self) -> int:
        return sum(1 for t in self._tasks.values() if not t.done())

    def schedule(self, slug: str, *, rollback_program: str | None = None) -> asyncio.Task[None]:
        """Drive compile → start → Running for the manifest's pipeline (idempotent per slug)."""
        if self.is_busy(slug):
            return self._tasks[slug]
        task = asyncio.create_task(
            self._guarded(slug, lambda: self.run_deploy(slug, rollback_program=rollback_program))
        )
        self._tasks[slug] = task
        return task

    def schedule_redeploy(self, slug: str, program: str, prev_program: str | None) -> asyncio.Task[None]:
        """stop(force) → Stopped → PATCH → compile → start, with rollback on compile failure."""
        if self.is_busy(slug):
            return self._tasks[slug]
        task = asyncio.create_task(
            self._guarded(slug, lambda: self.run_redeploy(slug, program, prev_program))
        )
        self._tasks[slug] = task
        return task

    async def resume(self) -> list[str]:
        resumed = []
        for slug, m in list(self.catalog.products.items()):
            if m.status in ("compiling", "redeploying"):
                log.info("resuming deployment of %s (status=%s)", slug, m.status)
                if m.status == "redeploying":
                    program = self.catalog.read_program(slug)
                    prev = self.catalog.read_prev_program(slug)
                    if program:
                        self.schedule_redeploy(slug, program, prev)
                        resumed.append(slug)
                        continue
                self.schedule(slug)
                resumed.append(slug)
        return resumed

    async def shutdown(self) -> None:
        for t in self._tasks.values():
            if not t.done():
                t.cancel()
        for t in self._tasks.values():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._tasks.clear()

    async def cancel(self, slug: str) -> None:
        t = self._tasks.pop(slug, None)
        if t is not None and not t.done():
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t

    async def wait_idle(self, slug: str) -> None:
        t = self._tasks.get(slug)
        if t is not None:
            with contextlib.suppress(Exception):
                await t

    # ------------------------------------------------------------------ steps
    async def stop_and_patch(self, pipeline: str, program: str, *, timeout: float = 60.0) -> None:
        """Force-stop and PATCH program_code once the engine reports Stopped."""
        info = await self.feldera.get_pipeline(pipeline)
        if info is None:
            raise FelderaError(404, f"pipeline {pipeline!r} not found")
        if info.get("deployment_status") != "Stopped":
            with contextlib.suppress(FelderaError):
                await self.feldera.stop_force(pipeline)
            await self.feldera.wait_for(
                pipeline,
                lambda i: i.get("deployment_status") == "Stopped",
                timeout=timeout,
                interval=min(2.0, self.settings.poll_interval_secs),
            )
        await self.feldera.patch_program(pipeline, program)

    async def run_redeploy(self, slug: str, program: str, prev_program: str | None) -> None:
        manifest = self.catalog.products.get(slug)
        if manifest is None:
            return
        try:
            await self.stop_and_patch(manifest.pipeline, program)
        except (FelderaError, TimeoutError) as exc:
            self._mark(manifest, "failed", f"could not update pipeline: {exc}", "redeploy-failed")
            return
        await self.run_deploy(slug, rollback_program=prev_program)

    async def run_deploy(self, slug: str, *, rollback_program: str | None = None) -> None:
        manifest = self.catalog.products.get(slug)
        if manifest is None:
            log.warning("deploy requested for unknown product %s", slug)
            return
        pipeline = manifest.pipeline
        started = time.monotonic()
        async with self.compile_lock:
            try:
                info = await self.feldera.wait_for(
                    pipeline,
                    lambda i: i.get("program_status") == "Success"
                    or i.get("program_status") in TERMINAL_PROGRAM_ERRORS,
                    timeout=self.settings.compile_timeout_secs,
                    interval=self.settings.poll_interval_secs,
                )
            except TimeoutError as exc:
                self._mark(manifest, "failed", str(exc), "compile-timeout")
                return
            except FelderaError as exc:
                self._mark(manifest, "failed", str(exc), "compile-error")
                return
            if info.get("program_status") in TERMINAL_PROGRAM_ERRORS:
                err = program_error_text(info) or f"program_status={info.get('program_status')}"
                if rollback_program:
                    await self._rollback(manifest, rollback_program, err)
                else:
                    self._mark(manifest, "failed", err, "compile-failed")
                return
            log.info("%s compiled in %.0fs", pipeline, time.monotonic() - started)

        # Start outside the compile lock: starting does not contend with compiles.
        try:
            if info.get("deployment_status") != "Running":
                await self.feldera.start(pipeline)
            await self.feldera.wait_for(
                pipeline,
                lambda i: i.get("deployment_status") == "Running"
                or (i.get("deployment_status") == "Stopped" and i.get("deployment_error")),
                timeout=self.settings.compile_timeout_secs,
                interval=self.settings.poll_interval_secs,
            )
            final = await self.feldera.get_pipeline(pipeline) or {}
            if final.get("deployment_status") == "Running":
                self._mark(manifest, "running", None, "running", f"ready in {time.monotonic() - started:.0f}s")
            else:
                self._mark(manifest, "failed", program_error_text(final) or "pipeline stopped during start", "start-failed")
        except (TimeoutError, FelderaError) as exc:
            self._mark(manifest, "failed", str(exc), "start-failed")

    async def _rollback(self, manifest: ProductManifest, prev_program: str, err: str) -> None:
        pipeline = manifest.pipeline
        log.warning("%s: compile failed, rolling back program: %s", pipeline, err)
        try:
            await self.stop_and_patch(pipeline, prev_program)
            await self.feldera.wait_for(
                pipeline,
                lambda i: i.get("program_status") == "Success"
                or i.get("program_status") in TERMINAL_PROGRAM_ERRORS,
                timeout=self.settings.compile_timeout_secs,
                interval=self.settings.poll_interval_secs,
            )
            await self.feldera.start(pipeline)
            await self.feldera.wait_for(
                pipeline,
                lambda i: i.get("deployment_status") == "Running",
                timeout=self.settings.compile_timeout_secs,
                interval=self.settings.poll_interval_secs,
            )
            self.catalog.save_manifest(manifest, program_sql=prev_program)
            self._mark(manifest, "running", f"change rejected by the Continuum engine and rolled back: {err}", "rolled-back")
        except (TimeoutError, FelderaError) as exc:
            self._mark(manifest, "failed", f"{err}; rollback also failed: {exc}", "rollback-failed")

    # ------------------------------------------------------------------ persist
    def _mark(
        self, manifest: ProductManifest, status: str, error: str | None, action: str, detail: str | None = None
    ) -> None:
        manifest.status = status  # type: ignore[assignment]
        manifest.error = error
        manifest.touch(action, detail or error)
        self.catalog.save_manifest(manifest)
        log.info("%s -> %s%s", manifest.pipeline, status, f" ({error})" if error else "")

    async def _guarded(self, slug: str, factory: Callable[[], Awaitable[None]]) -> None:
        """Run a deploy step; the coroutine is created lazily so a cancel before start leaves no orphan."""
        try:
            await factory()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a background task must never die silently
            log.exception("deployer task for %s crashed", slug)
            m = self.catalog.products.get(slug)
            if m is not None and m.status in ("compiling", "redeploying"):
                self._mark(m, "failed", f"deployer error: {exc}", "deployer-crash")

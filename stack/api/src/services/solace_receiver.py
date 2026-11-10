"""Solace sink subscriber for the FAA arrival-capacity dashboard.

Subscribes to the Feldera/Continuum sink topics and the SAM Agent Mesh output
topics, fans out JSON payloads to asyncio queues consumed by SSE endpoints, and
retains the latest payload per stream so the UI can poll instead of stream.

  fl/faa/arrival-capacity/demand/{airport}  — live per-bin demand vs capacity
  fl/faa/arrival-capacity/breach/{airport}  — enriched breach that triggers SAM
  fl/faa/arrival-capacity/incident-state/{airport} — SEVERE↔CLEAR edge (all-clear signal)
  solutions/arrival-capacity/decision/{airport} — SAM final triage decision + brief
  solutions/arrival-capacity/triage             — SAM interim fast-triage verdict

The Solace SDK fires on_message on its own thread; we bridge to asyncio via
loop.call_soon_threadsafe so SSE generators can await queue.get() cleanly.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any

log = logging.getLogger(__name__)

try:
    from solace.messaging.messaging_service import MessagingService, ServiceInterruptionListener
    from solace.messaging.receiver.message_receiver import MessageHandler
    from solace.messaging.resources.topic_subscription import TopicSubscription
    from solace.messaging.resources.topic import Topic

    _SDK = True
except Exception:
    _SDK = False
    MessagingService = None  # type: ignore[assignment]
    ServiceInterruptionListener = object  # type: ignore[assignment,misc]
    MessageHandler = object  # type: ignore[assignment,misc]
    TopicSubscription = None  # type: ignore[assignment]
    Topic = None  # type: ignore[assignment]

# ── Agentic-analysis toggle ────────────────────────────────────────────────────
# A presenter-facing on/off flag for the SAM (LLM) triage layer. The UI reads and
# flips it so a demo can show the Continuum breach go red without burning LLM
# tokens, then turn the agent ON to let SAM produce the triage verdict + brief.
_agent_enabled = threading.Event()  # cleared = OFF by default


def set_agent_enabled(enabled: bool) -> None:
    if enabled:
        _agent_enabled.set()
    else:
        _agent_enabled.clear()


def is_agent_enabled() -> bool:
    return _agent_enabled.is_set()


# Topic prefix → stream name. Order matters: the static triage topic is matched
# exactly before the per-airport prefixes below it.
_PREFIX_MAP = {
    "fl/faa/arrival-capacity/demand/": "demand",            # Continuum: per-bin demand vs capacity
    "fl/faa/arrival-capacity/incident-state/": "incident_state",  # Continuum: SEVERE↔CLEAR edge (all-clear)
    "fl/faa/arrival-capacity/breach/": "breach",            # Continuum: enriched breach (SAM trigger)
    "solutions/arrival-capacity/decision/": "decision",     # SAM: final triage decision + brief
    "solutions/arrival-capacity/triage": "triage",          # SAM: interim fast-triage verdict (static)
}

STREAM_NAMES = list(dict.fromkeys(_PREFIX_MAP.values()))

# Broker subscriptions (SMF wildcards: * = one level, > = multi level).
_SUBSCRIPTIONS = [
    "fl/faa/arrival-capacity/demand/>",
    "fl/faa/arrival-capacity/incident-state/>",
    "fl/faa/arrival-capacity/breach/>",
    "solutions/arrival-capacity/decision/>",
    "solutions/arrival-capacity/triage",
]

# How many recent payloads to retain per stream for the polling API.
_LATEST_WINDOW = 200


class FaaReceiver:
    """Singleton Solace receiver — one connection, several logical streams."""

    def __init__(self, broker_props: dict[str, str]) -> None:
        self._props = broker_props
        self._subs: dict[str, list[tuple[asyncio.Queue, asyncio.AbstractEventLoop]]] = {
            s: [] for s in STREAM_NAMES
        }
        # Latest-state store: stream -> {key -> payload dict}. For per-airport
        # streams the key is the airport; for the static triage topic it is a
        # single bucket. Lets the UI poll /api/faa/latest/{stream}.
        self._latest: dict[str, dict[str, Any]] = {s: {} for s in STREAM_NAMES}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._svc = None        # connected MessagingService
        self._publisher = None  # direct publisher (console inject / scenario control)

    def publish(self, topic: str, payload: str = "{}") -> None:
        """Public helper to publish a control/event message to a topic."""
        self._republish(topic, payload)

    def _republish(self, topic: str, payload: str) -> None:
        """Publish a raw payload to a topic (used by the console inject path)."""
        pub = self._publisher
        if pub is None or Topic is None:
            return
        try:
            msg = self._svc.message_builder().build(bytearray(payload, "utf-8"))
            pub.publish(destination=Topic.of(topic), message=msg)
        except Exception as exc:  # never let a publish error kill the receiver
            log.warning("publish failed for %s: %s", topic, exc)

    def start(self) -> None:
        if not _SDK:
            log.warning("solace messaging SDK not installed — SSE streams will be silent")
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="solace-recv")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

    def subscribe(self, stream: str) -> asyncio.Queue:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        with self._lock:
            self._subs[stream].append((q, loop))
        return q

    def unsubscribe(self, stream: str, q: asyncio.Queue) -> None:
        with self._lock:
            self._subs[stream] = [(qq, lo) for qq, lo in self._subs[stream] if qq is not q]

    def latest(self, stream: str) -> list[dict]:
        """Return the retained latest payloads for a stream (for polling)."""
        with self._lock:
            return list(self._latest.get(stream, {}).values())

    def _store_latest(self, stream: str, payload_str: str) -> None:
        try:
            obj = json.loads(payload_str)
        except Exception:
            return
        if not isinstance(obj, dict):
            return
        key = str(obj.get("airport") or "_")
        with self._lock:
            bucket = self._latest.setdefault(stream, {})
            bucket[key] = obj
            # bound the store so a long-running demo can't grow it unbounded
            if len(bucket) > _LATEST_WINDOW:
                for k in list(bucket.keys())[: len(bucket) - _LATEST_WINDOW]:
                    bucket.pop(k, None)

    def _dispatch(self, stream: str, payload_str: str) -> None:
        self._store_latest(stream, payload_str)
        with self._lock:
            subs = list(self._subs[stream])
        for q, loop in subs:
            try:
                loop.call_soon_threadsafe(q.put_nowait, payload_str)
            except (RuntimeError, asyncio.QueueFull):
                pass

    def _run(self) -> None:
        while not self._stop.is_set():
            svc = receiver = None
            # Signals that the broker dropped us — triggers reconnect without _stop
            disconnected = threading.Event()
            try:
                log.info("FaaReceiver connecting to Solace …")
                svc = MessagingService.builder().from_properties(self._props).build()

                # Register before connect so we catch disconnects immediately.
                # The SDK requires a typed class instance, not a plain callable.
                class _Interruption(ServiceInterruptionListener):  # type: ignore[misc,valid-type]
                    def on_service_interrupted(self, event: Any) -> None:
                        disconnected.set()

                try:
                    svc.add_service_interruption_listener(_Interruption())
                except Exception:
                    pass  # fall back to polling below

                svc.connect()
                log.info("FaaReceiver connected")

                # Direct publisher for console inject / scenario control.
                self._svc = svc
                try:
                    self._publisher = svc.create_direct_message_publisher_builder().build()
                    self._publisher.start()
                except Exception as exc:
                    log.warning("publisher init failed: %s", exc)
                    self._publisher = None

                parent = self

                class _Handler(MessageHandler):  # type: ignore[misc,valid-type]
                    def on_message(self, message: Any) -> None:
                        try:
                            topic: str = message.get_destination_name()
                        except Exception:
                            return
                        try:
                            raw = message.get_payload_as_string()
                            if raw is None:
                                b = message.get_payload_as_bytes()
                                raw = b.decode("utf-8", errors="replace") if b else "{}"
                        except Exception as exc:
                            log.error("Payload extraction failed: %s", exc)
                            raw = "{}"
                        stream = _route(topic)
                        if stream is None:
                            return
                        # Feldera sinks emit the default insert_delete envelope;
                        # flatten to plain insert rows. SAM output topics emit a
                        # plain JSON object, which _normalize_sink passes through.
                        for row in _normalize_sink(raw):
                            parent._dispatch(stream, row)

                receiver = (
                    svc.create_direct_message_receiver_builder()
                    .with_subscriptions([TopicSubscription.of(t) for t in _SUBSCRIPTIONS])
                    .on_back_pressure_drop_latest(1000)
                    .build()
                )
                receiver.start()
                receiver.receive_async(_Handler())

                # Wait until intentionally stopped or broker disconnects us.
                while not self._stop.is_set():
                    if disconnected.wait(timeout=2.0):
                        break
                    try:
                        if not svc.is_connected:
                            disconnected.set()
                            break
                    except Exception:
                        pass
                if disconnected.is_set() and not self._stop.is_set():
                    log.warning("FaaReceiver: broker disconnect detected — reconnecting in 5s")
                    self._stop.wait(timeout=5.0)

            except Exception as exc:
                log.error("FaaReceiver error: %s — reconnecting in 5s", exc)
                if self._stop.wait(timeout=5.0):
                    break
            finally:
                pub = self._publisher
                self._publisher = None
                self._svc = None
                for obj, method in [(pub, "terminate"), (receiver, "terminate"), (svc, "disconnect")]:
                    if obj is not None:
                        try:
                            getattr(obj, method)(grace_period=1000) if method == "terminate" else getattr(obj, method)()
                        except Exception:
                            pass


def _route(topic: str) -> str | None:
    for prefix, stream in _PREFIX_MAP.items():
        if topic == prefix or topic.startswith(prefix):
            return stream
    return None


def _normalize_sink(raw: str) -> list[str]:
    """Flatten Feldera insert envelope(s) to plain row JSON strings.

      {"insert": {row}}  -> {row}
      {"delete": {row}}  -> dropped (dashboard shows an accumulating view)

    A single Solace message may carry one envelope, a JSON array of envelopes, or
    a newline-delimited (NDJSON) batch. SAM output topics carry a single plain
    JSON object, which passes straight through. Returns one row per insert.
    """
    if not raw:
        return []

    envelopes: list[dict] = []
    stripped = raw.strip()
    try:
        parsed = json.loads(stripped)
    except Exception:
        parsed = None
    if isinstance(parsed, dict):
        envelopes = [parsed]
    elif isinstance(parsed, list):
        envelopes = [e for e in parsed if isinstance(e, dict)]
    else:
        for line in stripped.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                envelopes.append(obj)

    out: list[str] = []
    for obj in envelopes:
        if "insert" in obj:
            out.append(json.dumps(obj["insert"] or {}))
        elif "delete" in obj:
            continue  # accumulating view — ignore retractions
        else:
            out.append(json.dumps(obj))  # already flat / non-enveloped
    return out


# ── Module-level singleton ────────────────────────────────────────────────────

_receiver: FaaReceiver | None = None


def init_receiver(broker_props: dict[str, str]) -> None:
    global _receiver
    _receiver = FaaReceiver(broker_props)
    _receiver.start()


def get_receiver() -> FaaReceiver:
    if _receiver is None:
        raise RuntimeError("FaaReceiver not initialised — call init_receiver() at startup")
    return _receiver


def stop_receiver() -> None:
    if _receiver is not None:
        _receiver.stop()

#!/usr/bin/env python3
"""
replay.py — deterministic replay of a captured SWIM corpus over Solace.

Reads JSONL records from $DATA_DIR (one record per line) and re-publishes each as a
raw-bytes message on its recorded topic, pacing by the recorded inter-message delay.

The canned data is "now-relative": any payload (or topic) string of the form
"@now", "@+<seconds>", "@-<seconds>" is rewritten at publish time to a UTC
timestamp relative to the moment of publish. This is what makes captured ETAs,
rate effective-times and METAR report-times always land inside Feldera's live
window no matter when `make up` runs — the key trick behind "real but canned data".

Record shape (see stack/data/capture.py and the committed samples):
    {"delay": 0.25, "topic": "FDPS/position/UAL123/ACTIVE/.../GUFI1", "payload": {...}}

METAR is the exception: records on the aviation/metar/ topic prefix are produced to the
nisshi Kafka broker (topic NISSHI_METAR_TOPIC, key = station) instead of Solace, where
Feldera reads them with its native kafka_input connector. Every other feed still goes
to Solace over SMF.

Env:
    SOLACE_HOST / SOLACE_VPN / SOLACE_USER / SOLACE_PASSWORD
    NISSHI_BOOTSTRAP    nisshi Kafka bootstrap (default nisshi:9092)
    NISSHI_METAR_TOPIC  Kafka topic for METAR (default aviation.metar)
    DATA_DIR      directory of *.jsonl corpus files (default /data)
    REPLAY_SPEED  1.0 = recorded pace, 2.0 = twice as fast (default 1.0)
    REPLAY_LOOP   loop the corpus forever when it ends (default true)
"""
import glob
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from kafka import KafkaProducer

REL_TOKEN = re.compile(r"^@(now|[+-]\d+)$")

# Reference/context feeds are published ONCE (pass 1) and then left alone, so a
# looping replay doesn't re-assert the baseline and clobber the scenario driver's
# degraded rate / IFR weather / restriction. Only the flight traffic loops.
REFERENCE_PREFIXES = ("TTP/airportInfo/", "TTP/restriction/", "aviation/metar/")

# METAR rides the nisshi Kafka on-ramp instead of Solace SMF: records whose (resolved)
# topic starts with this prefix are produced to the nisshi Kafka topic (key = station),
# where Feldera reads them with its native kafka_input connector. Everything else still
# publishes to Solace. The pass-1-only REFERENCE_PREFIXES rule above is transport-agnostic.
NISSHI_METAR_PREFIX = "aviation/metar/"
NISSHI_METAR_TOPIC = os.environ.get("NISSHI_METAR_TOPIC", "aviation.metar")
NISSHI_BOOTSTRAP = os.environ.get("NISSHI_BOOTSTRAP", "nisshi:9092")


def log(msg: str) -> None:
    print(f"[replay] {datetime.now(timezone.utc):%H:%M:%S}  {msg}", flush=True)


def _fmt(ts: datetime) -> str:
    # Space-separated UTC — casts straight to SQL TIMESTAMP in Feldera/Calcite.
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def resolve_tokens(value, now: datetime):
    """Recursively rewrite @now / @+N / @-N tokens to UTC timestamps."""
    if isinstance(value, str):
        m = REL_TOKEN.match(value)
        if m:
            tok = m.group(1)
            if tok == "now":
                return _fmt(now)
            return _fmt(now + timedelta(seconds=int(tok)))
        return value
    if isinstance(value, dict):
        return {k: resolve_tokens(v, now) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_tokens(v, now) for v in value]
    return value


def load_corpus(data_dir: str):
    records = []
    files = sorted(glob.glob(os.path.join(data_dir, "*.jsonl")))
    for path in files:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as exc:
                    log(f"skip malformed line in {path}: {exc}")
    log(f"loaded {len(records)} records from {len(files)} file(s) in {data_dir}")
    return records


def connect() -> MessagingService:
    props = {
        "solace.messaging.transport.host": os.environ.get("SOLACE_HOST", "tcp://localhost:55555"),
        "solace.messaging.service.vpn-name": os.environ.get("SOLACE_VPN", "default"),
        "solace.messaging.authentication.scheme.basic.username": os.environ.get("SOLACE_USER", "default"),
        "solace.messaging.authentication.scheme.basic.password": os.environ.get("SOLACE_PASSWORD", "default"),
    }
    host = props["solace.messaging.transport.host"]
    # The broker's messaging plane can lag its healthcheck on a cold start, so retry
    # the initial connect with backoff instead of crash-looping the container.
    attempts = max(1, int(os.environ.get("CONNECT_MAX_ATTEMPTS", "30")))
    for attempt in range(1, attempts + 1):
        service = MessagingService.builder().from_properties(props).build()
        try:
            service.connect()
            log(f"connected to {host} (vpn {props['solace.messaging.service.vpn-name']})")
            return service
        except Exception as exc:  # broker not ready yet
            if attempt >= attempts:
                raise
            delay = min(10.0, 2.0 * attempt)
            log(f"broker not ready (attempt {attempt}/{attempts}): {exc}; retrying in {delay:.0f}s")
            time.sleep(delay)


def connect_kafka() -> KafkaProducer:
    """Connect to nisshi over the Kafka protocol, with the same cold-start backoff as
    the Solace connect above (the broker can lag just after compose brings it up)."""
    attempts = max(1, int(os.environ.get("CONNECT_MAX_ATTEMPTS", "30")))
    for attempt in range(1, attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=NISSHI_BOOTSTRAP.split(","),
                acks=1, retries=5, linger_ms=50,
            )
            log(f"connected to nisshi (Kafka) at {NISSHI_BOOTSTRAP}")
            return producer
        except Exception as exc:  # nisshi not ready yet
            if attempt >= attempts:
                raise
            delay = min(10.0, 2.0 * attempt)
            log(f"nisshi not ready (attempt {attempt}/{attempts}): {exc}; retrying in {delay:.0f}s")
            time.sleep(delay)


def main() -> int:
    data_dir = os.environ.get("DATA_DIR", "/data")
    speed = float(os.environ.get("REPLAY_SPEED", "1.0")) or 1.0
    loop = os.environ.get("REPLAY_LOOP", "true").lower() in ("1", "true", "yes")

    records = load_corpus(data_dir)
    if not records:
        log("no records to replay — exiting")
        return 1

    service = connect()
    publisher = service.create_direct_message_publisher_builder().build()
    publisher.start()
    kafka = connect_kafka()   # nisshi on-ramp for the METAR feed

    try:
        passes = 0
        while True:
            passes += 1
            log(f"replay pass {passes} ({len(records)} records, speed {speed}x)")
            for rec in records:
                topic = rec["topic"]
                # Reference/context feeds: publish on pass 1 only (see REFERENCE_PREFIXES).
                if passes > 1 and topic.startswith(REFERENCE_PREFIXES):
                    continue
                delay = float(rec.get("delay", 0.0)) / speed
                if delay > 0:
                    time.sleep(delay)
                now = datetime.now(timezone.utc)
                topic = resolve_tokens(topic, now)
                payload = resolve_tokens(rec.get("payload", {}), now)
                body = bytearray(json.dumps(payload), "utf-8")  # raw bytes — PARSE_JSON / kafka_input need it
                if topic.startswith(NISSHI_METAR_PREFIX):
                    # nisshi (Kafka): key = station (last topic segment) → Feldera kafka_input
                    station = topic.rsplit("/", 1)[-1]
                    kafka.send(NISSHI_METAR_TOPIC, key=station.encode("utf-8"), value=bytes(body))
                else:
                    msg = service.message_builder().build(body)
                    publisher.publish(destination=Topic.of(topic), message=msg)
            kafka.flush()
            if not loop:
                break
        log("replay complete")
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        try:
            kafka.flush()
            kafka.close()
        except Exception:
            pass
        try:
            publisher.terminate()
            service.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

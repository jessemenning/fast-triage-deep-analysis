#!/usr/bin/env python3
"""
capture.py — record a window of real SWIM/SCDS traffic into a replayable corpus.

Subscribes to the live FAA feeds on a Solace broker and writes one JSONL record per
message in the shape stack/feed/replay.py expects:

    {"delay": <seconds since previous msg>, "topic": <topic>, "payload": <dict>}

Recognised timestamp fields in the payload (eta, effective_time, reportTime,
observed_at, observed) are rewritten to "now-relative" tokens ("@+<seconds>")
measured from capture start, so replay.py can shift the whole corpus to land in the
live window whenever it runs. Everything else is captured verbatim.

This is a one-time authoring tool, NOT part of `make up`. Point it at a broker that
carries the real SWIM feeds (e.g. an ingestor bridging the FAA SWIM mesh), let it run
for the desired window, then Ctrl-C. Commit the resulting .jsonl under stack/data/.

Usage:
    SOLACE_HOST=tcp://host:55555 SOLACE_VPN=... SOLACE_USER=... SOLACE_PASSWORD=... \
        python capture.py --out stack/data/capture-$(date +%Y%m%d).jsonl --seconds 1800
"""
import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

from solace.messaging.messaging_service import MessagingService
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.topic_subscription import TopicSubscription

TOPICS = [
    "FDPS/position/>",
    "FDPS/flightPlan/>",
    "TTP/airportInfo/>",
    "TTP/flightData/>",
    "TTP/restriction/>",
    "aviation/metar/>",
]
TS_FIELDS = ("eta", "effective_time", "reportTime", "observed_at", "observed")


def log(msg: str) -> None:
    print(f"[capture] {datetime.now(timezone.utc):%H:%M:%S}  {msg}", file=sys.stderr, flush=True)


def _parse_ts(value: str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


class Recorder(MessageHandler):
    def __init__(self, out_fh, start: float):
        self.out = out_fh
        self.start = start
        self.last = start
        self.count = 0
        self.lock = threading.Lock()

    def on_message(self, message) -> None:
        topic = message.get_destination_name()
        text = message.get_payload_as_string()
        if text is None:
            raw = message.get_payload_as_bytes()
            text = raw.decode("utf-8", "ignore") if raw else "{}"
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return  # skip non-JSON
        now = time.time()
        capture_epoch = datetime.fromtimestamp(self.start, tz=timezone.utc)
        if isinstance(payload, dict):
            for field in TS_FIELDS:
                if field in payload and isinstance(payload[field], str):
                    ts = _parse_ts(payload[field])
                    if ts is not None:
                        payload[field] = f"@+{int((ts - capture_epoch).total_seconds())}"
        with self.lock:
            delay = max(0.0, now - self.last)
            self.last = now
            self.out.write(json.dumps({"delay": round(delay, 3), "topic": topic, "payload": payload}) + "\n")
            self.count += 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="output .jsonl path")
    ap.add_argument("--seconds", type=int, default=1800, help="capture duration")
    args = ap.parse_args()

    props = {
        "solace.messaging.transport.host": os.environ.get("SOLACE_HOST", "tcp://localhost:55555"),
        "solace.messaging.service.vpn-name": os.environ.get("SOLACE_VPN", "default"),
        "solace.messaging.authentication.scheme.basic.username": os.environ.get("SOLACE_USER", "default"),
        "solace.messaging.authentication.scheme.basic.password": os.environ.get("SOLACE_PASSWORD", "default"),
    }
    service = MessagingService.builder().from_properties(props).build()
    service.connect()
    log(f"connected; capturing {args.seconds}s to {args.out}")

    with open(args.out, "w", encoding="utf-8") as fh:
        rec = Recorder(fh, time.time())
        receiver = (
            service.create_direct_message_receiver_builder()
            .with_subscriptions([TopicSubscription.of(t) for t in TOPICS])
            .build()
        )
        receiver.start()
        receiver.receive_async(rec)
        try:
            threading.Event().wait(timeout=args.seconds)
        except KeyboardInterrupt:
            pass
        receiver.terminate()
        log(f"captured {rec.count} messages")

    service.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())

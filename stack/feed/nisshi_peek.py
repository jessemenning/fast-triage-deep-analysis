#!/usr/bin/env python3
"""nisshi_peek.py — tail a nisshi Kafka topic and print messages. Demo/ops helper.

Proves the Kafka on-ramp is live in BOTH directions:
  * PEEK_TOPIC=aviation.metar             → what producers publish INTO nisshi
  * PEEK_TOPIC=aviation.metar.normalized  → what Feldera writes BACK to nisshi (kafka_output)

Env:
    NISSHI_BOOTSTRAP  nisshi Kafka bootstrap (default nisshi:9092)
    PEEK_TOPIC        topic to read (default aviation.metar.normalized)
    PEEK_N            stop after this many messages (default 10)
    PEEK_TIMEOUT_MS   idle timeout before exiting (default 15000)
"""
import os
import sys

from kafka import KafkaConsumer


def main() -> int:
    bootstrap = os.environ.get("NISSHI_BOOTSTRAP", "nisshi:9092").split(",")
    topic = os.environ.get("PEEK_TOPIC", "aviation.metar.normalized")
    limit = int(os.environ.get("PEEK_N", "10"))
    idle_ms = int(os.environ.get("PEEK_TIMEOUT_MS", "15000"))

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=idle_ms,
    )
    print(f"[nisshi-peek] reading up to {limit} msg(s) from {topic} @ {','.join(bootstrap)}", flush=True)
    seen = 0
    for msg in consumer:
        key = msg.key.decode("utf-8", "replace") if msg.key else ""
        val = msg.value.decode("utf-8", "replace") if msg.value else ""
        print(f"  [{msg.offset}] {key}: {val}", flush=True)
        seen += 1
        if seen >= limit:
            break
    consumer.close()
    print(f"[nisshi-peek] done ({seen} msg(s))", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

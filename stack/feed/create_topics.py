#!/usr/bin/env python3
"""create_topics.py — one-shot: pre-create the nisshi Kafka topics the demo uses.

nisshi does not auto-create topics on produce, and Feldera's kafka_output connector
needs the output topic to exist when the pipeline starts. This runs before init (a
compose dependency), creates each topic in NISSHI_TOPICS idempotently, and exits 0.
Reuses the feed image (it ships kafka-python).

Env:
    NISSHI_BOOTSTRAP  nisshi Kafka bootstrap (default nisshi:9092)
    NISSHI_TOPICS     comma-separated topic names
                      (default: aviation.metar,aviation.metar.normalized)
"""
import os
import sys
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError


def log(msg: str) -> None:
    print(f"[nisshi-topics] {msg}", flush=True)


def main() -> int:
    bootstrap = os.environ.get("NISSHI_BOOTSTRAP", "nisshi:9092").split(",")
    topics = [t.strip() for t in os.environ.get(
        "NISSHI_TOPICS", "aviation.metar,aviation.metar.normalized").split(",") if t.strip()]

    admin = None
    attempts = max(1, int(os.environ.get("CONNECT_MAX_ATTEMPTS", "30")))
    for attempt in range(1, attempts + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap)
            break
        except (KafkaError, OSError) as exc:  # not yet reachable (no brokers / conn refused)
            if attempt >= attempts:
                log(f"FATAL: nisshi unreachable after {attempts} attempts: {exc}")
                return 1
            delay = min(10.0, 2.0 * attempt)
            log(f"nisshi not ready (attempt {attempt}/{attempts}): {exc}; retrying in {delay:.0f}s")
            time.sleep(delay)

    log(f"connected to nisshi at {','.join(bootstrap)}; ensuring topics: {', '.join(topics)}")
    for name in topics:
        nt = NewTopic(name=name, num_partitions=1, replication_factor=1)
        try:
            admin.create_topics([nt])
            log(f"created topic {name}")
        except TopicAlreadyExistsError:
            log(f"topic {name} already exists — ok")
        except Exception as exc:  # some brokers batch the error; treat already-exists as ok
            if "alreadyexists" in type(exc).__name__.lower() or "already exists" in str(exc).lower():
                log(f"topic {name} already exists — ok")
            else:
                log(f"WARN creating {name}: {exc}")
    admin.close()
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())

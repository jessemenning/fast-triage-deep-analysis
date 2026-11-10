#!/usr/bin/env python3
"""Assert the event-mesh gateway has a BOUND broker consumer.

Why this exists: `docker compose ps` can show `sam` "healthy/up" while its
event-mesh gateway failed to start (e.g. sam came up before solace was
DNS-resolvable; SAM logs `reconciliation complete ... gateways=0` and does NOT
retry a failed gateway). In that state nothing consumes the incident trigger
and the fast/slow path silently never fires. Container health does not catch
this — a bound consumer on the gateway's durable queue does.

Checks the broker (SEMP monitor) for a queue matching `eventmesh` that has at
least one active consumer flow. Polls up to TIMEOUT seconds because the runtime
binds the consumer a few seconds after `apply-reference` returns.

Exit 0 = a gateway consumer is bound (trigger wired). Exit 1 = none bound.
Stdlib only (no paho); safe under any python3.
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SEMP = os.environ.get("SEMP_URL", "http://localhost:8089").rstrip("/")
USER = os.environ.get("SEMP_USER", "admin")
PASS = os.environ.get("SEMP_PASS", "admin")
VPN = os.environ.get("SOLACE_VPN", "default")
MATCH = os.environ.get("GATEWAY_QUEUE_MATCH", "eventmesh")
TIMEOUT = int(os.environ.get("CHECK_GATEWAY_TIMEOUT", "60"))

GREEN, RED, RESET = "\033[32m", "\033[31m", "\033[0m"


def _get(path):
    req = urllib.request.Request(f"{SEMP}{path}")
    tok = base64.b64encode(f"{USER}:{PASS}".encode()).decode()
    req.add_header("Authorization", f"Basic {tok}")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def bound_gateway_queues():
    """Return [(queueName, flowCount)] for eventmesh queues with a consumer."""
    data = _get(
        f"/SEMP/v2/monitor/msgVpns/{VPN}/queues?select=queueName&count=100"
    ).get("data", [])
    hits = []
    for q in data:
        name = q.get("queueName", "")
        if MATCH not in name:
            continue
        enc = urllib.parse.quote(name, safe="")
        flows = _get(
            f"/SEMP/v2/monitor/msgVpns/{VPN}/queues/{enc}/txFlows"
        ).get("data", [])
        if flows:
            hits.append((name, len(flows)))
    return hits


def main():
    deadline = time.time() + TIMEOUT
    last_err = None
    seen_queue = False
    while time.time() < deadline:
        try:
            hits = bound_gateway_queues()
            if hits:
                for name, n in hits:
                    print(f"{GREEN}  PASS{RESET} gateway consumer bound "
                          f"({n} flow) -> {name}")
                return 0
            # queue may exist but no consumer yet, or not created yet
            seen_queue = seen_queue or bool(
                [q for q in _get(
                    f"/SEMP/v2/monitor/msgVpns/{VPN}/queues?select=queueName&count=100"
                ).get("data", []) if MATCH in q.get("queueName", "")]
            )
        except (urllib.error.URLError, OSError, ValueError) as e:
            last_err = e
        time.sleep(3)

    print(f"{RED}  FAIL{RESET} no event-mesh gateway consumer is bound "
          f"(queue match '{MATCH}') after {TIMEOUT}s")
    if last_err:
        print(f"         last SEMP error: {last_err}")
    elif seen_queue:
        print("         the gateway's durable queue exists but NO consumer is "
              "bound -> the gateway failed to start")
    else:
        print("         no event-mesh gateway queue exists yet -> the solution "
              "may not be applied")
    print("         SAM can report 'healthy' with gateways=0. Check / fix:")
    print("           docker compose logs sam | grep -E "
          "'reconciliation complete|failed to reconcile gateway'")
    print("           docker compose -f docker-compose.yml "
          "-f docker-compose.cache.yml restart sam")
    return 1


if __name__ == "__main__":
    sys.exit(main())

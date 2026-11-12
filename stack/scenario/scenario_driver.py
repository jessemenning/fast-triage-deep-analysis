#!/usr/bin/env python3
"""
scenario_driver.py — the scripted "unexpected wind shift degrades capacity" hero scenario.

An abrupt wind-direction change at the hero airport (Newark/KEWR) forces a runway
reconfiguration (the SW-favoring 22L|22R flow flips to single-runway 4R arrivals),
which craters the published arrival rate. When triggered it, over Solace (exactly as
real SWIM/SCDS would):
  1. publishes a degraded TTP Airport Information rate for the hero airport
     (AAR 44 -> 26, single-runway 4R config) — the versioned side input drops, so
     Feldera re-evaluates every affected arrival-demand bin;
  2. publishes a VFR-but-gusty METAR with the SHIFTED wind direction (SW ~210 -> NE ~040,
     30G42) to the nisshi Kafka on-ramp (Feldera kafka_input), and a TMR runway-
     reconfiguration restriction over Solace (context the System-2 brief reads);
  3. publishes a burst of extra inbound flights bunched into the near-term window,
     pushing rolling-hour demand above the reduced AAR — the breach fires.
After a hold it restores the normal rate + wind + 22L|22R flow so the demo resets to green.

Triggering:
  * SCENARIO_AUTOSTART=true  → run once automatically after a warmup (one baseline pass).
  * Always subscribes to control topic `faa/scenario/windshift`; publish anything
    there (see demo/burst.sh) to run the scenario on demand.

Env: SOLACE_HOST/VPN/USER/PASSWORD, HERO_AIRPORT (default KEWR),
     NORMAL_AAR (default 44), DEGRADED_AAR (default 26),
     SCENARIO_AUTOSTART (default false), SCENARIO_WARMUP (default 95s),
     SCENARIO_HOLD (default 240s), SCENARIO_BURST (default 24).
"""
import json
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone

from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.topic_subscription import TopicSubscription
from kafka import KafkaProducer

CONTROL_TOPIC = "faa/scenario/windshift"
RESET_TOPIC = "faa/scenario/reset"
# METAR rides the nisshi Kafka on-ramp (Feldera kafka_input), not Solace SMF — the
# degraded wind-shift weather must reach Feldera the same path as the baseline METAR.
NISSHI_METAR_TOPIC = os.environ.get("NISSHI_METAR_TOPIC", "aviation.metar")
NISSHI_BOOTSTRAP = os.environ.get("NISSHI_BOOTSTRAP", "nisshi:9092")
ORIGINS = ["KSFO", "KLAX", "KDEN", "KDFW", "KMIA", "KSEA", "KMCO", "KCLT", "KMSP", "KSLC"]
CARRIERS = ["UAL", "AAL", "DAL", "JBU", "SWA"]

HERO = os.environ.get("HERO_AIRPORT", "KEWR")
NORMAL_AAR = int(os.environ.get("NORMAL_AAR", "44"))
DEGRADED_AAR = int(os.environ.get("DEGRADED_AAR", "26"))
BURST = int(os.environ.get("SCENARIO_BURST", "24"))
HOLD = int(os.environ.get("SCENARIO_HOLD", "240"))

# Normal SW flow (favours 22s) vs the degraded NE flow after the wind shift (forces 4R).
NORMAL_CONFIG = "22L|22R"
DEGRADED_CONFIG = "4R"


def log(msg: str) -> None:
    print(f"[scenario] {datetime.now(timezone.utc):%H:%M:%S}  {msg}", flush=True)


def _ts(offset_secs: int = 0) -> str:
    """Real UTC timestamp (now + offset), Feldera-castable. The scenario publishes
    directly to the broker (it does NOT go through replay.py), so it must emit real
    timestamps — not the @now/@+N replay tokens, which only replay.py resolves."""
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_secs)).strftime("%Y-%m-%d %H:%M:%S")


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
        svc = MessagingService.builder().from_properties(props).build()
        try:
            svc.connect()
            log(f"connected to {host} (vpn {props['solace.messaging.service.vpn-name']})")
            return svc
        except Exception as exc:  # broker not ready yet
            if attempt >= attempts:
                raise
            delay = min(10.0, 2.0 * attempt)
            log(f"broker not ready (attempt {attempt}/{attempts}): {exc}; retrying in {delay:.0f}s")
            time.sleep(delay)


def connect_kafka() -> KafkaProducer:
    """Connect to nisshi (Kafka) with the same cold-start backoff as connect() above."""
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


class Scenario:
    def __init__(self, svc: MessagingService):
        self.svc = svc
        self.pub = svc.create_direct_message_publisher_builder().build()
        self.pub.start()
        self.kafka = connect_kafka()   # METAR is produced to nisshi (Kafka), not SMF
        self._running = threading.Lock()
        self._abort = threading.Event()   # set by reset() to end a storm's HOLD early

    def _send(self, topic: str, payload: dict) -> None:
        body = bytearray(json.dumps(payload), "utf-8")
        self.pub.publish(destination=Topic.of(topic), message=self.svc.message_builder().build(body))

    def _rate(self, aar: int, cfg: str) -> None:
        self._send(f"TTP/airportInfo/{HERO}",
                   {"airport": HERO, "aar": aar, "adr": aar,
                    "runway_config": cfg, "effective_time": _ts()})

    def _metar(self, cat: str, wx: str, wspd: int, wdir: str) -> None:
        # METAR rides the nisshi Kafka on-ramp (Feldera kafka_input), NOT Solace SMF —
        # so the degraded wind-shift weather reaches Feldera the same way the baseline
        # METAR does. key = station; value = raw JSON bytes (kafka_input json format).
        payload = {"icaoId": HERO, "reportTime": _ts(), "fltCat": cat,
                   "wdir": wdir, "wspd": wspd, "visib": "1" if cat == "IFR" else "10+",
                   "rawOb": f"{HERO} {wx}"}
        self.kafka.send(NISSHI_METAR_TOPIC, key=HERO.encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"))
        self.kafka.flush()

    def run(self) -> None:
        # Wait briefly for the lock rather than dropping the trigger outright. A ↺ Reset
        # sets _abort, which ends a held scenario's hold and releases the lock within
        # milliseconds — but the verified-reset handshake now returns the instant the
        # airport reads CLEAR, so a Reset-then-Wind-shift (UI or smoketest) can fire before
        # that release lands. A short blocking acquire lets the aborted run unwind first so
        # the new fire isn't dropped; a genuinely in-progress (non-aborted) run still wins.
        if not self._running.acquire(timeout=8.0):
            log("scenario already running — ignoring trigger")
            return
        self._abort.clear()   # fresh start — ignore any stale reset signal
        try:
            log(f"WIND SHIFT at {HERO}: AAR {NORMAL_AAR} -> {DEGRADED_AAR} "
                f"(flow {NORMAL_CONFIG} -> {DEGRADED_CONFIG}), +{BURST} arrivals")
            # 1. wind swings SW->NE: reconfigure to single-runway 4R (rate craters) + gusty METAR + GDP
            self._rate(DEGRADED_AAR, cfg=DEGRADED_CONFIG)
            self._metar("VFR", "04030G42KT 10SM FEW050 — wind shift, rwy change to 4R",
                        wspd=30, wdir="040")
            self._send(f"TTP/restriction/{HERO}/GDP",
                       {"airport": HERO, "restriction_type": "GDP",
                        "reason": "Wind shift — runway reconfiguration to single-runway 4R, reduced arrival rate",
                        "scope": "all arrivals", "effective_time": _ts()})
            # 2. burst of bunched near-term inbound arrivals
            base = 1_000_000
            for i in range(BURST):
                cs = random.choice(CARRIERS) + str(random.randint(100, 9999))
                g = f"GUFI{base + i:06d}"
                origin = random.choice(ORIGINS)
                src = random.choices(["TTPFD", "TBFM", "FILED"], weights=[55, 25, 20])[0]
                eta_off = random.randint(300, 1200)  # 5–20 min out: clusters into overlapping HOP bins so the breach sustains across ≥2 bins
                self._send(f"FDPS/flightPlan/{cs}/ACTIVE/{origin}/{HERO}/{g}",
                           {"flight_id": cs, "status": "ACTIVE", "departure": origin,
                            "arrival": HERO, "gufi": g})
                self._send(f"TTP/flightData/{g}/{HERO}/{src}",
                           {"gufi": g, "arrival": HERO, "eta": _ts(eta_off), "eta_source": src})
                time.sleep(0.15)
            log(f"burst published; holding degraded state {HOLD}s")
            # Interruptible hold: a ↺ Reset sets _abort, ending the hold at once and
            # skipping auto-recovery (reset already recovered) so a new storm can fire.
            if self._abort.wait(HOLD):
                log("hold interrupted by reset — skipping auto-recovery")
            else:
                # 3. auto-recovery (natural end of hold)
                log(f"recovery at {HERO}: AAR -> {NORMAL_AAR}, wind back to SW ({NORMAL_CONFIG})")
                self._rate(NORMAL_AAR, cfg=NORMAL_CONFIG)
                self._metar("VFR", "21010KT 10SM CLR", wspd=10, wdir="210")
                self._send(f"TTP/restriction/{HERO}/GDP",
                           {"airport": HERO, "restriction_type": "GDP",
                            "reason": "Cancelled — capacity restored", "scope": "none",
                            "effective_time": _ts()})
            log("scenario complete")
        finally:
            self._abort.clear()
            self._running.release()

    def reset(self) -> None:
        """Immediate recovery for the ↺ Reset button — makes the demo repeatable.
        Restores the published rate + VFR weather, cancels the restriction, and evicts the
        scenario's bunched arrivals from the rolling-hour demand window. The GUFIs are
        deterministic, so each is evicted TWO independent ways (belt-and-suspenders, so a
        broker_ts ordering race on either stream can't leave a flight behind):
          (a) re-publish its ETA +6h — moves it out of the HOP window (newest broker_ts
              wins in eta_latest), and
          (b) re-publish its flight-plan with a non-ACTIVE status — `flight` filters
              WHERE status='ACTIVE' (status comes from the topic segment), so it drops.
        Demand falls to baseline (< AAR), the SEVERE incident clears, the incident-state
        feed publishes CLEAR, and the next wind shift fires a fresh incident. Runs without
        the storm lock so it can recover even mid-hold."""
        # Abort ONLY a wind shift that was already running when this reset began. Checking the
        # lock up front matters: the verified-reset handshake returns the instant the airport
        # reads CLEAR, so a Reset-then-Wind-shift (UI or smoke test) can start the new run while
        # this reset is still publishing — setting _abort unconditionally at the end then killed
        # the fresh run.
        in_progress = self._running.locked()
        log(f"RESET at {HERO}: AAR -> {NORMAL_AAR}, wind back to SW ({NORMAL_CONFIG}), clearing {BURST} bunched arrivals"
            + (" and aborting the in-progress wind shift" if in_progress else ""))
        self._rate(NORMAL_AAR, cfg=NORMAL_CONFIG)
        self._metar("VFR", "21010KT 10SM CLR", wspd=10, wdir="210")
        self._send(f"TTP/restriction/{HERO}/GDP",
                   {"airport": HERO, "restriction_type": "GDP",
                    "reason": "Cancelled — capacity restored", "scope": "none",
                    "effective_time": _ts()})
        far = _ts(6 * 3600)  # +6h — well outside any rolling-hour demand window
        for i in range(BURST):
            g = f"GUFI{1_000_000 + i:06d}"
            # (a) push the ETA out of the rolling window
            self._send(f"TTP/flightData/{g}/{HERO}/TTPFD",
                       {"gufi": g, "arrival": HERO, "eta": far, "eta_source": "TTPFD"})
            # (b) flip the flight-plan out of ACTIVE (status is the 2nd topic segment)
            self._send(f"FDPS/flightPlan/RST{g}/COMPLETED/{HERO}/{HERO}/{g}",
                       {"flight_id": f"RST{g}", "status": "COMPLETED",
                        "departure": HERO, "arrival": HERO, "gufi": g})
        if in_progress:
            self._abort.set()   # free the storm that was mid-hold so the next fire can run
        log("reset complete")


class ControlHandler(MessageHandler):
    def __init__(self, scenario: Scenario):
        self.scenario = scenario

    def on_message(self, message) -> None:
        try:
            topic = message.get_destination_name() or ""
        except Exception:
            topic = ""
        if topic.endswith("/reset"):
            log("reset trigger received")
            threading.Thread(target=self.scenario.reset, daemon=True).start()
        else:
            log("control trigger received")
            threading.Thread(target=self.scenario.run, daemon=True).start()


def main() -> int:
    svc = connect()
    scenario = Scenario(svc)
    receiver = (
        svc.create_direct_message_receiver_builder()
        .with_subscriptions([TopicSubscription.of(CONTROL_TOPIC),
                             TopicSubscription.of(RESET_TOPIC)])
        .build()
    )
    receiver.start()
    receiver.receive_async(ControlHandler(scenario))
    log(f"listening on {CONTROL_TOPIC} + {RESET_TOPIC}; hero={HERO}")

    if os.environ.get("SCENARIO_AUTOSTART", "false").lower() in ("1", "true", "yes"):
        warmup = int(os.environ.get("SCENARIO_WARMUP", "95"))
        log(f"autostart in {warmup}s (letting one baseline pass settle)")
        threading.Timer(warmup, scenario.run).start()

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        receiver.terminate()
        try:
            scenario.kafka.flush()
            scenario.kafka.close()
        except Exception:
            pass
        svc.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

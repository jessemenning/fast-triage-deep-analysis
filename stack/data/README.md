# stack/data — canned SWIM corpus for the replay feed

The demo runs on **real-but-canned** data: a captured (or generated) corpus of SWIM/SCDS
messages that `stack/feed/replay.py` re-publishes over Solace on a timeline. This makes
`make up` fully offline and deterministic — no live FAA feed required.

## Files

| File | What |
|---|---|
| `sample-corpus.jsonl` | Committed baseline corpus. Keeps every airport's rolling-hour demand **below** its AAR, so nothing breaches until the scenario driver degrades the primary airport. |
| `gen_sample_corpus.py` | Regenerates `sample-corpus.jsonl` (pure Python, offline, seeded). |
| `capture.py` | One-time authoring tool: records a window of **real** SWIM traffic from a broker into a replayable corpus. |

## Record format

One JSON object per line:

```json
{"delay": 0.12, "topic": "FDPS/flightPlan/UAL123/ACTIVE/KORD/KMKG/GUFI000042", "payload": {"gufi": "GUFI000042", "arrival": "KMKG", "status": "ACTIVE"}}
```

- `delay` — seconds to wait after the previous record (replay pacing; scaled by `REPLAY_SPEED`).
- `topic` — the Solace topic to publish on.
- `payload` — the JSON body (published as raw bytes so Feldera's `PARSE_JSON` works).

### Now-relative time tokens

Any string of the form `@now`, `@+<seconds>`, or `@-<seconds>` is rewritten by
`replay.py` to a UTC timestamp relative to the **moment of publish**. This is what keeps
captured ETAs, rate effective-times and METAR report-times inside Feldera's live window
no matter when you run the demo. Example: `"eta": "@+900"` → 15 minutes from now.

## Capturing a fresh corpus from real SWIM

Point `capture.py` at a broker carrying the live feeds (e.g. an ingestor bridging the
FAA SWIM mesh), capture a window, commit the result:

```bash
SOLACE_HOST=tcp://<broker>:55555 SOLACE_VPN=<vpn> SOLACE_USER=<u> SOLACE_PASSWORD=<p> \
  python capture.py --out sample-corpus.jsonl --seconds 1800
```

`capture.py` converts the recognised timestamp fields (`eta`, `effective_time`,
`reportTime`, `observed_at`) to `@+N` tokens automatically, so the captured corpus is
immediately replayable and always lands in the live window.

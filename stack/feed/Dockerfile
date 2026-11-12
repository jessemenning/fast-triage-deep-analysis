FROM python:3.12-slim
WORKDIR /app
# solace-pubsubplus for the SMF feeds; kafka-python to produce METAR to the nisshi
# Kafka on-ramp (and to pre-create the nisshi topics via create_topics.py).
RUN pip install --no-cache-dir solace-pubsubplus kafka-python==3.0.13
# Captured SWIM corpus lives in stack/data/ and is mounted read-only at /data
# (DATA_DIR) by docker-compose. Committed sample JSONL makes `make up` work offline;
# refresh with stack/data/capture.py. See stack/data/README.md.
COPY replay.py /app/replay.py
# One-shot used by the `nisshi-topics` compose service to pre-create the Kafka topics.
COPY create_topics.py /app/create_topics.py
# Ops helper used by `make nisshi-peek` to tail a nisshi topic (both directions).
COPY nisshi_peek.py /app/nisshi_peek.py
CMD ["python", "replay.py"]

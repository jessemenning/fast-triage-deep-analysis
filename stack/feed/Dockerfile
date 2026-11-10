FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir solace-pubsubplus
# Captured SWIM corpus lives in stack/data/ and is mounted read-only at /data
# (DATA_DIR) by docker-compose. Committed sample JSONL makes `make up` work offline;
# refresh with stack/data/capture.py. See stack/data/README.md.
COPY replay.py /app/replay.py
CMD ["python", "replay.py"]

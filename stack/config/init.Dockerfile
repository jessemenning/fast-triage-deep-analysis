FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget bash ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY solace/  /config/solace/
COPY feldera/ /config/feldera/

RUN chmod +x /config/solace/setup.sh /config/feldera/init.sh

# Run Solace queue bootstrap first, then load + start the Feldera pipeline.
ENTRYPOINT ["/bin/bash", "-c", \
  "bash /config/solace/setup.sh solace 8080 && sh /config/feldera/init.sh"]

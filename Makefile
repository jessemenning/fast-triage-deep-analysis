# FAA Fast/Slow — Arrival Capacity Demand vs Published Rate. One repo, one command.
#
#   make up         bring the whole platform up, deployed and demo-ready
#   make demo-ready bring up + pre-warm the demo products (live creates become cache hits)
#   make demo       fire the wind shift (hero airport reconfigures runways, AAR drops, demand exceeds it)
#   make down       stop everything (keeps volumes, incl. the warm compile cache)
#   make clean      stop and remove volumes (true cold start — re-run `make demo-ready` after)
#
# The Feldera compile cache persists by default, so the "warm path" (a live product create
# as a cache hit, ~seconds) survives down/up and reboots. `make prewarm` (run by demo-ready)
# compiles the demo products once; after that it stays warm until `make clean`.
#
# Prereqs (see README): Docker, the `sam` CLI 2.409.2 on PATH, a filled .env,
# and `docker login` to the SAM image registry.
#
# The data plane is fully Solace-native: the base Continuum pipeline publishes the
# arrival-capacity breach trigger directly (fl/faa/arrival-capacity/breach/{airport}),
# so `make up` needs no runtime data-product. `make create-product` is a SEPARATE
# demo of the live generation engine (continuum-mcp), not a dependency of the demo.

SAM ?= sam-2.409.2
# The Feldera compile cache persists by DEFAULT (the cache overlay is always on), so the
# "warm path" survives down/up and host reboots — identical product SQL is a cache hit
# (seconds) instead of a cold compile (~7–8 min). This persists build artifacts + pipeline
# metadata only; the data plane is still fully event-sourced (no stored business state).
# `make clean` (down -v) wipes the cache for a true cold start; `make up-ephemeral` runs
# once without the cache (the old no-volume engine behaviour, e.g. for CI).
COMPOSE ?= docker compose -f docker-compose.yml -f docker-compose.cache.yml

SAM_RELEASE_TAG ?= 2.409.2-5ae9afd958
SAM_REPO ?= SolaceDev/solace-agent-mesh-go

# Prewarm the demo data products at the end of `make stack` (precreate -> delete, leaving the
# compile cache warm so a LIVE create in the demo is a cache hit, not a ~7-8 min compile).
# Set PREWARM=0 to skip (quick SQL iteration); auto-disabled for `make up-ephemeral`.
PREWARM ?= 1

# The native decision node (#4241) is behind a feature flag, off by default. The host
# CLI needs it to parse/apply a `type: decision` workflow; the runtime gets the same
# flag via docker-compose. `export` makes it visible to every `config apply` recipe.
export SAM_FEATURE_WORKFLOW_DECISION_NODE ?= true

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: doctor
doctor: ## Pre-flight: check Docker, image, keys, CLI, tooling — prints ✓/✗ with fixes
	@demo/doctor.sh

.PHONY: preflight
preflight: ## Check .env, sam CLI, and the SAM image are present
	@test -f .env || { echo "!! no .env — run: cp .env.example .env && edit it"; exit 1; }
	@command -v $(SAM) >/dev/null || { echo "!! sam CLI '$(SAM)' not on PATH (need 2.409.2; rollback: SAM=sam-2.380.0)"; exit 1; }
	@grep -q '^LLM_SERVICE_API_KEY=.\+' .env || { echo "!! LLM_SERVICE_API_KEY is empty in .env (powers both the fast decision node and the System-2 agents)"; exit 1; }
	@IMG=$$(grep -E '^SAM_IMAGE=' .env | cut -d= -f2-); \
	  IMG=$${IMG:-solace-agent-mesh:2.409.2}; \
	  docker image inspect "$$IMG" >/dev/null 2>&1 || \
	    echo "!! SAM image $$IMG not present — run: make pull-image (downloads the official prebuilt release image)";
	@SImg=$$(grep -E '^SIDECAR_IMAGE=' .env 2>/dev/null | cut -d= -f2-); \
	  SCtx=$$(grep -E '^SIDECAR_CONTEXT=' .env 2>/dev/null | cut -d= -f2-); SCtx=$${SCtx:-../continuum-solace-connector}; \
	  if [ -n "$$SImg" ] && docker image inspect "$$SImg" >/dev/null 2>&1; then echo "✓ sidecar image $$SImg present"; \
	  elif [ -f "$$SCtx/docker/Dockerfile" ]; then echo "✓ sidecar build context $$SCtx present (compose will build it)"; \
	  else echo "!! sidecar not available: no SIDECAR_IMAGE and no repo at $$SCtx — clone continuum-solace-connector beside this repo or set SIDECAR_CONTEXT/SIDECAR_IMAGE in .env"; fi

.PHONY: pull-image
pull-image: ## Download + docker-load the official prebuilt SAM runtime image from the GitHub release
	@IMG=$$(grep -E '^SAM_IMAGE=' .env 2>/dev/null | cut -d= -f2-); IMG=$${IMG:-solace-agent-mesh:2.409.2}; \
	if docker image inspect "$$IMG" >/dev/null 2>&1; then echo "✓ $$IMG already present"; exit 0; fi; \
	command -v gh >/dev/null || { echo "!! gh CLI not found. Install it + 'gh auth login' (needs read access to $(SAM_REPO)), then re-run."; exit 1; }; \
	case "$$(uname -m)" in x86_64|amd64) A=amd64;; aarch64|arm64) A=arm64;; *) echo "!! unsupported arch $$(uname -m)"; exit 1;; esac; \
	echo "downloading official prebuilt SAM image ($$A) from release $(SAM_RELEASE_TAG) ..."; \
	TMP=$$(mktemp -d); \
	gh release download "$(SAM_RELEASE_TAG)" --repo "$(SAM_REPO)" --pattern "*app-$$A.tar.gz" --dir "$$TMP" --clobber \
	  || { echo "!! gh release download failed — check 'gh auth status' and read access to $(SAM_REPO)"; rm -rf "$$TMP"; exit 1; }; \
	echo "docker load (~800MB, takes a minute) ..."; \
	docker load -i "$$TMP"/*app-$$A.tar.gz; rm -rf "$$TMP"; \
	docker image inspect "$$IMG" >/dev/null 2>&1 && echo "✓ loaded $$IMG" \
	  || echo "!! loaded, but not tagged '$$IMG' — check 'docker images solace-agent-mesh' and SAM_IMAGE in .env";

.PHONY: build
build: ## Build the local images (MCP servers, init, feed, scenario, api)
	$(COMPOSE) build

.PHONY: build-sidecar
build-sidecar: ## (Re)build the feldera-solace sidecar image from the sibling repo (OSS, non-FIPS)
	$(COMPOSE) build sidecar

.PHONY: stack
stack: pull-image preflight ## Start containers (no apply); wait for SAM health, then prewarm demo products (PREWARM=0 to skip)
	$(COMPOSE) up -d --build
	@echo "waiting for SAM platform on http://localhost:8800 ..."
	@ok=0; for i in $$(seq 1 90); do curl -fs http://127.0.0.1:8800/healthz >/dev/null 2>&1 && { echo "SAM ready"; ok=1; break; }; sleep 2; done; \
	  [ "$$ok" = 1 ] || { echo "!! SAM did not become healthy — docker compose logs sam"; exit 1; }
	@if [ "$(PREWARM)" != "0" ]; then \
	  echo "prewarming demo products (precreate -> delete; compile cache stays warm) ..."; \
	  demo/prewarm.sh || echo "!! prewarm failed — run 'make prewarm' manually (stack is up)"; \
	 fi

.PHONY: apply
apply: ## Deploy the platform layer (models + connectors the reference solution binds to)
	cd sam && $(SAM) config apply --manifest manifests/dev.yaml

.PHONY: apply-reference
apply-reference: ## Deploy the canned arrival-capacity solution (gateway + fast/slow workflow)
	# The base pipeline already publishes the breach trigger, so this is a plain
	# declarative apply — no runtime product, no workflow-id resolver, no restart.
	cd sam && $(SAM) config apply --manifest solutions/arrival-capacity/manifests/dev.yaml

.PHONY: up-cached
up-cached: up ## Alias for `make up` (the persistent compile cache is now on by default)

.PHONY: up-ephemeral
up-ephemeral: COMPOSE = docker compose
up-ephemeral: PREWARM = 0
up-ephemeral: up ## Bring up WITHOUT the persistent compile cache (ephemeral engine; cold compiles)

.PHONY: check-gateway
check-gateway: ## Assert the arrival-capacity gateway has a BOUND broker consumer (sam can report healthy with gateways=0; this catches the silent trigger-down state)
	@python3 demo/check-gateway.py

.PHONY: up
up: stack apply apply-reference ## Bring everything up, deployed and demo-ready
	@echo "verifying the gateway consumer is bound (incident trigger wired) ..."
	@python3 demo/check-gateway.py || { echo "!! gateway trigger is DOWN — the wind shift will not reach SAM; see remediation above"; exit 1; }
	@echo
	@echo "Ready. The arrival-capacity fast/slow solution is deployed on the live data plane."
	@echo "Watch the demo at http://localhost:8080/demo.html"
	@echo "Or from the terminal:  make ops   (watch)   and   make demo   (fire the wind shift)"

.PHONY: prewarm
prewarm: ## Arm the warm path: compile every demo/products/*.json into the persistent cache, then clear the records (live creates become cache hits)
	@demo/prewarm.sh

.PHONY: demo-ready
demo-ready: up ## Cold-start → fully warm: bring everything up (stack prewarms the demo products so live creates are cache hits; run once per machine / after `make clean`)
	@echo
	@echo "Warm path armed. Live product creates are now cache hits (seconds)."
	@echo "The cache persists across down/up + reboot — re-run only after 'make clean'."

.PHONY: create-product
create-product: ## DEMO the generation engine: build a Continuum data product + trigger via the MCP server
	@TOK=$$(grep -E '^CONTINUUM_MCP_TOKEN=' .env 2>/dev/null | cut -d= -f2- | sed 's/#.*//;s/[[:space:]]*$$//'); \
	  CONTINUUM_MCP_TOKEN="$${TOK:-continuum-dev-token-local}" python3 demo/create-product.py

.PHONY: register-ep
register-ep: ## Register the full event mesh in Event Portal: SWIM → Continuum → SAM → Console (needs SOLACE_API_TOKEN)
	@$(COMPOSE) cp demo/register-ep-topology.py continuum-mcp:/app/register-ep-topology.py
	@$(COMPOSE) exec -T continuum-mcp sh -c '/app/.venv/bin/python /app/register-ep-topology.py'

# ── AI-generated scenario solutions (see .claude/skills/faa-generate-solution) ──
.PHONY: register-ep-solution
register-ep-solution: ## Wire a generated solution's SAM app + decision events into EP. SLUG=<slug> TRIGGER=<name> [KEY=airport]
	@test -n "$(SLUG)" || { echo "usage: make register-ep-solution SLUG=<slug> TRIGGER=<trigger> [KEY=airport]"; exit 2; }
	@$(COMPOSE) cp demo/register-ep-solution.py continuum-mcp:/app/register-ep-solution.py
	@$(COMPOSE) exec -T continuum-mcp sh -c "/app/.venv/bin/python /app/register-ep-solution.py $(SLUG) --trigger $(TRIGGER) --key $(or $(KEY),airport)"

.PHONY: smoketest
smoketest: ## End-to-end readiness check of the base arrival-capacity solution (fires the wind shift). [AIRPORT=KEWR] [TIMEOUT=150] [RESET_WAIT=15] [NORESET=1]
	uv run --no-project --with 'paho-mqtt>=2' python demo/smoketest.py $(if $(AIRPORT),--airport $(AIRPORT),) $(if $(TIMEOUT),--timeout $(TIMEOUT),) $(if $(RESET_WAIT),--reset-wait $(RESET_WAIT),) $(if $(NORESET),--no-reset,)

.PHONY: verify-solution
verify-solution: ## Headless end-to-end check of a generated solution. SLUG=<slug> TRIGGER=<name> [NODRIVE=1]
	@test -n "$(SLUG)" || { echo "usage: make verify-solution SLUG=<slug> TRIGGER=<trigger> [NODRIVE=1]"; exit 2; }
	uv run --no-project --with 'paho-mqtt>=2' python demo/verify-solution.py $(SLUG) --trigger $(TRIGGER) $(if $(NODRIVE),--no-drive,)

.PHONY: new-solution
new-solution: ## One-command deploy+wire+verify of an authored generated solution. SLUG=<slug> TRIGGER=<name> [KEY=airport]
	@test -n "$(SLUG)" || { echo "usage: make new-solution SLUG=<slug> TRIGGER=<trigger> [KEY=airport]"; exit 2; }
	demo/new-solution.sh $(SLUG) $(TRIGGER) $(or $(KEY),airport)

.PHONY: ops
ops: ## Watch triggers and decisions on the mesh
	python3 demo/ops-console.py

.PHONY: demo
demo: ## Fire the wind shift at the hero airport (runway reconfig -> AAR drops + arrival burst -> breach)
	demo/burst.sh

.PHONY: prune-queues
prune-queues: ## Remove orphaned SAM gateway queues from earlier runs
	demo/prune-orphan-queues.sh --yes

.PHONY: reset
reset: ## Re-arm the demo: redeploy the reference solution
	@demo/prune-orphan-queues.sh --yes >/dev/null 2>&1 || true
	@$(MAKE) apply-reference
	@echo "reset: the arrival-capacity solution was redeployed. Watch http://localhost:8080/demo.html, then: make ops / make demo"

.PHONY: pipeline-status
pipeline-status: ## Show the base Continuum pipeline status
	@curl -s http://localhost:8081/v0/pipelines/arrival-capacity | \
	  python3 -c "import sys,json; d=json.load(sys.stdin); print({k:d.get(k) for k in ('name','program_status','deployment_status')})"

.PHONY: fq
fq: ## Run a SQL query against the base pipeline:  make fq SQL="SELECT ..."
	@curl -s -G http://localhost:8081/v0/pipelines/arrival-capacity/query \
	  --data-urlencode "sql=$(SQL)" --data-urlencode format=json

.PHONY: logs
logs: ## Tail logs for all services
	$(COMPOSE) logs -f --tail=50

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: down
down: ## Stop everything (keeps data volumes)
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop and remove volumes (fresh start next `make up`)
	$(COMPOSE) down -v

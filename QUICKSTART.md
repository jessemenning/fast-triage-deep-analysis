# FAA Fast/Slow — Laptop Quick Start

Run the whole demo locally with Docker. First run is ~15 min (it pulls images and does a one-time
Feldera compile); after that, startups are fast. For what the demo *is*, see [README.md](README.md).

## Access you need first
This pulls from three private GitHub repos and one LLM endpoint. Make sure you have:

- **GitHub read access** to `jessemenning/fast-triage-deep-analysis`, `SolaceDev/continuum-solace-connector`,
  and `SolaceDev/solace-agent-mesh-go` (the licensed SAM image + CLI). You're a Solace dev, so you
  likely already do — confirm with `gh auth status`.
- **An LLM endpoint + key** (required — the triage decision node and the System-2 agents run on it).
  The Solace MaaS LiteLLM proxy works out of the box; any OpenAI-compatible endpoint also works.
  Ask Jesse for the endpoint/model names if you don't have your own.

## Prerequisites
- **Docker Desktop** (Mac/Windows) — includes Compose v2 and Buildx. On Linux with bare Docker
  Engine, also install the `docker-compose-plugin` and `docker-buildx-plugin` (the image builds
  need Buildx ≥ 0.17).
- **`make`, `git`, `gh`** (GitHub CLI), and **Python 3.10+** (`python3 --version` — the helper
  scripts use 3.10 syntax; 3.9 will fail).
- ~8 GB free RAM for Docker and ~15 GB disk.

## Steps

```bash
# 1. Authenticate gh (once) — needs read access to the SolaceDev repos above.
gh auth login

# 2. Clone this repo AND the sidecar repo SIDE BY SIDE (the sidecar builds from ../).
git clone https://github.com/jessemenning/fast-triage-deep-analysis.git
git clone https://github.com/SolaceDev/continuum-solace-connector.git
cd fast-triage-deep-analysis

# 3. Pull the licensed SAM runtime image (gh release download + docker load).
make pull-image

# 4. Install the matching SAM CLI as `sam-2.409.2` on your PATH.
#    Adjust the --pattern for your OS/arch: darwin-arm64 (Apple Silicon), darwin-amd64 (Intel Mac),
#    linux-amd64. Then put the extracted `sam` binary on PATH named sam-2.409.2.
gh release download 2.409.2-5ae9afd958 --repo SolaceDev/solace-agent-mesh-go \
  --pattern '*cli-darwin-arm64*' --dir /tmp/samcli --clobber
tar -xzf /tmp/samcli/*.tar.gz -C /tmp/samcli
sudo install /tmp/samcli/sam /usr/local/bin/sam-2.409.2      # verify: sam-2.409.2 --version

# 5. Configure the LLM endpoint + key.
cp .env.example .env
#   Edit .env and set at minimum:
#     LLM_SERVICE_ENDPOINT=https://<your-litellm-or-openai-endpoint>      (or your OpenAI-compatible endpoint)
#     LLM_SERVICE_API_KEY=<your key>
#   The model-name defaults in .env.example work on the Solace MaaS proxy.

# 6. Pre-flight check (optional but recommended — prints ✓/✗ with fixes).
make doctor

# 7. Bring it all up (first run ~15 min: image pulls + cold Feldera compile).
make demo-ready
```

When it finishes, open **http://localhost:8080/demo.html**.

## Run the demo
```bash
make demo     # fire the wind shift at KEWR → AAR drops, demand exceeds it → breach
make reset    # re-arm (airport back to green) so you can fire again
make ops      # optional: watch triggers + decisions on the mesh in the terminal
```
In the UI: KEWR goes red, the alert storm collapses into one escalated incident, and the fast triage
(ESCALATE) is followed by a System-2 decision brief with options, tradeoffs, and provenance. The
**Agent Mesh UI** button opens the SAM runtime UI (http://localhost:8800 locally).

## If something's off
- **`make demo-ready` fails during image builds** → Buildx missing/old. Docker Desktop has it; on
  Linux install `docker-buildx-plugin`.
- **A Python traceback** mentioning `str | None` → your `python3` is < 3.10. Install 3.11+.
- **`sam-2.409.2: command not found`** → the CLI isn't on PATH under that exact name (step 4).
- **`make pull-image` fails** → `gh auth status`; confirm read access to `SolaceDev/solace-agent-mesh-go`.
- **Sidecar build error** → the `continuum-solace-connector` repo isn't cloned next to this one (step 2).
- **Decision brief stays blank** → check `LLM_SERVICE_ENDPOINT`/`LLM_SERVICE_API_KEY` in `.env`.
- Full pre-flight: `make doctor`. Ground-truth architecture + ops: [CLAUDE.md](CLAUDE.md).

## Teardown
```bash
make down     # stop everything, keep the warm compile cache (fast next start)
make clean    # also drop volumes (next start is a full cold compile again)
```

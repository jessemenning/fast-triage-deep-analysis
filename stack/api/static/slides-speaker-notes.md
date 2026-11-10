# Speaker notes — `slides.html`

Brief talk track for presenting the deck. One event's journey up the three
layers of the platform. Navigate with ← / → (or Space), click the left/right
screen edges, or the dots along the bottom. 8 slides, ~6–7 min.

A thin section bar (top-left of each slide) signals which layer it covers —
**Real-time AI Platform** (slides 1–2), **Event Mesh** (3), **Continuum** (4–6),
**Agent Mesh** (7), then a **Live Demo** bridge (8).

---

## 1 · Move. Understand. Act.
*On screen: three stacked layers inside "Solace Real-time AI Platform" — Event Mesh (move), Continuum (understand), Agent Mesh (act).*

- The platform is three layers. **Event Mesh** moves events everywhere. **Continuum** turns raw streams into understanding. **Agent Mesh** acts on it with AI.
- Each layer builds on the one below.
- **Transition:** "The rest of the deck follows one event up through these layers."

## 2 · From Firehose to Result
*On screen: a funnel narrowing from "the real-time firehose" down to "in-depth LLM," bracketed by which layer owns each step.*

- You never point an LLM at the firehose. Event Mesh filters by topic; Continuum consolidates context across time and domains and fires a **deterministic** trigger only when something genuinely matters; Agent Mesh runs a fast triage and escalates only the few that need a deep look.
- Each stage cuts volume so the expensive AI only ever sees what counts.
- **Transition:** "The funnel's first cut happens at the bottom layer — so let's start there: the Event Mesh."

## 3 · A Proven Foundation, Extended
*On screen: enterprise endpoints (apps, microservices, databases, mainframe, SaaS, cloud, edge/IoT, partners) → one **Event Mesh** core ("proven in production, enterprise-wide") → an "extend — don't replace" callout.*

- This is the **proven foundation** — the Event Mesh has moved events reliably across the entire enterprise for years: battle-tested, enterprise-wide connectivity plus topic-based routing (publish once; each consumer subscribes to exactly the topics it needs).
- The key message: we are **not replacing it**. Continuum and Agent Mesh **extend these proven strengths** to conquer new, real-time and agentic challenges — no rip-and-replace, no new fabric to stand up.
- This audience knows Event Mesh — keep it to ~30 seconds: it's the trusted base everything above builds on.
- **Transition:** "On that proven foundation, Continuum turns the streams into reusable understanding."

## 4 · Build It Once, Feed Everything
*On screen: source streams arrive over the **Event Mesh** ("filtered events") → a Continuum hub of **value-added data products** → out to many consumers via Events, MCP, and SQL.*

- **The handoff:** the source streams come in over the Event Mesh, already topic-filtered — Continuum picks up exactly the events it needs. (The proven foundation feeding the new layer.)
- Build a **value-added data product** once; everything consumes it — Agent Mesh, vendor AI, dashboards, external systems — **pushed** as events or **pulled** via MCP and SQL.
- One governed source of truth, many consumers. No re-deriving the same logic in five places.
- **Transition:** "And under the hood, that product stays current with almost no compute…"

## 5 · Compute the Change, Not the World
*On screen: a small pipeline — source streams → join/normalize → aggregate over a window → detect → result event — with three why-it-matters callouts: Affordable at scale · Never stale · Ship in minutes.*

- The headline is the mechanic as a principle: Continuum recomputes **only what changed**, never the whole dataset. Read it, pause a beat, then go straight to why it matters.
- **Affordable at scale:** computing the delta instead of the world makes always-on, sub-second analytics cheap enough to run on **every** stream — not just the one you could afford to batch.
- **Never stale:** every answer reflects what's true this instant, so decisions act on live reality instead of last night's batch.
- **Ship in minutes:** a new real-time capability is a few lines of SQL — instantly a service, not a data-engineering project.
- **Transition:** "Computed this way, the context it hands the AI is always current and trustworthy."

## 6 · Analysis-Ready Context
*On screen: "every trigger is a business object" (full business context, provably correct, deterministic, ultra-filtered) → "Services · People · AI — all benefit" (faster, cheaper, more accurate).*

- The point that's distinct from the engine slide: it's not just that Continuum is fast — it's **what the trigger carries**. Every trigger is a **business object that models the full situation** — not a raw row or a bare alert code — so the consumer gets the complete picture in one payload.
- **Full context:** everything relevant to the decision — provably correct and deterministic. A service, a person, or an AI agent acts immediately with **no data-gathering**, so it's **faster, cheaper, and more accurate**.
- **Transition:** "Here's how the AI layer actually uses it."

## 7 · Fast Triage, Deep Investigation
*On screen: a sky pill — "Continuum delivers the incident + its context" — drops into one **Solace Agent Mesh** box with two cards, Fast (decision model) and Deep (LLM investigation), an **ESCALATE-only** gate between them, and a dashed **MCP → Continuum** node to the right of the box (outside Agent Mesh); property chips: Real-time at scale · Deploy on your terms · Triggered by events.*

- **The handoff (continuity):** the incident and its context arrive straight from Continuum — Agent Mesh doesn't gather data, it reasons on context that's already analysis-ready.
- Agent Mesh runs a **fast / deep** pattern. The fast path is a sub-second decision model that triages every event — suppress, group, or escalate — so the alert storm never reaches a human.
- **The gate:** if triage says suppress or group, **no investigation happens** — it stops there. Only an **ESCALATE** opens the deep path, where specialist LLMs take a little longer to reason — **calling out via MCP to query Continuum** for live context — then write an advisory brief.
- **Why Agent Mesh (chips as outcomes):** Go-compiled, so it triages the full firehose **in real time at scale**; a single binary you **deploy on your terms** — your cloud, on-prem, or edge, no lock-in; and it's **event-native** — agents are **triggered by events** in real time (no polling, no batch), woven straight into the flow.
- **Close:** fast triage on everything, deep investigation only on what Continuum flagged as real — the whole funnel, paying off.

## 8 · See It Live (bridge to the demo)
*On screen: a neutral "Live Demo" bar; centered "See It Live" with "Event Mesh → Continuum → Agent Mesh — end to end."*

- One beat to switch from the deck to the running demo. Say: "That's the platform — now let's watch one event flow all the way through, end to end."
- Open the UI (demo.html) and go.
- Accuracy note for the presenter: the stack runs and processes in real time, but the data is a **scripted, replayed scenario** (a canned corpus), not live production feeds — don't claim the events are live.

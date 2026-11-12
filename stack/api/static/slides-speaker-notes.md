# Speaker notes — `slides.html`

Brief talk track for presenting the deck. One event's journey up the three
layers of the platform. Navigate with ← / → (or Space), click the left/right
screen edges, or the dots along the bottom. 9 slides, ~7–8 min.

A thin section bar (top-left of each slide) signals which layer it covers —
**Real-time AI Platform** (slides 1–3), **Event Mesh** (4), **Continuum** (5–7),
**Agent Mesh** (8), then a **Live Demo** bridge (9).

---

## 1 · Real-Time AI Needs a Real-Time Platform
*On screen: the headline over three challenge cards, one per layer (Your data's scattered everywhere · Raw events aren't answers · Agents can't work together), closing on "That takes a platform built for real-time AI — not an LLM bolted onto yesterday's stack."*

- **Open on the prize, then the catch.** The payoff is AI that responds the instant something business-critical happens. The challenge is that you can't get there just by pointing an LLM at your event firehose.  And in case you're wondering, as someone who has tried to write a lot of demos, I've tried that approach.
- **Your data's scattered everywhere:** the information AI needs is spread across apps, the mainframe, cloud, edge, SaaS, and partners — and it has to arrive fast, in order, and without loss. Point-to-point integration gives you neither the reach nor that kind of delivery at scale. The Event Mesh answer (slide 4): one single logical mesh that moves events to and from anywhere in the enterprise with high-speed, guaranteed, in-order delivery.
- **Raw events aren't answers:** AI needs consolidated, predigested context — not a flood of events like Solace traditionally has delivered. And micro-batching it through a warehouse isn't the answer either: by the time the warehouse refreshes, the moment that needed the decision can be long gone.
- **Agents can't work together:** real responses come from many kinds of agents across the enterprise — some reacting in milliseconds, others collaborating as a team to dig deep — but they're usually disconnected point tools with no common mesh to connect them. (The Agent Mesh answer — one mesh that connects and orchestrates agents of every type — lands on slide 8.)
- **The through-line:** each of these is a challenge the rest of the deck answers, layer by layer — so this slide doubles as the roadmap.
- **Transition:** "So what does that platform look like? Three layers."

## 2 · Move. Understand. Respond.
*On screen: three stacked layers inside "Solace Real-time AI Platform" — Event Mesh (move), Continuum (understand), Agent Mesh (respond).*

- The platform is three layers. **Event Mesh** moves events everywhere. **Continuum** turns raw streams into understanding. **Agent Mesh** responds — a mesh of agents acting on that understanding.
- Each layer builds on the one below.
- **Transition:** "The rest of the deck follows one event up through these layers."

## 3 · From Firehose to Result
*On screen: a funnel narrowing from "the real-time firehose" down to "in-depth LLM," bracketed by which layer owns each step.*

- You never point an LLM at the firehose. Event Mesh filters by topic; Continuum consolidates context across time and domains and fires a **deterministic** trigger only when something genuinely matters; Agent Mesh runs a fast triage and escalates only the few that need a deep look.
- Each stage cuts volume so the expensive AI only ever sees what counts.
- **Transition:** "The funnel's first cut happens at the bottom layer — so let's start there: the Event Mesh."

## 4 · A Proven Foundation, Extended
*On screen: enterprise endpoints (apps, microservices, databases, mainframe, SaaS, cloud, edge/IoT, partners) → one **Event Mesh** core → a callout on building on those strengths, not replacing them.*

- This is the **proven foundation** — the Event Mesh has moved events reliably across the enterprise for years: battle-tested connectivity, topic-based routing (publish once; each consumer subscribes to exactly the topics it needs), and high-speed, guaranteed, in-order delivery to and from anywhere. (This pays off the "fast, in order, without loss" promise from the slide-1 Event Mesh card.)
- The key message: we are **not replacing it**. Continuum and Agent Mesh **extend these proven strengths** to conquer new, real-time and agentic challenges — no rip-and-replace, no new mesh to stand up.
- This audience knows Event Mesh — keep it to ~30 seconds: it's the trusted base everything above builds on.
- **Transition:** "On that proven foundation, Continuum turns the streams into reusable understanding."

## 5 · Build It Once, Feed Everything
*On screen: source streams (Flights, Weather, Rates, ETAs, Restrictions) arrive over the **Event Mesh** ("filtered events") → a Continuum hub of **value-added data products** → out to many consumers (Agent Mesh, Vendor AI, Program tools, Dashboards, External consumers) via Events, MCP, and SQL.*

- **The handoff:** the source streams come in over the Event Mesh, already topic-filtered — Continuum picks up exactly the events it needs. (The proven foundation feeding the new layer.)
- Build a **value-added data product** once; everything consumes it — Agent Mesh, vendor AI, program tools, dashboards, external consumers — **pushed** as events or **pulled** via MCP and SQL.
- One governed source of truth, many consumers. No re-deriving the same logic in five places.
- **Transition:** "And under the hood, that product stays current with almost no compute…"

## 6 · Compute the Change, Not the World
*On screen: a small pipeline — source streams → Join & normalize → Aggregate over a window → Detect condition → Result event (each stage a materialized view; the result emitted on change) — with three why-it-matters callouts: Affordable at scale · Never stale · Ship in minutes.*

- The headline is the mechanic as a principle: Continuum recomputes **only what changed**, never the whole dataset. Read it, pause a beat, then go straight to why it matters.
- **Affordable at scale:** computing the delta instead of the world makes always-on, sub-second analytics cheap enough to run on **every** stream — not just the one you could afford to batch.
- **Never stale:** every answer reflects what's true this instant, so decisions act on live reality instead of last night's batch.
- **Ship in minutes:** a new real-time capability is a few lines of SQL — instantly a service, not a data-engineering project.
- **Transition:** "Computed this way, the context it hands the AI is always current and trustworthy."

## 7 · Analysis-Ready Context
*On screen: "every trigger is a business object" (full business context, provably correct, deterministic, ultra-filtered) → "Services · People · AI" (faster, cheaper, more accurate).*

- The point that's distinct from the engine slide: it's not just that Continuum is fast — it's **what the trigger carries**. Every trigger is a **business object that models the full situation** — not a raw row or a bare alert code — so the consumer gets the complete picture in one payload.
- **Full context:** everything relevant to the decision — provably correct and deterministic. A service, a person, or an AI agent acts immediately with **no data-gathering**, so it's **faster, cheaper, and more accurate**.
- **Transition:** "Here's how the AI layer actually uses it."

## 8 · Fast Triage, Deep Investigation
*On screen: a sky pill — "Continuum delivers the incident + its context" — drops into one **Solace Agent Mesh** box with two cards, Fast (decision model) and Deep (LLM investigation), an **ESCALATE-only** gate between them, and a dashed **MCP → Continuum** node to the right of the box (outside Agent Mesh); property chips: Any agent, one mesh · Real-time at scale · Deploy anywhere.*

- **The handoff (continuity):** the incident and its context arrive straight from Continuum — Agent Mesh doesn't gather data, it reasons on context that's already analysis-ready.
- **One mesh, many agent types:** Agent Mesh connects and orchestrates agents of every kind across the enterprise — the Fast decision model and the Deep investigation team on screen are just two. It runs a **fast / deep** pattern: the fast path is a sub-second decision model that triages every event — suppress, group, or escalate — so the alert storm never reaches a human.
- **The gate:** if triage says suppress or group, **no investigation happens** — it stops there. Only an **ESCALATE** opens the deep path, where specialist LLMs take a little longer to reason — **calling out via MCP to query Continuum** for live context — then write an advisory brief.
- **One harness, fast → deep:** both paths live in the same high-performance runtime, so escalation is a handoff *inside* one mesh — not a jump to a separate system. Triage runs in milliseconds; the instant something escalates, the deep investigation spins up in the same harness, no re-platforming and no glue code between the two. Speed on every event, depth only when it's earned.
- **Why Agent Mesh (the three chips):** **any agent, one mesh** — a fast reactive agent, a team of deep specialists, and whatever else you build, all orchestrated together; **real-time at scale** — Go-compiled, so it triages the full firehose, event-native with no polling or batch; **deploy anywhere** — a single binary you run on your own cloud, on-prem, or edge, with any model, no lock-in. If it helps, add the maturation story: it starts advisory (briefs for a human) and graduates to autonomous as confidence grows.
- **Close:** fast triage on everything, deep investigation only on what Continuum flagged as real — the whole funnel, paying off.

## 9 · See It Live (bridge to the demo)
*On screen: a neutral "Live Demo" bar; centered "See It Live" with "Event Mesh → Continuum → Agent Mesh — end to end."*

- One beat to switch from the deck to the running demo. Say: "That's the platform — now let's watch one event flow all the way through, end to end."
- Open the UI (demo.html) and go.
- Accuracy note for the presenter: the stack runs and processes in real time, but the data is a **scripted, replayed scenario** (a canned corpus), not live production feeds — don't claim the events are live.

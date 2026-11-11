# FAA Fast/Slow — Talk Script

Demo: arrival-capacity demand vs published rate, on the Solace Platform three-layer stack.
Run it with `make demo`; see [README.md](README.md) and [QUICKSTART.md](QUICKSTART.md) to stand it up.


**[Open — the problem, and why now]**

If I was an enterprise architect these days, what I'd be focused on is AI that responds the moment something business-critical happens. Not AI you have to go ask — AI that's already watching, and acts when it matters.

The trouble is that the obvious way to build that — pipe raw events straight into an LLM — is slow, expensive, and non-deterministic. So this whole talk is about a better way to do real-time AI, and I'm going to run it on one example the whole way through.

Here's the example. Picture an air traffic manager watching a busy hub. When the flights expected to arrive start to outrun the rate the airport can actually land them — its published arrival rate — today's tools raise an alert. And then another, and another, as the estimates shift and the rate gets bumped. Most of that is noise. A few are the real thing: the leading edge of delays that cascade across the whole national airspace. The manager has to tell which is which, then work out the why — pull up weather, restrictions, runway config, by hand, under time pressure. That's the problem we're solving, and it comes down to three parts.

**[Part 1 — the data]**

The first part is that the data isn't where you want it. To make a good call on an airport, AI needs events from every system — the flight plans and position reports, the published arrival rates, the per-flight ETAs, the weather, the restrictions already in effect. And it doesn't stop at aviation: apps, mainframe, cloud, edge, partners. It all has to arrive fast, in the right order, and without loss. And as we've seen through the years, point-to-point wiring just can't keep up.

**[Part 2 — the context]**

But having those raw events come in from everywhere still isn't enough. What AI needs is consolidated, analysis-ready context. A single position report for one airplane doesn't tell you anything. The question is about the airport: how many planes are now expected in the next hour, and is that more than this airport can land right now? Is a restriction already in play? And throwing it all into a warehouse — even at a micro-batch level — doesn't give you that answer. It has to be kept current as every event lands, and it has to trigger immediately.

**[Part 3 — the agents]**

And finally, even having that context correlated isn't enough. Picture the moment expected demand crosses the airport's rate. One agent isn't enough to handle that well. You want a fast agent that decides whether this one even matters, and then — only if it does — a weather agent, a downstream agent, a trend agent, all pulled in together to work out what's happening and what the options are. As we move into these differentiated agents — some that move really fast, some with deep analysis skills, some holding proprietary information — the thing that gives you complete answers is their ability to work together.

All three of these problems are interrelated. What it takes is a coherent platform built for real-time AI — not "let's take an off-the-shelf LLM and cram it into an existing stack," but something designed to solve them. The rest of this connects those three things, and then we'll watch it run.

**[The three layers]**

Solace's real-time AI platform is three layers.

First is the **Event Mesh** — which some of you know and love. That's our heritage, our bread and butter. It moves events.

**Continuum** is new. It turns those raw streams of real-time information into understanding.

And the **Agent Mesh** sits on top, takes that consolidated context, and creates a mesh of agents to respond. Here that response is advisory — a brief for a human — and I'll come back to why that line matters for this use case.

**[The funnel — fast, one line per stage]**

The shape is always the same: we take a fire hose of information and funnel it down to a result. Topic-based filtering takes the first cut. Continuum turns the events that survive into a value-added business object that cuts across time and feeds. Deterministic triggers fire only when something is genuinely business-critical. The Agent Mesh triages those triggers and escalates the few that need a real decision. By the time we're spending money on agents, we're not looking at the fire hose anymore — we're looking at exactly what matters. Let me take those one at a time.

**[Deep dive — Event Mesh]**

The Event Mesh is a proven foundation. It's moved events reliably across enterprises for years, in some of the harshest environments in the world — real-time stock trading, air traffic control, and plenty more. So for this one we're on home turf.

If you don't know the basics: consumers subscribe very precisely to only the events they need, and when those arrive, it's extreme high-speed, guaranteed in-order delivery from anywhere across the enterprise. And it runs the other way too — you publish into the mesh from anywhere: microservices, databases, mainframes, SaaS, you name it. For this demo, every FAA feed comes in that way. A rate change isn't a database row we poll — it *is* an event on the mesh. Everything else in the platform builds on that.

**[Deep dive — Continuum]**

Those source streams — already filtered down — feed into Continuum. Continuum builds a value-added data product. For our airport, that's a single object that pulls together everything we know: how many flights are now expected in the rolling hour, the rate the airport has actually published, the weather, the restrictions in effect. A 360-degree view of the thing you care about, built once and kept current.

Let me pause on how it works, because it matters. You've got your source streams — flights, rates, ETAs, weather, restrictions. We take them in and keep the fields that matter; a lot of the raw payloads are purely technical, so we hold the raw and refine it through a series of materialized views. You combine streams with plain SQL, then build views on views on views, and join them together.

One piece is worth calling out, because it's the thing a warehouse can't do cleanly. The published arrival rate changes through the day — it's versioned. So in SQL we derive each rate version's window, valid-from to valid-to, and a temporal join picks the rate that was actually *in effect* for each bin of demand. No side database. Drop the rate with a new event, and every affected bin re-evaluates itself.

Here's the key part, and I want to slow down on it. Every time an event comes in — a new position, a new ETA, a rate change — every materialized view, all the way down, updates. Not the whole view: only the parts the new data actually touched. That's what gives you sub-second results. And the model underneath is mathematically proven correct — every view is guaranteed to match what a full recompute would produce, deterministically, every time. You don't get that from a warehouse, and you don't get it from an LLM.

The triggers ride on the same SQL. The rule for this demo is written right there: demand has to exceed the rate in effect by a real margin — about ten percent of the published rate — *and* it has to be sustained across bins, not a one-off flicker. That hysteresis is what keeps estimates that wobble from flapping an alert. When it's genuinely over, it ships.

And there's a move here that's easy to miss. The raw per-bin breaches are an alert storm — overlapping rolling windows, demand climbing, the same airport firing over and over. Continuum collapses that in the data plane, before any AI sees it, into *one* incident per airport. And what ships isn't a thin alert — it's the whole object, the airport, the rate and runway in effect, the severity, the restriction, already attached. Proven correct, and deterministic.

Because it's this filtered, you're only ever handing the most important thing to your people or your AI. What does that buy you? Everything gets faster — nobody, human or agent, is reaching all across the enterprise to figure out what an event even means. It's cheaper, in tokens and in people's time. And it's more accurate, because we hand the model exactly the same shape every time.

**[Deep dive — Agent Mesh]**

So when the incident fires — who gets it? Solace built its own agentic framework, Solace Agent Mesh, designed to be event-triggered and built for this new world where multiple agents collaborate.

The emergence of fast decision models in just the past year has been a real shift here. We pair one of those up front — in this demo it's OpenAI's decisions capability — and it does a triage pass on every incident that arrives. It answers a handful of sharp questions: will this actually materialize, will it self-resolve, is it just a symptom of weather already in play, and what should we do — escalate, group, or suppress. Most breaches get suppressed or grouped right there, in under a second, even on a laptop. That's the whole point: we kill the noise before we spend real money.

For the few that escalate, the built-in workflow in Agent Mesh fans out — and that escalation is several agents at once: a weather agent, a downstream agent looking at which airports are at risk next, a trend agent. Each one reaches back into Continuum over MCP and pulls the live demand, the overage, the current weather — the detail we deliberately didn't cram into the trigger. Then a lead briefer synthesizes all of it into one advisory decision brief: what's happening, why, the downstream airports at risk, and two or three options with their tradeoffs.

Advisory is the operative word, and it's not a hedge — it's the product rule for this domain. The FAA will not accept an automated traffic-management decision, and this never pretends otherwise. Every claim in the brief traces back to a source feed; the human stays in command. The AI is the last mile, not the product.

And it deploys anywhere — multiple clouds, on-prem, fully air-gapped. Given our client base we've put a lot of work into that; plenty of our people want an agentic system running in-house with nothing leaving the building. It compiles down to a single binary, which makes it extremely portable.

**[Transition to demo — the dashboard]**

So — enough talking about this. Let's see it live, end to end. I've got three screens for you, and they line up with the three layers. This dashboard is the business picture. The Continuum pipeline is the data plane, doing the actual work. And the Agent Mesh UI is the agents responding.

The scenario is an unexpected wind shift at Newark. The wind swings around, the airport has to reconfigure to a single runway, and its published arrival rate drops hard — from 44 down to 26 — while inbound flights are already bunching in. A busy hub, suddenly over its own rate.

One thing to watch for as we go: when the incident fires, it carries the whole airport object with it — the rate, the runway, the weather, the restriction — not just an alert. That collapse the dozens of alerts that everything downstream fast.

`make demo` fires the wind shift. Let's run it. Watch Newark go red, and watch the alert storm collapse to a single incident.

**[Switch to the Continuum pipeline — the data plane, live]**

Before the agents, let me show you the layer in the middle actually doing its job. This is the Continuum pipeline view. It's the lineage I walked through earlier, except now it's running. Every box is a materialized view, and watch them pulse — each one lights up green the instant a real feed lands on it. That's not a canned animation. Those are live events off the Event Mesh, and each one updates only the views it actually touched.

Follow the bold thread down the middle. Flights and ETAs come in at the top, fold into the latest plan and ETA per flight, roll up into inbound flights, then into arrival demand over the rolling hour. That meets the published rate right here, at "Demand vs capacity," and anything genuinely over it spills into breaches.

Two boxes I want you to click with me. This one — "Rate in effect." That's the versioned side input I described, and here's the real SQL behind it: the LEAD, the valid-from to valid-to window. No side database, just SQL kept current. The join that picks the rate actually in effect for each bin happens one step down, where the thread hits "Demand vs capacity." And this box near the bottom — "Airport incident," one per airport. That's where the storm collapses. Dozens of per-bin breaches feeding in above it, one row coming out the bottom, straight into "Incident → SAM." That last box is the handoff. It's the single object we're about to watch the agents pick up.

**[Switch to the Agent Mesh UI — activities view]**

Now let me switch screens, because this is the part I actually want you to see. This is the Agent Mesh UI, the activities view. Up to now I've been *telling* you the agents collaborate — here you can watch the data move through the mesh and see it.

Follow it with me. The incident lands on the gateway — that's the one object Continuum shipped, arriving as an event. It goes straight into the fast triage, and you'll see that node light up and come back in well under a second with a verdict: escalate. The moment it does, watch the fan-out — the weather agent, the downstream agent, and the trend agent all fire in parallel, not one after another. And see those calls going back out to Continuum? That's each specialist reaching into the data product over MCP for the live detail we deliberately left out of the trigger — the current demand, the overage, the weather. Then it all funnels into the lead briefer, which composes the decision.

That's the whole point of a mesh, right there on the screen: differentiated agents, some fast, some deep, lighting up only for the one incident that earned it — and every arrow you just watched ran on the Event Mesh underneath.

**[Back to the dashboard — the brief]**

And back to the dashboard. Here's what the traffic manager actually sees: not a storm, one incident, with the brief the mesh just built — what's happening at Newark, why, the downstream airports at risk, and two or three options with their tradeoffs. Advisory, every line traceable to a source feed. That's the result we funneled the whole fire hose down to.

---

*Verify before delivery:* confirm the fast-decision model as you'll name it on stage — the demo runs OpenAI's **decisions** API through the Solace MaaS LiteLLM proxy (model `microsoft-decision-1`); say "OpenAI's decisions capability" or the exact model, whichever you prefer. Hero airport is **KEWR / Newark**, AAR **44 → 26** on the single-runway reconfig — reconfirm both against the live scenario before you present. The incident-collapse and the sub-second triage are real on a laptop; the throughput claims are deliberately soft here, so don't quote a messages-per-second number you haven't just measured.

*Continuum-pipeline beat:* the page is `continuum-pipeline.html` on the dashboard host (api service, `localhost:8080`), TV/kiosk mode by default — big node names, click a node for its SQL. Before you present, confirm the header reads "live · MQTT" and the boxes actually pulse as feeds arrive. It flashes on real events off the Event Mesh, so if the feed or the Feldera engine (`:8081`) isn't reachable from the browser, nothing lights up. Do a dry run and click "Rate in effect" and "Airport incident" once so you know exactly what SQL pops — and reconfirm the node labels I narrate ("Demand vs capacity," "Incident → SAM") still match what's on screen. For a less technical room, the stepper at the top reveals the lineage one stage at a time (seven stages) instead of the whole DAG at once. Open it in a third tab so the dashboard → pipeline → mesh switches are instant.

*Activities-view beat:* open it from the **Agent Mesh UI** button on the dashboard (SAM runtime, `localhost:8800`). Before you present, do a dry run and confirm the view actually renders what the script narrates — the gateway → triage → parallel specialists (weather / downstream / trend) → lead briefer flow, and whether the specialists' Continuum MCP calls show as discrete arrows or are rolled up. If the UI labels or layout differ, adjust the walk-through wording to match what's on screen rather than the other way round. Have the dashboard and the mesh UI open in two tabs so the switch is instant.

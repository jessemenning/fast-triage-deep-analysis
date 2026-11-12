# FAA Fast/Slow — Demo Walkthrough (cleaned)

The live demo portion, cleaned from the read-through and tightened to Jesse's voice.
For the full talk (problem, three layers, deep-dives) see [PRESENTATION_SCRIPT.md](PRESENTATION_SCRIPT.md);
for ground truth see [CLAUDE.md](CLAUDE.md). Run it with `make demo`.

---

**[Dashboard — three screens]**

The three different layers line up with the three we just talked about--event mesh moving data, Continuum pipeline contextualizing it, and Agent Mesh coordinating the agents' response.

The scenario is an unexpected wind shift at Newark airport. When the wind changes direction, you have to realign the runways, and at Newark one runway can accommodate a lot fewer planes. So the arrival rate — the number of planes that airport can handle — suddenly drops from 44 down to 26. And that's in the middle of a busy, busy day, with inbound flights already bunching. New York's a busy hub, and all of a sudden it's way over its allowed rate.

One thing to watch for: when the incident event is published — and it's going to happen very quickly — it carries the entire airport object with it. The rate, the runway, the weather, the restriction. Not just a plain alert. That's what lets the dozens of alerts a conventional system would fire collapse into a single event, and it keeps the agents from needing to contextualize on their own.

**[Continuum pipeline — the data plane, live]**

Before we run the scenario, let me show you how all the materialized views we talked about in the abstract are actually implemented. This is the Continuum pipeline view — the lineage I walked through earlier, except now it's live and reflects the data we're sending in. Every box is a materialized view. The first ones, flashing right now, are the Solace feeds — the raw events coming in, plus a rate feed, a weather feed, and a restriction feed. Just topics we're subscribed to.

As we move into Continuum, we persist the fields we care about. Most of the time we only want the latest, so in the next level we keep only the latest flight plan and the latest ETA, defined with pure SQL. And every time an event comes in, these update in real time — not the whole view, only the parts that event actually touched. That's what gives us sub-second results.

One box worth clicking: "Rate in effect." The published rate changes through the day and to adjust to it the changes, we just do a JOIN. No side database, just SQL kept current. That's the thing a warehouse can't do cleanly.

But the real magic happens as we move down. Instead of individual flights, we combine them to look at only the inbound flights for a particular airport — joining all the flights to see what's going to arrive, and updating the anticipated demand. Then we view that demand through the capacity view. And if demand surpasses capacity, we bring in the weather and the active restrictions for a full 360-degree view.

And it only fires when it's real: demand has to beat the rate by a real margin — about ten percent, not a one-off flicker. The model underneath is mathematically proven correct — every view matches what a full recompute would produce, deterministically, every time. You don't get that from a warehouse, and you don't get it from an LLM.

And this box near the bottom — "Airport incident," one per airport — is where it collapses. Continuum folds dozens of  breaches into one incident, in the data plane, before any AI sees it. And note the Solace sinks right here: the incident goes out on `fl/faa/arrival-capacity/incident`, straight into SAM. That's the handoff — the single object we're about to watch the agents pick up.

**[Fire the scenario]**

So — let's kick things off. We'll shift the wind. You can see instantly the arrivals surging past the limit here. As a result, we've published our breach event — which carries the full context — over into Agent Mesh.

**[Agent Mesh UI — the fast/slow run]**

Almost instantaneously, you can see a triage came back. I'll bring you over to the Agent Mesh screen to show you what happened. We run a decision node, and it decides whether this is something to pay attention to or something that's passing — will it materialize, will it self-resolve, is it just a symptom of weather already in play, and should we escalate, group, or suppress. On the previous screen you saw that happens within a second. We post it back to the UI, and then — because it's a real incident —

we call on four different agents. This one looks at the weather: is it improving or not? This one analyzes the impact downstream. This one looks at the trend of how capacity is moving. And this one looks at how we could reposition the airplanes to better handle the situation.

So now we're past the quick triage and into deep analysis.Each agent goes back to Continuum, multiple times, over MCP — retrieving the live detail we deliberately left out of the trigger and processing it with LLMs.  These agents happen to be on my laptop, but because Agent Mesh lets you bring in and govern agents from multiple locations, they could be anywhere, grounded in the Event Mesh. 

All that comes back into a briefer agent. The briefer looks through everything it received using the power of a deep-thinking LLM, and publishes back to the Event Mesh — which in turn displays it in the UI.

**[Dashboard — the brief]**

Here's what the traffic manager actually sees, and this came back within 30 seconds or so. Rather than a storm of uncontextualized alerts, they see one incident, and the brief the mesh just built: what's happening in New York, why, the downstream airports at risk, and two or three options with their tradeoffs. Advisory, every line traceable to a source feed — the FAA won't accept an automated traffic-management decision, and this never pretends otherwise. The human stays in command.

**[Recap]**

So, just to recap. We took a fire hose of events — made possible by an Event Mesh that pulls from different systems — and pumped it through Continuum, which gave us a contextualized view of what actually matters: an airport, triggered only when it was a real issue. We triaged it with a model built to be fast and accurate, and only then, when it was crucial, did we go to deep analysis — four interconnected agents from different specialties, potentially hosted on multiple systems, coming together into a consensus we hand off to the human in the loop, who makes the decision. It's a powerful model, made possible only by a true real-time AI platform.

---

*Verify before delivery:* the four specialist node labels against the live Activities view (weather / downstream / trend / repositioning); that "Rate in effect" and "Airport incident" are the clickable nodes; and KEWR's 44→26 against the running scenario.

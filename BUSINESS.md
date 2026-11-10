# BUSINESS.md — why this demo exists

## The problem

Air traffic managers drown in demand/capacity alerts. When an airport's expected
arrivals start to outrun the rate it can actually land (its published **AAR**), today's
tools raise an alert — and then another, and another, as estimates shift and rates get
bumped. Most of those alerts are transient noise. A few are the real thing: the leading
edge of delays that will cascade across the national airspace. A traffic manager has to
tell which is which, then work out the *why* and the options — pulling up weather, NOTAMs,
runway configuration and existing restrictions across separate systems, by hand, under time
pressure.

## What this demo shows

A **real-time digital nervous system** for arrival capacity, built on the Solace Platform:

1. **Event Mesh** carries the live FAA feeds — flight plans and flight data (FDPS),
   published arrival rates (TTP Airport Information), per-flight ETAs (TTPFD), weather
   (METAR) and restrictions (TMR) — as they move through the national airspace.
2. **Continuum** continuously maintains expected-arrival **demand per airport** and compares
   it, bin by bin, against the **published rate in effect** — raising an event only when
   demand genuinely exceeds capacity, with debouncing so estimates that shift don't flap.
3. **Agent Mesh** applies a **two-speed AI**: a sub-second triage decides whether each
   breach actually matters (will it materialize? will it self-resolve? is it just a symptom
   of weather already in play?), suppressing or grouping the noise; and for the few that
   matter, a deeper investigation assembles a **decision brief** — what's happening, why,
   the downstream airports at risk, and two or three options with their tradeoffs.

The demo moment: an unexpected wind-direction change at Newark (EWR) forces a runway
reconfiguration to a single runway, degrading arrival capacity — a busy hub already running near
its limit — as inbound arrivals bunch in. Instead of an alert storm, the traffic manager sees
**one escalated incident** with an explained brief and options (including which flights to hold or
reroute) — and a clear contrast with the dozens of alerts a conventional system would raise.

## Why it matters

- **Cuts alert fatigue.** The value is not the alert; it's deciding which alerts deserve a
  human, and giving the *why* behind each one.
- **Decision support, not automation.** Every brief is advisory, with its options and
  tradeoffs laid out and **every claim traced to a source feed**. The human stays in command;
  the FAA will not accept automated traffic-management decisions, and this demo never pretends
  otherwise.
- **The AI is the last mile, not the product.** The defensible story is the real-time data
  fabric — the event mesh fusing many SCDS services into one live picture, with stream
  processing doing the continuous math. The LLM only writes the brief on top of it.

## The Solace angle

This is the Solace Platform doing what it is built for: connecting systems, data and AI
securely and at scale, in real time. The same three layers — mesh, streaming, agents — that
catch a credit-exposure breach in treasury or a fraud burst in payments catch an arrival
overload in the national airspace. The domain changes; the platform does not.

For architecture and how to run it, see [CLAUDE.md](CLAUDE.md) and [README.md](README.md).

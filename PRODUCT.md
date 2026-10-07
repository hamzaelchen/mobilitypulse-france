# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

Two audiences with equal priority (confirmed): (1) a jury or recruiter evaluating a Big Data / data-engineering project (PFE) who must understand quickly that this is a real, running real-time pipeline; (2) a bike-share rebalancing operator, the business scenario the project tells, who needs to spot critical stations and decide transfers. Shown on a laptop/projector in a defense and as an online demo/portfolio on varied screens including mobile (confirmed: both).

## Product Purpose

MobilityPulse France follows the Naolib (Nantes) bike-share network in near real time (GBFS feed, polled every 60 s), detects empty and saturated stations, ranks stations to rebalance with a trend heuristic, and suggests bike transfers between nearby stations. The UI exposes this through a landing page (`/`) and a control-center application (`/app`) fed by a live stream.

## Positioning

A complete, honest data pipeline end to end (GBFS -> Kafka -> Spark Bronze/Silver/Gold -> PostgreSQL -> live UI) that shows the real age of its data and says what it does not do, instead of a mock-up.

## Operating Context

Single-machine Docker stack (16 GB RAM, Docker limited to about 6-8 GB). The UI is French, code is English. The API is read-only (PostgreSQL role `grafana_ro`). Data freshness is classified live / delayed / stale and the cause is reported (collection stopped, updater down, updater error, database unavailable).

## Capabilities and Constraints

- Live stream over SSE (`/api/stream`) with polling fallback; REST endpoints for overview, stations, priorities, transfers, hourly rates, quality, status.
- Priorities come from a trend heuristic; no predictive model is trained (history too short). Transfers are suggestions, not orders.
- Never invent performance figures: only measured numbers are shown or claimed. No secrets in the repository.
- Constraints: no heavy front-end libraries; Leaflet via pinned cdnjs for the map; prefers-reduced-motion respected; WCAG AA; keyboard navigation; responsive down to mobile.
- Undecided: public repository URL (project not pushed to GitHub); exposed through `PROJECT_REPO_URL`.

## Brand Commitments

Name: MobilityPulse France. French UI copy. The user asked explicitly for a dark, premium "control center" feel for the landing and the app (binding visual constraint, to be developed in new-work, not here).

## Evidence on Hand

Real data from the running stack (stations, bikes, empty/full counts, freshness). Landing and app captures in `docs/img/`. No testimonials, customers or benchmarks exist; none may be fabricated.

## Product Principles

1. Truth over polish: stale or missing data is shown as such, never dressed up as fresh.
2. Every number on screen comes from the API; absence is shown as absence.
3. Say plainly what is heuristic, what is measured, and what is not done yet.
4. The business story (rebalance the network) leads; the technology serves it.
5. Motion explains the live system (data arriving, state changing); it never hides latency or decorates idle UI.

## Accessibility & Inclusion

WCAG AA contrast, visible keyboard focus, full keyboard operation, `prefers-reduced-motion` calm variant, legible from a distance (projector) and on mobile.

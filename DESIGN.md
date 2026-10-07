---
name: MobilityPulse France
description: Dark control-room interface for a live bike-share network, where the stations themselves are the picture.
colors:
  ground: "#070b14"
  panel: "#0c1322"
  panel-raised: "#101a2e"
  raise: "#16233b"
  line: "#1b2a47"
  line-strong: "#2a3d63"
  text: "#e8eefb"
  text-secondary: "#a8b5cf"
  text-tertiary: "#8493b0"
  flow: "#8db4ff"
  state-empty: "#e66767"
  state-full: "#9085e9"
  state-low: "#c98500"
  state-high: "#3987e5"
  state-ok: "#199e70"
  state-out-of-service: "#6b7a90"
  warning: "#e0a526"
typography:
  display:
    fontFamily: "Bricolage Grotesque, ui-sans-serif, system-ui, sans-serif"
    fontSize: "clamp(2.4rem, 5.4vw, 4.4rem)"
    fontWeight: 700
    lineHeight: 1.03
    letterSpacing: "-0.028em"
  headline:
    fontFamily: "Bricolage Grotesque, ui-sans-serif, system-ui, sans-serif"
    fontSize: "clamp(1.7rem, 3.6vw, 2.6rem)"
    fontWeight: 700
    lineHeight: 1.08
    letterSpacing: "-0.02em"
  title:
    fontFamily: "Bricolage Grotesque, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 650
    lineHeight: 1.2
  body:
    fontFamily: "Hanken Grotesk, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 400
    lineHeight: 1.55
  figure:
    fontFamily: "Spline Sans Mono, ui-monospace, Consolas, monospace"
    fontSize: "clamp(1.7rem, 2.4vw, 2.1rem)"
    fontWeight: 600
    lineHeight: 1
    letterSpacing: "-0.02em"
rounded:
  sm: "6px"
  md: "12px"
  lg: "16px"
  pill: "999px"
spacing:
  xs: "6px"
  sm: "12px"
  md: "16px"
  lg: "24px"
  section: "clamp(80px, 12vw, 150px)"
components:
  button-primary:
    backgroundColor: "{colors.text}"
    textColor: "#0a1020"
    rounded: "{rounded.md}"
    height: "48px"
    padding: "0 22px"
  button-secondary:
    backgroundColor: "{colors.panel-raised}"
    textColor: "{colors.text}"
    rounded: "{rounded.md}"
    height: "48px"
    padding: "0 22px"
  chip:
    backgroundColor: "{colors.ground}"
    textColor: "{colors.text-secondary}"
    rounded: "{rounded.pill}"
    height: "40px"
    padding: "0 13px"
  chip-pressed:
    backgroundColor: "{colors.raise}"
    textColor: "{colors.text}"
  panel:
    backgroundColor: "{colors.panel}"
    rounded: "{rounded.lg}"
---

# Design System: MobilityPulse France

## Overview

**Creative North Star: "The Network Room"**

The interface is a room where the network is on the wall. Stations are drawn where they really are, coloured by what they are doing, and everything else (panels, type, chrome) stays quiet enough that the data is the loudest thing on screen. It is dark because it is meant to be read on a projector in a dim room or on a laptop at the end of a day, not because dark is a default.

The language is wayfinding, not dashboards-with-glow: hairline rules, ring stations, a transit-style line for the data path, signage-weight headings. Colour is spent on meaning. Six state colours are the only saturated hues on screen; the primary action is a light button, not a neon accent. Motion explains the live system (a packet travelling, a station changing state, a truck driving a transfer) and goes calm under `prefers-reduced-motion`.

**Key Characteristics:**
- Ink ground, hairline panels, no shadows at rest.
- State colours are the data language: vide, saturée, faible, élevée, normale, hors service.
- Figures in a mono face with tabular numerals; headings in a characterful grotesque.
- Truth shown, not styled away: freshness (live / en retard / périmé) is always visible.

## Colors

A near-black ink field, three blue-grey text tiers, and six validated state colours. One cool light blue (flow) carries movement and links.

### Primary
- **Flow Blue** (#8db4ff): links, focus rings, transfer lines, data packets. It means "something is moving or can be followed".

### Neutral
- **Ground Ink** (#070b14): page field.
- **Panel Ink** (#0c1322) and **Raised Ink** (#101a2e, #16233b): panels, selected chips, tab indicator.
- **Hairline** (#1b2a47) and **Strong Hairline** (#2a3d63): all borders and dividers.
- **Text** (#e8eefb), **Text Secondary** (#a8b5cf), **Text Tertiary** (#8493b0): body tiers, all at least 4.5:1 on the panels.

### State
- **Empty Coral** (#e66767), **Full Violet** (#9085e9), **Low Amber** (#c98500), **High Blue** (#3987e5), **Normal Green** (#199e70), **Out-of-service Slate** (#6b7a90).
- Order matters: this adjacency passed the dataviz validator on the dark surface (lightness band, chroma floor, colour-blind separation, contrast). Legends, stacked bars and chips use this order.

### Named Rules
**The Meaning Rule.** A saturated colour on screen is a station state or a status. Decoration never borrows them.
**The Light Primary Rule.** The main action is the lightest object in the room (Text on Ground), never a coloured button.

## Typography

**Display Font:** Bricolage Grotesque (fallback ui-sans-serif, system-ui)
**Body Font:** Hanken Grotesk (fallback system sans)
**Label/Mono Font:** Spline Sans Mono, for measured values only

**Character:** a signage-like grotesque with compressed, confident headings over a calm humanist body. The mono face is reserved for numbers people compare.

### Hierarchy
- **Display** (700, clamp(2.4rem, 5.4vw, 4.4rem), 1.03, -0.028em): the landing question, max 17ch.
- **Headline** (700, clamp(1.7rem, 3.6vw, 2.6rem), 1.08, -0.02em): section titles.
- **Title** (650, 1rem, 1.2): panel titles in the app.
- **Body** (400, 1rem, 1.55): copy, 65 to 70ch at most.
- **Figure** (mono 600, clamp(1.7rem, 2.4vw, 2.1rem), tabular): KPI values and counts.

### Named Rules
**The Measured Mono Rule.** Mono is for numbers, times and codes. Never for "technical" decoration.

## Layout

Landing: one column, 1120px max, long vertical rhythm (80 to 150px between sections), the hero a full-bleed live canvas with the copy on its left. App: 1520px max shell, a KPI strip, then a two-column grid (map 1.7fr, side panel 1fr) that stacks below 1100px; the map is clamp(460px, 66vh, 700px). Hairline cells instead of cards. Touch targets are at least 40px.

## Elevation & Depth

Flat by default. Depth comes from tonal steps (ground, panel, raised) and 1px hairlines. The only shadows belong to floating things: the station panel (0 10px 28px rgba(0,0,0,0.5)) and toasts. The map legend uses a blurred translucent panel because it sits on a busy map and must stay legible.

### Named Rules
**The Flat-At-Rest Rule.** No shadow on a resting panel. A shadow means "this floats above the map".

## Shapes

Soft rectangles at three radii (6px small controls and slots, 12px buttons and rows, 16px panels), full pills for chips and the status pill. Stations are circles; the data path is a 2px line with ring stops. No decorative clipping.

## Components

### Buttons
- **Shape:** 12px radius, 48px high.
- **Primary:** Text (#e8eefb) fill with ink text; the arrow icon nudges 3px on hover.
- **Secondary:** Raised Ink fill, strong hairline; border turns Flow Blue on hover.
- **Press:** every button and chip scales to 0.97 on press; hover is gated to fine pointers.

### Chips
- **Style:** pill, ground fill, hairline; pressed state is Raised with strong hairline. Each state chip carries its colour dot and live count.

### Status pill
A pill holding the live dot and a sentence: "En direct", "Données en retard" or "Données périmées", the observation time and its age. The dot pulses once per stream message.

### Tabs
Segmented control with a sliding indicator (translateX, 320ms), arrow-key navigation.

### Priority rows
Buttons in a list: rank badge, name (full name on hover), meta line, score and a bar (scaleX). Rows reorder with FLIP.

### Signature: transfer trucks
A small truck with a cargo count drives each suggested transfer along its dashed segment, easing at both ends. Under reduced motion it rests at the midpoint.

### Signature: the transit line
The landing's data path is a rail with five ring stops and two packets; each stop pulses as a packet passes.

## Do's and Don'ts

### Do:
- **Do** show data age and its cause next to every number that could be stale.
- **Do** keep state colours in the validated order and pair them with a label or count.
- **Do** animate transform and opacity; ease out with cubic-bezier(0.23, 1, 0.32, 1); keep UI transitions under 300ms.
- **Do** provide a calm `prefers-reduced-motion` variant for every animation.

### Don't:
- **Don't** use a state colour for decoration or a coloured primary button.
- **Don't** add eyebrow labels above headings, thick side stripes, gradient text, or glyphs standing in for icons.
- **Don't** present stale or missing data as fresh; show absence as absence.
- **Don't** animate layout properties (width, height, margin, padding); use grid-template-rows, transform or opacity.

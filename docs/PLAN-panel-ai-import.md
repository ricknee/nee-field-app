# PLAN — 📷 Fill a panel schedule from a picture or PDF (field app)

**Status:** PLANNED, not built. Owner asked 2026-09-30: on a job's Panels screen, upload "a screenshot of
a panel schedule from a print or a picture" and have an AI agent "fill in the proper information needed
for that panel."

**Decided with the owner (2026-09-30):**
- **Sources:** mostly **PDFs and pictures** (prints and phone photos).
- **Who:** **admin only**, for now.
- **Breaker size:** yes, capture it (`panel_circuits.amps`). **Watts: no.**

## What exists already (measured 2026-09-30)

- `panel_schedules`: `name, voltage, circuits, feed, mounting, enclosure, location, fed_from, notes, …`
- `panel_circuits`: `panel_id, number, description, watts, amps, poles`. `amps` and `watts` are **unused**:
  0 of 336 circuits have either, and the editor has no box for them.
- 9 panels / 336 circuits typed by hand, which are the **accuracy test set** (see step 1).
- Job **Prints** already live in R2 (`jobPrints`), so the source can be picked from the job instead of
  re-uploaded. Upload uses the presigned-PUT pattern from `_r2.js` (bytes never pass through a function).
- The editor already enforces gang rules: `panelBlocks()`, `panelMaxPoles()` (2-pole max single-phase,
  3-pole three-phase), same side, consecutive spaces.
- Nothing in the repo calls an AI API today.

## What the user sees

1. Job → **Panels** → **📷 Fill from picture** (admin only), on a new panel or an existing one.
2. Source: **take a photo**, **upload an image or PDF**, or **pick from the job's Prints**.
3. "Reading…" (10–40 s), then the editor fills in: **name, voltage/phase, circuit count, location, fed from**,
   each circuit's **description** and **breaker size (amps)**, and **2/3-pole gangs** merged.
4. **Review, then Save.** It never saves by itself. Cells it was unsure of are **highlighted**.
5. **A sheet with several schedules** (common on CAD prints): "Found Panel A, B, LP-1". Pick one, or add
   them all as new panels.

## Safety rules

- **Existing panel:** default is **fill blanks only**. "Replace" is a separate choice that shows a diff first.
- **Validated before it's shown:** circuit numbers ≤ panel size; gangs on one side, consecutive, and ≤ what
  the phase allows (reuse `panelMaxPoles`); amps a sane breaker size (e.g. 15–400 A) or left blank. A value
  that fails is left blank and flagged, **never guessed**.
- **The source image/PDF is kept on the job** (as a print) so the numbers can be traced back.

## Behind the scenes

- New `airtable.js` action, e.g. `panelScheduleExtract`, **`_ADMIN`** tier in `authzFor`.
- Calls the Anthropic Messages API directly with `fetch` (no SDK, so `tests/handlers.test.mjs` stays
  offline), sending the image or **PDF as a document block**, with a strict JSON schema for the reply.
  Use the current most capable Claude vision model; check model ids and pricing via the `claude-api`
  skill at build time, don't quote from memory.
- **Env:** `ANTHROPIC_API_KEY`, **fails soft** like R2: unset means the button doesn't render and nothing
  else changes. It is not in `ensureEnv()`.
- ⚠ **MAIN RISK — TIME.** A dense 42-circuit schedule can take longer than a synchronous Netlify function
  is allowed to run. Plan for a **background function** that writes the result to a small Neon table
  (`panel_extract_jobs`: id, status, result JSON, error), with the client polling. Decide in the spike
  after timing real sheets.
- **Breaker size needs UI too:** the editor, the 🖨 sheet / PDF and the DK-2205 strips have no amps
  column today. Adding it is part of this build: an "A" box per circuit in the editor and an optional
  column on the printouts.
- **Cost:** small per schedule. Confirm current pricing before building.

## Build order

1. **Spike: accuracy test, no UI.** A script runs the extraction against the job Prints behind the **9
   existing panels** and scores it against what was typed by hand (description match, gangs, circuit
   count). Include a few **phone photos**, since that's half the owner's input. **Stop if it isn't good
   enough.** Also time each call; this decides sync vs background.
2. **Server action** + (if needed) the background job and polling table.
3. **Amps in the editor** (and optionally on the printouts).
4. **Review flow** in the panel editor: highlighted uncertain cells, fill-blanks vs replace-with-diff.
5. **Multi-panel sheets** and **pick from job Prints**.

## Open

- Does the owner want amps on the **printed** sheet and strips, or only stored/visible in the editor?
- The 9 existing panels: which jobs' Prints hold their source sheets (needed for the step-1 score)?

# Bigger Bet: Unify the two estimate systems

**Status:** Planned, not started. Slice 2 or 3 of the Neon migration — runs *after* the
native time-tracking slice proves the Neon + app-shell pattern.

**One-line:** Don't merge the two estimate tools. Give them a shared `job_id` so they can
*see each other* on the admin side only, while each still works on its own. Do it as part of
the Neon move so the math lives in code, not doubled Airtable formulas.

---

## Why "only as part of the database move"

Both estimate tools today rely on **Airtable formulas** for their totals and rollups. Merging
them *inside Airtable* would mean maintaining **two copies of every formula** (one per base)
and keeping them in sync forever — high upkeep, fragile. Neon puts the math in **one place in
code**, so the unification is done once, in the new home, instead of twice in the old one.

## The two tools today (they are genuinely different jobs)

1. **Shopping cart** — inventory app (`inventory.html` → `inventory.js`, `INVENTORY_BASE_ID`).
   The owner's **private cost worksheet**: add real catalog items, eyeball a markup. Often kept
   *unlinked* — just a reference note to later check "did my guessed price beat real expenses?"
   Persistent line items (`Estimate Line Items`), live pricing from the catalog. No PDF. Job is
   stored as a typed **string**, not a real link.

2. **Customer estimate** — field app (`index.html` → `airtable.js`, main base). The thing
   actually **sent to the customer**, freely editable. One typed total + labor/material rollups,
   saved as a JSON **snapshot** (`Sent Estimate PDFs`). Generates the PDF (jsPDF → Make → pCloud).
   Real **link to the Job**; drives Expected Revenue rollups.

(Full handler/table inventory of both systems was mapped 2026-06-06; see commit history and the
estimate handlers in `airtable.js:1779–2111` and `inventory.js:1465–2300`.)

## The decision: loose link, NOT a merge

A full merge would force every estimate into one shape (e.g. always line-items), which **breaks
the owner's workflow** — he doesn't always want a cart, and the customer estimate must stay
freely editable. Constraints he set:

- **Optional, not forced.** Neither tool may require touching the other to work. Bang out a
  customer estimate with no cart; use a cart with no effect on any customer price.
- **Internal-only visibility.** When a cart *is* linked to a job, the estimate screen shows
  **one internal line** — *"Material cost from cart: $X"* — **on the admin screen only, never on
  the customer PDF.** It informs the markup eyeball; it does not set the price.

### Settled design choices

- **Linking:** the owner picks the job **on the cart** when saving (nullable — blank stays an
  unlinked reference note, exactly like today).
- **Internal view:** show **just one material-cost total** next to the estimate. Not the full
  item list, not auto-markup math. Never rendered into the PDF.

The shared `job_id` is the entire trick: both tools point at the same `jobs` row, so the field
app can read "sum of linked cart lines for this job" without the two systems being chained.

## Rough build order (when its turn comes)

1. **Neon schema** — `estimates`, `estimate_lines`, `estimate_templates`, `template_lines`,
   each carrying a real `job_id` FK to `jobs`. Model must represent *both* of today's styles:
   line-item carts and the simple total + snapshot customer estimate.
2. **Cart gets an optional `job_id`** — UI to tag a job on save; nullable so unlinked still works.
3. **Internal material-total read** — one handler returning "sum of linked cart lines for this
   job," shown internal-only on the estimate tab. Guarded so it can never reach the PDF builder.
4. **Backfill** existing estimates from both Airtable bases into Neon; leave unlinked carts
   unlinked.
5. **Retire** the old Airtable estimate formulas/tables once both apps read Neon.

Steps 1–3 are **additive** — the current send-an-estimate flow does not change until a cart is
deliberately linked. Low risk; matches the loose-coupling rule.

## Hard constraint — the GP / profit math must survive the move

**Owner requirement (2026-07-27): keep all of the gross-profit and live-profit reporting.**
Nothing in this bet may reduce what the Est. GP / Live GP / Final GP cards show today. Before
anything Airtable-side is retired (build order step 5), the existing formulas must be inventoried
and ported to Neon views, then diffed job-by-job against Airtable the same way `v_hours_by_job`
was diffed in the time-entries slice.

Three families exist today, and they are **not** built the same way:

1. **Gross Profit (Live) $ / %** — `F.job.grossProfitLiveDollar` / `…LivePct` (airtable.js:172–173).
   Airtable-side formula/rollup, read straight through by `mapJob`.
2. **Gross Profit (Final) $ / %** — `F.job.grossProfitFinalDollar` / `…FinalPct` (airtable.js:181–182).
   Same shape as Live, different inputs.
3. **Projected Gross Profit $ / %** — **computed in JS**, not read from Airtable
   (`mapJob`, airtable.js:1584–1596): `expectedRevenueAllStatus − (projectedEstimatedMaterialCost +
   projectedEstimatedLaborCost)`. The formula twins in Airtable sum *unfiltered* inputs and are
   deliberately not read — see the FILTERED-vs-UNFILTERED rollup note in CLAUDE.md.

**Why this bet specifically endangers them:** Projected GP is driven by Expected Revenue and the
projected cost rollups, which roll up **from the Job Estimates links**. Move estimates to a Neon
`job_id` FK and delete the Airtable estimate tables/links, and those rollups lose their inputs —
the GP cards go blank or silently wrong while every individual estimate still looks fine. Same
GP-formula risk already flagged before the JotForm wire/pipe table deletes.

**Rule:** capture the full formula text of all three families (plus their upstream rollups and
each rollup's Status filter) during the schema design step, port them as Neon views, prove
agreement on real jobs, and only then retire anything. Do not treat GP as a follow-up.

## Open questions / watch-outs

- **PDF for inventory estimates?** Out of scope for this bet — carts stay PDF-less; only the
  customer estimate generates one. Revisit only if the owner asks.
- **Templates** exist in *both* systems with different schemas (field-app metadata templates vs.
  inventory line-item templates with frozen pricing). Unify under one `estimate_templates` shape
  during the Neon design step.
- **Cross-job substring filter bug** (`docs/TODO.md`) touches `Job Estimates` at
  `airtable.js:546` — fix or render moot when estimates move to a real `job_id` FK in Neon.
- Interplay with the **Drop the Jobs-mirror** bet (in flight): both bets converge on jobs being
  referenced by a real key instead of a typed name. Sequence so they don't fight.

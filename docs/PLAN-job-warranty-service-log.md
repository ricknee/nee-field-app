# Plan — job warranty clock + service visit log

**Status: PLANNED, NOT STARTED.** Owner idea 2026-08-04. Design settled; build deliberately
deferred — see §1. Nothing in this document has been built, and `SYSTEM-MAP.html` has
deliberately *not* been updated (the map describes what exists; this doesn't yet).

*Last updated 2026-08-04.*

---

## Why this exists

Two gaps the owner raised, both on jobs we've already finished:

1. **We give a 1-year warranty on our work and track it nowhere.** Cummins generators have a
   real warranty system in this app — templates, start/end dates, a counter that begins at
   commissioning. Every other job has nothing. There is no way to answer "is this callback on
   our dime?" without remembering when we finished.
2. **Service work after completion leaves no record.** Going back to a finished job and doing
   work produces no note, no date, no author. The job's `Notes` field is a single shared blob
   (admin/office write only since 2026-07-31) — last writer wins, no history.

---

## 1. Timing — this is a §7 detour, and here is the honest read

`ROADMAP.md` §7 exists for exactly this: real ideas that don't move us off Airtable. By the
roadmap's own rule (§8: *"Should I do X first? → if X is in §7, no"*), this waits.

Two specifics sharpen it:

- **Warranties are already scheduled for migration.** Roadmap §3 Step **4c** is
  *"Inspections, Generators, Warranties"* — this feature lands squarely inside a domain that
  already has a migration slot. Anything built in Airtable now is built **twice**: once here,
  once again when 4c runs.
- **The two halves have very different cost profiles.**

| Part | Build cost | Double-work if built in Airtable now |
|---|---|---|
| **Part 1** — warranty dates + badge | ~1-1.5 h | **Near zero.** Neon's `jobs` table already exists and already carries `finish_date`. The Neon side is 2 columns + 2 ETL lines. |
| **Part 2** — service visit log | ~4-6 h | **Full.** An entire new Airtable table, 3-4 handlers, a new tab — all of which 4c then migrates. |

**Recommendation: split them.**

- **Part 1 can be done any time** it's wanted. It's an hour, it's two fields on a table that is
  already mirrored, and it closes a live liability question. It barely registers as a detour.
- **Part 2 should be built as part of Step 4c, Neon-native.** Building it there costs roughly
  what building it in Airtable costs today — and skips the migration entirely.

> **⚠ TWO UPDATES SINCE THIS WAS WRITTEN (2026-08-06).**
>
> 1. **Every "4b" in this file has been rewritten to "4c."** Step 4 was re-lettered on
>    2026-08-05 when Schedule was inserted at 4a, pushing every later letter along one. The
>    slice this feature belongs to did not change — only its label.
> 2. **The wait is over.** This section's "Step 4c sits behind roughly 12-15 hours of roadmap
>    work" is stale: Steps 1, 2, 4a and 4b have all landed, and **4c started 2026-08-06**. Part 2
>    is now slice **4c-4** in `PLAN-4c-inspections-generators-warranties.md` §7 — build it there.

If service notes are urgently needed before 4c, the cheap stopgap is a dated, initialled entry
appended to the job `Notes` field — ugly, but zero build and zero migration debt.

---

## 2. Part 1 — the warranty clock

### The anchor date is hand-entered. Nothing fires it.

Settled with the owner 2026-08-04: **the status change does not start the clock.** The job is
often marked Completed weeks after the work actually finished. The owner types the date.

This rules out `Project Completed At` (`fldDcH5hrH596OTdB`), which is stamped by the Airtable
automation **"Stamp Project Completed Date"** (`wflxAET4F7bXXaq0v`) when Job Status → Completed.
The evidence is visible in the data: the dates cluster in batches — 2026-04-02, 2026-04-28,
2026-06-02 and 2026-07-31 each appear on 5-8 different jobs — because they record when the
*office* flipped a status, not when the *crew* left site.

### Fields — two of the three already exist and are dead

| Airtable field | Id | Populated | Plan |
|---|---|---|---|
| `Finish Date` | `fld5gXEg5SZ0ZkLM8` | 7 of 112 | **The anchor.** Hand-entered. |
| `Warranty End Date` | `flduRWGLpryAv1R5v` | **0 of 112** | Server-computed, stored. |
| `Warranty Months` | *(new, number, default 12)* | — | Lets a 90-day or 2-year term exist without a code change. |

Both existing fields are dead in the codebase — the only reference anywhere is
`db/etl/time-entries-full.mjs:155`, which already copies `Finish Date` to Neon. No Airtable
automation writes either.

> ⚠ **Pre-flight before claiming `Finish Date`:** confirm none of the ~70 Make.com scenarios
> reads or writes it. Make owns a lot of the Jobs table and the codebase can't tell us.

`Warranty End Date` stays a **stored date, not a formula**, so office can override it for an
extended or voided term. Same pattern the `Warranties` table already uses for generators.

### Why not a row in the `Warranties` table

That table links to `Generators` only — no Job link — and models per-brand templates (Cummins
5-year powertrain, 2-year comprehensive). The workmanship warranty is one flat term per job with
no variation. A row there costs a new link field, a template row, and an extra fetch on every job
load to model something that doesn't vary. Two fields give the badge, the filter and the report.
The upgrade path stays open: the fields don't have to be removed to add rows later.

### Neon

`jobs` already exists (`db/schema/003_jobs_master.sql`, applied 2026-07-31) and already carries
`finish_date`. Airtable stays the source of truth — that file states *"nothing writes back"* — so
this is a **one-way mirror with no dual-write hazard**, and it cannot interfere with the
time-entries cutover.

```sql
-- db/schema/006_job_warranty.sql
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS warranty_months integer NOT NULL DEFAULT 12;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS warranty_end_date date
  GENERATED ALWAYS AS ((finish_date + make_interval(months => warranty_months))::date) STORED;
```

Plus two lines in the ETL's `JOB_FIELDS` array.

### ⚠ THE GOTCHA — JS and Postgres month arithmetic disagree

`addMonthsToDateStr()` (`netlify/functions/airtable.js:564`) **overflows** into the next month.
Postgres `+ interval` **clamps** to the last valid day. Both columns below were run for real —
the JS via node, the SQL against Neon project `damp-silence-99074350` on 2026-08-04:

| Finish date | JS +12mo | Postgres +12mo | JS +3mo | Postgres +3mo |
|---|---|---|---|---|
| 2028-02-29 | 2029-03-**01** | 2029-02-**28** | 2028-05-29 | 2028-05-29 |
| 2026-08-31 | 2027-08-31 | 2027-08-31 | 2026-12-**01** | 2026-11-**30** |
| 2026-01-31 | 2027-01-31 | 2027-01-31 | 2026-05-**01** | 2026-04-**30** |
| 2026-08-04 | 2027-08-04 | 2027-08-04 | 2026-11-04 | 2026-11-04 |

At a 12-month term only a leap-day finish disagrees. The moment `Warranty Months` is used for a
3- or 6-month term, **every job finished on the 31st disagrees** — silently. This is the same
failure shape as the `Hours` quarter-rounding bug documented in `001_time_entries.sql`, where
plain division mismatched 131 of 200 rows.

**Decision: adopt Postgres clamping as canonical.** Write `addMonthsClamped()` and use it for the
job warranty. Clamping is also the correct reading commercially — a warranty starting Aug 31
should end Aug 31, and a 3-month term starting Jan 31 should end Apr 30, not spill into May.

> **Open question:** switch the *generator* warranties to the clamped helper too? Existing
> warranties have stored end dates so nothing moves retroactively — only newly-issued ones change,
> and only at month-end/leap edges. Two date helpers with different arithmetic is a trap someone
> finds the hard way; one helper is preferred unless there's a reason not to.

### App changes

- `F.job`: add `finishDate`, `warrantyEndDate`, `warrantyMonths` (**names only** — reads only).
- `handleUpdateJobInfo`: when `finishDate` or `warrantyMonths` is written, recompute and store
  `Warranty End Date` with `addMonthsClamped()`. Write sites use field IDs inline. No new action.
- Project Info tab: a "Work Completed / In-Service" date input beside the existing Bird Date row,
  showing the computed warranty end live.
- Job header badge, **visible to every role** (crews need it most):
  `🛡 Under warranty — 214 days left` / `⚠ Expires in 23 days` / `⛔ Expired 2026-01-04`.
  Pure client-side maths off two dates — no extra fetch.

### Historical jobs

**No backfill.** Deriving from `Project Completed At` would start ~60 clocks on the wrong date,
and a wrong warranty date is worse than a blank one. The field is editable; fill in the ones that
matter by hand.

---

## 3. Part 2 — the service visit log (build at Step 4c)

### Not an extension of `Generator Service`

That table is generator-shaped — Oil Changed, Spark Plugs Changed, Generator Hours @ Service —
and `Generators` rolls up `Last Service Date` and a `Service Status` formula from it, which drives
the "Generator Service Call" automation (`wfledvx1A8oVscWla`). Non-generator rows would muddy a
working system, and `handleAddGeneratorService` requires a `generatorId` anyway.

### Not the existing service-call machinery either

Checking `Start Service Call` fires the Make scenario "Service Call Trigger"
(`wflMovlr8seWxSUul`), which creates **an entire new Job record**. That's right for a scheduled
billable return trip and far too heavy for "stopped by, tightened a lug, 20 minutes." This log is
the lightweight complement, not a replacement.

### Shape — Neon-native at 4c, or Airtable mirroring this DDL if built sooner

`001_time_entries.sql` states the rule this follows: *"keep BOTH job_id (nullable FK) AND job_name
(static text)... the text snapshot IS the history. Never derive job_name from job_id."*

```sql
-- db/schema/00N_job_service_visits.sql
CREATE TABLE job_service_visits (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id     text UNIQUE,              -- ON CONFLICT target while Airtable is in the path
  job_id          uuid REFERENCES jobs(id), -- nullable
  job_name        text NOT NULL,            -- static snapshot; survives archive/rename
  service_date    date NOT NULL,
  logged_by_id    uuid REFERENCES employees(id),
  logged_by       text,                     -- snapshot, same rule
  visit_type      text,                     -- CHECK mirrors the JS whitelist
  work_performed  text NOT NULL,
  parts_used      text,
  labor_hours     numeric(10,2),
  under_warranty  boolean,                  -- STORED at log time, not derived
  followup_needed boolean DEFAULT false,
  followup_notes  text,
  created_at      timestamptz DEFAULT now(),
  synced_at       timestamptz
);
```

Three design calls worth keeping:

- **`under_warranty` is stored, not derived.** A view *could* compute
  `service_date <= warranty_end_date`, but editing the finish date later would silently rewrite
  history. Store what the tech asserted at the time; derive a separate
  `_currently_within_warranty` in a view for reporting. Same discipline as the filtered-vs-
  unfiltered rollup trap.
- **`visit_type` is a JS whitelist** (the `SERVICE_TYPE_OPTS` precedent) — stops Airtable typecast
  inventing options, and transcribes directly to a Postgres `CHECK`.
  Proposed: `Warranty / Callback · Service – Billable · Punch List · Troubleshoot · Other`.
- **Snapshot job name and employee name** alongside the FKs, per the rule above.

### Handlers

| Action | Method | Tier |
|---|---|---|
| `jobServiceVisits` | GET | any signed-in role |
| `addJobServiceVisit` | POST | `_NON_VIEWER`; stamps `Logged By` server-side from the token, exactly like `Submitted By` on expenses |
| `updateJobServiceVisit` / `deleteJobServiceVisit` | POST | `_NON_VIEWER` + in-handler guard: admin/office any, employee own only |

> ⚠ If built against Airtable: filter by job with the `handleGetJobInvoices` pattern — `FIND` as a
> loose **prefilter**, then **verify the linked record ID in memory**. Seven of these sites were
> fixed on 2026-08-03; don't add an eighth.

### Frontend

A **🔧 Service** tab on the job detail, visible to **all roles** (not `admin-only`) — crews need to
know what the last person did. Newest-first cards (date · type · who · what). A
`+ Log Service Visit` button opening a modal that pushes a back-stack entry (`pushBackEntry`), per
the house convention. Where the job has a generator, the tab also lists that generator's service
records read-only, so one screen answers "what has been done here."

### The payoff from combining the two parts

The log form shows a live banner — *"This job is under warranty until 2027-02-14 (168 days left)"* —
and pre-checks `Under Warranty`. That is the actual question being asked on the drive out: is this
on our dime or theirs.

---

## 4. Slices

| # | Work | Touches | When |
|---|---|---|---|
| 0 | Airtable by hand: `Warranty Months` field | none | with slice 1 |
| 1 | Finish Date + warranty end + header badge; `addMonthsClamped()` + tests | `airtable.js`, `index.html` | any time |
| 2 | Neon: `006_job_warranty.sql` + 2 ETL lines | `db/` | any time after 1 |
| 3 | Service visit log — table, handlers, Service tab | both | **Step 4c** |
| 4 | Optional: expiring-in-60-days job filter, open-follow-ups list, photos on a visit | both | after 3 |

Tests go in `tests/handlers.test.mjs` per the house rule: the clamped month-add edges
(2028-02-29, 2026-01-31 at 3 months), the authz tiers, the own-record guard, and the job-link
verification.

---

## 5. Open decisions

1. **One clamped date helper, or two with different semantics?** (§2 — recommend one.)
2. **Employee edit window on their own service note** — forever, or ~7 days? Expenses gate on
   "until approved"; a service note has no approval state to key off.
3. **Does the Service tab show generator service records read-only** alongside the log when the
   job has a generator? (Recommended, but it's extra work in slice 3.)
4. **Part 1 now, or hold everything for 4c?** (§1 — recommend Part 1 now, Part 2 at 4c.)

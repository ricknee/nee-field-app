# Plan — Time Entries → Neon migration readiness

_Created 2026-06-06. First Neon vertical slice is native time tracking; this is the
data-side readiness work + the "Hours by Job" history view the owner asked for._

## Why this exists

Time Entries are the first table to move to Neon (see SYSTEM-MAP → The Neon Plan).
Before touching schema we needed to answer two questions:

1. **Is the historical hours data safe?** ~79% of all time entries are for jobs that no
   longer exist as Job (project) records in Airtable. The owner wants that history kept.
2. **Can we give the team a "scroll through jobs, see total hours" view** that includes
   those historical/orphaned jobs?

Both answers are **yes**, because each Time Entry stores the job name as a **static text
field** (`Job Name (Text)`, `singleLineText`), independent of the `Job` linked-record.
Deleting a project does not delete or blank its time entries.

## Coverage snapshot — production scan 2026-06-06 (acceptance baseline)

Read-only scan of all Time Entries in the main base (`appiqWg6SvKcGfMAu`, table
`tbl13k0Dq4EzgOFJh`). Use these numbers as the **migration acceptance checks** — after the
ETL, Neon must reproduce them exactly.

| Metric | Value |
|---|---|
| Total entries | **14,171** |
| Total hours | **50,178.8** |
| Date range | **2021-05-12 → 2026-06-05** (~5 yrs) |
| Has `Job Name (Text)` | **14,166 (100.0%)** — only 5 blank |
| Live `Job` link (current project) | 2,996 (21.1%) |
| No link (orphaned / historical) | 11,175 (78.9%) |
| &nbsp;&nbsp;↳ recoverable (has name) | **11,174 (78.9%)** |
| &nbsp;&nbsp;↳ lost label (no link, no name) | **1** |
| Distinct job buckets (all-time) | 376 |
| Distinct orphaned buckets | 327 |
| Current Jobs (projects) | 100 records |

**Takeaway:** history is intact and labeled. Exactly **1** record is unrecoverable
(no link, no name); **5** total lack a name. Everything else groups cleanly by job name.

## Target Neon schema — BUILT 2026-07-27

**Step 1 is done.** The schema is applied to the default branch of Neon project
`damp-silence-99074350` (`employees`, `jobs`, `time_entries` + 4 indexes). The
annotated source of truth is **`db/schema/001_time_entries.sql`** — read that, not the
sketch that used to live here.

**Core design rule (carry the Airtable pattern forward):** keep BOTH `job_id` (nullable
FK) AND `job_name` (static text). The text snapshot is what makes historical entries
survive when no project row exists — same principle as the inventory Jobs-mirror-drop work
(text snapshot + optional link). Do **not** make `job_name` a view/lookup off `job_id`.

### What the 200-row sample load corrected in the original sketch

Validated by loading 100 oldest + 100 newest entries into a throwaway Neon branch
(`db/etl/time-entries-sample.mjs` → `db/etl/load-sample.mjs`). Five things the sketch got
wrong or missed:

1. **`hours` is NOT `duration_seconds / 3600`.** Airtable's live formula is
   `ROUND((Duration (Seconds) / 3600) * 4, 0) / 4` — **rounded to the nearest quarter
   hour**. Plain division mismatched **131 of 200** sampled rows; the rounding rule
   mismatched **0**. The two differ by 1.27 h on 200 rows ≈ **~90 h of payroll error**
   across the full table. This was the single highest-value find of the step.
2. **The checkbox is `Labor Reviewed`, not `Reviewed`.** The sketch's field name does not
   exist, so the column would have silently imported as all-`false`.
3. **Missing fields:** `Notes` (~12% of rows), `Source` (`TSheets`/`Manual`), `Labor Type`,
   `Billable`. `Source` matters for the cutover — it identifies importer-owned rows.
4. **`job_name` must be nullable**, not `NOT NULL` — 5 prod rows are blank, so the ETL
   would have aborted on them.
5. **`duration_seconds` is `numeric(12,1)`, not `integer`** — the Airtable number field is
   precision 1.

Also added: **`week_start_date`** as a generated Monday-of-week column, mirroring the
Airtable `Week Start Date` formula that payroll groups on.

Confirmed working: FK resolution by `airtable_id`, upsert idempotency (re-running a load
left the row count unchanged), and the `hoursByJob` GROUP BY porting 1:1 including the
`historical` flag.

### Note on the acceptance baseline

The 2026-06-06 numbers below are a **June snapshot, not a fixed target** — the table keeps
growing as time is logged. The ETL therefore does **not** compare against hard-coded
numbers: it computes the Airtable-side aggregates in memory during extraction and diffs
Neon against *those*, so it self-baselines on every run and stays correct as the data
moves.

### Credentials

`.env` holds **`AIRTABLE_PROD_READ_PAT`** — a READ-ONLY PAT (`data.records:read` +
`schema.bases:read`) scoped to prod `appiqWg6SvKcGfMAu`. Verified 2026-07-27: reads
return 200, writes return 403. `AIRTABLE_API_KEY` stays pointed at the sandbox for
`netlify dev`. The ETL prefers the prod read PAT and falls back to `AIRTABLE_API_KEY`.

## The "Hours by Job" view — build now, ports 1:1

Build it now as a read-only Airtable-backed handler so the team gets the view immediately;
the query shape is identical in Neon, so it is **not throwaway work**.

- **Now (Airtable):** new `handleHoursByJob` in `airtable.js` — `fetchAll("Time Entries")`,
  group by `Job Name (Text)`, sum `Hours`, count entries, min/max `Work Date`. Read-only,
  `_PAYROLL`/admin tier. Frontend: a searchable, sortable scroll list in the payroll/admin
  area — row = **job name · total hours · # entries · date range**, historical jobs flagged.
- **Later (Neon):** the same view is one query —
  `SELECT job_name, SUM(hours), COUNT(*), MIN(work_date), MAX(work_date)
   FROM time_entries GROUP BY job_name ORDER BY SUM(hours) DESC;`

## Migration steps (when the slice begins)

**STATUS 2026-07-30 — steps 1-5 DONE, step 6 shipped as a fail-soft mirror rather than a
move, step 7 (reads) PARTIALLY done. On origin/main as `d64cb41` + `d6c1a1f`.**

Live shape — two independent pipes from the same source, reconciled daily:

```
QB Time API
  |                                   (unchanged safety net)
  +--> MAKE 4546051 (21:00) --------> Airtable Time Entries
  |
  +--> PULLER (hourly) -------------> Neon time_entries   [key: qb_timesheet_id]
                                            ^
App time-entry writes --> Airtable + Neon mirror          [key: airtable_id]
                                            |
handleHoursByJob --> NEON FIRST, Airtable fallback -------+
```

**Make is NOT retired.** It keeps writing Airtable as an independent copy to reconcile
against. Retiring it is a later sitting, once the check-only reconciler has agreed for
several days running.

**⚠ `QB_TIME_TOKEN` must be set in the Netlify dashboard** or the hourly function logs
"missing config" and does nothing. Fails safe, but the puller is inert until it is set.

Read "What the live data revealed" below before touching the puller's insert policy —
the counts are not intuitive and the defaults encode a payroll decision.

---

## FORWARD PLAN — what happens next (set 2026-07-30)

### The dependency that drives the order

**Make cannot be retired until payroll reads leave Airtable.** Kill Make and Airtable stops
receiving QB time, while `handlePayrollEntries` and the hours rollups are still reading it.
So the order is forced: **linkage → payroll reads → retire Make.** Billing is a separate track.

A useful discovery that splits the work: **`handlePayrollEntries` returns no billing fields** —
just id, employee, date, duration, hours, class, city tax, job, reviewed. Only the *per-job*
Time Entries tab (`handleTimeEntries`) needs the labor-billing layer. So the payroll reads are a
small job and the per-job tab is a large one; they do not have to happen together.

### Now → ~2026-08-09 — verification only (~1 h, spread out)

No building. Confirms what shipped on 07-30.

- **Payroll smoke test + generated PDF** (~20 min) — **before the ~Aug 9 run.** The weekly
  round-up (`4ac5afb`) is not smoke-tested; I could not generate the PDF.
- **City-tax allocation** — confirm with whoever handles withholding. The weekly round-up is
  assigned to the jurisdiction with the most hours that week.
- **Reconciler daily** (~5 min) — `node db/etl/time-entries-full.mjs`, no flags. The run the
  morning after a Make 21:00 is the one that empirically settles double-counting.
- **Watch the write mirror** on the next real add / edit / Labor-Reviewed tick — it shipped
  alongside the broken driver and has never been exercised.

> **🛑 STOP POINT — and a good one.** Everything works, Make is intact, nothing is half-migrated.
> This state is stable indefinitely. There is no decay if the next slice never happens.

### Slice 2 — Neon becomes the source of truth for time

**Sitting A — linkage + reads (~3-4 h)**

1. Backfill `airtable_id` onto puller-created rows by natural key — the claim pass in reverse,
   matching the rows Make creates in Airtable to the rows the puller already made (~45 min)
2. Decide the canonical entry id: Neon `uuid`, carrying `airtable_id` alongside while Make
   lives (~30 min)
3. Flip `handlePayrollEntries` to Neon-first + Airtable fallback + `_source` (~1 h)
4. Flip the payroll rollups and my-hours reads the same way (~1 h)

> **🛑 STOP POINT.** All time reads served by Neon, writes still Airtable-first. Fully
> reversible — the fallbacks stay in place.

**Sitting B — writes (~3 h)**

5. Flip the four write paths to Neon-first; Airtable becomes the mirror (the reverse of the
   07-30 arrangement)
6. Soak and reconcile

> **🛑 STOP POINT.** Neon authoritative for time. Airtable still written, still correct, still
> a working fallback.

### Slice 3 — retire Make from the time path (~2 h)

Only after **several consecutive clean reconciler days**. Turn off scenario `4546051`; keep the
Airtable table and Make's other ~69 scenarios. Rotate the QB `tsheets` credentials once it is
off (rotating sooner breaks Make).

> **🛑 STOP POINT.** Make out of the time path — the original goal of this whole slice.

### Slice 4 — Jobs master data (~3 h) — ADDED 2026-07-31

**Jobs has 165 distinct fields.** That is not one migration, it is four groups:

| Group | ~Count | Notes |
|---|---|---|
| **Master data** | ~30 | name, PO / PO-Locked / PO Number, status, type, year, full + split address, start/finish dates, contractor, notes, meter #, tax status |
| **Links** | ~25 | contractor, contacts, estimates, invoices, expenses, inspections, allocations, schedule |
| **Financial rollups** | ~40 | **BLOCKED** — GP families, actual/estimated/projected costs, T&M revenue, unbilled |
| **External refs** | ~25 | 18 pCloud folder IDs, Trello card IDs, automation flags |

Only the **master-data** group moves in this slice. The financial ~40 roll up from estimates,
invoices, expenses and labor allocations — none of which are in Neon — so they cannot be
computed there yet and are genuinely last, not next.

**Why this slice comes BEFORE labor billing:** `Billable Hourly Rate (from Labor Billable Rates)`
is a lookup from a **`Labor Billable Rates`** table hanging off Jobs. The T&M rate the billing
slice needs lives here.

**Bonus find:** Jobs already stores **`TSheets Job ID`** (e.g. `29725244`) — a far stronger key
for the puller's job resolution than the current `po_locked` string match. Adopt it when this
slice touches the table.

**Do not write** `Google Contact ID`, `Sync Status`, `Last Synced At`, `Needs Sync to Google` —
Make owns them (see CLAUDE.md).

> **🛑 STOP POINT.** Jobs master data in Neon, nothing reading it yet. Purely additive.

### Slice 5 — labor billing allocations (~3-4 sittings)

The big one. Two linked tables, four rollups, rate lookups, and the GP formulas. Unblocks the
per-job Time Entries tab and satisfies the hard constraint on the unify-estimates bet. Scoped in
"What the live data revealed" → treat as its own bet, not a phase.

### On these estimates

They are **build** time. On 2026-07-30 the seven planned phases landed roughly on schedule, but
debugging the dependency-bundling failure cost an extra hour that no estimate would have caught.
Assume a real sitting runs longer than the sum of its parts.

**The stop points matter more than the estimates.** Every one leaves the system coherent, with
fallbacks intact and nothing half-done.

**Only two steps in this entire plan are irreversible:** retiring Make (Slice 3), and eventually
dropping Airtable's Time Entries table (not scheduled at all). Everything before them is
additive and can be rolled back by reverting a commit.

1. ~~Model `time_entries` (+ `employees`, `jobs`) on a Neon branch with real FKs.~~ **DONE** —
   applied to the default branch; see `db/schema/001_time_entries.sql`.
2. ~~ETL: page all rows, map fields, **upsert by `airtable_id`** (idempotent re-runnable).~~
   **DONE** — `db/etl/time-entries-full.mjs`. Full prod load verified, then re-run end-to-end
   to prove idempotency (identical results, no duplicates).
3. ~~Repair the **5 nameless** rows.~~ **DONE 2026-07-27** — 4 of 5 were recoverable from
   their still-live `Job` link and were labeled **in Airtable** (`Adena DG (31614)` ×1,
   `Shop Work` ×3), then picked up by a re-run. Blank `job_name` is now **1**: Scott Koehn,
   2026-05-18, 8.00 h — no link, no name, genuinely unrecoverable without asking him.
   NOTE: repairs must be made in **Airtable**, not Neon — Airtable is still the source of
   truth, so a Neon-only edit is overwritten by the next ETL run.
   All 5 were recent manual entries (`source` is null, i.e. not from QB Time), so the
   root cause is hand-entry skipping the job field, not historical rot.
4. ~~Rebuild the payroll/rollup formulas as Neon views; verify against Airtable (dual-read).~~
   **DONE 2026-07-27.** 4a = the ETL's acceptance checks diff Neon vs Airtable every run.
   4b = three Neon views (`v_hours_by_job`, `v_hours_daily`, `v_hours_by_employee_week`)
   plus `netlify/functions/_neon.js` and a shadow read on `handleHoursByJob`.
   Verified live: `match: true`, 381 buckets both sides, totals identical, ~330 ms.
   **Also closed the deletion-drift gap** — the ETL now reconciles rows deleted upstream
   into a `time_entries_deleted` tombstone table, with a guardrail that aborts rather than
   tombstoning an implausible share of the table (a truncated extract must not look like a
   mass delete).
   **REMAINING for 4b: set `DATABASE_URL` in the Netlify dashboard**, then soak.
5. **Build a scheduled QB Time → Neon pull. DECIDED 2026-07-27: replace Make on this path
   rather than repoint it.**

   Original plan was to swap the destination module inside the Make `watchTimesheet`
   scenario. Owner's stated problem is that **Make breaks intermittently**, and QuickBooks
   Time exposes a direct API (owner already has app profiles under Company Settings → API;
   an old unused "Vacation Time API" profile exists and can be ignored/replaced).

   **Why a pull beats both Make and webhooks here:** Make and webhooks are *push* — one
   failed run or undelivered event silently loses a record, and nothing notices. A scheduled
   pull is **self-healing**: it asks "everything modified since my watermark," so a failed
   run costs a retry, not data. Same property that makes `time-entries-full.mjs` safe to
   re-run.

   Shape:
   - Netlify **Scheduled Function** (built-in; no new infra or cost)
   - QB Time API with a `modified_since` watermark
   - Upsert on **`qb_timesheet_id`** (see prerequisite below)
   - Acceptance-check style diffing so drift is loud, not silent
   - Lives in-repo: diffable, testable via `tests/handlers.test.mjs`, revertible with git —
     unlike a Make scenario, which is unversioned config

   **Runs in PARALLEL with Make at first** — Make keeps writing Airtable, the puller writes
   Neon, and the two get compared. Same shadow pattern as step 4b. Nothing is retired until
   they agree.

   **PREREQUISITE — do this before the puller writes anything:** add a nullable
   `qb_timesheet_id text UNIQUE` to `time_entries`. Today the idempotency key is
   `airtable_id`, which will not exist for rows QB writes directly; without a conflict
   target a replayed run duplicates hours. No QuickBooks/TSheets ID is currently stored
   anywhere in the Airtable Time Entries fields (only `Source` = "TSheets"), so this is net-
   new and must be confirmed available from the API response.

   **Open questions — ALL RESOLVED 2026-07-27 by probing the live API:**
   - The **"tsheets" profile IS Make's** — its OAuth Redirect URI is
     `https://www.integromat.com/oAuth/cb/tsheets` (Integromat = Make's former name).
     **Do not touch it while Make is live.** Use a separate profile for the puller.
   - **No OAuth flow needed.** QB Time mints long-lived tokens in the web UI (Add Token);
     the puller just sends `Authorization: Bearer $QB_TIME_TOKEN`. Verified working.
   - Rate limits are a non-issue at this volume.

### QB Time API — verified request shape

`GET https://rest.tsheets.com/api/v1/timesheets?modified_since=<ISO>&per_page=50`

- `modified_since` must be **ISO-8601 WITHOUT milliseconds** — `2026-07-13T23:21:44+00:00`.
  A JS `toISOString()` (which emits `.075Z`) is rejected with **HTTP 417**. Strip the ms.
- Response: `results.timesheets` is an **object keyed by id**, not an array.
- `more: true` signals another page — paginate with `page=N`.
- `supplemental_data` returns `jobcodes`, `users`, and `customfields` in the same call, so
  one request gives both the rows and their lookup tables.

### QB Time → Neon field map (verified against live data)

| QB Time | Neon `time_entries` |
|---|---|
| `id` (e.g. 611431596) | **`qb_timesheet_id`** — the upsert key |
| `duration` (seconds, e.g. 2880) | `duration_seconds` → `hours` generated (quarter-hour rule) |
| `date` ("2026-07-10") | `work_date` → `week_start_date` generated |
| `user_id` → `supplemental_data.users` | `employee_name` + `employee_id` FK |
| `jobcode_id` → `supplemental_data.jobcodes[].name` | `job_name` + `job_id` FK |
| customfield **65840** "Taxes" | `city_taxes` |
| customfield **71185** "Class" | `class` |
| customfield **71183** "Service Item" | (Airtable's "Service Item", = "LABOR") |
| customfield **71181** "Billable" ("Yes"/"No") | `billable` (coerce to boolean) |
| customfield 71833 "Job Services" | empty in all sampled rows — ignore for now |
| `notes` | `notes` |
| `last_modified` | the sync watermark |
| — | `source` = 'TSheets' |

Jobcode names already match the Airtable `Job Name (Text)` convention — e.g.
`"Shop Work (SHS 115)"`, `"Trail Cabinet (CLT 256)"` — so `job_name` maps straight across.
Jobcodes are hierarchical (`parent_id`); the leaf name is the one to use.

### Sync behaviour (verified 2026-07-27)

- **Cadence:** scheduled pull, NOT event-driven. Nothing fires on clock-out, so there is no
  event to miss. Run **hourly** — a pull is cheap and self-healing, and there's no reason to
  inherit Make's nightly batch cadence (that existed because Make bills per operation).
  Worst-case lag from clock-out to Neon: one hour.
- **Edits sync automatically.** Editing a timesheet bumps `last_modified`, so the next
  watermark pull returns it and the `ON CONFLICT (qb_timesheet_id)` upsert overwrites the
  existing row. No duplicate, no special handling. Strictly better than push, where edit
  propagation depends on scenario config.
- **DELETIONS — use the dedicated endpoint.** `GET /api/v1/timesheets_deleted?modified_since=`
  works with the same watermark (verified 200). **This is not theoretical: 3 timesheets were
  deleted in the 30 days to 2026-07-27.** Without polling this, deleted hours would live on
  in Neon forever and quietly inflate payroll. Poll BOTH endpoints each run.
- **Timesheet `type`:** `regular` (clock in/out) and `manual` (hand-entered) both seen; both
  are real hours. `state` is OPEN while clocked in — an open timesheet has partial duration,
  which is harmless since the upsert settles it to the final value once closed.
- **Breaks:** the `Lunch Break` jobcode has jobcode-`type` `unpaid_break` (the only
  non-regular jobcode). Breaks arrive as ordinary timesheets pointing at that jobcode.
  **DECIDE BEFORE CUTOVER — and first check what Make does today**, so the puller reproduces
  current behaviour rather than silently changing hour totals: exclude unpaid_break jobcodes,
  or import them flagged.

### ⚠ App-owned fields the puller MUST NOT overwrite

`labor_reviewed` does **not** exist in QuickBooks Time — it is set in the app/Airtable after
the fact. If the puller includes it in `ON CONFLICT DO UPDATE SET …`, every re-sync silently
resets reviewed flags on entries QB happens to touch. Exclude it (and any future app-owned
column) from the update set; the upsert should only overwrite QB-sourced columns.
Same class of trap as the quarter-hour `hours` bug: it would look fine and quietly corrupt.

6. Cut over: retire the Make time importer, make Neon authoritative. Also move the app's own
   time-entry writes (`handleCreateTimeEntry` / `handleUpdateTimeEntry`, currently writing to
   Airtable) over to Neon — "cut over writes" means these too, not just the importer.

## Acceptance checks — PASSED 2026-07-27

Automated in `db/etl/time-entries-full.mjs`; it exits non-zero on any mismatch. All eight
passed against production on the full load **and** on a second identical run:

| Check | Airtable | Neon |
|---|---|---|
| Row count | 14,522 | 14,522 |
| Total hours | 51,364.25 | 51,364.25 |
| First work date | 2021-05-12 | 2021-05-12 |
| Last work date | 2026-07-24 | 2026-07-24 |
| Blank `job_name` | 5 | 5 |
| No job link (historical) | 11,178 | 11,178 |
| Distinct `job_name` | 380 | 380 |
| Per-job hour buckets mismatched | 0 | 0 |

The **total-hours match is the load-bearing one**: Neon's `hours` is a generated column
computing the quarter-hour rule from `duration_seconds`, while the Airtable side is read
straight from Airtable's own `Hours` field. Agreement to the cent across 14,522 rows and
all 380 job buckets independently confirms the rounding rule is right — the naive
`seconds/3600` would have drifted by roughly 90 hours.

Historical share holds at **77%** (11,178 of 14,522 have no live job link) and every one
of them still carries its `job_name` snapshot, which was the original premise of the slice.

## What the live data revealed (2026-07-30)

Probing QuickBooks directly for the first time turned up two things the plan had assumed
away. Both are now encoded in `netlify/functions/qb-time-pull.js`.

**QB holds 23,669 timesheets; Airtable holds 14,556.** That gap is not drift:

1. **Make's implicit jobcode filter — by design, and replicated.** Make's "Seach Job Name"
   module looks up `{Job PO - Locked}` for the jobcode name and, finding no Job, **drops the
   bundle entirely**. So Make only ever imported timesheets whose jobcode maps to a real Job.
   **7,968** in-range timesheets fall out this way — **5,815 of them `Lunch Break`**, plus
   `Travel`, `Vacation`, unqualified `Shop Work`, and `Troy Koehn (MIT 380)` (a real-looking
   job with no Airtable record). A puller that honestly imported `jobcode_type: "all"` — which
   is what Make's trigger literally requests — would inject thousands of hours payroll has
   never counted. The puller replicates the filter and **reports every skip** rather than
   silently inflating. Whether unpaid breaks *should* be excluded is a real question; it is
   just not a question a data migration gets to answer as a side effect.

2. ~~**Make has been losing real hours — 712 of them.**~~ **RETRACTED, same day — this was a
   measurement error, not a finding.** The claim came from counting QB timesheet ids absent
   from Neon. That is the correct test for "should the puller insert this row"; it is the
   WRONG test for "is anyone unpaid". QB splits a working day into several timesheets per
   clock-in/out, and editing a timesheet mints a new id — so an absent id usually means an
   edited or re-split version of a segment already imported, not lost work.

   The honest comparison is total hours per **(employee, work_date, job)**. `db/etl/qb-gap-report.mjs`
   does exactly that. Across both paid periods (2026-06-28..07-25), with break-type jobcodes
   excluded:

   > **QB ahead of Neon: 0.00 h. Neon ahead of QB: 8.75 h.**

   Every one of the 10 "missing" July timesheets sits in a bucket Airtable already holds MORE
   hours for. Nobody is owed. If anything Airtable runs slightly ahead of QuickBooks, which is
   consistent with payroll adjustments made in the app after import.

**Insert policy (owner decision 2026-07-30):** `INSERT_FLOOR_DATE = "2026-07-26"`, the start of
the open pay period. Given the retraction above, this guard is doing something better than the
conservatism it was chosen for: inserting those rows would have **added 21.25 h on top of
buckets that already exceed QB** — an over-count, not a correction. Keep the floor.

**Rule this bought:** an unmatched id is a data-plumbing signal. Before it becomes a claim about
money, reconcile at the (employee, date, job) level and exclude jobcodes Make never imported.

**Record-id linkage blocks the remaining read flips.** Puller-created Neon rows carry
`qb_timesheet_id` but no `airtable_id`, while Make separately creates the Airtable record the
payroll UI edits against. Entry-level reads (`handlePayrollEntries`, my-hours) therefore stay
on Airtable deliberately — they are 14-day/YTD filtered queries, not the full-table scan that
made `hoursByJob` cost 15.4 s, so there is no urgency. `mirrorTimeEntryToNeon` already adopts
an unclaimed row by natural key and stamps `airtable_id` onto it, so rows the app touches
converge on one row carrying both keys; a general solution builds on that.

## Open / deferred

- **Name variants** (e.g. `Scenic Ridge Church (CLS 80 R)` vs `(CLS 80 F)` = rough/final
  phases) are intentionally NOT auto-merged — they are meaningful. Surface, don't dedupe.
- **Non-job buckets** (`Shop Work` 926 h, `Office Work`, etc.) are overhead — keep as their
  own buckets; decide later whether the view separates overhead from billable jobs.
- The 5 blank-name rows: decide repair vs sentinel before ETL.
- **`city_taxes` stays free text for the first cut** (matches the carry-the-text-forward rule).
  Today it originates in **QuickBooks Time**, so the values — including misspellings like
  `Massilon` and `New Philadephia` — are authoritative, and the payroll dropdown
  (`PR_CITY_TAXES`) must match them verbatim. **Trigger to normalize:** when time entry goes
  native in Neon and the QB Time importer is retired (step 5/6), QB stops being the source —
  that's the moment to promote `city_taxes` to a reference table (code + display name), map the
  legacy QB spellings on import, and make the dropdown data-driven. Don't normalize before then
  or the dropdown desyncs from QB's strings.

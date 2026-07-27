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

The 2026-06-06 numbers below are a **June snapshot, not a fixed target** — production
read **14,522 entries** on 2026-07-27 because time keeps being logged. Re-capture the
counts against prod immediately before the full ETL and use those as the acceptance
check.

### Credential gap for step 2

The local `.env` PAT is scoped to the **sandbox** base (`appojcmXxqDUdJDYB`) only — the
sample was validated there, which is fine for schema shape since it is a structural
duplicate. **The full ETL needs a prod-scoped (read) PAT** against `appiqWg6SvKcGfMAu`.
Sort that out before starting step 2.

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

**Step 1 DONE 2026-07-27** — schema applied + proven against a 200-row sample.
**RESUME AT STEP 2.**

1. ~~Model `time_entries` (+ `employees`, `jobs`) on a Neon branch with real FKs.~~ DONE.
2. ETL: page all 14,171 rows (the 2026-06-06 scan script is the extractor skeleton), map
   fields, **upsert by `airtable_id`** (idempotent re-runnable).
3. Repair the **5 nameless** rows first (or import with a sentinel `job_name` and flag).
4. Rebuild the payroll/rollup formulas as Neon views; verify against Airtable (dual-read).
5. Repoint the **QuickBooks Time importer** (Make `watchTimesheet`) from Airtable → Neon.
6. Cut over writes; retire the Make time importer.

## Acceptance checks (assert post-ETL)

- Row count = 14,171; total hours ≈ 50,178.8; date range 2021-05-12 → 2026-06-05.
- `job_name` non-null on ≥ 14,166 rows; distinct `job_name` = 376.
- Sum(hours) per `job_name` matches the Airtable scan per bucket.

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

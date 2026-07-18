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

## Target Neon schema (sketch)

```sql
CREATE TABLE time_entries (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id      text UNIQUE,            -- idempotent ETL key (ON CONFLICT), like Push ID
  employee_name    text,                   -- "Employee" text snapshot
  employee_id      uuid REFERENCES employees(id),   -- from "Employee (Linked)", nullable
  work_date        date,
  duration_seconds integer NOT NULL,       -- QB Time is authoritative in seconds
  hours            numeric GENERATED ALWAYS AS (duration_seconds / 3600.0) STORED,
  city_taxes       text,
  class            text,
  job_id           uuid REFERENCES jobs(id),   -- nullable: NULL for the 79% historical
  job_name         text NOT NULL,          -- STATIC label snapshot — this is the history
  reviewed         boolean DEFAULT false,
  created_at       timestamptz DEFAULT now()
);
CREATE INDEX ON time_entries (job_name);
CREATE INDEX ON time_entries (work_date);
```

**Core design rule (carry the Airtable pattern forward):** keep BOTH `job_id` (nullable
FK) AND `job_name` (static text). The text snapshot is what makes historical entries
survive when no project row exists — same principle as the inventory Jobs-mirror-drop work
(text snapshot + optional link). Do **not** make `job_name` a view/lookup off `job_id`.

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

1. Model `time_entries` (+ `employees`, `jobs`) on a Neon branch with real FKs.
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

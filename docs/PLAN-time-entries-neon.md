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

**Steps 1, 2 and the verify half of 4 are DONE (2026-07-27).**
**RESUME AT STEP 4b — rebuild the payroll rollups as Neon views + app-side dual-read.**

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
4. Rebuild the payroll/rollup formulas as Neon views; verify against Airtable (dual-read).
   **4a verify DONE** — the ETL's own acceptance checks diff Neon against Airtable on every
   run. **4b (views + app-side dual-read) is the next work.**
5. Repoint the **QuickBooks Time importer** (Make `watchTimesheet`) from Airtable → Neon.
6. Cut over writes; retire the Make time importer.

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

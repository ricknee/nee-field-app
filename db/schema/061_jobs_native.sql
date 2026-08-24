-- 061_jobs_native.sql — identity cutover, slice 6 (jobs). THE LAST ONE.
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- ── STEP ONE OF EVERY SLICE ────────────────────────────────────────────────
-- `airtable_id NOT NULL` is a formal statement that the table cannot hold a row
-- Airtable has never seen. Dropping it IS what "goes native" means. Slice 4 did
-- the views and the handlers and forgot this one line, and the first real
-- expense after that deploy failed on the constraint (058). Doing it first.

ALTER TABLE jobs ALTER COLUMN airtable_id DROP NOT NULL;

COMMENT ON COLUMN jobs.airtable_id IS
  'NULL on any job created by the app after 2026-08-24 — those are Neon-native and Airtable holds only a best-effort mirror. Resolve a job with `airtable_id = $1 OR id::text = $1` and EMIT `COALESCE(airtable_id, id::text)`, never a bare airtable_id. ⚠⚠ NEVER back-fill this from a mirror: _jobs-sync.js upserts ON CONFLICT (airtable_id) every hour, so a stamped rec id turns that sync from irrelevant into an hourly OVERWRITE of everything the app wrote.';

CREATE INDEX IF NOT EXISTS jobs_id_text_idx ON jobs ((id::text));

-- ── WHY THE REC ID IS NOT STAMPED BACK, AND WHY IT MATTERS MORE HERE ───────
-- Every earlier slice weighed stamp-back against R2 keys. Jobs have a second,
-- larger reason: **`_jobs-sync.js` runs HOURLY** (inside qb-time-pull) and does
--
--     INSERT INTO jobs (...38 columns...) VALUES (...)
--       ON CONFLICT ("airtable_id") DO UPDATE SET <every column> = EXCLUDED...
--
-- A native job has `airtable_id NULL`, conflicts with nothing, and is therefore
-- INVISIBLE to that statement — which is exactly what we want. Stamp the rec id
-- back and the same job becomes a conflict target, so every hour Airtable's copy
-- would overwrite all 38 columns, silently reverting anything edited in the app.
--
-- ⚠⚠ THIS IS THE `estimate_templates` TRAP, AT THE SCALE OF THE WHOLE JOB TABLE:
-- "giving a read-only table a write path turns its hourly ETL from PRESERVING
-- into OVERWRITING." That one reverted five template edits at the top of every
-- hour, silently, and nothing threw. Jobs would revert status, addresses,
-- markup, contacts and PO fields.
--
-- ✅ Verified before writing any code: `syncJobs` is upsert-only — it contains NO
-- DELETE, so it can never remove a native job either. And it early-returns on an
-- empty Airtable read, so a transient blank response cannot blank the table.

-- ── WHAT IS ALREADY SAFE, CHECKED NOT ASSUMED (2026-08-24) ─────────────────
-- 1. ALL 16 CHILD FKs REFERENCE `jobs(id)`, the uuid — time_entries,
--    schedule_entries, job_labor_allocations, job_estimates, expenses, invoices,
--    wire_weigh_ins, pipe_usage, panel_schedules, job_checklists,
--    job_inspections, generators, generator_service, sent_estimate_pdfs,
--    open_punches, clock_punches. A native job is correctly wired downstream;
--    the NOT NULL was the only thing making the row impossible.
--
-- 2. THE GP CHAIN IS UUID-KEYED. `handleJobs` joins
--    `v_job_rollups_true r ON r.id = j.id`, `v_job_financials_true f ON f.id = j.id`
--    and `v_job_labor_cost_true_by_job t ON t.job_id = j.id`. So gross profit,
--    revenue and true labor cost all work for a native job with no view changes —
--    the opposite of slice 3, where `v_invoices` resolved its money CTEs by rec id
--    and would have printed every native T&M invoice at $0.
--
-- 3. `v_invoices` ALREADY PREFERS THE UUID. Its estimate and prior-contract-billing
--    CTEs join `jobs j ON j.airtable_id = e.job_airtable_id` but group by
--    `COALESCE(e.job_id, j.id)` — the slice-3 fix. They stay correct **provided the
--    write side populates `job_estimates.job_id` / `invoices.job_id`**, which is
--    precisely what the resolve sweep in this slice is for. A resolve left on a
--    bare `airtable_id` does not error; it writes a NULL `job_id`, and the row
--    silently drops out of expected revenue.
--
-- ⚠ REMAINING NOT NULLs after this migration — and they are all deliberate:
--   job_labor_allocations, labor_cost_rates, labor_billable_rates
--                        → VESTIGIAL (see 058). Nothing reads or writes them.
--   employee_weekly_time, pipe_usage, wire_weigh_ins, time_entries_deleted
--                        → outside the cutover: ETL-fed from Airtable by
--                          definition, so a row without a rec id is meaningless.
-- With `jobs` free, EVERY table the cutover set out to move is native.

-- Neon slice 2 — schema for the QB Time puller (plan step 5).
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-07-30.
--
-- Same convention as 001: applied BARE via the Neon MCP (which mangles inline SQL
-- comments), with the reasoning kept here. Keep the two in sync by hand.
--
-- Context: until now every row in time_entries arrived via the Airtable ETL and was
-- keyed by `airtable_id`. From this migration on there are TWO independent writers:
--
--   QB Time API --> puller  --> keyed by qb_timesheet_id   (~99% of rows)
--   App writes  --> Airtable + Neon dual-write --> keyed by airtable_id  (~19 rows/yr)
--
-- Make keeps running in parallel writing Airtable, as a safety net to reconcile
-- against. It is NOT retired by this migration.

-- The ETL's idempotency key cannot be required any more: rows the puller creates
-- have no Airtable record at all. This is the single blocking change — without it
-- every puller insert fails on a NOT NULL violation.
ALTER TABLE time_entries ALTER COLUMN airtable_id DROP NOT NULL;

-- The puller's ON CONFLICT target. Nullable (historical + app rows have none) and
-- UNIQUE via a partial-friendly unique index: Postgres treats NULLs as distinct, so
-- the 14.5k pre-existing rows coexist happily.
ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS qb_timesheet_id text;
CREATE UNIQUE INDEX IF NOT EXISTS time_entries_qb_timesheet_id_idx
  ON time_entries (qb_timesheet_id);

-- Belt and braces: with both keys nullable, a bug could otherwise insert a row that
-- neither writer can ever address again — unreachable by upsert, invisible to the
-- reconciler's row counts, but still summed into payroll hours.
ALTER TABLE time_entries ADD CONSTRAINT te_has_a_key
  CHECK (airtable_id IS NOT NULL OR qb_timesheet_id IS NOT NULL);

-- The puller identifies people by QB `user_id`; Neon had no such column, so there was
-- no way to resolve employee_id without going back to Airtable per row. Sourced from
-- the Airtable Employees field "Employee ID" (fldvsUs0s8CCwrfIN) — the same field
-- Make's "Search Employee" module matches on. Values look like "600736".
ALTER TABLE employees ADD COLUMN IF NOT EXISTS qb_user_id text;
CREATE UNIQUE INDEX IF NOT EXISTS employees_qb_user_id_idx
  ON employees (qb_user_id) WHERE qb_user_id IS NOT NULL;

-- QB jobcode names are in the Airtable "Job PO - Locked" format — e.g.
--   "Joe Yoder (CAJ 436)"      NOT jobs.name, which is "Joe Yoder"
-- Make's "Seach Job Name" module matches on {Job PO - Locked} for exactly this reason.
-- Without this column the puller cannot populate job_id, which 2,189 of the 2,198
-- existing QB-sourced rows have. NOT unique: not every job has one (some are blank),
-- and duplicates are possible — Make itself takes maxRecords 1 and accepts the risk.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS po_locked text;
CREATE INDEX IF NOT EXISTS jobs_po_locked_idx ON jobs (po_locked);

-- Watermark store for the incremental pull. One row per sync ("qb_timesheets",
-- "qb_timesheets_deleted"). A pull is self-healing precisely because this is the only
-- state it carries: lose it and the puller re-asks for a wider window, which is free
-- because every write is an upsert.
CREATE TABLE IF NOT EXISTS sync_state (
  key        text PRIMARY KEY,
  watermark  timestamptz,
  updated_at timestamptz DEFAULT now(),
  note       text
);

-- 060_employees_native.sql — identity cutover, slice 5 (employees + login).
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- ── STEP ONE OF EVERY SLICE, AND THE ONE 057 SKIPPED ───────────────────────
-- `airtable_id NOT NULL` is a formal statement that the table cannot hold a row
-- Airtable has never seen. Dropping it IS what "goes native" means. Slice 4 did
-- the views and the handlers and forgot this, and the first real expense after
-- the deploy failed on the constraint (see 058). Doing it first, deliberately.

ALTER TABLE employees ALTER COLUMN airtable_id DROP NOT NULL;

COMMENT ON COLUMN employees.airtable_id IS
  'NULL on any employee hired through the app after 2026-08-24 — those are Neon-native and Airtable holds only a best-effort mirror. Resolve an employee with `airtable_id = $1 OR id::text = $1` and EMIT `COALESCE(airtable_id, id::text)`, never a bare airtable_id: a bare emit returns NULL for a native hire, and NULL as a session id matches nothing (see _revocation.js).';

-- ── WHY THIS SLICE IS SAFE FOR SESSIONS ALREADY IN THE WILD ────────────────
-- The login id is persisted client-side AND baked into a 30-day HMAC token, so
-- every phone in the field is holding a `rec…` right now and will keep sending
-- it for up to a month. The plan flagged that as slice 5's headline risk.
--
-- `COALESCE(airtable_id, id::text)` disposes of it: an employee that HAS a rec
-- id still emits that rec id, byte for byte. No existing id changes, so no
-- existing session can break. Only a native hire — a row that did not exist
-- when those tokens were minted — ever emits a uuid.
--
-- That is why the emit side is COALESCE and not a switch to `id`. Switching
-- would have been the version of this slice that logs the whole crew out.
--
-- ⚠ Verified before writing any code: EVERY child table references
-- `employees(id)` (the uuid), not the rec id —
--   time_entries, labor_cost_rates, employee_weekly_time, job_labor_allocations,
--   schedule_entry_crew, open_punches, clock_punches (+ edited_by, deleted_by),
--   pto_years, pto_requests (+ decided_by).
-- So a native employee is already wired correctly everywhere downstream; the
-- NOT NULL was the only thing making the row impossible.

-- ── THE INDEX THAT MUST FOLLOW THE LOOKUP ──────────────────────────────────
-- Slice 2's lesson: when a lookup moves to a new column the index does not
-- follow it. Here the resolve becomes `airtable_id = $1 OR id::text = $1`. The
-- `id` half is the primary key, so it is already indexed — but Postgres cannot
-- use a PK index for `id::text = $1`, because the cast makes it non-sargable.
-- An expression index on the cast keeps the native branch off a sequential scan
-- as the table grows.
CREATE INDEX IF NOT EXISTS employees_id_text_idx ON employees ((id::text));

-- ── LOGIN IS THE ONE READ THAT CANNOT BE ALLOWED TO GET SLOWER ─────────────
-- `neonLoginCandidate` matches on lower(btrim(...)) of four columns with no
-- index behind any of them. That is fine at 40 employees and is NOT what this
-- slice changes, so it is deliberately left alone — noted here only so the next
-- person does not assume it was considered and rejected for a reason.

-- ⚠ REMAINING NOT NULLs after this migration (checked 2026-08-24):
--   jobs                 → slice 6, the last one
--   job_labor_allocations, labor_cost_rates, labor_billable_rates
--                        → VESTIGIAL, see 058
--   employee_weekly_time, pipe_usage, wire_weigh_ins, time_entries_deleted
--                        → outside the cutover: ETL-fed from Airtable by
--                          definition, so a row without a rec id is meaningless.

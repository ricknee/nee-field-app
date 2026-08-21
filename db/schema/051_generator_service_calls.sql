-- 051_generator_service_calls.sql — the last Airtable automation that CREATED
-- a record moves into the app.
--
-- Replaces `wfledvx1A8oVscWla` "Generator Service Call": when a generator on a
-- service plan came due, it created a Jobs record and ticked a checkbox on the
-- generator. Undeployed with the rest of the base on 2026-08-20.
--
-- ⚠⚠ THE OLD CHECKBOX WAS A PERMANENT LATCH, AND THAT WAS THE BUG.
-- `generators.service_call_created` is set once and NOTHING clears it — not the
-- service record, not completing the job. So a generator got ONE automatic
-- service call in its life and was never prompted again. Seven of twelve
-- generators are already latched and six of those are OVERDUE, one since
-- 2025-02. A service plan that prompts once is not a service plan.
--
-- The fix is not "clear the latch" — that would create a fresh job EVERY HOUR
-- for any generator that stays overdue, which is a runaway, not a reminder.
-- Instead the guard becomes: **one call per DUE DATE.**
--
--   service_call_due_date  =  the `next_service_due` that triggered the call
--
-- and the check only fires when the generator's current `next_service_due`
-- DIFFERS from it. That is self-limiting in both directions:
--
--   · service logged      -> v_generators recomputes next_service_due from
--                            max(service_date) + interval, so it moves forward,
--                            differs, and the NEXT visit gets prompted. The plan
--                            recurs, which is the whole point.
--   · job completed with
--     no service logged   -> next_service_due has NOT moved, so nothing new is
--                            created. Correct: the work still owes a record.
--   · generator stays
--     overdue for months  -> exactly one open job, not one per hour.
--
-- `service_call_created` is KEPT and still written, because it is in the hourly
-- Airtable sync's column list and in `v_generators`. It is now a breadcrumb
-- ("this generator has been auto-called at least once"), not a gate.
--
-- ⚠ NOT BACKFILLED ON PURPOSE. Leaving `service_call_due_date` NULL on the
-- seven latched generators is what makes the six overdue ones eligible again —
-- they are the reason this exists. That is why the code ships behind the
-- `GENERATOR_SERVICE_CALLS` kill switch with a dry run: look at the list of
-- jobs it wants to create BEFORE flipping it on, because each one consumes a PO
-- number and PO numbers are not reusable.

ALTER TABLE generators
  ADD COLUMN IF NOT EXISTS service_call_job_at_id  text,
  ADD COLUMN IF NOT EXISTS service_call_due_date   date,
  ADD COLUMN IF NOT EXISTS service_call_created_at timestamptz;

COMMENT ON COLUMN generators.service_call_job_at_id IS
  'Airtable rec id of the Job the service-call check last created for this generator. There was no link at all before 2026-08-21 — generators.job_id is the INSTALL job, not the service call.';
COMMENT ON COLUMN generators.service_call_due_date IS
  'The v_generators.next_service_due value that triggered the last auto service call. The check fires only when the current next_service_due differs, which is what makes the plan recur exactly once per visit.';
COMMENT ON COLUMN generators.service_call_created IS
  'Legacy Airtable latch, now a breadcrumb only. Do NOT gate on this — it is never cleared. The real guard is service_call_due_date.';

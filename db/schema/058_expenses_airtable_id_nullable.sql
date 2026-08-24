-- 058_expenses_airtable_id_nullable.sql — identity cutover, slice 4.
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- ── THIS SHOULD HAVE SHIPPED INSIDE a04b11f, AND DID NOT ────────────────────
-- `a04b11f` made handleAddGeneralExpense / handleAddLiftExpense Neon-first, and
-- 057 moved every derived money column off Airtable's copy — but neither
-- dropped the constraint that says a native row is impossible. The first real
-- expense entered after the deploy failed with:
--
--   expense.create: null value in column "airtable_id" of relation "expenses"
--   violates not-null constraint
--
-- ⚠ `airtable_id NOT NULL` IS THE CHECKLIST. It is a formal statement that the
-- table cannot hold a row Airtable has never seen, so dropping it is precisely
-- what "goes native" means. Every prior slice did it as step one — 053 for the
-- reference leaves, 054 for payroll, 055 for estimates and invoices. Slice 4
-- did the views (057) and the handlers (a04b11f) and skipped the one-line
-- ALTER, because the view work looked like the schema work.
--
-- The tell that it was missed: 057 is titled "..._prep" and contains no ALTER
-- TABLE at all. A prep migration for a native cutover that drops no constraint
-- is not a prep migration.
--
-- ⚠ Nothing was written when it failed, and that is the reversal's error
-- contract working as designed: `createExpenseNative` uses `neonWrite`, which
-- throws, so the handler returned 500 BEFORE the Airtable mirror ran. No
-- half-record, no orphan in the base, nothing to clean up. Same shape as the
-- slice-3 `$5` bug — a refusal, not a wrong number.
--
-- UNIQUE (airtable_id) is deliberately KEPT. Postgres treats NULLs as distinct,
-- so any number of native rows coexist while a mirrored one still cannot be
-- inserted twice under the same rec id.

ALTER TABLE expenses ALTER COLUMN airtable_id DROP NOT NULL;

COMMENT ON COLUMN expenses.airtable_id IS
  'NULL on any expense created after 2026-08-24 by the field app — those are Neon-native and Airtable holds only a best-effort mirror. Resolve expenses with `airtable_id = $1 OR id::text = $1`, never a bare airtable_id. ⚠⚠ NEVER back-fill this from a mirror: R2 receipt keys are expenses/<handle>/ and listExpenseReceipts lists ONE prefix, so a handle that changes orphans every receipt already stored.';

-- ⚠ REMAINING NOT NULLs, AND WHAT EACH ONE MEANS (checked 2026-08-24):
--   employees            → slice 5, still to do
--   jobs                 → slice 6, still to do
--   job_labor_allocations, labor_cost_rates, labor_billable_rates
--                        → VESTIGIAL. Nothing in either function reads or writes
--                          them; their only writer is a one-off ETL. Dropping
--                          these would imply a native row is expected there.
--   employee_weekly_time, pipe_usage, wire_weigh_ins, time_entries_deleted
--                        → outside the cutover: ETL-fed from Airtable by
--                          definition, so a row without a rec id is meaningless.
-- Anything NOT on that list is already free.

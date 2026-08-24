-- 057_expenses_native_prep.sql — identity cutover, slice 4 (PREP, no handler flips).
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- ── WHAT THIS FIXES, AND WHY IT HAD TO COME FIRST ───────────────────────────
-- Four money columns on `expenses` are AIRTABLE FORMULAS that `syncExpenseToNeon`
-- copies across: Total Cost (Actual), Billable Material Amount $, Unbilled
-- Material Amount $, Reviewed Expenses. A NATIVE expense has no Airtable record
-- to compute them, so every one of those columns is NULL on it, forever.
--
-- `v_expenses` already reproduces all four as `*_calc` (schema 013) and the
-- handlers already read the calc columns. THREE PLACES DID NOT, and each fails
-- as a silent under-count rather than an error:
--
--   1. `v_job_rollups` read the stored `expenses.total_cost_actual` in SIX
--      places — actual_material_cost, actual_subcontract_expense,
--      actual_scissor_lift_expense, actual_rental_equipment_expense,
--      reviewed_expenses_rollup and total_actual_expenses_audit. That is every
--      job cost figure, which is to say GP. A native expense would have been
--      invisible to all six.
--   2. `v_invoices` computed `material_credits` from the stored
--      `billable_material_amount`, so a native CREDIT expense would not have
--      reduced the invoice it belongs to.
--   3. `v_expenses.reviewed_expenses_calc` — the "calc" column — was itself
--      reading the STORED `total_cost_actual`. It only looked reproduced.
--
-- Verified inert before applying, and again after. All 406 expenses, every
-- figure identical to the cent:
--   actual_material_cost 1,043,555.68 · subcontract 10,255.00 · lift 1,825.00
--   rental 15,279.15 · reviewed_expenses 843,704.47 · audit 1,074,151.83
--   invoice_total 1,267,086.19 · v_expenses.reviewed_expenses_calc 843,704.47
-- Credits: 8 rows / -4,235.67 both ways. `total_cost_actual` differs from its
-- calc on 0 of 406 rows at 2dp (sub-cent only: 1,074,151.83 vs 1,074,151.8269).
--
-- ⚠ ONE KNOWN, ACCEPTED DIFFERENCE. Expense reccHttOmXgdNFqRR reports
-- unbilled 0.01 stored vs 0.00 calc. Airtable rounded billable to 2563.50 and
-- subtracted 2563.49; the view keeps 2330.45 * 1.10 = 2563.495 unrounded, so the
-- residual is 0.005 and the view's `< 0.01` floor correctly calls it fully
-- billed. The calc is the more correct of the two and errs toward not proposing
-- a half-cent line onto an invoice. See the money-rounding note in project
-- memory: Airtable is IEEE-754 and inconsistent on half-cents.
--
-- ── HOW THIS MIGRATION IS WRITTEN, AND WHY ──────────────────────────────────
-- ⚠⚠ IT REWRITES FROM `pg_get_viewdef`, NOT FROM THE .sql FILES. The view
-- definitions checked into db/schema/ are STALE — 006 vs 024 once cost a
-- reinstated OT bug by replaying an out-of-date body. Transforming the LIVE
-- definition means this cannot reintroduce whatever else has changed since.
--
-- It is also idempotent: on a second run the replaces match nothing, the
-- assertions still hold, and CREATE OR REPLACE rewrites the identical body.
-- Each block RAISES if its target survived, so a silent partial rewrite is
-- impossible.
--
-- No handler is flipped here. Expenses are still Airtable-first; this only
-- removes the reasons a native one would have been mis-costed.

-- 1. v_job_rollups — six sites, all of them job cost.
DO $mig$
DECLARE
  d text;
  calc text := '(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric))';
BEGIN
  d := pg_get_viewdef('public.v_job_rollups'::regclass, true);
  d := replace(d, 'sum(x.total_cost_actual)',                  'sum'      || calc);
  d := replace(d, 'COALESCE(x.total_cost_actual, 0::numeric)', 'COALESCE' || calc);
  IF position('x.total_cost_actual' in d) > 0 THEN
    RAISE EXCEPTION 'v_job_rollups: a x.total_cost_actual reference survived the rewrite';
  END IF;
  EXECUTE 'CREATE OR REPLACE VIEW public.v_job_rollups AS ' || d;
END $mig$;

-- 2. v_invoices — material credits. Joining v_expenses rather than the base
--    table is what makes the calc available: it already carries the jobs join
--    the markup needs. `e.id` is unchanged, so the join condition still holds.
--
-- ⚠ THIS BLOCK IS THE ONE THAT IS NOT SELF-IDEMPOTENT, and it was caught by
-- replaying the migration rather than by reading it. `e.billable_material_amount`
-- is a PREFIX of `e.billable_material_amount_calc`, so a second pass rewrites the
-- already-migrated column into `..._calc_calc` and the view fails to compile.
-- The other two blocks are naturally idempotent — their targets no longer exist
-- after the first run — but this one needs an explicit "already done" gate.
-- Replay is the only thing that finds this; a migration that has never been run
-- twice has not been tested.
DO $mig$
DECLARE d text;
BEGIN
  d := pg_get_viewdef('public.v_invoices'::regclass, true);
  IF position('JOIN expenses e ON e.id = m.expense_id' in d) = 0 THEN
    RETURN;  -- already migrated; rewriting again would double the _calc suffix
  END IF;
  d := replace(d, 'JOIN expenses e ON e.id = m.expense_id', 'JOIN v_expenses e ON e.id = m.expense_id');
  d := replace(d, 'e.billable_material_amount', 'e.billable_material_amount_calc');
  IF position('JOIN expenses e ON' in d) > 0 THEN
    RAISE EXCEPTION 'v_invoices: the base-table expenses join survived the rewrite';
  END IF;
  IF position('_calc_calc' in d) > 0 THEN
    RAISE EXCEPTION 'v_invoices: the _calc suffix was applied twice';
  END IF;
  EXECUTE 'CREATE OR REPLACE VIEW public.v_invoices AS ' || d;
END $mig$;

-- 3. v_expenses — the calc column that was not a calc.
DO $mig$
DECLARE d text;
BEGIN
  d := pg_get_viewdef('public.v_expenses'::regclass, true);
  d := replace(d,
    'WHEN e.reviewed THEN COALESCE(e.total_cost_actual, 0::numeric)',
    'WHEN e.reviewed THEN COALESCE((COALESCE(e.legacy_material_cost, e.manual_material_cost, 0::numeric) - COALESCE(e.material_credit, 0::numeric)), 0::numeric)');
  IF position('WHEN e.reviewed THEN COALESCE(e.total_cost_actual' in d) > 0 THEN
    RAISE EXCEPTION 'v_expenses: reviewed_expenses_calc still reads the stored column';
  END IF;
  EXECUTE 'CREATE OR REPLACE VIEW public.v_expenses AS ' || d;
END $mig$;

-- The stored columns are deliberately KEPT and still selected by v_expenses.
-- They are Airtable's own answer, and having both side by side is what let the
-- equivalence above be proved. They simply stop being what anything COMPUTES
-- from — on a native expense they are NULL and mean "Airtable never saw this".
COMMENT ON COLUMN expenses.total_cost_actual IS
  'Airtable formula copy, NULL on any expense created natively. Nothing computes from it since 057 — read total_cost_actual_calc from v_expenses instead.';
COMMENT ON COLUMN expenses.billable_material_amount IS
  'Airtable formula copy, NULL on a native expense. Read billable_material_amount_calc; the stored one misses native credits (057).';
COMMENT ON COLUMN expenses.unbilled_material_amount IS
  'Airtable formula copy, NULL on a native expense. Read unbilled_material_amount_calc — it also floors sub-cent residuals, which Airtable does not.';
COMMENT ON COLUMN expenses.reviewed_expenses IS
  'Airtable formula copy, NULL on a native expense. Read reviewed_expenses_calc (057 made it an actual calc).';

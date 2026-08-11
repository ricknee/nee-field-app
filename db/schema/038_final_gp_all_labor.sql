-- ── Final GP counts ALL labor, and overhead jobs are flagged ───────────────
-- Applied BARE to the production branch via the Neon MCP 2026-08-11; this file
-- is the annotated source of truth.
--
-- ⚠⚠ THIS DELIBERATELY CHANGES REPORTED PROFIT. It is the owner's decision, not
-- a bug fix, and the numbers move DOWN on purpose.
--
-- ── 1. WHY: the GP audit's Finding 1 ───────────────────────────────────────
-- `Actual Job Cost (COGS)` counted only labor that had been ticked **Labor
-- Reviewed**. Unticked hours cost $0, so a finished job read as more profitable
-- than it was. Not a defect — it is Airtable's original rule, ported faithfully
-- — but the rule and reality had come apart: 23 of 61 finished jobs carried
-- unticked hours, and Shop Work reported **99.8% profit on 587 hours** because
-- almost none of them were ticked.
--
-- Owner's decision 2026-08-11, presented as (a) make ticking part of closing a
-- job vs (b) count all labor: **"b sounds good to me."**
--
-- The review tick is NOT redundant now — it still gates billing, because
-- `_allocations.js` only creates a billing allocation when hours are reviewed.
-- That was the argument for (b): the checkbox was doing two jobs and failing at
-- one. It keeps the job it is good at.
--
-- ── 2. HOW: one column, not a rewrite of the GP maths ──────────────────────
-- `006` established that labor enters the GP layer at exactly TWO inputs, both
-- in `v_job_rollups_true`, so the substitution happens once, there. That is why
-- this change is four lines and not a re-port:
--
--   actual_labor_cost_reviewed  ->  COALESCE(t.labor_cost_live, 0)   -- was labor_cost_reviewed
--   labor_cost_in_progress      ->  0                                 -- was live minus reviewed
--
-- Forcing `labor_cost_in_progress` to 0 keeps `total_labor_cost_live`
-- (= reviewed + in_progress) IDENTICAL in value. Only final GP and COGS move.
-- Proven, not assumed: live labor $342,295.59 before and after, and **0 rows**
-- changed on `gross_profit_live_dollar`.
--
-- ⚠⚠ THE COLUMN NAME IS NOW A HISTORICAL ARTEFACT. `actual_labor_cost_reviewed`
-- carries the FULL labor cost. Renaming it would break `v_job_financials_true`,
-- which is GENERATED from `v_job_financials` by string-substituting the rollups
-- source (see the DO block in 006) and must stay a faithful copy. The name is
-- wrong; the alternative was worse. Comments carry the truth in three places:
-- the view itself, 006, and here.
--
-- ── 3. WHAT MOVED — 8 jobs, all downward ───────────────────────────────────
--   total final GP   998,827.16  ->  964,332.04     (-34,495.12)
--   total COGS     1,377,631.66  -> 1,432,731.19    (+55,099.53, incl. jobs with no final GP)
--
--   Shop Work           38,065.62 -> 18,988.91   99.8% -> 49.8%
--   Blue Ridge Poultry  33,764.86 -> 28,330.78   95.8% -> 80.4%
--   Jenny Ln 2          28,733.15 -> 23,780.95  100.0% -> 82.8%
--   David Hodges        22,470.61 -> 18,549.95   69.7% -> 57.5%
--   Adam Burton          4,625.00 ->  3,947.96  100.0% -> 85.4%
--   Jenny Ln 8          23,964.97 -> 23,680.59   83.4% -> 82.4%
--   Hardwood Solutions       0.00 ->    -81.25
--   Dennis Hill         13,189.61 -> 13,120.81   94.5% -> 94.1%
--
-- The 100% figures are gone. An electrical contractor does not make 100% gross
-- margin, and those were the tell that cost was missing rather than profit real.
--
-- ── 4. OVERHEAD JOBS ───────────────────────────────────────────────────────
-- Owner, same conversation: *"shop and office work are normally overhead cost so
-- we [don't] worry about gp on those."*
--
-- `jobs.overhead` is a new explicit boolean, true for **Shop Work** and **Office
-- Work**.
--
-- ⚠ `clock_visibility` was NOT reused, even though it happens to name exactly
-- those two jobs today. It means "who may punch to this", not "this is
-- overhead" — the same trap as `labor_type`, which reads "Regular" for all three
-- salaried owners and would have flipped the whole crew off overtime if
-- repurposed (031). A column that correlates today is not a column that means
-- the thing.
--
-- Why it matters to totals: Shop Work is typed Time & Material and carries a
-- billable rate, so `hours × rate` invents **$38,155 of "revenue"** nobody was
-- ever invoiced.
--
--   company final GP, all jobs        964,332.04
--   company final GP, overhead only    18,988.91   <- not real profit
--   company final GP, REAL JOBS       945,343.13   <- the honest number
--
-- ⬜ NOT DONE, and deliberately: `overhead` is not yet surfaced in the app
-- payload (`mapJobFromNeon`) or used to filter any screen. Both `airtable.js`
-- and `index.html` had uncommitted changes from a parallel session when this
-- landed, and editing a file another session has open loses whoever saves last.
-- The column and its backfill are done; wiring it up is a small follow-up.
--
-- ── ROLLBACK ───────────────────────────────────────────────────────────────
-- Restore the two expressions in `v_job_rollups_true` from the block in `006`
-- (labor_cost_reviewed, and live-minus-reviewed). `jobs.overhead` can stay — it
-- is additive and nothing reads it yet.
CREATE OR REPLACE VIEW v_job_rollups_true AS
 SELECT r.id, r.airtable_id, r.po,
    r.expected_revenue, r.expected_revenue_all_status, r.base_contract_amount,
    r.est_labor_hours_rollup, r.est_labor_cost_rollup, r.est_material_cost_rollup,
    r.proj_est_labor_hours, r.proj_est_labor_cost, r.proj_est_material_cost,
    r.approved_estimates, r.actual_material_cost, r.actual_subcontract_expense,
    r.actual_scissor_lift_expense, r.actual_rental_equipment_expense,
    r.reviewed_expenses_rollup, r.total_actual_expenses_audit, r.total_contract_billed,
    r.total_wire_cost, r.reviewed_wire_cost_rollup, r.pipe_cost, r.pipe_cost_reviewed,
    -- Full labor cost despite the name — see §2 above.
    COALESCE(t.labor_cost_live, 0::numeric) AS actual_labor_cost_reviewed,
    -- 0 so total_labor_cost_live is unchanged in value.
    0::numeric AS labor_cost_in_progress,
    r.hours_rollup, r.unbilled_hours, r.unbilled_labor_revenue_tm, r.unallocated_labor_hours
   FROM v_job_rollups r
     LEFT JOIN v_job_labor_cost_true_by_job t ON t.job_id = r.id;

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS overhead boolean NOT NULL DEFAULT false;
UPDATE jobs SET overhead = true WHERE name IN ('Shop Work', 'Office Work');

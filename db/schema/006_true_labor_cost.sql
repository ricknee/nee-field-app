-- ── TRUE labor cost, computed from time entries ────────────────────────────
-- Added 2026-08-05. Owner's call: "I want true profit numbers."
--
-- WHY THIS EXISTS. Airtable costs labor through the Job Labor Allocation table —
-- a second set of rows, created by an automation, carrying a MANUAL `Reviewed`
-- checkbox. Four things go wrong with that, and all four disappear if the cost is
-- computed from the time entries themselves:
--
--   1. THE CHECKBOX IS NEVER TICKED. `Reviewed Labor Cost = IF(Reviewed, cost, 0)`,
--      and `Reviewed` is a plain checkbox that nothing sets — approving hours in the
--      app writes `Labor Reviewed` on the TIME ENTRY, which is a different record.
--      So `Actual Labor Cost (Reviewed)` was 0 on 31 of 55 jobs, which made
--      `Total Labor Cost (Final)` 0, which overstated final gross profit by the
--      entire labor cost of the job. Measured: closeout counted $89,406 of labor
--      against a true reviewed figure of $268,721 — a ~$179,000 hole.
--      Here `reviewed` is DERIVED: bool_and(labor_reviewed) over the entries.
--
--   2. THE ALLOCATION ROWS MISS HOURS. They are built by an automation that drops
--      some. Shop Work: 581.5 real hours vs 252.25 allocated. Adena DG: 412.5 vs
--      396.5. There is no second set of rows here to fall out of step.
--
--   3. OLD WORK IS PRICED AT TODAY'S WAGE. Airtable's `True Cost Rate` is a lookup
--      of `Employees.Current True Cost Rate`, filtered to `Is Current Rate` — so a
--      raise retroactively rewrites the cost of finished jobs. Base wage went
--      $25 → $26 (true cost 31.25 → 32.50) on 2025-12-30. This view joins the rate
--      in force during that WEEK instead.
--
--   4. THE OVERTIME DENOMINATOR COMES FROM MAKE. The weekly total that drives the
--      OT split is `Employee Weekly Time`, maintained by Make scenario 4546051 —
--      so retiring Make at migration Step 3 would silently degrade labor cost.
--      It also already disagrees with its own source: for Miles Unruh, week
--      2026-03-09, Airtable's weekly rollup says 40.5 while Airtable's own time
--      entries sum to 36.5. Computed here from the entries, so Make is not in it.
--
-- WHAT IS DELIBERATELY UNCHANGED: the overtime maths. Airtable's rule is sound and
-- is reproduced exactly — take the employee's total hours for the week across ALL
-- jobs, treat anything over 40 as overtime, and spread that overtime across the
-- week's jobs in proportion to hours. 50 h worked, 25 on each of two jobs, gives
-- 5 OT hours to each. Cost is regular × rate + overtime × rate × 1.5.
--
-- Validated against Airtable's own weekly rollup: 297 of 304 employee-weeks match
-- to the cent. Of the 7 that differ, 5 are hours Make missed and the puller caught,
-- 1 is same-day activity before Make's 21:00 run, and 1 is the stale rollup above.
--
-- ⚠⚠ THE VIEW BELOW IS SUPERSEDED AND MUST NOT BE COPIED. It is kept because the
-- comments above it are the reasoning for the whole true-labor-cost design, and
-- those are still correct. The DEFINITION is not:
--
--   024_labor_cost_excludes_pto.sql  keeps PTO and paid holidays out of the OT
--                                    denominator (a GP correctness fix)
--   030_gp_view_perf.sql             CURRENT — 024's rules, rate lookup rewritten
--                                    set-wise, 9x faster, numbers identical
--
-- Rewriting from this file instead of from the live database silently reinstates
-- the pre-024 overtime bug. That already happened once, during 030, and was caught
-- only by a per-employee-week diff. **Start from
-- `pg_get_viewdef('v_job_labor_cost_true'::regclass, true)`, never from here.**
CREATE OR REPLACE VIEW v_job_labor_cost_true AS
WITH weekly AS (
  -- The OT denominator: ALL hours that week, every job, including unlinked time.
  SELECT employee_id, week_start_date, sum(hours) AS weekly_total_hours
    FROM time_entries WHERE employee_id IS NOT NULL GROUP BY 1, 2
), job_week AS (
  SELECT t.employee_id, t.job_id, t.week_start_date,
         sum(t.hours) AS allocated_hours,
         count(*)::int AS entry_count,
         -- bool_and: a job-week counts as reviewed only when EVERY entry in it is.
         -- One unapproved entry holds the whole week open, which is the
         -- conservative reading and matches what the closeout gate is asking.
         bool_and(t.labor_reviewed) AS reviewed,
         count(*) FILTER (WHERE NOT t.labor_reviewed)::int AS unreviewed_entries
    FROM time_entries t
   WHERE t.job_id IS NOT NULL AND t.employee_id IS NOT NULL
   GROUP BY 1, 2, 3
)
SELECT jw.job_id, jw.employee_id, jw.week_start_date,
       jw.allocated_hours, jw.entry_count, jw.reviewed, jw.unreviewed_entries,
       w.weekly_total_hours,
       ot.overtime_hours,
       jw.allocated_hours - ot.overtime_hours AS regular_hours,
       r.true_cost_rate AS true_cost_rate_at_week,
       r.is_fallback AS used_earliest_rate_fallback,
       round((jw.allocated_hours - ot.overtime_hours) * r.true_cost_rate
             + ot.overtime_hours * r.true_cost_rate * 1.5, 2) AS allocated_labor_cost
  FROM job_week jw
  JOIN weekly w ON w.employee_id = jw.employee_id AND w.week_start_date = jw.week_start_date
  CROSS JOIN LATERAL (
    SELECT CASE WHEN w.weekly_total_hours > 40
                THEN round(jw.allocated_hours * ((w.weekly_total_hours - 40) / w.weekly_total_hours), 4)
                ELSE 0 END AS overtime_hours
  ) ot
  -- Rate in force during that week. The UNION branch is a FALLBACK for work that
  -- predates the employee's earliest rate row — Patrick and Scott have rates
  -- starting 2025-12-01 but logged hours from January, and Nicholas from 2025-10-01
  -- against work from August. Without it those hours cost zero, which is a worse
  -- lie than pricing them at the earliest known rate. `used_earliest_rate_fallback`
  -- flags every row it applies to; 12 jobs / $151,270 of cost currently rely on it,
  -- so adding the missing historical rate rows in Airtable would improve accuracy.
  -- ORDER BY is_fallback makes the exact match win whenever one exists.
  LEFT JOIN LATERAL (
    SELECT x.true_cost_rate, x.is_fallback FROM (
      (SELECT r1.true_cost_rate, false AS is_fallback FROM labor_cost_rates r1
        WHERE r1.employee_id = jw.employee_id
          AND jw.week_start_date >= r1.effective_start_date
          AND (r1.effective_end_date IS NULL OR jw.week_start_date <= r1.effective_end_date)
        ORDER BY r1.effective_start_date DESC LIMIT 1)
      UNION ALL
      (SELECT r2.true_cost_rate, true FROM labor_cost_rates r2
        WHERE r2.employee_id = jw.employee_id
        ORDER BY r2.effective_start_date LIMIT 1)
    ) x ORDER BY x.is_fallback LIMIT 1
  ) r ON true;

-- Job-level rollup — what mapJob will read once handleJobs flips.
-- `labor_cost_live` is every hour costed. `labor_cost_reviewed` is the closeout
-- figure: only job-weeks where every entry is approved. Airtable's equivalents are
-- Total Labor Cost (Live) and Actual Labor Cost (Reviewed).
CREATE OR REPLACE VIEW v_job_labor_cost_true_by_job AS
SELECT j.id AS job_id, j.airtable_id, j.name, j.po_locked,
       round(sum(c.allocated_hours), 2)   AS total_hours,
       round(sum(c.overtime_hours), 2)    AS overtime_hours,
       round(sum(c.regular_hours), 2)     AS regular_hours,
       sum(c.entry_count)::int            AS entry_count,
       sum(c.unreviewed_entries)::int     AS unreviewed_entries,
       round(sum(c.allocated_labor_cost), 2) AS labor_cost_live,
       round(sum(c.allocated_labor_cost) FILTER (WHERE c.reviewed), 2) AS labor_cost_reviewed,
       -- Replaces Airtable's "All Labor Reviewed" formula, which read the manual
       -- allocation checkbox and therefore answered "no" almost everywhere.
       bool_and(c.reviewed)               AS all_labor_reviewed,
       bool_or(c.used_earliest_rate_fallback) AS any_rate_fallback
  FROM v_job_labor_cost_true c
  JOIN jobs j ON j.id = c.job_id
 GROUP BY j.id, j.airtable_id, j.name, j.po_locked;

-- ── Feeding true labor into the GP layer ───────────────────────────────────
-- Labor enters v_job_financials at exactly two inputs:
--   actual_labor_cost_reviewed  -> total_labor_cost_final, actual_job_cost_cogs
--   labor_cost_in_progress      -> (with the above) total_labor_cost_live
-- Everything downstream — GP live, GP final, COGS — is derived from those. So the
-- substitution happens ONCE, here, rather than by rewriting the GP maths.
-- ⚠⚠ SUPERSEDED 2026-08-11 by `038_final_gp_all_labor.sql`. Owner decision:
-- FINAL GP COUNTS ALL LABOR, reviewed or not. The two labor expressions below
-- were replaced with `COALESCE(t.labor_cost_live, 0)` and a literal `0`, so
-- `actual_labor_cost_reviewed` now carries the FULL labor cost despite its name.
-- The name could not change without breaking the generated `v_job_financials_true`.
-- **Do not restore the block below unless you intend to un-do that decision** —
-- it is the correct ROLLBACK target, and nothing else.
CREATE OR REPLACE VIEW v_job_rollups_true AS
SELECT r.id, r.airtable_id, r.po,
       r.expected_revenue, r.expected_revenue_all_status, r.base_contract_amount,
       r.est_labor_hours_rollup, r.est_labor_cost_rollup, r.est_material_cost_rollup,
       r.proj_est_labor_hours, r.proj_est_labor_cost, r.proj_est_material_cost,
       r.approved_estimates, r.actual_material_cost, r.actual_subcontract_expense,
       r.actual_scissor_lift_expense, r.actual_rental_equipment_expense,
       r.reviewed_expenses_rollup, r.total_actual_expenses_audit, r.total_contract_billed,
       r.total_wire_cost, r.reviewed_wire_cost_rollup, r.pipe_cost, r.pipe_cost_reviewed,
       COALESCE(t.labor_cost_reviewed, 0::numeric) AS actual_labor_cost_reviewed,
       COALESCE(t.labor_cost_live - COALESCE(t.labor_cost_reviewed, 0::numeric), 0::numeric)
         AS labor_cost_in_progress,
       r.hours_rollup, r.unbilled_hours, r.unbilled_labor_revenue_tm, r.unallocated_labor_hours
  FROM v_job_rollups r
  LEFT JOIN v_job_labor_cost_true_by_job t ON t.job_id = r.id;

-- v_job_financials_true is GENERATED FROM v_job_financials' own definition, with the
-- single `FROM v_job_rollups r` swapped for the _true source. Deliberately not
-- hand-copied: the faithful view is the diff gate against Airtable and must stay
-- authoritative, and a hand-maintained twin would drift the first time it is
-- corrected. Re-run this to regenerate after any change to v_job_financials.
--
-- The count check is the safety catch — if the reference count is ever not exactly
-- 1 (a rename, a second join), it raises instead of silently producing a view that
-- still reads the OLD labor numbers while claiming to be the true one.
--
--   DO $do$
--   DECLARE d text; n int;
--   BEGIN
--     d := pg_get_viewdef('v_job_financials'::regclass, true);
--     n := (length(d) - length(replace(d, 'v_job_rollups r', ''))) / length('v_job_rollups r');
--     IF n <> 1 THEN RAISE EXCEPTION 'expected 1 reference, found %', n; END IF;
--     EXECUTE 'CREATE OR REPLACE VIEW v_job_financials_true AS '
--             || replace(d, 'v_job_rollups r', 'v_job_rollups_true r');
--   END $do$;
--
-- ── WHAT THIS CHANGES ON SCREEN (measured 2026-08-05, 60 jobs with a final GP) ──
-- Total final GP 1,017,242 -> 883,093, a drop of 134,149. Biggest movers:
-- Strongsville DG -19,463 · Wheeling DG -19,422 · Cambridge DG -14,530 ·
-- Adena DG -13,237 · Beliot DG -10,831.
--
-- ⚠ 14 of the 15 jobs that end up negative have ZERO recorded revenue — mostly
-- Service Calls. They read as losses because cost is now real while revenue was
-- never recorded against them. That is a SEPARATE pre-existing gap this exposes
-- rather than causes (possibly uninvoiced work), not a fault in the labor maths.
-- The one genuine outlier is Strongsville DG: 1,800 revenue against 29,562 cost.

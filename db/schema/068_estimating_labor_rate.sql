-- 068_estimating_labor_rate.sql — the estimating labor rate stops being a
-- literal in the source and starts being read from the crew.
--
-- ── WHAT WAS WRONG ─────────────────────────────────────────────────────────
-- Two systems that never spoke:
--
--   ACTUAL labor cost   `labor_cost_rates`, per employee, effective-dated.
--                       `base_hourly_wage` x (1 + `payroll_burden_pct`) with
--                       burden at 0.25 today, and `v_job_labor_cost_true` joins
--                       the row in force during THAT WEEK — so a raise prices
--                       only the hours worked after it. Correct already.
--
--   ESTIMATED labor cost  `const EST_LABOR_RATE = 32.50` in airtable.js. One
--                       number, every employee, every job, forever. It is a
--                       frozen snapshot of a $26.00 wage from the 2025-12-30
--                       raise and cannot move, because it is source code.
--
-- Measured 2026-08-30: the crew's hours-weighted true cost is $31.01, and
-- $33.09 once the overtime premium is counted (13.4% of hours run over 40).
-- So 32.50 is within ~2% today — coincidentally, not by construction — and it
-- goes stale silently at the next raise.
--
-- ── ⚠⚠ THE BACKFILL IS THE WHOLE SAFETY OF THIS FILE. DO NOT REORDER IT ────
-- 90 of 91 estimates carry `labor_burden_rate IS NULL` and fall through to the
-- constant. Change what that fallback resolves to WITHOUT stamping the old rows
-- first and **19,039 estimated hours reprice at once**: $11,233 of estimated GP
-- moves across historical jobs, on quotes that were sent and won at the old
-- number. Nothing errors. It reads as "the GP figures drifted".
--
-- Owner's instruction, 2026-08-30, and it is the correct one: "if a job started
-- out at $32.50 we need to keep that data — only change it on jobs where an
-- employee got a raise."
--
-- Verified before writing: all 90 rows satisfy
-- `estimated_labor_cost = round(estimated_labor_hours * 32.50, 2)` exactly,
-- worst delta $0.00. So this UPDATE records the rate those rows were ALREADY
-- costed at. It is a no-op numerically and the derived columns are untouched —
-- it exists to make the implicit explicit, which is what makes the fallback
-- safe to change afterwards.
UPDATE job_estimates
   SET labor_burden_rate = 32.50
 WHERE labor_burden_rate IS NULL;

COMMENT ON COLUMN job_estimates.labor_burden_rate IS
  'What an hour COSTS on this estimate, $/hr, STAMPED at create and never recomputed — a quote keeps the rate that was in force when it was written. Resolved from v_estimating_labor_rate at create time (db/schema/068); rows written before 2026-08-30 were backfilled with the 32.50 constant they had always been costed at. ⚠ Never bulk-update this column: it is the only record of what an old quote assumed.';

-- ── WHERE THE NUMBER COMES FROM NOW ────────────────────────────────────────
-- One view, so the formula lives in one place and can be inspected on its own
-- rather than being buried in a handler.
--
-- Hours-WEIGHTED, not a simple average: Nicholas at $22.50 works nearly as many
-- hours as the journeymen, so he has to pull the average down in proportion to
-- the work he actually does. A plain mean of the seven current rates gives
-- $31.16 and quietly over-weights whoever happens to be on the payroll.
--
-- ⚠ THE OVERTIME PREMIUM IS INCLUDED (owner's call 2026-08-30). An estimate has
-- no concept of overtime — it is hours x a rate — so a rate built from straight
-- time understates every job that will actually run over 40. 13.4% of the last
-- twelve months' hours were overtime; excluding them prices labor at $31.01
-- against a real $33.09.
--
-- ⚠ PTO and paid holidays are excluded, and the spelling matters: the labels
-- are 'PTO' and 'Paid Holiday', matching `v_job_labor_cost_true` exactly. This
-- system has been bitten twice by a singular/plural mismatch in a single-select
-- ('Service Call' vs 'Service Calls'), and a wrong label here does not error —
-- it silently includes paid non-productive hours in a productive-cost rate.
--
-- ⚠ Returns NULL when there is no rate or no time history. That is deliberate:
-- the caller COALESCEs to the 32.50 constant, so an empty database cannot make
-- an estimate cost labor at $0/hr.
CREATE OR REPLACE VIEW v_estimating_labor_rate AS
WITH cur AS (
  SELECT employee_id, true_cost_rate
    FROM labor_cost_rates
   WHERE effective_end_date IS NULL
     AND true_cost_rate IS NOT NULL
), wk AS (
  SELECT t.employee_id, t.week_start_date, SUM(t.hours) AS wk_hours
    FROM time_entries t
   WHERE t.work_date >= (CURRENT_DATE - INTERVAL '12 months')
     AND t.employee_id IS NOT NULL
     AND COALESCE(t.labor_type, '') <> ALL (ARRAY['PTO'::text, 'Paid Holiday'::text])
   GROUP BY t.employee_id, t.week_start_date
), split AS (
  -- Same overtime rule as v_job_labor_cost_true: anything over 40 in a week is
  -- overtime, at 1.5x. Reproduced rather than shared because that view splits
  -- OT across jobs and this one only needs the company total.
  SELECT w.wk_hours,
         GREATEST(w.wk_hours - 40, 0) AS ot_hours,
         LEAST(w.wk_hours, 40)        AS reg_hours,
         c.true_cost_rate             AS rate
    FROM wk w
    JOIN cur c ON c.employee_id = w.employee_id
)
SELECT
  round(SUM(reg_hours * rate + ot_hours * rate * 1.5) / NULLIF(SUM(wk_hours), 0), 2) AS burden_rate,
  round(SUM(wk_hours * rate) / NULLIF(SUM(wk_hours), 0), 2)                          AS burden_rate_no_ot,
  round(SUM(wk_hours), 1)                                                            AS hours_12mo,
  round(100.0 * SUM(ot_hours) / NULLIF(SUM(wk_hours), 0), 1)                         AS ot_pct
  FROM split;

COMMENT ON VIEW v_estimating_labor_rate IS
  'The company estimating labor rate, computed from the CURRENT crew: hours-weighted true cost over the last 12 months, including the 1.5x overtime premium. Read once at estimate create and STAMPED onto job_estimates.labor_burden_rate; never read again for an existing estimate. Raises therefore flow into NEW quotes only, which is the same rule v_job_labor_cost_true applies to actual hours.';

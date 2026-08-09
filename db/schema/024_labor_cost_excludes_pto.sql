-- Neon slice — keep PTO and paid holidays out of the overtime denominator.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- ⚠⚠ THIS IS A GP CORRECTNESS FIX, NOT A FEATURE.
--
-- `v_job_labor_cost_true` (006) decides how much of a person's week was overtime
-- by summing EVERY hour they recorded that week — every job plus unlinked time —
-- and treating anything over 40 as OT. It then prices that share of each job's
-- allocated hours at 1.5x. That was correct while every row in time_entries was
-- hours WORKED.
--
-- 023 makes PTO and paid holidays ordinary time entries. Without this change, a
-- week containing a holiday would look like this:
--
--     32 h worked on Job A  +  8 h Paid Holiday  =  40 h        -> no OT. Fine.
--     40 h worked on Job A  +  8 h Paid Holiday  =  48 h        -> 8 h of OT,
--                                                                  charged to Job A
--                                                                  at 1.5x.
--
-- The second line is wrong twice over: the person did not work overtime, and Job A
-- is charged for a holiday it had nothing to do with. Every job that person
-- touched in a holiday week would be overstated, quietly, in the direction that
-- makes GP look worse. This is structurally the same error as the manual
-- `Reviewed` checkbox that produced a ~$179k hole in closeout — a denominator
-- that silently stopped meaning what the formula assumed.
--
-- ONLY the `weekly` CTE changes. `job_week` already excludes PTO naturally,
-- because PTO entries carry no job_id and it filters on `job_id IS NOT NULL`.
-- Everything below the CTEs is copied from 006 unchanged; keep the two in sync by
-- hand if 006 is ever revised.
CREATE OR REPLACE VIEW v_job_labor_cost_true AS
WITH weekly AS (
  -- The OT denominator: all hours that week, every job, including unlinked time —
  -- but ONLY hours actually WORKED. Overtime is owed on work, and PTO and paid
  -- holidays are not work. See 023 for the single definition of "not worked".
  SELECT employee_id, week_start_date, sum(hours) AS weekly_total_hours
    FROM time_entries
   WHERE employee_id IS NOT NULL
     AND coalesce(class, '') NOT IN ('PTO', 'Paid Holiday')
   GROUP BY 1, 2
), job_week AS (
  SELECT t.employee_id, t.job_id, t.week_start_date,
         sum(t.hours) AS allocated_hours,
         count(*)::int AS entry_count,
         bool_and(t.labor_reviewed) AS reviewed,
         count(*) FILTER (WHERE NOT t.labor_reviewed)::int AS unreviewed_entries
    FROM time_entries t
   WHERE t.job_id IS NOT NULL AND t.employee_id IS NOT NULL
     -- Belt and braces. A PTO row should never carry a job, but if one ever does
     -- (a mis-set job on a PTO entry), it must not be costed to that job.
     AND coalesce(t.class, '') NOT IN ('PTO', 'Paid Holiday')
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

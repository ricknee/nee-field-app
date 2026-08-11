-- ── v_job_labor_cost_true — same numbers, 9x faster ────────────────────────
-- Applied BARE to the production branch via the Neon MCP 2026-08-10; this file is
-- the annotated source of truth.
--
-- ⚠⚠ THIS CHANGES NO NUMBER. It is a pure performance rewrite, and the proof that
-- it changes nothing is the whole point of the exercise — see THE DIFF below.
--
-- ── WHY ────────────────────────────────────────────────────────────────────
-- `handleJobs` — the first request the app makes when it opens — took ~985 ms on a
-- WARM database, touching 415,092 shared buffers (~3 GB of page traffic) to return
-- 112 rows. On the first open of the day it was far worse: Neon scale-to-zero means
-- the compute is asleep after 5 minutes idle, so those 415k buffers come back from
-- storage over the network instead of RAM. Measured user complaint: "up to 10 secs
-- to load jobs".
--
-- The data is tiny — 14,636 time entries, 15 rate rows, 112 jobs, 869 job-weeks.
-- This should be a ~20 ms query. Every bit of the 985 ms was REPEATED COMPUTATION:
--
--   1. THE RATE LOOKUP WAS CORRELATED. The `LEFT JOIN LATERAL` picking the rate in
--      force during a week ran once per job-week — 869 times per evaluation of the
--      view, each doing two seq scans of labor_cost_rates.
--   2. THE WHOLE VIEW WAS RE-EVALUATED PER JOB. Because of (1) the planner could not
--      compute the aggregate once and hash it, so it nested-looped: the plan showed
--      `GroupAggregate ... loops=112` and `Seq Scan on labor_cost_rates loops=97328`
--      (869 x 112).
--   3. IT IS JOINED THREE TIMES. JOB_SELECT joins v_job_rollups_true AND
--      v_job_financials_true AND v_job_labor_cost_true_by_job — but financials reads
--      rollups, and rollups reads the labor view. So the labor aggregate appears
--      three times in one query. Left as-is; see WHAT WAS DELIBERATELY NOT DONE.
--
-- ── WHAT CHANGED ───────────────────────────────────────────────────────────
-- ONLY the rate lookup. The correlated LATERAL becomes two set-based CTEs computed
-- once — `rate_exact` (DISTINCT ON the latest rate whose window contains the week)
-- and `rate_earliest` (the documented fallback for work predating an employee's
-- first rate row). The OT maths, the bool_and reviewed rule, the rounding and the
-- column list are untouched.
--
-- Two things that look cosmetic and are not:
--
--   * THE FALLBACK FLAG IS TESTED ON THE JOIN KEY, NOT THE RATE VALUE.
--     `CASE WHEN re.employee_id IS NOT NULL` — not `re.true_cost_rate IS NOT NULL`.
--     A matched rate row carrying a NULL rate must stay a match and yield NULL,
--     which is what the old `ORDER BY x.is_fallback LIMIT 1` did. Testing the value
--     would silently promote it to the earliest-rate fallback instead.
--   * `used_earliest_rate_fallback` KEEPS ITS THIRD STATE. false = exact match,
--     true = fallback used, NULL = the employee has no rate row at all. The old
--     LATERAL produced NULL by returning no row; the CASE has an explicit
--     `ELSE NULL::boolean` branch to reproduce it.
--
-- Verified first: no employee has two rate rows sharing an effective_start_date, so
-- the old `ORDER BY ... LIMIT 1` had no tie to break and DISTINCT ON is exactly
-- equivalent rather than merely equivalent-in-practice.
--
-- ── THE DIFF — this is the gate, the same treatment 013 and 015 got ─────────
-- Run on a branch (gp-view-perf, br-sparkling-sky-ap0jja6i) against a snapshot of
-- the four views taken before the change, then RE-RUN on production against a live
-- snapshot as a guard against a paste error. Symmetric EXCEPT ALL in both
-- directions, at all four grains:
--
--   v_job_labor_cost_true          869 rows   0 differences
--   v_job_labor_cost_true_by_job    58 rows   0 differences
--   v_job_rollups_true             112 rows   0 differences
--   v_job_financials_true          112 rows   0 differences
--
-- Headline totals identical either side: labor_cost_live 339,947.47 and
-- overtime_hours 1,351.28. Nothing else in the database depends on these four —
-- checked via pg_depend, the chain is exactly labor -> by_job -> rollups_true ->
-- financials_true and stops there.
--
-- ⚠⚠ THE TRAP THIS CAUGHT, WHICH IS THE REAL LESSON OF THIS FILE.
-- The first rewrite was made from `006_true_labor_cost.sql` and the diff failed on
-- 5 rows: one employee, one week, weekly_total_hours 41 -> 49. `006` IS STALE.
-- `024_labor_cost_excludes_pto.sql` later replaced this view to keep PTO and paid
-- holidays out of the OT denominator, and 006 was never updated — 024 even says
-- "keep the two in sync by hand", and they were not. Rewriting from the file
-- silently reinstated a GP bug that had been deliberately fixed, and would have
-- charged every job in a holiday week for overtime nobody worked.
--
-- **THE VIEW DEFINITIONS IN db/schema/ ARE NOT THE SOURCE OF TRUTH. THE DATABASE
-- IS.** Start any view rewrite from `pg_get_viewdef('<name>'::regclass, true)`.
-- The 8-hour discrepancy was visible only because the diff ran at the finest grain
-- (per employee-week) rather than on job totals, where it would have been a small
-- plausible-looking wobble.
--
-- ── RESULT (production, warm) ──────────────────────────────────────────────
--   before   985 ms   415,092 buffers
--   after    107 ms    21,499 buffers      9.2x faster, 19x less page traffic
--
-- The inner aggregates now run once (loops=1) instead of 112 times. The cold-start
-- win is larger than the warm number suggests: 19x less page traffic is 19x less to
-- pull back from storage when Neon has just woken up.
--
-- ── WHAT WAS DELIBERATELY NOT DONE ─────────────────────────────────────────
-- Point (3) above — the triple join — still stands, and is most of the remaining
-- 107 ms. Fixing it means collapsing JOB_SELECT onto a single view and re-proving
-- that `mapJobFromNeon` still returns all 87 keys. That is real risk against ~77 ms
-- nobody can perceive. If it is ever done, do it for a reason other than speed.
--
-- ── ROLLBACK ───────────────────────────────────────────────────────────────
-- Re-apply the CREATE OR REPLACE VIEW block in `024_labor_cost_excludes_pto.sql`
-- verbatim — NOT the one in 006, which is the stale definition described above.
-- No data is touched by either direction; this is a view definition and nothing else.
CREATE OR REPLACE VIEW v_job_labor_cost_true AS
WITH weekly AS (
  -- OT denominator: hours WORKED that week, every job plus unlinked time. PTO and
  -- paid holidays are excluded — see 024, this is a GP correctness rule, not a filter.
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
     AND coalesce(t.class, '') NOT IN ('PTO', 'Paid Holiday')
   GROUP BY 1, 2, 3
), rate_exact AS (
  -- Was a correlated LEFT JOIN LATERAL evaluated once per job-week (869 x per
  -- evaluation of this view, and the view was itself re-evaluated per job). Same
  -- rule, resolved set-wise once: the latest rate whose window contains the week.
  SELECT DISTINCT ON (w.employee_id, w.week_start_date)
         w.employee_id, w.week_start_date, r1.true_cost_rate
    FROM weekly w
    JOIN labor_cost_rates r1
      ON r1.employee_id = w.employee_id
     AND w.week_start_date >= r1.effective_start_date
     AND (r1.effective_end_date IS NULL OR w.week_start_date <= r1.effective_end_date)
   ORDER BY w.employee_id, w.week_start_date, r1.effective_start_date DESC
), rate_earliest AS (
  -- The documented fallback for work predating an employee's earliest rate row.
  SELECT DISTINCT ON (employee_id) employee_id, true_cost_rate
    FROM labor_cost_rates ORDER BY employee_id, effective_start_date
), priced AS (
  SELECT jw.job_id, jw.employee_id, jw.week_start_date,
         jw.allocated_hours, jw.entry_count, jw.reviewed, jw.unreviewed_entries,
         w.weekly_total_hours,
         CASE WHEN w.weekly_total_hours > 40
              THEN round(jw.allocated_hours * ((w.weekly_total_hours - 40) / w.weekly_total_hours), 4)
              ELSE 0 END AS overtime_hours,
         -- Tested on the JOIN's own key, not on the rate value: a matched row with a
         -- NULL rate must stay a match, exactly as the old ORDER BY is_fallback did.
         CASE WHEN re.employee_id IS NOT NULL THEN re.true_cost_rate
              ELSE rl.true_cost_rate END AS rate,
         CASE WHEN re.employee_id IS NOT NULL THEN false
              WHEN rl.employee_id IS NOT NULL THEN true
              ELSE NULL::boolean END AS is_fallback
    FROM job_week jw
    JOIN weekly w              ON w.employee_id  = jw.employee_id AND w.week_start_date = jw.week_start_date
    LEFT JOIN rate_exact re    ON re.employee_id = jw.employee_id AND re.week_start_date = jw.week_start_date
    LEFT JOIN rate_earliest rl ON rl.employee_id = jw.employee_id
)
SELECT p.job_id, p.employee_id, p.week_start_date,
       p.allocated_hours, p.entry_count, p.reviewed, p.unreviewed_entries,
       p.weekly_total_hours,
       p.overtime_hours,
       p.allocated_hours - p.overtime_hours AS regular_hours,
       p.rate AS true_cost_rate_at_week,
       p.is_fallback AS used_earliest_rate_fallback,
       round((p.allocated_hours - p.overtime_hours) * p.rate
             + p.overtime_hours * p.rate * 1.5, 2) AS allocated_labor_cost
  FROM priced p;

-- 072_prevailing_wage.sql — STEP 1 of docs/PLAN-prevailing-wage.md.
-- Ships the WHOLE machine INERT: the flag, the rate table, and the resolver,
-- with NO job flagged. The gate for this file is that nothing moves — all 123
-- jobs' est GP, live GP and closeout GP identical to the cent, before and after.
--
-- ── WHAT WAS WRONG ─────────────────────────────────────────────────────────
-- The system has exactly ONE answer to "what does an hour of labor cost," and
-- it is company-wide. Two of them, actually, that do not know about each other:
--
--   ACTUAL   `labor_cost_rates`, per employee, per week, effective-dated.
--   ESTIMATED `v_estimating_labor_rate` (db/schema/068), one company number.
--
-- Neither can say "on THIS job an hour costs more." That is the hole, and it is
-- not a prevailing-wage hole — PW is merely the first thing to fall in it.
-- `prevailing_wage` is the flag saying WHY the job has its own rate; `pw_rates`
-- is the rate. Do not generalise past that.
--
-- ── ⚠⚠ THE ONE THING THIS FILE IS BUILT AROUND ─────────────────────────────
-- An employee-week with NO prevailing-wage time must produce a BIT-IDENTICAL
-- number to today. Not "close" — identical, to the cent, on all 924 cost rows.
--
-- That is why `v_job_labor_cost_true` below is a UNION of two branches rather
-- than one clever formula. Branch A is today's view, reproduced expression for
-- expression, restricted to non-PW weeks. Branch B is the new chronological
-- allocation, and it only ever sees a week that actually contains PW work.
-- With no job flagged, branch B selects ZERO rows and the view is today's view
-- by construction, not by arithmetic coincidence. That property is the entire
-- point of shipping this inert, and it is worth the duplication.
--
-- ⚠ Written from `pg_get_viewdef('v_job_labor_cost_true', true)` on 2026-09-12,
-- NOT from db/schema/006_true_labor_cost.sql, which is superseded twice (024
-- excludes PTO from the overtime denominator; 030 is the current fast rewrite).
-- Starting from that file already reinstated the pre-024 overtime bug once.
-- Note in particular that the PTO/Paid-Holiday filter reads `class`, NOT
-- `labor_type` — 068 reads `labor_type` for a different purpose. Both spellings
-- exist in this database and they are not interchangeable.

ALTER TABLE jobs ADD COLUMN prevailing_wage boolean NOT NULL DEFAULT false;

COMMENT ON COLUMN jobs.prevailing_wage IS
  'This job is covered by a prevailing-wage determination, so its hours cost the rate in pw_rates instead of the employee''s own rate. App-owned, Neon-only — there is no Airtable twin and none should be created (db/schema/072). PW belongs to the JOB, never to a second toggle on the time entry: choosing the job IS choosing the wage treatment.';

-- ── THE RATE ───────────────────────────────────────────────────────────────
-- base + fringe EXPLAIN the determination; straight_hourly and overtime_hourly
-- are the amounts the resolver actually uses. Keep all four visible.
--
-- ⚠ DO NOT DERIVE THE OVERTIME NUMBER. `base * 1.5 + fringe` is a reasonable
-- guess and it is not authoritative: a determination may state an explicit OT
-- rate, and fringe does not necessarily receive the premium. The form may show
-- the derived figure as a sanity check, but it stores and uses the supplied one.
--
-- ⚠ Burden is explicit too — no silent 25% production default. A wrong burden
-- is invisible: it does not error, it just prices every hour on the job wrong.
CREATE TABLE pw_rates (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id            uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  classification    text,
  base_hourly       numeric(10,4) NOT NULL CHECK (base_hourly     >= 0),
  fringe_hourly     numeric(10,4) NOT NULL DEFAULT 0 CHECK (fringe_hourly >= 0),
  straight_hourly   numeric(10,4) NOT NULL CHECK (straight_hourly >= 0),
  overtime_hourly   numeric(10,4) NOT NULL CHECK (overtime_hourly >= 0),
  burden_pct        numeric(6,4)  NOT NULL CHECK (burden_pct      >= 0),
  effective_start   date NOT NULL,
  effective_end     date,
  notes             text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  CHECK (effective_end IS NULL OR effective_end >= effective_start)
);

-- Makes the one-classification simplification SAFE rather than merely
-- convenient: a second open rate raises an error instead of the lookup quietly
-- choosing one of two. If a determination ever names a journeyman AND an
-- apprentice NEE will actually staff, this constraint is what makes that
-- visible on day one rather than three months later inside a GP number.
CREATE UNIQUE INDEX pw_rates_one_open_per_job ON pw_rates (job_id) WHERE effective_end IS NULL;
CREATE INDEX pw_rates_job_effective ON pw_rates (job_id, effective_start DESC);

COMMENT ON TABLE pw_rates IS
  'The prevailing-wage determination for one job, effective-dated because Ohio adjusts determinations annually and a six-month job will see one. The rate in force on the WORK DATE prices the hour — the same rule db/schema/006 fought for on employee rates, so a later change can never rewrite a finished job. straight_hourly/overtime_hourly are SUPPLIED, never derived. One open row per job is enforced by pw_rates_one_open_per_job.';

-- ── WHAT THE RESOLVER READS ────────────────────────────────────────────────
-- One place that answers "what does an hour on job J on date D cost?", so the
-- three GP numbers cannot drift apart by being patched separately.
-- Returns loaded (burdened) cost, which is what every consumer wants.
CREATE OR REPLACE VIEW v_pw_job_rate AS
SELECT p.job_id,
       p.classification,
       p.base_hourly, p.fringe_hourly,
       p.straight_hourly, p.overtime_hourly, p.burden_pct,
       p.effective_start, p.effective_end,
       round(p.straight_hourly * (1 + p.burden_pct), 4) AS straight_loaded,
       round(p.overtime_hourly * (1 + p.burden_pct), 4) AS overtime_loaded
  FROM pw_rates p
  JOIN jobs j ON j.id = p.job_id
 WHERE j.prevailing_wage;

COMMENT ON VIEW v_pw_job_rate IS
  'THE rate resolver. All three GP numbers read this and nothing else. Filtered on jobs.prevailing_wage, so clearing the flag disables the rate without deleting the row someone typed. Caller must still pick the row whose effective range covers the work date.';

-- ═══════════════════════════════════════════════════════════════════════════
-- THE COST VIEW
-- ═══════════════════════════════════════════════════════════════════════════
-- Column list is APPEND-ONLY versus the previous definition: the first 13
-- columns keep their names, order and types so CREATE OR REPLACE works and
-- v_job_labor_cost_true_by_job -> v_job_rollups_true -> v_job_financials_true
-- need no rebuild.
CREATE OR REPLACE VIEW v_job_labor_cost_true AS
WITH
-- Every entry that counts toward a paycheck, tagged with whether its job is PW.
-- job_id IS NULL rows are KEPT here on purpose: unlinked worked hours still
-- consume the first 40 of the week, and that is true in both branches.
ent AS (
  SELECT t.employee_id, t.week_start_date, t.work_date, t.job_id, t.hours,
         t.labor_reviewed,
         (j.prevailing_wage IS TRUE) AS is_pw
    FROM time_entries t
    LEFT JOIN jobs j ON j.id = t.job_id
   WHERE t.employee_id IS NOT NULL
     AND (COALESCE(t.class, '') <> ALL (ARRAY['PTO'::text, 'Paid Holiday'::text]))
),
-- The branch key. One boolean per employee-week decides which allocation rule
-- the WHOLE week uses — you cannot mix rules inside a week and still have the
-- 40-hour threshold mean anything.
wk_flag AS (
  SELECT employee_id, week_start_date, bool_or(is_pw) AS has_pw
    FROM ent
   GROUP BY employee_id, week_start_date
),
weekly AS (
  SELECT employee_id, week_start_date, sum(hours) AS weekly_total_hours
    FROM ent
   GROUP BY employee_id, week_start_date
),
-- Employee rate in force during that week, and the earliest-rate fallback.
-- Reproduced verbatim from the previous definition.
rate_exact AS (
  SELECT DISTINCT ON (w.employee_id, w.week_start_date)
         w.employee_id, w.week_start_date, r1.true_cost_rate
    FROM weekly w
    JOIN labor_cost_rates r1
      ON r1.employee_id = w.employee_id
     AND w.week_start_date >= r1.effective_start_date
     AND (r1.effective_end_date IS NULL OR w.week_start_date <= r1.effective_end_date)
   ORDER BY w.employee_id, w.week_start_date, r1.effective_start_date DESC
),
rate_earliest AS (
  SELECT DISTINCT ON (employee_id) employee_id, true_cost_rate
    FROM labor_cost_rates
   ORDER BY employee_id, effective_start_date
),
emp_rate AS (
  SELECT w.employee_id, w.week_start_date,
         CASE WHEN re.employee_id IS NOT NULL THEN re.true_cost_rate ELSE rl.true_cost_rate END AS rate,
         CASE WHEN re.employee_id IS NOT NULL THEN false
              WHEN rl.employee_id IS NOT NULL THEN true
              ELSE NULL::boolean END AS is_fallback
    FROM weekly w
    LEFT JOIN rate_exact    re ON re.employee_id = w.employee_id AND re.week_start_date = w.week_start_date
    LEFT JOIN rate_earliest rl ON rl.employee_id = w.employee_id
),

-- ── BRANCH A — no PW hours in the week. TODAY'S VIEW, UNCHANGED. ───────────
-- Overtime is spread PROPORTIONALLY over every job the employee touched that
-- week. NEE schedules PW at the start of the week and does not plan overtime on
-- it, but for an ordinary week proportional spreading remains the rule — and
-- more to the point, changing it here would move GP on jobs that have nothing
-- to do with prevailing wage.
a_job_week AS (
  SELECT e.employee_id, e.job_id, e.week_start_date,
         sum(e.hours) AS allocated_hours,
         count(*)::integer AS entry_count,
         bool_and(e.labor_reviewed) AS reviewed,
         count(*) FILTER (WHERE NOT e.labor_reviewed)::integer AS unreviewed_entries
    FROM ent e
    JOIN wk_flag f ON f.employee_id = e.employee_id AND f.week_start_date = e.week_start_date
   WHERE e.job_id IS NOT NULL
     AND NOT f.has_pw
   GROUP BY e.employee_id, e.job_id, e.week_start_date
),
branch_a AS (
  SELECT jw.job_id, jw.employee_id, jw.week_start_date,
         jw.allocated_hours, jw.entry_count, jw.reviewed, jw.unreviewed_entries,
         w.weekly_total_hours,
         CASE WHEN w.weekly_total_hours > 40::numeric
              THEN round(jw.allocated_hours * ((w.weekly_total_hours - 40::numeric) / w.weekly_total_hours), 4)
              ELSE 0::numeric END AS overtime_hours,
         er.rate,
         er.is_fallback,
         0::numeric AS pw_hours,
         false AS pw_rate_missing,
         false AS ot_order_ambiguous
    FROM a_job_week jw
    JOIN weekly   w  ON w.employee_id  = jw.employee_id AND w.week_start_date  = jw.week_start_date
    JOIN emp_rate er ON er.employee_id = jw.employee_id AND er.week_start_date = jw.week_start_date
),

-- ── BRANCH B — the week contains PW hours. CHRONOLOGICAL. ──────────────────
-- ⚠⚠ Do NOT spread this week's overtime proportionally. If an employee works 24
-- PW hours Mon-Wed and then 21 normal hours, the PW job gets 24 STRAIGHT hours
-- and the normal job gets 16 straight + 5 overtime. Proportional spreading
-- would push 2.67 OT hours back onto the PW job purely because it held 24/45 of
-- the week — hours that were never overtime when they were worked.
b_day_job AS (
  SELECT e.employee_id, e.week_start_date, e.work_date, e.job_id, e.is_pw,
         sum(e.hours) AS day_job_hours,
         count(*)::integer AS entry_count,
         bool_and(e.labor_reviewed) AS reviewed,
         count(*) FILTER (WHERE NOT e.labor_reviewed)::integer AS unreviewed_entries
    FROM ent e
    JOIN wk_flag f ON f.employee_id = e.employee_id AND f.week_start_date = e.week_start_date
   WHERE f.has_pw
   GROUP BY e.employee_id, e.week_start_date, e.work_date, e.job_id, e.is_pw
),
b_day AS (
  SELECT employee_id, week_start_date, work_date,
         sum(day_job_hours) AS day_hours,
         -- More than one bucket on a day makes that day's order unprovable.
         -- Unlinked time is its own bucket: it is still an hour in the sequence.
         count(DISTINCT COALESCE(job_id::text, '~unlinked')) AS buckets
    FROM b_day_job
   GROUP BY employee_id, week_start_date, work_date
),
-- Hours worked EARLIER in the week, by date. This is the whole of "chronological".
b_cum AS (
  SELECT d.*,
         COALESCE(sum(d.day_hours) OVER (
           PARTITION BY d.employee_id, d.week_start_date
           ORDER BY d.work_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0::numeric) AS hours_before
    FROM b_day d
),
b_split AS (
  SELECT c.*,
         -- The part of THIS day that falls beyond the 40th hour of the week.
         GREATEST(c.hours_before + c.day_hours - 40::numeric, 0::numeric)
           - GREATEST(c.hours_before - 40::numeric, 0::numeric) AS day_ot
    FROM b_cum c
),
-- ⚠ Time entries carry a work date but no reliable start/end timestamp, so two
-- jobs on the day the week crosses 40 cannot be ordered. Split ONLY that day's
-- premium proportionally and say so — never invent chronology from insertion
-- order or uuids.
b_alloc AS (
  SELECT dj.employee_id, dj.week_start_date, dj.work_date, dj.job_id, dj.is_pw,
         dj.day_job_hours, dj.entry_count, dj.reviewed, dj.unreviewed_entries,
         CASE WHEN s.day_hours = 0 THEN 0::numeric
              ELSE round(s.day_ot * dj.day_job_hours / s.day_hours, 4) END AS ot_hours,
         (s.day_ot > 0 AND s.day_ot < s.day_hours AND s.buckets > 1) AS ot_order_ambiguous
    FROM b_day_job dj
    JOIN b_split s ON s.employee_id = dj.employee_id
                  AND s.week_start_date = dj.week_start_date
                  AND s.work_date = dj.work_date
),
-- Price each day-job slice. The PW rate is picked by WORK DATE, not by week.
b_priced AS (
  SELECT a.*,
         er.rate AS emp_rate,
         er.is_fallback,
         pr.straight_loaded,
         pr.overtime_loaded,
         (a.is_pw AND pr.job_id IS NULL) AS pw_rate_missing
    FROM b_alloc a
    JOIN emp_rate er ON er.employee_id = a.employee_id AND er.week_start_date = a.week_start_date
    LEFT JOIN LATERAL (
      SELECT r.job_id, r.straight_loaded, r.overtime_loaded
        FROM v_pw_job_rate r
       WHERE a.is_pw
         AND r.job_id = a.job_id
         AND r.effective_start <= a.work_date
         AND (r.effective_end IS NULL OR r.effective_end >= a.work_date)
       ORDER BY r.effective_start DESC
       LIMIT 1
    ) pr ON true
   WHERE a.job_id IS NOT NULL
),
branch_b AS (
  SELECT job_id, employee_id, week_start_date,
         sum(day_job_hours) AS allocated_hours,
         sum(entry_count)::integer AS entry_count,
         bool_and(reviewed) AS reviewed,
         sum(unreviewed_entries)::integer AS unreviewed_entries,
         sum(ot_hours) AS overtime_hours,
         -- ⚠ PW-with-no-usable-rate falls back to the employee rate rather than
         -- costing the hour at $0 or NULL, and raises pw_rate_missing so it
         -- cannot be mistaken for a normal number. Same spirit as
         -- used_earliest_rate_fallback.
         round(sum(
           CASE WHEN is_pw AND straight_loaded IS NOT NULL
                THEN (day_job_hours - ot_hours) * straight_loaded + ot_hours * overtime_loaded
                ELSE (day_job_hours - ot_hours) * emp_rate + ot_hours * emp_rate * 1.5
           END), 2) AS allocated_labor_cost,
         sum(CASE WHEN is_pw THEN day_job_hours ELSE 0 END) AS pw_hours,
         bool_or(pw_rate_missing) AS pw_rate_missing,
         bool_or(ot_order_ambiguous) AS ot_order_ambiguous,
         min(emp_rate) AS emp_rate,
         bool_or(is_fallback) AS is_fallback
    FROM b_priced
   GROUP BY job_id, employee_id, week_start_date
)

SELECT a.job_id,
       a.employee_id,
       a.week_start_date,
       a.allocated_hours,
       a.entry_count,
       a.reviewed,
       a.unreviewed_entries,
       a.weekly_total_hours,
       a.overtime_hours,
       a.allocated_hours - a.overtime_hours AS regular_hours,
       a.rate::numeric(12,2) AS true_cost_rate_at_week,
       a.is_fallback AS used_earliest_rate_fallback,
       round((a.allocated_hours - a.overtime_hours) * a.rate + a.overtime_hours * a.rate * 1.5, 2) AS allocated_labor_cost,
       false            AS is_pw_week,
       a.pw_hours,
       a.pw_rate_missing,
       a.ot_order_ambiguous
  FROM branch_a a
UNION ALL
SELECT b.job_id,
       b.employee_id,
       b.week_start_date,
       b.allocated_hours,
       b.entry_count,
       b.reviewed,
       b.unreviewed_entries,
       w.weekly_total_hours,
       b.overtime_hours,
       b.allocated_hours - b.overtime_hours AS regular_hours,
       b.emp_rate::numeric(12,2) AS true_cost_rate_at_week,
       b.is_fallback AS used_earliest_rate_fallback,
       b.allocated_labor_cost,
       true             AS is_pw_week,
       b.pw_hours,
       b.pw_rate_missing,
       b.ot_order_ambiguous
  FROM branch_b b
  JOIN weekly w ON w.employee_id = b.employee_id AND w.week_start_date = b.week_start_date;

COMMENT ON VIEW v_job_labor_cost_true IS
  'True labor cost per job/employee/week. TWO BRANCHES by design (db/schema/072): an employee-week with no prevailing-wage time uses the original proportional overtime spread and is bit-identical to the pre-072 view; a week containing PW time is allocated CHRONOLOGICALLY by work date, so hours worked before the 40th hour are straight time on whichever job actually held them. true_cost_rate_at_week remains the EMPLOYEE rate in both branches — on a PW row it is not what the hour was costed at; divide allocated_labor_cost by hours for that.';

-- Append-only again: the previous 13 columns keep their position.
CREATE OR REPLACE VIEW v_job_labor_cost_true_by_job AS
SELECT j.id AS job_id,
       j.airtable_id,
       j.name,
       j.po_locked,
       round(sum(c.allocated_hours), 2) AS total_hours,
       round(sum(c.overtime_hours), 2) AS overtime_hours,
       round(sum(c.regular_hours), 2) AS regular_hours,
       sum(c.entry_count)::integer AS entry_count,
       sum(c.unreviewed_entries)::integer AS unreviewed_entries,
       round(sum(c.allocated_labor_cost), 2) AS labor_cost_live,
       round(sum(c.allocated_labor_cost) FILTER (WHERE c.reviewed), 2) AS labor_cost_reviewed,
       bool_and(c.reviewed) AS all_labor_reviewed,
       bool_or(c.used_earliest_rate_fallback) AS any_rate_fallback,
       round(sum(c.pw_hours), 2) AS pw_hours,
       bool_or(c.pw_rate_missing) AS pw_rate_missing,
       bool_or(c.ot_order_ambiguous) AS ot_order_ambiguous
  FROM v_job_labor_cost_true c
  JOIN jobs j ON j.id = c.job_id
 GROUP BY j.id, j.airtable_id, j.po_locked, j.name;

-- ── THE LOUD DIAGNOSTIC ────────────────────────────────────────────────────
-- Job costing must not invent a weekly paycheck independently of payroll.
-- Whatever the allocation rule, the hours a week costs must equal the hours the
-- week contains: reallocating overtime between jobs may never create or destroy
-- company-wide labor cost. A mismatch is surfaced, never silently absorbed into
-- one job.
--
-- ⚠ `unlinked_hours` is EXPECTED to be non-zero and is not a defect: hours with
-- no job are real payroll that no job carries. The number to watch is
-- `cost_delta`, which compares like with like — only the hours that DID land on
-- a job, priced two ways.
CREATE OR REPLACE VIEW v_pw_week_reconcile AS
WITH src AS (
  SELECT t.employee_id, t.week_start_date, t.work_date, t.job_id, t.hours,
         (j.prevailing_wage IS TRUE) AS is_pw
    FROM time_entries t
    LEFT JOIN jobs j ON j.id = t.job_id
   WHERE t.employee_id IS NOT NULL
     AND (COALESCE(t.class, '') <> ALL (ARRAY['PTO'::text, 'Paid Holiday'::text]))
), wk AS (
  SELECT employee_id, week_start_date,
         sum(hours) AS week_hours,
         sum(hours) FILTER (WHERE job_id IS NULL) AS unlinked_hours,
         bool_or(is_pw) AS has_pw
    FROM src GROUP BY employee_id, week_start_date
), alloc AS (
  SELECT employee_id, week_start_date,
         sum(allocated_hours) AS allocated_hours,
         sum(overtime_hours)  AS allocated_ot,
         sum(allocated_labor_cost) AS allocated_cost,
         bool_or(pw_rate_missing)    AS pw_rate_missing,
         bool_or(ot_order_ambiguous) AS ot_order_ambiguous
    FROM v_job_labor_cost_true GROUP BY employee_id, week_start_date
)
SELECT w.employee_id, w.week_start_date, w.has_pw,
       w.week_hours,
       COALESCE(w.unlinked_hours, 0) AS unlinked_hours,
       COALESCE(a.allocated_hours, 0) AS allocated_hours,
       round(w.week_hours - COALESCE(w.unlinked_hours, 0) - COALESCE(a.allocated_hours, 0), 4) AS hours_delta,
       COALESCE(a.allocated_ot, 0) AS allocated_ot,
       COALESCE(a.allocated_cost, 0) AS allocated_cost,
       COALESCE(a.pw_rate_missing, false)    AS pw_rate_missing,
       COALESCE(a.ot_order_ambiguous, false) AS ot_order_ambiguous
  FROM wk w
  LEFT JOIN alloc a ON a.employee_id = w.employee_id AND a.week_start_date = w.week_start_date;

COMMENT ON VIEW v_pw_week_reconcile IS
  'Per employee-week: do the hours that landed on jobs still add up after allocation? hours_delta must be 0.0000 on every row in both branches — reallocating overtime may move cost between jobs but may never create or destroy it. Also surfaces pw_rate_missing (flag on, no rate covering the work date) and ot_order_ambiguous (two jobs on the day the week crossed 40). Read by GET ?action=integrityCheck.';

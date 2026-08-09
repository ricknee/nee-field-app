-- Neon slice — PTO and paid holidays.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- Owner, 2026-08-08: "i need PTO. i need to be able to track how much [is] taken.
-- we pay 2 weeks pto every year plus 6 major holidays. also in the payroll pdf to
-- the accountant we need to be able to tell them how many hours were pto or paid
-- holiday."
--
-- Decisions taken with the owner the same day:
--   • PTO is tracked for EMPLOYEES ONLY. The salaried people are the owners; they
--     take time off without it being counted against anything.
--   • The 6 holidays are AUTO-FILLED — nobody records them — for the same
--     employees-only population, because holiday hours only mean something where
--     hours drive pay.
--   • Allowance is PER PERSON and unused hours CARRY OVER.

-- ── 1. The holiday calendar ──────────────────────────────────────────────────
-- Dates rather than rules ("4th Thursday in November") on purpose: the rules are
-- fiddly, they change when a holiday falls on a weekend, and there are six of them
-- a year. Typing six dates once a year is less work than maintaining a rule engine
-- that is wrong every few years.
CREATE TABLE IF NOT EXISTS company_holidays (
  holiday_date date PRIMARY KEY,
  name         text NOT NULL,
  hours        numeric(5,2) NOT NULL DEFAULT 8,
  created_at   timestamptz DEFAULT now()
);

-- ── 2. Per-person, per-year PTO allowance ────────────────────────────────────
-- One row per employee per year. `carried_in_hours` is what rolled over from the
-- previous year — it is stored, not derived, so that closing a year is an explicit
-- act with a number you can see and correct, rather than a recursive calculation
-- back to the beginning of time that silently changes when an old entry is edited.
--
-- ⚠ The year-end rollover is NOT automatic. Someone has to create next year's rows
-- (see the TODO in docs/PLAN-time-clock.md). A missing row means "no allowance
-- recorded", which the balance view reports as such rather than assuming zero.
CREATE TABLE IF NOT EXISTS pto_years (
  employee_id      uuid NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
  year             int  NOT NULL,
  allowance_hours  numeric(6,2) NOT NULL DEFAULT 80,   -- 2 weeks
  carried_in_hours numeric(6,2) NOT NULL DEFAULT 0,
  note             text,
  updated_at       timestamptz DEFAULT now(),
  PRIMARY KEY (employee_id, year)
);

-- ── 3. How PTO and holidays are actually recorded ────────────────────────────
-- As ordinary `time_entries` rows with class = 'PTO' or 'Paid Holiday', no job.
-- That is deliberate: they are hours, payroll already sums hours, and the payroll
-- PDF already groups by employee and week. Inventing a parallel store would mean
-- teaching every one of those paths about a second source of hours.
--
-- ⚠⚠ BUT THEY ARE NOT HOURS WORKED, AND TWO PLACES MUST KNOW THAT:
--
--   1. OVERTIME ON THE PAYROLL PDF. Overtime is owed on hours WORKED. A week of
--      40 worked + 8 PTO is 40 regular + 8 PTO, NOT 40 regular + 8 overtime.
--      Getting this wrong overpays, every single holiday week.
--
--   2. THE OT DENOMINATOR IN v_job_labor_cost_true (006). That view's `weekly`
--      CTE sums ALL of an employee's hours for the week — every job plus unlinked
--      time — to decide how much of their work was overtime, and that feeds job
--      labor cost and therefore GP. Left alone, a PTO day would push genuinely
--      worked hours into the 1.5x band and overstate the cost of every job that
--      person touched that week. This is the same shape as the labor-cost error
--      that already cost this company a ~$179k hole in closeout.
--
-- The exclusion is applied in 024_labor_cost_excludes_pto.sql. This constant is
-- the single definition of "not worked" and both places must use it.
CREATE OR REPLACE VIEW v_nonworked_classes AS
  SELECT unnest(ARRAY['PTO', 'Paid Holiday']) AS class;

-- ── 4. Balances ──────────────────────────────────────────────────────────────
-- Used hours are DERIVED from the time entries, never stored, so correcting a
-- mis-entered PTO day fixes the balance automatically instead of leaving two
-- numbers to reconcile.
CREATE OR REPLACE VIEW v_pto_balances AS
SELECT p.employee_id,
       e.airtable_id,
       e.name,
       p.year,
       p.allowance_hours,
       p.carried_in_hours,
       p.allowance_hours + p.carried_in_hours                        AS entitled_hours,
       coalesce(u.used_hours, 0)                                     AS used_hours,
       p.allowance_hours + p.carried_in_hours - coalesce(u.used_hours, 0) AS remaining_hours
  FROM pto_years p
  JOIN employees e ON e.id = p.employee_id
  LEFT JOIN (
    SELECT employee_id,
           EXTRACT(YEAR FROM work_date)::int AS yr,
           sum(hours) AS used_hours
      FROM time_entries
     WHERE class = 'PTO'
       AND employee_id IS NOT NULL
     GROUP BY 1, 2
  ) u ON u.employee_id = p.employee_id AND u.yr = p.year;

CREATE INDEX IF NOT EXISTS time_entries_class_idx ON time_entries (class)
  WHERE class IN ('PTO', 'Paid Holiday');

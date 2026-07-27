-- Neon slice 1 — time_entries (+ minimal employees/jobs anchors).
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-07-27.
--
-- This file is the annotated source of truth. It was applied via the Neon MCP
-- migration path, which mangles inline SQL comments — so the SQL was applied
-- bare and the reasoning lives here. Keep the two in sync by hand.
--
-- Design rule (carried from the Airtable model): keep BOTH job_id (nullable FK)
-- AND job_name (static text). ~79% of historical entries have no live project
-- record; the text snapshot IS the history. Never derive job_name from job_id.

CREATE TABLE employees (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id text UNIQUE NOT NULL,          -- idempotent ETL key
  name        text NOT NULL,
  username    text,
  role        text,                          -- admin | office | viewer | employee
  active      boolean DEFAULT true,
  created_at  timestamptz DEFAULT now()
);

CREATE TABLE jobs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id text UNIQUE NOT NULL,
  name        text NOT NULL,
  created_at  timestamptz DEFAULT now()
);

CREATE TABLE time_entries (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id      text UNIQUE NOT NULL,     -- ON CONFLICT target; makes the ETL re-runnable
  employee_name    text,                     -- static snapshot; carries QB Time misspellings
                                             -- (e.g. "Patrick Gingrinch" vs "Patrick Gingerich")
  employee_id      uuid REFERENCES employees(id),   -- nullable; 38% of sampled rows had no link
  work_date        date,

  -- Mirrors the Airtable "Week Start Date" formula
  --   DATEADD(work_date, -MOD(WEEKDAY(work_date)+6, 7), 'days')
  -- i.e. the Monday of the work_date's week. Payroll groups on this.
  -- Verified: all 200 sampled rows land on ISO day-of-week 1.
  week_start_date  date GENERATED ALWAYS AS (work_date - (EXTRACT(ISODOW FROM work_date)::int - 1)) STORED,

  -- QB Time is authoritative in seconds. Airtable's field is number/precision 1,
  -- so numeric (not integer) — no fractional values seen in the sample, but the
  -- source type permits them.
  duration_seconds numeric(12,1) NOT NULL DEFAULT 0,

  -- CRITICAL — Airtable's Hours is NOT seconds/3600. The live formula is
  --   ROUND((Duration (Seconds) / 3600) * 4, 0) / 4
  -- i.e. rounded to the nearest QUARTER HOUR. Verified against 200 real rows:
  -- plain division mismatched 131/200; this rule mismatched 0/200. On the
  -- 200-row sample the two differ by 1.27 h, which scales to roughly 90 h of
  -- payroll error across the full ~14.5k-row table. Do not "simplify" this.
  hours            numeric(10,2) GENERATED ALWAYS AS (ROUND((duration_seconds / 3600.0) * 4) / 4) STORED,

  -- Free text on purpose for the first cut: these originate in QuickBooks Time
  -- and its spellings (Massilon, New Philadephia, ...) are authoritative, and
  -- the PR_CITY_TAXES dropdown must match them verbatim. Promote to a reference
  -- table only when the QB importer is retired (plan steps 5/6).
  city_taxes       text,

  class            text,                     -- "Class" (singleLineText)
  labor_type       text,                     -- "Labor Type" — duplicate of class in every sampled row;
                                             -- carried separately until we confirm they can diverge
  source           text,                     -- 'TSheets' | 'Manual' — marks importer-owned rows,
                                             -- needed to scope the cutover in step 5
  notes            text,                     -- present on ~12% of rows; missing from the original plan
  billable         boolean,

  job_id           uuid REFERENCES jobs(id), -- nullable: NULL for the ~79% historical
  job_name         text,                     -- nullable: 5 prod rows are blank (repair or sentinel
                                             -- before the full ETL — see plan "Open / deferred")
  labor_reviewed   boolean DEFAULT false,    -- Airtable field is "Labor Reviewed", NOT "Reviewed"

  airtable_created_at timestamptz,
  created_at       timestamptz DEFAULT now()
);

CREATE INDEX time_entries_job_name_idx   ON time_entries (job_name);
CREATE INDEX time_entries_work_date_idx  ON time_entries (work_date);
CREATE INDEX time_entries_employee_idx   ON time_entries (employee_id);
CREATE INDEX time_entries_week_start_idx ON time_entries (week_start_date);

-- The "Hours by Job" read (handleHoursByJob in airtable.js) ports to:
--   SELECT job_name, sum(hours) AS hours, count(*) AS entries,
--          min(work_date) AS first_date, max(work_date) AS last_date,
--          bool_and(job_id IS NULL) AS historical
--   FROM time_entries GROUP BY job_name ORDER BY hours DESC;
-- Confirmed equivalent against the sample load.

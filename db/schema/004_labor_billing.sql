-- Neon slice 5, phase A — the LABOR REVENUE chain.
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-07-31.
--
-- Applied BARE via the Neon MCP (which mangles inline SQL comments); reasoning lives
-- here. Keep the two in sync by hand.
--
-- PURPOSE: reproduce the four money fields Airtable computes on each Time Entry —
--   Billed Hours, Unbilled Hours, Unbilled Labor Revenue $, Ready to Invoice Hours
-- These are what block handleTimeEntries (the per-job Time Entries tab) from moving
-- to Neon. The COST chain (Labor Cost Rates, Job Labor Allocation) is phase B and is
-- deliberately not here.
--
-- ── WHAT THE DATA SAID, versus what the plan feared ────────────────────────
-- The plan warned about "date-ranged rate lookups — temporal joins are easy to get
-- subtly wrong". Inspecting production on 2026-07-31, that hazard does not exist:
--   * Labor Billable Rates has SIX rows total — a rate card (65/70/75/85/90/350)
--   * NO row sets Employee, so rates never resolve per person
--   * NO row sets an Effective End Date, so nothing is time-bounded
--   * every job resolves to at most ONE rate; 0 jobs return multiple
-- One rate row serves many jobs, so the FK direction is jobs -> rate.
--
-- 76 of 110 jobs have a rate. The other 34 have none: Airtable's lookup returns
-- empty, VALUE(ARRAYJOIN(empty)) is blank, and Unbilled Labor Revenue $ lands at 0.
-- The views must reproduce that, NOT treat a missing rate as an error.

CREATE TABLE labor_billable_rates (
  id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id          text UNIQUE NOT NULL,
  labor_type           text,
  billable_hourly_rate numeric(12,2),
  -- Carried for fidelity even though every row is currently open-ended. If a rate
  -- ever gets an end date the model is already shaped for it.
  effective_start_date date,
  effective_end_date   date,
  notes                text,
  synced_at            timestamptz
);

-- A job has at most one rate; a rate serves many jobs.
ALTER TABLE jobs ADD COLUMN billable_rate_id uuid REFERENCES labor_billable_rates(id);

-- Denormalised alongside the FK on purpose: this is the value Airtable's lookup
-- actually hands to the formula, so keeping it verbatim makes the diff gate a
-- like-for-like comparison instead of a re-derivation.
ALTER TABLE jobs ADD COLUMN billable_hourly_rate numeric(12,2);

-- One row per (Time Entry -> Invoice) allocation. 2,560 rows across 2,559 time
-- entries, so ~1:1 today, but the model must stay one-to-many: an entry can be
-- split across invoices, which is the entire point of the table.
CREATE TABLE labor_billing_allocations (
  id                     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id            text UNIQUE NOT NULL,
  time_entry_id          uuid REFERENCES time_entries(id),
  -- Kept alongside the FK because the ETL resolves links by Airtable id, and an
  -- allocation can arrive before its time entry does.
  time_entry_airtable_id text,
  -- Invoices are NOT in Neon yet (phase C). Holding the raw id keeps the link
  -- recoverable without a dangling FK to a table that does not exist.
  invoice_airtable_id    text,
  allocated_hours        numeric(12,2),
  -- Snapshot of the rate at allocation time. Airtable looks this through to the
  -- Time Entry's T&M Bill Rate; storing it makes historical revenue reproducible
  -- even if a job's rate is later changed.
  bill_rate              numeric(12,2),
  -- Present in Airtable but NULL on all 2,560 rows — carried so its emptiness is
  -- visible rather than looking like an ETL omission.
  billing_stage          text,
  synced_at              timestamptz
);

CREATE INDEX lba_time_entry_idx    ON labor_billing_allocations (time_entry_id);
CREATE INDEX lba_time_entry_at_idx ON labor_billing_allocations (time_entry_airtable_id);
CREATE INDEX lba_invoice_idx       ON labor_billing_allocations (invoice_airtable_id);

-- ── the four money fields, ported ──────────────────────────────────────────
-- Reproduces verbatim, from docs/GP-FORMULA-INVENTORY.md:
--   Billed Hours            = rollup SUM of Labor Billing Allocations.Allocated Hours
--   Unbilled Hours          = IF(Hours = BLANK(), 0, MAX(0, Hours - IF(Billed Hours, Billed Hours, 0)))
--   Unbilled Labor Revenue $= IF(Unbilled Hours > 0, Unbilled Hours * VALUE(ARRAYJOIN(T&M Bill Rate)), 0)
--   Ready to Invoice Hours  = IF(AND(Labor Reviewed, Billable), Unbilled Hours, 0)
--
-- Two Airtable behaviours that had to be reproduced rather than "fixed":
--   * T&M Bill Rate resolves through the JOB link, not the employee — so the rate
--     is per-job. Hence the join to jobs, not employees.
--   * A job with no rate makes VALUE(ARRAYJOIN(empty)) blank, and blank arithmetic
--     is 0 in Airtable. coalesce(...,0) matches that. 34 of 110 jobs have no rate,
--     so this is the normal path, not an edge case.
--   * Billable is nullable; Airtable's AND() treats null as false.
--
-- VERIFIED 2026-07-31: all four columns diffed against Airtable's stored values
-- across ALL 14,564 time entries — ZERO mismatches on every field.
CREATE OR REPLACE VIEW v_time_entry_billing AS
SELECT t.id, t.airtable_id, t.work_date, t.employee_name, t.job_name, t.hours,
       coalesce(b.billed_hours, 0)::numeric(12,2) AS billed_hours,
       GREATEST(0, t.hours - coalesce(b.billed_hours, 0))::numeric(12,2) AS unbilled_hours,
       (CASE WHEN GREATEST(0, t.hours - coalesce(b.billed_hours, 0)) > 0
             THEN GREATEST(0, t.hours - coalesce(b.billed_hours, 0)) * coalesce(j.billable_hourly_rate, 0)
             ELSE 0 END)::numeric(14,2) AS unbilled_labor_revenue,
       (CASE WHEN t.labor_reviewed AND coalesce(t.billable, false)
             THEN GREATEST(0, t.hours - coalesce(b.billed_hours, 0))
             ELSE 0 END)::numeric(12,2) AS ready_to_invoice_hours
  FROM time_entries t
  LEFT JOIN jobs j ON j.id = t.job_id
  LEFT JOIN (SELECT time_entry_id, sum(allocated_hours) AS billed_hours
               FROM labor_billing_allocations
              WHERE time_entry_id IS NOT NULL
              GROUP BY time_entry_id) b ON b.time_entry_id = t.id;

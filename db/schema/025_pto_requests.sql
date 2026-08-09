-- Neon slice — PTO requests and approval.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- Owner, 2026-08-08: "i would like for them to be able to request pto and admin
-- approve it then employee be able to see in the my hours how much they have left."
--
-- ── THE SHAPE ────────────────────────────────────────────────────────────────
-- A request is an INTENTION. It becomes hours only when approved, and the hours it
-- becomes are ordinary `time_entries` rows with class = 'PTO' — the same rows the
-- payroll PDF and v_pto_balances already understand. Nothing downstream learns a
-- new concept; approval is just the moment the entries get written.
--
-- That also means the balance needs no separate bookkeeping: `used_hours` in
-- v_pto_balances counts PTO time entries, so approving a request moves the balance
-- automatically and un-approving it moves it back.

CREATE TABLE IF NOT EXISTS pto_requests (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  employee_id   uuid NOT NULL REFERENCES employees(id) ON DELETE CASCADE,

  -- A range rather than one row per day: people ask for "the week of the 14th",
  -- not seven separate things, and a single decision should approve all of it.
  start_date    date NOT NULL,
  end_date      date NOT NULL,

  -- Half days are real and common, so hours-per-day is explicit rather than
  -- assumed to be 8.
  hours_per_day numeric(4,2) NOT NULL DEFAULT 8,

  note          text,

  status        text NOT NULL DEFAULT 'pending',
  requested_at  timestamptz DEFAULT now(),
  decided_by    uuid REFERENCES employees(id),
  decided_at    timestamptz,
  decision_note text,

  CONSTRAINT pto_request_ordered CHECK (end_date >= start_date),
  CONSTRAINT pto_request_hours   CHECK (hours_per_day > 0 AND hours_per_day <= 24),
  CONSTRAINT pto_request_status  CHECK (status IN ('pending', 'approved', 'declined', 'cancelled'))
);

CREATE INDEX IF NOT EXISTS pto_requests_employee_idx ON pto_requests (employee_id, start_date DESC);
-- The admin queue reads this constantly and pending should be a short list.
CREATE INDEX IF NOT EXISTS pto_requests_pending_idx  ON pto_requests (requested_at) WHERE status = 'pending';

-- ── The link back from hours to the request that created them ───────────────
-- ⚠ This is what makes an approval REVERSIBLE. Without it, undoing an approval
-- would mean guessing which PTO entries belonged to which request by matching
-- dates and employee — which breaks the moment someone also has a hand-entered PTO
-- day in the same week. Nullable: a PTO entry typed straight into Payroll has no
-- request behind it, and that stays legal.
ALTER TABLE time_entries
  ADD COLUMN IF NOT EXISTS pto_request_id uuid REFERENCES pto_requests(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS time_entries_pto_request_idx
  ON time_entries (pto_request_id) WHERE pto_request_id IS NOT NULL;

-- ── What a request is worth, in hours ────────────────────────────────────────
-- Computed rather than stored, because the answer depends on the holiday calendar
-- and on which days are weekends, and both are better read live than frozen at
-- request time.
--
-- Two exclusions, both deliberate:
--   • WEEKENDS. Asking for "the 14th to the 18th" means the working week.
--   • COMPANY HOLIDAYS. Nobody should burn PTO on a day the company already pays
--     for. A week containing Christmas costs four days of PTO, not five.
CREATE OR REPLACE VIEW v_pto_request_days AS
SELECT r.id AS request_id,
       r.employee_id,
       d::date AS work_date,
       r.hours_per_day AS hours
  FROM pto_requests r
  CROSS JOIN LATERAL generate_series(r.start_date, r.end_date, INTERVAL '1 day') d
 WHERE EXTRACT(ISODOW FROM d) < 6                                    -- Mon-Fri only
   AND NOT EXISTS (SELECT 1 FROM company_holidays h WHERE h.holiday_date = d::date);

CREATE OR REPLACE VIEW v_pto_requests AS
SELECT r.*,
       e.airtable_id AS employee_airtable_id,
       e.name        AS employee_name,
       coalesce(dd.days, 0)  AS days,
       coalesce(dd.hours, 0) AS total_hours
  FROM pto_requests r
  JOIN employees e ON e.id = r.employee_id
  LEFT JOIN (
    SELECT request_id, count(*)::int AS days, sum(hours) AS hours
      FROM v_pto_request_days GROUP BY 1
  ) dd ON dd.request_id = r.id;

-- ── Payroll Runs + Bonuses in Neon ─────────────────────────────────────────
-- Applied BARE to the production branch via the Neon MCP 2026-08-11; this file
-- is the annotated source of truth. Audit item 02.
--
-- ── WHY: THE LAST AIRTABLE CALL ON EVERY PAYROLL READ ──────────────────────
-- `computePayrollDateRanges` is called FIRST by all four payroll handlers, and
-- it paged the entire Airtable Payroll Runs table to extract ONE value: the
-- newest non-superseded Pay Period End. That is the 400-600 ms gap between
-- `_ms` (the Neon leg) and wall time that ROADMAP §3 Step 1 noticed and never
-- chased down — an unconditional Airtable round-trip on the hot path, not a
-- fallback.
--
-- In Neon it is an index scan on `pr_runs_open_period_idx`, which is PARTIAL on
-- `WHERE NOT superseded` — precisely the predicate the query uses.
--
-- ⚠ SUPERSEDING IS NOT DECORATION. The pay period 2026-07-26 → 08-08 has SIX
-- runs, five superseded. Read the wrong one and every payroll tile moves by a
-- fortnight. Both the flipped reads carry `NOT superseded`.
--
-- ── WHAT IS AND IS NOT HERE ────────────────────────────────────────────────
-- Scalars only. **The PDF and JSON attachments stay on the Airtable record** —
-- they are uploaded through content.airtable.com and `handlePayrollRunsList`
-- returns a live attachment URL for the download button.
--
-- ⬜ THEREFORE `handlePayrollRunsList` IS DELIBERATELY NOT FLIPPED. It needs
-- that URL, and moving it means moving payroll PDFs to R2 first — the same job
-- `copyEstimatePdfsToR2` did for estimates, and worth doing for the same reason
-- (Airtable attachment URLs expire; see db/schema/009). Until then that one read
-- stays on Airtable and this migration is partial by design, not by omission.
-- `handlePayrollBonusesRollup` is also still on Airtable: it joins employees and
-- filters by payroll-eligible role, so it is its own slice.
--
-- Flipped in this pass: `computePayrollDateRanges` (the hot path) and
-- `handleFindMatchingPayrollRun`. Both Neon-first with the Airtable path intact
-- as a fallback — an empty Neon must not silently shift a payroll figure.
--
-- ── THE WRITE MOVED IN THE SAME COMMIT, AND HAD TO ─────────────────────────
-- `handlePayrollRunCreate` now mirrors the run, its bonuses and the supersede
-- flag into Neon. Without it, a run saved to Airtable but not Neon would leave
-- every payroll screen pointing at the PREVIOUS fortnight — a plausible wrong
-- number rather than an error. "Flip a read without its write" is the recurring
-- bug in this project (ROADMAP §8 lists three in one day).
--
-- The mirror fails SOFT: by the time it runs, the run and its PDF are already in
-- Airtable and that is the artifact people are paid from. A failure surfaces as
-- `neonMirrorError` in the response instead of failing work that succeeded.
--
-- Bonuses are RE-READ from Airtable rather than taken from the request, because
-- only the created records carry their real rec ids — same reasoning as the
-- inventory expense push at Step E.
--
-- ── BACKFILL, VERIFIED BY THREE INDEPENDENT SUMS ───────────────────────────
-- 28 runs and 31 bonuses loaded from production Airtable. Every figure was
-- hand-totalled from the source before loading and matched exactly:
--   runs 28 · total_hours 9,795.75 · total_bonus 13,800.00
--   bonuses 31 · amount 12,900.00 · unresolved FKs 0
--   newest non-superseded pay_period_end = 2026-08-08 (what the Airtable path
--   derives today, so the flip is a no-op on current behaviour)
--
-- Parameter types checked with PREPARE: {text,date,date,text,numeric,numeric,text}
-- for the run mirror, {text,text,text,text,numeric} for the bonus mirror, and
-- {date,date} for the find-match read.
--
-- ⚠ Two data oddities carried across verbatim rather than "corrected", because
-- Airtable is the source and a loader is the wrong place to fix data:
--   * `recdyryDlCxFuAlfo` has Pay Period Start 2026-03-22 and End 2026-02-07 —
--     start AFTER end. It is non-superseded and carries $1,800 of bonuses.
--   * 17 of 28 runs are non-superseded, several sharing a pay period, which is
--     why every read orders by date and takes exactly one row.
--
-- ── ROLLBACK ───────────────────────────────────────────────────────────────
-- Revert the handler changes; the tables can stay (nothing else reads them).
-- Both flipped reads keep their Airtable path, so reverting is code-only.
CREATE TABLE IF NOT EXISTS payroll_runs (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id       text UNIQUE NOT NULL,
  pay_period_start  date,
  pay_period_end    date,
  generated_at      timestamptz,
  generated_by      text,
  total_hours       numeric(12,2),
  total_bonus       numeric(12,2),
  superseded        boolean NOT NULL DEFAULT false,
  notes             text,
  synced_at         timestamptz
);

-- Partial on purpose: every read that matters asks only about OPEN runs.
CREATE INDEX IF NOT EXISTS pr_runs_open_period_idx
    ON payroll_runs (pay_period_end DESC) WHERE NOT superseded;

CREATE TABLE IF NOT EXISTS payroll_bonuses (
  id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id             text UNIQUE NOT NULL,
  payroll_run_airtable_id text,
  payroll_run_id          uuid REFERENCES payroll_runs(id) ON DELETE CASCADE,
  employee_airtable_id    text,
  -- Denormalised like job_name and vendor_name elsewhere: the rollup groups by
  -- employee and the name is what it displays, so carrying it avoids a join
  -- against a dimension that is already Neon-owned.
  employee_name           text,
  amount                  numeric(12,2),
  synced_at               timestamptz
);

CREATE INDEX IF NOT EXISTS pr_bonuses_run_idx ON payroll_bonuses (payroll_run_airtable_id);

-- 056_payroll_native.sql — identity cutover, slice 2 (PART TWO OF TWO).
-- See docs/PLAN-airtable-identity-cutover.md. Pairs with 054, which shipped the
-- prep and deliberately stopped short of the flip.
--
-- ── THE GATE IN 054's HEADER WAS MET, 2026-08-24 ─────────────────────────────
-- The 2026-08-09 → 08-22 payroll run exercised the R2 write inside
-- `handlePayrollRunCreate` for the first time in production. All three checks:
--
--   · pdf_key = payroll/825eab70-.../20260824092819.pdf, json_key likewise —
--     BOTH SET. Note the shape: timestamp-named, not `att….pdf`, so this was
--     the in-handler write and not `copyPayrollFilesToR2`, which is the other
--     path and the one the 28 backfilled runs came through.
--   · the keys ARE the proof of the write, not merely a record of it — they are
--     stamped only after both putBufferToR2 calls resolve, and cleared on any
--     failure. Non-null keys therefore mean r2Error was null.
--   · the PDF opened from the Payroll Archive tab. That is only evidence
--     because ALL 29 runs carry a key: `payrollRunsList` falls back to Airtable
--     wholesale if even one is null, so with a gap that click would have been
--     opening an Airtable attachment and proving nothing.
--
-- So `handlePayrollRunCreate` is now NEON-FIRST. Airtable is a best-effort
-- mirror; a run is born with no rec id and is stamped with one afterwards.
--
-- ── WHAT THE FLIP ACTUALLY NEEDED, BEYOND ONE HANDLER ────────────────────────
-- ⚠⚠ 054's header and the plan both called this "~20 minutes, one handler, not
-- a sweep". That was WRONG, and the three sites it missed are all silent-wrong-
-- number bugs rather than errors. Recording it because the same undercount is
-- likely in slices 4-6:
--
--   1. `payrollBonusesRollup` joined `r.airtable_id = b.payroll_run_airtable_id`.
--      A native run has a NULL airtable_id; `NULL = NULL` is not true; the LEFT
--      JOIN then drops the row at `r.pay_period_end >= $1`. Every bonus on a
--      native run would vanish from the year-to-date total. This is the exact
--      shape of the `v_invoices` bug slice 3 caught — "a VIEW can join on rec
--      ids too" turns out to apply to plain handlers just as well.
--   2. `payrollEmployeeBonusHistory` — the same join, plus a bare
--      `payroll_run_airtable_id` as the returned run handle.
--   3. `findMatchingPayrollRun` returned a bare `airtable_id`. NULL for a native
--      run, so the client concludes there is no prior run for the period and
--      skips the supersede dialog: TWO non-superseded runs on one period, which
--      `computePayrollDateRanges` resolves by generated_at and gets wrong,
--      moving every payroll tile by a fortnight.
--
-- All three now resolve on the uuid or COALESCE both forms. Each was verified
-- equivalent against live data BEFORE the swap — 31 bonuses, 4 rollup rows,
-- $12,900, zero diff either way — so the change is provably inert today and
-- correct once native runs exist.
--
-- Every new statement was verified as `PREPARE name AS <sql>` with NO type
-- list, per the correction slice 3 paid for.

-- ── THE ONE ACTUAL SCHEMA CHANGE ─────────────────────────────────────────────
-- The bonus index was built on `payroll_run_airtable_id` (052), which is the
-- column both bonus reads have just STOPPED joining on. Without this the new
-- uuid joins fall back to a sequential scan on every payroll screen. Adding it
-- rather than replacing the old one: the Airtable-shaped column is still read
-- by the fallback paths and by `copyPayrollFilesToR2`.
CREATE INDEX IF NOT EXISTS pr_bonuses_run_uuid_idx
  ON payroll_bonuses (payroll_run_id);

COMMENT ON COLUMN payroll_runs.airtable_id IS
  'Nullable, and NOW NULL IN PRACTICE for runs created after 2026-08-24: handlePayrollRunCreate is Neon-first and stamps this from a best-effort Airtable mirror. Resolve runs with COALESCE(airtable_id, id::text), never a bare airtable_id.';

COMMENT ON COLUMN payroll_bonuses.payroll_run_id IS
  'The authoritative link to the run. JOIN ON THIS, not payroll_run_airtable_id — the latter is NULL for a native run and silently drops the bonus from bonus rollups (see the header of 056).';

-- 054_payroll_native_prep.sql — identity cutover, slice 2 (PART ONE OF TWO).
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- ⚠⚠ THIS SLICE IS DELIBERATELY INCOMPLETE, AND THE REASON IS NOT CAUTION FOR
-- ITS OWN SAKE. `handlePayrollRunCreate` is NOT reversed here. Two facts:
--
--   1. The R2 payroll write path has NEVER RUN IN PRODUCTION. The 28 runs
--      backfilled on 2026-08-21 went through `copyPayrollFilesToR2`, which is a
--      DIFFERENT code path — it downloads an Airtable attachment and PUTs it.
--      The write inside `handlePayrollRunCreate` (base64 from the browser →
--      Buffer → presigned PUT) has never executed. Reversing the create would
--      make the first real exercise of an untested path also the ONLY copy of a
--      payroll PDF, because a native run has no Airtable record to attach to.
--
--   2. It cannot be smoke-tested on demand. A payroll run is a real fortnightly
--      event, not a button you press twice to check.
--
-- THE GATE: when the next real payroll run is created, confirm
--   · the response carries `pdfArchived: true` and `r2Error: null`
--   · `SELECT pdf_key FROM payroll_runs ORDER BY generated_at DESC LIMIT 1` is set
--   · the PDF opens from the Payroll Archive tab
-- Then reversing the create is ~20 minutes: Neon INSERT first, Airtable POST and
-- its attachments become a best-effort mirror, exactly as slice 1 did.
--
-- WHAT DOES SHIP HERE is everything that makes that flip safe when it comes, and
-- costs nothing now:
--   · the NOT NULL constraints come off, so a native run is possible
--   · every lookup that resolves a run by id accepts EITHER form
--   · `payrollRunsList` stops emitting a bare `airtable_id` as the run id
--
-- ⚠ Until the create is reversed no native rows can appear, so all of the above
-- is inert. That is intentional: it means the flip, when it happens, is one
-- handler and not a sweep.

ALTER TABLE payroll_runs    ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE payroll_bonuses ALTER COLUMN airtable_id DROP NOT NULL;

COMMENT ON COLUMN payroll_runs.airtable_id IS
  'Nullable since db/schema/054, but NOT yet null in practice: handlePayrollRunCreate is still Airtable-first, gated on the R2 write path being exercised by a real payroll run. See the header of 054.';

-- 055_estimates_invoices_native.sql — identity cutover, slice 3.
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- Estimates, sent-estimate PDFs and invoices stop being minted in Airtable.
-- Same shape as slice 1 (db/schema/053): the row is real the moment Neon has it,
-- the Airtable POST becomes a best-effort mirror, and every lookup accepts
-- EITHER id form — `WHERE airtable_id = $1 OR id::text = $1`, never a generated
-- `handle` column, for the reason written at length in 053.
--
-- ── WHAT MADE THIS SLICE DIFFERENT FROM SLICE 1 ────────────────────────────
--
-- 1. AIRTABLE COMPUTES THREE OF THESE COLUMNS. A native row has no Airtable
--    record to compute anything, so the app has to. Verified against all 89
--    estimates before writing a line of code — both formulas reproduce EXACTLY,
--    zero mismatches:
--
--      Estimated Labor Cost       = {Estimated Labor Hours} * 32.50
--      Calculated Estimated Total = {Estimated Labor Cost} + {Estimated Material Cost}
--
--    ⚠ `estimated_labor_cost` is NOT decoration: `v_job_rollups` sums it into
--    `est_labor_cost_rollup` / `proj_est_labor_cost`, which is estimated GP. A
--    native estimate that left it NULL would quietly report a job as more
--    profitable than it is. The 32.50 is hardcoded in Airtable and is now
--    hardcoded here — see docs/PLAN-prevailing-wage.md, which is the project
--    that changes it, and which needs this constant to be findable.
--
--    The third, `Invoice Number`, is a formula worth knowing about:
--        {Job} & "-" & RIGHT("000" & {Invoice Sequence}, 3)
--    where `Invoice Sequence` = `Invoices for Job` = a COUNT of the records in
--    the invoice's own Job LINK field — which is always 1. So every invoice ever
--    written reads `<job name>-001`, and Bethel School has two of them. It is a
--    display label, not an identifier; `invoice_display_no` is the real number.
--    Reproduced here AS-IS, bug included: a cutover is the wrong moment to
--    change what a document says. Noted in docs/TODO.md instead.
--
-- 2. `v_invoices` JOINED ON REC IDS, AND THAT IS THE MONEY PATH.
--    Its labor and material CTEs joined `invoice_airtable_id = i.airtable_id`.
--    A native invoice has no rec id, so both CTEs would have missed, and
--    `invoice_total_calc` — the figure the invoice screen and the PDF both
--    print — would have come out as ZERO for every T&M invoice. Not an error,
--    not a warning: a $0 invoice. That is the same failure shape as the NULL
--    `bill_rate` in db/schema/036.
--    The view is rewritten below to resolve BOTH forms.
--
-- 3. `labor_billing_allocations` had no `invoice_id`. Its material twin has had
--    one since 033. Added and backfilled here — all 1,221 attached rows resolve,
--    zero orphans.
--
-- ⚠ NOT IN THIS SLICE, THOUGH THE PLAN SAID IT WOULD BE: `job_labor_allocations`.
-- It is the WEEKLY labor-allocation table from db/schema/004, and nothing in
-- either function reads or writes it — its only writer is db/etl/time-entries-full.mjs
-- and its last row arrived 2026-08-09. It has no create path to reverse, so its
-- NOT NULL is vestigial exactly like `labor_cost_rates`. Dropping the constraint
-- would buy nothing and would suggest a native row is expected there. Left alone
-- deliberately; the plan's scope table is corrected in the same commit.
--
-- ⚠ Re-checked for this slice, as 053 requires: `_billing-sync.js` is still the
-- only destructive set-difference in the codebase and still carries
-- `airtable_id IS NOT NULL`, so it cannot delete a native allocation.

ALTER TABLE job_estimates ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE invoices      ALTER COLUMN airtable_id DROP NOT NULL;

COMMENT ON COLUMN job_estimates.airtable_id IS
  'NULL means the estimate was created in the app and Airtable never saw it (or the mirror failed). Look it up with: WHERE airtable_id = $1 OR id::text = $1.';
COMMENT ON COLUMN invoices.airtable_id IS
  'NULL means the invoice was created in the app and Airtable never saw it (or the mirror failed). Look it up with: WHERE airtable_id = $1 OR id::text = $1.';

-- ── THE MISSING UUID LINK ON LABOR ALLOCATIONS ─────────────────────────────
-- ON DELETE SET NULL to match material_billing_allocations.invoice_id: deleting
-- an invoice must not delete the record that work was ever allocated, or the
-- hours silently become unbilled-and-unbillable.
ALTER TABLE labor_billing_allocations
  ADD COLUMN IF NOT EXISTS invoice_id uuid REFERENCES invoices(id) ON DELETE SET NULL;

UPDATE labor_billing_allocations l
   SET invoice_id = i.id
  FROM invoices i
 WHERE i.airtable_id = l.invoice_airtable_id
   AND l.invoice_id IS DISTINCT FROM i.id;

CREATE INDEX IF NOT EXISTS labor_billing_allocations_invoice_id
  ON labor_billing_allocations (invoice_id);

-- ── UNIQUENESS ON THE TWO NUMBERS THE APP NOW MINTS ────────────────────────
-- Both numbers come from a MAX()+1 scan, which is not atomic. While Airtable
-- minted the record the scan was at least reading the same store that created
-- it; now the app owns both ends, so two saves a second apart can read the same
-- MAX. A duplicate estimate number on two quotes to the same customer is the
-- kind of thing that is discovered in a dispute.
--
-- The trade is deliberate and is the same one slice 1 made on company names: the
-- second save FAILS instead of silently duplicating. A failed save is visible
-- and retryable — it re-reads MAX and gets the next number.
-- Verified before creating: zero duplicates exist in either column today.
CREATE UNIQUE INDEX IF NOT EXISTS invoices_display_no_unique
  ON invoices (invoice_display_no) WHERE invoice_display_no IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS sent_estimate_pdfs_display_number_unique
  ON sent_estimate_pdfs (display_number) WHERE display_number IS NOT NULL;

-- ── v_invoices, REWRITTEN TO SURVIVE A NATIVE ROW ──────────────────────────
-- Every CTE now resolves its parent by uuid when it has one and by rec id when
-- it does not, so the three populations coexist:
--   · historical rows, linked by rec id only;
--   · rows the app attaches from today, which carry both;
--   · native rows, which carry only a uuid.
--
-- COALESCE(uuid, resolved-from-rec-id) rather than two joins: a row is counted
-- under exactly one invoice, so nothing can be double-billed by this change.
--
-- ⚠ The uuid is preferred over the rec id, not the other way round. An
-- allocation attached to a native invoice has `invoice_airtable_id IS NULL`
-- forever, and the hourly Airtable sync will never fill it — so a rec-id-first
-- resolution would drop exactly the newest work.
--
-- Nothing else about the view changes: same columns, same order, same money.
CREATE OR REPLACE VIEW v_invoices AS
WITH er AS (
  SELECT COALESCE(e.job_id, j.id) AS job_id,
         COALESCE(sum(e.actual_estimate_sent), 0::numeric) AS expected_revenue
    FROM job_estimates e
    LEFT JOIN jobs j ON j.airtable_id = e.job_airtable_id
   GROUP BY 1
), pcb AS (
  SELECT COALESCE(i.job_id, j.id) AS job_id,
         COALESCE(sum(i.snapshot_total), 0::numeric) AS previous_contract_billing
    FROM invoices i
    LEFT JOIN jobs j ON j.airtable_id = i.job_airtable_id
   WHERE i.invoice_type = 'Contract'::text
   GROUP BY 1
), lab AS (
  SELECT COALESCE(a.invoice_id, i2.id) AS invoice_id,
         COALESCE(sum(a.allocated_hours * a.bill_rate), 0::numeric) AS labor_amount
    FROM labor_billing_allocations a
    LEFT JOIN invoices i2 ON i2.airtable_id = a.invoice_airtable_id
   WHERE a.invoice_id IS NOT NULL OR a.invoice_airtable_id IS NOT NULL
   GROUP BY 1
), mat AS (
  SELECT COALESCE(m.invoice_id, i2.id) AS invoice_id,
         COALESCE(sum(m.allocated_amount), 0::numeric) AS material_amount,
         COALESCE(sum(e.billable_material_amount)
                  FILTER (WHERE e.billable_material_amount < 0::numeric), 0::numeric) AS material_credits
    FROM material_billing_allocations m
    LEFT JOIN invoices i2 ON i2.airtable_id = m.invoice_airtable_id
    LEFT JOIN expenses e  ON e.id = m.expense_id
   WHERE m.invoice_id IS NOT NULL OR m.invoice_airtable_id IS NOT NULL
   GROUP BY 1
)
SELECT i.id,
       i.airtable_id,
       i.job_id,
       i.job_airtable_id,
       i.invoice_number,
       i.invoice_status,
       i.invoice_type,
       i.billing_mode,
       i.invoice_stage,
       i.invoice_date,
       i.snapshot_total,
       i.invoice_total,
       i.manual_labor,
       i.manual_material,
       i.percent_to_bill,
       i.auto_allocate,
       i.invoice_display_no,
       i.synced_at,
       COALESCE(er.expected_revenue, 0::numeric)          AS expected_revenue,
       COALESCE(pcb.previous_contract_billing, 0::numeric) AS previous_contract_billing,
       COALESCE(lab.labor_amount, 0::numeric)             AS invoice_labor_amount,
       COALESCE(mat.material_amount, 0::numeric)          AS invoice_material_amount,
       COALESCE(mat.material_credits, 0::numeric)         AS material_credits,
       (COALESCE(er.expected_revenue, 0::numeric)
        - COALESCE(pcb.previous_contract_billing, 0::numeric))::numeric(14,2) AS contract_remaining,
       CASE
         WHEN i.invoice_type = 'Contract'::text
           THEN round(COALESCE(er.expected_revenue, 0::numeric) * COALESCE(i.percent_to_bill, 0::numeric), 2)
         ELSE 0::numeric
       END AS contract_invoice_amount,
       CASE
         WHEN i.invoice_type = 'Contract'::text
           THEN LEAST(COALESCE(er.expected_revenue, 0::numeric) - COALESCE(pcb.previous_contract_billing, 0::numeric),
                      round(COALESCE(er.expected_revenue, 0::numeric) * COALESCE(i.percent_to_bill, 0::numeric), 2))
         ELSE 0::numeric
       END::numeric(14,2) AS final_contract_invoice_amount,
       CASE
         WHEN i.billing_mode = 'Contract % Progress'::text
              AND COALESCE(er.expected_revenue, 0::numeric) <> 0::numeric
           THEN round((COALESCE(er.expected_revenue, 0::numeric) - COALESCE(pcb.previous_contract_billing, 0::numeric))
                      / er.expected_revenue, 4)
         ELSE NULL::numeric
       END AS remaining_percent_to_bill,
       CASE
         WHEN i.billing_mode = 'Contract % Progress'::text THEN
           CASE
             WHEN i.invoice_type = 'Contract'::text
               THEN LEAST(COALESCE(er.expected_revenue, 0::numeric) - COALESCE(pcb.previous_contract_billing, 0::numeric),
                          round(COALESCE(er.expected_revenue, 0::numeric) * COALESCE(i.percent_to_bill, 0::numeric), 2))
             ELSE 0::numeric
           END
         ELSE COALESCE(lab.labor_amount, 0::numeric)
              + COALESCE(i.manual_labor, 0::numeric)
              + COALESCE(mat.material_amount, 0::numeric)
              + COALESCE(i.manual_material, 0::numeric)
              + COALESCE(mat.material_credits, 0::numeric)
       END::numeric(14,2) AS invoice_total_calc
  FROM invoices i
  LEFT JOIN er  ON er.job_id      = i.job_id
  LEFT JOIN pcb ON pcb.job_id     = i.job_id
  LEFT JOIN lab ON lab.invoice_id = i.id
  LEFT JOIN mat ON mat.invoice_id = i.id;

-- ── The invoice contract-billing chain, ported (roadmap Step 4e) ───────────
-- Applied to the DEFAULT branch of damp-silence-99074350 on 2026-08-08.
-- Every field reproduces Airtable EXACTLY across all 51 invoices.
--
-- This was billed as the deepest formula chain in the system and the last real
-- piece of the field-app migration. It decides what a customer is billed on a
-- contract job, so nothing here was inferred — every formula was read out of
-- the base and every input diffed row by row before this view was wired up.
--
-- ── THE CHAIN, AS AIRTABLE DEFINES IT ──────────────────────────────────────
--   Contract Invoice Amount  = IF(Type="Contract", ROUND(ExpectedRevenue * PercentToBill, 2), 0)
--   Contract Remaining       = ExpectedRevenue - PreviousContractBilling
--   Final Contract Inv Amt   = IF(Type="Contract", MIN(ContractRemaining, ContractInvoiceAmount), 0)
--   Remaining Percent to Bill= IF(Mode="Contract % Progress",
--                                 ROUND(ContractRemaining/ExpectedRevenue, 4), BLANK())
--   Invoice Total            = IF(Mode="Contract % Progress", FinalContractInvoiceAmount,
--                                 Labor + ManualLabor + Material + ManualMaterial + Credits)
--
-- The MIN() is a safety rail: a contract invoice can never bill more than the
-- contract has left, however the percentage is set.
--
-- ── WHY IT IS NOT CIRCULAR, WHICH IS THE THING TO UNDERSTAND ───────────────
-- Contract Remaining depends on `Previous Contract Billing`, which is a lookup
-- of `Jobs.Total Contract Billed`, which is a rollup over the job's INVOICES.
-- That looks like a cycle — Remaining → Final → Invoice Total → Total Billed →
-- Remaining — and it would be, except the rollup sums **Snapshot Total**, a
-- plain STORED currency field, not the computed Invoice Total.
--
-- So the cycle is broken by the snapshot: live totals never feed back into the
-- figure that constrains them. Reproduce that faithfully. Summing invoice_total
-- there instead would create genuine recursion AND silently change what every
-- contract invoice is allowed to bill.
--
-- ── THE FOUR INPUTS, EACH VERIFIED SEPARATELY BEFORE THE CHAIN WAS BUILT ───
--   Expected Revenue         = SUM(job_estimates.actual_estimate_sent)     51/51
--                              (no status filter — checked)
--   Previous Contract Billing= SUM(invoices.snapshot_total)
--                              WHERE invoice_type = 'Contract'             51/51
--     ⚠ Four hypotheses were tested against the data: all invoices (41/51),
--       excluding self (27/51), contract-only (51/51), contract-excluding-self
--       (28/51). Contract-only wins outright, and it does NOT exclude the
--       current invoice. Guessing either of those would have been wrong.
--   Invoice Labor Amount     = SUM(lba.allocated_hours * lba.bill_rate)
--   Invoice Material Amount  = SUM(mba.allocated_amount)                   51/51
--   Material Credits         = SUM(expenses.billable_material_amount)
--                              FILTER (WHERE it is NEGATIVE)
--     ⚠ This rollup targets Billable Material Amount, NOT the allocated
--       amount, so it looks like a duplicate of Invoice Material Amount. It is
--       not: it captures only credits (returned/refunded material). Exactly one
--       invoice in the base has a non-zero value, -189.30, and it reproduces.
--
-- ── ⚠ PRECISION: percent_to_bill WAS numeric(10,4) AND THAT WAS WRONG ──────
-- Airtable's percent field DISPLAYS as a whole number but stores full float
-- precision. Truncating to 4dp put three contract invoices out by $2-$9, in
-- both directions, on a job billing $171,475. Widened to numeric(18,10):
--   0.1581 -> 0.1581130000
-- Third time this exact lesson has appeared: wire costs (sub-cent), the
-- half-cent residues in 013, and now this. **A displayed value is not a stored
-- value. Round only the final result.**
--
-- ── VERIFICATION (all 51 invoices, every field) ────────────────────────────
--   Invoice Total 51/51 · Final Contract Invoice Amount 51/51 ·
--   Contract Remaining 51/51 · Contract Invoice Amount 51/51 ·
--   Invoice Labor Amount 51/51 · Invoice Material Amount 51/51 ·
--   Material Credits 51/51
--
-- Nothing reads this view yet — same discipline as 013 and 014. The handler
-- flip is a separate change, so the arithmetic is proven before it serves.
CREATE OR REPLACE VIEW v_invoices AS
WITH er AS (
  SELECT job_airtable_id, COALESCE(sum(actual_estimate_sent),0) AS expected_revenue
    FROM job_estimates GROUP BY job_airtable_id
), pcb AS (
  SELECT job_airtable_id, COALESCE(sum(snapshot_total),0) AS previous_contract_billing
    FROM invoices WHERE invoice_type = 'Contract' GROUP BY job_airtable_id
), lab AS (
  SELECT invoice_airtable_id, COALESCE(sum(allocated_hours * bill_rate),0) AS labor_amount
    FROM labor_billing_allocations WHERE invoice_airtable_id IS NOT NULL
   GROUP BY invoice_airtable_id
), mat AS (
  SELECT m.invoice_airtable_id,
         COALESCE(sum(m.allocated_amount),0) AS material_amount,
         COALESCE(sum(e.billable_material_amount)
                  FILTER (WHERE e.billable_material_amount < 0),0) AS material_credits
    FROM material_billing_allocations m
    LEFT JOIN expenses e ON e.id = m.expense_id
   WHERE m.invoice_airtable_id IS NOT NULL
   GROUP BY m.invoice_airtable_id
)
SELECT i.*,
       COALESCE(er.expected_revenue,0)           AS expected_revenue,
       COALESCE(pcb.previous_contract_billing,0) AS previous_contract_billing,
       COALESCE(lab.labor_amount,0)              AS invoice_labor_amount,
       COALESCE(mat.material_amount,0)           AS invoice_material_amount,
       COALESCE(mat.material_credits,0)          AS material_credits,
       (COALESCE(er.expected_revenue,0) - COALESCE(pcb.previous_contract_billing,0))::numeric(14,2)
         AS contract_remaining,
       CASE WHEN i.invoice_type = 'Contract'
            THEN round(COALESCE(er.expected_revenue,0) * COALESCE(i.percent_to_bill,0), 2)
            ELSE 0 END AS contract_invoice_amount,
       CASE WHEN i.invoice_type = 'Contract'
            THEN LEAST(COALESCE(er.expected_revenue,0) - COALESCE(pcb.previous_contract_billing,0),
                       round(COALESCE(er.expected_revenue,0) * COALESCE(i.percent_to_bill,0), 2))
            ELSE 0 END::numeric(14,2) AS final_contract_invoice_amount,
       CASE WHEN i.billing_mode = 'Contract % Progress' AND COALESCE(er.expected_revenue,0) <> 0
            THEN round((COALESCE(er.expected_revenue,0) - COALESCE(pcb.previous_contract_billing,0))
                       / er.expected_revenue, 4)
            END AS remaining_percent_to_bill,
       CASE WHEN i.billing_mode = 'Contract % Progress'
            THEN CASE WHEN i.invoice_type = 'Contract'
                      THEN LEAST(COALESCE(er.expected_revenue,0) - COALESCE(pcb.previous_contract_billing,0),
                                 round(COALESCE(er.expected_revenue,0) * COALESCE(i.percent_to_bill,0), 2))
                      ELSE 0 END
            ELSE COALESCE(lab.labor_amount,0)  + COALESCE(i.manual_labor,0)
               + COALESCE(mat.material_amount,0) + COALESCE(i.manual_material,0)
               + COALESCE(mat.material_credits,0)
       END::numeric(14,2) AS invoice_total_calc
  FROM invoices i
  LEFT JOIN er  ON er.job_airtable_id      = i.job_airtable_id
  LEFT JOIN pcb ON pcb.job_airtable_id     = i.job_airtable_id
  LEFT JOIN lab ON lab.invoice_airtable_id = i.airtable_id
  LEFT JOIN mat ON mat.invoice_airtable_id = i.airtable_id

-- 066_estimate_other_costs.sql — give an estimate somewhere to put the two job
-- costs it has never been able to hold: bought-in costs that are neither labor
-- nor material, and the sales tax paid on the purchase.
--
-- ── WHY, WITH THE MEASUREMENT ──────────────────────────────────────────────
-- A bid summary out of the estimating program (Southwood, 2026-08-27) carries
-- six cost lines. Four of them have a home on `job_estimates`; two do not:
--
--   Extended Labor Costs      $88,344.00   hours x $85 quoted   → labor_sell_rate
--   Extended Material Cost    $38,473.13   bare cost            → material_raw_cost
--   Extended Other Costs     $126,850.00   bare cost            → NOWHERE
--   Extended Quotes Costs          $0.00   bare cost            → NOWHERE
--   Sales tax (8%)            $13,225.85   real cash out        → NOWHERE
--   Selling Price            $311,303.98                        → actual_estimate_sent
--
-- Keyed into the tab as it stood, $140,075.85 of real cost simply vanished and
-- the screen reported **76.8% GP** on a job that estimates at 31.8%. That is
-- the 065 failure mode with the sign flipped, and the flipped sign is the
-- dangerous one: 065 made GP read LOW, which is why it survived five years
-- unchallenged. A GP that reads HIGH gets acted on.
--
-- ── WHAT GOES IN THEM, AND WHAT DELIBERATELY DOES NOT ──────────────────────
-- `other_costs` is bought-in DIRECT job cost at COST — subcontracts, quoted
-- gear, rentals, equipment. Owner confirmed 2026-08-28 that the bid program's
-- figure is bare: no tax, no markup inside it. Quotes and Other are one column
-- rather than two because nothing downstream tells them apart.
--
-- ⚠ LABOR DOES NOT GO IN HERE, and the bid summary is exactly the shape that
-- tempts it. Its "Extended Labor Costs" is priced at the QUOTED rate ($85/hr),
-- so it is labor SELL wearing the word "cost". Dropping it into `other_costs`
-- would put the labor margin on the cost side and report Southwood at 14.3%.
-- Labor belongs in `estimated_labor_hours` x `labor_burden_rate`, which is the
-- split 065 already built. Enter the HOURS; let the rate do its job.
--
-- ⚠ OVERHEAD AND PROFIT PERCENTS ARE NOT STORED AND MUST NOT BE. They are how
-- the bid program walks from its cost stack to a price, and the price is
-- already recorded as `actual_estimate_sent`. Storing them would let the screen
-- subtract company overhead as if it were job cost — which is what makes the
-- bid program's own subtotal read as a 14.3% job. Gross profit is BEFORE
-- overhead; overhead comes out of GP, not before it.
--
-- ── WHY TAX IS ITS OWN COLUMN AND NOT FOLDED INTO other_costs ──────────────
-- On a job WITH a bid summary, folding them costs nothing — the report prints
-- both and you type the sum. The failure is on the ~90% of jobs with no report,
-- where the estimator types other costs from scratch and simply forgets the
-- tax. That understates cost by 8% of (material + other) and overstates GP,
-- silently, in the direction nobody investigates. A separate box with a
-- "fill 8%" control makes forgetting it a visible blank rather than a wrong
-- number. The 8% base is (material + other): labor is not taxable, and a
-- subcontractor pays tax on their own purchases, so quotes are not taxed here
-- either. That is both what the bid program does and what Ohio does.
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS other_costs numeric(14,2);
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS sales_tax   numeric(14,2);

COMMENT ON COLUMN job_estimates.other_costs IS
  'Bought-in direct job cost at COST that is neither labor nor material: subcontracts, quoted gear, rentals, equipment. Excludes sales tax (see sales_tax) and excludes labor — a bid summary''s "Extended Labor Costs" is labor SELL and belongs in hours x labor_burden_rate. NULL = not recorded separately, which on a pre-2026-08-28 row means "unknown", not "zero".';
COMMENT ON COLUMN job_estimates.sales_tax IS
  'Sales tax dollars paid on the purchase, a real job cost. Stored rather than derived because the rate and the exempt/resale treatment vary by job. The UI fills it at 8% x (material_raw_cost + other_costs) — labor is not taxable and a sub pays their own.';

-- ── THE ROLLUPS ────────────────────────────────────────────────────────────
-- Rebuilt from `pg_get_viewdef('v_job_rollups'::regclass, true)`, NOT from
-- db/schema/065 — the files on disk go stale and rebuilding from one already
-- reinstated a fixed overtime bug once (006 vs 024, caught during 030). All 32
-- existing columns keep their name, type and ORDER, because CREATE OR REPLACE
-- VIEW cannot change any of the three. The two new ones are appended.
--
-- Both are NULL-safe sums of (other_costs + sales_tax) and on all 90 existing
-- rows both columns are NULL, so both new rollups are 0.00 everywhere today.
-- Nothing on any job moves until an estimate is written with the new boxes.
--
-- ⚠⚠ THE VIEW IS ONLY HALF THE JOB. `projectedEstimatedTotalCost` is summed in
-- JAVASCRIPT, in two mappers in netlify/functions/airtable.js, and only ONE of
-- them gains the term:
--
--   mapJobFromNeon (~5726)  reads this view          → three-term sum, uses the
--                                                      FILTERED est_other_cost_rollup
--   mapJob         (~5491)  reads AIRTABLE fields    → UNCHANGED, two-term sum
--
-- Airtable never had a column for bought-in cost and is not gaining one, so the
-- Airtable mapper can only answer with what it can read. It still emits the KEY
-- as NULL, because these two mappers are held to "same keys or a field silently
-- disappears" — and NULL there means "cannot know", not "zero".
--
-- The FILTERED/unfiltered choice is the trap CLAUDE.md documents: the Est. GP
-- card has always ignored Draft and Rejected estimates, so `proj_est_other_cost`
-- (unfiltered) is the wrong twin for that card even though its name sounds
-- right. It exists for parity with the other proj_* columns.
CREATE OR REPLACE VIEW v_job_rollups AS
 SELECT id,
    airtable_id,
    po,
    COALESCE(( SELECT sum(e.actual_estimate_sent) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS expected_revenue,
    COALESCE(( SELECT sum(e.actual_estimate_sent) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id), 0::numeric)::numeric(14,2) AS expected_revenue_all_status,
    COALESCE(( SELECT sum(e.actual_estimate_sent) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND e.status = 'Approved'::text AND e.estimate_type = 'Original'::text), 0::numeric)::numeric(14,2) AS base_contract_amount,
    COALESCE(( SELECT sum(e.estimated_labor_hours) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_labor_hours_rollup,
    COALESCE(( SELECT sum(e.estimated_labor_cost) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_labor_cost_rollup,
    COALESCE(( SELECT sum(COALESCE(e.material_raw_cost, e.estimated_material_cost)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_material_cost_rollup,
    COALESCE(( SELECT sum(e.estimated_labor_hours) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id), 0::numeric)::numeric(14,2) AS proj_est_labor_hours,
    COALESCE(( SELECT sum(e.estimated_labor_cost) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id), 0::numeric)::numeric(14,2) AS proj_est_labor_cost,
    COALESCE(( SELECT sum(COALESCE(e.material_raw_cost, e.estimated_material_cost)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id), 0::numeric)::numeric(14,2) AS proj_est_material_cost,
    COALESCE(( SELECT count(*) AS count
           FROM job_estimates e
          WHERE e.job_id = j.id AND e.status = 'Approved'::text), 0::bigint)::integer AS approved_estimates,
    COALESCE(( SELECT sum(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric)) AS sum
           FROM expenses x
          WHERE x.job_id = j.id AND x.expense_type = 'Materials'::text), 0::numeric)::numeric(14,2) AS actual_material_cost,
    COALESCE(( SELECT sum(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric)) AS sum
           FROM expenses x
          WHERE x.job_id = j.id AND x.expense_type = 'Subcontract'::text), 0::numeric)::numeric(14,2) AS actual_subcontract_expense,
    COALESCE(( SELECT sum(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric)) AS sum
           FROM expenses x
          WHERE x.job_id = j.id AND x.expense_type = 'Scissor Lift'::text), 0::numeric)::numeric(14,2) AS actual_scissor_lift_expense,
    COALESCE(( SELECT sum(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric)) AS sum
           FROM expenses x
          WHERE x.job_id = j.id AND x.expense_type = 'Rental Equipment'::text), 0::numeric)::numeric(14,2) AS actual_rental_equipment_expense,
    COALESCE(( SELECT sum(
                CASE
                    WHEN x.reviewed THEN COALESCE(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric))
                    ELSE 0::numeric
                END) AS sum
           FROM expenses x
          WHERE x.job_id = j.id), 0::numeric)::numeric(14,2) AS reviewed_expenses_rollup,
    COALESCE(( SELECT sum(COALESCE(x.legacy_material_cost, x.manual_material_cost, 0::numeric) - COALESCE(x.material_credit, 0::numeric)) AS sum
           FROM expenses x
          WHERE x.job_id = j.id), 0::numeric)::numeric(14,2) AS total_actual_expenses_audit,
    COALESCE(( SELECT sum(i.snapshot_total) AS sum
           FROM invoices i
          WHERE i.job_id = j.id AND i.invoice_type = 'Contract'::text), 0::numeric)::numeric(14,2) AS total_contract_billed,
    COALESCE(( SELECT sum(w.total_wire_cost) AS sum
           FROM wire_weigh_ins w
          WHERE w.job_id = j.id), 0::numeric)::numeric(14,2) AS total_wire_cost,
    COALESCE(( SELECT sum(w.reviewed_wire_cost) AS sum
           FROM wire_weigh_ins w
          WHERE w.job_id = j.id), 0::numeric)::numeric(14,2) AS reviewed_wire_cost_rollup,
    COALESCE(( SELECT sum(p.total_pipe_cost) AS sum
           FROM pipe_usage p
          WHERE p.job_id = j.id), 0::numeric)::numeric(14,2) AS pipe_cost,
    COALESCE(( SELECT sum(p.pipe_cost_reviewed) AS sum
           FROM pipe_usage p
          WHERE p.job_id = j.id), 0::numeric)::numeric(14,2) AS pipe_cost_reviewed,
    COALESCE(( SELECT sum(c.allocated_labor_cost) AS sum
           FROM v_job_labor_cost c
          WHERE c.job_id = j.id AND c.reviewed), 0::numeric)::numeric(14,2) AS actual_labor_cost_reviewed,
    COALESCE(( SELECT sum(c.allocated_labor_cost) AS sum
           FROM v_job_labor_cost c
          WHERE c.job_id = j.id AND NOT c.reviewed), 0::numeric)::numeric(14,2) AS labor_cost_in_progress,
    COALESCE(( SELECT sum(t.hours) AS sum
           FROM time_entries t
          WHERE t.job_id = j.id), 0::numeric)::numeric(14,2) AS hours_rollup,
    COALESCE(( SELECT sum(b.unbilled_hours) AS sum
           FROM v_time_entry_billing b
             JOIN time_entries t2 ON t2.id = b.id
          WHERE t2.job_id = j.id), 0::numeric)::numeric(14,2) AS unbilled_hours,
    COALESCE(( SELECT sum(b.unbilled_labor_revenue) AS sum
           FROM v_time_entry_billing b
             JOIN time_entries t3 ON t3.id = b.id
          WHERE t3.job_id = j.id), 0::numeric)::numeric(14,2) AS unbilled_labor_revenue_tm,
    COALESCE(( SELECT sum(b.ready_to_invoice_hours) AS sum
           FROM v_time_entry_billing b
             JOIN time_entries t4 ON t4.id = b.id
          WHERE t4.job_id = j.id), 0::numeric)::numeric(14,2) AS unallocated_labor_hours,
    COALESCE(( SELECT sum(COALESCE(e.material_markup, 0::numeric)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_material_markup_rollup,
    COALESCE(( SELECT sum(COALESCE(e.material_raw_cost + COALESCE(e.material_markup, 0::numeric), e.estimated_material_cost)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_material_sell_rollup,
    COALESCE(( SELECT sum(COALESCE(e.estimated_labor_hours, 0::numeric) * COALESCE(e.labor_sell_rate, j.billable_hourly_rate, 75.00)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_labor_sell_rollup,
    COALESCE(( SELECT count(*) AS count
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::bigint)::integer AS est_counted,
    COALESCE(( SELECT count(*) AS count
           FROM job_estimates e
          WHERE e.job_id = j.id AND e.material_raw_cost IS NULL AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::bigint)::integer AS est_legacy_count,
    -- ── APPENDED BY 066 ────────────────────────────────────────────────────
    -- Other costs and sales tax roll up as ONE figure. They are two columns on
    -- the estimate so the tax can be filled and checked independently, but
    -- nothing downstream needs them apart: both are bought-in direct job cost
    -- and both land in the same place in the GP arithmetic.
    COALESCE(( SELECT sum(COALESCE(e.other_costs, 0::numeric) + COALESCE(e.sales_tax, 0::numeric)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::numeric)::numeric(14,2) AS est_other_cost_rollup,
    COALESCE(( SELECT sum(COALESCE(e.other_costs, 0::numeric) + COALESCE(e.sales_tax, 0::numeric)) AS sum
           FROM job_estimates e
          WHERE e.job_id = j.id), 0::numeric)::numeric(14,2) AS proj_est_other_cost
   FROM jobs j;

-- ── AND THE VIEW THE APP ACTUALLY READS ────────────────────────────────────
-- ⚠⚠ ADDING A COLUMN TO `v_job_rollups` DOES NOT PUT IT ON THE JOB SCREEN, and
-- this was a live bug for the first twenty minutes of this change. `handleJobs`
-- and `handleJobById` select from **`v_job_rollups_true`** (airtable.js ~5721),
-- which is a hand-written passthrough that names all 35 of its parent's columns
-- ONE BY ONE. It has no `SELECT *`. So the new columns existed, the rollup was
-- correct, the mapper asked for `r.est_other_cost_rollup` — and got `undefined`,
-- which `n(...) || 0` turns into a clean $0.00.
--
-- Nothing errors. The tile renders "$0.00" and Estimated Direct Cost quietly
-- omits six figures of bought-in cost, which is the exact failure this file was
-- written to fix, reintroduced one view downstream. It was caught by asking
-- which view the handler reads, not by any test — the offline suite cannot see
-- a database and the live suite only exercised the estimate card.
--
-- THE RULE: a new rollup column has to be added to BOTH views, and the passthrough
-- is the one that is easy to forget because its name suggests it inherits.
CREATE OR REPLACE VIEW v_job_rollups_true AS
 SELECT r.id,
    r.airtable_id,
    r.po,
    r.expected_revenue,
    r.expected_revenue_all_status,
    r.base_contract_amount,
    r.est_labor_hours_rollup,
    r.est_labor_cost_rollup,
    r.est_material_cost_rollup,
    r.proj_est_labor_hours,
    r.proj_est_labor_cost,
    r.proj_est_material_cost,
    r.approved_estimates,
    r.actual_material_cost,
    r.actual_subcontract_expense,
    r.actual_scissor_lift_expense,
    r.actual_rental_equipment_expense,
    r.reviewed_expenses_rollup,
    r.total_actual_expenses_audit,
    r.total_contract_billed,
    r.total_wire_cost,
    r.reviewed_wire_cost_rollup,
    r.pipe_cost,
    r.pipe_cost_reviewed,
    COALESCE(t.labor_cost_live, 0::numeric) AS actual_labor_cost_reviewed,
    0::numeric AS labor_cost_in_progress,
    r.hours_rollup,
    r.unbilled_hours,
    r.unbilled_labor_revenue_tm,
    r.unallocated_labor_hours,
    r.est_material_markup_rollup,
    r.est_material_sell_rollup,
    r.est_labor_sell_rollup,
    r.est_counted,
    r.est_legacy_count,
    r.est_other_cost_rollup,
    r.proj_est_other_cost
   FROM v_job_rollups r
     LEFT JOIN v_job_labor_cost_true_by_job t ON t.job_id = r.id;

-- 065_estimate_gp_model.sql — split material COST from material SELL on an
-- estimate, so estimated GP stops being understated by exactly the markup.
--
-- ── THE BUG, MEASURED ──────────────────────────────────────────────────────
-- `estimated_material_cost` has always been whatever the estimator typed, and
-- on marked-up quotes that is the SELLING value. `calculated_estimated_total`
-- then adds labor COST to material SELL and calls the difference gross profit:
--
--   sent 126,000 · hrs 650 · material 77,250 → calc_total 98,375 → GP 21.9%
--   the same estimate, split:  70,250 cost + 7,000 markup → GP 27.5%
--
-- $7,000 of real profit, reported as cost. The error is always in the same
-- direction — GP looks WORSE than it is — which is why it survived five years:
-- nobody chases a number that looks too low.
--
-- ── WHY THE OLD ROWS ARE NOT MIGRATED, WITH THE EVIDENCE ───────────────────
-- The obvious move is to back out the markup from history. It cannot be done.
-- Across the 30 most recent estimates the implied labor sell rate
-- ((sent - material) / hours) ranges from $8.33 to $200.00/hr and only 3 land
-- anywhere near $75. Historic estimates were NOT built as
-- material_sell + hours × sell_rate; the material figure is sometimes cost and
-- sometimes sell, and nothing on the row records which.
--
-- So: NEW COLUMNS, and a legacy row is one where `material_raw_cost IS NULL`.
-- Legacy rows keep TODAY'S arithmetic exactly — their material figure is read
-- as cost, their markup as zero — so no job's GP moves because of this file.
-- Only estimates written from here on change anything. `est_legacy_count`
-- below exists so the screen can SAY so rather than blending the two silently.
--
-- ── WHAT THIS FILE DELIBERATELY DOES NOT TOUCH ─────────────────────────────
-- `v_job_financials` / `v_job_financials_true` carry `est_material_cost_rollup`
-- and `est_labor_cost_rollup` through their CTE chain but never read them —
-- checked column by column against the final SELECT of both. Live GP and final
-- GP are computed from ACTUALS (`actual_job_cost_cogs`), which already treat
-- material at cost and add markup on the revenue side. The estimate side was
-- the only one doing it backwards. Nothing in this file moves a closed job.

-- ── THE FIVE NEW FACTS ON AN ESTIMATE ──────────────────────────────────────
-- Every one is NULLABLE and every one is NULL on all 90 existing rows. NULL is
-- not "zero", it is "this estimate predates the split" — the reads below rely
-- on being able to tell those apart.
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS material_raw_cost  numeric(14,2);
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS material_markup    numeric(14,2);
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS labor_sell_rate    numeric(10,2);
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS labor_burden_rate  numeric(10,2);
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS price_adjustment   numeric(14,2);

-- Stored, not derived, for the same reason `calculated_estimated_total` is: the
-- create path and the partial-update path must not be able to drift, so the
-- arithmetic lives in ONE SQL fragment that both call (`sqlEstSellingPrice`).
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS calculated_selling_price numeric(14,2);

COMMENT ON COLUMN job_estimates.material_raw_cost IS
  'Estimated material cost BEFORE markup. NULL = legacy row, predates the split — read estimated_material_cost as the cost basis instead.';
COMMENT ON COLUMN job_estimates.material_markup IS
  'Markup dollars added to material_raw_cost. Defaults in the UI to raw x jobs.markup_pct (10% on every job today) so estimated and actual material use the same markup model, but is stored as DOLLARS because that is how an estimator prices.';
COMMENT ON COLUMN job_estimates.labor_sell_rate IS
  'What labor is CHARGED at, $/hr. Snapshotted per estimate so a later company rate change cannot rewrite an old quote. Resolved at create from jobs.billable_hourly_rate, falling back to 75.00.';
COMMENT ON COLUMN job_estimates.labor_burden_rate IS
  'What labor COSTS, $/hr. Snapshotted for the same reason. NULL falls back to the 32.50 company estimating rate. ⚠ docs/PLAN-prevailing-wage.md is the project that makes this per-job rather than company-wide — this column is where its resolver lands.';
COMMENT ON COLUMN job_estimates.estimated_material_cost IS
  'THE MATERIAL FIGURE AS ENTERED, and its meaning depends on the row. Native rows written after 2026-08-25: material SELL (= material_raw_cost + material_markup). Rows before it: unknowable — sometimes cost, sometimes sell. Never read this as a cost basis without COALESCE(material_raw_cost, ...) in front of it.';
COMMENT ON COLUMN job_estimates.calculated_estimated_total IS
  'Estimated DIRECT COST — labor cost + material cost basis. Named "total" because Airtable named it that; it has never been a price. The price twin is calculated_selling_price.';
COMMENT ON COLUMN job_estimates.calculated_selling_price IS
  'material sell + labor sell. What the quote WOULD be at the entered rates. actual_estimate_sent is what was really quoted and may deliberately differ.';

-- ⚠ A markup with no raw cost is the one shape that silently double-counts:
-- the cost basis would fall back to `estimated_material_cost` (the SELL figure)
-- while the markup is also reported as profit, so the same dollars appear on
-- both sides. Refuse it in the database rather than trusting 3 write paths.
ALTER TABLE job_estimates DROP CONSTRAINT IF EXISTS job_estimates_markup_needs_raw;
ALTER TABLE job_estimates ADD CONSTRAINT job_estimates_markup_needs_raw
  CHECK (material_markup IS NULL OR material_raw_cost IS NOT NULL);

-- ── TEMPLATE DEFAULTS ──────────────────────────────────────────────────────
-- A template seeds a blank form; the estimator edits every figure afterwards.
-- ⚠ `default_material_cost` now seeds the RAW cost box. The five existing
-- template rows were written under the old ambiguity, so whoever next opens the
-- template manager should check them — but nothing recalculates off a template,
-- so a stale default costs a keystroke, not a number.
ALTER TABLE estimate_templates ADD COLUMN IF NOT EXISTS default_material_markup numeric(14,2);
ALTER TABLE estimate_templates ADD COLUMN IF NOT EXISTS default_labor_sell_rate numeric(10,2);

COMMENT ON COLUMN estimate_templates.default_material_cost IS
  'Seeds the RAW material cost box (before markup) on a new estimate.';
COMMENT ON COLUMN estimate_templates.default_labor_sell_rate IS
  'Overrides the job billable rate when seeding an estimate. NULL = use the job rate, which is what almost every template should do.';

-- ── v_job_rollups ──────────────────────────────────────────────────────────
-- Rewritten from pg_get_viewdef, NOT from db/schema/005 — the file on disk is
-- stale by several revisions and rebuilding from it once reinstated a fixed
-- overtime bug (see 006 vs 024). All 30 existing columns keep their name, type
-- and ORDER because CREATE OR REPLACE cannot change any of the three; the new
-- ones are appended.
--
-- ONE existing column changes meaning, and it is the whole point of the file:
--
--   est_material_cost_rollup / proj_est_material_cost
--     were SUM(estimated_material_cost)                    ← the figure as typed
--     are  SUM(COALESCE(material_raw_cost, estimated_material_cost))
--
-- On all 90 rows that exist today `material_raw_cost` is NULL, so both sums are
-- byte-identical to what they were this morning. The change only bites on rows
-- written by the new form — which is exactly the intent.
--
-- ⚠ BOTH TWINS CHANGE TOGETHER. The filtered/unfiltered pair is already the
-- most counterintuitive naming in this schema (CLAUDE.md documents it, and
-- mapJobFromNeon carries a scar from getting it backwards); leaving one on a
-- cost basis and one on a sell basis would make it genuinely unreadable.
--
-- The four SELL columns are appended rather than folded into the cost ones
-- because a job's Est. GP screen has to show both sides at once — what material
-- costs AND what it is being sold for — and computing the second from the first
-- in JS is how the two drift.
--
-- ⚠ THE SELL COLUMNS INFER FOR LEGACY ROWS, and `est_legacy_count` is how the
-- screen admits it. A pre-split estimate has no labor_sell_rate, so its labor
-- sell falls back to the JOB's current billable rate — a reasonable guess, not
-- a record of what was quoted. Its markup reads as $0 and its material sell as
-- the figure typed. Those inferences are safe for the cost/GP columns (they
-- reproduce today's numbers exactly) and are a guess for the sell columns. Show
-- the count next to them or the screen is quietly lying about old quotes.
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
          WHERE e.job_id = j.id AND e.material_raw_cost IS NULL
            AND (e.status = ANY (ARRAY['Archived/Completed'::text, 'Sent'::text, 'Approved'::text]))), 0::bigint)::integer AS est_legacy_count
   FROM jobs j;

-- Straight passthrough — the true view exists only to swap the labor-cost
-- source, and it names every column explicitly, so the five new ones are
-- invisible to mapJobFromNeon until they are listed here too.
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
    r.est_legacy_count
   FROM v_job_rollups r
     LEFT JOIN v_job_labor_cost_true_by_job t ON t.job_id = r.id;

-- 067_estimate_tax_rate.sql — record WHICH tax rate produced the tax dollars.
--
-- ── WHY ────────────────────────────────────────────────────────────────────
-- db/schema/066 gave an estimate a `sales_tax` column in DOLLARS and the UI a
-- "fill 8%" button. 8% was a constant in the source, so a job in a county at
-- 7.25% meant working 165,323.13 x 0.0725 out on a calculator and typing the
-- result — and the estimate then held $11,985.93 with nothing on it saying
-- where that came from. Owner's call 2026-08-28: make the rate an input, and
-- store it.
--
-- ⚠ THE DOLLARS STAY AUTHORITATIVE. THIS COLUMN IS A RECORD, NOT AN INPUT.
-- Nothing derives tax from rate x base — not the GP arithmetic, not the
-- rollups, not the read. `sales_tax` is what is owed and `tax_rate_pct` says
-- how it was arrived at. That separation is deliberate and matches how
-- `material_markup` was built in 065 ("stored as DOLLARS because that is how an
-- estimator prices, and a stored % would fight the override"): a job can be
-- part-exempt, part-resale, or split across two counties, and in every one of
-- those cases the dollars are right and no single rate explains them. If
-- anything ever computes tax FROM this column, that override silently stops
-- working and the estimator's correction is overwritten by a formula.
--
-- Stored as a FRACTION (0.0725), matching `jobs.markup_pct`, which the same
-- screen already renders as `* 100`. Two percent conventions on one card is how
-- an 8 ends up meaning 0.08% of what it should.
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS tax_rate_pct numeric(6,4);

COMMENT ON COLUMN job_estimates.tax_rate_pct IS
  'The sales-tax rate used to arrive at sales_tax, as a FRACTION (0.0725 = 7.25%), matching jobs.markup_pct. A RECORD, never an input: sales_tax holds the authoritative dollars and nothing derives them from this. NULL = the dollars were entered directly, or the row predates 2026-08-28.';

-- ⚠ A rate outside 0–100% is a units mistake, not a tax policy. The two that
-- actually happen are typing 725 for 7.25% (the box takes a percent and divides
-- by 100, so 725 becomes a 725% rate) and typing 0.0725 into that same percent
-- box (which becomes 0.000725, quietly billing 0.07% of the purchase). The
-- upper end is caught here; the low end cannot be — 0.000725 is a legal number —
-- so the card carries a separate "that tax looks too small for this base" hint.
ALTER TABLE job_estimates DROP CONSTRAINT IF EXISTS job_estimates_tax_rate_sane;
ALTER TABLE job_estimates ADD CONSTRAINT job_estimates_tax_rate_sane
  CHECK (tax_rate_pct IS NULL OR (tax_rate_pct >= 0 AND tax_rate_pct <= 1));

-- ── NO VIEW CHANGES, AND THAT IS NOT AN OVERSIGHT ──────────────────────────
-- 066 had to touch three explicit column lists (v_job_rollups →
-- v_job_rollups_true → JOB_SELECT) because it added a column that ROLLS UP.
-- This one does not: a rate is per-estimate metadata read back through
-- `jobEstimates`, and summing rates across estimates would be meaningless.
-- Nothing in the GP arithmetic changes, so no job's numbers move.

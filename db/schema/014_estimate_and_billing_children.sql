-- ── The three missing child tables (roadmap Step 4e groundwork) ────────────
-- Added 2026-08-07. These are not a slice of their own — they are the tables
-- that BLOCK the estimate and invoice slices, and the reason 4d had to leave
-- `billed_material_amount` as an ETL-copied value.
--
-- Found by auditing before building, three slices running:
--   * material_billing_allocations — blocks BOTH the invoice material rollups
--     AND expenses.billed_material_amount (deferred at 4d for exactly this).
--   * sent_estimate_pdfs — handleJobEstimates joins it to attach `snapshot` to
--     each estimate. Flipping that read without this table would SILENTLY drop
--     every snapshot, which is what the frontend's "+ Add as Line" reads.
--   * estimate_templates — handleEstimateTemplates has no Neon source at all.
--
-- All three are small (25 / 5 / 252 rows) and almost entirely STORED fields.
-- The expensive part of 4e was never these; it was not knowing they were absent.

-- ── Sent Estimate PDFs ─────────────────────────────────────────────────────
-- A point-in-time record of what was actually sent to the customer. `snapshot`
-- is the customer-facing scope text as JSON — the master Job Estimates record
-- never receives it, which is why handleJobEstimates has to join across.
--
-- Both parent links are NULLABLE and that is not defensive padding: 5 of 25
-- rows have no `Job Estimate` back-link, which is exactly why the read carries
-- a fallback cascade (back-link first, then most-recent same-job PDF whose
-- Total equals the master's Actual Estimate Sent).
CREATE TABLE IF NOT EXISTS sent_estimate_pdfs (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id         text UNIQUE,
  job_airtable_id     text,
  job_id              uuid REFERENCES jobs(id),
  estimate_airtable_id text,
  estimate_id         uuid REFERENCES job_estimates(id) ON DELETE SET NULL,
  display_number      int,
  estimate_date       date,
  total               numeric(14,2),
  snapshot            text,
  synced_at           timestamptz
);

CREATE INDEX IF NOT EXISTS sent_estimate_pdfs_job      ON sent_estimate_pdfs (job_airtable_id);
CREATE INDEX IF NOT EXISTS sent_estimate_pdfs_estimate ON sent_estimate_pdfs (estimate_airtable_id);

-- ── Estimate Templates ─────────────────────────────────────────────────────
-- Reference data for the estimate builder. Three rich-text bodies (scope,
-- exclusions, terms) stored as text — Airtable's richText is markdown-ish and
-- the app renders it as-is, so nothing is lost by not modelling it.
CREATE TABLE IF NOT EXISTS estimate_templates (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id           text UNIQUE,
  template_name         text NOT NULL,
  contractor_airtable_id text,
  active                boolean NOT NULL DEFAULT true,
  scope_of_work         text,
  exclusions            text,
  standard_terms        text,
  base_price            numeric(14,2),
  default_labor_hours   numeric(10,2),
  default_material_cost numeric(14,2),
  internal_notes        text,
  synced_at             timestamptz
);

-- ── Material Billing Allocations ───────────────────────────────────────────
-- The join that makes invoice revenue computable: which EXPENSE was billed on
-- which INVOICE, and for how much. Everything else on the Airtable table is a
-- lookup or formula derived from those three facts.
--
-- ⚠ THIS TABLE IS WHY TWO OTHER THINGS WERE STUCK:
--   * Invoices.`Invoice Material Amount` and `Material Credits` are ROLLUPS over
--     it, so invoice totals cannot be computed in Neon without it.
--   * expenses.`billed_material_amount` is the same rollup seen from the expense
--     side. 4d left it ETL-copied and called it invoice-side work; this is that
--     work.
--
-- Mirrors labor_billing_allocations, which already exists and has the same
-- shape for the labor half.
--
-- Invoice link is NULLABLE: 88 of 252 rows carry no invoice, i.e. material that
-- has been allocated but not yet billed. That is the whole point of "unbilled".
CREATE TABLE IF NOT EXISTS material_billing_allocations (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id         text UNIQUE,
  expense_airtable_id text,
  expense_id          uuid REFERENCES expenses(id) ON DELETE CASCADE,
  invoice_airtable_id text,
  invoice_id          uuid REFERENCES invoices(id) ON DELETE SET NULL,
  allocated_amount    numeric(14,2),
  synced_at           timestamptz
);

CREATE INDEX IF NOT EXISTS mba_expense ON material_billing_allocations (expense_airtable_id);
CREATE INDEX IF NOT EXISTS mba_invoice ON material_billing_allocations (invoice_airtable_id);

-- ── Why no derived view yet ────────────────────────────────────────────────
-- The rollups these unblock (Invoice Material Amount, Material Credits,
-- expenses.billed_material_amount) are deliberately NOT computed here. They get
-- the same treatment as 013: ported, then diffed row by row against Airtable
-- BEFORE anything reads them. Landing the tables and the diff in one commit
-- would mean shipping revenue arithmetic that nothing has checked.

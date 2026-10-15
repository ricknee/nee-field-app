-- 084_vendor_statement_amount.sql — each statement's amount, for a per-vendor
-- "total to date" (owner, 2026-10-15: "sum of each month's statement added
-- together for a total to date").
--
-- ⛔ Still reference only. This number is DISPLAYED on the Statements screen
-- and goes nowhere else — not an expense, not a job, not a GP view.
--
-- Where it comes from (amount_source):
--   'bot'    — the MiniBee sent it with the PDF (`amount` on intake)
--   'pdf'    — the Statements screen read it out of the PDF's text layer
--   'manual' — a person typed it
-- A 'pdf' reading never overwrites 'bot' or 'manual' (the WHERE clause in
-- setVendorStatementAmount carries that), so a correction by hand sticks.
-- amount_evidence is the line of text the PDF reading came from, shown on the
-- screen so a wrong pick is visible instead of silently summed.

ALTER TABLE vendor_statements ADD COLUMN IF NOT EXISTS amount numeric;
ALTER TABLE vendor_statements ADD COLUMN IF NOT EXISTS amount_source text
  CHECK (amount_source IN ('bot', 'pdf', 'manual'));
ALTER TABLE vendor_statements ADD COLUMN IF NOT EXISTS amount_evidence text;
ALTER TABLE vendor_statements ADD COLUMN IF NOT EXISTS amount_set_by text;
ALTER TABLE vendor_statements ADD COLUMN IF NOT EXISTS amount_set_at timestamptz;

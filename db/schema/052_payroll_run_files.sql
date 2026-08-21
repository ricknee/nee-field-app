-- 052_payroll_run_files.sql — the payroll archive stops depending on Airtable.
--
-- `payrollRunsList` is the ONE read in the field app that could not be flipped
-- to Neon. Everything on the grid was already here — pay period, hours, bonus
-- total, superseded flag — except the two things that only Airtable had:
--
--   1. THE PDF. It lives as an Airtable ATTACHMENT, and an attachment url is
--      short-lived and signed. There is no stable link to copy into Neon; the
--      only way to hand someone a working link is to ask Airtable for a fresh
--      one at read time. So the read could not move until the FILE moved.
--   2. "Superseded By". Airtable stores the link the other way round
--      (`Supersedes`, on the newer run), and the list resolves it to show
--      "replaced on <date>". Neon had `superseded` as a bare boolean with no
--      record of WHICH run replaced it.
--
-- This adds both. The files go to R2 under `payroll/<run uuid>/`, the same
-- store as jobsite photos, prints, receipts and estimate PDFs.
--
-- ⚠ THE KEY IS STORED, NOT DERIVED. Photos and receipts find their objects by
-- LISTING a prefix, which is right when a record can have many files and their
-- names are user-supplied. A payroll run has exactly one PDF and one JSON, and
-- the grid shows 28 runs at once — deriving would mean listing 28 prefixes on
-- every page load, or one big list and a client-side group-by, to answer a
-- question a column answers for free.
--
-- ⚠ `supersedes_id` is a SELF-REFERENCE and points from the NEW run to the OLD
-- one, matching Airtable's direction. The grid wants the opposite ("what
-- replaced this?"), which is a LEFT JOIN on `s.supersedes_id = r.id` — cheap,
-- and it cannot drift the way a stored back-pointer could.
--
-- ⚠ NOT a replacement for the Airtable attachment yet. The write path uploads
-- to BOTH until the mirror writes come out (audit item 10). A payroll PDF is
-- the artifact people are paid from; it gets two homes until one is proven.

ALTER TABLE payroll_runs
  ADD COLUMN IF NOT EXISTS pdf_key       text,
  ADD COLUMN IF NOT EXISTS json_key      text,
  ADD COLUMN IF NOT EXISTS supersedes_id uuid REFERENCES payroll_runs(id);

-- The grid's own query: find the run that replaced this one.
CREATE INDEX IF NOT EXISTS payroll_runs_supersedes ON payroll_runs (supersedes_id);

COMMENT ON COLUMN payroll_runs.pdf_key IS
  'R2 object key for the run PDF, payroll/<run uuid>/<stamp>.pdf. NULL means the file has not been copied out of Airtable yet — the list falls back to the Airtable attachment url in that case.';
COMMENT ON COLUMN payroll_runs.json_key IS
  'R2 object key for the machine-readable payload that accompanies the PDF.';
COMMENT ON COLUMN payroll_runs.supersedes_id IS
  'The OLDER run this one replaces (same direction as Airtable Supersedes). To find what replaced a run, join the other way: s.supersedes_id = r.id.';

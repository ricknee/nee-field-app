-- Neon slice 4 — Jobs MASTER DATA (descriptive fields only).
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-07-31.
--
-- Same convention as 001/002: applied BARE via the Neon MCP (which mangles inline
-- SQL comments), with the reasoning kept here. Keep the two in sync by hand.
--
-- ── WHAT THIS DELIBERATELY DOES NOT INCLUDE ────────────────────────────────
-- The Airtable Jobs table has **165 distinct fields**. This migrates roughly 30 of
-- them — the descriptive spine. The other groups are out of scope, for reasons:
--
--   ~40 FINANCIAL ROLLUPS  — Gross Profit (Live/Final/Projected), Expected Revenue,
--     Actual Job Cost (COGS), Labor Revenue (T&M), Unbilled Hours, Material Cost...
--     These roll up from ESTIMATES, INVOICES, EXPENSES and LABOR ALLOCATIONS, none
--     of which exist in Neon. They cannot be computed here yet, and copying their
--     current VALUES would produce numbers that silently go stale. They come last,
--     with the labor-billing slice, under the hard constraint recorded on the
--     unify-estimates bet: port and PROVE every GP formula before retiring anything
--     Airtable-side.
--   ~25 LINKS — contractor, contacts, estimates, invoices, inspections, schedule.
--   ~25 EXTERNAL REFS — 18 pCloud folder ids, Trello card ids, automation flags.
--
-- ── NEVER WRITE THESE FROM THE APP ─────────────────────────────────────────
-- Google Contact ID, Sync Status, Last Synced At, Needs Sync to Google are owned by
-- the Make.com automation layer (see CLAUDE.md). They are not mirrored here at all,
-- so there is nothing to accidentally write back.

-- Identity. `po` is 100% populated across all 109 jobs; `po_locked` only 77%.
-- The QB puller matches jobcodes against po_locked, so the 25 jobs without one can
-- never match — harmless in practice, because every one of them is New Lead /
-- Not Awarded / Completed with no TSheets Job ID, i.e. nobody has logged time to it.
-- Verified 2026-07-31: where both exist they agree on 82 of 84; the 2 exceptions are
-- typo variants ("Jeanie" vs "Jeannie", MEC 389 vs MEC 398).
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS po             text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS po_number      integer;

-- QuickBooks Time's own jobcode id, on 60% of jobs. A far stronger key for the
-- puller's job resolution than matching po_locked as a STRING — adopt it there when
-- coverage allows, keeping the string match as the fallback.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tsheets_job_id text;

-- Classification — all 100% populated.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS status         text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_type       text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS job_year       integer;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS billing_method text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS billing_ready  text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS tax_status     text;

-- Dates. Sparse on purpose — start/finish are set on only ~6% of jobs and
-- project_completed_at on 41%. Carried anyway: cheap, and absence is meaningful.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS start_date           date;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS finish_date          date;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS project_completed_at date;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS bird_date            date;

-- Address. `address_full` is the composed one the app links to Maps and is 100%
-- populated; the split intake fields are ~92%. Both are kept — the full string is
-- what mileage was calculated against, so it is the historical record.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS address_full    text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS address_street  text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS address_city    text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS address_state   text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS address_zip     text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS miles_from_shop numeric(10,2);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS customer_first_name text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS customer_last_name  text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS customer_email      text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS customer_phone      text;

-- Contractor as TEXT, not a link. Same rule as time_entries.job_name: the text
-- snapshot is the history and survives the linked record being archived.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS contractor_code text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS contractor_name text;

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS notes              text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS meter_number       text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS work_order_number  text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS email_alias        text;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS power_company      text;

-- Stored as the fraction Airtable holds (0.1 = 10%), NOT converted to a percentage.
-- Converting here would mean two representations of the same number in two systems.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS markup_pct numeric(10,4);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS generator_installed     boolean;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS inspection_not_required boolean;

-- When the ETL last refreshed this row from Airtable. Airtable remains the source of
-- truth for Jobs — nothing writes back — so a stale row is the only failure mode,
-- and this makes staleness visible.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS synced_at timestamptz;

CREATE INDEX IF NOT EXISTS jobs_po_idx     ON jobs (po);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs (status);
-- Partial unique: 40% of jobs have no TSheets id, and NULLs must not collide.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_tsheets_job_id_idx
  ON jobs (tsheets_job_id) WHERE tsheets_job_id IS NOT NULL;

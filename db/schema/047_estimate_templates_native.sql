-- 047_estimate_templates_native.sql — estimate templates become app-editable
--
-- ── WHY ────────────────────────────────────────────────────────────────────
-- `estimate_templates` has been Neon-first on the READ side since 014, but
-- there has never been a write path anywhere in either app. Templates were
-- typed into Airtable by hand and carried across hourly by `_billing-sync.js`.
-- With nobody opening Airtable any more that is a dead end: the base prices in
-- the New Job Estimate modal cannot be corrected without leaving the app.
--
-- This file is step 1 of making the table natively writable. It does NOT flip
-- anything on its own — the handlers and the ETL removal are separate.
--
-- ⚠⚠ THE ORDER MATTERS, AND IT IS NOT THE OBVIOUS ONE. `_billing-sync.js`
-- upserts this table `ON CONFLICT (airtable_id) DO UPDATE` every hour inside
-- qb-time-pull. Until that block is deleted, ANY edit made in the app to a row
-- that still has an airtable_id is reverted at the top of the hour, silently
-- and with no error anywhere. So:
--
--   1. this file (columns exist, nothing reads them yet)
--   2. the write handlers ship; edits work but revert hourly
--   3. ONLY THEN the templates block leaves `_billing-sync.js`
--
-- Shipping 2 and 3 together would mean a bug in the write path leaves the
-- table frozen with neither an editor nor an ETL. Two pushes, in that order.

-- ── The dual handle, again ─────────────────────────────────────────────────
-- Same rule as items (041) and locations/vendors (042), enforced by 043: a
-- natively-created row has `airtable_id IS NULL` and is addressed by its uuid,
-- so every reader serves COALESCE(airtable_id, id::text) and every writer
-- resolves on `airtable_id = $1 OR id::text = $1`.
--
-- Nothing to add here — `id` and `airtable_id` both already exist, and the
-- UNIQUE index on airtable_id tolerates many NULLs in Postgres. The handle is
-- built in the query rather than stored, because unlike v_stock_levels there is
-- no view in between to put it in.

ALTER TABLE estimate_templates ADD COLUMN IF NOT EXISTS created_at  timestamptz NOT NULL DEFAULT now();
ALTER TABLE estimate_templates ADD COLUMN IF NOT EXISTS updated_at  timestamptz;
ALTER TABLE estimate_templates ADD COLUMN IF NOT EXISTS updated_by  text;
ALTER TABLE estimate_templates ADD COLUMN IF NOT EXISTS sort_order  int;

-- ⚠ `created_at` defaults to now() for the five rows that predate this file, so
-- their created_at is the migration date, not the date they were written in
-- Airtable. That is accepted: Airtable's createdTime was never carried into
-- this table, so there is no truer value available to backfill from.

-- ── contractor_name stops being the source of truth ────────────────────────
-- The column is populated by `_billing-sync.js`, which resolves the Contractor
-- link against a full fetch of the Companies table on every run. Once that
-- block is gone (step 3 above) the copy freezes, and a contractor renamed in
-- Companies would orphan its templates from the picker forever — the filter
-- matches on the NAME, because the frontend passes `job.contractor` as a name.
--
-- Companies IS migrated (item 06, slice 3), so the fix is a live join:
--   LEFT JOIN companies c ON c.airtable_id = t.contractor_airtable_id
--   ... lower(coalesce(c.name, t.contractor_name)) = lower($1)
--
-- contractor_name is KEPT as the coalesce fallback rather than dropped. Two of
-- the five rows would still resolve without it, but a template whose
-- contractor_airtable_id points at a company that has since been deleted has
-- nothing else left to identify it by, and silently vanishing from the picker
-- is worse than a stale string. New writes populate BOTH.
CREATE INDEX IF NOT EXISTS estimate_templates_contractor
  ON estimate_templates (contractor_airtable_id);

-- ── Blank contractor now means "every job" ─────────────────────────────────
-- Owner's call, 2026-08-20. Today a NULL contractor only surfaces on jobs that
-- ALSO have no contractor, which makes a genuinely generic template impossible
-- to build — "Commercial Bid — General" had to be pinned to Classical
-- Construction just to be reachable. After this, NULL = shows on every job,
-- sorted BELOW that job's own contractor-specific templates.
--
-- No data change: none of the five existing rows has a NULL contractor, so the
-- live picker is byte-identical until somebody creates a general template.

-- ── Provenance survives the uuid ───────────────────────────────────────────
-- `handleCreateJobEstimate` records which template seeded an estimate by
-- writing the Airtable link field `Source Template` (fldrni1Lkpw7tMBq8) — but
-- only `if (sourceTemplateId.startsWith("rec"))`. A natively-created template
-- has a uuid, so that guard silently drops the link, and `job_estimates` has no
-- column of its own to fall back on. Provenance would be lost in BOTH stores
-- the first time somebody used a template they made themselves.
--
-- Stores the HANDLE (rec id or uuid), not a uuid FK: the estimate may point at
-- an Airtable-era template, and a FK would also block archiving a template that
-- has been used. Templates snapshot their values into the estimate at create
-- time, so this is a breadcrumb, never a join for money.
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS source_template_handle text;

CREATE INDEX IF NOT EXISTS job_estimates_source_template
  ON job_estimates (source_template_handle)
  WHERE source_template_handle IS NOT NULL;

-- ⚠ NOT backfilled from Airtable's Source Template link. That link exists on
-- an unknown number of historical estimates and reading it would need a full
-- Job Estimates fetch for a breadcrumb nothing renders yet. If the estimate
-- detail ever shows "created from <template>", backfill it then.

-- ── No unique constraint on the name, deliberately ─────────────────────────
-- Two templates called "Case Farms — 2 Barn Setup" would be confusing, but the
-- live data has "Case Farms — 2 Barn Setup" and "Case Farms — 3 Barn Setup" as
-- separate legitimate rows, and a contractor may well want a 2026 and a 2027
-- version of the same name side by side. The save handler warns with a 409 and
-- the existing id (the `handleCreateCompany` / `handleCreateVendor` shape, which
-- the client already knows how to read via err.existingId) and lets the user
-- decide. A constraint here would turn a judgement call into a hard failure.

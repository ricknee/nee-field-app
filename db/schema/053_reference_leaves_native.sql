-- 053_reference_leaves_native.sql — identity cutover, slice 1.
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- `airtable_id NOT NULL` is a formal statement that a table cannot hold a row
-- Airtable has never seen. These three are the first to lose it.
--
-- ── WHAT THIS BUYS, PRECISELY ──────────────────────────────────────────────
-- Not "Airtable is gone". These tables keep their mirror, because job creation
-- still writes Airtable's `Contractor` LINKED-RECORD field and a company with no
-- rec id cannot be linked. What it buys is that **a create no longer fails when
-- Airtable does**: today `handleCreateCompany` throws on the POST and the
-- company exists nowhere, so the picker that asked for it comes back empty.
-- After this the row exists in Neon, is usable everywhere, and the mirror is
-- best-effort.
--
-- ⚠⚠ COMPANIES ARE NOT A LEAF, WHICH IS WHY THE MIRROR STAYS.
-- Leaf-first by Neon foreign key is NOT the same as leaf-first by Airtable LINK
-- FIELD. `createJobRecord` posts `Contractor: ["rec…"]`; a uuid there 422s the
-- whole job create. So companies keep minting rec ids until jobs go native in
-- slice 6. The one case where a company has no rec id is "Airtable was down when
-- it was created" — and in that same state job creation is already impossible,
-- because it POSTs to Airtable too. So this adds no new failure mode.
--
-- ⚠⚠ NO GENERATED `handle` COLUMN — the slice-0 recommendation was WRONG and is
-- corrected here. A column of `COALESCE(airtable_id, id::text)` holds ONE value,
-- so the moment a best-effort mirror succeeds and stamps `airtable_id`, the
-- handle FLIPS from uuid to rec id and every client holding the uuid can no
-- longer find the row. That is the exact "saves fine, then cannot be found
-- again" bug the inventory cutover hit three times.
-- The correct form accepts EITHER, permanently, and it is already proven in
-- inventory.js:
--
--     WHERE airtable_id = $1 OR id::text = $1
--
-- A generated handle is only safe on a table whose rows are native FOREVER.
-- None of these are, yet.
--
-- ⚠ Reads must stop filtering natives out. `handleListContractors` selected
-- `WHERE airtable_id IS NOT NULL`, which after this change would have made a
-- new company invisible to the picker that created it — the same bug
-- `createPowerCompany` shipped on 2026-08-12. Fixed in the same commit.
--
-- Contacts and power_contacts are already nullable and need no DDL; their
-- CREATES are Airtable-first out of habit rather than constraint, and move in
-- the same commit.

ALTER TABLE companies       ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE power_companies ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE expense_vendors ALTER COLUMN airtable_id DROP NOT NULL;

-- The ON CONFLICT (airtable_id) upserts these tables use still work: a NULL
-- never conflicts, so a native row is simply always an insert. That is correct —
-- it has no Airtable twin to reconcile against.
--
-- Uniqueness on the natural key matters more now that airtable_id can be NULL,
-- because the duplicate guard in each create handler is the only thing standing
-- between a fumbled retry and two companies with the same name.
CREATE UNIQUE INDEX IF NOT EXISTS companies_name_unique
  ON companies (lower(btrim(name))) WHERE coalesce(btrim(name), '') <> '';
CREATE UNIQUE INDEX IF NOT EXISTS power_companies_name_unique
  ON power_companies (lower(btrim(name))) WHERE coalesce(btrim(name), '') <> '';
CREATE UNIQUE INDEX IF NOT EXISTS expense_vendors_name_unique
  ON expense_vendors (lower(btrim(name))) WHERE coalesce(btrim(name), '') <> '';

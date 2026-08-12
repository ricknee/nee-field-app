-- 041_items_native.sql
-- Slice 5 of the inventory Airtable-write cutover (docs/PLAN-inventory-airtable-cutover.md).
-- Inventory Items — the last domain, and the most-referenced entity in the base.

ALTER TABLE inventory_items ALTER COLUMN airtable_id DROP NOT NULL;

-- ⚠⚠ ITEMS KEEP A DUAL HANDLE, AND THAT IS A DELIBERATE DEPARTURE FROM SLICES 1-4.
--
-- Every other domain switched its public id wholesale to the uuid. Items do not,
-- because the item id is the app's widest currency: ~40 backend sites and ~100
-- in the frontend carry it, and it flows item picker -> cart -> transaction ->
-- expense push. Rewriting all of that in one slice would be the largest and
-- riskiest change of the whole cutover, to no functional gain.
--
-- Instead an item's public handle is: its Airtable rec id if it has one, else
-- its uuid. Reads emit COALESCE(airtable_id, id::text); every child write
-- resolves with `airtable_id = $n OR id::text = $n`. Both sides are symmetric,
-- the handle stays an opaque string to everything downstream, and the 866
-- historical items keep the ids already sitting in transactions, estimate
-- lines, template lines, order lines and vendor pricing.
--
-- The two forms cannot collide: an Airtable rec id always starts "rec" and is
-- never a valid uuid.
--
-- This is a transition shape, not a destination. Once the base is archived
-- (slice 6) a follow-up can normalise to the uuid in one data migration plus
-- one read change — but that is cleanup, not cutover, and it is not urgent.

-- The lookups the resolver relies on. The uuid side is the primary key, so only
-- the text side needs help; airtable_id is already UNIQUE.
COMMENT ON COLUMN inventory_items.airtable_id IS
  'Airtable rec id for the 866 items predating the slice-5 cutover (2026-08-12). NULL on items created since. The PUBLIC HANDLE is COALESCE(airtable_id, id::text) — see db/schema/041 for why items keep a dual handle where every other table moved to the uuid.';

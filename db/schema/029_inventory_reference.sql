-- 029_inventory_reference.sql — Step B of the inventory→Neon migration.
-- Reference data: Locations, Vendors, Inventory Items, Vendor Pricing.
-- ---------------------------------------------------------------------------
-- WHAT THIS IS FOR. These four tables are the inventory base's own reference
-- data — the catalog and where things live. Steps A, B0 and E removed the
-- couplings between the two apps; this is the first slice of the inventory
-- base's actual contents.
--
-- ── WHY IT IS WORTH DOING, WHICH IS NOT "POSTGRES IS BETTER" ───────────────
-- `Inventory Items` encodes the LOCATION LIST IN ITS FIELD NAMES. Quantity on
-- hand exists there as fourteen fields — Qty In (Shop #1), Qty Out (Shop #1),
-- Quantity On Hand (Shop #1), then the same trio for Shop #2, #4 Transit,
-- #5 Express, #6 Trailer, plus a Global pair. Every one is a rollup over the
-- same transaction table, filtered by a location the field NAME hard-codes.
-- Adding a truck today means hand-adding three Airtable fields. Here a location
-- is a ROW, and on-hand is one view (Step C).
--
-- ⚠ NONE of those fourteen fields are migrated, and that is deliberate:
-- verified 2026-08-10 that the app reads them NOWHERE — zero hits in
-- inventory.js and zero in inventory.html. On-hand reaches the app through
-- `Stock Levels`, which is Step C. So Items does NOT have to wait for the
-- ledger.
--
-- ⚠ Derived Airtable fields are NOT columns here. `Unit Cost Rollup (Live)`,
-- `Price Variance ($)`, `Price Variance (%)`, `Suggested Default Unit Cost` and
-- `Price Per Ft. (Paid)` all derive from data in these same tables, so they
-- become the view at the bottom. Port the intent, not the field.
--
-- ⚠ MONEY IS numeric(14,4), NOT (14,2). Wire is priced per foot and unit costs
-- carry sub-cent precision; rounding at rest threw four expense rows off by a
-- cent at Step 4d. Round at the point of display, never in storage.
--
-- Airtable stays the identity authority for this slice: `airtable_id` is UNIQUE
-- on every table and the loader upserts on it, exactly like `expenses`.

BEGIN;

-- ── Locations ──────────────────────────────────────────────────────────────
-- 5 rows today: Shop #1, Shop #2, #4 Transit, #5 Express, #6 Box Trailer.
-- The whole point of the slice: this is a table you can INSERT into.
CREATE TABLE IF NOT EXISTS locations (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id    text UNIQUE NOT NULL,
  name           text NOT NULL,
  location_type  text,                      -- Shop | Truck (Airtable single-select)
  active         boolean NOT NULL DEFAULT true,
  notes          text,
  synced_at      timestamptz
);

-- ── Vendors ────────────────────────────────────────────────────────────────
-- 4 rows. Distinct from the MAIN base's Vendors table, which the field app
-- uses for expenses — these are inventory suppliers.
CREATE TABLE IF NOT EXISTS vendors (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id      text UNIQUE NOT NULL,
  name             text NOT NULL,
  vendor_type      text,
  account_number   text,
  phone            text,
  email            text,
  website          text,
  address          text,
  primary_contact  text,
  payment_terms    text,
  active           boolean NOT NULL DEFAULT true,
  notes            text,
  synced_at        timestamptz
);

-- ── Inventory Items ────────────────────────────────────────────────────────
-- 866 rows, and the most-read table in the inventory app (13 read sites).
-- Only the columns a human actually types are here; everything Airtable
-- computed is a view or belongs to Step C.
CREATE TABLE IF NOT EXISTS inventory_items (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id        text UNIQUE NOT NULL,
  name               text NOT NULL,
  category           text,
  product_size       text,
  unit_of_measure    text,
  barcode            text,
  alternate_barcodes text,                  -- newline-separated in Airtable
  default_unit_cost  numeric(14,4),
  wire_ft_per_lb     numeric(14,4),         -- >0 marks a wire item; drives ft↔lb
  reorder_point      numeric(14,4),
  active             boolean NOT NULL DEFAULT true,
  notes              text,
  synced_at          timestamptz
);

-- Barcode scanning is the hot path in the shop: one lookup per scan.
CREATE INDEX IF NOT EXISTS inventory_items_barcode_idx
  ON inventory_items (barcode) WHERE barcode IS NOT NULL AND barcode <> '';
-- The pickers list active items by name.
CREATE INDEX IF NOT EXISTS inventory_items_active_name_idx
  ON inventory_items (active, name);

-- ── Vendor Pricing ─────────────────────────────────────────────────────────
-- 2 rows today. Per-item, per-vendor cost.
--
-- Carries BOTH the Airtable ids and the real FKs. The ids are what the app
-- still speaks (Airtable remains the identity authority for this slice); the
-- FKs are what makes the live-cost view a join instead of a name match.
CREATE TABLE IF NOT EXISTS vendor_pricing (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id         text UNIQUE NOT NULL,
  item_airtable_id    text,
  item_id             uuid REFERENCES inventory_items(id) ON DELETE CASCADE,
  vendor_airtable_id  text,
  vendor_id           uuid REFERENCES vendors(id) ON DELETE SET NULL,
  active              boolean NOT NULL DEFAULT true,
  preferred           boolean NOT NULL DEFAULT false,   -- "Preferred for This Item"
  unit_cost           numeric(14,4),
  unit_of_measure     text,
  vendor_part_number  text,
  min_order_qty       numeric(14,4),
  lead_time_days      integer,
  last_price_update   date,
  price_valid_until   date,
  notes               text,
  synced_at           timestamptz
);

CREATE INDEX IF NOT EXISTS vendor_pricing_item_idx   ON vendor_pricing (item_id);
CREATE INDEX IF NOT EXISTS vendor_pricing_vendor_idx ON vendor_pricing (vendor_id);

-- ── v_item_live_cost — replaces the `Unit Cost Rollup (Live)` rollup ───────
-- Airtable rolls the preferred vendor's Unit Cost up onto the item. Two
-- handlers read it: the vendor-pricing panel (to show "live" beside the saved
-- default) and syncItemCostToVendor (to copy live → Default Unit Cost).
--
-- The rollup returns a number only when a PREFERRED row has a cost, and is
-- empty otherwise — so this filters on `preferred` and `active` the same way,
-- and MIN() collapses the case where two rows are both marked preferred
-- (Airtable cannot prevent that; it would silently pick one too).
CREATE OR REPLACE VIEW v_item_live_cost AS
SELECT i.id                AS item_id,
       i.airtable_id       AS item_airtable_id,
       MIN(p.unit_cost)    AS live_unit_cost
FROM inventory_items i
JOIN vendor_pricing p
  ON p.item_id = i.id AND p.preferred AND p.active AND p.unit_cost IS NOT NULL
GROUP BY i.id, i.airtable_id;

COMMIT;

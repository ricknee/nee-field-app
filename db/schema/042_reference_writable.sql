-- 042_reference_writable.sql
-- Locations, Vendors and Vendor Pricing become writable IN THE APP.
--
-- Not part of the cutover — this is the hole the cutover exposed. All three
-- were always read-only here, because you maintained them in Airtable. Once the
-- app stopped reading Airtable there was nowhere left to add a vendor, open a
-- new shop, or record what a supplier charges. The vendor-pricing panel still
-- said "Add a Vendor Pricing record in Airtable", which had become impossible.

ALTER TABLE locations      ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE vendors        ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE vendor_pricing ALTER COLUMN airtable_id DROP NOT NULL;

-- ⚠⚠ ONE PREFERRED VENDOR PER ITEM, ENFORCED.
--
-- v_item_live_cost filters on `preferred AND active` and wraps the result in
-- MIN() — that MIN exists precisely because Airtable let two rows for the same
-- item both be preferred, and something had to break the tie. It broke it
-- silently, by price, which is not a decision anyone made.
--
-- A partial unique index makes the ambiguity impossible instead of resolving it
-- after the fact. handleVendorPricingUpsert clears the flag from the item's
-- other rows in the same statement, so setting a new preferred vendor is one
-- action rather than two.
CREATE UNIQUE INDEX IF NOT EXISTS vendor_pricing_one_preferred_idx
    ON vendor_pricing (item_id) WHERE preferred AND active;

-- A vendor can only price a given item once. Two rows for the same pair is a
-- data-entry slip, not a modelling choice, and it is what makes "which price is
-- current?" unanswerable.
CREATE UNIQUE INDEX IF NOT EXISTS vendor_pricing_item_vendor_idx
    ON vendor_pricing (item_id, vendor_id) WHERE item_id IS NOT NULL AND vendor_id IS NOT NULL;

COMMENT ON INDEX vendor_pricing_one_preferred_idx IS
  'At most one preferred+active price per item. The MIN() in v_item_live_cost predates this and was only ever a tie-breaker for a state that should not exist.';

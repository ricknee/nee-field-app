-- 040_estimating_native.sql
-- Slice 4 of the inventory Airtable-write cutover (docs/PLAN-inventory-airtable-cutover.md).
-- The estimating cluster: estimates, their lines, templates, template lines,
-- material orders and order lines.
--
-- ⚠⚠ SIX TABLES IN ONE SLICE, ON PURPOSE. They cannot move separately, because
-- they reference each other by AIRTABLE RECORD ID:
--   · handleOrderCreate writes the estimate into an Airtable LINKED-RECORD
--     field — a uuid cannot go there.
--   · saveEstimateAsTemplate fetches the estimate from Airtable by rec id — a
--     native estimate has no Airtable record to fetch.
-- Flip estimates alone and orders and templates end up pointing at records that
-- do not exist. The id currency has to change across the whole cluster at once.

-- 1. A natively-created row has no Airtable record behind it. Postgres allows
--    unlimited NULLs in a UNIQUE index, so the existing airtable_id keys keep
--    protecting the historical rows without blocking native ones.
ALTER TABLE material_estimates                ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE material_estimate_lines           ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE material_estimate_templates       ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE material_estimate_template_lines  ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE material_orders                   ALTER COLUMN airtable_id DROP NOT NULL;
ALTER TABLE material_order_lines              ALTER COLUMN airtable_id DROP NOT NULL;

-- 2. `Order ID` was an Airtable autonumber and is the one number in this
--    cluster a human actually reads — it is printed on the order.
--
-- ⚠⚠ SEEDED FROM WHAT WAS ISSUED, NOT FROM max(). The numbers still in the
--    table are 13, 17, 23, 24, 25, 27, 28, 29, 30, 31 — every gap is an order
--    that was created and later deleted, and each of those numbers was seen by
--    someone. Airtable autonumbers never reclaim; max() does. #32 was minted
--    and deleted during the Step D smoke on 2026-08-11, so Airtable's next
--    value is 33 and max()+1 would re-mint a number already used.
--    Starting at 40 clears everything issued with headroom to spare, and still
--    reads as a continuation rather than a reset.
CREATE SEQUENCE IF NOT EXISTS material_order_number_seq START WITH 40;

-- 3. Line numbers are NOT sequences. `Line ID` and `Line Item ID` were global
--    Airtable autonumbers, but nothing outside the row ever reads them — they
--    only order lines within one parent (ORDER BY line_number). So a native
--    line is numbered by its position in its own estimate or order, which is
--    what the column always meant. No global counter, no seeding trap, and the
--    number finally means something on its own.

COMMENT ON COLUMN material_estimates.airtable_id IS
  'Airtable rec id for rows predating the slice-4 cutover (2026-08-12). NULL on every row born in Postgres. The handle the app uses is id (uuid).';
COMMENT ON COLUMN material_orders.order_number IS
  'User-facing order number. Airtable autonumber for historical rows; material_order_number_seq (started at 40) for native ones. Never re-seed from max() — deleted orders leave gaps whose numbers were already issued.';
COMMENT ON COLUMN material_estimate_lines.line_number IS
  'Position of the line within its estimate. Was a global Airtable autonumber; nothing outside the row read it, so native lines number 1..N per estimate.';

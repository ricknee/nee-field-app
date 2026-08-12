-- 039_stock_settings_native.sql
-- Slice 3 of the inventory Airtable-write cutover (docs/PLAN-inventory-airtable-cutover.md).
-- Reorder points stop being written to Airtable.
--
-- The smallest domain in the base and the only one nothing else references,
-- which is why it goes first now that the slices are ordered leaf-first.

-- A natively-created reorder point has no Airtable record behind it.
ALTER TABLE stock_settings ALTER COLUMN airtable_id DROP NOT NULL;

-- v_stock_levels exposed only ss.airtable_id, so the screen had no handle for a
-- setting that was never in Airtable. Adding the uuid at the END of the select
-- list is what CREATE OR REPLACE permits, and keeps every existing column in
-- place for the reads already using them.
--
-- ⚠ Rebuilt from pg_get_viewdef(), NOT from 032's copy of this view. The .sql
-- files in this directory drift from what is actually installed — a stale view
-- definition reinstated a fixed OT bug once already.
CREATE OR REPLACE VIEW v_stock_levels AS
 WITH pairs AS (
         SELECT v_stock_on_hand.item_id, v_stock_on_hand.location_id
           FROM v_stock_on_hand
        UNION
         SELECT stock_settings.item_id, stock_settings.location_id
           FROM stock_settings
          WHERE stock_settings.item_id IS NOT NULL AND stock_settings.location_id IS NOT NULL
        )
 SELECT i.id AS item_id,
    i.airtable_id AS item_airtable_id,
    i.name AS item_name,
    i.category,
    i.product_size,
    i.unit_of_measure,
    COALESCE(i.wire_ft_per_lb, 0::numeric)::numeric(14,4) AS wire_ft_per_lb,
    COALESCE(i.default_unit_cost, 0::numeric)::numeric(14,4) AS default_unit_cost,
    l.id AS location_id,
    l.airtable_id AS location_airtable_id,
    l.name AS location_name,
    COALESCE(oh.qty_on_hand, 0::numeric)::numeric(14,4) AS qty_on_hand,
    ss.airtable_id AS stock_airtable_id,
    ss.reorder_point,
    ss.notes,
    (COALESCE(oh.qty_on_hand, 0::numeric) * COALESCE(i.default_unit_cost, 0::numeric))::numeric(14,4) AS total_value,
    (COALESCE(oh.qty_on_hand, 0::numeric) * COALESCE(i.wire_ft_per_lb, 0::numeric))::numeric(14,4) AS wire_ft,
    ss.id AS stock_id
   FROM pairs p
     JOIN inventory_items i ON i.id = p.item_id
     JOIN locations l ON l.id = p.location_id
     LEFT JOIN v_stock_on_hand oh ON oh.item_id = p.item_id AND oh.location_id = p.location_id
     LEFT JOIN stock_settings ss ON ss.item_id = p.item_id AND ss.location_id = p.location_id;

COMMENT ON COLUMN stock_settings.airtable_id IS
  'Airtable rec id for the 269 rows that predate the slice-3 cutover (2026-08-12). NULL on every row born in Postgres. The handle the app uses is id (uuid); the natural key is the partial unique on (item_id, location_id).';

-- 043_native_handles.sql
-- Teach v_stock_levels the dual handle, and repair any row whose FK never resolved.
--
-- ── WHY ────────────────────────────────────────────────────────────────────
-- Slices 5 (items, 041) and the reference-data slice (locations/vendors, 042)
-- made both entities natively creatable. A native row has `airtable_id IS NULL`
-- and is addressed by its uuid, so every reader had to learn to accept EITHER
-- handle. `handleItems` and `handleLocations` both serve
-- `COALESCE(airtable_id, id::text)`, and the write paths (`handleItemUpdate`,
-- `handleLocationSave`, `handleUpdateItemCost`) all resolve on
-- `airtable_id = $1 OR id::text = $1`.
--
-- Three READERS were missed, all of them keyed on the Airtable id alone, and all
-- three sit downstream of this view:
--
--   * handleStockLevels    — `WHERE item_airtable_id = $1`. A native item's uuid
--     matches nothing, the query SUCCEEDS with zero rows, and Check Stock renders
--     a clean working-looking screen saying the item is nowhere.
--   * handleStockLevelsAll — returns `item_airtable_id || ""`, and the frontend
--     joins on that id (inventory.html:3312 / :3387 / :3531). An empty string
--     joins to nothing, so a native item's stock lands under "Uncategorized".
--   * handleReorderAlerts  — same, for item AND location.
--
-- Patching the three call sites would fix the instances. Putting the handle in
-- the view fixes the CLASS: any future reader that selects `item_handle` is
-- correct by construction, and the id currency is decided in exactly one place.
--
-- ⚠ The view body below is transcribed from `pg_get_viewdef('v_stock_levels')`
-- against the live database, NOT from an earlier file in this directory. The
-- .sql files here have drifted from the deployed definitions before (006 vs 024
-- cost a reinstated OT bug); the database is the source of truth for a view.
--
-- ⚠ CREATE OR REPLACE VIEW may only APPEND columns — existing names, types and
-- order must match exactly. That is why the two handles are last rather than
-- next to the ids they shadow.

CREATE OR REPLACE VIEW v_stock_levels AS
WITH pairs AS (
  SELECT item_id, location_id FROM v_stock_on_hand
  UNION
  SELECT item_id, location_id FROM stock_settings
   WHERE item_id IS NOT NULL AND location_id IS NOT NULL
)
SELECT i.id                                                            AS item_id,
       i.airtable_id                                                   AS item_airtable_id,
       i.name                                                          AS item_name,
       i.category,
       i.product_size,
       i.unit_of_measure,
       COALESCE(i.wire_ft_per_lb, 0::numeric)::numeric(14,4)           AS wire_ft_per_lb,
       COALESCE(i.default_unit_cost, 0::numeric)::numeric(14,4)        AS default_unit_cost,
       l.id                                                            AS location_id,
       l.airtable_id                                                   AS location_airtable_id,
       l.name                                                          AS location_name,
       COALESCE(oh.qty_on_hand, 0::numeric)::numeric(14,4)             AS qty_on_hand,
       ss.airtable_id                                                  AS stock_airtable_id,
       ss.reorder_point,
       ss.notes,
       (COALESCE(oh.qty_on_hand, 0::numeric)
        * COALESCE(i.default_unit_cost, 0::numeric))::numeric(14,4)    AS total_value,
       (COALESCE(oh.qty_on_hand, 0::numeric)
        * COALESCE(i.wire_ft_per_lb, 0::numeric))::numeric(14,4)       AS wire_ft,
       ss.id                                                           AS stock_id,
       -- ── new in 043 ──────────────────────────────────────────────────────
       -- The id the app speaks. Identical to what handleItems / handleLocations
       -- hand the browser, so a value that came out of a picker always matches.
       COALESCE(i.airtable_id, i.id::text)                             AS item_handle,
       COALESCE(l.airtable_id, l.id::text)                             AS location_handle
  FROM pairs p
  JOIN inventory_items i ON i.id = p.item_id
  JOIN locations       l ON l.id = p.location_id
  LEFT JOIN v_stock_on_hand oh ON oh.item_id = p.item_id AND oh.location_id = p.location_id
  LEFT JOIN stock_settings  ss ON ss.item_id = p.item_id AND ss.location_id = p.location_id;


-- ── REPAIR ─────────────────────────────────────────────────────────────────
-- `insertTxns` resolved the item on both handle forms but the two LOCATION
-- subselects on `airtable_id` only, so a movement into a natively-created
-- location inserted with `location_id` NULL while `location_airtable_id` held
-- the uuid. `v_stock_on_hand` filters on `*_location_id IS NOT NULL` and
-- `v_stock_levels` JOINs on `l.id`, so that stock was invisible everywhere —
-- logged, pushable as an expense, and absent from every stock figure.
--
-- 0 rows matched when this was written (the only native location was an unused
-- test row). It is here because the window between writing the fix and
-- deploying it is exactly when a row like this appears, and because the same
-- statement is the audit query: if it ever reports > 0, that is the bug.
--
-- Safe to re-run. Only touches rows whose id column is NULL while the text
-- column holds something that resolves.
UPDATE inventory_transactions t
   SET from_location_id = l.id
  FROM locations l
 WHERE t.from_location_id IS NULL
   AND t.from_location_airtable_id IS NOT NULL
   AND l.id::text = t.from_location_airtable_id;

UPDATE inventory_transactions t
   SET to_location_id = l.id
  FROM locations l
 WHERE t.to_location_id IS NULL
   AND t.to_location_airtable_id IS NOT NULL
   AND l.id::text = t.to_location_airtable_id;

-- Same shape, for the reorder point. `handleCreateStockLevel` had the identical
-- miss; its guard caught it and returned a 404, but the INSERT had already run
-- and a NULL location_id sits OUTSIDE the (item_id, location_id) partial unique
-- index — so the row was saved, invisible, and reported as a failure.
UPDATE stock_settings s
   SET location_id = l.id
  FROM locations l
 WHERE s.location_id IS NULL
   AND s.location_airtable_id IS NOT NULL
   AND l.id::text = s.location_airtable_id;

-- ── NOT REPAIRED HERE, ON PURPOSE ──────────────────────────────────────────
-- 12 `stock_settings` rows have `item_id` NULL — and `item_airtable_id` NULL
-- too, so there is no handle to resolve them by. They are empty Stock Levels
-- shells from the ORIGINAL Airtable load (029), not orphans this bug created:
-- every one carries a reorder point of NULL or 0, and they predate the item
-- cutover. They are already invisible (v_stock_levels INNER JOINs the item), so
-- they cost nothing but a row count.
--
-- Deleting them is a data decision, not a bug fix, so it is not bundled into
-- one. The audit query, if it is ever wanted:
--
--   SELECT count(*) FROM stock_settings WHERE item_id IS NULL;   -- 12
--
-- After this migration the *live* invariant to watch is the pair below, which
-- must stay at zero. Non-zero means a writer has gone back to resolving a
-- location by `airtable_id` alone.
--
--   SELECT count(*) FROM inventory_transactions
--    WHERE (from_location_airtable_id IS NOT NULL AND from_location_id IS NULL)
--       OR (to_location_airtable_id   IS NOT NULL AND to_location_id   IS NULL);

-- 032_inventory_ledger.sql — Step C of the inventory→Neon migration.
-- The ledger: Inventory Transactions, plus the real user data hiding inside
-- Stock Levels. Depends on 029 (items, locations, vendors, vendor pricing).
-- ---------------------------------------------------------------------------
-- ⚠⚠ `Quantity On Hand` IS DELIBERATELY NOT PORTED, AND THIS IS THE WHOLE POINT
-- OF THE SLICE. Airtable holds on-hand two ways, and on 2026-08-11 they were
-- reconciled against each other for the first time:
--
--   * the `Quantity On Hand (<location>)` ROLLUPS on Inventory Items reproduce
--     the raw transaction ledger EXACTLY — 4,330 item+location pairs, zero
--     disagreements;
--   * `Stock Levels.Quantity On Hand`, a cache an Airtable automation
--     maintains, disagrees with them on 237 of 269 pairs.
--
-- So the rollups are faithful arithmetic and the cache has silently drifted.
-- Porting the cache would import that drift and give it a new home; the view at
-- the bottom recomputes from the ledger instead, which makes it a *verified*
-- port rather than a hopeful one.
--
-- ⚠ EXPECT NEGATIVE NUMBERS, AND THEY ARE HONEST. 26,332 units have been used
-- against 15,039 received: material leaves the shop without always being
-- received in. The arithmetic is right and the inputs are incomplete. The fix
-- is a counting day, where each count is recorded as an Adjustment — there are
-- already 29 of those in the ledger, so the mechanism exists.
--
-- ⚠ THE APP WILL SHOW DIFFERENT NUMBERS after the reads flip, because it shows
-- the cache today. That is the migration surfacing what the ledger always said,
-- not the migration breaking. Owner informed 2026-08-11.
--
-- ⚠ LEDGER SEMANTICS, verified rather than assumed: quantity is added at
-- `To Location` and subtracted at `From Location`. Use/Return/Adjustment carry
-- only a From; Receive carries only a To; Transfer carries both. Reproducing
-- the rollups on 4,330 pairs is what confirms this reading is the right one.

BEGIN;

-- ── Inventory Transactions — the ledger everything else derives from ───────
CREATE TABLE IF NOT EXISTS inventory_transactions (
  id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id               text UNIQUE NOT NULL,
  txn_name                  text,          -- "TX-20260810-105119" display formula
  txn_date                  timestamptz,
  item_airtable_id          text,
  item_id                   uuid REFERENCES inventory_items(id) ON DELETE SET NULL,
  quantity                  numeric(14,4) NOT NULL DEFAULT 0,
  txn_type                  text,          -- Use | Receive | Return | Transfer | Adjustment
  from_location_airtable_id text,
  from_location_id          uuid REFERENCES locations(id) ON DELETE SET NULL,
  to_location_airtable_id   text,
  to_location_id            uuid REFERENCES locations(id) ON DELETE SET NULL,
  -- Money frozen at transaction time. NOT the item's current cost: re-deriving
  -- it later would silently re-price history every time a vendor price moved.
  unit_cost_snapshot        numeric(14,4),
  notes                     text,
  entered_by                text,
  -- The main-base job this was used on. Plain text, not a link — the cross-base
  -- link this replaced was deleted with the Jobs mirror (bet C3, 2026-08-10),
  -- and a rec-id string is exactly the shape Postgres wants anyway.
  job_airtable_id           text,
  job_name                  text,
  expense_created           boolean NOT NULL DEFAULT false,
  push_id                   text,          -- idempotency key of the expense push
  synced_at                 timestamptz
);

-- The pending-expenses read: un-pushed Use/Return rows, oldest first.
CREATE INDEX IF NOT EXISTS inv_txn_pending_idx
  ON inventory_transactions (txn_date) WHERE expense_created = false;
CREATE INDEX IF NOT EXISTS inv_txn_item_idx     ON inventory_transactions (item_id);
CREATE INDEX IF NOT EXISTS inv_txn_push_idx     ON inventory_transactions (push_id) WHERE push_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS inv_txn_job_idx      ON inventory_transactions (job_airtable_id) WHERE job_airtable_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS inv_txn_date_idx     ON inventory_transactions (txn_date DESC);
-- v_stock_on_hand scans by location on both legs.
CREATE INDEX IF NOT EXISTS inv_txn_from_loc_idx ON inventory_transactions (from_location_id) WHERE from_location_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS inv_txn_to_loc_idx   ON inventory_transactions (to_location_id)   WHERE to_location_id   IS NOT NULL;

-- ── stock_settings — what Stock Levels actually held that was DATA ─────────
-- Reorder Point and Notes are typed by a human and cannot be derived from
-- anything, so they survive the cache being dropped. Everything else on that
-- table was either the drifted quantity or a lookup/rollup of it.
--
-- 269 distinct item+location pairs today with ZERO duplicates, so the pair is
-- enforced unique — a second reorder point for the same item in the same place
-- is a bug, and Airtable had no way to say so.
CREATE TABLE IF NOT EXISTS stock_settings (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id           text UNIQUE NOT NULL,
  item_airtable_id      text,
  item_id               uuid REFERENCES inventory_items(id) ON DELETE CASCADE,
  location_airtable_id  text,
  location_id           uuid REFERENCES locations(id) ON DELETE CASCADE,
  reorder_point         numeric(14,4),
  notes                 text,
  synced_at             timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS stock_settings_item_loc_idx
  ON stock_settings (item_id, location_id)
  WHERE item_id IS NOT NULL AND location_id IS NOT NULL;

-- ── v_stock_on_hand — the fourteen hard-coded fields, as one view ──────────
-- This is the payoff. In Airtable, on-hand is `Qty In (Shop #1)`,
-- `Qty Out (Shop #1)`, `Quantity On Hand (Shop #1)` … repeated per location,
-- with the location list encoded in the FIELD NAMES. Adding a truck meant
-- hand-adding three fields and editing the frontend.
--
-- Here a location is a row, and this view covers every location that exists or
-- ever will. Adding a truck is an INSERT.
CREATE OR REPLACE VIEW v_stock_on_hand AS
SELECT leg.item_id,
       leg.location_id,
       SUM(leg.qty)::numeric(14,4) AS qty_on_hand
FROM (
  SELECT item_id, to_location_id   AS location_id,  quantity AS qty
    FROM inventory_transactions WHERE to_location_id   IS NOT NULL
  UNION ALL
  SELECT item_id, from_location_id AS location_id, -quantity AS qty
    FROM inventory_transactions WHERE from_location_id IS NOT NULL
) leg
WHERE leg.item_id IS NOT NULL
GROUP BY leg.item_id, leg.location_id;

-- Reorder alerts and the stock screen both want on-hand beside the settings and
-- the item, so the join lives here once rather than in three handlers.
CREATE OR REPLACE VIEW v_stock_levels AS
SELECT i.id                        AS item_id,
       i.airtable_id               AS item_airtable_id,
       i.name                      AS item_name,
       i.category,
       i.product_size,
       i.unit_of_measure,
       i.wire_ft_per_lb,
       i.default_unit_cost,
       l.id                        AS location_id,
       l.airtable_id               AS location_airtable_id,
       l.name                      AS location_name,
       COALESCE(oh.qty_on_hand, 0) AS qty_on_hand,
       ss.airtable_id              AS stock_airtable_id,
       ss.reorder_point,
       ss.notes,
       COALESCE(oh.qty_on_hand, 0) * COALESCE(i.default_unit_cost, 0) AS total_value
FROM v_stock_on_hand oh
JOIN inventory_items i ON i.id = oh.item_id
JOIN locations       l ON l.id = oh.location_id
LEFT JOIN stock_settings ss ON ss.item_id = oh.item_id AND ss.location_id = oh.location_id;

COMMIT;

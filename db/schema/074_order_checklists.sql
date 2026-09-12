-- 074_order_checklists.sql — an inventory ORDER becomes a LIST on its job.
--
-- Owner's ask (2026-09-12): "create a list showing each item that is ordered and
-- how much is ordered". Saving an order in the inventory app now creates one
-- checklist on the job — "Order #47 — Wolff Deliver to shop." — with a line per
-- order line ("500 — 1/2" EMT PIPE"), which the crew ticks off as it arrives.
-- The module that does it is netlify/functions/_order-checklists.js.
--
-- ── THE RULES THE OWNER PICKED ─────────────────────────────────────────────
--   * One list per order (the partial unique index below).
--   * Editing the ORDER updates its list: added lines appear, removed lines go,
--     changed qtys rewrite the line. A line already TICKED is never touched —
--     it is a record of what arrived, and rewriting "500" to "700" under a tick
--     would claim 700 arrived.
--   * Editing a line on the LIST does not change the order. The order is what
--     was sent to the vendor.
--
-- ── WHY order_line_key AND NOT A LINE ID ───────────────────────────────────
-- handleOrderUpdate replaces an order's lines wholesale (DELETE + INSERT), so a
-- material_order_lines.id does not survive an edit. The key is the thing that
-- does: `item:<item uuid>` for a catalog item (the build screen already refuses
-- to add one item twice), `misc:<lowercased description>` for a Misc line, with
-- `#2`, `#3` on repeats. A crew edit to the line's TEXT keeps the key, so a
-- later order edit still finds the line.
--
-- ── job_checklists.job_airtable_id LOSES ITS NOT NULL ──────────────────────
-- 061 dropped the NOT NULL from job keys when jobs began being born in Neon, and
-- its header lists job_checklists among the tables that were safe — but this
-- column was still NOT NULL in production on 2026-09-12 (information_schema,
-- checked). handleCreateChecklist writes NULL for a native job, so a list could
-- not be started on any of the 7 native jobs: an INSERT that failed with a 500.
-- The FK that matters is job_id; the reads already match on either.
--
-- ── material_orders.job_id ─────────────────────────────────────────────────
-- Orders have only ever carried job_name, a display STRING ("MT Liberty DG
-- (LIM 138)" — jobs.po_locked, not jobs.name). That is the substring/rename trap
-- CLAUDE.md warns about, so the order now records the job it was raised against.
-- Nullable: restock orders are raised against a LOCATION and have no job.

ALTER TABLE job_checklists ALTER COLUMN job_airtable_id DROP NOT NULL;

ALTER TABLE job_checklists
  ADD COLUMN material_order_id uuid REFERENCES material_orders(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX job_checklists_material_order
  ON job_checklists (material_order_id) WHERE material_order_id IS NOT NULL;

ALTER TABLE checklist_items ADD COLUMN order_line_key text;

CREATE INDEX checklist_items_order_key
  ON checklist_items (checklist_id, order_line_key) WHERE order_line_key IS NOT NULL;

ALTER TABLE material_orders
  ADD COLUMN job_id uuid REFERENCES jobs(id) ON DELETE SET NULL;

COMMENT ON COLUMN job_checklists.material_order_id IS
  'The inventory order this list was generated from (db/schema/074). NULL for a list someone typed. ON DELETE SET NULL, but deleting an order also deletes its list when nothing on it is ticked — see dropOrderChecklistIfUntouched.';
COMMENT ON COLUMN checklist_items.order_line_key IS
  'Stable identity of the order line this item mirrors — item:<uuid> or misc:<description>, #n on repeats. Survives the DELETE+INSERT that replaces order lines on edit. NULL for a typed item. db/schema/074.';
COMMENT ON COLUMN material_orders.job_id IS
  'The job the order was raised against. NULL for restock orders (raised against a location) and for orders older than db/schema/074 that no backfill matched.';

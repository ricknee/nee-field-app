-- 087 — Vendor part number → inventory item links (price tracking, step 2).
--
-- vendor_invoice_lines (085) records what each supplier ticket says, keyed by
-- the VENDOR'S part number (CED "ST300", Wolff "PLC5111"). This table is the
-- owner's one-time answer to "what is that, to us?" for each part number:
--
--   linked       → it is this inventory item. Its ticket prices count for the item.
--   not_stocked  → a job-direct buy, one-off or special. Never asks again; its
--                  ticket lines are still kept, just not tied to an item.
--
-- Owner's rules (2026-11-06):
--   • Items are GENERIC — "3\" STRUT CLAMP" is whatever brand the supplier
--     ships, so MANY part numbers (any vendor, any maker, old and renumbered)
--     may point at ONE item. Hence the key is (vendor, vendor_part), never item.
--   • Nothing here ever changes inventory_items.default_unit_cost. A price
--     only moves when a person taps Update on the Prices screen (step 3).
--   • The label QR is inventory_items.barcode and is untouched by any of this.
--
-- unit_factor: how many of the ITEM's units one TICKET unit is. 1 for almost
-- everything. It exists for the cases where they differ — CED prices strut
-- channel per foot with no unit code (stored 'ea'), a box of 25 vs each, or a
-- 10-ft stick vs per foot. item unit price = ticket unit_price / unit_factor.
-- Wire priced per foot against an item costed per POUND is NOT expressed here:
-- that conversion uses the item's own wire_ft_per_lb at read time, so it can't
-- go stale when that figure is corrected.
CREATE TABLE IF NOT EXISTS vendor_part_links (
  vendor       text NOT NULL,
  vendor_part  text NOT NULL,
  status       text NOT NULL CHECK (status IN ('linked', 'not_stocked')),
  -- CASCADE, not SET NULL: a link must name an item (CHECK below), so SET NULL
  -- would make deleting an item fail. Deleting the link instead sends that part
  -- number back to the New Part Numbers queue, which is the right outcome.
  item_id      uuid REFERENCES inventory_items(id) ON DELETE CASCADE,
  unit_factor  numeric NOT NULL DEFAULT 1 CHECK (unit_factor > 0),
  decided_by   text,
  decided_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (vendor, vendor_part),
  CHECK (status <> 'linked' OR item_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS vendor_part_links_item_idx ON vendor_part_links (item_id);

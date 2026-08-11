-- 037_ledger_native.sql
-- Slice 1 of the inventory Airtable-write cutover (docs/PLAN-inventory-airtable-cutover.md).
-- The ledger stops being written to Airtable. Transactions are now BORN in Postgres.
--
-- Three changes, and the second one is the whole point of the slice.

-- 1. A native transaction has no Airtable record, so it has no airtable_id.
--    Postgres allows unlimited NULLs in a UNIQUE index, so the existing
--    inventory_transactions_airtable_id_key keeps protecting the ~4,330
--    historical rows that DO have one, without blocking native rows.
ALTER TABLE inventory_transactions ALTER COLUMN airtable_id DROP NOT NULL;

-- 2. Cart idempotency. handleSubmitCart used to write one Airtable record per
--    line in a loop with no idempotency key at all, so a lost response on a
--    half-written cart left the user pressing Submit again and double-logging
--    the material. The client now generates submit_id once per cart and reuses
--    it on retry; the whole cart inserts in ONE statement, and a replay hits
--    this index and inserts nothing.
--
--    Keyed on (submit_id, submit_line_no) rather than submit_id alone because a
--    cart is many rows: the line number makes each row in the cart distinct
--    while still making the CART as a whole replay-safe.
ALTER TABLE inventory_transactions ADD COLUMN IF NOT EXISTS submit_id      text;
ALTER TABLE inventory_transactions ADD COLUMN IF NOT EXISTS submit_line_no integer;

CREATE UNIQUE INDEX IF NOT EXISTS inv_txn_submit_idx
    ON inventory_transactions (submit_id, submit_line_no)
 WHERE submit_id IS NOT NULL;

-- 3. Native rows are found by uuid, and so are the historical ones from now on.
--    The id currency of the whole ledger moves from the Airtable rec id to
--    inventory_transactions.id, because a native row has no rec id and the
--    pending -> push -> mark chain would otherwise carry a NULL handle and mark
--    nothing (re-offering already-pushed material: a double charge).
--    uuid works for BOTH old and new rows, which is why it is the one to keep.
COMMENT ON COLUMN inventory_transactions.airtable_id IS
  'Airtable rec id for the ~4,330 rows that predate the native-write cutover (slice 1, 2026-08-11). NULL on every row born in Postgres. Vestigial: nothing keys on it any more - id (uuid) is the handle the app uses.';

COMMENT ON COLUMN inventory_transactions.submit_id IS
  'Client-generated UUID identifying one cart submission. Reused verbatim on retry so a replay is a no-op via inv_txn_submit_idx. NULL on historical rows and on non-cart writes (receive/transfer/adjustment, which are single-row and user-initiated).';

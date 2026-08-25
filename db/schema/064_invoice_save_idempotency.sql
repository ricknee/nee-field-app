-- 064_invoice_save_idempotency.sql — one Save click = one invoice.
--
-- Test 10 collected SIX invoices (1670-1675) in a few minutes on 2026-08-25, all
-- for the same work. Nothing was wrong with any individual save: each one landed,
-- and each one minted a fresh display number. What was missing is any way for the
-- server to know that the second request was the SAME save as the first.
--
-- ⚠⚠ AND ONLY THE FIRST ONE IS REAL. `attachAllocationsToInvoice` claims the
-- job's unlinked allocations, so invoice #1 takes the labor and the material and
-- every duplicate after it computes $0.00 while still carrying a snapshot_total
-- that looks like money. A duplicate is not a harmless extra row — it is an
-- invoice that prints a number it cannot substantiate, and it burns a display
-- number that cannot be handed back.
--
-- ── THE PATTERN IS ALREADY IN THIS CODEBASE, THREE TIMES ───────────────────
--   expense_pushes.push_id                     the materials push
--   clock_punches.client_punch_id              a punch, replayed by a phone
--   inventory_transactions.submit_id + line_no  a cart submitted twice
-- Each one is a client-minted key with a UNIQUE index, and each exists because
-- the same failure happened there first: a slow response, an impatient second
-- press, two rows. Invoices are the last write of consequence without one.
--
-- The client mints a uuid when the invoice composer opens and keeps it until a
-- save is CONFIRMED, so a retry — a double-click, a 504 that actually landed,
-- a phone that lost signal mid-request — carries the same key and resolves to
-- the same invoice instead of a new one.

ALTER TABLE invoices ADD COLUMN IF NOT EXISTS client_save_id uuid;

COMMENT ON COLUMN invoices.client_save_id IS
  'Idempotency key minted by the browser for one invoice composition. Retrying a save reuses it, so ON CONFLICT resolves to the invoice already created rather than minting a second one and burning another display number. NULL on every invoice created before 2026-08-25 and on anything Airtable made.';

-- Partial-unique: pre-existing rows are all NULL and must stay allowed.
CREATE UNIQUE INDEX IF NOT EXISTS invoices_client_save_id_key
  ON invoices (client_save_id) WHERE client_save_id IS NOT NULL;

-- ⚠ NOT added: a unique index on (job_id, invoice_display_no). It is tempting —
-- a display number should be unique — but the data says otherwise and the data
-- is right: `invoice_number` is "<job name>-001" on EVERY invoice ever written
-- (Airtable's formula counts the invoice's own Job link, always 1), and Bethel
-- School legitimately has two. Numbering is a separate question from double
-- submission, and conflating them would fail honest progress billing.

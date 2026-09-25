-- 081_vendor_invoice_deleted.sql — a fourth exit from Invoice Review: 'deleted'.
--
-- Owner, 2026-09-24: "I need a way to delete receipt before i approve." The case
-- that prompted it: a Home Depot receipt the bot could not read — invoice number
-- 'UNREADABLE-…', no amount, no date, no PO. It is not a bill that belongs on no
-- job ('reviewed' says a person looked at a real invoice and decided); it is not
-- an invoice at all. Junk, a scan of the wrong page, a test send.
--
-- ⚠ SOFT, NOT A DELETE FROM. The row is kept because the dedupe index
-- (vendor_invoices_dedupe, vendor + invoice number) is what stops the bot's retry
-- re-creating it — erase the row and the same PDF lands back in the queue on the
-- next send. A deleted row also answers "what happened to that invoice" later.
-- Only a row still in needs_review can be deleted: a matched one has an expense
-- on a job, and removing that is an expense deletion, not this.

ALTER TABLE vendor_invoices DROP CONSTRAINT IF EXISTS vendor_invoices_status_check;

ALTER TABLE vendor_invoices
  ADD CONSTRAINT vendor_invoices_status_check
  CHECK (status IN ('needs_review', 'matched', 'reviewed', 'deleted'));

COMMENT ON COLUMN vendor_invoices.status IS
  'needs_review = waiting for a person. matched = an expense was created on a job (expense_id). reviewed = a person looked and decided it does not belong on any job; see note. deleted = not a real invoice (unreadable, wrong page, test); hidden from the screen, kept so the dedupe index stops a re-send.';

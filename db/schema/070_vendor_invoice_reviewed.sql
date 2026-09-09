-- 070_vendor_invoice_reviewed.sql — 'dismissed' becomes 'reviewed'.
-- Same day as 069, and worth its own file rather than an edit to that one: 069
-- is already applied to production.
--
-- ── WHY THE WORD MATTERS ───────────────────────────────────────────────────
-- 069 gave an invoice two exits from the queue: onto a job as an expense, or
-- 'dismissed'. The owner asked for a third thing and described it as "check it
-- as reviewed — but not applied to a job", which is not a third thing at all.
-- It is the second one, wearing a name that made it unusable.
--
-- "Dismissed" reads as *rejected* — thrown out, ignored, a mistake. So the
-- action nobody wanted to click was the one they needed for the commonest real
-- case: the $15,368 Wolff invoice whose PO says CASE FARMS, which is a genuine
-- bill that simply is not one job's material cost. Marking that "dismissed"
-- feels like denying it exists. Marking it "reviewed" is exactly true: a person
-- looked, decided, and moved on.
--
-- ⚠ The screen is called Invoice REVIEW, the queue status is needs_REVIEW, and
-- the exit was called something else entirely. A vocabulary that disagrees with
-- itself teaches people to distrust the screen.
--
-- Nothing is lost by the rename: the reason and the free-text note still record
-- *why*, which is the part that answers "so why is there no expense for this
-- invoice" months later.

ALTER TABLE vendor_invoices DROP CONSTRAINT IF EXISTS vendor_invoices_status_check;

-- Any existing rows carry the old word. (There are none in production at the
-- time of writing — the table is one day old — but this must be safe to run
-- against a database where somebody did dismiss one.)
UPDATE vendor_invoices SET status = 'reviewed' WHERE status = 'dismissed';
UPDATE vendor_invoices SET match_reason = 'reviewed-no-job' WHERE match_reason = 'dismissed';

ALTER TABLE vendor_invoices
  ADD CONSTRAINT vendor_invoices_status_check
  CHECK (status IN ('needs_review', 'matched', 'reviewed'));

-- 'reviewed' means: a person looked at this invoice and decided it does not
-- belong on a job. It is NOT 'matched' (no expense exists) and it is NOT still
-- waiting. `note` says why.
COMMENT ON COLUMN vendor_invoices.status IS
  'needs_review = waiting for a person. matched = an expense was created on a job (expense_id). reviewed = a person looked and decided it does not belong on any job; see note.';

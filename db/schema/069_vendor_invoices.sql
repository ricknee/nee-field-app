-- 069_vendor_invoices.sql — the vendor-invoice inbox. CED and Wolff invoices
-- arrive here BEFORE they are anybody's expense.
--
-- ── WHAT WAS ALREADY HAPPENING ─────────────────────────────────────────────
-- An external bot signs in as the `cedautomation` employee (role `office`,
-- id 4456a2fa-b8eb-4595-9428-61493a61b90c, created 2026-08-28) and calls
-- `addGeneralExpense` once per CED/Wolff invoice, having matched the invoice's
-- PO to a job itself. 215 expenses that way so far — 168 CED, 47 Wolff.
--
-- `handleAddGeneralExpense` requires a job: no jobId, or one that fails
-- `isJobHandle`, is a 400. So an invoice whose PO does not match a job has
-- nowhere to go. It is not rejected loudly, it simply never becomes an expense,
-- and the only trace is a PDF sitting in pCloud that the app cannot read
-- (PCLOUD_ACCESS_TOKEN is unset and unobtainable — see _pcloud.js).
--
-- That is the hole this table fills.
--
-- ── WHY A TABLE AND NOT A JOB-LESS EXPENSE ─────────────────────────────────
-- The obvious cheap version is to let the bot write an expense with job_id
-- NULL and call "the review list" a filter over `expenses`. Do not. Every job
-- view, every GP view and every allocation query joins expenses TO a job, so a
-- job-less expense would be a real money row that appears on no screen and in
-- no total — the exact failure this codebase has been bitten by repeatedly:
-- it does not throw, it matches nothing, and matching nothing reads as "there
-- is no data". An invoice awaiting review is not an expense yet. Separate
-- table, separate lifecycle, and `expenses` keeps its invariant that a row
-- there is money already charged to a job.
--
-- ── THE MATCH, AND WHY AMBIGUITY PARKS ─────────────────────────────────────
-- A job's PO string lives in `jobs.po_locked`, falling back to `jobs.po` for
-- the 14 jobs (all New Lead / Not Awarded) that have no locked one yet. It is
-- shaped "Joe Yoder (CAJ 436)" — the customer, then the code in parentheses.
-- The invoice carries only the code, and the two vendors do not agree on
-- spacing: PDF.co reads "CAJ 436" off CED's paper and "CAJ436" off Wolff's.
-- So both sides are reduced to A–Z0–9, uppercased, before comparison.
--
-- ⚠ EXACTLY ONE MATCH CREATES AN EXPENSE. Zero parks. TWO OR MORE ALSO PARKS,
-- and that is not theoretical: "Harlin Smith (2 Barn)" and "Rebecca Smith
-- (2 Barn)" both reduce to 2BARN today. Guessing between two jobs puts real
-- material cost on the wrong one, where it is invisible — nobody audits a job
-- whose costs look plausible. The correct answer to an ambiguous PO is a human.
--
-- ── EVERY INVOICE LANDS HERE, NOT JUST THE MISSES ──────────────────────────
-- The bot posts all of them and this table does the matching, so there is one
-- copy of the rule instead of two that can drift, and a permanent log of every
-- invoice that ever arrived. A matched row is resolved on arrival and carries
-- the expense it created; only `needs_review` rows show up on the screen.
--
-- ── DEDUPE ─────────────────────────────────────────────────────────────────
-- The bot runs on a daily timer over a folder, so re-sending the same invoice
-- is the normal case, not the exceptional one. (vendor, invoice_no) is unique
-- and intake is an upsert that returns the existing row untouched — the same
-- shape as the inventory push's `push_id` guard, and for the same reason:
-- without it a retry charges the job twice and nothing complains.

CREATE TABLE IF NOT EXISTS vendor_invoices (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- As sent by the bot. `vendor_name` is matched against expense_vendors on the
  -- way out, but is stored verbatim so an unrecognised vendor still parks with
  -- its real name on it rather than blank.
  vendor_name   text NOT NULL,
  invoice_no    text NOT NULL,

  -- `po_text` is the PO EXACTLY as read off the paper; `po_code` is that
  -- reduced to A–Z0–9 uppercase. Both are kept: the reviewer needs to see what
  -- the invoice actually said, and the normalised form is what matched (or
  -- didn't). NULL po_text means the parser found no PO at all — a distinct
  -- case from "found one, no job has it", and match_reason says which.
  po_text       text,
  po_code       text,

  invoice_date  date,

  -- Signed. A vendor credit memo arrives as a negative — Wolff prints them with
  -- a TRAILING minus ("773.85-"), which is why intake normalises rather than
  -- trusting Number(). On assignment a negative becomes the expense's
  -- `material_credit` and a positive its `manual_material_cost`; those are two
  -- different columns to the GP views and NULL is not 0 to them.
  amount        numeric,
  tax_amount    numeric,

  -- needs_review → matched | dismissed. A matched row is history; it is never
  -- re-matched, because the expense it points at is already costing a job.
  status        text NOT NULL DEFAULT 'needs_review'
                CHECK (status IN ('needs_review', 'matched', 'dismissed')),

  -- Set together on resolution, whether that was automatic or by hand.
  job_id        uuid REFERENCES jobs(id),
  expense_id    uuid REFERENCES expenses(id),

  -- Why it landed where it did: 'auto' | 'no-po-on-invoice' | 'no-job-match' |
  -- 'ambiguous-po' | 'manual' | 'dismissed'. Written for the human reading the
  -- screen, not for code to branch on — an ambiguous PO and an unreadable one
  -- need different things done about them and look identical without this.
  match_reason  text,

  -- R2 object key of the invoice PDF, under `vendor-invoices/<id>/`. NULL is
  -- allowed and survivable: R2 fails soft everywhere in this app, so an invoice
  -- with no readable PDF still shows its parsed figures rather than vanishing.
  pdf_key       text,

  received_at   timestamptz NOT NULL DEFAULT now(),
  resolved_at   timestamptz,
  resolved_by   text,
  note          text
);

-- The dedupe key. Case- and space-insensitive on both halves because the two
-- parsers disagree about both, and a duplicate that slips through is a double
-- charge, not a cosmetic problem.
CREATE UNIQUE INDEX IF NOT EXISTS vendor_invoices_dedupe
  ON vendor_invoices (upper(regexp_replace(vendor_name, '[^A-Za-z0-9]', '', 'g')),
                      upper(regexp_replace(invoice_no,  '[^A-Za-z0-9]', '', 'g')));

-- The screen's only query: the pending queue, oldest first.
CREATE INDEX IF NOT EXISTS vendor_invoices_pending
  ON vendor_invoices (received_at) WHERE status = 'needs_review';

CREATE INDEX IF NOT EXISTS vendor_invoices_job ON vendor_invoices (job_id);

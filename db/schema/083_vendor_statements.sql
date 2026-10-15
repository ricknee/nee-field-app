-- 083_vendor_statements.sql — CED and Wolff monthly account STATEMENTS.
--
-- Owner, 2026-10-15: a ☰ → 📑 Statements screen for the statements the two
-- supply houses mail out. The MiniBee bot (the `cedautomation` employee, role
-- `office`) reads them out of Gmail and posts each PDF to `vendorStatementIntake`.
--
-- ⛔ A STATEMENT IS A REFERENCE DOCUMENT AND NOTHING ELSE. It never creates an
-- expense, never matches a job, never changes an invoice total, never enters
-- accounting, never prints by itself. That is why this is its own table and NOT
-- rows in vendor_invoices: every row there is an invoice on its way to becoming
-- money on a job, and a statement filed there would be one tap on "Add to job"
-- away from charging a job the whole month's balance. Nothing here references
-- jobs, expenses or vendor_invoices, and nothing there references this.
--
-- Because statements create no financial records, importing old ones later is
-- SAFE — unlike vendor_invoices, where "DO NOT BACKFILL" is the rule.
--
-- ── DEDUPE ─────────────────────────────────────────────────────────────────
-- The bot re-sends on a timer, so a repeat is the normal case. Two guards:
--   • content_hash — SHA-256 of the PDF bytes, computed SERVER-side. The same
--     statement forwarded twice, or re-attached to a second email, is one row.
--   • (gmail_message_id, original_filename) — the same attachment of the same
--     email. NOT gmail_message_id alone: one email can carry a statement per
--     branch/account, and keying on the message alone would keep the first
--     attachment and silently drop the rest.

CREATE TABLE IF NOT EXISTS vendor_statements (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Canonical, two values only. The bot's spelling ("CED CONSOLIDATED
  -- ELECTRICAL…", "Wolff Bros") is normalised on the way in; anything else is
  -- refused with a 400, not stored under whatever name arrived.
  vendor            text NOT NULL CHECK (vendor IN ('CED', 'Wolff')),

  -- When the bot could read it. NULL is allowed: a statement with no legible
  -- date is still worth keeping, and losing the PDF over a date would be worse.
  statement_date    date,
  received_at       timestamptz NOT NULL DEFAULT now(),
  original_filename text,
  gmail_message_id  text,
  content_hash      text NOT NULL,

  -- R2 key under `vendor-statements/<id>/`. Same top-level-prefix reasoning as
  -- vendor-invoices/: outside the recycle bin's `_deleted/` lifecycle rule by
  -- construction. NULL only transiently — if the R2 PUT failed, the bot's retry
  -- dedupes onto this row and intake stores the PDF then.
  pdf_key           text,

  status            text NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'reviewed')),
  reviewed_at       timestamptz,
  reviewed_by       text
);

CREATE UNIQUE INDEX IF NOT EXISTS vendor_statements_hash
  ON vendor_statements (content_hash);

CREATE UNIQUE INDEX IF NOT EXISTS vendor_statements_gmail
  ON vendor_statements (gmail_message_id, lower(COALESCE(original_filename, '')))
  WHERE gmail_message_id IS NOT NULL;

-- The badge's count.
CREATE INDEX IF NOT EXISTS vendor_statements_new
  ON vendor_statements (received_at) WHERE status = 'new';

COMMENT ON TABLE vendor_statements IS
  'Supplier account statements (CED, Wolff). REFERENCE ONLY: never an expense, never on a job, never in any total. See db/schema/083.';

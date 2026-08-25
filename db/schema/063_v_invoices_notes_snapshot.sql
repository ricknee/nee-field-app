-- 063_v_invoices_notes_snapshot.sql — the invoice history panel was ALWAYS empty
-- on a native job, and always served from Airtable on every other job.
-- Found 2026-08-25 from a screenshot: "Invoice History for this Job (none yet)"
-- on a job with four saved invoices.
--
-- ── THE BUG WAS NOT IN THE HANDLER ─────────────────────────────────────────
-- Both invoice reads select these columns:
--
--   handleGetJobInvoices  (airtable.js ~12193)   v.invoice_notes, v.invoice_snapshot
--   handleJobInvoices     (airtable.js ~12386)   invoice_notes, invoice_snapshot
--
-- `v_invoices` did not expose either one. The base table has them; the view was
-- never widened when they were added. So every call raised
--
--   column "invoice_notes" does not exist
--
-- and `neonQuery` returned an error rather than rows. Both handlers then do what
-- this codebase asks of them — log and fall back to Airtable:
--
--   if (q?.error) console.error(`getJobInvoices: Neon read failed, falling back...`)
--
-- On a LEGACY job the fallback works: the invoice has a rec id and a Job link,
-- so the list comes back and nobody notices the Neon path is dead. On a NATIVE
-- job Airtable holds no invoice for that uuid, so the fallback returns [] and
-- the panel prints "(none yet)" — the exact words in the screenshot.
--
-- ⚠⚠ THE SHAPE WORTH REMEMBERING: a fail-soft fallback turned a hard SQL error
-- into an empty list. The error was in the logs the whole time; the screen said
-- "no invoices", which reads as data, not as a fault. Every read in this file
-- with an Airtable fallback can do this, and only the native rows expose it —
-- because for them the fallback has nothing to hide behind.
--
-- ⚠ Rebuilt from `pg_get_viewdef` and NOT from 015/055, per the standing rule:
-- the .sql files for views are stale, and a rewrite from disk would have
-- reverted the slice-3 money CTEs. Two columns appended at the end, which is all
-- CREATE OR REPLACE VIEW allows, and nothing else changed. Verified after: the
-- exact handler SELECT returns all five of Test 10's invoices with notes and a
-- non-null snapshot (the reprint payload).

DO $$
DECLARE d text; marker text; repl text;
BEGIN
  d := pg_get_viewdef('v_invoices'::regclass, true);
  marker := 'AS invoice_total_calc' || E'\n' || '   FROM invoices i';
  repl   := 'AS invoice_total_calc,' || E'\n' || '    i.invoice_notes,' || E'\n' || '    i.invoice_snapshot' || E'\n' || '   FROM invoices i';
  -- Refuse to guess if the view has moved on. Better to fail than to rebuild it
  -- from a definition this file assumed.
  IF position(marker in d) = 0 THEN
    RAISE EXCEPTION 'v_invoices: anchor not found — inspect pg_get_viewdef before re-running';
  END IF;
  EXECUTE 'CREATE OR REPLACE VIEW v_invoices AS ' || replace(d, marker, repl);
END $$;

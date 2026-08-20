-- 045_job_automation_flags.sql — the run-once guards move to Neon
--
-- WHY: the "Airtable – Job Awarded" scenario (4509804) ends with two
-- airtable:ActionUpdateRecords modules. They are not bookkeeping — they write
-- the flags that stop the work happening twice:
--
--   Automation – TSheets Created   fldWDs8praJa3iGlf
--   Automation – Trello Created    fldlgoNEaus3XGJel
--   Automation – pCloud Folders Created  fld8bPwqKtRXZJZTN  (app writes this one)
--   Automation – Trello Completed  fldewPWukfRLkgDCa
--
-- `fireJobStatusWebhooks` reads them straight off the Airtable PATCH response to
-- decide whether to fire at all. So deleting those Make modules — the last
-- Airtable dependency in that scenario — would leave the guard reading a value
-- nothing updates any more, and re-saving an awarded job would create a SECOND
-- Trello card and a SECOND QuickBooks Time jobcode. Real money, silently.
--
-- The order that makes it safe, and this file is step 1:
--   1. these columns exist and _jobs-sync.js carries them hourly from Airtable
--   2. the app writes the flag to Neon itself after a successful POST
--   3. the guard reads Neon first, falling back to the Airtable value while it
--      is still there — so a NULL can never be read as "not done yet"
--   4. only THEN can Make's two write modules go
--
-- ⚠ THE FIELD NAMES CONTAIN AN EN DASH (–), not a hyphen. Getting that wrong
-- reads undefined, which is falsy, which fires the webhook every single time.
--
-- ⚠ NO BACKFILL SCRIPT, deliberately. Adding these to JOB_FIELDS means the
-- hourly sync populates all 115 jobs from Airtable on its next run, which is the
-- same path that keeps them fresh afterwards. A separate one-off would be a
-- second definition of the same mapping.
--
-- `trello_po_card_id` is the SECOND Trello card ("Trello Card PO ID",
-- fldTWUzDcPB1EBnqS) — the copy filed on the shared Job PO list. Neon already had
-- `trello_card_id` for the contractor-list card; without this one the callback
-- would have nowhere to put the other id.

ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS pcloud_folders_created boolean,
  ADD COLUMN IF NOT EXISTS trello_created         boolean,
  ADD COLUMN IF NOT EXISTS tsheets_created        boolean,
  ADD COLUMN IF NOT EXISTS trello_completed       boolean,
  ADD COLUMN IF NOT EXISTS trello_po_card_id      text;

COMMENT ON COLUMN jobs.pcloud_folders_created IS
  'Run-once guard for the Estimating -> pCloud webhook. Written by the app after a successful POST.';
COMMENT ON COLUMN jobs.trello_created IS
  'Run-once guard for the Awarded -> Trello half. NULL means unknown, not "not done" — fall back to Airtable.';
COMMENT ON COLUMN jobs.tsheets_created IS
  'Run-once guard for the Awarded -> QuickBooks Time half. NULL means unknown, not "not done".';
COMMENT ON COLUMN jobs.trello_completed IS
  'Run-once guard for the Completed -> Trello "Completed by year" webhook.';
COMMENT ON COLUMN jobs.trello_po_card_id IS
  'Trello card on the shared Job PO list. jobs.trello_card_id is the contractor-list card.';

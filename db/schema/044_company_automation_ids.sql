-- 044_company_automation_ids.sql — per-contractor automation config into Neon
--
-- WHY: the "Airtable – Job Awarded" Make scenario (4509804) could not be freed
-- from Airtable the way the pCloud one was. pCloud only needed the contractor
-- NAME and the job PO, both of which the app already posts. Awarded also reads a
-- second Airtable record — the Companies row — purely for three configuration
-- values that exist nowhere else:
--
--   TSheets Group / Customer ID   fldIDvFjysBShODuk   -> the QuickBooks Time
--                                                        customer the new jobcode
--                                                        is created under
--   Trello List ID                fldQJTZM6JD4iGrDo   -> the contractor's own
--                                                        Trello list
--   Trello List Job PO ID         fldtofVE4A0mAF42o   -> the shared "Job PO" list
--
-- Without these in Neon there is no way for the app to hand them to Make, and no
-- way to delete that second Airtable read. This migration is that missing piece.
--
-- ⚠ `trello_list_job_po_id` is the SAME value on all 35 companies today
-- (60ef1561becb9f14e7a024a2). It is stored per-company anyway rather than
-- hardcoded, because the Airtable field is per-company and collapsing it to a
-- constant would quietly break the first time one contractor needed its own list.
--
-- ⚠ SEVEN COMPANIES HAVE NEITHER ID: JC Herbert, Marco Construction, Bunker Hill
-- Engine, Schlabach Equip., Springhill Construction, Mt Eaton Engine, Dollar
-- General. That is pre-existing — Airtable's Companies row is equally empty, so
-- the automation would have handed Make a blank list id too. Awarding a job for
-- one of them has always been broken; this does not change that, it just makes it
-- visible in a log line instead of a Make error. Fill them in when one comes up.
--
-- `pcloud_parent_folder_id` is carried along because it is the same class of
-- config read from the same row, and the backfill was free. NOTHING READS IT
-- TODAY — the pCloud scenario builds its paths from folder NAMES, not ids.

ALTER TABLE companies
  ADD COLUMN IF NOT EXISTS tsheets_group_id        text,
  ADD COLUMN IF NOT EXISTS trello_list_id          text,
  ADD COLUMN IF NOT EXISTS trello_list_job_po_id   text,
  ADD COLUMN IF NOT EXISTS pcloud_parent_folder_id text;

COMMENT ON COLUMN companies.tsheets_group_id IS
  'QuickBooks Time customer/group id. Make 4509804 creates the jobcode under this parent.';
COMMENT ON COLUMN companies.trello_list_id IS
  'This contractor''s own Trello list. Make 4509804 creates the contractor card here.';
COMMENT ON COLUMN companies.trello_list_job_po_id IS
  'Shared "Job PO" Trello list. Identical across all companies today; kept per-row to match Airtable.';
COMMENT ON COLUMN companies.pcloud_parent_folder_id IS
  'pCloud parent folder id. UNUSED — the pCloud scenario paths by name. Backfilled for completeness.';

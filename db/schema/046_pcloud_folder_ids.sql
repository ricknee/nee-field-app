-- 046_pcloud_folder_ids.sql — the rest of the pCloud folder ids
--
-- WHY: the "Airtable – Service Call" Make scenario (4545219) is the last of the
-- four job scenarios still reading and writing Airtable, and it is the biggest —
-- 25 modules, six of them Airtable. It builds a seven-folder pCloud tree and
-- writes every folder id back onto the Job:
--
--   pCloud Folder ID          fldoicx7bnb2Gdg1D   the job folder itself   <- NEW
--   pCloud Job Receipts ID    fld06WOq5dA4F9CUA                           <- NEW
--   pCloud Jobsite Files ID   fldn0dg7E42B2Pimg                           <- NEW
--   pCloud Invoices Sent ID   fldVtTkUcuh96TgXh   already pcloud_invoices_sent_id
--   pCloud Photo's ID         fld655NnOgjRhaVSe   already pcloud_photo_folder_id
--
-- Three had no Neon home, so the callback that replaces those Airtable writes
-- had nowhere to put them. This is that missing piece — the same shape as
-- db/schema/044 for the Awarded scenario.
--
-- ⚠ These are pCloud FOLDER IDS, not paths. The scenario builds paths by NAME
-- (".../<year>/<company>/<job PO>/…") and only stores the ids afterwards, which
-- is why a renamed company silently creates a new tree rather than moving one.
-- Storing the ids does not fix that; it just records where things went.

ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS pcloud_job_folder_id           text,
  ADD COLUMN IF NOT EXISTS pcloud_receipts_folder_id      text,
  ADD COLUMN IF NOT EXISTS pcloud_jobsite_files_folder_id text;

COMMENT ON COLUMN jobs.pcloud_job_folder_id IS
  'pCloud folder for the job itself ("<Job PO>"). Airtable: pCloud Folder ID.';
COMMENT ON COLUMN jobs.pcloud_receipts_folder_id IS
  'pCloud "Job Receipts" folder. Airtable: pCloud Job Receipts ID.';
COMMENT ON COLUMN jobs.pcloud_jobsite_files_folder_id IS
  'pCloud "<Job PO> Jobsite Files" folder — parent of Photos. Airtable: pCloud Jobsite Files ID.';

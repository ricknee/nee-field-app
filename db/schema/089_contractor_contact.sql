-- 089 — the contractor's contact person on a job (owner 2026-11-10).
--
-- A job had ONE set of contact boxes (customer_first_name / _last_name /
-- _phone / _email). They were used for whoever the job was dealt with — on
-- Sullivan Pullet that is Jeff Miller, who installs for Aviary Poultry, not the
-- barn owner. This adds a second, separate slot: a LINK to a person in
-- `contacts` (normally one filed under the job's contractor), so the job can
-- show the contractor's person AND the customer.
--
-- A link, not copied text: change a contact's phone once and every job shows
-- it. ON DELETE SET NULL — nothing deletes contacts today, but if something
-- ever does, the job loses the link rather than refusing the delete.
--
-- The customer_* columns are unchanged and still mean "the customer".
ALTER TABLE jobs
  ADD COLUMN IF NOT EXISTS contractor_contact_id uuid
  REFERENCES contacts(id) ON DELETE SET NULL;

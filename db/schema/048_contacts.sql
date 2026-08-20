-- 048_contacts.sql — the last domain with no Neon home
--
-- Item 06's final slice. Every other reference domain moved months ago; Contacts
-- stayed because the audit found it needed "a loader, not transcription", and
-- because BOTH tables were Google-sync triggers — their Airtable mirror writes
-- had to survive until item 07 replaced that sync.
--
-- ⚠ THAT SECOND CONSTRAINT IS GONE. All six Google contact-sync automations were
-- undeployed on 2026-08-20, so nothing is listening to these tables any more.
-- The mirror writes stay for now anyway (Airtable is still the identity authority
-- while `airtable_id` is what every client-side id is), but they are no longer
-- load-bearing for anything downstream.
--
-- Two tables, deliberately not one. They share a shape but nothing else: Contacts
-- hang off Companies and carry a postal address; Power Company Contacts hang off
-- Power Companies, carry two phone numbers and a job-role list, and have no
-- address at all. Merging them would mean half the columns null on every row.
--
-- ⚠ `power_contacts.name` is GENERATED, because Airtable's "Contact Name" is a
-- FORMULA (first + last). The read filters on it being non-empty, so a stored
-- copy that drifted out of step with the parts would silently drop people from
-- the picker. Let Postgres keep it true instead.
--
-- ⚠ Role / job_roles are Airtable multipleSelects. Stored as the joined display
-- string, matching what `g()` produces on the read path today — the flip must not
-- change what appears on screen.

CREATE TABLE IF NOT EXISTS contacts (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id         text UNIQUE,
  first_name          text,
  last_name           text,
  primary_phone       text,
  primary_email       text,
  company_airtable_id text,
  company_id          uuid REFERENCES companies(id),
  role                text,
  street              text,
  city                text,
  state               text,
  zip                 text,
  active              boolean NOT NULL DEFAULT true,
  synced_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS power_contacts (
  id                        uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id               text UNIQUE,
  first_name                text,
  last_name                 text,
  name                      text GENERATED ALWAYS AS (
                              btrim(coalesce(first_name, '') || ' ' || coalesce(last_name, ''))
                            ) STORED,
  cell_phone                text,
  office_phone              text,
  email                     text,
  power_company_airtable_id text,
  power_company_id          uuid REFERENCES power_companies(id),
  power_company_name        text,
  job_roles                 text,
  notes                     text,
  active                    boolean NOT NULL DEFAULT true,
  synced_at                 timestamptz NOT NULL DEFAULT now()
);

-- The picker reads are always "this company's ACTIVE contacts", so index the
-- pair rather than the link alone.
CREATE INDEX IF NOT EXISTS contacts_company_active_idx
  ON contacts (company_airtable_id) WHERE active;
CREATE INDEX IF NOT EXISTS power_contacts_company_active_idx
  ON power_contacts (power_company_airtable_id) WHERE active;

COMMENT ON TABLE contacts IS
  'Customer/company contacts. Airtable tbl7vZpySDNfZX9Sq. Airtable remains the identity authority while client-side ids are rec ids.';
COMMENT ON TABLE power_contacts IS
  'Power company contacts. Airtable tblvouoPMTYh27FGT.';
COMMENT ON COLUMN power_contacts.name IS
  'Generated, mirroring Airtable''s "Contact Name" formula. Never write it directly.';

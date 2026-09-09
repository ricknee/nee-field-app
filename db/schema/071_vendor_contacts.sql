-- 071_vendor_contacts.sql — the last 7 records that lived only in Airtable.
--
-- ── WHY, AND WHY NOW ───────────────────────────────────────────────────────
-- Written 2026-09-09 while answering "if I cancel Airtable, does anything
-- break?". The app: no. The DATA: all 43 Airtable tables were compared against
-- Neon's, and seven tables had no counterpart:
--
--   Pipe Price History  80 | Wire Price History 58 | Pipe Types 41
--   Wire Types          36 | Vendor Contacts     7 | Job Labor Summary 27
--   Job Vehicle Trips    0
--
--   * The wire/pipe pricing is the retired JotForm path. Owner's call: dead,
--     not used, not worth migrating. It goes with the base.
--   * Job Labor Summary is a ROLLUP (job × employee × hours), reproducible from
--     time_entries and job_labor_allocations. Nothing original in it.
--   * Job Vehicle Trips is empty.
--   * Vendor Contacts is 7 real supplier reps the owner may want later.
--
-- So this table exists to make the Airtable base disposable. It has no reader
-- yet, and that is fine — the alternative was losing the rows.
--
-- ── WHY NOT THE EXISTING `contacts` TABLE ──────────────────────────────────
-- `contacts` is customers and jobsite people — 241 rows, roles Customer /
-- Jobsite Contact / Owner / Foreman / Inspector, and `company_id` REFERENCES
-- `companies`. Vendors are not in `companies` (0 of its 35 rows are a supplier);
-- they are in `expense_vendors`. Forcing a supplier rep into `contacts` would
-- mean a NULL company link on a row whose whole point is which supplier it
-- belongs to. Different parent, different table.
--
-- ⚠⚠ THE GOOGLE IDS ARE THE REASON THIS IS NOT JUST A CSV EXPORT.
-- All 7 already exist in BOTH company address books, and
-- `google_person_id_1/2` are the only thing that makes a future sync an UPDATE
-- rather than a CREATE. Drop them and wiring vendor contacts into the Google
-- sync later would put 7 duplicates into each of two address books that are
-- live on people's phones — the same failure `docs/PLAN-google-contacts.md`
-- exists to prevent, in miniature. Column names match `contacts` exactly so the
-- existing id-first logic can be pointed here unchanged.
--
-- ⚠ Loaded VERBATIM, typos included. "Harold Gilbert" of Buckeye Power is
-- stored as first "Harlod", last "Buckeye Power" (his email is hgilbert@). That
-- is what Airtable holds. Silently correcting data during a migration is how you
-- lose the ability to reconcile it against the source — fix it in the app later,
-- deliberately, or not at all.

CREATE TABLE IF NOT EXISTS vendor_contacts (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id        text UNIQUE,

  first_name         text,
  last_name          text,
  role               text,
  primary_phone      text,
  office_phone       text,
  primary_email      text,

  -- Dual handle, same shape as everywhere else in this schema: the uuid is the
  -- real link, the rec id is kept so the row can still be traced back to the
  -- Airtable record it came from after the base is gone.
  vendor_id          uuid REFERENCES expense_vendors(id),
  vendor_airtable_id text,

  active             boolean NOT NULL DEFAULT true,
  notes              text,

  -- Google People API resource ids, BARE (no "people/" prefix) — matching
  -- `contacts`. _1 = rick@, _2 = nee@. See the warning above.
  google_person_id_1 text,
  google_person_id_2 text,

  synced_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS vendor_contacts_vendor ON vendor_contacts (vendor_id);

-- ── Inspections, Generators, Warranties → Neon (roadmap Step 4c) ───────────
-- Added 2026-08-06. Plan: docs/PLAN-4c-inspections-generators-warranties.md
--
-- ⚠ NOT YET APPLIED TO THE DEFAULT BRANCH — unlike 009/010, which say "APPLIED
-- BARE to the default branch". As of 2026-08-06 these tables exist ONLY on the
-- test branch br-aged-cake-ap0h78yk of damp-silence-99074350, where the ETL has
-- been run and verified. Production Neon has none of them. Apply to default as
-- part of the read-flip, not before — nothing reads these tables yet, so there
-- is no reason to carry them in production ahead of the handlers.
-- Apply BARE (the Neon MCP mangles inline SQL comments); reasoning stays here.
--
-- SEVEN tables, 75 rows total — smaller than fleet+lifts. The work here is
-- RELATIONAL, not volumetric: Generators alone carries 3 formulas and a rollup,
-- and Job Inspections is mostly lookups that become joins.
--
-- All seven tables are created together even though the HANDLER flips are
-- sliced (4c-1 .. 4c-4). The DDL for the two contact tables is not in doubt —
-- only the Google sync mechanism is, and that is a separate table shipped at
-- 4c-3. Creating them now keeps job_inspections' foreign keys resolvable.
--
-- ── WHAT IS DELIBERATELY NOT HERE ──────────────────────────────────────────
-- 1. ATTACHMENTS. Owner's call 2026-08-06. `Inspection Contacts.Files / Images`
--    is dead (0 of 5 rows populated, read by no code). `Job Inspections.
--    Attachments` holds 2 photos on 2 of 22 rows, also read by no code — let
--    them go; inspection photos belong in the existing job-photo path if they
--    are ever wanted. No column, no R2 copy. They survive in Airtable until
--    that table is retired, so this stays reversible for now.
-- 2. LOOKUP FIELDS. Generators stores ~11 fields that arrive through the Job
--    link (Customer, Customer Phone #, Site Address, Jobsite City/State/Zip,
--    Contractor …). None become columns — they are JOIN jobs in a view.
--    Copying them would recreate the duplication this migration exists to
--    remove.
-- 3. GOOGLE SYNC COLUMNS. Airtable duplicates six sync fields onto every
--    contact-bearing table. They collapse into ONE `google_contact_sync` table
--    at 4c-3. Until then those fields stay Make-owned in Airtable and the app
--    must not write them (standing rule, CLAUDE.md).

-- ── THE JOBS-FK LAG TRAP — same as panel schedules and checklists ──────────
-- The `jobs` table refreshes HOURLY. A job created ten minutes ago is not in
-- Neon yet, so a NOT NULL FK would refuse the first inspection or generator
-- anyone records against it. Everywhere a job is referenced below:
--   job_airtable_id  text NOT NULL   <- the real key, always present
--   job_id           uuid NULL       <- convenience FK, backfills hourly
-- See 007_panel_schedules.sql and 008_job_checklists.sql.

CREATE TABLE IF NOT EXISTS inspection_agencies (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id     text UNIQUE,
  name            text NOT NULL,
  phone           text,
  email           text,
  scheduling_link text,
  notes           text,
  -- Airtable's `Active` checkbox drove BOTH the agency picker and the Google
  -- sync trigger. Here it only means "show in the picker" — sync eligibility
  -- is a query against google_contact_sync at 4c-3, not a flag on the row.
  active          boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now(),
  -- updated_at exists specifically so the contact sync can ask
  -- "changed since last synced?" instead of Airtable's `Needs Sync to Google`
  -- FORMULA, which could drift out of step with the data it described.
  updated_at      timestamptz NOT NULL DEFAULT now(),
  synced_at       timestamptz
);

CREATE TABLE IF NOT EXISTS inspection_contacts (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id   text UNIQUE,
  -- ON DELETE SET NULL, not CASCADE: an inspector who outlives their agency is
  -- still a real person with a phone number. Only the affiliation is lost.
  agency_id     uuid REFERENCES inspection_agencies(id) ON DELETE SET NULL,
  first_name    text,
  last_name     text,
  -- Airtable's `Inspector Name` was a formula over the two name parts. Kept as
  -- a generated column so it cannot fall out of step, and so existing read code
  -- that expects one display name keeps working unchanged.
  -- NULLIF(...,'') guards the both-names-empty case -> NULL, not ' '.
  inspector_name text GENERATED ALWAYS AS (
    NULLIF(TRIM(COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')), '')
  ) STORED,
  phone         text,
  email         text,
  notes         text,
  active        boolean NOT NULL DEFAULT true,
  created_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now(),
  synced_at     timestamptz
);

CREATE INDEX IF NOT EXISTS inspection_contacts_agency ON inspection_contacts (agency_id);

-- Job Inspections carried BOTH `Inspection Agency` (a lookup) and `Inspection
-- Agency (Linked)` (a real record link). The LINK is migrated; the lookup was
-- only ever its shadow.
CREATE TABLE IF NOT EXISTS job_inspections (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id      text UNIQUE,
  job_airtable_id  text NOT NULL,
  job_id           uuid REFERENCES jobs(id),
  agency_id        uuid REFERENCES inspection_agencies(id) ON DELETE SET NULL,
  inspector_id     uuid REFERENCES inspection_contacts(id) ON DELETE SET NULL,
  inspection_type  text,
  inspection_date  date,
  inspection_status text,
  notes            text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  synced_at        timestamptz
);

CREATE INDEX IF NOT EXISTS job_inspections_job ON job_inspections (job_airtable_id);

CREATE TABLE IF NOT EXISTS generators (
  id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id             text UNIQUE,
  job_airtable_id         text NOT NULL,
  job_id                  uuid REFERENCES jobs(id),
  -- ⚠ SNAPSHOT, not a lookup — and it is load-bearing, not a convenience.
  -- Airtable's `Generator Asset ID` leads with {Customer}, a lookup through the
  -- Job link. Reproducing that as a pure join makes the asset id go NULL for up
  -- to an hour whenever a generator is recorded against a job the hourly sync
  -- has not seen yet (proved on the branch: job_id NULL -> asset_id NULL).
  -- A generator with no name on screen is a support call.
  -- Same pattern as time_entries.job_name, snapshotted from po_locked.
  customer_name           text,
  brand                   text,
  model                   text,
  kw                      numeric(8,2),
  serial_number           text,
  transfer_switch_model   text,
  transfer_switch_serial  text,
  fuel_type               text,
  install_date            date,
  service_plan_active     boolean NOT NULL DEFAULT false,
  service_interval_months int,
  warranty_expiration     date,
  status                  text,
  notes                   text,
  battery_install_date    date,
  service_call_created    boolean NOT NULL DEFAULT false,
  job_type                text,
  tax_status              text,
  billing_method          text,
  created_at              timestamptz NOT NULL DEFAULT now(),
  synced_at               timestamptz
);

CREATE INDEX IF NOT EXISTS generators_job ON generators (job_airtable_id);

CREATE TABLE IF NOT EXISTS generator_service (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id           text UNIQUE,
  -- CASCADE, unlike the agency link above: a service record describes work on a
  -- specific generator and is meaningless once that generator is gone.
  generator_id          uuid REFERENCES generators(id) ON DELETE CASCADE,
  job_airtable_id       text,
  job_id                uuid REFERENCES jobs(id),
  service_date          date,
  service_type          text,
  -- A NAME, not an employee FK. Matches how fleet_maintenance.performed_by and
  -- job_checklists.created_by already store this: the technician on an eight-row
  -- historical table is not worth a join, and Airtable stored it as a lookup.
  technician            text,
  service_plan_visit    boolean NOT NULL DEFAULT false,
  -- ⚠ NINE SEPARATE BOOLEANS, NOT a text[]. fleet_maintenance.service_types is
  -- an array because Airtable had it as ONE multipleSelects field. These are
  -- nine distinct named checkbox fields, so they map 1:1. Do not collapse them
  -- into an array by analogy with fleet.
  oil_changed           boolean NOT NULL DEFAULT false,
  oil_filter_changed    boolean NOT NULL DEFAULT false,
  air_filter_changed    boolean NOT NULL DEFAULT false,
  spark_plugs_changed   boolean NOT NULL DEFAULT false,
  battery_tested        boolean NOT NULL DEFAULT false,
  battery_replaced      boolean NOT NULL DEFAULT false,
  load_test_performed   boolean NOT NULL DEFAULT false,
  firmware_checked      boolean NOT NULL DEFAULT false,
  exercise_checked      boolean NOT NULL DEFAULT false,
  trouble_codes         text,
  work_performed_notes  text,
  parts_used            text,
  labor_hours           numeric(8,2),
  generator_hours       numeric(10,1),
  created_at            timestamptz NOT NULL DEFAULT now(),
  synced_at             timestamptz
);

CREATE INDEX IF NOT EXISTS generator_service_generator ON generator_service (generator_id);

CREATE TABLE IF NOT EXISTS warranty_templates (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id     text UNIQUE,
  template_name   text NOT NULL,
  brand           text,
  model           text,
  warranty_type   text,
  duration_months int,
  notes           text,
  active          boolean NOT NULL DEFAULT true,
  created_at      timestamptz NOT NULL DEFAULT now(),
  synced_at       timestamptz
);

CREATE TABLE IF NOT EXISTS warranties (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id     text UNIQUE,
  generator_id    uuid REFERENCES generators(id) ON DELETE CASCADE,
  -- SET NULL: retiring a template must not delete warranties issued from it.
  template_id     uuid REFERENCES warranty_templates(id) ON DELETE SET NULL,
  name            text,
  warranty_type   text,
  start_date      date,
  end_date        date,
  duration_months int,
  source          text,
  voided          boolean NOT NULL DEFAULT false,
  voided_reason   text,
  notes           text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  synced_at       timestamptz
);

CREATE INDEX IF NOT EXISTS warranties_generator ON warranties (generator_id);

-- ── DERIVED FIELDS LIVE HERE, NOT IN STORED COLUMNS ────────────────────────
-- Airtable computed four of these on the Generators row: `Last Service Date`
-- (rollup), `Next Service Due`, `Service Status` and `Battery Age` (formulas).
--
-- ⚠⚠ THE MONTH-ARITHMETIC GOTCHA APPLIES HERE — see
-- docs/PLAN-job-warranty-service-log.md §2. JS and Postgres disagree about what
-- "Jan 31 + 1 month" means. `next_service_due` and `warranty_expiration` are
-- BOTH month-add fields. Compute them in ONE place — this view — and never
-- re-derive them client-side, or the app and the database will quietly
-- disagree about when a generator is due for service.
-- The four Airtable expressions were read out of the base on 2026-08-06 and are
-- reproduced here EXACTLY rather than guessed. For the record:
--   Next Service Due  IF({Last Service Date},
--                        DATEADD({Last Service Date}, {Interval}, 'months'),
--                        IF({Install Date},
--                           DATEADD({Install Date}, {Interval}, 'months')))
--   Service Status    IF({Next} < TODAY(), "OVERDUE",
--                        IF({Next} <= DATEADD(TODAY(),21,'days'), "DUE SOON", "OK"))
--   Battery Age       DATETIME_DIFF(TODAY(), {Battery Install Date}, 'years')
--   Asset ID          {Customer} & " - " & {KW} & "KW " & {Brand}
--
-- ⚠ `Asset ID` was NOT brand+serial, which is the obvious guess and is wrong.
-- It leads with {Customer} — a LOOKUP through the Job link — which is why this
-- has to be a view over a join and could never have been a stored column.
CREATE OR REPLACE VIEW v_generators AS
SELECT
  g.*,
  s.last_service_date,
  -- Reproduces "Betty Huber - 20KW Cummins".
  --
  -- ⚠ TWO THINGS THE BRANCH TEST CAUGHT, both of which would have shipped:
  --
  -- 1. trim_scale(). `kw` is numeric(8,2), so a bare ::text renders "20.00KW"
  --    where Airtable shows "20KW" — a visible change to every asset id in the
  --    system. trim_scale drops the trailing zeros without touching 12.5.
  -- 2. COALESCE onto the snapshot FIRST. Joining jobs alone produced a NULL
  --    asset id for a generator whose job had not yet reached Neon. The join is
  --    kept only as a repair path for rows migrated before the snapshot column
  --    existed.
  COALESCE(
    g.customer_name,
    NULLIF(TRIM(COALESCE(j.customer_first_name,'') || ' ' ||
                COALESCE(j.customer_last_name,'')), '')
  ) || ' - ' || trim_scale(g.kw)::text || 'KW ' || g.brand    AS asset_id,
  d.next_service_due,
  -- Threshold is 21 DAYS. Verified against live data: 6 of 11 generators read
  -- OVERDUE today and none are DUE SOON, so this branch is currently unexercised
  -- on production — check it deliberately rather than trusting a green screen.
  CASE
    WHEN d.next_service_due IS NULL                                THEN NULL
    WHEN d.next_service_due <  CURRENT_DATE                        THEN 'OVERDUE'
    WHEN d.next_service_due <= CURRENT_DATE + INTERVAL '21 days'   THEN 'DUE SOON'
    ELSE 'OK'
  END AS service_status,
  -- DATETIME_DIFF(..,'years') counts COMPLETE years, which is age(), not a
  -- /365.25 division — those disagree either side of a birthday.
  CASE WHEN g.battery_install_date IS NULL THEN NULL
       ELSE EXTRACT(YEAR FROM age(CURRENT_DATE, g.battery_install_date))::int
  END AS battery_age_years
FROM generators g
LEFT JOIN jobs j ON j.id = g.job_id
LEFT JOIN (
  SELECT generator_id, MAX(service_date) AS last_service_date
  FROM generator_service GROUP BY generator_id
) s ON s.generator_id = g.id
LEFT JOIN LATERAL (
  SELECT CASE WHEN g.service_interval_months IS NULL THEN NULL
              ELSE (COALESCE(s.last_service_date, g.install_date)
                    + make_interval(months => g.service_interval_months))::date
         END AS next_service_due
) d ON true;

-- ⚠ THIS VIEW SELECTS g.* — SO `CREATE OR REPLACE` WILL FAIL ON ANY FUTURE
-- COLUMN ADD. Adding a column to `generators` shifts the expanded position of
-- everything after it, and Postgres refuses to rename a view column in place
-- ("cannot change name of view column"). DROP VIEW first, then CREATE. Found
-- the hard way adding customer_name.

-- ── VERIFIED ON NEON BRANCH br-aged-cake-ap0h78yk, 2026-08-06 ──────────────
-- Five fixture generators reproducing real production rows. All four derived
-- fields match Airtable exactly:
--
--   Betty Huber  last svc 2026-05-12 -> next 2027-05-12, OK       (Airtable: same)
--   Jared Sargent last svc 2026-03-13, installed 2025-05-22
--                                     -> next 2027-03-13, OK      (Airtable: same)
--                 ^ proves last-service WINS over install date
--   Kevin Price  no service, installed 2024-08-12
--                                     -> next 2025-08-12, OVERDUE (Airtable: same)
--   12.5 KW      -> "12.5KW", not "12.50KW"  (trim_scale, both integer + decimal)
--   Due Soon     next 2026-08-20, 14 days out -> DUE SOON
--                 ^ THE 21-DAY BRANCH. Unexercised on production — all 11 live
--                   generators are OK or OVERDUE — so it is only ever going to
--                   be tested here. It is tested here.
--
-- ⚠ ONE DELIBERATE DEVIATION, affecting zero rows today.
-- When `Next Service Due` is blank, Airtable's comparison chain falls through
-- to the ELSE and reports **"OK"** — i.e. a generator with no service interval
-- looks healthy rather than unknown. This view returns NULL instead, so the UI
-- can render "—" and not a false reassurance.
--
-- Safe to change because ALL 11 live generators have interval = 12, so no
-- current value moves. If a generator is ever added without an interval, this
-- is the line that makes it visible instead of silently "OK".

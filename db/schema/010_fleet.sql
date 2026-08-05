-- ── Fleet → Neon (roadmap Step 4b, fleet half) ─────────────────────────────
-- Added 2026-08-05, right after the lifts half and following the same pattern.
-- APPLIED BARE to the default branch of damp-silence-99074350 (the Neon MCP
-- mangles inline SQL comments); reasoning kept here.
--
-- THREE tables: Fleet Vehicles (11 rows, 9 photos), Fleet Maintenance (91) and
-- Fleet Mileage Log (8). `Job Vehicle Trips` is deliberately NOT migrated — it
-- has 0 rows and no handler in either SPA references it.
--
-- Photos are in R2 under fleet/<uuid>/, copied by the admin action
-- `copyFleetPhotosToR2`, for the same reason as lifts: Airtable serves
-- attachments on SIGNED URLS THAT EXPIRE (~2 h), so a stored URL breaks the same
-- afternoon. See db/schema/009_scissor_lifts.sql for the full note.
CREATE TABLE IF NOT EXISTS fleet_vehicles (
  id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id    text UNIQUE,
  name           text NOT NULL,
  year           int,
  make           text,
  model          text,
  color          text,
  vin            text,
  plate          text,
  vehicle_type   text,
  current_mileage int,
  mileage_date   date,
  oil_type       text,
  oil_capacity   numeric(6,2),
  tire_brand     text,
  tire_size      text,
  tire_install_date date,
  notes          text,
  wrench_size    text,
  lug_torque     numeric(8,2),
  -- The vehicle list returns ACTIVE only, matching the Airtable path: a sold
  -- truck leaves the list but keeps its service history.
  active         boolean NOT NULL DEFAULT true,
  created_at     timestamptz NOT NULL DEFAULT now(),
  synced_at      timestamptz
);

-- ⚠ A REAL FOREIGN KEY — the point of this half of the migration.
-- Airtable linked service records to a vehicle by NAME:
--   handleFleetServiceHistory filtered {Vehicle}="<name>", interpolated UNESCAPED.
-- Two trucks named alike, or a rename, and the history follows the wrong one;
-- an apostrophe in a name broke the formula outright. Here it is a uuid FK, and
-- the branch test proves it: a second vehicle created with an identical name
-- gets its own empty history rather than inheriting the first one's.
--
-- ON DELETE CASCADE so retiring a vehicle cannot strand its service history.
CREATE TABLE IF NOT EXISTS fleet_maintenance (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id   text UNIQUE,
  vehicle_id    uuid REFERENCES fleet_vehicles(id) ON DELETE CASCADE,
  service_date  date,
  mileage       int,
  -- Airtable multipleSelects. A real array, so "did this service include an oil
  -- change" is a containment test rather than a string search.
  service_types text[] NOT NULL DEFAULT '{}',
  filter_no     text,
  oil_type_used text,
  oil_qty       numeric(6,2),
  tire_brand    text,
  tire_size     text,
  cost          numeric(12,2),
  performed_by  text,
  shop          text,
  notes         text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  synced_at     timestamptz
);

CREATE TABLE IF NOT EXISTS fleet_mileage_log (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id  text UNIQUE,
  vehicle_id   uuid REFERENCES fleet_vehicles(id) ON DELETE CASCADE,
  log_date     date,
  mileage      int,
  recorded_by  text,
  notes        text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  synced_at    timestamptz
);

-- ── Logging mileage is now ATOMIC ──────────────────────────────────────────
-- handleLogMileage always did two things: append to the log AND update the
-- vehicle's current reading. In Airtable those were two round-trips that could
-- half-succeed, leaving a log entry the odometer never caught up with. In Neon
-- it is one statement:
--
--   WITH ins AS (INSERT INTO fleet_mileage_log ... RETURNING id),
--        upd AS (UPDATE fleet_vehicles SET current_mileage = ..., mileage_date = ...)
--   SELECT id FROM ins;
--
-- A data-modifying CTE runs even when nothing references it, so `upd` fires.
--
-- ⚠ TRAP FOUND BY THE BRANCH TEST: handleLogMileage carried a guard demanding a
-- `rec…` prefix on vehicleId. The moment the vehicle list started returning Neon
-- uuids that guard rejected every truck with "Invalid vehicleId". It now accepts
-- both forms. Worth checking for the same shape in any handler migrated later.
--
-- Backfill + acceptance checks: db/etl/fleet.mjs
-- First load 2026-08-05: 11 vehicles (10 active), 91 service records, 8 mileage
-- entries, and ZERO child rows failed to resolve to a vehicle.

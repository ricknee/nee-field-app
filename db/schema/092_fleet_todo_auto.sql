-- 092: automatic items on the Fleet "Needs fixing" list (owner 2026-11-12).
--
-- First one: tire rotation every 7,000 miles. When a truck's mileage is saved
-- and it is 7,000+ past its last "Tire Rotation" / "Tires Replaced" service
-- (or 7,000+ on the odometer with none on record), the app adds a
-- "Rotate tires" item to the list.
--
--   auto_key   — which rule made the row ('tire_rotation'); NULL = typed by hand.
--   service_id — ticking an auto row logs the service in fleet_maintenance;
--                this is that record, so un-ticking can take it back out.
--
-- The partial unique index is the de-dupe: at most ONE open auto row per truck
-- per rule, so saving mileage twice never adds it twice (ON CONFLICT DO NOTHING).

ALTER TABLE fleet_todos ADD COLUMN IF NOT EXISTS auto_key text;
ALTER TABLE fleet_todos ADD COLUMN IF NOT EXISTS service_id uuid
  REFERENCES fleet_maintenance(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX IF NOT EXISTS fleet_todos_auto_open_uq
  ON fleet_todos (vehicle_id, auto_key)
  WHERE done_at IS NULL AND auto_key IS NOT NULL;

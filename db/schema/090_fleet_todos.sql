-- 090 — fleet "Needs fixing" checklist (owner 2026-11-11).
--
-- Owner: "a place under fleet that has a running checklist of things that need
-- to be done — for instance we need to fix the driver's mirror."
--
-- One row per thing to fix. vehicle_id NULL means it isn't tied to one truck
-- (shop / whole fleet). A tick sets done_at/done_by rather than deleting, so
-- the Done fold can show what was fixed and an accidental tick can be undone.
-- added_by / done_by are display names (actorName), like scissor_lift_history.
--
-- ON DELETE CASCADE — matches fleet_maintenance: a vehicle row is never
-- deleted today (sold trucks go active = false), but if one ever is, its
-- to-dos go with it.
CREATE TABLE IF NOT EXISTS fleet_todos (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  vehicle_id  uuid REFERENCES fleet_vehicles(id) ON DELETE CASCADE,
  task        text NOT NULL,
  added_by    text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  done_at     timestamptz,
  done_by     text
);

CREATE INDEX IF NOT EXISTS fleet_todos_open_idx ON fleet_todos (created_at) WHERE done_at IS NULL;

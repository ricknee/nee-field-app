-- 105: Machines in Lifts & Equipment (owner 2026-11-23, JCB 35Z-1 mini ex).
-- A row in scissor_lifts is now either a 'lift' or a 'machine'. A machine has an
-- hour meter, a service plan (what to do every how many hours) and a service log.
-- Lifts are unchanged (kind defaults to 'lift').
ALTER TABLE scissor_lifts ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'lift';
ALTER TABLE scissor_lifts DROP CONSTRAINT IF EXISTS scissor_lifts_kind_chk;
ALTER TABLE scissor_lifts ADD CONSTRAINT scissor_lifts_kind_chk CHECK (kind IN ('lift', 'machine'));
ALTER TABLE scissor_lifts ADD COLUMN IF NOT EXISTS hour_meter numeric(9,1);
ALTER TABLE scissor_lifts ADD COLUMN IF NOT EXISTS hour_meter_on date;
-- [{ "item": "Engine oil & filter", "everyHours": 500 }, ...]
ALTER TABLE scissor_lifts ADD COLUMN IF NOT EXISTS service_plan jsonb;

CREATE TABLE IF NOT EXISTS equipment_service (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lift_id      uuid NOT NULL REFERENCES scissor_lifts(id) ON DELETE CASCADE,
  service_date date NOT NULL,
  hours        numeric(9,1),
  items        text[] NOT NULL DEFAULT '{}',
  notes        text,
  done_by      text,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS equipment_service_lift ON equipment_service (lift_id, service_date DESC);

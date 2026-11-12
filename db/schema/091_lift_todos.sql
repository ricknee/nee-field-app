-- 091 — scissor lift "Needs fixing" checklist (owner 2026-11-12: "same edits
-- to this as fleet"). Same shape as fleet_todos (090); see that file.
-- lift_id NULL = not tied to one lift. ON DELETE CASCADE: retiring a sold lift
-- (deleteScissorLift) takes its to-dos with it, like its photos.
CREATE TABLE IF NOT EXISTS lift_todos (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  lift_id     uuid REFERENCES scissor_lifts(id) ON DELETE CASCADE,
  task        text NOT NULL,
  added_by    text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  done_at     timestamptz,
  done_by     text
);

CREATE INDEX IF NOT EXISTS lift_todos_open_idx ON lift_todos (created_at) WHERE done_at IS NULL;

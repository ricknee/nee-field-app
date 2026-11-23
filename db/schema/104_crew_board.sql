-- 104: Crew board (owner 2026-11-23).
-- Announcements and shop tasks the office posts for the crew. Employees see them
-- read-only on their home screen; admin + office post and take them down from the
-- Overview. Taking one down sets archived_at (kept for the record, never deleted).
CREATE TABLE IF NOT EXISTS crew_board (
  id          bigserial PRIMARY KEY,
  kind        text NOT NULL DEFAULT 'note' CHECK (kind IN ('note', 'task')),
  body        text NOT NULL CHECK (length(btrim(body)) > 0),
  created_by  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  archived_at timestamptz,
  archived_by text
);
CREATE INDEX IF NOT EXISTS crew_board_active ON crew_board (created_at DESC) WHERE archived_at IS NULL;

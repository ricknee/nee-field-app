-- 107 — "✓ Clear" on the Overview's app-error line (owner 2026-11-24).
-- Clearing hides errors from ☀ This morning; the rows stay for the log.
ALTER TABLE client_errors ADD COLUMN IF NOT EXISTS cleared_at timestamptz;

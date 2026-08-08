-- ── Employee admin: session revocation + employment dates ──────────────────
-- Slice 1 of docs/PLAN-employee-admin.md.
--
-- ── WHY THIS EXISTS: unchecking "Active" does not log anyone out ────────────
-- Session tokens are stateless HMAC (_auth.js) with a 30-DAY TTL, and
-- verifyToken reads no database at all — it checks a signature and an expiry.
-- Both handleLogin's do check `Active`, so unchecking it blocks a NEW login and
-- has zero effect on a device already signed in. Someone who quits keeps full
-- field-app access, on the phone in their pocket, for up to a month. Deleting
-- their Airtable record doesn't help either; nothing re-reads it.
--
-- `token_valid_from` is the fix: a per-person "sessions issued before this
-- moment are dead" stamp, compared against the token's own `iat`. See
-- netlify/functions/_revocation.js.
--
-- ── WHY THESE COLUMNS ARE SAFE TO ADD NOW, BUT `active` IS NOT ──────────────
-- db/etl/time-entries-full.mjs:241-247 is a LIVE dimension load that upserts
-- employees with ON CONFLICT DO UPDATE SET name, username, role, active from
-- Airtable — its own comment says those "stay live even when" the rest of the
-- ETL is skipped. An active=false written here would be ERASED by the next run.
-- That is why the People screen writes `active` to AIRTABLE, not to Neon.
--
-- The columns below are safe precisely because that upsert names its columns
-- explicitly and does not touch them. Airtable has no equivalent of any of
-- them, so nothing upstream can clobber them.
--
--   ⚠ When login flips to Neon (ROADMAP §4, "(Shared, last) — Login"), drop the
--   Airtable half of the People writes and retire that dimension load IN THE
--   SAME COMMIT. Until then, Airtable owns `active` and Neon owns these five.

ALTER TABLE employees ADD COLUMN IF NOT EXISTS hired_on         date;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS terminated_on    date;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS termination_note text;

-- Sessions issued strictly BEFORE this instant are rejected. NULL for everyone
-- normally — which is the whole point: the revocation query below returns zero
-- rows on a healthy day, so the check costs nothing.
--
-- timestamptz, and written from the FUNCTION's clock rather than Postgres
-- now(), so it is compared against a token `iat` produced by the same family of
-- clock. The two are within milliseconds either way, but there is no reason to
-- introduce a second clock into a comparison that decides access.
ALTER TABLE employees ADD COLUMN IF NOT EXISTS token_valid_from timestamptz;

-- Stamped by handleLogin. The only way to spot an account nobody has used in a
-- year — which is exactly the account worth turning off.
ALTER TABLE employees ADD COLUMN IF NOT EXISTS last_login_at    timestamptz;

-- Partial index: the revocation loader reads ONLY the non-NULL rows, and there
-- will never be many. Keeps the once-a-minute refresh an index-only scan
-- instead of a seq scan over the whole table.
CREATE INDEX IF NOT EXISTS employees_token_valid_from_idx
  ON employees (airtable_id, token_valid_from)
  WHERE token_valid_from IS NOT NULL;

COMMENT ON COLUMN employees.token_valid_from IS
  'Session revocation stamp. Tokens with iat < this are rejected by _revocation.js. NULL = not revoked. Set when an employee is deactivated or force-logged-out.';
COMMENT ON COLUMN employees.terminated_on IS
  'Set when deactivated via the People screen. Deactivation is NOT deletion — time entries, expenses, payroll and bonus history all stay linked and keep reporting.';

-- ── Employees: the rest of the attributes, ahead of the login flip ─────────
-- Stage 1 of the employees/login migration (ROADMAP §4, "(Shared, last) —
-- Login"). Plan: docs/PLAN-employee-admin.md.
--
-- ── WHAT THIS IS FOR ───────────────────────────────────────────────────────
-- Employees are the LAST Airtable-owned dimension in the field app. 16 read
-- sites across both functions plus both handleLogin's still read the Airtable
-- `Employees` table. Neon's copy has only enough to be a foreign key —
-- name, username, role, active — which is not enough to log anybody in.
--
-- This adds the missing attributes so Neon holds a COMPLETE employee record.
-- Nothing reads them yet: this migration is deliberately inert, and the app
-- behaves identically before and after it. That is the point — it can ship,
-- sit, and be reverted without consequence, and the risky part (moving login
-- for BOTH apps at once) happens later against data already proven correct.
--
-- ── PINs ARE STORED AS PLAINTEXT, DELIBERATELY ─────────────────────────────
-- The roadmap's plan was to hash during this flip ("rather than moving twice").
-- Owner decision 2026-08-08: **keep them readable.** The People screen's
-- "Show PIN" is a workflow they asked for and use — an admin reads a forgotten
-- PIN back to someone over the phone — and a hash cannot be un-hashed.
--
-- This is not a downgrade. PINs are ALREADY plaintext in Airtable, readable by
-- any admin who opens the grid, so Neon is no worse than the status quo it
-- replaces. Hashing stays available as a later, separate decision: add
-- `pin_hash`, migrate on next login or next reset, then drop this column and
-- retire `handleEmployeePin`. Written down so that path is obvious later.
--
--   ⚠ A 4-8 digit PIN is weak against offline brute force whether hashed or
--   not (10k-100M candidates). The real protections here are that the app is
--   internal, tokens expire in 30 days, and revocation now works.

ALTER TABLE employees ADD COLUMN IF NOT EXISTS pin         text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS email       text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS phone       text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS employee_no text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS labor_type  text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS notes       text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS first_name  text;
ALTER TABLE employees ADD COLUMN IF NOT EXISTS last_name   text;

-- handleLogin matches an identifier against name / username / email, so the
-- flipped version will filter on exactly these three. Lower-cased because the
-- Airtable version normalises before comparing and the Neon one must match
-- that behaviour precisely — a login that works in one and not the other is
-- the worst possible outcome of this migration.
CREATE INDEX IF NOT EXISTS employees_login_name_idx     ON employees (lower(name));
CREATE INDEX IF NOT EXISTS employees_login_username_idx ON employees (lower(username));
CREATE INDEX IF NOT EXISTS employees_login_email_idx    ON employees (lower(email));

-- Two people sharing a PIN means either can log in AS the other, because login
-- matches identifier + PIN. That was live on 2026-08-08 (an admin and two
-- office users all on 1184) and is now refused by handleSetEmployeePin.
--
-- NOT a UNIQUE constraint, on purpose: the existing rows still collide, and a
-- constraint that fails the backfill would block the migration on a data
-- cleanup the owner has explicitly declined. This view makes the collisions
-- visible instead, so the flip can assert on it rather than discover it.
CREATE OR REPLACE VIEW v_employee_pin_collisions AS
SELECT pin, count(*) AS holders, array_agg(name ORDER BY name) AS names
  FROM employees
 WHERE pin IS NOT NULL AND pin <> '' AND active
 GROUP BY pin
HAVING count(*) > 1;

COMMENT ON COLUMN employees.pin IS
  'Plaintext, matching Airtable. Owner decision 2026-08-08 to keep PINs readable so the People screen can show them. To hash later: add pin_hash, migrate on next login/reset, drop this, retire handleEmployeePin.';

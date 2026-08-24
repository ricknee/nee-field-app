-- 059_expense_push_native.sql — identity cutover, slice 4c (the inventory push).
-- See docs/PLAN-airtable-identity-cutover.md.
--
-- ── WHAT THIS SLICE ACTUALLY CHANGES ────────────────────────────────────────
-- `handlePushExpenses` (netlify/functions/inventory.js) was the inventory app's
-- LAST direct Airtable write, and it existed for exactly one reason: Airtable
-- was the identity authority for expenses. Slice 4b ended that for the field
-- app (a04b11f + 058); this ends it for the push, so the app now makes zero
-- writes to any Airtable base.
--
-- ── THERE IS NO `ALTER TABLE` HERE, AND THAT IS CHECKED, NOT ASSUMED ────────
-- `airtable_id NOT NULL` IS the checklist for a native cutover — 057 skipped it
-- and the first real expense after the deploy failed on the constraint. It is
-- absent here because 058 already dropped it on this very table, verified:
--
--   SELECT attnotnull FROM pg_attribute
--    WHERE attrelid = 'expenses'::regclass AND attname = 'airtable_id';  -- false
--
-- Nothing else the push writes gains a native row: `expense_pushes`,
-- `expense_push_lines` and `inventory_transactions` all went native in the
-- inventory write cutover (2026-08-12, schema 032/038) and their airtable_id
-- columns have been nullable since.
--
-- ── THE INDEX IS THE POINT OF THIS FILE ─────────────────────────────────────
-- Guard #1 ("this exact push already created its Expenses") used to be an
-- Airtable `filterByFormula` on {Push ID}. It is now a Neon read of
-- `expenses.push_id`, which had no index — the column was written by the sync
-- and never filtered on. Same shape as slice 2, where moving a join from
-- `airtable_id` to the uuid left `pr_bonuses_run_uuid_idx` needing to be added:
-- ⚠⚠ WHEN A LOOKUP MOVES TO A NEW COLUMN, THE INDEX DOES NOT FOLLOW IT.
--
-- Partial because 387 of 408 expenses carry no push id — they were entered in
-- the field app, not pushed from inventory.
CREATE INDEX IF NOT EXISTS expenses_push_id_idx
    ON expenses (push_id) WHERE push_id IS NOT NULL;

-- ── EQUIVALENCE, PROVED BEFORE THE READ MOVED (2026-08-24) ──────────────────
-- The guard is the only thing standing between a retried push and charging a
-- customer twice, so "Neon knows the same push ids Airtable does" was proved,
-- not assumed: all 21 Airtable Expenses with a non-empty {Push ID} were diffed
-- against `SELECT airtable_id, push_id FROM expenses WHERE push_id IS NOT NULL`
-- with EXCEPT in both directions. Zero rows either way, on (rec id, push id)
-- pairs — not just counts. The move is therefore provably inert on today's data
-- and correct once native rows exist.
--
-- ⚠ That equivalence holds because `syncExpenseToNeon` has copied `Push ID`
-- into Neon since Step E. It is a snapshot of 2026-08-24, and it stops being
-- re-provable the moment the base is archived — which is fine, because from
-- this slice on Neon is the only writer of the column.

COMMENT ON COLUMN expenses.push_id IS
  'Idempotency key of the inventory materials push that created this expense (NULL on a field-app expense). Since slice 4c the push writes it into Neon FIRST and guard #1 reads it back from here, so this column — not Airtables {Push ID} — is what prevents the same material being charged to a job twice. The Airtable mirror still carries a copy while the base lives; nothing reads it.';

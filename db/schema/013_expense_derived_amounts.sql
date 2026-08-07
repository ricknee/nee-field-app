-- ── Expenses: the four Airtable money formulas, ported (roadmap Step 4d) ───
-- Verified on branch br-aged-cake-ap0h78yk 2026-08-07. NOT yet applied to the
-- default branch — that happens with the handler flip, same as 011.
--
-- ── WHAT THE ROADMAP GOT WRONG ─────────────────────────────────────────────
-- §3 calls expenses "money, but plain arithmetic rather than rollup formulas."
-- It is not: **14 of 33 fields on the Airtable Expenses table are derived**, and
-- `Total Cost (Actual)` — the figure GP consumes — sits inside a 4-level chain:
--
--   Material Cost       = IF(WireCost, WireCost, IF(PipeCost, PipeCost, Manual))
--   Total Cost (Actual) = Material Cost - Material Credit
--   Billable Material $ = Material Cost * (1 + Job Markup %) - Material Credit
--   Unbilled Material $ = MAX(0, Billable - Billed Material $)
--
-- All four reproduce EXACTLY against all 386 live rows (0 mismatches), including
-- Airtable's IF() treating 0 as FALSY — "wire if non-zero", not COALESCE.
--
-- ── BUT THE WIRE/PIPE PATH IS DEAD, AND THAT COLLAPSES THE WHOLE CHAIN ─────
-- Owner, 2026-08-07: wire and pipe data comes from the INVENTORY APP now; those
-- Airtable tables are legacy. The data agrees — last wire-costed expense
-- 2026-04-14, last pipe 2026-02-18, and all 362 rows since are manual-only.
-- (Matches the JotForm retirement: scenarios 4522313/4527034 paused 2026-06-06.)
--
-- So for every current and future expense: **material cost IS
-- manual_material_cost**, a stored column. The chain really is plain arithmetic
-- over three stored inputs — manual_material_cost, material_credit, and
-- jobs.markup_pct. `legacy_material_cost` below exists ONLY so the 24 pre-April
-- rows keep the value the retired path produced; it is never set on a new row
-- and so can never affect one.
--
--   ⚠ Do NOT drop the Airtable wire/pipe tables without keeping this fallback.
--   Those 24 rows would recompute to manual_material_cost, which is a DIFFERENT
--   (rounded, sometimes zero) number. That is the "GP-formula risk before any
--   delete" the TODO has been warning about, now quantified.
ALTER TABLE expenses ADD COLUMN IF NOT EXISTS legacy_material_cost numeric(14,4);

-- ⚠ numeric(14,4), NOT (14,2). Wire costs carry SUB-CENT precision — they are
-- weight x price, e.g. 234.6243. Storing at 2dp truncated it and threw 4 rows
-- off by a cent on billable. Money columns fed to a formula need more than
-- cent precision; only the FINAL result should be rounded to cents.

CREATE OR REPLACE VIEW v_expenses AS
SELECT e.*,
       COALESCE(e.legacy_material_cost, e.manual_material_cost, 0)::numeric(14,2) AS material_cost_calc,
       (COALESCE(e.legacy_material_cost, e.manual_material_cost, 0)
        - COALESCE(e.material_credit,0))::numeric(14,2) AS total_cost_actual_calc,
       (COALESCE(e.legacy_material_cost, e.manual_material_cost, 0) * (1 + COALESCE(j.markup_pct,0))
        - COALESCE(e.material_credit,0))::numeric(14,2) AS billable_material_amount_calc,
       -- Sub-cent residue counts as FULLY BILLED. See the note below.
       CASE WHEN (COALESCE(e.legacy_material_cost, e.manual_material_cost, 0) * (1 + COALESCE(j.markup_pct,0))
              - COALESCE(e.material_credit,0)) - COALESCE(e.billed_material_amount,0) < 0.01 THEN 0
            ELSE round((COALESCE(e.legacy_material_cost, e.manual_material_cost, 0) * (1 + COALESCE(j.markup_pct,0))
              - COALESCE(e.material_credit,0)) - COALESCE(e.billed_material_amount,0), 2)
       END::numeric(14,2) AS unbilled_material_amount_calc,
       -- Same fix as 012, applied at the row level rather than in the rollup.
       (CASE WHEN e.reviewed THEN COALESCE(e.total_cost_actual,0) ELSE 0 END)::numeric(14,2) AS reviewed_expenses_calc
  FROM expenses e
  LEFT JOIN jobs j ON j.id = e.job_id;

-- ── RESULT: 381 rows on the branch — 0 mismatches on total_cost_actual,
--    billable_material_amount and reviewed_expenses. ONE row differs on
--    unbilled, by $0.01, and that difference is DELIBERATE.
--
-- ⚠ AIRTABLE IS INTERNALLY INCONSISTENT HERE AND WE ARE NOT COPYING THAT.
-- Five expenses land on an exact half-cent residue (billable x.xx5, billed
-- x.xx). Airtable computes in IEEE-754 floating point, so that 0.005 becomes
-- 0.00499999999999545 on four rows and 0.00500000000010914 on the fifth —
-- and it therefore reports **0.00 on four and 0.01 on the other**, from
-- identical inputs. Postgres numeric is exact and has no such wobble.
--
-- Emulating the float bug would be absurd, and rounding half-up would report a
-- phantom penny of unbilled material on an expense that is fully billed —
-- which matters, because "unbilled" is what flags work as still needing to be
-- invoiced. So: **a residue under one cent means fully billed.** Consistent
-- across all five rows, and the total disagreement with Airtable across all
-- 386 expenses is one cent.
--
-- ── STILL AN AIRTABLE DEPENDENCY, DELIBERATELY OUT OF SCOPE ────────────────
-- `billed_material_amount` is an Airtable ROLLUP over Material Billing
-- Allocations, and **that table does not exist in Neon** (only
-- labor_billing_allocations does). It stays an ETL-copied value for now. It is
-- invoice-side and belongs with Step 4e; pulling it in here would drag the
-- invoicing model into an expenses slice.

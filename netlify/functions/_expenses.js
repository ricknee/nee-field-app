// Shared expense → Neon sync. Used by BOTH functions, which is the whole point
// of it living here.
// ---------------------------------------------------------------------------
// WHY THIS FILE EXISTS (migration Step E, 2026-08-10).
//
// It started in airtable.js at Step 4d, when the field app's expense reads moved
// to Neon. `handleExpenses` reads Neon first, so from that moment an
// Airtable-only expense write simply never appeared on screen.
//
// What nobody noticed was that the **inventory app** writes expenses too — the
// materials push — and it wrote Airtable only. So for three days every pushed
// material cost was invisible to the field app AND to GP. Not stale: absent.
// There is no scheduled reload of `expenses` anywhere in the repo; the 390 rows
// in Neon all shared one hand-run `synced_at`, and Airtable had 392.
//
// It was found by pushing $7.50 of pipe and looking for it on the job. Two test
// pushes were all that was affected. The next real one would have gone the same
// way, just as quietly.
//
// ── THE CONTRACTS, WHICH DIFFER BY CALLER ON PURPOSE ───────────────────────
// AIRTABLE STAYS THE IDENTITY AUTHORITY for expenses: R2 receipt keys are built
// from the rec id and receipts can be attached at create time, so the record has
// to exist in Airtable before anything can reference it. Neon is kept in step
// rather than made authoritative. Invert only when receipts move too.
//
//   * The field app (airtable.js) treats a failed sync as cosmetic and swallows
//     it — its own read falls back to Airtable, so the user still sees the row.
//   * The inventory push (inventory.js) FAILS CLOSED. Its expenses are read by
//     the *other* app, which has no such fallback, so a swallowed failure is
//     exactly the silent-invisibility bug above. The push is idempotent on
//     `Push ID`, so the retry re-hits guard #1 and re-syncs instead of
//     re-charging — which is what makes failing closed affordable here.
//
// Callers choose by catching or not catching. `syncExpenseToNeon` throws.

import { neonWrite } from "./_neon.js";

// Airtable hands back numbers, single-selects and lookups in three different
// shapes depending on field type; these normalise all of them.
const num = (v) => { if (Array.isArray(v)) v = v[0]; const x = Number(v); return Number.isFinite(x) ? x : null; };
const str = (v) => { const x = Array.isArray(v) ? v[0] : v; return (x === undefined || x === "" || x === null) ? null : String(x); };
const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : str(v));

// Upsert one Airtable expense record into Neon.
//
// ⚠ `description` reads `Description ?? Notes`, and both are real. Expenses has
// TWO text fields: field-app expenses carry their text in `Description`, while
// the inventory materials push writes its line-item summary into `Notes`
// ("Inventory materials — 1/2\" EMT PIPE ×10"). Mapping only `Description`
// synced every pushed expense with a blank one. They are never both set.
//
// ⚠ FEED THIS A RECORD WHOSE COMPUTED FIELDS ARE POPULATED. `Total Cost
// (Actual)`, `Billable Material Amount $` and `Unbilled Material Amount $` are
// Airtable formulas/rollups, and they feed GP. Syncing a record whose derived
// fields haven't been returned writes zeros into the money columns, which is
// worse than the gap this closes. A record from a GET is safe; see the note at
// the inventory push's call site for why it re-reads rather than trusting the
// create response.
//
// THROWS on failure (neonWrite fails closed). Swallow at the call site if the
// caller's contract says a failed sync is cosmetic.
export async function syncExpenseToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  await neonWrite("expense.sync",
    `INSERT INTO expenses
       (airtable_id, job_airtable_id, job_id, expense_type, expense_status, expense_date,
        total_cost_actual, reviewed, reviewed_expenses, billable, billable_material_amount,
        billed_material_amount, unbilled_material_amount, manual_material_cost, material_credit,
        vendor_name, description, push_id, submitted_by_at_id, submitted_by_name, synced_at)
     VALUES ($1,$2,(SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),$3,$4,$5::date,$6,$7,$8,$9,$10,
             $11,$12,$13,$14,$15,$16,$17,$18,$19, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       job_airtable_id=EXCLUDED.job_airtable_id, job_id=EXCLUDED.job_id,
       expense_type=EXCLUDED.expense_type, expense_status=EXCLUDED.expense_status,
       expense_date=EXCLUDED.expense_date, total_cost_actual=EXCLUDED.total_cost_actual,
       reviewed=EXCLUDED.reviewed, reviewed_expenses=EXCLUDED.reviewed_expenses,
       billable=EXCLUDED.billable, billable_material_amount=EXCLUDED.billable_material_amount,
       billed_material_amount=EXCLUDED.billed_material_amount,
       unbilled_material_amount=EXCLUDED.unbilled_material_amount,
       manual_material_cost=EXCLUDED.manual_material_cost, material_credit=EXCLUDED.material_credit,
       vendor_name=EXCLUDED.vendor_name, description=EXCLUDED.description,
       push_id=EXCLUDED.push_id, submitted_by_at_id=EXCLUDED.submitted_by_at_id,
       submitted_by_name=EXCLUDED.submitted_by_name, synced_at=now()`,
    [rec.id, str(f["Job"]), sel(f["Expense Type"]), sel(f["Expense Status"]), str(f["Expense Date"]),
     num(f["Total Cost (Actual)"]), f["Reviewed"] === true, num(f["Reviewed Expenses"]),
     f["Billable?"] === true, num(f["Billable Material Amount $"]),
     num(f["Billed Material Amount $"]), num(f["Unbilled Material Amount $"]),
     num(f["Manual Material Cost"]), num(f["Material Credit"]),
     str(f["Vendor Name (from Vendor)"]), str(f["Description"] ?? f["Notes"]), str(f["Push ID"]),
     str(f["Submitted By"]),
     (Array.isArray(f["Submitted By Name"]) ? f["Submitted By Name"].filter(Boolean).join(", ")
                                            : str(f["Submitted By Name"]))]);
}

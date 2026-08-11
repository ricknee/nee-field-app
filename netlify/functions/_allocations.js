// ── Billing allocations, written by the app ────────────────────────────────
// Added 2026-08-11. Replaces four deployed Airtable automations:
//
//   wflTwXb6dG32FFv9s  create labor allocation on review
//   wflNmJsnIhWtSjUlL  create material allocation on review
//   wflOcxtmkzdxKMVQW  attach labor allocations on invoice save
//   wfl7bzJpZY9kcJ27i  attach material allocations on invoice save
//
// They are the only reason anyone still has to open Airtable in normal
// operation. Full design, including the field-by-field decode of all four,
// is in docs/PLAN-billing-allocations.md.
//
// ⚠⚠ AIRTABLE IS WRITTEN FIRST HERE, AND THAT IS DELIBERATE — it is the
// OPPOSITE of the Neon-first contract used for time entries, expenses,
// estimates and invoices. The reason is `_billing-sync.js`, which runs hourly
// and ends with:
//
//     DELETE FROM labor_billing_allocations
//      WHERE NOT (airtable_id = ANY($1::text[]))    -- $1 = every id in Airtable
//
// So a Neon row with no Airtable twin is deleted within the hour, and the
// invoice total silently drops AFTER having looked correct. That delete pass is
// not a bug — un-allocating in Airtable removes the row, and without it Neon
// would keep billing a customer for material no longer on the invoice. It does
// mean Airtable is authoritative for EXISTENCE on these two tables.
//
// Ordering consequences, both acceptable:
//   Airtable fails            -> nothing written anywhere, caller sees the error
//   Airtable ok, Neon fails   -> Airtable-only row, ADOPTED by the hourly sync
//                                within the hour. The invoice total is briefly
//                                low rather than permanently wrong.
//
// ⚠⚠ THIS SHIPS INERT. `ALLOCATIONS_WRITE` must be `on` before any of it does
// anything, because the four automations cannot be live at the same time: both
// would fire, producing TWO allocations for one time entry, and invoice totals
// sum them — that is double-billing a customer. The existence check below
// cannot close that on its own; it is a race with Airtable's trigger, not a
// logic gap. Undeploy the automations and flip the switch together.

import { neonQuery, neonWrite } from "./_neon.js";

const T_LABOR    = "tblHyJWVAcBczn3hn";   // Labor Billing Allocations
const T_MATERIAL = "tblMoKg7txcfYczQQ";   // Material Billing Allocations

// Field names, transcribed from the live automations rather than guessed.
const F_LABOR_TIME_ENTRY = "Time Entry";
const F_LABOR_HOURS      = "Allocated Hours";
const F_MAT_EXPENSE      = "Expense";
const F_MAT_AMOUNT       = "Allocated Material Amount $";
const F_INVOICE          = "Invoice";      // same name on both tables

export function allocationsWriteEnabled() {
  return String(process.env.ALLOCATIONS_WRITE || "").toLowerCase() === "on";
}

// Every function here returns a {created|attached, skipped} report rather than
// throwing on a skip. A skip is the NORMAL case — most payroll edits are not a
// review flipping on — so a caller that treated it as failure would be wrong
// most of the time. Genuine write failures still throw.
const skip = (reason, extra = {}) => ({ created: 0, skipped: reason, ...extra });

// ── Labor ──────────────────────────────────────────────────────────────────
// Reproduces wflTwXb6dG32FFv9s exactly:
//   gate   Billable ✓ AND Unbilled Hours > 0 AND no allocation yet AND Labor Reviewed ✓
//   write  { Time Entry: [id], Allocated Hours: <the entry's HOURS> }
//
// ⚠ The gate reads UNBILLED hours and the write uses FULL hours. That is not a
// transcription slip — it is what the automation does. The two are equal at this
// moment only because "no allocation yet" guarantees Billed Hours is 0. Writing
// unbilled hours instead would be identical today and wrong the first time a
// partially-billed entry is re-reviewed.
export async function createLaborAllocation(atFetch, timeEntryAirtableId) {
  if (!allocationsWriteEnabled()) return skip("disabled");
  if (!timeEntryAirtableId) {
    // ⚠ NOT A NO-OP WORTH SWALLOWING. A time entry with no Airtable twin can
    // never be allocated — the allocation's Time Entry field is an Airtable
    // LINK, so there is nothing to point at. As of 2026-08-11 that is 24
    // billable entries / 48.25 h, none reviewed yet, and it GROWS: the QB
    // puller and the time clock both write Neon-only rows.
    //
    // The old automation had the same blind spot and could not even report it,
    // because a record that is not in Airtable cannot trigger an Airtable
    // automation. This at least says so out loud. The real fix is allocations
    // going Neon-native, which needs `_billing-sync.js`'s delete pass to stop
    // treating Airtable as the authority on existence. See the plan, §2.
    console.warn("allocations: time entry has no Airtable twin — it cannot be billed");
    return skip("no-airtable-twin");
  }

  const q = await neonQuery(
    `SELECT t.hours::float8            AS hours,
            t.billable                 AS billable,
            t.labor_reviewed           AS reviewed,
            b.unbilled_hours::float8   AS unbilled_hours,
            (SELECT count(*) FROM labor_billing_allocations a
              WHERE a.time_entry_airtable_id = t.airtable_id) AS existing
       FROM time_entries t
       LEFT JOIN v_time_entry_billing b ON b.airtable_id = t.airtable_id
      WHERE t.airtable_id = $1`, [timeEntryAirtableId]);
  if (!q?.rows?.length) return skip("entry-not-found");
  const r = q.rows[0];

  if (r.billable !== true)        return skip("not-billable");
  if (r.reviewed !== true)        return skip("not-reviewed");
  if (Number(r.existing) > 0)     return skip("already-allocated");
  if (!(Number(r.unbilled_hours) > 0)) return skip("nothing-unbilled");

  const created = await atFetch(T_LABOR, {
    method: "POST",
    body: JSON.stringify({ fields: {
      [F_LABOR_TIME_ENTRY]: [timeEntryAirtableId],
      [F_LABOR_HOURS]: Number(r.hours),
    } }),
  });
  if (!created?.id) throw new Error("createLaborAllocation: Airtable returned no record id");

  await mirrorLaborToNeon(created.id, timeEntryAirtableId, Number(r.hours));
  return { created: 1, skipped: null, allocationId: created.id, hours: Number(r.hours) };
}

// ── Material ───────────────────────────────────────────────────────────────
// Reproduces wflNmJsnIhWtSjUlL exactly:
//   gate   Billable? ✓ AND Unbilled Material Amount $ > 0 AND no allocation yet AND Reviewed ✓
//   write  { Expense: [id], Allocated Material Amount $: <the UNBILLED amount> }
//
// ⚠ NOT symmetric with labor. Here the automation writes the same field it
// gates on — the unbilled remainder, not the full amount. Making the two halves
// "consistent" would change what customers are billed.
export async function createMaterialAllocation(atFetch, expenseAirtableId) {
  if (!allocationsWriteEnabled()) return skip("disabled");
  if (!expenseAirtableId) return skip("no-expense-id");

  const q = await neonQuery(
    `SELECT e.billable AS billable, e.reviewed AS reviewed,
            v.unbilled_material_amount_calc::float8 AS unbilled,
            (SELECT count(*) FROM material_billing_allocations a
              WHERE a.expense_airtable_id = e.airtable_id) AS existing
       FROM expenses e
       LEFT JOIN v_expenses v ON v.airtable_id = e.airtable_id
      WHERE e.airtable_id = $1`, [expenseAirtableId]);
  if (!q?.rows?.length) return skip("expense-not-found");
  const r = q.rows[0];

  if (r.billable !== true)    return skip("not-billable");
  if (r.reviewed !== true)    return skip("not-reviewed");
  if (Number(r.existing) > 0) return skip("already-allocated");
  const amount = Number(r.unbilled);
  if (!(amount > 0))          return skip("nothing-unbilled");

  const created = await atFetch(T_MATERIAL, {
    method: "POST",
    body: JSON.stringify({ fields: {
      [F_MAT_EXPENSE]: [expenseAirtableId],
      [F_MAT_AMOUNT]: amount,
    } }),
  });
  if (!created?.id) throw new Error("createMaterialAllocation: Airtable returned no record id");

  await mirrorMaterialToNeon(created.id, expenseAirtableId, amount);
  return { created: 1, skipped: null, allocationId: created.id, amount };
}

// ── Attach on invoice save ─────────────────────────────────────────────────
// Reproduces wflOcxtmkzdxKMVQW + wfl7bzJpZY9kcJ27i: on an invoice with
// Auto Allocate ✓, claim every UNLINKED allocation belonging to that job.
//
// The candidate list comes from NEON rather than an Airtable FIND, which also
// sidesteps the cross-job substring trap in CLAUDE.md — a job id is exact where
// FIND(jobName, ARRAYJOIN({Job})) is a substring test that leaks between
// "Jenny Ln 1" and "Jenny Ln 10/11/12". The old automation matched on the Job
// LOOKUP, which is id-based and safe; this keeps that property.
//
// Airtable is updated one record at a time on purpose: the automation did the
// same (a repeatingGroup over findRecords), and a partial failure here must
// leave the successful ones attached rather than rolling back money that is
// already on an invoice.
export async function attachAllocationsToInvoice(atFetch, invoiceAirtableId, jobAirtableId) {
  if (!allocationsWriteEnabled()) return { attached: 0, skipped: "disabled" };
  if (!invoiceAirtableId || !jobAirtableId) return { attached: 0, skipped: "missing-ids" };

  const q = await neonQuery(
    `SELECT a.airtable_id, 'labor' AS kind
       FROM labor_billing_allocations a
       JOIN time_entries t ON t.id = a.time_entry_id
       JOIN jobs j ON j.id = t.job_id
      WHERE j.airtable_id = $1 AND a.invoice_airtable_id IS NULL
        AND a.airtable_id IS NOT NULL
      UNION ALL
     SELECT a.airtable_id, 'material'
       FROM material_billing_allocations a
       JOIN expenses e ON e.id = a.expense_id
       JOIN jobs j ON j.id = e.job_id
      WHERE j.airtable_id = $1 AND a.invoice_airtable_id IS NULL
        AND a.airtable_id IS NOT NULL`, [jobAirtableId]);
  if (!q?.rows) return { attached: 0, skipped: "lookup-failed" };
  if (!q.rows.length) return { attached: 0, skipped: null };

  const failures = [];
  let attached = 0;
  for (const row of q.rows) {
    const table = row.kind === "labor" ? T_LABOR : T_MATERIAL;
    try {
      await atFetch(`${table}/${row.airtable_id}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: { [F_INVOICE]: [invoiceAirtableId] } }),
      });
      const tbl = row.kind === "labor" ? "labor_billing_allocations" : "material_billing_allocations";
      await neonWrite(`allocation.attach.${row.kind}`,
        `UPDATE ${tbl} SET invoice_airtable_id = $2
          WHERE airtable_id = $1`, [row.airtable_id, invoiceAirtableId]);
      attached++;
    } catch (e) {
      // Collected, not thrown: aborting mid-loop would strand the rest while
      // leaving the earlier ones attached — the worst of both. Same stance the
      // inventory expense push settled on at Step E.
      failures.push(`${row.airtable_id}: ${e?.message || e}`);
    }
  }
  if (failures.length) console.error(`allocations: ${failures.length} attach(es) failed — ${failures.join("; ")}`);
  return { attached, skipped: null, failed: failures.length };
}

// ── Neon mirrors ───────────────────────────────────────────────────────────
// Separate from the create calls so the ordering above stays readable, and so a
// Neon failure throws with a name that says which half succeeded.
//
// `bill_rate` is deliberately NOT set here. In Airtable it is a lookup through
// the Time Entry to the job's T&M rate, so Airtable fills it and the hourly
// sync carries the computed value across. Writing our own guess would create a
// second opinion about a number that feeds Invoice Total.
async function mirrorLaborToNeon(allocationId, timeEntryAirtableId, hours) {
  await neonWrite("allocation.labor.insert",
    `INSERT INTO labor_billing_allocations
       (airtable_id, time_entry_airtable_id, time_entry_id, allocated_hours, synced_at)
     VALUES ($1, $2, (SELECT id FROM time_entries WHERE airtable_id = $2), $3, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       time_entry_airtable_id = EXCLUDED.time_entry_airtable_id,
       time_entry_id          = EXCLUDED.time_entry_id,
       allocated_hours        = EXCLUDED.allocated_hours,
       synced_at              = EXCLUDED.synced_at`,
    [allocationId, timeEntryAirtableId, hours]);
}

async function mirrorMaterialToNeon(allocationId, expenseAirtableId, amount) {
  await neonWrite("allocation.material.insert",
    `INSERT INTO material_billing_allocations
       (airtable_id, expense_airtable_id, expense_id, allocated_amount, synced_at)
     VALUES ($1, $2, (SELECT id FROM expenses WHERE airtable_id = $2), $3, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       expense_airtable_id = EXCLUDED.expense_airtable_id,
       expense_id          = EXCLUDED.expense_id,
       allocated_amount    = EXCLUDED.allocated_amount,
       synced_at           = EXCLUDED.synced_at`,
    [allocationId, expenseAirtableId, amount]);
}

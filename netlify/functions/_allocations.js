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
// ⚠⚠ TWO PATHS, AND THE SECOND ONE IS NOW THE NORMAL CASE.
//
// Step 3 retired Make from the time path on 2026-08-07, so QB-pulled entries
// land in Neon with NO Airtable twin. Measured 2026-08-11: 0% of the week of
// 07-20 lacked a twin, 20% of 08-03, and **100% of the week of 08-10**.
//
// An allocation for such an entry cannot exist in Airtable — its Time Entry
// field is an Airtable link with nothing to point at — so it is created
// NEON-NATIVE, keyed on the time entry's uuid with a NULL airtable_id.
//
// The first version of this refused those outright and returned
// "no-airtable-twin". That was honest but useless: it meant no labor logged
// after 2026-08-07 could ever reach an invoice. The old Airtable automation had
// the identical blind spot and could not even report it. Found by reviewing two
// real entries ten minutes after cutover.
//
// ⚠ A Neon-native row only survives because `_billing-sync.js`'s delete pass
// skips `airtable_id IS NULL`. Remove that guard and every one of these
// disappears within the hour, taking the invoice total with it.
export async function createLaborAllocation(atFetch, timeEntryNeonId, timeEntryAirtableId) {
  if (!allocationsWriteEnabled()) return skip("disabled");
  if (!timeEntryNeonId) return skip("no-entry-id");

  // Billed hours are counted through BOTH keys. A given allocation carries one
  // or the other, never both, and an entry that acquires a twin later must not
  // be double-allocated because the count only looked at one of them.
  const q = await neonQuery(
    `SELECT t.airtable_id, t.hours::float8 AS hours, t.billable, t.labor_reviewed AS reviewed,
            (t.hours - COALESCE((SELECT sum(a.allocated_hours) FROM labor_billing_allocations a
                WHERE a.time_entry_id = t.id
                   OR (t.airtable_id IS NOT NULL AND a.time_entry_airtable_id = t.airtable_id)), 0))::float8
              AS unbilled_hours,
            (SELECT count(*) FROM labor_billing_allocations a
              WHERE a.time_entry_id = t.id
                 OR (t.airtable_id IS NOT NULL AND a.time_entry_airtable_id = t.airtable_id)) AS existing
       FROM time_entries t
      WHERE t.id = $1::uuid`, [timeEntryNeonId]);
  if (!q?.rows?.length) return skip("entry-not-found");
  const r = q.rows[0];

  if (r.billable !== true)        return skip("not-billable");
  if (r.reviewed !== true)        return skip("not-reviewed");
  if (Number(r.existing) > 0)     return skip("already-allocated");
  if (!(Number(r.unbilled_hours) > 0)) return skip("nothing-unbilled");

  const airtableId = timeEntryAirtableId || r.airtable_id || null;

  // ── Neon-native: no twin to link to ──────────────────────────────────────
  if (!airtableId) {
    // Conditional INSERT rather than check-then-insert. The gate above is a
    // read, so two concurrent reviews of the same entry could both pass it;
    // `WHERE NOT EXISTS` makes the create itself the guard, in one statement.
    const ins = await neonWrite("allocation.labor.native",
      `INSERT INTO labor_billing_allocations (time_entry_id, allocated_hours, synced_at)
       SELECT $1::uuid, $2, now()
        WHERE NOT EXISTS (SELECT 1 FROM labor_billing_allocations WHERE time_entry_id = $1::uuid)
       RETURNING id`, [timeEntryNeonId, Number(r.hours)]);
    if (!Array.isArray(ins) || !ins.length) return skip("already-allocated");
    return { created: 1, skipped: null, allocationId: ins[0].id,
             hours: Number(r.hours), neonNative: true };
  }

  // ── Airtable-first: the entry has a twin, so keep them in step ───────────
  const created = await atFetch(T_LABOR, {
    method: "POST",
    body: JSON.stringify({ fields: {
      [F_LABOR_TIME_ENTRY]: [airtableId],
      [F_LABOR_HOURS]: Number(r.hours),
    } }),
  });
  if (!created?.id) throw new Error("createLaborAllocation: Airtable returned no record id");

  await mirrorLaborToNeon(created.id, airtableId, Number(r.hours));
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
// ⚠⚠ BATCHED, AND THE BATCH SIZE IS AIRTABLE'S LIMIT, NOT A TUNING KNOB.
// This used to PATCH one allocation at a time, mirroring the automation's
// repeatingGroup over findRecords. That is two round trips per allocation, and
// Bethel School's first invoice after a summer of approvals carried 163 of them:
// the function ran past Netlify's gateway timeout, so the browser got a 504 and
// alerted "Error saving invoice" over an invoice that had saved perfectly. That
// is the worst failure to read, because the natural response — press Save again
// — POSTs a SECOND invoice for the same work. Airtable takes 10 records per
// write request and Neon takes a whole chunk in one UPDATE, so those 163
// allocations now cost ~33 requests instead of 326.
//
// A partial failure still leaves earlier work attached, at a granularity of ten
// rows rather than one. Airtable applies a batch PATCH all-or-nothing, so a
// failed batch leaves its ten unlinked and re-attachable by saving again; it
// never half-writes a row.
const AT_BATCH = 10;   // Airtable's hard cap on records per write request

export async function attachAllocationsToInvoice(atFetch, invoiceAirtableId, jobAirtableId) {
  if (!allocationsWriteEnabled()) return { attached: 0, skipped: "disabled" };
  if (!invoiceAirtableId || !jobAirtableId) return { attached: 0, skipped: "missing-ids" };

  // `a.airtable_id IS NOT NULL` is deliberately ABSENT. Neon-native allocations
  // have no Airtable row to PATCH, but they are exactly the ones covering work
  // logged since 2026-08-07 — excluding them would attach the old allocations to
  // an invoice and silently leave this week's labor off it.
  const q = await neonQuery(
    `SELECT a.id::text AS id, a.airtable_id, 'labor' AS kind
       FROM labor_billing_allocations a
       JOIN time_entries t ON t.id = a.time_entry_id
       JOIN jobs j ON j.id = t.job_id
      WHERE j.airtable_id = $1 AND a.invoice_airtable_id IS NULL
      UNION ALL
     SELECT a.id::text, a.airtable_id, 'material'
       FROM material_billing_allocations a
       JOIN expenses e ON e.id = a.expense_id
       JOIN jobs j ON j.id = e.job_id
      WHERE j.airtable_id = $1 AND a.invoice_airtable_id IS NULL`, [jobAirtableId]);
  if (!q?.rows) return { attached: 0, skipped: "lookup-failed" };
  if (!q.rows.length) return { attached: 0, skipped: null };

  const failures = [];
  let attached = 0, native = 0;

  // Commit a chunk to Neon right after its PATCH lands, rather than once at the
  // end: if the function dies mid-run, the window in which Airtable says "on
  // invoice X" while Neon still says "unlinked" is ten rows wide, not the whole
  // invoice. Rows caught in that window are re-attached by the next save —
  // the same recovery the per-row version had.
  const commit = async (kind, ids) => {
    if (!ids.length) return;
    const tbl = kind === "labor" ? "labor_billing_allocations" : "material_billing_allocations";
    await neonWrite(`allocation.attach.${kind}`,
      `UPDATE ${tbl} SET invoice_airtable_id = $2 WHERE id = ANY($1::uuid[])`,
      [ids, invoiceAirtableId]);
    attached += ids.length;
  };

  for (const kind of ["labor", "material"]) {
    const rows = q.rows.filter(r => r.kind === kind);
    if (!rows.length) continue;
    const table = kind === "labor" ? T_LABOR : T_MATERIAL;

    // Neon-native allocations skip Airtable entirely. One is invoiced purely in
    // Neon — which is where v_invoices.invoice_total_calc reads from, so the
    // total is right either way. Its Airtable counterpart does not exist to be
    // updated, so there is nothing to batch and nothing that can fail there.
    const nativeIds = rows.filter(r => !r.airtable_id).map(r => r.id);
    native += nativeIds.length;
    try {
      await commit(kind, nativeIds);
    } catch (e) {
      failures.push(`${kind} native ×${nativeIds.length}: ${e?.message || e}`);
    }

    const mirrored = rows.filter(r => r.airtable_id);
    for (let i = 0; i < mirrored.length; i += AT_BATCH) {
      const batch = mirrored.slice(i, i + AT_BATCH);
      try {
        await atFetch(table, {
          method: "PATCH",
          body: JSON.stringify({
            records: batch.map(r => ({ id: r.airtable_id, fields: { [F_INVOICE]: [invoiceAirtableId] } })),
          }),
        });
        await commit(kind, batch.map(r => r.id));
      } catch (e) {
        // Collected, not thrown: aborting mid-loop would strand the rest while
        // leaving the earlier ones attached — the worst of both. Same stance the
        // inventory expense push settled on at Step E.
        failures.push(`${kind} ${batch.map(r => r.airtable_id).join(",")}: ${e?.message || e}`);
      }
    }
  }
  if (failures.length) console.error(`allocations: ${failures.length} attach(es) failed — ${failures.join("; ")}`);
  return { attached, skipped: null, failed: failures.length, neonNative: native };
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

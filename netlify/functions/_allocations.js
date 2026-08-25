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
            j.billable_hourly_rate::float8 AS job_rate,
            (t.hours - COALESCE((SELECT sum(a.allocated_hours) FROM labor_billing_allocations a
                WHERE a.time_entry_id = t.id
                   OR (t.airtable_id IS NOT NULL AND a.time_entry_airtable_id = t.airtable_id)), 0))::float8
              AS unbilled_hours,
            (SELECT count(*) FROM labor_billing_allocations a
              WHERE a.time_entry_id = t.id
                 OR (t.airtable_id IS NOT NULL AND a.time_entry_airtable_id = t.airtable_id)) AS existing
       FROM time_entries t
       LEFT JOIN jobs j ON j.id = t.job_id
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
    // ⚠ bill_rate IS WRITTEN HERE AND NOWHERE ELSE, and the asymmetry with the
    // mirrored path below is the point. In Airtable the column is a lookup
    // through Time Entry → Job, so Airtable fills it and the hourly sync carries
    // the value over — which is why mirrorLaborToNeon must not guess at it. A
    // native row has no Airtable counterpart to do that filling, so left alone
    // it stays NULL forever, and v_invoices computes labor as
    // sum(allocated_hours * bill_rate): a NULL rate silently values those hours
    // at ZERO. Found on Bethel School invoice 1665 — 10.75 hours billed on the
    // PDF, $698.75 missing from the invoice's own computed total. Since Step 3
    // every new time entry arrives without a twin, so every one of these would
    // have been rate-less.
    //
    // The job's CURRENT rate is the right value to freeze in: it is what
    // Airtable's lookup resolves to at this moment (verified 2026-08-11 —
    // 2,696 of 2,696 rated allocations carry exactly their job's rate), and
    // storing it is what makes historical revenue survive a later rate change.
    // A job with no rate still writes NULL. That is a real condition, not a
    // gap to paper over — see the three rate-less T&M jobs in the GP audit —
    // and inventing a number here would hide it.
    const ins = await neonWrite("allocation.labor.native",
      `INSERT INTO labor_billing_allocations (time_entry_id, allocated_hours, bill_rate, synced_at)
       SELECT $1::uuid, $2, $3, now()
        WHERE NOT EXISTS (SELECT 1 FROM labor_billing_allocations WHERE time_entry_id = $1::uuid)
       RETURNING id`, [timeEntryNeonId, Number(r.hours), r.job_rate ?? null]);
    if (!Array.isArray(ins) || !ins.length) return skip("already-allocated");
    return { created: 1, skipped: null, allocationId: ins[0].id,
             hours: Number(r.hours), billRate: r.job_rate ?? null, neonNative: true };
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
// ⚠⚠ TAKES EITHER HANDLE (cutover slice 4, 2026-08-24), and this is the half of
// slice 4 that would have cost money.
//
// It used to be parameterised on the expense REC ID and depended on it three
// separate times in one query: `WHERE e.airtable_id = $1`, the `v_expenses`
// join, and the already-allocated guard. A native expense has a NULL
// `airtable_id`, so all three fail — and they fail QUIETLY. The lookup returns
// nothing, this returns `skip("expense-not-found")`, and the expense simply
// never gets a material billing allocation: the material is a cost with no route
// onto an invoice, and **the customer is never billed for it**. No error
// anywhere. That is the same silent shape as the Bethel School labor loss, from
// the other side of the ledger.
//
// Now every clause takes either form, matching `createLaborAllocation` above,
// which already had to solve exactly this when time entries stopped having
// Airtable twins on 2026-08-07.
//
// Verified equivalent before the swap: all 406 expenses, identical billable /
// reviewed / unbilled / existing on every row, zero diff either way.
export async function createMaterialAllocation(atFetch, expenseId) {
  if (!allocationsWriteEnabled()) return skip("disabled");
  if (!expenseId) return skip("no-expense-id");

  // The allocation count is taken through BOTH keys, for the same reason the
  // labor twin does it: an allocation carries one or the other, never both, and
  // an expense that acquires a rec id later must not be double-allocated
  // because the guard only looked at one of them.
  const q = await neonQuery(
    `SELECT e.id AS expense_uuid, e.airtable_id AS expense_airtable_id,
            e.billable AS billable, e.reviewed AS reviewed,
            v.unbilled_material_amount_calc::float8 AS unbilled,
            (SELECT count(*) FROM material_billing_allocations a
              WHERE a.expense_id = e.id
                 OR (e.airtable_id IS NOT NULL AND a.expense_airtable_id = e.airtable_id)) AS existing
       FROM expenses e
       LEFT JOIN v_expenses v ON v.id = e.id
      WHERE e.id::text = $1 OR e.airtable_id = $1`, [String(expenseId)]);
  if (!q?.rows?.length) return skip("expense-not-found");
  const r = q.rows[0];

  if (r.billable !== true)    return skip("not-billable");
  if (r.reviewed !== true)    return skip("not-reviewed");
  if (Number(r.existing) > 0) return skip("already-allocated");
  const amount = Number(r.unbilled);
  if (!(amount > 0))          return skip("nothing-unbilled");

  const airtableId = r.expense_airtable_id || null;

  // ── Neon-native: no twin to link to ──────────────────────────────────────
  // An Airtable allocation's `Expense` field is a link, so it has nothing to
  // point at for a native expense. The row is created Neon-only with a NULL
  // airtable_id, exactly as the labor path has done since 2026-08-07.
  //
  // ⚠ No `bill_rate` equivalent to worry about here, and the asymmetry with
  // labor is worth stating: labor's rate is an Airtable LOOKUP that a native row
  // would leave NULL (valuing those hours at $0 — the Bethel 1665 bug), whereas
  // material's `allocated_amount` is written directly on both paths. Nothing is
  // left for Airtable to fill in.
  //
  // ⚠ Survives only because `_billing-sync.js`'s delete pass skips
  // `airtable_id IS NULL`. Remove that guard and every native allocation
  // disappears within the hour, taking the invoice total with it.
  if (!airtableId) {
    const ins = await neonWrite("allocation.material.native",
      `INSERT INTO material_billing_allocations (expense_id, allocated_amount, synced_at)
       SELECT $1::uuid, $2::numeric, now()
        WHERE NOT EXISTS (SELECT 1 FROM material_billing_allocations WHERE expense_id = $1::uuid)
       RETURNING id`, [r.expense_uuid, amount]);
    if (!Array.isArray(ins) || !ins.length) return skip("already-allocated");
    return { created: 1, skipped: null, allocationId: ins[0].id, amount, neonNative: true };
  }

  const created = await atFetch(T_MATERIAL, {
    method: "POST",
    body: JSON.stringify({ fields: {
      [F_MAT_EXPENSE]: [airtableId],
      [F_MAT_AMOUNT]: amount,
    } }),
  });
  if (!created?.id) throw new Error("createMaterialAllocation: Airtable returned no record id");

  await mirrorMaterialToNeon(created.id, airtableId, amount, r.expense_uuid);
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

// ⚠⚠ THE INVOICE IS PASSED AS BOTH HANDLES, and that is the whole of the
// identity cutover in this function (slice 3, db/schema/055).
//
// `invoice.id` is the Neon uuid and ALWAYS exists — the invoice row is written
// to Neon before this is called. `invoice.airtableId` is the mirror's rec id and
// is NULL when Airtable was unreachable at create time. Only the Airtable PATCH
// half needs the rec id; the Neon half never did, and `v_invoices` now resolves
// its labor and material by uuid first (see 055), so an invoice with no rec id
// still totals correctly. Before this change the uuid was never written at all
// and a native invoice would have printed a $0 labor line.
export async function attachAllocationsToInvoice(atFetch, invoice, jobAirtableId) {
  if (!allocationsWriteEnabled()) return { attached: 0, skipped: "disabled" };
  const invoiceNeonId      = invoice?.id || null;
  const invoiceAirtableId  = invoice?.airtableId || null;
  if (!invoiceNeonId || !jobAirtableId) return { attached: 0, skipped: "missing-ids" };

  // `a.airtable_id IS NOT NULL` is deliberately ABSENT. Neon-native allocations
  // have no Airtable row to PATCH, but they are exactly the ones covering work
  // logged since 2026-08-07 — excluding them would attach the old allocations to
  // an invoice and silently leave this week's labor off it.
  //
  // ⚠ "Unattached" now means BOTH handles are empty. Checking only
  // `invoice_airtable_id IS NULL` would re-attach — and re-bill — every
  // allocation already sitting on a NATIVE invoice, every time any invoice on
  // that job was saved.
  const q = await neonQuery(
    `SELECT a.id::text AS id, a.airtable_id, 'labor' AS kind
       FROM labor_billing_allocations a
       JOIN time_entries t ON t.id = a.time_entry_id
       JOIN jobs j ON j.id = t.job_id
      WHERE (j.airtable_id = $1 OR j.id::text = $1)
        AND a.invoice_airtable_id IS NULL AND a.invoice_id IS NULL
      UNION ALL
     SELECT a.id::text, a.airtable_id, 'material'
       FROM material_billing_allocations a
       JOIN expenses e ON e.id = a.expense_id
       JOIN jobs j ON j.id = e.job_id
      WHERE (j.airtable_id = $1 OR j.id::text = $1)
        AND a.invoice_airtable_id IS NULL AND a.invoice_id IS NULL`, [jobAirtableId]);
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
    // BOTH columns, every time. The uuid is what `v_invoices` reads; the rec id
    // is what the hourly Airtable sync reconciles against and is simply NULL
    // when the invoice never reached Airtable.
    await neonWrite(`allocation.attach.${kind}`,
      `UPDATE ${tbl} SET invoice_id = $2, invoice_airtable_id = $3
        WHERE id = ANY($1::uuid[])`,
      [ids, invoiceNeonId, invoiceAirtableId]);
    attached += ids.length;

    // ── MARK THE MATERIAL BILLED (2026-08-25) ───────────────────────────────
    // "Bill it once it is approved, and then never again" is the T&M rule, and
    // `unbilled_material_amount_calc` is what enforces it:
    //
    //   unbilled = billable − expenses.billed_material_amount
    //
    // That column was an AIRTABLE ROLLUP over the expense's linked allocations,
    // and the ONLY thing that ever wrote it into Neon is syncExpenseToNeon,
    // copying the rollup back. A Neon-native expense has no rollup to copy, so
    // it stayed NULL forever: the material read fully unbilled no matter how
    // many times it had been invoiced, and every later invoice offered it again.
    // Measured before this fix — 406 legacy expenses had the column set, all 7
    // native ones had it NULL. Since expenses went native on 2026-08-24 that is
    // every expense from here on, so the second invoice on any new T&M job would
    // have re-billed its material.
    //
    // ⚠ SCOPED TO NATIVE EXPENSES ON PURPOSE. A legacy expense still gets its
    // number from Airtable's rollup, which the hourly sync carries back and
    // would overwrite anything written here — and its allocations still mint
    // Airtable rows, so that rollup is still correct for them. Widening this to
    // legacy rows would put two writers on one column, which is how the stored
    // value and the allocations drifted apart in the first place (135 of 406
    // legacy expenses disagree with their own allocations by $274k — a separate
    // question, deliberately not answered by this change).
    //
    // ⚠ Contract invoices never reach here: `auto_allocate` is false for them,
    // so handleSaveInvoice skips the attach entirely. Contract jobs bill from
    // the estimate and its addenda; their material is tracked, not billed.
    //
    // Idempotent by construction: the lookup above only returns allocations with
    // BOTH invoice handles NULL, so an allocation is claimed once and can only
    // be added to its expense once.
    if (kind === "material") {
      await neonWrite("allocation.attach.markBilled",
        `WITH claimed AS (
           SELECT m.expense_id, SUM(m.allocated_amount) AS amt
             FROM material_billing_allocations m
             JOIN expenses e ON e.id = m.expense_id
            WHERE m.id = ANY($1::uuid[])
              AND e.airtable_id IS NULL
            GROUP BY m.expense_id
         )
         UPDATE expenses e
            SET billed_material_amount = COALESCE(e.billed_material_amount, 0) + c.amt,
                synced_at = now()
           FROM claimed c
          WHERE e.id = c.expense_id`,
        [ids]);
    }
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

    // An allocation that HAS an Airtable row, being attached to an invoice that
    // does NOT. There is no rec id to write into the Airtable link field, so the
    // Airtable half is skipped and the attach is committed in Neon alone —
    // which is where the money is read from. The visible consequence is that
    // Airtable's copy of that allocation keeps showing "no invoice"; the app is
    // correct and Airtable is the stale one, which is the direction of travel.
    const mirrored = invoiceAirtableId ? rows.filter(r => r.airtable_id) : [];
    if (!invoiceAirtableId) {
      const orphanIds = rows.filter(r => r.airtable_id).map(r => r.id);
      if (orphanIds.length) {
        try {
          await commit(kind, orphanIds);
        } catch (e) {
          failures.push(`${kind} neon-only ×${orphanIds.length}: ${e?.message || e}`);
        }
      }
    }
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

// ⚠ `expense_id` is now PASSED IN rather than re-resolved by a
// `(SELECT id FROM expenses WHERE airtable_id = $2)` subselect. The caller has
// already looked the expense up through the dual handle, so re-deriving it from
// the rec id here would reintroduce the very dependency this slice removed —
// and it is the column `v_invoices` joins on (`e.id = m.expense_id`), so a NULL
// there drops the expense's credit out of the invoice total.
async function mirrorMaterialToNeon(allocationId, expenseAirtableId, amount, expenseNeonId) {
  await neonWrite("allocation.material.insert",
    `INSERT INTO material_billing_allocations
       (airtable_id, expense_airtable_id, expense_id, allocated_amount, synced_at)
     VALUES ($1, $2, $4::uuid, $3, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       expense_airtable_id = EXCLUDED.expense_airtable_id,
       expense_id          = EXCLUDED.expense_id,
       allocated_amount    = EXCLUDED.allocated_amount,
       synced_at           = EXCLUDED.synced_at`,
    [allocationId, expenseAirtableId, amount, expenseNeonId]);
}

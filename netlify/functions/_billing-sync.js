// ── Hourly sync for the tables the APP CANNOT WRITE ────────────────────────
// Added 2026-08-08, after the field-app migration completed.
//
// Everything else moved to Neon-first writes. These two did not, and the
// reason matters: **there is no write path for them in the app at all.**
//
//   labor_billing_allocations     which time entry was billed on which invoice
//   material_billing_allocations  which expense was billed on which invoice
//
// The app READS unlinked allocations (handleUnlinkedLaborAllocations /
// handleUnlinkedMaterialAllocations) so you can choose them, but nothing creates
// or links one. That happens inside Airtable.
//
// ── estimate_templates LEFT THIS FILE on 2026-08-20 ────────────────────────
// It was the third table here, for the same "no write path" reason. It has one
// now (db/schema/047 + handleEstimateTemplateSave), so this sync stopped being
// the thing that kept it populated and became the thing that overwrote it: the
// upsert is ON CONFLICT (airtable_id) DO UPDATE, so every edit made in the app
// to one of the five Airtable-era templates was reverted at the top of the
// hour. Silently — nothing here throws, and the app had no way to know.
//
// Companies went with it. It was fetched ONLY to resolve template contractor
// names into `estimate_templates.contractor_name`; nothing else here reads it.
// `handleEstimateTemplates` now JOINs companies live, so a contractor renamed
// after this change still resolves — the stored copy is only a fallback for a
// template whose company has since been deleted.
//
// Airtable's Estimate Templates table is frozen history from here. Do NOT wire
// it back up: re-adding it would resume clobbering the app's edits, which is
// the exact bug this removal fixes.
//
// ⚠ WHY THIS IS NOT COSMETIC. `v_invoices.invoice_total_calc` is computed FROM
// these allocations — Invoice Labor Amount is SUM(hours × rate) over them, and
// Invoice Material Amount is SUM(allocated amount). So before this existed, the
// sequence was:
//
//   invoice a job -> Airtable creates the allocations -> Neon does not know ->
//   the invoice total reads LOW until somebody remembers to run the ETL by hand.
//
// That is a wrong number on a customer-facing figure, with no error anywhere.
// The hand-run ETL was the only thing standing between the migration and that,
// which is a bad place to leave money.
//
// Runs inside qb-time-pull (@hourly) rather than as a second scheduled function:
// one scheduler, one place to look when something stops.
//
// Fails SOFT, deliberately. The timesheet pull is the job that must not stop,
// and a stale allocation is a smaller problem than a missed hour.

const AT_API = "https://api.airtable.com/v0";

async function fetchAllRecords(table, apiKey, baseId) {
  const out = [];
  let offset;
  do {
    const u = new URL(`${AT_API}/${baseId}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const r = await fetch(u, { headers: { Authorization: `Bearer ${apiKey}` } });
    if (!r.ok) throw new Error(`${table} ${r.status}: ${(await r.text()).slice(0, 200)}`);
    const j = await r.json();
    out.push(...(j.records || []));
    offset = j.offset;
  } while (offset);
  return out;
}

const link1 = (v) => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));
const num   = (v) => { if (Array.isArray(v)) v = v[0]; const n = Number(v); return Number.isFinite(n) ? n : null; };
const str   = (v) => { const x = Array.isArray(v) ? v[0] : v; return (x === undefined || x === "" || x === null) ? null : String(x); };

async function upsert(sql, table, cols, rows, syncedAt) {
  if (!rows.length) return 0;
  const all = [...cols, "synced_at"];
  const setList = all.slice(1).map(c => `"${c}" = EXCLUDED."${c}"`).join(", ");
  for (let i = 0; i < rows.length; i += 100) {
    const chunk = rows.slice(i, i + 100);
    const params = [];
    const tuples = chunk.map(r => {
      const row = [...r, syncedAt];
      return `(${row.map(v => { params.push(v); return `$${params.length}`; }).join(",")})`;
    });
    await sql.query(
      `INSERT INTO "${table}" (${all.map(c => `"${c}"`).join(",")}) VALUES ${tuples.join(",")}
         ON CONFLICT ("airtable_id") DO UPDATE SET ${setList}`,
      params
    );
  }
  return rows.length;
}

export async function syncBillingTables(sql, apiKey, baseId) {
  try {
    const syncedAt = new Date().toISOString();
    const [labor, material] = await Promise.all([
      fetchAllRecords("Labor Billing Allocations", apiKey, baseId),
      fetchAllRecords("Material Billing Allocations", apiKey, baseId),
    ]);

    // Allocated Revenue $ is an Airtable FORMULA (hours × rate). It is carried
    // rather than recomputed so Neon and Airtable cannot disagree about a
    // number that feeds Invoice Total.
    await upsert(sql, "labor_billing_allocations",
      ["airtable_id", "time_entry_airtable_id", "invoice_airtable_id", "allocated_hours",
       "bill_rate", "billing_stage"],
      labor.map(r => [r.id, link1(r.fields?.["Time Entry"]), link1(r.fields?.["Invoice"]),
        num(r.fields?.["Allocated Hours"]), num(r.fields?.["Bill Rate"]),
        str(r.fields?.["Billing Stage"])]), syncedAt);

    await upsert(sql, "material_billing_allocations",
      ["airtable_id", "expense_airtable_id", "invoice_airtable_id", "allocated_amount"],
      material.map(r => [r.id, link1(r.fields?.["Expense"]), link1(r.fields?.["Invoice"]),
        num(r.fields?.["Allocated Material Amount $"])]), syncedAt);

    // Resolve the uuid FKs once the parents are present. Separate statements
    // rather than a correlated subquery per row — 2,800+ round trips otherwise.
    await sql.query(`UPDATE material_billing_allocations m SET expense_id = e.id
                       FROM expenses e WHERE e.airtable_id = m.expense_airtable_id
                        AND m.expense_id IS DISTINCT FROM e.id`);
    await sql.query(`UPDATE material_billing_allocations m SET invoice_id = i.id
                       FROM invoices i WHERE i.airtable_id = m.invoice_airtable_id
                        AND m.invoice_id IS DISTINCT FROM i.id`);
    await sql.query(`UPDATE labor_billing_allocations l SET time_entry_id = t.id
                       FROM time_entries t WHERE t.airtable_id = l.time_entry_airtable_id
                        AND l.time_entry_id IS DISTINCT FROM t.id`);
    // Labor's invoice uuid, the twin of the material line above (db/schema/055).
    // It exists for the same reason: someone can still link an allocation to an
    // invoice in the Airtable UI, and that arrives here as a rec id only.
    // `v_invoices` resolves either form, so this is not what keeps the total
    // right — it keeps the COLUMN honest, so a future reader can join on
    // invoice_id without discovering it is populated for some rows and not
    // others. Rows on a NATIVE invoice have no rec id and are untouched by it.
    await sql.query(`UPDATE labor_billing_allocations l SET invoice_id = i.id
                       FROM invoices i WHERE i.airtable_id = l.invoice_airtable_id
                        AND l.invoice_id IS DISTINCT FROM i.id`);

    // ⚠ DELETIONS MATTER HERE, unlike most syncs. Un-allocating material from an
    // invoice DELETES the allocation row in Airtable. Upserting alone would leave
    // the orphan in Neon and the invoice would keep billing for material that is
    // no longer on it — an OVERCHARGE that upsert-only sync cannot correct.
    //
    // ⚠⚠ BUT THE EMPTY GUARD IS NOT OPTIONAL. `WHERE NOT (airtable_id = ANY($1))`
    // with an EMPTY array matches every row, so a single empty-but-successful
    // Airtable response would DELETE THE ENTIRE TABLE and drop every invoice
    // total to zero — silently, because nothing here throws. Neither table is
    // ever legitimately empty (2,606 and 252 rows), so treat empty as a failed
    // fetch and skip the delete rather than trusting it.
    // ⚠⚠ `airtable_id IS NOT NULL` IS LOAD-BEARING, ADDED 2026-08-11. Without it
    // this pass deletes every NEON-NATIVE allocation within the hour.
    //
    // Since Step 3 retired Make from the time path (2026-08-07), QB-pulled time
    // entries land in Neon with NO Airtable twin — 100% of them by the week of
    // 2026-08-10. An allocation for such an entry cannot exist in Airtable at
    // all (its Time Entry field is an Airtable link with nothing to point at),
    // so `_allocations.js` creates it Neon-native with a NULL airtable_id.
    //
    // Those rows are invisible to the fetch above, so the un-guarded predicate
    // classified every one of them as "deleted upstream" and removed it. The
    // invoice total would read correct, then silently drop at the top of the
    // hour. Deleting a row this sync never created is not drift correction.
    //
    // The guard costs nothing: a row with a NULL airtable_id can never have been
    // deleted in Airtable, because it was never there.
    let deleted = 0;
    if (labor.length && material.length) {
      const delL = await sql.query(
        `DELETE FROM labor_billing_allocations
          WHERE airtable_id IS NOT NULL AND NOT (airtable_id = ANY($1::text[])) RETURNING 1`,
        [labor.map(r => r.id)]);
      const delM = await sql.query(
        `DELETE FROM material_billing_allocations
          WHERE airtable_id IS NOT NULL AND NOT (airtable_id = ANY($1::text[])) RETURNING 1`,
        [material.map(r => r.id)]);
      deleted = delL.length + delM.length;
    } else {
      console.error("billing-sync: an allocation fetch came back EMPTY — skipping deletes. " +
                    `labor=${labor.length} material=${material.length}`);
    }

    return { ok: true, labor: labor.length, material: material.length, deleted,
             deletesSkipped: !(labor.length && material.length) };
  } catch (e) {
    console.error(`billing-sync: failed (continuing) — ${e?.message || e}`);
    return { ok: false, error: String(e?.message || e) };
  }
}

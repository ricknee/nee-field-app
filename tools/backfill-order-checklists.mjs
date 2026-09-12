// tools/backfill-order-checklists.mjs — ONE-TIME. Give every ACTIVE inventory
// order that predates db/schema/074 its job and its list on that job.
//
// Orders before 074 carry only job_name, a display string — jobs.po_locked,
// e.g. "MT Liberty DG (LIM 138)". An order is matched only when that string
// names EXACTLY ONE job; anything else is reported and left alone.
//
// Uses the same _order-checklists.js the live path uses, on purpose: lines must
// be keyed identically, or the first order edit after the backfill would
// duplicate every line instead of updating it.
//
// Dry run (writes nothing):
//   $env:DATABASE_URL = "<neon url>"; node tools/backfill-order-checklists.mjs
// Apply:
//   $env:DATABASE_URL = "<neon url>"; node tools/backfill-order-checklists.mjs --apply
//
// Safe to re-run: an order that already has a job_id is not selected, and the
// sync never adds a key a list already holds.

import { neonWrite } from "../netlify/functions/_neon.js";
import { readOrderLineMap, syncOrderChecklist } from "../netlify/functions/_order-checklists.js";

if (!process.env.DATABASE_URL) {
  console.error("DATABASE_URL is not set.");
  process.exit(2);
}
const apply = process.argv.includes("--apply");

const orders = await neonWrite("backfill.orders",
  `SELECT o.id, o.order_number, o.job_name, o.vendor_notes, o.created_by,
          COALESCE(m.ids, '{}') AS job_ids
     FROM material_orders o
     LEFT JOIN LATERAL (
       SELECT array_agg(j.id::text) AS ids
         FROM jobs j
        WHERE j.po_locked = o.job_name
           OR (j.po_locked IS NULL AND j.name = o.job_name)
     ) m ON true
    WHERE o.status = 'Active' AND o.job_id IS NULL
    ORDER BY o.order_number`,
  [], 20000);

console.log(`${apply ? "APPLYING" : "DRY RUN"} — ${orders.length} active order(s) with no job recorded\n`);

let matched = 0, skipped = 0;
for (const o of orders) {
  const ids = Array.isArray(o.job_ids) ? o.job_ids : [];
  const label = `#${o.order_number}  ${o.job_name || "(no job name)"}`;
  if (ids.length !== 1) {
    skipped++;
    console.log(`  SKIP  ${label} — matches ${ids.length} jobs`);
    continue;
  }
  matched++;
  const lines = await readOrderLineMap(o.id);
  const sample = [...lines.values()].slice(0, 3).join(" | ");
  console.log(`  MATCH ${label} → job ${ids[0]} — ${lines.size} line(s): ${sample}${lines.size > 3 ? " | …" : ""}`);

  if (!apply) continue;
  const set = await neonWrite("backfill.setJob",
    `UPDATE material_orders SET job_id = $2::uuid WHERE id = $1::uuid AND job_id IS NULL RETURNING id`,
    [String(o.id), ids[0]]);
  if (!set.length) { console.log("        (job already set by someone else — left alone)"); continue; }
  const r = await syncOrderChecklist(o.id, new Map(), o.created_by || null);
  console.log(`        → ${JSON.stringify(r)}`);
}

console.log(`\n${matched} matched, ${skipped} skipped${apply ? "" : " — nothing written (pass --apply)"}`);

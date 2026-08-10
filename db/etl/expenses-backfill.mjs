// One-off backfill: push any main-base Expense that Neon is missing through the
// SAME sync the app uses. Step E, 2026-08-10.
//
// WHY IT EXISTS. The inventory app's materials push wrote Expenses to Airtable
// only, while the field app has read them from Neon since Step 4d. There is no
// scheduled reload of `expenses` anywhere, so anything pushed after the last
// hand-run load was invisible on the job and absent from GP. Step E fixes the
// push; this catches the rows it already missed.
//
// It DISCOVERS what's missing rather than being told — a hand-written list would
// only ever fix the rows somebody already knew about.
//
// Safe to re-run: syncExpenseToNeon upserts ON CONFLICT (airtable_id).
//
//   node db/etl/expenses-backfill.mjs           # report only
//   node db/etl/expenses-backfill.mjs --apply   # actually write

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const APPLY = process.argv.includes("--apply");

// minimal .env reader — no dependency, tolerates quotes/comments
const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8")
    .split(/\r?\n/)
    .filter(l => l.trim() && !l.trim().startsWith("#") && l.includes("="))
    .map(l => { const i = l.indexOf("="); return [l.slice(0, i).trim(), l.slice(i + 1).trim().replace(/^["']|["']$/g, "")]; })
);

// _neon.js reads DATABASE_URL; the local .env calls it NEON_URL.
process.env.DATABASE_URL = env.DATABASE_URL || env.NEON_URL;
if (!process.env.DATABASE_URL) throw new Error("no DATABASE_URL / NEON_URL in .env");

// PRODUCTION main base, named explicitly rather than read from .env.
// `.env`'s AIRTABLE_BASE_ID points at the SANDBOX (appojcmXxqDUdJDYB) for local
// dev, and silently backfilling Neon from a sandbox copy of Expenses would be a
// quiet disaster. This job only ever means the real base, so it says so.
const BASE = "appiqWg6SvKcGfMAu";
const PAT  = env.AIRTABLE_PROD_READ_PAT;
if (!PAT) throw new Error("AIRTABLE_PROD_READ_PAT missing — the local AIRTABLE_API_KEY is sandbox-scoped");

const { neonQuery } = await import(`file://${path.join(ROOT, "netlify/functions/_neon.js")}`);
const { syncExpenseToNeon } = await import(`file://${path.join(ROOT, "netlify/functions/_expenses.js")}`);

// ── read every Airtable expense (fields keyed by NAME, which is what the sync wants)
const airtable = [];
let offset;
do {
  const u = new URL(`https://api.airtable.com/v0/${BASE}/${encodeURIComponent("Expenses")}`);
  u.searchParams.set("pageSize", "100");
  if (offset) u.searchParams.set("offset", offset);
  const r = await fetch(u, { headers: { Authorization: `Bearer ${PAT}` } });
  if (!r.ok) throw new Error(`Airtable ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const j = await r.json();
  airtable.push(...j.records);
  offset = j.offset;
} while (offset);

// ── what Neon already has
const q = await neonQuery(`SELECT airtable_id FROM expenses WHERE airtable_id IS NOT NULL`);
if (!q?.rows) throw new Error(`Neon read failed: ${q?.error || "not configured"}`);
const inNeon = new Set(q.rows.map(r => r.airtable_id));

const missing = airtable.filter(r => !inNeon.has(r.id));

console.log(`Airtable expenses : ${airtable.length}`);
console.log(`Already in Neon   : ${inNeon.size}`);
console.log(`MISSING           : ${missing.length}`);
for (const r of missing) {
  const f = r.fields || {};
  console.log(`  ${r.id}  ${f["Expense Date"] || "?"}  ${String(f["Total Cost (Actual)"] ?? "?").padStart(9)}  ${(f["Description"] || "").slice(0, 60)}`);
}

if (!missing.length) { console.log("\nNothing to do."); process.exit(0); }
if (!APPLY) { console.log("\nReport only. Re-run with --apply to write."); process.exit(0); }

let done = 0;
for (const rec of missing) {
  try { await syncExpenseToNeon(rec); done++; }
  catch (e) { console.error(`  FAILED ${rec.id}: ${e.message}`); }
}

const after = await neonQuery(`SELECT count(*)::int AS n FROM expenses`);
console.log(`\nsynced ${done}/${missing.length}; Neon now holds ${after?.rows?.[0]?.n} expenses`);
process.exit(done === missing.length ? 0 : 1);

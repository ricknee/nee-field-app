// 🔴 RETIRED 2026-08-24 BY THE IDENTITY CUTOVER, SLICE 4c. THIS JOB REFUSES TO
// RUN, AND MUST NOT BE RE-ENABLED. See the hard stop below.
//
// One-off backfill: push any main-base Expense that Neon is missing through the
// SAME sync the app uses. Step E, 2026-08-10.
//
// WHY IT EXISTED. The inventory app's materials push wrote Expenses to Airtable
// only, while the field app has read them from Neon since Step 4d. There is no
// scheduled reload of `expenses` anywhere, so anything pushed after the last
// hand-run load was invisible on the job and absent from GP. Step E fixed the
// push; this caught the rows it had already missed. It DISCOVERED what was
// missing rather than being told, because a hand-written list would only ever
// fix the rows somebody already knew about.
//
// ── WHY IT IS NOW DANGEROUS ────────────────────────────────────────────────
// Expenses are Neon-native since slices 4b (`a04b11f`, field app) and 4c
// (inventory push). A native expense has `airtable_id NULL` and gets a
// best-effort Airtable MIRROR whose rec id is deliberately NEVER stamped back —
// R2 receipt keys are `expenses/<handle>/`, so a handle that flips orphans every
// receipt.
//
// That is precisely what breaks this job. Its definition of "missing" is
// `Airtable rec id not present in expenses.airtable_id`, and EVERY mirror of a
// native expense matches it — permanently, by design. Running it would
// `syncExpenseToNeon` each mirror, and `ON CONFLICT (airtable_id)` does not
// conflict on a NULL, so it INSERTS A SECOND EXPENSE for spend that is already
// recorded. Both copies then count in every job-cost and GP figure.
//
// ⚠⚠ AND IT CANNOT BE FIXED BY FILTERING. Nothing on the Airtable side
// identifies a record as the mirror of a Neon row — that back-pointer is the
// exact thing the R2 rule forbids. The 4b note said "if an expense ETL is ever
// added it MUST skip rows it can't match by rec id"; this one predates that
// note, and there is no such rule it can implement. So it stops, rather than
// being made clever.
//
// Kept on disk, not deleted, because the discovery query and the
// production-PAT/sandbox-PAT note below are the reason anyone would reach for
// it again — and this comment is what should meet them when they do.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const APPLY = process.argv.includes("--apply");

// ── HARD STOP. Read the header before removing this. ───────────────────────
// Deliberately before the .env read, so it fires on any machine, configured or
// not, and cannot be reached by "it printed a report, so --apply must be fine".
// The report is not safe either: it would list every native expense's mirror as
// MISSING, which reads as a backlog to fix and is a list of duplicates to
// create.
console.error(`
🔴 expenses-backfill is RETIRED and refuses to run.

Expenses are Neon-native (identity cutover slices 4b + 4c, 2026-08-24).
Every Airtable Expense record whose rec id is absent from Neon is now a MIRROR
of a row Neon already holds — not a gap. Syncing them would insert a second
copy of spend that is already recorded, and both would count in GP.

There is no filter that fixes this: nothing Airtable-side marks a record as a
mirror. If you are chasing a genuinely missing expense, find it by hand in Neon
and add it there; do not reopen this direction.

See the header of this file and docs/PLAN-airtable-identity-cutover.md.
`);
process.exit(2);

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

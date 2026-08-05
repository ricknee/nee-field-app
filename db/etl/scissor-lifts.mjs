// Scissor Lifts → Neon + photos → R2 (roadmap Step 4b, lifts half).
//
// TWO PHASES, and the second one is the reason this is a script rather than a
// hand-run query:
//
//   1. DATA   — 10 rows, upserted by airtable_id. Trivial, idempotent.
//   2. PHOTOS — the actual work. Airtable serves attachments from
//               v5.airtableusercontent.com on SIGNED URLs THAT EXPIRE (~2 h).
//               They only work today because the live handler re-fetches them
//               from Airtable on every request. Copy the URL into Neon and every
//               lift photo breaks the same afternoon — silently, and hours later.
//               So each photo is fetched and re-uploaded to R2 in ONE PASS while
//               its URL is still valid.
//
// Phase 2 needs R2_* credentials, which live in Netlify and are usually NOT in the
// local .env. Without them phase 2 SKIPS LOUDLY and phase 1 still completes — it
// does not half-migrate and claim success.
//
//   node db/etl/scissor-lifts.mjs             # both phases if R2 is configured
//   node db/etl/scissor-lifts.mjs --data-only # phase 1 only
//
// Re-runnable: photos already in R2 are skipped by key, so a second run costs
// nothing and cannot duplicate.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "../../netlify/functions/node_modules/@neondatabase/serverless/index.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.REPO_ROOT || path.resolve(HERE, "..", "..");
const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8").split(/\r?\n/)
    .filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);
// _r2.js reads these off process.env at call time, so seed them from .env first.
for (const k of ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"]) {
  if (env[k] && !process.env[k]) process.env[k] = env[k];
}

const KEY  = env.AIRTABLE_PROD_READ_PAT || env.AIRTABLE_API_KEY;
const BASE = env.AIRTABLE_PROD_BASE_ID || "appiqWg6SvKcGfMAu";
const NEON = process.env.NEON_URL || env.NEON_URL || env.DATABASE_URL;
if (!KEY)  throw new Error("no Airtable PAT in .env (AIRTABLE_PROD_READ_PAT)");
if (!NEON) throw new Error("no Neon connection string (NEON_URL in .env)");
const DATA_ONLY = process.argv.includes("--data-only");

const sql = neon(NEON);
const nul = v => (v === undefined || v === "" ? null : v);

// ── phase 1: data ──────────────────────────────────────────────────────────
console.log(`extracting Scissor Lifts from ${BASE} (read-only) ...`);
const r = await fetch(`https://api.airtable.com/v0/${BASE}/${encodeURIComponent("Scissor Lifts")}?pageSize=100`,
  { headers: { Authorization: `Bearer ${KEY}` } });
if (!r.ok) throw new Error(`Scissor Lifts ${r.status}: ${await r.text()}`);
const records = (await r.json()).records;
console.log(`  ${records.length} lifts`);

const idByAirtable = new Map();
for (const rec of records) {
  const f = rec.fields || {};
  const [row] = await sql.query(
    `INSERT INTO scissor_lifts
       (airtable_id, name, status, current_job, assigned_to, date_deployed, notes,
        hooks_left, box_left, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6::date,$7,$8,$9,now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       name = EXCLUDED.name, status = EXCLUDED.status,
       current_job = EXCLUDED.current_job, assigned_to = EXCLUDED.assigned_to,
       date_deployed = EXCLUDED.date_deployed, notes = EXCLUDED.notes,
       hooks_left = EXCLUDED.hooks_left, box_left = EXCLUDED.box_left,
       synced_at = now()
     RETURNING id`,
    [rec.id, f["Lift Name"] || "(unnamed)", f["Status"] || "Available",
     nul(f["Current Job"]), nul(f["Assigned To"]), nul(f["Date Deployed"]),
     nul(f["Notes"]), f["Lift Hooks Left at Job"] === true, f["Lift Box Left at Job"] === true]);
  idByAirtable.set(rec.id, row.id);
}
console.log(`  loaded ${idByAirtable.size} rows`);

// Natural sort check — "Lift #10" must land after "Lift #2", not between #1 and #2.
const ordered = await sql.query(
  `SELECT name FROM scissor_lifts
    ORDER BY NULLIF(regexp_replace(name, '\\D', '', 'g'), '')::int NULLS LAST, name`);
console.log(`  natural order: ${ordered.map(x => x.name).join(", ")}`);

// ── phase 2: photos ────────────────────────────────────────────────────────
if (DATA_ONLY) {
  console.log("\n--data-only: skipping the photo copy.");
  process.exit(0);
}
const { r2Enabled, presignPut, listByPrefix } = await import("../../netlify/functions/_r2.js");
if (!r2Enabled()) {
  console.log(
    "\n⚠ PHOTOS NOT COPIED — R2 is not configured locally.\n" +
    "  Phase 1 (data) is complete and correct; the photos are still only in Airtable.\n" +
    "  Add R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_BUCKET to .env\n" +
    "  (copy them from the Netlify dashboard) and re-run. Re-running is free — photos\n" +
    "  already in R2 are skipped by key.\n" +
    "  DO NOT flip the read handler to Neon until this has run: Airtable attachment\n" +
    "  URLs expire in about two hours, so a stored URL would break the same day.");
  process.exit(2);
}

// One list call up front, so an interrupted run resumes instead of re-uploading.
const existing = new Set((await listByPrefix("lifts/")).map(o => o.key));
console.log(`\ncopying photos to R2 (${existing.size} already there) ...`);

let copied = 0, skipped = 0, failed = 0;
for (const rec of records) {
  const liftId = idByAirtable.get(rec.id);
  const photos = rec.fields?.["Photo"] || [];
  for (const att of photos) {
    // Keyed on the ATTACHMENT id, not the filename: two lifts can both have
    // "photo.jpg", and a rename in Airtable must not orphan the copy.
    const ext = (att.filename?.match(/\.[a-z0-9]+$/i) || [".jpg"])[0].toLowerCase();
    const key = `lifts/${liftId}/${att.id}${ext}`;
    if (existing.has(key)) { skipped++; continue; }
    try {
      // Fetch and upload back to back — the URL is only valid for a couple of hours.
      const img = await fetch(att.url);
      if (!img.ok) throw new Error(`download ${img.status}`);
      const body = Buffer.from(await img.arrayBuffer());
      const put = await fetch(presignPut(key, att.type || "image/jpeg"), {
        method: "PUT", body, headers: { "content-type": att.type || "image/jpeg" } });
      if (!put.ok) throw new Error(`upload ${put.status} ${(await put.text()).slice(0, 120)}`);
      console.log(`  ✓ ${rec.fields["Lift Name"]} → ${key} (${body.length} bytes)`);
      copied++;
    } catch (e) {
      console.error(`  ✗ ${rec.fields["Lift Name"]} ${key}: ${e.message}`);
      failed++;
    }
  }
}

const after = (await listByPrefix("lifts/")).length;
const expected = records.reduce((n, x) => n + (x.fields?.Photo?.length || 0), 0);
console.log(`\ncopied ${copied}, already present ${skipped}, failed ${failed}`);
console.log(`R2 now holds ${after} objects under lifts/ — Airtable has ${expected} attachments`);
console.log(after === expected && !failed
  ? "All photos accounted for."
  : "⚠ MISMATCH — do not flip the read handler until this reconciles.");
process.exit(failed || after !== expected ? 1 : 0);

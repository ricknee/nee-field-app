// Schedule Entries → Neon (migration Step 4a).
//
// Reads PRODUCTION Airtable read-only and upserts by `airtable_id`, so it is safe
// to re-run. Crew is a many-to-many, so it lands in schedule_entry_crew rather than
// an array column — Step 4c (generators + service history) needs real relations and
// this is the slice where that pattern gets proven.
//
//   node db/etl/schedule-entries.mjs            # load + verify
//   NEON_URL='postgres://…' node …             # override to run against a branch
//
// Credentials come from the repo .env, same as time-entries-full.mjs.
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
const KEY  = env.AIRTABLE_PROD_READ_PAT || env.AIRTABLE_API_KEY;
const BASE = env.AIRTABLE_PROD_BASE_ID || "appiqWg6SvKcGfMAu";
const NEON = process.env.NEON_URL || env.NEON_URL || env.DATABASE_URL;
if (!KEY)  throw new Error("no Airtable PAT in .env (AIRTABLE_PROD_READ_PAT)");
if (!NEON) throw new Error("no Neon connection string (NEON_URL in .env)");

const sql = neon(NEON);
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchAll(table) {
  const out = []; let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${BASE}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const r = await fetch(u, { headers: { Authorization: `Bearer ${KEY}` } });
    if (!r.ok) throw new Error(`${table} ${r.status}: ${await r.text()}`);
    const d = await r.json();
    out.push(...d.records); offset = d.offset;
    if (offset) await sleep(220);
  } while (offset);
  return out;
}

console.log(`extracting Schedule Entries from ${BASE} (read-only) ...`);
const records = await fetchAll("Schedule Entries");
console.log(`  ${records.length} rows`);

// Resolve Airtable ids to Neon uuids up front — one query each, not per row.
const jobMap = new Map((await sql.query(`SELECT id, airtable_id FROM jobs WHERE airtable_id IS NOT NULL`))
  .map(r => [r.airtable_id, r.id]));
const empMap = new Map((await sql.query(`SELECT id, airtable_id FROM employees WHERE airtable_id IS NOT NULL`))
  .map(r => [r.airtable_id, r.id]));

const nul = v => (v === undefined || v === "" ? null : v);
const first = v => (Array.isArray(v) && v.length ? v[0] : null);

let loaded = 0, unmatchedJobs = 0, unmatchedCrew = 0;
const now = new Date().toISOString();
for (const rec of records) {
  const f = rec.fields || {};
  const jobAt = first(f["Job"]);
  const jobId = jobAt ? (jobMap.get(jobAt) ?? null) : null;
  if (jobAt && !jobId) unmatchedJobs++;

  const [row] = await sql.query(
    `INSERT INTO schedule_entries
       (airtable_id, title, entry_type, job_id, start_date, end_date, notes, source, synced_at)
     VALUES ($1,$2,$3,$4,$5::date,$6::date,$7,'airtable',$8)
     ON CONFLICT (airtable_id) DO UPDATE SET
       title = EXCLUDED.title, entry_type = EXCLUDED.entry_type, job_id = EXCLUDED.job_id,
       start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
       notes = EXCLUDED.notes, synced_at = EXCLUDED.synced_at
     RETURNING id`,
    [rec.id, nul(f["Title"]), f["Entry Type"] || "Job", jobId,
     nul(f["Start Date"]), nul(f["End Date"]), nul(f["Notes"]), now]);

  // Crew is replaced wholesale rather than diffed — the set is tiny and a
  // delete+insert cannot leave a half-updated crew behind.
  const crewAt = Array.isArray(f["Crew"]) ? f["Crew"] : [];
  await sql.query(`DELETE FROM schedule_entry_crew WHERE schedule_entry_id = $1`, [row.id]);
  for (const at of crewAt) {
    const empId = empMap.get(at);
    if (!empId) { unmatchedCrew++; continue; }
    await sql.query(
      `INSERT INTO schedule_entry_crew (schedule_entry_id, employee_id) VALUES ($1,$2)
       ON CONFLICT DO NOTHING`, [row.id, empId]);
  }
  loaded++;
}
console.log(`loaded ${loaded} entries` +
  (unmatchedJobs ? `, ${unmatchedJobs} with a job link that is not in Neon` : "") +
  (unmatchedCrew ? `, ${unmatchedCrew} crew link(s) unmatched` : ""));

// ── acceptance checks ─────────────────────────────────────────────────────
const atCrewPairs = records.reduce((n, r) => n + (Array.isArray(r.fields?.Crew) ? r.fields.Crew.length : 0), 0);
const atWithJob   = records.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.length).length;
const [neonCounts] = await sql.query(
  `SELECT (SELECT count(*) FROM schedule_entries)::int AS entries,
          (SELECT count(*) FROM schedule_entry_crew)::int AS crew_pairs,
          (SELECT count(*) FROM schedule_entries WHERE job_id IS NOT NULL)::int AS with_job,
          (SELECT count(*) FROM schedule_entries WHERE start_date IS NOT NULL)::int AS with_start`);
const atWithStart = records.filter(r => r.fields?.["Start Date"]).length;

const checks = [
  ["row count",        records.length, neonCounts.entries],
  ["crew links",       atCrewPairs - unmatchedCrew, neonCounts.crew_pairs],
  ["entries with job", atWithJob - unmatchedJobs,   neonCounts.with_job],
  ["entries with start date", atWithStart,          neonCounts.with_start],
];
console.log("\n== ACCEPTANCE CHECKS (Airtable vs Neon) ==");
console.table(checks.map(([check, airtable, neon]) => ({ check, airtable, neon, ok: airtable === neon })));
const failed = checks.filter(([, a, b]) => a !== b);
console.log(failed.length ? `\n${failed.length} CHECK(S) FAILED` : "\nAll checks passed.");
process.exit(failed.length ? 1 : 0);

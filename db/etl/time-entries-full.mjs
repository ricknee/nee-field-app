// Time Entries → Neon: FULL ETL (plan step 2) + acceptance checks (step 4a).
//
// Reads PRODUCTION Airtable (read-only) and upserts into Neon by `airtable_id`,
// so it is safe to re-run at any time — that idempotency is what lets the
// migration pause between sittings and catch up later.
//
// Nothing in the app reads Neon yet, so this touches no live behavior.
//
// Usage (from a scratch dir with the driver installed):
//   npm i @neondatabase/serverless
//   NEON_URL='postgres://...' node <repo>/db/etl/time-entries-full.mjs
//
// Credentials, both read from the repo .env (gitignored):
//   AIRTABLE_PROD_READ_PAT  preferred — a READ-ONLY PAT scoped to prod
//   AIRTABLE_API_KEY        fallback (the local-dev PAT only reaches sandbox)
// Override the base with AIRTABLE_PROD_BASE_ID if needed.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "@neondatabase/serverless";

const HERE = path.dirname(fileURLToPath(import.meta.url));
// REPO_ROOT lets the script run from a scratch dir that has the Neon driver
// installed (the repo deliberately has no root package.json).
const ROOT = process.env.REPO_ROOT || path.resolve(HERE, "..", "..");

const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8")
    .split(/\r?\n/)
    .filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);

const KEY  = env.AIRTABLE_PROD_READ_PAT || env.AIRTABLE_API_KEY;
const BASE = env.AIRTABLE_PROD_BASE_ID || "appiqWg6SvKcGfMAu";
const NEON = process.env.NEON_URL;
if (!KEY)  throw new Error("no Airtable PAT in .env (AIRTABLE_PROD_READ_PAT)");
if (!NEON) throw new Error("set NEON_URL");

const sql = neon(NEON);
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── extract ───────────────────────────────────────────────────────────────
// Airtable REST returns records keyed by FIELD NAME. Rate limit is 5 req/sec
// per base; requests are sequential with a floor to stay well under it.
async function fetchAll(table, params = {}) {
  const out = [];
  let offset, pages = 0;
  do {
    const p = new URLSearchParams({ pageSize: "100", ...params });
    if (offset) p.set("offset", offset);
    const t0 = Date.now();
    const r = await fetch(`https://api.airtable.com/v0/${BASE}/${encodeURIComponent(table)}?${p}`,
      { headers: { Authorization: `Bearer ${KEY}` } });
    if (!r.ok) throw new Error(`${table} ${r.status}: ${await r.text()}`);
    const d = await r.json();
    out.push(...d.records);
    offset = d.offset;
    if (++pages % 25 === 0) process.stdout.write(`  ${table}: ${out.length} rows\n`);
    const spent = Date.now() - t0;
    if (offset && spent < 220) await sleep(220 - spent);
  } while (offset);
  return out;
}

console.log(`extracting from ${BASE} (read-only) ...`);
const employees = await fetchAll("Employees");
const jobs      = await fetchAll("Jobs", { "fields[]": "Job Name" });
const entries   = await fetchAll("Time Entries");
console.log(`extracted: ${employees.length} employees, ${jobs.length} jobs, ${entries.length} time entries`);

// ── Airtable-side truth, computed BEFORE loading ──────────────────────────
// These are the acceptance numbers. Airtable's own `Hours` field is the
// authority (it applies the quarter-hour rounding rule); Neon must reproduce
// it from duration_seconds via the generated column.
const src = { entries: entries.length, hours: 0, blankName: 0, noLink: 0, first: "", last: "", buckets: new Map() };
for (const r of entries) {
  const f = r.fields;
  const name = String(f["Job Name (Text)"] || "").trim();
  const hrs  = Number(f["Hours"]) || 0;
  const d    = f["Work Date"] || "";
  src.hours += hrs;
  if (!name) src.blankName++;
  if (!Array.isArray(f["Job"]) || !f["Job"].length) src.noLink++;
  if (d) {
    if (!src.first || d < src.first) src.first = d;
    if (!src.last  || d > src.last)  src.last  = d;
  }
  if (name) src.buckets.set(name, (src.buckets.get(name) || 0) + hrs);
}
src.hours = Math.round(src.hours * 100) / 100;

// ── load ──────────────────────────────────────────────────────────────────
const nul = v => (v === undefined || v === "" ? null : v);
const link = v => (Array.isArray(v) && v.length ? v[0] : null);

async function upsertBatch(table, cols, rows, conflict, batch = 300) {
  let done = 0;
  for (let i = 0; i < rows.length; i += batch) {
    const chunk = rows.slice(i, i + batch);
    const params = [];
    const tuples = chunk.map(row => {
      const ph = row.map(v => { params.push(v); return `$${params.length}`; });
      return `(${ph.join(",")})`;
    });
    const setList = cols.filter(c => c !== conflict).map(c => `"${c}"=EXCLUDED."${c}"`).join(", ");
    await sql.query(
      `INSERT INTO ${table} (${cols.map(c => `"${c}"`).join(",")}) VALUES ${tuples.join(",")}
       ON CONFLICT ("${conflict}") DO UPDATE SET ${setList}`,
      params
    );
    done += chunk.length;
    process.stdout.write(`  ${table}: ${done}/${rows.length}\r`);
  }
  process.stdout.write(`  ${table}: ${done}/${rows.length}\n`);
}

console.log("loading ...");
await upsertBatch("employees", ["airtable_id", "name", "username", "role", "active"],
  employees.map(e => [e.id, e.fields["Employee Name"] || "(unnamed)", nul(e.fields["Username"]),
                      nul(e.fields["Role"]), e.fields["Active"] === true]), "airtable_id");

await upsertBatch("jobs", ["airtable_id", "name"],
  jobs.map(j => [j.id, j.fields["Job Name"] || "(unnamed)"]), "airtable_id");

// Resolve FKs in JS so the row inserts carry literal uuids (no per-row subselect).
const empMap = new Map((await sql.query(`SELECT id, airtable_id FROM employees`)).map(r => [r.airtable_id, r.id]));
const jobMap = new Map((await sql.query(`SELECT id, airtable_id FROM jobs`)).map(r => [r.airtable_id, r.id]));

await upsertBatch("time_entries",
  ["airtable_id", "employee_name", "employee_id", "work_date", "duration_seconds", "city_taxes",
   "class", "labor_type", "source", "notes", "billable", "job_id", "job_name", "labor_reviewed",
   "airtable_created_at"],
  entries.map(r => {
    const f = r.fields;
    return [
      r.id,
      nul(f["Employee"]),
      empMap.get(link(f["Employee (Linked)"])) ?? null,
      nul(f["Work Date"]),
      Number(f["Duration (Seconds)"] ?? 0),
      nul(f["City Taxes"]),
      nul(f["Class"]),
      nul(f["Labor Type"]),
      nul(f["Source"]),
      nul(f["Notes"]),
      f["Billable"] === undefined ? null : f["Billable"] === true,
      jobMap.get(link(f["Job"])) ?? null,
      nul(f["Job Name (Text)"]),
      f["Labor Reviewed"] === true,
      r.createdTime,
    ];
  }), "airtable_id", 200);

// ── acceptance checks ─────────────────────────────────────────────────────
const [tot] = await sql.query(`
  SELECT count(*)::int AS entries,
         round(sum(hours), 2)::float8 AS hours,
         min(work_date)::text AS first,
         max(work_date)::text AS last,
         count(*) FILTER (WHERE job_name IS NULL)::int AS blank_name,
         count(*) FILTER (WHERE job_id IS NULL)::int AS no_link,
         count(DISTINCT job_name)::int AS distinct_names
  FROM time_entries`);

const neonBuckets = new Map((await sql.query(
  `SELECT job_name, round(sum(hours),2)::float8 AS hours
   FROM time_entries WHERE job_name IS NOT NULL GROUP BY job_name`
)).map(r => [r.job_name, r.hours]));

const checks = [];
const chk = (label, a, b, tol = 0) =>
  checks.push({ check: label, airtable: a, neon: b, ok: typeof a === "number" ? Math.abs(a - b) <= tol : a === b });

chk("row count",        src.entries, tot.entries);
chk("total hours",      src.hours, tot.hours, 0.01);
chk("first work date",  src.first, tot.first);
chk("last work date",   src.last, tot.last);
chk("blank job_name",   src.blankName, tot.blank_name);
chk("no job link",      src.noLink, tot.no_link);
chk("distinct job_name", src.buckets.size, tot.distinct_names);

let bucketDiffs = 0;
for (const [name, hrs] of src.buckets) {
  const n = neonBuckets.get(name);
  if (n === undefined || Math.abs(n - hrs) > 0.01) bucketDiffs++;
}
chk("per-job hour buckets matching", 0, bucketDiffs);

console.log("\n== ACCEPTANCE CHECKS (Airtable vs Neon) ==");
console.table(checks);

const failed = checks.filter(c => !c.ok);
if (failed.length) {
  console.error(`\nFAILED ${failed.length} check(s) — Neon copy is NOT verified.`);
  process.exit(1);
}
console.log("\nAll checks passed — Neon copy verified against production Airtable.");

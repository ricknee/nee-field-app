// Time Entries → Neon: sample extractor (step 1 validation).
// READ-ONLY against the PRODUCTION base. Emits a .sql file of INSERTs plus a
// shape report, so the schema can be proven against real data before the full
// 14.5k-row ETL. This is the extractor skeleton for step 2.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const OUT  = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(OUT, "..", "..");

// .env holds a local-dev PAT; base is pinned to PROD deliberately (read-only).
const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8")
    .split(/\r?\n/)
    .filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);
const KEY  = env.AIRTABLE_API_KEY;
// The local-dev PAT is scoped to the SANDBOX base only (a structural duplicate
// of prod). Schema/field-mapping validation is identical there; the acceptance
// row counts must be re-checked against prod in step 2 with a prod-scoped PAT.
const BASE = env.AIRTABLE_BASE_ID;       // appojcmXxqDUdJDYB — sandbox
if (!KEY) throw new Error("AIRTABLE_API_KEY missing from .env");

async function fetchPage(table, params) {
  const qs = new URLSearchParams(params);
  const url = `https://api.airtable.com/v0/${BASE}/${encodeURIComponent(table)}?${qs}`;
  const r = await fetch(url, { headers: { Authorization: `Bearer ${KEY}` } });
  if (!r.ok) throw new Error(`${table} ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchAll(table, params = {}) {
  const out = [];
  let offset;
  do {
    const p = { pageSize: "100", ...params };
    if (offset) p.offset = offset;
    const d = await fetchPage(table, p);
    out.push(...d.records);
    offset = d.offset;
  } while (offset);
  return out;
}

// Airtable REST returns records keyed by FIELD NAME (why F.* uses names).
async function sample(table, n, dir) {
  const out = [];
  let offset;
  do {
    const p = {
      pageSize: "100",
      "sort[0][field]": "Work Date",
      "sort[0][direction]": dir,
    };
    if (offset) p.offset = offset;
    const d = await fetchPage(table, p);
    out.push(...d.records);
    offset = d.offset;
  } while (offset && out.length < n);
  return out.slice(0, n);
}

const q = s => s === null || s === undefined || s === "" ? "NULL" : `'${String(s).replace(/'/g, "''")}'`;
const num = v => (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) ? "NULL" : String(Number(v));
const bool = v => v === true ? "true" : "false";
const linkId = v => Array.isArray(v) && v.length ? v[0] : null;

const [oldest, newest, employees, jobs] = await Promise.all([
  sample("Time Entries", 100, "asc"),
  sample("Time Entries", 100, "desc"),
  fetchAll("Employees"),
  fetchAll("Jobs", { "fields[]": "Job Name" }),
]);

// ── shape report ──────────────────────────────────────────────────────────
const entries = [...oldest, ...newest];
const seenFields = new Map();
for (const r of entries) {
  for (const [k, v] of Object.entries(r.fields)) {
    if (!seenFields.has(k)) seenFields.set(k, { n: 0, sample: v });
    seenFields.get(k).n++;
  }
}

const report = [];
report.push(`sampled ${entries.length} time entries (100 oldest + 100 newest)`);
report.push(`employees: ${employees.length}   jobs: ${jobs.length}`);
report.push("");
report.push("FIELD NAMES PRESENT ON TIME ENTRIES (name | count | sample):");
for (const [k, v] of [...seenFields].sort()) {
  report.push(`  ${k.padEnd(34)} ${String(v.n).padStart(4)}  ${JSON.stringify(v.sample).slice(0, 70)}`);
}

// Verify the quarter-hour rounding rule against real rows.
let mismatchDiv = 0, mismatchRound = 0, checked = 0;
for (const r of entries) {
  const sec = r.fields["Duration (Seconds)"];
  const hrs = r.fields["Hours"];
  if (sec === undefined || hrs === undefined) continue;
  checked++;
  if (Math.abs(sec / 3600 - hrs) > 0.001) mismatchDiv++;
  if (Math.abs(Math.round((sec / 3600) * 4) / 4 - hrs) > 0.001) mismatchRound++;
}
report.push("");
report.push(`HOURS RULE CHECK over ${checked} rows:`);
report.push(`  plain  seconds/3600      mismatches: ${mismatchDiv}`);
report.push(`  quarter-hour ROUND rule  mismatches: ${mismatchRound}`);

const noName = entries.filter(r => !String(r.fields["Job Name (Text)"] || "").trim()).length;
const noLink = entries.filter(r => !linkId(r.fields["Job"])).length;
const fracSec = entries.filter(r => { const s = r.fields["Duration (Seconds)"]; return s != null && s % 1 !== 0; }).length;
report.push("");
report.push(`blank Job Name (Text): ${noName}   no Job link: ${noLink}   fractional seconds: ${fracSec}`);

fs.writeFileSync(path.join(OUT, "shape-report.txt"), report.join("\n"), "utf8");

// ── SQL ───────────────────────────────────────────────────────────────────
const sql = [];
sql.push("BEGIN;");

for (const e of employees) {
  const f = e.fields;
  sql.push(`INSERT INTO employees (airtable_id, name, username, role, active) VALUES (${q(e.id)}, ${q(f["Employee Name"] || "(unnamed)")}, ${q(f["Username"])}, ${q(f["Role"])}, ${bool(f["Active"] === true)}) ON CONFLICT (airtable_id) DO UPDATE SET name=EXCLUDED.name, username=EXCLUDED.username, role=EXCLUDED.role, active=EXCLUDED.active;`);
}
for (const j of jobs) {
  const f = j.fields;
  sql.push(`INSERT INTO jobs (airtable_id, name) VALUES (${q(j.id)}, ${q(f["Job Name"] || "(unnamed)")}) ON CONFLICT (airtable_id) DO UPDATE SET name=EXCLUDED.name;`);
}
for (const r of entries) {
  const f = r.fields;
  const empRec = linkId(f["Employee (Linked)"]);
  const jobRec = linkId(f["Job"]);
  sql.push(
    `INSERT INTO time_entries (airtable_id, employee_name, employee_id, work_date, duration_seconds, city_taxes, class, labor_type, source, notes, billable, job_id, job_name, labor_reviewed, airtable_created_at) VALUES (` +
    [
      q(r.id),
      q(f["Employee"]),
      empRec ? `(SELECT id FROM employees WHERE airtable_id=${q(empRec)})` : "NULL",
      q(f["Work Date"]),
      num(f["Duration (Seconds)"] ?? 0),
      q(f["City Taxes"]),
      q(f["Class"]),
      q(f["Labor Type"]),
      q(f["Source"]),
      q(f["Notes"]),
      f["Billable"] === undefined ? "NULL" : bool(f["Billable"] === true),
      jobRec ? `(SELECT id FROM jobs WHERE airtable_id=${q(jobRec)})` : "NULL",
      q(f["Job Name (Text)"]),
      bool(f["Labor Reviewed"] === true),
      q(r.createdTime),
    ].join(", ") +
    `) ON CONFLICT (airtable_id) DO UPDATE SET employee_name=EXCLUDED.employee_name, employee_id=EXCLUDED.employee_id, work_date=EXCLUDED.work_date, duration_seconds=EXCLUDED.duration_seconds, city_taxes=EXCLUDED.city_taxes, class=EXCLUDED.class, labor_type=EXCLUDED.labor_type, source=EXCLUDED.source, notes=EXCLUDED.notes, billable=EXCLUDED.billable, job_id=EXCLUDED.job_id, job_name=EXCLUDED.job_name, labor_reviewed=EXCLUDED.labor_reviewed;`
  );
}
sql.push("COMMIT;");
fs.writeFileSync(path.join(OUT, "sample-load.sql"), sql.join("\n"), "utf8");

console.log(report.join("\n"));
console.log(`\nwrote sample-load.sql (${sql.length} statements)`);

// Day-level reconciliation of QuickBooks Time against Neon, for deciding whether
// hours Make failed to import represent real unpaid work.
//
// WHY NOT JUST TRUST THE "MISSING" COUNT: the puller flags a timesheet as new when
// its qb_timesheet_id is absent from Neon. That is the right test for "should I
// insert this row", but it is the WRONG test for "is someone owed money". QB splits
// a working day into several timesheets per clock-in/out segment, and a timesheet
// whose duration was edited in QB after Make imported it also looks unmatched. So an
// unmatched id can mean any of:
//   - a genuinely dropped segment  -> real unpaid hours
//   - an edited/superseded version -> already paid, possibly at the wrong amount
//   - a re-created duplicate       -> already paid, nothing owed
//
// The only honest comparison is TOTAL HOURS PER (employee, work_date, job), both
// sides. That is what this reports. Anything it flags still deserves a human check
// against the employee's actual day before money moves.
//
// Usage:
//   NEON_URL='postgres://...' node db/etl/qb-gap-report.mjs 2026-07-07 2026-07-24
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "../../netlify/functions/node_modules/@neondatabase/serverless/index.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.REPO_ROOT || path.resolve(HERE, "..", "..");
const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8")
    .split(/\r?\n/).filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);

const TOKEN = env.QB_TIME_TOKEN;
const NEON  = process.env.NEON_URL;
const FROM  = process.argv[2] || "2026-07-07";
const TO    = process.argv[3] || "2026-07-24";
if (!TOKEN) throw new Error("no QB_TIME_TOKEN in .env");
if (!NEON)  throw new Error("set NEON_URL");

const sql = neon(NEON);
const QB = "https://rest.tsheets.com/api/v1";

// Quarter-hour rounding — the same rule Airtable's Hours field applies. Comparing
// raw seconds against rounded hours would manufacture differences.
const toHours = secs => Math.round((Number(secs) / 3600) * 4) / 4;

const jobcodes = new Map(), users = new Map();
const sheets = [];
for (let page = 1; ; page++) {
  const p = new URLSearchParams({ start_date: FROM, end_date: TO, per_page: "50", page: String(page) });
  const r = await fetch(`${QB}/timesheets?${p}`, { headers: { Authorization: `Bearer ${TOKEN}` } });
  if (!r.ok) throw new Error(`QB ${r.status}: ${(await r.text()).slice(0, 200)}`);
  const d = await r.json();
  for (const [id, j] of Object.entries(d.supplemental_data?.jobcodes || {})) jobcodes.set(String(id), j.name);
  for (const [id, u] of Object.entries(d.supplemental_data?.users || {})) users.set(String(id), `${u.first_name || ""} ${u.last_name || ""}`.trim());
  const items = Object.values(d.results?.timesheets || {});
  sheets.push(...items);
  if (d.more !== true || !items.length) break;
}
console.log(`QB: ${sheets.length} timesheets in ${FROM}..${TO}\n`);

const key = (e, d, j) => `${e}|${d}|${j}`;
const qb = new Map();
for (const ts of sheets) {
  const emp = users.get(String(ts.user_id)) || "(unknown)";
  const job = jobcodes.get(String(ts.jobcode_id)) || "(none)";
  const k = key(emp.toLowerCase(), ts.date, job.toLowerCase());
  if (!qb.has(k)) qb.set(k, { emp, date: ts.date, job, hours: 0, ids: [] });
  const b = qb.get(k);
  b.hours += toHours(ts.duration);
  b.ids.push(String(ts.id));
}

const rows = await sql.query(
  `SELECT employee_name, work_date::text AS work_date, coalesce(job_name,'(none)') AS job_name,
          round(sum(hours),2)::float8 AS hours, count(*)::int AS n
     FROM time_entries
    WHERE work_date BETWEEN $1::date AND $2::date
    GROUP BY 1,2,3`, [FROM, TO]
);
const neonMap = new Map(rows.map(r =>
  [key((r.employee_name || "").toLowerCase(), r.work_date, r.job_name.toLowerCase()), r]));

// Buckets whose jobcode maps to no Job were NEVER going to be in Airtable — Make
// drops them by design (Lunch Break, Travel, Vacation, unqualified Shop Work...).
// Reporting them as "QB ahead" is noise that buries the real signal, and counting
// unpaid breaks as owed wages would be actively wrong.
const poSet = new Set(
  (await sql.query(`SELECT lower(po_locked) AS p FROM jobs WHERE po_locked IS NOT NULL`)).map(r => r.p)
);

const diffs = [], excluded = [];
for (const [k, b] of qb) {
  const n = neonMap.get(k);
  const neonHours = n ? Number(n.hours) : 0;
  const delta = Math.round((b.hours - neonHours) * 100) / 100;
  if (Math.abs(delta) <= 0.01) continue;
  const row = { emp: b.emp, date: b.date, job: b.job, qb: b.hours, neon: neonHours, delta,
                qbSheets: b.ids.length, neonRows: n?.n || 0 };
  if (poSet.has(b.job.trim().toLowerCase())) diffs.push(row); else excluded.push(row);
}
diffs.sort((a, b) => a.date.localeCompare(b.date) || a.emp.localeCompare(b.emp));

const exclHours = Math.round(excluded.reduce((s, r) => s + r.delta, 0) * 100) / 100;
const exclJobs = [...new Set(excluded.map(r => r.job))];
console.log(`Excluded ${excluded.length} buckets / ${exclHours} h on jobcodes that map to no Job —`);
console.log(`Make never imported these and they are NOT owed wages: ${exclJobs.join(", ")}\n`);

if (!diffs.length) {
  console.log("NO DIFFERENCE — QB and Neon agree on every (employee, date, job) bucket.");
  console.log("The unmatched timesheet ids were edits or duplicates, not lost work. Nobody is owed.");
} else {
  console.log("BUCKETS WHERE QB AND NEON DISAGREE (positive delta = QB has hours Neon lacks):\n");
  console.table(diffs);
  const owed = diffs.filter(d => d.delta > 0).reduce((s, d) => s + d.delta, 0);
  const over = diffs.filter(d => d.delta < 0).reduce((s, d) => s + d.delta, 0);
  console.log(`\nQB ahead of Neon by ${Math.round(owed * 100) / 100} h  (potentially unpaid)`);
  console.log(`Neon ahead of QB by ${Math.abs(Math.round(over * 100) / 100)} h  (paid but not in QB)`);

  const byEmp = new Map();
  for (const d of diffs) byEmp.set(d.emp, Math.round(((byEmp.get(d.emp) || 0) + d.delta) * 100) / 100);
  console.log(`\nNet by employee:`);
  console.table([...byEmp.entries()].map(([employee, netHours]) => ({ employee, netHours })));
  console.log(`\nVERIFY EACH ROW WITH THE EMPLOYEE BEFORE PAYING. A QB-side surplus can also be a`);
  console.log(`forgotten clock-out or a duplicate entry, not work actually performed.`);
}

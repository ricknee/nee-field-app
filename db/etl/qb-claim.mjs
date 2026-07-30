// QB Time -> Neon: ONE-OFF CLAIM PASS (plan step 5, phase 2).
//
// WHY THIS EXISTS
// Every row currently in time_entries arrived via the Airtable ETL and is keyed by
// `airtable_id`; none carries a QB timesheet id, because Make never stored one.
// Once the puller starts upserting on `qb_timesheet_id`, any QB timesheet it sees
// that predates the cutover would MISS its existing row and insert a duplicate —
// and it will see them, because `modified_since` returns old timesheets whenever
// someone edits one.
//
// So before the puller runs even once, we walk the full QB history and stamp
// `qb_timesheet_id` onto the Neon row each timesheet already corresponds to. After
// this pass an edit to a 2025 timesheet upserts in place instead of duplicating.
//
// SAFETY RULES
//   - INSERTS NOTHING. This pass only adopts rows that already exist.
//   - Matches on (employee_name, work_date, duration_seconds, job_name) and only
//     when EXACTLY ONE unclaimed candidate exists. Ambiguous -> skipped and counted.
//   - Never re-stamps a row that already has a qb_timesheet_id.
//   - --dry-run (default) reports without writing. Pass --commit to apply.
//
// The match key is reliable because Make built the Airtable row from the same QB
// fields: employee_name is QB's `first_name last_name`, job_name is the jobcode name.
//
// Usage (from a scratch dir with the driver installed):
//   NEON_URL='postgres://...' REPO_ROOT=<repo> node qb-claim.mjs [--commit]
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "@neondatabase/serverless";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.REPO_ROOT || path.resolve(HERE, "..", "..");

const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8")
    .split(/\r?\n/)
    .filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);

const TOKEN = env.QB_TIME_TOKEN;
const NEON  = process.env.NEON_URL;
const COMMIT = process.argv.includes("--commit");
if (!TOKEN) throw new Error("no QB_TIME_TOKEN in .env");
if (!NEON)  throw new Error("set NEON_URL");

const sql = neon(NEON);
const QB = "https://rest.tsheets.com/api/v1";

// ── QB extract ────────────────────────────────────────────────────────────
// `results.timesheets` is an OBJECT KEYED BY ID, not an array. `more: true`
// means fetch the next page. supplemental_data carries jobcodes + users in the
// same response, so no extra lookups are needed.
const jobcodes = new Map();   // id -> name
const users    = new Map();   // id -> "First Last"

async function qbPage(pathname, params, page) {
  const p = new URLSearchParams({ ...params, per_page: "50", page: String(page) });
  const r = await fetch(`${QB}/${pathname}?${p}`, {
    headers: { Authorization: `Bearer ${TOKEN}` },
  });
  if (!r.ok) throw new Error(`${pathname} p${page} ${r.status}: ${(await r.text()).slice(0, 300)}`);
  const d = await r.json();

  const supp = d.supplemental_data || {};
  for (const [id, j] of Object.entries(supp.jobcodes || {})) jobcodes.set(String(id), j.name);
  for (const [id, u] of Object.entries(supp.users || {})) {
    users.set(String(id), `${u.first_name || ""} ${u.last_name || ""}`.trim());
  }

  const items = Object.values((d.results || {})[pathname] || {});
  return { items, more: d.more === true };
}

async function qbAll(pathname, params) {
  const out = [];
  for (let page = 1; ; page++) {
    const { items, more } = await qbPage(pathname, params, page);
    out.push(...items);
    if (page % 10 === 0) process.stdout.write(`  ${pathname}: ${out.length}\n`);
    if (!more || !items.length) break;
  }
  return out;
}

// Full history. Neon's earliest work_date is 2021-05-12; start a year earlier so
// nothing is missed, and end tomorrow so today's open shifts are included.
const today = new Date();
const end   = new Date(today.getTime() + 86400000).toISOString().slice(0, 10);
console.log("pulling full QB timesheet history ...");
const sheets = await qbAll("timesheets", { start_date: "2020-01-01", end_date: end });
console.log(`QB returned ${sheets.length} timesheets, ${jobcodes.size} jobcodes, ${users.size} users`);

// ── Neon candidates ───────────────────────────────────────────────────────
// Pull every unclaimed row once and match in memory. Per-row SQL would mean
// thousands of HTTP round trips against the serverless driver.
const rows = await sql.query(
  `SELECT id, employee_name, work_date::text AS work_date,
          duration_seconds::float8 AS duration_seconds, job_name
     FROM time_entries
    WHERE qb_timesheet_id IS NULL`
);
console.log(`Neon has ${rows.length} unclaimed rows`);

const keyOf = (emp, date, secs, job) =>
  `${(emp || "").trim().toLowerCase()}|${date || ""}|${Number(secs).toFixed(1)}|${(job || "").trim().toLowerCase()}`;

// key -> array of row ids. Length > 1 means the natural key is not unique for that
// combination (two identical shifts on the same day) and we must not guess.
const buckets = new Map();
for (const r of rows) {
  const k = keyOf(r.employee_name, r.work_date, r.duration_seconds, r.job_name);
  if (!buckets.has(k)) buckets.set(k, []);
  buckets.get(k).push(r.id);
}

// ── match ─────────────────────────────────────────────────────────────────
const claims = [];                 // [rowId, qbId]
const unmatched = [], ambiguous = [], noJobcode = [];
const usedRowIds = new Set();

for (const ts of sheets) {
  const qbId = String(ts.id);
  const emp  = users.get(String(ts.user_id)) || "";
  const job  = jobcodes.get(String(ts.jobcode_id)) || "";
  if (!job) noJobcode.push(qbId);

  const k = keyOf(emp, ts.date, ts.duration, job);
  const bucket = buckets.get(k);

  if (!bucket || !bucket.length) { unmatched.push({ qbId, emp, date: ts.date, job, secs: ts.duration }); continue; }
  if (bucket.length > 1)         { ambiguous.push({ qbId, emp, date: ts.date, job, n: bucket.length }); continue; }

  const rowId = bucket[0];
  // Two QB timesheets that normalise to the same key would otherwise both claim the
  // same row; the first wins and the second is reported as ambiguous.
  if (usedRowIds.has(rowId)) { ambiguous.push({ qbId, emp, date: ts.date, job, n: 1, reason: "row already claimed this run" }); continue; }
  usedRowIds.add(rowId);
  claims.push([rowId, qbId]);
}

console.log(`\nmatched:   ${claims.length}`);
console.log(`unmatched: ${unmatched.length}   (QB timesheet with no corresponding Neon row)`);
console.log(`ambiguous: ${ambiguous.length}   (more than one identical candidate — skipped)`);
if (noJobcode.length) console.log(`no jobcode name resolved: ${noJobcode.length}`);

const sample = (label, arr) => {
  if (!arr.length) return;
  console.log(`\n-- ${label} (first 10) --`);
  console.table(arr.slice(0, 10));
};
sample("UNMATCHED", unmatched);
sample("AMBIGUOUS", ambiguous);

// ── WHY didn't the unmatched match? ───────────────────────────────────────
// This is the number that decides the puller's insert policy. Make only creates an
// Airtable row when its "Seach Job Name" module finds a Job whose {Job PO - Locked}
// equals the jobcode name — no match, no row. So QB legitimately holds timesheets
// (Lunch Break, Travel, shop units) that Airtable has NEVER had. A puller that
// inserted them would add hours payroll has never counted.
const poSet = new Set(
  (await sql.query(`SELECT lower(po_locked) AS p FROM jobs WHERE po_locked IS NOT NULL`)).map(r => r.p)
);
const [{ d: neonFirst }] = await sql.query(`SELECT min(work_date)::text AS d FROM time_entries`);

const byYear = new Map();
const byJob  = new Map();
let inRangeKnownJob = 0, inRangeUnknownJob = 0, beforeNeonHistory = 0;

for (const u of unmatched) {
  const y = String(u.date).slice(0, 4);
  byYear.set(y, (byYear.get(y) || 0) + 1);
  const j = (u.job || "(none)").trim();
  byJob.set(j, (byJob.get(j) || 0) + 1);

  if (String(u.date) < neonFirst) { beforeNeonHistory++; continue; }
  if (poSet.has(j.toLowerCase())) inRangeKnownJob++; else inRangeUnknownJob++;
}

console.log(`\n== WHY UNMATCHED ==`);
console.log(`Neon history starts ${neonFirst}`);
console.log(`  predates Neon history:                 ${beforeNeonHistory}`);
console.log(`  in range, jobcode NOT a known Job:     ${inRangeUnknownJob}   <- Make drops these by design`);
console.log(`  in range, jobcode IS a known Job:      ${inRangeKnownJob}   <- REAL GAP, investigate`);

console.log(`\n-- unmatched by year --`);
console.table([...byYear.entries()].sort().map(([year, n]) => ({ year, n })));

console.log(`\n-- top 20 unmatched jobcodes --`);
console.table(
  [...byJob.entries()].sort((a, b) => b[1] - a[1]).slice(0, 20)
    .map(([jobcode, n]) => ({ jobcode, n, isKnownJob: poSet.has(jobcode.toLowerCase()) }))
);

if (!COMMIT) {
  console.log(`\nDRY RUN — nothing written. Re-run with --commit to stamp ${claims.length} rows.`);
  process.exit(0);
}

// ── write ─────────────────────────────────────────────────────────────────
console.log(`\nstamping ${claims.length} rows ...`);
let done = 0;
for (let i = 0; i < claims.length; i += 500) {
  const chunk = claims.slice(i, i + 500);
  const params = [];
  const tuples = chunk.map(([rowId, qbId]) => {
    params.push(rowId, qbId);
    return `($${params.length - 1}::uuid, $${params.length}::text)`;
  });
  await sql.query(
    `UPDATE time_entries t SET qb_timesheet_id = v.qb_id
       FROM (VALUES ${tuples.join(",")}) AS v(id, qb_id)
      WHERE t.id = v.id AND t.qb_timesheet_id IS NULL`,
    params
  );
  done += chunk.length;
  process.stdout.write(`  ${done}/${claims.length}\r`);
}

const [after] = await sql.query(
  `SELECT count(*) FILTER (WHERE qb_timesheet_id IS NOT NULL)::int AS claimed,
          count(*)::int AS total FROM time_entries`
);
console.log(`\n\ndone — ${after.claimed} of ${after.total} rows now carry a qb_timesheet_id.`);

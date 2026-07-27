// Applies sample-load.sql to a Neon branch and runs the verification checks.
//
// Usage (from a scratch dir — the repo has no root package.json by design):
//   npm i @neondatabase/serverless
//   node ../path/to/db/etl/time-entries-sample.mjs      # writes sample-load.sql
//   NEON_URL='postgres://...' node ../path/to/db/etl/load-sample.mjs
//
// ALWAYS point NEON_URL at a throwaway Neon branch, never the default branch.
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "@neondatabase/serverless";

const OUT = path.dirname(fileURLToPath(import.meta.url));
const URL_ = process.env.NEON_URL;
if (!URL_) throw new Error("set NEON_URL");
const sql = neon(URL_);

const text = fs.readFileSync(path.join(OUT, "sample-load.sql"), "utf8");
const stmts = text.split("\n").filter(l => l.trim() && !/^(BEGIN|COMMIT);$/.test(l.trim()));

let ok = 0;
for (const s of stmts) {
  try { await sql.query(s); ok++; }
  catch (e) { console.error("FAILED:", s.slice(0, 160)); throw e; }
}
console.log(`applied ${ok}/${stmts.length} statements`);

const show = async (label, q) => {
  const rows = await sql.query(q);
  console.log(`\n== ${label}`);
  console.table(rows);
};

await show("row counts", `
  SELECT (SELECT count(*) FROM employees) AS employees,
         (SELECT count(*) FROM jobs) AS jobs,
         (SELECT count(*) FROM time_entries) AS time_entries,
         (SELECT count(*) FROM time_entries WHERE job_id IS NULL) AS te_no_job_link,
         (SELECT count(*) FROM time_entries WHERE employee_id IS NULL) AS te_no_emp_link,
         (SELECT count(*) FROM time_entries WHERE job_name IS NULL) AS te_no_job_name`);

await show("hours + generated cols", `
  SELECT round(sum(hours),2) AS total_hours,
         round(sum(duration_seconds)/3600.0,2) AS naive_hours,
         min(work_date) AS first_date, max(work_date) AS last_date,
         count(DISTINCT job_name) AS distinct_job_names
  FROM time_entries`);

await show("week_start_date is Monday (dow must be 1)", `
  SELECT DISTINCT EXTRACT(ISODOW FROM week_start_date)::int AS iso_dow, count(*)
  FROM time_entries GROUP BY 1`);

await show("hoursByJob view shape (top 8)", `
  SELECT job_name, sum(hours) AS hours, count(*) AS entries,
         min(work_date) AS first_date, max(work_date) AS last_date,
         bool_and(job_id IS NULL) AS historical
  FROM time_entries GROUP BY job_name ORDER BY hours DESC LIMIT 8`);

await show("idempotency: re-run one upsert, count must not change", `
  SELECT count(*) AS before_rerun FROM time_entries`);
await sql.query(stmts[stmts.length - 1]);
await show("after re-running last upsert", `
  SELECT count(*) AS after_rerun FROM time_entries`);

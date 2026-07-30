// Local harness for the QB Time puller (plan step 5, phase 4 verification).
//
// Runs netlify/functions/qb-time-pull.js against a real QB token and a real Neon
// connection, WITHOUT writing, and then diffs what QB says against what Neon already
// holds for the same window. Make has been importing that window into Airtable all
// along, so agreement here is the evidence that the two pipes see the same hours.
//
// The important number is `wouldInsert`. For a backfill window it should be ~0:
// every timesheet in it should already exist in Neon via Make -> Airtable -> ETL.
// A large wouldInsert means the puller is about to add hours payroll never counted.
//
// Usage (from the repo root — the function resolves its driver from
// netlify/functions/node_modules):
//   NEON_URL='postgres://...' node db/etl/qb-pull-dryrun.mjs [days]
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "../../netlify/functions/node_modules/@neondatabase/serverless/index.mjs";
import { runPull } from "../../netlify/functions/qb-time-pull.js";

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
if (!TOKEN) throw new Error("no QB_TIME_TOKEN in .env");
if (!NEON)  throw new Error("set NEON_URL");

const days  = Number(process.argv[2]) || 30;
const since = new Date(Date.now() - days * 86400000);
const sinceDate = since.toISOString().slice(0, 10);
const COMMIT = process.argv.includes("--commit");

const sql = neon(NEON);

// Totals before, so a --commit run can prove it didn't move the numbers. Every row
// in this window came from the same QB timesheets Make already imported, so a
// correct puller is a no-op on hours.
const [before] = await sql.query(`SELECT count(*)::int AS rows, round(sum(hours),2)::float8 AS hours FROM time_entries`);

console.log(`${COMMIT ? "LIVE RUN" : "DRY RUN"} — QB timesheets modified in the last ${days} days (since ${sinceDate})`);
console.log(`before: ${before.rows} rows, ${before.hours} h\n`);
const report = await runPull({ sql, token: TOKEN, since, dryRun: !COMMIT });

const { unknownJobcodes, insertSamples, deletedSamples, ...counts } = report;
console.table([counts]);

if (unknownJobcodes.length) {
  console.log(`\n-- jobcodes skipped for having no matching Job (Make parity) --`);
  console.log("   " + unknownJobcodes.join("\n   "));
}

if (insertSamples.length) {
  console.log(`\n-- WOULD INSERT (new rows Neon does not have) --`);
  console.table(insertSamples);
}

if (deletedSamples.length) {
  console.log(`\n-- WOULD TOMBSTONE (deleted in QB, still in Neon) --`);
  console.table(deletedSamples);
}

// Independent cross-check: what Neon already holds for the same work-date window.
const [neonSide] = await sql.query(
  `SELECT count(*)::int AS rows, round(sum(hours),2)::float8 AS hours,
          count(*) FILTER (WHERE qb_timesheet_id IS NOT NULL)::int AS with_qb_id
     FROM time_entries WHERE work_date >= $1::date`,
  [sinceDate]
);
console.log(`\nNeon already holds, work_date >= ${sinceDate}:`);
console.table([neonSide]);

console.log(
  report.wouldInsert === 0
    ? `\nOK — nothing new to insert. The puller and Make agree on this window.`
    : `\nATTENTION — ${report.wouldInsert} timesheet(s) would be INSERTED as new rows.\n` +
      `Confirm these are genuinely new (logged since the last Make run) and not hours\n` +
      `Make deliberately never imported, before deploying.`
);

if (COMMIT) {
  const [after] = await sql.query(`SELECT count(*)::int AS rows, round(sum(hours),2)::float8 AS hours FROM time_entries`);
  const dRows = after.rows - before.rows, dHours = Math.round((after.hours - before.hours) * 100) / 100;
  console.log(`\nafter:  ${after.rows} rows, ${after.hours} h   (delta: ${dRows >= 0 ? "+" : ""}${dRows} rows, ${dHours >= 0 ? "+" : ""}${dHours} h)`);
  console.log(dRows === 0 && dHours === 0
    ? `TOTALS UNCHANGED — the puller re-derived the same hours Make did.`
    : `TOTALS MOVED — expected only if wouldInsert/deleted were non-zero. Verify before trusting.`);
}

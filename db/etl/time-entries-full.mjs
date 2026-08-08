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
// Resolved by RELATIVE PATH, not as a bare specifier. The repo has no root
// package.json, so a bare import would look in db/etl -> db -> repo root and find
// nothing; running it used to require copying this file into a scratch directory
// that happened to have the driver. The functions directory already declares it.
import { neon } from "../../netlify/functions/node_modules/@neondatabase/serverless/index.mjs";
// Shared with the hourly puller so the job-link matching rule has ONE definition.
// The module imports nothing itself — it takes an already-connected `sql`.
// JOB_FIELDS comes from there too: both this script and the hourly sync write the
// `jobs` table, and a second copy of the map here would let them drift apart.
import { backfillJobLinks, JOB_FIELDS } from "../../netlify/functions/_jobs-sync.js";

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
// NEON_URL comes from .env like every other credential, so the daily command is
// just `node db/etl/time-entries-full.mjs` with nothing to paste. An explicit
// environment variable still wins, for one-off runs against a Neon branch.
const NEON = process.env.NEON_URL || env.NEON_URL || env.DATABASE_URL;
if (!KEY)  throw new Error("no Airtable PAT in .env (AIRTABLE_PROD_READ_PAT)");
if (!NEON) throw new Error(
  "No Neon connection string.\n" +
  "  Add a line to .env:   NEON_URL=postgresql://...\n" +
  "  Get it from the Neon console (project damp-silence-99074350) -> Connect -> POOLED connection string.");

const sql = neon(NEON);
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── MODE — check-only by DEFAULT since 2026-07-30 ────────────────────────────
// This script used to be the only writer of time_entries. It is not any more: the
// QB Time puller (netlify/functions/qb-time-pull.js) upserts the same timesheets
// keyed by qb_timesheet_id, while this ETL keys by airtable_id. Running both as
// loaders means every QB timesheet lands TWICE and payroll hours double. Airtable
// stores no QB id, so this script cannot dedupe against puller rows.
//
// So the fact load is now opt-in (--load) and the default job is reconciliation:
// read both sides, diff them, say nothing else. Dimension loads (employees, jobs)
// stay live in both modes — single writer, keyed by airtable_id, cannot double.
const LOAD = process.argv.includes("--load");

// --repair: UPDATE existing Neon rows from Airtable where the two disagree.
// NEVER inserts, never deletes — it can only correct a row that already exists on
// both sides, so it cannot duplicate hours no matter how often it runs.
//
// ⛔ DISABLED AT MIGRATION STEP 2 (2026-08-05). IT NOW POINTS THE WRONG WAY.
//
// It was written when Airtable was authoritative and the app mirrored into Neon
// fail-soft: a failed mirror left Airtable right and Neon stale, and this fixed it.
// Step 2 inverted that. The four write paths now write NEON first and mirror to
// Airtable, so a failed mirror leaves NEON right and AIRTABLE stale — and running
// this would overwrite the correct value with the stale one, on payroll data, from
// what used to be a routine daily command. Exactly the direction that must not run.
//
// Repairing AIRTABLE from NEON is the correct direction now, but it is not a flag
// flip: this script holds AIRTABLE_PROD_READ_PAT, which is read-only by design
// (verified read 200 / write 403), so it cannot write Airtable at all. That needs a
// write credential and a deliberate decision about blast radius.
//
// Drift DETECTION is untouched and still runs on every pass — you keep the signal,
// you just no longer have a one-flag correction pointed at the wrong system.
const REPAIR = process.argv.includes("--repair");
if (REPAIR) {
  console.error(
    "\n--repair is DISABLED as of migration Step 2.\n" +
    "It updates Neon FROM Airtable, but Neon is now the source of truth for time\n" +
    "entries — running it would overwrite correct payroll data with stale mirror\n" +
    "values. Drift is still detected and reported; re-run without the flag.\n");
  process.exit(2);
}

// Make imports into Airtable once nightly at 21:00; the puller runs hourly. So
// between those, Neon legitimately holds hours Airtable has not seen yet and a
// naive diff would scream every day. Compare only work dates Make has had a chance
// to import — yesterday and earlier by default.
const asofArg = process.argv.find(a => a.startsWith("--asof="));
const ASOF = asofArg
  ? asofArg.slice("--asof=".length)
  : new Date(Date.now() - 86400000).toISOString().slice(0, 10);

console.log(LOAD
  ? `MODE: --load (writing time_entries). Only valid while the QB puller is NOT live.`
  : `MODE: check-only (default). Comparing work_date <= ${ASOF}.${REPAIR ? " --repair ON (update-only)." : ""}`);

// ── extract ───────────────────────────────────────────────────────────────
// Airtable REST returns records keyed by FIELD NAME. Rate limit is 5 req/sec
// per base; requests are sequential with a floor to stay well under it.
async function fetchAll(table, params = {}) {
  const out = [];
  let offset, pages = 0;
  do {
    // Airtable expects REPEATED `fields[]` keys for a multi-field projection, which an
    // object spread cannot express — array values are appended one at a time.
    const p = new URLSearchParams({ pageSize: "100" });
    for (const [k, v] of Object.entries(params)) {
      if (Array.isArray(v)) v.forEach(x => p.append(k, x));
      else p.set(k, v);
    }
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
// Field coercers for the tables THIS script still maps itself (allocations, rates,
// weekly time, GP sources). The jobs coercers moved to _jobs-sync.js along with
// JOB_FIELDS; `bool` went with them and is no longer referenced here.
const nul  = v => (v === undefined || v === "" ? null : v);
const num  = v => (v === undefined || v === "" || v === null ? null : Number(v));
// Airtable lookups come back as ARRAYS even when they resolve to one value.
// Verified 2026-07-31: no job resolves to more than one billable rate, so taking
// the first element is faithful — but it is a first, not a sum, deliberately.
const firstNum = v => (Array.isArray(v) ? num(v[0]) : num(v));
const firstId  = v => (Array.isArray(v) && v.length ? v[0] : null);

// Jobs MASTER DATA. The field map is NOT defined here any more — it is imported
// from netlify/functions/_jobs-sync.js, which is the single source of truth.
//
// It used to be a second copy maintained in parallel with the hourly sync. Two
// writers against one table, and if the lists disagreed the hourly sync would
// silently revert whatever this script last wrote. Keeping them identical was a
// comment; now it is structural.
//
// Excluded on purpose (unchanged): the ~40 financial rollups and the five
// "All … Reviewed?" gates. They roll up from estimates / invoices / expenses /
// allocations, so a copied value would go stale between runs — they are computed
// live from v_job_rollups / v_job_financials instead.

const jobs = await fetchAll("Jobs", { "fields[]": JOB_FIELDS.map(([, at]) => at) });
const entries   = await fetchAll("Time Entries");
// Slice 5 phase A — the labor REVENUE chain. The cost chain (Labor Cost Rates,
// Job Labor Allocation) is phase B and is deliberately not loaded yet.
const rates     = await fetchAll("Labor Billable Rates");
const billAllocs = await fetchAll("Labor Billing Allocations");
// Phase B — the labor COST chain.
// "Labor Cost Rates" is NO LONGER fetched: Neon owns cost rates as of
// 2026-08-08 (the People screen writes them), so pulling the Airtable copy
// would only tempt someone into re-enabling the upsert further down. See the
// long note there before changing this.
const weeklyTime = await fetchAll("Employee Weekly Time");
const jobAllocs  = await fetchAll("Job Labor Allocation");
// Phase C — the remaining sources every Jobs rollup reads from. Wire and Pipe are
// LEGACY (tracked in the inventory app now, pushed across as expenses) so they are
// copied as frozen history: they still feed Actual Job Cost (COGS) on old jobs but
// will not move again.
const estimates  = await fetchAll("Job Estimates");
const expenses   = await fetchAll("Expenses");
const invoices   = await fetchAll("Invoices");
const wireWeighs = await fetchAll("Wire Weigh-Ins");
const pipeUsage  = await fetchAll("Pipe Usage");
// The three child tables that were BLOCKING the estimate and invoice slices —
// see db/schema/014. Material Billing Allocations in particular is what makes
// invoice revenue computable at all, and is the same rollup that 4d had to
// leave ETL-copied on expenses.billed_material_amount.
const sentEstPdfs = await fetchAll("Sent Estimate PDFs");
const estTemplates = await fetchAll("Estimate Templates");
const matAllocs   = await fetchAll("Material Billing Allocations");
console.log(`extracted: ${employees.length} employees, ${jobs.length} jobs, ${entries.length} time entries`);

// ── Airtable-side truth, computed BEFORE loading ──────────────────────────
// These are the acceptance numbers. Airtable's own `Hours` field is the
// authority (it applies the quarter-hour rounding rule); Neon must reproduce
// it from duration_seconds via the generated column.
// In check-only mode both sides are restricted to work_date <= ASOF so the nightly
// Make cadence vs the hourly puller doesn't register as drift. In --load mode the
// whole table is compared, as it always was.
const inScope = r => LOAD || !r.fields["Work Date"] || r.fields["Work Date"] <= ASOF;
const scoped  = entries.filter(inScope);
if (!LOAD) console.log(`  comparing ${scoped.length} of ${entries.length} Airtable rows (work_date <= ${ASOF})`);

const src = { entries: scoped.length, hours: 0, blankName: 0, noLink: 0, first: "", last: "", buckets: new Map() };
for (const r of scoped) {
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

// NOTE: the employees/jobs loads below are DIMENSION loads and stay live even when the
// fact load is disabled (see --check-only). They are keyed by airtable_id, have exactly
// one writer, and cannot double-count — unlike time_entries, which after the QB puller
// goes live has two independent writers and must never be ETL-loaded again.
console.log("loading dimensions ...");
await upsertBatch("employees", ["airtable_id", "name", "username", "role", "active", "qb_user_id"],
  employees.map(e => [e.id, e.fields["Employee Name"] || "(unnamed)", nul(e.fields["Username"]),
                      nul(e.fields["Role"]), e.fields["Active"] === true,
                      // QB Time user id — the puller's employee lookup key.
                      nul(e.fields["Employee ID"])]), "airtable_id");

// Billable rates BEFORE jobs — jobs carry an FK to them.
await upsertBatch("labor_billable_rates",
  ["airtable_id", "labor_type", "billable_hourly_rate", "effective_start_date", "effective_end_date", "notes", "synced_at"],
  rates.map(r => [r.id, nul(r.fields["Labor Type"]), num(r.fields["Billable Hourly Rate"]),
                  nul(r.fields["Effective Start Date"]), nul(r.fields["Effective End Date"]),
                  nul(r.fields["Notes"]), new Date().toISOString()]), "airtable_id");

const rateMap = new Map((await sql.query(`SELECT id, airtable_id FROM labor_billable_rates`))
  .map(r => [r.airtable_id, r.id]));

// Jobs master data. Airtable stays the source of truth for Jobs — nothing writes
// back — so `synced_at` is the only thing that makes a stale row visible.
await upsertBatch("jobs",
  ["airtable_id", ...JOB_FIELDS.map(([col]) => col), "billable_rate_id", "synced_at"],
  jobs.map(j => [j.id,
                 ...JOB_FIELDS.map(([, at, coerce]) => coerce(j.fields[at])),
                 rateMap.get(firstId(j.fields["Labor Billable Rates"])) ?? null,
                 new Date().toISOString()]),
  "airtable_id");

// Labor billing allocations. Loads in BOTH modes like the other dimensions —
// Airtable is the only writer, they are keyed by airtable_id, and they carry no
// hours of their own that could double-count. `time_entry_id` is resolved against
// whatever time_entries already holds; an allocation whose entry has not landed yet
// simply keeps a NULL FK and picks it up on a later run.
{
  const teMap = new Map((await sql.query(`SELECT id, airtable_id FROM time_entries WHERE airtable_id IS NOT NULL`))
    .map(r => [r.airtable_id, r.id]));

  await upsertBatch("labor_billing_allocations",
    ["airtable_id", "time_entry_id", "time_entry_airtable_id", "invoice_airtable_id",
     "allocated_hours", "bill_rate", "billing_stage", "synced_at"],
    billAllocs.map(a => {
      const teAt = firstId(a.fields["Time Entry"]);
      return [a.id, teMap.get(teAt) ?? null, teAt, firstId(a.fields["Invoice"]),
              num(a.fields["Allocated Hours"]), firstNum(a.fields["Bill Rate"]),
              nul(a.fields["Billing Stage"]), new Date().toISOString()];
    }), "airtable_id", 200);

  const [ac] = await sql.query(
    `SELECT count(*)::int AS total, count(time_entry_id)::int AS linked,
            count(invoice_airtable_id)::int AS invoiced FROM labor_billing_allocations`);
  console.log(`billing allocations: ${ac.total} rows, ${ac.linked} linked to a time entry, ${ac.invoiced} on an invoice`);
}

// Phase B — the labor COST chain. Same posture as the rest: Airtable is the only
// writer, keyed by airtable_id, loads in both modes.
//
// employee_weekly_time is ported deliberately. Nothing in the REPO reads it, but
// Airtable does — its Weekly Hours drives Job Labor Allocation's overtime split,
// which drives labor cost, which drives Gross Profit. It is maintained by MAKE
// (scenario 4546051), so without a copy here, retiring Make would silently degrade
// GP. Porting it is what makes Make retirable.
{
  const empMapAt = new Map((await sql.query(`SELECT id, airtable_id FROM employees`)).map(r => [r.airtable_id, r.id]));
  const jobMapAt = new Map((await sql.query(`SELECT id, airtable_id FROM jobs`)).map(r => [r.airtable_id, r.id]));
  const now = new Date().toISOString();

  // ⚠⚠ labor_cost_rates IS NO LONGER LOADED HERE — 2026-08-08. DO NOT RESTORE.
  //
  // The app now writes cost rates directly to Neon (handleAddEmployeeRaise /
  // handleCorrectEmployeeRate / handleCreateEmployee, via the People screen), so
  // NEON IS THE SOURCE OF TRUTH for this table and Airtable's copy is historical.
  //
  // Re-enabling this upsert would overwrite every app-written rate with the stale
  // Airtable row on the next run. That is not a cosmetic revert: true_cost_rate
  // drives v_job_labor_cost_true, which drives GP on every job the employee has
  // ever booked hours to. A raise entered in the app would silently disappear and
  // every job's labor cost would jump back to the old number.
  //
  // It is also why app-created rate rows carry a synthetic `app:<rec>:<date>`
  // airtable_id: they have no Airtable counterpart to be keyed against.
  //
  // Same shape as the decision already taken for expenses, estimates and
  // invoices — Airtable keeps identity, Neon answers the question.
  //
  // (Historical rows loaded before this date are already in Neon and unaffected.)

  await upsertBatch("employee_weekly_time",
    ["airtable_id", "employee_id", "employee_airtable_id", "week_start_date", "weekly_hours", "employee_week_key", "synced_at"],
    weeklyTime.map(w => {
      const e = firstId(w.fields["Employee"]);
      return [w.id, empMapAt.get(e) ?? null, e, nul(w.fields["Week Start Date"]),
              num(w.fields["Weekly Hours"]), nul(w.fields["Employee Week Key"]), now];
    }), "airtable_id", 200);

  await upsertBatch("job_labor_allocations",
    ["airtable_id", "job_id", "job_airtable_id", "employee_id", "employee_airtable_id",
     "week_start_date", "reviewed", "allocated_hours", "weekly_total_hours", "notes", "synced_at"],
    jobAllocs.map(a => {
      const j = firstId(a.fields["Job"]), e = firstId(a.fields["Employee"]);
      return [a.id, jobMapAt.get(j) ?? null, j, empMapAt.get(e) ?? null, e,
              nul(a.fields["Week Start Date"]), a.fields["Reviewed"] === true,
              num(a.fields["Allocated Hours"]),
              // Weekly Total Hours is a LOOKUP through Employee Weekly Time, so it
              // arrives as an array. Snapshotting the value keeps the cost views
              // computable even if that link is ever broken.
              firstNum(a.fields["Weekly Total Hours"]),
              nul(a.fields["Notes"]), now];
    }), "airtable_id", 200);

  const [cc] = await sql.query(
    `SELECT (SELECT count(*) FROM labor_cost_rates)::int AS rates,
            (SELECT count(*) FROM employee_weekly_time)::int AS weeks,
            (SELECT count(*) FROM job_labor_allocations)::int AS allocs,
            (SELECT count(*) FROM job_labor_allocations WHERE job_id IS NOT NULL)::int AS allocs_linked`);
  console.log(`cost chain: ${cc.rates} rates, ${cc.weeks} employee-weeks, ${cc.allocs} allocations (${cc.allocs_linked} linked to a job)`);
}

// Phase C — the remaining GP source tables. Formula fields are stored as their
// COMPUTED VALUE, not re-derived: Airtable is still the calculator, and these are
// inputs to the Jobs rollups rather than things Neon decides. Re-running the ETL
// refreshes them.
{
  const jobMapAt = new Map((await sql.query(`SELECT id, airtable_id FROM jobs`)).map(r => [r.airtable_id, r.id]));
  const now = new Date().toISOString();
  const J = rec => { const a = firstId(rec.fields["Job"]); return [jobMapAt.get(a) ?? null, a]; };

  await upsertBatch("job_estimates",
    ["airtable_id", "job_id", "job_airtable_id", "estimate_type", "status", "actual_estimate_sent",
     "estimated_labor_hours", "estimated_labor_cost", "estimated_material_cost",
     "calculated_estimated_total", "estimate_date", "notes",
     // ⚠ display_number and estimate_snapshot are carried but are ALWAYS NULL,
     // verified directly against Airtable: 0 of 83 estimates populate either.
     // Both live on Sent Estimate PDFs instead — Save Estimate writes there and
     // never back to the master record, exactly as handleJobEstimates' own
     // comment says. Kept in the load only so the columns cannot silently
     // diverge if Airtable ever starts populating them.
     //
     // Consequence worth knowing: handleJobEstimates' `onlySaved` filter tests
     // displayNumber != null, so it can only ever return an EMPTY list. It has
     // no caller in the frontend either. Dead parameter, dead filter.
     "display_number", "estimate_snapshot", "synced_at"],
    estimates.map(r => [r.id, ...J(r), nul(r.fields["Estimate Type"]), nul(r.fields["Status"]),
      num(r.fields["Actual Estimate Sent"]), num(r.fields["Estimated Labor Hours"]),
      num(r.fields["Estimated Labor Cost"]), num(r.fields["Estimated Material Cost"]),
      num(r.fields["Calculated Estimated Total"]), nul(r.fields["Estimate Date"]),
      nul(r.fields["Notes"]), num(r.fields["Estimate Display #"]),
      nul(r.fields["Estimate Snapshot"]), now]), "airtable_id", 200);

  // ⚠ submitted_by_at_id IS AN AUTHORIZATION FIELD, not decoration.
  // handleExpenses scopes by it: admin/office see every expense on a job, an
  // EMPLOYEE sees only their own. This ETL was written for GP aggregation, where
  // who submitted a row is irrelevant, so it never carried the field — and a
  // Neon-first read without it would either break that scope or leak every
  // employee's expenses to every employee. Added at Step 4d (2026-08-07).
  // Stored as the Airtable EMPLOYEE REC ID because that is what authUser.id is.
  await upsertBatch("expenses",
    ["airtable_id", "job_id", "job_airtable_id", "expense_type", "expense_status", "expense_date",
     "total_cost_actual", "reviewed", "reviewed_expenses", "billable", "billable_material_amount",
     "billed_material_amount", "unbilled_material_amount", "manual_material_cost", "material_credit",
     "vendor_name", "description", "push_id", "submitted_by_at_id", "submitted_by_name", "synced_at"],
    expenses.map(r => [r.id, ...J(r), nul(r.fields["Expense Type"]), nul(r.fields["Expense Status"]),
      nul(r.fields["Expense Date"]), num(r.fields["Total Cost (Actual)"]),
      r.fields["Reviewed"] === true, num(r.fields["Reviewed Expenses"]),
      r.fields["Billable?"] === true, num(r.fields["Billable Material Amount $"]),
      num(r.fields["Billed Material Amount $"]), num(r.fields["Unbilled Material Amount $"]),
      num(r.fields["Manual Material Cost"]), num(r.fields["Material Credit"]),
      // Lookup -> array; the vendor NAME is the durable snapshot, same rule as job_name.
      (Array.isArray(r.fields["Vendor Name (from Vendor)"]) ? r.fields["Vendor Name (from Vendor)"][0] : null) ?? null,
      nul(r.fields["Description"]), nul(r.fields["Push ID"]),
      // Submitted By is a LINK (array of employee rec ids); Submitted By Name is
      // its lookup. Blank on legacy rows entered before the field existed —
      // those are pre-self-service and belong to no employee, which is correct:
      // an employee should not see them, and a manager sees everything anyway.
      (Array.isArray(r.fields["Submitted By"]) ? r.fields["Submitted By"][0] : null) ?? null,
      (Array.isArray(r.fields["Submitted By Name"])
        ? r.fields["Submitted By Name"].filter(Boolean).join(", ")
        : (r.fields["Submitted By Name"] || null)) || null,
      now]), "airtable_id", 200);

  await upsertBatch("invoices",
    ["airtable_id", "job_id", "job_airtable_id", "invoice_number", "invoice_status", "invoice_type",
     "billing_mode", "invoice_stage", "invoice_date", "snapshot_total", "invoice_total",
     // invoice_notes and invoice_snapshot are returned by handleGetJobInvoices;
     // without them a Neon-first read drops the notes and the saved snapshot.
     "manual_labor", "manual_material", "percent_to_bill", "auto_allocate", "invoice_display_no",
     "invoice_notes", "invoice_snapshot", "synced_at"],
    invoices.map(r => [r.id, ...J(r), nul(r.fields["Invoice Number"]), nul(r.fields["Invoice Status"]),
      nul(r.fields["Invoice Type"]), nul(r.fields["Billing Mode"]), nul(r.fields["Invoice Stage"]),
      nul(r.fields["Invoice Date"]), num(r.fields["Snapshot Total"]), num(r.fields["Invoice Total"]),
      num(r.fields["Manual Labor $"]), num(r.fields["Manual Material $"]),
      num(r.fields["Percent to Bill"]), r.fields["Auto Allocate?"] === true,
      num(r.fields["Invoice Display #"]),
      nul(r.fields["Invoice Notes"]), nul(r.fields["Invoice Snapshot"]), now]), "airtable_id", 200);

  // ── The three child tables (schema 014) ──────────────────────────────────
  // Loaded AFTER invoices and expenses, because material_billing_allocations
  // carries real FKs to both and resolving them needs the parents present.
  const link1 = (v) => (Array.isArray(v) ? (v[0] ?? null) : (v ?? null));

  await upsertBatch("sent_estimate_pdfs",
    // ⚠ J(r) yields [job_id, job_airtable_id] IN THAT ORDER — same as the
    // invoices upsert above. Listing them the other way round sends a `rec…`
    // string into the uuid column and the load dies on the first row.
    ["airtable_id", "job_id", "job_airtable_id", "estimate_airtable_id", "estimate_id",
     "display_number", "estimate_date", "total", "snapshot", "synced_at"],
    sentEstPdfs.map(r => [r.id, ...J(r),
      link1(r.fields["Job Estimate"]),
      null, // estimate_id resolved below — the FK needs job_estimates loaded
      num(r.fields["Estimate Display #"]), nul(r.fields["Estimate Date"]),
      num(r.fields["Total"]), nul(r.fields["Snapshot"]), now]), "airtable_id", 200);

  // handleEstimateTemplates filters by contractor NAME, but the Airtable link
  // only yields rec ids and Companies is not migrated. Resolving the name once
  // here is far cheaper than migrating a whole table for one filter — and it is
  // the same "store the name, not the link" rule already used for job_name and
  // vendor_name.
  const companies = await fetchAll("Companies");
  const companyName = new Map(companies.map(c =>
    [c.id, c.fields?.["Company Name"] || c.fields?.["Name"] || null]));

  await upsertBatch("estimate_templates",
    ["airtable_id", "template_name", "contractor_airtable_id", "contractor_name", "active",
     "scope_of_work", "exclusions", "standard_terms", "base_price", "default_labor_hours",
     "default_material_cost", "internal_notes", "synced_at"],
    estTemplates.map(r => [r.id, r.fields["Template Name"] || "(unnamed)",
      link1(r.fields["Contractor"]),
      companyName.get(link1(r.fields["Contractor"])) ?? null,
      r.fields["Active"] === true,
      nul(r.fields["Scope of Work"]), nul(r.fields["Exclusions"]),
      nul(r.fields["Standard Terms"]), num(r.fields["Base Price"]),
      num(r.fields["Default Labor Hours"]), num(r.fields["Default Material Cost"]),
      nul(r.fields["Internal Notes"]), now]), "airtable_id", 200);

  // Only three fields here are real; the other seven on the Airtable table are
  // lookups and formulas derived from these.
  await upsertBatch("material_billing_allocations",
    ["airtable_id", "expense_airtable_id", "invoice_airtable_id", "allocated_amount", "synced_at"],
    matAllocs.map(r => [r.id, link1(r.fields["Expense"]), link1(r.fields["Invoice"]),
      num(r.fields["Allocated Material Amount $"]), now]), "airtable_id", 200);

  // Resolve the uuid FKs now that every parent row exists. Kept as a separate
  // pass rather than a subquery per row: 252 + 25 correlated subqueries would
  // be 277 extra round trips for what is two statements.
  await sql`UPDATE sent_estimate_pdfs s SET estimate_id = e.id
              FROM job_estimates e WHERE e.airtable_id = s.estimate_airtable_id
               AND s.estimate_id IS DISTINCT FROM e.id`;
  await sql`UPDATE material_billing_allocations m SET expense_id = e.id
              FROM expenses e WHERE e.airtable_id = m.expense_airtable_id
               AND m.expense_id IS DISTINCT FROM e.id`;
  await sql`UPDATE material_billing_allocations m SET invoice_id = i.id
              FROM invoices i WHERE i.airtable_id = m.invoice_airtable_id
               AND m.invoice_id IS DISTINCT FROM i.id`;
  console.log(`children: ${sentEstPdfs.length} sent-estimate PDFs, ${estTemplates.length} templates, ${matAllocs.length} material allocations`);

  await upsertBatch("wire_weigh_ins",
    ["airtable_id", "job_id", "job_airtable_id", "weigh_in_date", "net_weight", "footage_used",
     "total_wire_cost", "reviewed", "reviewed_wire_cost", "wire_status", "synced_at"],
    wireWeighs.map(r => [r.id, ...J(r), nul(r.fields["Weigh-In Date"]), num(r.fields["Net Weight"]),
      num(r.fields["Footage Used"]), num(r.fields["Total Wire Cost"]), r.fields["Reviewed"] === true,
      num(r.fields["Reviewed Wire Cost"]), nul(r.fields["Wire Status"]), now]), "airtable_id", 200);

  await upsertBatch("pipe_usage",
    ["airtable_id", "job_id", "job_airtable_id", "usage_date", "feet_used", "total_feet",
     "total_pipe_cost", "reviewed", "pipe_cost_reviewed", "pipe_cost_in_progress", "synced_at"],
    pipeUsage.map(r => [r.id, ...J(r), nul(r.fields["Date"]), num(r.fields["Feet Used"]),
      num(r.fields["Total Feet"]), num(r.fields["Total Pipe Cost"]), r.fields["Reviewed"] === true,
      num(r.fields["Pipe Cost – Reviewed"]), num(r.fields["Pipe Cost – In Progress"]), now]), "airtable_id", 200);

  const [pc] = await sql.query(
    `SELECT (SELECT count(*) FROM job_estimates)::int  AS est,
            (SELECT count(*) FROM expenses)::int       AS exp,
            (SELECT count(*) FROM invoices)::int       AS inv,
            (SELECT count(*) FROM wire_weigh_ins)::int AS wire,
            (SELECT count(*) FROM pipe_usage)::int     AS pipe`);
  console.log(`GP sources: ${pc.est} estimates, ${pc.exp} expenses, ${pc.inv} invoices, ${pc.wire} wire weigh-ins, ${pc.pipe} pipe usage`);
}

// ── link puller rows to their Airtable twins ──────────────────────────────
// Runs in BOTH modes, like the dimension loads — it writes only the linkage
// column, never hours.
//
// The QB puller creates a Neon row keyed by qb_timesheet_id within the hour;
// Make creates the matching Airtable record at 21:00. So for up to a day a
// timesheet exists in both systems with neither knowing the other's id. That
// matters because the payroll UI edits by AIRTABLE record id — an unlinked row
// can be read but not edited.
//
// This pass closes that gap using the same one-candidate-only rule as the claim
// pass: match on (employee, work_date, duration, job_name), and only when the
// key is unambiguous in both directions. Ambiguity is reported, never guessed.
{
  const unlinked = await sql.query(
    `SELECT id, employee_name, work_date::text AS work_date,
            duration_seconds::float8 AS duration_seconds, job_name
       FROM time_entries WHERE airtable_id IS NULL`
  );

  if (!unlinked.length) {
    console.log("link: no unlinked rows");
  } else {
    // Airtable ids already claimed by a Neon row must never be re-assigned.
    const taken = new Set(
      (await sql.query(`SELECT airtable_id FROM time_entries WHERE airtable_id IS NOT NULL`))
        .map(r => r.airtable_id)
    );

    const key = (emp, date, secs, job) =>
      `${(emp || "").trim().toLowerCase()}|${date || ""}|${Number(secs).toFixed(1)}|${(job || "").trim().toLowerCase()}`;

    const byKey = new Map();
    for (const r of entries) {
      if (taken.has(r.id)) continue;
      const f = r.fields;
      const k = key(f["Employee"], f["Work Date"], Number(f["Duration (Seconds)"] ?? 0), f["Job Name (Text)"]);
      if (!byKey.has(k)) byKey.set(k, []);
      byKey.get(k).push(r.id);
    }

    const pairs = [];
    let ambiguous = 0, unmatched = 0;
    const usedAt = new Set();
    for (const row of unlinked) {
      const bucket = byKey.get(key(row.employee_name, row.work_date, row.duration_seconds, row.job_name));
      if (!bucket || !bucket.length) { unmatched++; continue; }
      if (bucket.length > 1)         { ambiguous++; continue; }
      if (usedAt.has(bucket[0]))     { ambiguous++; continue; }
      usedAt.add(bucket[0]);
      pairs.push([row.id, bucket[0]]);
    }

    console.log(`link: ${unlinked.length} unlinked -> ${pairs.length} matched, ${unmatched} not yet in Airtable, ${ambiguous} ambiguous`);

    for (let i = 0; i < pairs.length; i += 200) {
      const chunk = pairs.slice(i, i + 200);
      const params = [];
      const tuples = chunk.map(([nid, aid]) => {
        params.push(nid, aid);
        return `($${params.length - 1}::uuid, $${params.length}::text)`;
      });
      await sql.query(
        `UPDATE time_entries t SET airtable_id = v.aid
           FROM (VALUES ${tuples.join(",")}) AS v(id, aid)
          WHERE t.id = v.id AND t.airtable_id IS NULL`,
        params
      );
    }
  }
}

// ── backfill job_id on rows whose job arrived later ───────────────────────
// Runs in BOTH modes and needs no flag: like the linker above it writes only a
// linkage column, never hours, and it is UPDATE-only on rows that already exist.
//
// The linker directly above closes the airtable_id gap. This closes the matching
// job_id gap, which nothing owned until 2026-08-05 — see netlify/functions/
// _jobs-sync.js for the full account. Imported rather than reimplemented so the
// hourly puller and this script can never drift apart on the matching rule.
{
  const r = await backfillJobLinks(sql);
  if (r.ok && !r.linked) console.log("job links: none to backfill");
  else if (!r.ok)        console.log(`job links: backfill FAILED — ${r.error}`);
}

// ── repair drifted rows (--repair) ────────────────────────────────────────
// UPDATE-only, matched on airtable_id, bounded to work_date <= ASOF. See the flag
// definition at the top for why each of those constraints matters.
// Drift is DETECTED on every run and only CORRECTED with --repair. Keeping those
// separate matters: the acceptance checks below compare aggregates, so a row can be
// wrong on class / labor_type / employee_name and still pass all 8. (Proven the hard
// way on 2026-07-31 — a bad UPDATE nulled two fields on a row and every check stayed
// green.) And if repair ran automatically, a permanently broken write mirror would be
// silently papered over every night instead of being noticed.
let driftCount = 0;
if (!LOAD) {
  const norm = v => (v === undefined || v === null || v === "" ? null : String(v));
  const FIELDS = [
    ["employee_name",    f => norm(f["Employee"])],
    ["city_taxes",       f => norm(f["City Taxes"])],
    ["class",            f => norm(f["Class"])],
    ["labor_type",       f => norm(f["Labor Type"])],
    ["job_name",         f => norm(f["Job Name (Text)"])],
  ];

  const neonRows = await sql.query(
    `SELECT airtable_id, employee_name, city_taxes, class, labor_type, job_name,
            duration_seconds::float8 AS duration_seconds, labor_reviewed
       FROM time_entries
      WHERE airtable_id IS NOT NULL AND (work_date IS NULL OR work_date <= $1::date)`,
    [ASOF]
  );
  const neonById = new Map(neonRows.map(r => [r.airtable_id, r]));

  const fixes = [];
  const reasons = new Map();
  for (const r of scoped) {
    const n = neonById.get(r.id);
    if (!n) continue;                       // not in Neon — an INSERT, which repair never does
    const f = r.fields;

    const why = [];
    for (const [col, get] of FIELDS) if (norm(n[col]) !== get(f)) why.push(col);
    // Hours are what payroll pays, so duration gets its own tolerance check.
    const atSecs = Number(f["Duration (Seconds)"] ?? 0);
    if (Math.abs(Number(n.duration_seconds) - atSecs) > 0.05) why.push("duration_seconds");
    if ((n.labor_reviewed === true) !== (f["Labor Reviewed"] === true)) why.push("labor_reviewed");
    if (!why.length) continue;

    for (const w of why) reasons.set(w, (reasons.get(w) || 0) + 1);
    fixes.push([r.id, norm(f["Employee"]), atSecs, norm(f["City Taxes"]), norm(f["Class"]),
                norm(f["Labor Type"]), norm(f["Job Name (Text)"]), f["Labor Reviewed"] === true]);
  }

  driftCount = fixes.length;

  if (!fixes.length) {
    console.log("drift: none — Airtable and Neon agree field-by-field on every row in scope");
  } else if (!REPAIR) {
    // ⛔ DO NOT restore the old "re-run with --repair" advice that used to print
    // here. Since Step 2, NEON IS AUTHORITATIVE — drift means the AIRTABLE mirror
    // is stale, and --repair updates Neon *from* Airtable, i.e. it would overwrite
    // correct payroll data with the stale value. It is disabled at the flag guard;
    // this message was still recommending it, which is how a soak-time operator
    // ends up running the one command that breaks the thing being soaked.
    console.log(`drift: ${fixes.length} row(s) differ —`,
      [...reasons.entries()].map(([k, v]) => `${k}:${v}`).join(" "),
      `\n       NEON IS AUTHORITATIVE — this means the Airtable mirror is stale, not Neon.`,
      `\n       Do NOT run --repair (disabled; it points Airtable -> Neon). Investigate the`,
      `\n       mirror write for these rows instead.`);
  } else {
    console.log(`repair: updating ${fixes.length} drifted row(s) —`,
      [...reasons.entries()].map(([k, v]) => `${k}:${v}`).join(" "));
    for (let i = 0; i < fixes.length; i += 200) {
      const chunk = fixes.slice(i, i + 200);
      const params = [];
      const tuples = chunk.map(row => {
        const ph = row.map(v => { params.push(v); return `$${params.length}`; });
        return `(${ph[0]}::text, ${ph[1]}::text, ${ph[2]}::numeric, ${ph[3]}::text, ${ph[4]}::text, ${ph[5]}::text, ${ph[6]}::text, ${ph[7]}::boolean)`;
      });
      await sql.query(
        `UPDATE time_entries t SET
           employee_name = v.employee_name, duration_seconds = v.duration_seconds,
           city_taxes = v.city_taxes, class = v.class, labor_type = v.labor_type,
           job_name = v.job_name, labor_reviewed = v.labor_reviewed
         FROM (VALUES ${tuples.join(",")})
              AS v(airtable_id, employee_name, duration_seconds, city_taxes, class,
                   labor_type, job_name, labor_reviewed)
         WHERE t.airtable_id = v.airtable_id`,
        params
      );
    }
    driftCount = 0;   // corrected — the checks below now verify the repair
  }
}

if (LOAD) {
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

// ── reconcile deletions ───────────────────────────────────────────────────
// An upsert can insert and update but never remove, so a time entry DELETED in
// Airtable would live on in Neon forever — inflating hours and surfacing as a
// phantom dual-read mismatch. Every run extracts the COMPLETE id set, so we can
// diff it against Neon and tombstone the orphans.
//
// Tombstone rather than DELETE: silently dropping payroll history is exactly the
// kind of thing you want a record of. Rows are moved aside, not destroyed.
await sql.query(`CREATE TABLE IF NOT EXISTS time_entries_deleted (
  LIKE time_entries INCLUDING DEFAULTS,
  deleted_detected_at timestamptz DEFAULT now()
)`);

const liveIds = new Set(entries.map(r => r.id));
const neonIds = (await sql.query(`SELECT airtable_id FROM time_entries`)).map(r => r.airtable_id);
const orphans = neonIds.filter(id => !liveIds.has(id));

// Guardrail: a truncated or partially-failed extract would make most of the
// table look "deleted". Refuse to tombstone an implausible share of rows and
// make a human look instead — the ETL is re-runnable, so stopping costs nothing.
const orphanShare = neonIds.length ? orphans.length / neonIds.length : 0;
if (orphans.length > 50 && orphanShare > 0.05) {
  console.error(`\nABORT: ${orphans.length} of ${neonIds.length} rows (${(orphanShare * 100).toFixed(1)}%) ` +
    `look deleted upstream. That is too many to be real — suspecting a bad extract. ` +
    `Nothing was changed; investigate before re-running.`);
  process.exit(2);
}

if (orphans.length) {
  console.log(`reconcile: ${orphans.length} row(s) gone from Airtable -> tombstoning`);
  // Explicit column list, NOT `SELECT t.*`: time_entries_deleted was created with
  // LIKE time_entries before qb_timesheet_id existed, so the two shapes no longer
  // line up positionally and a star-select would misalign or fail outright.
  const TS_COLS = ["airtable_id", "qb_timesheet_id", "employee_name", "employee_id", "work_date",
                   "duration_seconds", "city_taxes", "class", "labor_type", "source", "notes",
                   "billable", "job_id", "job_name", "labor_reviewed", "airtable_created_at"];
  const list = TS_COLS.map(c => `"${c}"`).join(",");
  for (let i = 0; i < orphans.length; i += 200) {
    const chunk = orphans.slice(i, i + 200);
    const ph = chunk.map((_, k) => `$${k + 1}`).join(",");
    // Copy to the tombstone table first, then remove from the live table.
    await sql.query(
      `INSERT INTO time_entries_deleted (${list}, deleted_detected_at)
       SELECT ${TS_COLS.map(c => `t."${c}"`).join(",")}, now()
         FROM time_entries t WHERE t.airtable_id IN (${ph})`, chunk);
    await sql.query(`DELETE FROM time_entries WHERE airtable_id IN (${ph})`, chunk);
  }
} else {
  console.log("reconcile: no deletions detected");
}
} else {
  // Deletions are the puller's job now (it polls /timesheets_deleted). Tombstoning
  // from here would delete every row the puller legitimately owns but Airtable has
  // never had.
  console.log("skipping fact load + deletion reconcile (check-only). Pass --load to write.");
}

// ── acceptance checks ─────────────────────────────────────────────────────
// The Neon side is bounded by the same ASOF window as the Airtable side, so the
// nightly-Make / hourly-puller cadence gap can't masquerade as drift. In --load
// mode the bound is wide open and this behaves exactly as it always did.
const BOUND = LOAD ? "9999-12-31" : ASOF;

const [tot] = await sql.query(`
  SELECT count(*)::int AS entries,
         round(sum(hours), 2)::float8 AS hours,
         min(work_date)::text AS first,
         max(work_date)::text AS last,
         count(*) FILTER (WHERE job_name IS NULL)::int AS blank_name,
         count(*) FILTER (WHERE job_id IS NULL)::int AS no_link,
         count(DISTINCT job_name)::int AS distinct_names
  FROM time_entries WHERE work_date IS NULL OR work_date <= $1::date`, [BOUND]);

const neonBuckets = new Map((await sql.query(
  `SELECT job_name, round(sum(hours),2)::float8 AS hours
   FROM time_entries
   WHERE job_name IS NOT NULL AND (work_date IS NULL OR work_date <= $1::date)
   GROUP BY job_name`, [BOUND]
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
// Aggregates alone cannot see a row that is wrong on class / labor_type /
// employee_name, so field-level drift gets its own line. Fails loudly rather than
// being silently corrected — see the note above the drift block.
if (!LOAD) chk("rows with field-level drift", 0, driftCount);

console.log("\n== ACCEPTANCE CHECKS (Airtable vs Neon) ==");
console.table(checks);

const failed = checks.filter(c => !c.ok);
if (failed.length) {
  console.error(`\nFAILED ${failed.length} check(s) — Neon copy is NOT verified.`);
  process.exit(1);
}
console.log("\nAll checks passed — Neon copy verified against production Airtable.");

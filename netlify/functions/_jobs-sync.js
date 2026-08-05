// Jobs master sync + job-link backfill, run hourly from qb-time-pull.
// ---------------------------------------------------------------------------
// WHY THIS EXISTS
// The puller resolves a timesheet's job by looking its QB jobcode name up against
// `jobs.po_locked` (see jobByPo in qb-time-pull.js). Until 2026-08-05 the only
// thing that ever wrote the `jobs` table was the hand-run ETL
// (db/etl/time-entries-full.mjs). So a job created in Airtable today did not exist
// in Neon until somebody remembered to run that script — and every timesheet
// logged against it in the meantime landed with job_id NULL, permanently:
//
//   - the linker (ETL ~line 499) writes only `airtable_id`, never `job_id`
//   - `--repair`'s field list is employee_name / city_taxes / class / labor_type /
//     job_name — no `job_id` either
//
// so nothing ever went back and fixed it. Found 2026-08-05: 7 rows / 10.75 h across
// 3 jobs, the oldest from 2025-09-05, accumulating at roughly 7 rows a year. The
// visible symptom is that the per-job Time Entries tab (handleTimeEntries) INNER
// JOINs `jobs`, so those hours vanish from the job entirely — and because an empty
// result is not an error, the fail-soft fallback never fires and Airtable is never
// consulted. Payroll uses a LEFT JOIN, so pay was never affected.
//
// Two halves, matching the two halves of the bug:
//   syncJobs        keeps `jobs` fresh so new jobs are resolvable within the hour
//   backfillJobLinks heals any row that slipped through anyway
//
// BOTH FAIL SOFT. This runs inside the payroll-adjacent puller; a broken jobs sync
// must never stop timesheets importing. Errors are returned in the report, logged,
// and otherwise ignored — same contract as _neon.js, the opposite of _auth.js.
//
// ── WHY THIS NOW SETS `synced_at` (widened 2026-08-05) ──────────────────────
// It originally carried only the 8 identity columns needed for linkage, and
// deliberately did NOT stamp `synced_at` — that column means "when the FULL master
// record was last refreshed from Airtable" (db/schema/003_jobs_master.sql), and
// claiming it after an 8-column refresh would have hidden staleness.
//
// It now carries the full master set, so the stamp is honest and is set. This is the
// prerequisite for flipping `handleJobs` to Neon: `mapJob` reads status, addresses,
// customer and markup, so serving the job list from a mirror refreshed only when
// somebody hand-ran the ETL would have shown stale statuses on screen.
//
// ONE THING IT STILL LEAVES TO THE FULL ETL: `jobs.billable_rate_id`, the FK to
// labor_billable_rates. Resolving it needs a second lookup table loaded first, and
// nothing reads the column — `v_time_entry_billing` uses the VALUE,
// `jobs.billable_hourly_rate`, which IS carried below as a lookup. Checked before
// widening; recheck before relying on the FK.

const AT_API = "https://api.airtable.com/v0";

// [neon column, Airtable field, coercion]. This is the full master set and must stay
// in step with JOB_FIELDS in db/etl/time-entries-full.mjs — that array is the
// reference, and the two writers must agree or an hourly sync would silently revert
// whatever the ETL last wrote. The ~40 financial rollups are excluded on purpose:
// they roll up from estimates / invoices / expenses / allocations and their values
// here would go stale. See db/schema/003_jobs_master.sql.
//
// `po_locked` remains the load-bearing one — it is what the puller matches QB
// jobcode names against.
const nul  = v => (v === undefined || v === "" ? null : v);
const num  = v => (v === undefined || v === "" || v === null ? null : Number(v));
const bool = v => (v === undefined ? null : v === true);
// Airtable lookups come back as ARRAYS even when they resolve to one value. No job
// resolves to more than one billable rate (verified 2026-07-31), so taking the first
// is faithful — but it is a first, not a sum, deliberately.
const firstNum = v => (Array.isArray(v) ? num(v[0]) : num(v));
const FIELDS = [
  ["name",                    "Job Name",                         v => v || "(unnamed)"],
  ["po",                      "Job PO",                           nul],
  ["po_locked",               "Job PO - Locked",                  nul],
  ["po_number",               "Job PO Number",                    num],
  ["tsheets_job_id",          "TSheets Job ID",                   nul],
  ["status",                  "Job Status",                       nul],
  ["job_type",                "Job Type",                         nul],
  ["job_year",                "Job Year",                         num],
  ["billing_method",          "Billing Method",                   nul],
  ["billing_ready",           "Billing Ready",                    nul],
  ["tax_status",              "Tax Status",                       nul],
  ["start_date",              "Start Date",                       nul],
  ["finish_date",             "Finish Date",                      nul],
  ["project_completed_at",    "Project Completed At",             nul],
  ["bird_date",               "Bird Date",                        nul],
  ["address_full",            "Job Address - Full",               nul],
  ["address_street",          "Job Site Street Address (Intake)",  nul],
  ["address_city",            "Job Site City (Intake)",            nul],
  ["address_state",           "Job Site State (Intake)",           nul],
  ["address_zip",             "Job Site Zip Code (Intake)",        nul],
  ["miles_from_shop",         "Miles from Shop",                  num],
  ["customer_first_name",     "Customer 1st Name (Intake)",       nul],
  ["customer_last_name",      "Customer Last Name (Intake)",      nul],
  ["customer_email",          "Customer Email (Intake)",          nul],
  ["customer_phone",          "Customer Phone (Intake)",          nul],
  ["contractor_code",         "Contractor Code",                  nul],
  ["contractor_name",         "Contractor Name (Text)",           nul],
  ["notes",                   "Notes",                            nul],
  ["meter_number",            "Meter Number",                     nul],
  ["work_order_number",       "Permanent Work Order #",           nul],
  ["email_alias",             "Job Email Alias",                  nul],
  ["power_company",           "Power Company (Intake)",           nul],
  ["markup_pct",              "Job Markup %",                     num],
  ["generator_installed",     "Generator Installed",              bool],
  ["inspection_not_required", "Inspection Not Required",          bool],
  ["billable_hourly_rate",    "Billable Hourly Rate (from Labor Billable Rates)", firstNum],
];

// Airtable paginates at 100 and rate-limits at 5 req/sec per base. Requesting only
// the fields we map keeps the payload small — the Jobs table has 184 fields, several
// of them attachments, so this is still a small fraction of the record.
async function fetchJobs(apiKey, baseId) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    for (const [, at] of FIELDS) qs.append("fields[]", at);
    if (offset) qs.set("offset", offset);
    const r = await fetch(`${AT_API}/${baseId}/${encodeURIComponent("Jobs")}?${qs}`, {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    if (!r.ok) throw new Error(`Airtable Jobs ${r.status}: ${(await r.text()).slice(0, 200)}`);
    const d = await r.json();
    out.push(...(d.records || []));
    offset = d.offset;
    if (offset) await new Promise(res => setTimeout(res, 220));
  } while (offset);
  return out;
}

// Upsert on airtable_id, every mapped column in DO UPDATE SET. Airtable is the sole
// source of truth for Jobs — nothing writes back — so overwriting unconditionally is
// correct: there is no app-owned job state on this table that a refresh could clobber.
// The columns NOT listed in FIELDS (billable_rate_id, created_at) are untouched.
//
// A guard worth keeping: an empty Airtable response returns early rather than
// upserting nothing, so a transient read that comes back blank can never be mistaken
// for "there are no jobs".
export async function syncJobs(sql, apiKey, baseId) {
  try {
    const records = await fetchJobs(apiKey, baseId);
    if (!records.length) return { ok: true, jobs: 0 };

    const cols = ["airtable_id", ...FIELDS.map(([c]) => c), "synced_at"];
    const setList = cols.slice(1).map(c => `"${c}" = EXCLUDED."${c}"`).join(", ");
    const syncedAt = new Date().toISOString();

    // 100 rows × 38 columns ≈ 3,800 bind parameters per statement, comfortably under
    // Postgres's 65,535 ceiling. The chunk was 200 while this carried 8 columns.
    for (let i = 0; i < records.length; i += 100) {
      const chunk = records.slice(i, i + 100);
      const params = [];
      const tuples = chunk.map(j => {
        const row = [j.id, ...FIELDS.map(([, at, coerce]) => coerce(j.fields?.[at])), syncedAt];
        const ph = row.map(v => { params.push(v); return `$${params.length}`; });
        return `(${ph.join(",")})`;
      });
      await sql.query(
        `INSERT INTO jobs (${cols.map(c => `"${c}"`).join(",")}) VALUES ${tuples.join(",")}
           ON CONFLICT ("airtable_id") DO UPDATE SET ${setList}`,
        params
      );
    }
    return { ok: true, jobs: records.length };
  } catch (e) {
    console.error(`jobs-sync: failed (continuing) — ${e?.message || e}`);
    return { ok: false, error: String(e?.message || e) };
  }
}

// Link any time entry that has no job_id but whose job_name now resolves to exactly
// ONE job. The comparison mirrors the puller's jobByPo lookup — lower(trim()) on
// both sides — so a row this links is a row the puller would have linked itself had
// the job existed at import time.
//
// The `= 1` guard is not decoration: two jobs sharing a Job PO - Locked would
// otherwise attach hours to an arbitrary one of them. Ambiguity is left alone and
// reported, never guessed — the same rule the ETL's linker uses for airtable_id.
//
// Rows whose job_name matches NO job are untouched by design. ~11,173 of them are
// pre-migration history the owner deliberately kept for the timestamps; they carry
// job_name text and will never have a job record. See db/schema/001_time_entries.sql.
export async function backfillJobLinks(sql) {
  try {
    const rows = await sql.query(
      `UPDATE time_entries t SET job_id = j.id
         FROM jobs j
        WHERE t.job_id IS NULL
          AND lower(trim(j.po_locked)) = lower(trim(t.job_name))
          AND (SELECT count(*) FROM jobs j2
                WHERE lower(trim(j2.po_locked)) = lower(trim(t.job_name))) = 1
       RETURNING t.id`
    );
    const linked = rows?.length ?? 0;
    if (linked) console.log(`jobs-sync: backfilled job_id on ${linked} time entr${linked === 1 ? "y" : "ies"}`);
    return { ok: true, linked };
  } catch (e) {
    console.error(`jobs-sync: backfill failed (continuing) — ${e?.message || e}`);
    return { ok: false, error: String(e?.message || e) };
  }
}

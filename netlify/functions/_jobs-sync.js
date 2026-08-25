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

// [neon column, Airtable field, coercion]. **THIS IS THE SINGLE SOURCE OF TRUTH for
// the jobs field map.** It is exported as JOB_FIELDS and imported by the ETL
// (db/etl/time-entries-full.mjs), which used to keep its own copy — two writers
// against one table, and if they disagreed an hourly sync would silently revert
// whatever the ETL last wrote. That risk is now structural rather than a comment:
// there is only one list.
//
// The ~40 financial rollups are excluded on purpose — they roll up from estimates /
// invoices / expenses / allocations, so a copied VALUE here would go stale between
// syncs. Those are computed live from v_job_rollups / v_job_financials instead.
// Same for the five "All … Reviewed?" gates. See db/schema/003_jobs_master.sql.
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
// Lookup fields (multipleLookupValues) also arrive as arrays. `g()` in airtable.js
// renders them by joining on ", ", so this MUST join the same way — the stored value
// has to equal what mapJob produces today or the read flip changes what is on screen.
const joinText = v => (Array.isArray(v) ? (v.length ? v.join(", ") : null) : nul(v));
// Link fields (multipleRecordLinks) arrive as ["rec…"]. The UI wants a single id for
// typeahead prefill, and these tables stay Airtable-owned, so the rec id is stored
// verbatim as text rather than resolved to a Neon FK.
const firstAtId = v => (Array.isArray(v) && v.length
  ? (typeof v[0] === "string" ? v[0] : v[0]?.id || null) : null);
export const JOB_FIELDS = [
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

  // ── Added 2026-08-05 for the handleJobs flip ─────────────────────────────
  // Everything below is what `mapJob` returns beyond the original master set.
  // It looked like it needed three new dimension tables (Power Companies,
  // Inspection Agencies, Inspection Contacts) — it does not. Every value field
  // is a multipleLookupValues ON Jobs, so Airtable already hands back the
  // resolved value inside the Jobs record. Only the links need an id.
  ["power_company_name",       "Power Company – Name (lookup)",             joinText],
  ["power_company_contact",    "Power Company – Primary Contact (lookup)",  joinText],
  ["power_company_cell_phone", "Power Company – Cell Phone (lookup)",       joinText],
  ["power_company_office_phone","Power Company – Office Phone (lookup)",    joinText],
  ["power_company_email",      "Power Company – Email (lookup)",            joinText],
  ["power_company_at_id",      "Power Companies",                           firstAtId],
  ["power_contact_at_id",      "Power Company Contacts",                    firstAtId],
  ["aic_number",               "AIC Number",                                joinText],
  ["temp_work_order",          "Temporary Work Order #",                    joinText],
  ["permit_number",            "Permit Number",                             joinText],
  ["inspection_agency",        "Inspection Agency Name (from Inspection Agency)", joinText],
  ["inspection_agency_phone",  "Inspection Agency Phone #",                 joinText],
  ["inspection_agency_email",  "Inspection Agency Email Address",           joinText],
  ["inspection_agency_at_id",  "Inspection Agency",                         firstAtId],
  ["inspection_scheduling_link","Inspection Scheduling Link",               joinText],
  // One column feeds TWO mapJob keys: `inspectionContacts` (the joined string)
  // and `inspectorName` (the first element). The UI constrains a job to a single
  // inspector, so in practice they are the same value.
  ["inspector_name",           "Inspector Name (from Inspection Contacts)", joinText],
  ["inspector_phone",          "Inspector Phone",                           joinText],
  ["inspector_email",          "Inspector Email",                           joinText],
  ["inspector_at_id",          "Inspection Contacts",                       firstAtId],
  ["job_inspections",          "Inspection Name (from Job Inspections)",    joinText],
  // Stored RAW. mapJob runs extractUrl() over these at read time to pull the href
  // out of the formula text — keep that transform at the read, not here, so the
  // stored value stays faithful to Airtable.
  //
  // NOT carried: the old `wireLink` / `pipeLink` keys. Their Airtable fields
  // ("Wire (Mobile) or THHN (Mobile)", "Add Pipe (Mobile)") no longer exist — the
  // JotForm wire/pipe path was retired in favour of the inventory app's expense
  // push — so mapJob had been returning null for both on every job, and nothing in
  // either SPA read them. Requesting them here 422s the whole fetch, which is how
  // they were found. Removed from F.job and mapJob in the same change.
  ["add_photos_link",          "Add Photos (Mobile)",                       joinText],
  ["view_photos_link",         "View pCloud Photos",                        joinText],
  ["pcloud_photo_folder_id",   "pCloud Photo's ID",                         joinText],
  ["pcloud_invoices_sent_id",  "pCloud Invoices Sent ID",                   joinText],
  ["trello_card_id",           "Trello Card ID",                            joinText],
  // ── Added 2026-08-20, db/schema/045 ──────────────────────────────────────
  // The run-once guards. Carried hourly so Neon can take over the decision from
  // the Airtable PATCH response, which is what lets Make's two write-back
  // modules be deleted. ⚠ EN DASH in every "Automation – …" name — a hyphen
  // reads undefined, which is falsy, which fires the webhook every single time.
  ["pcloud_folders_created",   "Automation – pCloud Folders Created",       bool],
  ["trello_created",           "Automation – Trello Created",               bool],
  ["tsheets_created",          "Automation – TSheets Created",              bool],
  ["trello_completed",         "Automation – Trello Completed",             bool],
  ["trello_po_card_id",        "Trello Card PO ID",                         joinText],
  ["start_service_call",       "Start Service Call",                        bool],
  ["service_call_created",     "Service Call Created",                      bool],
  ["project_complete",         "Project Complete (Ready to Invoice)",       bool],
  // "Worklfow" is misspelled IN AIRTABLE. Match it verbatim — correcting it here
  // silently returns null for every job.
  ["workflow_status",          "Worklfow Status",                           joinText],
  ["contractor_at_id",         "Contractor",                                firstAtId],
  ["labor_billable_rate_at_id","Labor Billable Rates",                      firstAtId],
];

// Kept as the module-local name the rest of this file already uses.
const FIELDS = JOB_FIELDS;

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
//
// ── ⚠⚠ THE MIRROR SKIP (2026-08-24, schema 062) ────────────────────────────
// "Airtable is the sole source of truth for Jobs — nothing writes back" stopped
// being true when JOB_CREATE_SOURCE=native shipped. A job born in Neon POSTs a
// best-effort MIRROR into this very table, and that mirror is a record like any
// other: its rec id matches no Neon row (the id is deliberately never stamped
// back), so the upsert below took the INSERT branch and re-imported the app's
// own job as a SECOND one, an hour after it was created. It listed twice —
// `handleGetJobs` is `${JOB_SELECT} ORDER BY j.name`, unfiltered — and the copy
// frozen at the mirror's creation-time values sorted right next to the real one.
//
// So a mirror is skipped by rec id, from a list this database owns
// (`jobs.airtable_mirror_id`). Not by name, not by PO number, not by a marker
// field on the Airtable side: this must never be able to skip a job somebody
// really did create in Airtable, and only an exact id can promise that.
//
// ⚠ The skip is not "Airtable no longer matters for native jobs" — it is
// narrower than that. It says: this ONE record is our own outbound copy, and
// re-reading our own writing is how a mirror becomes a ghost.
export async function syncJobs(sql, apiKey, baseId) {
  try {
    const records = await fetchJobs(apiKey, baseId);
    if (!records.length) return { ok: true, jobs: 0 };

    const { mirrors, ghosts } = await mirrorSkipList(sql, records);
    const incoming = mirrors.size
      ? records.filter(r => !mirrors.has(r.id))
      : records;
    const skipped = records.length - incoming.length;
    if (skipped) console.log(`jobs-sync: skipped ${skipped} native-job mirror(s)`);
    // Not skipped — REPORTED. A PO number the counter issued to a native job,
    // arriving on some other Airtable record, is either a mirror we failed to
    // record (see recordMirrorId) or the stale-counter duplicate-PO bug. Both
    // need a human; neither is safe to silently drop, because dropping a real
    // job means its QuickBooks hours never link and nobody finds out for weeks.
    for (const g of ghosts) {
      console.error(
        `jobs-sync: ⚠ Airtable job ${g.airtable_id} carries PO ${g.po_number}, which ` +
        `belongs to native job ${g.id}. Unrecorded mirror, or a duplicate PO — check before it bills.`);
    }
    if (!incoming.length) return { ok: true, jobs: 0, skippedMirrors: skipped };

    const cols = ["airtable_id", ...FIELDS.map(([c]) => c), "synced_at"];
    const setList = cols.slice(1).map(c => `"${c}" = EXCLUDED."${c}"`).join(", ");
    const syncedAt = new Date().toISOString();

    // 100 rows × 38 columns ≈ 3,800 bind parameters per statement, comfortably under
    // Postgres's 65,535 ceiling. The chunk was 200 while this carried 8 columns.
    for (let i = 0; i < incoming.length; i += 100) {
      const chunk = incoming.slice(i, i + 100);
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
    return { ok: true, jobs: incoming.length, skippedMirrors: skipped };
  } catch (e) {
    console.error(`jobs-sync: failed (continuing) — ${e?.message || e}`);
    return { ok: false, error: String(e?.message || e) };
  }
}

// The skip list, plus the tell for anything it did not catch.
//
// ⚠ FAILS SOFT, and the fallback is the OLD behaviour, not a blocked sync. If this
// query throws — the column missing because the migration has not run, Neon slow,
// anything — the sync proceeds unfiltered: one duplicate job in a list is a far
// smaller harm than an hour of timesheets that cannot resolve their job, which is
// what refusing to sync would cost. That is this file's whole contract (see the
// header): a broken jobs sync must never stop timesheets importing.
//
// Both halves come from ONE round trip. `mirrors` is the exact id skip list;
// `ghosts` is every OTHER Airtable record whose PO number is already held by a
// native job, which is what an unrecorded mirror looks like from here.
async function mirrorSkipList(sql, records) {
  const empty = { mirrors: new Set(), ghosts: [] };
  try {
    const ids = records.map(r => r.id);
    const pos = [...new Set(records
      .map(r => num(r.fields?.["Job PO Number"]))
      .filter(v => v !== null))];
    const r = await sql.query(
      `SELECT id::text AS id, airtable_mirror_id, po_number
         FROM jobs
        WHERE airtable_id IS NULL
          AND (airtable_mirror_id = ANY($1::text[]) OR po_number = ANY($2::int[]))`,
      [ids, pos]);
    // ⚠⚠ `sql` HERE IS `neon()` FROM @neondatabase/serverless, AND ITS `.query()`
    // RESOLVES TO A BARE ARRAY OF ROWS — not the `{ rows }` result object that
    // `neonQuery` in airtable.js returns. Two shapes, one codebase. This line was
    // `r?.rows || []` for one deploy: the skip list came back empty on every run,
    // `mirrorSkipList` reported success, and the ghost was re-inserted at the very
    // next hourly sync — no error anywhere, because an empty skip list is exactly
    // what "there are no mirrors" looks like. `backfillJobLinks` below has always
    // read `rows?.length` on the same client and is the proof.
    //
    // So this ASSERTS the shape rather than absorbing both. A defensive
    // `r?.rows || []` fallback is what silence is made of: it turns a changed
    // driver into an empty answer. Throwing lands in the catch below, which logs
    // and syncs unfiltered — the same fail-soft outcome, but audible.
    if (!Array.isArray(r)) {
      throw new Error(`sql.query returned ${r === null ? "null" : typeof r}, expected an array of rows — driver shape changed`);
    }
    const rows = r;
    const mirrors = new Set(rows.map(x => x.airtable_mirror_id).filter(Boolean));
    // A record is a ghost candidate only if it is NOT the recorded mirror: the
    // mirror shares its job's PO by definition, so reporting it would be noise.
    const byPo = new Map(rows.filter(x => x.po_number !== null).map(x => [Number(x.po_number), x]));
    const ghosts = [];
    for (const rec of records) {
      if (mirrors.has(rec.id)) continue;
      const hit = byPo.get(num(rec.fields?.["Job PO Number"]));
      if (hit) ghosts.push({ airtable_id: rec.id, po_number: hit.po_number, id: hit.id });
    }
    return { mirrors, ghosts };
  } catch (e) {
    console.error(`jobs-sync: mirror skip list unavailable, syncing unfiltered — ${e?.message || e}`);
    return empty;
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

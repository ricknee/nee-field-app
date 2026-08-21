// QB Time -> Neon incremental puller (plan step 5). Netlify Scheduled Function,
// hourly — see the [functions."qb-time-pull"] block in netlify.toml.
// ---------------------------------------------------------------------------
// WHY A PULL AND NOT A PUSH
// Make and webhooks are push: one failed run silently loses a record and nothing
// notices. This asks "everything modified since my watermark", so a failed run
// costs a retry, not data. Every write is an upsert, so replaying is free.
//
// RUNS IN PARALLEL WITH MAKE. Make is still importing the same timesheets into
// Airtable; this writes Neon. The two are reconciled by the check-only ETL. Do not
// retire Make until they have agreed for several days.
// ---------------------------------------------------------------------------
// TWO NON-OBVIOUS RULES THIS FILE EXISTS TO ENFORCE
//
// 1. `labor_reviewed` is APP-OWNED and is NOT in QuickBooks. It must never appear
//    in ON CONFLICT DO UPDATE SET, or every re-sync silently wipes the reviewed
//    flags payroll depends on.
//
// 2. OPEN timesheets are NOT in the `modified_since` feed. Verified 2026-07-30: two
//    employees were clocked in (state OPEN, duration 0, last_modified that morning)
//    and a modified_since query covering that window returned neither. A timesheet
//    only enters the feed once it is CLOSED and has a real duration.
//    So the puller sees a shift roughly an hour after CLOCK-OUT, not while it runs.
//    This is better than the alternative — no zero-duration placeholder rows to
//    update later — but it has one consequence: a shift nobody clocks out of stays
//    OPEN and never imports. Make has the same blind spot, so it is not a
//    regression, but hourly pulls make it visible sooner. Watch for a shift that
//    never lands rather than assuming the puller dropped it.
//
// 3. What Make actually excludes is TIME OFF — not unmatched jobcodes.
//    An earlier version of this file skipped any timesheet whose jobcode had no
//    matching Job, on the theory that Make's "Seach Job Name" module drops the
//    bundle when its search finds nothing. That was WRONG and it was skipping real
//    work. Disproved 2026-07-31 by Airtable's own contents: "Troy Koehn (MIT 380)"
//    (6 rows, 20 h), bare "Shop Work" (450 rows, 944 h), "Travel" and
//    "Office Work (MIO 427)" all exist as Time Entries with an EMPTY Job link —
//    so Make imports unmatched jobcodes quite happily.
//
//    Comparing 2026 only, so it reflects the CURRENT Make scenario: QB used 64
//    jobcodes, Airtable holds 58. The six absent are Lunch Break (567), Paid
//    Vacation (8), Holiday (7), Vacation (4) — all time off — plus 12 timesheets
//    on two REAL jobs (Jeannie Oyster 11, Alpine Kitchen 1) that Make simply
//    missed. So the rule is an explicit time-off list, and the stragglers are
//    Make's intermittent failures, which is what this puller exists to fix.
//
//    Consequence worth expecting: the puller is MORE complete than Make, so while
//    both run the reconciler may report Neon slightly ahead. That is correct, not
//    drift — INSERT_FLOOR_DATE keeps it out of closed pay periods.
import { neon } from "@neondatabase/serverless";
import { syncJobs, backfillJobLinks } from "./_jobs-sync.js";
import { syncBillingTables } from "./_billing-sync.js";
import { runGeneratorServiceCheck } from "./_generator-service.js";
import { makeAtFetch } from "./_jobs.js";

const QB = "https://rest.tsheets.com/api/v1";

// INSERT POLICY — guards, not preferences. Updates to already-known timesheets are
// never blocked by these; they only gate creating rows that have never existed.
//   - a timesheet whose jobcode is not a known Job is skipped (Make parity)
//   - a timesheet older than this floor is skipped, so a cold-start watermark can
//     never backfill years of history into payroll
//
// SET TO THE START OF THE OPEN PAY PERIOD (owner decision 2026-07-30).
// The dry run found 10 timesheets / 21.25 h from 2026-07-07..07-24 that Make never
// imported — real hours on real jobs, simply lost. Across all history the claim pass
// counted 712 such rows. That is the intermittent-Make problem this migration exists
// to fix, but those dates sit inside the pay period ending 2026-07-25, which was RUN
// AND PAID on 07-27. Inserting them would retroactively change a closed period.
//
// So the puller starts clean at the open period: no paid period moves, Neon and
// Airtable keep agreeing, and the reconciler stays meaningful. The missing hours are
// NOT lost — they are still in QuickBooks. Recovering them is a deliberate, separate
// decision: lower this floor and re-run, then expect (and reconcile) the divergence.
const INSERT_FLOOR_DATE = "2026-07-26";

// Jobcodes that are TIME OFF, not work. Derived from data, not assumed: these are
// the only non-job jobcodes with QB timesheets in 2026 and zero Time Entries in
// Airtable. Everything else QB reports IS imported by Make, including jobcodes with
// no matching Job record — see note 3 above.
//
// Compared lowercase. If a new time-off jobcode is added in QuickBooks it will start
// importing as if it were work; the run report lists every jobcode carrying a NULL
// job_id, so a newcomer shows up there rather than silently inflating payroll.
const NON_WORK_JOBCODES = new Set([
  "lunch break",
  "vacation",
  "paid vacation",
  "holiday",
]);

// Overlap re-asked on every run. Costs nothing (upserts are idempotent) and covers
// clock skew between QB's clock and ours.
const WATERMARK_OVERLAP_MS = 15 * 60 * 1000;
const COLD_START_LOOKBACK_MS = 48 * 60 * 60 * 1000;

// A pull that suddenly wants to delete a large share of the table is far more likely
// to be a bad extract than a real mass deletion. Same guardrail as the ETL.
const MAX_DELETE_PER_RUN = 200;

// QB rejects `modified_since` with milliseconds — JS toISOString() emits ".075Z"
// and the API answers HTTP 417. This is the whole reason this helper exists.
const qbTime = d => new Date(d).toISOString().replace(/\.\d{3}Z$/, "Z");

async function qbFetch(token, pathname, params) {
  const out = [];
  const jobcodes = new Map();
  const users = new Map();

  for (let page = 1; ; page++) {
    const p = new URLSearchParams({ ...params, per_page: "50", page: String(page) });
    const r = await fetch(`${QB}/${pathname}?${p}`, { headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) throw new Error(`QB ${pathname} p${page} ${r.status}: ${(await r.text()).slice(0, 300)}`);
    const d = await r.json();

    const supp = d.supplemental_data || {};
    for (const [id, j] of Object.entries(supp.jobcodes || {})) jobcodes.set(String(id), j.name);
    for (const [id, u] of Object.entries(supp.users || {})) {
      users.set(String(id), `${u.first_name || ""} ${u.last_name || ""}`.trim());
    }

    // `results.<pathname>` is an OBJECT KEYED BY ID, not an array.
    const items = Object.values((d.results || {})[pathname] || {});
    out.push(...items);
    if (d.more !== true || !items.length) break;
    if (page > 400) throw new Error(`QB ${pathname}: refusing to page past 400`);
  }
  return { items: out, jobcodes, users };
}

export async function runPull({ sql, token, since, dryRun = false }) {
  const report = {
    since, dryRun,
    fetched: 0, upserted: 0, claimed: 0,
    skippedTimeOff: 0, skippedTooOld: 0, skippedNoEmployee: 0,
    deleted: 0, wouldInsert: 0, wouldUpdate: 0,
    // Jobcodes imported with a NULL job_id. NOT skipped — reported so an unfamiliar
    // one is noticed rather than silently becoming payable hours.
    unknownJobcodes: [], insertSamples: [], deletedSamples: [],
    newWatermark: null, errors: [],
  };

  // Lookup dimensions. These are maintained by the ETL's dimension load, which stays
  // live even when its fact load is disabled.
  const [emps, jobs] = await Promise.all([
    sql.query(`SELECT id, qb_user_id, name FROM employees WHERE qb_user_id IS NOT NULL`),
    sql.query(`SELECT id, po_locked FROM jobs WHERE po_locked IS NOT NULL`),
  ]);
  const empByQbId = new Map(emps.map(e => [String(e.qb_user_id), e.id]));
  const jobByPo   = new Map(jobs.map(j => [String(j.po_locked).trim().toLowerCase(), j.id]));

  const { items, jobcodes, users } = await qbFetch(token, "timesheets", { modified_since: qbTime(since) });
  report.fetched = items.length;

  const rows = [];
  const unknown = new Set();
  let maxModified = null;

  for (const ts of items) {
    if (ts.last_modified && (!maxModified || ts.last_modified > maxModified)) maxModified = ts.last_modified;

    const jobName = jobcodes.get(String(ts.jobcode_id)) || null;
    const jobId   = jobName ? jobByPo.get(jobName.trim().toLowerCase()) || null : null;

    // Time off is not work — the only category Make genuinely excludes.
    if (jobName && NON_WORK_JOBCODES.has(jobName.trim().toLowerCase())) {
      report.skippedTimeOff++;
      continue;
    }
    // A jobcode with no matching Job is still imported, with job_id left NULL and
    // job_name carrying the text — exactly what Make does. Tracked so an unfamiliar
    // jobcode is visible in the run report instead of quietly becoming payable hours.
    if (!jobId) unknown.add(jobName || `(jobcode ${ts.jobcode_id})`);

    const empName = users.get(String(ts.user_id)) || null;
    const empId   = empByQbId.get(String(ts.user_id)) || null;
    if (!empName && !empId) { report.skippedNoEmployee++; continue; }

    const cf = ts.customfields || {};
    rows.push({
      qb_timesheet_id: String(ts.id),
      employee_name:   empName,
      employee_id:     empId,
      work_date:       ts.date,
      duration_seconds: Number(ts.duration) || 0,
      city_taxes:      cf["65840"] || null,          // Taxes
      // Make maps customfield 71185 to BOTH Class and Labor Type. Replicated.
      class:           cf["71185"] || null,
      labor_type:      cf["71185"] || null,
      source:          "TSheets",
      notes:           ts.notes || null,
      // Make HARDCODES Billable = true and ignores customfield 71181. Replicated so
      // the reconciler doesn't report a difference that is really a Make quirk.
      billable:        true,
      job_id:          jobId,
      job_name:        jobName,
    });
  }
  report.unknownJobcodes = [...unknown].slice(0, 25);

  if (!rows.length) {
    report.newWatermark = maxModified || null;
    return report;
  }

  // ── claim before upsert ─────────────────────────────────────────────────
  // Rows that predate the cutover exist in Neon from the Airtable ETL with a NULL
  // qb_timesheet_id. Without this an edit to an old timesheet would INSERT a
  // duplicate instead of updating. Only claims when the natural key is unambiguous
  // in BOTH directions (one QB timesheet <-> one Neon row).
  const cp = [];
  const cTuples = rows.map(r => {
    cp.push(r.qb_timesheet_id, (r.employee_name || "").trim().toLowerCase(), r.work_date,
            r.duration_seconds, (r.job_name || "").trim().toLowerCase());
    const n = cp.length;
    return `($${n - 4}::text, $${n - 3}::text, $${n - 2}::date, $${n - 1}::numeric, $${n}::text)`;
  });

  if (!dryRun) {
    const claimed = await sql.query(
      `WITH incoming(qb_id, emp, wd, secs, job) AS (VALUES ${cTuples.join(",")}),
            cand AS (
              SELECT i.qb_id, t.id AS row_id,
                     count(*) OVER (PARTITION BY i.qb_id) AS n_rows,
                     count(*) OVER (PARTITION BY t.id)    AS n_qb
                FROM incoming i
                JOIN time_entries t
                  ON t.qb_timesheet_id IS NULL
                 AND t.work_date = i.wd
                 AND t.duration_seconds = i.secs
                 AND lower(coalesce(t.employee_name,'')) = i.emp
                 AND lower(coalesce(t.job_name,''))      = i.job
            )
       UPDATE time_entries t SET qb_timesheet_id = c.qb_id
         FROM cand c
        WHERE t.id = c.row_id AND c.n_rows = 1 AND c.n_qb = 1
        RETURNING t.id`,
      cp
    );
    report.claimed = claimed.length;
  }

  // ── upsert ──────────────────────────────────────────────────────────────
  // Split by whether the row already exists, so the insert guards apply only to
  // genuinely new timesheets and never block an update.
  const known = new Set(
    (await sql.query(
      `SELECT qb_timesheet_id FROM time_entries WHERE qb_timesheet_id = ANY($1::text[])`,
      [rows.map(r => r.qb_timesheet_id)]
    )).map(r => r.qb_timesheet_id)
  );

  const toWrite = [];
  for (const r of rows) {
    if (known.has(r.qb_timesheet_id)) { report.wouldUpdate++; toWrite.push(r); continue; }
    if (r.work_date < INSERT_FLOOR_DATE) { report.skippedTooOld++; continue; }
    report.wouldInsert++;
    // Inserts are the only way this puller can change historical totals, so keep a
    // sample for the operator rather than reporting a bare count.
    if (report.insertSamples.length < 25) {
      report.insertSamples.push({
        qb: r.qb_timesheet_id, date: r.work_date, emp: r.employee_name,
        job: r.job_name, hrs: Math.round((r.duration_seconds / 3600) * 4) / 4,
      });
    }
    toWrite.push(r);
  }

  const COLS = ["qb_timesheet_id", "employee_name", "employee_id", "work_date", "duration_seconds",
                "city_taxes", "class", "labor_type", "source", "notes", "billable", "job_id", "job_name"];

  if (!dryRun && toWrite.length) {
    for (let i = 0; i < toWrite.length; i += 200) {
      const chunk = toWrite.slice(i, i + 200);
      const params = [];
      const tuples = chunk.map(r => {
        const ph = COLS.map(c => { params.push(r[c]); return `$${params.length}`; });
        return `(${ph.join(",")})`;
      });
      // labor_reviewed and airtable_id are DELIBERATELY absent from the SET list —
      // both are owned by this app, not by QuickBooks. See rule 1 at the top.
      const setList = COLS.filter(c => c !== "qb_timesheet_id")
                          .map(c => `"${c}"=EXCLUDED."${c}"`).join(", ");
      await sql.query(
        `INSERT INTO time_entries (${COLS.map(c => `"${c}"`).join(",")}) VALUES ${tuples.join(",")}
         ON CONFLICT (qb_timesheet_id) DO UPDATE SET ${setList}`,
        params
      );
      report.upserted += chunk.length;
    }
  }

  // ── deletions ───────────────────────────────────────────────────────────
  // Without this, hours deleted in QB live on in Neon and inflate payroll. Not
  // theoretical: 3 real deletions in the 30 days to 2026-07-27.
  try {
    const del = await qbFetch(token, "timesheets_deleted", { modified_since: qbTime(since) });
    const ids = del.items.map(d => String(d.id));
    if (ids.length > MAX_DELETE_PER_RUN) {
      report.errors.push(`refusing to delete ${ids.length} rows in one run (max ${MAX_DELETE_PER_RUN}) — investigate`);
    } else if (ids.length && !dryRun) {
      // Explicit column list, NOT `SELECT t.*`: time_entries_deleted was created with
      // LIKE time_entries before qb_timesheet_id existed, so the shapes differ. It
      // also has no generated columns, so hours/week_start_date are plain and are
      // simply left null on the tombstone.
      const cols = ["qb_timesheet_id", "airtable_id", "employee_name", "employee_id", "work_date",
                    "duration_seconds", "city_taxes", "class", "labor_type", "source", "notes",
                    "billable", "job_id", "job_name", "labor_reviewed", "airtable_created_at"];
      const list = cols.map(c => `"${c}"`).join(",");
      const moved = await sql.query(
        `WITH gone AS (
           INSERT INTO time_entries_deleted (${list}, deleted_detected_at)
           SELECT ${cols.map(c => `t."${c}"`).join(",")}, now()
             FROM time_entries t WHERE t.qb_timesheet_id = ANY($1::text[])
           RETURNING qb_timesheet_id)
         DELETE FROM time_entries WHERE qb_timesheet_id IN (SELECT qb_timesheet_id FROM gone)
         RETURNING id`,
        [ids]
      );
      report.deleted = moved.length;
    } else if (ids.length) {
      // Dry run: report only what actually corresponds to a Neon row. Most QB
      // deletions are of timesheets Neon never held (Lunch Break and friends), so
      // the raw count from QB overstates the real impact.
      report.deletedSamples = await sql.query(
        `SELECT qb_timesheet_id AS qb, work_date::text AS date, employee_name AS emp,
                job_name AS job, hours::float8 AS hrs
           FROM time_entries WHERE qb_timesheet_id = ANY($1::text[])`,
        [ids]
      );
      report.deleted = report.deletedSamples.length;
    }
  } catch (e) {
    report.errors.push(`deleted-poll failed: ${String(e?.message || e).slice(0, 200)}`);
  }

  // ── watermark ───────────────────────────────────────────────────────────
  report.newWatermark = maxModified || null;
  if (!dryRun && maxModified) {
    await sql.query(
      `INSERT INTO sync_state (key, watermark, updated_at, note)
       VALUES ('qb_timesheets', $1::timestamptz, now(), $2)
       ON CONFLICT (key) DO UPDATE SET watermark = EXCLUDED.watermark,
                                       updated_at = now(), note = EXCLUDED.note`,
      [maxModified, `fetched=${report.fetched} upserted=${report.upserted} deleted=${report.deleted}`]
    );
  }
  return report;
}

export const handler = async () => {
  const token = process.env.QB_TIME_TOKEN;
  const url   = process.env.DATABASE_URL;
  if (!token || !url) {
    // Fail loudly in logs but don't retry-storm: a missing secret is a deploy problem.
    console.error("qb-time-pull: missing QB_TIME_TOKEN or DATABASE_URL");
    return { statusCode: 500, body: "missing config" };
  }

  const sql = neon(url);

  // Refresh the Jobs master record BEFORE pulling timesheets, so a job created
  // in Airtable since the last run is already resolvable by the jobByPo lookup in
  // runPull. Order matters: run it after, and a timesheet for a brand-new job lands
  // with job_id NULL and nothing ever goes back to fix it — that was the bug this
  // exists to close (see _jobs-sync.js). Then heal anything that slipped through
  // historically. Both fail soft: the timesheet pull is the job that must not stop.
  const atKey  = process.env.AIRTABLE_API_KEY;
  const atBase = process.env.AIRTABLE_BASE_ID;
  let jobsReport = { ok: false, error: "AIRTABLE_API_KEY / AIRTABLE_BASE_ID unset" };
  if (atKey && atBase) jobsReport = await syncJobs(sql, atKey, atBase);
  else console.error("qb-time-pull: jobs sync skipped — AIRTABLE_API_KEY / AIRTABLE_BASE_ID unset");
  const linkReport = await backfillJobLinks(sql);

  // ⚠ The billing allocations have NO WRITE PATH in the app — they are created
  // inside Airtable — and invoice totals are computed FROM them. Before this ran
  // hourly, invoicing a job left Neon's total reading LOW until somebody
  // remembered to run the ETL by hand. Fails soft: a stale allocation is a
  // smaller problem than a missed timesheet.
  //
  // Estimate templates used to ride along here. They got a write path on
  // 2026-08-20, at which point syncing them stopped preserving the table and
  // started overwriting the app's edits — see the header of _billing-sync.js.
  let billingReport = { ok: false, error: "AIRTABLE_API_KEY / AIRTABLE_BASE_ID unset" };
  if (atKey && atBase) billingReport = await syncBillingTables(sql, atKey, atBase);

  // Generator service calls — replaces Airtable automation wfledvx1A8oVscWla,
  // the last one in the base that CREATED a record. Runs here rather than on its
  // own schedule because this function is already the hourly heartbeat and a
  // second scheduled function is a second thing to notice has stopped.
  //
  // ORDER MATTERS, mildly: it runs AFTER syncJobs so a service-call job created
  // by the previous hour's run is already in `jobs` and its status can be read.
  //
  // ⚠ INERT unless GENERATOR_SERVICE_CALLS=on — it returns {enabled:false} and
  // touches nothing. See the header of _generator-service.js.
  //
  // Fails soft and is caught here on purpose: the timesheet pull is the job that
  // must not stop, and a generator reminder is the least urgent thing this
  // function does.
  let generatorReport = { ok: false, error: "AIRTABLE_API_KEY / AIRTABLE_BASE_ID unset" };
  if (atKey && atBase) {
    try {
      generatorReport = await runGeneratorServiceCheck(makeAtFetch(atKey, atBase));
    } catch (e) {
      console.error("qb-time-pull: generator service check failed", e);
      generatorReport = { ok: false, error: String(e?.message || e) };
    }
  }

  const [state] = await sql.query(`SELECT watermark FROM sync_state WHERE key = 'qb_timesheets'`);
  const since = state?.watermark
    ? new Date(new Date(state.watermark).getTime() - WATERMARK_OVERLAP_MS)
    : new Date(Date.now() - COLD_START_LOOKBACK_MS);

  try {
    const report = { ...(await runPull({ sql, token, since })), jobsSync: jobsReport, jobLinks: linkReport,
                     billingSync: billingReport, generatorServiceCalls: generatorReport };
    console.log("qb-time-pull", JSON.stringify(report));
    return { statusCode: 200, body: JSON.stringify(report) };
  } catch (e) {
    console.error("qb-time-pull FAILED", e);
    // Watermark is only advanced on success, so the next run re-asks the same
    // window. A failure costs a retry, not data.
    return { statusCode: 500, body: String(e?.message || e) };
  }
};

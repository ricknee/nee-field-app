// ── Generator service calls, opened by the app ─────────────────────────────
// Added 2026-08-21. Replaces the last Airtable automation that CREATED a
// record:
//
//   wfledvx1A8oVscWla  "Generator Service Call"
//
// It watched Generators for `Service Status != OK` AND `Service Call Created`
// unticked AND `Service Plan Active` = yes, created a Jobs record from the
// generator's own fields, then ticked the checkbox.
//
// Everything it read is already native. `service_status` is computed inside
// `v_generators` (OVERDUE / DUE SOON / OK off `next_service_due`), so this needs
// Airtable for exactly one thing: POSTing the job, which `_jobs.js` does.
//
// ⚠⚠ WHY THE GUARD CHANGED — see db/schema/051 for the full argument.
// The old checkbox was a PERMANENT latch: one automatic call per generator,
// ever. Six generators are overdue right now with the latch already set and
// would never be prompted again. The new guard is one call per DUE DATE
// (`service_call_due_date`), which recurs when a service is logged and does NOT
// re-fire hourly while a generator sits overdue.
//
// ⚠⚠ SHIPS INERT. `GENERATOR_SERVICE_CALLS` must be `on` before anything is
// created. Two reasons, and the second is the real one:
//   1. The first run is not a normal run. Six back-dated generators become
//      eligible at once, and every job created BURNS A PO NUMBER that cannot be
//      handed back. Look at the dry run first.
//   2. This is the second caller of the PO allocator. Everything about job
//      numbering was rebuilt on 2026-08-20 and is one week old.
// `dry` runs the whole check and reports what it WOULD create, writing nothing.
import { neonEnabled, neonQuery, neonWrite } from "./_neon.js";
import { createJobRecord } from "./_jobs.js";

// Blast-radius cap. Not a preference — a runaway here mints PO numbers and
// pollutes the job list, and both are annoying to undo. Anything skipped is
// logged by name (never silently truncated) and picked up on the next run.
const MAX_PER_RUN = 10;

// The contractor every service call is billed under. There is a real Company
// record literally named "Service Calls" — that is why every service-call PO
// reads SE?, since `Contractor Code` is LEFT(UPPER({Contractor}),2) plus the
// job's initial. Resolved by NAME at run time rather than hardcoding the rec id,
// so it survives the company being recreated.
const SERVICE_CALL_COMPANY = "Service Calls";

// A job in one of these is finished; anything else is still open work.
const CLOSED_STATUSES = ["Completed", "Not Awarded"];

export function generatorServiceCallsMode() {
  return String(process.env.GENERATOR_SERVICE_CALLS || "").trim().toLowerCase();
}

/**
 * Finds generators whose service plan has come due and opens a service-call job
 * for each. Safe to call on every scheduled run — the due-date guard is what
 * makes it idempotent, not a lock.
 *
 * @param {(path: string, options?: object) => Promise<any>} atFetch
 * @param {{ dryRun?: boolean }} [opts]
 */
export async function runGeneratorServiceCheck(atFetch, opts = {}) {
  const mode = generatorServiceCallsMode();
  const dryRun = opts.dryRun === true || mode === "dry";

  if (mode !== "on" && !dryRun) return { ok: true, enabled: false, created: 0, candidates: 0 };
  if (!neonEnabled()) return { ok: false, enabled: true, error: "DATABASE_URL unset", created: 0, candidates: 0 };

  // Everything the job needs, in one read.
  //
  // ⚠ The customer block comes from the INSTALL JOB, not the generator. The
  // Airtable automation copied the generator's own address/phone fields, which
  // were never carried into Neon's `generators` table — but the install job has
  // the same block, already native, and is where the generator got them from.
  //
  // `oc` is the job the LAST auto call created. It is joined only to report the
  // status in the dry run; the guard that matters is the due-date compare.
  let rows;
  try {
    rows = await neonQuery(
      `SELECT g.id, g.airtable_id, g.customer_name, g.brand, g.model,
              g.job_type, g.tax_status, g.billing_method,
              g.service_call_job_at_id, g.service_call_due_date,
              v.service_status, v.next_service_due, v.asset_id,
              j.name AS job_name,
              j.customer_first_name, j.customer_last_name,
              j.customer_phone, j.customer_email,
              j.address_street, j.address_city, j.address_state, j.address_zip,
              oc.status AS last_call_status
         FROM v_generators v
         JOIN generators g ON g.id = v.id
         LEFT JOIN jobs j  ON j.id = g.job_id
         LEFT JOIN jobs oc ON oc.airtable_id = g.service_call_job_at_id
        WHERE g.service_plan_active IS TRUE
          AND v.service_status IN ('OVERDUE', 'DUE SOON')
          AND v.next_service_due IS DISTINCT FROM g.service_call_due_date
        ORDER BY v.next_service_due`);
  } catch (e) {
    console.error(`generatorServiceCheck: query failed — ${e?.message || e}`);
    return { ok: false, enabled: true, error: String(e?.message || e), created: 0, candidates: 0 };
  }

  if (!rows.length) return { ok: true, enabled: true, dryRun, candidates: 0, created: 0, jobs: [] };

  // The contractor is looked up once. If it is missing, stop — creating these
  // jobs under the wrong company would put them in the wrong PO series and
  // misfile them for billing, which is worse than not creating them at all.
  const [company] = await neonQuery(
    `SELECT airtable_id, name FROM companies WHERE name = $1 LIMIT 1`, [SERVICE_CALL_COMPANY]);
  if (!company?.airtable_id) {
    console.error(`generatorServiceCheck: no Company named "${SERVICE_CALL_COMPANY}" — refusing to create service calls`);
    return { ok: false, enabled: true, error: `Company "${SERVICE_CALL_COMPANY}" not found`,
             candidates: rows.length, created: 0 };
  }

  const batch = rows.slice(0, MAX_PER_RUN);
  const deferred = rows.slice(MAX_PER_RUN);
  if (deferred.length) {
    console.log(`generatorServiceCheck: capped at ${MAX_PER_RUN}, deferring to the next run — ` +
      deferred.map((r) => r.asset_id || r.airtable_id).join(", "));
  }

  const jobs = [];
  for (const g of batch) {
    // Named after the install job, exactly as the automation did — the office
    // recognises "Tim Yoder", not "Tim Yoder generator service 2026". The PO
    // suffix is what tells the two apart in the job list.
    //
    // ⚠ Falls back to the generator's customer name so a generator whose install
    // job somehow went missing still gets a usable name rather than an empty one
    // that would fail the create.
    const jobName = String(g.job_name || g.customer_name || "").trim();
    if (!jobName) {
      console.error(`generatorServiceCheck: generator ${g.airtable_id} has no job or customer name — skipped`);
      continue;
    }

    const plan = {
      generatorId: g.id,
      generatorAtId: g.airtable_id,
      asset: g.asset_id,
      serviceStatus: g.service_status,
      nextServiceDue: g.next_service_due,
      lastCallJob: g.service_call_job_at_id,
      lastCallStatus: g.last_call_status,
      lastCallOpen: g.service_call_job_at_id != null && !CLOSED_STATUSES.includes(g.last_call_status),
      jobName
    };

    if (dryRun) { jobs.push({ ...plan, wouldCreate: true }); continue; }

    try {
      const { record, poNumber, po } = await createJobRecord(atFetch, {
        jobName,
        // The generator carries its own template values — Job Type "Service
        // Calls", Tax Status, Billing Method "Direct Customer" — and the
        // automation copied all three onto the job. One generator has them null;
        // the defaults below are the values the other eleven all hold.
        jobType:        g.job_type || "Service Calls",
        taxStatus:      g.tax_status || "Taxable",
        billingMethod:  g.billing_method || "Direct Customer",
        contractorId:   company.airtable_id,
        contractorName: company.name,
        customerFirstName: g.customer_first_name,
        customerLastName:  g.customer_last_name,
        customerPhone:     g.customer_phone,
        customerEmail:     g.customer_email,
        customerStreet:    g.address_street,
        customerCity:      g.address_city,
        customerState:     g.address_state,
        customerZip:       g.address_zip,
        notes: `Auto-opened service call — ${g.service_status} as of ${g.next_service_due}. ` +
               `Generator: ${g.asset_id || [g.brand, g.model].filter(Boolean).join(" ") || g.airtable_id}.`
      });

      // ⚠ The due date is stamped in the SAME step as the job id. If this write
      // is lost, the next run sees an unchanged due date and creates a SECOND
      // job for the same visit — the duplicate this whole design exists to
      // prevent. It is therefore NOT fail-soft: a failure is logged loudly and
      // the generator is reported so somebody looks.
      await neonWrite("generator.serviceCallCreated",
        `UPDATE generators
            SET service_call_created    = true,
                service_call_job_at_id  = $2,
                service_call_due_date    = $3,
                service_call_created_at = now(),
                synced_at               = now()
          WHERE id = $1`, [g.id, record.id, g.next_service_due]);

      jobs.push({ ...plan, created: true, jobAtId: record.id, po: po || null, poNumber: poNumber ?? null });
      console.log(`generatorServiceCheck: opened ${po || record.id} for ${g.asset_id || g.airtable_id}`);
    } catch (e) {
      console.error(`generatorServiceCheck: FAILED for generator ${g.airtable_id} — ${e?.message || e}`);
      jobs.push({ ...plan, created: false, error: String(e?.message || e) });
    }
  }

  return {
    ok: true,
    enabled: true,
    dryRun,
    candidates: rows.length,
    deferred: deferred.length,
    created: jobs.filter((j) => j.created === true).length,
    jobs
  };
}

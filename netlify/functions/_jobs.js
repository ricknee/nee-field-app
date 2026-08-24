// ── Creating a job: the ONE implementation ─────────────────────────────────
// Extracted from `handleCreateJob` in airtable.js on 2026-08-21, unchanged in
// behaviour, because a SECOND caller appeared: the generator service-call check
// (`_generator-service.js`, replacing Airtable automation wfledvx1A8oVscWla).
//
// ⚠⚠ THE REASON THIS IS A MODULE AND NOT A COPY IS THE PO ALLOCATOR.
// PO numbers moved off Airtable on 2026-08-20 and the counter — not the jobs
// table — is the authority (see the long note at `allocatePoNumber`). A second
// place that allocates is exactly how two jobs end up sharing a PO, and a
// duplicate PO silently un-costs BOTH jobs' hours: QuickBooks Time jobcodes key
// on the PO string, so the hours land against whichever job the lookup happens
// to find first and the other job shows labour it never paid for. That failure
// is invisible until someone questions a GP number weeks later.
//
// So: one allocator, one create, one Neon insert. Callers differ only in what
// they put in the `input` object.
//
// ORDERING (unchanged from the original, and load-bearing):
//   1. allocate the PO from Neon
//   2. POST to Airtable WITH the number already set
//   3. re-read the record for the computed `Job PO` / `Job PO - Locked`
//   4. insert into Neon
// Step 2 is what let the old Airtable automation stand down on its own: its
// condition was "status = New Lead AND Job PO Number is EMPTY", which a record
// created with the number already in it never satisfies.
import { neonWrite } from "./_neon.js";

const AT_API = "https://api.airtable.com/v0";

// `Jobs` is the table NAME, not an id, matching TABLES.jobs in airtable.js.
export const JOBS_TABLE = "Jobs";

// ── IS THIS A PLAUSIBLE JOB HANDLE? (cutover slice 6, 2026-08-24) ──────────
// Replaces the `String(jobId).startsWith("rec")` guards that 400'd on a uuid.
// A job created by the app is Neon-native and has no rec id, so those guards
// would have refused a job the app itself had just created — its expenses, its
// photos, its panel schedules, all "Invalid jobId".
//
// ⚠ Deliberately a SUPERSET: anything starting with "rec" still passes exactly
// as before, so no rec id that works today can begin to fail. The uuid branch is
// the only new acceptance. Same contract as isEmployeeHandle (slice 5).
//
// ⚠⚠ This is ONLY for guards — for deciding whether an id is well-formed enough
// to act on. It is NOT for the sites that narrow a value to a rec id in order to
// fill an Airtable LINKED-RECORD field. Those must keep the bare
// `startsWith("rec")` test, because a native job genuinely has nothing to put
// there and `typecast: true` would CREATE a junk Jobs record from a uuid.
export function isJobHandle(v) {
  const s = String(v ?? "").trim();
  if (s.startsWith("rec")) return true;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);
}

// ⚠ Do NOT fix the spellings ("Milla Construcion", "Kalmback"). They are the
// configured option names in Airtable's `Contractor (Intake)` singleSelect, and
// correcting one here just makes it fail the match.
//
// That field is a singleSelect and this module posts with typecast OFF on
// purpose, so an unknown value does not fail that one field — it 422s the WHOLE
// create. Hence the whitelist: an unrecognised name writes the linked
// `Contractor` only, which is the real data anyway.
//
// ⚠ Code can never add an option here. A new contractor's breadcrumb stays
// blank until someone adds the option in Airtable — or the field is deleted,
// which is the noted cleanup.
export const CONTRACTOR_INTAKE_OPTS = [
  "3DEE Construction", "Administration", "Aviary Poultry", "Case Farms",
  "Classical Construction", "Deerfield Construction", "Double LL Construction",
  "Gerber Poultry", "Granite Ridge Poultry", "Hardchuck Construction",
  "Hardwood Solutions", "Heartwood Construction", "Hosterman Development Inc",
  "J.J.O Construction", "JC Herbert", "Justin Biery", "Kalmback Feeds",
  "KDC Properties", "Koehn Konstruction", "Linden Ave Developers",
  "LK Construction", "Marco Construction", "Metis Construction",
  "Milla Construcion", "Miller Poultry", "Misc Jobs", "P&H Builders",
  "Penntex Ventures", "Service Calls", "Shop", "Ware Construction"
];

// Thrown for input the caller got wrong, so a handler can answer 400 instead of
// letting it surface as a 500. Anything else (Airtable down, network) throws a
// plain Error and stays a 500, which is correct — those are not the caller's fault.
export class JobInputError extends Error {
  constructor(message) { super(message); this.name = "JobInputError"; }
}

// A standalone Airtable fetcher for callers that are not airtable.js — the
// hourly scheduled function has no `atFetch` of its own. Deliberately the same
// shape (path, options) so either can be injected interchangeably.
export function makeAtFetch(apiKey, baseId) {
  if (!apiKey || !baseId) throw new Error("makeAtFetch: apiKey and baseId are required");
  return async function atFetch(path, options = {}) {
    const res = await fetch(`${AT_API}/${baseId}/${path}`, {
      ...options,
      headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json", ...(options.headers || {}) }
    });
    const text = await res.text();
    let json = {};
    try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
    if (!res.ok) throw new Error(json?.error?.message || `Airtable error ${res.status}`);
    return json;
  };
}

const nz = (v) => { const s = String(v ?? "").trim(); return s || null; };

// ── PO NUMBER (audit item 05, db/schema/039) ───────────────────────────────
// One statement, so two people creating a job at the same instant cannot both
// read the same value — the flaw Airtable's read-then-write has always had and
// got away with because one person creates jobs at a time. A brand-new year
// starts at 100, matching the 2025/2027 counter rows.
//
// ⚠ DO NOT derive the next number from the jobs table. 112 jobs run 102→436 and
// 22 sit ABOVE the counter, because Dollar General jobs carry the general
// contractor's own numbering. max(po)+1 would jump to 437 and abandon 150
// unused numbers. The counter is the authority; the jobs table is not.
//
// Returns null rather than throwing: a failed allocation must not block someone
// creating a job. With no number, Airtable's "PO is empty" condition is
// satisfied and it assigns one, exactly as it did before the cutover.
// ── WHERE IS A JOB BORN? (cutover slice 6) ────────────────────────────────
//   unset / "airtable"  Airtable creates the job AND assigns the PO number.
//   "neon"              Airtable still creates the job; Neon assigns the PO.
//                       ← production today.
//   "native"            The job is BORN IN NEON. Airtable gets a fail-soft
//                       mirror and never sees the id again.
//
// Ships INERT: the code below only takes the native path on "native", which is
// not set anywhere, so this deploy changes nothing. Same pattern as
// ALLOCATIONS_WRITE, TIME_CLOCK_PAYROLL and LOGIN_SOURCE — and the reason is
// stronger here than for any of them: **a PO number cannot be handed back.** A
// bad native create burns one, permanently, on every attempt.
export function jobCreateSource() {
  return String(process.env.JOB_CREATE_SOURCE || "").toLowerCase();
}
export function jobsAreNative() { return jobCreateSource() === "native"; }

// Every one of the 116 jobs in the table carries markup_pct = 0.1000, without
// exception (checked 2026-08-24). In Airtable the value arrives from a FIELD
// DEFAULT on `Job Markup %`, which is why this code never sent it — and why a
// native job, having no Airtable field to default from, must send it explicitly.
//
// ⚠⚠ IT IS NOT COSMETIC AND IT DOES NOT SELF-CORRECT. `unbilled_material_amount_calc`
// multiplies by COALESCE(j.markup_pct, 0), and `createMaterialAllocation`
// SNAPSHOTS that figure into material_billing_allocations.allocated_amount. A
// job created with a NULL markup bills its material at COST, and every
// allocation written before anyone notices stays wrong — the fix does not
// recompute rows already written. That is the a04b11f bug exactly.
const DEFAULT_MARKUP_PCT = 0.10;

async function allocatePoNumber() {
  const src = jobCreateSource();
  if (src !== "neon" && src !== "native") return null;
  try {
    const rows = await neonWrite("job.allocatePo",
      `INSERT INTO job_po_counters (year, last_used) VALUES ($1, 100)
       ON CONFLICT (year) DO UPDATE SET last_used = job_po_counters.last_used + 1,
                                        synced_at = now()
       RETURNING last_used`, [new Date().getFullYear()]);
    return rows?.[0]?.last_used ?? null;
  } catch (e) {
    console.error(`createJob: PO allocation failed, leaving it to Airtable — ${e?.message || e}`);
    return null;
  }
}

/**
 * Creates a job in Airtable and mirrors it into Neon in the same request.
 *
 * @param {(path: string, options?: object) => Promise<any>} atFetch
 * @param {object} input  jobName + contractorId are required; everything else optional.
 * @returns {Promise<{ record: object, poNumber: number|null, po: string|null, poLocked: string|null }>}
 */
// ── THE NATIVE CREATE (cutover slice 6, 2026-08-24) ───────────────────────
// The job is INSERTed into Neon first and Airtable gets a fail-soft mirror.
//
// ⚠⚠ THE REC ID IS NOT STAMPED BACK, and here that is not a preference — it is
// what keeps `_jobs-sync.js` harmless. That sync runs HOURLY and does
// `INSERT … ON CONFLICT (airtable_id) DO UPDATE SET <all 38 columns>`. A native
// job has `airtable_id` NULL, conflicts with nothing, and is invisible to it.
// Stamp the rec id back and the same job becomes a conflict target, so every
// hour Airtable's copy would overwrite status, addresses, markup, contacts and
// PO fields — silently reverting whatever the app wrote. That is the
// `estimate_templates` trap at the scale of the whole jobs table.
//
// ⚠⚠ …AND THAT WAS AN ANSWER ABOUT THE WRONG ROW. Correct as far as it goes:
// the NATIVE row is invisible to the sync. The MIRROR is not. It is a real
// Airtable record with its own rec id that no Neon row carries, so the sync
// took the INSERT branch and the mirror came back an hour later as a SECOND
// job — same name, same PO number, frozen at "New Lead", no PO string, no
// pCloud folders, and listed right next to the real one. Caught on the first
// native job ever created, three hours after the flip (schema 062).
//
// So the mirror's id IS recorded — in `jobs.airtable_mirror_id`, which is a
// skip list for the sync and NOTHING else. Never resolve or emit through it.
//
// ⚠ THE RULE THIS COST US: "not stamping the id back" is not the same claim as
// "the sync cannot see this job." Not stamping protects the row we wrote; it
// says nothing about the row we caused to exist somewhere else. Any time this
// app creates a record in a system that an ETL reads back wholesale, ask what
// that ETL does with it — the answer here was "inserts it as new."
//
// ⚠⚠ TWO AIRTABLE FORMULAS ARE REPRODUCED HERE, because a native job has no
// Airtable record to read them back from. Both were READ OUT OF THE BASE via the
// meta API rather than inferred from the data — the Generator Asset ID lesson,
// where a plausible guess put the wrong string on all 11 generators:
//
//   Contractor Code = CONCATENATE(LEFT(UPPER({Contractor Name}), 2),
//                                 LEFT(UPPER({Job Name}), 1))
//   Job PO          = {Job Name} & " (" & {Contractor Code} & " " & {Job PO Number} & ")"
//
// ⚠ `po_locked` is a singleLineText that something else fills in Airtable, NOT a
// formula — and it is already NULL on 24 of 116 jobs, including recent ones, so
// nothing fills it reliably. It matters because `qb-time-pull` matches a
// timesheet's jobcode against `jobs.po_locked`: NULL there means QuickBooks
// hours can never attach to the job. A native job therefore seeds it equal to
// `po`, which is what it holds on the 90 jobs where it is set at all. It stays a
// snapshot — 26 jobs have `po_locked` deliberately diverged from `po` after a
// rename, and nothing here recomputes it later.
async function createJobNative(a) {
  const {
    atFetch, fields, poNumber, trimmedName, jobType, taxStatus, billing,
    trimmedContractorId, contractorName, generatorInstalled, notes,
    customerFirstName, customerLastName, customerPhone, customerEmail,
    customerStreet, customerCity, customerState, customerZip,
  } = a;

  // ⚠ FAIL CLOSED. A job with no PO number is not a job: the PO is its identity
  // on invoices, in pCloud folder names, on Trello cards and in QuickBooks Time.
  // Better to refuse than to create one that can never be matched or billed.
  if (poNumber == null) {
    throw new JobInputError("Could not assign a PO number, so the job was not created. Please try again.");
  }

  const contractorCode =
    String(contractorName || "").toUpperCase().slice(0, 2) +
    String(trimmedName).toUpperCase().slice(0, 1);
  const poString = trimmedName + " (" + contractorCode + " " + poNumber + ")";
  const poLocked = poString;

  const addressFull = [
    [nz(customerStreet), nz(customerCity)].filter(Boolean).join(", "),
    [nz(customerState) ? nz(customerState).toUpperCase() : null, nz(customerZip)]
      .filter(Boolean).join(" ")
  ].filter(Boolean).join(", ") || null;

  // No ON CONFLICT: a native row has no natural key to conflict on, and every
  // call has already burned a fresh PO number, so a retry is a different job.
  const rows = await neonWrite("job.createNative",
    `INSERT INTO jobs (name, status, job_type, tax_status, billing_method,
                       contractor_at_id, contractor_name, contractor_code,
                       po_number, po, po_locked, job_year,
                       customer_first_name, customer_last_name, customer_phone,
                       customer_email, address_street, address_city, address_state,
                       address_zip, address_full, notes, generator_installed,
                       markup_pct, synced_at)
     VALUES ($1::text,$2::text,$3::text,$4::text,$5::text,$6::text,$7::text,$8::text,
             $9::int,$10::text,$11::text,$12::int,
             $13::text,$14::text,$15::text,$16::text,$17::text,$18::text,$19::text,
             $20::text,$21::text,$22::text,$23::boolean,$24::numeric, now())
     RETURNING id`,
    [trimmedName, "New Lead", jobType ? String(jobType).trim() : null,
     taxStatus || "Taxable", billing, trimmedContractorId,
     contractorName ? String(contractorName).trim() : null, contractorCode,
     poNumber, poString, poLocked, new Date().getFullYear(),
     nz(customerFirstName), nz(customerLastName), nz(customerPhone), nz(customerEmail),
     nz(customerStreet), nz(customerCity),
     nz(customerState) ? nz(customerState).toUpperCase() : null,
     nz(customerZip), addressFull, nz(notes), generatorInstalled === true,
     DEFAULT_MARKUP_PCT]);

  const neonId = rows?.[0]?.id ? String(rows[0].id) : null;
  if (!neonId) throw new Error("job.createNative: no id returned");

  // The mirror. Best-effort — the job already exists and is fully usable without
  // it. Kept because everything downstream of a job (the contractor link, the
  // Estimating-time pCloud folder) still triggers off the Airtable record.
  //
  // ⚠ The linked-record fields (Contractor, Billing Company, Primary Contact)
  // still carry rec ids and still work: companies and contacts keep minting them
  // until their own slices. If they ever go native, these must be DROPPED from
  // the mirror rather than sent as uuids — `typecast: true` would CREATE a junk
  // Company from one, exactly as it would have on Submitted By in slice 5.
  fields["Job PO Number"] = poNumber;
  await recordMirrorId(neonId, await mirrorJobToAirtable(atFetch, fields));

  // Airtable-shaped so callers (handleCreateJob → mapJob, _generator-service)
  // need no changes. The id is the Neon uuid, which is the handle everything
  // downstream now speaks.
  return {
    record: { id: neonId, fields: { ...fields, "Job PO": poString, "Job PO - Locked": poLocked } },
    poNumber, po: poString, poLocked,
  };
}

async function mirrorJobToAirtable(atFetch, fields) {
  try {
    return await atFetch(`${encodeURIComponent(JOBS_TABLE)}`, {
      method: "POST", body: JSON.stringify({ fields })
    });
  } catch (e) {
    console.error(`createJobNative: Airtable mirror failed (ignored) — ${e?.message || e}`);
    return null;
  }
}

// Hands the mirror's rec id to `_jobs-sync.js`'s skip list (schema 062). This is
// the whole of the ghost-job fix on the write side.
//
// ⚠ FAIL SOFT, LIKE THE MIRROR ITSELF. The job exists and is fully usable; a
// mirror we failed to record costs one duplicate row in the job list, not a
// broken create, and throwing here after the PO has been burned would be the
// worse trade. But it is logged with BOTH ids at error level rather than
// swallowed, because that log line is the only way anyone learns a ghost is
// coming — the duplicate appears up to an hour later, in a different system,
// with nothing to connect it back to this request.
//
// The window is real: mirror POSTed, this UPDATE lost. Reconcile by hand with
//   UPDATE jobs SET airtable_mirror_id = '<rec id from the log>' WHERE id = '<uuid>';
// then delete the duplicate row if the sync has already run.
async function recordMirrorId(neonId, mirror) {
  const mirrorId = mirror?.id ? String(mirror.id) : null;
  if (!mirrorId || !neonId) return null;
  try {
    await neonWrite("job.recordMirrorId",
      `UPDATE jobs SET airtable_mirror_id = $1 WHERE id = $2::uuid`,
      [mirrorId, String(neonId)]);
    return mirrorId;
  } catch (e) {
    console.error(
      `createJobNative: mirror ${mirrorId} NOT recorded against job ${neonId} — ` +
      `_jobs-sync will import it as a duplicate job within the hour. ${e?.message || e}`);
    return null;
  }
}

export async function createJobRecord(atFetch, input) {
  const {
    jobName, jobType, taxStatus, billingMethod, contractorId, contractorName, contactId,
    customerFirstName, customerLastName,
    customerStreet, customerCity, customerState, customerZip,
    customerPhone, customerEmail, notes, generatorInstalled
  } = input || {};

  const trimmedName = String(jobName || "").trim();
  if (!trimmedName) throw new JobInputError("Job Name is required.");

  const trimmedContractorId = String(contractorId || "").trim();
  if (!trimmedContractorId) throw new JobInputError("Contractor is required.");

  // Billing Method defaults to "Contractor" — every job created from the New
  // Project form is contractor-billed. The generator service check overrides it
  // with the generator's own value ("Direct Customer"), which is why this is a
  // parameter at all.
  const billing = String(billingMethod || "").trim() || "Contractor";

  const fields = {};
  fields["Job Name"]       = trimmedName;
  fields["Job Status"]     = "New Lead";
  fields["Tax Status"]     = taxStatus || "Taxable";
  fields["Billing Method"] = billing;

  if (jobType && String(jobType).trim()) fields["Job Type"] = String(jobType).trim();

  // Contractor + Billing Company default to the same Company on create.
  fields["Contractor"]      = [trimmedContractorId];
  fields["Billing Company"] = [trimmedContractorId];

  // Keep the legacy text breadcrumb populated for downstream readers — but only
  // when the name is a configured option. See CONTRACTOR_INTAKE_OPTS above for
  // why omitting is the safe fallback.
  const intakeName = String(contractorName || "").trim();
  if (intakeName) {
    if (CONTRACTOR_INTAKE_OPTS.includes(intakeName)) {
      fields["Contractor (Intake)"] = intakeName;
    } else {
      console.log(`createJob: "${intakeName}" is not a Contractor (Intake) option — writing the linked Contractor only.`);
    }
  }

  const trimmedContactId = String(contactId || "").trim();
  if (trimmedContactId) fields["Primary Contact"] = [trimmedContactId];

  // ⚠ THE FLAG THAT REVEALS THE GENERATOR TAB. Not cosmetic: `index.html` gates
  // the whole Generator panel on `job.generatorInstalled`, so a job without it
  // shows no generator no matter what the data says. The service-call check sets
  // it, because a work order for a generator service that cannot display the
  // generator is a work order with no serial number on it. Only ever set TRUE
  // here — clearing it is a deliberate act with a confirm prompt behind it.
  if (generatorInstalled === true) fields["Generator Installed"] = true;

  if (nz(customerFirstName)) fields["Customer 1st Name (Intake)"]       = nz(customerFirstName);
  if (nz(customerLastName )) fields["Customer Last Name (Intake)"]      = nz(customerLastName );
  if (nz(customerStreet   )) fields["Job Site Street Address (Intake)"] = nz(customerStreet   );
  if (nz(customerCity     )) fields["Job Site City (Intake)"]           = nz(customerCity     );
  if (nz(customerState    )) fields["Job Site State (Intake)"]          = nz(customerState).toUpperCase();
  if (nz(customerZip      )) fields["Job Site Zip Code (Intake)"]       = nz(customerZip      );
  if (nz(customerPhone    )) fields["Customer Phone (Intake)"]          = nz(customerPhone    );
  if (nz(customerEmail    )) fields["Customer Email (Intake)"]          = nz(customerEmail    );
  if (nz(notes            )) fields["Notes"]                            = String(notes);

  const poNumber = await allocatePoNumber();
  if (poNumber != null) fields["Job PO Number"] = poNumber;

  // ═══ NATIVE PATH — the job is born in Neon (cutover slice 6) ═════════════
  if (jobsAreNative()) {
    return await createJobNative({
      atFetch, fields, poNumber, trimmedName, jobType, taxStatus, billing,
      trimmedContractorId, contractorName, generatorInstalled, notes,
      customerFirstName, customerLastName, customerPhone, customerEmail,
      customerStreet, customerCity, customerState, customerZip,
    });
  }

  const record = await atFetch(`${encodeURIComponent(JOBS_TABLE)}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });

  // ⚠ RE-READ, don't trust the create response, for `Job PO`.
  // It is an Airtable FORMULA (Contractor Code + job initial + Job PO Number),
  // and the same trap the inventory push hit at Step E applies: a record read
  // back before its computed fields settle hands you a blank. Writing that blank
  // into Neon would leave the job list showing NO PO for an hour — which is the
  // exact lag this slice exists to remove, just moved from one column to another.
  //
  // One extra round trip on a handful of job creations a week. Fails soft: worst
  // case `po` stays null and the hourly sync fills it, i.e. today's behaviour.
  // ⚠⚠ `Job Markup %` COMES BACK ON THIS SAME RE-READ, and it must. It is a
  // plain percent field whose value on a new job comes from an AIRTABLE FIELD
  // DEFAULT (10%), so this code never sends it and never knew it — the column
  // was simply absent from the INSERT below and Neon held NULL until
  // `_jobs-sync.js` ran, up to an hour later.
  //
  // That hour is not cosmetic here, and this is the same lesson as the intake
  // block above, on a column that bills. `unbilled_material_amount_calc`
  // multiplies by `COALESCE(j.markup_pct, 0)`, and `createMaterialAllocation`
  // SNAPSHOTS that figure into `material_billing_allocations.allocated_amount`.
  // So an expense approved on a job less than an hour old was allocated at COST
  // — and the hourly sync fixes the job afterwards but never recomputes an
  // allocation already written. The under-billing is permanent for that row.
  // Found 2026-08-24 on Test 2 (MIT 298): Airtable said 83.60, Neon said 76.00.
  //
  // Costs nothing — the round trip already exists for the PO formulas.
  let poString = null, poLocked = null, markupPct = null;
  try {
    const fresh = await atFetch(`${encodeURIComponent(JOBS_TABLE)}/${record.id}`);
    poString = fresh?.fields?.["Job PO"] || null;
    poLocked = fresh?.fields?.["Job PO - Locked"] || null;
    const mk = fresh?.fields?.["Job Markup %"];
    markupPct = (mk === undefined || mk === null || mk === "") ? null : Number(mk);
    if (markupPct !== null && !Number.isFinite(markupPct)) markupPct = null;
  } catch (e) {
    console.error(`createJob: PO re-read failed, hourly sync will fill it — ${e?.message || e}`);
  }

  // ── The job lands in Neon NOW, not up to an hour from now ────────────────
  // `_jobs-sync.js` runs hourly, which is why a new job has shown an empty Time
  // Entries tab for its first hour. Airtable is still created first because
  // `jobs.airtable_id` is NOT NULL and every client-side job id is the rec id —
  // a Neon-first job would have no id the app could use.
  //
  // Fails SOFT: the job exists in Airtable and the hourly sync will adopt it,
  // so a Neon hiccup costs the old one-hour lag rather than the job itself.
  //
  // ⚠ THE INTAKE BLOCK CARRIES TOO, and originally did not. The job appeared in
  // Neon immediately but with no customer name, phone, email or address, because
  // the app reads jobs Neon-FIRST — so "the one-hour lag is gone" was only true
  // of the job's existence, not its details. Found on the first real job after
  // the flip (Craig Davidson (Garage), MIC 287, 2026-08-20): Airtable had the
  // whole customer block, Neon had nulls until the sync caught up.
  //
  // `address_full` is composed here because Airtable's is a FORMULA field this
  // code cannot write. Empty parts are dropped rather than left as stray commas,
  // and the hourly sync overwrites it with Airtable's own rendering — so a
  // format difference costs an hour of cosmetics, never a blank address.
  const addressFull = [
    [nz(customerStreet), nz(customerCity)].filter(Boolean).join(", "),
    [nz(customerState) ? nz(customerState).toUpperCase() : null, nz(customerZip)]
      .filter(Boolean).join(" ")
  ].filter(Boolean).join(", ") || null;

  try {
    await neonWrite("job.create",
      `INSERT INTO jobs (airtable_id, name, status, job_type, tax_status, billing_method,
                         contractor_at_id, contractor_name, po_number, po, po_locked,
                         job_year, customer_first_name, customer_last_name, customer_phone,
                         customer_email, address_street, address_city, address_state,
                         address_zip, address_full, notes, generator_installed, markup_pct, synced_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24::numeric, now())
       ON CONFLICT (airtable_id) DO UPDATE SET
         name=EXCLUDED.name, status=EXCLUDED.status, job_type=EXCLUDED.job_type,
         tax_status=EXCLUDED.tax_status, billing_method=EXCLUDED.billing_method,
         contractor_at_id=EXCLUDED.contractor_at_id, contractor_name=EXCLUDED.contractor_name,
         po_number=COALESCE(EXCLUDED.po_number, jobs.po_number),
         -- COALESCE so a failed re-read never BLANKS a PO the sync already had.
         po=COALESCE(EXCLUDED.po, jobs.po), po_locked=COALESCE(EXCLUDED.po_locked, jobs.po_locked),
         -- Same reasoning for the intake block: a retry that omits them must
         -- never blank details the sync has already carried over.
         customer_first_name=COALESCE(EXCLUDED.customer_first_name, jobs.customer_first_name),
         customer_last_name =COALESCE(EXCLUDED.customer_last_name,  jobs.customer_last_name),
         customer_phone     =COALESCE(EXCLUDED.customer_phone,      jobs.customer_phone),
         customer_email     =COALESCE(EXCLUDED.customer_email,      jobs.customer_email),
         address_street     =COALESCE(EXCLUDED.address_street,      jobs.address_street),
         address_city       =COALESCE(EXCLUDED.address_city,        jobs.address_city),
         address_state      =COALESCE(EXCLUDED.address_state,       jobs.address_state),
         address_zip        =COALESCE(EXCLUDED.address_zip,         jobs.address_zip),
         address_full       =COALESCE(EXCLUDED.address_full,        jobs.address_full),
         notes              =COALESCE(EXCLUDED.notes,               jobs.notes),
         -- OR, never overwrite: a retry that omits the flag must not hide a
         -- Generator tab somebody has already been using.
         generator_installed = jobs.generator_installed OR EXCLUDED.generator_installed,
         -- COALESCE like the PO: a retry whose re-read failed must never blank a
         -- markup the sync already carried, or the next allocation bills at cost.
         markup_pct=COALESCE(EXCLUDED.markup_pct, jobs.markup_pct),
         synced_at=now()`,
      [record.id, trimmedName, "New Lead", jobType ? String(jobType).trim() : null,
       taxStatus || "Taxable", billing, trimmedContractorId,
       contractorName ? String(contractorName).trim() : null,
       poNumber, poString, poLocked, new Date().getFullYear(),
       nz(customerFirstName), nz(customerLastName), nz(customerPhone), nz(customerEmail),
       nz(customerStreet), nz(customerCity),
       nz(customerState) ? nz(customerState).toUpperCase() : null,
       nz(customerZip), addressFull, nz(notes), generatorInstalled === true, markupPct]);
  } catch (e) {
    console.error(`createJob: Neon insert failed, hourly sync will adopt it — ${e?.message || e}`);
  }

  return { record, poNumber, po: poString, poLocked };
}

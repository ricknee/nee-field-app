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
async function allocatePoNumber() {
  if (String(process.env.JOB_CREATE_SOURCE || "").toLowerCase() !== "neon") return null;
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
export async function createJobRecord(atFetch, input) {
  const {
    jobName, jobType, taxStatus, billingMethod, contractorId, contractorName, contactId,
    customerFirstName, customerLastName,
    customerStreet, customerCity, customerState, customerZip,
    customerPhone, customerEmail, notes
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
  let poString = null, poLocked = null;
  try {
    const fresh = await atFetch(`${encodeURIComponent(JOBS_TABLE)}/${record.id}`);
    poString = fresh?.fields?.["Job PO"] || null;
    poLocked = fresh?.fields?.["Job PO - Locked"] || null;
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
                         address_zip, address_full, notes, synced_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22, now())
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
         synced_at=now()`,
      [record.id, trimmedName, "New Lead", jobType ? String(jobType).trim() : null,
       taxStatus || "Taxable", billing, trimmedContractorId,
       contractorName ? String(contractorName).trim() : null,
       poNumber, poString, poLocked, new Date().getFullYear(),
       nz(customerFirstName), nz(customerLastName), nz(customerPhone), nz(customerEmail),
       nz(customerStreet), nz(customerCity),
       nz(customerState) ? nz(customerState).toUpperCase() : null,
       nz(customerZip), addressFull, nz(notes)]);
  } catch (e) {
    console.error(`createJob: Neon insert failed, hourly sync will adopt it — ${e?.message || e}`);
  }

  return { record, poNumber, po: poString, poLocked };
}

// netlify/functions/inventory.js
// NEE Inventory App v2 — Neon-backed, with one foot still in the main Airtable base
//
// Env vars: DATABASE_URL, AUTH_SECRET, and AIRTABLE_API_KEY + AIRTABLE_BASE_ID
// for the MAIN base only.
//
// ⚠ INVENTORY_BASE_ID is no longer read. As of the write cutover (2026-08-12)
// this file does not touch the Airtable INVENTORY base at all — not a read, not
// a write, not a loader. Everything it once held lives in Neon. The only
// Airtable calls left go to the MAIN base's `Expenses`, which is the expense
// push feeding GP and was never part of this migration.
// The variable can be deleted from Netlify once the base itself is archived.
import { signToken, authedUser, hasRole } from "./_auth.js";
import { isSessionRevoked } from "./_revocation.js";
import { shadowLoginCheck, neonLoginCandidate, loginSource, neonEmployees } from "./_employees.js";
// Both fail-soft by contract: neonExec for the last-login stamp, neonQuery for
// the main-base job reads (Step B0). The driver is lazy-imported so the offline
// test suites stay install-free.
import { neonExec, neonQuery, neonWrite } from "./_neon.js";
// Step E. The materials push writes Expenses into the MAIN base, and the field
// app has read expenses from Neon since Step 4d — so an Airtable-only write is
// invisible over there. Shared with airtable.js; see _expenses.js for why this
// caller fails closed and that one doesn't.
import { syncExpenseToNeon } from "./_expenses.js";
import { randomUUID } from "node:crypto";
// Archiving the generated materials PDF into the same R2 bucket the field app's
// jobsite photos use. Optional infrastructure — fails soft, never in ensureEnv.
// See docs/PLAN-expense-receipts.md §11.
import { r2Enabled, jobDocsPrefix, presignPut, R2Error } from "./_r2.js";

const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const MAIN_BASE_ID     = process.env.AIRTABLE_BASE_ID;
const API_ROOT_MAIN    = `https://api.airtable.com/v0/${MAIN_BASE_ID}`;

function resp(code, body) {
  return {
    statusCode: code,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function ensureEnv() {
  if (!AIRTABLE_API_KEY) throw new Error("Missing AIRTABLE_API_KEY");
  if (!MAIN_BASE_ID)     throw new Error("Missing AIRTABLE_BASE_ID");
  if (!process.env.AUTH_SECRET) throw new Error("Missing AUTH_SECRET");
}

// ── Authorization policy (see _auth.js) ──────────────────────────────────────
// Reads: any signed-in role. Writes: any signed-in non-viewer, EXCEPT the
// money/catalog/destructive ops, which require admin. A shared AUTH_SECRET means
// tokens from the field app validate here too, so all four roles can appear.
const _ADMIN_INV   = ["admin"];
const _NON_VIEWER  = ["admin", "office", "employee"];
const _ADMIN_WRITES = new Set([
  "pushExpenses",        // pushes material cost into job Expenses (money)
  "jobDocUploadUrl",     // archives the materials PDF — same tier as the push it documents
  "updateItemCost", "createItem", "itemUpdate", "itemDelete",   // catalog / pricing
  "locationSave", "vendorSave", "vendorPricingSave", "vendorPricingDelete", // reference data
  "syncItemCostToVendor",
  "delete",              // transaction deletion
  "orderDelete", "estimateDelete", "estimateTemplateDelete", // destructive
]);

function authzFor(method, action) {
  if (method === "GET") return null;                 // any signed-in role may read
  return _ADMIN_WRITES.has(action) ? _ADMIN_INV : _NON_VIEWER; // viewer blocked on writes
}

// Parse the POST body's action without throwing on malformed JSON.
function safeBodyAction(event) {
  try { return event.body ? JSON.parse(event.body).action : undefined; }
  catch { return undefined; }
}

function normalize(v) { return String(v || "").trim().toLowerCase(); }

function gBool(fields, name) {
  const v = fields[name];
  if (typeof v === "boolean") return v;
  if (typeof v === "number")  return v !== 0;
  if (typeof v === "string")  return ["true","yes","1"].includes(v.trim().toLowerCase());
  return false;
}

async function atFetch(root, path, options = {}) {
  const res = await fetch(`${root}/${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_API_KEY}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const text = await res.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  if (!res.ok) throw new Error(json?.error?.message || `Airtable error ${res.status}`);
  return json;
}

async function fetchAll(root, table, opts = {}) {
  const params = new URLSearchParams();
  if (opts.filter)     params.set("filterByFormula", opts.filter);
  if (opts.sortField)  params.set("sort[0][field]", opts.sortField);
  if (opts.sortDir)    params.set("sort[0][direction]", opts.sortDir);
  if (opts.maxRecords) params.set("maxRecords", String(opts.maxRecords));

  const records = [];
  let offset = null;
  do {
    const qs = new URLSearchParams(params);
    if (offset) qs.set("offset", offset);
    const data = await atFetch(root, `${encodeURIComponent(table)}?${qs}`, { method: "GET" });
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return records;
}

// ── LOGIN ──────────────────────────────────────────────
// The People screen shows "Last login", and it has to mean the last login to
// EITHER app. Only airtable.js stamped it at first, so anyone who lives in the
// inventory app looked dormant — which is exactly backwards, since a dormant
// account is the one you'd want to switch off.
//
// neonExec, NOT neonWrite: this is cosmetic and a login must never fail
// because Neon is unreachable. Called on both the Neon and Airtable paths, so
// it keeps working if LOGIN_SOURCE is switched back.
async function stampLastLogin(airtableId) {
  await neonExec("login.lastSeen",
    `UPDATE employees SET last_login_at = now() WHERE airtable_id = $1`, [airtableId]);
}

async function handleLogin(body) {
  const { identifier, pin } = body || {};
  if (!identifier || !pin) return resp(400, { ok: false, error: "Missing name or PIN." });

  // Stage 3, gated by LOGIN_SOURCE — see _employees.js and airtable.js's
  // handleLogin, which this mirrors exactly. It has to: one token validates in
  // BOTH apps, so if these two disagreed about who a login belongs to, a
  // session minted here would carry the wrong identity over there.
  if (loginSource() === "neon") {
    const r = await neonLoginCandidate(identifier, pin);
    if (r.ok) {
      if (r.ambiguous) {
        console.warn(`login[inventory]: refusing ambiguous identifier — ${r.n} employees match.`);
        return resp(401, { ok: false, error: "That name matches more than one person. Use your username." });
      }
      if (!r.user) return resp(401, { ok: false, error: "Invalid name or PIN." });
      await stampLastLogin(r.user.id);
      return resp(200, {
        ok: true, user: r.user, _source: "neon",
        token: signToken({ id: r.user.id, role: r.user.role }),
      });
    }
    console.warn("login[inventory]: Neon unavailable, falling back to Airtable.");
  }

  const records = await fetchAll(API_ROOT_MAIN, "Employees", { filter: `{Active}=1` });
  const id = normalize(identifier);

  const match = records.find(r => {
    const f = r.fields || {};
    const fullName  = normalize(f["Employee Name"] || "");
    const firstName = fullName.split(" ")[0];
    const username  = normalize(f["Username"] || "");
    const savedPin  = String(f["PIN"] || "").trim();
    return (fullName === id || firstName === id || username === id)
      && savedPin !== ""
      && savedPin === String(pin).trim();
  });

  if (!match) {
    await shadowLoginCheck("inventory", identifier, pin, null);
    return resp(401, { ok: false, error: "Invalid name or PIN." });
  }

  const f       = match.fields || {};
  // `Role` only. This used to read `Role New || Role`, which the field app
  // never did — so the same person could be a different role depending on which
  // app they opened. Worse, `Role New`'s options are employee/admin/viewer with
  // **no `office`**, so a populated `Role New` would silently demote the office
  // staff. `Role New` is empty on every record today; both apps now agree.
  const rawRole = normalize(f["Role"] || "");
  // Return the full canonical role (admin/office/viewer/employee) — both the
  // picker and the server-side authz need the real role, not a collapsed one.
  let role;
  if      (rawRole === "admin")  role = "admin";
  else if (rawRole === "office") role = "office";
  else if (rawRole === "viewer") role = "viewer";
  else                            role = "employee";
  const user = { id: match.id, name: f["Employee Name"] || "Unknown", role };
  // Stage 2 of the login flip. Shadowed here too, and separately labelled,
  // because this app's matching rule is NOT the field app's — it accepts a
  // first name and ignores email. See _employees.js.
  await shadowLoginCheck("inventory", identifier, pin, user);
  await stampLastLogin(user.id);
  return resp(200, { ok: true, user, _source: "airtable", token: signToken({ id: user.id, role: user.role }) });
}

// ── EMPLOYEES (for name picker) ────────────────────────────
async function handleEmployees() {
  // Neon-first (Stage 4 of the employees migration). Ids stay AIRTABLE rec ids —
  // they flow into the cart and onto `Job ID (Main)`-style fields, so a Neon
  // uuid here would write garbage downstream.
  const neon = await neonEmployees(true);
  if (neon) {
    return resp(200, {
      ok: true, _source: "neon",
      employees: neon.map(e => ({ id: e.id, name: e.name, role: e.role })),
    });
  }
  // null means Neon had no opinion, never "nobody works here" — an empty picker
  // would look like a working screen with no staff.
  const records = await fetchAll(API_ROOT_MAIN, "Employees", {
    filter: `{Active}=1`,
    sortField: "Employee Name",
    sortDir: "asc"
  });
  const employees = records.map(r => ({
    id:   r.id,
    name: r.fields["Employee Name"] || "",
    role: normalize(r.fields["Role"] || "")   // `Role` only — see handleLogin
  }));
  return resp(200, { ok: true, _source: "airtable", employees });
}

// ── Main-base job reads, served from Neon (Step B0) ─────────────────────────
// The handlers below used to page the main NEE base's Jobs table over the
// Airtable API. Neon's `jobs` is a complete mirror of it (112 rows, identity
// columns refreshed hourly) and carries every column they need, so they read
// Neon first and keep Airtable as the fallback.
//
// ⚠⚠ These return `airtable_id` AS `id` — never the Neon uuid. That id flows
// picker → cart → `submitCart` stamps it onto `Job ID (Main)` → the expense push
// writes it into a **linked-record field** on main-base Expenses. A uuid there
// writes garbage into an Airtable link. This is deliberate and stays this way
// until Step E moves the push itself.
//
// ⚠ Display name uses `po`, NOT `po_locked`. The PO only locks when a job is
// awarded, so `po_locked` is blank on all 13 New Lead jobs — and those are in
// the estimating picker. Using it would silently drop them, and a short list
// looks exactly like a complete one.
const JOB_STATUS_AWARDED    = ["Awarded", "Service Call Scheduled", "Ready to Invoice"];
const JOB_STATUS_ESTIMATING = ["New Lead", "Estimating", ...JOB_STATUS_AWARDED];

function mapNeonJob(r) {
  return {
    id:         r.airtable_id,
    name:       (r.po || r.name || "").trim(),
    status:     r.status || "",
    taxable:    (r.tax_status || "") === "Taxable",
    contractor: (r.contractor_name || "").trim(),
  };
}

// Returns an array on success, or null meaning "Neon had no opinion" so the
// caller falls back to Airtable. `statuses` omitted = every job.
//
// The guard is `q?.rows` — the query SUCCEEDING — not `rows.length`. Falling
// back on an empty result would be wrong twice over: an empty status set is a
// legitimate answer (nothing is "Service Call Scheduled" today), and a
// length-based guard is what serves half a list when only some rows match.
async function neonJobs(statuses) {
  const filtered = Array.isArray(statuses);
  const q = await neonQuery(
    `SELECT airtable_id, name, po, status, tax_status, contractor_name
       FROM jobs
      WHERE COALESCE(airtable_id, '') <> ''
        ${filtered ? "AND status = ANY($1::text[])" : ""}
      ORDER BY name ASC`,
    filtered ? [statuses] : []
  );
  return q?.rows ? q.rows.map(mapNeonJob) : null;
}

// Every main-base job indexed by its Airtable record id, as
// { id, taxable, display }. Used by the expense push to resolve the
// `Job ID (Main)` text each transaction carries. Neon-first, Airtable fallback;
// the shape is identical either way so the caller can't tell them apart.
async function mainJobIndex() {
  const index = {};
  const neon = await neonJobs();
  if (neon) {
    neon.forEach(j => { index[j.id] = { id: j.id, taxable: j.taxable, display: j.name }; });
    return { index, source: "neon" };
  }
  const records = await fetchAll(API_ROOT_MAIN, "Jobs", {});
  records.forEach(r => {
    const f = r.fields || {};
    index[r.id] = {
      id: r.id,
      taxable: (f["Tax Status"]?.name || f["Tax Status"] || "") === "Taxable",
      display: (f["Job PO"] || f["Job Name"] || "").trim(),
    };
  });
  return { index, source: "airtable" };
}

// ── JOBS (the "log materials USED" cart picker) ──────────────
// Reads the AWARDED set from the main NEE base (Awarded + Service Call
// Scheduled + Ready to Invoice) — same filter/shape as handleAwardedJobs —
// and returns main-base record IDs. submitCart stamps that id onto the
// Inventory Transaction as "Job ID (Main)" text so the expense push can group
// by a stable main-base id instead of name-matching the synced Jobs mirror
// (Drop-Jobs-mirror bet, Step B). Kept separate from handleAwardedJobs so the
// two pickers can diverge later without coupling.
async function handleJobs() {
  const neon = await neonJobs(JOB_STATUS_AWARDED);
  if (neon) {
    return resp(200, {
      ok: true, _source: "neon",
      jobs: neon.map(j => ({ id: j.id, name: j.name })).filter(j => j.name),
    });
  }

  const records = await fetchAll(API_ROOT_MAIN, "Jobs", {
    filter: `OR({Job Status}='Awarded',{Job Status}='Service Call Scheduled',{Job Status}='Ready to Invoice')`,
    sortField: "Job Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true, _source: "airtable",
    jobs: records
      .map(r => {
        const f = r.fields || {};
        return {
          id:   r.id,
          // Prefer the "Name (PO)" display so the picker is disambiguated by PO.
          name: f["Job PO"] || f["Job Name"] || ""
        };
      })
      .filter(j => j.name)
  });
}

// ── ESTIMATING JOBS (from main NEE base, filtered by status) ──
// For the Estimates feature — pulls jobs in New Lead, Estimating, Awarded,
// Service Call Scheduled, or Ready to Invoice so estimates can be built
// or revised for any of them (Ready to Invoice covers cases where a late
// change-order or correction needs to go on a job that's already been
// flagged complete).
async function handleEstimatingJobs() {
  const neon = await neonJobs(JOB_STATUS_ESTIMATING);
  if (neon) {
    return resp(200, { ok: true, _source: "neon", jobs: neon.filter(j => j.name) });
  }

  // Everything we need lives on the main NEE base Jobs table: the status filter,
  // the "Name (PO)" display, tax status, and the contractor name. "Contractor Name
  // (Text)" (= ARRAYJOIN({Contractor})) replaces the old "Contractor (Combined)"
  // field from the synced inventory-base Jobs mirror, so there's no cross-base
  // fetch or name-matching join anymore.
  const mainRecs = await fetchAll(API_ROOT_MAIN, "Jobs", {
    filter: `OR({Job Status}='New Lead',{Job Status}='Estimating',{Job Status}='Awarded',{Job Status}='Service Call Scheduled',{Job Status}='Ready to Invoice')`,
    sortField: "Job Name",
    sortDir: "asc"
  });

  return resp(200, {
    ok: true, _source: "airtable",
    jobs: mainRecs
      .map(r => {
        const f       = r.fields || {};
        const po      = f["Job PO"] || "";
        const nm      = f["Job Name"] || "";
        // Use the formatted "Name (PO)" field when available so the user picks
        // a job that's already disambiguated by its PO (e.g. "Blue Ridge Poultry (GRB 126)")
        const display = po || nm;
        return {
          id:         r.id,
          name:       display,
          status:     f["Job Status"]?.name || f["Job Status"] || "",
          taxable:    (f["Tax Status"]?.name || f["Tax Status"] || "") === "Taxable",
          contractor: (f["Contractor Name (Text)"] || "").trim()
        };
      })
      .filter(j => j.name)
  });
}

// ── DISTINCT CONTRACTORS (from main NEE base Jobs) ─────────
// Used by the Save-as-Template modal to populate a contractor datalist and
// by the Templates list filter pills. Reads "Contractor Name (Text)" off the
// main base Jobs table (no longer the synced inventory-base Jobs mirror).
async function handleTemplateContractors() {
  const dedupe = (names) =>
    [...new Set(names.map(c => (c || "").trim()).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b));

  const neon = await neonJobs();          // every job, not just a status set
  if (neon) {
    return resp(200, {
      ok: true, _source: "neon",
      contractors: dedupe(neon.map(j => j.contractor)),
    });
  }

  const records = await fetchAll(API_ROOT_MAIN, "Jobs", {});
  return resp(200, {
    ok: true, _source: "airtable",
    contractors: dedupe(records.map(r => r.fields?.["Contractor Name (Text)"])),
  });
}

// ── AWARDED JOBS ONLY (for employee-side material ordering) ──
// Despite the name, this also includes Service Call Scheduled and
// Ready to Invoice jobs so guys in the field can order materials for
// service calls AND can still order last-minute supplies for jobs that
// have been flagged ready-to-invoice but aren't fully wrapped yet.
async function handleAwardedJobs() {
  const neon = await neonJobs(JOB_STATUS_AWARDED);
  if (neon) {
    return resp(200, {
      ok: true, _source: "neon",
      jobs: neon.map(j => ({ id: j.id, name: j.name })).filter(j => j.name),
    });
  }

  const records = await fetchAll(API_ROOT_MAIN, "Jobs", {
    filter: `OR({Job Status}='Awarded',{Job Status}='Service Call Scheduled',{Job Status}='Ready to Invoice')`,
    sortField: "Job Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true, _source: "airtable",
    jobs: records
      .map(r => {
        const f = r.fields || {};
        return {
          id:   r.id,
          // Prefer the formula field that combines Job Name + PO ("Blue Ridge Poultry (GRB 126)")
          // so material order PDFs and order lists show the PO right alongside the name.
          // Falls back to Job Name if PO is missing on a particular job.
          name: f["Job PO"] || f["Job Name"] || ""
        };
      })
      .filter(j => j.name)
  });
}

// ── Reference data, served from Neon (Step B) ───────────────────────────────
// Locations, Vendors, Inventory Items and Vendor Pricing live in Neon as of
// `db/schema/029`. Same contract as the job reads above: Neon first, Airtable
// as the fallback, `null` meaning "Neon had no opinion".
//
// ⚠⚠ Ids stay AIRTABLE rec ids here too. They flow into the cart, onto
// Inventory Transactions, into estimate and order lines, and into the expense
// push. Airtable is still the identity authority for this slice, so every
// query selects `airtable_id AS id`. A Neon uuid escaping into any of those is
// the same class of bug as the job-picker trap at B0.
//
// ⚠ The 14 location-derived quantity fields on Inventory Items are NOT here and
// never will be — verified 2026-08-10 that nothing reads them. On-hand comes
// from Stock Levels, which is Step C.

// Every item, indexed by its public handle. This one helper replaced a
// fetch-all-items-and-build-a-map pattern that appeared at ELEVEN separate call
// sites; they all wanted the same five fields, which is what made a single
// index possible rather than eleven bespoke reads.
//
// ⚠ The key is COALESCE(airtable_id, id::text) — the rec id for the 866 items
// that predate the cutover, the uuid for anything created since. See
// db/schema/041 for why items kept a dual handle where everything else moved.
//
// Returns null on failure; itemIndex() turns that into a thrown error, because
// there is no second item table to fall back to any more.
async function neonItemIndex() {
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS airtable_id, name, category,
            unit_of_measure, default_unit_cost, wire_ft_per_lb
       FROM inventory_items`);
  if (!q?.rows) return null;
  const index = {};
  for (const r of q.rows) {
    index[r.airtable_id] = {
      id:          r.airtable_id,
      // Falls back to the rec id, not "", because the call sites this replaces
      // all did `Item Name || r.id` — a debugging tell for an unnamed item.
      // (Nothing is unnamed today: 0 blank names across 866 rows.)
      name:        r.name || r.airtable_id,
      cat:         r.category || "",
      uom:         r.unit_of_measure || "",
      cost:        Number(r.default_unit_cost ?? 0),
      wireFtPerLb: Number(r.wire_ft_per_lb ?? 0),
    };
  }
  return index;
}

// The same index, built from Airtable. Kept beside its Neon twin so the two
// shapes can't drift — every caller consumes one or the other interchangeably.
// airtableItemIndex() is gone — there is no Airtable item table to index any
// more, and itemIndex() answers from Neon alone.

// What the eleven call sites actually use. Neon only now — there is no second
// item table to fall back to, and the callers that price a cart or a template
// would rather fail than quote from a frozen copy.
async function itemIndex() {
  const idx = await neonItemIndex();
  if (!idx) throw new Error("Inventory items are unavailable (Neon unreachable).");
  return idx;
}

// ── KEEP NEON IN STEP AFTER AN ITEM WRITE ──────────────────────────────────
// ⚠⚠ These MUST move in the same commit as the reads above, and this is not a
// style preference — it is the bug this project has shipped THREE times. Once
// `handleItems` and `itemIndex()` read Neon, an Airtable-only write means the
// new cost is simply never seen: the estimate still prices at the old number,
// the cart still snapshots the old number, and nothing errors. It is the same
// shape as the inventory-push gap that hid material cost from the field app for
// three days (Step E).
//
// syncItemToNeon() is gone: an item is created in Postgres and updated there,
// so there is no Airtable record to mirror from, no rec id to key on, and no
// loader to catch up anything a mirror missed. All three of those assumptions
// held while Airtable was the authority; none of them do now.

// ── THE LEDGER IS NATIVE (cutover slice 1) ────────────────────────────────
// Transactions are BORN here. Airtable is no longer written for the ledger at
// all, so these fail **CLOSED** — the exact opposite of the mirror helpers
// above, and the opposite of what this block did while Airtable was authority.
//
// The old contract was: fail soft, because the Airtable record already exists
// and the loader repairs Neon on its next run. Both halves of that are now
// false. There is no second copy and nothing to re-run: if the insert does not
// land, **the transaction did not happen**, and the caller has to say so rather
// than report success over a stock movement that was never recorded.
//
// `submitCart` used to have no idempotency key, which is why it was allowed to
// fail soft — telling the user to retry risked double-logging material out of
// stock. It has one now (`submit_id`, generated by the client and reused on
// retry), so a retry is free and failing closed is strictly better.

// One statement for the whole cart, so a half-written cart is not a state the
// database can be left in. `submitId` makes a replay a no-op: the partial
// unique index swallows the rows and the ids of the ORIGINAL submission are
// returned, so a retry looks identical to the first success from the outside.
//
// Item and location ids arriving here are still Airtable rec ids — those tables
// remain Airtable-authoritative until slices 6 and 7 — so the FKs resolve by
// subselect exactly as the mirror did, and both id forms are stored.
async function insertTxns(rows, submitId = null) {
  if (!rows || !rows.length) return [];
  const params = [];
  const p = (v) => { params.push(v); return `$${params.length}`; };
  const tuples = rows.map((r, i) => {
    const item = p(r.itemId ?? null);
    const from = p(r.fromLocationId ?? null);
    const to   = p(r.toLocationId ?? null);
    return `(${p(r.txnDate)}, ${item}, (SELECT id FROM inventory_items WHERE airtable_id=${item} OR id::text=${item}),` +
           `${p(Number(r.qty) || 0)}, ${p(r.type ?? null)},` +
           `${from}, (SELECT id FROM locations WHERE airtable_id=${from}),` +
           `${to}, (SELECT id FROM locations WHERE airtable_id=${to}),` +
           `${p(r.unitCost ?? null)}, ${p(r.notes ?? null)}, ${p(r.enteredBy ?? null)},` +
           `${p(r.jobId ?? null)}, ${p(r.jobName ?? null)},` +
           `${p(submitId)}, ${p(submitId ? i + 1 : null)}, now())`;
  });

  const inserted = await neonWrite("insertTxns",
    `INSERT INTO inventory_transactions
       (txn_date, item_airtable_id, item_id, quantity, txn_type,
        from_location_airtable_id, from_location_id,
        to_location_airtable_id, to_location_id,
        unit_cost_snapshot, notes, entered_by, job_airtable_id, job_name,
        submit_id, submit_line_no, synced_at)
     VALUES ${tuples.join(",")}
     ${submitId ? "ON CONFLICT (submit_id, submit_line_no) WHERE submit_id IS NOT NULL DO NOTHING" : ""}
     RETURNING id`, params);

  // Short row count on a keyed submit means some or all of it was already
  // there — a retry after a lost response. Return what the first attempt
  // created rather than a partial list, or the caller reports fewer lines
  // logged than the user actually has in stock.
  if (submitId && inserted.length < rows.length) {
    const all = await neonWrite("insertTxns:replay",
      `SELECT id FROM inventory_transactions WHERE submit_id = $1 ORDER BY submit_line_no`,
      [submitId]);
    return all.map(r => r.id);
  }
  return inserted.map(r => r.id);
}

// Deleting is the one operation the loader could never repair even when it ran
// — it upserts and never removes — so this has always had to be right. It now
// also has to report failure, because on-hand keeps counting a transaction that
// did not actually leave.
async function deleteTxn(id) {
  if (!id) return false;
  const rows = await neonWrite("deleteTxn",
    `DELETE FROM inventory_transactions WHERE id = $1::uuid RETURNING id`, [id]);
  return rows.length > 0;
}

// ── KEEP NEON IN STEP AFTER AN ESTIMATE WRITE (Step D) ─────────────────────
// Same fail-soft contract as the ledger: every read here keeps a live Airtable
// fallback and the loader repairs anything missed, so breaking "save an
// estimate" over a database blip would cost more than the staleness.
//
// ⚠ Deletes are again the exception the loader cannot repair — it upserts and
// never removes — so an estimate or line deleted in Airtable but left in Neon
// would keep showing on the list and keep counting toward the total.

// Lines arrive in batches of 10 from createLineItems, so this takes an array.




// FK is ON DELETE CASCADE, so the lines go with it.






// The FK is ON DELETE CASCADE, so this takes the lines with it.

// Reorder point / notes. Same fail-soft reasoning; these are settings, not money.
// syncStockSettingToNeon is gone: reorder points are written straight to
// Neon now, so there is no Airtable record to mirror from.

// Re-read the item from Airtable and sync it. Used by the cost writers, which
// PATCH a single field and so don't have a full record in hand.
// syncItemToNeonById() is gone with it: the cost writers UPDATE the row
// directly rather than PATCHing Airtable and re-reading it back.

// ── VENDORS ────────────────────────────────────────────────
// For the pricing picker and the manage screen. There was no such action
// before: vendors were only ever reached through an item's pricing rows,
// because you added them in Airtable.
async function handleVendors(params) {
  const all = params?.all === "1";
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS id, name, vendor_type, account_number,
            phone, email, website, address, primary_contact, payment_terms, active, notes
       FROM vendors
      WHERE ($1::boolean OR active)
      ORDER BY name ASC`, [all]);
  if (!q?.rows) {
    return resp(503, { ok: false, error: "Vendors are unavailable right now. Please try again." });
  }
  return resp(200, {
    ok: true, _source: "neon",
    vendors: q.rows.map(r => ({
      id: r.id, name: r.name || "", type: r.vendor_type || "",
      accountNumber: r.account_number || "", phone: r.phone || "", email: r.email || "",
      website: r.website || "", address: r.address || "",
      primaryContact: r.primary_contact || "", paymentTerms: r.payment_terms || "",
      active: r.active === true, notes: r.notes || "",
    })),
  });
}

// ── LOCATIONS ──────────────────────────────────────────────
async function handleLocations(params) {
  // `all=1` is the manage screen only: everywhere else a retired location must
  // stay out of the pickers, but the screen that retires them has to be able to
  // show one in order to restore it.
  const all = params?.all === "1";
  // The table this migration exists for. In Airtable a location is a set of
  // field NAMES on Inventory Items; here it is a row you can insert.
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS id, name, location_type AS type, active
       FROM locations WHERE ($1::boolean OR active)
      ORDER BY name ASC`, [all]);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      locations: q.rows.map(r => ({ id: r.id, name: r.name || "", type: r.type || "", active: r.active === true })),
    });
  }

  // Fail closed. The Airtable copy of the reference tables is frozen at the
  // slice-5 cutover — missing every item created since and every price moved
  // since. Reference data quietly a day out of date is how an estimate gets
  // quoted at last week's cost.
  return resp(503, { ok: false, error: "Locations are unavailable right now. Please try again." });
}

// ── ITEMS ──────────────────────────────────────────────────
async function handleItems() {
  // 866 rows, the most-read table in the app. Note `size` (Product Size) is
  // returned here and nowhere else, so this read carries one column more than
  // the shared itemIndex() does — that is why it has its own query.
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS id, name, category, product_size, unit_of_measure,
            barcode, default_unit_cost, wire_ft_per_lb
       FROM inventory_items
      WHERE active
      ORDER BY name ASC`);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      items: q.rows.map(r => ({
        id:          r.id,
        name:        r.name || "",
        cat:         r.category || "",
        size:        r.product_size || "",
        uom:         r.unit_of_measure || "",
        barcode:     r.barcode || "",
        cost:        Number(r.default_unit_cost ?? 0),
        wireFtPerLb: Number(r.wire_ft_per_lb ?? 0),
      })),
    });
  }

  // Fail closed. The Airtable copy of the reference tables is frozen at the
  // slice-5 cutover — missing every item created since and every price moved
  // since. Reference data quietly a day out of date is how an estimate gets
  // quoted at last week's cost.
  return resp(503, { ok: false, error: "Items are unavailable right now. Please try again." });
}

// ── SUBMIT CART (multiple transactions at once) ────────────
async function handleSubmitCart(body) {
  const { lines, jobName, jobId, locationId, enteredBy, submitId } = body || {};
  if (!lines || !lines.length) return resp(400, { ok: false, error: "No items in cart." });
  if (!locationId) return resp(400, { ok: false, error: "Missing location." });

  console.log(`submitCart: jobName="${jobName}" jobId="${jobId}" location="${locationId}" lines=${lines.length}`);

  // Fetch current item costs as a fallback safety net for snapshot capture.
  // If the frontend forgets to send unitCost, we look it up here so snapshots
  // never end up as $0 unless the item itself has no cost set.
  let itemCostMap = {};
  try {
    const idx = await itemIndex();
    for (const id of Object.keys(idx)) itemCostMap[id] = idx[id].cost;
  } catch(e) {
    console.warn("Could not fetch item costs for snapshot fallback:", e.message);
  }

  const now = new Date().toISOString();

  // The whole cart in ONE insert. It used to be one Airtable POST per line in a
  // loop, so a failure halfway through left some of the material logged out of
  // stock and the rest not — with no record of which. That state no longer
  // exists: either every line lands or none do.
  const rows = lines.map(line => {
    // Snapshot the unit cost at transaction time. This freezes the cost
    // for billing so retroactive price changes do not alter historical job costs.
    // Priority: explicit unitCost from cart > current Default Unit Cost lookup.
    let snapshotCost = 0;
    if (line.unitCost !== undefined && line.unitCost !== null && Number(line.unitCost) > 0) {
      snapshotCost = Number(line.unitCost);
    } else if (itemCostMap[line.itemId] > 0) {
      snapshotCost = itemCostMap[line.itemId];
    }

    // The job is stamped as TEXT, not a link (Drop-Jobs-mirror bet). `jobId` is
    // a MAIN-base job id; handlePendingExpenses groups by it directly, which is
    // what killed the name-matching join.
    return {
      txnDate:        now,
      itemId:         String(line.itemId),
      qty:            Number(line.qty),
      type:           line.type || "Use",
      fromLocationId: String(locationId),
      toLocationId:   null,
      unitCost:       snapshotCost > 0 ? snapshotCost : null,
      notes:          line.notes || "",
      enteredBy:      enteredBy || "",
      jobId:          jobId   ? String(jobId)   : null,
      jobName:        jobName ? String(jobName) : null,
    };
  });

  // `submitId` is generated by the client once per cart and resent verbatim on
  // retry, so pressing Submit again after a lost response returns the original
  // ids instead of logging the material out of stock a second time.
  const ids = await insertTxns(rows, submitId ? String(submitId) : null);
  return resp(200, { ok: true, ids });
}

// ── RECEIVE ITEMS (with optional price update) ─────────────
async function handleReceive(body) {
  const { itemId, locationId, qty, unitCost, enteredBy, notes } = body || {};
  if (!itemId || !locationId || !qty) return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();

  // Snapshot the cost on receive transactions too — lets the receive history
  // show what was actually paid even if Default Unit Cost is updated later.
  const [txId] = await insertTxns([{
    txnDate:        now,
    itemId:         String(itemId),
    qty:            Number(qty),
    type:           "Receive",
    fromLocationId: null,
    toLocationId:   String(locationId),
    unitCost:       unitCost && Number(unitCost) > 0 ? Number(unitCost) : null,
    notes:          notes || "",
    enteredBy:      enteredBy || "",
  }]);

  // Receiving at a price moves the item's default cost. Native now, and it
  // still runs AFTER the ledger insert on purpose: the transaction is the part
  // that must not fail silently, while a missed cost update is visible on the
  // next screen and re-doable.
  if (unitCost && Number(unitCost) > 0) {
    await neonWrite("receive.itemCost",
      `UPDATE inventory_items SET default_unit_cost = $2, synced_at = now()
        WHERE airtable_id = $1 OR id::text = $1`,
      [String(itemId), Number(unitCost)]);
  }

  return resp(200, { ok: true, id: txId });
}

// ── TRANSFER ───────────────────────────────────────────────
async function handleTransfer(body) {
  const { itemId, fromLocationId, toLocationId, qty, enteredBy, notes } = body || {};
  if (!itemId || !fromLocationId || !toLocationId || !qty)
    return resp(400, { ok: false, error: "Missing required fields." });

  // A transfer is the one type that moves stock in TWO places at once — it is a
  // single row carrying both a from and a to, and v_stock_on_hand reads it as
  // two legs. So a lost transfer skews the source AND the destination, which is
  // the clearest case for failing closed rather than reporting success.
  const [txId] = await insertTxns([{
    txnDate:        new Date().toISOString(),
    itemId:         String(itemId),
    qty:            Number(qty),
    type:           "Transfer",
    fromLocationId: String(fromLocationId),
    toLocationId:   String(toLocationId),
    notes:          notes || "",
    enteredBy:      enteredBy || "",
  }]);

  return resp(200, { ok: true, id: txId });
}

// ── HISTORY ────────────────────────────────────────────────
async function handleHistory(params) {
  const { enteredBy, all } = params || {};

  // ⚠⚠ THE ID CURRENCY OF THE LEDGER IS THE NEON uuid, not the Airtable rec id.
  // A native transaction has no rec id at all, so returning one would hand the
  // client a null handle: Del would delete nothing, and — far worse — the
  // pending → push → mark chain would mark nothing and re-offer already-pushed
  // material. uuid is the only key that exists on BOTH the ~4,330 historical
  // rows and everything written since, which is why it is the one that stays.
  //
  // ⚠ There is no Airtable fallback here any more, deliberately. Airtable no
  // longer receives ledger writes, so falling back to it would silently serve a
  // stock figure missing everything logged since the cutover — a wrong number
  // presented as a right one. An error is the honest answer.
  const nq = await neonQuery(
    `SELECT t.id, t.txn_date, t.quantity, t.txn_type, t.notes, t.entered_by,
            t.unit_cost_snapshot, t.item_airtable_id,
            i.name AS item_name, i.unit_of_measure, i.default_unit_cost,
            fl.name AS from_name, tl.name AS to_name
       FROM inventory_transactions t
       LEFT JOIN inventory_items i ON i.id = t.item_id
       LEFT JOIN locations fl     ON fl.id = t.from_location_id
       LEFT JOIN locations tl     ON tl.id = t.to_location_id
      WHERE ($1::text IS NULL OR t.entered_by = $1)
      ORDER BY t.txn_date DESC NULLS LAST
      LIMIT 200`,
    [all === "1" ? null : (enteredBy || null)]);

  if (nq?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      transactions: nq.rows.map(r => {
        let dateStr = "";
        try {
          dateStr = new Date(r.txn_date).toLocaleDateString("en-US",
            { month: "short", day: "numeric", year: "numeric" });
        } catch { /* leave blank, same as the Airtable branch */ }
        const parts = String(r.notes || "").split(" | ");
        const qty   = Number(r.quantity ?? 0);
        const snap  = Number(r.unit_cost_snapshot ?? 0);
        const cost  = snap > 0 ? snap : Number(r.default_unit_cost ?? 0);
        return {
          id:        r.id,
          date:      dateStr,
          item:      r.item_name || r.item_airtable_id || "",
          itemId:    r.item_airtable_id || "",
          uom:       r.unit_of_measure || "",
          cost,
          total:     Math.round(cost * qty * 100) / 100,
          from:      r.from_name || "",
          to:        r.to_name || "",
          qty,
          type:      r.txn_type || "",
          job:       parts[0] || "",
          notes:     parts.slice(1).join(" | "),
          enteredBy: r.entered_by || "",
        };
      }),
    });
  }

  // Fail closed. The Airtable fallback that used to stand here read a table
  // that no longer receives ledger writes, so it would have answered with a
  // history missing everything logged since the cutover — and presented it as
  // complete. A wrong number offered as a right one is worse than an error.
  return resp(503, { ok: false, error: "Inventory history is unavailable right now. Please try again." });
}

// ── PENDING EXPENSES ───────────────────────────────────────
async function handlePendingExpenses() {

  // Fetch all Use AND Return transactions not yet pushed.
  // No longer reads the inventory-base Jobs *mirror* — transactions carry the
  // main-base job id directly in "Job ID (Main)" (Drop-Jobs-mirror bet, Step C;
  // legacy link-only rows were backfilled, so the name-match path is gone).
  // Pull the pending rows from Neon, SHAPED LIKE AIRTABLE RECORDS. The grouping,
  // wire maths and tax logic below are the money path and are left untouched;
  // adapting at the edge keeps this flip to a data-source swap rather than a
  // rewrite of the part that decides what a customer is charged.
  const pendingFromNeon = async () => {
    const q = await neonQuery(
      `SELECT id, item_airtable_id, quantity, txn_type, unit_cost_snapshot,
              job_airtable_id, job_name
         FROM inventory_transactions
        WHERE expense_created = false AND txn_type IN ('Use','Return')
        ORDER BY txn_date ASC NULLS LAST`);
    if (!q?.rows) return null;
    return q.rows.map(r => ({
      // ⚠⚠ The uuid, NOT airtable_id. These ids travel to the client, come back
      // in the push body, and end up in markTransactionsPushed. A native
      // transaction has no rec id, so the old mapping would have sent a null
      // here — and a null marks nothing, which re-offers material that was
      // already pushed and charges the customer for it twice.
      id: r.id,
      fields: {
        "Inventory Item":      r.item_airtable_id ? [r.item_airtable_id] : [],
        "Quantity":            Number(r.quantity ?? 0),
        "Transaction Type":    r.txn_type || "",
        "Unit Cost (Snapshot)": r.unit_cost_snapshot === null ? undefined : Number(r.unit_cost_snapshot),
        "Job ID (Main)":       r.job_airtable_id || undefined,
        "Job Name":            r.job_name || undefined,
      },
    }));
  };

  // ⚠ No Airtable fallback, and this one is not a judgement call. Airtable no
  // longer receives ledger writes OR the pushed-mark, so its copy is wrong in
  // both directions: it is missing every transaction logged since the cutover,
  // and it still shows `Expense Created?` false for material that has already
  // been pushed and paid for. Falling back to it would hand the user a push
  // list that double-charges. Fail closed instead.
  // The chargeable rows are fetched and checked BEFORE the two indexes, not
  // alongside them. itemIndex() throws when Neon is unreachable (there is no
  // second item table to fall back to), and a rejected Promise.all would skip
  // the check below and surface as a bare 500 — losing the one message that
  // tells the user nothing was charged.
  const txRecords = await pendingFromNeon();
  if (!txRecords) {
    return resp(503, { ok: false, error: "Pending expenses are unavailable right now. Please try again." });
  }
  const [itemRecords, mainJobs] = await Promise.all([itemIndex(), mainJobIndex()]);

  const itemMap = itemRecords;   // itemIndex() already returns { id -> {name, cost, wireFtPerLb, …} }

  // Main-base jobs indexed by record ID. Transactions carry the main-base job id
  // in "Job ID (Main)" text, so taxable/display resolve straight from it — no
  // cross-base mirror, no name matching (Drop-Jobs-mirror bet, Step C). The index
  // comes from Neon now, keyed on the same Airtable rec ids (Step B0).
  const mainJobById = mainJobs.index;

  // Build per-job, per-item accumulations using cost-per-transaction so that
  // multiple transactions at different snapshot prices are weighted correctly.
  // Structure: jobKey -> { jobData, items: { itemId -> { name, wireFtPerLb, netQty, totalCost } }, txIds: [] }
  const jobGroups = {};

  // Transactions whose "Job ID (Main)" doesn't resolve to a main-base job
  // (blank or stale id). These used to be silently dropped (the cost just
  // vanished). Instead, bucket them so the UI can warn the user that real
  // material costs went unpushed. Keyed by the best stable handle we have.
  const unmatched = {};

  txRecords.forEach(r => {
    const f        = r.fields || {};
    const itemArr  = f["Inventory Item"] || [];
    const txType   = f["Transaction Type"]?.name || f["Transaction Type"] || "";
    const itemId   = typeof itemArr[0] === "object" ? itemArr[0]?.id : String(itemArr[0] || "");
    const qty      = Math.abs(f["Quantity"] ?? 0);
    const notesRaw = f["Notes"] || "";
    const snapshotCost = Number(f["Unit Cost (Snapshot)"] || 0);

    // Resolve the main-base job straight from the "Job ID (Main)" text field.
    const mainIdText = String(f["Job ID (Main)"] || "").trim();
    let mainJobId = null, jobLabel = "", taxable = false;
    if (mainIdText && mainJobById[mainIdText]) {
      const mj  = mainJobById[mainIdText];
      mainJobId = mj.id;
      taxable   = mj.taxable;
      jobLabel  = (f["Job Name"] || "").trim() || mj.display;
    }

    // Skip transactions with no item, or no job reference at all. A jobless
    // Use/Return (logged without picking a job — e.g. scratch/reversed stock
    // moves) isn't a "job that couldn't be matched": there's no job to charge it
    // to, so don't surface it as unmatched (matches pre-Step-C behavior).
    if (!itemId || !mainIdText) return;

    if (!mainJobId) {
      // "Job ID (Main)" is present but doesn't resolve to a current main-base job
      // (stale/deleted job). Don't silently drop — tally an estimate of the
      // unpushed cost (snapshot price, Use positive / Return negative) so the UI
      // can surface "$X across N jobs couldn't be matched — fix it & re-run".
      // Key by the best stable handle.
      const itemData = itemMap[itemId] || {};
      const txCost   = snapshotCost > 0 ? snapshotCost : (itemData.cost || 0);
      const delta    = txType === "Return" ? -qty : qty;
      const uKey     = mainIdText || (f["Job Name"] || "").trim() || r.id;
      const u = unmatched[uKey] || (unmatched[uKey] = {
        jobName:  (f["Job Name"] || "").trim() || notesRaw.split(" | ")[0] || uKey,
        txCount:  0,
        estTotal: 0
      });
      u.txCount  += 1;
      u.estTotal += txCost * delta;
      return; // still not pushed — there is no safe main-base job to charge
    }

    // Group by main-base job id (Neon-aligned).
    const jobKey = mainJobId;
    if (!jobGroups[jobKey]) {
      jobGroups[jobKey] = {
        jobName:   jobLabel || notesRaw.split(" | ")[0] || "",
        mainJobId: mainJobId,
        taxable:   taxable,
        items:     {},
        txIds:     []
      };
    }

    // Accumulate tx IDs (all get marked as pushed regardless)
    jobGroups[jobKey].txIds.push(r.id);

    // Net qty: Use = positive, Return = negative
    const delta = txType === "Return" ? -qty : qty;

    // Per-transaction cost: prefer snapshot, fall back to current item cost
    // for legacy transactions created before the snapshot field existed.
    const itemData = itemMap[itemId] || {};
    const txCost = snapshotCost > 0 ? snapshotCost : (itemData.cost || 0);
    const lineValue = txCost * delta; // signed: negative for returns

    if (!jobGroups[jobKey].items[itemId]) {
      jobGroups[jobKey].items[itemId] = {
        name:        itemData.name || itemId,
        wireFtPerLb: itemData.wireFtPerLb || 0,
        netQty:      0,
        totalCost:   0
      };
    }
    jobGroups[jobKey].items[itemId].netQty    += delta;
    jobGroups[jobKey].items[itemId].totalCost += lineValue;
  });

  // Build the pending array for the UI — one entry per job
  const pending = Object.values(jobGroups).map(g => {
    const lines = Object.values(g.items)
      .filter(i => i.netQty !== 0)
      .map(i => {
        // Effective per-unit cost for display = totalCost / netQty
        // (handles mixed-snapshot case correctly)
        const effectiveCost = i.netQty !== 0 ? i.totalCost / i.netQty : 0;
        return {
          item:   i.name,
          qty:    i.netQty,
          cost:   Math.round(effectiveCost * 100) / 100,
          total:  Math.round(i.totalCost * 100) / 100,
          wireFt: i.wireFtPerLb > 0 ? Math.round(Math.abs(i.netQty) * i.wireFtPerLb) : 0
        };
      });

    const jobTotal = lines.reduce((s, l) => s + l.total, 0);

    return {
      jobName:   g.jobName,
      jobId:     g.mainJobId,
      taxable:   g.taxable,
      txIds:     g.txIds,
      lines,
      jobTotal
    };
  }).filter(g => g.jobTotal !== 0);
  // ^ Was `> 0` — now `!== 0` so jobs with a *negative* net (credit-only, e.g.
  // leftover vendor materials returned to shop after the job was already
  // pushed) still show up in pending and can be pushed as a credit memo.
  // Zero-dollar groups (returns exactly cancel uses) are still skipped.

  // Surface unmatched jobs alongside the pushable ones (sorted, dollars rounded).
  const unmatchedList = Object.values(unmatched)
    .map(u => ({ ...u, estTotal: Math.round(u.estTotal * 100) / 100 }))
    .filter(u => u.txCount > 0)
    .sort((a, b) => String(a.jobName).localeCompare(String(b.jobName)));

  return resp(200, { ok: true, pending, unmatched: unmatchedList });
}

// ── RECEIPT FIELD LOOKUP ──────────────────────────────────
// Returns field ID for "Receipt / Document" on the Expenses table
// Uses the Airtable meta API — requires schema:read scope on the token
async function getReceiptFieldId() {
  try {
    const res  = await fetch(`https://api.airtable.com/v0/meta/bases/${MAIN_BASE_ID}/tables`, {
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
    });
    if (!res.ok) {
      console.error(`Meta API ${res.status} — token may lack schema:read scope`);
      return null;
    }
    const data  = await res.json();
    const exp   = (data.tables || []).find(t => t.name === "Expenses");
    if (!exp) { console.error("Expenses table not found in meta"); return null; }
    const field = exp.fields.find(f => f.name === "Receipt / Document");
    const id    = field?.id || null;
    console.log("Receipt/Document field ID:", id);
    return id;
  } catch(e) {
    console.error("getReceiptFieldId failed:", e.message);
    return null;
  }
}

// ── DEBUG: GET EXPENSE FIELD IDS ──────────────────────────
async function handleGetExpenseFields() {
  try {
    const res  = await fetch(`https://api.airtable.com/v0/meta/bases/${MAIN_BASE_ID}/tables`, {
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
    });
    if (!res.ok) return resp(res.status, { ok: false, error: `Meta API error ${res.status} — token needs schema:read scope` });
    const data  = await res.json();
    const exp   = (data.tables || []).find(t => t.name === "Expenses");
    if (!exp) return resp(404, { ok: false, error: "Expenses table not found" });
    const fields = exp.fields.map(f => ({ id: f.id, name: f.name, type: f.type }));
    return resp(200, { ok: true, fields });
  } catch(e) {
    return resp(500, { ok: false, error: e.message });
  }
}

// ── PDF ATTACHMENT UPLOAD ─────────────────────────────────
async function uploadPdfToExpense(recordId, fieldId, pdfBase64, filename) {
  const pdfBuffer = Buffer.from(pdfBase64, "base64");
  const boundary  = "NEEBoundary" + Date.now().toString(36);
  const CRLF      = "\r\n";

  // Manually construct multipart/form-data body (more reliable than FormData in Node.js)
  const pre = Buffer.from([
    `--${boundary}${CRLF}`,
    `Content-Disposition: form-data; name="contentType"${CRLF}${CRLF}`,
    `application/pdf${CRLF}`,
    `--${boundary}${CRLF}`,
    `Content-Disposition: form-data; name="filename"${CRLF}${CRLF}`,
    `${filename}${CRLF}`,
    `--${boundary}${CRLF}`,
    `Content-Disposition: form-data; name="file"; filename="${filename}"${CRLF}`,
    `Content-Type: application/pdf${CRLF}${CRLF}`
  ].join(""));
  const post = Buffer.from(`${CRLF}--${boundary}--${CRLF}`);
  const body  = Buffer.concat([pre, pdfBuffer, post]);

  const res = await fetch(
    `https://content.airtable.com/v0/${MAIN_BASE_ID}/${recordId}/uploadAttachment/${fieldId}`,
    {
      method:  "POST",
      headers: {
        Authorization:  `Bearer ${AIRTABLE_API_KEY}`,
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "Content-Length": body.length.toString()
      },
      body
    }
  );
  const text = await res.text();
  if (!res.ok) throw new Error(`Attachment upload ${res.status}: ${text.substring(0, 300)}`);
  try { return JSON.parse(text); } catch(e) { return { ok: true }; }
}

// ═══════════════════════════════════════════════════════════
// PUSH HISTORY — table IDs and field IDs (in inventory base)
// ═══════════════════════════════════════════════════════════
const PUSH_TABLE_ID       = "tbl4txpj2l3pGk5E1";  // Expense Pushes
const PUSH_LINES_TABLE_ID = "tblloWlcSE7aXAX1o";  // Expense Push Lines

// Expense Pushes fields
// The Expense Pushes / Expense Push Lines field ids that stood here are gone
// with the slice-2 cutover: the push history is written to Neon only, and the
// Airtable tables are never touched again except by the loader reading the 34
// historical rows.

// ── Write a Push History header + lines.
// Best-effort: if either write fails we log and continue so the main
// expense push still appears as success to the user. The Push ID is
// returned so the caller can include it in the response.
async function recordPushHistory({ jobName, mainJobId, materialsTotal, taxTotal, taxable, txCount, lines, expenseIds, description, pushedBy, pushId }) {
  try {
    const now = new Date();
    const iso = now.toISOString();
    const dateOnly = iso.split("T")[0];
    const titleSafe = String(jobName || "Unknown").substring(0, 80);
    const title = `${dateOnly} — ${titleSafe}`;

    // ON CONFLICT on push_id makes a retry a no-op instead of a duplicate
    // history row for a charge that only happened once. RETURNING on the
    // conflict path too, via the DO UPDATE — a bare DO NOTHING returns no row,
    // and then the lines below would have no parent to attach to.
    const hdr = await neonWrite("recordPushHistory.header",
      `INSERT INTO expense_pushes
         (push_id, title, pushed_at, pushed_by, job_name, job_airtable_id,
          materials_total, tax_total, total_pushed, tx_count, item_count, taxable,
          expense_record_ids, description, synced_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14, now())
       ON CONFLICT (push_id) WHERE push_id IS NOT NULL DO UPDATE SET
         expense_record_ids = EXCLUDED.expense_record_ids, synced_at = now()
       RETURNING id`,
      [pushId ? String(pushId) : null, title, iso, String(pushedBy || ""),
       String(jobName || ""), String(mainJobId || ""),
       Math.round(Number(materialsTotal || 0) * 100) / 100,
       Math.round(Number(taxTotal || 0) * 100) / 100,
       Math.round((Number(materialsTotal || 0) + Number(taxTotal || 0)) * 100) / 100,
       Number(txCount || 0), (lines || []).length, !!taxable,
       (expenseIds || []).join(", "), String(description || "")]);

    const pushHeaderId = hdr[0]?.id;
    if (!pushHeaderId) {
      console.warn("Push History: header write returned no id");
      return null;
    }

    // Lines in ONE statement rather than batches of ten — the batching existed
    // for Airtable's request limit, not ours. Replacing first makes a retry
    // idempotent: the header is reused via ON CONFLICT, so without this the
    // same lines would accumulate underneath it.
    if ((lines || []).length) {
      await neonWrite("recordPushHistory.clearLines",
        `DELETE FROM expense_push_lines WHERE expense_push_id = $1::uuid`, [pushHeaderId]);

      const params = [];
      const p = (v) => { params.push(v); return `$${params.length}`; };
      const tuples = (lines || []).map(l => {
        const itemName = String(l.item || "Item").substring(0, 100);
        const qty      = Number(l.qty || 0);
        return `(${p(pushHeaderId)}, ${p(`${itemName} × ${qty}`.substring(0, 100))}, ${p(itemName)},` +
               `${p(qty)}, ${p(Number(l.cost || 0))}, ${p(Number(l.total || 0))},` +
               `${p(Number(l.wireFt || 0) > 0 ? Number(l.wireFt) : null)}, now())`;
      });
      await neonWrite("recordPushHistory.lines",
        `INSERT INTO expense_push_lines
           (expense_push_id, line_title, item_name, quantity, unit_cost, line_total, wire_ft, synced_at)
         VALUES ${tuples.join(",")}`, params);
    }

    return pushHeaderId;
  } catch(e) {
    console.warn("Push History: header write failed (non-fatal):", e.message);
    return null;
  }
}

// ── PUSH HISTORY LIST (most recent first) ─────────────────
async function handlePushHistory(params) {
  const limit = Math.min(Number(params?.limit || 100), 500);

  // ⚠ `id` is the uuid, for the historical rows as well as the new ones. It
  // travels to the client and comes straight back to pushHistoryDetail, and a
  // natively-written push has no rec id to send — the same trap that broke the
  // ledger's push in slice 1.
  //
  // No Airtable fallback: it no longer receives push history, so falling back
  // would show a list missing every push since this slice and present it as
  // complete. The audit trail is the one thing that must not quietly lie.
  const q = await neonQuery(
    `SELECT id, title, pushed_at, pushed_by, job_name, job_airtable_id,
            materials_total, tax_total, total_pushed, tx_count, item_count,
            taxable, expense_record_ids, description
       FROM expense_pushes
      ORDER BY pushed_at DESC NULLS LAST
      LIMIT $1`, [limit]);
  if (!q?.rows) {
    return resp(503, { ok: false, error: "Push history is unavailable right now. Please try again." });
  }

  return resp(200, {
    ok: true, _source: "neon",
    pushes: q.rows.map(r => ({
      id:            r.id,
      title:         r.title || "",
      datePushed:    r.pushed_at ? new Date(r.pushed_at).toISOString() : "",
      pushedBy:      r.pushed_by || "",
      jobName:       r.job_name || "",
      jobIdMain:     r.job_airtable_id || "",
      materialsTotal: Number(r.materials_total ?? 0),
      taxTotal:      Number(r.tax_total ?? 0),
      total:         Number(r.total_pushed ?? 0),
      txCount:       Number(r.tx_count ?? 0),
      itemCount:     Number(r.item_count ?? 0),
      taxable:       r.taxable === true,
      expenseIds:    r.expense_record_ids || "",
      description:   r.description || ""
    })),
  });
}

// ── PUSH HISTORY DETAIL (one push with its line snapshots) ───
async function handlePushHistoryDetail(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing push id." });

  // One query instead of "fetch the header, then pull EVERY push line and
  // filter in JS" — which is what the Airtable version had to do, because
  // filtering a linked record needs FIND(ARRAYJOIN()).
  //
  // `id` is the uuid. No Airtable fallback, for the same reason as the list:
  // its copy stops at this slice, and a partial audit trail shown as a complete
  // one is worse than an error.
  const q = await neonQuery(
    `SELECT p.id, p.title, p.pushed_at, p.pushed_by, p.job_name, p.job_airtable_id,
            p.materials_total, p.tax_total, p.total_pushed, p.tx_count, p.item_count,
            p.taxable, p.expense_record_ids, p.description,
            l.id AS line_id, l.item_name, l.line_title, l.quantity, l.unit_cost,
            l.line_total, l.wire_ft
       FROM expense_pushes p
       LEFT JOIN expense_push_lines l ON l.expense_push_id = p.id
      WHERE p.id = $1::uuid
      ORDER BY l.line_total DESC NULLS LAST`, [id]);
  if (!q?.rows) {
    return resp(503, { ok: false, error: "Push history is unavailable right now. Please try again." });
  }
  if (!q.rows.length) return resp(404, { ok: false, error: "Push not found." });

  const h = q.rows[0];
  return resp(200, {
    ok: true, _source: "neon",
    push: {
      id:            h.id,
      title:         h.title || "",
      datePushed:    h.pushed_at ? new Date(h.pushed_at).toISOString() : "",
      pushedBy:      h.pushed_by || "",
      jobName:       h.job_name || "",
      jobIdMain:     h.job_airtable_id || "",
      materialsTotal: Number(h.materials_total ?? 0),
      taxTotal:      Number(h.tax_total ?? 0),
      total:         Number(h.total_pushed ?? 0),
      txCount:       Number(h.tx_count ?? 0),
      itemCount:     Number(h.item_count ?? 0),
      taxable:       h.taxable === true,
      expenseIds:    h.expense_record_ids || "",
      description:   h.description || "",
      // Biggest dollars first, done in SQL. A push with no lines yields one row
      // of NULLs from the LEFT JOIN, which is not a line.
      lines: q.rows.filter(r => r.line_id).map(r => ({
        id:        r.line_id,
        itemName:  r.item_name || r.line_title || "Item",
        qty:       Number(r.quantity ?? 0),
        unitCost:  Number(r.unit_cost ?? 0),
        lineTotal: Number(r.line_total ?? 0),
        wireFt:    Number(r.wire_ft ?? 0),
      })),
    }
  });
}

// ── PUSH EXPENSES TO MAIN BASE ─────────────────────────────
// Idempotency: each pending job group carries a stable `pushId` (a UUID the
// client mints when it loads the pending list and reuses on every retry of that
// same group). We stamp it on the created Expenses, on the source transactions,
// and on the Expense Pushes header, so the same materials can never be charged
// to a job twice. Three guards, in order of the failure they close:
//   1. Same pushId already produced Expenses  -> the create succeeded but the
//      response/marking didn't land and the client retried. Skip the create,
//      just (re)mark the transactions.
//   2. Some/all of the group's transactions are no longer pending (already
//      `Expense Created?`) -> the client snapshot is stale; its line totals
//      include already-charged transactions, so charging now double-bills.
//      Refuse the group and let the user reload pending.
//   3. Per-group marking happens immediately after each group's expense is
//      created (not one trailing batch), so a mid-loop failure can't leave an
//      earlier group charged-but-unmarked -- the original re-push foot-gun.
// In Airtable this stays a read-then-write (no unique constraint); when this
// slice moves to Neon the pushId becomes a UNIQUE column + INSERT ... ON CONFLICT.
const EXP_PUSH_ID_FIELD = "flddMVlSELtNT48ez";  // Expenses -> Push ID (main base)
// The two Inventory Transactions field ids that used to live here are gone with
// the ledger cutover: `Push ID` and `Expense Created?` are Neon columns now and
// the Airtable copies are never written again.

// Mark a group's transactions as pushed and stamp the push ID. Batched by 10
// (Airtable's PATCH cap). Called per-group right after that group's expense is
// created so a failure later in the loop can't strand it unmarked.
// Marking is now a single Neon UPDATE keyed on the uuid — no Airtable PATCH, no
// batching-by-ten (that existed for Airtable's request limit, not ours).
//
// ⚠⚠ It fails CLOSED, and this is the most consequential place in the slice for
// it to do so. `expense_created` is what the pending-expenses read filters on:
// if marking silently fails, the same materials are offered for pushing again
// and the customer is charged twice. There is no longer a loader run that
// repairs it — Airtable no longer receives the mark at all.
async function markTransactionsPushed(txIds, pushId) {
  if (!txIds || !txIds.length) return;
  await neonWrite("markTransactionsPushed",
    `UPDATE inventory_transactions
        SET expense_created = true, push_id = $2, synced_at = now()
      WHERE id = ANY($1::uuid[])`,
    [txIds, String(pushId || "")]);
}

async function handlePushExpenses(body) {
  const { pending, pdfs, pushedBy } = body || {};
  if (!pending || !pending.length) return resp(400, { ok: false, error: "Nothing to push." });

  const TAX_RATE      = 0.075;
  const today         = new Date().toISOString().split("T")[0];
  const NEE_VENDOR_ID = "recdVrxXdSOH0dlXO";
  const expenseIds    = [];
  const pushHistoryIds = [];
  let   pdfUploads    = 0;
  let   txMarked      = 0;
  let   created       = 0;  // groups freshly charged this call
  let   alreadyPushed = 0;  // groups short-circuited by guard #1 (same pushId)
  let   staleSkipped  = 0;  // groups refused by guard #2 (stale snapshot)

  // ── Idempotency reads (authoritative, before any write) ───────────────────
  // (a) The set of transactions that are *genuinely* still pending right now.
  //     The client's `pending` payload can be stale; this re-read decides what
  //     is actually chargeable — a transaction already marked can't re-charge.
  // This read decides what is chargeable, so it must see the same world the
  // marking writes to. Now that markTransactionsPushed sets `expense_created`
  // in Neon, reading it from Airtable while marking both would let a
  // Neon-marked transaction look pending again — guard #2 would then refuse a
  // legitimate push, or worse, an un-marked one would re-charge.
  //
  // ⚠⚠ `id`, NOT airtable_id — this is the THIRD place that speaks the ledger's
  // id, after the pending read and the mark, and it was missed when the currency
  // changed. The symptom was total: every id in the request failed to match a
  // set full of rec ids, so every job was refused as a "stale snapshot" and
  // nothing could be pushed at all. The guard failing safe is the only reason
  // that was an outage rather than a double charge.
  const nqFresh = await neonQuery(
    `SELECT id FROM inventory_transactions
      WHERE expense_created = false AND txn_type IN ('Use','Return')`);
  // No Airtable fallback, for the reason above it: that copy is missing every
  // native row AND still says unpushed for material already charged.
  if (!nqFresh?.rows) {
    return resp(503, { ok: false, error: "Cannot verify what is still pending. Nothing was pushed — please try again." });
  }
  const stillPending = new Set(nqFresh.rows.map(r => r.id));

  // (b) Which of this request's push IDs already produced Expenses. UUIDs are a
  //     safe charset ([0-9a-f-]) so they need no formula escaping.
  const reqPushIds = [...new Set(pending.map(g => g && g.pushId).filter(Boolean))];
  const pushIdsWithExpenses = new Set();
  // The records themselves, kept so guard #1 can RE-SYNC them to Neon on a
  // retry (Step E). Without this, a push whose Airtable write landed but whose
  // Neon write failed could never be healed: the retry would short-circuit as
  // "already pushed" and the expense would stay invisible to the field app
  // forever — which is the exact bug Step E exists to close.
  const existingByPushId = new Map();
  if (reqPushIds.length) {
    const clauses = reqPushIds.map(id => `{Push ID}='${id}'`);
    const existing = await fetchAll(API_ROOT_MAIN, "Expenses", {
      filter: clauses.length === 1 ? clauses[0] : `OR(${clauses.join(",")})`
    });
    existing.forEach(r => {
      const pid = r.fields?.["Push ID"];
      if (!pid) return;
      pushIdsWithExpenses.add(pid);
      if (!existingByPushId.has(pid)) existingByPushId.set(pid, []);
      existingByPushId.get(pid).push(r);
    });
  }

  // Expenses that reached Airtable but not Neon. Collected rather than thrown
  // mid-loop: the Airtable records already exist, so aborting would strand the
  // groups after this one too. Reported at the end, and the push answers
  // ok:false so the caller retries — which heals via guard #1 above.
  const neonSyncFailures = [];

  // Re-read the record before syncing instead of trusting the create response.
  // `Total Cost (Actual)`, `Billable Material Amount $` and `Unbilled Material
  // Amount $` are Airtable formulas/rollups and they feed GP; syncing a record
  // whose derived fields haven't been computed yet would write zeros into the
  // money columns, which is worse than the gap this closes. One extra GET per
  // expense (1-2 per job) is a cheap price for not having to trust that.
  async function syncCreatedExpense(expenseId, label) {
    try {
      const rec = await atFetch(API_ROOT_MAIN, `${encodeURIComponent("Expenses")}/${expenseId}`);
      // Assert the read came back as the record we asked for. syncExpenseToNeon
      // early-returns on a record with no `id`, which is right for a fail-soft
      // caller and WRONG here — it would turn "the re-read returned something
      // unexpected" into a silent success and strand the expense, which is the
      // whole failure mode this step exists to remove.
      if (rec?.id !== expenseId) throw new Error(`re-read returned ${rec?.id || "no record"}`);
      await syncExpenseToNeon(rec);
    } catch (e) {
      console.error(`Push: expense ${expenseId} (${label}) did NOT reach Neon: ${e.message}`);
      neonSyncFailures.push(expenseId);
    }
  }

  // Look up the "Receipt / Document" field ID once if PDFs are provided
  let receiptFieldId = null;
  if (pdfs && pdfs.some(p => p)) {
    console.log(`PDF array received: ${pdfs.length} entries, non-null: ${pdfs.filter(Boolean).length}`);
    receiptFieldId = await getReceiptFieldId();
    console.log("Using receipt field ID:", receiptFieldId || "NOT FOUND — PDF upload will be skipped");
  } else {
    console.log("No PDFs in payload — skipping attachment upload");
  }

  for (let i = 0; i < pending.length; i++) {
    const g = pending[i];
    const { jobId, jobName, taxable, lines, txIds } = g;
    if (!jobId || !lines?.length) continue;

    const txList = Array.isArray(txIds) ? txIds : [];
    // Per-group push ID. Falls back to a server-minted UUID for older clients
    // that don't send one — note a fallback isn't stable across retries, so the
    // client SHOULD always supply it (see inventory.html).
    const pushId = (g.pushId && String(g.pushId)) || randomUUID();

    // ── Guard #1: this exact push already created its Expenses. Don't create a
    // second set; just make sure the transactions are marked (the usual reason
    // for the retry is the original mark didn't land).
    if (pushIdsWithExpenses.has(pushId)) {
      if (txList.length) {
        try { await markTransactionsPushed(txList, pushId); txMarked += txList.length; }
        catch (e) { console.warn("Idempotent re-mark failed (non-fatal):", e.message); }
      }
      // Re-sync to Neon as well (Step E). Marking transactions was the original
      // reason for this retry path; a Neon write that failed last time is the
      // other, and it is the one that would otherwise be unfixable. The upsert
      // is ON CONFLICT so re-syncing an already-synced expense is a no-op.
      for (const rec of existingByPushId.get(pushId) || []) {
        try { await syncExpenseToNeon(rec); }
        catch (e) {
          console.error(`Push: re-sync of ${rec.id} still failing: ${e.message}`);
          neonSyncFailures.push(rec.id);
        }
      }
      alreadyPushed++;
      continue;
    }

    // ── Guard #2: any of this group's transactions are no longer pending — they
    // were charged under another push. The client's line totals still include
    // them, so charging now would double-bill. Refuse; the user reloads pending.
    if (txList.length && txList.some(id => !stillPending.has(id))) {
      console.warn(`Push: skipping "${jobName}" — ${txList.filter(id => !stillPending.has(id)).length}/${txList.length} txns already pushed (stale snapshot).`);
      staleSkipped++;
      continue;
    }

    const jobTotal = lines.reduce((s, l) => s + (l.total || 0), 0);
    // Skip exactly-zero groups (uses and returns cancelled out — nothing to
    // record). Negative totals are allowed and produce a credit-memo expense
    // on the job (e.g. leftover vendor materials returned to shop post-push).
    if (jobTotal === 0) continue;

    // Build description — show footage for wire items
    const desc = lines.map(l => {
      const sign = l.qty < 0 ? "−" : "";
      const qtyStr = l.wireFt > 0
        ? `${Math.abs(l.qty)}lbs (${l.wireFt.toLocaleString()}ft)`
        : `${sign}${Math.abs(l.qty)}`;
      return `${l.item} ×${qtyStr}`;
    }).join(", ");

    // Track expense IDs created for this single job, for the history record
    const jobExpenseIds = [];
    let   jobTaxAmount  = 0;

    // Create materials expense. Amount can be negative when the push is a
    // net credit (returns > uses) — Airtable accepts negative numbers fine,
    // and it appears in the job's expenses as a credit line item.
    const matFields = {
      "fldPNFIzq1grsdxYi": [String(jobId)],
      "fldlTUL8hsPkReBAB": [String(NEE_VENDOR_ID)],
      "fldwbLPIafVtmaSeb": Math.round(jobTotal * 100) / 100,
      "fldX2x2J0xkRyMY3y": "Materials",
      "fldCCPYdyWAOGchWb": today,
      "fldJTg0ekrdZ4Jqr6": "Not Reviewed",
      "fld9Afieu4ofjvhSb": true,
      [EXP_PUSH_ID_FIELD]: pushId,
      "fldnSQEOnyq3sho5g": (jobTotal < 0 ? "Inventory credit (materials returned to shop) — " : "Inventory materials — ") + desc
    };

    const matResp = await atFetch(API_ROOT_MAIN, encodeURIComponent("Expenses"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields: matFields }], typecast: true })
    });
    const matExpenseId = matResp.records?.[0]?.id;
    if (matExpenseId) {
      expenseIds.push(matExpenseId);
      jobExpenseIds.push(matExpenseId);
      await syncCreatedExpense(matExpenseId, `materials — ${jobName}`);

      // Upload PDF receipt if provided
      const pdfBase64 = pdfs?.[i];
      if (pdfBase64 && receiptFieldId) {
        try {
          const safeName = (jobName || "job").replace(/[^a-z0-9]/gi, "_").substring(0, 30);
          const filename = `NEE_Materials_${safeName}_${today}.pdf`;
          await uploadPdfToExpense(matExpenseId, receiptFieldId, pdfBase64, filename);
          pdfUploads++;
          console.log(`PDF uploaded for job: ${jobName}`);
        } catch(uploadErr) {
          console.error("PDF upload failed (non-fatal):", uploadErr.message);
        }
      }
    }

    // Create sales tax expense if taxable. Tax is the same sign as the
    // materials total — a negative materials push yields a negative tax
    // credit, which is correct accounting.
    if (taxable) {
      jobTaxAmount = Math.round(jobTotal * TAX_RATE * 100) / 100;
      const taxFields = {
        "fldPNFIzq1grsdxYi": [String(jobId)],
        "fldlTUL8hsPkReBAB": [String(NEE_VENDOR_ID)],
        "fldwbLPIafVtmaSeb": jobTaxAmount,
        "fldX2x2J0xkRyMY3y": "Materials",
        "fldCCPYdyWAOGchWb": today,
        "fldJTg0ekrdZ4Jqr6": "Not Reviewed",
        [EXP_PUSH_ID_FIELD]: pushId,
        "fld9Afieu4ofjvhSb": true,
        "fldnSQEOnyq3sho5g": (jobTotal < 0 ? "Sales tax credit (7.5%) on returned materials — " : "Sales tax (7.5%) on inventory materials — ") + jobName
      };
      const taxResp = await atFetch(API_ROOT_MAIN, encodeURIComponent("Expenses"), {
        method: "POST",
        body: JSON.stringify({ records: [{ fields: taxFields }], typecast: true })
      });
      const taxExpenseId = taxResp.records?.[0]?.id;
      if (taxExpenseId) {
        expenseIds.push(taxExpenseId);
        jobExpenseIds.push(taxExpenseId);
        await syncCreatedExpense(taxExpenseId, `tax — ${jobName}`);
      }
    }

    // ── Mark THIS group's transactions immediately (guard #3). Best-effort: if
    // it fails the expense already carries the push ID, so a retry hits guard #1
    // and re-marks instead of re-charging.
    if (txList.length) {
      try { await markTransactionsPushed(txList, pushId); txMarked += txList.length; }
      catch (e) { console.warn(`Mark transactions failed for "${jobName}" (non-fatal):`, e.message); }
    }

    // Write Push History snapshot for this job — best-effort, non-fatal
    const historyId = await recordPushHistory({
      jobName,
      mainJobId:      jobId,
      materialsTotal: jobTotal,
      taxTotal:       jobTaxAmount,
      taxable:        !!taxable,
      txCount:        txList.length,
      lines,                 // [{item, qty, cost, total, wireFt}, ...]
      expenseIds:     jobExpenseIds,
      description:    desc,
      pushedBy:       pushedBy || "",
      pushId
    });
    if (historyId) pushHistoryIds.push(historyId);
    created++;
  }

  const summary = {
    count:           expenseIds.length,
    created,
    alreadyPushed,
    staleSkipped,
    txCount:         txMarked,
    pdfUploads,
    pushHistoryIds
  };

  // ── FAIL CLOSED if anything didn't reach Neon (Step E) ────────────────────
  // The expense exists in Airtable and the money is right there, but the field
  // app reads expenses from Neon — so an unsynced expense is invisible on the
  // job and absent from GP. Reporting success would be a lie of exactly the
  // kind that hid this for three days.
  //
  // Safe to tell the user to retry: the push is idempotent on `Push ID`, so the
  // retry re-hits guard #1, creates nothing new, and re-runs only the sync.
  if (neonSyncFailures.length) {
    return resp(502, {
      ok: false,
      error: `Pushed to Airtable, but ${neonSyncFailures.length} expense(s) did not reach the ` +
             `database, so they won't show on the job yet. Push again to finish — it won't charge twice.`,
      neonSyncFailures,
      ...summary
    });
  }

  return resp(200, { ok: true, ...summary });
}

// ── THE LOADER IS RETIRED (cutover slice 6, 2026-08-12) ────────────────────
// `handleLoadInventoryReference` bulk-loaded Locations, Vendors, Inventory
// Items and Vendor Pricing from Airtable into Neon. Every table it touched has
// since gone native, the last three on the same day this was removed — and a
// loader that refreshes a table the app now owns does not repair anything, it
// OVERWRITES. A renamed location would have reverted, an edited vendor price
// would have gone back to its April figure, and a deleted row would have been
// re-inserted, because a deleted row has nothing to conflict with.
//
// With nothing left to load, the function goes rather than shrinks. There is
// now no reference to the Airtable INVENTORY base anywhere in this file — not
// a read, not a write. The two remaining atFetch calls go to the MAIN base's
// Expenses, which is the GP expense push and was never in scope.
//
// The base itself is archived by hand in Airtable. That is deliberately a
// separate, later decision: leaving it there costs nothing and keeps the old
// copy readable for a few weeks.

// ── ADJUSTMENT ─────────────────────────────────────────────
// An Adjustment SETS the count. `qty` is the number the person standing in the
// shop just counted, not a movement — which is what the UI has always promised
// ("Set 1/2\" EMT PIPE at Shop #1 to 400 units?").
//
// ⚠⚠ It did not do that. The old handler wrote `qty` on the FROM leg, and
// v_stock_on_hand subtracts that leg — so counting 400 REMOVED 400. Thirty rows
// and 10,219 units went out that way, which is a large part of why on-hand read
// so negative. The bug arrived with Step C rather than the write cutover: while
// Airtable's automation maintained the Stock Levels cache it evidently treated
// an Adjustment as "set the cache to this", and deriving on-hand from the raw
// ledger silently turned the same row into a subtraction.
//
// A set cannot be expressed as one fixed row — it depends on what is there now.
// So: read current on-hand, post the DIFFERENCE, and let the ledger stay a pure
// record of movements.
//
// It self-heals, which is why no historical repair is needed: because the delta
// is measured from current on-hand, the first correct count lands an item on the
// right number however wrong its history was.
async function handleAdjustment(body) {
  const { itemId, locationId, qty, enteredBy, notes } = body || {};
  if (!itemId || !locationId || qty === undefined) return resp(400, { ok: false, error: "Missing required fields." });

  const target = Number(qty);
  if (!Number.isFinite(target)) return resp(400, { ok: false, error: "Count must be a number." });

  // Fails CLOSED. Without the current figure there is no delta to post, and
  // guessing would write a movement nobody counted.
  const cur = await neonWrite("adjustment.onHand",
    `SELECT COALESCE(s.qty_on_hand, 0) AS on_hand
       FROM inventory_items i
       CROSS JOIN locations l
       LEFT JOIN v_stock_on_hand s ON s.item_id = i.id AND s.location_id = l.id
      WHERE (i.airtable_id = $1 OR i.id::text = $1) AND l.airtable_id = $2`,
    [String(itemId), String(locationId)]);
  if (!cur.length) return resp(404, { ok: false, error: "Item or location not found." });

  const previous = Number(cur[0].on_hand ?? 0);
  const delta    = Math.round((target - previous) * 10000) / 10000;   // money-grade rounding, per the 4dp columns

  // Already right — recording a zero-quantity movement would just be noise in
  // the history of an item somebody counted and found correct.
  if (delta === 0) {
    return resp(200, { ok: true, id: null, previous, adjustedTo: target, delta: 0, noChange: true });
  }

  // Positive delta ADDS, so it goes on the to-leg; negative SUBTRACTS, so it
  // goes on the from-leg with the sign dropped. This is the whole fix.
  const [txId] = await insertTxns([{
    txnDate:        new Date().toISOString(),
    itemId:         String(itemId),
    qty:            Math.abs(delta),
    type:           "Adjustment",
    fromLocationId: delta < 0 ? String(locationId) : null,
    toLocationId:   delta > 0 ? String(locationId) : null,
    notes:          notes || "",
    enteredBy:      enteredBy || "",
  }]);

  return resp(200, { ok: true, id: txId, previous, adjustedTo: target, delta });
}

// ── CREATE NEW ITEM ───────────────────────────────────────
async function handleCreateItem(body) {
  const { name, category, productSize, uom, barcode, cost, wireFtPerLb, active } = body || {};
  if (!name || !name.trim()) return resp(400, { ok: false, error: "Item name is required." });

  // Duplicate-barcode guard, now a single indexed lookup instead of Neon-then-
  // Airtable-scan. It still fails OPEN on purpose: if the check itself cannot
  // run, creating a possible duplicate beats blocking the add, because a
  // duplicate is visible and fixable where a blocked add is a dead end.
  if (barcode && barcode.trim()) {
    const bc = barcode.trim();
    const q = await neonQuery(
      `SELECT name FROM inventory_items WHERE barcode = $1 LIMIT 1`, [bc]);
    const clash = q?.rows?.[0]?.name || null;
    if (clash) return resp(409, { ok: false, error: `Barcode already used by: ${clash}` });
  }

  // Born in Postgres, so it has no rec id. Its public handle is the uuid — see
  // db/schema/041 for why items keep a dual handle rather than moving wholesale.
  const made = await neonWrite("createItem",
    `INSERT INTO inventory_items
       (name, category, product_size, unit_of_measure, barcode,
        default_unit_cost, wire_ft_per_lb, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now())
     RETURNING id, name, category, product_size, unit_of_measure, barcode,
               default_unit_cost, wire_ft_per_lb`,
    [name.trim(),
     category && category.trim() ? category.trim() : null,
     productSize && productSize.trim() ? productSize.trim() : null,
     uom && uom.trim() ? uom.trim() : null,
     barcode && barcode.trim() ? barcode.trim() : null,
     cost && Number(cost) > 0 ? Number(cost) : null,
     wireFtPerLb && Number(wireFtPerLb) > 0 ? Number(wireFtPerLb) : null,
     active === false ? false : true]);

  const r = made[0];
  if (!r) return resp(500, { ok: false, error: "Failed to create item." });

  return resp(200, {
    ok:   true,
    item: {
      id:      r.id,
      name:    r.name || "",
      cat:     r.category || "",
      size:    r.product_size || "",
      uom:     r.unit_of_measure || "",
      barcode: r.barcode || "",
      cost:    Number(r.default_unit_cost ?? 0),
      wireFtPerLb: Number(r.wire_ft_per_lb ?? 0)
    }
  });
}

// ── UPDATE ITEM COST ───────────────────────────────────────
async function handleUpdateItemCost(body) {
  const { itemId, cost } = body || {};
  if (!itemId || cost === undefined) return resp(400, { ok: false, error: "Missing itemId or cost." });
  // Dual handle: rec id for the historical items, uuid for anything created
  // since. Fails closed — estimates quote this number, so a price change that
  // silently did not save is money.
  const rows = await neonWrite("updateItemCost",
    `UPDATE inventory_items SET default_unit_cost = $2, synced_at = now()
      WHERE airtable_id = $1 OR id::text = $1
     RETURNING id`, [String(itemId), Number(cost)]);
  if (!rows.length) return resp(404, { ok: false, error: "Item not found." });
  return resp(200, { ok: true });
}

// ── EDIT AN ITEM ──────────────────────────────────────────
// New with the write cutover. While Airtable was the authority you edited an
// item there; now that items are born in Postgres and nobody opens Airtable,
// there was no way to correct a typo, retire a part, or fix a wrong cost.
//
// Every field is optional: an absent key means "leave it alone", which is what
// omitting it from the old Airtable PATCH did.
async function handleItemUpdate(body) {
  const { itemId, name, category, productSize, uom, barcode, cost, wireFtPerLb, active } = body || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });
  if ([name, category, productSize, uom, barcode, cost, wireFtPerLb, active].every(v => v === undefined)) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }
  if (name !== undefined && !String(name).trim()) {
    return resp(400, { ok: false, error: "Item name cannot be blank." });
  }

  // Barcodes have to stay unique or the scanner picks whichever row it finds
  // first. Checked against OTHER items only, so re-saving an item without
  // touching its barcode is not a clash with itself.
  if (barcode !== undefined && String(barcode).trim()) {
    const bc = String(barcode).trim();
    const clash = await neonWrite("itemUpdate.barcode",
      `SELECT name FROM inventory_items
        WHERE barcode = $1 AND NOT (airtable_id = $2 OR id::text = $2) LIMIT 1`,
      [bc, String(itemId)]);
    if (clash.length) return resp(409, { ok: false, error: `Barcode already used by: ${clash[0].name}` });
  }

  const rows = await neonWrite("itemUpdate",
    `UPDATE inventory_items SET
       name              = COALESCE($2, name),
       category          = COALESCE($3, category),
       product_size      = COALESCE($4, product_size),
       unit_of_measure   = COALESCE($5, unit_of_measure),
       barcode           = COALESCE($6, barcode),
       default_unit_cost = COALESCE($7, default_unit_cost),
       wire_ft_per_lb    = COALESCE($8, wire_ft_per_lb),
       active            = COALESCE($9, active),
       synced_at         = now()
     WHERE airtable_id = $1 OR id::text = $1
     RETURNING COALESCE(airtable_id, id::text) AS id, name, category, product_size,
               unit_of_measure, barcode, default_unit_cost, wire_ft_per_lb, active`,
    [String(itemId),
     name        === undefined ? null : String(name).trim().toUpperCase(),
     category    === undefined ? null : (String(category).trim() || null),
     productSize === undefined ? null : (String(productSize).trim() || null),
     uom         === undefined ? null : String(uom),
     barcode     === undefined ? null : (String(barcode).trim() || null),
     cost        === undefined ? null : Number(cost),
     wireFtPerLb === undefined ? null : Number(wireFtPerLb),
     active      === undefined ? null : !!active]);

  if (!rows.length) return resp(404, { ok: false, error: "Item not found." });
  const r = rows[0];
  return resp(200, {
    ok: true,
    item: {
      id: r.id, name: r.name || "", cat: r.category || "", size: r.product_size || "",
      uom: r.unit_of_measure || "", barcode: r.barcode || "",
      cost: Number(r.default_unit_cost ?? 0), wireFtPerLb: Number(r.wire_ft_per_lb ?? 0),
      active: r.active === true,
    }
  });
}

// ── DELETE AN ITEM ────────────────────────────────────────
// ⚠ Refuses if anything references it, and that refusal is the point. An item
// that appears on an old estimate or a pushed transaction cannot be deleted
// without blanking the line it appears on — history would silently change. The
// answer for a real part you have stopped stocking is to untick Active, which
// hides it from the pickers and leaves every record intact.
//
// So delete only exists for the case it is actually safe for: something created
// by mistake that nothing has ever used.
async function handleItemDelete(body) {
  const { itemId } = body || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  const found = await neonWrite("itemDelete.refs",
    `SELECT i.id, i.name,
            (SELECT count(*) FROM inventory_transactions x WHERE x.item_id = i.id)::int AS txns,
            (SELECT count(*) FROM material_estimate_lines x WHERE x.item_id = i.id)::int AS est_lines,
            (SELECT count(*) FROM material_estimate_template_lines x WHERE x.item_id = i.id)::int AS tmpl_lines,
            (SELECT count(*) FROM material_order_lines x WHERE x.item_id = i.id)::int AS order_lines,
            (SELECT count(*) FROM vendor_pricing x WHERE x.item_id = i.id)::int AS pricing
       FROM inventory_items i
      WHERE i.airtable_id = $1 OR i.id::text = $1`, [String(itemId)]);
  if (!found.length) return resp(404, { ok: false, error: "Item not found." });

  const f = found[0];
  const used = ["txns", "est_lines", "tmpl_lines", "order_lines", "pricing"]
    .reduce((n, k) => n + Number(f[k] || 0), 0);
  if (used > 0) {
    const parts = [];
    if (Number(f.txns))        parts.push(`${f.txns} stock movement(s)`);
    if (Number(f.est_lines))   parts.push(`${f.est_lines} estimate line(s)`);
    if (Number(f.tmpl_lines))  parts.push(`${f.tmpl_lines} template line(s)`);
    if (Number(f.order_lines)) parts.push(`${f.order_lines} order line(s)`);
    if (Number(f.pricing))     parts.push(`${f.pricing} vendor price(s)`);
    return resp(409, { ok: false, inUse: true,
      error: `"${f.name}" is used by ${parts.join(", ")}. Untick Active to retire it instead — deleting would blank those records.` });
  }

  // stock_settings is NOT in the guard above: a reorder point is a setting on
  // the item, not a record of anything that happened, and the FK cascades.
  await neonWrite("itemDelete", `DELETE FROM inventory_items WHERE id = $1::uuid`, [f.id]);
  return resp(200, { ok: true, deleted: f.name });
}

// ═══════════════════════════════════════════════════════════
// LOCATIONS, VENDORS AND VENDOR PRICING — writable in the app
//
// These three were always read-only here: you maintained them in Airtable. The
// write cutover removed that without replacing it, so there was nowhere left to
// open a new shop, add a supplier, or record what one charges. Not part of the
// cutover — the hole it exposed.
// ═══════════════════════════════════════════════════════════

async function handleLocationSave(body) {
  const { locationId, name, locationType, active, notes } = body || {};
  const nm = String(name || "").trim();
  if (!locationId && !nm) return resp(400, { ok: false, error: "Location name is required." });

  if (locationId) {
    const rows = await neonWrite("locationSave.update",
      `UPDATE locations SET
         name          = COALESCE($2, name),
         location_type = COALESCE($3, location_type),
         active        = COALESCE($4, active),
         notes         = COALESCE($5, notes),
         synced_at     = now()
       WHERE airtable_id = $1 OR id::text = $1
       RETURNING COALESCE(airtable_id, id::text) AS id, name, location_type, active`,
      [String(locationId), nm || null,
       locationType === undefined ? null : (String(locationType).trim() || null),
       active === undefined ? null : !!active,
       notes  === undefined ? null : String(notes || "")]);
    if (!rows.length) return resp(404, { ok: false, error: "Location not found." });
    return resp(200, { ok: true, location: { id: rows[0].id, name: rows[0].name || "",
      type: rows[0].location_type || "", active: rows[0].active === true } });
  }

  // Names are how a location is picked and read on every screen, so a duplicate
  // is worse than an error — two "Shop #2"s cannot be told apart in a dropdown.
  const clash = await neonWrite("locationSave.dupe",
    `SELECT name FROM locations WHERE lower(name) = lower($1) LIMIT 1`, [nm]);
  if (clash.length) return resp(409, { ok: false, error: `"${clash[0].name}" already exists.` });

  const made = await neonWrite("locationSave.insert",
    `INSERT INTO locations (name, location_type, active, notes, synced_at)
     VALUES ($1,$2,$3,$4, now())
     RETURNING id, name, location_type, active`,
    [nm, locationType ? String(locationType).trim() : null,
     active === false ? false : true, notes ? String(notes) : null]);
  const r = made[0];
  return resp(200, { ok: true, location: { id: r.id, name: r.name || "",
    type: r.location_type || "", active: r.active === true } });
}

async function handleVendorSave(body) {
  const { vendorId, name, vendorType, accountNumber, phone, email, website,
          address, primaryContact, paymentTerms, active, notes } = body || {};
  const nm = String(name || "").trim();
  if (!vendorId && !nm) return resp(400, { ok: false, error: "Vendor name is required." });

  const cols = [nm || null,
    vendorType     === undefined ? null : (String(vendorType).trim() || null),
    accountNumber  === undefined ? null : (String(accountNumber).trim() || null),
    phone          === undefined ? null : (String(phone).trim() || null),
    email          === undefined ? null : (String(email).trim() || null),
    website        === undefined ? null : (String(website).trim() || null),
    address        === undefined ? null : (String(address).trim() || null),
    primaryContact === undefined ? null : (String(primaryContact).trim() || null),
    paymentTerms   === undefined ? null : (String(paymentTerms).trim() || null),
    active         === undefined ? null : !!active,
    notes          === undefined ? null : String(notes || "")];

  if (vendorId) {
    const rows = await neonWrite("vendorSave.update",
      `UPDATE vendors SET
         name = COALESCE($2, name), vendor_type = COALESCE($3, vendor_type),
         account_number = COALESCE($4, account_number), phone = COALESCE($5, phone),
         email = COALESCE($6, email), website = COALESCE($7, website),
         address = COALESCE($8, address), primary_contact = COALESCE($9, primary_contact),
         payment_terms = COALESCE($10, payment_terms), active = COALESCE($11, active),
         notes = COALESCE($12, notes), synced_at = now()
       WHERE airtable_id = $1 OR id::text = $1
       RETURNING COALESCE(airtable_id, id::text) AS id, name, active`,
      [String(vendorId), ...cols]);
    if (!rows.length) return resp(404, { ok: false, error: "Vendor not found." });
    return resp(200, { ok: true, vendor: { id: rows[0].id, name: rows[0].name || "",
      active: rows[0].active === true } });
  }

  const clash = await neonWrite("vendorSave.dupe",
    `SELECT name FROM vendors WHERE lower(name) = lower($1) LIMIT 1`, [nm]);
  if (clash.length) return resp(409, { ok: false, error: `"${clash[0].name}" already exists.` });

  const made = await neonWrite("vendorSave.insert",
    `INSERT INTO vendors (name, vendor_type, account_number, phone, email, website,
                          address, primary_contact, payment_terms, active, notes, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,COALESCE($10,true),$11, now())
     RETURNING id, name, active`, cols);
  const r = made[0];
  return resp(200, { ok: true, vendor: { id: r.id, name: r.name || "", active: r.active === true } });
}

// ── VENDOR PRICING ────────────────────────────────────────
// The money one: v_item_live_cost reads preferred+active rows here, and that
// live cost is what an estimate quotes and what "sync cost to vendor" copies
// into the item.
async function handleVendorPricingSave(body) {
  const { pricingId, itemId, vendorId, unitCost, preferred, active,
          vendorPartNumber, minOrderQty, leadTimeDays, priceValidUntil, notes } = body || {};
  if (!pricingId && (!itemId || !vendorId)) {
    return resp(400, { ok: false, error: "Item and vendor are both required." });
  }

  // ⚠ Setting preferred CLEARS it from the item's other prices, in the same
  // request. A partial unique index makes two preferred rows impossible
  // (db/schema/042), so without this the second one would simply be rejected
  // and the user would have to go and untick the first — for what is really one
  // decision: "buy this from them now".
  const clearOthers = async (itemHandle, keepId) => {
    await neonWrite("vendorPricingSave.clearPreferred",
      `UPDATE vendor_pricing SET preferred = false, synced_at = now()
        WHERE preferred
          AND ($2::uuid IS NULL OR id <> $2::uuid)
          AND item_id = (SELECT id FROM inventory_items WHERE airtable_id = $1 OR id::text = $1)`,
      [String(itemHandle), keepId || null]);
  };

  if (pricingId) {
    const cur = await neonWrite("vendorPricingSave.read",
      `SELECT p.id, COALESCE(i.airtable_id, i.id::text) AS item_handle
         FROM vendor_pricing p JOIN inventory_items i ON i.id = p.item_id
        WHERE p.airtable_id = $1 OR p.id::text = $1`, [String(pricingId)]);
    if (!cur.length) return resp(404, { ok: false, error: "Vendor price not found." });
    if (preferred === true) await clearOthers(cur[0].item_handle, cur[0].id);

    const rows = await neonWrite("vendorPricingSave.update",
      `UPDATE vendor_pricing SET
         unit_cost          = COALESCE($2, unit_cost),
         preferred          = COALESCE($3, preferred),
         active             = COALESCE($4, active),
         vendor_part_number = COALESCE($5, vendor_part_number),
         min_order_qty      = COALESCE($6, min_order_qty),
         lead_time_days     = COALESCE($7, lead_time_days),
         price_valid_until  = COALESCE($8, price_valid_until),
         notes              = COALESCE($9, notes),
         -- The price moved, so the date it moved has to move with it, or the
         -- panel keeps claiming a stale quote is current.
         last_price_update  = CASE WHEN $2 IS NOT NULL AND $2 <> unit_cost
                                   THEN CURRENT_DATE ELSE last_price_update END,
         synced_at          = now()
       WHERE id = $1::uuid RETURNING id`,
      [cur[0].id,
       unitCost         === undefined ? null : Number(unitCost),
       preferred        === undefined ? null : !!preferred,
       active           === undefined ? null : !!active,
       vendorPartNumber === undefined ? null : (String(vendorPartNumber).trim() || null),
       minOrderQty      === undefined ? null : Number(minOrderQty),
       leadTimeDays     === undefined ? null : Number(leadTimeDays),
       priceValidUntil  === undefined ? null : (String(priceValidUntil) || null),
       notes            === undefined ? null : String(notes || "")]);
    return resp(200, { ok: true, pricingId: rows[0].id });
  }

  if (preferred === true) await clearOthers(itemId, null);

  // ON CONFLICT on (item_id, vendor_id): re-pricing the same supplier for the
  // same item is an UPDATE, not a second row. Two rows for one pair is what
  // makes "which price is current?" unanswerable.
  const made = await neonWrite("vendorPricingSave.insert",
    `INSERT INTO vendor_pricing
       (item_airtable_id, item_id, vendor_airtable_id, vendor_id, unit_cost,
        preferred, active, vendor_part_number, min_order_qty, lead_time_days,
        price_valid_until, notes, last_price_update, synced_at)
     VALUES ($1, (SELECT id FROM inventory_items WHERE airtable_id = $1 OR id::text = $1),
             $2, (SELECT id FROM vendors WHERE airtable_id = $2 OR id::text = $2),
             $3, COALESCE($4,false), COALESCE($5,true), $6, $7, $8, $9, $10, CURRENT_DATE, now())
     ON CONFLICT (item_id, vendor_id) WHERE item_id IS NOT NULL AND vendor_id IS NOT NULL
       DO UPDATE SET unit_cost = EXCLUDED.unit_cost, preferred = EXCLUDED.preferred,
                     active = EXCLUDED.active, vendor_part_number = EXCLUDED.vendor_part_number,
                     min_order_qty = EXCLUDED.min_order_qty, lead_time_days = EXCLUDED.lead_time_days,
                     price_valid_until = EXCLUDED.price_valid_until, notes = EXCLUDED.notes,
                     last_price_update = CURRENT_DATE, synced_at = now()
     RETURNING id, item_id, vendor_id`,
    [String(itemId), String(vendorId),
     unitCost === undefined ? null : Number(unitCost),
     preferred === undefined ? null : !!preferred,
     active === undefined ? null : !!active,
     vendorPartNumber ? String(vendorPartNumber).trim() : null,
     minOrderQty === undefined ? null : Number(minOrderQty),
     leadTimeDays === undefined ? null : Number(leadTimeDays),
     priceValidUntil ? String(priceValidUntil) : null,
     notes ? String(notes) : null]);

  const r = made[0];
  // An unresolved FK would save a price attached to nothing — invisible to the
  // panel and to v_item_live_cost.
  if (!r?.item_id || !r?.vendor_id) return resp(404, { ok: false, error: "Item or vendor not found." });
  return resp(200, { ok: true, pricingId: r.id });
}

async function handleVendorPricingDelete(body) {
  const { pricingId } = body || {};
  if (!pricingId) return resp(400, { ok: false, error: "Missing pricingId." });
  const gone = await neonWrite("vendorPricingDelete",
    `DELETE FROM vendor_pricing WHERE airtable_id = $1 OR id::text = $1 RETURNING id`,
    [String(pricingId)]);
  if (!gone.length) return resp(404, { ok: false, error: "Vendor price not found." });
  return resp(200, { ok: true, deleted: pricingId });
}



// ── DELETE ─────────────────────────────────────────────────
async function handleDelete(body) {
  const { txId } = body || {};
  if (!txId) return resp(400, { ok: false, error: "Missing txId." });
  // txId is now the Neon uuid, for historical rows as well as native ones —
  // see the id-currency note on handleHistory. Reporting a delete that removed
  // nothing would leave on-hand counting stock the user believes they removed.
  const gone = await deleteTxn(txId);
  if (!gone) return resp(404, { ok: false, error: "Transaction not found." });
  return resp(200, { ok: true, deleted: txId });
}

// Stock Levels table fields
// The Stock Levels field ids are gone with the slice-3 cutover: reorder
// points live in Neon, and Airtable's Quantity On Hand cache was never
// carried across at all (on-hand is derived from the ledger).

// ── STOCK LEVELS BY ITEM ──────────────────────────────────
async function handleStockLevels(params) {
  const { itemId, itemName } = params || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  // ⚠ On-hand is DERIVED here, not stored. The Airtable branch below reads
  // `Quantity On Hand`, a cache that disagreed with the raw ledger on 237 of
  // 269 pairs; this reads v_stock_on_hand, which reproduces the ledger exactly.
  // Numbers WILL differ from what this screen used to show, and the ledger is
  // the one that is right. See db/schema/032.
  const q = await neonQuery(
    `SELECT stock_id, location_name, qty_on_hand, default_unit_cost,
            total_value, reorder_point, wire_ft_per_lb, wire_ft
       FROM v_stock_levels WHERE item_airtable_id = $1
      ORDER BY location_name ASC`, [itemId]);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      levels: q.rows.map(r => ({
        // The uuid, not the rec id — a reorder point set after the cutover has
        // no Airtable record behind it, and this handle comes straight back as
        // `stockLevelId` on the update.
        // Still null when a pair has movements but no setting row at all; the
        // frontend uses that to choose create-vs-update.
        id:           r.stock_id || null,
        locationName: r.location_name || "",
        qtyOnHand:    Number(r.qty_on_hand ?? 0),
        unitCost:     Number(r.default_unit_cost ?? 0),
        totalValue:   Number(r.total_value ?? 0),
        reorderPoint: Number(r.reorder_point ?? 0),
        wireWeight:   Number(r.wire_ft_per_lb ?? 0),
        wireFt:       Number(r.wire_ft ?? 0),
      })),
    });
  }

  // Fail closed. Airtable's Stock Levels table stopped being written on the
  // slice-3 cutover, so falling back to it would serve reorder points frozen
  // at that date AND its `Quantity On Hand` cache — the very column that
  // disagreed with the ledger on 237 of 269 pairs and was deliberately never
  // carried across. Stale settings on top of a discredited stock figure is not
  // a degraded answer, it is a wrong one.
  return resp(503, { ok: false, error: "Stock levels are unavailable right now. Please try again." });
}

// ── STOCK LEVELS ALL (for Category Browse in Check Stock) ─
// Returns every stock level record with its linked item ID so the
// client can group by item and show per-location breakdowns.
async function handleStockLevelsAll() {
  const q = await neonQuery(
    `SELECT stock_id, item_airtable_id, location_name, qty_on_hand,
            default_unit_cost, total_value, reorder_point, wire_ft_per_lb, wire_ft
       FROM v_stock_levels ORDER BY item_name ASC, location_name ASC`);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      levels: q.rows.map(r => ({
        id:           r.stock_id || null,
        itemId:       r.item_airtable_id || "",
        locationName: r.location_name || "",
        qtyOnHand:    Number(r.qty_on_hand ?? 0),
        unitCost:     Number(r.default_unit_cost ?? 0),
        totalValue:   Number(r.total_value ?? 0),
        reorderPoint: Number(r.reorder_point ?? 0),
        wireWeight:   Number(r.wire_ft_per_lb ?? 0),
        wireFt:       Number(r.wire_ft ?? 0),
      })),
    });
  }

  // Fail closed. Airtable's Stock Levels table stopped being written on the
  // slice-3 cutover, so falling back to it would serve reorder points frozen
  // at that date AND its `Quantity On Hand` cache — the very column that
  // disagreed with the ledger on 237 of 269 pairs and was deliberately never
  // carried across. Stale settings on top of a discredited stock figure is not
  // a degraded answer, it is a wrong one.
  return resp(503, { ok: false, error: "Stock levels are unavailable right now. Please try again." });
}

// ── REORDER ALERTS ────────────────────────────────────────
async function handleReorderAlerts() {
  // The Airtable filter compares against the CACHED quantity. This compares
  // against the ledger, so expect more alerts than before — the ones the stale
  // cache was hiding.
  const q = await neonQuery(
    `SELECT item_airtable_id, item_name, location_airtable_id, location_name,
            qty_on_hand, reorder_point, wire_ft_per_lb, wire_ft
       FROM v_stock_levels
      WHERE reorder_point > 0 AND qty_on_hand <= reorder_point
      ORDER BY location_name ASC, item_name ASC`);
  if (q?.rows) {
    const g = {};
    for (const r of q.rows) {
      const loc = r.location_name || "Unknown";
      (g[loc] = g[loc] || []).push({
        itemId:       r.item_airtable_id || "",
        itemName:     r.item_name || "",
        locationId:   r.location_airtable_id || "",
        qtyOnHand:    Number(r.qty_on_hand ?? 0),
        reorderPoint: Number(r.reorder_point ?? 0),
        shortBy:      Number(r.reorder_point ?? 0) - Number(r.qty_on_hand ?? 0),
        wireWeight:   Number(r.wire_ft_per_lb ?? 0),
        wireFt:       Number(r.wire_ft ?? 0),
      });
    }
    return resp(200, { ok: true, _source: "neon", groups: g });
  }

  // Fail closed. Airtable's Stock Levels table stopped being written on the
  // slice-3 cutover, so falling back to it would serve reorder points frozen
  // at that date AND its `Quantity On Hand` cache — the very column that
  // disagreed with the ledger on 237 of 269 pairs and was deliberately never
  // carried across. Stale settings on top of a discredited stock figure is not
  // a degraded answer, it is a wrong one.
  return resp(503, { ok: false, error: "Reorder alerts are unavailable right now. Please try again." });
}

// ── UPDATE REORDER POINT ──────────────────────────────────
async function handleUpdateReorderPoint(body) {
  const { stockLevelId, reorderPoint } = body || {};
  if (!stockLevelId) return resp(400, { ok: false, error: "Missing stockLevelId." });
  if (reorderPoint === undefined) return resp(400, { ok: false, error: "Missing reorderPoint." });

  // Keyed on the uuid, which every row has — the historical 269 as well as
  // anything set since. Fails closed: the reorder point drives the alerts
  // screen, and reporting a save that did not happen means an item quietly
  // stops warning that it is running out.
  const rows = await neonWrite("updateReorderPoint",
    `UPDATE stock_settings SET reorder_point = $2, synced_at = now()
      WHERE id = $1::uuid RETURNING id`,
    [stockLevelId, Number(reorderPoint)]);
  if (!rows.length) return resp(404, { ok: false, error: "Stock setting not found." });
  return resp(200, { ok: true });
}

// ── CREATE STOCK LEVEL ────────────────────────────────────
// Used when an admin sets a reorder point on an item × location pair that has
// no setting row yet.
//
// Native now, and simpler for it. The Airtable version had to create a Stock
// Levels record carrying a `Quantity On Hand` of 0 and a "Item | Location"
// display string, then rely on an Airtable automation to keep that cache
// updated without clobbering the reorder point. None of that exists here:
// on-hand is derived from the ledger by v_stock_on_hand, so the only thing
// worth storing is the number a human chose.
//
// An upsert rather than an insert, on the (item_id, location_id) partial unique
// — that pair IS the identity of a setting, and a second row for the same pair
// would make "what is the reorder point here?" ambiguous. It also makes the
// create/update split in the UI harmless: either path lands on the same row.
async function handleCreateStockLevel(body) {
  const { itemId, locationId, reorderPoint } = body || {};
  if (!itemId)     return resp(400, { ok: false, error: "Missing itemId." });
  if (!locationId) return resp(400, { ok: false, error: "Missing locationId." });
  if (reorderPoint === undefined) return resp(400, { ok: false, error: "Missing reorderPoint." });

  const rows = await neonWrite("createStockLevel",
    `INSERT INTO stock_settings
       (item_airtable_id, item_id, location_airtable_id, location_id, reorder_point, synced_at)
     VALUES ($1, (SELECT id FROM inventory_items WHERE airtable_id = $1 OR id::text = $1),
             $2, (SELECT id FROM locations WHERE airtable_id = $2), $3, now())
     ON CONFLICT (item_id, location_id) WHERE item_id IS NOT NULL AND location_id IS NOT NULL
       DO UPDATE SET reorder_point = EXCLUDED.reorder_point, synced_at = now()
     RETURNING id, item_id, location_id`,
    [String(itemId), String(locationId), Number(reorderPoint)]);

  const made = rows[0];
  if (!made) return resp(500, { ok: false, error: "Failed to save the reorder point." });
  // A row whose FKs did not resolve would sit outside the (item_id, location_id)
  // unique index and outside v_stock_levels — saved, and invisible.
  if (!made.item_id || !made.location_id) {
    return resp(404, { ok: false, error: "Item or location not found." });
  }
  return resp(200, { ok: true, recordId: made.id });
}

// ═══════════════════════════════════════════════════════════
// ESTIMATES — list, get, create, update, delete
// ═══════════════════════════════════════════════════════════


// Estimates table fields

// Estimate Line Items table fields

// ── ESTIMATES LIST ─────────────────────────────────────────
async function handleEstimatesList(params) {
  // `total` and `lineCount` come from v_material_estimate_totals rather than
  // Airtable's rollup and link-array length. The view was reconciled against
  // that rollup on all 14 estimates before this flipped — agreeing to the cent,
  // with 591 lines counted on both sides.
  const q = await neonQuery(
    `SELECT e.id, e.job_name, e.job_airtable_id, e.created_at, e.created_by,
            e.status, e.notes, t.total, t.line_count
       FROM material_estimates e
       LEFT JOIN v_material_estimate_totals t ON t.estimate_id = e.id
      ORDER BY e.created_at DESC NULLS LAST`);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      estimates: q.rows.map(r => ({
        id:          r.id,
        jobName:     r.job_name || "",
        jobId:       r.job_airtable_id || "",
        dateCreated: r.created_at ? new Date(r.created_at).toISOString() : "",
        createdBy:   r.created_by || "",
        status:      r.status || "Draft",
        notes:       r.notes || "",
        total:       Number(r.total ?? 0),
        lineCount:   Number(r.line_count ?? 0),
      })),
    });
  }

  // Fail closed. Airtable stopped receiving estimate writes at the slice-4
  // cutover, so its copy is missing every estimate created since AND every
  // edit to the ones it still has. An estimate list that silently omits work
  // somebody quoted is worse than a screen that says it cannot load.
  return resp(503, { ok: false, error: "Estimates are unavailable right now. Please try again." });
}

// ── GET ONE ESTIMATE WITH LINES ────────────────────────────
async function handleEstimateGet(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // One query instead of "fetch the estimate, read its link array, then fetch
  // EVERY line item and filter in JS" — which is what the Airtable branch below
  // has to do, because a linked-record filter needs FIND(ARRAYJOIN()).
  //
  // ⚠ `lineTotal` is computed here (qty × unit cost). In Airtable it was a
  // formula field; storing it would be the Stock Levels mistake in miniature.
  const nq = await neonQuery(
    `SELECT e.id AS est_id, e.job_name, e.job_airtable_id, e.created_at,
            e.created_by, e.status, e.notes,
            (SELECT total FROM v_material_estimate_totals v WHERE v.estimate_id = e.id) AS total,
            l.id AS line_id, l.line_number, l.item_airtable_id, l.description,
            l.quantity, l.unit_cost_at_estimate,
            i.name AS item_name, i.unit_of_measure, i.category
       FROM material_estimates e
       LEFT JOIN material_estimate_lines l ON l.estimate_id = e.id
       LEFT JOIN inventory_items i         ON i.id = l.item_id
      WHERE e.id = $1::uuid
      ORDER BY l.line_number ASC NULLS LAST`, [id]);

  if (nq?.rows) {
    if (!nq.rows.length) return resp(404, { ok: false, error: "Estimate not found." });
    const h = nq.rows[0];
    return resp(200, {
      ok: true, _source: "neon",
      estimate: {
        id:          h.est_id,
        jobName:     h.job_name || "",
        jobId:       h.job_airtable_id || "",
        dateCreated: h.created_at ? new Date(h.created_at).toISOString() : "",
        createdBy:   h.created_by || "",
        status:      h.status || "Draft",
        notes:       h.notes || "",
        total:       Number(h.total ?? 0),
        // LEFT JOIN gives one null-line row for an estimate with no lines.
        lines: nq.rows.filter(r => r.line_id).map(r => {
          const qty  = Number(r.quantity ?? 0);
          const cost = Number(r.unit_cost_at_estimate ?? 0);
          return {
            id:          r.line_id,
            lineNum:     Number(r.line_number ?? 0),
            itemId:      r.item_airtable_id || "",
            // A "Misc" line has no item link and carries its own text.
            itemName:    r.item_name || r.description || "",
            uom:         r.unit_of_measure || "",
            category:    r.category || "",
            isMisc:      !r.item_airtable_id,
            description: r.description || "",
            qty,
            unitCost:    cost,
            lineTotal:   Math.round(qty * cost * 10000) / 10000,
          };
        }),
      },
    });
  }

  // Fail closed. Airtable stopped receiving estimate writes at the slice-4
  // cutover, so its copy is missing every estimate created since AND every
  // edit to the ones it still has. An estimate list that silently omits work
  // somebody quoted is worse than a screen that says it cannot load.
  return resp(503, { ok: false, error: "That estimate is unavailable right now. Please try again." });
}

// ── CREATE ESTIMATE ────────────────────────────────────────
async function handleEstimateCreate(body) {
  const { jobName, jobId, status, notes, createdBy, lines } = body || {};
  if (!jobName || !jobName.trim()) return resp(400, { ok: false, error: "Job name is required." });

  // The header still goes first, but for a plainer reason than before: the
  // lines need its uuid. They no longer look the parent up by rec id, so a
  // line can't land with a null FK the way it could when the mirror lagged.
  const made = await neonWrite("estimateCreate",
    `INSERT INTO material_estimates
       (job_name, job_airtable_id, status, created_by, created_at, notes, synced_at)
     VALUES ($1,$2,$3,$4, now(), $5, now())
     RETURNING id`,
    [String(jobName).trim(), String(jobId || "").trim() || null,
     status || "Estimating", String(createdBy || "").trim() || null,
     String(notes || "").trim() || null]);

  const newId = made[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create estimate." });

  if (lines && lines.length) {
    await createLineItems(newId, lines);
  }

  return resp(200, { ok: true, id: newId });
}

// ── UPDATE ESTIMATE ────────────────────────────────────────
async function handleEstimateUpdate(body) {
  const { id, status, notes, jobName, jobId, lines, replaceLines } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // COALESCE rather than a built field list: an absent key means "leave it
  // alone", which is what the Airtable PATCH did by omitting the field. Passing
  // NULL for the ones the caller didn't send keeps that behaviour in one
  // statement instead of assembling a partial update.
  const patched = await neonWrite("estimateUpdate",
    `UPDATE material_estimates SET
       status          = COALESCE($2, status),
       notes           = COALESCE($3, notes),
       job_name        = COALESCE($4, job_name),
       job_airtable_id = COALESCE($5, job_airtable_id),
       synced_at       = now()
     WHERE id = $1::uuid RETURNING id`,
    [id,
     status  === undefined ? null : String(status),
     notes   === undefined ? null : String(notes || ""),
     jobName === undefined ? null : String(jobName || ""),
     jobId   === undefined ? null : String(jobId || "")]);
  if (!patched.length) return resp(404, { ok: false, error: "Estimate not found." });

  if (replaceLines && lines !== undefined) {
    // One DELETE for the estimate's lines instead of fetching their ids and
    // removing them ten at a time. The old path had to ask Airtable which lines
    // existed before it could delete them; the FK answers that here.
    //
    // Still the operation with no second chance: nothing repairs a line left
    // behind, and it would keep counting toward a total the user believes they
    // replaced.
    await neonWrite("estimateUpdate.clearLines",
      `DELETE FROM material_estimate_lines WHERE estimate_id = $1::uuid`, [id]);
    if (lines.length) await createLineItems(id, lines);
  } else if (lines && lines.length) {
    await createLineItems(id, lines);
  }

  return resp(200, { ok: true, id });
}

// ── DELETE ESTIMATE ────────────────────────────────────────
async function handleEstimateDelete(body) {
  const { id } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });
  // ON DELETE CASCADE takes the lines with it — the Airtable version had to
  // read the link array and delete them itself first.
  const gone = await neonWrite("estimateDelete",
    `DELETE FROM material_estimates WHERE id = $1::uuid RETURNING id`, [id]);
  if (!gone.length) return resp(404, { ok: false, error: "Estimate not found." });
  return resp(200, { ok: true, deleted: id });
}

// ── HELPER: Create line items in batches of 10 ─────────────
// Insert an estimate's lines in ONE statement.
//
// `estimateUuid` is the Neon id, not a rec id. Line numbers are the line's
// position within THIS estimate: the old `Line ID` was a global Airtable
// autonumber, but nothing outside the row ever read it — it only ordered lines
// within one estimate, which is exactly what 1..N does, without a global
// counter to seed or collide.
//
// Misc lines are the reason `item_airtable_id` can be null: a line with no item
// carries its own description text instead, and the read falls back to it for
// the display name.
async function createLineItems(estimateUuid, lines) {
  if (!lines || !lines.length) return;
  const params = [];
  const p = (v) => { params.push(v); return `$${params.length}`; };
  const tuples = lines.map((l, i) => {
    const item = p(l.itemId ? String(l.itemId) : null);
    return `(${p(estimateUuid)}, ${p(i + 1)}, ${item},` +
           `(SELECT id FROM inventory_items WHERE airtable_id = ${item} OR id::text = ${item}),` +
           `${p(Number(l.qty || 0))}, ${p(Number(l.unitCost || 0))},` +
           `${p(l.isMisc && l.description ? String(l.description).trim() : null)}, now())`;
  });
  await neonWrite("createLineItems",
    `INSERT INTO material_estimate_lines
       (estimate_id, line_number, item_airtable_id, item_id, quantity,
        unit_cost_at_estimate, description, synced_at)
     VALUES ${tuples.join(",")}`, params);
}

// ═══════════════════════════════════════════════════════════
// ESTIMATE TEMPLATES — save an estimate as a reusable template,
// list/edit templates, and clone a template into a fresh estimate.
//
// Pricing model: templates store a frozen Unit Cost at Save snapshot
// for reference. When a template is cloned into a new estimate, the
// backend looks up the CURRENT Default Unit Cost on each Inventory
// Item — so the new estimate always reflects today's pricing.
// The Refresh All Prices action re-snapshots a template against
// current pricing in one shot for templates that have drifted.
// ═══════════════════════════════════════════════════════════


// Estimate Templates fields

// Estimate Template Lines fields

// Inventory Items: Default Unit Cost field id (used for live pricing on clone)
const F_ITEM_DEFAULT_COST = "fld8aEhTzmEbqgIg4";

// ── HELPER: build a lineTitle for template lines ───────────

// ── HELPER: Recompute Total at Save on a template ──────────
// Sums Line Total at Save across the template's lines and PATCHes the
// frozen Total at Save back. Called after any line mutation.
// Total at Save is a stored snapshot, so it has to be rewritten whenever the
// lines beneath it change. It is NOT a rollup on purpose: the gap between it
// and today's prices is what tells a user their template has drifted, and
// v_material_template_totals carries both figures for exactly that comparison.
async function recomputeTemplateTotal(templateId) {
  await neonWrite("recomputeTemplateTotal",
    `UPDATE material_estimate_templates SET
       total_at_save = COALESCE((SELECT sum(l.quantity * l.unit_cost_at_save)
                                   FROM material_estimate_template_lines l
                                  WHERE l.template_id = $1::uuid), 0),
       synced_at = now()
     WHERE id = $1::uuid`, [templateId]);
}

// ── LIST TEMPLATES ─────────────────────────────────────────
async function handleEstimateTemplatesList(params) {
  const activeOnly = params?.activeOnly === "1" || params?.activeOnly === "true";
  const contractorRaw = (params?.contractor || "").trim();

  // Filtering happens in SQL rather than in JS after pulling everything.
  // `contractor` is matched case-insensitively, the same as the JS below.
  const q = await neonQuery(
    `SELECT t.id, t.name, t.description, t.active, t.contractor,
            t.source_estimate_ref, t.total_at_save, t.created_at, t.created_by,
            v.line_count
       FROM material_estimate_templates t
       LEFT JOIN v_material_template_totals v ON v.template_id = t.id
      WHERE ($1::boolean IS NOT TRUE OR t.active)
        AND ($2::text IS NULL OR lower(t.contractor) = lower($2))
      ORDER BY t.created_at DESC NULLS LAST`,
    [!!activeOnly, contractorRaw || null]);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      templates: q.rows.map(r => ({
        id:          r.id,
        name:        r.name || "",
        description: r.description || "",
        active:      r.active === true,
        contractor:  r.contractor || "",
        sourceRef:   r.source_estimate_ref || "",
        totalAtSave: Number(r.total_at_save ?? 0),
        createdDate: r.created_at ? new Date(r.created_at).toISOString() : "",
        createdBy:   r.created_by || "",
        lineCount:   Number(r.line_count ?? 0),
      })),
    });
  }

  // Fail closed. Airtable stopped receiving template writes at the slice-4
  // cutover, so its copy is frozen: missing templates saved since, and still
  // showing the prices every refresh has moved on from. A picker that quietly
  // offers stale templates would put wrong money into a new estimate.
  return resp(503, { ok: false, error: "Templates are unavailable right now. Please try again." });
}

// ── GET ONE TEMPLATE WITH LINES ────────────────────────────
async function handleEstimateTemplateGet(params) {
  const { templateId } = params || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  // ⚠ `wireFtPerLb` comes from the item, LIVE — not from the snapshot. It is a
  // physical property of the wire, not a price, so freezing it would be wrong.
  // The costs beside it are snapshots and stay snapshots.
  const nq = await neonQuery(
    `SELECT t.id AS tmpl_id, t.name, t.description, t.active, t.contractor,
            t.source_estimate_ref, t.total_at_save, t.created_at, t.created_by,
            l.id AS line_id, l.item_airtable_id, l.quantity,
            l.unit_cost_at_save, l.notes,
            i.name AS item_name, i.unit_of_measure, i.wire_ft_per_lb
       FROM material_estimate_templates t
       LEFT JOIN material_estimate_template_lines l ON l.template_id = t.id
       LEFT JOIN inventory_items i                  ON i.id = l.item_id
      WHERE t.id = $1::uuid
      ORDER BY i.name ASC NULLS LAST`, [templateId]);

  if (nq?.rows) {
    if (!nq.rows.length) return resp(404, { ok: false, error: "Template not found." });
    const h = nq.rows[0];
    return resp(200, {
      ok: true, _source: "neon",
      template: {
        id:          h.tmpl_id,
        name:        h.name || "",
        description: h.description || "",
        active:      h.active === true,
        contractor:  h.contractor || "",
        sourceRef:   h.source_estimate_ref || "",
        totalAtSave: Number(h.total_at_save ?? 0),
        createdDate: h.created_at ? new Date(h.created_at).toISOString() : "",
        createdBy:   h.created_by || "",
      },
      lines: nq.rows.filter(r => r.line_id).map(r => {
        const qty  = Number(r.quantity ?? 0);
        const cost = Number(r.unit_cost_at_save ?? 0);
        return {
          id:              r.line_id,
          itemId:          r.item_airtable_id || "",
          itemName:        r.item_name || "",
          uom:             r.unit_of_measure || "",
          wireFtPerLb:     Number(r.wire_ft_per_lb ?? 0),
          quantity:        qty,
          unitCostAtSave:  cost,
          lineTotalAtSave: Math.round(qty * cost * 10000) / 10000,
          notes:           r.notes || "",
        };
      }),
    });
  }

  // Fail closed. Airtable stopped receiving template writes at the slice-4
  // cutover, so its copy is frozen: missing templates saved since, and still
  // showing the prices every refresh has moved on from. A picker that quietly
  // offers stale templates would put wrong money into a new estimate.
  return resp(503, { ok: false, error: "That template is unavailable right now. Please try again." });
}

// ── SAVE ESTIMATE AS TEMPLATE ──────────────────────────────
// Reads the source estimate's lines, snapshots their Unit Costs into
// frozen template lines, and links the new template back to the
// estimate by Source Estimate Reference (a label, not a link — so
// later renames/deletes of the source estimate don't affect the
// template). Misc lines on the source estimate are dropped because
// templates are item-only.
async function handleSaveEstimateAsTemplate(body) {
  const { estimateId, templateName, description, contractor, createdBy } = body || {};
  if (!estimateId) return resp(400, { ok: false, error: "Missing estimateId." });
  const name = String(templateName || "").trim();
  if (!name) return resp(400, { ok: false, error: "Template name is required." });

  // Source estimate + the lines worth templating, in one read. Misc lines are
  // excluded here rather than filtered afterwards — a line with no item has no
  // price to re-look-up later, which is the whole point of a template.
  // `skippedMiscCount` still has to be reported, so both counts come back.
  const src = await neonWrite("saveAsTemplate.source",
    `SELECT e.job_name, e.job_airtable_id,
            count(l.id) FILTER (WHERE l.item_id IS NOT NULL)::int AS usable,
            count(l.id)::int AS total_lines
       FROM material_estimates e
       LEFT JOIN material_estimate_lines l ON l.estimate_id = e.id
      WHERE e.id = $1::uuid
      GROUP BY e.job_name, e.job_airtable_id`, [estimateId]);
  if (!src.length) return resp(404, { ok: false, error: "Source estimate not found." });

  const s = src[0];
  const sourceRef = (s.job_name || "") + (s.job_airtable_id ? " (" + s.job_airtable_id + ")" : "");
  const skippedMiscCount = Number(s.total_lines || 0) - Number(s.usable || 0);

  // Total at Save is a stored SNAPSHOT, not a rollup — the whole reason a
  // template can show how far its prices have drifted since. Computed from the
  // same lines that are about to be copied so the two cannot disagree.
  const tmpl = await neonWrite("saveAsTemplate.header",
    `INSERT INTO material_estimate_templates
       (name, description, active, contractor, source_estimate_ref,
        total_at_save, created_by, created_at, synced_at)
     VALUES ($1,$2,true,$3,$4,
             COALESCE((SELECT sum(l.quantity * l.unit_cost_at_estimate)
                         FROM material_estimate_lines l
                        WHERE l.estimate_id = $6::uuid AND l.item_id IS NOT NULL), 0),
             $5, now(), now())
     RETURNING id`,
    [name, String(description || "").trim() || null, String(contractor || "").trim() || null,
     sourceRef, String(createdBy || "").trim() || null, estimateId]);

  const templateId = tmpl[0]?.id;
  if (!templateId) return resp(500, { ok: false, error: "Failed to create template." });

  // Copy the lines straight across. line_title is built the same way it always
  // was ("<template> — <item>"), which is why the item name is joined in.
  const made = await neonWrite("saveAsTemplate.lines",
    // No line total stored: Airtable had a "Line Total at Save" currency field,
    // but Neon never carried it because it is quantity × unit_cost_at_save and
    // v_material_template_totals derives it. Storing it would be the Stock
    // Levels cache mistake in miniature.
    `INSERT INTO material_estimate_template_lines
       (template_id, line_title, item_airtable_id, item_id, quantity,
        unit_cost_at_save, notes, synced_at)
     SELECT $1::uuid,
            $2 || ' — ' || COALESCE(i.name, ''),
            l.item_airtable_id, l.item_id, COALESCE(l.quantity, 0),
            COALESCE(l.unit_cost_at_estimate, 0),
            l.description, now()
       FROM material_estimate_lines l
       LEFT JOIN inventory_items i ON i.id = l.item_id
      WHERE l.estimate_id = $3::uuid AND l.item_id IS NOT NULL
     RETURNING id`, [templateId, name, estimateId]);

  return resp(200, {
    ok: true,
    templateId,
    lineCount: made.length,
    skippedMiscCount
  });
}

// ── CREATE ESTIMATE FROM TEMPLATE ──────────────────────────
// Clones a template into a new Estimate. Quantities come from the
// template; Unit Costs are pulled fresh from each Inventory Item's
// current Default Unit Cost (live pricing). Misc lines never exist
// on templates so we don't have to worry about them here.
async function handleCreateEstimateFromTemplate(body) {
  const { templateId, jobId, jobName, createdBy } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });
  if (!jobName || !String(jobName).trim()) return resp(400, { ok: false, error: "Missing jobName." });

  // The template's description becomes the new estimate's notes, exactly as it
  // did before.
  const tmpl = await neonWrite("createEstimateFromTemplate.template",
    `SELECT id, description FROM material_estimate_templates WHERE id = $1::uuid`, [templateId]);
  if (!tmpl.length) return resp(404, { ok: false, error: "Template not found." });

  const made = await neonWrite("createEstimateFromTemplate.header",
    `INSERT INTO material_estimates
       (job_name, job_airtable_id, status, created_by, created_at, notes, synced_at)
     VALUES ($1,$2,'Draft',$3, now(), $4, now())
     RETURNING id`,
    [String(jobName).trim(), String(jobId || "").trim() || null,
     String(createdBy || "").trim() || null, String(tmpl[0].description || "").trim() || null]);
  const newId = made[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create estimate." });

  // ⚠ THE WHOLE POINT OF A TEMPLATE, in one line of SQL: quantities clone from
  // the template, prices come LIVE from the item. unit_cost_at_save on the
  // template line is a reference snapshot and is deliberately not used here.
  //
  // This handler is why slice-D shipped a 404 to production: it is the SECOND
  // path that creates an estimate, it was missed when the reads flipped, and it
  // synced nothing. There is only one store to write to now, so that particular
  // shape of bug cannot recur here.
  //
  // Lines with no item are dropped, matching the old filter(l => !!l.itemId) —
  // a template line without an item has no price to look up.
  const inserted = await neonWrite("createEstimateFromTemplate.lines",
    `INSERT INTO material_estimate_lines
       (estimate_id, line_number, item_airtable_id, item_id, quantity,
        unit_cost_at_estimate, description, synced_at)
     SELECT $1::uuid,
            row_number() OVER (ORDER BY tl.line_title NULLS LAST, tl.id),
            tl.item_airtable_id, tl.item_id, COALESCE(tl.quantity, 0),
            COALESCE(i.default_unit_cost, 0), tl.notes, now()
       FROM material_estimate_template_lines tl
       LEFT JOIN inventory_items i ON i.id = tl.item_id
      WHERE tl.template_id = $2::uuid AND tl.item_id IS NOT NULL
     RETURNING id`, [newId, templateId]);

  return resp(200, { ok: true, estimateId: newId, lineCount: inserted.length });
}

// ── UPDATE TEMPLATE METADATA ───────────────────────────────
// Header-only patch — does not touch lines.
async function handleEstimateTemplateUpdate(body) {
  const { templateId, templateName, description, contractor, active } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });
  if (templateName === undefined && description === undefined &&
      contractor === undefined && active === undefined) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }

  // COALESCE per field: an absent key means leave it alone, which is what
  // omitting it from the Airtable PATCH used to do.
  const rows = await neonWrite("templateUpdate",
    `UPDATE material_estimate_templates SET
       name        = COALESCE($2, name),
       description = COALESCE($3, description),
       contractor  = COALESCE($4, contractor),
       active      = COALESCE($5, active),
       synced_at   = now()
     WHERE id = $1::uuid RETURNING id`,
    [templateId,
     templateName === undefined ? null : String(templateName || "").trim(),
     description  === undefined ? null : String(description || ""),
     contractor   === undefined ? null : String(contractor || "").trim(),
     active       === undefined ? null : !!active]);
  if (!rows.length) return resp(404, { ok: false, error: "Template not found." });
  return resp(200, { ok: true, templateId });
}

// ── UPSERT TEMPLATE LINE (create or update one line) ───────
async function handleEstimateTemplateLineUpsert(body) {
  const { templateId, lineId, itemId, quantity, notes } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  if (lineId) {
    // Editing an existing line changes the QUANTITY, never the price. The
    // snapshot is the template's whole reason to exist, so unit_cost_at_save is
    // deliberately untouched here — refreshTemplatePrices is the only thing
    // allowed to move it.
    const rows = await neonWrite("templateLineUpsert.update",
      `UPDATE material_estimate_template_lines SET
         quantity = COALESCE($2, quantity),
         notes    = COALESCE($3, notes),
         synced_at = now()
       WHERE id = $1::uuid RETURNING id`,
      [lineId,
       quantity === undefined ? null : Number(quantity || 0),
       notes    === undefined ? null : String(notes || "")]);
    if (!rows.length) return resp(404, { ok: false, error: "Template line not found." });
    await recomputeTemplateTotal(templateId);
    return resp(200, { ok: true, lineId });
  }

  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  // A new line snapshots the item's CURRENT default cost, and builds its title
  // from both names — all three resolved in the insert rather than fetched
  // first, which is what the Airtable version needed two round trips for.
  const made = await neonWrite("templateLineUpsert.insert",
    `INSERT INTO material_estimate_template_lines
       (template_id, line_title, item_airtable_id, item_id, quantity,
        unit_cost_at_save, notes, synced_at)
     SELECT t.id, t.name || ' — ' || COALESCE(i.name, ''),
            i.airtable_id, i.id, $3, COALESCE(i.default_unit_cost, 0), $4, now()
       FROM material_estimate_templates t
       JOIN inventory_items i ON (i.airtable_id = $2 OR i.id::text = $2)
      WHERE t.id = $1::uuid
     RETURNING id`,
    [templateId, String(itemId), Number(quantity || 0),
     notes !== undefined && String(notes || "").trim() ? String(notes).trim() : null]);

  // No row means the JOIN found no template or no item — the Airtable version
  // reported those as two separate 404s after two GETs.
  if (!made.length) return resp(404, { ok: false, error: "Template or inventory item not found." });
  await recomputeTemplateTotal(templateId);
  return resp(200, { ok: true, lineId: made[0].id });
}

// ── DELETE ONE TEMPLATE LINE ───────────────────────────────
async function handleEstimateTemplateLineDelete(body) {
  const { templateId, lineId } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });
  if (!lineId)     return resp(400, { ok: false, error: "Missing lineId." });

  const gone = await neonWrite("templateLineDelete",
    `DELETE FROM material_estimate_template_lines WHERE id = $1::uuid RETURNING id`, [lineId]);
  if (!gone.length) return resp(404, { ok: false, error: "Template line not found." });
  // The stored total has to follow the line out, or the template keeps quoting
  // material it no longer contains.
  await recomputeTemplateTotal(templateId);
  return resp(200, { ok: true, deleted: lineId });
}

// ── DELETE TEMPLATE (cascading) ────────────────────────────
async function handleEstimateTemplateDelete(body) {
  const { templateId } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });
  // ON DELETE CASCADE takes the lines. The Airtable version had to read the
  // link array and delete them in batches of ten first.
  const gone = await neonWrite("templateDelete",
    `DELETE FROM material_estimate_templates WHERE id = $1::uuid RETURNING id`, [templateId]);
  if (!gone.length) return resp(404, { ok: false, error: "Template not found." });
  return resp(200, { ok: true, deleted: templateId });
}

// ── REFRESH ALL PRICES ────────────────────────────────────
// Re-snapshots every line's Unit Cost at Save (and Line Total at Save) to
// the current Default Unit Cost on its Inventory Item, then recomputes
// the template's Total at Save. Used when a template has been sitting
// for a while and pricing has drifted — saves the user from manually
// re-adding every line.
async function handleRefreshTemplatePrices(body) {
  const { templateId } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  // Re-snapshot every line against the item's current cost. This is the ONE
  // operation allowed to move unit_cost_at_save — everywhere else it is frozen.
  // One statement instead of read-all-lines, compute, PATCH in batches of ten.
  const updated = await neonWrite("refreshTemplatePrices",
    `UPDATE material_estimate_template_lines l SET
       unit_cost_at_save = COALESCE(i.default_unit_cost, 0),
       synced_at = now()
     FROM inventory_items i
     WHERE l.item_id = i.id AND l.template_id = $1::uuid
     RETURNING l.id`, [templateId]);

  // And the header total follows, or the screen reports refreshed prices while
  // still showing the old sum — the exact complaint this action exists to fix.
  await recomputeTemplateTotal(templateId);
  return resp(200, { ok: true, updated: updated.length });
}

// ═══════════════════════════════════════════════════════════
// MATERIAL ORDERS — list, get, create, mark complete, delete
// ═══════════════════════════════════════════════════════════

const ORDER_TABLE_ID      = "tblLMunp1fSrZV4mH";
const ORDER_LINE_TABLE_ID = "tblERYikTOpPhklPw";



// ── ACTIVE ORDERS LIST ────────────────────────────────────
async function handleOrdersList(params) {
  const showComplete = params?.includeComplete === "1";
  const createdBy    = params?.createdBy;   // optional filter by user

  // Build filter formula
  const filters = [];
  if (!showComplete) filters.push(`{Status}='Active'`);
  if (createdBy) {
    // Escape single quotes in name
    const safeName = String(createdBy).replace(/'/g, "\\'");
    filters.push(`{Created By}='${safeName}'`);
  }
  const filter = filters.length === 0 ? undefined
               : filters.length === 1 ? filters[0]
               : `AND(${filters.join(",")})`;

  // The Airtable path hand-escapes a name into a formula string; here it is a
  // bind, so a created-by containing a quote can't reshape the query.
  const q = await neonQuery(
    `SELECT o.id, o.order_number, o.job_name, o.vendor_notes, o.status,
            o.created_at, o.created_by, v.line_count
       FROM material_orders o
       LEFT JOIN v_material_order_totals v ON v.order_id = o.id
      WHERE ($1::boolean IS TRUE OR o.status = 'Active')
        AND ($2::text IS NULL OR o.created_by = $2)
      ORDER BY o.created_at DESC NULLS LAST`,
    [!!showComplete, createdBy || null]);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      orders: q.rows.map(r => ({
        id:          r.id,
        orderId:     Number(r.order_number ?? 0),
        jobName:     r.job_name || "",
        vendor:      r.vendor_notes || "",
        status:      r.status || "Active",
        dateCreated: r.created_at ? new Date(r.created_at).toISOString() : "",
        createdBy:   r.created_by || "",
        lineCount:   Number(r.line_count ?? 0),
        // `Total Items` in Airtable is a COUNT of lines, not a sum of quantities.
        totalItems:  Number(r.line_count ?? 0),
      })),
    });
  }

  // Fail closed. Airtable stopped receiving order writes at the slice-4
  // cutover, so its copy is missing every order raised since and still shows
  // Active for ones that have been completed. An order list that is wrong in
  // both directions is worse than one that admits it cannot load.
  return resp(503, { ok: false, error: "Orders are unavailable right now. Please try again." });
}

// ── GET ONE ORDER WITH LINES ──────────────────────────────
async function handleOrderGet(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  const nq = await neonQuery(
    `SELECT o.id AS order_id, o.order_number, o.job_name, o.vendor_notes,
            o.status, o.created_at, o.created_by,
            l.id AS line_id, l.line_number, l.item_airtable_id,
            l.description, l.quantity_ordered,
            i.name AS item_name, i.unit_of_measure, i.category
       FROM material_orders o
       LEFT JOIN material_order_lines l ON l.order_id = o.id
       LEFT JOIN inventory_items i      ON i.id = l.item_id
      WHERE o.id = $1::uuid
      ORDER BY l.line_number ASC NULLS LAST`, [id]);

  if (nq?.rows) {
    if (!nq.rows.length) return resp(404, { ok: false, error: "Order not found." });
    const h = nq.rows[0];
    // A " [BOX]" suffix on the description is a marker, not text — it means the
    // line was ordered by the box rather than the each. Stripping it has to
    // happen here too, or the marker leaks onto the printed order.
    const BOX_MARKER = " [BOX]";
    return resp(200, {
      ok: true, _source: "neon",
      order: {
        id:          h.order_id,
        orderId:     Number(h.order_number ?? 0),
        jobName:     h.job_name || "",
        vendor:      h.vendor_notes || "",
        status:      h.status || "Active",
        dateCreated: h.created_at ? new Date(h.created_at).toISOString() : "",
        createdBy:   h.created_by || "",
        lines: nq.rows.filter(r => r.line_id).map(r => {
          let rawDesc = r.description || "";
          let isBox = false;
          if (rawDesc.endsWith(BOX_MARKER)) { isBox = true; rawDesc = rawDesc.slice(0, -BOX_MARKER.length); }
          return {
            id:          r.line_id,
            lineNum:     Number(r.line_number ?? 0),
            itemId:      r.item_airtable_id || "",
            itemName:    r.item_name || rawDesc || "",
            uom:         r.unit_of_measure || "",
            category:    r.category || "",
            description: rawDesc,
            qty:         Number(r.quantity_ordered ?? 0),
            isMisc:      !r.item_airtable_id,
            isBox,
          };
        }),
      },
    });
  }

  // Fail closed. Airtable stopped receiving order writes at the slice-4
  // cutover, so its copy is missing every order raised since and still shows
  // Active for ones that have been completed. An order list that is wrong in
  // both directions is worse than one that admits it cannot load.
  return resp(503, { ok: false, error: "That order is unavailable right now. Please try again." });
}

// ── CREATE ORDER ──────────────────────────────────────────
async function handleOrderCreate(body) {
  const { estimateId, jobName, vendor, createdBy, lines } = body || {};
  if (!jobName || !jobName.trim()) return resp(400, { ok: false, error: "Job name is required." });
  if (!lines || !lines.length) return resp(400, { ok: false, error: "Order has no items." });

  // ⚠⚠ THE #0 BUG IS GONE BY CONSTRUCTION. `Order ID` was an Airtable
  // autonumber absent from the create response, so the order had to be written,
  // re-fetched and re-synced just to learn its own number — and if that second
  // round trip failed, every screen showed the order as #0. A sequence hands it
  // over in the same INSERT.
  //
  // ⚠ The sequence starts at 40, NOT at max(order_number)+1. Numbers 13, 17,
  // 23-25 and 27-31 survive; every gap is an order someone saw and later
  // deleted, and #32 was minted and deleted during the Step D smoke. Airtable
  // autonumbers never reclaim, max() does — seeding from max() would reissue a
  // number already printed on someone's order.
  const made = await neonWrite("orderCreate",
    `INSERT INTO material_orders
       (order_number, estimate_id, job_name, vendor_notes, created_by, status, created_at, synced_at)
     VALUES (nextval('material_order_number_seq'), $1, $2, $3, $4, 'Active', now(), now())
     RETURNING id, order_number`,
    [estimateId ? String(estimateId) : null, String(jobName).trim(),
     String(vendor || "").trim() || null, String(createdBy || "").trim() || null]);

  const order = made[0];
  if (!order?.id) return resp(500, { ok: false, error: "Failed to create order." });

  await createOrderLinesHelper(order.id, lines);

  return resp(200, { ok: true, id: order.id, orderId: Number(order.order_number) });
}

// ── HELPER: Create order lines in batches of 10 ──────────
// One statement for the whole order.
//
// ⚠ The " [BOX]" suffix is a MARKER, not prose: it records that a line was
// ordered by the box rather than the each, stored in the description to avoid a
// schema change. The read strips it again. Keep the two in step or it leaks
// onto the printed order.
async function createOrderLinesHelper(orderUuid, lines) {
  if (!lines || !lines.length) return;
  const params = [];
  const p = (v) => { params.push(v); return `$${params.length}`; };
  const tuples = lines.map((l, i) => {
    const item = p(l.itemId ? String(l.itemId) : null);
    let desc = String(l.description || "").trim();
    if (l.isBox) desc = (desc || "Box order") + " [BOX]";
    return `(${p(orderUuid)}, ${p(i + 1)}, ${item},` +
           `(SELECT id FROM inventory_items WHERE airtable_id = ${item} OR id::text = ${item}),` +
           `${p(Number(l.qty || 0))}, ${p(desc || null)}, now())`;
  });
  await neonWrite("createOrderLines",
    `INSERT INTO material_order_lines
       (order_id, line_number, item_airtable_id, item_id, quantity_ordered, description, synced_at)
     VALUES ${tuples.join(",")}`, params);
}

// ── HELPER: Delete all lines for an order ────────────────
async function deleteOrderLines(orderUuid) {
  // One DELETE by FK, where the Airtable version had to fetch the order, read
  // its link array, and remove the lines ten at a time.
  await neonWrite("deleteOrderLines",
    `DELETE FROM material_order_lines WHERE order_id = $1::uuid`, [orderUuid]);
}

// ── UPDATE ORDER (status / vendor / notes / lines) ────────────────
async function handleOrderUpdate(body) {
  const { id, status, vendor, lines, replaceLines } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  const touchesHeader = status !== undefined || vendor !== undefined;
  if (!touchesHeader && !replaceLines) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }

  if (touchesHeader) {
    // Status is what the active list and the home badge both filter on, so a
    // "Complete" that does not land leaves the order sitting in the list
    // looking outstanding.
    const rows = await neonWrite("orderUpdate",
      `UPDATE material_orders SET
         status       = COALESCE($2, status),
         vendor_notes = COALESCE($3, vendor_notes),
         synced_at    = now()
       WHERE id = $1::uuid RETURNING id`,
      [id,
       status === undefined ? null : String(status),
       vendor === undefined ? null : String(vendor || "")]);
    if (!rows.length) return resp(404, { ok: false, error: "Order not found." });
  }

  if (replaceLines && lines !== undefined) {
    await deleteOrderLines(id);
    if (lines.length) await createOrderLinesHelper(id, lines);
  }

  return resp(200, { ok: true, id });
}

// ── DELETE ORDER ──────────────────────────────────────────
async function handleOrderDelete(body) {
  const { id } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });
  // ON DELETE CASCADE takes the lines with it.
  const gone = await neonWrite("orderDelete",
    `DELETE FROM material_orders WHERE id = $1::uuid RETURNING id`, [id]);
  if (!gone.length) return resp(404, { ok: false, error: "Order not found." });
  return resp(200, { ok: true, deleted: id });
}

// ── ACTIVE ORDERS COUNT (for badge on home button) ────────
async function handleOrdersCount() {
  // A badge on the home screen, so it runs on nearly every page load — one
  // COUNT beats paging the whole table to call .length on it.
  const q = await neonQuery(
    `SELECT count(*)::int AS n FROM material_orders WHERE status = 'Active'`);
  if (q?.rows) return resp(200, { ok: true, _source: "neon", count: q.rows[0]?.n ?? 0 });

  // No Airtable fallback: a badge counting a frozen table would under-report
  // outstanding orders, which is the one direction that matters.
  return resp(503, { ok: false, error: "Order count is unavailable right now." });
}

// ═══════════════════════════════════════════════════════════
// VENDOR PRICING — per-item lookup + sync to Default Unit Cost
// ═══════════════════════════════════════════════════════════

// ── GET VENDOR PRICING FOR ONE ITEM ───────────────────────
// Returns the list of Vendor Pricing records for a single item plus a summary
// block with Default Unit Cost, the live rollup, and the variance between them.
// Rollup field name must match exactly what's on the Inventory Items table.
async function handleItemVendorPricing(params) {
  const { itemId } = params || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  // Neon path. The Airtable version pulls ALL pricing and ALL vendors and
  // filters in JS, because filtering a linked record needs FIND(ARRAYJOIN()).
  // Here it is a join on a real FK, and `Unit Cost Rollup (Live)` — which has no
  // column, because it never was data — comes from v_item_live_cost.
  const q = await neonQuery(
    `SELECT i.name AS item_name,
            i.default_unit_cost,
            (SELECT live_unit_cost FROM v_item_live_cost lc WHERE lc.item_id = i.id) AS live_cost,
            COALESCE(p.airtable_id, p.id::text) AS pricing_id, p.unit_cost, p.preferred, p.active,
            p.vendor_part_number, p.min_order_qty, p.lead_time_days,
            p.last_price_update, p.price_valid_until, p.unit_of_measure, p.notes,
            COALESCE(v.airtable_id, v.id::text) AS vendor_id, v.name AS vendor_name
       FROM inventory_items i
       LEFT JOIN vendor_pricing p ON p.item_id = i.id
       LEFT JOIN vendors v        ON v.id = p.vendor_id
      WHERE i.airtable_id = $1 OR i.id::text = $1
      ORDER BY p.preferred DESC NULLS LAST, v.name ASC`, [String(itemId)]);

  if (q?.rows) {
    if (!q.rows.length) return resp(404, { ok: false, error: "Item not found." });
    const head        = q.rows[0];
    const defaultCost = Number(head.default_unit_cost ?? 0);
    const liveCost    = Number(head.live_cost ?? 0);

    // Key names, defaults and sort below are copied from the Airtable branch on
    // purpose — the frontend reads `partNumber`/`leadTime`/`lastUpdate`, and
    // missing numbers are 0 rather than null. A near-miss here is a silently
    // blank pricing panel.
    const vendors = q.rows
      .filter(r => r.pricing_id)     // LEFT JOIN: an item with no pricing gives one null row
      .map(r => ({
        id:          r.pricing_id,
        vendorId:    r.vendor_id || "",
        vendorName:  r.vendor_name || "",
        unitCost:    Number(r.unit_cost ?? 0),
        uom:         r.unit_of_measure || "",
        partNumber:  r.vendor_part_number || "",
        minOrderQty: Number(r.min_order_qty ?? 0),
        leadTime:    Number(r.lead_time_days ?? 0),
        lastUpdate:  r.last_price_update ? String(r.last_price_update).slice(0, 10) : "",
        validUntil:  r.price_valid_until ? String(r.price_valid_until).slice(0, 10) : "",
        preferred:   r.preferred === true,
        active:      r.active === true,
        notes:       r.notes || "",
      }));

    vendors.sort((a, b) => {
      if (a.preferred !== b.preferred) return a.preferred ? -1 : 1;
      if (a.unitCost  !== b.unitCost)  return a.unitCost - b.unitCost;
      return (a.vendorName || "").localeCompare(b.vendorName || "");
    });

    const variance = (liveCost > 0 && defaultCost > 0)
      ? { dollar: Math.round((liveCost - defaultCost) * 10000) / 10000,
          pct:    (liveCost - defaultCost) / defaultCost }
      : null;
    const preferredVendor = vendors.find(v => v.preferred);

    return resp(200, {
      ok: true, _source: "neon",
      summary: {
        defaultCost, liveCost, variance,
        preferredVendor:  preferredVendor?.vendorName || "",
        preferredUpdated: preferredVendor?.lastUpdate || "",
      },
      vendors,
    });
  }

  // Fetch the item record AND all Vendor Pricing records AND all Vendors in parallel.
  // Filtering Vendor Pricing by linked record on the API side requires FIND()
  // against an ARRAYJOIN, which is brittle across renames — simpler and safer to
  // pull all (vendor pricing is a small table) and filter in JS.
  // Vendors are pulled to build an ID -> name map because the REST API returns
  // linked records as plain record IDs, not hydrated {id, name} objects.
  // Fail closed. Vendor pricing is what the live cost is computed from, so a
  // frozen copy here would quote yesterday's price as today's.
  return resp(503, { ok: false, error: "Vendor pricing is unavailable right now. Please try again." });
}

// ── SYNC DEFAULT UNIT COST TO LIVE VENDOR PRICE ───────────
// Copies the Unit Cost Rollup (Live) value onto Default Unit Cost for this item.
// Admin-convenience button — doesn't touch any Vendor Pricing records.
async function handleSyncItemCostToVendor(body) {
  const { itemId } = body || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  // `Unit Cost Rollup (Live)` was an Airtable rollup; v_item_live_cost is the
  // view that replaced it in Step B (preferred AND active vendor price, MIN()
  // to collapse a dual-preferred pair). Read the old cost in the same query so
  // the response can still report what it changed from.
  const cur = await neonWrite("syncItemCostToVendor.read",
    `SELECT i.id, COALESCE(i.default_unit_cost, 0) AS old_cost,
            COALESCE(v.live_unit_cost, 0) AS live_cost
       FROM inventory_items i
       LEFT JOIN v_item_live_cost v ON v.item_id = i.id
      WHERE i.airtable_id = $1 OR i.id::text = $1`, [String(itemId)]);
  if (!cur.length) return resp(404, { ok: false, error: "Item not found." });

  const liveCost = Number(cur[0].live_cost || 0);
  if (!liveCost || liveCost <= 0) {
    return resp(400, { ok: false, error: "No live vendor price for this item — set a Preferred vendor with Unit Cost first." });
  }

  await neonWrite("syncItemCostToVendor.write",
    `UPDATE inventory_items SET default_unit_cost = $2, synced_at = now() WHERE id = $1::uuid`,
    [cur[0].id, liveCost]);

  return resp(200, {
    ok: true,
    newDefaultCost: liveCost,
    oldDefaultCost: Number(cur[0].old_cost || 0)
  });
}

// ── ROUTER ─────────────────────────────────────────────────
// Presigned PUT for archiving the materials PDF this app generates on every
// push. See docs/PLAN-expense-receipts.md §11.
//
// Until now that PDF was generated in the browser, handed straight to a
// download, and never stored — so the document backing a job's material costs
// existed only in whichever Downloads folder did the push.
//
// The key uses the caller's pushId, which is the SAME idempotency key stamped
// on the expenses and transactions. A retried push therefore overwrites its own
// document rather than accumulating near-duplicates.
async function handleJobDocUploadUrl(body) {
  const jobId  = String(body?.jobId  || "").trim();
  const pushId = String(body?.pushId || "").trim();
  const date   = String(body?.date   || "").trim();

  if (!jobId)  return resp(400, { ok: false, error: "Missing jobId." });
  if (!pushId) return resp(400, { ok: false, error: "Missing pushId." });
  // Both land in an object key, so neither may contain path separators.
  if (/[/\\]/.test(jobId) || /[/\\]/.test(pushId) || /\.\./.test(jobId + pushId)) {
    return resp(400, { ok: false, error: "Invalid jobId or pushId." });
  }
  // Archiving is a bonus, not part of the push. If R2 isn't configured the
  // client must still complete the push and its local download.
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured" });

  const safeDate = /^\d{4}-\d{2}-\d{2}$/.test(date) ? date : new Date().toISOString().slice(0, 10);
  const key = `${jobDocsPrefix(jobId)}NEE_Materials_${safeDate}-${pushId}.pdf`;

  try {
    return resp(200, {
      ok: true,
      available: true,
      key,
      putUrl: await presignPut(key, "application/pdf"),
      contentType: "application/pdf",
    });
  } catch (e) {
    const detail = e instanceof R2Error ? e.code : "error";
    console.error(`jobDocUploadUrl failed for job ${jobId}: ${String(e?.message || e).slice(0, 200)}`);
    return resp(200, { ok: true, available: false, reason: detail });
  }
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    // ── Server-side authn + authz (see _auth.js) ─────────────────────────────
    // Every action except `login` requires a valid signed token; role checked per action.
    const reqAction = event.httpMethod === "GET"
      ? event.queryStringParameters?.action
      : safeBodyAction(event);
    if (reqAction !== "login") {
      const authUser = authedUser(event);
      if (!authUser) return resp(401, { ok: false, error: "Not signed in. Please log in again." });
      // Revocation is shared: a token issued by either function validates in
      // both, so turning someone off in the field app must lock them out of
      // inventory too. Same _revocation.js, same 60s cache. See that file.
      if (await isSessionRevoked(authUser)) {
        return resp(401, { ok: false, error: "Your access has been turned off. Please log in again." });
      }
      if (!hasRole(authUser.role, authzFor(event.httpMethod, reqAction))) {
        return resp(403, { ok: false, error: "You don't have permission to do that." });
      }
    }

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "employees")         return await handleEmployees();
      if (action === "jobs")              return await handleJobs();
      if (action === "estimatingJobs")    return await handleEstimatingJobs();
      if (action === "awardedJobs")       return await handleAwardedJobs();
      if (action === "locations")         return await handleLocations(params);
      if (action === "vendors")           return await handleVendors(params);
      if (action === "items")             return await handleItems();
      if (action === "history")           return await handleHistory(params);
      if (action === "pendingExpenses")   return await handlePendingExpenses();
      if (action === "pushHistory")       return await handlePushHistory(params);
      if (action === "pushHistoryDetail") return await handlePushHistoryDetail(params);
      if (action === "stockLevels")       return await handleStockLevels(params);
      if (action === "stockLevelsAll")    return await handleStockLevelsAll();
      if (action === "reorderAlerts")     return await handleReorderAlerts();
      if (action === "getExpenseFields")  return await handleGetExpenseFields();
      if (action === "estimatesList")     return await handleEstimatesList(params);
      if (action === "estimateGet")       return await handleEstimateGet(params);
      if (action === "estimateTemplatesList") return await handleEstimateTemplatesList(params);
      if (action === "estimateTemplateGet")   return await handleEstimateTemplateGet(params);
      if (action === "templateContractors")   return await handleTemplateContractors();
      if (action === "ordersList")        return await handleOrdersList(params);
      if (action === "orderGet")          return await handleOrderGet(params);
      if (action === "ordersCount")       return await handleOrdersCount();
      if (action === "itemVendorPricing") return await handleItemVendorPricing(params);
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login")           return await handleLogin(body);
      if (body.action === "submitCart")      return await handleSubmitCart(body);
      if (body.action === "receive")         return await handleReceive(body);
      if (body.action === "transfer")        return await handleTransfer(body);
      if (body.action === "adjustment")      return await handleAdjustment(body);
      if (body.action === "pushExpenses")    return await handlePushExpenses(body);
      if (body.action === "jobDocUploadUrl") return await handleJobDocUploadUrl(body);
      if (body.action === "createItem")        return await handleCreateItem(body);
      if (body.action === "updateItemCost")     return await handleUpdateItemCost(body);
      if (body.action === "itemUpdate")         return await handleItemUpdate(body);
      if (body.action === "itemDelete")         return await handleItemDelete(body);
      if (body.action === "locationSave")       return await handleLocationSave(body);
      if (body.action === "vendorSave")         return await handleVendorSave(body);
      if (body.action === "vendorPricingSave")   return await handleVendorPricingSave(body);
      if (body.action === "vendorPricingDelete") return await handleVendorPricingDelete(body);
      if (body.action === "updateReorderPoint") return await handleUpdateReorderPoint(body);
      if (body.action === "createStockLevel")   return await handleCreateStockLevel(body);
      if (body.action === "delete")             return await handleDelete(body);
      if (body.action === "estimateCreate")     return await handleEstimateCreate(body);
      if (body.action === "estimateUpdate")     return await handleEstimateUpdate(body);
      if (body.action === "estimateDelete")     return await handleEstimateDelete(body);
      if (body.action === "saveEstimateAsTemplate")     return await handleSaveEstimateAsTemplate(body);
      if (body.action === "createEstimateFromTemplate") return await handleCreateEstimateFromTemplate(body);
      if (body.action === "estimateTemplateUpdate")     return await handleEstimateTemplateUpdate(body);
      if (body.action === "estimateTemplateLineUpsert") return await handleEstimateTemplateLineUpsert(body);
      if (body.action === "estimateTemplateLineDelete") return await handleEstimateTemplateLineDelete(body);
      if (body.action === "estimateTemplateDelete")     return await handleEstimateTemplateDelete(body);
      if (body.action === "refreshTemplatePrices")      return await handleRefreshTemplatePrices(body);
      if (body.action === "orderCreate")        return await handleOrderCreate(body);
      if (body.action === "orderUpdate")        return await handleOrderUpdate(body);
      if (body.action === "orderDelete")        return await handleOrderDelete(body);
      if (body.action === "syncItemCostToVendor") return await handleSyncItemCostToVendor(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Inventory Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

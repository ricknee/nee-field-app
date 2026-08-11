// netlify/functions/inventory.js
// NEE Inventory App v2 — Netlify Proxy
// Env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (main NEE), INVENTORY_BASE_ID, AUTH_SECRET
import { signToken, authedUser, hasRole } from "./_auth.js";
import { isSessionRevoked } from "./_revocation.js";
import { shadowLoginCheck, neonLoginCandidate, loginSource, neonEmployees } from "./_employees.js";
// Both fail-soft by contract: neonExec for the last-login stamp, neonQuery for
// the main-base job reads (Step B0). The driver is lazy-imported so the offline
// test suites stay install-free.
import { neonExec, neonQuery } from "./_neon.js";
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
const INV_BASE_ID      = process.env.INVENTORY_BASE_ID;
const API_ROOT_MAIN    = `https://api.airtable.com/v0/${MAIN_BASE_ID}`;
const API_ROOT_INV     = `https://api.airtable.com/v0/${INV_BASE_ID}`;

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
  if (!INV_BASE_ID)      throw new Error("Missing INVENTORY_BASE_ID");
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
  "updateItemCost", "createItem", "syncItemCostToVendor", // catalog / pricing
  "delete",              // transaction deletion
  "orderDelete", "estimateDelete", "estimateTemplateDelete", // destructive
  "loadInventoryReference",  // bulk-loads Neon reference tables (Step B)
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

// Every item, indexed by Airtable rec id. This one helper replaces the
// `fetchAll(API_ROOT_INV, "Inventory Items", {})` + build-a-map pattern that
// appeared at ELEVEN separate call sites. They all wanted the same five fields,
// which is what made a single index possible rather than eleven bespoke reads.
//
// Returns null on any Neon failure so the caller falls back to Airtable.
async function neonItemIndex() {
  const q = await neonQuery(
    `SELECT airtable_id, name, category, unit_of_measure, default_unit_cost, wire_ft_per_lb
       FROM inventory_items WHERE COALESCE(airtable_id,'') <> ''`);
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
async function airtableItemIndex() {
  const records = await fetchAll(API_ROOT_INV, "Inventory Items", {});
  const index = {};
  for (const r of records) {
    const f = r.fields || {};
    index[r.id] = {
      id:          r.id,
      name:        f["Item Name"] || r.id,
      cat:         f["Category"]?.name || f["Category"] || "",
      uom:         f["Unit of Measure"]?.name || f["Unit of Measure"] || "",
      cost:        Number(f["Default Unit Cost"] || 0),
      wireFtPerLb: Number(f["Wire ft/lb"] || 0),
    };
  }
  return index;
}

// What the eleven call sites actually use. Neon first, Airtable if Neon is down.
async function itemIndex() {
  return (await neonItemIndex()) || (await airtableItemIndex());
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
// Airtable stays the identity authority for this slice, so the record is
// created/updated there FIRST and its rec id is what Neon keys on.
//
// Fails SOFT (logged, swallowed) rather than closed, deliberately: unlike the
// expense push, every read here has a live Airtable fallback, and the loader
// (`loadInventoryReference`) is an idempotent catch-up that repairs any row this
// misses. Breaking "add an item" over a database blip would cost more than the
// staleness it prevents.
async function syncItemToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : (v ?? null));
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const q = await neonQuery(
    `INSERT INTO inventory_items
       (airtable_id, name, category, product_size, unit_of_measure, barcode,
        alternate_barcodes, default_unit_cost, wire_ft_per_lb, reorder_point, active, notes, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       name=EXCLUDED.name, category=EXCLUDED.category, product_size=EXCLUDED.product_size,
       unit_of_measure=EXCLUDED.unit_of_measure, barcode=EXCLUDED.barcode,
       alternate_barcodes=EXCLUDED.alternate_barcodes,
       default_unit_cost=EXCLUDED.default_unit_cost, wire_ft_per_lb=EXCLUDED.wire_ft_per_lb,
       reorder_point=EXCLUDED.reorder_point, active=EXCLUDED.active, notes=EXCLUDED.notes,
       synced_at=now()`,
    [rec.id, f["Item Name"] || "", sel(f["Category"]), sel(f["Product Size"]),
     sel(f["Unit of Measure"]), f["Barcode Value"] ?? null, f["Alternate Barcodes"] ?? null,
     num(f["Default Unit Cost"]), num(f["Wire ft/lb"]), num(f["Reorder Point"]),
     f["Active Item"] === true, f["Notes"] ?? null]);
  if (!q?.rows) console.error(`syncItemToNeon ${rec.id} failed (item will be stale until the next loader run): ${q?.error || "Neon unavailable"}`);
}

// ── KEEP NEON IN STEP AFTER A LEDGER WRITE (Step C) ────────────────────────
// On-hand is DERIVED in Neon (v_stock_on_hand), so a transaction that doesn't
// reach the ledger doesn't just go missing — it silently changes the stock
// figure for that item and location.
//
// ⚠ These fail SOFT, and that is a deliberate trade rather than an oversight.
// The expense push fails closed because it is idempotent on `Push ID`, so a
// retry is free. `submitCart` has NO idempotency key: telling the user to
// retry would risk logging the same material out of stock twice, which is
// worse than an on-hand figure that is briefly stale and repairable. The
// loader (`loadInventoryReference`) upserts on `airtable_id`, so re-running it
// heals anything this misses. Failures are logged loudly for that reason.
//
// Give it the record Airtable returned from the create.
async function syncTxnToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : (v ?? null));
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const q = await neonQuery(
    `INSERT INTO inventory_transactions
       (airtable_id, txn_name, txn_date, item_airtable_id, item_id, quantity, txn_type,
        from_location_airtable_id, from_location_id, to_location_airtable_id, to_location_id,
        unit_cost_snapshot, notes, entered_by, job_airtable_id, job_name,
        expense_created, push_id, synced_at)
     VALUES ($1,$2,$3,$4,(SELECT id FROM inventory_items WHERE airtable_id=$4),$5,$6,
             $7,(SELECT id FROM locations WHERE airtable_id=$7),
             $8,(SELECT id FROM locations WHERE airtable_id=$8),
             $9,$10,$11,$12,$13,$14,$15, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       txn_name=EXCLUDED.txn_name, txn_date=EXCLUDED.txn_date,
       item_airtable_id=EXCLUDED.item_airtable_id, item_id=EXCLUDED.item_id,
       quantity=EXCLUDED.quantity, txn_type=EXCLUDED.txn_type,
       from_location_airtable_id=EXCLUDED.from_location_airtable_id,
       from_location_id=EXCLUDED.from_location_id,
       to_location_airtable_id=EXCLUDED.to_location_airtable_id,
       to_location_id=EXCLUDED.to_location_id,
       unit_cost_snapshot=EXCLUDED.unit_cost_snapshot, notes=EXCLUDED.notes,
       entered_by=EXCLUDED.entered_by, job_airtable_id=EXCLUDED.job_airtable_id,
       job_name=EXCLUDED.job_name, expense_created=EXCLUDED.expense_created,
       push_id=EXCLUDED.push_id, synced_at=now()`,
    // `Name` is an Airtable formula and may be absent from a create response;
    // it is a display string ("TX-20260810-105119") that nothing computes from,
    // and the loader fills it in on the next run.
    [rec.id, f["Name"] ?? null, f["Transaction Date"] ?? null, lnk(f["Inventory Item"]),
     num(f["Quantity"]) ?? 0, sel(f["Transaction Type"]),
     lnk(f["From Location"]), lnk(f["To Location"]),
     num(f["Unit Cost (Snapshot)"]), f["Notes"] ?? null, f["Entered By"] ?? null,
     f["Job ID (Main)"] ?? null, f["Job Name"] ?? null,
     f["Expense Created?"] === true, f["Push ID"] ?? null]);
  if (!q?.rows) console.error(`syncTxnToNeon ${rec.id} FAILED — on-hand is now stale for this item until the loader re-runs: ${q?.error || "Neon unavailable"}`);
}

// A transaction deleted in Airtable must leave the Neon ledger too, or
// v_stock_on_hand keeps counting stock that was never really there.
async function deleteTxnFromNeon(airtableId) {
  if (!airtableId) return;
  const q = await neonQuery(`DELETE FROM inventory_transactions WHERE airtable_id = $1`, [airtableId]);
  if (!q?.rows) console.error(`deleteTxnFromNeon ${airtableId} FAILED — on-hand still counts this transaction: ${q?.error || "Neon unavailable"}`);
}

// ── KEEP NEON IN STEP AFTER AN ESTIMATE WRITE (Step D) ─────────────────────
// Same fail-soft contract as the ledger: every read here keeps a live Airtable
// fallback and the loader repairs anything missed, so breaking "save an
// estimate" over a database blip would cost more than the staleness.
//
// ⚠ Deletes are again the exception the loader cannot repair — it upserts and
// never removes — so an estimate or line deleted in Airtable but left in Neon
// would keep showing on the list and keep counting toward the total.
async function syncEstimateToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : (v ?? null));
  const q = await neonQuery(
    `INSERT INTO material_estimates
       (airtable_id, job_name, job_airtable_id, status, created_by, created_at, notes, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       job_name=EXCLUDED.job_name, job_airtable_id=EXCLUDED.job_airtable_id,
       status=EXCLUDED.status, created_by=EXCLUDED.created_by,
       created_at=COALESCE(material_estimates.created_at, EXCLUDED.created_at),
       notes=EXCLUDED.notes, synced_at=now()`,
    [rec.id, f["Job Name"] ?? null, f["Job ID"] ?? null, sel(f["Status"]),
     f["Created By"] ?? null, f["Date Created"] ?? rec.createdTime ?? null, f["Notes"] ?? null]);
  if (!q?.rows) console.error(`syncEstimateToNeon ${rec.id} failed (stale until loader): ${q?.error || "Neon unavailable"}`);
}

// Lines arrive in batches of 10 from createLineItems, so this takes an array.
async function syncEstimateLinesToNeon(recs) {
  const list = (recs || []).filter(r => r?.id);
  if (!list.length) return;
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const params = [];
  const tuples = list.map(r => {
    const f = r.fields || {};
    const vals = [r.id, num(f["Line ID"]), lnk(f["Estimate"]), lnk(f["Inventory Item"]),
                  num(f["Quantity"]) ?? 0, num(f["Unit Cost at Time of Estimate"]),
                  f["Description"] ?? null];
    const ph = vals.map(v => { params.push(v); return `$${params.length}`; });
    // FKs resolve inline so a new line is immediately visible to the totals view.
    return `(${ph[0]},${ph[1]},${ph[2]},(SELECT id FROM material_estimates WHERE airtable_id=${ph[2]}),` +
           `${ph[3]},(SELECT id FROM inventory_items WHERE airtable_id=${ph[3]}),${ph[4]},${ph[5]},${ph[6]}, now())`;
  });
  const q = await neonQuery(
    `INSERT INTO material_estimate_lines
       (airtable_id, line_number, estimate_airtable_id, estimate_id, item_airtable_id, item_id,
        quantity, unit_cost_at_estimate, description, synced_at)
     VALUES ${tuples.join(",")}
     ON CONFLICT (airtable_id) DO UPDATE SET
       line_number=EXCLUDED.line_number, estimate_airtable_id=EXCLUDED.estimate_airtable_id,
       estimate_id=EXCLUDED.estimate_id, item_airtable_id=EXCLUDED.item_airtable_id,
       item_id=EXCLUDED.item_id, quantity=EXCLUDED.quantity,
       unit_cost_at_estimate=EXCLUDED.unit_cost_at_estimate,
       description=EXCLUDED.description, synced_at=now()`, params);
  if (!q?.rows) console.error(`syncEstimateLinesToNeon (${list.length} lines) failed: ${q?.error || "Neon unavailable"}`);
}

async function syncTemplateToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const q = await neonQuery(
    `INSERT INTO material_estimate_templates
       (airtable_id, name, description, contractor, source_estimate_ref,
        total_at_save, active, created_by, created_at, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       name=EXCLUDED.name, description=EXCLUDED.description, contractor=EXCLUDED.contractor,
       source_estimate_ref=EXCLUDED.source_estimate_ref, total_at_save=EXCLUDED.total_at_save,
       active=EXCLUDED.active, created_by=EXCLUDED.created_by,
       created_at=COALESCE(material_estimate_templates.created_at, EXCLUDED.created_at),
       synced_at=now()`,
    [rec.id, f["Template Name"] || "", f["Description"] ?? null, f["Contractor"] ?? null,
     f["Source Estimate Reference"] ?? null, num(f["Total at Save"]),
     f["Active"] === true, f["Created By"] ?? null,
     f["Created Date"] ?? rec.createdTime ?? null]);
  if (!q?.rows) console.error(`syncTemplateToNeon ${rec.id} failed (stale until loader): ${q?.error || "Neon unavailable"}`);
}

async function syncTemplateLinesToNeon(recs) {
  const list = (recs || []).filter(r => r?.id);
  if (!list.length) return;
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const params = [];
  const tuples = list.map(r => {
    const f = r.fields || {};
    const vals = [r.id, f["Line Title"] ?? null, lnk(f["Template"]), lnk(f["Inventory Item"]),
                  num(f["Quantity"]) ?? 0, num(f["Unit Cost at Save"]), f["Notes"] ?? null];
    const ph = vals.map(v => { params.push(v); return `$${params.length}`; });
    return `(${ph[0]},${ph[1]},${ph[2]},(SELECT id FROM material_estimate_templates WHERE airtable_id=${ph[2]}),` +
           `${ph[3]},(SELECT id FROM inventory_items WHERE airtable_id=${ph[3]}),${ph[4]},${ph[5]},${ph[6]}, now())`;
  });
  const q = await neonQuery(
    `INSERT INTO material_estimate_template_lines
       (airtable_id, line_title, template_airtable_id, template_id, item_airtable_id, item_id,
        quantity, unit_cost_at_save, notes, synced_at)
     VALUES ${tuples.join(",")}
     ON CONFLICT (airtable_id) DO UPDATE SET
       line_title=EXCLUDED.line_title, template_airtable_id=EXCLUDED.template_airtable_id,
       template_id=EXCLUDED.template_id, item_airtable_id=EXCLUDED.item_airtable_id,
       item_id=EXCLUDED.item_id, quantity=EXCLUDED.quantity,
       unit_cost_at_save=EXCLUDED.unit_cost_at_save, notes=EXCLUDED.notes, synced_at=now()`, params);
  if (!q?.rows) console.error(`syncTemplateLinesToNeon (${list.length}) failed: ${q?.error || "Neon unavailable"}`);
}

async function deleteTemplateLinesFromNeon(ids) {
  const list = (ids || []).filter(Boolean);
  if (!list.length) return;
  const q = await neonQuery(`DELETE FROM material_estimate_template_lines WHERE airtable_id = ANY($1::text[])`, [list]);
  if (!q?.rows) console.error(`deleteTemplateLinesFromNeon failed — the template total still counts them: ${q?.error || "Neon unavailable"}`);
}

// FK is ON DELETE CASCADE, so the lines go with it.
async function deleteTemplateFromNeon(airtableId) {
  if (!airtableId) return;
  const q = await neonQuery(`DELETE FROM material_estimate_templates WHERE airtable_id = $1`, [airtableId]);
  if (!q?.rows) console.error(`deleteTemplateFromNeon ${airtableId} failed — it will keep showing in the picker: ${q?.error || "Neon unavailable"}`);
}

async function syncOrderToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : (v ?? null));
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const q = await neonQuery(
    `INSERT INTO material_orders
       (airtable_id, order_number, estimate_airtable_id, estimate_id, job_name,
        vendor_notes, status, order_type, created_by, created_at, synced_at)
     VALUES ($1,$2,$3,(SELECT id FROM material_estimates WHERE airtable_id=$3),$4,$5,$6,$7,$8,$9, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       order_number=COALESCE(EXCLUDED.order_number, material_orders.order_number),
       estimate_airtable_id=EXCLUDED.estimate_airtable_id, estimate_id=EXCLUDED.estimate_id,
       job_name=EXCLUDED.job_name, vendor_notes=EXCLUDED.vendor_notes,
       status=EXCLUDED.status, order_type=EXCLUDED.order_type, created_by=EXCLUDED.created_by,
       created_at=COALESCE(material_orders.created_at, EXCLUDED.created_at), synced_at=now()`,
    // `Order ID` is an Airtable autonumber and is absent from the create
    // response, which is why the handler re-fetches for it. COALESCE above keeps
    // whatever is already stored rather than nulling it on a later update.
    [rec.id, num(f["Order ID"]), lnk(f["Estimate"]), f["Job Name"] ?? null,
     f["Vendor / Notes"] ?? null, sel(f["Status"]), sel(f["Order Type"]),
     f["Created By"] ?? null, f["Date Created"] ?? rec.createdTime ?? null]);
  if (!q?.rows) console.error(`syncOrderToNeon ${rec.id} failed (stale until loader): ${q?.error || "Neon unavailable"}`);
}

async function syncOrderLinesToNeon(recs) {
  const list = (recs || []).filter(r => r?.id);
  if (!list.length) return;
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const params = [];
  const tuples = list.map(r => {
    const f = r.fields || {};
    const vals = [r.id, num(f["Line Item ID"]), lnk(f["Material Order"]), lnk(f["Inventory Item"]),
                  f["Description"] ?? null, num(f["Quantity Ordered"]) ?? 0,
                  num(f["Unit Cost at Time of Order"]), num(f["Line Total"]),
                  f["Received"] === true, f["Notes"] ?? null];
    const ph = vals.map(v => { params.push(v); return `$${params.length}`; });
    return `(${ph[0]},${ph[1]},${ph[2]},(SELECT id FROM material_orders WHERE airtable_id=${ph[2]}),` +
           `${ph[3]},(SELECT id FROM inventory_items WHERE airtable_id=${ph[3]}),` +
           `${ph[4]},${ph[5]},${ph[6]},${ph[7]},${ph[8]},${ph[9]}, now())`;
  });
  const q = await neonQuery(
    `INSERT INTO material_order_lines
       (airtable_id, line_number, order_airtable_id, order_id, item_airtable_id, item_id,
        description, quantity_ordered, unit_cost_at_order, line_total_stored, received, notes, synced_at)
     VALUES ${tuples.join(",")}
     ON CONFLICT (airtable_id) DO UPDATE SET
       line_number=COALESCE(EXCLUDED.line_number, material_order_lines.line_number),
       order_airtable_id=EXCLUDED.order_airtable_id, order_id=EXCLUDED.order_id,
       item_airtable_id=EXCLUDED.item_airtable_id, item_id=EXCLUDED.item_id,
       description=EXCLUDED.description, quantity_ordered=EXCLUDED.quantity_ordered,
       unit_cost_at_order=EXCLUDED.unit_cost_at_order,
       line_total_stored=EXCLUDED.line_total_stored, received=EXCLUDED.received,
       notes=EXCLUDED.notes, synced_at=now()`, params);
  if (!q?.rows) console.error(`syncOrderLinesToNeon (${list.length}) failed: ${q?.error || "Neon unavailable"}`);
}

async function deleteOrderLinesFromNeon(ids) {
  const list = (ids || []).filter(Boolean);
  if (!list.length) return;
  const q = await neonQuery(`DELETE FROM material_order_lines WHERE airtable_id = ANY($1::text[])`, [list]);
  if (!q?.rows) console.error(`deleteOrderLinesFromNeon failed — the order still lists them: ${q?.error || "Neon unavailable"}`);
}

async function deleteOrderFromNeon(airtableId) {
  if (!airtableId) return;
  const q = await neonQuery(`DELETE FROM material_orders WHERE airtable_id = $1`, [airtableId]);
  if (!q?.rows) console.error(`deleteOrderFromNeon ${airtableId} failed — it keeps showing on the list: ${q?.error || "Neon unavailable"}`);
}

async function deleteEstimateLinesFromNeon(ids) {
  const list = (ids || []).filter(Boolean);
  if (!list.length) return;
  const q = await neonQuery(`DELETE FROM material_estimate_lines WHERE airtable_id = ANY($1::text[])`, [list]);
  if (!q?.rows) console.error(`deleteEstimateLinesFromNeon failed — the estimate total still counts them: ${q?.error || "Neon unavailable"}`);
}

// The FK is ON DELETE CASCADE, so this takes the lines with it.
async function deleteEstimateFromNeon(airtableId) {
  if (!airtableId) return;
  const q = await neonQuery(`DELETE FROM material_estimates WHERE airtable_id = $1`, [airtableId]);
  if (!q?.rows) console.error(`deleteEstimateFromNeon ${airtableId} failed — it will keep showing on the list: ${q?.error || "Neon unavailable"}`);
}

// Reorder point / notes. Same fail-soft reasoning; these are settings, not money.
async function syncStockSettingToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const num = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const q = await neonQuery(
    `INSERT INTO stock_settings
       (airtable_id, item_airtable_id, item_id, location_airtable_id, location_id,
        reorder_point, notes, synced_at)
     VALUES ($1,$2,(SELECT id FROM inventory_items WHERE airtable_id=$2),
             $3,(SELECT id FROM locations WHERE airtable_id=$3),$4,$5, now())
     ON CONFLICT (airtable_id) DO UPDATE SET
       item_airtable_id=EXCLUDED.item_airtable_id, item_id=EXCLUDED.item_id,
       location_airtable_id=EXCLUDED.location_airtable_id, location_id=EXCLUDED.location_id,
       reorder_point=EXCLUDED.reorder_point, notes=EXCLUDED.notes, synced_at=now()`,
    [rec.id, lnk(f["Item"]), lnk(f["Location"]), num(f["Reorder Point"]), f["Notes"] ?? null]);
  if (!q?.rows) console.error(`syncStockSettingToNeon ${rec.id} failed (stale until loader): ${q?.error || "Neon unavailable"}`);
}

// Re-read the item from Airtable and sync it. Used by the cost writers, which
// PATCH a single field and so don't have a full record in hand.
async function syncItemToNeonById(itemId) {
  try {
    const rec = await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, { method: "GET" });
    if (rec?.id === itemId) await syncItemToNeon(rec);
  } catch (e) {
    console.error(`syncItemToNeonById ${itemId} failed (stale until next loader run): ${e.message}`);
  }
}

// ── LOCATIONS ──────────────────────────────────────────────
async function handleLocations() {
  // The table this migration exists for. In Airtable a location is a set of
  // field NAMES on Inventory Items; here it is a row you can insert.
  const q = await neonQuery(
    `SELECT airtable_id AS id, name, location_type AS type
       FROM locations WHERE active AND COALESCE(airtable_id,'') <> ''
      ORDER BY name ASC`);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      locations: q.rows.map(r => ({ id: r.id, name: r.name || "", type: r.type || "" })),
    });
  }

  const records = await fetchAll(API_ROOT_INV, "Locations", {
    filter: `{Active Location}=1`,
    sortField: "Location Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true, _source: "airtable",
    locations: records.map(r => ({
      id:   r.id,
      name: r.fields["Location Name"] || "",
      type: r.fields["Location Type"]?.name || ""
    }))
  });
}

// ── ITEMS ──────────────────────────────────────────────────
async function handleItems() {
  // 866 rows, the most-read table in the app. Note `size` (Product Size) is
  // returned here and nowhere else, so this read carries one column more than
  // the shared itemIndex() does — that is why it has its own query.
  const q = await neonQuery(
    `SELECT airtable_id AS id, name, category, product_size, unit_of_measure,
            barcode, default_unit_cost, wire_ft_per_lb
       FROM inventory_items
      WHERE active AND COALESCE(airtable_id,'') <> ''
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

  const records = await fetchAll(API_ROOT_INV, "Inventory Items", {
    filter: `{Active Item}=1`,
    sortField: "Item Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true, _source: "airtable",
    items: records.map(r => {
      const f = r.fields || {};
      return {
        id:          r.id,
        name:        f["Item Name"] || "",
        cat:         f["Category"]?.name || f["Category"] || "",
        size:        f["Product Size"]?.name || f["Product Size"] || "",
        uom:         f["Unit of Measure"]?.name || f["Unit of Measure"] || "",
        barcode:     f["Barcode Value"] || "",
        cost:        f["Default Unit Cost"] || 0,
        wireFtPerLb: f["Wire ft/lb"] || 0
      };
    })
  });
}

// ── SUBMIT CART (multiple transactions at once) ────────────
async function handleSubmitCart(body) {
  const { lines, jobName, jobId, locationId, enteredBy } = body || {};
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
  const results = [];

  for (const line of lines) {
    const fields = {
      "fldGq37LD9YuyCf5e": now,
      "fldmookC8mdyXxVuw": [String(line.itemId)],
      "fldFQlArrzUnjCTxr": Number(line.qty),
      "fldjvIy3X1DJowGsd": line.type || "Use",
      "fldpyLadbcc9NHO6c": [String(locationId)],
      "fldIFffLxtcQTbExd": enteredBy || "",
      "fldrcq8wSyfz8O3UB": line.notes || ""
    };

    // Snapshot the unit cost at transaction time. This freezes the cost
    // for billing so retroactive price changes do not alter historical job costs.
    // Priority: explicit unitCost from cart > current Default Unit Cost lookup.
    let snapshotCost = 0;
    if (line.unitCost !== undefined && line.unitCost !== null && Number(line.unitCost) > 0) {
      snapshotCost = Number(line.unitCost);
    } else if (itemCostMap[line.itemId] > 0) {
      snapshotCost = itemCostMap[line.itemId];
    }
    if (snapshotCost > 0) {
      fields["fldUStmydYotYBFoE"] = snapshotCost;
    }

    // Stamp the job as TEXT (Drop-Jobs-mirror bet, Step B). handleJobs now
    // returns MAIN NEE base record IDs, so jobId here is a main-base job id —
    // recorded as "Job ID (Main)" plus a human-readable "Job Name". The expense
    // push (handlePendingExpenses) groups by this id directly, killing the
    // name-matching join.
    //
    // We deliberately do NOT also write the old cross-base "Job" link
    // (fld7OG04Sgkp88JsU): that field links into the INVENTORY Jobs *mirror*, and
    // a main-base id can't be stored there. Transition safety for pre-Step-B
    // transactions (which still carry the link) lives in handlePendingExpenses,
    // which falls back to the link→name path when "Job ID (Main)" is blank. The
    // link field itself is removed in Step C.
    if (jobId) {
      fields["fldePDNz1zc2bmNkk"] = String(jobId);    // Job ID (Main)
    }
    if (jobName) {
      fields["fldZlC25ou4d6CzCl"] = String(jobName);  // Job Name (display)
    }
    const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields }], typecast: true })
    });
    const created = data.records?.[0];
    results.push(created?.id);
    await syncTxnToNeon(created);      // or on-hand silently ignores this usage
  }

  return resp(200, { ok: true, ids: results });
}

// ── RECEIVE ITEMS (with optional price update) ─────────────
async function handleReceive(body) {
  const { itemId, locationId, qty, unitCost, enteredBy, notes } = body || {};
  if (!itemId || !locationId || !qty) return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();
  const fields = {
    "fldGq37LD9YuyCf5e": now,
    "fldmookC8mdyXxVuw": [String(itemId)],
    "fldFQlArrzUnjCTxr": Number(qty),
    "fldjvIy3X1DJowGsd": "Receive",
    "fld5FZc9oBHNn4YF7": [String(locationId)],  // To Location
    "fldIFffLxtcQTbExd": enteredBy || "",
    "fldrcq8wSyfz8O3UB": notes || ""
  };

  // Snapshot the cost on receive transactions too — lets the receive history
  // show what was actually paid even if Default Unit Cost is updated later.
  if (unitCost && Number(unitCost) > 0) {
    fields["fldUStmydYotYBFoE"] = Number(unitCost);
  }

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  await syncTxnToNeon(data.records?.[0]);

  // Update unit cost on item if provided
  if (unitCost && Number(unitCost) > 0) {
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, {
      method: "PATCH",
      body: JSON.stringify({ fields: { "fld8aEhTzmEbqgIg4": Number(unitCost) } })
    });
    await syncItemToNeonById(itemId);   // receiving can move the item's cost too
  }

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── TRANSFER ───────────────────────────────────────────────
async function handleTransfer(body) {
  const { itemId, fromLocationId, toLocationId, qty, enteredBy, notes } = body || {};
  if (!itemId || !fromLocationId || !toLocationId || !qty)
    return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();
  const fields = {
    "fldGq37LD9YuyCf5e": now,
    "fldmookC8mdyXxVuw": [String(itemId)],
    "fldFQlArrzUnjCTxr": Number(qty),
    "fldjvIy3X1DJowGsd": "Transfer",
    "fldpyLadbcc9NHO6c": [String(fromLocationId)],
    "fld5FZc9oBHNn4YF7": [String(toLocationId)],
    "fldIFffLxtcQTbExd": enteredBy || "",
    "fldrcq8wSyfz8O3UB": notes || ""
  };

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  // A transfer is the one type that moves stock in TWO places at once, so a
  // missed sync skews both the source and the destination.
  await syncTxnToNeon(data.records?.[0]);

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── HISTORY ────────────────────────────────────────────────
async function handleHistory(params) {
  const { enteredBy, all } = params || {};

  // Neon path. The date string, the "job | notes" split and the
  // snapshot-cost-else-current-cost rule are all reproduced from the Airtable
  // branch below rather than reinvented — this feeds a screen people read.
  const nq = await neonQuery(
    `SELECT t.airtable_id, t.txn_date, t.quantity, t.txn_type, t.notes, t.entered_by,
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
          id:        r.airtable_id,
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

  const [txRecords, itemRecords, locRecords] = await Promise.all([
    fetchAll(API_ROOT_INV, "Inventory Transactions", {
      filter: all === "1" ? undefined : (enteredBy ? `{Entered By}='${enteredBy}'` : undefined),
      sortField: "Transaction Date",
      sortDir: "desc",
      maxRecords: 200
    }),
    itemIndex(),
    fetchAll(API_ROOT_INV, "Locations", {})
  ]);

  const itemMap = itemRecords;   // itemIndex() already returns { id -> {name, cost, uom, …} }
  const locMap = {};
  locRecords.forEach(r => { locMap[r.id] = r.fields["Location Name"] || r.id; });

  const resolveArr = (arr, map) => {
    if (!Array.isArray(arr) || !arr.length) return "";
    const first = arr[0];
    const id = typeof first === "object" ? first.id : String(first);
    return map[id] || id;
  };

  const transactions = txRecords.map(r => {
    const f = r.fields || {};
    const itemArr = f["Inventory Item"] || [];
    const fromArr = f["From Location"]  || [];
    const toArr   = f["To Location"]    || [];

    const itemId   = typeof itemArr[0] === "object" ? itemArr[0]?.id : String(itemArr[0] || "");
    const itemData = itemMap[itemId] || {};

    let dateStr = "";
    try {
      const d = new Date(f["Transaction Date"]);
      dateStr = d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
    } catch(e) {}

    const notesRaw   = f["Notes"] || "";
    const notesParts = notesRaw.split(" | ");
    const jobName    = notesParts[0] || "";
    const userNotes  = notesParts.slice(1).join(" | ");
    const qty        = f["Quantity"] ?? 0;

    // Prefer snapshot cost from the transaction; fall back to current item cost
    const snapshotCost = Number(f["Unit Cost (Snapshot)"] || 0);
    const lineCost = snapshotCost > 0 ? snapshotCost : (itemData.cost || 0);

    return {
      id:         r.id,
      date:       dateStr,
      item:       itemData.name || itemId,
      itemId:     itemId,
      uom:        itemData.uom  || "",
      cost:       lineCost,
      total:      Math.round(lineCost * qty * 100) / 100,
      from:       resolveArr(fromArr, locMap),
      to:         resolveArr(toArr,   locMap),
      qty,
      type:       f["Transaction Type"]?.name || f["Transaction Type"] || "",
      job:        jobName,
      notes:      userNotes,
      enteredBy:  f["Entered By"] || ""
    };
  });

  return resp(200, { ok: true, transactions });
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
      `SELECT airtable_id, item_airtable_id, quantity, txn_type, unit_cost_snapshot,
              job_airtable_id, job_name
         FROM inventory_transactions
        WHERE expense_created = false AND txn_type IN ('Use','Return')
        ORDER BY txn_date ASC NULLS LAST`);
    if (!q?.rows) return null;
    return q.rows.map(r => ({
      id: r.airtable_id,
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

  const [txRecords, itemRecords, mainJobs] = await Promise.all([
    pendingFromNeon().then(rows => rows ?? fetchAll(API_ROOT_INV, "Inventory Transactions", {
      filter: `AND(OR({Transaction Type}='Use', {Transaction Type}='Return'), NOT({Expense Created?}=1))`,
      sortField: "Transaction Date",
      sortDir: "asc"
    })),
    itemIndex(),
    mainJobIndex()
  ]);

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
const F_PUSH_TITLE       = "fldtLnuQkc2DOjuLf";
const F_PUSH_DATE        = "fldpCqWwprRsFLlWZ";
const F_PUSH_BY          = "fld94yrSbW5Pgy3R7";
const F_PUSH_JOB_NAME    = "flduHf59GYpd2BpY4";
const F_PUSH_JOB_ID_MAIN = "fldEKePmtgCIbBuQi";
const F_PUSH_MAT_TOTAL   = "fldX3sYU1i6c0nk9I";
const F_PUSH_TAX_TOTAL   = "fldKPeCOawEogqZ0B";
const F_PUSH_TOTAL       = "fldMmYbl7vDtcRDTc";
const F_PUSH_TX_COUNT    = "fldorTygoGLOB0Kkv";
const F_PUSH_ITEM_COUNT  = "fldPvphfqpuYQZhgd";
const F_PUSH_TAXABLE     = "fldX5Drh72lqcEUvR";
const F_PUSH_EXP_IDS     = "fldZNQLIlZbQPMF9B";
const F_PUSH_DESCRIPTION = "fldswL4blm5aFx14G";
const F_PUSH_PUSHID      = "fldpGddBp19KLT7dW";  // idempotency key (one per job group)

// Expense Push Lines fields
const F_PL_TITLE     = "fld7XZyGWzWKC2H1O";
const F_PL_PUSH      = "fldXIWYGSqLvcs2tJ";
const F_PL_ITEM_NAME = "fldz2kyBPufNRUzuj";
const F_PL_QTY       = "fldhzxPwqz9OPRT1i";
const F_PL_UNIT_COST = "fldIAkYTpZFnoYIY7";
const F_PL_LINE_TOT  = "fldzwMnfOwszQ6ozH";
const F_PL_WIRE_FT   = "fldvpWg3Ky74GiE7q";

// ── Write a Push History header + lines to the inventory base.
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

    const headerFields = {};
    headerFields[F_PUSH_TITLE]       = title;
    headerFields[F_PUSH_DATE]        = iso;
    headerFields[F_PUSH_BY]          = String(pushedBy || "");
    headerFields[F_PUSH_JOB_NAME]    = String(jobName || "");
    headerFields[F_PUSH_JOB_ID_MAIN] = String(mainJobId || "");
    headerFields[F_PUSH_MAT_TOTAL]   = Math.round(Number(materialsTotal || 0) * 100) / 100;
    headerFields[F_PUSH_TAX_TOTAL]   = Math.round(Number(taxTotal || 0) * 100) / 100;
    headerFields[F_PUSH_TOTAL]       = Math.round((Number(materialsTotal || 0) + Number(taxTotal || 0)) * 100) / 100;
    headerFields[F_PUSH_TX_COUNT]    = Number(txCount || 0);
    headerFields[F_PUSH_ITEM_COUNT]  = (lines || []).length;
    headerFields[F_PUSH_TAXABLE]     = !!taxable;
    headerFields[F_PUSH_EXP_IDS]     = (expenseIds || []).join(", ");
    headerFields[F_PUSH_DESCRIPTION] = String(description || "");
    if (pushId) headerFields[F_PUSH_PUSHID] = String(pushId);

    const created = await atFetch(API_ROOT_INV, encodeURIComponent("Expense Pushes"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields: headerFields }], typecast: true })
    });
    const pushHeaderId = created.records?.[0]?.id;
    if (!pushHeaderId) {
      console.warn("Push History: header create returned no ID");
      return null;
    }

    // Write line snapshots in batches of 10 — pure best-effort, errors are non-fatal
    const lineRecords = (lines || []).map(l => {
      const itemName = String(l.item || "Item").substring(0, 100);
      const qty      = Number(l.qty || 0);
      const cost     = Number(l.cost || 0);
      const total    = Number(l.total || 0);
      const wireFt   = Number(l.wireFt || 0);
      const lineTitle = `${itemName} × ${qty}`.substring(0, 100);

      const f = {};
      f[F_PL_TITLE]     = lineTitle;
      f[F_PL_PUSH]      = [String(pushHeaderId)];
      f[F_PL_ITEM_NAME] = itemName;
      f[F_PL_QTY]       = qty;
      f[F_PL_UNIT_COST] = cost;
      f[F_PL_LINE_TOT]  = total;
      if (wireFt > 0) f[F_PL_WIRE_FT] = wireFt;
      return { fields: f };
    });

    for (let i = 0; i < lineRecords.length; i += 10) {
      const batch = lineRecords.slice(i, i + 10);
      try {
        await atFetch(API_ROOT_INV, encodeURIComponent("Expense Push Lines"), {
          method: "POST",
          body: JSON.stringify({ records: batch, typecast: true })
        });
      } catch(lineErr) {
        console.warn("Push History: line batch write failed (non-fatal):", lineErr.message);
      }
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
  const records = await fetchAll(API_ROOT_INV, "Expense Pushes", {
    sortField: "Date Pushed",
    sortDir: "desc",
    maxRecords: limit
  });

  const pushes = records.map(r => {
    const f = r.fields || {};
    return {
      id:            r.id,
      title:         f["Push Title"] || "",
      datePushed:    f["Date Pushed"] || "",
      pushedBy:      f["Pushed By"] || "",
      jobName:       f["Job Name"] || "",
      jobIdMain:     f["Job ID (Main)"] || "",
      materialsTotal: Number(f["Materials Total"] || 0),
      taxTotal:      Number(f["Tax Total"] || 0),
      total:         Number(f["Total Pushed"] || 0),
      txCount:       Number(f["Tx Count"] || 0),
      itemCount:     Number(f["Item Count"] || 0),
      taxable:       !!f["Taxable"],
      expenseIds:    f["Expense Record IDs"] || "",
      description:   f["Description"] || ""
    };
  });

  return resp(200, { ok: true, pushes });
}

// ── PUSH HISTORY DETAIL (one push with its line snapshots) ───
async function handlePushHistoryDetail(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing push id." });

  // Fetch header
  const headerData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Expense Pushes")}/${id}`,
    { method: "GET" }
  );
  if (!headerData?.id) return resp(404, { ok: false, error: "Push not found." });
  const f = headerData.fields || {};

  // Fetch lines linked to this push — pull all lines and filter client-side
  // (small table, simpler than building a complex formula filter)
  const allLines = await fetchAll(API_ROOT_INV, "Expense Push Lines", {});
  const lines = allLines
    .filter(r => {
      const linkArr = r.fields?.["Push"] || [];
      return linkArr.some(link => {
        const linkId = typeof link === "object" ? link.id : String(link);
        return linkId === id;
      });
    })
    .map(r => {
      const lf = r.fields || {};
      return {
        id:        r.id,
        itemName:  lf["Item Name"] || lf["Line Title"] || "Item",
        qty:       Number(lf["Quantity"] || 0),
        unitCost:  Number(lf["Unit Cost"] || 0),
        lineTotal: Number(lf["Line Total"] || 0),
        wireFt:    Number(lf["Wire Ft"] || 0)
      };
    })
    .sort((a, b) => b.lineTotal - a.lineTotal);  // biggest dollars first

  return resp(200, {
    ok: true,
    push: {
      id:            headerData.id,
      title:         f["Push Title"] || "",
      datePushed:    f["Date Pushed"] || "",
      pushedBy:      f["Pushed By"] || "",
      jobName:       f["Job Name"] || "",
      jobIdMain:     f["Job ID (Main)"] || "",
      materialsTotal: Number(f["Materials Total"] || 0),
      taxTotal:      Number(f["Tax Total"] || 0),
      total:         Number(f["Total Pushed"] || 0),
      txCount:       Number(f["Tx Count"] || 0),
      itemCount:     Number(f["Item Count"] || 0),
      taxable:       !!f["Taxable"],
      expenseIds:    f["Expense Record IDs"] || "",
      description:   f["Description"] || "",
      lines
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
const TX_PUSH_ID_FIELD  = "fldv9iY9ZKrV1SOsA";  // Inventory Transactions -> Push ID (inv base)
const TX_EXP_CREATED    = "fldO7Z0L7tpAvrgtH";  // Inventory Transactions -> Expense Created?

// Mark a group's transactions as pushed and stamp the push ID. Batched by 10
// (Airtable's PATCH cap). Called per-group right after that group's expense is
// created so a failure later in the loop can't strand it unmarked.
async function markTransactionsPushed(txIds, pushId) {
  for (let i = 0; i < txIds.length; i += 10) {
    const batch = txIds.slice(i, i + 10).map(id => ({
      id,
      fields: { [TX_EXP_CREATED]: true, [TX_PUSH_ID_FIELD]: String(pushId || "") }
    }));
    await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
      method: "PATCH",
      body: JSON.stringify({ records: batch })
    });
    // Mirror the same two columns. `expense_created` is what the pending-expenses
    // read filters on, so leaving Neon un-marked would offer the same materials
    // for pushing again — the double-charge these guards exist to prevent.
    const q = await neonQuery(
      `UPDATE inventory_transactions
          SET expense_created = true, push_id = $2, synced_at = now()
        WHERE airtable_id = ANY($1::text[])`,
      [batch.map(b => b.id), String(pushId || "")]);
    if (!q?.rows) console.error(`markTransactionsPushed: Neon not marked (${batch.length} txns may be re-offered until the loader runs): ${q?.error || "Neon unavailable"}`);
  }
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
  const nqFresh = await neonQuery(
    `SELECT airtable_id FROM inventory_transactions
      WHERE expense_created = false AND txn_type IN ('Use','Return')`);
  const freshTx = nqFresh?.rows
    ? nqFresh.rows.map(r => ({ id: r.airtable_id }))
    : await fetchAll(API_ROOT_INV, "Inventory Transactions", {
        filter: `AND(OR({Transaction Type}='Use', {Transaction Type}='Return'), NOT({Expense Created?}=1))`
      });
  const stillPending = new Set(freshTx.map(r => r.id));

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

// ── STEP B: LOAD THE REFERENCE TABLES INTO NEON (admin action) ─────────────
// Locations, Vendors, Inventory Items, Vendor Pricing → Neon (029 schema).
//
// WHY THIS RUNS IN THE FUNCTION rather than as a script in db/etl/. Nothing on
// a dev machine can read the inventory base: both PATs in `.env` return 403 on
// `appfsLJwfow4CepCw` (one is main-base scoped, the other points at the
// sandbox). The deployed function holds the credential that can. Same reason
// `copyAirtablePhotosToR2` runs here — the credential lives where the function
// does, not where the developer does.
//
// Idempotent: every table upserts ON CONFLICT (airtable_id), so re-running
// refreshes rather than duplicating. Safe to run as often as you like, and it
// is the catch-up path until the write handlers move in a later slice.
//
//   POST { action: "loadInventoryReference" }        (admin only)
async function handleLoadInventoryReference() {
  // Chunked multi-row upsert. 866 items in one statement would build a
  // parameter list Postgres rejects; 100 keeps it comfortable.
  async function upsertChunked(label, table, cols, rows, updateCols) {
    let written = 0;
    for (let i = 0; i < rows.length; i += 100) {
      const chunk = rows.slice(i, i + 100);
      const params = [];
      const tuples = chunk.map(r => {
        const ph = cols.map(c => { params.push(r[c] ?? null); return `$${params.length}`; });
        return `(${ph.join(",")}, now())`;
      });
      const q = await neonQuery(
        `INSERT INTO ${table} (${cols.join(",")}, synced_at) VALUES ${tuples.join(",")}
         ON CONFLICT (airtable_id) DO UPDATE SET
           ${updateCols.map(c => `${c}=EXCLUDED.${c}`).join(", ")}, synced_at=now()`,
        params
      );
      if (!q?.rows) throw new Error(`${label}: ${q?.error || "Neon unavailable"}`);
      written += chunk.length;
    }
    return written;
  }

  const bool = (v) => v === true;
  const sel  = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : (v ?? null));
  const num  = (v) => { const x = Number(v); return Number.isFinite(x) ? x : null; };
  const lnk  = (v) => { const a = Array.isArray(v) ? v[0] : v; return a ? (typeof a === "object" ? a.id : String(a)) : null; };
  const date = (v) => (v ? String(v).slice(0, 10) : null);

  const [locs, vends, items, pricing] = await Promise.all([
    fetchAll(API_ROOT_INV, "Locations", {}),
    fetchAll(API_ROOT_INV, "Vendors", {}),
    fetchAll(API_ROOT_INV, "Inventory Items", {}),
    fetchAll(API_ROOT_INV, "Vendor Pricing", {}),
  ]);

  const counts = {};

  counts.locations = await upsertChunked("locations", "locations",
    ["airtable_id", "name", "location_type", "active", "notes"],
    locs.map(r => ({ airtable_id: r.id, name: r.fields?.["Location Name"] || "",
      location_type: sel(r.fields?.["Location Type"]), active: bool(r.fields?.["Active Location"]),
      notes: r.fields?.["Notes"] ?? null })),
    ["name", "location_type", "active", "notes"]);

  counts.vendors = await upsertChunked("vendors", "vendors",
    ["airtable_id", "name", "vendor_type", "account_number", "phone", "email", "website",
     "address", "primary_contact", "payment_terms", "active", "notes"],
    vends.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      name: f["Vendor Name"] || "", vendor_type: sel(f["Vendor Type"]),
      account_number: f["Account Number"] ?? null, phone: f["Phone"] ?? null,
      email: f["Email"] ?? null, website: f["Website"] ?? null, address: f["Address"] ?? null,
      primary_contact: f["Primary Contact"] ?? null, payment_terms: sel(f["Payment Terms"]),
      active: bool(f["Active"]), notes: f["Notes"] ?? null }; }),
    ["name", "vendor_type", "account_number", "phone", "email", "website", "address",
     "primary_contact", "payment_terms", "active", "notes"]);

  counts.items = await upsertChunked("inventory_items", "inventory_items",
    ["airtable_id", "name", "category", "product_size", "unit_of_measure", "barcode",
     "alternate_barcodes", "default_unit_cost", "wire_ft_per_lb", "reorder_point", "active", "notes"],
    items.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      name: f["Item Name"] || "", category: sel(f["Category"]), product_size: sel(f["Product Size"]),
      unit_of_measure: sel(f["Unit of Measure"]), barcode: f["Barcode Value"] ?? null,
      alternate_barcodes: f["Alternate Barcodes"] ?? null,
      default_unit_cost: num(f["Default Unit Cost"]), wire_ft_per_lb: num(f["Wire ft/lb"]),
      reorder_point: num(f["Reorder Point"]), active: bool(f["Active Item"]),
      notes: f["Notes"] ?? null }; }),
    ["name", "category", "product_size", "unit_of_measure", "barcode", "alternate_barcodes",
     "default_unit_cost", "wire_ft_per_lb", "reorder_point", "active", "notes"]);

  counts.vendorPricing = await upsertChunked("vendor_pricing", "vendor_pricing",
    ["airtable_id", "item_airtable_id", "vendor_airtable_id", "active", "preferred", "unit_cost",
     "unit_of_measure", "vendor_part_number", "min_order_qty", "lead_time_days",
     "last_price_update", "price_valid_until", "notes"],
    pricing.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      item_airtable_id: lnk(f["Inventory Item"]), vendor_airtable_id: lnk(f["Vendor"]),
      active: bool(f["Active"]), preferred: bool(f["Preferred for This Item"]),
      unit_cost: num(f["Unit Cost"]), unit_of_measure: sel(f["Unit of Measure"]),
      vendor_part_number: f["Vendor Part Number"] ?? null, min_order_qty: num(f["Min Order Qty"]),
      lead_time_days: num(f["Lead Time (days)"]), last_price_update: date(f["Last Price Update"]),
      price_valid_until: date(f["Price Valid Until"]), notes: f["Notes"] ?? null }; }),
    ["item_airtable_id", "vendor_airtable_id", "active", "preferred", "unit_cost",
     "unit_of_measure", "vendor_part_number", "min_order_qty", "lead_time_days",
     "last_price_update", "price_valid_until", "notes"]);

  // ── Step C: the ledger, and the settings hiding inside Stock Levels ──────
  // ⚠⚠ `Quantity On Hand` is NOT loaded. It is an automation-maintained cache
  // that disagrees with the raw ledger on 237 of 269 item+location pairs, while
  // the Inventory Items rollups reproduce that ledger EXACTLY on 4,330. On-hand
  // is therefore recomputed by `v_stock_on_hand` rather than copied — see
  // db/schema/032. Only Reorder Point and Notes come across, because only they
  // are things a human typed.
  const [txns, stock] = await Promise.all([
    fetchAll(API_ROOT_INV, "Inventory Transactions", {}),
    fetchAll(API_ROOT_INV, "Stock Levels", {}),
  ]);

  counts.transactions = await upsertChunked("inventory_transactions", "inventory_transactions",
    ["airtable_id", "txn_name", "txn_date", "item_airtable_id", "quantity", "txn_type",
     "from_location_airtable_id", "to_location_airtable_id", "unit_cost_snapshot",
     "notes", "entered_by", "job_airtable_id", "job_name", "expense_created", "push_id"],
    txns.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      txn_name: f["Name"] ?? null, txn_date: f["Transaction Date"] ?? null,
      item_airtable_id: lnk(f["Inventory Item"]), quantity: num(f["Quantity"]) ?? 0,
      txn_type: sel(f["Transaction Type"]),
      from_location_airtable_id: lnk(f["From Location"]),
      to_location_airtable_id: lnk(f["To Location"]),
      unit_cost_snapshot: num(f["Unit Cost (Snapshot)"]),
      notes: f["Notes"] ?? null, entered_by: f["Entered By"] ?? null,
      job_airtable_id: f["Job ID (Main)"] ?? null, job_name: f["Job Name"] ?? null,
      expense_created: bool(f["Expense Created?"]), push_id: f["Push ID"] ?? null }; }),
    ["txn_name", "txn_date", "item_airtable_id", "quantity", "txn_type",
     "from_location_airtable_id", "to_location_airtable_id", "unit_cost_snapshot",
     "notes", "entered_by", "job_airtable_id", "job_name", "expense_created", "push_id"]);

  counts.stockSettings = await upsertChunked("stock_settings", "stock_settings",
    ["airtable_id", "item_airtable_id", "location_airtable_id", "reorder_point", "notes"],
    stock.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      item_airtable_id: lnk(f["Item"]), location_airtable_id: lnk(f["Location"]),
      reorder_point: num(f["Reorder Point"]), notes: f["Notes"] ?? null }; }),
    ["item_airtable_id", "location_airtable_id", "reorder_point", "notes"]);

  // ── Step D: estimating ───────────────────────────────────────────────────
  // ⚠ `material_` prefix throughout — Neon's `job_estimates` is the MAIN base's
  // and feeds GP. See db/schema/035.
  // ⚠ Rollups/counts/formulas are NOT loaded: Total, Line Total, Total Items and
  // $ Current Line Total are views. A derived number that gets stored is how the
  // Stock Levels cache drifted.
  const [ests, estLines, tmpls, tmplLines, orders, orderLines] = await Promise.all([
    fetchAll(API_ROOT_INV, "Estimates", {}),
    fetchAll(API_ROOT_INV, "Estimate Line Items", {}),
    fetchAll(API_ROOT_INV, "Estimate Templates", {}),
    fetchAll(API_ROOT_INV, "Estimate Template Lines", {}),
    fetchAll(API_ROOT_INV, "Material Orders", {}),
    fetchAll(API_ROOT_INV, "Material Order Lines", {}),
  ]);

  counts.estimates = await upsertChunked("material_estimates", "material_estimates",
    ["airtable_id", "job_name", "job_airtable_id", "status", "created_by", "created_at", "notes"],
    ests.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      job_name: f["Job Name"] ?? null, job_airtable_id: f["Job ID"] ?? null,
      status: sel(f["Status"]), created_by: f["Created By"] ?? null,
      created_at: f["Date Created"] ?? r.createdTime ?? null, notes: f["Notes"] ?? null }; }),
    ["job_name", "job_airtable_id", "status", "created_by", "created_at", "notes"]);

  counts.estimateLines = await upsertChunked("material_estimate_lines", "material_estimate_lines",
    ["airtable_id", "line_number", "estimate_airtable_id", "item_airtable_id", "quantity",
     "unit_cost_at_estimate", "description"],
    estLines.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      line_number: num(f["Line ID"]), estimate_airtable_id: lnk(f["Estimate"]),
      item_airtable_id: lnk(f["Inventory Item"]), quantity: num(f["Quantity"]) ?? 0,
      unit_cost_at_estimate: num(f["Unit Cost at Time of Estimate"]),
      description: f["Description"] ?? null }; }),
    ["line_number", "estimate_airtable_id", "item_airtable_id", "quantity",
     "unit_cost_at_estimate", "description"]);

  counts.templates = await upsertChunked("material_estimate_templates", "material_estimate_templates",
    ["airtable_id", "name", "description", "contractor", "source_estimate_ref",
     "total_at_save", "active", "created_by", "created_at"],
    tmpls.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      name: f["Template Name"] || "", description: f["Description"] ?? null,
      contractor: f["Contractor"] ?? null, source_estimate_ref: f["Source Estimate Reference"] ?? null,
      total_at_save: num(f["Total at Save"]), active: bool(f["Active"]),
      created_by: f["Created By"] ?? null, created_at: f["Created Date"] ?? r.createdTime ?? null }; }),
    ["name", "description", "contractor", "source_estimate_ref", "total_at_save",
     "active", "created_by", "created_at"]);

  counts.templateLines = await upsertChunked("material_estimate_template_lines", "material_estimate_template_lines",
    ["airtable_id", "line_title", "template_airtable_id", "item_airtable_id", "quantity",
     "unit_cost_at_save", "notes"],
    tmplLines.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      line_title: f["Line Title"] ?? null, template_airtable_id: lnk(f["Template"]),
      item_airtable_id: lnk(f["Inventory Item"]), quantity: num(f["Quantity"]) ?? 0,
      unit_cost_at_save: num(f["Unit Cost at Save"]), notes: f["Notes"] ?? null }; }),
    ["line_title", "template_airtable_id", "item_airtable_id", "quantity",
     "unit_cost_at_save", "notes"]);

  counts.orders = await upsertChunked("material_orders", "material_orders",
    ["airtable_id", "order_number", "estimate_airtable_id", "job_name", "vendor_notes",
     "status", "order_type", "created_by", "created_at"],
    orders.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      order_number: num(f["Order ID"]), estimate_airtable_id: lnk(f["Estimate"]),
      job_name: f["Job Name"] ?? null, vendor_notes: f["Vendor / Notes"] ?? null,
      status: sel(f["Status"]), order_type: sel(f["Order Type"]),
      created_by: f["Created By"] ?? null, created_at: f["Date Created"] ?? r.createdTime ?? null }; }),
    ["order_number", "estimate_airtable_id", "job_name", "vendor_notes", "status",
     "order_type", "created_by", "created_at"]);

  counts.orderLines = await upsertChunked("material_order_lines", "material_order_lines",
    ["airtable_id", "line_number", "order_airtable_id", "item_airtable_id", "description",
     "quantity_ordered", "unit_cost_at_order", "line_total_stored", "received", "notes"],
    orderLines.map(r => { const f = r.fields || {}; return { airtable_id: r.id,
      line_number: num(f["Line Item ID"]), order_airtable_id: lnk(f["Material Order"]),
      item_airtable_id: lnk(f["Inventory Item"]), description: f["Description"] ?? null,
      quantity_ordered: num(f["Quantity Ordered"]) ?? 0,
      unit_cost_at_order: num(f["Unit Cost at Time of Order"]),
      line_total_stored: num(f["Line Total"]), received: bool(f["Received"]),
      notes: f["Notes"] ?? null }; }),
    ["line_number", "order_airtable_id", "item_airtable_id", "description", "quantity_ordered",
     "unit_cost_at_order", "line_total_stored", "received", "notes"]);

  // Resolve the real FKs from the Airtable ids. Done as a second pass because
  // the parents have to exist first, and re-run each time so a pricing row
  // loaded before its item still ends up linked.
  const fk = await neonQuery(
    `UPDATE vendor_pricing p SET
       item_id   = (SELECT i.id FROM inventory_items i WHERE i.airtable_id = p.item_airtable_id),
       vendor_id = (SELECT v.id FROM vendors v         WHERE v.airtable_id = p.vendor_airtable_id)
     WHERE p.item_id IS DISTINCT FROM (SELECT i.id FROM inventory_items i WHERE i.airtable_id = p.item_airtable_id)
        OR p.vendor_id IS DISTINCT FROM (SELECT v.id FROM vendors v      WHERE v.airtable_id = p.vendor_airtable_id)`);
  if (!fk?.rows) throw new Error(`vendor_pricing FK resolve: ${fk?.error || "Neon unavailable"}`);

  // Same second pass for the ledger and the stock settings. v_stock_on_hand
  // groups on item_id/location_id, so a row whose FKs never resolve is simply
  // absent from on-hand — hence the orphan report below rather than silence.
  const fkTxn = await neonQuery(
    `UPDATE inventory_transactions t SET
       item_id          = (SELECT i.id FROM inventory_items i WHERE i.airtable_id = t.item_airtable_id),
       from_location_id = (SELECT l.id FROM locations l WHERE l.airtable_id = t.from_location_airtable_id),
       to_location_id   = (SELECT l.id FROM locations l WHERE l.airtable_id = t.to_location_airtable_id)`);
  if (!fkTxn?.rows) throw new Error(`inventory_transactions FK resolve: ${fkTxn?.error || "Neon unavailable"}`);

  const fkStock = await neonQuery(
    `UPDATE stock_settings s SET
       item_id     = (SELECT i.id FROM inventory_items i WHERE i.airtable_id = s.item_airtable_id),
       location_id = (SELECT l.id FROM locations l WHERE l.airtable_id = s.location_airtable_id)`);
  if (!fkStock?.rows) throw new Error(`stock_settings FK resolve: ${fkStock?.error || "Neon unavailable"}`);

  // Step D's four child tables. Parents (estimates, templates, orders, items)
  // all exist by now, and re-running relinks anything loaded out of order.
  for (const [label, sql] of [
    ["material_estimate_lines", `UPDATE material_estimate_lines x SET
        estimate_id = (SELECT e.id FROM material_estimates e WHERE e.airtable_id = x.estimate_airtable_id),
        item_id     = (SELECT i.id FROM inventory_items i    WHERE i.airtable_id = x.item_airtable_id)`],
    ["material_estimate_template_lines", `UPDATE material_estimate_template_lines x SET
        template_id = (SELECT t.id FROM material_estimate_templates t WHERE t.airtable_id = x.template_airtable_id),
        item_id     = (SELECT i.id FROM inventory_items i             WHERE i.airtable_id = x.item_airtable_id)`],
    ["material_orders", `UPDATE material_orders x SET
        estimate_id = (SELECT e.id FROM material_estimates e WHERE e.airtable_id = x.estimate_airtable_id)`],
    ["material_order_lines", `UPDATE material_order_lines x SET
        order_id = (SELECT o.id FROM material_orders o    WHERE o.airtable_id = x.order_airtable_id),
        item_id  = (SELECT i.id FROM inventory_items i    WHERE i.airtable_id = x.item_airtable_id)`],
  ]) {
    const r = await neonQuery(sql);
    if (!r?.rows) throw new Error(`${label} FK resolve: ${r?.error || "Neon unavailable"}`);
  }

  // Report anything that couldn't be linked rather than leaving it silent — an
  // unlinked pricing row is invisible to v_item_live_cost.
  const orphan = await neonQuery(
    `SELECT
       (SELECT count(*) FROM vendor_pricing
         WHERE (item_airtable_id IS NOT NULL AND item_id IS NULL)
            OR (vendor_airtable_id IS NOT NULL AND vendor_id IS NULL))::int AS pricing,
       (SELECT count(*) FROM inventory_transactions
         WHERE item_airtable_id IS NOT NULL AND item_id IS NULL)::int AS txn_no_item,
       (SELECT count(*) FROM inventory_transactions
         WHERE from_location_id IS NULL AND to_location_id IS NULL)::int AS txn_no_location,
       (SELECT count(*) FROM stock_settings
         WHERE item_id IS NULL OR location_id IS NULL)::int AS stock_unlinked,
       (SELECT count(*) FROM material_estimate_lines
         WHERE estimate_airtable_id IS NOT NULL AND estimate_id IS NULL)::int AS est_lines_orphaned,
       (SELECT count(*) FROM material_estimate_template_lines
         WHERE template_airtable_id IS NOT NULL AND template_id IS NULL)::int AS tmpl_lines_orphaned,
       (SELECT count(*) FROM material_order_lines
         WHERE order_airtable_id IS NOT NULL AND order_id IS NULL)::int AS order_lines_orphaned`);

  return resp(200, {
    ok: true,
    airtable: { locations: locs.length, vendors: vends.length, items: items.length,
                vendorPricing: pricing.length, transactions: txns.length, stockSettings: stock.length,
                estimates: ests.length, estimateLines: estLines.length,
                templates: tmpls.length, templateLines: tmplLines.length,
                orders: orders.length, orderLines: orderLines.length },
    written: counts,
    // Rows whose FKs didn't resolve. A transaction with no location is invisible
    // to on-hand, so this is reported rather than swallowed.
    unlinked: orphan?.rows?.[0] ?? null,
  });
}

// ── ADJUSTMENT ─────────────────────────────────────────────
async function handleAdjustment(body) {
  const { itemId, locationId, qty, enteredBy, notes } = body || {};
  if (!itemId || !locationId || qty === undefined) return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();
  const fields = {
    "fldGq37LD9YuyCf5e": now,
    "fldmookC8mdyXxVuw": [String(itemId)],
    "fldFQlArrzUnjCTxr": Number(qty),
    "fldjvIy3X1DJowGsd": "Adjustment",
    "fldpyLadbcc9NHO6c": [String(locationId)],
    "fldIFffLxtcQTbExd": enteredBy || "",
    "fldrcq8wSyfz8O3UB": notes || ""
  };

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  // This is the write the counting day runs on: an Adjustment is how a physical
  // count becomes truth. If it misses Neon, the corrected figure never appears.
  await syncTxnToNeon(data.records?.[0]);

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── CREATE NEW ITEM ───────────────────────────────────────
async function handleCreateItem(body) {
  const { name, category, productSize, uom, barcode, cost, wireFtPerLb, active } = body || {};
  if (!name || !name.trim()) return resp(400, { ok: false, error: "Item name is required." });

  // Check for duplicate barcode. Neon-first — this is the indexed lookup the
  // barcode index in 029 exists for — with the Airtable scan as the fallback.
  // Note the guard must fail OPEN: if neither store can answer, creating a
  // possible duplicate is better than blocking the add, and the duplicate is
  // visible and fixable where a blocked add is just a dead end.
  if (barcode && barcode.trim()) {
    const bc = barcode.trim();
    const q = await neonQuery(
      `SELECT name FROM inventory_items WHERE barcode = $1 LIMIT 1`, [bc]);
    let clash = q?.rows ? (q.rows[0]?.name || null) : undefined;
    if (clash === undefined) {
      const existing = await fetchAll(API_ROOT_INV, "Inventory Items", {
        filter: `{Barcode Value}='${bc}'`
      });
      clash = existing.length ? (existing[0].fields["Item Name"] || "another item") : null;
    }
    if (clash) return resp(409, { ok: false, error: `Barcode already used by: ${clash}` });
  }

  // Use field NAMES with typecast:true — works for all field types including singleSelect
  const fields = {
    "Item Name":   name.trim(),
    "Active Item": active === false ? false : true   // default true when not specified
  };
  if (category && category.trim())       fields["Category"]          = category.trim();
  if (productSize && productSize.trim()) fields["Product Size"]      = productSize.trim();
  if (uom && uom.trim())                 fields["Unit of Measure"]   = uom.trim();
  if (barcode && barcode.trim())         fields["Barcode Value"]     = barcode.trim();
  if (cost && Number(cost) > 0)          fields["Default Unit Cost"] = Number(cost);
  if (wireFtPerLb && Number(wireFtPerLb) > 0) fields["Wire ft/lb"]   = Number(wireFtPerLb);

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Items"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  const newRecord = data.records?.[0];
  if (!newRecord) throw new Error("No record returned from Airtable");

  // Without this the new item is invisible to every read above.
  await syncItemToNeon(newRecord);

  return resp(200, {
    ok:   true,
    item: {
      id:      newRecord.id,
      name:    newRecord.fields["Item Name"] || name.trim(),
      cat:     newRecord.fields["Category"]?.name || newRecord.fields["Category"] || category || "",
      size:    newRecord.fields["Product Size"]?.name || newRecord.fields["Product Size"] || productSize || "",
      uom:     newRecord.fields["Unit of Measure"]?.name || newRecord.fields["Unit of Measure"] || uom || "",
      barcode: newRecord.fields["Barcode Value"] || barcode || "",
      cost:    newRecord.fields["Default Unit Cost"] || cost || 0,
      wireFtPerLb: newRecord.fields["Wire ft/lb"] || wireFtPerLb || 0
    }
  });
}

// ── UPDATE ITEM COST ───────────────────────────────────────
async function handleUpdateItemCost(body) {
  const { itemId, cost } = body || {};
  if (!itemId || cost === undefined) return resp(400, { ok: false, error: "Missing itemId or cost." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fld8aEhTzmEbqgIg4": Number(cost) } })
  });
  // The reads serve from Neon now — without this the price change is invisible
  // and estimates keep quoting the old cost.
  await syncItemToNeonById(itemId);
  return resp(200, { ok: true });
}

// ── DELETE ─────────────────────────────────────────────────
async function handleDelete(body) {
  const { txId } = body || {};
  if (!txId) return resp(400, { ok: false, error: "Missing txId." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Transactions")}/${txId}`, { method: "DELETE" });
  // Must reach Neon, or v_stock_on_hand keeps counting stock that no longer has
  // a transaction behind it — and unlike a missed insert, nothing later repairs
  // this: the loader upserts, it never removes rows Airtable no longer has.
  await deleteTxnFromNeon(txId);
  return resp(200, { ok: true, deleted: txId });
}

// Stock Levels table fields
const F_SL_STOCK_ID      = "fldrBCRyiuelyekGu";
const F_SL_ITEM          = "flduTAU0KQojkrjW7";
const F_SL_LOC           = "fldqiB4eTuEH5ebOw";
const F_SL_QOH           = "fldYS6soPXlHkxI1V";
const F_SL_REORDER_POINT = "fldy08kLJ1YH7lMVG";

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
    `SELECT stock_airtable_id, location_name, qty_on_hand, default_unit_cost,
            total_value, reorder_point, wire_ft_per_lb, wire_ft
       FROM v_stock_levels WHERE item_airtable_id = $1
      ORDER BY location_name ASC`, [itemId]);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      levels: q.rows.map(r => ({
        // May be null when a pair has transactions but no Stock Levels row —
        // the frontend uses this to decide update-vs-create for a reorder point.
        id:           r.stock_airtable_id || null,
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

  // Fetch ALL stock level records and filter in JavaScript by item record ID.
  // This is reliable regardless of how the item name appears in the Stock ID formula.
  const allRecords = await fetchAll(API_ROOT_INV, "Stock Levels", {});

  const records = allRecords.filter(r => {
    const itemLinks = r.fields?.["Item"] || [];
    return itemLinks.some(link => {
      const linkId = typeof link === "object" ? link.id : String(link);
      return linkId === itemId;
    });
  });

  const levels = records.map(r => {
    const f = r.fields || {};
    const stockId = f["Stock ID"] || "";
    const parts   = stockId.split(" | ");
    const locName = parts[parts.length - 1] || "";
    const wireFtLbRaw = f["Wire ft/lb"];
    const wireFtLb    = Array.isArray(wireFtLbRaw) ? (wireFtLbRaw[0] || 0) : (wireFtLbRaw || 0);
    const wireFtRaw   = f["Wire (Ft.)"];
    const wireFt      = Array.isArray(wireFtRaw) ? (wireFtRaw[0] || 0) : (wireFtRaw || 0);
    return {
      id:           r.id,
      locationName: locName,
      qtyOnHand:    f["Quantity On Hand"]      || 0,
      unitCost:     f["Unit Cost (from Item)"] || 0,
      totalValue:   f["Total Value"]           || 0,
      reorderPoint: f["Reorder Point"]         || 0,
      wireWeight:   wireFtLb,
      wireFt:       wireFt
    };
  });

  levels.sort((a, b) => a.locationName.localeCompare(b.locationName));
  return resp(200, { ok: true, levels });
}

// ── STOCK LEVELS ALL (for Category Browse in Check Stock) ─
// Returns every stock level record with its linked item ID so the
// client can group by item and show per-location breakdowns.
async function handleStockLevelsAll() {
  const q = await neonQuery(
    `SELECT stock_airtable_id, item_airtable_id, location_name, qty_on_hand,
            default_unit_cost, total_value, reorder_point, wire_ft_per_lb, wire_ft
       FROM v_stock_levels ORDER BY item_name ASC, location_name ASC`);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      levels: q.rows.map(r => ({
        id:           r.stock_airtable_id || null,
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

  const allRecords = await fetchAll(API_ROOT_INV, "Stock Levels", {});

  const levels = allRecords.map(r => {
    const f = r.fields || {};

    // Extract the linked item record ID
    const itemLinks = f["Item"] || [];
    const itemId = itemLinks.length
      ? (typeof itemLinks[0] === "object" ? itemLinks[0].id : String(itemLinks[0]))
      : "";

    // Location name is the last segment of the Stock ID formula field
    const stockId = f["Stock ID"] || "";
    const parts   = stockId.split(" | ");
    const locName = parts[parts.length - 1] || "";

    // Wire fields — lookup fields return arrays, normalize to number
    const wireFtLbRaw = f["Wire ft/lb"];
    const wireFtLb    = Array.isArray(wireFtLbRaw) ? (wireFtLbRaw[0] || 0) : (wireFtLbRaw || 0);
    const wireFtRaw   = f["Wire (Ft.)"];
    const wireFt      = Array.isArray(wireFtRaw) ? (wireFtRaw[0] || 0) : (wireFtRaw || 0);

    return {
      id:           r.id,
      itemId:       itemId,
      locationName: locName,
      qtyOnHand:    f["Quantity On Hand"]      || 0,
      unitCost:     f["Unit Cost (from Item)"] || 0,
      totalValue:   f["Total Value"]           || 0,
      reorderPoint: f["Reorder Point"]         || 0,
      wireWeight:   wireFtLb,
      wireFt:       wireFt
    };
  });

  return resp(200, { ok: true, levels });
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

  const records = await fetchAll(API_ROOT_INV, "Stock Levels", {
    filter: `AND({Reorder Point} > 0, {Quantity On Hand} <= {Reorder Point})`
  });

  const groups = {};

  records.forEach(r => {
    const f       = r.fields || {};
    const stockId = f["Stock ID"] || "";
    const parts   = stockId.split(" | ");
    const locName  = parts[parts.length - 1] || "Unknown";
    const itemName = parts.slice(0, -1).join(" | ") || stockId;
    const qty      = f["Quantity On Hand"] || 0;
    const reorder  = f["Reorder Point"]    || 0;
    // Handle lookup field returning array
    const wireFtLbRaw = f["Wire ft/lb"];
    const wireWeight  = Array.isArray(wireFtLbRaw) ? (wireFtLbRaw[0] || 0) : (wireFtLbRaw || 0);
    const wireFtRaw   = f["Wire (Ft.)"];
    const wireFt      = Array.isArray(wireFtRaw) ? (wireFtRaw[0] || 0) : (wireFtRaw || 0);

    // Pull the linked record IDs so the frontend can deep-link this alert
    // straight into the Receive screen with item + location prefilled. Names
    // alone aren't safe for lookup (two items could share part of a name),
    // so we always pass IDs when they're available.
    const itemLinks = f["Item"] || [];
    const itemId = itemLinks.length
      ? (typeof itemLinks[0] === "object" ? itemLinks[0].id : String(itemLinks[0]))
      : "";
    const locLinks = f["Location"] || [];
    const locationId = locLinks.length
      ? (typeof locLinks[0] === "object" ? locLinks[0].id : String(locLinks[0]))
      : "";

    if (!groups[locName]) groups[locName] = [];
    groups[locName].push({
      itemId,
      itemName,
      locationId,
      qtyOnHand:    qty,
      reorderPoint: reorder,
      shortBy:      reorder - qty,
      wireWeight,
      wireFt
    });
  });

  Object.keys(groups).forEach(loc => {
    groups[loc].sort((a, b) => a.itemName.localeCompare(b.itemName));
  });

  return resp(200, { ok: true, groups });
}

// ── UPDATE REORDER POINT ──────────────────────────────────
async function handleUpdateReorderPoint(body) {
  const { stockLevelId, reorderPoint } = body || {};
  if (!stockLevelId) return resp(400, { ok: false, error: "Missing stockLevelId." });
  if (reorderPoint === undefined) return resp(400, { ok: false, error: "Missing reorderPoint." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Stock Levels")}/${stockLevelId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { [F_SL_REORDER_POINT]: Number(reorderPoint) } })
  });
  // Reorder point drives the alerts screen, which reads Neon now.
  const q = await neonQuery(
    `UPDATE stock_settings SET reorder_point = $2, synced_at = now() WHERE airtable_id = $1`,
    [stockLevelId, Number(reorderPoint)]);
  if (!q?.rows) console.error(`updateReorderPoint: Neon not updated for ${stockLevelId} (stale until loader): ${q?.error || "Neon unavailable"}`);
  return resp(200, { ok: true });
}

// ── CREATE STOCK LEVEL ────────────────────────────────────
// Used when an admin sets a reorder point on an item × location combo that
// has no Stock Level record yet. Creates the row with QoH=0; the existing
// Airtable automation keys on Item ID + Location ID and will update this row
// (without overwriting Reorder Point) on the next transaction.
async function handleCreateStockLevel(body) {
  const { itemId, itemName, locationId, locationName, reorderPoint } = body || {};
  if (!itemId)     return resp(400, { ok: false, error: "Missing itemId." });
  if (!locationId) return resp(400, { ok: false, error: "Missing locationId." });
  if (reorderPoint === undefined) return resp(400, { ok: false, error: "Missing reorderPoint." });

  const fields = {
    [F_SL_STOCK_ID]:      `${itemName || ""} | ${locationName || ""}`,
    [F_SL_ITEM]:          [String(itemId)],
    [F_SL_LOC]:           [String(locationId)],
    [F_SL_QOH]:           0,
    [F_SL_REORDER_POINT]: Number(reorderPoint)
  };

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Stock Levels"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  const recordId = created.records?.[0]?.id;
  if (!recordId) return resp(500, { ok: false, error: "Failed to create stock level." });
  // Note the QoH=0 written above is Airtable's cache column, which Neon does not
  // carry at all — on-hand there is derived from the ledger. Only the reorder
  // point and the item/location pairing come across.
  await syncStockSettingToNeon(created.records[0]);
  return resp(200, { ok: true, recordId });
}

// ═══════════════════════════════════════════════════════════
// ESTIMATES — list, get, create, update, delete
// ═══════════════════════════════════════════════════════════

const EST_TABLE_ID  = "tblULCJaVsLXk4Af0";
const LINE_TABLE_ID = "tblhRadsyvlLw5Lp5";

// Estimates table fields
const F_EST_JOB_NAME    = "fld5QDgzSOXNAZdOc";
const F_EST_JOB_ID      = "fldId8eR0C8TeSfy4";
const F_EST_CREATED_BY  = "fldl0xEYcPvNbs69U";
const F_EST_STATUS      = "fldAu3oNbywGe8vBh";
const F_EST_NOTES       = "fld7sOLbNZxEqP0zs";

// Estimate Line Items table fields
const F_LINE_ESTIMATE   = "fldCXpRJt9g3yCB9r";
const F_LINE_ITEM       = "fld50ttitFcM2uPap";
const F_LINE_QTY        = "fld9mDWjvdd4AfXnn";
const F_LINE_UNIT_COST  = "fldkTzFNJydVX1iK3";

// ── ESTIMATES LIST ─────────────────────────────────────────
async function handleEstimatesList(params) {
  // `total` and `lineCount` come from v_material_estimate_totals rather than
  // Airtable's rollup and link-array length. The view was reconciled against
  // that rollup on all 14 estimates before this flipped — agreeing to the cent,
  // with 591 lines counted on both sides.
  const q = await neonQuery(
    `SELECT e.airtable_id, e.job_name, e.job_airtable_id, e.created_at, e.created_by,
            e.status, e.notes, t.total, t.line_count
       FROM material_estimates e
       LEFT JOIN v_material_estimate_totals t ON t.estimate_id = e.id
      ORDER BY e.created_at DESC NULLS LAST`);
  if (q?.rows) {
    return resp(200, {
      ok: true, _source: "neon",
      estimates: q.rows.map(r => ({
        id:          r.airtable_id,
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

  const records = await fetchAll(API_ROOT_INV, "Estimates", {
    sortField: "Date Created",
    sortDir: "desc"
  });

  const estimates = records.map(r => {
    const f = r.fields || {};
    return {
      id:        r.id,
      jobName:   f["Job Name"] || "",
      jobId:     f["Job ID"] || "",
      dateCreated: f["Date Created"] || "",
      createdBy: f["Created By"] || "",
      status:    f["Status"]?.name || f["Status"] || "Draft",
      notes:     f["Notes"] || "",
      total:     f["Total"] || 0,
      lineCount: (f["Estimate Line Items"] || []).length
    };
  });

  return resp(200, { ok: true, _source: "airtable", estimates });
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
    `SELECT e.airtable_id AS est_id, e.job_name, e.job_airtable_id, e.created_at,
            e.created_by, e.status, e.notes,
            (SELECT total FROM v_material_estimate_totals v WHERE v.estimate_id = e.id) AS total,
            l.airtable_id AS line_id, l.line_number, l.item_airtable_id, l.description,
            l.quantity, l.unit_cost_at_estimate,
            i.name AS item_name, i.unit_of_measure, i.category
       FROM material_estimates e
       LEFT JOIN material_estimate_lines l ON l.estimate_id = e.id
       LEFT JOIN inventory_items i         ON i.id = l.item_id
      WHERE e.airtable_id = $1
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

  // Fetch estimate
  const estData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimates")}/${id}`,
    { method: "GET" }
  );
  if (!estData?.id) return resp(404, { ok: false, error: "Estimate not found." });

  const ef = estData.fields || {};
  const lineIds = (ef["Estimate Line Items"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  // Fetch all line items belonging to this estimate
  let lines = [];
  if (lineIds.length) {
    const [lineRecords, itemRecords] = await Promise.all([
      fetchAll(API_ROOT_INV, "Estimate Line Items", {}),
      itemIndex()
    ]);

    const itemMap = itemRecords;

    lines = lineRecords
      .filter(r => lineIds.includes(r.id))
      .map(r => {
        const f = r.fields || {};
        const itemArr = f["Inventory Item"] || [];
        const itemId = itemArr.length
          ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
          : "";
        const itemData = itemMap[itemId] || {};
        return {
          id:       r.id,
          lineNum:  f["Line ID"] || 0,
          itemId:   itemId,
          itemName: itemData.name || (f["Description"] || ""),
          uom:      itemData.uom || "",
          category: itemData.cat || "",
          isMisc:   !itemId,
          description: f["Description"] || "",
          qty:      f["Quantity"] || 0,
          unitCost: f["Unit Cost at Time of Estimate"] || 0,
          lineTotal: f["Line Total"] || 0
        };
      })
      .sort((a, b) => (a.lineNum || 0) - (b.lineNum || 0));
  }

  return resp(200, {
    ok: true,
    estimate: {
      id:        estData.id,
      jobName:   ef["Job Name"] || "",
      jobId:     ef["Job ID"] || "",
      dateCreated: ef["Date Created"] || "",
      createdBy: ef["Created By"] || "",
      status:    ef["Status"]?.name || ef["Status"] || "Draft",
      notes:     ef["Notes"] || "",
      total:     ef["Total"] || 0,
      lines
    }
  });
}

// ── CREATE ESTIMATE ────────────────────────────────────────
async function handleEstimateCreate(body) {
  const { jobName, jobId, status, notes, createdBy, lines } = body || {};
  if (!jobName || !jobName.trim()) return resp(400, { ok: false, error: "Job name is required." });

  const estFields = {
    [F_EST_JOB_NAME]:   String(jobName).trim(),
    [F_EST_JOB_ID]:     String(jobId || "").trim(),
    [F_EST_STATUS]:     status || "Estimating",
    [F_EST_NOTES]:      String(notes || "").trim(),
    [F_EST_CREATED_BY]: String(createdBy || "").trim()
  };

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimates"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields: estFields }], typecast: true })
  });

  const newId = created.records?.[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create estimate." });

  // The header has to reach Neon BEFORE the lines: each line resolves its
  // estimate_id by looking the parent up, and a line whose parent isn't there
  // yet lands with a null FK and never counts toward the total.
  await syncEstimateToNeon(created.records[0]);

  // Create line items if provided (in batches of 10)
  if (lines && lines.length) {
    await createLineItems(newId, lines);
  }

  return resp(200, { ok: true, id: newId });
}

// ── UPDATE ESTIMATE ────────────────────────────────────────
async function handleEstimateUpdate(body) {
  const { id, status, notes, jobName, jobId, lines, replaceLines } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // Update header fields if provided
  const headerFields = {};
  if (status !== undefined)  headerFields[F_EST_STATUS]   = status;
  if (notes  !== undefined)  headerFields[F_EST_NOTES]    = String(notes || "");
  if (jobName !== undefined) headerFields[F_EST_JOB_NAME] = String(jobName || "");
  if (jobId   !== undefined) headerFields[F_EST_JOB_ID]   = String(jobId || "");

  if (Object.keys(headerFields).length) {
    const patched = await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimates")}/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ fields: headerFields, typecast: true })
    });
    await syncEstimateToNeon(patched);
  }

  // If replaceLines is true, delete existing lines and add new ones.
  // Otherwise, just append the new lines if provided.
  if (replaceLines && lines !== undefined) {
    // Get existing line IDs for this estimate
    const estData = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Estimates")}/${id}`,
      { method: "GET" }
    );
    const existingIds = (estData.fields["Estimate Line Items"] || [])
      .map(l => typeof l === "object" ? l.id : String(l));

    // Delete existing lines in batches of 10
    for (let i = 0; i < existingIds.length; i += 10) {
      const batch = existingIds.slice(i, i + 10);
      const qs = batch.map(rid => `records[]=${rid}`).join("&");
      await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Line Items")}?${qs}`, {
        method: "DELETE"
      });
      // Nothing repairs a missed delete — the loader only upserts — so a line
      // left behind here would keep counting toward the estimate's total after
      // the user replaced it.
      await deleteEstimateLinesFromNeon(batch);
    }

    // Create new lines
    if (lines.length) await createLineItems(id, lines);
  } else if (lines && lines.length) {
    // Just append
    await createLineItems(id, lines);
  }

  return resp(200, { ok: true, id });
}

// ── DELETE ESTIMATE ────────────────────────────────────────
async function handleEstimateDelete(body) {
  const { id } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // Get linked line items first
  const estData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimates")}/${id}`,
    { method: "GET" }
  );
  const lineIds = (estData.fields["Estimate Line Items"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  // Delete lines in batches of 10
  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Line Items")}?${qs}`, {
      method: "DELETE"
    });
  }

  // Delete the estimate
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimates")}/${id}`, {
    method: "DELETE"
  });
  // ON DELETE CASCADE takes the lines with it, so the loop above only needs to
  // mirror Airtable's own line deletions, not clean up after this one.
  await deleteEstimateFromNeon(id);

  return resp(200, { ok: true, deleted: id });
}

// ── HELPER: Create line items in batches of 10 ─────────────
async function createLineItems(estimateId, lines) {
  for (let i = 0; i < lines.length; i += 10) {
    const batch = lines.slice(i, i + 10).map(l => {
      const fields = {
        [F_LINE_ESTIMATE]:  [String(estimateId)],
        [F_LINE_QTY]:       Number(l.qty || 0),
        [F_LINE_UNIT_COST]: Number(l.unitCost || 0)
      };
      // Inventory item link — only for non-Misc lines
      if (l.itemId) {
        fields[F_LINE_ITEM] = [String(l.itemId)];
      }
      // Description — only attempt to set if it's a Misc line.
      if (l.isMisc && l.description) {
        fields["Description"] = String(l.description).trim();
      }
      return { fields };
    });

    let created;
    try {
      created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Line Items"), {
        method: "POST",
        body: JSON.stringify({ records: batch, typecast: true })
      });
    } catch (err) {
      // If failure is due to missing Description field, retry without it
      if (err.message && err.message.toLowerCase().includes("description")) {
        const retryBatch = batch.map(b => {
          const f = { ...b.fields };
          delete f.Description;
          return { fields: f };
        });
        created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Line Items"), {
          method: "POST",
          body: JSON.stringify({ records: retryBatch, typecast: true })
        });
      } else {
        throw err;
      }
    }
    // Mirror each batch as it lands rather than collecting and syncing at the
    // end: a failure partway through then leaves Neon holding the batches that
    // did succeed, which is what the estimate actually contains.
    await syncEstimateLinesToNeon(created?.records);
  }
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

const TMPL_TABLE_ID  = "tblpGp6Dp1PE4m9MM";
const TLINE_TABLE_ID = "tblVKHFKMuaUM5NEA";

// Estimate Templates fields
const F_TMPL_NAME         = "fldzbsFfD6ajik3dU";
const F_TMPL_DESC         = "fld7D2v2WGISfioNL";
const F_TMPL_ACTIVE       = "fldUTxUGOEpZRNNAR";
const F_TMPL_CONTRACTOR   = "fldj5vseL63A9mm4k";
const F_TMPL_SOURCE_REF   = "fldYHyEcxQC77Jrjo";
const F_TMPL_TOTAL        = "fldvr0xvwD6C2uDTK";
const F_TMPL_CREATED_DATE = "fldIMzA7k09aAdS3h";
const F_TMPL_CREATED_BY   = "fldIpnqacDhxgv5Vd";

// Estimate Template Lines fields
const F_TLINE_TITLE     = "fldCDQjNS6hBgxvFJ";
const F_TLINE_TEMPLATE  = "fldu2l87Uq8VMMXdz";
const F_TLINE_ITEM      = "fldtFJyAMnx2rOyxo";
const F_TLINE_QTY       = "fldOF5tMUjud0ssGT";
const F_TLINE_UNIT_COST = "fld8NUdErnXOCj5ak";
const F_TLINE_TOTAL     = "fldQVInVUDuULC40O";
const F_TLINE_NOTES     = "fldFxStyLujlmuO0N";

// Inventory Items: Default Unit Cost field id (used for live pricing on clone)
const F_ITEM_DEFAULT_COST = "fld8aEhTzmEbqgIg4";

// ── HELPER: build a lineTitle for template lines ───────────
function tlineTitle(templateName, itemName) {
  const tn = String(templateName || "").trim() || "Template";
  const inm = String(itemName || "").trim() || "Item";
  return tn + " — " + inm;
}

// ── HELPER: Recompute Total at Save on a template ──────────
// Sums Line Total at Save across the template's lines and PATCHes the
// frozen Total at Save back. Called after any line mutation.
async function recomputeTemplateTotal(templateId, allTemplateLines) {
  const lines = allTemplateLines !== undefined
    ? allTemplateLines
    : await fetchAll(API_ROOT_INV, "Estimate Template Lines", {});
  const myLines = lines.filter(r => {
    const links = r.fields?.["Template"] || [];
    return links.some(l => (typeof l === "object" ? l.id : String(l)) === templateId);
  });
  const total = myLines.reduce(
    (s, r) => s + (Number(r.fields?.["Line Total at Save"] || 0)),
    0
  );
  const patched = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimate Templates")}/${templateId}`,
    {
      method: "PATCH",
      body: JSON.stringify({ fields: { [F_TMPL_TOTAL]: total }, typecast: true })
    }
  );
  // `Total at Save` is a stored snapshot rather than a rollup, which is why this
  // helper exists at all — so it has to be mirrored like any other value. Neon
  // also derives the same figure in v_material_template_totals; carrying both
  // means a disagreement is visible instead of assumed away.
  await syncTemplateToNeon(patched);
  return total;
}

// ── LIST TEMPLATES ─────────────────────────────────────────
async function handleEstimateTemplatesList(params) {
  const activeOnly = params?.activeOnly === "1" || params?.activeOnly === "true";
  const contractorRaw = (params?.contractor || "").trim();

  // Filtering happens in SQL rather than in JS after pulling everything.
  // `contractor` is matched case-insensitively, the same as the JS below.
  const q = await neonQuery(
    `SELECT t.airtable_id, t.name, t.description, t.active, t.contractor,
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
        id:          r.airtable_id,
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

  const records = await fetchAll(API_ROOT_INV, "Estimate Templates", {
    sortField: "Created Date",
    sortDir: "desc"
  });

  let templates = records.map(r => {
    const f = r.fields || {};
    return {
      id:           r.id,
      name:         f["Template Name"] || "",
      description:  f["Description"] || "",
      active:       gBool(f, "Active"),
      contractor:   f["Contractor"] || "",
      sourceRef:    f["Source Estimate Reference"] || "",
      totalAtSave:  Number(f["Total at Save"] || 0),
      createdDate:  f["Created Date"] || "",
      createdBy:    f["Created By"] || "",
      lineCount:    (f["Estimate Template Lines"] || []).length
    };
  });

  if (activeOnly) {
    templates = templates.filter(t => t.active);
  }
  if (contractorRaw) {
    const cn = contractorRaw.toLowerCase();
    templates = templates.filter(t => (t.contractor || "").toLowerCase() === cn);
  }

  return resp(200, { ok: true, templates });
}

// ── GET ONE TEMPLATE WITH LINES ────────────────────────────
async function handleEstimateTemplateGet(params) {
  const { templateId } = params || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  // ⚠ `wireFtPerLb` comes from the item, LIVE — not from the snapshot. It is a
  // physical property of the wire, not a price, so freezing it would be wrong.
  // The costs beside it are snapshots and stay snapshots.
  const nq = await neonQuery(
    `SELECT t.airtable_id AS tmpl_id, t.name, t.description, t.active, t.contractor,
            t.source_estimate_ref, t.total_at_save, t.created_at, t.created_by,
            l.airtable_id AS line_id, l.item_airtable_id, l.quantity,
            l.unit_cost_at_save, l.notes,
            i.name AS item_name, i.unit_of_measure, i.wire_ft_per_lb
       FROM material_estimate_templates t
       LEFT JOIN material_estimate_template_lines l ON l.template_id = t.id
       LEFT JOIN inventory_items i                  ON i.id = l.item_id
      WHERE t.airtable_id = $1
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

  const tmplData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimate Templates")}/${templateId}`,
    { method: "GET" }
  );
  if (!tmplData?.id) return resp(404, { ok: false, error: "Template not found." });

  const tf = tmplData.fields || {};
  const lineIds = (tf["Estimate Template Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  let lines = [];
  if (lineIds.length) {
    const [lineRecords, itemRecords] = await Promise.all([
      fetchAll(API_ROOT_INV, "Estimate Template Lines", {}),
      itemIndex()
    ]);

    const itemMap = itemRecords;

    lines = lineRecords
      .filter(r => lineIds.includes(r.id))
      .map(r => {
        const f = r.fields || {};
        const itemArr = f["Inventory Item"] || [];
        const itemId = itemArr.length
          ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
          : "";
        const itemData = itemMap[itemId] || {};
        return {
          id:               r.id,
          itemId:           itemId,
          itemName:         itemData.name || "",
          uom:              itemData.uom || "",
          wireFtPerLb:      itemData.wireFtPerLb || 0,
          quantity:         Number(f["Quantity"] || 0),
          unitCostAtSave:   Number(f["Unit Cost at Save"] || 0),
          lineTotalAtSave:  Number(f["Line Total at Save"] || 0),
          notes:            f["Notes"] || ""
        };
      })
      .sort((a, b) => (a.itemName || "").localeCompare(b.itemName || ""));
  }

  return resp(200, {
    ok: true,
    template: {
      id:           tmplData.id,
      name:         tf["Template Name"] || "",
      description:  tf["Description"] || "",
      active:       gBool(tf, "Active"),
      contractor:   tf["Contractor"] || "",
      sourceRef:    tf["Source Estimate Reference"] || "",
      totalAtSave:  Number(tf["Total at Save"] || 0),
      createdDate:  tf["Created Date"] || "",
      createdBy:    tf["Created By"] || ""
    },
    lines
  });
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

  // Fetch source estimate + its lines + inventory items in parallel
  const estData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimates")}/${estimateId}`,
    { method: "GET" }
  );
  if (!estData?.id) return resp(404, { ok: false, error: "Source estimate not found." });
  const ef = estData.fields || {};
  const lineIds = (ef["Estimate Line Items"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  const [allLineRecords, allItemRecords] = await Promise.all([
    lineIds.length ? fetchAll(API_ROOT_INV, "Estimate Line Items", {}) : Promise.resolve([]),
    itemIndex()
  ]);

  const itemNameById = {};
  for (const [id, it] of Object.entries(allItemRecords)) itemNameById[id] = it.name;

  // Pull source lines, drop Misc lines (no Inventory Item link).
  const sourceLines = allLineRecords
    .filter(r => lineIds.includes(r.id))
    .map(r => {
      const f = r.fields || {};
      const itemArr = f["Inventory Item"] || [];
      const itemId = itemArr.length
        ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
        : "";
      return {
        itemId,
        qty:        Number(f["Quantity"] || 0),
        unitCost:   Number(f["Unit Cost at Time of Estimate"] || 0),
        notes:      f["Description"] || ""
      };
    })
    .filter(l => !!l.itemId);

  const skippedMiscCount = lineIds.length - sourceLines.length;
  const totalAtSave = sourceLines.reduce(
    (s, l) => s + (l.qty * l.unitCost),
    0
  );

  const sourceRef = (ef["Job Name"] || "")
    + (ef["Job ID"] ? " (" + ef["Job ID"] + ")" : "");

  // Create the template record first so we have its id for line links
  const tmplFields = {
    [F_TMPL_NAME]:         name,
    [F_TMPL_DESC]:         String(description || "").trim(),
    [F_TMPL_ACTIVE]:       true,
    [F_TMPL_CONTRACTOR]:   String(contractor || "").trim(),
    [F_TMPL_SOURCE_REF]:   sourceRef,
    [F_TMPL_TOTAL]:        totalAtSave,
    [F_TMPL_CREATED_DATE]: new Date().toISOString(),
    [F_TMPL_CREATED_BY]:   String(createdBy || "").trim()
  };

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Templates"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields: tmplFields }], typecast: true })
  });
  const templateId = created.records?.[0]?.id;
  if (!templateId) return resp(500, { ok: false, error: "Failed to create template." });

  // Header before lines — each line resolves template_id by looking the parent
  // up, so a line written first lands with a null FK and never counts.
  await syncTemplateToNeon(created.records[0]);

  // Create the template lines in batches of 10
  if (sourceLines.length) {
    for (let i = 0; i < sourceLines.length; i += 10) {
      const batch = sourceLines.slice(i, i + 10).map(l => {
        const lineTotal = (Number(l.qty) || 0) * (Number(l.unitCost) || 0);
        const fields = {
          [F_TLINE_TITLE]:     tlineTitle(name, itemNameById[l.itemId] || ""),
          [F_TLINE_TEMPLATE]:  [String(templateId)],
          [F_TLINE_ITEM]:      [String(l.itemId)],
          [F_TLINE_QTY]:       Number(l.qty || 0),
          [F_TLINE_UNIT_COST]: Number(l.unitCost || 0),
          [F_TLINE_TOTAL]:     lineTotal
        };
        if (l.notes && String(l.notes).trim()) {
          fields[F_TLINE_NOTES] = String(l.notes).trim();
        }
        return { fields };
      });
      const madeLines = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Template Lines"), {
        method: "POST",
        body: JSON.stringify({ records: batch, typecast: true })
      });
      await syncTemplateLinesToNeon(madeLines?.records);
    }
  }

  return resp(200, {
    ok: true,
    templateId,
    lineCount: sourceLines.length,
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

  // Fetch the template record (for description) + its line ids + all
  // template lines + all inventory items (for live cost lookup) in parallel.
  const [tmplData, allTemplateLines, itemRecords] = await Promise.all([
    atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Templates")}/${templateId}`, { method: "GET" }),
    fetchAll(API_ROOT_INV, "Estimate Template Lines", {}),
    itemIndex()
  ]);
  if (!tmplData?.id) return resp(404, { ok: false, error: "Template not found." });

  const tf = tmplData.fields || {};
  const tmplLineIds = (tf["Estimate Template Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  const itemCostById = {};
  for (const [id, it] of Object.entries(itemRecords)) itemCostById[id] = it.cost;

  // Build the lines that will be inserted into the new estimate.
  // Snapshot LIVE pricing here, not the template's frozen cost.
  const cloneLines = allTemplateLines
    .filter(r => tmplLineIds.includes(r.id))
    .map(r => {
      const f = r.fields || {};
      const itemArr = f["Inventory Item"] || [];
      const itemId = itemArr.length
        ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
        : "";
      return {
        itemId,
        qty:         Number(f["Quantity"] || 0),
        unitCost:    Number(itemCostById[itemId] || 0),
        description: f["Notes"] || ""
      };
    })
    .filter(l => !!l.itemId);

  // Create the new estimate
  const estFields = {
    [F_EST_JOB_NAME]:   String(jobName).trim(),
    [F_EST_JOB_ID]:     String(jobId || "").trim(),
    [F_EST_STATUS]:     "Draft",
    [F_EST_NOTES]:      String(tf["Description"] || "").trim(),
    [F_EST_CREATED_BY]: String(createdBy || "").trim()
  };
  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimates"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields: estFields }], typecast: true })
  });
  const newId = created.records?.[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create estimate." });

  // This is the SECOND path that creates an estimate, and it was missed when
  // the estimate reads flipped to Neon — so a template-built estimate existed
  // in Airtable, was absent from Neon, and the app 404'd the moment it opened
  // the thing it had just created. Header before lines, same as
  // handleEstimateCreate: a line resolves its estimate_id by looking the
  // parent up, and one written first lands with a null FK.
  await syncEstimateToNeon(created.records[0]);

  // Bulk-insert the cloned lines. We can't reuse createLineItems for these
  // because it gates Description on isMisc — for clones we want template
  // notes carried into the Description column on the new estimate line.
  if (cloneLines.length) {
    for (let i = 0; i < cloneLines.length; i += 10) {
      const batch = cloneLines.slice(i, i + 10).map(l => {
        const fields = {
          [F_LINE_ESTIMATE]:  [String(newId)],
          [F_LINE_ITEM]:      [String(l.itemId)],
          [F_LINE_QTY]:       Number(l.qty || 0),
          [F_LINE_UNIT_COST]: Number(l.unitCost || 0)
        };
        if (l.description && String(l.description).trim()) {
          fields["Description"] = String(l.description).trim();
        }
        return { fields };
      });
      let made;
      try {
        made = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Line Items"), {
          method: "POST",
          body: JSON.stringify({ records: batch, typecast: true })
        });
      } catch (err) {
        // Same defensive retry as createLineItems — Description is optional
        // and shouldn't sink the whole clone if Airtable rejects it.
        if (err.message && err.message.toLowerCase().includes("description")) {
          const retryBatch = batch.map(b => {
            const f = { ...b.fields };
            delete f.Description;
            return { fields: f };
          });
          made = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Line Items"), {
            method: "POST",
            body: JSON.stringify({ records: retryBatch, typecast: true })
          });
        } else {
          throw err;
        }
      }
      // Mirrored per batch, not once at the end — the retry path above swaps in
      // a different record set, so the response is the only reliable source of
      // what actually landed.
      await syncEstimateLinesToNeon(made?.records);
    }
  }

  return resp(200, { ok: true, estimateId: newId, lineCount: cloneLines.length });
}

// ── UPDATE TEMPLATE METADATA ───────────────────────────────
// Header-only patch — does not touch lines.
async function handleEstimateTemplateUpdate(body) {
  const { templateId, templateName, description, contractor, active } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  const fields = {};
  if (templateName !== undefined) fields[F_TMPL_NAME]       = String(templateName || "").trim();
  if (description  !== undefined) fields[F_TMPL_DESC]       = String(description  || "");
  if (contractor   !== undefined) fields[F_TMPL_CONTRACTOR] = String(contractor   || "").trim();
  if (active       !== undefined) fields[F_TMPL_ACTIVE]     = !!active;

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const patchedTmpl = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimate Templates")}/${templateId}`,
    {
      method: "PATCH",
      body: JSON.stringify({ fields, typecast: true })
    }
  );
  await syncTemplateToNeon(patchedTmpl);

  // If the name changed, re-write Line Title on every line so the
  // human-readable title stays in sync. Cheap (PATCH up to 10 per call).
  if (templateName !== undefined) {
    const newName = String(templateName || "").trim();
    const [tmplRec, allLines, allItems] = await Promise.all([
      atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Templates")}/${templateId}`, { method: "GET" }),
      fetchAll(API_ROOT_INV, "Estimate Template Lines", {}),
      itemIndex()
    ]);
    const lineIds = (tmplRec.fields?.["Estimate Template Lines"] || [])
      .map(l => typeof l === "object" ? l.id : String(l));
    const itemNameById = {};
    for (const [id, it] of Object.entries(allItems)) itemNameById[id] = it.name;
    const myLines = allLines.filter(r => lineIds.includes(r.id));
    const updates = myLines.map(r => {
      const f = r.fields || {};
      const itemArr = f["Inventory Item"] || [];
      const itemId = itemArr.length
        ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
        : "";
      return {
        id: r.id,
        fields: { [F_TLINE_TITLE]: tlineTitle(newName, itemNameById[itemId] || "") }
      };
    });
    for (let i = 0; i < updates.length; i += 10) {
      const batch = updates.slice(i, i + 10);
      const patchedLines = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Template Lines"), {
        method: "PATCH",
        body: JSON.stringify({ records: batch, typecast: true })
      });
      await syncTemplateLinesToNeon(patchedLines?.records);
    }
  }

  return resp(200, { ok: true });
}

// ── UPSERT TEMPLATE LINE (create or update one line) ───────
async function handleEstimateTemplateLineUpsert(body) {
  const { templateId, lineId, itemId, quantity, notes } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  if (lineId) {
    // PATCH existing line: update qty/notes, recompute Line Total at Save
    const existing = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Estimate Template Lines")}/${lineId}`,
      { method: "GET" }
    );
    const ef = existing.fields || {};
    const newQty  = quantity !== undefined ? Number(quantity || 0) : Number(ef["Quantity"] || 0);
    const cost    = Number(ef["Unit Cost at Save"] || 0);
    const total   = newQty * cost;
    const fields  = {
      [F_TLINE_QTY]:   newQty,
      [F_TLINE_TOTAL]: total
    };
    if (notes !== undefined) fields[F_TLINE_NOTES] = String(notes || "");

    const patchedLine = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Estimate Template Lines")}/${lineId}`,
      {
        method: "PATCH",
        body: JSON.stringify({ fields, typecast: true })
      }
    );
    await syncTemplateLinesToNeon([patchedLine]);
    await recomputeTemplateTotal(templateId);
    return resp(200, { ok: true, lineId });
  }

  // CREATE new line — snapshot current Default Unit Cost from the item
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  const [tmplRec, itemRec] = await Promise.all([
    atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Templates")}/${templateId}`, { method: "GET" }),
    atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`,        { method: "GET" })
  ]);
  if (!tmplRec?.id) return resp(404, { ok: false, error: "Template not found." });
  if (!itemRec?.id) return resp(404, { ok: false, error: "Inventory item not found." });

  const templateName = tmplRec.fields?.["Template Name"] || "";
  const itemName     = itemRec.fields?.["Item Name"]     || "";
  const unitCost     = Number(itemRec.fields?.["Default Unit Cost"] || 0);
  const qty          = Number(quantity || 0);

  const fields = {
    [F_TLINE_TITLE]:     tlineTitle(templateName, itemName),
    [F_TLINE_TEMPLATE]:  [String(templateId)],
    [F_TLINE_ITEM]:      [String(itemId)],
    [F_TLINE_QTY]:       qty,
    [F_TLINE_UNIT_COST]: unitCost,
    [F_TLINE_TOTAL]:     qty * unitCost
  };
  if (notes !== undefined && String(notes || "").trim()) {
    fields[F_TLINE_NOTES] = String(notes).trim();
  }

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Template Lines"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });
  const newLineId = created.records?.[0]?.id;
  await syncTemplateLinesToNeon(created?.records);
  await recomputeTemplateTotal(templateId);
  return resp(200, { ok: true, lineId: newLineId });
}

// ── DELETE ONE TEMPLATE LINE ───────────────────────────────
async function handleEstimateTemplateLineDelete(body) {
  const { lineId, templateId } = body || {};
  if (!lineId) return resp(400, { ok: false, error: "Missing lineId." });

  // Resolve the templateId from the line if the frontend didn't send it
  let tid = templateId;
  if (!tid) {
    const ln = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Estimate Template Lines")}/${lineId}`,
      { method: "GET" }
    );
    const links = ln.fields?.["Template"] || [];
    tid = links.length
      ? (typeof links[0] === "object" ? links[0].id : String(links[0]))
      : "";
  }

  await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimate Template Lines")}/${lineId}`,
    { method: "DELETE" }
  );
  await deleteTemplateLinesFromNeon([lineId]);

  if (tid) await recomputeTemplateTotal(tid);
  return resp(200, { ok: true, deleted: lineId });
}

// ── DELETE TEMPLATE (cascading) ────────────────────────────
async function handleEstimateTemplateDelete(body) {
  const { templateId } = body || {};
  if (!templateId) return resp(400, { ok: false, error: "Missing templateId." });

  const tmplData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimate Templates")}/${templateId}`,
    { method: "GET" }
  );
  const lineIds = (tmplData.fields?.["Estimate Template Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Estimate Template Lines")}?${qs}`,
      { method: "DELETE" }
    );
  }

  await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimate Templates")}/${templateId}`,
    { method: "DELETE" }
  );
  // ON DELETE CASCADE takes the lines, so the loop above only mirrors Airtable's
  // own line deletions. A template left in Neon would keep appearing in the
  // picker and nothing would ever remove it — the loader only upserts.
  await deleteTemplateFromNeon(templateId);

  return resp(200, { ok: true, deleted: templateId, deletedLineCount: lineIds.length });
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

  const [tmplData, allLines, allItemRecords] = await Promise.all([
    atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Templates")}/${templateId}`, { method: "GET" }),
    fetchAll(API_ROOT_INV, "Estimate Template Lines", {}),
    itemIndex()
  ]);
  if (!tmplData?.id) return resp(404, { ok: false, error: "Template not found." });

  const lineIds = (tmplData.fields?.["Estimate Template Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  const itemCostById = {};
  for (const [id, it] of Object.entries(allItemRecords)) itemCostById[id] = it.cost;

  const myLines = allLines.filter(r => lineIds.includes(r.id));
  const updates = myLines.map(r => {
    const f = r.fields || {};
    const qty = Number(f["Quantity"] || 0);
    const itemArr = f["Inventory Item"] || [];
    const itemId = itemArr.length
      ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
      : "";
    const newCost = Number(itemCostById[itemId] || 0);
    return {
      id: r.id,
      fields: {
        [F_TLINE_UNIT_COST]: newCost,
        [F_TLINE_TOTAL]:     qty * newCost
      }
    };
  });

  let updated = 0;
  for (let i = 0; i < updates.length; i += 10) {
    const batch = updates.slice(i, i + 10);
    const patched = await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Template Lines"), {
      method: "PATCH",
      body: JSON.stringify({ records: batch, typecast: true })
    });
    // Re-snapshotting is the whole point of this handler, so the new snapshot
    // has to reach Neon — otherwise the template keeps quoting the old prices
    // on the very screen that just said it refreshed them.
    await syncTemplateLinesToNeon(patched?.records);
    updated += batch.length;
  }

  // Recompute Total at Save against the freshly-PATCHed lines (refetch so
  // the rollup reads the new totals, not the stale cache).
  const refreshedLines = await fetchAll(API_ROOT_INV, "Estimate Template Lines", {});
  const newTotal = await recomputeTemplateTotal(templateId, refreshedLines);

  return resp(200, { ok: true, lineCount: updated, total: newTotal });
}

// ═══════════════════════════════════════════════════════════
// MATERIAL ORDERS — list, get, create, mark complete, delete
// ═══════════════════════════════════════════════════════════

const ORDER_TABLE_ID      = "tblLMunp1fSrZV4mH";
const ORDER_LINE_TABLE_ID = "tblERYikTOpPhklPw";

const F_ORD_ESTIMATE   = "fld446AqUqFbATskC";
const F_ORD_JOB_NAME   = "fldst9PryHJTJYWeC";
const F_ORD_CREATED_BY = "fldeRDqMUpfIQF36D";
const F_ORD_VENDOR     = "fldgXcZ2EMNR5nfTG";
const F_ORD_STATUS     = "fldshk9Rek2BnVAxc";

const F_OL_ORDER       = "fldkAHSFDQwsQqCyd";
const F_OL_ITEM        = "fldlNL42Hj9fEKNVR";
const F_OL_DESCRIPTION = "fldoDLObzUjkHyhcA";
const F_OL_QTY         = "fldI8RfBvcD8oGpcg";

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
    `SELECT o.airtable_id, o.order_number, o.job_name, o.vendor_notes, o.status,
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
        id:          r.airtable_id,
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

  const records = await fetchAll(API_ROOT_INV, "Material Orders", {
    filter,
    sortField: "Date Created",
    sortDir: "desc"
  });

  const orders = records.map(r => {
    const f = r.fields || {};
    return {
      id:          r.id,
      orderId:     f["Order ID"] || 0,
      jobName:     f["Job Name"] || "",
      vendor:      f["Vendor / Notes"] || "",
      status:      f["Status"]?.name || f["Status"] || "Active",
      dateCreated: f["Date Created"] || "",
      createdBy:   f["Created By"] || "",
      lineCount:   (f["Material Order Lines"] || []).length,
      totalItems:  f["Total Items"] || 0
    };
  });

  return resp(200, { ok: true, orders });
}

// ── GET ONE ORDER WITH LINES ──────────────────────────────
async function handleOrderGet(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  const nq = await neonQuery(
    `SELECT o.airtable_id AS order_id, o.order_number, o.job_name, o.vendor_notes,
            o.status, o.created_at, o.created_by,
            l.airtable_id AS line_id, l.line_number, l.item_airtable_id,
            l.description, l.quantity_ordered,
            i.name AS item_name, i.unit_of_measure, i.category
       FROM material_orders o
       LEFT JOIN material_order_lines l ON l.order_id = o.id
       LEFT JOIN inventory_items i      ON i.id = l.item_id
      WHERE o.airtable_id = $1
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

  const orderData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Material Orders")}/${id}`,
    { method: "GET" }
  );
  if (!orderData?.id) return resp(404, { ok: false, error: "Order not found." });

  const of = orderData.fields || {};
  const lineIds = (of["Material Order Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  let lines = [];
  if (lineIds.length) {
    const [lineRecords, itemRecords] = await Promise.all([
      fetchAll(API_ROOT_INV, "Material Order Lines", {}),
      itemIndex()
    ]);

    const itemMap = itemRecords;

    lines = lineRecords
      .filter(r => lineIds.includes(r.id))
      .map(r => {
        const f = r.fields || {};
        const itemArr = f["Inventory Item"] || [];
        const itemId = itemArr.length
          ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
          : "";
        const itemData = itemMap[itemId] || {};

        // Detect " [BOX]" suffix marker on description — strip it, set isBox
        const BOX_MARKER = " [BOX]";
        let rawDesc = f["Description"] || "";
        let isBox = false;
        if (typeof rawDesc === "string" && rawDesc.endsWith(BOX_MARKER)) {
          isBox = true;
          rawDesc = rawDesc.slice(0, -BOX_MARKER.length);
        }

        return {
          id:          r.id,
          lineNum:     f["Line Item ID"] || 0,
          itemId:      itemId,
          itemName:    itemData.name || rawDesc || "",
          uom:         itemData.uom || "",
          category:    itemData.cat || "",
          description: rawDesc,
          qty:         f["Quantity Ordered"] || 0,
          isMisc:      !itemId,
          isBox:       isBox
        };
      })
      .sort((a, b) => (a.lineNum || 0) - (b.lineNum || 0));
  }

  return resp(200, {
    ok: true,
    order: {
      id:          orderData.id,
      orderId:     of["Order ID"] || 0,
      jobName:     of["Job Name"] || "",
      vendor:      of["Vendor / Notes"] || "",
      status:      of["Status"]?.name || of["Status"] || "Active",
      dateCreated: of["Date Created"] || "",
      createdBy:   of["Created By"] || "",
      lines
    }
  });
}

// ── CREATE ORDER ──────────────────────────────────────────
async function handleOrderCreate(body) {
  const { estimateId, jobName, vendor, createdBy, lines } = body || {};
  if (!jobName || !jobName.trim()) return resp(400, { ok: false, error: "Job name is required." });
  if (!lines || !lines.length) return resp(400, { ok: false, error: "Order has no items." });

  const orderFields = {
    [F_ORD_JOB_NAME]:   String(jobName).trim(),
    [F_ORD_VENDOR]:     String(vendor || "").trim(),
    [F_ORD_CREATED_BY]: String(createdBy || "").trim(),
    [F_ORD_STATUS]:     "Active"
  };
  if (estimateId) {
    orderFields[F_ORD_ESTIMATE] = [String(estimateId)];
  }

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Material Orders"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields: orderFields }], typecast: true })
  });

  const newId = created.records?.[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create order." });

  // Header first — lines resolve order_id by looking the parent up.
  await syncOrderToNeon(created.records[0]);

  // Create order lines in batches of 10
  if (lines && lines.length) {
    await createOrderLinesHelper(newId, lines);
  }

  // Re-fetch the created order to get the autonumber Order ID
  let orderId = null;
  try {
    const refreshed = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Material Orders")}/${newId}`,
      { method: "GET" }
    );
    orderId = refreshed.fields?.["Order ID"] || null;
    // Re-sync now that the autonumber exists — the create response has no
    // `Order ID`, so without this the order shows as #0 on every screen.
    await syncOrderToNeon(refreshed);
  } catch(e) {
    console.warn("Failed to fetch new order autonumber:", e.message);
  }

  return resp(200, { ok: true, id: newId, orderId });
}

// ── HELPER: Create order lines in batches of 10 ──────────
async function createOrderLinesHelper(orderId, lines) {
  for (let i = 0; i < lines.length; i += 10) {
    const batch = lines.slice(i, i + 10).map(l => {
      const fields = {
        [F_OL_ORDER]: [String(orderId)],
        [F_OL_QTY]:   Number(l.qty || 0)
      };
      if (l.itemId) {
        fields[F_OL_ITEM] = [String(l.itemId)];
      }
      // Always store description for traceability — for inventory items this
      // captures the name at order time so historical orders survive renames.
      // Also append " [BOX]" marker so isBox persists without a schema change.
      let desc = String(l.description || "").trim();
      if (l.isBox) {
        // Ensure we have something in description so the BOX marker isn't orphaned
        if (!desc) desc = "Box order";
        desc = desc + " [BOX]";
      }
      if (desc) {
        fields[F_OL_DESCRIPTION] = desc;
      }
      return { fields };
    });

    const madeLines = await atFetch(API_ROOT_INV, encodeURIComponent("Material Order Lines"), {
      method: "POST",
      body: JSON.stringify({ records: batch, typecast: true })
    });
    await syncOrderLinesToNeon(madeLines?.records);
  }
}

// ── HELPER: Delete all lines for an order ────────────────
async function deleteOrderLines(orderId) {
  const orderData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Material Orders")}/${orderId}`,
    { method: "GET" }
  );
  const lineIds = (orderData.fields["Material Order Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Order Lines")}?${qs}`, {
      method: "DELETE"
    });
    await deleteOrderLinesFromNeon(batch);
  }
}

// ── UPDATE ORDER (status / vendor / notes / lines) ────────────────
async function handleOrderUpdate(body) {
  const { id, status, vendor, lines, replaceLines } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  const fields = {};
  if (status !== undefined) fields[F_ORD_STATUS] = status;
  if (vendor !== undefined) fields[F_ORD_VENDOR] = String(vendor || "");

  // Header fields — only patch if we have any
  if (Object.keys(fields).length) {
    const patched = await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Orders")}/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ fields, typecast: true })
    });
    // Status is what the list filters on, so an unsynced "Complete" would leave
    // the order sitting in the active list.
    await syncOrderToNeon(patched);
  }

  // Line editing
  if (replaceLines && lines !== undefined) {
    await deleteOrderLines(id);
    if (lines.length) {
      await createOrderLinesHelper(id, lines);
    }
  }

  if (!Object.keys(fields).length && !replaceLines) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }

  return resp(200, { ok: true, id });
}

// ── DELETE ORDER ──────────────────────────────────────────
async function handleOrderDelete(body) {
  const { id } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  // Get linked line ids
  const orderData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Material Orders")}/${id}`,
    { method: "GET" }
  );
  const lineIds = (orderData.fields["Material Order Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  // Delete lines in batches of 10
  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Order Lines")}?${qs}`, {
      method: "DELETE"
    });
    await deleteOrderLinesFromNeon(batch);
  }

  // Delete the order
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Orders")}/${id}`, {
    method: "DELETE"
  });
  // ON DELETE CASCADE clears the lines; nothing else would ever remove this row.
  await deleteOrderFromNeon(id);

  return resp(200, { ok: true, deleted: id });
}

// ── ACTIVE ORDERS COUNT (for badge on home button) ────────
async function handleOrdersCount() {
  // A badge on the home screen, so it runs on nearly every page load — one
  // COUNT beats paging the whole table to call .length on it.
  const q = await neonQuery(
    `SELECT count(*)::int AS n FROM material_orders WHERE status = 'Active'`);
  if (q?.rows) return resp(200, { ok: true, _source: "neon", count: q.rows[0]?.n ?? 0 });

  const records = await fetchAll(API_ROOT_INV, "Material Orders", {
    filter: `{Status}='Active'`
  });
  return resp(200, { ok: true, _source: "airtable", count: records.length });
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
            p.airtable_id AS pricing_id, p.unit_cost, p.preferred, p.active,
            p.vendor_part_number, p.min_order_qty, p.lead_time_days,
            p.last_price_update, p.price_valid_until, p.unit_of_measure, p.notes,
            v.airtable_id AS vendor_id, v.name AS vendor_name
       FROM inventory_items i
       LEFT JOIN vendor_pricing p ON p.item_id = i.id
       LEFT JOIN vendors v        ON v.id = p.vendor_id
      WHERE i.airtable_id = $1
      ORDER BY p.preferred DESC NULLS LAST, v.name ASC`, [itemId]);

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
  const [itemData, allPricing, allVendors] = await Promise.all([
    atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, { method: "GET" }),
    fetchAll(API_ROOT_INV, "Vendor Pricing", {}),
    fetchAll(API_ROOT_INV, "Vendors", {})
  ]);

  // ID -> Vendor Name
  const vendorNameById = {};
  allVendors.forEach(v => {
    vendorNameById[v.id] = v.fields?.["Vendor Name"] || "";
  });

  const f           = itemData?.fields || {};
  const defaultCost = Number(f["Default Unit Cost"] || 0);
  // Rollup returns a number if a preferred record has Unit Cost set, else empty
  const liveCost    = Number(f["Unit Cost Rollup (Live)"] || 0);

  // Filter Vendor Pricing to this item only
  const pricing = allPricing.filter(r => {
    const links = r.fields?.["Inventory Item"] || [];
    return links.some(l => {
      const lid = typeof l === "object" ? l.id : String(l);
      return lid === itemId;
    });
  });

  const vendors = pricing.map(r => {
    const pf        = r.fields || {};
    const vendorArr = pf["Vendor"] || [];
    const first     = vendorArr[0];
    // Link fields come back as either plain IDs (REST API default) or {id, name}
    // objects — handle both so this works regardless of Airtable response shape.
    const vendorId  = first
      ? (typeof first === "object" ? first.id : String(first))
      : "";
    const vendorName = vendorNameById[vendorId]
      || (typeof first === "object" && first.name ? first.name : "")
      || "";
    return {
      id:          r.id,
      vendorId,
      vendorName,
      unitCost:    Number(pf["Unit Cost"] || 0),
      uom:         pf["Unit of Measure"]?.name || pf["Unit of Measure"] || "",
      partNumber:  pf["Vendor Part Number"] || "",
      minOrderQty: Number(pf["Min Order Qty"] || 0),
      leadTime:    Number(pf["Lead Time (days)"] || 0),
      lastUpdate:  pf["Last Price Update"] || "",
      validUntil:  pf["Price Valid Until"] || "",
      preferred:   !!pf["Preferred for This Item"],
      active:      !!pf["Active"],
      notes:       pf["Notes"] || ""
    };
  });

  // Display order: preferred first, then ascending by unit cost, then by name
  vendors.sort((a, b) => {
    if (a.preferred !== b.preferred) return a.preferred ? -1 : 1;
    if (a.unitCost  !== b.unitCost)  return a.unitCost - b.unitCost;
    return (a.vendorName || "").localeCompare(b.vendorName || "");
  });

  // Variance only meaningful when we have both a live price and a default cost
  const variance = (liveCost > 0 && defaultCost > 0)
    ? {
        dollar: Math.round((liveCost - defaultCost) * 10000) / 10000,
        pct:    (liveCost - defaultCost) / defaultCost
      }
    : null;

  const preferredVendor = vendors.find(v => v.preferred);

  return resp(200, {
    ok: true,
    summary: {
      defaultCost,
      liveCost,
      variance,
      preferredVendor: preferredVendor?.vendorName || "",
      preferredUpdated: preferredVendor?.lastUpdate || ""
    },
    vendors
  });
}

// ── SYNC DEFAULT UNIT COST TO LIVE VENDOR PRICE ───────────
// Copies the Unit Cost Rollup (Live) value onto Default Unit Cost for this item.
// Admin-convenience button — doesn't touch any Vendor Pricing records.
async function handleSyncItemCostToVendor(body) {
  const { itemId } = body || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  const itemData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Inventory Items")}/${itemId}`,
    { method: "GET" }
  );
  const f        = itemData?.fields || {};
  const liveCost = Number(f["Unit Cost Rollup (Live)"] || 0);

  if (!liveCost || liveCost <= 0) {
    return resp(400, { ok: false, error: "No live vendor price for this item — set a Preferred vendor with Unit Cost first." });
  }

  // Default Unit Cost field ID on Inventory Items — same one used by the receive
  // flow to write back updated costs. Using the ID (not name) for writes.
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fld8aEhTzmEbqgIg4": liveCost } })
  });
  // Same reason as updateItemCost: the reads are Neon's now.
  await syncItemToNeonById(itemId);

  return resp(200, {
    ok: true,
    newDefaultCost: liveCost,
    oldDefaultCost: Number(f["Default Unit Cost"] || 0)
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
      if (action === "locations")         return await handleLocations();
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
      if (body.action === "loadInventoryReference") return await handleLoadInventoryReference();
      if (body.action === "pushExpenses")    return await handlePushExpenses(body);
      if (body.action === "jobDocUploadUrl") return await handleJobDocUploadUrl(body);
      if (body.action === "createItem")        return await handleCreateItem(body);
      if (body.action === "updateItemCost")     return await handleUpdateItemCost(body);
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

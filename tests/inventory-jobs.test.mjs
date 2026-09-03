// Tier-1 backend regression harness for netlify/functions/inventory.js
// ---------------------------------------------------------------------------
// Covers the two job/contractor read handlers after the "drop the synced Jobs
// mirror" Phase 1 repoint: estimatingJobs + templateContractors now read the
// MAIN base Jobs table (Contractor Name (Text)) instead of the inventory-base
// synced Jobs mirror (Contractor (Combined)). These tests assert the new shape
// AND that the inventory base is no longer read for jobs.
//
// Run (portable node):
//   & "C:\Users\irick\nodejs\node.exe" tests/inventory-jobs.test.mjs
// or, if node is on PATH:
//   node tests/inventory-jobs.test.mjs
// Exit code is 0 on all-pass, 1 on any failure (CI-friendly).
// ---------------------------------------------------------------------------

// 1) Fake env BEFORE importing the module (read at import time). Distinct base
//    IDs so the mock (and the assertions) can tell the bases apart by URL.
const MAIN_BASE = "appMain0000000000";
const INV_BASE  = "appInv00000000000";
process.env.AIRTABLE_API_KEY  = "test-key";
process.env.AIRTABLE_BASE_ID  = MAIN_BASE;
process.env.INVENTORY_BASE_ID = INV_BASE;
process.env.AUTH_SECRET       = "test-secret";

// 2) Mock Airtable. Tests set records per (base, table). The mock records every
//    requested URL so a test can prove the inventory-base mirror is NOT read.
let mainJobs = [];
let invJobs  = [];        // mirror — should stay empty/unused after Step A
let invTx    = [];        // Inventory Transactions (for pendingExpenses dual-read)
let invItems = [];        // Inventory Items
const requested = [];     // every fetched URL, for "which base?" assertions

// 2b) Mock Neon (Step B0). @neondatabase/serverless POSTs {query, params} to
//     /sql over fetch and expects {fields, rows} back with rows as VALUE
//     ARRAYS — it zips them into objects itself. Handing it objects throws
//     "c.map is not a function". Shape verified against the installed driver.
//     `neonFail` simulates a query error so the Airtable fallback is provable.
const NEON_COLS = ["handle", "name", "po", "status", "tax_status", "contractor_name"];
let neonRows  = [];
let neonFail  = false;
const neonQueries = [];       // every {query, params} sent, for assertions

function neonOn(rows) {
  process.env.DATABASE_URL = "postgresql://u:p@fake.neon.tech/db";
  neonRows = rows; neonFail = false; neonQueries.length = 0;
}
function neonOff() { delete process.env.DATABASE_URL; neonRows = []; neonFail = false; }

globalThis.fetch = async (url, opts) => {
  requested.push(String(url));

  if (String(url).includes("/sql")) {
    try { neonQueries.push(JSON.parse(opts?.body || "{}")); } catch { /* ignore */ }
    if (neonFail) return { ok: false, status: 500, text: async () => "neon exploded" };

    // ⚠ This used to FAIL the ledger query on purpose so pendingExpenses would
    // fall back to Airtable and read this suite's transaction fixtures. That
    // fallback is gone: since the write cutover the ledger and the item index
    // are Neon-only, and pendingExpenses answers 503 rather than serving a
    // frozen copy. The fixtures stay Airtable-shaped — this suite is about JOB
    // RESOLUTION and rewriting them would lose that — so they are reshaped into
    // Neon rows here instead.
    const sqlText = String(opts?.body || "");
    const reply = (cols, rows) => {
      const p2 = {
        command: "SELECT", rowCount: rows.length, rowAsArray: false,
        fields: cols.map((n, i) => ({ name: n, dataTypeID: 25, tableID: 0, columnID: i + 1,
                                      dataTypeSize: -1, dataTypeModifier: -1, format: "text" })),
        rows: rows.map(r => cols.map(c => (c in r ? r[c] : null))),
      };
      return { ok: true, status: 200, headers: { get: () => "application/json" },
               text: async () => JSON.stringify(p2), json: async () => p2 };
    };

    if (/FROM inventory_transactions/i.test(sqlText)) {
      const lnk = (v) => Array.isArray(v) ? (typeof v[0] === "object" ? v[0]?.id : v[0]) : v;
      const sel = (v) => (v && typeof v === "object" ? v.name : v);
      return reply(["id", "item_airtable_id", "quantity", "txn_type", "unit_cost_snapshot",
                    "job_airtable_id", "job_name"],
        invTx.map(t => ({
          id: t.id,
          item_airtable_id: lnk(t.fields?.["Inventory Item"]),
          quantity: String(t.fields?.["Quantity"] ?? 0),
          txn_type: sel(t.fields?.["Transaction Type"]),
          unit_cost_snapshot: t.fields?.["Unit Cost (Snapshot)"] == null
            ? null : String(t.fields["Unit Cost (Snapshot)"]),
          job_airtable_id: t.fields?.["Job ID (Main)"] ?? null,
          job_name: t.fields?.["Job Name"] ?? null,
        })));
    }

    if (/FROM inventory_items/i.test(sqlText)) {
      return reply(["airtable_id", "name", "category", "unit_of_measure",
                    "default_unit_cost", "wire_ft_per_lb"],
        invItems.map(i => ({
          airtable_id: i.id, name: i.fields?.["Item Name"] || "",
          category: i.fields?.["Category"] || null,
          unit_of_measure: i.fields?.["Unit of Measure"] || null,
          default_unit_cost: i.fields?.["Default Unit Cost"] == null
            ? null : String(i.fields["Default Unit Cost"]),
          wire_ft_per_lb: i.fields?.["Wire ft/lb"] == null
            ? null : String(i.fields["Wire ft/lb"]),
        })));
    }

    const payload = {
      command: "SELECT", rowCount: neonRows.length, rowAsArray: false,
      fields: NEON_COLS.map((n, i) => ({
        name: n, dataTypeID: 25, tableID: 0, columnID: i + 1,
        dataTypeSize: -1, dataTypeModifier: -1, format: "text",
      })),
      rows: neonRows.map(r => NEON_COLS.map(c => (c in r ? r[c] : null))),
    };
    return {
      ok: true, status: 200,
      headers: { get: () => "application/json" },
      text: async () => JSON.stringify(payload),
      json: async () => payload,
    };
  }

  const m = String(url).match(/\/v0\/([^/]+)\/([^?]+)/);
  const base  = m ? m[1] : "";
  const table = m ? decodeURIComponent(m[2]) : "";
  let records = [];
  if (table === "Jobs") records = base === MAIN_BASE ? mainJobs : invJobs;
  else if (table === "Inventory Transactions") records = invTx;
  else if (table === "Inventory Items")         records = invItems;
  return { ok: true, status: 200, text: async () => JSON.stringify({ records }) };
};

// 3) Import the real handler + auth after env + mock are in place.
const { handler }   = await import("../netlify/functions/inventory.js");
const { signToken } = await import("../netlify/functions/_auth.js");
const { primeRevocationCache } = await import("../netlify/functions/_revocation.js");
const TOK = signToken({ id: "recEmp", role: "employee" });

// Nobody is revoked. Primed explicitly because the Neon tests below set
// DATABASE_URL, which would otherwise send the revocation loader into the Neon
// mock and have it read job rows as a revocation list.
primeRevocationCache([]);

// ── tiny assert framework (no deps) ──
let pass = 0, fail = 0;
const log = [];
async function test(name, fn) {
  try { await fn(); log.push(["✓", name]); pass++; }
  catch (e) { log.push(["✗", `${name} — ${e.message}`]); fail++; }
}
const eq = (a, b, m) => { if (a !== b) throw new Error(`${m || ""} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); };

const GET = (action) => handler({
  httpMethod: "GET",
  queryStringParameters: { action },
  headers: { authorization: `Bearer ${TOK}` }
});
const json = (res) => JSON.parse(res.body);
const hitInvJobs = () => requested.some(u => u.includes(`/v0/${INV_BASE}/Jobs`));

// Shared fixture: two main-base jobs, one with a PO display + linked contractor.
function seedMain() {
  mainJobs = [
    { id: "recJobA", fields: {
        "Job Name": "Blue Ridge Poultry", "Job PO": "Blue Ridge Poultry (BRB 126)",
        "Job Status": { name: "Awarded" }, "Tax Status": { name: "Taxable" },
        "Contractor Name (Text)": "Case Farms" } },
    { id: "recJobB", fields: {
        "Job Name": "Miller Barn", "Job PO": "",
        "Job Status": { name: "Estimating" }, "Tax Status": "Non-Taxable",
        "Contractor Name (Text)": "Miller Poultry" } }
  ];
  invJobs = [{ id: "recMirror", fields: { "Job Name": "STALE MIRROR JOB", "Contractor (Combined)": "Should Not Appear" } }];
  invTx = [];
  invItems = [];
  requested.length = 0;
  neonOff();          // callers that want Neon call neonOn() after seeding
}

// ── cases ──
// ⚠ The four Airtable-path cases that stood here are GONE (2026-09-03), not
// merely disabled: `estimatingJobs`, `templateContractors` and `jobs` have no
// Airtable path left to exercise. Everything they asserted about the display
// string, the contractor and the tax flag is covered against Neon fixtures in
// the Step B0 block below — see "B0 estimatingJobs: a New Lead with no PO".
// What replaces them is the property that actually needs guarding now.

await test("every job picker REFUSES when Neon is down, and reads no Airtable", async () => {
  // One case over all four, because the failure they share is the dangerous one:
  // any single picker quietly falling back would serve a frozen list that looks
  // complete. Asserting the URL was never requested is the durable half — a
  // reintroduced fetch fails here even if it happens to return the right shape.
  for (const action of ["jobs", "awardedJobs", "estimatingJobs", "templateContractors"]) {
    seedMain();                     // ends with neonOff() — Neon has no opinion
    const res = await GET(action);
    eq(res.statusCode, 503, `${action}: refuses rather than answering`);
    eq(json(res).ok, false, `${action}: ok:false`);
    eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), false,
       `${action}: the main base was NOT read`);
    eq(hitInvJobs(), false, `${action}: nor the inventory-base mirror`);
  }
});

await test("employees REFUSES when Neon is down — an empty picker is not an answer", async () => {
  // The specific harm: a blank name picker looks like a working screen with no
  // staff, so someone logs material against nobody rather than seeing an error.
  seedMain();
  const res = await GET("employees");
  eq(res.statusCode, 503, "refuses");
  eq(requested.some(u => u.includes("Employees")), false, "and never pages the Airtable roster");
});

await test("pendingExpenses: resolves by 'Job ID (Main)' text; never reads the mirror", async () => {
  seedMain();
  // seedMain() ends with neonOff(), which suited the pre-B0 cases that assert
  // the Airtable job path. pendingExpenses has no Airtable path any more — the
  // ledger, the item index and the job index are all Neon — so it needs Neon on.
  neonOn([
    { handle: "recJobA", name: "Blue Ridge Poultry", po: "Blue Ridge Poultry (BRB 126)",
      status: "Awarded", tax_status: "Taxable", contractor_name: "Case Farms" },
    { handle: "recJobB", name: "Miller Barn", po: "",
      status: "Estimating", tax_status: "Non-Taxable", contractor_name: "Miller Poultry" },
  ]);
  invItems = [{ id: "recItem1", fields: { "Item Name": "12-2 Wire", "Default Unit Cost": 10 } }];
  invTx = [
    { id: "recTxA", fields: {
        "Inventory Item": ["recItem1"], "Quantity": 2, "Transaction Type": { name: "Use" },
        "Unit Cost (Snapshot)": 10, "Job ID (Main)": "recJobA", "Job Name": "Blue Ridge Poultry (BRB 126)" } },
    { id: "recTxB", fields: {
        "Inventory Item": ["recItem1"], "Quantity": 3, "Transaction Type": { name: "Use" },
        "Unit Cost (Snapshot)": 10, "Job ID (Main)": "recJobA", "Job Name": "Blue Ridge Poultry (BRB 126)" } }
  ];
  requested.length = 0;
  const r = json(await GET("pendingExpenses"));
  eq(r.ok, true, "ok");
  eq(hitInvJobs(), false, "Step C: must NOT fetch the inventory-base Jobs mirror");
  eq(r.pending.length, 1, "both txns group under one main-base job id");
  const g = r.pending[0];
  eq(g.jobId, "recJobA", "grouped under the main-base job id");
  eq(g.txIds.length, 2, "both transactions captured");
  eq(g.jobTotal, 50, "(2+3) × $10 = $50");
  eq(r.unmatched.length, 0, "nothing stranded");
});

// ── Step B0: the main-base Jobs reads move to Neon ───────────────────────────
// Neon is the only store these have. Airtable was the fallback until 2026-09-03;
// now a failed read refuses, so the cases below prove BOTH halves — that Neon
// answers, and that nothing reaches for Airtable when it doesn't.

const NEON_FIXTURE = [
  // PO present → PO wins as the display.
  { handle: "recJobA", name: "Blue Ridge Poultry", po: "Blue Ridge Poultry (BRB 126)",
    status: "Awarded", tax_status: "Taxable", contractor_name: "Case Farms" },
  // A New Lead with NO po. This is the po_locked trap in miniature: the PO only
  // locks at award time, so every New Lead job has a blank locked PO. If the
  // query read po_locked these would vanish from the estimating picker and the
  // shortened list would look complete.
  { handle: "recJobN", name: "Miller Barn", po: "",
    status: "New Lead", tax_status: "Tax Exempt", contractor_name: "Miller Poultry" },
];

await test("B0 jobs: served from Neon, ids are the DUAL handle", async () => {
  seedMain();
  neonOn([NEON_FIXTURE[0]]);
  const r = json(await GET("jobs"));
  eq(r.ok, true, "ok");
  eq(r._source, "neon", "served from Neon");
  eq(r.jobs.length, 1, "one awarded job");
  // ⚠ This assertion USED TO READ "never the uuid", and that was the bug. A job
  // with a rec id must still emit it byte for byte — that half is unchanged and
  // is what keeps every cart, ledger row and Job ID (Main) already in the data
  // meaning the same thing.
  eq(r.jobs[0].id, "recJobA", "a job Airtable created still emits its rec id");
  eq(r.jobs[0].name, "Blue Ridge Poultry (BRB 126)", "PO display preferred");
  eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), false, "must not also page Airtable");
});

// The regression this file shipped with. JOB_CREATE_SOURCE=native went live on
// 2026-08-24 and the picker query filtered `COALESCE(airtable_id,'') <> ''`, so
// a job born in the app was missing from every inventory picker — awarded,
// visible in the field app, and impossible to log material against. Reported on
// Test 10. The filter is gone; the emit is COALESCE(airtable_id, id::text).
await test("B0 jobs: a NATIVE job (no rec id) appears, keyed by its uuid", async () => {
  seedMain();
  const NATIVE_UUID = "846245ef-294f-423b-a2b1-4b4a919607f8";
  neonOn([
    NEON_FIXTURE[0],
    { handle: NATIVE_UUID, name: "Test 10", po: "Test 10 (MIT 301)",
      status: "Awarded", tax_status: "Taxable", contractor_name: "Misc Jobs" },
  ]);
  const r = json(await GET("jobs"));
  eq(r.jobs.length, 2, "the native job is NOT filtered out");
  const nativeJob = r.jobs.find(j => j.id === NATIVE_UUID);
  eq(!!nativeJob, true, "native job present, keyed by uuid");
  eq(nativeJob.name, "Test 10 (MIT 301)", "PO display");
  // And the query must not have re-grown a native-row filter in any spelling —
  // the one that shipped was `COALESCE(airtable_id, '') <> ''`, which is exactly
  // why sweeping for `airtable_id IS NOT NULL` came back clean.
  const sql = String(neonQueries[0]?.query || "").replace(/COALESCE\(airtable_id, id::text\)/g, "");
  eq(/airtable_id\s+IS\s+NOT\s+NULL|COALESCE\(airtable_id|airtable_id\s*(<>|!=)\s*''/.test(sql), false,
     `no native-row filter in the picker query — got: ${sql.slice(0, 160)}`);
});

await test("B0 estimatingJobs: a New Lead with no PO still appears, named by Job Name", async () => {
  seedMain();
  neonOn(NEON_FIXTURE);
  const r = json(await GET("estimatingJobs"));
  eq(r._source, "neon", "served from Neon");
  eq(r.jobs.length, 2, "New Lead job NOT dropped");
  const n = r.jobs.find(j => j.id === "recJobN");
  eq(n.name, "Miller Barn", "falls back to name when po is blank");
  eq(n.taxable, false, "Tax Exempt → taxable false");
  eq(n.contractor, "Miller Poultry", "contractor carried through");
  // the status set must still be the estimating one, not the awarded one
  const statuses = neonQueries[0]?.params?.[0] || "";
  eq(String(statuses).includes("New Lead"), true, "query asked for the estimating status set");
});

await test("B0 zero rows is an ANSWER, not a reason to fall back", async () => {
  seedMain();
  neonOn([]);                      // Neon succeeded and says nothing matches
  const r = json(await GET("jobs"));
  eq(r._source, "neon", "still Neon — an empty status set is legitimate");
  eq(r.jobs.length, 0, "no jobs");
  eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), false,
     "must NOT fall back on empty, or a partial list gets served as a whole one");
});

await test("B0 Neon failure REFUSES — it does not serve the frozen Airtable list", async () => {
  // ⚠⚠ THE INVERSION, 2026-09-03. This case used to assert the opposite, and
  // the fallback was right while Airtable was still being written. It stopped
  // being right at AIRTABLE_WRITES=off (2026-08-25): the Jobs table has been
  // frozen since, so "the whole list intact" would now mean the list as it stood
  // in August — missing every job created since, and a short list looks exactly
  // like a complete one.
  seedMain();
  neonOn([]); neonFail = true;     // query errors
  const res = await GET("jobs");
  eq(res.statusCode, 503, "refused");
  eq(json(res).ok, false, "and says so");
  eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), false, "Airtable was NOT read");
});

await test("B0 templateContractors: dedupes and sorts from Neon", async () => {
  seedMain();
  neonOn([
    ...NEON_FIXTURE,
    { handle: "recJobC", name: "Dup", po: "Dup", status: "Completed", tax_status: "Taxable", contractor_name: "Case Farms" },
    { handle: "recJobD", name: "Blank", po: "Blank", status: "Completed", tax_status: "Taxable", contractor_name: "" },
  ]);
  const r = json(await GET("templateContractors"));
  eq(r._source, "neon", "served from Neon");
  eq(JSON.stringify(r.contractors), JSON.stringify(["Case Farms", "Miller Poultry"]), "sorted, deduped, no blanks");
  eq(neonQueries[0]?.params?.length || 0, 0, "asks for ALL jobs — contractors aren't status-scoped");
});

await test("B0 pendingExpenses: resolves the job index out of Neon", async () => {
  seedMain();
  neonOn([NEON_FIXTURE[0]]);
  invItems = [{ id: "recItem1", fields: { "Item Name": "12-2 Wire", "Default Unit Cost": 10 } }];
  invTx = [{ id: "recTxA", fields: {
    "Inventory Item": ["recItem1"], "Quantity": 2, "Transaction Type": { name: "Use" },
    "Unit Cost (Snapshot)": 10, "Job ID (Main)": "recJobA" } }];
  const r = json(await GET("pendingExpenses"));
  eq(r.ok, true, "ok");
  eq(r.pending.length, 1, "grouped");
  eq(r.pending[0].jobId, "recJobA", "resolved by the main-base rec id, out of Neon");
  eq(r.pending[0].taxable, true, "taxable came from Neon's tax_status");
  eq(r.unmatched.length, 0, "nothing stranded");
  eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), false, "main-base Jobs not paged");
});

await test("pendingExpenses: stale 'Job ID (Main)' → unmatched; jobless/link-only rows → skipped (no noise)", async () => {
  seedMain();
  // seedMain() ends with neonOff(), which suited the pre-B0 cases that assert
  // the Airtable job path. pendingExpenses has no Airtable path any more — the
  // ledger, the item index and the job index are all Neon — so it needs Neon on.
  neonOn([
    { handle: "recJobA", name: "Blue Ridge Poultry", po: "Blue Ridge Poultry (BRB 126)",
      status: "Awarded", tax_status: "Taxable", contractor_name: "Case Farms" },
    { handle: "recJobB", name: "Miller Barn", po: "",
      status: "Estimating", tax_status: "Non-Taxable", contractor_name: "Miller Poultry" },
  ]);
  invItems = [{ id: "recItem1", fields: { "Item Name": "12-2 Wire", "Default Unit Cost": 10 } }];
  invTx = [
    // stale id (job no longer in main base) → genuinely "couldn't be matched" → surfaced
    { id: "recTxStale", fields: {
        "Inventory Item": ["recItem1"], "Quantity": 4, "Transaction Type": { name: "Use" },
        "Unit Cost (Snapshot)": 10, "Job ID (Main)": "recGhostGone", "Job Name": "Ghost Job" } },
    // jobless scratch row (no Job ID (Main), no link) → skipped, NOT surfaced
    { id: "recTxJobless", fields: {
        "Inventory Item": ["recItem1"], "Quantity": 5, "Transaction Type": { name: "Use" },
        "Unit Cost (Snapshot)": 10 } },
    // legacy link-only row (no Job ID (Main)) → also skipped now (mirror no longer read)
    { id: "recTxLinkOnly", fields: {
        "Inventory Item": ["recItem1"], "Quantity": 1, "Transaction Type": { name: "Use" },
        "Unit Cost (Snapshot)": 10, "Job": ["recMirrorX"] } }
  ];
  requested.length = 0;
  const r = json(await GET("pendingExpenses"));
  eq(r.ok, true, "ok");
  eq(r.pending.length, 0, "nothing pushable");
  eq(r.unmatched.length, 1, "only the stale-id row is surfaced; jobless/link-only are skipped");
  eq(r.unmatched[0].estTotal, 40, "the stale row's $40 is flagged");
});

// ── report ──
console.log("\ninventory.js jobs/contractors + push (Steps A+B+C, mirror-free)\n" + "-".repeat(54));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(54));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

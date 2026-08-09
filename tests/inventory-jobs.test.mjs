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
const NEON_COLS = ["airtable_id", "name", "po", "status", "tax_status", "contractor_name"];
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
  neonOff();          // the pre-B0 cases assert the Airtable path specifically
}

// ── cases ──

await test("estimatingJobs reads MAIN base only (mirror untouched)", async () => {
  seedMain();
  const r = json(await GET("estimatingJobs"));
  eq(r.ok, true, "ok");
  eq(r.jobs.length, 2, "two jobs");
  eq(hitInvJobs(), false, "must NOT fetch the inventory-base Jobs mirror");
});

await test("estimatingJobs uses Job PO display and Contractor Name (Text)", async () => {
  seedMain();
  const r = json(await GET("estimatingJobs"));
  const a = r.jobs.find(j => j.id === "recJobA");
  const b = r.jobs.find(j => j.id === "recJobB");
  eq(a.name, "Blue Ridge Poultry (BRB 126)", "PO display preferred");
  eq(a.contractor, "Case Farms", "contractor from main formula");
  eq(a.taxable, true, "taxable parsed from object value");
  eq(b.name, "Miller Barn", "falls back to Job Name when PO empty");
  eq(b.taxable, false, "non-taxable parsed from string value");
});

await test("templateContractors returns sorted distinct contractors from MAIN base", async () => {
  seedMain();
  // add a duplicate + an empty to prove dedupe + blank-skip
  mainJobs.push({ id: "recJobC", fields: { "Job Name": "Dup", "Contractor Name (Text)": "Case Farms" } });
  mainJobs.push({ id: "recJobD", fields: { "Job Name": "Blank", "Contractor Name (Text)": "" } });
  requested.length = 0;
  const r = json(await GET("templateContractors"));
  eq(r.ok, true, "ok");
  eq(JSON.stringify(r.contractors), JSON.stringify(["Case Farms", "Miller Poultry"]), "sorted, deduped, no blanks");
  eq(hitInvJobs(), false, "must NOT fetch the inventory-base Jobs mirror");
});

// ── Step B: USE-cart picker repoint + expense-push dual-read ──

await test("jobs (USE cart picker) reads MAIN base only (mirror untouched)", async () => {
  seedMain();
  const r = json(await GET("jobs"));
  eq(r.ok, true, "ok");
  eq(hitInvJobs(), false, "must NOT fetch the inventory-base Jobs mirror");
  // returns main-base record IDs with a Name (PO) display
  const a = r.jobs.find(j => j.id === "recJobA");
  eq(!!a, true, "main job recJobA present");
  eq(a.name, "Blue Ridge Poultry (BRB 126)", "PO display preferred");
});

await test("pendingExpenses: resolves by 'Job ID (Main)' text; never reads the mirror", async () => {
  seedMain();
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
// Airtable stays the fallback, so every case here also has to prove the
// fallback still fires — a Neon-first read that can't fall back is worse than
// no migration at all.

const NEON_FIXTURE = [
  // PO present → PO wins as the display.
  { airtable_id: "recJobA", name: "Blue Ridge Poultry", po: "Blue Ridge Poultry (BRB 126)",
    status: "Awarded", tax_status: "Taxable", contractor_name: "Case Farms" },
  // A New Lead with NO po. This is the po_locked trap in miniature: the PO only
  // locks at award time, so every New Lead job has a blank locked PO. If the
  // query read po_locked these would vanish from the estimating picker and the
  // shortened list would look complete.
  { airtable_id: "recJobN", name: "Miller Barn", po: "",
    status: "New Lead", tax_status: "Tax Exempt", contractor_name: "Miller Poultry" },
];

await test("B0 jobs: served from Neon, ids are Airtable rec ids (never the uuid)", async () => {
  seedMain();
  neonOn([NEON_FIXTURE[0]]);
  const r = json(await GET("jobs"));
  eq(r.ok, true, "ok");
  eq(r._source, "neon", "served from Neon");
  eq(r.jobs.length, 1, "one awarded job");
  eq(r.jobs[0].id, "recJobA", "id is the AIRTABLE rec id — a uuid here corrupts Job ID (Main)");
  eq(r.jobs[0].name, "Blue Ridge Poultry (BRB 126)", "PO display preferred");
  eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), false, "must not also page Airtable");
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

await test("B0 Neon failure falls back to Airtable, whole list intact", async () => {
  seedMain();
  neonOn([]); neonFail = true;     // query errors
  const r = json(await GET("jobs"));
  eq(r._source, "airtable", "fell back");
  eq(requested.some(u => u.includes(`/v0/${MAIN_BASE}/Jobs`)), true, "Airtable was read");
  eq(r.jobs.length >= 1, true, "served the Airtable list, not an empty one");
});

await test("B0 templateContractors: dedupes and sorts from Neon", async () => {
  seedMain();
  neonOn([
    ...NEON_FIXTURE,
    { airtable_id: "recJobC", name: "Dup", po: "Dup", status: "Completed", tax_status: "Taxable", contractor_name: "Case Farms" },
    { airtable_id: "recJobD", name: "Blank", po: "Blank", status: "Completed", tax_status: "Taxable", contractor_name: "" },
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

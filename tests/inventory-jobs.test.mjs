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
let invJobs  = [];        // mirror — should stay empty/unused after Phase 1
const requested = [];     // every fetched URL, for "which base?" assertions

globalThis.fetch = async (url) => {
  requested.push(String(url));
  const m = String(url).match(/\/v0\/([^/]+)\/([^?]+)/);
  const base  = m ? m[1] : "";
  const table = m ? decodeURIComponent(m[2]) : "";
  let records = [];
  if (table === "Jobs") records = base === MAIN_BASE ? mainJobs : invJobs;
  return { ok: true, status: 200, text: async () => JSON.stringify({ records }) };
};

// 3) Import the real handler + auth after env + mock are in place.
const { handler }   = await import("../netlify/functions/inventory.js");
const { signToken } = await import("../netlify/functions/_auth.js");
const TOK = signToken({ id: "recEmp", role: "employee" });

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
  requested.length = 0;
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

// ── report ──
console.log("\ninventory.js jobs/contractors (Phase 1 mirror repoint)\n" + "-".repeat(54));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(54));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

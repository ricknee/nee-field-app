// Tier-1 backend regression harness for netlify/functions/airtable.js
// ---------------------------------------------------------------------------
// Runs the REAL exported handler() with env faked and global fetch mocked, so
// it needs NO Netlify and NO Airtable — fully offline, deterministic, < 1s.
// This is the highest-leverage automated layer: the functions are near-pure
// (action in -> JSON out). Add a case here for every bug you fix so it can't
// regress.
//
// Run (portable node):
//   & "C:\Users\irick\nodejs\node.exe" tests/handlers.test.mjs
// or, if node is on PATH:
//   node tests/handlers.test.mjs
// Exit code is 0 on all-pass, 1 on any failure (CI-friendly).
// ---------------------------------------------------------------------------

// 1) Fake env BEFORE importing the module (it reads these at import time).
process.env.AIRTABLE_API_KEY = "test-key";
process.env.AIRTABLE_BASE_ID = "testbase";
process.env.GOOGLE_MAPS_API_KEY = "test-maps";
process.env.ADMIN_BACKFILL_TOKEN = "test-token";
process.env.AUTH_SECRET = "test-secret"; // signs/verifies session tokens

// 2) Mock Airtable. Tests set `mockTables[<tableName>] = [ {id, fields} ]`.
//    The mock parses the table name out of the REST URL and returns those
//    records (single page, no offset). Reads only — these handlers don't write.
let mockTables = {};
globalThis.fetch = async (url) => {
  const m = String(url).match(/\/v0\/[^/]+\/([^?]+)/);
  const table = m ? decodeURIComponent(m[1]) : "";
  const records = mockTables[table] || [];
  return { ok: true, status: 200, text: async () => JSON.stringify({ records }) };
};

// 3) Import the real handler (dynamic import = after env is set).
const { handler } = await import("../netlify/functions/airtable.js");
const { signToken, verifyToken } = await import("../netlify/functions/_auth.js");

// Test session tokens — auth is now enforced, so reads/writes need a valid one.
const ADMIN_TOK  = signToken({ id: "recAdmin",  role: "admin" });
const EMP_TOK    = signToken({ id: "recEmp",    role: "employee" });
const OFFICE_TOK = signToken({ id: "recOffice", role: "office" });
const VIEWER_TOK = signToken({ id: "recViewer", role: "viewer" });

// ── tiny assert framework (no deps) ──
let pass = 0, fail = 0;
const log = [];
async function test(name, fn) {
  try { await fn(); log.push(["✓", name]); pass++; }
  catch (e) { log.push(["✗", `${name} — ${e.message}`]); fail++; }
}
const eq = (a, b, m) => { if (a !== b) throw new Error(`${m || ""} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); };
const ok = (c, m) => { if (!c) throw new Error(m || "expected truthy"); };
// GET/POST default to an admin token so existing data-shape tests keep passing;
// pass a different token (or null) to exercise auth/role behavior.
const hdr  = (tok) => (tok ? { authorization: `Bearer ${tok}` } : {});
const GET  = (action, p = {}, tok = ADMIN_TOK) => handler({ httpMethod: "GET",  queryStringParameters: { action, ...p }, headers: hdr(tok) });
const POST = (action, b = {}, tok = ADMIN_TOK) => handler({ httpMethod: "POST", body: JSON.stringify({ action, ...b }), headers: hdr(tok) });
const json = (res) => JSON.parse(res.body);

// ── cases ──
await test("OPTIONS preflight → 200 + CORS", async () => {
  const res = await handler({ httpMethod: "OPTIONS" });
  eq(res.statusCode, 200, "status");
  eq(res.headers["Access-Control-Allow-Origin"], "*", "CORS");
});

await test("unknown GET action → 400", async () => {
  eq((await GET("definitelyNotAnAction")).statusCode, 400, "status");
});

await test("login: correct identifier + PIN → 200 with role", async () => {
  mockTables = { Employees: [
    { id: "recE1", fields: { "Employee Name": "Rick Nee", PIN: "1234", Role: "admin", Active: true } },
  ] };
  const res = await POST("login", { identifier: "rick nee", pin: "1234" }); // case-insensitive
  eq(res.statusCode, 200, "status");
  const b = json(res);
  ok(b.ok, "ok"); eq(b.user.role, "admin", "role"); eq(b.user.id, "recE1", "id");
  ok(b.token && verifyToken(b.token), "login issues a verifiable token");
  eq(verifyToken(b.token).role, "admin", "token carries the role");
});

await test("login: wrong PIN → 401", async () => {
  mockTables = { Employees: [
    { id: "recE1", fields: { "Employee Name": "Rick Nee", PIN: "1234", Role: "admin", Active: true } },
  ] };
  eq((await POST("login", { identifier: "rick nee", pin: "0000" })).statusCode, 401, "status");
});

await test("login: inactive employee → 401", async () => {
  mockTables = { Employees: [
    { id: "recE1", fields: { "Employee Name": "Rick Nee", PIN: "1234", Role: "admin", Active: false } },
  ] };
  eq((await POST("login", { identifier: "rick nee", pin: "1234" })).statusCode, 401, "status");
});

await test("login: unknown role normalizes to 'employee'", async () => {
  mockTables = { Employees: [
    { id: "recE9", fields: { "Employee Name": "Temp Guy", PIN: "9", Role: "Intern", Active: true } },
  ] };
  eq(json(await POST("login", { identifier: "temp guy", pin: "9" })).user.role, "employee", "role fallback");
});

await test("jobs: maps records and filters out archived/closed", async () => {
  mockTables = { Jobs: [
    { id: "recJ1", fields: { "Job Name": "Live Job", "Job Status": "Awarded" } },
    { id: "recJ2", fields: { "Job Name": "Old Job",  "Job Status": "Archived" } },
    { id: "recJ3", fields: { "Job Name": "Done Job", "Job Status": "Closed" } },
  ] };
  const b = json(await GET("jobs"));
  eq(b.jobs.length, 1, "filtered"); eq(b.jobs[0].name, "Live Job", "name"); eq(b.jobs[0].id, "recJ1", "id");
});

await test("getNextInvoiceNumber: floors at 1633 when empty", async () => {
  mockTables = { Invoices: [] };
  eq(json(await POST("getNextInvoiceNumber")).nextNumber, 1633, "floor");
});

await test("getNextInvoiceNumber: returns max + 1", async () => {
  mockTables = { Invoices: [
    { id: "recI1", fields: { "Invoice Display #": 1700 } },
    { id: "recI2", fields: { "Invoice Display #": 1699 } },
  ] };
  eq(json(await POST("getNextInvoiceNumber")).nextNumber, 1701, "max+1");
});

// ── auth / authorization cases ──
await test("auth: no token → 401 on a read", async () => {
  mockTables = { Jobs: [] };
  eq((await GET("jobs", {}, null)).statusCode, 401, "no-token");
});

await test("auth: garbage/forged token → 401", async () => {
  mockTables = { Jobs: [] };
  eq((await GET("jobs", {}, "garbage.sig")).statusCode, 401, "bad-token");
});

await test("authz: viewer is read-only → 403 on a write", async () => {
  eq((await POST("addGeneralExpense", { jobId: "recJ" }, VIEWER_TOK)).statusCode, 403, "viewer write");
});

await test("authz: viewer → 200 on a read", async () => {
  mockTables = { Jobs: [] };
  eq((await GET("jobs", {}, VIEWER_TOK)).statusCode, 200, "viewer read");
});

await test("authz: employee → 403 on admin-only payrollRunCreate", async () => {
  eq((await POST("payrollRunCreate", {}, EMP_TOK)).statusCode, 403, "emp admin-only");
});

await test("authz: office → 403 on payroll read (payroll-eligible-only)", async () => {
  eq((await GET("payrollRunsList", {}, OFFICE_TOK)).statusCode, 403, "office payroll");
});

await test("authz: employee passes auth on a field write (not 401/403)", async () => {
  mockTables = {};
  const s = (await POST("addGeneralExpense", { jobId: "recJ" }, EMP_TOK)).statusCode;
  ok(s !== 401 && s !== 403, `employee field write should pass auth, got ${s}`);
});

// ── report ──
console.log("\nTier-1 backend handler tests (airtable.js)\n");
for (const [s, n] of log) console.log(`  ${s} ${n}`);
console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

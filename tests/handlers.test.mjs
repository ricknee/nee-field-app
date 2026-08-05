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
let lastFetch = null; // {url, opts} of the most recent request — lets write tests inspect the PATCH body
// Google Distance Matrix stub for handleCalculateMileage. Tests set
// `mockGoogle` to the JSON body Google would return. Note this branch returns a
// response with .json() (what that handler calls), not .text() like Airtable.
let mockGoogle = null;
// pCloud stub for the jobsite-photo handlers. Tests set `mockPcloud.listfolder`
// to the folder contents and `mockPcloud.bytes` to the thumbnail payload.
// pCloud always answers HTTP 200 and signals failure in the body's `result`
// field, so the stub mirrors that — a stub that used HTTP status would let a
// real bug (treating result!==0 as success) pass.
let mockPcloud = { listfolder: [], bytes: Buffer.from("JPEGBYTES"), result: 0 };
globalThis.fetch = async (url, opts) => {
  lastFetch = { url: String(url), opts: opts || {} };
  const method = (opts?.method || "GET").toUpperCase();
  if (String(url).includes("maps.googleapis.com")) {
    return { ok: true, status: 200, json: async () => mockGoogle };
  }
  if (String(url).includes("pcloud.com")) {
    const u = new URL(String(url));
    if (mockPcloud.result !== 0) {
      return { ok: true, status: 200,
        headers: { get: () => "application/json" },
        json: async () => ({ result: mockPcloud.result, error: "pcloud says no" }) };
    }
    if (u.pathname === "/listfolder") {
      return { ok: true, status: 200,
        headers: { get: () => "application/json" },
        json: async () => ({ result: 0, metadata: { contents: mockPcloud.listfolder } }) };
    }
    if (u.pathname === "/getthumb") {
      return { ok: true, status: 200,
        headers: { get: () => "image/jpeg" },
        arrayBuffer: async () => mockPcloud.bytes };
    }
    if (u.pathname === "/getfilelink") {
      return { ok: true, status: 200,
        headers: { get: () => "application/json" },
        json: async () => ({ result: 0, hosts: ["p-def1.pcloud.com"], path: "/full.jpg" }) };
    }
  }
  if (String(url).includes("p-def1.pcloud.com")) {
    return { ok: true, status: 200,
      headers: { get: () => "image/jpeg" },
      arrayBuffer: async () => mockPcloud.bytes };
  }
  // Path after /v0/<base>/ → "<Table>" (list) or "<Table>/<recId>" (single record).
  const m = String(url).match(/\/v0\/[^/]+\/([^/?]+)(?:\/([^?]+))?/);
  const table = m ? decodeURIComponent(m[1]) : "";
  const recId = m && m[2] ? decodeURIComponent(m[2]) : "";
  const rows = mockTables[table] || [];
  let bodyObj;
  if (recId) {
    // Single-record op (GET one / PATCH / DELETE). Airtable returns the record
    // object directly, not a {records:[...]} envelope.
    const rec = rows.find(r => r.id === recId) || { id: recId, fields: {} };
    if (method === "PATCH")       bodyObj = { id: recId, fields: { ...rec.fields, ...(opts?.body ? JSON.parse(opts.body).fields : {}) } };
    else if (method === "DELETE") bodyObj = { id: recId, deleted: true };
    else                          bodyObj = rec;
  } else if (method === "POST") {
    bodyObj = { id: "recNEW", fields: opts?.body ? JSON.parse(opts.body).fields : {} };
  } else {
    bodyObj = { records: rows }; // list read (single page, no offset)
  }
  return { ok: true, status: 200, text: async () => JSON.stringify(bodyObj) };
};

// 3) Import the real handler (dynamic import = after env is set).
const { handler } = await import("../netlify/functions/airtable.js");
const { signToken, verifyToken, signScope } = await import("../netlify/functions/_auth.js");

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

await test("jobs: exposes Job Year for the sidebar year filter, null when absent", async () => {
  mockTables = { Jobs: [
    { id: "recJ1", fields: { "Job Name": "This Year", "Job Status": "Awarded", "Job Year": 2026 } },
    { id: "recJ2", fields: { "Job Name": "No Year",   "Job Status": "Awarded" } },
  ] };
  const b = json(await GET("jobs"));
  eq(b.jobs.find(j => j.id === "recJ1").year, 2026, "year is a NUMBER, not a string — the filter compares on it");
  // A yearless job must stay reachable under "All Years" rather than vanish.
  eq(b.jobs.find(j => j.id === "recJ2").year, null, "missing Job Year maps to null, not 0 or undefined");
});

// Regression: the Closeout tab's three checkboxes had NEVER rendered checked.
// The "All … Reviewed?" formulas return "✅ Yes" / "⚠️ Pending Review", but the
// coercer only accepted a bare "yes"/"true"/"1", so every gate read false on every
// job — 102 of 112 jobs were materials-reviewed while the app showed none.
await test("jobs: the All-Reviewed gates read the ✅/⚠ formula strings", async () => {
  mockTables = { Jobs: [
    { id: "recJ1", fields: { "Job Name": "Reviewed", "Job Status": "Awarded",
      "All Materials Reviewed?": "✅ Yes", "All Expenses Reviewed?": "✅ Yes",
      "All Labor Reviewed": "✅ Yes", "All Wire Reviewed?": "✅ Yes",
      "All Pipe Reviewed?": "✅ Yes" } },
    { id: "recJ2", fields: { "Job Name": "Pending", "Job Status": "Awarded",
      "All Materials Reviewed?": "⚠️ Pending Review",
      "All Expenses Reviewed?": "⚠️ Pending Review",
      "All Labor Reviewed": "⚠️ Pending Review" } },
  ] };
  const b = json(await GET("jobs"));
  const done = b.jobs.find(j => j.id === "recJ1");
  const open = b.jobs.find(j => j.id === "recJ2");
  eq(done.allMaterialsReviewed, true,  "✅ Yes → true (this was false for the app's whole life)");
  eq(done.allExpensesReviewed,  true,  "expenses gate");
  eq(done.allLaborReviewed,     true,  "labor gate");
  eq(done.allWireReviewed,      true,  "wire gate");
  eq(done.allPipeReviewed,      true,  "pipe gate");
  eq(open.allMaterialsReviewed, false, "⚠️ Pending Review stays false");
  eq(open.allLaborReviewed,     false, "pending labor stays false");
  // A missing field must not read as reviewed — that would show a job as closed
  // out when nobody has checked anything.
  eq(open.allWireReviewed,      false, "absent field is false, never true");
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

// ── bird date (poultry move-in) ──
await test("scheduleEntries: surfaces jobs' Bird Date in birdDates[]", async () => {
  mockTables = {
    "Schedule Entries": [],
    Jobs: [
      { id: "recBird", fields: { "Job Name": "Case Farms 2-Barn", "Contractor Name (Text)": "Case Farms", "Job Status": "Awarded", "Bird Date": "2026-08-15" } },
      { id: "recNoBird", fields: { "Job Name": "Regular Job", "Job Status": "Awarded" } },
    ],
    Employees: [],
  };
  const b = json(await GET("scheduleEntries"));
  ok(b.ok, "ok");
  eq(b.birdDates.length, 1, "only the job with a Bird Date");
  eq(b.birdDates[0].jobId, "recBird", "jobId");
  eq(b.birdDates[0].date, "2026-08-15", "date");
  eq(b.birdDates[0].jobName, "Case Farms 2-Barn", "jobName");
});

await test("scheduleEntries: birdDates respects since/until window", async () => {
  mockTables = {
    "Schedule Entries": [],
    Jobs: [
      { id: "recIn",  fields: { "Job Name": "In Window",  "Job Status": "Awarded", "Bird Date": "2026-08-15" } },
      { id: "recOut", fields: { "Job Name": "Out Window", "Job Status": "Awarded", "Bird Date": "2027-01-01" } },
    ],
    Employees: [],
  };
  const b = json(await GET("scheduleEntries", { since: "2026-08-01", until: "2026-08-31" }));
  eq(b.birdDates.length, 1, "windowed"); eq(b.birdDates[0].jobId, "recIn", "in-window job");
});

await test("updateJobInfo: writes Bird Date field id; clears with null", async () => {
  mockTables = {};
  await POST("updateJobInfo", { jobId: "recJ1", birdDate: "2026-08-15" });
  let fields = JSON.parse(lastFetch.opts.body).fields;
  eq(fields["fldyKjtcqganpbhNc"], "2026-08-15", "sets the date on the Bird Date field id");
  await POST("updateJobInfo", { jobId: "recJ1", birdDate: "" });
  fields = JSON.parse(lastFetch.opts.body).fields;
  eq(fields["fldyKjtcqganpbhNc"], null, "empty clears to null (not empty string)");
});

// ── hours by job (first Neon-slice read pattern) ──
await test("hoursByJob: groups by static Job Name (Text), sums hours, flags historical", async () => {
  mockTables = { "Time Entries": [
    // Live job "Alpha": two entries, one still linked to a project.
    { id: "recT1", fields: { "Job Name (Text)": "Alpha", "Hours": 8,   "Work Date": "2026-01-10", "Job": ["recJob1"] } },
    { id: "recT2", fields: { "Job Name (Text)": "Alpha", "Hours": 2.5, "Work Date": "2026-01-05" } },
    // Historical job "Bravo": no live Job link on any entry.
    { id: "recT3", fields: { "Job Name (Text)": "Bravo", "Hours": 4,   "Work Date": "2021-06-01" } },
    // Nameless entry — excluded from buckets, still counted in totals.
    { id: "recT4", fields: { "Hours": 1, "Work Date": "2022-03-03" } },
  ] };
  const b = json(await GET("hoursByJob"));
  ok(b.ok, "ok");
  eq(b.jobs.length, 2, "two named buckets");
  eq(b.jobs[0].jobName, "Alpha", "sorted by hours desc → Alpha first");
  eq(b.jobs[0].hours, 10.5, "Alpha hours summed");
  eq(b.jobs[0].entries, 2, "Alpha entry count");
  eq(b.jobs[0].firstDate, "2026-01-05", "Alpha earliest date");
  eq(b.jobs[0].lastDate, "2026-01-10", "Alpha latest date");
  eq(b.jobs[0].historical, false, "Alpha has a live link → not historical");
  const bravo = b.jobs.find(j => j.jobName === "Bravo");
  eq(bravo.historical, true, "Bravo has no live link → historical");
  eq(b.summary.jobCount, 2, "job count");
  eq(b.summary.totalEntries, 4, "total entries incl. nameless");
  eq(b.summary.namelessEntries, 1, "nameless excluded from buckets");
});

await test("hoursByJob: office role → 403 (payroll-eligible-only)", async () => {
  eq((await GET("hoursByJob", {}, OFFICE_TOK)).statusCode, 403, "office blocked");
});

// ── calculateMileage: unresolvable address must not look like an error ──
// Regression for the 400 seen in the browser console on every open of a job
// whose address Google can't geocode (e.g. "8250 Ohio 676"). A per-job data
// quirk must not be indistinguishable from a broken Maps integration.
await test("calculateMileage: resolvable address → 200 with miles, cached to Airtable", async () => {
  mockGoogle = { status: "OK", rows: [{ elements: [{ status: "OK", distance: { value: 48280 } }] }] };
  const r = await POST("calculateMileage", { jobId: "recJob1", address: "123 Main St" });
  eq(r.statusCode, 200, "200");
  const b = JSON.parse(r.body);
  ok(b.ok, "ok true");
  eq(b.miles, 30, "48280 m → 30.0 miles");
});

await test("calculateMileage: unresolvable address → 200 ok:false (no console 400)", async () => {
  mockGoogle = { status: "OK", rows: [{ elements: [{ status: "NOT_FOUND" }] }] };
  const r = await POST("calculateMileage", { jobId: "recJob1", address: "8250 Ohio 676" });
  eq(r.statusCode, 200, "200 so the browser logs no failed request");
  const b = JSON.parse(r.body);
  eq(b.ok, false, "ok:false → client still hides the mileage line");
  eq(b.reason, "address_unresolved", "reason distinguishes it from a broken integration");
});

await test("calculateMileage: Google config/quota failure stays loud → 502 with detail", async () => {
  mockGoogle = { status: "REQUEST_DENIED",
                 error_message: "API keys with referer restrictions cannot be used with this API." };
  const r = await POST("calculateMileage", { jobId: "recJob1", address: "123 Main St" });
  eq(r.statusCode, 502, "502 — a broken key affects every job and must stay visible");
  const b = JSON.parse(r.body);
  eq(b.reason, "upstream_error", "reason marks it as an integration failure");
  ok(/referer restrictions/.test(b.detail), "Google's error_message is passed through, not swallowed");
  mockGoogle = null;
});

// ── Neon shadow read (migration step 4b) ──
// The dual-read must be observability ONLY: Airtable stays authoritative and a
// missing/broken Neon must never alter the response. These lock that contract in.
const SHADOW_ROWS = { "Time Entries": [
  { id: "recS1", fields: { "Job Name (Text)": "Alpha", "Hours": 8, "Work Date": "2026-01-10" } },
] };

// NOTE: as of the step-7 cutover (2026-07-30) Neon is the PRIMARY read for
// hoursByJob and Airtable is the FALLBACK — the inverse of the old shadow contract.
// What must still hold is that an unset or broken Neon returns the correct Airtable
// answer, because Make keeps importing into Airtable in parallel and that copy stays
// complete. `_source` must report which side served the request, so that a silent
// permanent fallback is visible instead of looking like success.
await test("hoursByJob: serves from Airtable when DATABASE_URL is unset", async () => {
  delete process.env.DATABASE_URL;
  mockTables = SHADOW_ROWS;
  const b = json(await GET("hoursByJob"));
  ok(b.ok, "ok");
  eq(b._source, "airtable", "reports which side served it");
  eq(b._shadow, undefined, "shadow block is gone — Neon is primary now, not a shadow");
  eq(b.jobs[0].hours, 8, "Airtable answer correct");
});

await test("hoursByJob: unreachable Neon falls back to Airtable, response intact", async () => {
  // Deliberately malformed so the driver fails fast without any network I/O.
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = SHADOW_ROWS;
  const b = json(await GET("hoursByJob"));
  ok(b.ok, "request still succeeds");
  eq(b._source, "airtable", "fallback is reported, not silent");
  eq(b.jobs[0].jobName, "Alpha", "Airtable answer unchanged");
  eq(b.jobs[0].hours, 8, "Airtable hours unchanged");
  eq(b.summary.jobCount, 1, "summary unchanged");
  delete process.env.DATABASE_URL;
});

// ── Neon-first payroll reads (slice 2, 2026-07-31) ──
// payrollEntries, payrollHoursRollup and myHoursRollup moved to Neon-first with an
// Airtable fallback. These lock the fallback contract: a broken or absent Neon must
// still return the CORRECT Airtable answer, and `_source` must say which side served
// it so a silent permanent fallback can't masquerade as success.
//
// `id` is the thing to watch on payrollEntries — the payroll UI edits and deletes
// through it, and it must remain the AIRTABLE record id on both paths.
const PR_ROWS = { "Time Entries": [
  { id: "recPR1", fields: {
      "Employee": "Jeff Koehn", "Work Date": "2026-07-27", "Duration (Seconds)": 28800,
      "Hours": 8, "City Taxes": "A No Tax", "Class": "Contract",
      "Job Name (Text)": "Bethel School (MIB 433)", "Labor Reviewed": true } },
] };

await test("payrollEntries: serves from Airtable when DATABASE_URL is unset", async () => {
  delete process.env.DATABASE_URL;
  mockTables = PR_ROWS;
  const b = json(await GET("payrollEntries", { startDate: "2026-07-26", endDate: "2026-08-08" }));
  ok(b.ok, "ok");
  eq(b._source, "airtable", "reports which side served it");
  eq(b.entries[0].id, "recPR1", "id is the Airtable record id the UI edits through");
  eq(b.entries[0].hours, 8, "hours correct");
  eq(b.entries[0].reviewed, true, "Labor Reviewed carried through");
});

await test("payrollEntries: unreachable Neon falls back to Airtable, ids intact", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = PR_ROWS;
  const b = json(await GET("payrollEntries", { startDate: "2026-07-26", endDate: "2026-08-08" }));
  ok(b.ok, "request still succeeds");
  eq(b._source, "airtable", "fallback is reported, not silent");
  eq(b.entries.length, 1, "entry still returned");
  eq(b.entries[0].id, "recPR1", "editable id survives the fallback");
  eq(b.entries[0].cityTaxes, "A No Tax", "city tax carried through");
  delete process.env.DATABASE_URL;
});

await test("payrollEntries: still 400s without a date range on either path", async () => {
  delete process.env.DATABASE_URL;
  mockTables = PR_ROWS;
  eq((await GET("payrollEntries", {})).statusCode, 400, "missing dates rejected before any read");
});

// ── time-entry WRITE paths — NEON-FIRST, FAIL-CLOSED (migration Step 2) ──
// ⚠ THE CONTRACT HERE WAS DELIBERATELY INVERTED ON 2026-08-05. The previous version
// of this block asserted "a broken Neon must NEVER fail a write Airtable accepted".
// That was right while Airtable was the source of truth. It is now exactly wrong:
// every payroll read is served from Neon, so a write that lands in Airtable but not
// Neon is INVISIBLE — hours nobody can see, on the screen people are paid from.
//
// What is locked here now:
//   1. a write that cannot reach Neon FAILS (500) rather than half-succeeding
//   2. and leaves NOTHING behind in Airtable — no orphan record to reconcile later
//   3. validation still rejects bad input before either system is touched
//
// ⚠ COVERAGE GAP, KNOWINGLY ACCEPTED: the old tests also asserted the Airtable field
// mapping (duration in SECONDS, the bare ["rec…"] link shape, verbatim QB city-tax
// spellings). Those assertions cannot run offline any more — the Airtable write is
// now downstream of a successful Neon write, so it never executes without a real
// connection. This is the same conclusion the 2026-07-31 mirror bug reached: a test
// of a Neon write path that does not actually connect proves nothing. Restoring that
// coverage needs a live-Neon test against a BRANCH, which is the right follow-up.

await test("createTimeEntry: fails CLOSED when Neon is unreachable, writing nothing", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = {};
  lastFetch = null;
  const res = await POST("createTimeEntry", {
    employee: "Jeff Koehn", workDate: "2026-07-27", duration: 3600,
  });
  eq(res.statusCode, 500, "500 — a write that can't reach the source of truth must fail");
  eq(lastFetch, null, "and NOTHING was written to Airtable — no orphan record");
  delete process.env.DATABASE_URL;
});

await test("createTimeEntry: an unset DATABASE_URL is a failure, not a bypass", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  const res = await POST("createTimeEntry", {
    employee: "Jeff Koehn", employeeId: "recEmp1", workDate: "2026-07-27",
    duration: 28800, class: "Contract", cityTaxes: "A No Tax", jobId: "recJob1",
  });
  // Misconfiguration must not silently fall back to the old Airtable-only path —
  // that would recreate the invisible-hours bug this whole step exists to prevent.
  eq(res.statusCode, 500, "unset DATABASE_URL fails the write");
  eq(lastFetch, null, "no Airtable write on the way past");
});

await test("createTimeEntry: rejects a missing employee or work date before writing", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  eq((await POST("createTimeEntry", { workDate: "2026-07-27" })).statusCode, 400, "no employee");
  eq((await POST("createTimeEntry", { employee: "Jeff Koehn" })).statusCode, 400, "no work date");
  eq(lastFetch, null, "validation runs before either system is touched");
});

await test("updateTimeEntry: Labor Reviewed fails closed rather than half-landing", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = { "Time Entries": [{ id: "recTE1", fields: { "Hours": 8 } }] };
  lastFetch = null;
  const res = await POST("updateTimeEntry", { entryId: "recTE1", reviewed: true });
  eq(res.statusCode, 500, "500 rather than ticking Airtable only");
  eq(lastFetch, null, "Airtable untouched — the flag the puller must never clobber");
  delete process.env.DATABASE_URL;
});

await test("updateTimeEntryPayroll: fails closed, leaving no partial edit", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = { "Time Entries": [{ id: "recTE1", fields: {} }] };
  lastFetch = null;
  const res = await POST("updateTimeEntryPayroll", {
    entryId: "recTE1", duration: 7200, cityTaxes: "Massilon",
  });
  eq(res.statusCode, 500, "500 — payroll edits do not half-apply");
  eq(lastFetch, null, "no Airtable PATCH");
  delete process.env.DATABASE_URL;
});

await test("deleteTimeEntry: fails closed — never deletes from Airtable alone", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = { "Time Entries": [{ id: "recTE1", fields: {} }] };
  lastFetch = null;
  const res = await POST("deleteTimeEntry", { entryId: "recTE1" });
  // Deleting from the mirror while the authoritative row survives is the worst of
  // both: the hours come back on the next reconcile and the deletion looks done.
  eq(res.statusCode, 500, "500 rather than deleting the mirror copy");
  eq(lastFetch, null, "Airtable record still there");
  delete process.env.DATABASE_URL;
});

await test("writes still require a non-viewer role before any of this runs", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  const res = await POST("createTimeEntry",
    { employee: "Jeff Koehn", workDate: "2026-07-27" }, VIEWER_TOK);
  eq(res.statusCode, 403, "viewer blocked at authz, ahead of the write path");
  eq(lastFetch, null, "nothing written");
});

// ── employee self-service expenses ──
const OWNER_TOK = signToken({ id: "recEmpOwner", role: "employee" });
const OTHER_TOK = signToken({ id: "recEmpOther", role: "employee" });
const SUBMITTED_BY = "fldRWV0eIKwBrXwHV"; // Expenses → Submitted By (Employee link)

await test("addGeneralExpense: stamps Submitted By from the token (not client input)", async () => {
  mockTables = {};
  await POST("addGeneralExpense", { jobId: "recJob1", amount: 50, type: "Materials" }, OWNER_TOK);
  const fields = JSON.parse(lastFetch.opts.body).fields;
  eq(JSON.stringify(fields[SUBMITTED_BY]), JSON.stringify(["recEmpOwner"]), "Submitted By = token user id");
});

await test("expenses: employee sees only own; admin/office see all", async () => {
  mockTables = {
    Jobs: [{ id: "recJob1", fields: { "Job Name": "Alpha" } }],
    Expenses: [
      { id: "recX1", fields: { "Job": ["recJob1"], "Total Cost (Actual)": 100, "Submitted By": ["recEmpOwner"], "Submitted By Name": ["Owner Emp"] } },
      { id: "recX2", fields: { "Job": ["recJob1"], "Total Cost (Actual)": 200, "Submitted By": ["recEmpOther"] } },
      { id: "recX3", fields: { "Job": ["recJob1"], "Total Cost (Actual)": 300 } }, // legacy, no submitter
    ],
  };
  const emp = json(await GET("expenses", { jobId: "recJob1" }, OWNER_TOK));
  eq(emp.expenses.length, 1, "employee sees only their own"); eq(emp.expenses[0].id, "recX1", "own row");
  const adm = json(await GET("expenses", { jobId: "recJob1" }, ADMIN_TOK));
  eq(adm.expenses.length, 3, "admin sees all");
  eq(adm.expenses.find(e => e.id === "recX1").submittedBy, "Owner Emp", "admin sees who logged it");
  eq(adm.expenses.find(e => e.id === "recX3").submittedBy, "", "legacy expense has blank submitter");
  eq(json(await GET("expenses", { jobId: "recJob1" }, OFFICE_TOK)).expenses.length, 3, "office sees all");
});

await test("updateExpense: employee edits own unreviewed → 200 + patches amount", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Not Reviewed" } }] };
  const res = await POST("updateExpense", { expenseId: "recX1", amount: 75, type: "Fuel" }, OWNER_TOK);
  eq(res.statusCode, 200, "ok");
  eq(JSON.parse(lastFetch.opts.body).fields["fldwbLPIafVtmaSeb"], 75, "amount patched");
});

await test("updateExpense: employee edits someone else's → 403", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Not Reviewed" } }] };
  eq((await POST("updateExpense", { expenseId: "recX1", amount: 75 }, OTHER_TOK)).statusCode, 403, "not owner");
});

await test("updateExpense: employee edits an approved one → 403 (locked)", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Reviewed", "Reviewed": true } }] };
  eq((await POST("updateExpense", { expenseId: "recX1", amount: 75 }, OWNER_TOK)).statusCode, 403, "locked after approval");
});

await test("updateExpense: admin edits any expense → 200", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOther"], "Expense Status": "Reviewed", "Reviewed": true } }] };
  eq((await POST("updateExpense", { expenseId: "recX1", amount: 75 }, ADMIN_TOK)).statusCode, 200, "admin any");
});

await test("deleteExpense: employee deletes own unreviewed → 200; other's → 403", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Not Reviewed" } }] };
  eq((await POST("deleteExpense", { expenseId: "recX1" }, OWNER_TOK)).statusCode, 200, "own unreviewed");
  eq((await POST("deleteExpense", { expenseId: "recX1" }, OTHER_TOK)).statusCode, 403, "not owner");
});

await test("deleteExpense: viewer still blocked (403)", async () => {
  eq((await POST("deleteExpense", { expenseId: "recX1" }, VIEWER_TOK)).statusCode, 403, "viewer read-only");
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

// ── expense receipts (docs/PLAN-expense-receipts.md, slice 1) ──
const EXPENSE = (submittedBy = 'recEmp', reviewed = false) => ({
  Expenses: [{ id: 'recX1', fields: {
    'Submitted By': [submittedBy],
    Reviewed: reviewed,
    Amount: 42,
  } }],
});

await test('expenseReceipts: employee sees own, not someone else\'s', async () => {
  setR2();
  mockTables = EXPENSE('recEmp');
  ok((await GET('expenseReceipts', { expenseId: 'recX1' }, EMP_TOK)).statusCode !== 403, 'own expense');
  mockTables = EXPENSE('recSomeoneElse');
  eq((await GET('expenseReceipts', { expenseId: 'recX1' }, EMP_TOK)).statusCode, 403, "someone else's");
  // Admin and office are not scoped — same rule handleExpenses already applies.
  ok((await GET('expenseReceipts', { expenseId: 'recX1' }, ADMIN_TOK)).statusCode !== 403, 'admin any');
  ok((await GET('expenseReceipts', { expenseId: 'recX1' }, OFFICE_TOK)).statusCode !== 403, 'office any');
});

await test('receipt delete: manager-only, and the bin escapes the 30-day purge', async () => {
  const r2 = await import('../netlify/functions/_r2.js');
  const live   = 'expenses/recX1/20260803-01-ab.jpg';
  const binned = 'expenses/recX1/_deleted/20260803-01-ab.jpg';

  ok(!r2.isDeletedReceiptKey('recX1', live), 'live not flagged');
  ok(r2.isDeletedReceiptKey('recX1', binned), 'binned detected');
  // The photo lifecycle rule targets the TOP-LEVEL `_deleted/` prefix. Receipts
  // are financial records the owner exempted from auto-purge, so their bin is
  // nested inside the expense where that rule cannot reach it.
  ok(!binned.startsWith('_deleted/'), 'receipt bin is outside the 30-day rule');
  ok(binned.startsWith('expenses/'), 'still under expenses/');

  setR2();
  mockTables = EXPENSE('recEmp');
  const body = { expenseId: 'recX1', keys: [live] };
  // No "reviewed" state to key an employee window off, so deletion is
  // manager-only rather than owner-until-approved.
  eq((await POST('deleteExpenseReceipts', body, EMP_TOK)).statusCode, 403, 'employee blocked');
  eq((await POST('deleteExpenseReceipts', body, VIEWER_TOK)).statusCode, 403, 'viewer blocked');
  ok((await POST('deleteExpenseReceipts', body, OFFICE_TOK)).statusCode !== 403, 'office allowed');
  eq((await POST('deleteExpenseReceipts', { expenseId: 'recX1', keys: [] })).statusCode, 400, 'no keys');
});

await test('expenseReceiptSummary: an employee only sees their own expenses', async () => {
  setR2();
  // Two expenses on the job, submitted by different people. The summary must
  // not become a way for an employee to enumerate the job's expenses.
  mockTables = {
    Jobs: [{ id: 'recJ1', fields: { 'Job Name': 'Bethel' } }],
    Expenses: [
      { id: 'recX1', fields: { Job: ['recJ1'], 'Submitted By': ['recEmp'] } },
      { id: 'recX2', fields: { Job: ['recJ1'], 'Submitted By': ['recOther'] } },
    ],
  };
  const emp = json(await GET('expenseReceiptSummary', { jobId: 'recJ1' }, EMP_TOK));
  const keys = Object.keys(emp.receipts || {});
  ok(!keys.includes('recX2'), `employee must not see another's expense (got ${keys})`);

  const admin = json(await GET('expenseReceiptSummary', { jobId: 'recJ1' }, ADMIN_TOK));
  ok(admin.available !== false || admin.reason, 'admin gets a real answer');

  eq((await GET('expenseReceiptSummary', {})).statusCode, 400, 'missing jobId');
  clearR2();
  const off = json(await GET('expenseReceiptSummary', { jobId: 'recJ1' }));
  eq(off.available, false, 'soft-fails when R2 is off');
});

await test('expenseReceipts: soft-fails when R2 is off, 400 without an id', async () => {
  clearR2();
  mockTables = EXPENSE('recEmp');
  const b = json(await GET('expenseReceipts', { expenseId: 'recX1' }, EMP_TOK));
  eq(b.available, false, 'available'); eq(b.reason, 'not-configured', 'reason');
  eq((await GET('expenseReceipts', {})).statusCode, 400, 'missing expenseId');
});

await test('receipt kinds: a ScanSnap PDF never enters the image path', async () => {
  const { receiptFileKind, expensePrefix, assertKeyInExpense } = await import('../netlify/functions/_r2.js');

  // This is the rule that matters. Compressing a 300dpi scan through a canvas
  // would either fail or silently rasterise it into something worse.
  const pdf = receiptFileKind('application/pdf');
  eq(pdf.contentType, 'application/pdf', 'type preserved');
  eq(pdf.isPdf, true, 'flagged');
  eq(pdf.ext, 'pdf', 'extension');
  eq(pdf.wantsThumb, false, 'PDFs get NO thumbnail - pdf.js is too heavy for a tile');

  const jpg = receiptFileKind('image/jpeg');
  eq(jpg.isPdf, false, 'image not flagged'); eq(jpg.ext, 'jpg', 'extension');
  eq(jpg.wantsThumb, true, 'images get a thumbnail');

  eq(receiptFileKind('image/png').ext, 'png', 'png');
  // Anything unrecognised becomes a JPEG rather than an unknown blob.
  eq(receiptFileKind('application/zip').contentType, 'image/jpeg', 'unknown falls back');
  eq(receiptFileKind(undefined).contentType, 'image/jpeg', 'missing falls back');

  // Keys are scoped to the owning expense, and a foreign one is refused.
  eq(expensePrefix('recX1'), 'expenses/recX1/', 'prefix');
  let code = null;
  try { assertKeyInExpense('recX1', 'expenses/recOTHER/x.jpg'); } catch (e) { code = e.code; }
  eq(code, 'KEY_OUTSIDE_EXPENSE', 'foreign key refused');
});

await test('receipt upload: validation and the reviewed-expense lock', async () => {
  setR2();
  mockTables = EXPENSE('recEmp');
  eq((await POST('expenseReceiptUploadUrls', { expenseId: 'recX1', files: [] })).statusCode, 400, 'no files');
  const many = Array.from({ length: 11 }, () => ({ contentType: 'image/jpeg' }));
  eq((await POST('expenseReceiptUploadUrls', { expenseId: 'recX1', files: many })).statusCode, 400, 'too many');
  // An approved expense is locked for employees — attaching reuses that window.
  mockTables = EXPENSE('recEmp', true);
  eq((await POST('expenseReceiptUploadUrls',
    { expenseId: 'recX1', files: [{ contentType: 'image/jpeg' }] }, EMP_TOK)).statusCode, 403, 'locked once reviewed');
});

// ── cross-job filter safety (docs/TODO.md sweep) ──
// A bare FIND(name, ARRAYJOIN({Job})) is a SUBSTRING test, so a job whose name
// contains another's ("Jenny Ln 1" inside "Jenny Ln 10/11/12") pulls the wrong
// job's records. Two defences: newline-delimit so FIND matches per linked
// element, and verify the linked record id in memory for duplicate names.
const TWO_JOBS = {
  Jobs: [{ id: 'recJ1', fields: { 'Job Name': 'Jenny Ln 1' } }],
};
const linkedTo = (id, extra = {}) => ({ Job: [id], ...extra });

await test('jobInspections: does not leak a longer-named job\'s inspections', async () => {
  mockTables = {
    ...TWO_JOBS,
    'Job Inspections': [
      { id: 'recI1', fields: linkedTo('recJ1',    { 'Inspection Type': 'Rough' }) },
      { id: 'recI2', fields: linkedTo('recJ10',   { 'Inspection Type': 'Final' }) },  // "Jenny Ln 10/11/12"
    ],
  };
  const b = json(await GET('jobInspections', { jobId: 'recJ1' }));
  eq(b.inspections.length, 1, 'only this job');
  eq(b.inspections[0].id, 'recI1', 'the right one');
  // And the filter itself is exact-per-element, not a bare substring test.
  ok(/%0A/.test(lastFetch.url), `filter should newline-delimit, got ${decodeURIComponent(lastFetch.url)}`);
});

await test('jobEstimates: does not leak a longer-named job\'s estimates', async () => {
  mockTables = {
    ...TWO_JOBS,
    'Job Estimates': [
      { id: 'recE1', fields: linkedTo('recJ1') },
      { id: 'recE2', fields: linkedTo('recJ10') },
    ],
    'Sent Estimate PDFs': [],
  };
  const b = json(await GET('jobEstimates', { jobId: 'recJ1' }));
  eq(b.estimates.length, 1, 'only this job');
  eq(b.estimates[0].id, 'recE1', 'the right one');
});

await test('generator: does not resolve to a longer-named job\'s generator', async () => {
  mockTables = {
    ...TWO_JOBS,
    Generators: [
      { id: 'recG2', fields: { Job: ['recJ10'], 'Asset ID': 'GEN-10' } },  // wrong job, listed first
      { id: 'recG1', fields: { Job: ['recJ1'],  'Asset ID': 'GEN-1'  } },
    ],
    'Generator Service': [],
  };
  const b = json(await GET('generator', { jobId: 'recJ1' }));
  // Before the fix this returned recG2 — the first row Airtable handed back.
  ok(b.generator, 'a generator was found');
  eq(b.generator.id, 'recG1', 'the one actually linked to this job');
});

// ── jobsite photos on R2 (docs/PLAN-job-photos.md) ──
// COVERAGE NOTE: the signing path (presigned URLs, ListObjectsV2) needs
// aws4fetch, which is intentionally NOT installed for this suite — the harness
// stays offline and install-free (same reason _neon.js lazy-imports its
// driver). So these cover validation, authorization, and the fail-soft
// contract; the signature itself is proven by the admin r2Status probe against
// the real bucket. Don't add a test here that needs node_modules.
const JOB_ONLY = () => ({ Jobs: [{ id: 'recJ1', fields: { 'Job Name': 'Bethel' } }] });

function clearR2() {
  for (const k of ['R2_ACCOUNT_ID','R2_ACCESS_KEY_ID','R2_SECRET_ACCESS_KEY','R2_BUCKET']) delete process.env[k];
}
function setR2() {
  process.env.R2_ACCOUNT_ID = 'acct123';
  process.env.R2_ACCESS_KEY_ID = 'akid';
  process.env.R2_SECRET_ACCESS_KEY = 'secret';
  process.env.R2_BUCKET = 'nee-job-photos';
}

await test('jobPhotos: R2 unconfigured → soft available:false, never a 500', async () => {
  clearR2();
  mockTables = JOB_ONLY();
  const res = await GET('jobPhotos', { jobId: 'recJ1' });
  eq(res.statusCode, 200, 'status');
  const b = json(res);
  ok(b.ok, 'ok'); eq(b.available, false, 'available'); eq(b.reason, 'not-configured', 'reason');
  eq(b.photos.length, 0, 'photos');
});

await test('jobPhotos: unknown job → 404 (before any storage call)', async () => {
  setR2();
  mockTables = { Jobs: [] };
  eq((await GET('jobPhotos', { jobId: 'recNOPE' })).statusCode, 404, 'status');
});

await test('jobPhotos: missing jobId → 400', async () => {
  eq((await GET('jobPhotos', {})).statusCode, 400, 'status');
});

await test('jobPhotos requires a bearer token', async () => {
  eq((await GET('jobPhotos', { jobId: 'recJ1' }, null)).statusCode, 401, 'status');
});

await test('jobPhotoUploadUrls: validates jobId, file list and batch size', async () => {
  setR2();
  mockTables = JOB_ONLY();
  eq((await POST('jobPhotoUploadUrls', { files: [{}] })).statusCode, 400, 'no jobId');
  eq((await POST('jobPhotoUploadUrls', { jobId: 'recJ1', files: [] })).statusCode, 400, 'no files');
  const many = Array.from({ length: 26 }, () => ({ contentType: 'image/jpeg' }));
  eq((await POST('jobPhotoUploadUrls', { jobId: 'recJ1', files: many })).statusCode, 400, 'too many');
});

await test('jobPhotoUploadUrls: unknown job → 404, never mints a key', async () => {
  setR2();
  mockTables = { Jobs: [] };
  eq((await POST('jobPhotoUploadUrls', { jobId: 'recNOPE', files: [{ contentType: 'image/jpeg' }] })).statusCode, 404, 'status');
});

await test('jobPhotoUploadUrls: R2 unconfigured → 503, not a 500', async () => {
  clearR2();
  mockTables = JOB_ONLY();
  eq((await POST('jobPhotoUploadUrls', { jobId: 'recJ1', files: [{ contentType: 'image/jpeg' }] })).statusCode, 503, 'status');
});

await test('jobPhotoUploadUrls: viewer is read-only → 403', async () => {
  setR2();
  mockTables = JOB_ONLY();
  eq((await POST('jobPhotoUploadUrls', { jobId: 'recJ1', files: [{ contentType: 'image/jpeg' }] }, VIEWER_TOK)).statusCode, 403, 'viewer');
});

await test('job notes: readable by every role, writable only by admin/office', async () => {
  mockTables = { Jobs: [{ id: 'recJ1', fields: { 'Job Name': 'Bethel', Notes: 'Gate code 4417' } }] };
  // Read: notes ride along on the job payload, which every signed-in role gets.
  for (const [label, tok] of [['employee', EMP_TOK], ['viewer', VIEWER_TOK], ['office', OFFICE_TOK]]) {
    const b = json(await GET('jobById', { jobId: 'recJ1' }, tok));
    eq(b.job.notes, 'Gate code 4417', `${label} can read notes`);
  }
  // Write: opening up reading must NOT open up writing. Before this change the
  // action defaulted to _NON_VIEWER, so only the hidden UI stopped an employee.
  const write = { jobId: 'recJ1', notes: 'edited' };
  eq((await POST('updateJobNotes', write, EMP_TOK)).statusCode, 403, 'employee cannot write');
  eq((await POST('updateJobNotes', write, VIEWER_TOK)).statusCode, 403, 'viewer cannot write');
  ok((await POST('updateJobNotes', write, OFFICE_TOK)).statusCode !== 403, 'office can write');
  ok((await POST('updateJobNotes', write, ADMIN_TOK)).statusCode !== 403, 'admin can write');
});

await test('jobDocs: admin/office only — the materials PDF shows costs', async () => {
  setR2();
  mockTables = JOB_ONLY();
  // The materials PDF itemises unit costs and job totals. handleExpenses
  // already scopes employees to their own submissions and hides job totals;
  // this must not become the back door around that.
  eq((await GET('jobDocs', { jobId: 'recJ1' }, EMP_TOK)).statusCode, 403, 'employee blocked');
  eq((await GET('jobDocs', { jobId: 'recJ1' }, VIEWER_TOK)).statusCode, 403, 'viewer blocked');
  ok((await GET('jobDocs', { jobId: 'recJ1' }, OFFICE_TOK)).statusCode !== 403, 'office allowed');
  ok((await GET('jobDocs', { jobId: 'recJ1' }, ADMIN_TOK)).statusCode !== 403, 'admin allowed');

  clearR2();
  const b = json(await GET('jobDocs', { jobId: 'recJ1' }));
  eq(b.available, false, 'soft-fails when R2 is off'); eq(b.reason, 'not-configured', 'reason');
  eq(b.docs.length, 0, 'docs');

  setR2();
  eq((await GET('jobDocs', {})).statusCode, 400, 'missing jobId');
  mockTables = { Jobs: [] };
  eq((await GET('jobDocs', { jobId: 'recNOPE' })).statusCode, 404, 'unknown job');
});

await test('job docs live outside the photo gallery', async () => {
  const { jobDocsPrefix, isDocKey, jobPrefix } = await import('../netlify/functions/_r2.js');
  // listJobPhotos returns every non-thumb object under the job prefix, so a PDF
  // filed among the photos would render as a broken image tile.
  eq(jobDocsPrefix('recJ1'), 'jobs/recJ1/_docs/', 'prefix');
  ok(isDocKey('recJ1', 'jobs/recJ1/_docs/NEE_Materials_2026-08-01-abc.pdf'), 'detects a doc');
  ok(!isDocKey('recJ1', `${jobPrefix('recJ1')}Gym/20260801-01-x.jpg`), 'a photo is not a doc');
});

// ── job prints (docs/PLAN-job-prints.md) ──
await test('prints: EVERY signed-in role can read them — that is the feature', async () => {
  setR2();
  mockTables = JOB_ONLY();
  // The whole point is a crew opening drawings on site without a pCloud login,
  // so an employee reading this must never 403. jobDocs is the deliberate
  // contrast: same storage, same job, admin/office only, because the materials
  // PDF itemises unit costs. If these two ever agree, one of them is wrong.
  for (const tok of [EMP_TOK, VIEWER_TOK, OFFICE_TOK, ADMIN_TOK]) {
    ok((await GET('jobPrints', { jobId: 'recJ1' }, tok)).statusCode !== 403, 'prints readable');
  }
  eq((await GET('jobDocs', { jobId: 'recJ1' }, EMP_TOK)).statusCode, 403, 'employee still blocked from jobDocs');
});

await test('prints: any non-viewer may upload, only admin/office may remove', async () => {
  setR2();
  mockTables = JOB_ONLY();
  const files = { jobId: 'recJ1', files: [{ name: 'E-1.pdf', contentType: 'application/pdf' }] };
  // A crew member photographing a marked-up sheet is a legitimate print.
  ok((await POST('jobPrintUploadUrls', files, EMP_TOK)).statusCode !== 403, 'employee may upload');
  eq((await POST('jobPrintUploadUrls', files, VIEWER_TOK)).statusCode, 403, 'viewer may not');

  // Removing one is manager-only: a crew that arrives to find the drawing gone
  // cannot do the job, and purge is unrecoverable.
  const body = { jobId: 'recJ1', keys: ['jobs/recJ1/_prints/E-1.pdf'] };
  for (const action of ['deleteJobPrints', 'restoreJobPrints', 'purgeJobPrints']) {
    eq((await POST(action, body, EMP_TOK)).statusCode, 403, `${action} employee`);
    eq((await POST(action, body, VIEWER_TOK)).statusCode, 403, `${action} viewer`);
    ok((await POST(action, body, OFFICE_TOK)).statusCode !== 403, `${action} office allowed`);
  }
  eq((await GET('jobPrintsDeleted', { jobId: 'recJ1' }, EMP_TOK)).statusCode, 403, 'employee cannot browse the prints bin');
  ok((await GET('jobPrintsDeleted', { jobId: 'recJ1' }, OFFICE_TOK)).statusCode !== 403, 'office can');
});

await test('prints live outside the photo gallery and outside the photo bin', async () => {
  const r2 = await import('../netlify/functions/_r2.js');
  eq(r2.jobPrintsPrefix('recJ1'), 'jobs/recJ1/_prints/', 'prefix');
  ok(r2.isPrintKey('recJ1', 'jobs/recJ1/_prints/E-1 Rev B.pdf'), 'detects a print');
  ok(!r2.isPrintKey('recJ1', 'jobs/recJ1/Gym/20260801-01-x.jpg'), 'a photo is not a print');
  // listJobPhotos returns every non-thumb object under the job prefix, so a
  // 30 MB PDF among the photos would render as a broken tile AND albumFromKey
  // would invent an album called "_prints".
  eq(r2.albumFromKey('recJ1', 'jobs/recJ1/_prints/E-1.pdf'), '_prints', 'why the exclusion exists');
  // The prints bin is NESTED, not the top-level _deleted/ root: listDeletedJobPhotos
  // keeps everything under _deleted/jobs/<id>/, so a print binned there would
  // appear in the PHOTO recycle bin and could be restored into an album.
  const binned = 'jobs/recJ1/_prints/_deleted/E-1.pdf';
  ok(r2.isPrintDeletedKey('recJ1', binned), 'binned print detected');
  ok(!r2.isDeletedKey('recJ1', binned), 'not in the photo bin');
  ok(!r2.isLegacyDeletedKey('recJ1', binned), 'not in the legacy photo bin');
  ok(r2.isPrintKey('recJ1', binned), 'still inside the prints prefix');
});

await test('prints: the filename is preserved but can never forge a path', async () => {
  const { sanitizePrintName, MAX_PRINT_NAME_LEN } = await import('../netlify/functions/_r2.js');
  // The name is client-supplied AND becomes the object key, so it is the one
  // string in this feature that can do damage.
  eq(sanitizePrintName('E-1 Rev B.pdf'), 'E-1 Rev B.pdf', 'revision survives verbatim');
  eq(sanitizePrintName('Panel (2).pdf'), 'Panel (2).pdf', 'parens survive');
  ok(!sanitizePrintName('../../etc/passwd').includes('/'), 'no slashes');
  ok(!sanitizePrintName('a/../b.pdf').includes('..'), 'no climb');
  eq(sanitizePrintName('..'), null, 'dot-dot rejected outright');
  eq(sanitizePrintName('   '), null, 'whitespace rejected');
  eq(sanitizePrintName('.hidden.pdf'), 'hidden.pdf', 'no leading dot');
  // presign() builds the URL with encodeURI, which leaves these intact — a
  // print named "Panel #3.pdf" would sign one URL and address another object.
  ok(!/[#?&]/.test(sanitizePrintName('Panel #3 ?a&b.pdf')), 'url-breaking chars neutralised');
  // Truncation takes from the FRONT so the extension survives; a name cut from
  // the back would lose ".pdf" and open as a download of unknown type.
  const long = sanitizePrintName('x'.repeat(300) + '.pdf');
  eq(long.length, MAX_PRINT_NAME_LEN, 'length capped');
  ok(long.endsWith('.pdf'), 'extension survives truncation');
});

await test('prints: purge refuses anything still live', async () => {
  const { purgeJobPrint, softDeleteJobPrint } = await import('../netlify/functions/_r2.js');
  let threw = null;
  try { await purgeJobPrint('recJ1', 'jobs/recJ1/_prints/E-1.pdf'); } catch (e) { threw = e; }
  ok(threw && threw.code === 'NOT_DELETED', `purge refuses a live print (got ${threw && threw.code})`);
  // And no print operation may be pointed at a photo — assertKeyInPrints is
  // stricter than the photo guard, which only checks the job prefix.
  threw = null;
  try { await softDeleteJobPrint('recJ1', 'jobs/recJ1/Gym/20260801-01-x.jpg'); } catch (e) { threw = e; }
  ok(threw && threw.code === 'KEY_OUTSIDE_JOB', `a photo key is refused (got ${threw && threw.code})`);
  threw = null;
  try { await softDeleteJobPrint('recJ1', 'jobs/recOTHER/_prints/E-1.pdf'); } catch (e) { threw = e; }
  ok(threw && threw.code === 'KEY_OUTSIDE_JOB', `another job's print is refused (got ${threw && threw.code})`);
});

await test('recycle-bin actions: admin/office only, viewer and employee blocked', async () => {
  setR2();
  mockTables = JOB_ONLY();
  const body = { jobId: 'recJ1', keys: ['jobs/recJ1/a.jpg'] };
  for (const action of ['deleteJobPhotos', 'restoreJobPhotos', 'purgeJobPhotos']) {
    eq((await POST(action, body, VIEWER_TOK)).statusCode, 403, `${action} viewer`);
    eq((await POST(action, body, EMP_TOK)).statusCode, 403, `${action} employee`);
    // admin/office must at least pass the authz gate (storage call fails offline)
    ok((await POST(action, body, OFFICE_TOK)).statusCode !== 403, `${action} office allowed`);
  }
  // Browsing what was deleted must match the tier of the actions available on
  // it — restore and purge are admin/office, so listing is too. Office was
  // previously locked out by a strict-admin read tier.
  eq((await GET('jobPhotosDeleted', { jobId: 'recJ1' }, EMP_TOK)).statusCode, 403, 'employee cannot list the bin');
  ok((await GET('jobPhotosDeleted', { jobId: 'recJ1' }, ADMIN_TOK)).statusCode !== 403, 'admin can list the bin');
  ok((await GET('jobPhotosDeleted', { jobId: 'recJ1' }, OFFICE_TOK)).statusCode !== 403, 'office can list the bin');
});

await test('moveJobPhotos: any non-viewer may re-file (reversible)', async () => {
  setR2();
  mockTables = JOB_ONLY();
  const body = { jobId: 'recJ1', keys: ['jobs/recJ1/a.jpg'], album: 'Gym' };
  eq((await POST('moveJobPhotos', body, VIEWER_TOK)).statusCode, 403, 'viewer blocked');
  ok((await POST('moveJobPhotos', body, EMP_TOK)).statusCode !== 403, 'employee allowed');
});

await test('bulk photo ops: validate job and selection before touching storage', async () => {
  setR2();
  mockTables = JOB_ONLY();
  eq((await POST('deleteJobPhotos', { keys: ['x'] })).statusCode, 400, 'no jobId');
  eq((await POST('deleteJobPhotos', { jobId: 'recJ1', keys: [] })).statusCode, 400, 'no keys');
  const many = Array.from({ length: 13 }, (_, i) => `jobs/recJ1/${i}.jpg`);
  eq((await POST('deleteJobPhotos', { jobId: 'recJ1', keys: many })).statusCode, 400, 'too many');
  mockTables = { Jobs: [] };
  eq((await POST('deleteJobPhotos', { jobId: 'recNOPE', keys: ['jobs/recNOPE/a.jpg'] })).statusCode, 404, 'unknown job');
});

await test('r2 mutation guard: a key from another job is refused', async () => {
  const r2 = await import('../netlify/functions/_r2.js');
  // The client sends keys back to us, so this is the check that stops a
  // signed-in user reaching another job's photos by editing one string.
  const foreign = 'jobs/recOTHER/a.jpg';
  const cases = [
    ['move',    () => r2.moveJobPhoto('recJ1', foreign, 'Gym')],
    ['delete',  () => r2.softDeleteJobPhoto('recJ1', foreign)],
    ['restore', () => r2.restoreJobPhoto('recJ1', foreign)],
    ['purge',   () => r2.purgeJobPhoto('recJ1', 'jobs/recOTHER/_deleted/_none/a.jpg')],
  ];
  for (const [label, fn] of cases) {
    let threw = null;
    try { await fn(); } catch (e) { threw = e; }
    ok(threw && threw.code === 'KEY_OUTSIDE_JOB', `${label} rejects a foreign key (got ${threw && threw.code})`);
  }
});

await test('recycle bin: top-level prefix so ONE lifecycle rule can expire it', async () => {
  const r2 = await import('../netlify/functions/_r2.js');
  const live   = 'jobs/recJ1/Gym/20260731-01-a.jpg';
  const binned = r2.deletedKeyFor(live);

  // R2 lifecycle rules match a literal prefix with no wildcards. The bin MUST
  // sit at the top level or no single rule can cover every job's bin.
  eq(binned, '_deleted/jobs/recJ1/Gym/20260731-01-a.jpg', 'binned key');
  ok(binned.startsWith(r2.DELETED_ROOT), 'one rule on _deleted/ catches it');
  ok(!'expenses/recE1/receipt.jpg'.startsWith(r2.DELETED_ROOT), 'receipts excluded by construction');

  // Keeping the original key verbatim makes restore a prefix strip and carries
  // the album with it — no marker segment to invent.
  eq(r2.restoredKeyFor(binned), live, 'restore round-trips');
  eq(r2.deletedFromAlbum('recJ1', binned), 'Gym', 'album survives');
  ok(r2.isDeletedKey('recJ1', binned), 'binned detected');
  ok(!r2.isDeletedKey('recJ1', live), 'live is not binned');
});

await test('recycle bin: photos binned under the OLD layout stay recoverable', async () => {
  const r2 = await import('../netlify/functions/_r2.js');
  // Anything deleted before 2026-08-03 sits at jobs/<id>/_deleted/<album>/.
  // It must still be listed, restorable and purgeable, and must NOT leak back
  // into the gallery now that the exclusion rule changed.
  const legacy = 'jobs/recJ1/_deleted/Gym/20260731-01-a.jpg';
  ok(r2.isLegacyDeletedKey('recJ1', legacy), 'legacy detected');
  eq(r2.deletedFromAlbum('recJ1', legacy), 'Gym', 'legacy album remembered');
  eq(r2.deletedFromAlbum('recJ1', 'jobs/recJ1/_deleted/_none/x.jpg'), '', 'legacy loose photo');
  eq(r2.deletedFromAlbum('recJ1', 'jobs/recJ1/_deleted/Panel%20Room/x.jpg'), 'Panel Room', 'decodes spaces');
});

await test('recycle bin: purge refuses live photos in either layout', async () => {
  const { purgeJobPhoto } = await import('../netlify/functions/_r2.js');
  // Permanent delete must never be reachable for a photo still in the gallery,
  // even if the client asks for it directly.
  let threw = null;
  try { await purgeJobPhoto('recJ1', 'jobs/recJ1/Gym/20260731-01-a.jpg'); } catch (e) { threw = e; }
  ok(threw && threw.code === 'NOT_DELETED', `purge refuses a live photo (got ${threw && threw.code})`);
});

await test('r2 albums: one safe path segment, names survive a round trip', async () => {
  const { albumSegment, albumFromKey, sanitizeAlbum, jobPrefix } = await import('../netlify/functions/_r2.js');
  // The album name is the ONLY client-supplied part of an object key, so it
  // must not be able to forge extra segments or climb out of the job prefix.
  eq(albumSegment('Gym'), 'Gym/', 'simple');
  eq(albumSegment('a/../../b'), 'a%20..%20..%20b/', 'slashes neutralised');
  eq(albumSegment('..'), '', 'dot-dot rejected');
  eq(albumSegment(''), '', 'empty = no album');
  eq(albumSegment('   '), '', 'whitespace = no album');
  ok(!albumSegment('x/y').includes('/', 0) || albumSegment('x/y').split('/').length === 2, 'never more than one segment');
  // Display names with spaces and punctuation must come back exactly.
  const key = `${jobPrefix('recJ1')}${albumSegment('Panel Room #2')}20260731-01-a.jpg`;
  eq(albumFromKey('recJ1', key), 'Panel Room #2', 'round trip');
  eq(albumFromKey('recJ1', `${jobPrefix('recJ1')}loose.jpg`), null, 'no album for a loose photo');
  eq(sanitizeAlbum('x'.repeat(200)).length, 60, 'length capped');
});

await test('r2 keys: scoped per job, thumbs pair with their original', async () => {
  const { jobPrefix, thumbKeyFor, isThumbKey } = await import('../netlify/functions/_r2.js');
  // Scoping is by Airtable record id, so two jobs named the same never mix —
  // the substring trap that bites the FIND-by-name filters elsewhere.
  eq(jobPrefix('recJ1'), 'jobs/recJ1/', 'prefix');
  ok(!jobPrefix('recJ1').startsWith(jobPrefix('recJ')), 'one job folder is never inside another');
  eq(thumbKeyFor('jobs/recJ1/20260731-01-abc.jpg'), 'jobs/recJ1/20260731-01-abc_thumb.jpg', 'thumb key');
  ok(isThumbKey('a_thumb.jpg'), 'detects thumb');
  ok(!isThumbKey('a.jpg'), 'detects original');
});

await test("r2Status: reports exactly which env vars are missing", async () => {
  for (const k of ["R2_ACCOUNT_ID","R2_ACCESS_KEY_ID","R2_SECRET_ACCESS_KEY","R2_BUCKET"]) delete process.env[k];
  const b = json(await GET("r2Status"));
  eq(b.r2.ok, false, "ok"); eq(b.r2.reason, "not-configured", "reason");
  eq(b.r2.missing.length, 4, "names all four");
});

await test("r2Status: admin only — employee gets 403", async () => {
  eq((await GET("r2Status", {}, EMP_TOK)).statusCode, 403, "employee");
  eq((await GET("r2Status", {}, VIEWER_TOK)).statusCode, 403, "viewer");
});

await test("fleet: employees get full parity with admin; viewer stays read-only", async () => {
  // The 🚗 Fleet button was admin-only in the UI even though every fleet action
  // already sat at the permissive tier. The button is now open to all roles, so
  // pin the backend tiers here — moving any of these into _ADMIN_POSTS /
  // _ADMIN_READS would break the crews without touching a line of index.html.
  mockTables = { "Fleet Vehicles": [{ id: "recV1", fields: { Name: "Truck 1" } }] };
  for (const tok of [EMP_TOK, VIEWER_TOK]) {
    ok((await GET("fleetVehicles", {}, tok)).statusCode !== 403, "reads vehicles");
    ok((await GET("fleetServiceHistory", { vehicleId: "recV1" }, tok)).statusCode !== 403, "reads history");
  }
  const writes = [
    ["updateFleetVehicle", { vehicleId: "recV1", currentMileage: 1000 }],
    ["logMileage",         { vehicleId: "recV1", newMileage: 1000, date: "2026-08-03" }],
    ["addFleetService",    { vehicleId: "recV1", date: "2026-08-03", serviceTypes: ["Oil Change"] }],
    ["updateFleetService", { serviceRecordId: "recS1", date: "2026-08-03" }],
    ["deleteFleetService", { serviceRecordId: "recS1" }],
  ];
  for (const [action, body] of writes) {
    ok((await POST(action, body, EMP_TOK)).statusCode !== 403, `${action} employee allowed`);
    eq((await POST(action, body, VIEWER_TOK)).statusCode, 403, `${action} viewer blocked`);
  }
});

// ── report ──
console.log("\nTier-1 backend handler tests (airtable.js)\n");
for (const [s, n] of log) console.log(`  ${s} ${n}`);
console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

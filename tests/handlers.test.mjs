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

// A Neon uuid, for the handles that are no longer rec-shaped (cutover slice 3).
const NEON_INVOICE_ID = "1f0c9d84-6b1e-4f6a-9c3a-2b7f5e0d4a11";

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
await test("calculateMileage: resolvable address → 200 with miles, cached to BOTH stores", async () => {
  // ⚠ CONTRACT CHANGED 2026-08-12, deliberately. This used to pass with Neon
  // unreachable, because the handler only wrote Airtable — which is exactly the
  // bug the sweep found: `miles_from_shop` is in JOB_SELECT, so the value
  // looked right when you pressed the button and was gone by the next visit.
  //
  // The Neon write fails CLOSED, matching the other five job writers. With
  // Neon down the caller now gets an error and can retry, instead of a mileage
  // that displays, persists to Airtable, disappears for an hour, then returns.
  // A number that comes back from the dead is worse than one that refused.
  //
  // So the test must supply a working Neon. It cannot here — the offline suite
  // has no database — hence the explicit DATABASE_URL guard: this asserts the
  // happy path only when a real connection is available, and asserts the
  // fail-closed contract otherwise. Both are real behaviour; neither is skipped.
  mockGoogle = { status: "OK", rows: [{ elements: [{ status: "OK", distance: { value: 48280 } }] }] };
  const r = await POST("calculateMileage", { jobId: "recJob1", address: "123 Main St" });
  if (process.env.DATABASE_URL) {
    eq(r.statusCode, 200, "200");
    const b = JSON.parse(r.body);
    ok(b.ok, "ok true");
    eq(b.miles, 30, "48280 m → 30.0 miles");
  } else {
    ok(r.statusCode >= 500, `no Neon → fails closed rather than caching to Airtable alone (got ${r.statusCode})`);
  }
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

// ── Inspections (migration Step 4c) ───────────────────────────────────────
await test("createInspection: fails CLOSED when Neon is unreachable", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = {};
  lastFetch = null;
  const res = await POST("createInspection",
    { jobId: "recJ1", inspectionType: "Rough", date: "2026-08-07", status: "Scheduled" });
  eq(res.statusCode, 500, "500 rather than writing only the mirror");
  // handleJobInspections reads Neon and only falls through on ZERO rows, so an
  // Airtable-only inspection is invisible on any job that already has one.
  eq(lastFetch, null, "and nothing written to Airtable");
  delete process.env.DATABASE_URL;
});

await test("createInspection: a stray single-select value never reaches Airtable", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = {};
  lastFetch = null;
  // Airtable's typecast:true would CREATE "Roof Inspection" as a new option, and
  // Postgres has no guard at all. The whitelist has to reject it in code.
  // The write fails closed here, so what this locks is that a bad value cannot
  // sneak through as a side effect of the failure path either.
  const res = await POST("createInspection",
    { jobId: "recJ1", inspectionType: "Roof Inspection", status: "Maybe Passed" });
  eq(res.statusCode, 500, "still fails closed");
  eq(lastFetch, null, "nothing written anywhere");
  delete process.env.DATABASE_URL;
});

// ── Generators (migration Step 4c) ────────────────────────────────────────
// Same offline limits as the schedule block below: no DATABASE_URL means the SQL
// never runs, so what is locked here is the contract at the boundary. The SQL
// itself is proven against the live branch.
await test("addGeneratorService: fails CLOSED when Neon is unreachable", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = {};
  lastFetch = null;
  const res = await POST("addGeneratorService",
    { generatorId: "recG1", serviceDate: "2026-08-06", serviceType: "Annual Service" });
  eq(res.statusCode, 500, "500 rather than logging service only into the mirror");
  // A service record that exists ONLY in Airtable is invisible to handleGenerator's
  // primary path — worse than no record, because the tech believes it was logged.
  eq(lastFetch, null, "and nothing written to Airtable");
  delete process.env.DATABASE_URL;
});

await test("addGeneratorService: rejects an id that is neither a rec id nor a uuid", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  // Guards the fleet bug in the other direction: the check must accept BOTH forms
  // (handleGenerator returns a uuid on the Neon path, a rec id on the fallback),
  // so it can only reject values that are neither.
  const res = await POST("addGeneratorService",
    { generatorId: "Betty Huber", serviceDate: "2026-08-06" });
  eq(res.statusCode, 400, "garbage id refused before any write");
  eq(lastFetch, null, "nothing written");

  lastFetch = null;
  eq((await POST("addGeneratorService", { generatorId: "recG1" })).statusCode, 400, "missing serviceDate");
  eq(lastFetch, null, "still nothing written");
});

// ── Schedule (migration Step 4a) — same fail-closed contract as time entries ──
// The Neon path cannot be exercised offline (no DATABASE_URL means the SQL never
// runs), so what is locked here is the contract at the boundary: a write that
// cannot reach the source of truth fails, and leaves nothing behind in Airtable.
// The SQL itself is proven by the live branch test, per the 2026-07-31 lesson.
await test("schedule writes fail CLOSED when Neon is unreachable", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = {};
  lastFetch = null;
  const add = await POST("addScheduleEntry",
    { jobId: "recJ1", startDate: "2026-09-01", endDate: "2026-09-02" });
  eq(add.statusCode, 500, "add fails rather than half-landing");
  eq(lastFetch, null, "and writes nothing to Airtable");

  lastFetch = null;
  eq((await POST("updateScheduleEntry", { entryId: "recS1", title: "x" })).statusCode, 500, "update fails");
  eq(lastFetch, null, "no Airtable PATCH");

  lastFetch = null;
  eq((await POST("deleteScheduleEntry", { entryId: "recS1" })).statusCode, 500, "delete fails");
  eq(lastFetch, null, "Airtable record still there");
  delete process.env.DATABASE_URL;
});

await test("schedule: validation runs before either system is touched", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  eq((await POST("addScheduleEntry", { startDate: "2026-09-01", endDate: "2026-09-02" })).statusCode,
     400, "a Job entry without a jobId is rejected");
  eq((await POST("addScheduleEntry", { jobId: "recJ1", endDate: "2026-09-02" })).statusCode,
     400, "missing startDate rejected");
  eq((await POST("updateScheduleEntry", {})).statusCode, 400, "update needs an entryId");
  eq((await POST("deleteScheduleEntry", {})).statusCode, 400, "delete needs an entryId");
  eq(lastFetch, null, "nothing written on any of them");
});

await test("schedule: viewers cannot write", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  eq((await POST("addScheduleEntry",
    { jobId: "recJ1", startDate: "2026-09-01", endDate: "2026-09-02" }, VIEWER_TOK)).statusCode,
    403, "blocked at authz, ahead of the write path");
  eq(lastFetch, null, "nothing written");
});

// ── employee self-service expenses ──
const OWNER_TOK = signToken({ id: "recEmpOwner", role: "employee" });
const OTHER_TOK = signToken({ id: "recEmpOther", role: "employee" });
const SUBMITTED_BY = "fldRWV0eIKwBrXwHV"; // Expenses → Submitted By (Employee link)

// ⚠ INVERTED 2026-08-24 by cutover slice 4. Expenses are born in Neon now, so
// the create FAILS CLOSED without a database instead of writing Airtable-only —
// same contract as the slice-1 reference creates. An Airtable-only expense
// would be invisible forever: every read is Neon-first and nothing back-fills
// it, since no ETL re-reads Airtable Expenses.
await test("addGeneralExpense: fails CLOSED without a database, and writes nothing", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  const res = await POST("addGeneralExpense", { jobId: "recJob1", amount: 50, type: "Materials" }, OWNER_TOK);
  ok(res.statusCode >= 500, `refused, got ${res.statusCode}`);
  eq(lastFetch, null, "and never created the Airtable row it could no longer track");
});

await test("addGeneralExpense: still stamps Submitted By from the token, never client input", async () => {
  // The Airtable mirror payload is now built behind a Neon write that this
  // offline suite cannot run, so the guarantee is source-pinned. What it
  // protects is that ownership comes from the verified token — it is what
  // scopes an employee to their own expenses in every read and guard.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const fn = src.slice(src.indexOf("async function handleAddGeneralExpense"),
                       src.indexOf("async function handleUpdateExpense"));
  ok(new RegExp(`fields\\["${SUBMITTED_BY}"\\] = \\[authUser\\.id\\]`).test(fn),
     "Submitted By is stamped from authUser.id");
  ok(!/body\.submittedBy|body\.employeeId/.test(fn), "and never from the request body");
  ok(/createExpenseNative\(/.test(fn), "the row is created in Neon first");
  ok(/authUser\?\.id \? String\(authUser\.id\) : null/.test(src.slice(src.indexOf("async function createExpenseNative"))),
     "the native insert takes its submitter from the token too");
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

// ⚠ INVERTED with the create, and for the same reason: the edit form is how a
// mis-typed cost gets corrected, so an update that did not reach the
// authoritative store must not report success.
await test("updateExpense: employee passes the guard, then fails CLOSED without a database", async () => {
  delete process.env.DATABASE_URL;
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Not Reviewed" } }] };
  const res = await POST("updateExpense", { expenseId: "recX1", amount: 75, type: "Fuel" }, OWNER_TOK);
  ok(res.statusCode >= 500, `not a silent success, got ${res.statusCode}`);

  // The amount still maps to Manual Material Cost, NOT Total Cost (Actual).
  // The comment on this field said the latter for months; they are different
  // columns and the second is derived (cost minus credit), so writing the
  // amount there would double-count every credit in GP.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const fn = src.slice(src.indexOf("async function handleUpdateExpense"),
                       src.indexOf("async function handleUpdateInspection"));
  ok(/"fldwbLPIafVtmaSeb": hasAmount \? Number\(amount\) : null,\s*\/\/ Manual Material Cost/.test(fn),
     "fldwbLPIafVtmaSeb is Manual Material Cost");
  ok(/manual_material_cost = \$4::numeric/.test(fn), "and the Neon write agrees");
  ok(/WHERE airtable_id = \$1 OR id::text = \$1/.test(fn), "resolved by either handle");
});

await test("updateExpense: employee edits someone else's → 403", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Not Reviewed" } }] };
  eq((await POST("updateExpense", { expenseId: "recX1", amount: 75 }, OTHER_TOK)).statusCode, 403, "not owner");
});

await test("updateExpense: employee edits an approved one → 403 (locked)", async () => {
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Reviewed", "Reviewed": true } }] };
  eq((await POST("updateExpense", { expenseId: "recX1", amount: 75 }, OWNER_TOK)).statusCode, 403, "locked after approval");
});

await test("updateExpense: admin still clears the guard on a reviewed expense", async () => {
  // The 403s below are the half worth pinning offline — they are decided in the
  // guard, before any store is written. An admin gets PAST the guard (so not
  // 403) and then meets the same fail-closed write as everyone else.
  delete process.env.DATABASE_URL;
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOther"], "Expense Status": "Reviewed", "Reviewed": true } }] };
  const res = await POST("updateExpense", { expenseId: "recX1", amount: 75 }, ADMIN_TOK);
  ok(res.statusCode !== 403, "admin is not blocked by ownership or the approval lock");
  ok(res.statusCode >= 500, `and then fails closed like everyone else, got ${res.statusCode}`);
});

await test("deleteExpense: ownership is enforced, and the delete fails CLOSED", async () => {
  // ⚠ The Neon delete used to be `WHERE airtable_id = $1` with .catch(() => {}),
  // so on a native expense it matched nothing, swallowed the miss, and the row
  // survived in the only store that counts while the caller was told it was
  // deleted. It now takes either handle and throws.
  delete process.env.DATABASE_URL;
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmpOwner"], "Expense Status": "Not Reviewed" } }] };
  eq((await POST("deleteExpense", { expenseId: "recX1" }, OTHER_TOK)).statusCode, 403, "not owner → still 403");
  const own = await POST("deleteExpense", { expenseId: "recX1" }, OWNER_TOK);
  ok(own.statusCode >= 500, `owner passes the guard, then the delete fails closed, got ${own.statusCode}`);

  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const fn = src.slice(src.indexOf("async function handleDeleteExpense"),
                       src.indexOf("async function handleApproveExpense"));
  ok(/DELETE FROM expenses WHERE airtable_id = \$1 OR id::text = \$1/.test(fn), "deletes by either handle");
  ok(!/expense\.delete[\s\S]{0,200}catch\(\(\) => \{\}\)/.test(fn), "and no longer swallows the failure");
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

await test('prints: the download filename is safe in an HTTP header', async () => {
  const { attachmentDisposition } = await import('../netlify/functions/_r2.js');
  // Downloading rather than previewing is the ONLY handoff to a native PDF app
  // — <a download> is ignored cross-origin, so the instruction has to ride in
  // the signed URL as response-content-disposition.
  //
  // Only the escaping is asserted here: presign() needs the aws4fetch signer,
  // which is lazy-imported and deliberately absent from this offline suite.
  // That the param sits INSIDE the signature (append it afterwards and R2 says
  // 403) is covered by the browser smoke test, not here.
  eq(attachmentDisposition('E-1 Rev B.pdf'), 'attachment; filename="E-1 Rev B.pdf"', 'name preserved');
  // A quote would truncate the filename mid-header; a newline would be header
  // injection. sanitizePrintName blocks both upstream — this is the backstop.
  const nasty = attachmentDisposition('a"b\nc.pdf');
  ok(!/["\n]/.test(nasty.slice('attachment; filename="'.length, -1)), 'quote and newline neutralised');
  eq(attachmentDisposition(''), 'attachment; filename="download"', 'empty name still valid');
  ok(attachmentDisposition('x'.repeat(300)).length < 160, 'length capped');
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

// ── panel schedules (docs/PLAN-panel-schedules.md) ──
// DATABASE_URL is deliberately unset in this harness, so every panel handler
// short-circuits to 503 before touching Neon. That is exactly what makes these
// authz assertions meaningful offline: 403 is decided by authzFor BEFORE the
// handler runs, so "not 403" proves the role tier without needing a database.
await test('panels: every signed-in role can READ a panel schedule', async () => {
  mockTables = JOB_ONLY();
  // The electrician standing at the panel is the person who needs to know what
  // circuit 23 feeds. Same contract as prints, and the same trap: if this ever
  // starts matching jobDocs (admin/office), someone has confused the two.
  for (const tok of [EMP_TOK, VIEWER_TOK, OFFICE_TOK, ADMIN_TOK]) {
    ok((await GET('panelSchedules', { jobId: 'recJ1' }, tok)).statusCode !== 403, 'list readable');
    ok((await GET('panelSchedule', { panelId: 'p1' }, tok)).statusCode !== 403, 'one panel readable');
  }
});

await test('panels: the field fills them in, only managers delete them', async () => {
  mockTables = JOB_ONLY();
  const create = { jobId: 'recJ1', name: 'Panel A', voltage: '120/240V 1-Phase', circuits: 42 };
  const save   = { panelId: 'p1', circuits_list: [{ number: 1, description: 'AC' }] };

  // Writing is the whole point — an employee at the panel must not be blocked.
  ok((await POST('createPanelSchedule', create, EMP_TOK)).statusCode !== 403, 'employee creates');
  ok((await POST('savePanelSchedule',  save,   EMP_TOK)).statusCode !== 403, 'employee saves');
  eq((await POST('createPanelSchedule', create, VIEWER_TOK)).statusCode, 403, 'viewer cannot create');
  eq((await POST('savePanelSchedule',  save,   VIEWER_TOK)).statusCode, 403, 'viewer cannot save');

  // Deleting takes every circuit with it and there is no bin.
  eq((await POST('deletePanelSchedule', { panelId: 'p1' }, EMP_TOK)).statusCode, 403, 'employee cannot delete');
  eq((await POST('deletePanelSchedule', { panelId: 'p1' }, VIEWER_TOK)).statusCode, 403, 'viewer cannot delete');
  ok((await POST('deletePanelSchedule', { panelId: 'p1' }, OFFICE_TOK)).statusCode !== 403, 'office can delete');
});

await test('panels: an odd or absurd circuit count is refused, not rounded silently', async () => {
  mockTables = JOB_ONLY();
  const base = { jobId: 'recJ1', name: 'Panel A' };
  // Validation runs BEFORE the database check so these are reachable offline —
  // and so a bad count reports itself instead of hiding behind a 503.
  for (const bad of [41, 0, -2, 86, 'forty-two', null]) {
    eq((await POST('createPanelSchedule', { ...base, circuits: bad })).statusCode, 400, `circuits=${bad} refused`);
  }
  for (const good of [2, 12, 42, 84]) {
    ok((await POST('createPanelSchedule', { ...base, circuits: good })).statusCode !== 400, `circuits=${good} accepted`);
  }
  // A nameless panel is unusable in a list of five panels on one job.
  eq((await POST('createPanelSchedule', { jobId: 'recJ1', name: '  ', circuits: 42 })).statusCode, 400, 'name required');
  eq((await POST('createPanelSchedule', { name: 'Panel A', circuits: 42 })).statusCode, 400, 'jobId required');
});

await test('panels: without DATABASE_URL they fail CLOSED, not soft', async () => {
  mockTables = JOB_ONLY();
  // The opposite of every other Neon path in this file. Reads elsewhere fall
  // back to Airtable and answer correctly but slowly; panel schedules have no
  // Airtable table to fall back TO, so pretending they are empty would tell a
  // crew the panel was never walked. 503 is the honest answer.
  eq((await GET('panelSchedules', { jobId: 'recJ1' })).statusCode, 503, 'list 503s');
  eq((await GET('panelSchedule', { panelId: 'p1' })).statusCode, 503, 'read 503s');
  eq((await POST('savePanelSchedule', { panelId: 'p1' })).statusCode, 503, 'save 503s');
  eq((await POST('deletePanelSchedule', { panelId: 'p1' })).statusCode, 503, 'delete 503s');
});

// ── job checklists (docs/PLAN-job-checklists.md) ──
// Same offline reasoning as the panel tests: DATABASE_URL is unset, so these
// short-circuit to 503 and any "not 403" proves the tier without a database.
await test('checklists: every signed-in role can read the job lists', async () => {
  mockTables = JOB_ONLY();
  // A crew loading the truck at 6am is the whole audience for a supply list.
  for (const tok of [EMP_TOK, VIEWER_TOK, OFFICE_TOK, ADMIN_TOK]) {
    ok((await GET('jobChecklists', { jobId: 'recJ1' }, tok)).statusCode !== 403, 'lists readable');
    ok((await GET('jobChecklist', { listId: 'l1' }, tok)).statusCode !== 403, 'one list readable');
  }
});

await test('checklists: the crew keeps the list, only managers delete the whole thing', async () => {
  mockTables = JOB_ONLY();
  const writes = [
    ['createChecklist',      { jobId: 'recJ1', name: 'Supplies from shop' }],
    ['addChecklistItem',     { listId: 'l1', body: '200ft 2" PVC' }],
    ['setChecklistItemDone', { itemId: 'i1', done: true }],
    ['deleteChecklistItem',  { itemId: 'i1' }],
  ];
  for (const [action, body] of writes) {
    ok((await POST(action, body, EMP_TOK)).statusCode !== 403, `${action} employee allowed`);
    eq((await POST(action, body, VIEWER_TOK)).statusCode, 403, `${action} viewer blocked`);
  }
  // Removing one line you typed wrong is not the same as binning the list.
  eq((await POST('deleteChecklist', { listId: 'l1' }, EMP_TOK)).statusCode, 403, 'employee cannot delete a list');
  ok((await POST('deleteChecklist', { listId: 'l1' }, OFFICE_TOK)).statusCode !== 403, 'office can');
});

await test('checklists: empty names and empty items are refused', async () => {
  mockTables = JOB_ONLY();
  eq((await POST('createChecklist', { jobId: 'recJ1', name: '   ' })).statusCode, 400, 'blank list name');
  eq((await POST('createChecklist', { name: 'Supplies' })).statusCode, 400, 'jobId required');
  eq((await POST('addChecklistItem', { listId: 'l1', body: '  ' })).statusCode, 400, 'blank item');
  eq((await POST('addChecklistItem', { body: 'PVC' })).statusCode, 400, 'listId required');
  eq((await POST('setChecklistItemDone', {})).statusCode, 400, 'itemId required');
});

await test('checklists: reorder validates its id list before it renumbers anything', async () => {
  mockTables = JOB_ONLY();
  const good = '3f2504e0-4f89-11d3-9a0c-0305e82c3301';
  eq((await POST('reorderChecklistItems', { itemIds: [good] })).statusCode, 400, 'listId required');
  eq((await POST('reorderChecklistItems', { listId: 'l1', itemIds: [] })).statusCode, 400, 'empty order refused');
  // A malformed id would abort the whole UPDATE on the uuid cast, so it is
  // caught here and reported instead of surfacing as a 500.
  eq((await POST('reorderChecklistItems', { listId: 'l1', itemIds: [good, 'not-a-uuid'] })).statusCode, 400, 'bad id refused');
  // Reordering is the crew's own housekeeping, so it is not manager-gated.
  ok((await POST('reorderChecklistItems', { listId: 'l1', itemIds: [good] }, EMP_TOK)).statusCode !== 403, 'employee may reorder');
  eq((await POST('reorderChecklistItems', { listId: 'l1', itemIds: [good] }, VIEWER_TOK)).statusCode, 403, 'viewer may not');
});

await test('checklists: without DATABASE_URL they fail CLOSED, not soft', async () => {
  mockTables = JOB_ONLY();
  // There is no Airtable table to fall back to, so answering "no lists" would
  // tell a crew there is nothing to bring. 503 is the honest answer.
  eq((await GET('jobChecklists', { jobId: 'recJ1' })).statusCode, 503, 'list 503s');
  eq((await GET('jobChecklist', { listId: 'l1' })).statusCode, 503, 'read 503s');
  eq((await POST('setChecklistItemDone', { itemId: 'i1', done: true })).statusCode, 503, 'tick 503s');
  eq((await POST('deleteChecklist', { listId: 'l1' })).statusCode, 503, 'delete 503s');
});

// ── session revocation: "deactivate" must mean "logged out" ──
// Slice 1 of docs/PLAN-employee-admin.md. Tokens are stateless with a 30-day
// TTL and verifyToken reads no database, so before this existed, unchecking
// `Active` blocked only a NEW login — the leaver's phone kept working for up
// to a month. These lock the contract in both directions, because BOTH
// directions are dangerous: too loose and a leaver keeps access, too tight and
// a Neon blip logs out every crew member in the field.
const { primeRevocationCache, clearRevocationCache } =
  await import("../netlify/functions/_revocation.js");

await test("revocation: a token issued BEFORE the stamp is rejected", async () => {
  mockTables = { Employees: [] };
  const t0 = Date.now();
  const stale = signToken({ id: "recGone", role: "employee" }, t0 - 60_000);
  primeRevocationCache([["recGone", t0]]);
  const res = await GET("jobs", {}, stale);
  eq(res.statusCode, 401, "revoked session is refused");
  ok(/turned off/i.test(json(res).error), "and says why, so the client can bounce to login");
  clearRevocationCache();
});

await test("revocation: a token issued AFTER the stamp still works", async () => {
  // The re-hire / undo case. A revocation must not be permanent for the person,
  // only for the sessions that predate it.
  mockTables = { Employees: [] };
  const t0 = Date.now();
  const fresh = signToken({ id: "recGone", role: "admin" }, t0 + 60_000);
  primeRevocationCache([["recGone", t0]]);
  ok((await GET("jobs", {}, fresh)).statusCode !== 401, "newer token survives");
  clearRevocationCache();
});

// ── Cutover slice 5: employees can be Neon-native ──────────────────────────
// A hire made in the app has NO Airtable record, so their handle is a uuid.
// Every id-shaped thing in the employee path has to accept that, and the ones
// that emit an id have to keep emitting the REC id for everyone who has one —
// otherwise a 30-day token in a field phone stops matching its own account.
const NATIVE_EMP = "9f1c2d3e-4a5b-4c6d-8e9f-0a1b2c3d4e5f";

await test("slice 5: a native employee's session revokes like anyone else's", async () => {
  // ⚠⚠ SECURITY, not id tidiness. The revocation map used to key on a bare
  // `airtable_id`, which is NULL for a native hire — so their entry landed under
  // the string "null", their session carried a uuid, the lookup missed and
  // `isSessionRevoked` answered "not revoked". Deactivating that person would
  // not have ended their session at all: full access for the rest of a 30-day
  // token, while the admin watched the toggle flip and the UI say it was done.
  mockTables = { Employees: [] };
  const t0 = Date.now();
  const stale = signToken({ id: NATIVE_EMP, role: "employee" }, t0 - 60_000);
  primeRevocationCache([[NATIVE_EMP, t0]]);
  const res = await GET("jobs", {}, stale);
  eq(res.statusCode, 401, "the native hire's revoked session is refused too");
  clearRevocationCache();
});

await test("slice 5: isEmployeeHandle accepts BOTH forms and nothing else", async () => {
  const { isEmployeeHandle } = await import("../netlify/functions/_employees.js");
  ok(isEmployeeHandle("recAbCdEfGhIjKlMn"), "a rec id, exactly as before");
  ok(isEmployeeHandle(NATIVE_EMP), "and a native uuid");
  ok(isEmployeeHandle(NATIVE_EMP.toUpperCase()), "case-insensitively");
  ok(!isEmployeeHandle(""), "not empty");
  ok(!isEmployeeHandle("nonsense"), "not arbitrary text");
  ok(!isEmployeeHandle(null), "not null");
});

await test("slice 5: the 400 guards no longer reject a native employee's own id", async () => {
  // Fifteen handlers tested `String(employeeId).startsWith("rec")` and 400'd
  // otherwise. A native hire would have been able to log in and then be refused
  // by their own PIN screen, hours, rate history and the People screen — every
  // one a flat "invalid employeeId" with nothing to suggest the id was fine and
  // the guard was stale.
  //
  // These must now fail LATER (at the unreachable Neon write), not at the guard.
  mockTables = { Employees: [] };
  delete process.env.DATABASE_URL;
  for (const [action, body] of [
    ["setEmployeeSalaried", { employeeId: NATIVE_EMP, salaried: true }],
    ["setEmployeeActive",   { employeeId: NATIVE_EMP, active: false }],
    ["updateEmployee",      { employeeId: NATIVE_EMP, name: "New Hire", role: "employee" }],
  ]) {
    const res = await POST(action, body);
    ok(res.statusCode !== 400, `${action} does not 400 on a uuid (got ${res.statusCode})`);
  }
  const rates = await GET("employeeRates", { employeeId: NATIVE_EMP });
  ok(rates.statusCode !== 400, `employeeRates does not 400 on a uuid (got ${rates.statusCode})`);
});

await test("slice 5: the employee source rules the sweep depends on", async () => {
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const emp = await fs.readFile(new URL("../netlify/functions/_employees.js", import.meta.url), "utf8");
  const rev = await fs.readFile(new URL("../netlify/functions/_revocation.js", import.meta.url), "utf8");

  // The emit side is COALESCE, never a bare id. If anyone "simplifies" this to
  // `id`, every phone in the field is holding a rec id that stops matching —
  // the whole crew is logged out at once.
  ok(/COALESCE\(airtable_id, id::text\) AS handle, name, role/.test(emp),
     "login emits the handle, so a rec id stays a rec id");
  ok(/COALESCE\(airtable_id, id::text\) AS handle, token_valid_from/.test(rev),
     "revocation keys on the handle");

  // updateEmployee must NOT be an upsert on airtable_id. Handed a uuid it would
  // conflict with nothing and INSERT A SECOND employee carrying that uuid as its
  // airtable_id — same name, same PIN, login then ambiguous and BOTH locked out.
  ok(!/neonWrite\("updateEmployee",\s*\n\s*`INSERT INTO employees/.test(src),
     "updateEmployee is a plain UPDATE, not an ON CONFLICT upsert");
  ok(/neonWrite\("updateEmployee",\s*\n\s*`UPDATE employees/.test(src),
     "and it is keyed on the dual handle");

  // The crew picker filtered `airtable_id IS NOT NULL`, which after this slice
  // describes every native hire — they would be silently unschedulable.
  ok(!/WHERE active IS TRUE AND airtable_id IS NOT NULL/.test(src),
     "the crew picker no longer filters native employees out");

  // Airtable linked-record fields written with typecast:true CREATE a record for
  // an unknown value. A uuid there adds a junk person to the Employees table.
  const submittedBy = src.match(/fields\["fldRWV0eIKwBrXwHV"\] = \[authUser\.id\]/g) || [];
  eq(submittedBy.length, 2, "both Submitted By writes still exist");
  ok(!/if \(authUser\?\.id\) fields\["fldRWV0eIKwBrXwHV"\]/.test(src),
     "and neither is written without a rec-id guard");
});

await test("revocation: everyone else is untouched", async () => {
  // The regression that would take the whole app down. Only listed ids revoke.
  mockTables = { Employees: [] };
  primeRevocationCache([["recSomeoneElse", Date.now()]]);
  ok((await GET("jobs", {}, ADMIN_TOK)).statusCode !== 401, "unrelated session unaffected");
  clearRevocationCache();
});

await test("revocation: unreachable Neon FAILS SOFT — requests still serve", async () => {
  // Deliberately the opposite of the write path below. Rejecting everyone when
  // the revocation list can't be read would trade a leaver (who already can't
  // log in) for the entire field crew.
  mockTables = { Employees: [] };
  clearRevocationCache();
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  ok((await GET("jobs", {}, ADMIN_TOK)).statusCode !== 401, "Neon down must not log people out");
  delete process.env.DATABASE_URL;
  clearRevocationCache();
});

await test("setEmployeeActive: admin only, and never yourself", async () => {
  mockTables = { Employees: [] };
  eq((await POST("setEmployeeActive", { employeeId: "recX", active: false }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("setEmployeeActive", { employeeId: "recX", active: false }, OFFICE_TOK)).statusCode, 403, "office refused — access is not a money op");
  eq((await POST("setEmployeeActive", { employeeId: "recX", active: false }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  // recAdmin is the id inside ADMIN_TOK. Locking yourself out bricks the only
  // screen that could undo it.
  const self = await POST("setEmployeeActive", { employeeId: "recAdmin", active: false });
  eq(self.statusCode, 400, "admin cannot deactivate themselves");
});

await test("setEmployeeActive: fails CLOSED when Neon is unreachable", async () => {
  // The read fails soft; this must not. An admin who is told "deactivated"
  // while the leaver's phone still works is the exact failure this feature
  // exists to remove — so a revocation we cannot record must not report success.
  mockTables = { Employees: [{ id: "recGone", fields: { "Employee Name": "Ex Employee", Active: true } }] };
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  const res = await POST("setEmployeeActive", { employeeId: "recGone", active: false });
  ok(res.statusCode >= 500, `expected a server error, got ${res.statusCode}`);
  delete process.env.DATABASE_URL;
});

await test("setEmployeeActive: an employee Neon doesn't have never reports success", async () => {
  // The risk: an UPDATE matching zero rows is a SUCCESSFUL query, so without
  // the explicit RETURNING row-check in the handler this would answer
  // "deactivated" while recording no revocation — the leaver's phone keeps
  // working and the screen says it doesn't. Reachable for real, since anyone
  // hired since the last ETL run is in Airtable but not yet in Neon.
  //
  // ⚠ HONEST SCOPE: offline, this fails at the CONNECTION, so it does not
  // actually exercise the zero-row branch — it proves the endpoint can't
  // report success without a working Neon write, which is the outer guarantee.
  // The row-check itself needs a live-Neon test against a branch, the same gap
  // already noted for the createTimeEntry write path above. Do not read this
  // green tick as proof that mustHaveMatched() fires.
  mockTables = { Employees: [{ id: "recNotInNeon", fields: { "Employee Name": "New Hire", Active: true } }] };
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  const res = await POST("setEmployeeActive", { employeeId: "recNotInNeon", active: false });
  ok(res.statusCode >= 500, `expected a server error, got ${res.statusCode}`);
  delete process.env.DATABASE_URL;
});

// Runs `fn` and returns the body of the first POST made to `table`. Exists
// because `lastFetch` is the LAST request, and handlers increasingly do
// something after their write — createJob re-reads the record for its formula
// fields — so "the last request" and "the write" are no longer the same thing.
async function capturePostTo(table, fn) {
  const real = globalThis.fetch;
  let body = null;
  globalThis.fetch = async (url, opts) => {
    const isPost = (opts?.method || "GET").toUpperCase() === "POST";
    if (isPost && body === null && new RegExp(`/${table}$`).test(String(url).split("?")[0])) {
      body = JSON.parse(opts.body);
    }
    return real(url, opts);
  };
  try { await fn(); } finally { globalThis.fetch = real; }
  ok(body, `no POST to ${table} was made`);
  return body;
}

await test("warmup: unauthenticated, reads nothing, and never errors", async () => {
  // The ONLY other action besides clockWidget that skips the bearer check, so
  // the bar is: it must be impossible to learn anything from it. It takes no
  // parameters, touches no table, and returns ok/ms — there is no data to leak
  // because none is read. If this ever grows a parameter or a table, it stops
  // qualifying and must move behind auth.
  mockTables = {};
  const r = await GET("warmup", {}, null);   // ← no token at all
  eq(r.statusCode, 200, "200 without a token");
  const b = json(r);
  eq(b.ok, true, "ok");
  // Offline there is no DATABASE_URL, so it reports honestly rather than
  // pretending it warmed something.
  eq(b.warmed, false, "no database → warmed:false, not a lie");
  eq(b.reason, "no-database", "and says why");
  // Nothing that could identify a person or a record.
  eq(Object.keys(b).sort().join(","), "ok,reason,warmed", "returns nothing else");
});

await test("pCloud upload: the contractor folder reads the field the jobs API actually returns", async () => {
  // Regression for 2026-08-24. eb38e2e added a fallback that resolved the
  // contractor from state.jobs as `j.contractorName` — but mapJob returns
  // `contractor`. `contractorName` exists on the client only on estimate
  // TEMPLATES, so the fallback was undefined on all three upload call sites
  // (invoice, estimate, generator report), none of which pass it explicitly.
  //
  // It cost nothing for four hours: Make still read the contractor out of
  // Airtable, so the file just filed one folder too high, silently. When the
  // scenario was repointed at this payload the path became
  // `/NEE Jobs/2026//<job PO>/...` and pCloud returned
  // `[2005] Directory does not exist` on every upload.
  //
  // Pinned as a pair, because the bug is the two names DISAGREEING — asserting
  // either one alone would not have caught it.
  const fs = await import("node:fs/promises");
  const [html, src] = await Promise.all([
    fs.readFile(new URL("../index.html", import.meta.url), "utf8"),
    fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8"),
  ]);
  ok(/contractor: s\(r\.contractor_name\)/.test(src),
     "mapJob still returns the contractor as `contractor`");
  const fn = html.slice(html.indexOf("async function uploadPDFToPCloud"),
                        html.indexOf("MAKE_PCLOUD_UPLOAD_WEBHOOK,", html.indexOf("async function uploadPDFToPCloud")));
  ok(/jobRef\?\.contractor\b/.test(fn) && !/\?\.contractorName\b/.test(fn),
     "the upload fallback reads `contractor`, not `contractorName`");
  ok(/contractorName: contractor/.test(fn),
     "and still sends it to Make under the key the scenario maps");
});

await test("createJob: carries the job's markup into Neon, or new jobs bill at cost", async () => {
  // Regression for 2026-08-24, found on the first inventory push to a brand-new
  // job. `Job Markup %` is a plain percent field whose value on create comes
  // from an AIRTABLE FIELD DEFAULT (10%), so this code never sent it — and the
  // column was simply missing from the INSERT, leaving Neon NULL until
  // _jobs-sync.js ran up to an hour later.
  //
  // That hour bills. `unbilled_material_amount_calc` multiplies by
  // COALESCE(j.markup_pct, 0), and createMaterialAllocation SNAPSHOTS that into
  // material_billing_allocations.allocated_amount. An expense approved on a job
  // under an hour old was allocated at COST, and the hourly sync fixes the job
  // afterwards but never recomputes an allocation already written — so the
  // under-billing is permanent for that row. Test 2 (MIT 298): Airtable 83.60,
  // Neon 76.00, a 10% shortfall on everything pushed that hour.
  //
  // Source-pinned: the insert needs a live Neon and this suite is offline.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_jobs.js", import.meta.url), "utf8");
  ok(/Job Markup %/.test(src), "the markup is read back from Airtable");
  const insStart = src.indexOf("INSERT INTO jobs");
  ok(insStart > -1, "the job insert is still there");
  ok(/markup_pct/.test(src.slice(insStart, src.indexOf("ON CONFLICT", insStart))),
     "and markup_pct is in the INSERT column list");
  ok(/markup_pct=COALESCE\(EXCLUDED\.markup_pct, jobs\.markup_pct\)/.test(src),
     "a retry whose re-read failed must not blank a markup the sync already carried");
  // It must come from the RE-READ, not the create response: the create response
  // is built from the fields we sent, and we never send this one.
  const rrStart = src.indexOf("const fresh = await atFetch");
  ok(rrStart > -1, "the PO re-read is still there");
  const reread = src.slice(rrStart, src.indexOf("} catch", rrStart));
  ok(/Job Markup %/.test(reread), "read from the re-read, where the default has landed");
});

await test("createJob: ships INERT — Airtable still assigns the PO", async () => {
  // The automation wfltJAiEaavVLA0wB triggers on "New Lead AND Job PO Number
  // empty". While the switch is off we must NOT send that field, or the
  // automation stands down and NOTHING assigns a PO — a job with no number,
  // which is worse than the status quo.
  mockTables = { Jobs: [], Companies: [] };
  delete process.env.JOB_CREATE_SOURCE;
  // ⚠ Capture the POST specifically, not lastFetch. handleCreateJob RE-READS the
  // record afterwards to pick up the `Job PO` formula, so lastFetch is that GET
  // and has no body. This is the assertion breaking when the handler grew a
  // round trip — which is the test doing its job, not noise.
  const { fields: sent } = await capturePostTo("Jobs", async () => {
    const res = await POST("createJob", { jobName: "Inert Test", contractorId: "recCo1" });
    eq(res.statusCode, 200, "job created");
    eq(json(res).poNumber, undefined, "no PO reported back");
  });
  eq(Object.prototype.hasOwnProperty.call(sent, "Job PO Number"), false,
     "Job PO Number must be ABSENT so Airtable's automation still fires");
});

await test("createJob: the PO field is only ever sent when the switch is on", async () => {
  // Guards the switch itself rather than the allocation, which needs live Neon.
  // Offline the allocation throws at the connection, and the handler is written
  // to fall through to Airtable on any failure — so the job must still be
  // created, and still carry NO number, exactly as when the switch is off.
  mockTables = { Jobs: [], Companies: [] };
  process.env.JOB_CREATE_SOURCE = "neon";
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  const { fields: sent } = await capturePostTo("Jobs", async () => {
    const res = await POST("createJob", { jobName: "Fallback Test", contractorId: "recCo1" });
    eq(res.statusCode, 200, "a failed PO allocation must not block creating the job");
  });
  eq(Object.prototype.hasOwnProperty.call(sent, "Job PO Number"), false,
     "allocation failed, so the field is omitted and Airtable assigns as usual");
  delete process.env.DATABASE_URL;
  delete process.env.JOB_CREATE_SOURCE;
});

await test("job webhooks: ship INERT — JOB_WEBHOOKS unset posts nothing", async () => {
  // The four Airtable automations are still deployed. If both they and this
  // fire, every job reaching Estimating gets TWO sets of pCloud folders and
  // every Awarded job two Trello cards. The switch is what keeps them apart.
  const { jobWebhooksEnabled, fireJobStatusWebhooks, fireServiceCallWebhook } =
    await import("../netlify/functions/_job-webhooks.js");

  delete process.env.JOB_WEBHOOKS;
  eq(jobWebhooksEnabled(), false, "unset → off");

  // fetch is stubbed to EXPLODE. Checking the return value alone would pass
  // even if we had already POSTed to Make.
  const realFetch = globalThis.fetch;
  let posted = 0;
  globalThis.fetch = async () => { posted++; throw new Error("must not reach Make while disabled"); };
  try {
    const rec = { id: "recX", fields: { "Job Status": "Estimating", "Job Name": "Test" } };
    eq(await fireJobStatusWebhooks(rec, async () => {}), null, "status hooks inert");
    eq(await fireServiceCallWebhook(rec), null, "service-call hook inert");
    eq(posted, 0, "Make was not called at all");

    for (const v of ["", "on", "true", "1", "yes", "airtable"]) {
      process.env.JOB_WEBHOOKS = v;
      eq(jobWebhooksEnabled(), false, `"${v}" is not "app" → still off`);
    }
    process.env.JOB_WEBHOOKS = "app";
    eq(jobWebhooksEnabled(), true, `"app" enables it`);
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.JOB_WEBHOOKS;
  }
});

await test("job webhooks: the guard flags use an EN DASH, and a wrong name fires every time", async () => {
  // ⚠ "Automation – pCloud Folders Created" contains U+2013, not a hyphen.
  // Read the name wrong and the lookup is undefined, which is falsy, which
  // means the "already done" guard never holds and every status re-save makes
  // another set of pCloud folders. Asserted against the source so a tidy-up
  // that "fixes" the dash fails here instead of in pCloud.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_job-webhooks.js", import.meta.url), "utf8");
  for (const name of ["Automation – pCloud Folders Created",
                      "Automation – Trello Created",
                      "Automation – TSheets Created",
                      "Automation – Trello Completed"]) {
    ok(src.includes(name), `en-dash field name missing or altered: ${name}`);
  }
  ok(!/Automation - (pCloud|Trello|TSheets)/.test(src), "a hyphen crept into an Automation flag name");
});

await test("job webhooks: fire only on the right status, and respect the done flags", async () => {
  const { fireJobStatusWebhooks } = await import("../netlify/functions/_job-webhooks.js");
  process.env.JOB_WEBHOOKS = "app";
  const realFetch = globalThis.fetch;
  const hits = [];
  globalThis.fetch = async (url, opts) => {
    hits.push(JSON.parse(opts.body).event || "plain");
    return { ok: true };
  };
  const noop = async () => {};
  try {
    // Wrong status → nothing.
    hits.length = 0;
    await fireJobStatusWebhooks({ id: "r1", fields: { "Job Status": "New Lead" } }, noop);
    eq(hits.length, 0, "New Lead fires nothing");

    // Estimating, flag already set → nothing. This is the re-save case that
    // would otherwise duplicate folders in pCloud.
    hits.length = 0;
    await fireJobStatusWebhooks({ id: "r2", fields: {
      "Job Status": "Estimating", "Automation – pCloud Folders Created": true } }, noop);
    eq(hits.length, 0, "pCloud does not re-fire once its flag is set");

    // Estimating, flag clear → exactly one, and it is the pCloud event.
    hits.length = 0;
    await fireJobStatusWebhooks({ id: "r3", fields: { "Job Status": "Estimating" } }, noop);
    eq(hits.length, 1, "one post");
    eq(hits[0], "create_pcloud_folders", "the pCloud payload");
  } finally {
    globalThis.fetch = realFetch;
    delete process.env.JOB_WEBHOOKS;
  }
});

await test("allocations: ship INERT — ALLOCATIONS_WRITE unset writes nothing", async () => {
  // The most important test in this file's billing section. The four Airtable
  // automations are still deployed, and if both they and this code are live the
  // same time entry gets TWO allocations — which v_invoices.invoice_total_calc
  // sums, i.e. the customer is billed twice. The switch is what keeps them from
  // overlapping, so "off means off" has to be provable, not assumed.
  const { allocationsWriteEnabled, createLaborAllocation, createMaterialAllocation,
          attachAllocationsToInvoice } = await import("../netlify/functions/_allocations.js");

  delete process.env.ALLOCATIONS_WRITE;
  eq(allocationsWriteEnabled(), false, "unset → off");

  // A stub that FAILS the test if it is ever called. Asserting "returned
  // skipped" alone would pass even if we had already written to Airtable.
  let touched = 0;
  const boom = async () => { touched++; throw new Error("Airtable must not be touched while disabled"); };

  eq((await createLaborAllocation(boom, "00000000-0000-0000-0000-000000000001")).skipped, "disabled", "labor inert");
  eq((await createMaterialAllocation(boom, "recAnything")).skipped, "disabled", "material inert");
  eq((await attachAllocationsToInvoice(boom, { id: NEON_INVOICE_ID, airtableId: "recInv" }, "recJob")).skipped,
     "disabled", "attach inert");
  eq(touched, 0, "Airtable was not called at all");

  for (const v of ["off", "OFF", "false", "1", "yes", ""]) {
    process.env.ALLOCATIONS_WRITE = v;
    eq(allocationsWriteEnabled(), false, `"${v}" is not "on" → still off`);
  }
  process.env.ALLOCATIONS_WRITE = "on";
  eq(allocationsWriteEnabled(), true, `"on" enables it`);
  process.env.ALLOCATIONS_WRITE = "ON";
  eq(allocationsWriteEnabled(), true, "case-insensitive");
  delete process.env.ALLOCATIONS_WRITE;
});

await test("allocations: an entry with no Neon id is refused before anything is written", async () => {
  // The only remaining hard refusal. A time entry ALWAYS has a Neon uuid — it is
  // the primary key — so reaching this means the caller passed nothing, and
  // guessing would be worse than failing.
  //
  // ⚠ "no Airtable twin" is NOT a refusal any more, and that reversal is the
  // whole point of the 2026-08-11 change. It used to return "no-airtable-twin",
  // which meant no labor logged after Step 3 (2026-08-07) could ever reach an
  // invoice — 100% of the week of 08-10. Those now go Neon-native, and the test
  // for that lives against a live database, because the insert is the behaviour.
  const { createLaborAllocation } = await import("../netlify/functions/_allocations.js");
  process.env.ALLOCATIONS_WRITE = "on";
  let touched = 0;
  const boom = async () => { touched++; throw new Error("must not reach Airtable"); };
  const r = await createLaborAllocation(boom, null);
  eq(r.skipped, "no-entry-id", "refused for the right reason");
  eq(r.created, 0, "nothing created");
  eq(touched, 0, "Airtable not called");
  delete process.env.ALLOCATIONS_WRITE;
});

await test("unlinkedLaborAllocations: every response carries BOTH id forms", async () => {
  // Regression for the six-week silent one: the invoice draft matches these
  // allocations against the job's reviewed time entries, `timeEntries` went
  // Neon-first on 2026-07-31 and started returning uuids, this handler kept
  // returning Airtable rec ids, and the sets stopped intersecting. Labor summed
  // to $0 and the hasPriorInvoices guard hid it on exactly the jobs being
  // re-invoiced. Bethel School's $34,937.50 was typed in by hand.
  //
  // The fix is that BOTH keys always ship, so it cannot matter which store
  // either read came from. Here that is exercised on the Airtable fallback (this
  // suite is offline); the Neon branch is source-pinned below.
  mockTables = {
    ...TWO_JOBS,
    tblHyJWVAcBczn3hn: [
      { id: "recA1", fields: { Job: ["Jenny Ln 1"], "Time Entry": ["recTE1"],
                               "Allocated Hours": 8, "Allocated Revenue $": 520 } },
    ],
  };
  const b = json(await GET("unlinkedLaborAllocations", { jobId: "recJ1" }));
  eq(b.allocations.length, 1, "the job's unlinked allocation");
  const a = b.allocations[0];
  eq(a.timeEntryId, "recTE1", "time entry id present");
  eq(a.timeEntryAirtableId, "recTE1", "…and the Airtable key alongside it");
  eq(a.allocatedRevenue, 520, "revenue carried through");

  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const fn = src.slice(src.indexOf("async function handleUnlinkedLaborAllocations"),
                       src.indexOf("async function handleExpenses"));
  ok(/t\.id::text\s+AS time_entry_id/.test(fn), "Neon branch returns the uuid");
  ok(/t\.airtable_id\s+AS time_entry_airtable_id/.test(fn), "Neon branch returns the rec id too");
  ok(/la\.invoice_airtable_id IS NULL/.test(fn), "still only UNLINKED allocations");
  ok(/COALESCE\(la\.bill_rate, j\.billable_hourly_rate\)/.test(fn),
     "a rate-less mirrored row must not propose $0 for hours approved minutes ago");
});

await test("allocations: a Neon-native row carries its own bill rate", async () => {
  // Regression for 2026-08-11. v_invoices computes labor as
  // sum(allocated_hours * bill_rate). Airtable fills bill_rate through a lookup;
  // a Neon-native allocation has nothing to fill it, so it stayed NULL — and a
  // NULL rate makes the product NULL, which sum() skips. Bethel School invoice
  // 1665 printed 10.75 hours it valued at $0. Every time entry since Step 3
  // (2026-08-07) arrives without an Airtable twin, so this was every new row.
  //
  // Source-pinned: the INSERT needs a live Neon to run, and this suite is
  // offline. What it protects is the asymmetry — the native write MUST set the
  // rate, the mirror write must NOT (Airtable owns that value, and a second
  // opinion about a number feeding Invoice Total is worse than none).
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_allocations.js", import.meta.url), "utf8");

  const native = src.match(/allocation\.labor\.native[\s\S]*?RETURNING id/);
  ok(native, "the native insert is still there");
  ok(/INSERT INTO labor_billing_allocations \([^)]*bill_rate/.test(native[0]),
     "native insert must write bill_rate, or its hours are billed at $0");

  const mirror = src.match(/allocation\.labor\.insert[\s\S]*?EXCLUDED\.synced_at/);
  ok(mirror, "the mirror insert is still there");
  ok(!/bill_rate/.test(mirror[0]),
     "mirror must NOT write bill_rate — Airtable's lookup owns it");
});

// ── cutover slice 4: the allocation chain stops depending on the expense rec id
await test("allocations: the material gate resolves an expense by EITHER handle", async () => {
  // Regression for the slice-4 pre-flight finding. createMaterialAllocation was
  // parameterised on the expense REC ID and depended on it three times in one
  // query — the WHERE, the v_expenses join, and the already-allocated guard.
  // A native expense has a NULL airtable_id, so all three miss, the lookup
  // returns nothing, and the function reports "expense-not-found". The expense
  // then never gets a material allocation at all: the material is a cost with no
  // route onto an invoice and the customer is never billed for it. Silent, like
  // the Bethel School labor loss, from the other side of the ledger.
  //
  // Source-pinned for the same reason as the labor twin above — the query needs
  // a live Neon. Behaviour that IS checkable offline: no id still refuses, and
  // refuses before touching Airtable.
  const { createMaterialAllocation } = await import("../netlify/functions/_allocations.js");
  process.env.ALLOCATIONS_WRITE = "on";
  let touched = 0;
  const boom = async () => { touched++; throw new Error("must not reach Airtable"); };
  const r = await createMaterialAllocation(boom, null);
  eq(r.skipped, "no-expense-id", "refused for the right reason");
  eq(r.created, 0, "nothing created");
  eq(touched, 0, "Airtable not called");
  delete process.env.ALLOCATIONS_WRITE;

  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_allocations.js", import.meta.url), "utf8");
  const gate = src.slice(src.indexOf("export async function createMaterialAllocation"),
                         src.indexOf("// ── Attach on invoice save"));
  ok(/WHERE e\.id::text = \$1 OR e\.airtable_id = \$1/.test(gate),
     "the expense lookup must accept either handle");
  ok(/LEFT JOIN v_expenses v ON v\.id = e\.id/.test(gate),
     "v_expenses must join on the uuid — v.airtable_id = e.airtable_id is NULL=NULL for a native row");
  ok(/a\.expense_id = e\.id[\s\S]*?a\.expense_airtable_id = e\.airtable_id/.test(gate),
     "the already-allocated guard must count through BOTH keys, or a row is double-allocated");
  ok(/allocation\.material\.native/.test(gate),
     "a native expense must get a Neon-native allocation, not a refusal");

  const mirror = src.match(/allocation\.material\.insert[\s\S]*?EXCLUDED\.synced_at/);
  ok(mirror, "the mirror insert is still there");
  ok(!/SELECT id FROM expenses WHERE airtable_id/.test(mirror[0]),
     "expense_id must be passed in, not re-derived from the rec id — v_invoices joins on it");
});

await test("unlinkedMaterialAllocations: keyed on the JOB ID, and both id forms ship", async () => {
  // Two problems, one rewrite. (1) It filtered by job NAME, because {Job} on
  // Material Billing Allocations is a lookup through Expense → Job and returns a
  // name. Eight job names are shared by two jobs — "Strongsville DG" is MES 252
  // and MES 394 — so the endpoint offered one job's material under the other.
  // The invoice draft's own expenseId intersection contained it, but the API
  // answer was only safe because a caller filtered it again. (2) It had no Neon
  // path at all, so a native allocation or a native expense would have been
  // invisible — an empty picker, not an error, which on an invoice draft means
  // quietly proposing less material than was spent.
  // ⚠ AND THE AIRTABLE FALLBACK IS GONE, deleted in the same commit that made
  // expenses native — it could not see a Neon-native allocation or one on a
  // native expense, so it would have returned a SUBSET. This endpoint proposes
  // the material line on an invoice draft; a subset is an invoice that goes out
  // short. There is no honest degraded answer, so it fails closed.
  delete process.env.DATABASE_URL;
  mockTables = {
    ...TWO_JOBS,
    tblMoKg7txcfYczQQ: [
      { id: "recMA1", fields: { Job: ["Jenny Ln 1"], Expense: ["recEX1"],
                                "Allocated Material Amount $": 250 } },
    ],
  };
  const res = await GET("unlinkedMaterialAllocations", { jobId: "recJ1" });
  eq(res.statusCode, 503, "refuses rather than serving Airtable's partial answer");
  eq(json(res).ok, false, "and says so");

  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const fn = src.slice(src.indexOf("async function handleUnlinkedMaterialAllocations"),
                       src.indexOf("const SHOP_ADDRESS"));
  ok(/j\.airtable_id = \$1 OR j\.id::text = \$1/.test(fn),
     "Neon branch must key on the JOB ID, not a name — duplicate names exist");
  ok(/COALESCE\(e\.airtable_id, e\.id::text\) AS expense_id/.test(fn),
     "expense_id must match the `expenses` handler's id exactly, or the draft's intersection empties");
  ok(/a\.invoice_id IS NULL[\s\S]*?a\.invoice_airtable_id IS NULL/.test(fn),
     "unattached must test BOTH invoice columns, or native-invoiced material is re-billed");
});

await test("allocations: the attach is BATCHED, and never touches Airtable without Neon", async () => {
  // Regression for 2026-08-11: Bethel School's invoice carried 163 allocations,
  // the attach did two round trips each, and the function ran past Netlify's
  // gateway timeout. The browser alerted "Error saving invoice" over an invoice
  // that HAD saved — and pressing Save again would have made a duplicate.
  //
  // The batching itself is asserted against the source, same as the billing-sync
  // guard below: exercising it needs a live Neon (the candidate list is a Neon
  // read) and this suite is deliberately offline. What IS behavioural here is
  // the half that can be checked without one — a failed lookup must not leave a
  // half-attached invoice behind it.
  const { attachAllocationsToInvoice } = await import("../netlify/functions/_allocations.js");
  process.env.ALLOCATIONS_WRITE = "on";
  delete process.env.DATABASE_URL;
  let touched = 0;
  const boom = async () => { touched++; throw new Error("must not reach Airtable"); };
  const r = await attachAllocationsToInvoice(boom, { id: NEON_INVOICE_ID, airtableId: "recInv" }, "recJob");
  eq(r.skipped, "lookup-failed", "no candidate list → refuse, don't guess");
  eq(r.attached, 0, "nothing attached");
  eq(touched, 0, "Airtable not called when Neon can't be read");

  // ⚠ CUTOVER SLICE 3: the invoice arrives as BOTH handles, and the NEON uuid is
  // the required half. A native invoice has no rec id, and passing the rec id
  // alone — the old signature — would have attached nothing to it, printing an
  // invoice with no labor and no material on it. Missing uuid must refuse
  // outright rather than write a NULL invoice_id over the allocations.
  eq((await attachAllocationsToInvoice(boom, { airtableId: "recInv" }, "recJob")).skipped,
     "missing-ids", "no Neon uuid → refuse; the rec id alone is not enough");
  eq((await attachAllocationsToInvoice(boom, { id: NEON_INVOICE_ID }, null)).skipped,
     "missing-ids", "no job → refuse");
  eq(touched, 0, "still no Airtable call");
  delete process.env.ALLOCATIONS_WRITE;

  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_allocations.js", import.meta.url), "utf8");
  const attach = src.slice(src.indexOf("export async function attachAllocationsToInvoice"));

  const cap = src.match(/const AT_BATCH = (\d+)/);
  ok(cap, "AT_BATCH is declared");
  ok(Number(cap[1]) <= 10, `AT_BATCH is ${cap[1]}; Airtable rejects writes over 10 records`);

  // A batch PATCH sends { records: [...] } to the TABLE. The per-record form —
  // `${table}/${id}` — is what made it slow, so its absence is the assertion.
  ok(/records: batch\.map/.test(attach), "Airtable write sends a records[] batch");
  ok(!/\$\{table\}\/\$\{/.test(attach), "no per-record PATCH left in the attach path");
  ok(/ANY\(\$1::uuid\[\]\)/.test(attach), "Neon commits a chunk per UPDATE, not a row");
});

await test("billing-sync: the delete pass spares Neon-native allocations", async () => {
  // ⚠⚠ THE SINGLE MOST DANGEROUS LINE IN THE BILLING PATH. The sync deletes any
  // allocation absent from the Airtable fetch, which is right for rows Airtable
  // owns and catastrophic for Neon-native ones: they are invisible to that fetch
  // by construction, so an unguarded predicate deletes every one within the hour
  // and the invoice total silently drops AFTER looking correct.
  //
  // Asserted against the source text rather than a live database, because the
  // offline suite cannot reach Neon and this is a predicate, not a behaviour we
  // can observe here. Crude, and far better than no check at all: if someone
  // "tidies" the guard away, this fails.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_billing-sync.js", import.meta.url), "utf8");
  const deletes = src.match(/DELETE FROM \w*billing_allocations[\s\S]*?RETURNING 1/g) || [];
  eq(deletes.length, 2, "two delete statements, labor and material");
  for (const d of deletes) {
    ok(/airtable_id IS NOT NULL/.test(d),
       `delete pass is missing the Neon-native guard:\n${d}`);
  }
});

await test("deleteJobEstimate: STRICT admin — office is out, and a uuid is now a real id", async () => {
  // ⚠ This action has NO STATUS GUARD by owner's explicit decision (2026-08-20):
  // a Sent or Approved estimate — the record of what a customer was quoted —
  // will be deleted without complaint. The role tier IS the guard, so it sits at
  // _ADMIN, not the _ADMIN_OFFICE that the other back-office money ops use.
  // Office handling money already earned is not the same as office destroying
  // the record of what was promised.
  mockTables = { "Job Estimates": [] };
  eq((await POST("deleteJobEstimate", { estimateId: "recX" }, OFFICE_TOK)).statusCode, 403, "OFFICE refused — this is the guard");
  eq((await POST("deleteJobEstimate", { estimateId: "recX" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("deleteJobEstimate", { estimateId: "recX" }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  eq((await POST("deleteJobEstimate", {}, ADMIN_TOK)).statusCode, 400, "missing id rejected");

  // ⚠⚠ INVERTED BY CUTOVER SLICE 3 (2026-08-22). This asserted the opposite —
  // that a uuid is refused, "because estimates are still Airtable-identity".
  // They are not: `handleCreateJobEstimate` writes Neon first, so a uuid IS an
  // estimate id now, and the old `startsWith("rec")` guard would have made every
  // estimate created since undeletable. Both shapes must get past the guard and
  // reach the database, which offline resolves to the 503 below rather than to a
  // 400. What is still asserted is that neither shape is rejected on FORM.
  for (const id of ["recX", "4f3a4be6-88ba-4cab-af82-f9fea6915ac9"]) {
    const r = await POST("deleteJobEstimate", { estimateId: id }, ADMIN_TOK);
    eq(r.statusCode, 503, `${id}: accepted on form, refused only for want of a database`);
  }
});

// ── identity cutover, slice 3 (docs/PLAN-airtable-identity-cutover.md) ──────

await test("slice 3: every estimate and invoice write fails CLOSED, and nothing half-lands in Airtable", async () => {
  // The contract these seven share: the row is born in Neon, Airtable is a
  // best-effort mirror. With no database that means REFUSE — an Airtable-only
  // estimate or invoice is invisible to the app forever, because every read of
  // both tables has been Neon-first since Step 4e and nothing back-fills them.
  //
  // The half that would be silent is the second assertion: a write that refuses
  // but has already POSTed to Airtable leaves a record the app cannot see and
  // the owner can, which is worse than either outcome on its own.
  mockTables = { "Job Estimates": [], "Invoices": [], "Sent Estimate PDFs": [], Jobs: [], Employees: [] };
  const writes = [
    ["createJobEstimate",    { jobId: "recJob", baseAmount: 1000, laborHours: 10 }],
    ["updateEstimate",       { estimateId: "recEst", actualEstimate: 900 }],
    ["updateEstimateStatus", { estimateId: "recEst", status: "Sent" }],
    ["deleteJobEstimate",    { estimateId: "recEst" }],
    ["saveEstimate",         { jobId: "recJob", totalAmount: 1000, estimateNumber: 2215 }],
    ["saveInvoice",          { jobId: "recJob", totalAmount: 1000, billingMode: "tm" }],
    ["setInvoiceStatus",     { invoiceId: "recInv", status: "Paid" }],
  ];
  for (const [action, body] of writes) {
    const before = lastFetch;
    const res = await POST(action, body, ADMIN_TOK);
    eq(res.statusCode, 503, `${action}: no database → refuse`);
    const wrote = lastFetch !== before &&
                  ["POST", "PATCH", "DELETE"].includes((lastFetch?.opts?.method || "").toUpperCase());
    eq(wrote, false, `${action}: nothing was written to Airtable on the way to refusing`);
  }
});

await test("slice 3: the columns Airtable used to compute are computed here, and correctly", async () => {
  // These four cannot be exercised offline — they are SQL, and the harness has
  // no database — so they are asserted at source. Each one is a number that a
  // person is paid or billed from, and each failed silently rather than loudly
  // when it was wrong before.
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const src = readFileSync(fileURLToPath(new URL("../netlify/functions/airtable.js", import.meta.url)), "utf8");
  const sql = readFileSync(fileURLToPath(new URL("../db/schema/055_estimates_invoices_native.sql", import.meta.url)), "utf8");

  // 1. Estimated Labor Cost = hours × 32.50, verified against all 89 estimates
  //    before the reversal. It feeds v_job_rollups.est_labor_cost_rollup, i.e.
  //    estimated GP, so a native estimate that left it null would report a job
  //    as more profitable than it is.
  ok(/const EST_LABOR_RATE = 32\.50;/.test(src), "the estimate labor rate is 32.50 and is named");
  ok(/sqlEstLaborCost\("\$5::numeric"\)/.test(src), "the create computes labor cost rather than storing null");
  ok(/sqlEstTotal\("\$5::numeric", "\$6::numeric"\)/.test(src), "the create computes the estimate total");

  // ⚠⚠ REGRESSION, 2026-08-22: this shipped broken and the first click in
  // production failed with `inconsistent types deduced for parameter $5`.
  // A bare `0` is an INTEGER literal, so `COALESCE($5, 0)` deduced $5 as
  // integer while the numeric column it also feeds deduced numeric — and a
  // parameter used twice must resolve to ONE type. The casts are the fix, and
  // they are asserted because nothing else in this offline suite can see them.
  const code = src.split("\n").filter(l => !/^\s*(\/\/|\*|\/\*)/.test(l)).join("\n");
  ok(!/COALESCE\(\$\d+, 0\)/.test(code), "no untyped zero literal against a parameter");
  ok(/COALESCE\(\$\{hoursExpr\}, 0::numeric\)/.test(src), "the coalesce literal is numeric, not integer");

  // 2. A partial update recomputes from the STORED values of the fields it was
  //    not given. Editing only the material cost still moves the total.
  const upd = src.slice(src.indexOf("async function handleUpdateEstimate"));
  ok(/COALESCE\(\$3, estimated_labor_hours\)/.test(upd.slice(0, 2000)),
     "update derives from stored hours when hours weren't sent");
  ok(/COALESCE\(\$4, estimated_material_cost\)/.test(upd.slice(0, 2000)),
     "update derives from stored material when material wasn't sent");

  // 3. invoice_total stays NULL on a native invoice: v_invoices.invoice_total_calc
  //    is the computed figure and the stored one goes stale the moment an
  //    allocation changes (db/schema/015). Two opinions about a total is how a
  //    wrong number gets quoted.
  const inv = src.slice(src.indexOf('neonWrite("invoice.create"'), src.indexOf('neonWrite("invoice.create"') + 2500);
  ok(!/\binvoice_total\b\s*,/.test(inv.split("VALUES")[0]), "invoice_total is not written by the create");
  ok(/'-001'/.test(inv), "invoice_number reproduces the Airtable label verbatim");

  // 4. v_invoices resolves BOTH handle shapes. If it resolved by rec id only, a
  //    native invoice would print with no labor and no material on it — a $0
  //    invoice, with no error anywhere.
  ok(/COALESCE\(a\.invoice_id, i2\.id\)/.test(sql), "labor resolves uuid-first, rec id second");
  ok(/COALESCE\(m\.invoice_id, i2\.id\)/.test(sql), "material resolves uuid-first, rec id second");
  ok(/ADD COLUMN IF NOT EXISTS invoice_id uuid/.test(sql), "labor allocations gained the uuid link");
});

await test("slice 3: the mirror can fail without duplicating an invoice", async () => {
  // ⚠⚠ The bug this guards against is a DUPLICATE INVOICE, not a lost one.
  // `syncInvoiceToNeon` is an INSERT … ON CONFLICT (airtable_id). If the mirror
  // POST succeeds and the stamp that records its rec id then fails, the row is
  // still native, nothing conflicts, and the upsert writes a SECOND invoice for
  // the same work — one native, one mirrored, both billable.
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const src = readFileSync(fileURLToPath(new URL("../netlify/functions/airtable.js", import.meta.url)), "utf8");
  const save = src.slice(src.indexOf("async function handleSaveInvoice"));
  const body = save.slice(0, save.indexOf("\n}\n"));

  ok(/if \(data\?\.id && \(recId \|\| stamped\)\) await syncInvoiceToNeon\(data\)/.test(body),
     "the carry-back only runs on a row that is known to hold that rec id");
  ok(/stamped = true/.test(body), "the stamp records whether it actually succeeded");

  // And the attach gets both handles — the uuid is the half that always exists.
  ok(/attachAllocationsToInvoice\(\s*atFetch, \{ id: row\.id, airtableId:/.test(body),
     "allocations are attached by uuid, with the rec id only for Airtable's link");

  const alloc = readFileSync(fileURLToPath(new URL("../netlify/functions/_allocations.js", import.meta.url)), "utf8");
  ok(/SET invoice_id = \$2, invoice_airtable_id = \$3/.test(alloc), "the attach writes both columns");
  ok(/a\.invoice_airtable_id IS NULL AND a\.invoice_id IS NULL/.test(alloc),
     "\"unattached\" means BOTH are empty — or a native invoice's work is re-billed on the next save");
});

await test("estimateTemplateDelete: admin+office, and it clears provenance before deleting", async () => {
  mockTables = {};
  eq((await POST("estimateTemplateDelete", { templateId: "recX" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("estimateTemplateDelete", { templateId: "recX" }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  eq((await POST("estimateTemplateDelete", {}, ADMIN_TOK)).statusCode, 400, "missing id rejected");

  // ORDER IS LOAD-BEARING. The UPDATE that clears job_estimates.
  // source_template_handle must run BEFORE the DELETE: if the row went first and
  // the clear then failed, every referencing estimate would carry a handle
  // pointing at nothing — a breadcrumb to a deleted row reads like data and is
  // worse than no breadcrumb. Asserted against the source because the offline
  // suite has no Neon to observe it in.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const fn = src.slice(src.indexOf("async function handleEstimateTemplateDelete"));
  const body = fn.slice(0, fn.indexOf("\n}\n"));
  const clearAt  = body.indexOf("source_template_handle = NULL");
  const deleteAt = body.indexOf("DELETE FROM estimate_templates");
  ok(clearAt > 0 && deleteAt > 0, "both statements present");
  ok(clearAt < deleteAt, "provenance is cleared BEFORE the template row is deleted");
});

await test("index.html: no role-gated element sits below the main <script>", async () => {
  // ⚠ THE CLASS DOES NOTHING DOWN THERE. renderAuth() clears `hidden` with a
  // one-shot document.querySelectorAll(".admin-only") at init. The main <script>
  // closes around line 23900 and every modal's markup follows it, so an element
  // added below that point does not exist when the sweep runs — its `hidden`
  // class is never cleared and it is invisible to EVERY role, admin included.
  //
  // Cost a live bug on 2026-08-20: the "Manage templates" button shipped
  // correct in every other respect and simply never appeared. It looked like a
  // stale cache, and the deployed bytes were verified identical before anyone
  // suspected DOM order. The fix is to toggle visibility when the modal opens
  // (openNewEstModal does), which is also what openNewAgencyModal has always
  // done for its submit-button wiring.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../index.html", import.meta.url), "utf8");
  const lines = src.split(/\r?\n/);

  // The main script is the longest inline <script> block — find where it ends.
  let scriptEnd = -1, openAt = -1, best = -1;
  lines.forEach((l, i) => {
    if (/<script(?![^>]*\bsrc=)[^>]*>/.test(l)) openAt = i;
    if (/<\/script>/.test(l) && openAt >= 0) {
      if (i - openAt > best) { best = i - openAt; scriptEnd = i; }
      openAt = -1;
    }
  });
  ok(scriptEnd > 0, "found the main <script> block");

  const ROLE_CLASSES = /class="[^"]*\b(admin-only|strict-admin-only|non-admin-only|payroll-eligible-only|employee-only)\b/;
  const offenders = [];
  for (let i = scriptEnd + 1; i < lines.length; i++) {
    if (ROLE_CLASSES.test(lines[i])) offenders.push(`${i + 1}: ${lines[i].trim().slice(0, 110)}`);
  }
  eq(offenders.length, 0,
     "role-gating class below the main <script> — renderAuth() never sees it, so it stays hidden for everyone.\n" +
     "Toggle it on modal open instead.\n" + offenders.join("\n"));
});

await test("billing-sync: estimate templates are NOT synced — the app owns them now", async () => {
  // Templates got a write path on 2026-08-20 (db/schema/047). The instant they
  // did, this sync flipped from being the thing that KEPT the table populated to
  // the thing that OVERWROTE it: the upsert is ON CONFLICT (airtable_id) DO
  // UPDATE, so every edit to one of the five Airtable-era templates was reverted
  // at the top of the hour. Silently — nothing in that file throws.
  //
  // Source-text assertion for the same reason as the delete-pass guard above:
  // the offline suite cannot reach Neon, and re-adding the block is a plausible
  // "the templates look stale, let's sync them again" mistake that would
  // reinstate a bug nobody would attribute to this file.
  const fs = await import("node:fs/promises");
  const src = await fs.readFile(new URL("../netlify/functions/_billing-sync.js", import.meta.url), "utf8");
  const code = src.replace(/\/\/[^\n]*/g, "");   // strip comments; they discuss it on purpose
  ok(!/estimate_templates/.test(code), "no estimate_templates write may return to the hourly sync");
  ok(!/"Estimate Templates"/.test(code), "and it must not fetch the Airtable table either");
  // Companies was fetched ONLY to resolve template contractor names. Nothing
  // else here reads it, so a lingering fetch is dead weight on every hourly run.
  ok(!/"Companies"/.test(code), "the Companies fetch went with it");
});

await test("setEmployeeSalaried: admin only — it decides how someone is paid", async () => {
  // Office is refused deliberately. Office handles money already earned
  // (approving expenses, marking invoices paid); this decides whether a person
  // earns time-and-a-half over 40, which sits with payroll runs at admin.
  mockTables = { Employees: [] };
  eq((await POST("setEmployeeSalaried", { employeeId: "recX", salaried: true }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("setEmployeeSalaried", { employeeId: "recX", salaried: true }, OFFICE_TOK)).statusCode, 403, "office refused — this is a pay decision");
  eq((await POST("setEmployeeSalaried", { employeeId: "recX", salaried: true }, VIEWER_TOK)).statusCode, 403, "viewer refused");
});

await test("setEmployeeSalaried: refuses anything that isn't an explicit true/false", async () => {
  // `typeof salaried !== "boolean"` rather than a truthiness check, and this is
  // the test that keeps it that way. A missing field, "false", or 0 must not be
  // coerced: every one of those would silently answer a question about somebody's
  // pay that the caller never actually asked.
  mockTables = { Employees: [] };
  eq((await POST("setEmployeeSalaried", { salaried: true })).statusCode, 400, "no employeeId → 400");
  eq((await POST("setEmployeeSalaried", { employeeId: "notarec", salaried: true })).statusCode, 400, "malformed id → 400");
  eq((await POST("setEmployeeSalaried", { employeeId: "recX" })).statusCode, 400, "missing salaried → 400");
  eq((await POST("setEmployeeSalaried", { employeeId: "recX", salaried: "false" })).statusCode, 400, "string not coerced");
  eq((await POST("setEmployeeSalaried", { employeeId: "recX", salaried: 0 })).statusCode, 400, "number not coerced");
});

await test("setEmployeeSalaried: fails CLOSED when Neon is unreachable", async () => {
  // Neon-only, no Airtable mirror, so there is nowhere else this could have
  // landed. Reporting success on a write that didn't happen would leave the
  // People card showing "Salary" for someone payroll still treats as hourly.
  //
  // ⚠ HONEST SCOPE: offline this fails at the CONNECTION, so it proves the
  // endpoint cannot report success without a working Neon write — NOT that the
  // zero-row 404 branch fires. Same gap as setEmployeeActive above.
  mockTables = { Employees: [{ id: "recSal", fields: { "Employee Name": "Someone", Active: true } }] };
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  const res = await POST("setEmployeeSalaried", { employeeId: "recSal", salaried: true });
  ok(res.statusCode >= 400, `expected an error, got ${res.statusCode}`);
  delete process.env.DATABASE_URL;
});

await test("people: admin only", async () => {
  mockTables = { Employees: [] };
  eq((await GET("people", {}, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await GET("people", {}, OFFICE_TOK)).statusCode, 403, "office refused — the roster carries wages");
  eq((await GET("people", {}, VIEWER_TOK)).statusCode, 403, "viewer refused");
});

await test("employeePin: strict admin only, and reports a missing PIN as missing", async () => {
  // A live credential. Office is excluded exactly as it is from `people`.
  mockTables = { Employees: [
    { id: "recHasPin", fields: { "Employee Name": "Has Pin", PIN: "4821" } },
    { id: "recNoPin",  fields: { "Employee Name": "No Pin" } },
  ] };
  eq((await GET("employeePin", { employeeId: "recHasPin" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await GET("employeePin", { employeeId: "recHasPin" }, OFFICE_TOK)).statusCode, 403, "office refused");
  eq((await GET("employeePin", { employeeId: "recHasPin" }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  const b = json(await GET("employeePin", { employeeId: "recHasPin" }));
  eq(b.pin, "4821", "admin gets the PIN");
  eq(b.hasPin, true, "hasPin true");
  // An empty PIN is not cosmetic — handleLogin refuses to match one, so the
  // screen has to say so rather than render a blank.
  const n = json(await GET("employeePin", { employeeId: "recNoPin" }));
  eq(n.hasPin, false, "missing PIN reported as missing, not as an empty string");
  eq((await GET("employeePin", {})).statusCode, 400, "no employeeId → 400");
});

await test("setEmployeePin: admin only, digits only, and no duplicates", async () => {
  mockTables = { Employees: [
    { id: "recA", fields: { "Employee Name": "Larry Unruh", PIN: "1184" } },
    { id: "recB", fields: { "Employee Name": "Tisha",       PIN: "2222" } },
  ] };
  eq((await POST("setEmployeePin", { employeeId: "recB", pin: "4321" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("setEmployeePin", { employeeId: "recB", pin: "4321" }, OFFICE_TOK)).statusCode, 403, "office refused");
  eq((await POST("setEmployeePin", { employeeId: "recB", pin: "12" }, ADMIN_TOK)).statusCode, 400, "too short");
  eq((await POST("setEmployeePin", { employeeId: "recB", pin: "abcd" }, ADMIN_TOK)).statusCode, 400, "not digits");
  // The live bug this guard exists for: Larry (admin) and two office users all
  // had 1184, so either office user could log in as `larry` and get admin.
  // Login matches identifier + PIN, so a shared PIN IS a working credential
  // for someone else's account.
  // ⚠ INVERTED 2026-08-24, for the same reason as the createEmployee twin: this
  // resolve-and-clash-check reads NEON now, not Airtable.
  //
  // It had to move because it BROKE IN THE FIELD the day slice 5 shipped. The
  // handler opened with `fetchAll(TABLES.employees)` and `.find(r => r.id ===
  // employeeId)` — an Airtable existence check — so changing a natively-hired
  // employee's PIN answered "No such employee." for a person who had just
  // logged in successfully. The slice-5 sweep covered SQL sites, rec-id guards
  // and Airtable WRITES, and still missed an Airtable **READ** used as a
  // lookup. A handler can be fully dual-handled in every statement it writes
  // and still 404 in its first three lines.
  //
  // The clash half would have failed the quieter way: Airtable cannot see a
  // native hire's PIN, so two people could end up sharing one — and a shared
  // PIN makes `neonLoginCandidate` ambiguous, which it refuses, locking BOTH
  // of them out rather than letting one impersonate the other.
  //
  // Offline, what this can pin is the contract that matters: an unanswerable
  // duplicate check REFUSES the change rather than assuming the PIN is free.
  const dup = await POST("setEmployeePin", { employeeId: "recB", pin: "1184" }, ADMIN_TOK);
  eq(dup.statusCode, 503, "an unanswerable duplicate check refuses the change");
  ok(/nothing was changed/i.test(json(dup).error), "and says plainly that nothing happened");
});

// ── Cutover slice 6: jobs can be Neon-native ───────────────────────────────
// Jobs are the spine — every expense, photo, estimate, invoice, panel and
// schedule entry hangs off a job id. This slice is inert until the first native
// job exists, so what these pin is the SHAPE, not behaviour.
const NATIVE_JOB = "7c2e4a10-9b3d-4f52-8a61-0d5e7c9b1234";

await test("slice 6: the 400 guards no longer reject a native job's own id", async () => {
  // Fifteen job guards tested String(jobId).startsWith("rec"). A job the app had
  // just created would have been refused by its own expense, photo and panel
  // handlers with "Invalid jobId" — the id was fine, the guard was stale.
  mockTables = { Jobs: [] };
  delete process.env.DATABASE_URL;
  for (const [action, body] of [
    ["addGeneralExpense", { jobId: NATIVE_JOB, type: "Materials", amount: 10 }],
    ["addLiftExpense",    { jobId: NATIVE_JOB, amount: 10 }],
  ]) {
    const res = await POST(action, body);
    ok(res.statusCode !== 400, `${action} does not 400 on a uuid (got ${res.statusCode})`);
  }
});

await test("slice 6: the job source rules the sweep depends on", async () => {
  const fs = await import("node:fs/promises");
  const raw = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  const src = raw.split("\n").filter(l => !l.trim().startsWith("//")).join("\n");

  // JOB_SELECT's first column IS the job id the whole app speaks — it comes back
  // as job.id and goes straight out again as jobId on expenses, photos,
  // estimates, invoices, panels, the schedule and every R2 prefix. A bare emit
  // hands the client NULL for a native job: the job lists, and nothing on it
  // opens.
  ok(/SELECT COALESCE\(j\.airtable_id, j\.id::text\) AS airtable_id/.test(src),
     "JOB_SELECT emits the dual handle");

  // No job resolve may be left on a bare airtable_id. A missed one does not
  // error — it writes a NULL job_id, and the row silently drops out of the job's
  // costs, its GP and (for estimates) its expected revenue.
  const bare = (src.match(/FROM jobs WHERE airtable_id = \$\d+\)/g) || []);
  eq(bare.length, 0, `bare job resolves left: ${bare.join(", ")}`);

  // ⚠ AND binds tighter than OR. `WHERE j.airtable_id = $1 OR j.id::text = $1
  // AND a.invoice_id IS NULL` returns every allocation on every job. Three of
  // these shipped into the working tree during this slice before being caught.
  const lines = src.split("\n");
  const unparenthesised = [];
  lines.forEach((l, i) => {
    if (!/OR j\.id::text = \$/.test(l)) return;
    if (/\)\s*$/.test(l.trim())) return;                 // already wrapped
    if (/^AND\b/i.test((lines[i + 1] || "").trim())) unparenthesised.push(i + 1);
  });
  eq(unparenthesised.length, 0,
     `dual handle followed by AND without parentheses at line(s): ${unparenthesised.join(", ")}`);
});

await test("every dual handle resolves BOTH halves with the SAME parameter", async () => {
  // ⚠⚠ THE BUG THIS EXISTS FOR, WHICH SHIPPED AND WAS CAUGHT BY LUCK.
  // The slice-5 sweep converted clauses with a plain string replace on
  // `... airtable_id = $1`. That text is a PREFIX OF `... airtable_id = $10`, so
  // it matched inside the longer placeholder and left behind
  //     WHERE airtable_id = $1 OR id::text = $10 OR id::text = $10
  // in createExpenseNative — resolving the submitting EMPLOYEE by $1, the job
  // handle. Nothing errored; `submitted_by_name` would simply have come back
  // NULL on every new expense. Same family as the schema-057 replay bug, where
  // `billable_material_amount` is a prefix of `billable_material_amount_calc`.
  //
  // A dual handle whose two sides read different parameters is ALWAYS wrong, so
  // that is a rule a machine can hold rather than a reviewer.
  const fs = await import("node:fs/promises");
  const files = ["airtable.js", "inventory.js", "_employees.js", "_revocation.js",
                 "_allocations.js", "_expenses.js", "_job-webhooks.js"];
  const offenders = [];
  for (const name of files) {
    const src = await fs.readFile(new URL(`../netlify/functions/${name}`, import.meta.url), "utf8");
    src.split("\n").forEach((line, i) => {
      const t = line.trim();
      if (t.startsWith("//") || t.startsWith("--")) return;   // prose quotes SQL on purpose
      const re = /airtable_id ?= ?\$(\d+)[^$]*?id::text ?= ?\$(\d+)/g;
      let m;
      while ((m = re.exec(line)) !== null) {
        if (m[1] !== m[2]) offenders.push(`${name}:${i + 1} ($${m[1]} vs $${m[2]})`);
      }
    });
  }
  eq(offenders.length, 0, `mismatched dual handles: ${offenders.join(", ")}`);
});

await test("slice 5 follow-up: no employee handler resolves via an Airtable existence check", async () => {
  // The regression above, source-pinned. `fetchAll(TABLES.employees)` is fine as
  // a Neon FALLBACK (`?? fetchAll(...)`) and fine for the payroll roster; it is
  // NOT fine as the lookup that decides whether a person exists, because a
  // native hire is not in that table at all.
  const fs = await import("node:fs/promises");
  const raw = await fs.readFile(new URL("../netlify/functions/airtable.js", import.meta.url), "utf8");
  // Strip line comments before matching. The note left at the fix site quotes
  // the offending code verbatim — deliberately, so the next reader sees what it
  // looked like — and an unfiltered grep flags the explanation as the bug.
  const src = raw.split("\n").filter(l => !l.trim().startsWith("//")).join("\n");
  ok(!/const all = await fetchAll\(TABLES\.employees\);\s*\n\s*const target = all\.find/.test(src),
     "setEmployeePin no longer resolves its target out of Airtable");
  ok(!/\.find\(r => r\.id === employeeId\)/.test(src),
     "and nothing else matches an employee handle against Airtable record ids");

  // ── The QUIET half of the rec-id trap, which shipped and broke scheduling.
  // `setScheduleCrew` filtered its crew array with `x.startsWith("rec")`, so a
  // native hire's uuid was dropped BEFORE the SQL ran. No error: the rest of the
  // crew saved and the new person was simply absent from the entry.
  //
  // ⚠ It escaped the slice-5 sweep because the grep was for
  // `String(employeeId).startsWith("rec")` — here the id is an anonymous array
  // element. A filter on a LIST of ids reads nothing like a guard on a single
  // one. Grep the predicate, not the variable name.
  ok(/\.filter\(isEmployeeHandle\)/.test(src),
     "setScheduleCrew accepts either id form instead of dropping uuids");
  ok(!/crewAtIds : \[\]\)\.filter\(x => typeof x === "string" && x\.startsWith\("rec"\)\)/.test(src),
     "and the old rec-only crew filter is gone");

  // ── The READ half of the same bug, and the worse one. `crew_ids` and
  // `crew_names` are zipped BY POSITION in index.html (~21501, ~21805). They
  // were aggregated with DIFFERENT filters — ids on `e.airtable_id IS NOT NULL`,
  // names on `e.name IS NOT NULL` — so a native hire lost their id but kept
  // their name, the arrays fell out of step, and everyone sorting after them was
  // paired with the wrong id. A mis-assigned crew member beats an absent one for
  // damage. One shared filter is what stops them diverging again.
  ok(/array_agg\(COALESCE\(e\.airtable_id, e\.id::text\) ORDER BY e\.name\)/.test(src),
     "the schedule read emits the dual handle for crew");
  eq((src.match(/FILTER \(WHERE c\.employee_id IS NOT NULL\), '\{\}'\)/g) || []).length, 2,
     "and both crew arrays share ONE filter so they cannot fall out of step");
  ok(!/FILTER \(WHERE e\.airtable_id IS NOT NULL\), '\{\}'\) AS crew_ids/.test(src),
     "the old airtable_id-only crew filter is gone");
});

await test("setEmployeePin: a PIN change signs the person out, and fails closed", async () => {
  // Same contract as setEmployeeActive: Neon first, and a change we could not
  // record as a sign-out is not reported as done.
  mockTables = { Employees: [{ id: "recA", fields: { "Employee Name": "Larry Unruh", PIN: "1184" } }] };
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  const res = await POST("setEmployeePin", { employeeId: "recA", pin: "5678" }, ADMIN_TOK);
  ok(res.statusCode >= 500, `expected a server error, got ${res.statusCode}`);
  delete process.env.DATABASE_URL;
});

await test("login: the Neon shadow cannot affect the answer", async () => {
  // Stage 2 contract. Airtable decides; the shadow only logs. A broken or
  // absent Neon must leave both the allow and the refuse path untouched —
  // this is login, so the blast radius of getting it wrong is everybody.
  mockTables = { Employees: [
    { id: "recE1", fields: { "Employee Name": "Rick Nee", PIN: "1234", Role: "admin", Active: true } },
  ] };
  for (const url of [undefined, "not-a-valid-connection-string"]) {
    if (url) process.env.DATABASE_URL = url; else delete process.env.DATABASE_URL;
    const good = await POST("login", { identifier: "rick nee", pin: "1234" });
    eq(good.statusCode, 200, `login still succeeds (DATABASE_URL=${url})`);
    eq(json(good).user.role, "admin", "role unchanged by the shadow");
    ok(json(good).token, "token still issued");
    const bad = await POST("login", { identifier: "rick nee", pin: "9999" });
    eq(bad.statusCode, 401, `wrong PIN still refused (DATABASE_URL=${url})`);
  }
  delete process.env.DATABASE_URL;
});

await test("login: LOGIN_SOURCE defaults OFF — the flip ships inert", async () => {
  // Stage 3 lands in production switched off. If this ever fails, the flip has
  // taken effect without anyone deciding to enable it.
  delete process.env.LOGIN_SOURCE;
  mockTables = { Employees: [
    { id: "recE1", fields: { "Employee Name": "Rick Nee", PIN: "1234", Role: "admin", Active: true } },
  ] };
  const res = await POST("login", { identifier: "rick nee", pin: "1234" });
  eq(res.statusCode, 200, "login works");
  eq(json(res)._source, "airtable", "Airtable answered, not Neon");
});

await test("login: with the flip ON and Neon dead, Airtable still lets you in", async () => {
  // The property the whole kill-switch design exists for. Neon having no
  // opinion — unset OR unreachable — must fall back, never refuse: a database
  // blip that locks out every crew member is far worse than the stale-copy
  // risk it would be avoiding.
  mockTables = { Employees: [
    { id: "recE1", fields: { "Employee Name": "Rick Nee", PIN: "1234", Role: "admin", Active: true } },
  ] };
  process.env.LOGIN_SOURCE = "neon";
  for (const url of [undefined, "not-a-valid-connection-string"]) {
    if (url) process.env.DATABASE_URL = url; else delete process.env.DATABASE_URL;
    const res = await POST("login", { identifier: "rick nee", pin: "1234" });
    eq(res.statusCode, 200, `fallback login succeeds (DATABASE_URL=${url})`);
    eq(json(res)._source, "airtable", "and reports honestly which store answered");
    eq(json(res).user.role, "admin", "role intact");
    // A wrong PIN must still be refused on the fallback path.
    eq((await POST("login", { identifier: "rick nee", pin: "9999" })).statusCode, 401, "bad PIN still refused");
  }
  delete process.env.DATABASE_URL;
  delete process.env.LOGIN_SOURCE;
});

await test("updateEmployee: admin only, and you can't change your own role", async () => {
  mockTables = { Employees: [{ id: "recAdmin", fields: { "Employee Name": "Rick Unruh", Role: "admin" } }] };
  const base = { employeeId: "recX", name: "X", role: "employee" };
  eq((await POST("updateEmployee", base, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("updateEmployee", base, OFFICE_TOK)).statusCode, 403, "office refused — role edits are authz changes");
  eq((await POST("updateEmployee", { ...base, name: "" })).statusCode, 400, "empty name refused");
  // Whitelisted, so a stray value can't trip Airtable typecast into inventing
  // a new single-select option (CLAUDE.md).
  eq((await POST("updateEmployee", { ...base, role: "superuser" })).statusCode, 400, "unknown role refused");
  // recAdmin is the id inside ADMIN_TOK. Demoting yourself locks you out of the
  // only screen that could undo it.
  const self = await POST("updateEmployee", { employeeId: "recAdmin", name: "Rick Unruh", role: "employee" });
  eq(self.statusCode, 400, "can't demote yourself");
  // ...but editing your own non-role fields is fine, so this must NOT 400 on
  // the self check (it fails later, at the unreachable Neon write).
  const selfSameRole = await POST("updateEmployee", { employeeId: "recAdmin", name: "Rick U", role: "admin" });
  ok(selfSameRole.statusCode !== 400, "same-role self edit passes the self guard");
});

await test("rates + createEmployee: admin only, and validated", async () => {
  mockTables = { Employees: [{ id: "recA", fields: { "Employee Name": "Larry Unruh", PIN: "1184" } }] };
  eq((await GET("employeeRates", { employeeId: "recA" }, OFFICE_TOK)).statusCode, 403, "office can't read wages");
  eq((await POST("addEmployeeRaise", { employeeId: "recA", startDate: "2026-01-01", wage: 30, burdenPct: 25 }, OFFICE_TOK)).statusCode, 403, "office can't set rates");
  eq((await POST("createEmployee", { name: "X", role: "employee", pin: "1111" }, OFFICE_TOK)).statusCode, 403, "office can't create people");

  // Burden arrives as a PERCENT (25) and is stored as a FRACTION (0.25).
  // Getting that backwards would multiply every job's labor cost by 25, so the
  // bounds are deliberately tight.
  eq((await POST("addEmployeeRaise", { employeeId: "recA", startDate: "2026-01-01", wage: 0, burdenPct: 25 })).statusCode, 400, "zero wage refused");
  eq((await POST("addEmployeeRaise", { employeeId: "recA", startDate: "2026-01-01", wage: 30, burdenPct: 900 })).statusCode, 400, "absurd burden refused");
  eq((await POST("addEmployeeRaise", { employeeId: "recA", startDate: "not-a-date", wage: 30, burdenPct: 25 })).statusCode, 400, "bad date refused");

  eq((await POST("createEmployee", { name: "", role: "employee", pin: "1111" })).statusCode, 400, "empty name refused");
  eq((await POST("createEmployee", { name: "New Guy", role: "wizard", pin: "1111" })).statusCode, 400, "bogus role refused");
  eq((await POST("createEmployee", { name: "New Guy", role: "employee", pin: "12" })).statusCode, 400, "short PIN refused");
  // ⚠ INVERTED BY CUTOVER SLICE 5 (2026-08-24). This used to assert a 409 for a
  // duplicate PIN, satisfied from the mocked Airtable Employees table above.
  // The check reads NEON now, and it had to move: after this slice the Airtable
  // table does not contain natively-hired people at all, so their PINs were
  // invisible to it and a second person could be handed the same one.
  //
  // Login matches identifier + PIN, so a duplicate PIN is a working credential
  // for someone else's account. It is also worse than that here — two rows
  // matching one PIN make `neonLoginCandidate` ambiguous, which it refuses, so
  // the collision locks BOTH people out rather than letting one impersonate the
  // other.
  //
  // This suite has no Neon, so what it can pin is the contract that matters:
  // an unanswerable duplicate check must REFUSE the hire, never assume the PIN
  // is free. Guessing "free" is the answer that creates the collision.
  const dup = await POST("createEmployee", { name: "New Guy", role: "employee", pin: "1184" });
  eq(dup.statusCode, 503, "an unanswerable duplicate check refuses the hire");
  ok(/nobody was added/i.test(json(dup).error), "and says plainly that nothing happened");
});

await test("rates: a rate write with Neon down fails CLOSED", async () => {
  // These numbers drive GP on every job the person has booked hours to. A rate
  // write that silently didn't land is worse than an error.
  mockTables = { Employees: [{ id: "recA", fields: { "Employee Name": "Larry Unruh" } }] };
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  ok((await POST("addEmployeeRaise", { employeeId: "recA", startDate: "2026-01-01", wage: 30, burdenPct: 25 })).statusCode >= 500, "raise fails closed");
  ok((await POST("correctEmployeeRate", { rateId: "app:recA:2026-01-01", wage: 30, burdenPct: 25 })).statusCode >= 500, "correction fails closed");
  eq((await GET("employeeRates", { employeeId: "recA" })).statusCode, 503, "reading rates says so rather than showing none");
  delete process.env.DATABASE_URL;
});

// ── Stage 4: the secondary employee reads move to Neon ──
// These handlers now read Neon first and fall back to Airtable. The contract
// worth pinning is the FALLBACK, because the failure mode is silent: a null
// from Neon must mean "ask Airtable", never "there are no employees". An empty
// employee list in a payroll rollup drops people from a pay period without
// erroring anywhere.
await test("payroll reads: unreachable Neon falls back, nobody vanishes", async () => {
  mockTables = {
    Employees: [
      { id: "recEmp1", fields: { "Employee Name": "Jeff Koehn", Role: "employee", Active: true } },
      { id: "recOff1", fields: { "Employee Name": "Tisha",      Role: "office",   Active: true } },
    ],
    "Time Entries": [], Bonuses: [], "Payroll Runs": [],
  };
  for (const url of [undefined, "not-a-valid-connection-string"]) {
    if (url) process.env.DATABASE_URL = url; else delete process.env.DATABASE_URL;
    for (const a of ["payrollBonusesRollup", "payrollHoursBreakdown"]) {
      const res = await GET(a, { today: "2026-08-09" });
      ok(res.statusCode < 500, `${a} survives (DATABASE_URL=${url}) — got ${res.statusCode}`);
    }
    // Office is not payroll-eligible and must stay refused on the fallback path
    // too — the role gate travels with the record, whichever store answered.
    //
    // Single-row fixture on purpose: the mock does NOT apply filterByFormula,
    // it returns every row for the table. With both employees present the
    // fallback's RECORD_ID() filter is a no-op and [0] is whoever is listed
    // first, so a two-row fixture would test the fixture, not the gate.
    const both = mockTables;
    mockTables = { ...both, Employees: [both.Employees[1]] };   // office only
    eq((await GET("myHoursRollup", { employeeId: "recOff1", today: "2026-08-09" })).statusCode, 403, "office refused");
    mockTables = { ...both, Employees: [] };
    eq((await GET("myHoursRollup", { employeeId: "recNope", today: "2026-08-09" })).statusCode, 404, "unknown employee 404s");
    mockTables = both;
  }
  delete process.env.DATABASE_URL;
});

await test("createVendor: gated by the signed token, not a body field", async () => {
  // The handler used to re-read the employee from Airtable and check their role
  // — weaker than it looked, since `employeeId` came from the request body and
  // any caller could send an admin's id. authzFor already gates this at
  // admin+office on the SIGNED token, so the guard is gone and employeeId is
  // ignored. It must still work when the client doesn't send one.
  mockTables = { Vendors: [], Employees: [] };
  eq((await POST("createVendor", { name: "New Vendor Co" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("createVendor", { name: "New Vendor Co" }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  // ⚠ 503, not 2xx, since cutover slice 1 (db/schema/053): the create is
  // Neon-FIRST now and this harness has no DATABASE_URL. What is being asserted
  // is that office got PAST the role gate — 403 would mean the gate broke.
  // An Airtable-only vendor would be invisible to the Neon-first picker that
  // created it, so refusing is the correct half-write-free answer.
  const officeRes = await POST("createVendor", { name: "New Vendor Co" }, OFFICE_TOK);
  eq(officeRes.statusCode, 503, "office is past the gate; no database is what stops it");
  ok(officeRes.statusCode !== 403, "office allowed with no employeeId");
});

await test("createCompany: same admin/office tier as createVendor, and name is required", async () => {
  // Companies had no create path at all until this shipped — two reads and
  // nothing else — while createJob REQUIRES a contractorId. So the gate here is
  // the gate on onboarding a new customer, not on a convenience field.
  mockTables = { Companies: [], Employees: [] };
  eq((await POST("createCompany", { name: "Newco Construction" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("createCompany", { name: "Newco Construction" }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  eq((await POST("createCompany", { name: "   " }, OFFICE_TOK)).statusCode, 400, "blank name rejected");

  // ⚠ CHANGED BY CUTOVER SLICE 1 (db/schema/053). The create is Neon-FIRST now,
  // so with no database it must refuse — writing Airtable alone would create a
  // company invisible to the Neon-first picker that asked for it, permanently,
  // because nothing back-fills this table. This harness has no DATABASE_URL, so
  // that is the branch it can reach, and "no POST to Companies was made" is the
  // assertion that matters.
  let posted = false;
  const before = lastFetch;
  const res = await POST("createCompany", { name: "Newco Construction" }, OFFICE_TOK);
  posted = lastFetch !== before && /Companies/.test(lastFetch?.url || "") &&
           (lastFetch?.opts?.method || "").toUpperCase() === "POST";
  eq(res.statusCode, 503, "no database → refuse, don't half-write");
  eq(posted, false, "and nothing reached Airtable");

  // The Airtable payload itself can no longer be asserted here, because the
  // mirror is unreachable without a database. Keep the field-id trap covered at
  // source instead: fldWzDYqRUShxXUKW is Active Contractor, NOT the sales-tax
  // checkbox — reading those two backwards is a mistake already made once on
  // Vendors, and it is silent when it happens.
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const fnSrc = readFileSync(fileURLToPath(new URL("../netlify/functions/airtable.js", import.meta.url)), "utf8");
  const body = fnSrc.slice(fnSrc.indexOf("async function handleCreateCompany"));
  ok(/fldWzDYqRUShxXUKW"\]\s*=\s*activeContractor !== false/.test(body),
     "Active Contractor still defaults on");
  ok(/fldA30AUOUbarysdp"\]\s*=\s*trimmedName/.test(body), "Company Name still written");
});

await test("estimateTemplates: no Airtable fallback — a frozen price never reaches a quote", async () => {
  // Templates went Neon-native on 2026-08-20 (db/schema/047) and the templates
  // block left `_billing-sync.js`, so Airtable's copy is frozen history. The
  // Airtable read path was DELETED rather than left as a fallback: a stale base
  // price loaded into a live customer estimate is wrong money with no error
  // anywhere, which is a worse failure than the modal saying "unavailable".
  //
  // This test is the guard on that decision. DATABASE_URL is unset in the
  // harness, so Neon is unreachable — and a fully-populated Airtable table is
  // planted to prove the handler does NOT reach for it.
  mockTables = {
    "Estimate Templates": [
      { id: "recSTALE", fields: { "Template Name": "Stale Template", "Active": true, "Base Price": 99999 } },
    ],
  };
  const r = await GET("estimateTemplates", { contractor: "Case Farms" });
  eq(r.statusCode, 503, "read fails closed with Neon down");
  ok(!String(r.body).includes("Stale Template"), "the frozen Airtable copy is NOT served");
  ok(!String(r.body).includes("99999"), "and neither is its price");

  eq((await GET("estimateTemplatesAll")).statusCode, 503, "the manager read fails closed too");

  // The manager read carries internal notes and archived templates — neither of
  // which the picker shows — so it sits at the tier of the writes it feeds.
  // The picker read stays open to any signed-in role.
  eq((await GET("estimateTemplatesAll", {}, EMP_TOK)).statusCode, 403, "employee can't list all templates");
  eq((await GET("estimateTemplatesAll", {}, VIEWER_TOK)).statusCode, 403, "viewer can't either");
  eq((await GET("estimateTemplates", { contractor: "X" }, EMP_TOK)).statusCode, 503,
     "but the picker read is NOT role-gated — it reaches the handler and fails on Neon, not on 403");
});

await test("estimateTemplateSave/Archive: admin+office, and validated before Neon is touched", async () => {
  // Templates carry the base price, labor hours and material cost that seed a
  // customer quote, so editing one is a back-office money op in the same tier as
  // updateJobBillableRate — NOT the _NON_VIEWER default a write would otherwise
  // fall to. Reads stay open to any signed-in role.
  mockTables = {};
  eq((await POST("estimateTemplateSave", { name: "T" }, EMP_TOK)).statusCode, 403, "employee refused");
  eq((await POST("estimateTemplateSave", { name: "T" }, VIEWER_TOK)).statusCode, 403, "viewer refused");
  eq((await POST("estimateTemplateArchive", { templateId: "recX" }, EMP_TOK)).statusCode, 403, "employee can't archive");
  eq((await POST("estimateTemplateArchive", { templateId: "recX" }, VIEWER_TOK)).statusCode, 403, "viewer can't archive");

  // Validation runs before any database call, so these answer 400 (a real
  // complaint about the input) rather than 502/503 (Neon is down). Getting that
  // order wrong tells someone their name was fine and the server broke.
  eq((await POST("estimateTemplateSave", { name: "   " }, OFFICE_TOK)).statusCode, 400, "blank name rejected");
  eq((await POST("estimateTemplateSave", {}, ADMIN_TOK)).statusCode, 400, "missing name rejected");
  eq((await POST("estimateTemplateArchive", { templateId: "  " }, ADMIN_TOK)).statusCode, 400, "archive needs a templateId");

  // Nothing here may write Airtable. The Estimate Templates table there is
  // frozen history now; a write would re-create the clobber problem from the
  // other direction and put the two copies back into a fight.
  const before = lastFetch;
  await POST("estimateTemplateSave", { name: "Neon Only Template" }, ADMIN_TOK);
  eq(lastFetch, before, "save issues NO Airtable request at all");
});

await test("createPowerCompany: the record has to reach Neon, not just Airtable", async () => {
  // getPowerCompanies went Neon-first in item 06 slice 4 while this write stayed
  // Airtable-only, and NOTHING else writes power_companies — no hourly sync, no
  // loader. A utility added here was invisible to the picker that created it,
  // permanently. Same bug the Companies flip had; found by re-measuring on 08-12.
  //
  // ⚠ INVERTED BY CUTOVER SLICE 1 (db/schema/053). The create is Neon-FIRST
  // now, so the old concern — "does it reach Neon?" — is answered by
  // construction, and the new question is the opposite one: with no database,
  // does it refuse rather than writing Airtable alone? An Airtable-only utility
  // is precisely the permanently-invisible row this test was written about.
  mockTables = { "Power Companies": [], Employees: [] };
  const before = lastFetch;
  const res = await POST("createPowerCompany", { name: "Ohio Edison", utilityRegion: "Northeast" }, OFFICE_TOK);
  eq(res.statusCode, 503, "no database → refuse");
  ok(!(lastFetch !== before && /tblgxHavdZybnuMhM/.test(lastFetch?.url || "")
       && (lastFetch?.opts?.method || "").toUpperCase() === "POST"),
     "and nothing reached Airtable — an Airtable-only utility is the bug this test exists for");
  eq((await POST("createPowerCompany", { name: "  " }, OFFICE_TOK)).statusCode, 400, "blank name still rejected first");

  // Field mapping kept covered at source, since the mirror is unreachable here.
  // fldFa3QqewblhWOID is Active — a utility that lands inactive is invisible.
  const { readFileSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const fnSrc = readFileSync(fileURLToPath(new URL("../netlify/functions/airtable.js", import.meta.url)), "utf8");
  const body = fnSrc.slice(fnSrc.indexOf("async function handleCreatePowerCompany"));
  ok(/fldj7HRiBvKNp9DpN"\]\s*=\s*trimmedName/.test(body), "name still written");
  ok(/fldFa3QqewblhWOID"\]\s*=\s*true/.test(body), "Active still forced on");
});

await test("createJob: an unknown contractor name omits the intake breadcrumb, not the job", async () => {
  // "Contractor (Intake)" is a singleSelect and the create POST has typecast
  // OFF, so sending a name that isn't a configured option 422s the WHOLE job.
  // Every contractor on file today happens to be an option, which is why this
  // never bit — but the first company added through createCompany would have
  // been unable to have a job created at all. The linked Contractor field is
  // the real data and must still be written.
  mockTables = { Jobs: [], Companies: [], Employees: [] };
  const fresh = await capturePostTo("Jobs", () =>
    POST("createJob", { jobName: "Watersedge 2", contractorId: "recNew1",
                        contractorName: "Newco Construction" }, OFFICE_TOK));
  ok(!("Contractor (Intake)" in fresh.fields), "unknown option omitted");
  eq(fresh.fields["Contractor"][0], "recNew1", "linked Contractor still written");

  // A known option must still come through, or this guard would quietly strip
  // the breadcrumb from every job Make still reads it on.
  const known = await capturePostTo("Jobs", () =>
    POST("createJob", { jobName: "Watersedge 3", contractorId: "recKnown1",
                        contractorName: "Ware Construction" }, OFFICE_TOK));
  eq(known.fields["Contractor (Intake)"], "Ware Construction", "known option kept");
});

await test("backfillTimeEntryEmployeeLinks is gone, not just unlisted", async () => {
  // Deleted in Stage 4 — it repaired the Airtable Time Entries table, which has
  // been a frozen historical copy since Step 3. An unknown action must 400.
  eq((await POST("backfillTimeEntryEmployeeLinks", { confirm: "YES" })).statusCode, 400, "unknown action");
});

await test("hours breakdowns: fall back cleanly when Neon is unreachable", async () => {
  // These two served the AIRTABLE Time Entries table, frozen by Step 3 on
  // 2026-08-07, while the rollup tiles above them served Neon — so the
  // drill-down disagreed with the tile it opened from, and drifted daily.
  // They are Neon-first now. The fallback still exists and still works; it
  // just reads a frozen table, which is why the log line says so out loud.
  mockTables = {
    Employees: [{ id: "recEmp1", fields: { "Employee Name": "Jeff Koehn", Role: "employee", Active: true } }],
    "Time Entries": [], "Payroll Runs": [],
  };
  for (const url of [undefined, "not-a-valid-connection-string"]) {
    if (url) process.env.DATABASE_URL = url; else delete process.env.DATABASE_URL;
    const a = await GET("payrollHoursBreakdown", { bucket: "ytd", today: "2026-08-09" });
    eq(a.statusCode, 200, `admin breakdown answers (DATABASE_URL=${url})`);
    ok(Array.isArray(json(a).employees), "employees array present");
    const m = await GET("myHoursBreakdown", { employeeId: "recEmp1", bucket: "ytd", today: "2026-08-09" });
    eq(m.statusCode, 200, `my breakdown answers (DATABASE_URL=${url})`);
    ok(Array.isArray(json(m).entries), "entries array present");
  }
  delete process.env.DATABASE_URL;
  // Bucket validation must survive ahead of either data path.
  eq((await GET("payrollHoursBreakdown", { bucket: "lastTuesday" })).statusCode, 400, "bad bucket refused");
  eq((await GET("myHoursBreakdown", { employeeId: "recEmp1", bucket: "nope" })).statusCode, 400, "bad bucket refused");
});

await test("payroll bonuses: the Airtable fallback still works, and still excludes superseded runs", async () => {
  // Both handlers went Neon-first (audit item 02, second slice). The money used
  // to come from Airtable while the employee list already came from Neon, which
  // is what made a handler count call them "migrated".
  //
  // These tests run offline, so DATABASE_URL is never reachable — which means
  // this exercises exactly the branch a Neon outage takes. The restructure moved
  // the two fetchAll calls inside an `else`, so a mistake there would be
  // invisible in production until the day Neon blinked.
  //
  // ⚠ Superseded runs are the whole reason this logic is not a SUM. The period
  // 2026-07-26 → 08-08 has six runs, five superseded; counting them all would
  // multiply somebody's YTD bonus by six.
  mockTables = {
    Employees: [
      { id: "recEmp1", fields: { "Employee Name": "Jeff Koehn",  Role: "employee", Active: true } },
      { id: "recEmp2", fields: { "Employee Name": "Dana Office", Role: "office",   Active: true } },
    ],
    tbln9nU1BtFmTYMYB: [
      { id: "recRunLive", fields: { "Superseded": false, "Generated At": "2026-05-01T13:41:42.000Z" } },
      { id: "recRunDead", fields: { "Superseded": true,  "Generated At": "2026-05-01T13:44:02.000Z" } },
    ],
    tblpE3emzU3J1P5jx: [
      { id: "recBon1", fields: { Amount: 500, Employee: ["recEmp1"], "Payroll Run": ["recRunLive"],
                                 "Pay Period Start": "2026-01-25", "Pay Period End": "2026-02-07" } },
      { id: "recBon2", fields: { Amount: 900, Employee: ["recEmp1"], "Payroll Run": ["recRunDead"],
                                 "Pay Period Start": "2026-01-25", "Pay Period End": "2026-02-07" } },
      { id: "recBon3", fields: { Amount: 250, Employee: ["recEmp2"], "Payroll Run": ["recRunLive"],
                                 "Pay Period Start": "2026-01-25", "Pay Period End": "2026-02-07" } },
    ],
  };
  delete process.env.DATABASE_URL;

  const roll = await GET("payrollBonusesRollup", { year: "2026" });
  eq(roll.statusCode, 200, "rollup answers with Neon down");
  eq(json(roll)._source, "airtable", "and says which store answered");
  const emps = json(roll).employees;
  const jeff = emps.find(e => e.id === "recEmp1");
  ok(jeff, "the payroll-eligible employee is present");
  eq(jeff.ytdBonus, 500, "the superseded run's 900 is excluded");
  eq(emps.some(e => e.id === "recEmp2"), false, "office never appears in a payroll view");

  const hist = await GET("payrollEmployeeBonusHistory", { employeeId: "recEmp1", limit: "5" });
  eq(hist.statusCode, 200, "history answers with Neon down");
  const bonuses = json(hist).bonuses;
  eq(bonuses.length, 1, "only the live run's bonus survives");
  eq(bonuses[0].id, "recBon1", "and it is the right one");
  eq(bonuses[0].runGeneratedAt, "2026-05-01T13:41:42.000Z", "run timestamp comes through");

  // The role guard sits ahead of both data paths — an office id must never
  // return bonus history, whichever store is answering.
  //
  // ⚠ The mock returns every row of a table and ignores `filterByFormula`, so a
  // by-record-id lookup here resolves to whatever sits FIRST in the array, not
  // to the id asked for. Isolating the office employee is what actually puts the
  // office record in front of the guard — leaving both in place tests nothing
  // and quietly passes for the wrong reason.
  mockTables.Employees = [{ id: "recEmp2", fields: { "Employee Name": "Dana Office", Role: "office", Active: true } }];
  const leak = await GET("payrollEmployeeBonusHistory", { employeeId: "recEmp2" });
  eq(leak.statusCode, 200, "office id answers");
  eq(json(leak).bonuses.length, 0, "but with nothing in it");

  eq((await GET("payrollEmployeeBonusHistory", { employeeId: "notARecordId" })).statusCode, 400,
     "a malformed employeeId is refused before either store is touched");
});

await test("job automation callback: a token signed for one job unlocks only that job", async () => {
  // This endpoint skips the bearer check because Make.com has no session, so the
  // scope token IS the security model — these rejections are the whole of it.
  // Every call below deliberately passes NO auth header (third arg null): if one
  // of them ever succeeds, the endpoint is open to the internet.
  const forJob1 = signScope(["jobAutomation", "recJob1"]);

  eq((await POST("jobAutomationResult", { recordId: "recJob1" }, null)).statusCode, 403,
     "no token at all");
  eq((await POST("jobAutomationResult", { recordId: "recJob1", token: "not-a-token" }, null)).statusCode, 403,
     "a junk token");
  // The point of scoping: a leaked token cannot be aimed at another record.
  eq((await POST("jobAutomationResult", { recordId: "recJob2", token: forJob1 }, null)).statusCode, 403,
     "a valid token belonging to a DIFFERENT job");

  // Shape checks run before the token, so a caller learns nothing from them.
  eq((await POST("jobAutomationResult", { token: forJob1 }, null)).statusCode, 400, "no recordId");
  eq((await POST("jobAutomationResult", { recordId: "notarec", token: forJob1 }, null)).statusCode, 400,
     "recordId that is not a record id");
});

await test("contacts: the picker can reach someone filed under another company", async () => {
  // The real case: a customer first entered under a GC rings up directly, and
  // the new job goes under Misc Jobs. Before `otherContacts` his details were
  // invisible, and the only way forward was retyping them — which creates a
  // SECOND record for one person, after which his phone number is wrong in one
  // of the two places.
  mockTables = {
    tbl7vZpySDNfZX9Sq: [
      { id: "recC1", fields: { "First Name": "Craig", "Last Name": "Davidson", "Company": ["recKoehn"],
                               "Active": true, "Primary Phone": "330-555-0100", "Role": ["Customer"] } },
      { id: "recC2", fields: { "First Name": "Dave",  "Last Name": "Miller",   "Company": ["recMisc"],
                               "Active": true } },
      { id: "recC3", fields: { "First Name": "Gone",  "Last Name": "Away",     "Company": ["recKoehn"],
                               "Active": false } },
    ],
  };
  delete process.env.DATABASE_URL;

  const res = await GET("listContactsByCompany", { companyId: "recMisc" });
  eq(res.statusCode, 200, "answers");
  const b = json(res);
  eq(b.contacts.length, 1, "the main list is still only the selected company's own");
  eq(b.contacts[0].id, "recC2", "and it is the right one");

  const other = b.otherContacts.map(c => c.id);
  ok(other.includes("recC1"),  "Craig is reachable from the other-companies list");
  ok(!other.includes("recC2"), "the selected company's own contact is not repeated in it");
  ok(!other.includes("recC3"), "an inactive contact is offered in neither list");

  // Company names come from Neon, which is unreachable here. The row must still
  // be OFFERED — losing the label is acceptable, losing the contact is not.
  const craig = b.otherContacts.find(c => c.id === "recC1");
  eq(craig.companyId, "recKoehn", "the company id still comes through");
  eq(craig.companyName, "", "and the name degrades to empty instead of failing the read");
  eq(craig.primaryPhone, "330-555-0100", "details ride along so the form can prefill from them");

  eq((await GET("listContactsByCompany", {})).statusCode, 400, "companyId is still required");
});

await test("people: renders off Airtable when Neon is unavailable", async () => {
  // Fail-soft read. A roster missing hire dates beats an error page.
  mockTables = { Employees: [
    { id: "recE9", fields: {
      "Employee Name": "Pat Gingerich", "Username": "pat", "Role": "employee",
      "Role New": "employee", "Active": true, "Primary Email": "pat@example.com",
      "Current True Cost Rate": 42.5,
    } },
  ] };
  const b = json(await GET("people"));
  eq(b.ok, true, "still answers");
  eq(b.neonOk, false, "and admits the Neon half is missing rather than showing blanks as fact");
  eq(b.people.length, 1, "one person");
  eq(b.people[0].name, "Pat Gingerich", "name");
  eq(b.people[0].active, true, "active flag");
  eq(b.people[0].currentRate, 42.5, "rate off the live Airtable rollup");
  // "Primary Email" — NOT "Email". F.emp.email names a column that does not
  // exist, which is why login-by-email has never worked (plan problem 3).
  eq(b.people[0].email, "pat@example.com", "reads the real email column");
  eq(b.people[0].hiredOn, null, "Neon-owned column is null, not invented");
});

// ── TIME CLOCK (docs/PLAN-time-clock.md, db/schema/018_time_clock.sql) ──────
// The clock ships INERT and stays that way until two separate env switches are
// thrown. These cases exist mostly to prove the OFF state really is off: while
// QuickBooks Time is still the book of record, a punch that reaches payroll by
// accident is double-paid hours, which is the one failure here that costs money.
//
// Note what these can and can't cover. There is no Neon in this harness, so the
// SQL itself is exercised against a real branch by hand (PREPARE + a full punch
// cycle) — offline tests die at the connection and would pass over broken SQL.
// What IS covered here is every decision made before the database is touched:
// the switches, the roles, and the punch validation.

await test("clock: ships INERT — TIME_CLOCK unset means the clock doesn't exist", async () => {
  delete process.env.TIME_CLOCK;
  const b = json(await GET("clockStatus", {}, EMP_TOK));
  eq(b.ok, true, "still answers cleanly");
  eq(b.enabled, false, "but reports itself off");
  eq(b.open, null, "and nobody is on a clock that isn't running");
});

await test("clock: punching while switched off → 403, not a silent no-op", async () => {
  delete process.env.TIME_CLOCK;
  const r = await POST("clockIn", { clientPunchId: "p1", startedAt: new Date().toISOString() }, EMP_TOK);
  eq(r.statusCode, 403, "clockIn refused");
  const r2 = await POST("clockOut", { clientPunchId: "p1", endedAt: new Date().toISOString() }, EMP_TOK);
  eq(r2.statusCode, 403, "clockOut refused");
});

await test("clock: TIME_CLOCK=admin keeps the crew out while it's being built", async () => {
  process.env.TIME_CLOCK = "admin";
  const emp = await POST("clockIn", { clientPunchId: "p2", startedAt: new Date().toISOString() }, EMP_TOK);
  eq(emp.statusCode, 403, "an employee cannot punch yet");
  const empStatus = json(await GET("clockStatus", {}, EMP_TOK));
  eq(empStatus.enabled, false, "and isn't shown a clock they can't use");
  // Admin gets PAST the gate — it fails later, on the absent Neon employee row,
  // which is exactly how far this harness can follow it.
  const adm = await POST("clockIn", { clientPunchId: "p3", startedAt: new Date().toISOString() }, ADMIN_TOK);
  ok(adm.statusCode !== 403, "admin is past the audience gate");
  delete process.env.TIME_CLOCK;
});

await test("clock: office and viewer are not payroll roles — 403 from authzFor", async () => {
  process.env.TIME_CLOCK = "on";
  eq((await GET("clockStatus", {}, OFFICE_TOK)).statusCode, 403, "office has no hours to read");
  eq((await GET("clockStatus", {}, VIEWER_TOK)).statusCode, 403, "viewer is read-only");
  eq((await POST("clockIn", { clientPunchId: "p4" }, VIEWER_TOK)).statusCode, 403, "viewer cannot punch");
  delete process.env.TIME_CLOCK;
});

await test("clock: a punch with no clientPunchId is refused — replay safety is not optional", async () => {
  process.env.TIME_CLOCK = "on";
  const r = await POST("clockIn", { startedAt: new Date().toISOString() }, EMP_TOK);
  eq(r.statusCode, 400, "rejected");
  ok(/clientPunchId/.test(json(r).error), "and says why");
  delete process.env.TIME_CLOCK;
});

await test("clock: a phone with a wrong clock can't file hours into a closed pay period", async () => {
  process.env.TIME_CLOCK = "on";
  // The punch timestamp is deliberately trusted from the CLIENT (that is what
  // makes offline replay honest), so the ±36h window is the only thing standing
  // between a device with a wrong year and someone else's payroll.
  const lastYear = new Date(Date.now() - 400 * 24 * 3600 * 1000).toISOString();
  const r = await POST("clockIn", { clientPunchId: "p5", startedAt: lastYear }, EMP_TOK);
  eq(r.statusCode, 400, "out-of-range punch refused");
  const nonsense = await POST("clockIn", { clientPunchId: "p6", startedAt: "not-a-date" }, EMP_TOK);
  eq(nonsense.statusCode, 400, "unparseable punch refused");
  // An overnight replay a few hours late is NORMAL and must still be accepted.
  // Asserted on the MESSAGE, not the status: with no Neon in this harness the
  // punch still 400s a step later on the employee lookup, so a status check here
  // would pass for the wrong reason and keep passing if the window ever narrowed.
  const lateButFine = new Date(Date.now() - 10 * 3600 * 1000).toISOString();
  const okPunch = json(await POST("clockIn", { clientPunchId: "p7", startedAt: lateButFine }, EMP_TOK));
  ok(!/out-of-range/.test(okPunch.error || ""), "a genuinely late replay is not rejected as out of range");
  delete process.env.TIME_CLOCK;
});

await test("pto: requesting is self-service, approving and allowances are admin", async () => {
  // Asking for time off is a _PAYROLL self-write; granting it creates PAID HOURS,
  // so it sits at _ADMIN. Office is excluded from both — they manage money, not
  // people's leave.
  eq((await POST("requestPto", { startDate: "2026-09-14", endDate: "2026-09-18" }, VIEWER_TOK)).statusCode, 403,
     "viewers have no leave to request");
  eq((await POST("decidePtoRequest", { requestId: "x", approve: true }, EMP_TOK)).statusCode, 403,
     "an employee cannot approve their own time off");
  eq((await POST("decidePtoRequest", { requestId: "x", approve: true }, OFFICE_TOK)).statusCode, 403,
     "nor can office");
  eq((await POST("setPtoAllowance", { employeeId: "recE9", allowanceHours: 80 }, EMP_TOK)).statusCode, 403,
     "nor set their own allowance");
  eq((await GET("ptoRequests", {}, EMP_TOK)).statusCode, 403, "the queue is admin-only");
  eq((await GET("ptoRequests", {}, OFFICE_TOK)).statusCode, 403, "including for office");
  // ...but reading YOUR OWN balance is not admin-gated.
  ok((await GET("ptoBalance", {}, EMP_TOK)).statusCode !== 403, "employees can read their own balance");
});

await test("pto: bulk actions are admin-only and refuse to run blind", async () => {
  eq((await POST("fillHolidays", { from: "2026-08-09", confirm: "YES" }, EMP_TOK)).statusCode, 403,
     "employees can't fill holidays");
  eq((await POST("ptoRollover", { toYear: 2027, confirm: "YES" }, OFFICE_TOK)).statusCode, 403,
     "nor can office roll the year over");

  // ⚠ The `from` date is REQUIRED, and this is the guard that matters: three of
  // 2026's holidays had already been paid through QuickBooks, so a "fill
  // everything" button would pay them a second time.
  const noFrom = await POST("fillHolidays", { confirm: "YES" }, ADMIN_TOK);
  eq(noFrom.statusCode, 400, "no blanket fill");
  ok(/QuickBooks/.test(json(noFrom).error), "and it says why the date is needed");

  eq((await POST("fillHolidays", { from: "2026-08-09" }, ADMIN_TOK)).statusCode, 400,
     "and it still wants confirmation");
  eq((await POST("ptoRollover", { toYear: 2027 }, ADMIN_TOK)).statusCode, 400,
     "so does the rollover");
});

await test("pto: a request has to make sense before it reaches the queue", async () => {
  eq((await POST("requestPto", { startDate: "2026-09-18", endDate: "2026-09-14" }, EMP_TOK)).statusCode, 400,
     "end before start is refused");
  eq((await POST("requestPto", { startDate: "nope" }, EMP_TOK)).statusCode, 400, "garbage dates refused");
  const badHrs = await POST("requestPto",
    { startDate: "2026-09-14", endDate: "2026-09-14", hoursPerDay: 30 }, EMP_TOK);
  eq(badHrs.statusCode, 400, "a 30-hour day is refused");
  // Allowances are bounded too — a typo'd 8000 would silently grant four years off.
  eq((await POST("setPtoAllowance", { employeeId: "recE9", allowanceHours: 8000 }, ADMIN_TOK)).statusCode, 400,
     "an absurd allowance is refused");
});

await test("clock delete + reconcile: gated like everything else", async () => {
  delete process.env.TIME_CLOCK;
  eq((await POST("clockDeletePunch", { punchId: "x" }, EMP_TOK)).statusCode, 403,
     "no deleting while the clock is off");
  process.env.TIME_CLOCK = "on";
  eq((await POST("clockDeletePunch", {}, EMP_TOK)).statusCode, 400, "and it needs a punch");
  eq((await POST("clockDeletePunch", { punchId: "x" }, VIEWER_TOK)).statusCode, 403,
     "viewers have no punches to delete");
  // Reconciliation reads everyone's hours across both systems — strict admin,
  // same tier as the roster.
  eq((await GET("clockReconcile", { from: "2026-08-01", to: "2026-08-08" }, OFFICE_TOK)).statusCode, 403,
     "office can't read it");
  eq((await GET("clockReconcile", { from: "2026-08-01", to: "2026-08-08" }, EMP_TOK)).statusCode, 403,
     "nor employees");
  const noRange = await GET("clockReconcile", {}, ADMIN_TOK);
  eq(noRange.statusCode, 400, "admin must give a date range");
  delete process.env.TIME_CLOCK;
});

await test("clock edit: a corrected city tax is whitelisted too", async () => {
  process.env.TIME_CLOCK = "on";
  // Same guard as the per-job setting: an unrecognised string would be written
  // verbatim into payroll and degrade silently to "A No Tax".
  const bad = await POST("clockEditTimes",
    { punchId: "00000000-0000-0000-0000-000000000000", cityTaxes: "Massillon Tax" }, EMP_TOK);
  eq(bad.statusCode, 400, "the correct spelling is still the wrong data");
  ok(/Unknown city tax/.test(json(bad).error), "and says so");
  delete process.env.TIME_CLOCK;
});

await test("clock edit: bounded, and refused outright when the clock is off", async () => {
  delete process.env.TIME_CLOCK;
  eq((await POST("clockEditTimes", { startedAt: new Date().toISOString() }, EMP_TOK)).statusCode, 403,
     "no edits while the clock is off");

  process.env.TIME_CLOCK = "on";
  eq((await POST("clockEditTimes", {}, EMP_TOK)).statusCode, 400, "an empty edit is refused");

  // A correction is made deliberately at a keyboard, so it gets a wider berth than
  // a punch replayed from a phone — but it is still bounded at both ends.
  const future = await POST("clockEditTimes",
    { startedAt: new Date(Date.now() + 3 * 3600 * 1000).toISOString() }, EMP_TOK);
  eq(future.statusCode, 400, "you cannot start a shift in the future");
  ok(/future/.test(json(future).error), "and it says why");

  const ancient = await POST("clockEditTimes",
    { startedAt: new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString() }, EMP_TOK);
  eq(ancient.statusCode, 400, "nor reach back into a closed pay period");
  ok(/Payroll/.test(json(ancient).error), "and it points at where that belongs");

  eq((await POST("clockEditTimes", { startedAt: "not-a-time" }, EMP_TOK)).statusCode, 400,
     "garbage is refused");
  // The realistic case — "I got here an hour before I punched" — must NOT be
  // rejected by the bounds. It fails later on the absent Neon shift, which is as
  // far as this harness can follow it.
  const realistic = json(await POST("clockEditTimes",
    { startedAt: new Date(Date.now() - 3600 * 1000).toISOString() }, EMP_TOK));
  ok(!/future|Payroll|valid/.test(realistic.error || ""),
     "an hour ago is a normal correction, not a bounds violation");
  delete process.env.TIME_CLOCK;
});

await test("job city tax: admin+office may set it, the crew may not, and the value is whitelisted", async () => {
  eq((await POST("updateJobCityTax", { jobId: "recJ1", cityTax: "Canton Tax" }, EMP_TOK)).statusCode, 403,
     "an employee cannot change what a job is taxed at");
  eq((await POST("updateJobCityTax", { jobId: "recJ1", cityTax: "Canton Tax" }, VIEWER_TOK)).statusCode, 403,
     "nor a viewer");
  eq((await POST("updateJobCityTax", { cityTax: "Canton Tax" }, ADMIN_TOK)).statusCode, 400,
     "a write with no job is refused");
  // ⚠ The whitelist is the guard that matters. These strings are written verbatim
  // into a time entry's free-text city_taxes, and anything QuickBooks doesn't
  // recognise silently degrades to "A No Tax" — the exact failure this feature
  // exists to prevent. A typo must fail loudly here, not quietly downstream.
  const bad = await POST("updateJobCityTax", { jobId: "recJ1", cityTax: "Massillon Tax" }, ADMIN_TOK);
  eq(bad.statusCode, 400, "the CORRECT spelling of Massillon is rejected — QB stores 'Massilon Tax'");
  ok(/Unknown city tax/.test(json(bad).error), "and says so plainly");
});

await test("clock: the roster and punching others are STRICT admin — office is out", async () => {
  process.env.TIME_CLOCK = "on";
  // clockRoster shows where every person is and backs a screen that starts and
  // stops paid time, so it sits with `people` in _ADMIN, not admin+office.
  eq((await GET("clockRoster", {}, OFFICE_TOK)).statusCode, 403, "office can't see the roster");
  eq((await GET("clockRoster", {}, EMP_TOK)).statusCode, 403, "employees can't see the roster");
  eq((await POST("adminClockIn", { employeeId: "recE9" }, EMP_TOK)).statusCode, 403,
     "an employee cannot punch somebody else in");
  eq((await POST("adminClockOut", { employeeId: "recE9" }, OFFICE_TOK)).statusCode, 403,
     "nor can office punch anyone out");
  // Admin gets through the tier, and is refused for a MISSING person rather than
  // being allowed to punch a blank one.
  const noWho = await POST("adminClockIn", {}, ADMIN_TOK);
  eq(noWho.statusCode, 400, "admin still has to say who");
  delete process.env.TIME_CLOCK;
});

await test("job clock visibility: admin+office, whitelisted, and clearable", async () => {
  eq((await POST("updateJobClockVisibility", { jobId: "recJ1", visibility: "all" }, EMP_TOK)).statusCode, 403,
     "the crew can't decide which jobs they're offered");
  eq((await POST("updateJobClockVisibility", { visibility: "all" }, ADMIN_TOK)).statusCode, 400,
     "needs a job");
  const bad = await POST("updateJobClockVisibility", { jobId: "recJ1", visibility: "sometimes" }, ADMIN_TOK);
  eq(bad.statusCode, 400, "an unknown value is refused");
  ok(/Unknown clock visibility/.test(json(bad).error), "and says so");
});

await test("widget: the unauthenticated endpoint refuses everything it should", async () => {
  // ⚠ This is the ONE action that skips the bearer check, so its refusals matter
  // more than most. It must never leak whether a person or a token exists.
  const noArgs = await GET("clockWidget", {}, null);
  eq(noArgs.statusCode, 200, "answers 200 so a widget host doesn't show an error box");
  eq(json(noArgs).ok, false, "but refuses");
  eq(json(noArgs).state, "error", "with a state a widget can render");

  const forged = await GET("clockWidget", { e: "recE9", t: "999999999999.abcdef" }, null);
  eq(json(forged).ok, false, "a forged signature is refused");
  // Identical shape for "no such person" and "bad token" — otherwise this becomes
  // an oracle for probing which employee ids exist.
  const unknown = await GET("clockWidget", { e: "recNOPE", t: "999999999999.abcdef" }, null);
  eq(JSON.stringify(json(unknown)), JSON.stringify(json(forged)),
     "unknown person and bad token are indistinguishable");
});

await test("widget: minting a link is self-service and never for someone else", async () => {
  eq((await POST("widgetLink", {}, VIEWER_TOK)).statusCode, 403,
     "viewers have no clock, so no widget");
  // There is deliberately NO employeeId parameter — the person comes from the
  // token — so passing one must not change who the link is for.
  const r = await POST("widgetLink", { employeeId: "recSOMEONEELSE" }, EMP_TOK);
  ok(!/recSOMEONEELSE/.test(JSON.stringify(json(r))),
     "a supplied employeeId is ignored, not honoured");
});

await test("clock switch: gated, and validated before it touches anything", async () => {
  delete process.env.TIME_CLOCK;
  eq((await POST("clockSwitch", { class: "Contract", clientPunchId: "s1" }, EMP_TOK)).statusCode, 403,
     "no switching while the clock is off");

  process.env.TIME_CLOCK = "on";
  eq((await POST("clockSwitch", { clientPunchId: "s1" }, EMP_TOK)).statusCode, 400,
     "it needs to know what you're switching to");
  eq((await POST("clockSwitch", { class: "Contract" }, EMP_TOK)).statusCode, 400,
     "and a replay key, like every other punch");
  // Switching sets the city tax on the new segment, so it gets the same whitelist
  // guard as everywhere else — a bad value would land verbatim in payroll.
  const bad = await POST("clockSwitch",
    { class: "Contract", clientPunchId: "s2", cityTaxes: "Massillon Tax" }, EMP_TOK);
  eq(bad.statusCode, 400, "the correct spelling is still the wrong data");
  ok(/Unknown city tax/.test(json(bad).error), "and says so");
  delete process.env.TIME_CLOCK;
});

await test("clock: breaks obey the same switch and the same roles as punching", async () => {
  delete process.env.TIME_CLOCK;
  eq((await POST("clockBreak", { start: true, at: new Date().toISOString() }, EMP_TOK)).statusCode, 403,
     "no breaks while the clock is off");
  process.env.TIME_CLOCK = "on";
  eq((await POST("clockBreak", { start: true, at: new Date().toISOString() }, VIEWER_TOK)).statusCode, 403,
     "viewers can't take a break they can't be on");
  // A break carries a client timestamp for the same reason a punch does — it may
  // replay late — so it gets the same ±36h sanity window.
  const skewed = await POST("clockBreak",
    { start: true, at: new Date(Date.now() - 400 * 24 * 3600 * 1000).toISOString() }, EMP_TOK);
  eq(skewed.statusCode, 400, "an out-of-range break time is refused");
  delete process.env.TIME_CLOCK;
});

await test("clock: punches can't become payroll hours while TIME_CLOCK_PAYROLL is off", async () => {
  delete process.env.TIME_CLOCK_PAYROLL;
  const r = await POST("promoteClockPunches", { confirm: "YES" }, ADMIN_TOK);
  eq(r.statusCode, 400, "promotion refused outright");
  ok(/TIME_CLOCK_PAYROLL/.test(json(r).error), "and names the switch that's holding it");
});

await test("clock: promoting punches into payroll is admin-only, and needs confirmation", async () => {
  eq((await POST("promoteClockPunches", { confirm: "YES" }, EMP_TOK)).statusCode, 403, "employee cannot");
  eq((await POST("promoteClockPunches", { confirm: "YES" }, OFFICE_TOK)).statusCode, 403, "office cannot");
  process.env.TIME_CLOCK_PAYROLL = "on";
  const noConfirm = await POST("promoteClockPunches", {}, ADMIN_TOK);
  eq(noConfirm.statusCode, 400, "admin still has to say YES");
  delete process.env.TIME_CLOCK_PAYROLL;
});

// ── generator service calls (replaces Airtable automation wfledvx1A8oVscWla) ──
// This action CREATES JOBS, and every job it creates burns a PO number that
// cannot be handed back. So the two things worth pinning are that only an admin
// can reach it, and that it does nothing at all until somebody deliberately
// turns it on.
await test("generator service calls: creating jobs unattended is strict admin", async () => {
  eq((await POST("generatorServiceCheck", {}, EMP_TOK)).statusCode, 403, "the crew cannot open service calls");
  eq((await POST("generatorServiceCheck", {}, OFFICE_TOK)).statusCode, 403, "nor can office — this mints PO numbers");
  eq((await POST("generatorServiceCheck", {}, VIEWER_TOK)).statusCode, 403, "and certainly not a viewer");
});

await test("generator service calls: ship INERT — no switch, no jobs", async () => {
  delete process.env.GENERATOR_SERVICE_CALLS;
  const res = await POST("generatorServiceCheck", {}, ADMIN_TOK);
  eq(res.statusCode, 200, "an off switch is the normal state, not an error");
  const b = json(res);
  eq(b.enabled, false, "reports itself off");
  eq(b.created, 0, "and creates nothing");
});

await test("generator service calls: GENERATOR_SERVICE_CALLS=on still needs Neon", async () => {
  // DATABASE_URL is unset in this harness, and the whole check is a Neon read —
  // v_generators computes service_status. It must say so rather than silently
  // reporting "nothing due", which would look identical to a healthy fleet.
  process.env.GENERATOR_SERVICE_CALLS = "on";
  const res = await POST("generatorServiceCheck", {}, ADMIN_TOK);
  eq(res.statusCode, 500, "no database is a real failure, not a quiet zero");
  ok(/DATABASE_URL/.test(json(res).error || ""), "and it names what is missing");
  eq(json(res).created, 0, "nothing was created");
  delete process.env.GENERATOR_SERVICE_CALLS;
});

await test("generator service calls: a dry run never depends on the switch", async () => {
  // The dry run is how the owner decides whether to flip the switch at all, so
  // it has to work while the switch is still off.
  delete process.env.GENERATOR_SERVICE_CALLS;
  const res = await POST("generatorServiceCheck", { dryRun: true }, ADMIN_TOK);
  ok(res.statusCode !== 200 || json(res).enabled !== false,
     "a dry run runs the check rather than short-circuiting on the switch");
});

// ── payroll archive → R2 (audit item 04, db/schema/052) ─────────────────────
await test("payroll archive: copying every payroll PDF in the company is strict admin", async () => {
  eq((await POST("copyPayrollFilesToR2", {}, EMP_TOK)).statusCode, 403, "the crew cannot");
  eq((await POST("copyPayrollFilesToR2", {}, OFFICE_TOK)).statusCode, 403,
     "nor office — payroll is the one tier office does not get");
  eq((await POST("copyPayrollFilesToR2", {}, VIEWER_TOK)).statusCode, 403, "nor a viewer");
});

await test("payrollRunsList: a run with no pdf_key sends the WHOLE list back to Airtable", async () => {
  // The half of this that matters. Until the backfill has run, some runs have
  // their PDF only in Airtable — and a payroll archive that lists a run you
  // cannot open is worse than a slower page. Neon is unreachable here, which is
  // the same branch, and the grid must come back whole and openable.
  mockTables = {
    "tbln9nU1BtFmTYMYB": [
      { id: "recR1", fields: {
        "Pay Period Start": "2026-07-26", "Pay Period End": "2026-08-08",
        "Generated At": "2026-08-09T12:00:00.000Z", "Total Hours": 812, "Total Bonus": 0,
        "PDF": [{ url: "https://airtable.example/p.pdf", filename: "NEE_Payroll.pdf" }],
      } },
    ],
  };
  const res = await GET("payrollRunsList", {}, ADMIN_TOK);
  eq(res.statusCode, 200, "the grid still loads");
  const b = json(res);
  eq(b.runs.length, 1, "and still has the run");
  eq(b.runs[0].pdfAvailable, true, "with a working link");
  eq(b._source, undefined, "served from Airtable, not Neon");
});

// ── payrollRunCreate, reversed (cutover slice 2, 2026-08-24) ────────────────
// The handler is Neon-first now, and its refusals changed shape with it. Both
// cases below pin the SAME property: a payroll run either lands completely or
// not at all — there is no half-record. That is what made slice 3's `$5` bug
// survivable, and it is the only guarantee this offline harness can check,
// since the SQL itself needs live Neon (verified there with `PREPARE name AS`,
// no type list).
const PR_RUN_BODY = {
  payPeriodStart: "2026-08-09", payPeriodEnd: "2026-08-22",
  generatedBy: "Rick Unruh", totalHours: 289.25, totalBonus: 0,
  pdfBase64: Buffer.from("%PDF-1.4 fake").toString("base64"),  pdfFilename: "NEE_Payroll.pdf",
  jsonBase64: Buffer.from(JSON.stringify({ employees: [] })).toString("base64"), jsonFilename: "NEE_Payroll.json",
  bonuses: [],
};

await test("payrollRunCreate: R2 unconfigured is a REFUSAL now, not a warning", async () => {
  // ⚠ THIS INVERTED WITH THE FLIP. While the run was Airtable-first the PDF was
  // also an Airtable attachment, so a missing R2 was reported in `r2Error` and
  // cost nothing. A native run has no Airtable record, so R2 holds the ONLY
  // copy of the artifact people are paid from — an unconfigured store has to
  // stop the request before anything is written, not after.
  clearR2();
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  const res = await POST("payrollRunCreate", PR_RUN_BODY);
  eq(res.statusCode, 503, "refused up front");
  ok(/not configured/i.test(json(res).error), "and says why");
  ok(/nothing was saved/i.test(json(res).error), "and says nothing was saved");
  // The whole point of checking BEFORE the insert: no run to unwind, and no
  // orphan left in Airtable either.
  eq(lastFetch, null, "not one Airtable call was made");
});

await test("payrollRunCreate: no Neon, no run — and no orphan record in Airtable", async () => {
  // The run is born in Neon, so an unreachable database must fail the request
  // outright. The half-record this guards against is the expensive one: an
  // Airtable Payroll Run with a PDF attached that no payroll screen can see,
  // because every read resolves runs from Neon now.
  setR2();
  delete process.env.DATABASE_URL;
  mockTables = {};
  lastFetch = null;
  const res = await POST("payrollRunCreate", PR_RUN_BODY);
  ok(res.statusCode >= 500, `fails closed, got ${res.statusCode}`);
  eq(lastFetch, null, "and never created the Airtable run it can no longer track");
  clearR2();
});

await test("payrollRunCreate: still validates its payload before any of that", async () => {
  // Unchanged by the flip, and worth pinning because the R2 refusal now sits
  // very close to the front of the handler — a missing PDF must still be a 400
  // about the missing PDF, not a 503 about the store.
  setR2();
  const { pdfBase64, ...noPdf } = PR_RUN_BODY;
  const res = await POST("payrollRunCreate", noPdf);
  eq(res.statusCode, 400, "bad request");
  ok(/pdfBase64/.test(json(res).error), "names the missing field");
  clearR2();
});

// ── the shared job-id gate (jobExists) ──────────────────────────────────────
// Ten handlers used to each fetch the whole Jobs record just to check it
// existed. They now share one helper that asks Neon first and only re-asks
// Airtable when Neon says no. DATABASE_URL is unset here, so these run the
// FALLBACK path — which is the half worth pinning, because it is what protects
// photos when Neon is down or a job's create-time insert failed.
await test("job id gate: an unknown job is still refused by every handler that shares it", async () => {
  setR2();
  const GATED_GETS = ["jobPhotos", "jobPhotosDeleted", "jobDocs", "jobPrints", "jobPrintsDeleted"];

  // ⚠ The Airtable mock ignores filterByFormula and hands back whatever is in
  // `mockTables`, so "unknown job" has to be modelled as an EMPTY Jobs table.
  // Leaving recJ1 in place would make every one of these pass for the wrong
  // reason — which is exactly what an earlier draft of this test did.
  mockTables = { Jobs: [] };
  for (const a of GATED_GETS) {
    eq((await GET(a, { jobId: "recNOPE" })).statusCode, 404, `${a} must 404 an unknown job`);
  }
  // createPanelSchedule and createChecklist share the same gate, but both
  // return 503 for an unconfigured database BEFORE reaching it, so they are not
  // reachable from this offline harness. Covered by the smoke test instead.

  mockTables = JOB_ONLY();
  for (const a of GATED_GETS) {
    ok((await GET(a, { jobId: "recJ1" })).statusCode !== 404, `${a} must let a real job through`);
  }
  clearR2();
});

await test("expenseReceipts: the owner check survived moving off the Airtable record", async () => {
  setR2();
  // The handler used to fetch the whole expense for one field. It now reads
  // Neon and falls back — so this exercises the fallback, and the RULE, which
  // is what matters: an employee sees receipts only on their own expenses.
  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recOther"] } }] };
  eq((await GET("expenseReceipts", { expenseId: "recX1" }, EMP_TOK)).statusCode, 403,
     "an employee cannot open someone else's receipts");
  ok((await GET("expenseReceipts", { expenseId: "recX1" }, ADMIN_TOK)).statusCode !== 403, "admin can");
  ok((await GET("expenseReceipts", { expenseId: "recX1" }, OFFICE_TOK)).statusCode !== 403, "office can");

  mockTables = { Expenses: [{ id: "recX1", fields: { "Submitted By": ["recEmp"] } }] };
  ok((await GET("expenseReceipts", { expenseId: "recX1" }, EMP_TOK)).statusCode !== 403,
     "but their own is fine");

  // An expense with no submitter is nobody's, not everybody's — legacy rows
  // read that way in both stores.
  mockTables = { Expenses: [{ id: "recX1", fields: {} }] };
  eq((await GET("expenseReceipts", { expenseId: "recX1" }, EMP_TOK)).statusCode, 403,
     "an unattributed expense is not an employee's");
  clearR2();
});

// ── source guard: neonQuery returns { rows, ms }, never an array ─────────────
// This is here because it already bit, on the day the generator service check
// was written: `const rows = await neonQuery(...)` followed by `if (!rows.length)`.
// `undefined` is falsy, so the check reported "nothing due" and did NOTHING —
// forever, silently, with a green deploy. Nothing else in this suite can catch
// it: every Neon path short-circuits offline because DATABASE_URL is unset, so
// the misuse is never executed here.
//
// A source scan can, and it costs nothing. Two shapes are flagged: destructuring
// the result as an array, and holding it in a variable that is then used like
// one. If a legitimate case ever trips this, the fix is `.rows`, not an
// exemption. (neonWrite is fine — it DOES return rows.)
await test("neonQuery: its { rows } shape is respected — the silent-death guard", async () => {
  const { readFileSync, readdirSync } = await import("node:fs");
  const { fileURLToPath } = await import("node:url");
  const dir = fileURLToPath(new URL("../netlify/functions/", import.meta.url));
  const offenders = [];

  for (const f of readdirSync(dir).filter(n => n.endsWith(".js"))) {
    const src = readFileSync(dir + f, "utf8");

    src.split("\n").forEach((l, i) => {
      if (/\[[^\]]*\]\s*=\s*await\s+neonQuery\s*\(/.test(l)) {
        offenders.push(`${f}:${i + 1} destructures a neonQuery result as an array`);
      }
    });

    const assign = /(?:const|let|var)?\s*\b(\w+)\s*=\s*await\s+neonQuery\s*\(/g;
    let m;
    while ((m = assign.exec(src))) {
      const name = m[1];
      // Look only as far as the next top-level function, so an unrelated later
      // use of a common name like `rows` can't produce a false positive.
      const rest = src.slice(m.index + m[0].length);
      const stop = rest.search(/\n(?:export )?(?:async )?function /);
      const scope = stop === -1 ? rest : rest.slice(0, stop);
      const asArray = new RegExp(`\\b${name}\\s*(?:\\.length\\b|\\.map\\s*\\(|\\.filter\\s*\\(|\\.slice\\s*\\(|\\[0\\])`);
      if (asArray.test(scope)) {
        offenders.push(`${f}: \`${name}\` holds a neonQuery result but is used as an array — did you mean ${name}.rows?`);
      }
    }
  }

  eq(offenders.length, 0, offenders.join(" | "));
});

// ── report ──
console.log("\nTier-1 backend handler tests (airtable.js)\n");
for (const [s, n] of log) console.log(`  ${s} ${n}`);
console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

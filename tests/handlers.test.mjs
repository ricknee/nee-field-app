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

// ── time-entry WRITE paths + the Neon mirror ──
// These had NO coverage before 2026-07-31, which mattered because every one of them
// now also mirrors into Neon — and payroll READS come from Neon. If a write reaches
// Airtable but not Neon, the payroll screen shows stale hours while the edit looks
// saved. The contract being locked here:
//   1. the Airtable write is correct and unchanged (field IDs, linked-record shape)
//   2. a missing or broken Neon must NEVER fail a write that Airtable accepted
const TE_F = {
  employee: "fldG8nGxyJcXRxBNQ", employeeLink: "fldYgTcZcQzNslRT5",
  workDate: "fldzFwSSjLmAkWYHt", duration: "fld9mz6As3099VPVp",
  cityTaxes: "flddCniABjh4Xib1c", class: "fld4MG0FcFDnqYmtW",
  jobLink: "fldmGwS0qXMdC7FlA", reviewed: "fldQn7d06doEkrGBv",
};

await test("createTimeEntry: writes the right Airtable fields and returns the new id", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  const b = json(await POST("createTimeEntry", {
    employee: "Jeff Koehn", employeeId: "recEmp1", workDate: "2026-07-27",
    duration: 28800, class: "Contract", cityTaxes: "A No Tax", jobId: "recJob1",
  }));
  ok(b.ok, "ok");
  eq(b.id, "recNEW", "returns the created Airtable record id");
  const f = JSON.parse(lastFetch.opts.body).fields;
  eq(f[TE_F.employee], "Jeff Koehn", "employee text");
  eq(f[TE_F.workDate], "2026-07-27", "work date");
  eq(f[TE_F.duration], 28800, "duration in SECONDS, not hours");
  // Linked records must be a bare ["rec…"] array — the [{id}] shape silently drops.
  eq(JSON.stringify(f[TE_F.employeeLink]), JSON.stringify(["recEmp1"]), "employee link shape");
  eq(JSON.stringify(f[TE_F.jobLink]), JSON.stringify(["recJob1"]), "job link shape");
});

await test("createTimeEntry: a broken Neon must not fail a write Airtable accepted", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = {};
  const res = await POST("createTimeEntry", {
    employee: "Jeff Koehn", workDate: "2026-07-27", duration: 3600,
  });
  eq(res.statusCode, 200, "still 200 — the mirror fails soft");
  eq(json(res).id, "recNEW", "id still returned");
  delete process.env.DATABASE_URL;
});

await test("createTimeEntry: rejects a missing employee or work date before writing", async () => {
  delete process.env.DATABASE_URL;
  mockTables = {};
  eq((await POST("createTimeEntry", { workDate: "2026-07-27" })).statusCode, 400, "no employee");
  eq((await POST("createTimeEntry", { employee: "Jeff Koehn" })).statusCode, 400, "no work date");
});

await test("updateTimeEntry: sets Labor Reviewed — the flag the puller must never clobber", async () => {
  delete process.env.DATABASE_URL;
  mockTables = { "Time Entries": [{ id: "recTE1", fields: { "Hours": 8 } }] };
  const b = json(await POST("updateTimeEntry", { entryId: "recTE1", reviewed: true }));
  ok(b.ok, "ok");
  eq(JSON.parse(lastFetch.opts.body).fields[TE_F.reviewed], true, "Labor Reviewed written");
});

await test("updateTimeEntryPayroll: patches duration and city tax by field id", async () => {
  delete process.env.DATABASE_URL;
  mockTables = { "Time Entries": [{ id: "recTE1", fields: {} }] };
  const b = json(await POST("updateTimeEntryPayroll", {
    entryId: "recTE1", duration: 7200, cityTaxes: "Massilon",
  }));
  ok(b.ok, "ok");
  const f = JSON.parse(lastFetch.opts.body).fields;
  eq(f[TE_F.duration], 7200, "duration patched");
  eq(f[TE_F.cityTaxes], "Massilon", "city tax patched verbatim (QB spelling)");
});

await test("deleteTimeEntry: succeeds even when Neon is unreachable", async () => {
  process.env.DATABASE_URL = "not-a-valid-connection-string";
  mockTables = { "Time Entries": [{ id: "recTE1", fields: {} }] };
  const res = await POST("deleteTimeEntry", { entryId: "recTE1" });
  eq(res.statusCode, 200, "delete still succeeds");
  eq(json(res).deleted, "recTE1", "reports what was deleted");
  delete process.env.DATABASE_URL;
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

await test('deleteJobPhotos: admin/office only, viewer and employee blocked', async () => {
  setR2();
  mockTables = JOB_ONLY();
  const body = { jobId: 'recJ1', keys: ['jobs/recJ1/a.jpg'] };
  eq((await POST('deleteJobPhotos', body, VIEWER_TOK)).statusCode, 403, 'viewer');
  eq((await POST('deleteJobPhotos', body, EMP_TOK)).statusCode, 403, 'employee');
  // admin/office must at least pass the authz gate (storage call may fail offline)
  ok((await POST('deleteJobPhotos', body, OFFICE_TOK)).statusCode !== 403, 'office allowed');
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
  const { moveJobPhoto, deleteJobPhoto } = await import('../netlify/functions/_r2.js');
  // The client sends keys back to us, so this is the check that stops a
  // signed-in user reaching another job's photos by editing one string.
  for (const [label, fn] of [['move', () => moveJobPhoto('recJ1', 'jobs/recOTHER/a.jpg', 'Gym')],
                             ['delete', () => deleteJobPhoto('recJ1', 'jobs/recOTHER/a.jpg')]]) {
    let threw = null;
    try { await fn(); } catch (e) { threw = e; }
    ok(threw && threw.code === 'KEY_OUTSIDE_JOB', `${label} rejects a foreign key (got ${threw && threw.code})`);
  }
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

// ── report ──
console.log("\nTier-1 backend handler tests (airtable.js)\n");
for (const [s, n] of log) console.log(`  ${s} ${n}`);
console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

// Tier-1 backend regression harness for netlify/functions/inventory.js
// ---------------------------------------------------------------------------
// Focused on the expense-push IDEMPOTENCY guards (the "same materials charged
// twice" foot-gun in docs/SYSTEM-MAP.html). Runs the REAL exported handler()
// with env faked and a STATEFUL fetch mock that simulates Neon and the main
// Airtable base, so it's fully offline and deterministic.
//
// ⚠⚠ THE DIRECTION REVERSED IN IDENTITY-CUTOVER SLICE 4c (2026-08-24). The push
// used to write Expenses to AIRTABLE and copy them into Neon (Step E), failing
// closed when the copy didn't land. Now the expense is BORN IN NEON and Airtable
// gets a best-effort mirror that nothing reads. Every assertion about "did it
// reach Neon" therefore flipped into "was it charged at all", and the two Step-E
// cases below are inverted rather than deleted — they are the regression tests
// for the direction, and a suite that still passed with the old direction in
// place would be worthless.
//
// Run (portable node):
//   & "C:\Users\irick\nodejs\node.exe" tests/inventory-push.test.mjs
// or, if node is on PATH:
//   node tests/inventory-push.test.mjs
// Exit code is 0 on all-pass, 1 on any failure (CI-friendly).
// ---------------------------------------------------------------------------

// 1) Fake env BEFORE importing the module (read at import time).
const MAIN_BASE = "appMain0000000000";
process.env.AIRTABLE_API_KEY   = "test-key";
process.env.AIRTABLE_BASE_ID   = MAIN_BASE;
process.env.AUTH_SECRET        = "test-secret";

// Field IDs that the handler stamps on the MIRROR (must match inventory.js).
const EXP_PUSH_ID_FIELD = "flddMVlSELtNT48ez"; // Expenses -> Push ID

// 2) Stateful mock.
//    `state.neonExpenses` is the real store — the expenses the push charges.
//    `state.expenses` is the Airtable MIRROR, which is decorative.
//    `state.txns` is the inventory ledger (Neon, keyed by uuid).
const state = {
  txns: {},          // id -> { pushed: bool, pushId: string|null }
  neonExpenses: [],  // { id, pushId, amount, descr } — the authoritative rows
  expenses: [],      // Airtable mirror records { id, pushId, amount, description }
  neonSeq: 0,
  expSeq: 0,
  pushHdrSeq: 0,
  // Slice 2: the push history is a Neon table now, and it is the audit trail
  // behind every charged dollar. Keyed by push_id so a retry can be seen to
  // reuse a header rather than mint a second one.
  history: {},       // push_id -> { id, lineCount }
  // Which job handles resolve in Neon's `jobs`. The push's INSERT is a CROSS
  // JOIN against a CTE that looks the job up, so an unknown handle inserts ZERO
  // rows rather than an expense with a NULL job_id — see the "unresolvable job"
  // case for why that matters in dollars.
  jobs: new Set(["recJob1"]),
};
function resetState(txIds) {
  state.txns = {};
  txIds.forEach(id => { state.txns[id] = { pushed: false, pushId: null }; });
  state.history = {};
  state.neonExpenses = [];
  state.expenses = [];
  state.neonSeq = 0;
  state.expSeq = 0;
  state.pushHdrSeq = 0;
  state.jobs = new Set(["recJob1"]);
  neonDown = false;
  neonFailExpenseWrite = false;
  airtableMirrorDown = false;
}

// Driver contract: POST {query, params} to /sql, expects {fields, rows} back
// with rows as VALUE ARRAYS. `neonDown = true` simulates the database being
// unreachable; `neonFailExpenseWrite` fails ONLY the expense INSERT.
process.env.DATABASE_URL = "postgresql://u:p@fake.neon.tech/db";
let neonDown = false;
let neonFailExpenseWrite = false;
let airtableMirrorDown = false;

const textCol = (name) => [{ name, dataTypeID: 25, tableID: 0, columnID: 1,
                             dataTypeSize: -1, dataTypeModifier: -1, format: "text" }];

globalThis.fetch = async (url, opts = {}) => {
  const method = opts.method || "GET";
  const body  = opts.body ? JSON.parse(opts.body) : null;
  const ok = (records) => ({ ok: true, status: 200, text: async () => JSON.stringify({ records }) });

  if (String(url).includes("/sql")) {
    if (neonDown) return { ok: false, status: 500, text: async () => "neon down" };
    const sql = String(body?.query || "");
    let payload = { command: "INSERT", rowCount: 0, rowAsArray: false, fields: [], rows: [] };

    // The push-history header. ON CONFLICT (push_id) is modelled by keying on
    // it: a retry finds the existing header and reuses its id, which is what
    // stops a duplicate audit row for a charge that happened once.
    if (/INSERT INTO expense_pushes/i.test(sql)) {
      const pushId = body.params?.[0];
      const existing = pushId && state.history[pushId];
      const id = existing ? existing.id : `hdr-${++state.pushHdrSeq}`;
      state.history[pushId] = { id, lineCount: existing ? existing.lineCount : 0 };
      payload = { command: "INSERT", rowCount: 1, rowAsArray: false,
        fields: textCol("id"), rows: [[id]] };

    } else if (/DELETE FROM expense_push_lines/i.test(sql)) {
      const hdrId = body.params?.[0];
      for (const h of Object.values(state.history)) if (h.id === hdrId) h.lineCount = 0;

    } else if (/INSERT INTO expense_push_lines/i.test(sql)) {
      const hdrId = body.params?.[0];          // 7 binds per line, parent first
      const n = Math.round((body.params || []).length / 7);
      for (const h of Object.values(state.history)) if (h.id === hdrId) h.lineCount += n;

    // ── GUARD #1, and it reads NEON now (slice 4c). It used to be an Airtable
    // filterByFormula on {Push ID}; the Airtable GET below throws precisely so
    // that a regression back to it cannot pass this suite.
    } else if (/SELECT DISTINCT push_id FROM expenses/i.test(sql)) {
      const wanted = new Set(parseArrayBind(body.params?.[0]));
      const ids = [...new Set(state.neonExpenses.map(e => e.pushId).filter(Boolean))]
        .filter(p => wanted.has(p));
      payload = { command: "SELECT", rowCount: ids.length, rowAsArray: false,
        fields: textCol("push_id"), rows: ids.map(v => [v]) };

    // ── THE CHARGE. Params are [job_airtable_id, handle, date, pushId, vendor,
    // then (amount, descr) per row]. Modelled faithfully in two respects that
    // both cost money if the handler gets them wrong:
    //   * an unknown job handle inserts ZERO rows (the `j` CTE is empty), and
    //   * one id comes back per (amount, descr) pair, so a taxable push that
    //     silently created only its materials row fails here.
    } else if (/INSERT INTO expenses/i.test(sql)) {
      if (neonFailExpenseWrite) return { ok: false, status: 500, text: async () => "neon write failed" };
      const p = body.params || [];
      const handle = p[1], pushId = p[3];
      const rows = [];
      if (state.jobs.has(handle)) {
        for (let i = 5; i + 1 < p.length; i += 2) {
          const id = `neon-exp-${++state.neonSeq}`;
          state.neonExpenses.push({ id, pushId, amount: Number(p[i]), descr: String(p[i + 1]) });
          rows.push([id]);
        }
      }
      payload = { command: "INSERT", rowCount: rows.length, rowAsArray: false,
        fields: textCol("id"), rows };

    // ⚠⚠ The rec id is NEVER carried back onto a native expense: R2 receipt keys
    // are `expenses/<handle>/` and listExpenseReceipts lists ONE prefix, so a
    // handle that flips orphans every receipt already stored. Nothing in the
    // push may write that column — make the attempt loud rather than latent.
    } else if (/UPDATE expenses[\s\S]*airtable_id/i.test(sql)) {
      throw new Error("the push stamped airtable_id back onto an expense — that orphans its R2 receipts");

    // Step C: the push decides what is chargeable by reading Neon, and marks
    // Neon when it charges. `state.txns` is the single source both halves of
    // this mock answer from, so they cannot drift apart mid-test.
    // ⚠⚠ TWO id spaces, deliberately. `state.txns` is keyed by the uuid — the
    // ledger's handle since the native-write cutover — and the Airtable rec id
    // is a DIFFERENT string ("rec-" + uuid), exactly as it is in production
    // where a native row has no rec id at all.
    //
    // This mock used to answer both from one id space, which is why it let a
    // real bug ship: the chargeable-set read still said SELECT airtable_id
    // while the push carried uuids, so nothing matched, every job was refused
    // as a "stale snapshot", and the push was dead on arrival. Answering with
    // the column the SQL actually asked for is what makes that fail here.
    } else if (/SELECT (id|airtable_id) FROM inventory_transactions/i.test(sql)) {
      const col = /SELECT airtable_id FROM inventory_transactions/i.test(sql) ? "airtable_id" : "id";
      const pending = Object.entries(state.txns)
        .filter(([, t]) => !t.pushed)
        .map(([id]) => (col === "id" ? id : `rec-${id}`));
      payload = { command: "SELECT", rowCount: pending.length, rowAsArray: false,
        fields: textCol(col), rows: pending.map(v => [v]) };

    } else if (/UPDATE inventory_transactions[\s\S]*expense_created = true/i.test(sql)) {
      for (const id of parseArrayBind(body.params?.[0])) {
        const t = state.txns[id] || (state.txns[id] = { pushed: false, pushId: null });
        t.pushed = true; t.pushId = body.params?.[1] || null;
      }
    }

    return { ok: true, status: 200, headers: { get: () => "application/json" },
             text: async () => JSON.stringify(payload), json: async () => payload };
  }

  const m = String(url).match(/\/v0\/([^/]+)\/([^/?]+)(?:\/([^?]+))?/);
  const table    = m ? decodeURIComponent(m[2]) : "";
  const recordId = m && m[3] ? decodeURIComponent(m[3]) : null;

  // ── Reads (GET) ──
  if (method === "GET") {
    // ⚠ Both Airtable reads the push used to make are GONE with slice 4c, and
    // both throw rather than returning empty — an empty answer would let the old
    // code path pass silently, and each of these is a money bug:
    //   * the {Push ID} lookup WAS guard #1. Asking Airtable about an expense
    //     born in Neon answers "never pushed" and the retry charges again.
    //   * the single-record re-read fed syncExpenseToNeon, which would insert a
    //     SECOND expense for the same spend (ON CONFLICT can't fire on a NULL).
    if (table === "Expenses" && recordId) {
      throw new Error("Airtable GET of an Expense — the push does not re-read or re-sync any more");
    }
    if (table === "Expenses") {
      throw new Error("Airtable {Push ID} lookup — guard #1 reads Neon since slice 4c");
    }
    if (table === "Inventory Transactions") {
      throw new Error("Airtable GET of Inventory Transactions — the ledger is native now");
    }
    return ok([]);
  }

  // ── Writes (POST/PATCH) ──
  if (method === "POST" && table === "Expenses") {
    // The MIRROR. Best-effort by contract: when it fails the push must still
    // succeed, because the money already landed in Neon.
    if (airtableMirrorDown) throw new Error("Airtable 503");
    const created = (body.records || []).map(r => {
      const id = `recExp${++state.expSeq}`;
      state.expenses.push({
        id,
        pushId:      r.fields?.[EXP_PUSH_ID_FIELD] || null,
        jobId:       r.fields?.["fldPNFIzq1grsdxYi"]?.[0] || null,
        amount:      r.fields?.["fldwbLPIafVtmaSeb"] ?? 0,
        description: r.fields?.["fldnSQEOnyq3sho5g"] || "",
      });
      return { id, fields: r.fields };
    });
    return ok(created);
  }
  if (method === "PATCH" && table === "Inventory Transactions") {
    // Since the ledger cutover the pushed-mark is a Neon UPDATE and nothing
    // else. This used to record the mark, which meant the suite would still
    // have passed if the Neon half silently did nothing — the Step E trap. It
    // now throws, so "both txns marked pushed" can only be satisfied by the
    // statement that actually prevents the double charge.
    throw new Error("Airtable PATCH of Inventory Transactions — the ledger is native now");
  }
  if (method === "POST" && (table === "Expense Pushes" || table === "Expense Push Lines")) {
    // Slice 2: the history is Neon-only. Recording the write here again would
    // let the suite pass with the Neon half doing nothing.
    throw new Error(`Airtable POST to ${table} — the push history is native now`);
  }
  return ok([]);
};

// An array bind arrives as the Postgres LITERAL '{a,b}', not a JS array —
// iterating it directly walks the string character by character.
function parseArrayBind(v) {
  if (Array.isArray(v)) return v;
  if (typeof v !== "string") return [];
  return v.slice(1, -1).split(",").map(s => s.replace(/^"|"$/g, "")).filter(Boolean);
}

// 3) Import the real handler after env + mock are in place.
const { handler } = await import("../netlify/functions/inventory.js");
const { signToken } = await import("../netlify/functions/_auth.js");
const ADMIN_TOK = signToken({ id: "recAdmin", role: "admin" });

// ── tiny assert framework (no deps) ──
let pass = 0, fail = 0;
const log = [];
async function test(name, fn) {
  try { await fn(); log.push(["✓", name]); pass++; }
  catch (e) { log.push(["✗", `${name} — ${e.message}`]); fail++; }
}
const eq = (a, b, m) => { if (a !== b) throw new Error(`${m || ""} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); };

const PUSH = (pending) => handler({
  httpMethod: "POST",
  body: JSON.stringify({ action: "pushExpenses", pending, pushedBy: "Tester" }),
  headers: { authorization: `Bearer ${ADMIN_TOK}` }
});
const json = (res) => JSON.parse(res.body);
const group = (pushId, txIds) => ({
  jobId: "recJob1", jobName: "Test Job", taxable: false,
  lines: [{ item: "Wire", qty: 5, cost: 20, total: 100, wireFt: 0 }],
  txIds, pushId
});

// ── cases ──

await test("first push creates one expense and marks its transactions", async () => {
  resetState(["tx1", "tx2"]);
  const r = json(await PUSH([group("pid-1", ["tx1", "tx2"])]));
  eq(r.ok, true, "ok");
  eq(r.created, 1, "created");
  eq(r.count, 1, "expense count");
  eq(r.txCount, 2, "txns marked");
  eq(state.neonExpenses.length, 1, "one expense persisted IN NEON");
  eq(state.neonExpenses[0].pushId, "pid-1", "expense stamped with push id");
  eq(state.neonExpenses[0].amount, 100, "for the group's total");
  eq(state.txns.tx1.pushed && state.txns.tx2.pushed, true, "both txns marked pushed");
  eq(state.txns.tx1.pushId, "pid-1", "tx stamped with push id");
  // Slice 2: the audit trail behind the charge, and it is Neon-only now.
  eq(!!state.history["pid-1"], true, "a push-history header was written");
  eq(state.history["pid-1"].lineCount, 1, "with its line snapshot");
});

await test("4c: the expense is BORN IN NEON and the id returned is the Neon uuid", async () => {
  resetState(["tx1"]);
  const r = json(await PUSH([group("pid-native", ["tx1"])]));
  eq(r.ok, true, "ok");
  // The push history's expense id list is what a person sees in Push History.
  // It must be the handle the rest of the app speaks, not a rec id.
  eq(state.neonExpenses[0].id.startsWith("neon-exp-"), true, "Neon minted the id");
  eq(state.expenses.length, 1, "and Airtable got its mirror");
  eq(state.expenses[0].pushId, "pid-native", "mirror carries the push id too");
});

await test("S2: a retried push reuses its history header instead of duplicating it", async () => {
  resetState(["tx1"]);
  json(await PUSH([group("pid-dup", ["tx1"])]));
  const firstId = state.history["pid-dup"].id;
  eq(state.history["pid-dup"].lineCount, 1, "one line first time");

  // Same pushId again — guard #1 recognises the charge, and ON CONFLICT means
  // the history row is reused rather than a second audit entry appearing for a
  // charge that only happened once.
  json(await PUSH([group("pid-dup", ["tx1"])]));
  eq(Object.keys(state.history).length, 1, "still ONE header");
  eq(state.history["pid-dup"].id, firstId, "the same header");
  eq(state.history["pid-dup"].lineCount, 1, "lines replaced, not accumulated");
});

await test("guard #1: re-push with the SAME pushId does NOT create a second expense", async () => {
  resetState(["tx1", "tx2"]);
  await PUSH([group("pid-1", ["tx1", "tx2"])]);          // first push
  const r = json(await PUSH([group("pid-1", ["tx1", "tx2"])])); // exact retry
  eq(r.created, 0, "nothing freshly created");
  eq(r.alreadyPushed, 1, "recognized as already pushed");
  eq(state.neonExpenses.length, 1, "still only one expense — no double charge");
});

await test("guard #1 answers from NEON, so it still holds when the mirror never existed", async () => {
  resetState(["tx1"]);
  // The push that charged could not reach Airtable at all. Its expense exists
  // only in Neon — which is the normal state once the base is archived. If the
  // guard were still asking Airtable it would see nothing and charge again.
  airtableMirrorDown = true;
  const first = json(await PUSH([group("pid-nomirror", ["tx1"])]));
  eq(first.ok, true, "the push still succeeded");
  eq(state.expenses.length, 0, "no Airtable copy exists");
  eq(state.neonExpenses.length, 1, "but the charge is in Neon");

  airtableMirrorDown = false;
  const r = json(await PUSH([group("pid-nomirror", ["tx1"])]));
  eq(r.alreadyPushed, 1, "guard #1 recognised it anyway");
  eq(r.created, 0, "nothing freshly charged");
  eq(state.neonExpenses.length, 1, "STILL one expense — no double charge");
});

await test("guard #2: re-push same materials under a NEW pushId is refused as stale", async () => {
  resetState(["tx1", "tx2"]);
  await PUSH([group("pid-1", ["tx1", "tx2"])]);          // first push marks the txns
  const r = json(await PUSH([group("pid-2", ["tx1", "tx2"])])); // different key, same txns
  eq(r.created, 0, "nothing freshly created");
  eq(r.staleSkipped, 1, "refused as stale snapshot");
  eq(state.neonExpenses.length, 1, "still only one expense — no double charge");
});

await test("taxable push creates materials + tax expense, both stamped", async () => {
  resetState(["tx9"]);
  const g = group("pid-tax", ["tx9"]); g.taxable = true;
  const r = json(await PUSH([g]));
  eq(r.created, 1, "one group charged");
  eq(r.count, 2, "materials + tax expense");
  eq(state.neonExpenses.length, 2, "both rows in Neon");
  eq(state.neonExpenses.every(e => e.pushId === "pid-tax"), true, "both expenses stamped");
  eq(state.neonExpenses[1].amount, 7.5, "tax is 7.5% of the materials total");
  eq(state.expenses.length, 2, "and both mirrored");
});

// ── Slice 4c: the failure direction is inverted ─────────────────────────────
// Neon holds the money and Airtable holds a copy nobody reads, so "Airtable
// didn't get it" is cosmetic and "Neon didn't get it" means nothing was charged.
// These three are the Step E cases, turned around.

await test("4c: an Airtable mirror failure does NOT fail the push", async () => {
  resetState(["tx1"]);
  airtableMirrorDown = true;
  const res = await PUSH([group("pid-mirror", ["tx1"])]);
  const r = json(res);
  eq(res.statusCode, 200, "still a success");
  eq(r.ok, true, "ok:true — the money landed where it is read from");
  eq(r.created, 1, "charged");
  eq(state.neonExpenses.length, 1, "the expense exists in Neon");
  eq(state.expenses.length, 0, "and nowhere in Airtable, which is fine");
  eq(state.txns.tx1.pushed, true, "the transactions were still marked");
});

await test("4c: if the NEON insert fails, NOTHING is charged and the txns stay pending", async () => {
  resetState(["tx1"]);
  neonFailExpenseWrite = true;
  const res = await PUSH([group("pid-nofail", ["tx1"])]);
  const r = json(res);
  eq(res.statusCode, 502, "reported as a failure");
  eq(r.ok, false, "ok:false");
  eq(r.created, 0, "nothing charged");
  eq(state.neonExpenses.length, 0, "no expense anywhere");
  eq(state.expenses.length, 0, "and no Airtable orphan — the mirror runs AFTER the charge");
  eq(state.txns.tx1.pushed, false, "the material is still pending, so it can be pushed again");

  // And the retry is a normal push, not a guard #1 short-circuit, because
  // nothing carries that push id.
  neonFailExpenseWrite = false;
  const r2 = json(await PUSH([group("pid-nofail", ["tx1"])]));
  eq(r2.ok, true, "retry succeeds");
  eq(r2.created, 1, "and actually charges this time");
  eq(state.neonExpenses.length, 1, "exactly once");
});

await test("S1: Neon unreachable → the push is REFUSED before anything is charged", async () => {
  resetState(["tx1"]);
  neonDown = true;
  const res = await PUSH([group("pid-fail", ["tx1"])]);
  const r = json(res);
  // Guard #2 — "are these transactions still chargeable?" — can only be answered
  // by Neon now that the ledger is native. Airtable's copy is missing every
  // native row AND still reports unpushed for material already charged, so there
  // is no second opinion to fall back on. Charging while blind to that is
  // precisely the double-charge guard #2 exists to stop, so the push does not
  // start. Since 4c the same is true of guard #1.
  eq(res.statusCode, 503, "refused, not attempted");
  eq(r.ok, false, "ok:false");
  eq(/nothing was pushed/i.test(r.error), true, "says plainly that nothing happened");
  eq(state.neonExpenses.length, 0, "NOTHING was charged");
  eq(state.expenses.length, 0, "not even an Airtable orphan");
});

await test("4c: a job that doesn't resolve in Neon is refused, not billed at cost", async () => {
  resetState(["tx1"]);
  // `v_expenses` derives the billable amount as cost × (1 + jobs.markup_pct)
  // through job_id. A NULL job_id doesn't error — it silently prices material at
  // COST. The INSERT is a CROSS JOIN against the job lookup so it writes zero
  // rows instead, and the handler refuses the group.
  const g = group("pid-nojob", ["tx1"]);
  g.jobId = "recGhostJob";        // not in state.jobs
  const res = await PUSH([g]);
  const r = json(res);
  eq(res.statusCode, 502, "reported as a failure");
  eq(r.ok, false, "ok:false");
  eq(r.created, 0, "nothing charged");
  eq(state.neonExpenses.length, 0, "no expense with a NULL job");
  eq(state.txns.tx1.pushed, false, "the material stays pending");
});

await test("4c: one job failing does not strand the groups after it", async () => {
  resetState(["txA", "txB"]);
  const bad  = group("pid-bad", ["txA"]);  bad.jobId = "recGhostJob"; bad.jobName = "Ghost";
  const good = group("pid-good", ["txB"]);
  const r = json(await PUSH([bad, good]));
  eq(r.ok, false, "the response is honest about the failure");
  eq(r.created, 1, "the healthy job was still charged");
  eq(r.failedJobs.length, 1, "one job reported as left pending");
  eq(r.failedJobs[0], "Ghost", "named, so the user knows which");
  eq(state.neonExpenses.length, 1, "exactly one expense");
  eq(state.txns.txB.pushed, true, "the charged job's txns are marked");
  eq(state.txns.txA.pushed, false, "the failed job's are not");
});

// ── report ──
console.log("\ninventory.js push-idempotency tests\n" + "-".repeat(44));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(44));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

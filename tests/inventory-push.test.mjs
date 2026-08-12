// Tier-1 backend regression harness for netlify/functions/inventory.js
// ---------------------------------------------------------------------------
// Focused on the expense-push IDEMPOTENCY guards (the "same materials charged
// twice" foot-gun in docs/SYSTEM-MAP.html). Runs the REAL exported handler()
// with env faked and a STATEFUL fetch mock that simulates the two Airtable
// bases (reads + POST/PATCH writes), so it's fully offline and deterministic.
//
// Run (portable node):
//   & "C:\Users\irick\nodejs\node.exe" tests/inventory-push.test.mjs
// or, if node is on PATH:
//   node tests/inventory-push.test.mjs
// Exit code is 0 on all-pass, 1 on any failure (CI-friendly).
// ---------------------------------------------------------------------------

// 1) Fake env BEFORE importing the module (read at import time). Distinct base
//    IDs so the mock can tell the main base from the inventory base by URL.
const MAIN_BASE = "appMain0000000000";
const INV_BASE  = "appInv00000000000";
process.env.AIRTABLE_API_KEY   = "test-key";
process.env.AIRTABLE_BASE_ID   = MAIN_BASE;
process.env.INVENTORY_BASE_ID  = INV_BASE;
process.env.AUTH_SECRET        = "test-secret";

// Field IDs that the handler stamps (must match inventory.js).
const EXP_PUSH_ID_FIELD = "flddMVlSELtNT48ez"; // Expenses -> Push ID
const TX_PUSH_ID_FIELD  = "fldv9iY9ZKrV1SOsA"; // Inventory Transactions -> Push ID
const TX_EXP_CREATED    = "fldO7Z0L7tpAvrgtH"; // Inventory Transactions -> Expense Created?

// 2) Stateful Airtable mock. `state.txns` are inventory transactions; `state.
//    expenses` are records created in the main base (each carries its Push ID).
const state = {
  txns: {},        // id -> { pushed: bool, pushId: string|null }
  expenses: [],    // { id, pushId }
  expSeq: 0,
  pushHdrSeq: 0,
  // Slice 2: the push history is a Neon table now, and it is the audit trail
  // behind every charged dollar. Keyed by push_id so a retry can be seen to
  // reuse a header rather than mint a second one.
  history: {},     // push_id -> { id, lineCount }
};
function resetState(txIds) {
  state.txns = {};
  txIds.forEach(id => { state.txns[id] = { pushed: false, pushId: null }; });
  state.history = {};
  state.expenses = [];
  state.expSeq = 0;
  state.pushHdrSeq = 0;
  neonWrites.length = 0;
  neonDown = false;
  neonFailExpenseWrite = false;
}

// Neon mock (Step E). The push now writes every expense it creates into Neon
// and FAILS CLOSED if that doesn't land, so the suite has to model it or every
// push test fails for the wrong reason. Driver contract: POST {query, params}
// to /sql, expects {fields, rows} back with rows as VALUE ARRAYS.
// `neonDown = true` simulates the database being unreachable.
process.env.DATABASE_URL = "postgresql://u:p@fake.neon.tech/db";
let neonDown = false;
let neonFailExpenseWrite = false;   // only the expense mirror fails; reads still work
const neonWrites = [];          // every expense airtable_id upserted

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
        fields: [{ name: "id", dataTypeID: 25, tableID: 0, columnID: 1,
                   dataTypeSize: -1, dataTypeModifier: -1, format: "text" }],
        rows: [[id]] };

    } else if (/DELETE FROM expense_push_lines/i.test(sql)) {
      const hdrId = body.params?.[0];
      for (const h of Object.values(state.history)) if (h.id === hdrId) h.lineCount = 0;

    } else if (/INSERT INTO expense_push_lines/i.test(sql)) {
      const hdrId = body.params?.[0];          // 7 binds per line, parent first
      const n = Math.round((body.params || []).length / 7);
      for (const h of Object.values(state.history)) if (h.id === hdrId) h.lineCount += n;

    } else if (/INSERT INTO expenses/i.test(sql)) {
      // Fails ONLY the expense mirror, leaving the chargeable-set read working.
      // That separation matters since the ledger went native: a total outage is
      // now refused up front (guard #2 cannot run), so the heal path can only be
      // reached by this narrower failure — the expense landing in Airtable while
      // its Neon copy does not.
      if (neonFailExpenseWrite) return { ok: false, status: 500, text: async () => "neon write failed" };
      neonWrites.push(body.params?.[0]);

    // Step C: the push now decides what is chargeable by reading Neon, and
    // marks Neon when it charges. `state.txns` is the single source both the
    // Airtable mock and this one answer from, so the two cannot drift apart
    // mid-test the way they would if the ledger were mocked separately.
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
      payload = {
        command: "SELECT", rowCount: pending.length, rowAsArray: false,
        fields: [{ name: col, dataTypeID: 25, tableID: 0, columnID: 1,
                   dataTypeSize: -1, dataTypeModifier: -1, format: "text" }],
        rows: pending.map(v => [v]),
      };

    } else if (/UPDATE inventory_transactions[\s\S]*expense_created = true/i.test(sql)) {
      // An array bind arrives as the Postgres LITERAL '{a,b}', not a JS array —
      // iterating params[0] directly walks the string character by character.
      const ids = typeof body.params?.[0] === "string"
        ? body.params[0].slice(1, -1).split(",").map(s => s.replace(/^"|"$/g, "")).filter(Boolean)
        : (body.params?.[0] || []);
      for (const id of ids) {
        const t = state.txns[id] || (state.txns[id] = { pushed: false, pushId: null });
        t.pushed = true; t.pushId = body.params?.[1] || null;
      }
    }

    return { ok: true, status: 200, headers: { get: () => "application/json" },
             text: async () => JSON.stringify(payload), json: async () => payload };
  }

  // Split "Table" from "Table/recXXX" — the push re-reads each created expense
  // by id before syncing it, so the single-record form has to be modelled.
  const m = String(url).match(/\/v0\/([^/]+)\/([^/?]+)(?:\/([^?]+))?/);
  const base     = m ? m[1] : "";
  const table    = m ? decodeURIComponent(m[2]) : "";
  const recordId = m && m[3] ? decodeURIComponent(m[3]) : null;

  // ── Reads (GET) ──
  if (method === "GET") {
    // Single-expense re-read before the Neon sync. Returns the derived money
    // fields too — the reason the handler re-reads instead of trusting the
    // create response is that those are Airtable formulas.
    if (table === "Expenses" && recordId) {
      const e = state.expenses.find(x => x.id === recordId);
      if (!e) return { ok: false, status: 404, text: async () => JSON.stringify({ error: { message: "Not found" } }) };
      return { ok: true, status: 200, text: async () => JSON.stringify({
        id: e.id,
        fields: {
          "Push ID": e.pushId, "Job": e.jobId, "Expense Type": "Materials",
          "Expense Status": "Not Reviewed", "Expense Date": "2026-08-10",
          "Total Cost (Actual)": e.amount, "Billable?": true,
          "Billable Material Amount $": e.amount, "Unbilled Material Amount $": e.amount,
          "Description": e.description,
        },
      }) };
    }
    if (table === "Inventory Transactions") {
      // Mirrors the pending filter: only transactions not yet pushed.
      const recs = Object.entries(state.txns)
        .filter(([, t]) => !t.pushed)
        .map(([id]) => ({ id, fields: { "Transaction Type": "Use" } }));
      return ok(recs);
    }
    if (table === "Expenses") {
      // The {Push ID}=... lookup — return stored expenses whose pushId appears
      // in the decoded filter string.
      const q = decodeURIComponent(String(url));
      const recs = state.expenses
        .filter(e => e.pushId && q.includes(e.pushId))
        .map(e => ({ id: e.id, fields: { "Push ID": e.pushId } }));
      return ok(recs);
    }
    return ok([]);
  }

  // ── Writes (POST/PATCH) ──
  if (method === "POST" && table === "Expenses") {
    const created = (body.records || []).map(r => {
      const id = `recExp${++state.expSeq}`;
      const pushId = r.fields?.[EXP_PUSH_ID_FIELD] || null;
      // Keep enough to answer the single-record re-read the sync performs.
      state.expenses.push({
        id, pushId,
        jobId:       r.fields?.["fldPNFIzq1grsdxYi"]?.[0] || null,
        amount:      r.fields?.["fldotVu0jhqmh4A4h"] ?? r.fields?.["fldZyi6nVUHzshIaT"] ?? 0,
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
  eq(state.expenses.length, 1, "one expense persisted");
  eq(state.expenses[0].pushId, "pid-1", "expense stamped with push id");
  eq(state.txns.tx1.pushed && state.txns.tx2.pushed, true, "both txns marked pushed");
  eq(state.txns.tx1.pushId, "pid-1", "tx stamped with push id");
  // Slice 2: the audit trail behind the charge, and it is Neon-only now.
  eq(!!state.history["pid-1"], true, "a push-history header was written");
  eq(state.history["pid-1"].lineCount, 1, "with its line snapshot");
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
  eq(state.expenses.length, 1, "still only one expense — no double charge");
});

await test("guard #2: re-push same materials under a NEW pushId is refused as stale", async () => {
  resetState(["tx1", "tx2"]);
  await PUSH([group("pid-1", ["tx1", "tx2"])]);          // first push marks the txns
  const r = json(await PUSH([group("pid-2", ["tx1", "tx2"])])); // different key, same txns
  eq(r.created, 0, "nothing freshly created");
  eq(r.staleSkipped, 1, "refused as stale snapshot");
  eq(state.expenses.length, 1, "still only one expense — no double charge");
});

await test("taxable push creates materials + tax expense, both stamped", async () => {
  resetState(["tx9"]);
  const g = group("pid-tax", ["tx9"]); g.taxable = true;
  const r = json(await PUSH([g]));
  eq(r.created, 1, "one group charged");
  eq(r.count, 2, "materials + tax expense");
  eq(state.expenses.every(e => e.pushId === "pid-tax"), true, "both expenses stamped");
});

// ── Step E: the push keeps Neon in step, and fails closed if it can't ────────
// The field app reads expenses from Neon, so an Airtable-only push is invisible
// on the job and absent from GP. These cover the three states that matter.

await test("E: every created expense is written to Neon", async () => {
  resetState(["tx1"]);
  const g = group("pid-neon", ["tx1"]); g.taxable = true;
  const r = json(await PUSH([g]));
  eq(r.ok, true, "ok");
  eq(r.count, 2, "materials + tax");
  eq(neonWrites.length, 2, "BOTH expenses synced — not just the materials one");
  eq(neonWrites.every(id => state.expenses.some(e => e.id === id)), true,
     "synced the real Airtable rec ids");
});

await test("S1: Neon unreachable → the push is REFUSED before anything is charged", async () => {
  resetState(["tx1"]);
  neonDown = true;
  const res = await PUSH([group("pid-fail", ["tx1"])]);
  const r = json(res);
  // Stricter than the Step E contract this replaces, and it has to be. Guard #2
  // — "are these transactions still chargeable?" — can only be answered by Neon
  // now that the ledger is native. Airtable's copy is missing every native row
  // AND still reports unpushed for material already charged, so there is no
  // second opinion to fall back on. Charging while blind to that is precisely
  // the double-charge guard #2 exists to stop, so the push does not start.
  eq(res.statusCode, 503, "refused, not attempted");
  eq(r.ok, false, "ok:false");
  eq(/nothing was pushed/i.test(r.error), true, "says plainly that nothing happened");
  eq(state.expenses.length, 0, "NOTHING was charged — the previous design created the expense first");
});

await test("E: retrying a half-failed push heals it without charging twice", async () => {
  resetState(["tx1"]);
  // The narrower failure that can still happen: the chargeable-set read works,
  // the expense reaches Airtable, and only its Neon mirror fails.
  neonFailExpenseWrite = true;
  const first = json(await PUSH([group("pid-heal", ["tx1"])]));
  eq(first.ok, false, "first attempt fails closed");
  eq(state.expenses.length, 1, "one expense in Airtable");
  eq(neonWrites.length, 0, "nothing reached Neon");

  neonFailExpenseWrite = false;           // the mirror comes back
  const r = json(await PUSH([group("pid-heal", ["tx1"])]));
  eq(r.ok, true, "retry succeeds");
  eq(r.created, 0, "guard #1: nothing freshly charged");
  eq(r.alreadyPushed, 1, "recognised as already pushed");
  eq(state.expenses.length, 1, "STILL one expense — no double charge");
  eq(neonWrites.length, 1, "and the stranded expense finally reached Neon");
  eq(neonWrites[0], state.expenses[0].id, "the same record, healed");
});

// ── report ──
console.log("\ninventory.js push-idempotency tests\n" + "-".repeat(44));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(44));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

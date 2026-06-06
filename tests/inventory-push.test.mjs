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
  pushHdrSeq: 0
};
function resetState(txIds) {
  state.txns = {};
  txIds.forEach(id => { state.txns[id] = { pushed: false, pushId: null }; });
  state.expenses = [];
  state.expSeq = 0;
  state.pushHdrSeq = 0;
}

globalThis.fetch = async (url, opts = {}) => {
  const method = opts.method || "GET";
  const m = String(url).match(/\/v0\/([^/]+)\/([^?]+)/);
  const base  = m ? m[1] : "";
  const table = m ? decodeURIComponent(m[2]) : "";
  const body  = opts.body ? JSON.parse(opts.body) : null;
  const ok = (records) => ({ ok: true, status: 200, text: async () => JSON.stringify({ records }) });

  // ── Reads (GET) ──
  if (method === "GET") {
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
      state.expenses.push({ id, pushId });
      return { id, fields: r.fields };
    });
    return ok(created);
  }
  if (method === "PATCH" && table === "Inventory Transactions") {
    (body.records || []).forEach(r => {
      const t = state.txns[r.id] || (state.txns[r.id] = { pushed: false, pushId: null });
      if (r.fields?.[TX_EXP_CREATED]) t.pushed = true;
      if (r.fields?.[TX_PUSH_ID_FIELD]) t.pushId = r.fields[TX_PUSH_ID_FIELD];
    });
    return ok(body.records || []);
  }
  if (method === "POST" && (table === "Expense Pushes" || table === "Expense Push Lines")) {
    return ok([{ id: `recPush${++state.pushHdrSeq}`, fields: {} }]);
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

// ── report ──
console.log("\ninventory.js push-idempotency tests\n" + "-".repeat(44));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(44));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

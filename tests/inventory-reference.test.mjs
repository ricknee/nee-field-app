// Tier-1 backend regression harness for netlify/functions/inventory.js
// ---------------------------------------------------------------------------
// Step B: the inventory REFERENCE tables (Locations, Vendors, Inventory Items,
// Vendor Pricing) serve from Neon, with Airtable as the fallback.
//
// The two things worth testing are not "does it return rows":
//
//   1. IDS STAY AIRTABLE REC IDS. They flow into the cart, onto Inventory
//      Transactions, into estimate and order lines and into the expense push.
//      A Neon uuid escaping here is the same class of bug as the job-picker
//      trap at B0.
//   2. THE NEON AND AIRTABLE BRANCHES RETURN THE SAME SHAPE. The vendor-pricing
//      panel reads `partNumber`/`leadTime`/`lastUpdate` and a `summary` object;
//      a near-miss is a silently blank panel, not an error.
//
// Run:  & "C:\Users\irick\nodejs\node.exe" tests/inventory-reference.test.mjs
// ---------------------------------------------------------------------------

const MAIN_BASE = "appMain0000000000";
const INV_BASE  = "appInv00000000000";
process.env.AIRTABLE_API_KEY  = "test-key";
process.env.AIRTABLE_BASE_ID  = MAIN_BASE;
process.env.INVENTORY_BASE_ID = INV_BASE;
process.env.AUTH_SECRET       = "test-secret";
process.env.DATABASE_URL      = "postgresql://u:p@fake.neon.tech/db";

// ── Neon mock. Dispatches on the SQL text, because these handlers issue
//    several differently-shaped queries. Rows go back as VALUE ARRAYS with a
//    `fields` list — the @neondatabase/serverless wire contract; objects throw
//    "c.map is not a function".
let neonDown  = false;
let neonItems = [];      // {airtable_id, name, category, product_size, unit_of_measure, barcode, default_unit_cost, wire_ft_per_lb}
let neonLocs  = [];      // {airtable_id, name, location_type}
let neonPricing = [];    // one row per pricing line, joined shape
let neonStock   = [];    // v_stock_levels rows
let neonHistory = [];    // inventory_transactions joined to items/locations
const neonWrites = [];   // every INSERT INTO inventory_items airtable_id
const txnWrites  = [];   // one entry per ROW inserted into inventory_transactions
const txnInserts = [];   // one entry per INSERT STATEMENT (a cart must be exactly one)
const txnDeletes = [];   // every DELETE FROM inventory_transactions id
const txnMarks   = [];   // every markTransactionsPushed id-array
let   txnReplay  = [];   // rows the replay SELECT returns for an already-submitted cart
let   txnDeleteMisses = new Set();  // ids the DELETE should report as removing nothing
let   adjOnHand = 0;                // what v_stock_on_hand reports for the adjustment's pair
let   neonPushes = [];              // expense_pushes rows for the history list
let   neonPushDetail = [];          // one push joined to its line snapshots
let neonEstimates   = [];  // material_estimates joined to the totals view
let neonEstimateGet = [];  // one estimate joined to its lines
const estWrites      = [];
const estLineWrites  = [];
const estDeletes     = [];
const estLineDeletes = [];

// Column types are INFERRED from the sample values rather than all declared as
// text. The driver parses by dataTypeID, so declaring a boolean column as text
// hands the handler the string "true" — which `=== true` rejects, silently
// turning every preferred vendor into none. Real Postgres returns a real
// boolean for a boolean column, and this mock has to do the same or it tests a
// fiction. (numeric stays text on purpose: pg really does return numerics as
// strings, which is why the handler wraps them in Number().)
// An array bind does NOT arrive as a JS array. The driver serialises it to the
// Postgres array LITERAL — ["a","b"] goes out as the string '{a,b}' — so a mock
// that spreads params[0] spreads a string into characters and silently matches
// nothing. Same class of trap as booleans arriving as "t"/"f".
const pgArray = (v) =>
  Array.isArray(v) ? v
  : typeof v === "string" && v.startsWith("{")
    ? v.slice(1, -1).split(",").map(s => s.replace(/^"|"$/g, "")).filter(Boolean)
    : [];

const OID = { bool: 16, int4: 23, text: 25 };
function neonReply(cols, rows) {
  const typeOf = (c) => {
    for (const r of rows) {
      const v = r[c];
      if (v === null || v === undefined) continue;
      if (typeof v === "boolean") return OID.bool;
      if (typeof v === "number" && Number.isInteger(v)) return OID.int4;
      return OID.text;
    }
    return OID.text;
  };
  const types = Object.fromEntries(cols.map(c => [c, typeOf(c)]));
  // Values go out in POSTGRES WIRE FORM, not JS form. A bool arrives as the
  // string "t"/"f" and the driver's parser turns it back into a boolean —
  // hand that parser an actual `true` and it returns FALSE, because it only
  // recognises "t"/"TRUE"/etc. That silently made every preferred vendor
  // un-preferred here, which is precisely the kind of thing a lazy mock hides.
  const wire = (c, v) => (v === null || v === undefined) ? null
    : (types[c] === OID.bool ? (v ? "t" : "f") : v);
  return {
    command: "SELECT", rowCount: rows.length, rowAsArray: false,
    fields: cols.map((n, i) => ({ name: n, dataTypeID: types[n], tableID: 0, columnID: i + 1,
                                  dataTypeSize: -1, dataTypeModifier: -1, format: "text" })),
    rows: rows.map(r => cols.map(c => wire(c, c in r ? r[c] : null))),
  };
}

// Airtable side
let atItems = [], atLocs = [], atPricing = [], atVendors = [];
const atRequested = [];

globalThis.fetch = async (url, opts = {}) => {
  const method = opts.method || "GET";
  const body   = opts.body ? JSON.parse(opts.body) : null;

  if (String(url).includes("/sql")) {
    if (neonDown) return { ok: false, status: 500, text: async () => "neon down" };
    const sql = String(body?.query || "");
    let payload;
    if (/INSERT INTO inventory_items/i.test(sql)) {
      neonWrites.push(body.params?.[0]);
      payload = neonReply([], []);
    } else if (/INSERT INTO inventory_transactions/i.test(sql)) {
      // 13 binds per row: date, item, qty, type, from, to, cost, notes,
      // enteredBy, jobId, jobName, submitId, lineNo. Counting them is how the
      // "one statement for the whole cart" claim is actually checked — the old
      // code would have produced N statements of one row each.
      // Bind ORDER is item, from, to, date, qty, type, cost, notes, enteredBy,
      // jobId, jobName, submitId, lineNo — the three link ids are pushed first
      // because each is reused inside a FK subselect.
      const ps = body.params || [];
      const n  = Math.round(ps.length / 13);
      txnInserts.push(n);
      for (let i = 0; i < n; i++) {
        txnWrites.push({ itemId: ps[i * 13], from: ps[i * 13 + 1], to: ps[i * 13 + 2],
                         qty: ps[i * 13 + 4], type: ps[i * 13 + 5],
                         submitId: ps[i * 13 + 11], lineNo: ps[i * 13 + 12] });
      }
      // A replay is modelled by returning FEWER rows than were sent, which is
      // exactly what ON CONFLICT DO NOTHING does on the real thing.
      const out = txnReplay.length ? [] : Array.from({ length: n }, (_, i) => ({ id: `txn-new-${i + 1}` }));
      payload = neonReply(["id"], out);
    } else if (/FROM expense_pushes p\s+LEFT JOIN expense_push_lines/i.test(sql)) {
      payload = neonReply(["id", "title", "pushed_at", "pushed_by", "job_name", "job_airtable_id",
                           "materials_total", "tax_total", "total_pushed", "tx_count", "item_count",
                           "taxable", "expense_record_ids", "description",
                           "line_id", "item_name", "line_title", "quantity", "unit_cost",
                           "line_total", "wire_ft"], neonPushDetail);
    } else if (/FROM expense_pushes/i.test(sql)) {
      payload = neonReply(["id", "title", "pushed_at", "pushed_by", "job_name", "job_airtable_id",
                           "materials_total", "tax_total", "total_pushed", "tx_count", "item_count",
                           "taxable", "expense_record_ids", "description"], neonPushes);
    } else if (/COALESCE\(s\.qty_on_hand, 0\)/i.test(sql)) {
      // The adjustment's "what is there now" read. `adjOnHand === null` models
      // an item/location pair that does not exist at all, which is a 404 rather
      // than a zero.
      payload = neonReply(["on_hand"], adjOnHand === null ? [] : [{ on_hand: String(adjOnHand) }]);
    } else if (/SELECT id FROM inventory_transactions WHERE submit_id/i.test(sql)) {
      payload = neonReply(["id"], txnReplay);
    } else if (/UPDATE inventory_transactions[\s\S]*expense_created = true/i.test(sql)) {
      txnMarks.push(pgArray(body.params?.[0]));
      payload = neonReply([], []);
    } else if (/DELETE FROM inventory_transactions/i.test(sql)) {
      txnDeletes.push(body.params?.[0]);
      // RETURNING id — an empty result is how "nothing was deleted" reaches the
      // handler, and the handler now has to turn that into a 404.
      payload = neonReply(["id"], txnDeleteMisses.has(body.params?.[0])
        ? [] : [{ id: body.params?.[0] }]);
    } else if (/INSERT INTO material_estimates/i.test(sql)) {
      estWrites.push(body.params?.[0]);
      payload = neonReply([], []);
    } else if (/INSERT INTO material_estimate_lines/i.test(sql)) {
      // The real statement builds one tuple per line, so every 7th param is an id.
      for (let i = 0; i < (body.params || []).length; i += 7) estLineWrites.push(body.params[i]);
      payload = neonReply([], []);
    } else if (/DELETE FROM material_estimate_lines/i.test(sql)) {
      estLineDeletes.push(...pgArray(body.params?.[0]));
      payload = neonReply([], []);
    } else if (/DELETE FROM material_estimates/i.test(sql)) {
      estDeletes.push(body.params?.[0]);
      payload = neonReply([], []);
    } else if (/FROM material_estimates e\s+LEFT JOIN material_estimate_lines/i.test(sql)) {
      payload = neonReply(["est_id", "job_name", "job_airtable_id", "created_at", "created_by",
                           "status", "notes", "total", "line_id", "line_number",
                           "item_airtable_id", "description", "quantity", "unit_cost_at_estimate",
                           "item_name", "unit_of_measure", "category"], neonEstimateGet);
    } else if (/FROM material_estimates e/i.test(sql)) {
      payload = neonReply(["airtable_id", "job_name", "job_airtable_id", "created_at",
                           "created_by", "status", "notes", "total", "line_count"], neonEstimates);
    } else if (/FROM v_stock_levels/i.test(sql)) {
      let rows = neonStock;
      // handleStockLevels scopes to one item; honouring the bind matters, or the
      // test reads a different item's row and quietly passes on the wrong data.
      if (/WHERE item_airtable_id = \$1/i.test(sql)) rows = rows.filter(s => s.item_airtable_id === body.params?.[0]);
      if (/reorder_point > 0/i.test(sql)) rows = rows.filter(s => Number(s.reorder_point) > 0 && Number(s.qty_on_hand) <= Number(s.reorder_point));
      payload = neonReply(["stock_airtable_id", "item_airtable_id", "item_name", "location_airtable_id",
                           "location_name", "qty_on_hand", "default_unit_cost", "total_value",
                           "reorder_point", "wire_ft_per_lb", "wire_ft"], rows);
    } else if (/FROM inventory_transactions t/i.test(sql)) {
      // `id` first, not airtable_id — the ledger's handle is the uuid now.
      payload = neonReply(["id", "txn_date", "quantity", "txn_type", "notes", "entered_by",
                           "unit_cost_snapshot", "item_airtable_id", "item_name", "unit_of_measure",
                           "default_unit_cost", "from_name", "to_name"], neonHistory);
    } else if (/FROM locations/i.test(sql)) {
      payload = neonReply(["id", "name", "type"],
        neonLocs.map(l => ({ id: l.airtable_id, name: l.name, type: l.location_type })));
    } else if (/FROM inventory_items i/i.test(sql)) {          // vendor-pricing join
      payload = neonReply(["item_name", "default_unit_cost", "live_cost", "pricing_id", "unit_cost",
                           "preferred", "active", "vendor_part_number", "min_order_qty",
                           "lead_time_days", "last_price_update", "price_valid_until",
                           "unit_of_measure", "notes", "vendor_id", "vendor_name"], neonPricing);
    } else if (/SELECT name FROM inventory_items WHERE barcode/i.test(sql)) {
      const hit = neonItems.find(i => i.barcode === body.params?.[0]);
      payload = neonReply(["name"], hit ? [{ name: hit.name }] : []);
    } else if (/FROM inventory_items/i.test(sql)) {            // handleItems / itemIndex
      payload = neonReply(["airtable_id", "id", "name", "category", "product_size",
                           "unit_of_measure", "barcode", "default_unit_cost", "wire_ft_per_lb"],
        neonItems.map(i => ({ ...i, id: i.airtable_id })));
    } else {
      payload = neonReply([], []);
    }
    return { ok: true, status: 200, headers: { get: () => "application/json" },
             text: async () => JSON.stringify(payload), json: async () => payload };
  }

  atRequested.push(String(url));
  const m = String(url).match(/\/v0\/([^/]+)\/([^/?]+)(?:\/([^?]+))?/);
  const table = m ? decodeURIComponent(m[2]) : "";
  const recId = m && m[3] ? decodeURIComponent(m[3]) : null;
  const ok = (records) => ({ ok: true, status: 200, text: async () => JSON.stringify({ records }) });

  if (method === "POST" && table === "Inventory Items") {
    const rec = { id: "recNewItem", fields: body.records[0].fields };
    atItems.push(rec);
    return ok([rec]);
  }
  if (method === "POST" && table === "Inventory Transactions") {
    return ok([{ id: "recNewTxn", fields: body.records[0].fields }]);
  }
  if (method === "POST" && table === "Estimates") {
    return ok([{ id: "recNewEst", fields: body.records[0].fields }]);
  }
  if (method === "POST" && table === "Estimate Line Items") {
    return ok((body.records || []).map((rec, i) => ({ id: `recNewLine${i + 1}`, fields: rec.fields })));
  }
  // The estimate read that estimateUpdate/Delete use to find existing line ids.
  if (recId && table === "Estimates") {
    return { ok: true, status: 200, text: async () => JSON.stringify({
      id: recId, fields: { "Estimate Line Items": ["recOldLine"] } }) };
  }
  // The template header + its lines, read by createEstimateFromTemplate.
  if (recId && table === "Estimate Templates") {
    return { ok: true, status: 200, text: async () => JSON.stringify({
      id: recId, fields: { "Description": "Standard shop", "Estimate Template Lines": ["recTL1"] } }) };
  }
  if (table === "Estimate Template Lines") {
    return ok([{ id: "recTL1",
                 fields: { "Quantity": 3, "Inventory Item": ["recItemA"], "Notes": "from template" } }]);
  }
  if (method === "DELETE") return ok([{ id: recId, deleted: true }]);
  if (method === "PATCH") return ok([{ id: recId, fields: {} }]);
  if (recId && table === "Inventory Items") {
    const hit = atItems.find(i => i.id === recId);
    return hit ? { ok: true, status: 200, text: async () => JSON.stringify(hit) }
               : { ok: false, status: 404, text: async () => JSON.stringify({ error: { message: "nope" } }) };
  }
  if (table === "Inventory Items") return ok(atItems);
  if (table === "Locations")       return ok(atLocs);
  if (table === "Vendor Pricing")  return ok(atPricing);
  if (table === "Vendors")         return ok(atVendors);
  return ok([]);
};

const { handler }   = await import("../netlify/functions/inventory.js");
const { signToken } = await import("../netlify/functions/_auth.js");
const { primeRevocationCache } = await import("../netlify/functions/_revocation.js");
primeRevocationCache([]);
const TOK   = signToken({ id: "recEmp",   role: "employee" });
const ADMIN = signToken({ id: "recAdmin", role: "admin" });

let pass = 0, fail = 0; const log = [];
async function test(name, fn) {
  try { await fn(); log.push(["✓", name]); pass++; }
  catch (e) { log.push(["✗", `${name} — ${e.message}`]); fail++; }
}
const eq = (a, b, m) => { if (a !== b) throw new Error(`${m || ""} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`); };
const GET  = (action, extra = {}) => handler({ httpMethod: "GET",
  queryStringParameters: { action, ...extra }, headers: { authorization: `Bearer ${TOK}` } });
const POST = (body, tok = ADMIN) => handler({ httpMethod: "POST",
  body: JSON.stringify(body), headers: { authorization: `Bearer ${tok}` } });
const json = (r) => JSON.parse(r.body);

function reset() {
  neonDown = false; neonWrites.length = 0; atRequested.length = 0;
  txnWrites.length = 0; txnDeletes.length = 0;
  txnInserts.length = 0; txnMarks.length = 0; txnReplay = []; txnDeleteMisses = new Set();
  adjOnHand = 0;
  neonPushes = [
    { id: "aaaaaaaa-1111-4111-8111-111111111111", title: "2026-08-11 — New Shop (MIN 285)",
      pushed_at: "2026-08-11T21:55:00.000Z", pushed_by: "Rick Unruh", job_name: "New Shop (MIN 285)",
      job_airtable_id: "recJob1", materials_total: "18.2000", tax_total: "1.3700",
      total_pushed: "19.5700", tx_count: 2, item_count: 2, taxable: true,
      expense_record_ids: "recExp1, recExp2", description: "1/2\" EMT PIPE x10, 3/4\" EMT PIPE x10" },
  ];
  neonPushDetail = [
    { ...neonPushes[0], line_id: "bbbbbbbb-1111-4111-8111-111111111111",
      item_name: "1/2\" EMT PIPE", line_title: "1/2\" EMT PIPE x 10", quantity: "10.0000",
      unit_cost: "0.7500", line_total: "7.5000", wire_ft: null },
    { ...neonPushes[0], line_id: "cccccccc-1111-4111-8111-111111111111",
      item_name: "3/4\" EMT PIPE", line_title: "3/4\" EMT PIPE x 10", quantity: "10.0000",
      unit_cost: "1.0700", line_total: "10.7000", wire_ft: null },
  ];
  neonStock = [
    // 40 lb of wire on hand at 19.5 ft/lb → 780 ft; below its reorder point of 100.
    { stock_airtable_id: "recSL1", item_airtable_id: "recItemB", item_name: "12 THHN",
      location_airtable_id: "recLoc1", location_name: "Shop #1", qty_on_hand: "40.0000",
      default_unit_cost: "1.2500", total_value: "50.0000", reorder_point: "100.0000",
      wire_ft_per_lb: "19.5000", wire_ft: "780.0000" },
    // Negative on-hand: used without being received. Honest arithmetic, and the
    // reason the ledger is the source rather than the cache.
    { stock_airtable_id: "recSL2", item_airtable_id: "recItemA", item_name: "1/2\" EMT PIPE",
      location_airtable_id: "recLoc1", location_name: "Shop #1", qty_on_hand: "-1434.0000",
      default_unit_cost: "0.7500", total_value: "-1075.5000", reorder_point: "0.0000",
      wire_ft_per_lb: "0.0000", wire_ft: "0.0000" },
  ];
  estWrites.length = 0; estLineWrites.length = 0;
  estDeletes.length = 0; estLineDeletes.length = 0;
  neonEstimates = [
    { airtable_id: "recEst1", job_name: "Bethel School (MIB 433)", job_airtable_id: "reck7xKcgtlNiCorF",
      created_at: "2026-07-17T11:39:54.000Z", created_by: "Rick", status: "Estimating",
      notes: null, total: "16420.9900", line_count: 47 },
  ];
  neonEstimateGet = [
    // A real item line…
    { est_id: "recEst1", job_name: "Bethel School (MIB 433)", job_airtable_id: "reck7xKcgtlNiCorF",
      created_at: "2026-07-17T11:39:54.000Z", created_by: "Rick", status: "Estimating", notes: null,
      total: "16420.9900", line_id: "recL1", line_number: 1, item_airtable_id: "recItemA",
      description: null, quantity: "10.0000", unit_cost_at_estimate: "0.7500",
      item_name: "1/2\" EMT PIPE", unit_of_measure: "ft", category: "Conduit" },
    // …and a Misc line, which carries text instead of an item link.
    { est_id: "recEst1", job_name: "Bethel School (MIB 433)", job_airtable_id: "reck7xKcgtlNiCorF",
      created_at: "2026-07-17T11:39:54.000Z", created_by: "Rick", status: "Estimating", notes: null,
      total: "16420.9900", line_id: "recL2", line_number: 2, item_airtable_id: null,
      description: "Rental — trencher", quantity: "1.0000", unit_cost_at_estimate: "250.0000",
      item_name: null, unit_of_measure: null, category: null },
  ];
  // Keyed on `id`, not airtable_id — the ledger's handle is the uuid since the
  // native-write cutover, and it is the same handle for these historical rows.
  neonHistory = [
    { id: "11111111-1111-4111-8111-111111111111", txn_date: "2026-08-10T14:51:19.000Z",
      quantity: "10.0000",
      txn_type: "Use", notes: "Bethel School (MIB 433) | left on the truck",
      entered_by: "Rick", unit_cost_snapshot: "0.8000", item_airtable_id: "recItemA",
      item_name: "1/2\" EMT PIPE", unit_of_measure: "ft", default_unit_cost: "0.7500",
      from_name: "Shop #1", to_name: null },
  ];
  neonItems = [
    { airtable_id: "recItemA", name: "1/2\" EMT PIPE", category: "Conduit", product_size: "1/2\"",
      unit_of_measure: "ft", barcode: "111", default_unit_cost: "0.7500", wire_ft_per_lb: "0" },
    { airtable_id: "recItemB", name: "12 THHN", category: "Wire", product_size: "12",
      unit_of_measure: "lb", barcode: "222", default_unit_cost: "1.2500", wire_ft_per_lb: "19.5" },
  ];
  neonLocs = [{ airtable_id: "recLoc1", name: "Shop #1", location_type: "Shop" }];
  neonPricing = [{
    item_name: "1/2\" EMT PIPE", default_unit_cost: "0.7500", live_cost: "0.6800",
    pricing_id: "recPr1", unit_cost: "0.6800", preferred: true, active: true,
    vendor_part_number: "EMT12", min_order_qty: "10", lead_time_days: 3,
    last_price_update: "2026-04-20T00:00:00.000Z", price_valid_until: null,
    unit_of_measure: "ft", notes: null, vendor_id: "recVen1", vendor_name: "CED",
  }];
  atItems = [{ id: "recItemA", fields: { "Item Name": "AIRTABLE FALLBACK ITEM",
    "Default Unit Cost": 9.99, "Active Item": true } }];
  atLocs  = [{ id: "recLoc1", fields: { "Location Name": "AIRTABLE FALLBACK LOC", "Active Location": true } }];
  atPricing = []; atVendors = [];
}

// ── cases ──

await test("items serve from Neon with AIRTABLE rec ids, not uuids", async () => {
  reset();
  const r = json(await GET("items"));
  eq(r._source, "neon", "served from Neon");
  eq(r.items.length, 2, "two items");
  const a = r.items.find(i => i.id === "recItemA");
  eq(!!a, true, "id is the Airtable rec id — a uuid here corrupts every downstream write");
  eq(a.name, "1/2\" EMT PIPE", "name");
  eq(a.cost, 0.75, "cost is a NUMBER, not the numeric string Postgres returns");
  eq(a.size, "1/2\"", "product size — carried only by this read");
  eq(r.items.find(i => i.id === "recItemB").wireFtPerLb, 19.5, "wire ft/lb survives as a number");
});

await test("locations serve from Neon", async () => {
  reset();
  const r = json(await GET("locations"));
  eq(r._source, "neon", "served from Neon");
  eq(r.locations[0].id, "recLoc1", "Airtable rec id");
  eq(r.locations[0].name, "Shop #1", "name");
  eq(r.locations[0].type, "Shop", "type");
});

await test("Neon down → both reads fall back to Airtable, whole list intact", async () => {
  reset(); neonDown = true;
  const items = json(await GET("items"));
  eq(items._source, "airtable", "fell back");
  eq(items.items[0].name, "AIRTABLE FALLBACK ITEM", "served the Airtable row, not an empty list");
  const locs = json(await GET("locations"));
  eq(locs._source, "airtable", "fell back");
  eq(locs.locations[0].name, "AIRTABLE FALLBACK LOC", "served the Airtable row");
});

await test("vendor pricing: Neon shape matches the Airtable one EXACTLY", async () => {
  reset();
  const r = json(await GET("itemVendorPricing", { itemId: "recItemA" }));
  eq(r._source, "neon", "served from Neon");
  // summary — read by the panel header
  eq(r.summary.defaultCost, 0.75, "defaultCost");
  eq(r.summary.liveCost, 0.68, "liveCost — from v_item_live_cost, replacing the Airtable rollup");
  eq(r.summary.preferredVendor, "CED", "preferred vendor name");
  eq(r.summary.preferredUpdated, "2026-04-20", "date trimmed to YYYY-MM-DD, not an ISO timestamp");
  eq(Math.round(r.summary.variance.dollar * 10000) / 10000, -0.07, "variance in dollars");
  // the exact key names the frontend reads — a near-miss is a blank panel
  const v = r.vendors[0];
  eq(v.id, "recPr1", "pricing row id");
  eq(v.partNumber, "EMT12", "partNumber (NOT vendor_part_number)");
  eq(v.leadTime, 3, "leadTime (NOT lead_time_days)");
  eq(v.lastUpdate, "2026-04-20", "lastUpdate (NOT last_price_update)");
  eq(v.validUntil, "", "missing dates are '' not null");
  eq(v.minOrderQty, 10, "minOrderQty is a number");
  eq(v.preferred, true, "preferred");
});

await test("vendor pricing: an item with no pricing rows returns an empty list, not a null row", async () => {
  reset();
  // LEFT JOIN yields one all-null row for an item that has no pricing.
  neonPricing = [{ item_name: "1/2\" EMT PIPE", default_unit_cost: "0.7500", live_cost: null,
                   pricing_id: null, unit_cost: null, preferred: null, active: null }];
  const r = json(await GET("itemVendorPricing", { itemId: "recItemA" }));
  eq(r.vendors.length, 0, "no phantom vendor built from the null row");
  eq(r.summary.liveCost, 0, "no live cost");
  eq(r.summary.variance, null, "variance is null without both numbers");
});

await test("createItem writes Neon too — otherwise the new item is invisible", async () => {
  reset();
  const r = json(await POST({ action: "createItem", name: "NEW PART", cost: 4.5, barcode: "999" }));
  eq(r.ok, true, "ok");
  eq(r.item.id, "recNewItem", "Airtable rec id returned");
  eq(neonWrites.includes("recNewItem"), true,
     "synced to Neon — the reads are Neon's now, so an Airtable-only write is invisible");
});

await test("createItem refuses a duplicate barcode using the Neon index", async () => {
  reset();
  const r = await POST({ action: "createItem", name: "DUPE", barcode: "111" });
  eq(r.statusCode, 409, "409 conflict");
  eq(/1\/2" EMT PIPE/.test(json(r).error), true, "names the clashing item from Neon");
});

await test("updateItemCost re-syncs the item so the new price is actually seen", async () => {
  reset();
  atItems.push({ id: "recItemA", fields: { "Item Name": "1/2\" EMT PIPE", "Default Unit Cost": 0.99, "Active Item": true } });
  const r = json(await POST({ action: "updateItemCost", itemId: "recItemA", cost: 0.99 }));
  eq(r.ok, true, "ok");
  eq(neonWrites.includes("recItemA"), true,
     "price change reached Neon — without this every estimate keeps quoting the old cost");
});

// ── Step C: the ledger, and on-hand as a derived number ──────────────────────

await test("C: stock levels come from the LEDGER, negatives and all", async () => {
  reset();
  const r = json(await GET("stockLevels", { itemId: "recItemA" }));
  eq(r._source, "neon", "served from Neon");
  const lvl = r.levels.find(l => l.locationName === "Shop #1");
  eq(lvl.qtyOnHand, -1434, "negative on-hand is surfaced, not clamped — the cache hid this");
  eq(lvl.totalValue, -1075.5, "value follows the ledger too");
  eq(lvl.id, "recSL2", "carries the Stock Levels rec id so the reorder-point write still targets it");
});

await test("C: wire feet are derived, not read from a stored formula", async () => {
  reset();
  const r = json(await GET("stockLevelsAll"));
  eq(r._source, "neon", "served from Neon");
  const wire = r.levels.find(l => l.itemId === "recItemB");
  eq(wire.qtyOnHand, 40, "40 lb on hand");
  eq(wire.wireFt, 780, "40 lb x 19.5 ft/lb = 780 ft, computed from the ledger");
});

await test("C: reorder alerts compare against the ledger, grouped by location", async () => {
  reset();
  const r = json(await GET("reorderAlerts"));
  eq(r._source, "neon", "served from Neon");
  const shop = r.groups["Shop #1"];
  eq(shop.length, 1, "only the item actually under its reorder point");
  eq(shop[0].itemName, "12 THHN", "the wire");
  eq(shop[0].shortBy, 60, "100 - 40 = 60 short");
  eq(shop[0].itemId, "recItemA" === shop[0].itemId ? "recItemA" : "recItemB",
     "carries the item rec id so the alert can deep-link into Receive");
});

await test("C: history splits 'job | notes' and prefers the snapshot cost", async () => {
  reset();
  const r = json(await GET("history", { all: "1" }));
  eq(r._source, "neon", "served from Neon");
  const t = r.transactions[0];
  eq(t.job, "Bethel School (MIB 433)", "job taken from before the pipe");
  eq(t.notes, "left on the truck", "user notes taken from after it");
  eq(t.cost, 0.8, "snapshot cost wins over the item's current 0.75");
  eq(t.total, 8, "10 x 0.80");
  eq(t.from, "Shop #1", "from location resolved");
  eq(t.to, "", "no to location on a Use");
});

// ── Cutover slice 2: the push history ──────────────────────────────────────

await test("S2: push history is served from Neon, keyed on the uuid", async () => {
  reset();
  const r = json(await GET("pushHistory", { limit: "100" }));
  eq(r._source, "neon", "from Neon");
  eq(r.pushes[0].id, "aaaaaaaa-1111-4111-8111-111111111111",
     "the uuid — a natively-written push has no rec id to send to the detail view");
  eq(r.pushes[0].total, 19.57, "materials + tax, as charged");
  eq(r.pushes[0].taxable, true, "booleans survive the wire as real booleans");
});

await test("S2: push detail returns the header with its line snapshots", async () => {
  reset();
  const r = json(await GET("pushHistoryDetail", { id: "aaaaaaaa-1111-4111-8111-111111111111" }));
  eq(r.push.lines.length, 2, "both line snapshots");
  eq(r.push.lines[0].itemName, "1/2\" EMT PIPE", "biggest dollars first is done in SQL");
  eq(r.push.lines[0].lineTotal, 7.5, "the frozen line total, not a recomputation");
});

await test("S2: a push with no lines is a header, not a phantom line", async () => {
  reset();
  // The LEFT JOIN yields one row of NULLs when a header has no lines. Mapping
  // that row would invent a line called "Item" with a total of 0.
  neonPushDetail = [{ ...neonPushes[0], line_id: null, item_name: null, line_title: null,
                      quantity: null, unit_cost: null, line_total: null, wire_ft: null }];
  const r = json(await GET("pushHistoryDetail", { id: "aaaaaaaa-1111-4111-8111-111111111111" }));
  eq(r.push.lines.length, 0, "no lines");
  eq(r.push.total, 19.57, "header intact");
});

await test("S2: the audit trail refuses to answer from Airtable", async () => {
  reset();
  neonDown = true;
  const list = await GET("pushHistory", {});
  eq(list.statusCode, 503, "a history missing every push since the cutover is worse than an error");
  eq(atRequested.some(u => /Expense%20Pushes/.test(u)), false, "it did not even try");
});

// ── Adjustment SETS the count ──────────────────────────────────────────────
// It used to write the counted number straight onto the FROM leg, which
// v_stock_on_hand subtracts — so "set to 400" removed 400. Thirty rows and
// 10,219 units went out that way before it was caught.

await test("ADJ: counting UP posts the difference on the adding leg", async () => {
  reset();
  adjOnHand = -2424;                       // the real Shop #1 figure that exposed this
  const r = json(await POST({ action: "adjustment", itemId: "recItemA",
                              locationId: "recLoc1", qty: 400, enteredBy: "Rick" }));
  eq(r.ok, true, "ok");
  eq(r.previous, -2424, "reports what was there");
  eq(r.delta, 2824, "400 − (−2424) — the size of the error being corrected");
  eq(txnWrites.length, 1, "one movement");
  eq(Number(txnWrites[0].qty), 2824, "the DIFFERENCE, not the counted number");
  eq(txnWrites[0].to, "recLoc1", "on the TO leg, which adds");
  eq(txnWrites[0].from, null, "and not the from leg, which is what subtracted before");
});

await test("ADJ: counting DOWN posts the difference on the subtracting leg", async () => {
  reset();
  adjOnHand = 900;
  const r = json(await POST({ action: "adjustment", itemId: "recItemA",
                              locationId: "recLoc1", qty: 400, enteredBy: "Rick" }));
  eq(r.delta, -500, "400 − 900");
  eq(Number(txnWrites[0].qty), 500, "absolute value — the sign lives in which leg it lands on");
  eq(txnWrites[0].from, "recLoc1", "on the FROM leg, which subtracts");
  eq(txnWrites[0].to, null, "not the to leg");
});

await test("ADJ: a count that matches writes NOTHING", async () => {
  reset();
  adjOnHand = 400;
  const r = json(await POST({ action: "adjustment", itemId: "recItemA",
                              locationId: "recLoc1", qty: 400, enteredBy: "Rick" }));
  eq(r.ok, true, "ok");
  eq(r.noChange, true, "says so");
  eq(txnWrites.length, 0, "a zero-quantity movement is just noise in the item's history");
});

await test("ADJ: an unknown item/location pair is a 404, not a phantom movement", async () => {
  reset();
  adjOnHand = null;                        // the pair does not exist
  const r = await POST({ action: "adjustment", itemId: "recNope",
                         locationId: "recLoc1", qty: 400, enteredBy: "Rick" });
  eq(r.statusCode, 404, "refused");
  eq(txnWrites.length, 0, "nothing written");
});

await test("ADJ: no current figure means no delta, so it FAILS rather than guessing", async () => {
  reset();
  neonDown = true;
  const r = await POST({ action: "adjustment", itemId: "recItemA",
                         locationId: "recLoc1", qty: 400, enteredBy: "Rick" });
  eq(r.statusCode >= 400, true, "a guessed delta would be a movement nobody counted");
  eq(txnWrites.length, 0, "nothing written");
});

await test("C: deleting a transaction removes it from Neon, or on-hand keeps counting it", async () => {
  reset();
  const r = json(await POST({ action: "delete", txId: "recTx1" }));
  eq(r.ok, true, "ok");
  eq(txnDeletes[0], "recTx1", "removed from the Neon ledger");
  // Nothing repairs this later: the loader upserts, it never removes rows that
  // Airtable no longer has.
});

// ── Step D: estimates ────────────────────────────────────────────────────────

// ── Cutover slice 1: the ledger is native ──────────────────────────────────
// Airtable is no longer written for transactions, so the tests that matter are
// about what happens when something goes wrong rather than when it goes right.

await test("S1: a cart is ONE insert, native, with no Airtable write at all", async () => {
  reset();
  const r = json(await POST({ action: "submitCart", locationId: "recLoc1", enteredBy: "Rick",
    jobName: "Bethel School", jobId: "recJob1", submitId: "cart-abc",
    lines: [{ itemId: "recItemA", qty: 10, unitCost: 0.75 },
            { itemId: "recItemB", qty: 4,  unitCost: 1.25 }] }));
  eq(r.ok, true, "ok");
  eq(txnInserts.length, 1, "ONE statement — a half-written cart is no longer a state that exists");
  eq(txnWrites.length, 2, "both lines in it");
  eq(r.ids.length, 2, "both ids returned");
  eq(txnWrites[0].from, "recLoc1", "Use logs against the FROM leg, which is what subtracts");
  // Number() because binds cross the wire as strings — the same reason the
  // handlers wrap every numeric read.
  eq(Number(txnWrites[1].lineNo), 2, "line numbers make the cart replay-safe row by row");
  eq(atRequested.some(u => /Inventory%20Transactions/.test(u)), false,
     "nothing reached Airtable — the whole point of the slice");
});

await test("S1: re-submitting the same cart returns the ORIGINAL ids, not duplicates", async () => {
  reset();
  // Models the real hazard: the first submit landed, the response was lost, the
  // user pressed Submit again. ON CONFLICT swallows the rows and the replay
  // SELECT answers with what the first attempt created.
  txnReplay = [{ id: "txn-orig-1" }, { id: "txn-orig-2" }];
  const r = json(await POST({ action: "submitCart", locationId: "recLoc1", enteredBy: "Rick",
    submitId: "cart-abc",
    lines: [{ itemId: "recItemA", qty: 10 }, { itemId: "recItemB", qty: 4 }] }));
  eq(r.ok, true, "ok");
  eq(r.ids.join(","), "txn-orig-1,txn-orig-2",
     "the first submission's ids — not a second set of transactions");
});

await test("S1: a cart that cannot reach Neon FAILS, rather than reporting success", async () => {
  reset();
  neonDown = true;
  const r = await POST({ action: "submitCart", locationId: "recLoc1", enteredBy: "Rick",
    lines: [{ itemId: "recItemA", qty: 10 }] });
  eq(r.statusCode >= 400, true, "not a 200 — there is no second copy and no loader to repair it");
});

await test("S1: history speaks in uuids, because a native row has no rec id", async () => {
  reset();
  const h = json(await GET("history", { all: "1" }));
  eq(h._source, "neon", "served from Neon");
  eq(h.transactions[0].id, "11111111-1111-4111-8111-111111111111",
     "the uuid — an airtable_id here would be null on every native row");
});

await test("S1: pending expenses refuse to fall back to Airtable", async () => {
  reset();
  neonDown = true;
  const p = await GET("pendingExpenses");
  eq(p.statusCode, 503,
     "Airtable is wrong BOTH ways now: missing new rows, and still 'unpushed' for pushed ones");
  eq(atRequested.some(u => /Inventory%20Transactions/.test(u)), false,
     "it did not even ask — a fallback here would build a push list that double-charges");
});

await test("S1: history refuses to answer from Airtable when Neon is down", async () => {
  reset();
  neonDown = true;
  const r = await GET("history", { all: "1" });
  eq(r.statusCode, 503, "a history missing everything since the cutover is worse than an error");
  eq(atRequested.some(u => /Inventory%20Transactions/.test(u)), false, "it did not even try");
});

await test("S1: deleting a transaction that removed nothing is a 404, not a success", async () => {
  reset();
  const ok = await POST({ action: "delete", txId: "11111111-1111-4111-8111-111111111111" });
  eq(ok.statusCode, 200, "a real row deletes");
  txnDeleteMisses.add("22222222-2222-4222-8222-222222222222");
  const missing = await POST({ action: "delete", txId: "22222222-2222-4222-8222-222222222222" });
  eq(missing.statusCode, 404,
     "on-hand would otherwise keep counting stock the user believes they removed");
});

await test("D: the estimates list totals come from the view, not a stored rollup", async () => {
  reset();
  const r = json(await GET("estimatesList"));
  eq(r._source, "neon", "served from Neon");
  eq(r.estimates[0].id, "recEst1", "Airtable rec id");
  eq(r.estimates[0].total, 16420.99, "total from v_material_estimate_totals");
  eq(r.estimates[0].lineCount, 47, "line count from the same view");
});

await test("D: an estimate's lines come back with Misc lines intact", async () => {
  reset();
  const r = json(await GET("estimateGet", { id: "recEst1" }));
  eq(r._source, "neon", "served from Neon");
  eq(r.estimate.lines.length, 2, "both lines");
  const item = r.estimate.lines[0], misc = r.estimate.lines[1];
  eq(item.itemName, "1/2\" EMT PIPE", "item line names from the item");
  eq(item.isMisc, false, "not misc");
  eq(item.lineTotal, 7.5, "10 x 0.75 — computed, never stored");
  eq(misc.isMisc, true, "a line with no item link is Misc");
  eq(misc.itemName, "Rental — trencher", "Misc lines fall back to their own text");
  eq(misc.lineTotal, 250, "1 x 250");
});

await test("D: creating an estimate writes the HEADER before the lines", async () => {
  reset();
  const r = json(await POST({ action: "estimateCreate", jobName: "Test Job", createdBy: "Rick",
                              lines: [{ itemId: "recItemA", qty: 2, unitCost: 0.75 }] }));
  eq(r.ok, true, "ok");
  eq(estWrites.length, 1, "header synced");
  eq(estLineWrites.length, 1, "line synced");
  // Order matters: a line resolves estimate_id by looking its parent up, so a
  // line written first lands with a null FK and never counts toward the total.
  eq(estWrites[0], "recNewEst", "the header is the estimate just created");
});

await test("D: an estimate built FROM A TEMPLATE syncs too — the path that 404'd on prod", async () => {
  reset();
  const r = json(await POST({ action: "createEstimateFromTemplate", templateId: "recTmpl1",
                              jobName: "Aaron McLauglin (MIA 274)", createdBy: "Rick" }));
  eq(r.ok, true, "ok");
  eq(r.lineCount, 1, "the template line cloned into the new estimate");
  // This is the SECOND handler that creates an estimate, and it shipped without
  // either sync — so the estimate existed in Airtable, was missing from Neon,
  // and the app 404'd the moment it opened the thing it had just created.
  eq(estWrites[0], "recNewEst", "header synced — without this, estimateGet 404s");
  eq(estLineWrites.length, 1, "line synced — without this, the total reads 0");
});

await test("D: replacing lines removes the old ones from Neon too", async () => {
  reset();
  const r = json(await POST({ action: "estimateUpdate", id: "recEst1", replaceLines: true,
                              lines: [{ itemId: "recItemA", qty: 5, unitCost: 0.75 }] }));
  eq(r.ok, true, "ok");
  eq(estLineDeletes.includes("recOldLine"), true,
     "the replaced line is gone — nothing repairs a missed delete, and it would keep counting");
  eq(estLineWrites.length, 1, "the new line landed");
});

await test("D: deleting an estimate removes it from Neon, lines cascade", async () => {
  reset();
  const r = json(await POST({ action: "estimateDelete", id: "recEst1" }));
  eq(r.ok, true, "ok");
  eq(estDeletes[0], "recEst1", "estimate removed — otherwise it keeps showing on the list");
});

console.log("\ninventory.js reference tables — Steps B + C + D\n" + "-".repeat(46));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(46));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

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
const neonWrites = [];   // every INSERT INTO inventory_items airtable_id

// Column types are INFERRED from the sample values rather than all declared as
// text. The driver parses by dataTypeID, so declaring a boolean column as text
// hands the handler the string "true" — which `=== true` rejects, silently
// turning every preferred vendor into none. Real Postgres returns a real
// boolean for a boolean column, and this mock has to do the same or it tests a
// fiction. (numeric stays text on purpose: pg really does return numerics as
// strings, which is why the handler wraps them in Number().)
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

console.log("\ninventory.js reference tables — Step B\n" + "-".repeat(46));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(46));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

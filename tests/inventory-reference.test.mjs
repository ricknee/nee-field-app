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
const txnInsertSql = []; // the SQL text of each, so a test can check how its FKs resolve
const txnDeletes = [];   // every DELETE FROM inventory_transactions id
const txnMarks   = [];   // every markTransactionsPushed id-array
let   txnReplay  = [];   // rows the replay SELECT returns for an already-submitted cart
let   txnDeleteMisses = new Set();  // ids the DELETE should report as removing nothing
let   adjOnHand = 0;                // what v_stock_on_hand reports for the adjustment's pair
const stockUpdates = [];            // every reorder-point UPDATE
const stockCreates = [];            // every stock_settings upsert
let   stockMissing = new Set();     // ids the UPDATE should report as matching nothing
let   neonPushes = [];              // expense_pushes rows for the history list
let   neonPushDetail = [];          // one push joined to its line snapshots
let neonEstimates   = [];  // material_estimates joined to the totals view
let neonEstimateGet = [];  // one estimate joined to its lines
const estWrites      = [];
const estLineWrites  = [];
const estDeletes     = [];
const estLineDeletes = [];
const estUpdates     = [];
let   estMissing     = new Set();   // estimate ids that should match nothing
const EST1           = "e5717a7e-0000-4000-8000-000000000001";
const NEW_ITEM       = "17e11111-1111-4111-8111-111111111111";
let   bulkOnHand     = [];
const itemCostWrites = [];
const itemEdits      = [];
const itemDeletes    = [];
let   itemRefs       = { txns:0, est_lines:0, tmpl_lines:0, order_lines:0, pricing:0 };
let   itemMissing    = new Set();
const NEW_EST        = "e5717a7e-1111-4111-8111-111111111111";
let   tmplCloneLines = [];          // what the from-template INSERT..SELECT copies

// ── TWO ID SPACES, and the mock has to model both ──────────────────────────
// The slice-1 outage shipped past a green suite because the mock used the SAME
// STRING for the uuid and the rec id, so asking for the wrong column still
// matched. Everything below keeps them genuinely different values, and adds a
// row for each entity that has NO rec id at all — which is what an item or
// location created in the app since the cutover actually looks like.
//
// A native row's handle is its uuid: `COALESCE(airtable_id, id::text)`, exactly
// what handleItems and handleLocations serve to the pickers. Any reader that
// filters on `*_airtable_id` instead of `*_handle` will match nothing here, and
// that is the bug class these fixtures exist to catch (db/schema/043).
const NATIVE_ITEM = "17e11111-2222-4222-8222-222222222222";  // airtable_id NULL
const NATIVE_LOC  = "10c11111-3333-4333-8333-333333333333";  // airtable_id NULL
const LOC1_UUID   = "10c11111-1111-4111-8111-111111111111";  // recLoc1's uuid
const ITEMA_UUID  = "17e11111-1111-4111-8111-111111111111";  // recItemA's uuid
const handleOf = (recId, uuid) => recId || uuid;

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
      // Born here, so it returns its own row — there is no Airtable record to
      // mirror and no rec id to hand back.
      neonWrites.push(body.params?.[0]);
      payload = neonReply(["id", "name", "category", "product_size", "unit_of_measure",
                           "barcode", "default_unit_cost", "wire_ft_per_lb"],
        [{ id: NEW_ITEM, name: body.params?.[0], category: body.params?.[1],
           product_size: body.params?.[2], unit_of_measure: body.params?.[3],
           barcode: body.params?.[4], default_unit_cost: body.params?.[5],
           wire_ft_per_lb: body.params?.[6] }]);
    } else if (/COALESCE\(i\.default_unit_cost, 0\) AS cost/i.test(sql)) {
      // bulkResolveItems — matches on barcode OR lower(name), both as arrays.
      // ⚠ pgArray() above is the SIMPLE splitter and it is not enough here: a
      // real item name is `1/2" EMT PIPE`, which Postgres serialises as a
      // QUOTED, BACKSLASH-ESCAPED element. Splitting on commas and trimming
      // quotes mangles it, every name misses, and the whole upload silently
      // reports "no matching item" — which is exactly the failure this suite
      // exists to catch, so the mock has to parse the literal properly.
      const parseArr = (v) => {
        if (Array.isArray(v)) return v;
        if (typeof v !== "string" || !v.startsWith("{")) return [];
        const out = []; let cur = "", inQ = false;
        for (let i = 1; i < v.length - 1; i++) {
          const c = v[i];
          if (inQ) {
            if (c === "\\") cur += v[++i];
            else if (c === '"') inQ = false;
            else cur += c;
          } else if (c === '"') inQ = true;
          else if (c === ",") { out.push(cur); cur = ""; }
          else cur += c;
        }
        if (cur !== "") out.push(cur);
        return out;
      };
      const bcs   = parseArr(body.params?.[0]);
      const names = parseArr(body.params?.[1]);
      payload = neonReply(["id", "handle", "name", "barcode", "cost", "active"],
        neonItems
          .filter(i => bcs.includes(i.barcode) || names.includes(String(i.name).toLowerCase()))
          .map(i => ({ id: i.airtable_id || i.id, handle: i.airtable_id || i.id, name: i.name,
                       barcode: i.barcode, cost: i.default_unit_cost, active: true })));
    } else if (/SELECT id, COALESCE\(airtable_id, id::text\) AS handle, name FROM vendors/i.test(sql)) {
      payload = neonReply(["id", "handle", "name"],
        [{ id: "v-1", handle: "recVend1", name: "Wolff Bros" }]);
    } else if (/SELECT id, COALESCE\(airtable_id, id::text\) AS handle, name FROM locations/i.test(sql)) {
      payload = neonReply(["id", "handle", "name"],
        [{ id: "l-1", handle: "recLoc1", name: "Shop #1" }]);
    } else if (/SELECT item_id, location_id, qty_on_hand FROM v_stock_on_hand/i.test(sql)) {
      payload = neonReply(["item_id", "location_id", "qty_on_hand"], bulkOnHand);
    } else if (/SELECT name FROM inventory_items[\s\S]*NOT \(airtable_id/i.test(sql)) {
      // The edit-form barcode guard, which must exclude the item itself.
      const hit = neonItems.find(i => i.barcode === body.params?.[0]
        && i.airtable_id !== body.params?.[1] && i.id !== body.params?.[1]);
      payload = neonReply(["name"], hit ? [{ name: hit.name }] : []);
    } else if (/UPDATE inventory_items SET[\s\S]*product_size/i.test(sql)) {
      itemEdits.push({ handle: body.params?.[0], name: body.params?.[1], active: body.params?.[8] });
      payload = neonReply(["id", "name", "category", "product_size", "unit_of_measure",
                           "barcode", "default_unit_cost", "wire_ft_per_lb", "active"],
        itemMissing.has(body.params?.[0]) ? [] :
        [{ id: body.params?.[0], name: body.params?.[1] || "TEST PART", category: body.params?.[2],
           product_size: body.params?.[3], unit_of_measure: body.params?.[4],
           barcode: body.params?.[5], default_unit_cost: body.params?.[6],
           wire_ft_per_lb: body.params?.[7], active: body.params?.[8] !== false }]);
    } else if (/FROM inventory_items i\s+WHERE i.airtable_id = \$1 OR i.id::text = \$1/i.test(sql)
               && /inventory_transactions x/i.test(sql)) {
      // itemDelete's reference count.
      payload = neonReply(["id", "name", "txns", "est_lines", "tmpl_lines", "order_lines", "pricing"],
        itemMissing.has(body.params?.[0]) ? []
          : [{ id: NEW_ITEM, name: "TEST PART", ...itemRefs }]);
    } else if (/DELETE FROM inventory_items/i.test(sql)) {
      itemDeletes.push(body.params?.[0]);
      payload = neonReply([], []);
    } else if (/UPDATE inventory_items SET default_unit_cost/i.test(sql)) {
      itemCostWrites.push({ handle: body.params?.[0], cost: body.params?.[1] });
      payload = neonReply(["id"], itemMissing.has(body.params?.[0]) ? [] : [{ id: NEW_ITEM }]);
    } else if (/'Adjustment',\s*\n?\s*\$4/.test(sql) || /\$3, 'Adjustment'/.test(sql)) {
      // The BULK count insert: 9 binds, a different shape from insertTxns.
      const ps = body.params || [];
      txnWrites.push({ itemId: ps[0], qty: ps[2], from: ps[3], to: ps[5], notes: ps[7] });
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
      txnInsertSql.push(sql);
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
    } else if (/UPDATE stock_settings SET reorder_point/i.test(sql)) {
      stockUpdates.push({ id: body.params?.[0], reorderPoint: body.params?.[1] });
      // RETURNING id — empty means the uuid matched nothing, which the handler
      // has to turn into a 404 rather than a cheerful ok.
      payload = neonReply(["id"], stockMissing.has(body.params?.[0]) ? [] : [{ id: body.params?.[0] }]);
    } else if (/INSERT INTO stock_settings/i.test(sql)) {
      stockCreates.push({ itemId: body.params?.[0], locationId: body.params?.[1],
                          reorderPoint: body.params?.[2], sql });
      // ⚠ This models INSERT…SELECT, not INSERT…VALUES.
      //
      // The old shape resolved the FKs as scalar subselects inside VALUES, so an
      // unknown handle still INSERTED a row — one with NULL ids, sitting outside
      // the partial unique index and outside v_stock_levels. The handler caught
      // it and returned 404, so the user was told it failed while the row
      // existed. As a SELECT there is nothing to insert, so an unresolved handle
      // returns NO ROW — which is what this returns.
      //
      // Both handle forms resolve, because that is what the app serves.
      const itemOk = ["recItemA", ITEMA_UUID, NATIVE_ITEM].includes(body.params?.[0]);
      const locOk  = ["recLoc1",  LOC1_UUID,  NATIVE_LOC ].includes(body.params?.[1]);
      payload = neonReply(["id"], (itemOk && locOk)
        ? [{ id: "5da5a5a5-9999-4999-8999-999999999999" }]
        : []);
    } else if (/COALESCE\(s\.qty_on_hand, 0\)/i.test(sql)) {
      // The adjustment's "what is there now" read. `adjOnHand === null` models
      // an item/location pair that does not exist at all, which is a 404 rather
      // than a zero.
      // item_id/location_id are selected by the BULK apply path too, and a row
      // missing them would insert a transaction attached to nothing.
      payload = neonReply(["item_id", "location_id", "on_hand"], adjOnHand === null ? []
        : [{ item_id: "recItemA", location_id: "l-1", on_hand: String(adjOnHand) }]);
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
      // The estimate is BORN here now — it returns its own uuid rather than
      // mirroring one Airtable handed over.
      estWrites.push(body.params?.[0]);
      payload = neonReply(["id"], [{ id: NEW_EST }]);
    // Anchored on the template table, not just on the word SELECT: the ordinary
    // per-line insert resolves its item FK with a subselect, so a looser match
    // caught both statements and quietly double-counted the lines.
    } else if (/FROM material_estimate_template_lines tl/i.test(sql)) {
      // The from-template clone: one INSERT ... SELECT, no per-line binds.
      estLineWrites.push(...tmplCloneLines);
      payload = neonReply(["id"], tmplCloneLines.map((_, i) => ({ id: `line-${i + 1}` })));
    } else if (/INSERT INTO material_estimate_lines/i.test(sql)) {
      // 8 binds per line: estimate, line_no, item (twice via the FK subselect
      // is ONE bind), qty, cost, description.
      const ps = body.params || [];
      const n = Math.round(ps.length / 6);
      for (let i = 0; i < n; i++) estLineWrites.push(ps[i * 6 + 2]);
      payload = neonReply([], []);
    } else if (/DELETE FROM material_estimate_lines/i.test(sql)) {
      // One DELETE by parent uuid, not a list of line ids.
      estLineDeletes.push(body.params?.[0]);
      payload = neonReply([], []);
    } else if (/UPDATE material_estimates SET/i.test(sql)) {
      estUpdates.push(body.params?.[0]);
      payload = neonReply(["id"], estMissing.has(body.params?.[0]) ? [] : [{ id: body.params?.[0] }]);
    } else if (/DELETE FROM material_estimates/i.test(sql)) {
      estDeletes.push(body.params?.[0]);
      payload = neonReply(["id"], estMissing.has(body.params?.[0]) ? [] : [{ id: body.params?.[0] }]);
    } else if (/SELECT id, description FROM material_estimate_templates/i.test(sql)) {
      payload = neonReply(["id", "description"], [{ id: body.params?.[0], description: "Standard shop" }]);
    } else if (/FROM material_estimates e\s+LEFT JOIN material_estimate_lines/i.test(sql)) {
      payload = neonReply(["est_id", "job_name", "job_airtable_id", "created_at", "created_by",
                           "status", "notes", "total", "line_id", "line_number",
                           "item_airtable_id", "description", "quantity", "unit_cost_at_estimate",
                           "item_name", "unit_of_measure", "category"], neonEstimateGet);
    } else if (/FROM material_estimates e/i.test(sql)) {
      payload = neonReply(["id", "job_name", "job_airtable_id", "created_at",
                           "created_by", "status", "notes", "total", "line_count"], neonEstimates);
    } else if (/FROM v_stock_levels/i.test(sql)) {
      let rows = neonStock;
      // handleStockLevels scopes to one item; honouring the bind matters, or the
      // test reads a different item's row and quietly passes on the wrong data.
      //
      // ⚠⚠ The filter is applied on whichever column the SQL actually NAMED.
      // Matching on `item_handle` while the handler still asks for
      // `item_airtable_id` is how the mock would hide the bug instead of
      // catching it — the same "one id space" mistake that let the slice-1
      // outage ship. Ask for the rec-id column and you get rec-id matching,
      // which returns nothing for the native row.
      if (/WHERE item_handle = \$1/i.test(sql)) {
        rows = rows.filter(s => s.item_handle === body.params?.[0]);
      } else if (/WHERE item_airtable_id = \$1/i.test(sql)) {
        rows = rows.filter(s => s.item_airtable_id === body.params?.[0]);
      }
      if (/reorder_point > 0/i.test(sql)) rows = rows.filter(s => Number(s.reorder_point) > 0 && Number(s.qty_on_hand) <= Number(s.reorder_point));
      payload = neonReply(["stock_id", "stock_airtable_id", "item_airtable_id", "item_name",
                           "item_handle",
                           "location_airtable_id", "location_name", "location_handle",
                           "qty_on_hand",
                           "default_unit_cost", "total_value",
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
      // ⚠ The WHERE is HONOURED here, and it has to be. A mock that returns
      // every row regardless of the filter cannot see a query that excludes
      // exactly the rows you care about — which is how
      // `COALESCE(airtable_id,'') <> ''` hid every natively-created item while
      // the tests stayed green.
      let rows = neonItems;
      if (/COALESCE\(airtable_id,''\) <> ''/.test(sql)) rows = rows.filter(i => i.airtable_id);
      // The handler selects COALESCE(airtable_id, id::text) AS id — the dual
      // handle — so a native row answers with its uuid.
      payload = neonReply(["airtable_id", "id", "name", "category", "product_size",
                           "unit_of_measure", "barcode", "default_unit_cost", "wire_ft_per_lb"],
        rows.map(i => ({ ...i, airtable_id: i.airtable_id || i.id, id: i.airtable_id || i.id })));
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
// For claims that are not an equality — "this row exists", "this SQL has this
// shape". `eq(!!x, true)` reports "expected true, got false", which tells you
// nothing about what was actually being asserted.
const ok = (v, m) => { if (!v) throw new Error(m || "expected a truthy value"); };
const GET  = (action, extra = {}) => handler({ httpMethod: "GET",
  queryStringParameters: { action, ...extra }, headers: { authorization: `Bearer ${TOK}` } });
const POST = (body, tok = ADMIN) => handler({ httpMethod: "POST",
  body: JSON.stringify(body), headers: { authorization: `Bearer ${tok}` } });
const json = (r) => JSON.parse(r.body);

function reset() {
  neonDown = false; neonWrites.length = 0; atRequested.length = 0;
  txnWrites.length = 0; txnDeletes.length = 0;
  txnInserts.length = 0; txnMarks.length = 0; txnReplay = []; txnDeleteMisses = new Set();
  txnInsertSql.length = 0;
  adjOnHand = 0;
  stockUpdates.length = 0; stockCreates.length = 0; stockMissing = new Set();
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
  // ⚠ `item_handle` / `location_handle` are the columns the app reads. They are
  // spelled out per row rather than derived, so a row that gets them wrong is
  // visible in the fixture rather than hidden behind a helper.
  neonStock = [
    // 40 lb of wire on hand at 19.5 ft/lb → 780 ft; below its reorder point of 100.
    { stock_id: "5da5a5a5-1111-4111-8111-111111111111",
      stock_airtable_id: "recSL1", item_airtable_id: "recItemB", item_name: "12 THHN",
      item_handle: "recItemB",
      location_airtable_id: "recLoc1", location_name: "Shop #1", location_handle: "recLoc1",
      qty_on_hand: "40.0000",
      default_unit_cost: "1.2500", total_value: "50.0000", reorder_point: "100.0000",
      wire_ft_per_lb: "19.5000", wire_ft: "780.0000" },
    // Negative on-hand: used without being received. Honest arithmetic, and the
    // reason the ledger is the source rather than the cache.
    { stock_id: "5da5a5a5-2222-4222-8222-222222222222",
      stock_airtable_id: "recSL2", item_airtable_id: "recItemA", item_name: "1/2\" EMT PIPE",
      item_handle: "recItemA",
      location_airtable_id: "recLoc1", location_name: "Shop #1", location_handle: "recLoc1",
      qty_on_hand: "-1434.0000",
      default_unit_cost: "0.7500", total_value: "-1075.5000", reorder_point: "0.0000",
      wire_ft_per_lb: "0.0000", wire_ft: "0.0000" },
    // ── The row that only exists after the cutover ──────────────────────────
    // An item created in the app, stocked at a location created in the app.
    // BOTH rec ids are NULL, so both handles are uuids. Every reader that still
    // filters on or returns `*_airtable_id` fails on exactly this row — which is
    // the whole point of it being here. Under its reorder point, so it must also
    // appear in the alerts.
    { stock_id: "5da5a5a5-3333-4333-8333-333333333333",
      stock_airtable_id: null, item_airtable_id: null, item_name: "TEST PART",
      item_handle: NATIVE_ITEM,
      location_airtable_id: null, location_name: "Shop #3", location_handle: NATIVE_LOC,
      qty_on_hand: "2.0000",
      default_unit_cost: "3.0000", total_value: "6.0000", reorder_point: "10.0000",
      wire_ft_per_lb: "0.0000", wire_ft: "0.0000" },
  ];
  estWrites.length = 0; estLineWrites.length = 0;
  estDeletes.length = 0; estLineDeletes.length = 0;
  estUpdates.length = 0; estMissing = new Set();
  itemCostWrites.length = 0; itemMissing = new Set();
  bulkOnHand = [{ item_id: "recItemA", location_id: "l-1", qty_on_hand: "40" }];
  itemEdits.length = 0; itemDeletes.length = 0;
  itemRefs = { txns:0, est_lines:0, tmpl_lines:0, order_lines:0, pricing:0 };
  tmplCloneLines = ["recItemA", "recItemB"];
  neonEstimates = [
    { id: EST1, job_name: "Bethel School (MIB 433)", job_airtable_id: "reck7xKcgtlNiCorF",
      created_at: "2026-07-17T11:39:54.000Z", created_by: "Rick", status: "Estimating",
      notes: null, total: "16420.9900", line_count: 47 },
  ];
  neonEstimateGet = [
    // A real item line…
    { est_id: EST1, job_name: "Bethel School (MIB 433)", job_airtable_id: "reck7xKcgtlNiCorF",
      created_at: "2026-07-17T11:39:54.000Z", created_by: "Rick", status: "Estimating", notes: null,
      total: "16420.9900", line_id: "recL1", line_number: 1, item_airtable_id: "recItemA",
      description: null, quantity: "10.0000", unit_cost_at_estimate: "0.7500",
      item_name: "1/2\" EMT PIPE", unit_of_measure: "ft", category: "Conduit" },
    // …and a Misc line, which carries text instead of an item link.
    { est_id: EST1, job_name: "Bethel School (MIB 433)", job_airtable_id: "reck7xKcgtlNiCorF",
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

await test("S5: reference reads REFUSE to fall back to a frozen Airtable", async () => {
  reset(); neonDown = true;
  // Items and locations kept an Airtable fallback right up to slice 5. It goes
  // with the cutover: that copy is missing every item created since and every
  // price moved since, and reference data quietly a day out of date is how an
  // estimate gets quoted at last week's cost.
  eq((await GET("items")).statusCode, 503, "items refuse");
  eq((await GET("locations")).statusCode, 503, "locations refuse");
  eq(atRequested.some(u => /Inventory%20Items|Locations/.test(u)), false, "neither even tried");
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

await test("S5: creating an item is native, and its handle is the uuid", async () => {
  reset();
  const r = json(await POST({ action: "createItem", name: "NEW PART", cost: 4.5, barcode: "999" }));
  eq(r.ok, true, "ok");
  eq(r.item.id, NEW_ITEM, "the uuid Postgres minted — a native item has no rec id");
  eq(r.item.name, "NEW PART", "and the row it returned, not a re-read");
  eq(atRequested.some(u => /Inventory%20Items/.test(u)), false, "nothing reached Airtable");
});

await test("createItem refuses a duplicate barcode using the Neon index", async () => {
  reset();
  const r = await POST({ action: "createItem", name: "DUPE", barcode: "111" });
  eq(r.statusCode, 409, "409 conflict");
  eq(/1\/2" EMT PIPE/.test(json(r).error), true, "names the clashing item from Neon");
});

await test("S5: a cost change is one UPDATE, and accepts either handle form", async () => {
  reset();
  const r = json(await POST({ action: "updateItemCost", itemId: "recItemA", cost: 0.99 }));
  eq(r.ok, true, "ok");
  eq(itemCostWrites[0].handle, "recItemA", "a historical item is still found by its rec id");
  eq(Number(itemCostWrites[0].cost), 0.99, "the new price");

  // And the other half of the dual handle: an item created since the cutover.
  reset();
  json(await POST({ action: "updateItemCost", itemId: NEW_ITEM, cost: 1.25 }));
  eq(itemCostWrites[0].handle, NEW_ITEM, "a native item is found by its uuid");
});

await test("S5: a NATIVE item appears in the list and in the shared index", async () => {
  reset();
  // The item created in the app has no airtable_id. Three queries used to carry
  // `WHERE COALESCE(airtable_id,'') <> ''` — a Step-B guard that meant "skip
  // malformed rows" and quietly came to mean "skip every item created since the
  // cutover". The item saved fine and then vanished on refresh, and the shared
  // index dropped it too, so it would not have priced in a cart either.
  neonItems.push({ airtable_id: null, id: NEW_ITEM, name: "TEST PART", category: "EMT Fittings",
                   product_size: null, unit_of_measure: "ea", barcode: null,
                   default_unit_cost: "0.9900", wire_ft_per_lb: null });
  const r = json(await GET("items"));
  eq(r.items.some(i => i.name === "TEST PART"), true, "it is in the list");
  eq(r.items.find(i => i.name === "TEST PART").cost, 0.99, "with its price");
});

// ── Bulk upload ────────────────────────────────────────────────────────────
// This one writes hundreds of rows from a file somebody was emailed, so the
// tests that matter are the refusals, not the happy path.

await test("BULK counts: preview reports the DELTA, and writes nothing", async () => {
  reset();
  const r = json(await POST({ action: "bulkPreview", type: "counts",
    rows: [{ item: "1/2\" EMT PIPE", location: "Shop #1", counted: "55" }] }));
  eq(r.ok, true, "ok");
  eq(r.rows[0].before, 40, "what the ledger says");
  eq(r.rows[0].counted, 55, "what was counted");
  eq(r.rows[0].delta, 15, "the difference — this is what gets posted");
  eq(txnWrites.length, 0, "PREVIEW WRITES NOTHING");
});

await test("BULK counts: a row that already agrees is not a change", async () => {
  reset();
  const r = json(await POST({ action: "bulkPreview", type: "counts",
    rows: [{ item: "1/2\" EMT PIPE", location: "Shop #1", counted: "40" }] }));
  eq(r.willAdd, 0, "nothing to do");
  eq(r.unchanged, 1, "counted separately — on a real count day most rows are these");
});

await test("BULK counts: apply RECOMPUTES the delta rather than trusting the preview", async () => {
  reset();
  // The preview said +15 from 40. Someone has since used 10, so on-hand is 30
  // and the honest adjustment is +25. Carrying the preview's 15 would land on
  // 45 and silently undo their movement.
  adjOnHand = 30;
  const r = json(await POST({ action: "bulkApply", type: "counts", enteredBy: "Rick",
    rows: [{ row: 1, itemHandle: "recItemA", itemName: "1/2\" EMT PIPE",
             locationHandle: "recLoc1", counted: 55, delta: 15, before: 40 }] }));
  eq(r.applied, 1, "applied");
  eq(Number(txnWrites[0].qty), 25, "posted 25, not the preview's 15");
  eq(txnWrites[0].to, "recLoc1", "on the adding leg");
});

await test("BULK: an unmatched item is skipped with a reason, not guessed at", async () => {
  reset();
  const r = json(await POST({ action: "bulkPreview", type: "counts",
    rows: [{ item: "NO SUCH PART", location: "Shop #1", counted: "5" }] }));
  eq(r.willAdd, 0, "nothing to do");
  eq(r.skipped.length, 1, "one skipped");
  eq(r.skipped[0].why, "No matching item", "and it says why");
  eq(r.skipped[0].row, 1, "with the row number, so it can be found in the file");
});

await test("BULK items: a name that already exists is refused, not duplicated", async () => {
  reset();
  const r = json(await POST({ action: "bulkPreview", type: "items",
    rows: [{ item: "1/2\" EMT PIPE", cost: "1.00" },
           { item: "GENUINELY NEW PART", cost: "2.00" }] }));
  eq(r.willAdd, 1, "only the new one");
  eq(/already exists/.test(r.skipped[0].why), true, "the existing name is refused");
  eq(r.rows[0].name, "GENUINELY NEW PART", "names upper-cased, as on the single-item form");
  eq(r.rows[0].active, true, "absent 'active' means active — a catalogue is things you stock");
});

await test("BULK pricing: an unknown vendor is refused by name", async () => {
  reset();
  const r = json(await POST({ action: "bulkPreview", type: "pricing",
    rows: [{ item: "1/2\" EMT PIPE", vendor: "Nobody Ltd", cost: "1.00" }] }));
  eq(r.willAdd, 0, "nothing to do");
  eq(/No vendor called "Nobody Ltd"/.test(r.skipped[0].why), true, "names the vendor it could not find");
});

await test("BULK: a file bigger than the cap is refused before any work", async () => {
  reset();
  const rows = Array.from({ length: 5001 }, () => ({ item: "x", location: "y", counted: "1" }));
  const r = await POST({ action: "bulkPreview", type: "counts", rows });
  eq(r.statusCode, 400, "refused");
  eq(/5001/.test(json(r).error), true, "says how many it saw");
});

// ── Editing an item — new with the cutover ─────────────────────────────────
// While Airtable was the authority you edited an item there. Once items are
// born in Postgres and nobody opens Airtable, there was no way to fix a typo,
// correct a cost, or retire a part at all.

await test("EDIT: an item can be corrected, by either handle form", async () => {
  reset();
  const r = json(await POST({ action: "itemUpdate", itemId: NEW_ITEM,
                              name: "test part", cost: 1.25, active: true }));
  eq(r.ok, true, "ok");
  eq(itemEdits[0].handle, NEW_ITEM, "found by uuid");
  eq(itemEdits[0].name, "TEST PART", "names are upper-cased, same as on create");
});

await test("EDIT: nothing to update is refused rather than writing a no-op", async () => {
  reset();
  const r = await POST({ action: "itemUpdate", itemId: NEW_ITEM });
  eq(r.statusCode, 400, "400");
  eq(itemEdits.length, 0, "no statement ran");
});

await test("EDIT: a blank name is refused — it is the only required field", async () => {
  reset();
  const r = await POST({ action: "itemUpdate", itemId: NEW_ITEM, name: "   " });
  eq(r.statusCode, 400, "400");
});

await test("EDIT: a barcode already on ANOTHER item is refused", async () => {
  reset();
  // recItemA carries barcode 111 in the fixture.
  const r = await POST({ action: "itemUpdate", itemId: NEW_ITEM, barcode: "111" });
  eq(r.statusCode, 409, "409 conflict");
  eq(/1\/2" EMT PIPE/.test(json(r).error), true, "names the item holding it");
});

await test("EDIT: re-saving an item's OWN barcode is not a clash with itself", async () => {
  reset();
  const r = json(await POST({ action: "itemUpdate", itemId: "recItemA", barcode: "111" }));
  eq(r.ok, true, "allowed — the guard excludes the row being edited");
});

await test("DELETE: an orphaned item is removed", async () => {
  reset();
  const r = json(await POST({ action: "itemDelete", itemId: NEW_ITEM }));
  eq(r.ok, true, "ok");
  eq(itemDeletes.length, 1, "one delete");
});

await test("DELETE: an item with history is REFUSED, and told what to do instead", async () => {
  reset();
  itemRefs = { txns: 4, est_lines: 2, tmpl_lines: 0, order_lines: 0, pricing: 0 };
  const res = await POST({ action: "itemDelete", itemId: NEW_ITEM });
  const r = json(res);
  eq(res.statusCode, 409, "refused");
  eq(itemDeletes.length, 0, "nothing deleted");
  // Deleting an item on an old estimate would blank that line — history would
  // silently change. Retiring it is the correct answer, so the error says so.
  eq(/4 stock movement/.test(r.error), true, "counts what blocks it");
  eq(/2 estimate line/.test(r.error), true, "all of it, not just the first");
  eq(/Untick Active/i.test(r.error), true, "and names the alternative");
});

await test("S5: a cost change that matched no item is a 404", async () => {
  reset();
  itemMissing.add("recGONE");
  const r = await POST({ action: "updateItemCost", itemId: "recGONE", cost: 1 });
  eq(r.statusCode, 404, "estimates quote this number — a silent no-op is money");
});

// ── Step C: the ledger, and on-hand as a derived number ──────────────────────

await test("C: stock levels come from the LEDGER, negatives and all", async () => {
  reset();
  const r = json(await GET("stockLevels", { itemId: "recItemA" }));
  eq(r._source, "neon", "served from Neon");
  const lvl = r.levels.find(l => l.locationName === "Shop #1");
  eq(lvl.qtyOnHand, -1434, "negative on-hand is surfaced, not clamped — the cache hid this");
  eq(lvl.totalValue, -1075.5, "value follows the ledger too");
  eq(lvl.id, "5da5a5a5-2222-4222-8222-222222222222",
     "carries the stock setting's UUID — the handle the reorder-point write now targets");
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
  // Was `eq(shop[0].itemId, "recItemA" === shop[0].itemId ? "recItemA" : "recItemB")`
  // — which compares the value against itself and can never fail. It stayed
  // green through the whole of F-02, including while this field was "".
  eq(shop[0].itemId, "recItemB",
     "carries the item handle so the alert can deep-link into Receive");
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

// ── Cutover slice 3: reorder points are native ─────────────────────────────

await test("S3: a reorder point is written to Neon only, keyed on the uuid", async () => {
  reset();
  const r = json(await POST({ action: "updateReorderPoint",
                              stockLevelId: "5da5a5a5-2222-4222-8222-222222222222",
                              reorderPoint: 250 }));
  eq(r.ok, true, "ok");
  eq(stockUpdates.length, 1, "one update");
  eq(Number(stockUpdates[0].reorderPoint), 250, "the number the admin typed");
  eq(atRequested.some(u => /Stock%20Levels/.test(u)), false, "Airtable untouched");
});

await test("S3: a reorder point that matched nothing is a 404, not a silent success", async () => {
  reset();
  stockMissing.add("5da5a5a5-0000-4000-8000-000000000000");
  const r = await POST({ action: "updateReorderPoint",
                         stockLevelId: "5da5a5a5-0000-4000-8000-000000000000", reorderPoint: 250 });
  eq(r.statusCode, 404,
     "reporting a save that did not happen means an item quietly stops warning it is low");
});

await test("S3: setting a point on a fresh pair upserts rather than duplicating", async () => {
  reset();
  const r = json(await POST({ action: "createStockLevel", itemId: "recItemA",
                              locationId: "recLoc1", reorderPoint: 40 }));
  eq(r.ok, true, "ok");
  eq(stockCreates.length, 1, "one statement");
  eq(atRequested.some(u => /Stock%20Levels/.test(u)), false,
     "no Airtable record, and no QoH=0 cache row to keep in step either");
});

await test("S3: an unknown item or location is refused, not saved invisibly", async () => {
  reset();
  const r = await POST({ action: "createStockLevel", itemId: "recNope",
                         locationId: "recLoc1", reorderPoint: 40 });
  eq(r.statusCode, 404,
     "unresolved FKs would sit outside the unique index AND outside v_stock_levels — saved and unfindable");
});

// ── 043: the dual handle, on the two entities that went native last ────────
// Items (041) and locations (042) both became creatable in the app. A native row
// has no rec id, so its handle is its uuid — and three READERS were still keyed
// on the rec id alone. Every test below FAILS on the code as it shipped, which
// is the only reason any of them is worth having.

await test("043/F-02: a natively-created item's stock is findable at all", async () => {
  reset();
  const r = json(await GET("stockLevels", { itemId: NATIVE_ITEM }));
  eq(r._source, "neon", "served from Neon");
  // The old code filtered on item_airtable_id, which is NULL here. The query
  // SUCCEEDED with zero rows, so this came back `levels: []` with ok:true — a
  // clean, working-looking Check Stock screen saying the item is nowhere.
  // A short list looks exactly like a complete one.
  eq(r.levels.length, 1, "the new item HAS stock, and asking by the handle it was given finds it");
  eq(r.levels[0].locationName, "Shop #3", "at the location it was received into");
  eq(r.levels[0].qtyOnHand, 2, "the quantity the ledger says");
});

await test("043/F-02: a rec-id item is unaffected — the handle is a superset", async () => {
  reset();
  const r = json(await GET("stockLevels", { itemId: "recItemA" }));
  eq(r.levels.length, 1, "still exactly its own row");
  eq(r.levels[0].qtyOnHand, -1434, "and still the ledger's honest negative");
});

await test("043/F-02: stockLevelsAll carries the native item's uuid, not an empty string", async () => {
  reset();
  const r = json(await GET("stockLevelsAll"));
  const native = r.levels.find(l => l.locationName === "Shop #3");
  ok(native, "the native pair is in the list");
  // This id is joined straight back against the items list for category and
  // name (inventory.html:3312 / :3387 / :3531). `item_airtable_id || ""` made it
  // "", which joins to nothing — so the item's stock and its dollars landed
  // under "Uncategorized" against a blank row.
  eq(native.itemId, NATIVE_ITEM, "the handle the items list will actually match on");
  eq(r.levels.find(l => l.qtyOnHand === -1434).itemId, "recItemA", "rec-id items unchanged");
});

await test("043/F-02: reorder alerts carry handles for BOTH item and location", async () => {
  reset();
  const r = json(await GET("reorderAlerts"));
  const shop3 = r.groups["Shop #3"];
  ok(shop3 && shop3.length === 1, "the native pair raises an alert like any other");
  // Display-only in today's UI, which is why this one could not yet bite. It is
  // the same defect though, and left alone the next screen that wants to ACT on
  // an alert — count it, order it — would inherit an id that resolves to nothing.
  eq(shop3[0].itemId, NATIVE_ITEM, "item handle, not a NULL rec id flattened to \"\"");
  eq(shop3[0].locationId, NATIVE_LOC, "location handle too");
  eq(shop3[0].shortBy, 8, "10 - 2");
});

await test("043/F-01: every FK in the ledger insert resolves on BOTH handle forms", async () => {
  reset();
  json(await POST({ action: "submitCart", locationId: "recLoc1", enteredBy: "Rick",
    jobName: "Bethel School", jobId: "recJob1", submitId: "cart-handle",
    lines: [{ itemId: "recItemA", qty: 10, unitCost: 0.75 }] }));
  eq(txnInsertSql.length, 1, "one insert to inspect");

  // A white-box assertion on purpose. The FK resolution itself happens inside
  // Postgres, so an offline suite cannot observe it — but it CAN observe that
  // the statement was written to accept both forms, which is the thing that was
  // wrong. The item subselect took both; the two location subselects took
  // `airtable_id` only, so a location created in the app resolved to NULL. The
  // row still inserted, and v_stock_on_hand skips legs with a NULL location_id
  // — so the movement was logged, chargeable, in History, and absent from every
  // stock figure, with no error raised.
  //
  // Checked as a RULE over every subselect rather than by naming the two that
  // were broken, so the next entity to go native is covered before it exists.
  const subselects = txnInsertSql[0].match(/\(SELECT id FROM \w+ WHERE [^)]*\)/gi) || [];
  eq(subselects.length, 3, "item, from-location, to-location");
  for (const s of subselects) {
    ok(/airtable_id\s*=/.test(s) && /id::text\s*=/.test(s),
       `FK subselect must accept a uuid handle too, got: ${s}`);
  }
});

await test("043/F-03: a reorder point on a native item and location saves", async () => {
  reset();
  const r = json(await POST({ action: "createStockLevel", itemId: NATIVE_ITEM,
                              locationId: NATIVE_LOC, reorderPoint: 15 }));
  eq(r.ok, true, "saved");
  eq(stockCreates.length, 1, "one statement");
  eq(stockCreates[0].itemId, NATIVE_ITEM, "the handle it was given, stored as given");
});

await test("043/F-03: an unresolved handle writes NOTHING, rather than an invisible row", async () => {
  reset();
  const r = await POST({ action: "createStockLevel", itemId: "recNope",
                         locationId: "recLoc1", reorderPoint: 40 });
  eq(r.statusCode, 404, "refused");
  // The 404 was always right. What was wrong is that the INSERT had already run:
  // resolving the FKs as scalar subselects inside VALUES meant an unknown handle
  // still wrote a row with NULL ids, which sits OUTSIDE the (item_id,
  // location_id) partial unique index and outside v_stock_levels. Saved,
  // invisible, and reported as a failure — and a retry appended another one.
  ok(/INSERT INTO stock_settings[\s\S]*\bSELECT\b[\s\S]*\bFROM\b[\s\S]*\bWHERE\b/i.test(stockCreates[0].sql),
     "INSERT…SELECT, so an unresolvable handle produces no row to insert at all");
  ok(!/VALUES\s*\(/i.test(stockCreates[0].sql),
     "not INSERT…VALUES with scalar subselects — that is the shape that saved the orphan");
});

await test("S3: stock levels refuse to fall back to a frozen Airtable", async () => {
  reset();
  neonDown = true;
  const r = await GET("stockLevels", { itemId: "recItemA" });
  eq(r.statusCode, 503, "stale reorder points over a discredited on-hand cache is a wrong answer");
  eq(atRequested.some(u => /Stock%20Levels/.test(u)), false, "it did not even try");
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

await test("S2: a big push returns EVERY line, not just the expensive ones", async () => {
  reset();
  // The real Lance Koehn push: 42 lines, biggest $1,301.69, smallest $0.00.
  // A screen showing only the top handful would be a truncation somewhere, and
  // this is the case that says whether the handler is the one doing it.
  neonPushDetail = Array.from({ length: 42 }, (_, i) => ({
    ...neonPushes[0],
    line_id: `dddddddd-0000-4000-8000-${String(i).padStart(12, "0")}`,
    item_name: `ITEM ${i + 1}`, line_title: `ITEM ${i + 1} x 1`,
    quantity: "1.0000", unit_cost: String(42 - i), line_total: String(42 - i), wire_ft: null,
  }));
  const r = json(await GET("pushHistoryDetail", { id: "aaaaaaaa-1111-4111-8111-111111111111" }));
  eq(r.push.lines.length, 42, "all 42 — no cap, no LIMIT, no slice");
  eq(r.push.lines[41].itemName, "ITEM 42", "including the cheapest one");
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

await test("ADJ: a NATIVE item can be counted — the on-hand read takes either handle", async () => {
  reset();
  adjOnHand = 0;
  // Adjusting a natively-created item 404'd with "Item or location not found":
  // the on-hand lookup still said `i.airtable_id = $1`, so an item with no rec
  // id matched nothing. Every OTHER item query had been moved to the dual
  // handle; this one sat on its own line and the sweep missed it.
  const r = json(await POST({ action: "adjustment", itemId: NEW_ITEM,
                              locationId: "recLoc1", qty: 15, enteredBy: "Rick" }));
  eq(r.ok, true, "counted");
  eq(r.delta, 15, "0 → 15 posts +15");
  eq(txnWrites[0].to, "recLoc1", "on the adding leg");
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
  eq(r._source, "neon", "from Neon");
  eq(r.estimates[0].id, EST1,
     "the uuid — an estimate created since the cutover has no rec id to hand the detail view");
  eq(r.estimates[0].total, 16420.99, "from v_material_estimate_totals, not a stored rollup");
  eq(r.estimates[0].lineCount, 47, "and the line count with it");
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

await test("S4: creating an estimate is native, header then lines, no Airtable", async () => {
  reset();
  const r = json(await POST({ action: "estimateCreate", jobName: "Test Job", createdBy: "Rick",
                              lines: [{ itemId: "recItemA", qty: 2, unitCost: 0.75 }] }));
  eq(r.ok, true, "ok");
  eq(r.id, NEW_EST, "returns the uuid Postgres minted");
  eq(estWrites.length, 1, "one header");
  eq(estLineWrites.length, 1, "one line");
  eq(atRequested.some(u => /Estimates/.test(u)), false, "nothing reached Airtable");
  // Header still first, but for a plainer reason: the lines need its uuid. They
  // no longer look the parent up by rec id, so the null-FK race is gone.
});

await test("S4: an estimate built from a template clones in ONE insert…select", async () => {
  reset();
  const r = json(await POST({ action: "createEstimateFromTemplate", templateId: EST1,
                              jobName: "Aaron McLauglin (MIA 274)", createdBy: "Rick" }));
  eq(r.ok, true, "ok");
  eq(r.estimateId, NEW_EST, "the new estimate's uuid");
  eq(r.lineCount, 2, "both template lines cloned");
  // This is the handler that shipped a 404 to production by writing Airtable and
  // syncing nothing. There is one store now, so that shape of bug cannot recur.
  eq(atRequested.some(u => /Estimate/.test(u)), false, "and none of it touched Airtable");
});

await test("S4: replacing an estimate's lines is one DELETE by parent, not a list", async () => {
  reset();
  const r = json(await POST({ action: "estimateUpdate", id: EST1, replaceLines: true,
                              lines: [{ itemId: "recItemA", qty: 5, unitCost: 0.75 }] }));
  eq(r.ok, true, "ok");
  eq(estLineDeletes[0], EST1,
     "deleted by estimate_id — the old path had to ask Airtable which lines existed first");
  eq(estLineWrites.length, 1, "the replacement landed");
});

await test("S4: deleting an estimate cascades, and a miss is a 404", async () => {
  reset();
  const r = json(await POST({ action: "estimateDelete", id: EST1 }));
  eq(r.ok, true, "ok");
  eq(estDeletes[0], EST1, "removed — ON DELETE CASCADE takes the lines");
  estMissing.add("e5717a7e-9999-4999-8999-999999999999");
  const missing = await POST({ action: "estimateDelete", id: "e5717a7e-9999-4999-8999-999999999999" });
  eq(missing.statusCode, 404, "a delete that removed nothing does not report success");
});

console.log("\ninventory.js reference tables — Steps B + C + D\n" + "-".repeat(46));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(46));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);

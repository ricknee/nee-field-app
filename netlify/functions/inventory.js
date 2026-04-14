// netlify/functions/inventory.js
// NEE Inventory App v2 — Netlify Proxy
// Env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (main NEE), INVENTORY_BASE_ID

const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const MAIN_BASE_ID     = process.env.AIRTABLE_BASE_ID;
const INV_BASE_ID      = process.env.INVENTORY_BASE_ID;
const API_ROOT_MAIN    = `https://api.airtable.com/v0/${MAIN_BASE_ID}`;
const API_ROOT_INV     = `https://api.airtable.com/v0/${INV_BASE_ID}`;

function resp(code, body) {
  return {
    statusCode: code,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
    },
    body: JSON.stringify(body)
  };
}

function ensureEnv() {
  if (!AIRTABLE_API_KEY) throw new Error("Missing AIRTABLE_API_KEY");
  if (!MAIN_BASE_ID)     throw new Error("Missing AIRTABLE_BASE_ID");
  if (!INV_BASE_ID)      throw new Error("Missing INVENTORY_BASE_ID");
}

function normalize(v) { return String(v || "").trim().toLowerCase(); }

function gBool(fields, name) {
  const v = fields[name];
  if (typeof v === "boolean") return v;
  if (typeof v === "number")  return v !== 0;
  if (typeof v === "string")  return ["true","yes","1"].includes(v.trim().toLowerCase());
  return false;
}

async function atFetch(root, path, options = {}) {
  const res = await fetch(`${root}/${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_API_KEY}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });
  const text = await res.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  if (!res.ok) throw new Error(json?.error?.message || `Airtable error ${res.status}`);
  return json;
}

async function fetchAll(root, table, opts = {}) {
  const params = new URLSearchParams();
  if (opts.filter)     params.set("filterByFormula", opts.filter);
  if (opts.sortField)  params.set("sort[0][field]", opts.sortField);
  if (opts.sortDir)    params.set("sort[0][direction]", opts.sortDir);
  if (opts.maxRecords) params.set("maxRecords", String(opts.maxRecords));

  const records = [];
  let offset = null;
  do {
    const qs = new URLSearchParams(params);
    if (offset) qs.set("offset", offset);
    const data = await atFetch(root, `${encodeURIComponent(table)}?${qs}`, { method: "GET" });
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return records;
}

// ── LOGIN ──────────────────────────────────────────────────
async function handleLogin(body) {
  const { identifier, pin } = body || {};
  if (!identifier || !pin) return resp(400, { ok: false, error: "Missing name or PIN." });

  const records = await fetchAll(API_ROOT_MAIN, "Employees", { filter: `{Active}=1` });
  const id = normalize(identifier);

  const match = records.find(r => {
    const f = r.fields || {};
    const fullName  = normalize(f["Employee Name"] || "");
    const firstName = fullName.split(" ")[0];
    const username  = normalize(f["Username"] || "");
    const savedPin  = String(f["PIN"] || "").trim();
    return (fullName === id || firstName === id || username === id)
      && savedPin !== ""
      && savedPin === String(pin).trim();
  });

  if (!match) return resp(401, { ok: false, error: "Invalid name or PIN." });

  const f    = match.fields || {};
  const role = normalize(f["Role New"] || f["Role"] || "");
  return resp(200, {
    ok: true,
    user: {
      id:   match.id,
      name: f["Employee Name"] || "Unknown",
      role: role === "admin" ? "admin" : "employee"
    }
  });
}

// ── EMPLOYEES (for name picker) ────────────────────────────
async function handleEmployees() {
  const records = await fetchAll(API_ROOT_MAIN, "Employees", {
    filter: `{Active}=1`,
    sortField: "Employee Name",
    sortDir: "asc"
  });
  const employees = records.map(r => ({
    id:   r.id,
    name: r.fields["Employee Name"] || ""
  }));
  return resp(200, { ok: true, employees });
}

// ── JOBS (from inventory base synced Jobs table) ──────────
async function handleJobs() {
  const records = await fetchAll(API_ROOT_INV, "Jobs", {});
  return resp(200, {
    ok: true,
    jobs: records
      .map(r => ({
        id:   r.id,
        name: r.fields["Job PO"] || r.fields["Job Name"] || ""
      }))
      .filter(j => j.name)
      .sort((a, b) => a.name.localeCompare(b.name))
  });
}

// ── LOCATIONS ──────────────────────────────────────────────
async function handleLocations() {
  const records = await fetchAll(API_ROOT_INV, "Locations", {
    filter: `{Active Location}=1`,
    sortField: "Location Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true,
    locations: records.map(r => ({
      id:   r.id,
      name: r.fields["Location Name"] || "",
      type: r.fields["Location Type"]?.name || ""
    }))
  });
}

// ── ITEMS ──────────────────────────────────────────────────
async function handleItems() {
  const records = await fetchAll(API_ROOT_INV, "Inventory Items", {
    filter: `{Active Item}=1`,
    sortField: "Item Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true,
    items: records.map(r => {
      const f = r.fields || {};
      return {
        id:      r.id,
        name:    f["Item Name"] || "",
        cat:     f["Category"]?.name || f["Category"] || "",
        uom:     f["Unit of Measure"]?.name || f["Unit of Measure"] || "",
        barcode: f["Barcode Value"] || "",
        cost:    f["Default Unit Cost"] || 0
      };
    })
  });
}

// ── SUBMIT CART (multiple transactions at once) ────────────
async function handleSubmitCart(body) {
  const { lines, jobName, jobId, locationId, enteredBy } = body || {};
  if (!lines || !lines.length) return resp(400, { ok: false, error: "No items in cart." });
  if (!locationId) return resp(400, { ok: false, error: "Missing location." });

  console.log(`submitCart: jobName="${jobName}" jobId="${jobId}" location="${locationId}" lines=${lines.length}`);

  const now = new Date().toISOString();
  const results = [];

  for (const line of lines) {
    const fields = {
      "fldGq37LD9YuyCf5e": now,
      "fldmookC8mdyXxVuw": [String(line.itemId)],
      "fldFQlArrzUnjCTxr": Number(line.qty),
      "fldjvIy3X1DJowGsd": line.type || "Use",
      "fldpyLadbcc9NHO6c": [String(locationId)],
      "fldIFffLxtcQTbExd": enteredBy || "",
      "fldrcq8wSyfz8O3UB": line.notes || ""
    };
    // Link to job record if we have the inventory base job ID
    if (jobId) {
      fields["fld7OG04Sgkp88JsU"] = [String(jobId)];
    }
    const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields }], typecast: true })
    });
    results.push(data.records?.[0]?.id);
  }

  return resp(200, { ok: true, ids: results });
}

// ── RECEIVE ITEMS (with optional price update) ─────────────
async function handleReceive(body) {
  const { itemId, locationId, qty, unitCost, enteredBy, notes } = body || {};
  if (!itemId || !locationId || !qty) return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();
  const fields = {
    "fldGq37LD9YuyCf5e": now,
    "fldmookC8mdyXxVuw": [String(itemId)],
    "fldFQlArrzUnjCTxr": Number(qty),
    "fldjvIy3X1DJowGsd": "Receive",
    "fld5FZc9oBHNn4YF7": [String(locationId)],  // To Location
    "fldIFffLxtcQTbExd": enteredBy || "",
    "fldrcq8wSyfz8O3UB": notes || ""
  };

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  // Update unit cost on item if provided
  if (unitCost && Number(unitCost) > 0) {
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, {
      method: "PATCH",
      body: JSON.stringify({ fields: { "fld8aEhTzmEbqgIg4": Number(unitCost) } })
    });
  }

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── TRANSFER ───────────────────────────────────────────────
async function handleTransfer(body) {
  const { itemId, fromLocationId, toLocationId, qty, enteredBy, notes } = body || {};
  if (!itemId || !fromLocationId || !toLocationId || !qty)
    return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();
  const fields = {
    "fldGq37LD9YuyCf5e": now,
    "fldmookC8mdyXxVuw": [String(itemId)],
    "fldFQlArrzUnjCTxr": Number(qty),
    "fldjvIy3X1DJowGsd": "Transfer",
    "fldpyLadbcc9NHO6c": [String(fromLocationId)],
    "fld5FZc9oBHNn4YF7": [String(toLocationId)],
    "fldIFffLxtcQTbExd": enteredBy || "",
    "fldrcq8wSyfz8O3UB": notes || ""
  };

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── HISTORY ────────────────────────────────────────────────
async function handleHistory(params) {
  const { enteredBy, all } = params || {};

  const [txRecords, itemRecords, locRecords] = await Promise.all([
    fetchAll(API_ROOT_INV, "Inventory Transactions", {
      filter: all === "1" ? undefined : (enteredBy ? `{Entered By}='${enteredBy}'` : undefined),
      sortField: "Transaction Date",
      sortDir: "desc",
      maxRecords: 200
    }),
    fetchAll(API_ROOT_INV, "Inventory Items", {}),
    fetchAll(API_ROOT_INV, "Locations", {})
  ]);

  const itemMap = {};
  itemRecords.forEach(r => {
    itemMap[r.id] = {
      name: r.fields["Item Name"] || r.id,
      cost: r.fields["Default Unit Cost"] || 0,
      uom:  r.fields["Unit of Measure"]?.name || r.fields["Unit of Measure"] || ""
    };
  });
  const locMap = {};
  locRecords.forEach(r => { locMap[r.id] = r.fields["Location Name"] || r.id; });

  const resolveArr = (arr, map) => {
    if (!Array.isArray(arr) || !arr.length) return "";
    const first = arr[0];
    const id = typeof first === "object" ? first.id : String(first);
    return map[id] || id;
  };

  const transactions = txRecords.map(r => {
    const f = r.fields || {};
    const itemArr = f["Inventory Item"] || [];
    const fromArr = f["From Location"]  || [];
    const toArr   = f["To Location"]    || [];

    const itemId   = typeof itemArr[0] === "object" ? itemArr[0]?.id : String(itemArr[0] || "");
    const itemData = itemMap[itemId] || {};

    let dateStr = "";
    try {
      const d = new Date(f["Transaction Date"]);
      dateStr = d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
    } catch(e) {}

    const notesRaw   = f["Notes"] || "";
    const notesParts = notesRaw.split(" | ");
    const jobName    = notesParts[0] || "";
    const userNotes  = notesParts.slice(1).join(" | ");
    const qty        = f["Quantity"] ?? 0;

    return {
      id:         r.id,
      date:       dateStr,
      item:       itemData.name || itemId,
      uom:        itemData.uom  || "",
      cost:       itemData.cost || 0,
      total:      Math.round((itemData.cost || 0) * qty * 100) / 100,
      from:       resolveArr(fromArr, locMap),
      to:         resolveArr(toArr,   locMap),
      qty,
      type:       f["Transaction Type"]?.name || f["Transaction Type"] || "",
      job:        jobName,
      notes:      userNotes,
      enteredBy:  f["Entered By"] || ""
    };
  });

  return resp(200, { ok: true, transactions });
}

// ── PENDING EXPENSES ───────────────────────────────────────
async function handlePendingExpenses() {
  const TAX_RATE = 0.075;

  // Fetch all Use transactions that haven't been pushed yet
  const [txRecords, itemRecords, jobRecords] = await Promise.all([
    fetchAll(API_ROOT_INV, "Inventory Transactions", {
      filter: `AND({Transaction Type}='Use', NOT({Expense Created?}=1))`,
      sortField: "Transaction Date",
      sortDir: "asc"
    }),
    fetchAll(API_ROOT_INV, "Inventory Items", {}),
    fetchAll(API_ROOT_INV, "Jobs", {})
  ]);

  const itemMap = {};
  itemRecords.forEach(r => {
    itemMap[r.id] = { name: r.fields["Item Name"] || r.id, cost: r.fields["Default Unit Cost"] || 0 };
  });

  // Build job map with tax status from main base
  const jobMap = {};
  for (const r of jobRecords) {
    const invJobId  = r.id;
    const jobPO     = r.fields["Job PO"] || r.fields["Job Name"] || "";
    // Fetch tax status from main base using same record ID (synced tables share IDs)
    jobMap[invJobId] = { name: jobPO, taxable: false, mainJobId: invJobId };
  }

  // Fetch tax status from main base for all jobs
  const mainJobIds = Object.keys(jobMap);
  if (mainJobIds.length) {
    try {
      const mainJobs = await fetchAll(API_ROOT_MAIN, "Jobs", {});
      mainJobs.forEach(r => {
        if (jobMap[r.id]) {
          const taxStatus = r.fields["Tax Status"]?.name || r.fields["Tax Status"] || "";
          jobMap[r.id].taxable = taxStatus === "Taxable";
        }
      });
    } catch(e) { console.log("Could not fetch tax status:", e.message); }
  }

  const pending = txRecords.map(r => {
    const f       = r.fields || {};
    const itemArr = f["Inventory Item"] || [];
    const jobArr  = f["Job"] || [];
    const itemId  = typeof itemArr[0] === "object" ? itemArr[0]?.id : String(itemArr[0] || "");
    const jobId   = typeof jobArr[0]  === "object" ? jobArr[0]?.id  : String(jobArr[0]  || "");
    const itemData = itemMap[itemId] || {};
    const jobData  = jobMap[jobId]   || {};
    const qty      = f["Quantity"] ?? 0;
    const total    = Math.round((itemData.cost || 0) * Math.abs(qty) * 100) / 100;
    const notesRaw = f["Notes"] || "";
    const jobName  = jobData.name || notesRaw.split(" | ")[0] || "";

    return {
      txId:    r.id,
      item:    itemData.name || itemId,
      qty:     Math.abs(qty),
      cost:    itemData.cost || 0,
      total,
      jobId,
      jobName,
      taxable: jobData.taxable || false
    };
  }).filter(t => t.total > 0 && t.jobId);

  return resp(200, { ok: true, pending });
}

// ── PUSH EXPENSES TO MAIN BASE ─────────────────────────────
async function handlePushExpenses(body) {
  const { groups, order } = body || {};
  if (!groups || !order) return resp(400, { ok: false, error: "Missing expense data." });

  const TAX_RATE   = 0.075;
  const today      = new Date().toISOString().split("T")[0];
  const expenseIds = [];
  const txIds      = [];

  for (const jobName of order) {
    const g       = groups[jobName];
    const txs     = g.txs || [];
    const jobId   = g.jobId;
    const taxable = g.taxable;

    if (!jobId || !txs.length) continue;

    const jobTotal = txs.reduce((s, t) => s + (t.total || 0), 0);
    if (jobTotal <= 0) continue;

    // Create materials expense in main NEE base
    const matFields = {
      "fldPNFIzq1grsdxYi": [{ id: String(jobId) }],  // Job
      "fldwbLPIafVtmaSeb": jobTotal,                   // Manual Material Cost
      "fldX2x2J0xkRyMY3y": "Materials",               // Expense Type
      "fldCCPYdyWAOGchWb": today,                      // Expense Date
      "fldJTg0ekrdZ4Jqr6": "Not Reviewed",             // Expense Status
      "fld9Afieu4ofjvhSb": true,                       // Billable?
      "fldnSQEOnyq3sho5g": "Inventory materials — " + txs.map(t => t.item + " ×" + t.qty).join(", ")
    };

    const matResp = await atFetch(API_ROOT_MAIN, encodeURIComponent("Expenses"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields: matFields }], typecast: true })
    });
    expenseIds.push(matResp.records?.[0]?.id);

    // Create sales tax expense if taxable
    if (taxable) {
      const taxAmt = Math.round(jobTotal * TAX_RATE * 100) / 100;
      const taxFields = {
        "fldPNFIzq1grsdxYi": [{ id: String(jobId) }],
        "fldwbLPIafVtmaSeb": taxAmt,
        "fldX2x2J0xkRyMY3y": "Materials",
        "fldCCPYdyWAOGchWb": today,
        "fldJTg0ekrdZ4Jqr6": "Not Reviewed",
        "fld9Afieu4ofjvhSb": true,
        "fldnSQEOnyq3sho5g": "Sales tax (7.5%) on inventory materials"
      };
      const taxResp = await atFetch(API_ROOT_MAIN, encodeURIComponent("Expenses"), {
        method: "POST",
        body: JSON.stringify({ records: [{ fields: taxFields }], typecast: true })
      });
      expenseIds.push(taxResp.records?.[0]?.id);
    }

    // Mark all transactions as pushed
    txs.forEach(t => txIds.push(t.txId));
  }

  // Mark all transactions as Expense Created in inventory base
  // Airtable allows max 10 per PATCH request
  for (let i = 0; i < txIds.length; i += 10) {
    const batch = txIds.slice(i, i + 10).map(id => ({
      id,
      fields: { "fldO7Z0L7tpAvrgtH": true }  // Expense Created? checkbox
    }));
    await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
      method: "PATCH",
      body: JSON.stringify({ records: batch })
    });
  }

  return resp(200, { ok: true, count: expenseIds.length, txCount: txIds.length });
}

// ── ADJUSTMENT ─────────────────────────────────────────────
async function handleAdjustment(body) {
  const { itemId, locationId, qty, enteredBy, notes } = body || {};
  if (!itemId || !locationId || qty === undefined) return resp(400, { ok: false, error: "Missing required fields." });

  const now = new Date().toISOString();
  const fields = {
    "fldGq37LD9YuyCf5e": now,
    "fldmookC8mdyXxVuw": [String(itemId)],
    "fldFQlArrzUnjCTxr": Number(qty),
    "fldjvIy3X1DJowGsd": "Adjustment",
    "fldpyLadbcc9NHO6c": [String(locationId)],
    "fldIFffLxtcQTbExd": enteredBy || "",
    "fldrcq8wSyfz8O3UB": notes || ""
  };

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── DELETE ─────────────────────────────────────────────────
async function handleDelete(body) {
  const { txId } = body || {};
  if (!txId) return resp(400, { ok: false, error: "Missing txId." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Transactions")}/${txId}`, { method: "DELETE" });
  return resp(200, { ok: true, deleted: txId });
}

// ── ROUTER ─────────────────────────────────────────────────
export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "employees")       return await handleEmployees();
      if (action === "jobs")            return await handleJobs();
      if (action === "locations")       return await handleLocations();
      if (action === "items")           return await handleItems();
      if (action === "history")         return await handleHistory(params);
      if (action === "pendingExpenses") return await handlePendingExpenses();
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login")        return await handleLogin(body);
      if (body.action === "submitCart")   return await handleSubmitCart(body);
      if (body.action === "receive")      return await handleReceive(body);
      if (body.action === "transfer")     return await handleTransfer(body);
      if (body.action === "adjustment")   return await handleAdjustment(body);
      if (body.action === "pushExpenses") return await handlePushExpenses(body);
      if (body.action === "delete")       return await handleDelete(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Inventory Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

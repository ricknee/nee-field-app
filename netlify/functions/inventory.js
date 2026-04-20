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

// ── ESTIMATING JOBS (from main NEE base, filtered by status) ──
// For the Estimates feature — pulls jobs in New Lead, Estimating, or Awarded
async function handleEstimatingJobs() {
  const records = await fetchAll(API_ROOT_MAIN, "Jobs", {
    filter: `OR({Job Status}='New Lead',{Job Status}='Estimating',{Job Status}='Awarded')`,
    sortField: "Job Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true,
    jobs: records
      .map(r => {
        const f = r.fields || {};
        return {
          id:     r.id,
          // Use the formatted "Name (PO)" field when available so the user picks
          // a job that's already disambiguated by its PO (e.g. "Blue Ridge Poultry (GRB 126)")
          name:   f["Job PO"] || f["Job Name"] || "",
          status: f["Job Status"]?.name || f["Job Status"] || "",
          taxable: (f["Tax Status"]?.name || f["Tax Status"] || "") === "Taxable"
        };
      })
      .filter(j => j.name)
  });
}

// ── AWARDED JOBS ONLY (for employee-side material ordering) ──
async function handleAwardedJobs() {
  const records = await fetchAll(API_ROOT_MAIN, "Jobs", {
    filter: `{Job Status}='Awarded'`,
    sortField: "Job Name",
    sortDir: "asc"
  });
  return resp(200, {
    ok: true,
    jobs: records
      .map(r => {
        const f = r.fields || {};
        return {
          id:   r.id,
          // Prefer the formula field that combines Job Name + PO ("Blue Ridge Poultry (GRB 126)")
          // so material order PDFs and order lists show the PO right alongside the name.
          // Falls back to Job Name if PO is missing on a particular job.
          name: f["Job PO"] || f["Job Name"] || ""
        };
      })
      .filter(j => j.name)
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
        id:          r.id,
        name:        f["Item Name"] || "",
        cat:         f["Category"]?.name || f["Category"] || "",
        size:        f["Product Size"]?.name || f["Product Size"] || "",
        uom:         f["Unit of Measure"]?.name || f["Unit of Measure"] || "",
        barcode:     f["Barcode Value"] || "",
        cost:        f["Default Unit Cost"] || 0,
        wireFtPerLb: f["Wire ft/lb"] || 0
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

  // Fetch current item costs as a fallback safety net for snapshot capture.
  // If the frontend forgets to send unitCost, we look it up here so snapshots
  // never end up as $0 unless the item itself has no cost set.
  let itemCostMap = {};
  try {
    const itemRecords = await fetchAll(API_ROOT_INV, "Inventory Items", {});
    itemRecords.forEach(r => {
      itemCostMap[r.id] = Number(r.fields["Default Unit Cost"] || 0);
    });
  } catch(e) {
    console.warn("Could not fetch item costs for snapshot fallback:", e.message);
  }

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

    // Snapshot the unit cost at transaction time. This freezes the cost
    // for billing so retroactive price changes do not alter historical job costs.
    // Priority: explicit unitCost from cart > current Default Unit Cost lookup.
    let snapshotCost = 0;
    if (line.unitCost !== undefined && line.unitCost !== null && Number(line.unitCost) > 0) {
      snapshotCost = Number(line.unitCost);
    } else if (itemCostMap[line.itemId] > 0) {
      snapshotCost = itemCostMap[line.itemId];
    }
    if (snapshotCost > 0) {
      fields["fldUStmydYotYBFoE"] = snapshotCost;
    }

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

  // Snapshot the cost on receive transactions too — lets the receive history
  // show what was actually paid even if Default Unit Cost is updated later.
  if (unitCost && Number(unitCost) > 0) {
    fields["fldUStmydYotYBFoE"] = Number(unitCost);
  }

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

    // Prefer snapshot cost from the transaction; fall back to current item cost
    const snapshotCost = Number(f["Unit Cost (Snapshot)"] || 0);
    const lineCost = snapshotCost > 0 ? snapshotCost : (itemData.cost || 0);

    return {
      id:         r.id,
      date:       dateStr,
      item:       itemData.name || itemId,
      itemId:     itemId,
      uom:        itemData.uom  || "",
      cost:       lineCost,
      total:      Math.round(lineCost * qty * 100) / 100,
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

  // Fetch all Use AND Return transactions not yet pushed
  const [txRecords, itemRecords, invJobRecords, mainJobRecords] = await Promise.all([
    fetchAll(API_ROOT_INV, "Inventory Transactions", {
      filter: `AND(OR({Transaction Type}='Use', {Transaction Type}='Return'), NOT({Expense Created?}=1))`,
      sortField: "Transaction Date",
      sortDir: "asc"
    }),
    fetchAll(API_ROOT_INV, "Inventory Items", {}),
    fetchAll(API_ROOT_INV, "Jobs", {}),
    fetchAll(API_ROOT_MAIN, "Jobs", {})
  ]);

  const itemMap = {};
  itemRecords.forEach(r => {
    itemMap[r.id] = { name: r.fields["Item Name"] || r.id, cost: r.fields["Default Unit Cost"] || 0, wireFtPerLb: r.fields["Wire ft/lb"] || 0 };
  });

  // Match inv base jobs to main base jobs by Job Name (more reliable than formula field)
  const mainJobByName = {};
  mainJobRecords.forEach(r => {
    const name = (r.fields["Job Name"] || "").trim();
    if (name) mainJobByName[name] = {
      id:      r.id,
      taxable: (r.fields["Tax Status"]?.name || r.fields["Tax Status"] || "") === "Taxable"
    };
  });

  console.log("Main base job names sample:", Object.keys(mainJobByName).slice(0,5));

  const invJobMap = {};
  invJobRecords.forEach(r => {
    const jobName = (r.fields["Job Name"] || "").trim();
    const jobPO   = (r.fields["Job PO"]   || r.fields["Job Name"] || "").trim();
    const mainJob = mainJobByName[jobName] || null;
    console.log(`Inv job: "${jobName}" -> mainJobId: ${mainJob?.id || "NO MATCH"}`);
    invJobMap[r.id] = {
      name:      jobPO || jobName,
      mainJobId: mainJob?.id   || null,
      taxable:   mainJob?.taxable || false
    };
  });

  // Build per-job, per-item accumulations using cost-per-transaction so that
  // multiple transactions at different snapshot prices are weighted correctly.
  // Structure: jobKey -> { jobData, items: { itemId -> { name, wireFtPerLb, netQty, totalCost } }, txIds: [] }
  const jobGroups = {};

  txRecords.forEach(r => {
    const f        = r.fields || {};
    const itemArr  = f["Inventory Item"] || [];
    const jobArr   = f["Job"] || [];
    const txType   = f["Transaction Type"]?.name || f["Transaction Type"] || "";
    const itemId   = typeof itemArr[0] === "object" ? itemArr[0]?.id : String(itemArr[0] || "");
    const invJobId = typeof jobArr[0]  === "object" ? jobArr[0]?.id  : String(jobArr[0]  || "");
    const qty      = Math.abs(f["Quantity"] ?? 0);
    const notesRaw = f["Notes"] || "";
    const snapshotCost = Number(f["Unit Cost (Snapshot)"] || 0);

    if (!itemId || !invJobId) return;

    const jobData  = invJobMap[invJobId] || {};
    if (!jobData.mainJobId) return; // skip if no matching main base job

    const jobKey = invJobId;
    if (!jobGroups[jobKey]) {
      jobGroups[jobKey] = {
        jobName:   jobData.name || notesRaw.split(" | ")[0] || "",
        mainJobId: jobData.mainJobId,
        taxable:   jobData.taxable,
        items:     {},
        txIds:     []
      };
    }

    // Accumulate tx IDs (all get marked as pushed regardless)
    jobGroups[jobKey].txIds.push(r.id);

    // Net qty: Use = positive, Return = negative
    const delta = txType === "Return" ? -qty : qty;

    // Per-transaction cost: prefer snapshot, fall back to current item cost
    // for legacy transactions created before the snapshot field existed.
    const itemData = itemMap[itemId] || {};
    const txCost = snapshotCost > 0 ? snapshotCost : (itemData.cost || 0);
    const lineValue = txCost * delta; // signed: negative for returns

    if (!jobGroups[jobKey].items[itemId]) {
      jobGroups[jobKey].items[itemId] = {
        name:        itemData.name || itemId,
        wireFtPerLb: itemData.wireFtPerLb || 0,
        netQty:      0,
        totalCost:   0
      };
    }
    jobGroups[jobKey].items[itemId].netQty    += delta;
    jobGroups[jobKey].items[itemId].totalCost += lineValue;
  });

  // Build the pending array for the UI — one entry per job
  const pending = Object.values(jobGroups).map(g => {
    const lines = Object.values(g.items)
      .filter(i => i.netQty !== 0)
      .map(i => {
        // Effective per-unit cost for display = totalCost / netQty
        // (handles mixed-snapshot case correctly)
        const effectiveCost = i.netQty !== 0 ? i.totalCost / i.netQty : 0;
        return {
          item:   i.name,
          qty:    i.netQty,
          cost:   Math.round(effectiveCost * 100) / 100,
          total:  Math.round(i.totalCost * 100) / 100,
          wireFt: i.wireFtPerLb > 0 ? Math.round(Math.abs(i.netQty) * i.wireFtPerLb) : 0
        };
      });

    const jobTotal = lines.reduce((s, l) => s + l.total, 0);

    return {
      jobName:   g.jobName,
      jobId:     g.mainJobId,
      taxable:   g.taxable,
      txIds:     g.txIds,
      lines,
      jobTotal
    };
  }).filter(g => g.jobTotal > 0); // skip jobs where returns cancel out all uses

  return resp(200, { ok: true, pending });
}

// ── RECEIPT FIELD LOOKUP ──────────────────────────────────
// Returns field ID for "Receipt / Document" on the Expenses table
// Uses the Airtable meta API — requires schema:read scope on the token
async function getReceiptFieldId() {
  try {
    const res  = await fetch(`https://api.airtable.com/v0/meta/bases/${MAIN_BASE_ID}/tables`, {
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
    });
    if (!res.ok) {
      console.error(`Meta API ${res.status} — token may lack schema:read scope`);
      return null;
    }
    const data  = await res.json();
    const exp   = (data.tables || []).find(t => t.name === "Expenses");
    if (!exp) { console.error("Expenses table not found in meta"); return null; }
    const field = exp.fields.find(f => f.name === "Receipt / Document");
    const id    = field?.id || null;
    console.log("Receipt/Document field ID:", id);
    return id;
  } catch(e) {
    console.error("getReceiptFieldId failed:", e.message);
    return null;
  }
}

// ── DEBUG: GET EXPENSE FIELD IDS ──────────────────────────
async function handleGetExpenseFields() {
  try {
    const res  = await fetch(`https://api.airtable.com/v0/meta/bases/${MAIN_BASE_ID}/tables`, {
      headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}` }
    });
    if (!res.ok) return resp(res.status, { ok: false, error: `Meta API error ${res.status} — token needs schema:read scope` });
    const data  = await res.json();
    const exp   = (data.tables || []).find(t => t.name === "Expenses");
    if (!exp) return resp(404, { ok: false, error: "Expenses table not found" });
    const fields = exp.fields.map(f => ({ id: f.id, name: f.name, type: f.type }));
    return resp(200, { ok: true, fields });
  } catch(e) {
    return resp(500, { ok: false, error: e.message });
  }
}

// ── PDF ATTACHMENT UPLOAD ─────────────────────────────────
async function uploadPdfToExpense(recordId, fieldId, pdfBase64, filename) {
  const pdfBuffer = Buffer.from(pdfBase64, "base64");
  const boundary  = "NEEBoundary" + Date.now().toString(36);
  const CRLF      = "\r\n";

  // Manually construct multipart/form-data body (more reliable than FormData in Node.js)
  const pre = Buffer.from([
    `--${boundary}${CRLF}`,
    `Content-Disposition: form-data; name="contentType"${CRLF}${CRLF}`,
    `application/pdf${CRLF}`,
    `--${boundary}${CRLF}`,
    `Content-Disposition: form-data; name="filename"${CRLF}${CRLF}`,
    `${filename}${CRLF}`,
    `--${boundary}${CRLF}`,
    `Content-Disposition: form-data; name="file"; filename="${filename}"${CRLF}`,
    `Content-Type: application/pdf${CRLF}${CRLF}`
  ].join(""));
  const post = Buffer.from(`${CRLF}--${boundary}--${CRLF}`);
  const body  = Buffer.concat([pre, pdfBuffer, post]);

  const res = await fetch(
    `https://content.airtable.com/v0/${MAIN_BASE_ID}/${recordId}/uploadAttachment/${fieldId}`,
    {
      method:  "POST",
      headers: {
        Authorization:  `Bearer ${AIRTABLE_API_KEY}`,
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "Content-Length": body.length.toString()
      },
      body
    }
  );
  const text = await res.text();
  if (!res.ok) throw new Error(`Attachment upload ${res.status}: ${text.substring(0, 300)}`);
  try { return JSON.parse(text); } catch(e) { return { ok: true }; }
}

// ═══════════════════════════════════════════════════════════
// PUSH HISTORY — table IDs and field IDs (in inventory base)
// ═══════════════════════════════════════════════════════════
const PUSH_TABLE_ID       = "tbl4txpj2l3pGk5E1";  // Expense Pushes
const PUSH_LINES_TABLE_ID = "tblloWlcSE7aXAX1o";  // Expense Push Lines

// Expense Pushes fields
const F_PUSH_TITLE       = "fldtLnuQkc2DOjuLf";
const F_PUSH_DATE        = "fldpCqWwprRsFLlWZ";
const F_PUSH_BY          = "fld94yrSbW5Pgy3R7";
const F_PUSH_JOB_NAME    = "flduHf59GYpd2BpY4";
const F_PUSH_JOB_ID_MAIN = "fldEKePmtgCIbBuQi";
const F_PUSH_MAT_TOTAL   = "fldX3sYU1i6c0nk9I";
const F_PUSH_TAX_TOTAL   = "fldKPeCOawEogqZ0B";
const F_PUSH_TOTAL       = "fldMmYbl7vDtcRDTc";
const F_PUSH_TX_COUNT    = "fldorTygoGLOB0Kkv";
const F_PUSH_ITEM_COUNT  = "fldPvphfqpuYQZhgd";
const F_PUSH_TAXABLE     = "fldX5Drh72lqcEUvR";
const F_PUSH_EXP_IDS     = "fldZNQLIlZbQPMF9B";
const F_PUSH_DESCRIPTION = "fldswL4blm5aFx14G";

// Expense Push Lines fields
const F_PL_TITLE     = "fld7XZyGWzWKC2H1O";
const F_PL_PUSH      = "fldXIWYGSqLvcs2tJ";
const F_PL_ITEM_NAME = "fldz2kyBPufNRUzuj";
const F_PL_QTY       = "fldhzxPwqz9OPRT1i";
const F_PL_UNIT_COST = "fldIAkYTpZFnoYIY7";
const F_PL_LINE_TOT  = "fldzwMnfOwszQ6ozH";
const F_PL_WIRE_FT   = "fldvpWg3Ky74GiE7q";

// ── Write a Push History header + lines to the inventory base.
// Best-effort: if either write fails we log and continue so the main
// expense push still appears as success to the user. The Push ID is
// returned so the caller can include it in the response.
async function recordPushHistory({ jobName, mainJobId, materialsTotal, taxTotal, taxable, txCount, lines, expenseIds, description, pushedBy }) {
  try {
    const now = new Date();
    const iso = now.toISOString();
    const dateOnly = iso.split("T")[0];
    const titleSafe = String(jobName || "Unknown").substring(0, 80);
    const title = `${dateOnly} — ${titleSafe}`;

    const headerFields = {};
    headerFields[F_PUSH_TITLE]       = title;
    headerFields[F_PUSH_DATE]        = iso;
    headerFields[F_PUSH_BY]          = String(pushedBy || "");
    headerFields[F_PUSH_JOB_NAME]    = String(jobName || "");
    headerFields[F_PUSH_JOB_ID_MAIN] = String(mainJobId || "");
    headerFields[F_PUSH_MAT_TOTAL]   = Math.round(Number(materialsTotal || 0) * 100) / 100;
    headerFields[F_PUSH_TAX_TOTAL]   = Math.round(Number(taxTotal || 0) * 100) / 100;
    headerFields[F_PUSH_TOTAL]       = Math.round((Number(materialsTotal || 0) + Number(taxTotal || 0)) * 100) / 100;
    headerFields[F_PUSH_TX_COUNT]    = Number(txCount || 0);
    headerFields[F_PUSH_ITEM_COUNT]  = (lines || []).length;
    headerFields[F_PUSH_TAXABLE]     = !!taxable;
    headerFields[F_PUSH_EXP_IDS]     = (expenseIds || []).join(", ");
    headerFields[F_PUSH_DESCRIPTION] = String(description || "");

    const created = await atFetch(API_ROOT_INV, encodeURIComponent("Expense Pushes"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields: headerFields }], typecast: true })
    });
    const pushId = created.records?.[0]?.id;
    if (!pushId) {
      console.warn("Push History: header create returned no ID");
      return null;
    }

    // Write line snapshots in batches of 10 — pure best-effort, errors are non-fatal
    const lineRecords = (lines || []).map(l => {
      const itemName = String(l.item || "Item").substring(0, 100);
      const qty      = Number(l.qty || 0);
      const cost     = Number(l.cost || 0);
      const total    = Number(l.total || 0);
      const wireFt   = Number(l.wireFt || 0);
      const lineTitle = `${itemName} × ${qty}`.substring(0, 100);

      const f = {};
      f[F_PL_TITLE]     = lineTitle;
      f[F_PL_PUSH]      = [String(pushId)];
      f[F_PL_ITEM_NAME] = itemName;
      f[F_PL_QTY]       = qty;
      f[F_PL_UNIT_COST] = cost;
      f[F_PL_LINE_TOT]  = total;
      if (wireFt > 0) f[F_PL_WIRE_FT] = wireFt;
      return { fields: f };
    });

    for (let i = 0; i < lineRecords.length; i += 10) {
      const batch = lineRecords.slice(i, i + 10);
      try {
        await atFetch(API_ROOT_INV, encodeURIComponent("Expense Push Lines"), {
          method: "POST",
          body: JSON.stringify({ records: batch, typecast: true })
        });
      } catch(lineErr) {
        console.warn("Push History: line batch write failed (non-fatal):", lineErr.message);
      }
    }

    return pushId;
  } catch(e) {
    console.warn("Push History: header write failed (non-fatal):", e.message);
    return null;
  }
}

// ── PUSH HISTORY LIST (most recent first) ─────────────────
async function handlePushHistory(params) {
  const limit = Math.min(Number(params?.limit || 100), 500);
  const records = await fetchAll(API_ROOT_INV, "Expense Pushes", {
    sortField: "Date Pushed",
    sortDir: "desc",
    maxRecords: limit
  });

  const pushes = records.map(r => {
    const f = r.fields || {};
    return {
      id:            r.id,
      title:         f["Push Title"] || "",
      datePushed:    f["Date Pushed"] || "",
      pushedBy:      f["Pushed By"] || "",
      jobName:       f["Job Name"] || "",
      jobIdMain:     f["Job ID (Main)"] || "",
      materialsTotal: Number(f["Materials Total"] || 0),
      taxTotal:      Number(f["Tax Total"] || 0),
      total:         Number(f["Total Pushed"] || 0),
      txCount:       Number(f["Tx Count"] || 0),
      itemCount:     Number(f["Item Count"] || 0),
      taxable:       !!f["Taxable"],
      expenseIds:    f["Expense Record IDs"] || "",
      description:   f["Description"] || ""
    };
  });

  return resp(200, { ok: true, pushes });
}

// ── PUSH HISTORY DETAIL (one push with its line snapshots) ───
async function handlePushHistoryDetail(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing push id." });

  // Fetch header
  const headerData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Expense Pushes")}/${id}`,
    { method: "GET" }
  );
  if (!headerData?.id) return resp(404, { ok: false, error: "Push not found." });
  const f = headerData.fields || {};

  // Fetch lines linked to this push — pull all lines and filter client-side
  // (small table, simpler than building a complex formula filter)
  const allLines = await fetchAll(API_ROOT_INV, "Expense Push Lines", {});
  const lines = allLines
    .filter(r => {
      const linkArr = r.fields?.["Push"] || [];
      return linkArr.some(link => {
        const linkId = typeof link === "object" ? link.id : String(link);
        return linkId === id;
      });
    })
    .map(r => {
      const lf = r.fields || {};
      return {
        id:        r.id,
        itemName:  lf["Item Name"] || lf["Line Title"] || "Item",
        qty:       Number(lf["Quantity"] || 0),
        unitCost:  Number(lf["Unit Cost"] || 0),
        lineTotal: Number(lf["Line Total"] || 0),
        wireFt:    Number(lf["Wire Ft"] || 0)
      };
    })
    .sort((a, b) => b.lineTotal - a.lineTotal);  // biggest dollars first

  return resp(200, {
    ok: true,
    push: {
      id:            headerData.id,
      title:         f["Push Title"] || "",
      datePushed:    f["Date Pushed"] || "",
      pushedBy:      f["Pushed By"] || "",
      jobName:       f["Job Name"] || "",
      jobIdMain:     f["Job ID (Main)"] || "",
      materialsTotal: Number(f["Materials Total"] || 0),
      taxTotal:      Number(f["Tax Total"] || 0),
      total:         Number(f["Total Pushed"] || 0),
      txCount:       Number(f["Tx Count"] || 0),
      itemCount:     Number(f["Item Count"] || 0),
      taxable:       !!f["Taxable"],
      expenseIds:    f["Expense Record IDs"] || "",
      description:   f["Description"] || "",
      lines
    }
  });
}

// ── PUSH EXPENSES TO MAIN BASE ─────────────────────────────
async function handlePushExpenses(body) {
  const { pending, pdfs, pushedBy } = body || {};
  if (!pending || !pending.length) return resp(400, { ok: false, error: "Nothing to push." });

  const TAX_RATE      = 0.075;
  const today         = new Date().toISOString().split("T")[0];
  const NEE_VENDOR_ID = "recdVrxXdSOH0dlXO";
  const expenseIds    = [];
  const allTxIds      = [];
  const pushHistoryIds = [];
  let   pdfUploads    = 0;

  // Look up the "Receipt / Document" field ID once if PDFs are provided
  let receiptFieldId = null;
  if (pdfs && pdfs.some(p => p)) {
    console.log(`PDF array received: ${pdfs.length} entries, non-null: ${pdfs.filter(Boolean).length}`);
    receiptFieldId = await getReceiptFieldId();
    console.log("Using receipt field ID:", receiptFieldId || "NOT FOUND — PDF upload will be skipped");
  } else {
    console.log("No PDFs in payload — skipping attachment upload");
  }

  for (let i = 0; i < pending.length; i++) {
    const g = pending[i];
    const { jobId, jobName, taxable, lines, txIds } = g;
    if (!jobId || !lines?.length) continue;

    const jobTotal = lines.reduce((s, l) => s + (l.total || 0), 0);
    if (jobTotal <= 0) continue;

    // Build description — show footage for wire items
    const desc = lines.map(l => {
      const sign = l.qty < 0 ? "−" : "";
      const qtyStr = l.wireFt > 0
        ? `${Math.abs(l.qty)}lbs (${l.wireFt.toLocaleString()}ft)`
        : `${sign}${Math.abs(l.qty)}`;
      return `${l.item} ×${qtyStr}`;
    }).join(", ");

    // Track expense IDs created for this single job, for the history record
    const jobExpenseIds = [];
    let   jobTaxAmount  = 0;

    // Create materials expense
    const matFields = {
      "fldPNFIzq1grsdxYi": [String(jobId)],
      "fldlTUL8hsPkReBAB": [String(NEE_VENDOR_ID)],
      "fldwbLPIafVtmaSeb": Math.round(jobTotal * 100) / 100,
      "fldX2x2J0xkRyMY3y": "Materials",
      "fldCCPYdyWAOGchWb": today,
      "fldJTg0ekrdZ4Jqr6": "Not Reviewed",
      "fld9Afieu4ofjvhSb": true,
      "fldnSQEOnyq3sho5g": "Inventory materials — " + desc
    };

    const matResp = await atFetch(API_ROOT_MAIN, encodeURIComponent("Expenses"), {
      method: "POST",
      body: JSON.stringify({ records: [{ fields: matFields }], typecast: true })
    });
    const matExpenseId = matResp.records?.[0]?.id;
    if (matExpenseId) {
      expenseIds.push(matExpenseId);
      jobExpenseIds.push(matExpenseId);

      // Upload PDF receipt if provided
      const pdfBase64 = pdfs?.[i];
      if (pdfBase64 && receiptFieldId) {
        try {
          const safeName = (jobName || "job").replace(/[^a-z0-9]/gi, "_").substring(0, 30);
          const filename = `NEE_Materials_${safeName}_${today}.pdf`;
          await uploadPdfToExpense(matExpenseId, receiptFieldId, pdfBase64, filename);
          pdfUploads++;
          console.log(`PDF uploaded for job: ${jobName}`);
        } catch(uploadErr) {
          console.error("PDF upload failed (non-fatal):", uploadErr.message);
        }
      }
    }

    // Create sales tax expense if taxable
    if (taxable) {
      jobTaxAmount = Math.round(jobTotal * TAX_RATE * 100) / 100;
      const taxFields = {
        "fldPNFIzq1grsdxYi": [String(jobId)],
        "fldlTUL8hsPkReBAB": [String(NEE_VENDOR_ID)],
        "fldwbLPIafVtmaSeb": jobTaxAmount,
        "fldX2x2J0xkRyMY3y": "Materials",
        "fldCCPYdyWAOGchWb": today,
        "fldJTg0ekrdZ4Jqr6": "Not Reviewed",
        "fld9Afieu4ofjvhSb": true,
        "fldnSQEOnyq3sho5g": "Sales tax (7.5%) on inventory materials — " + jobName
      };
      const taxResp = await atFetch(API_ROOT_MAIN, encodeURIComponent("Expenses"), {
        method: "POST",
        body: JSON.stringify({ records: [{ fields: taxFields }], typecast: true })
      });
      const taxExpenseId = taxResp.records?.[0]?.id;
      if (taxExpenseId) {
        expenseIds.push(taxExpenseId);
        jobExpenseIds.push(taxExpenseId);
      }
    }

    if (txIds?.length) allTxIds.push(...txIds);

    // Write Push History snapshot for this job — best-effort, non-fatal
    const historyId = await recordPushHistory({
      jobName,
      mainJobId:      jobId,
      materialsTotal: jobTotal,
      taxTotal:       jobTaxAmount,
      taxable:        !!taxable,
      txCount:        (txIds || []).length,
      lines,                 // [{item, qty, cost, total, wireFt}, ...]
      expenseIds:     jobExpenseIds,
      description:    desc,
      pushedBy:       pushedBy || ""
    });
    if (historyId) pushHistoryIds.push(historyId);
  }

  // Mark all transactions as Expense Created
  for (let i = 0; i < allTxIds.length; i += 10) {
    const batch = allTxIds.slice(i, i + 10).map(id => ({
      id,
      fields: { "fldO7Z0L7tpAvrgtH": true }
    }));
    await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Transactions"), {
      method: "PATCH",
      body: JSON.stringify({ records: batch })
    });
  }

  return resp(200, {
    ok: true,
    count:           expenseIds.length,
    txCount:         allTxIds.length,
    pdfUploads,
    pushHistoryIds
  });
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

// ── CREATE NEW ITEM ───────────────────────────────────────
async function handleCreateItem(body) {
  const { name, category, productSize, uom, barcode, cost, wireFtPerLb, active } = body || {};
  if (!name || !name.trim()) return resp(400, { ok: false, error: "Item name is required." });

  // Check for duplicate barcode
  if (barcode && barcode.trim()) {
    const existing = await fetchAll(API_ROOT_INV, "Inventory Items", {
      filter: `{Barcode Value}='${barcode.trim()}'`
    });
    if (existing.length > 0) {
      return resp(409, { ok: false, error: `Barcode already used by: ${existing[0].fields["Item Name"] || "another item"}` });
    }
  }

  // Use field NAMES with typecast:true — works for all field types including singleSelect
  const fields = {
    "Item Name":   name.trim(),
    "Active Item": active === false ? false : true   // default true when not specified
  };
  if (category && category.trim())       fields["Category"]          = category.trim();
  if (productSize && productSize.trim()) fields["Product Size"]      = productSize.trim();
  if (uom && uom.trim())                 fields["Unit of Measure"]   = uom.trim();
  if (barcode && barcode.trim())         fields["Barcode Value"]     = barcode.trim();
  if (cost && Number(cost) > 0)          fields["Default Unit Cost"] = Number(cost);
  if (wireFtPerLb && Number(wireFtPerLb) > 0) fields["Wire ft/lb"]   = Number(wireFtPerLb);

  const data = await atFetch(API_ROOT_INV, encodeURIComponent("Inventory Items"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  const newRecord = data.records?.[0];
  if (!newRecord) throw new Error("No record returned from Airtable");

  return resp(200, {
    ok:   true,
    item: {
      id:      newRecord.id,
      name:    newRecord.fields["Item Name"] || name.trim(),
      cat:     newRecord.fields["Category"]?.name || newRecord.fields["Category"] || category || "",
      size:    newRecord.fields["Product Size"]?.name || newRecord.fields["Product Size"] || productSize || "",
      uom:     newRecord.fields["Unit of Measure"]?.name || newRecord.fields["Unit of Measure"] || uom || "",
      barcode: newRecord.fields["Barcode Value"] || barcode || "",
      cost:    newRecord.fields["Default Unit Cost"] || cost || 0,
      wireFtPerLb: newRecord.fields["Wire ft/lb"] || wireFtPerLb || 0
    }
  });
}

// ── UPDATE ITEM COST ───────────────────────────────────────
async function handleUpdateItemCost(body) {
  const { itemId, cost } = body || {};
  if (!itemId || cost === undefined) return resp(400, { ok: false, error: "Missing itemId or cost." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fld8aEhTzmEbqgIg4": Number(cost) } })
  });
  return resp(200, { ok: true });
}

// ── DELETE ─────────────────────────────────────────────────
async function handleDelete(body) {
  const { txId } = body || {};
  if (!txId) return resp(400, { ok: false, error: "Missing txId." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Transactions")}/${txId}`, { method: "DELETE" });
  return resp(200, { ok: true, deleted: txId });
}

// ── STOCK LEVELS BY ITEM ──────────────────────────────────
async function handleStockLevels(params) {
  const { itemId, itemName } = params || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  // Fetch ALL stock level records and filter in JavaScript by item record ID.
  // This is reliable regardless of how the item name appears in the Stock ID formula.
  const allRecords = await fetchAll(API_ROOT_INV, "Stock Levels", {});

  const records = allRecords.filter(r => {
    const itemLinks = r.fields?.["Item"] || [];
    return itemLinks.some(link => {
      const linkId = typeof link === "object" ? link.id : String(link);
      return linkId === itemId;
    });
  });

  const levels = records.map(r => {
    const f = r.fields || {};
    const stockId = f["Stock ID"] || "";
    const parts   = stockId.split(" | ");
    const locName = parts[parts.length - 1] || "";
    const wireFtLbRaw = f["Wire ft/lb"];
    const wireFtLb    = Array.isArray(wireFtLbRaw) ? (wireFtLbRaw[0] || 0) : (wireFtLbRaw || 0);
    const wireFtRaw   = f["Wire (Ft.)"];
    const wireFt      = Array.isArray(wireFtRaw) ? (wireFtRaw[0] || 0) : (wireFtRaw || 0);
    return {
      id:           r.id,
      locationName: locName,
      qtyOnHand:    f["Quantity On Hand"]      || 0,
      unitCost:     f["Unit Cost (from Item)"] || 0,
      totalValue:   f["Total Value"]           || 0,
      reorderPoint: f["Reorder Point"]         || 0,
      wireWeight:   wireFtLb,
      wireFt:       wireFt
    };
  });

  levels.sort((a, b) => a.locationName.localeCompare(b.locationName));
  return resp(200, { ok: true, levels });
}

// ── STOCK LEVELS ALL (for Category Browse in Check Stock) ─
// Returns every stock level record with its linked item ID so the
// client can group by item and show per-location breakdowns.
async function handleStockLevelsAll() {
  const allRecords = await fetchAll(API_ROOT_INV, "Stock Levels", {});

  const levels = allRecords.map(r => {
    const f = r.fields || {};

    // Extract the linked item record ID
    const itemLinks = f["Item"] || [];
    const itemId = itemLinks.length
      ? (typeof itemLinks[0] === "object" ? itemLinks[0].id : String(itemLinks[0]))
      : "";

    // Location name is the last segment of the Stock ID formula field
    const stockId = f["Stock ID"] || "";
    const parts   = stockId.split(" | ");
    const locName = parts[parts.length - 1] || "";

    // Wire fields — lookup fields return arrays, normalize to number
    const wireFtLbRaw = f["Wire ft/lb"];
    const wireFtLb    = Array.isArray(wireFtLbRaw) ? (wireFtLbRaw[0] || 0) : (wireFtLbRaw || 0);
    const wireFtRaw   = f["Wire (Ft.)"];
    const wireFt      = Array.isArray(wireFtRaw) ? (wireFtRaw[0] || 0) : (wireFtRaw || 0);

    return {
      id:           r.id,
      itemId:       itemId,
      locationName: locName,
      qtyOnHand:    f["Quantity On Hand"]      || 0,
      unitCost:     f["Unit Cost (from Item)"] || 0,
      totalValue:   f["Total Value"]           || 0,
      reorderPoint: f["Reorder Point"]         || 0,
      wireWeight:   wireFtLb,
      wireFt:       wireFt
    };
  });

  return resp(200, { ok: true, levels });
}

// ── REORDER ALERTS ────────────────────────────────────────
async function handleReorderAlerts() {
  const records = await fetchAll(API_ROOT_INV, "Stock Levels", {
    filter: `AND({Reorder Point} > 0, {Quantity On Hand} <= {Reorder Point})`
  });

  const groups = {};

  records.forEach(r => {
    const f       = r.fields || {};
    const stockId = f["Stock ID"] || "";
    const parts   = stockId.split(" | ");
    const locName  = parts[parts.length - 1] || "Unknown";
    const itemName = parts.slice(0, -1).join(" | ") || stockId;
    const qty      = f["Quantity On Hand"] || 0;
    const reorder  = f["Reorder Point"]    || 0;
    // Handle lookup field returning array
    const wireFtLbRaw = f["Wire ft/lb"];
    const wireWeight  = Array.isArray(wireFtLbRaw) ? (wireFtLbRaw[0] || 0) : (wireFtLbRaw || 0);
    const wireFtRaw   = f["Wire (Ft.)"];
    const wireFt      = Array.isArray(wireFtRaw) ? (wireFtRaw[0] || 0) : (wireFtRaw || 0);

    if (!groups[locName]) groups[locName] = [];
    groups[locName].push({
      itemName,
      qtyOnHand:    qty,
      reorderPoint: reorder,
      shortBy:      reorder - qty,
      wireWeight,
      wireFt
    });
  });

  Object.keys(groups).forEach(loc => {
    groups[loc].sort((a, b) => a.itemName.localeCompare(b.itemName));
  });

  return resp(200, { ok: true, groups });
}

// ── UPDATE REORDER POINT ──────────────────────────────────
async function handleUpdateReorderPoint(body) {
  const { stockLevelId, reorderPoint } = body || {};
  if (!stockLevelId) return resp(400, { ok: false, error: "Missing stockLevelId." });
  if (reorderPoint === undefined) return resp(400, { ok: false, error: "Missing reorderPoint." });
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Stock Levels")}/${stockLevelId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fldYS6soPXlHkxI1V": Number(reorderPoint) } })
  });
  return resp(200, { ok: true });
}

// ═══════════════════════════════════════════════════════════
// ESTIMATES — list, get, create, update, delete
// ═══════════════════════════════════════════════════════════

const EST_TABLE_ID  = "tblULCJaVsLXk4Af0";
const LINE_TABLE_ID = "tblhRadsyvlLw5Lp5";

// Estimates table fields
const F_EST_JOB_NAME    = "fld5QDgzSOXNAZdOc";
const F_EST_JOB_ID      = "fldId8eR0C8TeSfy4";
const F_EST_CREATED_BY  = "fldl0xEYcPvNbs69U";
const F_EST_STATUS      = "fldAu3oNbywGe8vBh";
const F_EST_NOTES       = "fld7sOLbNZxEqP0zs";

// Estimate Line Items table fields
const F_LINE_ESTIMATE   = "fldCXpRJt9g3yCB9r";
const F_LINE_ITEM       = "fld50ttitFcM2uPap";
const F_LINE_QTY        = "fld9mDWjvdd4AfXnn";
const F_LINE_UNIT_COST  = "fldkTzFNJydVX1iK3";

// ── ESTIMATES LIST ─────────────────────────────────────────
async function handleEstimatesList(params) {
  const records = await fetchAll(API_ROOT_INV, "Estimates", {
    sortField: "Date Created",
    sortDir: "desc"
  });

  const estimates = records.map(r => {
    const f = r.fields || {};
    return {
      id:        r.id,
      jobName:   f["Job Name"] || "",
      jobId:     f["Job ID"] || "",
      dateCreated: f["Date Created"] || "",
      createdBy: f["Created By"] || "",
      status:    f["Status"]?.name || f["Status"] || "Draft",
      notes:     f["Notes"] || "",
      total:     f["Total"] || 0,
      lineCount: (f["Estimate Line Items"] || []).length
    };
  });

  return resp(200, { ok: true, estimates });
}

// ── GET ONE ESTIMATE WITH LINES ────────────────────────────
async function handleEstimateGet(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // Fetch estimate
  const estData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimates")}/${id}`,
    { method: "GET" }
  );
  if (!estData?.id) return resp(404, { ok: false, error: "Estimate not found." });

  const ef = estData.fields || {};
  const lineIds = (ef["Estimate Line Items"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  // Fetch all line items belonging to this estimate
  let lines = [];
  if (lineIds.length) {
    const [lineRecords, itemRecords] = await Promise.all([
      fetchAll(API_ROOT_INV, "Estimate Line Items", {}),
      fetchAll(API_ROOT_INV, "Inventory Items", {})
    ]);

    const itemMap = {};
    itemRecords.forEach(r => {
      itemMap[r.id] = {
        name: r.fields["Item Name"] || r.id,
        uom:  r.fields["Unit of Measure"]?.name || r.fields["Unit of Measure"] || "",
        cost: r.fields["Default Unit Cost"] || 0
      };
    });

    lines = lineRecords
      .filter(r => lineIds.includes(r.id))
      .map(r => {
        const f = r.fields || {};
        const itemArr = f["Inventory Item"] || [];
        const itemId = itemArr.length
          ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
          : "";
        const itemData = itemMap[itemId] || {};
        return {
          id:       r.id,
          lineNum:  f["Line ID"] || 0,
          itemId:   itemId,
          itemName: itemData.name || (f["Description"] || ""),
          uom:      itemData.uom || "",
          isMisc:   !itemId,
          description: f["Description"] || "",
          qty:      f["Quantity"] || 0,
          unitCost: f["Unit Cost at Time of Estimate"] || 0,
          lineTotal: f["Line Total"] || 0
        };
      })
      .sort((a, b) => (a.lineNum || 0) - (b.lineNum || 0));
  }

  return resp(200, {
    ok: true,
    estimate: {
      id:        estData.id,
      jobName:   ef["Job Name"] || "",
      jobId:     ef["Job ID"] || "",
      dateCreated: ef["Date Created"] || "",
      createdBy: ef["Created By"] || "",
      status:    ef["Status"]?.name || ef["Status"] || "Draft",
      notes:     ef["Notes"] || "",
      total:     ef["Total"] || 0,
      lines
    }
  });
}

// ── CREATE ESTIMATE ────────────────────────────────────────
async function handleEstimateCreate(body) {
  const { jobName, jobId, status, notes, createdBy, lines } = body || {};
  if (!jobName || !jobName.trim()) return resp(400, { ok: false, error: "Job name is required." });

  const estFields = {
    [F_EST_JOB_NAME]:   String(jobName).trim(),
    [F_EST_JOB_ID]:     String(jobId || "").trim(),
    [F_EST_STATUS]:     status || "Estimating",
    [F_EST_NOTES]:      String(notes || "").trim(),
    [F_EST_CREATED_BY]: String(createdBy || "").trim()
  };

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Estimates"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields: estFields }], typecast: true })
  });

  const newId = created.records?.[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create estimate." });

  // Create line items if provided (in batches of 10)
  if (lines && lines.length) {
    await createLineItems(newId, lines);
  }

  return resp(200, { ok: true, id: newId });
}

// ── UPDATE ESTIMATE ────────────────────────────────────────
async function handleEstimateUpdate(body) {
  const { id, status, notes, jobName, jobId, lines, replaceLines } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // Update header fields if provided
  const headerFields = {};
  if (status !== undefined)  headerFields[F_EST_STATUS]   = status;
  if (notes  !== undefined)  headerFields[F_EST_NOTES]    = String(notes || "");
  if (jobName !== undefined) headerFields[F_EST_JOB_NAME] = String(jobName || "");
  if (jobId   !== undefined) headerFields[F_EST_JOB_ID]   = String(jobId || "");

  if (Object.keys(headerFields).length) {
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimates")}/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ fields: headerFields, typecast: true })
    });
  }

  // If replaceLines is true, delete existing lines and add new ones.
  // Otherwise, just append the new lines if provided.
  if (replaceLines && lines !== undefined) {
    // Get existing line IDs for this estimate
    const estData = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Estimates")}/${id}`,
      { method: "GET" }
    );
    const existingIds = (estData.fields["Estimate Line Items"] || [])
      .map(l => typeof l === "object" ? l.id : String(l));

    // Delete existing lines in batches of 10
    for (let i = 0; i < existingIds.length; i += 10) {
      const batch = existingIds.slice(i, i + 10);
      const qs = batch.map(rid => `records[]=${rid}`).join("&");
      await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Line Items")}?${qs}`, {
        method: "DELETE"
      });
    }

    // Create new lines
    if (lines.length) await createLineItems(id, lines);
  } else if (lines && lines.length) {
    // Just append
    await createLineItems(id, lines);
  }

  return resp(200, { ok: true, id });
}

// ── DELETE ESTIMATE ────────────────────────────────────────
async function handleEstimateDelete(body) {
  const { id } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing estimate id." });

  // Get linked line items first
  const estData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Estimates")}/${id}`,
    { method: "GET" }
  );
  const lineIds = (estData.fields["Estimate Line Items"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  // Delete lines in batches of 10
  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimate Line Items")}?${qs}`, {
      method: "DELETE"
    });
  }

  // Delete the estimate
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Estimates")}/${id}`, {
    method: "DELETE"
  });

  return resp(200, { ok: true, deleted: id });
}

// ── HELPER: Create line items in batches of 10 ─────────────
async function createLineItems(estimateId, lines) {
  for (let i = 0; i < lines.length; i += 10) {
    const batch = lines.slice(i, i + 10).map(l => {
      const fields = {
        [F_LINE_ESTIMATE]:  [String(estimateId)],
        [F_LINE_QTY]:       Number(l.qty || 0),
        [F_LINE_UNIT_COST]: Number(l.unitCost || 0)
      };
      // Inventory item link — only for non-Misc lines
      if (l.itemId) {
        fields[F_LINE_ITEM] = [String(l.itemId)];
      }
      // Description — only attempt to set if it's a Misc line.
      if (l.isMisc && l.description) {
        fields["Description"] = String(l.description).trim();
      }
      return { fields };
    });

    try {
      await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Line Items"), {
        method: "POST",
        body: JSON.stringify({ records: batch, typecast: true })
      });
    } catch (err) {
      // If failure is due to missing Description field, retry without it
      if (err.message && err.message.toLowerCase().includes("description")) {
        const retryBatch = batch.map(b => {
          const f = { ...b.fields };
          delete f.Description;
          return { fields: f };
        });
        await atFetch(API_ROOT_INV, encodeURIComponent("Estimate Line Items"), {
          method: "POST",
          body: JSON.stringify({ records: retryBatch, typecast: true })
        });
      } else {
        throw err;
      }
    }
  }
}

// ═══════════════════════════════════════════════════════════
// MATERIAL ORDERS — list, get, create, mark complete, delete
// ═══════════════════════════════════════════════════════════

const ORDER_TABLE_ID      = "tblLMunp1fSrZV4mH";
const ORDER_LINE_TABLE_ID = "tblERYikTOpPhklPw";

const F_ORD_ESTIMATE   = "fld446AqUqFbATskC";
const F_ORD_JOB_NAME   = "fldst9PryHJTJYWeC";
const F_ORD_CREATED_BY = "fldeRDqMUpfIQF36D";
const F_ORD_VENDOR     = "fldgXcZ2EMNR5nfTG";
const F_ORD_STATUS     = "fldshk9Rek2BnVAxc";

const F_OL_ORDER       = "fldkAHSFDQwsQqCyd";
const F_OL_ITEM        = "fldlNL42Hj9fEKNVR";
const F_OL_DESCRIPTION = "fldoDLObzUjkHyhcA";
const F_OL_QTY         = "fldI8RfBvcD8oGpcg";

// ── ACTIVE ORDERS LIST ────────────────────────────────────
async function handleOrdersList(params) {
  const showComplete = params?.includeComplete === "1";
  const createdBy    = params?.createdBy;   // optional filter by user

  // Build filter formula
  const filters = [];
  if (!showComplete) filters.push(`{Status}='Active'`);
  if (createdBy) {
    // Escape single quotes in name
    const safeName = String(createdBy).replace(/'/g, "\\'");
    filters.push(`{Created By}='${safeName}'`);
  }
  const filter = filters.length === 0 ? undefined
               : filters.length === 1 ? filters[0]
               : `AND(${filters.join(",")})`;

  const records = await fetchAll(API_ROOT_INV, "Material Orders", {
    filter,
    sortField: "Date Created",
    sortDir: "desc"
  });

  const orders = records.map(r => {
    const f = r.fields || {};
    return {
      id:          r.id,
      orderId:     f["Order ID"] || 0,
      jobName:     f["Job Name"] || "",
      vendor:      f["Vendor / Notes"] || "",
      status:      f["Status"]?.name || f["Status"] || "Active",
      dateCreated: f["Date Created"] || "",
      createdBy:   f["Created By"] || "",
      lineCount:   (f["Material Order Lines"] || []).length,
      totalItems:  f["Total Items"] || 0
    };
  });

  return resp(200, { ok: true, orders });
}

// ── GET ONE ORDER WITH LINES ──────────────────────────────
async function handleOrderGet(params) {
  const { id } = params || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  const orderData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Material Orders")}/${id}`,
    { method: "GET" }
  );
  if (!orderData?.id) return resp(404, { ok: false, error: "Order not found." });

  const of = orderData.fields || {};
  const lineIds = (of["Material Order Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  let lines = [];
  if (lineIds.length) {
    const [lineRecords, itemRecords] = await Promise.all([
      fetchAll(API_ROOT_INV, "Material Order Lines", {}),
      fetchAll(API_ROOT_INV, "Inventory Items", {})
    ]);

    const itemMap = {};
    itemRecords.forEach(r => {
      itemMap[r.id] = {
        name: r.fields["Item Name"] || r.id,
        uom:  r.fields["Unit of Measure"]?.name || r.fields["Unit of Measure"] || ""
      };
    });

    lines = lineRecords
      .filter(r => lineIds.includes(r.id))
      .map(r => {
        const f = r.fields || {};
        const itemArr = f["Inventory Item"] || [];
        const itemId = itemArr.length
          ? (typeof itemArr[0] === "object" ? itemArr[0].id : String(itemArr[0]))
          : "";
        const itemData = itemMap[itemId] || {};

        // Detect " [BOX]" suffix marker on description — strip it, set isBox
        const BOX_MARKER = " [BOX]";
        let rawDesc = f["Description"] || "";
        let isBox = false;
        if (typeof rawDesc === "string" && rawDesc.endsWith(BOX_MARKER)) {
          isBox = true;
          rawDesc = rawDesc.slice(0, -BOX_MARKER.length);
        }

        return {
          id:          r.id,
          lineNum:     f["Line Item ID"] || 0,
          itemId:      itemId,
          itemName:    itemData.name || rawDesc || "",
          uom:         itemData.uom || "",
          description: rawDesc,
          qty:         f["Quantity Ordered"] || 0,
          isMisc:      !itemId,
          isBox:       isBox
        };
      })
      .sort((a, b) => (a.lineNum || 0) - (b.lineNum || 0));
  }

  return resp(200, {
    ok: true,
    order: {
      id:          orderData.id,
      orderId:     of["Order ID"] || 0,
      jobName:     of["Job Name"] || "",
      vendor:      of["Vendor / Notes"] || "",
      status:      of["Status"]?.name || of["Status"] || "Active",
      dateCreated: of["Date Created"] || "",
      createdBy:   of["Created By"] || "",
      lines
    }
  });
}

// ── CREATE ORDER ──────────────────────────────────────────
async function handleOrderCreate(body) {
  const { estimateId, jobName, vendor, createdBy, lines } = body || {};
  if (!jobName || !jobName.trim()) return resp(400, { ok: false, error: "Job name is required." });
  if (!lines || !lines.length) return resp(400, { ok: false, error: "Order has no items." });

  const orderFields = {
    [F_ORD_JOB_NAME]:   String(jobName).trim(),
    [F_ORD_VENDOR]:     String(vendor || "").trim(),
    [F_ORD_CREATED_BY]: String(createdBy || "").trim(),
    [F_ORD_STATUS]:     "Active"
  };
  if (estimateId) {
    orderFields[F_ORD_ESTIMATE] = [String(estimateId)];
  }

  const created = await atFetch(API_ROOT_INV, encodeURIComponent("Material Orders"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields: orderFields }], typecast: true })
  });

  const newId = created.records?.[0]?.id;
  if (!newId) return resp(500, { ok: false, error: "Failed to create order." });

  // Create order lines in batches of 10
  if (lines && lines.length) {
    await createOrderLinesHelper(newId, lines);
  }

  // Re-fetch the created order to get the autonumber Order ID
  let orderId = null;
  try {
    const refreshed = await atFetch(
      API_ROOT_INV,
      `${encodeURIComponent("Material Orders")}/${newId}`,
      { method: "GET" }
    );
    orderId = refreshed.fields?.["Order ID"] || null;
  } catch(e) {
    console.warn("Failed to fetch new order autonumber:", e.message);
  }

  return resp(200, { ok: true, id: newId, orderId });
}

// ── HELPER: Create order lines in batches of 10 ──────────
async function createOrderLinesHelper(orderId, lines) {
  for (let i = 0; i < lines.length; i += 10) {
    const batch = lines.slice(i, i + 10).map(l => {
      const fields = {
        [F_OL_ORDER]: [String(orderId)],
        [F_OL_QTY]:   Number(l.qty || 0)
      };
      if (l.itemId) {
        fields[F_OL_ITEM] = [String(l.itemId)];
      }
      // Always store description for traceability — for inventory items this
      // captures the name at order time so historical orders survive renames.
      // Also append " [BOX]" marker so isBox persists without a schema change.
      let desc = String(l.description || "").trim();
      if (l.isBox) {
        // Ensure we have something in description so the BOX marker isn't orphaned
        if (!desc) desc = "Box order";
        desc = desc + " [BOX]";
      }
      if (desc) {
        fields[F_OL_DESCRIPTION] = desc;
      }
      return { fields };
    });

    await atFetch(API_ROOT_INV, encodeURIComponent("Material Order Lines"), {
      method: "POST",
      body: JSON.stringify({ records: batch, typecast: true })
    });
  }
}

// ── HELPER: Delete all lines for an order ────────────────
async function deleteOrderLines(orderId) {
  const orderData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Material Orders")}/${orderId}`,
    { method: "GET" }
  );
  const lineIds = (orderData.fields["Material Order Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Order Lines")}?${qs}`, {
      method: "DELETE"
    });
  }
}

// ── UPDATE ORDER (status / vendor / notes / lines) ────────────────
async function handleOrderUpdate(body) {
  const { id, status, vendor, lines, replaceLines } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  const fields = {};
  if (status !== undefined) fields[F_ORD_STATUS] = status;
  if (vendor !== undefined) fields[F_ORD_VENDOR] = String(vendor || "");

  // Header fields — only patch if we have any
  if (Object.keys(fields).length) {
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Orders")}/${id}`, {
      method: "PATCH",
      body: JSON.stringify({ fields, typecast: true })
    });
  }

  // Line editing
  if (replaceLines && lines !== undefined) {
    await deleteOrderLines(id);
    if (lines.length) {
      await createOrderLinesHelper(id, lines);
    }
  }

  if (!Object.keys(fields).length && !replaceLines) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }

  return resp(200, { ok: true, id });
}

// ── DELETE ORDER ──────────────────────────────────────────
async function handleOrderDelete(body) {
  const { id } = body || {};
  if (!id) return resp(400, { ok: false, error: "Missing order id." });

  // Get linked line ids
  const orderData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Material Orders")}/${id}`,
    { method: "GET" }
  );
  const lineIds = (orderData.fields["Material Order Lines"] || [])
    .map(l => typeof l === "object" ? l.id : String(l));

  // Delete lines in batches of 10
  for (let i = 0; i < lineIds.length; i += 10) {
    const batch = lineIds.slice(i, i + 10);
    const qs = batch.map(rid => `records[]=${rid}`).join("&");
    await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Order Lines")}?${qs}`, {
      method: "DELETE"
    });
  }

  // Delete the order
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Material Orders")}/${id}`, {
    method: "DELETE"
  });

  return resp(200, { ok: true, deleted: id });
}

// ── ACTIVE ORDERS COUNT (for badge on home button) ────────
async function handleOrdersCount() {
  const records = await fetchAll(API_ROOT_INV, "Material Orders", {
    filter: `{Status}='Active'`
  });
  return resp(200, { ok: true, count: records.length });
}

// ═══════════════════════════════════════════════════════════
// VENDOR PRICING — per-item lookup + sync to Default Unit Cost
// ═══════════════════════════════════════════════════════════

// ── GET VENDOR PRICING FOR ONE ITEM ───────────────────────
// Returns the list of Vendor Pricing records for a single item plus a summary
// block with Default Unit Cost, the live rollup, and the variance between them.
// Rollup field name must match exactly what's on the Inventory Items table.
async function handleItemVendorPricing(params) {
  const { itemId } = params || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  // Fetch the item record AND all Vendor Pricing records AND all Vendors in parallel.
  // Filtering Vendor Pricing by linked record on the API side requires FIND()
  // against an ARRAYJOIN, which is brittle across renames — simpler and safer to
  // pull all (vendor pricing is a small table) and filter in JS.
  // Vendors are pulled to build an ID -> name map because the REST API returns
  // linked records as plain record IDs, not hydrated {id, name} objects.
  const [itemData, allPricing, allVendors] = await Promise.all([
    atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, { method: "GET" }),
    fetchAll(API_ROOT_INV, "Vendor Pricing", {}),
    fetchAll(API_ROOT_INV, "Vendors", {})
  ]);

  // ID -> Vendor Name
  const vendorNameById = {};
  allVendors.forEach(v => {
    vendorNameById[v.id] = v.fields?.["Vendor Name"] || "";
  });

  const f           = itemData?.fields || {};
  const defaultCost = Number(f["Default Unit Cost"] || 0);
  // Rollup returns a number if a preferred record has Unit Cost set, else empty
  const liveCost    = Number(f["Unit Cost Rollup (Live)"] || 0);

  // Filter Vendor Pricing to this item only
  const pricing = allPricing.filter(r => {
    const links = r.fields?.["Inventory Item"] || [];
    return links.some(l => {
      const lid = typeof l === "object" ? l.id : String(l);
      return lid === itemId;
    });
  });

  const vendors = pricing.map(r => {
    const pf        = r.fields || {};
    const vendorArr = pf["Vendor"] || [];
    const first     = vendorArr[0];
    // Link fields come back as either plain IDs (REST API default) or {id, name}
    // objects — handle both so this works regardless of Airtable response shape.
    const vendorId  = first
      ? (typeof first === "object" ? first.id : String(first))
      : "";
    const vendorName = vendorNameById[vendorId]
      || (typeof first === "object" && first.name ? first.name : "")
      || "";
    return {
      id:          r.id,
      vendorId,
      vendorName,
      unitCost:    Number(pf["Unit Cost"] || 0),
      uom:         pf["Unit of Measure"]?.name || pf["Unit of Measure"] || "",
      partNumber:  pf["Vendor Part Number"] || "",
      minOrderQty: Number(pf["Min Order Qty"] || 0),
      leadTime:    Number(pf["Lead Time (days)"] || 0),
      lastUpdate:  pf["Last Price Update"] || "",
      validUntil:  pf["Price Valid Until"] || "",
      preferred:   !!pf["Preferred for This Item"],
      active:      !!pf["Active"],
      notes:       pf["Notes"] || ""
    };
  });

  // Display order: preferred first, then ascending by unit cost, then by name
  vendors.sort((a, b) => {
    if (a.preferred !== b.preferred) return a.preferred ? -1 : 1;
    if (a.unitCost  !== b.unitCost)  return a.unitCost - b.unitCost;
    return (a.vendorName || "").localeCompare(b.vendorName || "");
  });

  // Variance only meaningful when we have both a live price and a default cost
  const variance = (liveCost > 0 && defaultCost > 0)
    ? {
        dollar: Math.round((liveCost - defaultCost) * 10000) / 10000,
        pct:    (liveCost - defaultCost) / defaultCost
      }
    : null;

  const preferredVendor = vendors.find(v => v.preferred);

  return resp(200, {
    ok: true,
    summary: {
      defaultCost,
      liveCost,
      variance,
      preferredVendor: preferredVendor?.vendorName || "",
      preferredUpdated: preferredVendor?.lastUpdate || ""
    },
    vendors
  });
}

// ── SYNC DEFAULT UNIT COST TO LIVE VENDOR PRICE ───────────
// Copies the Unit Cost Rollup (Live) value onto Default Unit Cost for this item.
// Admin-convenience button — doesn't touch any Vendor Pricing records.
async function handleSyncItemCostToVendor(body) {
  const { itemId } = body || {};
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });

  const itemData = await atFetch(
    API_ROOT_INV,
    `${encodeURIComponent("Inventory Items")}/${itemId}`,
    { method: "GET" }
  );
  const f        = itemData?.fields || {};
  const liveCost = Number(f["Unit Cost Rollup (Live)"] || 0);

  if (!liveCost || liveCost <= 0) {
    return resp(400, { ok: false, error: "No live vendor price for this item — set a Preferred vendor with Unit Cost first." });
  }

  // Default Unit Cost field ID on Inventory Items — same one used by the receive
  // flow to write back updated costs. Using the ID (not name) for writes.
  await atFetch(API_ROOT_INV, `${encodeURIComponent("Inventory Items")}/${itemId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fld8aEhTzmEbqgIg4": liveCost } })
  });

  return resp(200, {
    ok: true,
    newDefaultCost: liveCost,
    oldDefaultCost: Number(f["Default Unit Cost"] || 0)
  });
}

// ── ROUTER ─────────────────────────────────────────────────
export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "employees")         return await handleEmployees();
      if (action === "jobs")              return await handleJobs();
      if (action === "estimatingJobs")    return await handleEstimatingJobs();
      if (action === "awardedJobs")       return await handleAwardedJobs();
      if (action === "locations")         return await handleLocations();
      if (action === "items")             return await handleItems();
      if (action === "history")           return await handleHistory(params);
      if (action === "pendingExpenses")   return await handlePendingExpenses();
      if (action === "pushHistory")       return await handlePushHistory(params);
      if (action === "pushHistoryDetail") return await handlePushHistoryDetail(params);
      if (action === "stockLevels")       return await handleStockLevels(params);
      if (action === "stockLevelsAll")    return await handleStockLevelsAll();
      if (action === "reorderAlerts")     return await handleReorderAlerts();
      if (action === "getExpenseFields")  return await handleGetExpenseFields();
      if (action === "estimatesList")     return await handleEstimatesList(params);
      if (action === "estimateGet")       return await handleEstimateGet(params);
      if (action === "ordersList")        return await handleOrdersList(params);
      if (action === "orderGet")          return await handleOrderGet(params);
      if (action === "ordersCount")       return await handleOrdersCount();
      if (action === "itemVendorPricing") return await handleItemVendorPricing(params);
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login")           return await handleLogin(body);
      if (body.action === "submitCart")      return await handleSubmitCart(body);
      if (body.action === "receive")         return await handleReceive(body);
      if (body.action === "transfer")        return await handleTransfer(body);
      if (body.action === "adjustment")      return await handleAdjustment(body);
      if (body.action === "pushExpenses")    return await handlePushExpenses(body);
      if (body.action === "createItem")        return await handleCreateItem(body);
      if (body.action === "updateItemCost")     return await handleUpdateItemCost(body);
      if (body.action === "updateReorderPoint") return await handleUpdateReorderPoint(body);
      if (body.action === "delete")             return await handleDelete(body);
      if (body.action === "estimateCreate")     return await handleEstimateCreate(body);
      if (body.action === "estimateUpdate")     return await handleEstimateUpdate(body);
      if (body.action === "estimateDelete")     return await handleEstimateDelete(body);
      if (body.action === "orderCreate")        return await handleOrderCreate(body);
      if (body.action === "orderUpdate")        return await handleOrderUpdate(body);
      if (body.action === "orderDelete")        return await handleOrderDelete(body);
      if (body.action === "syncItemCostToVendor") return await handleSyncItemCostToVendor(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Inventory Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

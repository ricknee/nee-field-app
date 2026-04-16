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
        id:          r.id,
        name:        f["Item Name"] || "",
        cat:         f["Category"]?.name || f["Category"] || "",
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
      itemId:     itemId,
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

  // Build per-job, per-item net quantities and collect all tx IDs
  // Structure: jobKey -> { jobData, items: { itemId -> { name, cost, netQty } }, txIds: [] }
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
    if (!jobGroups[jobKey].items[itemId]) {
      const itemData = itemMap[itemId] || {};
      jobGroups[jobKey].items[itemId] = {
        name:        itemData.name || itemId,
        cost:        itemData.cost || 0,
        wireFtPerLb: itemData.wireFtPerLb || 0,
        netQty:      0
      };
    }
    jobGroups[jobKey].items[itemId].netQty += delta;
  });

  // Build the pending array for the UI — one entry per job
  const pending = Object.values(jobGroups).map(g => {
    const lines = Object.values(g.items)
      .filter(i => i.netQty !== 0)
      .map(i => ({
        item:  i.name,
        qty:   i.netQty,
        cost:  i.cost,
        total: Math.round(i.cost * i.netQty * 100) / 100,
        wireFt: i.wireFtPerLb > 0 ? Math.round(Math.abs(i.netQty) * i.wireFtPerLb) : 0
      }));

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

// ── PUSH EXPENSES TO MAIN BASE ─────────────────────────────
async function handlePushExpenses(body) {
  const { pending, pdfs } = body || {};
  if (!pending || !pending.length) return resp(400, { ok: false, error: "Nothing to push." });

  const TAX_RATE      = 0.075;
  const today         = new Date().toISOString().split("T")[0];
  const NEE_VENDOR_ID = "recdVrxXdSOH0dlXO";
  const expenseIds    = [];
  const allTxIds      = [];
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
      const taxAmt = Math.round(jobTotal * TAX_RATE * 100) / 100;
      const taxFields = {
        "fldPNFIzq1grsdxYi": [String(jobId)],
        "fldlTUL8hsPkReBAB": [String(NEE_VENDOR_ID)],
        "fldwbLPIafVtmaSeb": taxAmt,
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
      if (taxResp.records?.[0]?.id) expenseIds.push(taxResp.records[0].id);
    }

    if (txIds?.length) allTxIds.push(...txIds);
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

  return resp(200, { ok: true, count: expenseIds.length, txCount: allTxIds.length, pdfUploads });
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
  const { name, category, uom, barcode, cost, wireFtPerLb } = body || {};
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
    "Item Name":          name.trim(),
    "Active Item":        true
  };
  if (category && category.trim())   fields["Category"]          = category.trim();
  if (uom && uom.trim())             fields["Unit of Measure"]   = uom.trim();
  if (barcode && barcode.trim())     fields["Barcode Value"]     = barcode.trim();
  if (cost && Number(cost) > 0)      fields["Default Unit Cost"] = Number(cost);
  if (wireFtPerLb && Number(wireFtPerLb) > 0) fields["Wire ft/lb"] = Number(wireFtPerLb);

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
      if (action === "locations")         return await handleLocations();
      if (action === "items")             return await handleItems();
      if (action === "history")           return await handleHistory(params);
      if (action === "pendingExpenses")   return await handlePendingExpenses();
      if (action === "stockLevels")       return await handleStockLevels(params);
      if (action === "reorderAlerts")     return await handleReorderAlerts();
      if (action === "getExpenseFields")  return await handleGetExpenseFields();
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
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Inventory Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

// netlify/functions/materials.js
// NEE Materials App — Netlify Proxy
// Uses env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID (main), INVENTORY_BASE_ID

const AIRTABLE_API_KEY  = process.env.AIRTABLE_API_KEY;
const MAIN_BASE_ID      = process.env.AIRTABLE_BASE_ID;
const INV_BASE_ID       = process.env.INVENTORY_BASE_ID;

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
  if (!AIRTABLE_API_KEY) throw new Error("Missing env var AIRTABLE_API_KEY");
  if (!MAIN_BASE_ID)     throw new Error("Missing env var AIRTABLE_BASE_ID");
  if (!INV_BASE_ID)      throw new Error("Missing env var INVENTORY_BASE_ID");
}

async function atFetch(baseId, path, options = {}) {
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${path}`, {
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

async function fetchAll(baseId, tableName, opts = {}) {
  const params = new URLSearchParams();
  if (opts.filter)      params.set("filterByFormula", opts.filter);
  if (opts.sortField)   params.set("sort[0][field]", opts.sortField);
  if (opts.sortDir)     params.set("sort[0][direction]", opts.sortDir);
  if (opts.maxRecords)  params.set("maxRecords", opts.maxRecords);

  const records = [];
  let offset = null;
  do {
    const qs = new URLSearchParams(params);
    if (offset) qs.set("offset", offset);
    const data = await atFetch(baseId, `${encodeURIComponent(tableName)}?${qs}`, { method: "GET" });
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);
  return records;
}

// ── LOGIN ─────────────────────────────────────────────────
async function handleLogin(body) {
  const { pin } = body || {};
  if (!pin) return resp(400, { ok: false, error: "Missing PIN." });

  const records = await fetchAll(MAIN_BASE_ID, "Employees", {
    filter: `AND({PIN}='${pin}', {Active}=1)`
  });

  if (!records.length) return resp(401, { ok: false, error: "Invalid PIN." });

  const f = records[0].fields || {};
  return resp(200, {
    ok: true,
    user: {
      id:   records[0].id,
      name: f["Employee Name"] || "Unknown",
      role: (f["Role New"] || f["Role"] || "").toLowerCase() === "admin" ? "admin" : "employee"
    }
  });
}

// ── JOBS (from main base, active only) ───────────────────
const ACTIVE_STATUSES = ["Awarded", "In Progress", "Service Call Scheduled", "Ready to Invoice"];

async function handleJobs() {
  const filter = `OR(${ACTIVE_STATUSES.map(s => `{Job Status}='${s}'`).join(",")})`;
  const records = await fetchAll(MAIN_BASE_ID, "Jobs", { filter, sortField: "Job PO", sortDir: "asc" });

  const jobs = records.map(r => ({
    id:     r.id,
    name:   r.fields["Job PO"] || r.fields["Job Name"] || "",
    status: r.fields["Job Status"] || ""
  }));

  return resp(200, { ok: true, jobs });
}

// ── LOCATIONS (from inventory base) ──────────────────────
async function handleLocations() {
  const records = await fetchAll(INV_BASE_ID, "Locations", {
    filter: "{Active Location}=1",
    sortField: "Location Name",
    sortDir: "asc"
  });

  const locations = records.map(r => ({
    id:   r.id,
    name: r.fields["Location Name"] || ""
  }));

  return resp(200, { ok: true, locations });
}

// ── ITEMS (from inventory base) ───────────────────────────
async function handleItems() {
  const records = await fetchAll(INV_BASE_ID, "Inventory Items", {
    filter: "{Active Item}=1",
    sortField: "Item Name",
    sortDir: "asc"
  });

  const items = records.map(r => {
    const f = r.fields || {};
    return {
      id:   r.id,
      name: f["Item Name"] || "",
      cat:  f["Category"]?.name || f["Category"] || "",
      uom:  f["Unit of Measure"]?.name || f["Unit of Measure"] || "",
      cost: f["Default Unit Cost"] || 0
    };
  });

  return resp(200, { ok: true, items });
}

// ── LOG TRANSACTION ───────────────────────────────────────
async function handleLogTransaction(body) {
  const { itemId, locationId, jobName, quantity, enteredBy, notes } = body || {};
  if (!itemId)    return resp(400, { ok: false, error: "Missing itemId." });
  if (!locationId) return resp(400, { ok: false, error: "Missing locationId." });
  if (!quantity)   return resp(400, { ok: false, error: "Missing quantity." });

  const now = new Date().toISOString();

  const fields = {
    "fldGq37LD9YuyCf5e": now,                                          // Transaction Date
    "fldmookC8mdyXxVuw": [String(itemId)],                             // Inventory Item
    "fldFQlArrzUnjCTxr": Number(quantity),                             // Quantity
    "fldjvIy3X1DJowGsd": "Use",                                        // Transaction Type
    "fldpyLadbcc9NHO6c": [String(locationId)],                         // From Location
    "fldIFffLxtcQTbExd": enteredBy || "",                              // Entered By
    "fldrcq8wSyfz8O3UB": [jobName, notes].filter(Boolean).join(" | ") // Notes: job | user notes
  };

  const data = await atFetch(INV_BASE_ID, encodeURIComponent("Inventory Transactions"), {
    method: "POST",
    body: JSON.stringify({ records: [{ fields }], typecast: true })
  });

  return resp(200, { ok: true, id: data.records?.[0]?.id });
}

// ── HISTORY ───────────────────────────────────────────────
async function handleHistory(params) {
  const { enteredBy } = params || {};
  if (!enteredBy) return resp(400, { ok: false, error: "Missing enteredBy." });

  // Build lookup maps for items and locations so we can resolve IDs to names
  const [txRecords, itemRecords, locRecords] = await Promise.all([
    fetchAll(INV_BASE_ID, "Inventory Transactions", {
      filter:     `{Entered By}='${enteredBy}'`,
      sortField:  "Transaction Date",
      sortDir:    "desc",
      maxRecords: 30
    }),
    fetchAll(INV_BASE_ID, "Inventory Items", {}),
    fetchAll(INV_BASE_ID, "Locations", {})
  ]);

  const itemMap = {};
  itemRecords.forEach(r => { itemMap[r.id] = r.fields["Item Name"] || r.id; });

  const locMap = {};
  locRecords.forEach(r => { locMap[r.id] = r.fields["Location Name"] || r.id; });

  const resolveName = (arr, map) => {
    if (!Array.isArray(arr) || !arr.length) return "";
    return arr.map(i => {
      if (typeof i === "object" && i !== null) return i.name || map[i.id] || i.id || "";
      return map[String(i)] || String(i);
    }).filter(Boolean).join(", ");
  };

  const transactions = txRecords.map(r => {
    const f = r.fields || {};

    const itemArr = f["Inventory Item"] || [];
    const locArr  = f["From Location"]  || [];

    let dateStr = "";
    const rawDate = f["Transaction Date"];
    if (rawDate) {
      try {
        const d = new Date(rawDate);
        dateStr = d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
      } catch(e) { dateStr = rawDate; }
    }

    const notesRaw = f["Notes"] || "";
    const notesParts = notesRaw.split(" | ");
    const jobName   = notesParts[0] || "";
    const userNotes = notesParts.slice(1).join(" | ");

    return {
      id:       r.id,
      date:     dateStr,
      item:     resolveName(itemArr, itemMap),
      location: resolveName(locArr, locMap),
      qty:      f["Quantity"] ?? "",
      type:     typeof f["Transaction Type"] === "object" ? (f["Transaction Type"]?.name || "") : (f["Transaction Type"] || ""),
      job:      jobName,
      notes:    userNotes
    };
  });

  return resp(200, { ok: true, transactions });
}

// ── DELETE TRANSACTION ────────────────────────────────────
async function handleDeleteTransaction(body) {
  const { txId } = body || {};
  if (!txId) return resp(400, { ok: false, error: "Missing txId." });
  await atFetch(INV_BASE_ID, `${encodeURIComponent("Inventory Transactions")}/${txId}`, { method: "DELETE" });
  return resp(200, { ok: true, deleted: txId });
}

// ── ROUTER ────────────────────────────────────────────────
export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "jobs")      return await handleJobs();
      if (action === "locations") return await handleLocations();
      if (action === "items")     return await handleItems();
      if (action === "history")   return await handleHistory(params);
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login")             return await handleLogin(body);
      if (body.action === "logTransaction")    return await handleLogTransaction(body);
      if (body.action === "deleteTransaction") return await handleDeleteTransaction(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Materials Proxy Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

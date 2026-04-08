// netlify/functions/airtable.js
// Northeastern Electric Field App — Netlify Proxy
// Reads env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID

const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;
const API_ROOT = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

const TABLES = {
  employees:        "Employees",
  jobs:             "Jobs",
  generators:       "Generators",
  generatorService: "Generator Service"
};

const F = {
  emp: {
    name:     "Employee Name",
    username: "Username",
    email:    "Email",
    pin:      "PIN",
    role:     "Role",
    active:   "Active"
  },
  job: {
    name:                    "Job Name",
    po:                      "Job PO",
    status:                  "Job Status",
    type:                    "Job Type",
    address:                 "Job Address - Full",
    contractor:              "Contractor Name (Text)",
    generatorInstalled:      "Generator Installed",
    powerCompanyName:        "Power Company – Name (lookup)",
    powerCompanyContact:     "Power Company – Primary Contact (lookup)",
    powerCompanyPhone:       "Power Company – Phone (lookup)",
    powerCompanyEmail:       "Power Company – Email (lookup)",
    aicNumber:               "AIC Number",
    tempWorkOrder:           "Temporary Work Order #",
    permWorkOrder:           "Permanent Work Order #",
    permitNumber:            "Permit Number",
    inspectionAgency:        "Inspection Agency Name (from Inspection Agency)",
    inspectionAgencyPhone:   "Inspection Agency Phone #",
    inspectionAgencyEmail:   "Inspection Agency Email Address",
    inspectionSchedulingLink:"Inspection Scheduling Link",
    inspectionContacts:      "Inspection Name (from Inspection Contacts)",
    jobInspections:          "Inspection Name (from Job Inspections)",
    wireLink:                "Wire (Mobile) or THHN (Mobile)",
    pipeLink:                "Add Pipe (Mobile)",
    addPhotosLink:           "Add Photos (Mobile)",
    viewPhotosLink:          "View pCloud Photos",
    trelloCardId:               "Trello Card ID",
    generatorCommissioningForm: "Generator Startup / Commissioning Form",
    newGeneratorServiceForm:    "New Generator Service"
  },
  gen: {
    assetId:              "Generator Asset ID",
    customer:             "Customer Name",
    customerPhone:        "Customer Phone #",
    job:                  "Job",
    siteAddress:          "Site Address",
    brand:                "Generator Brand",
    model:                "Generator Model",
    kw:                   "Generator KW",
    serialNumber:         "Generator Serial Number",
    transferSwitchModel:  "Transfer Switch Model",
    transferSwitchSerial: "Transfer Switch Serial Number",
    fuelType:             "Fuel Type",
    installDate:          "Install / In-Service Date",
    servicePlanActive:    "Service Plan Active",
    serviceIntervalMonths:"Service Interval Months",
    nextServiceDue:       "Next Service Due",
    warrantyExpiration:   "Warranty Expiration",
    status:               "Status",
    batteryInstallDate:   "Battery Install Date",
    batteryAge:           "Battery Age",
    serviceStatus:        "Service Status",
    notes:                "Notes"
  },
  svc: {
    serviceRecordId:  "Service Record ID",
    generator:        "Generator",
    customer:         "Customer",
    job:              "Job",
    serviceDate:      "Service Date",
    serviceType:      "Service Type",
    technician:       "Technician",
    servicePlanVisit: "Service Plan Visit",
    oilChanged:       "Oil Changed",
    oilFilterChanged: "Oil Filter Changed",
    airFilterChanged: "Air Filter Changed",
    sparkPlugsChanged:"Spark Plugs Changed",
    batteryTested:    "Battery Tested",
    batteryReplaced:  "Battery Replaced",
    loadTestPerformed:"Load Test Performed",
    firmwareChecked:  "Firmware / Settings Checked",
    exerciseChecked:  "Exercise Checked",
    troubleCodesFound:"Trouble Codes Found",
    workNotes:        "Work Performed Notes",
    partsUsed:        "Parts Used",
    laborHours:       "Labor Hours",
    generatorHours:   "Generator Hours @ Service"
  }
};

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
  if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID)
    throw new Error("Missing env vars AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
}

function g(fields, fieldName) {
  const v = fields[fieldName];
  if (Array.isArray(v)) return v.join(", ");
  return v ?? null;
}

function gBool(fields, fieldName) {
  const v = fields[fieldName];
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string") return ["true","yes","1"].includes(v.trim().toLowerCase());
  return false;
}

function extractUrl(formulaValue) {
  if (!formulaValue) return null;
  const s = String(formulaValue);
  const match = s.match(/https?:\/\/\S+/);
  return match ? match[0] : null;
}

function normalize(v) { return String(v || "").trim().toLowerCase(); }

async function atFetch(path, options = {}) {
  ensureEnv();
  const res = await fetch(`${API_ROOT}/${path}`, {
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

async function fetchAll(tableName, opts = {}) {
  const params = new URLSearchParams();
  if (opts.view)      params.set("view", opts.view);
  if (opts.filter)    params.set("filterByFormula", opts.filter);
  if (opts.sortField) params.set("sort[0][field]", opts.sortField);
  if (opts.sortDir)   params.set("sort[0][direction]", opts.sortDir);

  const records = [];
  let offset = null;
  do {
    const qs = new URLSearchParams(params);
    if (offset) qs.set("offset", offset);
    const data = await atFetch(`${encodeURIComponent(tableName)}?${qs}`, { method: "GET" });
    records.push(...(data.records || []));
    offset = data.offset || null;
  } while (offset);

  return records;
}

async function handleLogin(body) {
  const { identifier, pin } = body || {};
  if (!identifier || !pin) return resp(400, { ok: false, error: "Missing identifier or PIN." });

  const records = await fetchAll(TABLES.employees);
  const match = records.find(r => {
    const f = r.fields || {};
    const name     = normalize(f[F.emp.name]);
    const username = normalize(f[F.emp.username]);
    const email    = normalize(f[F.emp.email]);
    const savedPin = String(f[F.emp.pin] || "").trim();
    const active   = f[F.emp.active] == null ? true : gBool(f, F.emp.active);
    const id       = normalize(identifier);
    return [name, username, email].includes(id) && savedPin !== "" && savedPin === String(pin).trim() && active;
  });

  if (!match) return resp(401, { ok: false, error: "Invalid login. Check your name and PIN." });

  const f = match.fields || {};
  return resp(200, {
    ok: true,
    user: {
      id:   match.id,
      name: f[F.emp.name] || "Unknown",
      role: normalize(f[F.emp.role]) === "admin" ? "admin" : "employee"
    }
  });
}

async function handleJobs() {
  const records = await fetchAll(TABLES.jobs);
  const jobs = records.map(r => {
    const f = r.fields || {};
    return {
      id:           r.id,
      name:         g(f, F.job.name) || "",
      po:           g(f, F.job.po) || "",
      status:       g(f, F.job.status) || "",
      type:         g(f, F.job.type) || "",
      address:      g(f, F.job.address) || "",
      contractor:   g(f, F.job.contractor) || "",
      generatorInstalled: gBool(f, F.job.generatorInstalled),
      powerCompanyName:    g(f, F.job.powerCompanyName) || "",
      powerCompanyContact: g(f, F.job.powerCompanyContact) || "",
      powerCompanyPhone:   g(f, F.job.powerCompanyPhone) || "",
      powerCompanyEmail:   g(f, F.job.powerCompanyEmail) || "",
      aicNumber:           g(f, F.job.aicNumber) || "",
      tempWorkOrder:       g(f, F.job.tempWorkOrder) || "",
      permWorkOrder:       g(f, F.job.permWorkOrder) || "",
      permitNumber:             g(f, F.job.permitNumber) || "",
      inspectionAgency:         g(f, F.job.inspectionAgency) || "",
      inspectionAgencyPhone:    g(f, F.job.inspectionAgencyPhone) || "",
      inspectionAgencyEmail:    g(f, F.job.inspectionAgencyEmail) || "",
      inspectionSchedulingLink: g(f, F.job.inspectionSchedulingLink) || "",
      inspectionContacts:       g(f, F.job.inspectionContacts) || "",
      jobInspections:           g(f, F.job.jobInspections) || "",
      wireLink:       extractUrl(g(f, F.job.wireLink)),
      pipeLink:       extractUrl(g(f, F.job.pipeLink)),
      addPhotosLink:  extractUrl(g(f, F.job.addPhotosLink)),
      viewPhotosLink: extractUrl(g(f, F.job.viewPhotosLink)),
      trelloCardId:               g(f, F.job.trelloCardId) || "",
      generatorCommissioningForm: g(f, F.job.generatorCommissioningForm) || "",
      newGeneratorServiceForm:    g(f, F.job.newGeneratorServiceForm) || ""
    };
  }).filter(j => !["archived","cancelled","canceled","closed"].includes(normalize(j.status)));

  return resp(200, { ok: true, jobs });
}

async function handleGenerator(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // Look up the job name first so we can search generators by name
  const jobRecords = await fetchAll(TABLES.jobs, {
    filter: `RECORD_ID()="${jobId}"`
  });
  if (!jobRecords.length) return resp(200, { ok: true, generator: null, serviceRecords: [] });
  const jobName = jobRecords[0].fields[F.job.name] || "";

  const filter = `FIND("${jobName}", ARRAYJOIN({${F.gen.job}}))`;
  const genRecords = await fetchAll(TABLES.generators, { filter });

  if (!genRecords.length) return resp(200, { ok: true, generator: null, serviceRecords: [] });

  const r = genRecords[0];
  const f = r.fields || {};
  const generator = {
    id:                   r.id,
    assetId:              g(f, F.gen.assetId) || "",
    customer:             g(f, F.gen.customer) || "",
    customerPhone:        g(f, F.gen.customerPhone) || "",
    siteAddress:          g(f, F.gen.siteAddress) || "",
    brand:                g(f, F.gen.brand) || "",
    model:                g(f, F.gen.model) || "",
    kw:                   g(f, F.gen.kw) || "",
    serialNumber:         g(f, F.gen.serialNumber) || "",
    transferSwitchModel:  g(f, F.gen.transferSwitchModel) || "",
    transferSwitchSerial: g(f, F.gen.transferSwitchSerial) || "",
    fuelType:             g(f, F.gen.fuelType) || "",
    installDate:          g(f, F.gen.installDate) || "",
    servicePlanActive:    gBool(f, F.gen.servicePlanActive),
    serviceIntervalMonths:g(f, F.gen.serviceIntervalMonths) || "",
    nextServiceDue:       g(f, F.gen.nextServiceDue) || "",
    warrantyExpiration:   g(f, F.gen.warrantyExpiration) || "",
    status:               g(f, F.gen.status) || "",
    batteryInstallDate:   g(f, F.gen.batteryInstallDate) || "",
    batteryAge:           g(f, F.gen.batteryAge) || "",
    serviceStatus:        g(f, F.gen.serviceStatus) || "",
    notes:                g(f, F.gen.notes) || ""
  };

  // Search service records by generator display name (linked field returns name not ID)
  const genAssetId = generator.assetId || "";
  const svcFilter = genAssetId ? `FIND("${genAssetId}", ARRAYJOIN({${F.svc.generator}}))` : `FALSE()`;
  const svcRecords = await fetchAll(TABLES.generatorService, { filter: svcFilter, sortField: F.svc.serviceDate, sortDir: "desc" });

  const serviceRecords = svcRecords.map(sr => {
    const sf = sr.fields || {};
    return {
      id:               sr.id,
      serviceDate:      g(sf, F.svc.serviceDate) || "",
      serviceType:      g(sf, F.svc.serviceType) || "",
      technician:       g(sf, F.svc.technician) || "",
      servicePlanVisit: gBool(sf, F.svc.servicePlanVisit),
      oilChanged:       gBool(sf, F.svc.oilChanged),
      oilFilterChanged: gBool(sf, F.svc.oilFilterChanged),
      airFilterChanged: gBool(sf, F.svc.airFilterChanged),
      sparkPlugsChanged:gBool(sf, F.svc.sparkPlugsChanged),
      batteryTested:    gBool(sf, F.svc.batteryTested),
      batteryReplaced:  gBool(sf, F.svc.batteryReplaced),
      loadTestPerformed:gBool(sf, F.svc.loadTestPerformed),
      firmwareChecked:  gBool(sf, F.svc.firmwareChecked),
      exerciseChecked:  gBool(sf, F.svc.exerciseChecked),
      troubleCodesFound:g(sf, F.svc.troubleCodesFound) || "",
      workNotes:        g(sf, F.svc.workNotes) || "",
      partsUsed:        g(sf, F.svc.partsUsed) || "",
      laborHours:       g(sf, F.svc.laborHours) || "",
      generatorHours:   g(sf, F.svc.generatorHours) || ""
    };
  });

  return resp(200, { ok: true, generator, serviceRecords });
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "jobs")      return await handleJobs();
      if (action === "generator") return await handleGenerator(params);
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login") return await handleLogin(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Proxy Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

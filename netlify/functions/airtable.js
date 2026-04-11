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
  generatorService: "Generator Service",
  timeEntries:      "Time Entries",
  scissorLifts:     "Scissor Lifts"
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
    meterNumber:             "Meter Number",
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
    newGeneratorServiceForm:    "New Generator Service",
    taxStatus:                  "Tax Status",
    powerCompanyIntake:         "Power Company (Intake)",

    // ── ADMIN: Live Performance ──
    totalRevenueLive:          "Total Revenue (Live)",
    totalMaterialsLive:        "Total Materials (Live)",
    totalLaborCostLive:        "Total Labor Cost (Live)",
    totalWireCost:             "Total Wire Cost",
    pipeCost:                  "Pipe Cost",
    materialsInProgress:       "Materials In Progress",
    grossProfitLiveDollar:     "Gross Profit (Live) $",
    grossProfitLivePct:        "Gross Profit (Live) %",
    workflowStatus:            "Worklfow Status",
    estimatedLaborHoursRollup: "Estimated Labor Hours Rollup (from Job Estimates)",
    hoursRollup:               "Hours Rollup (from Time Entries)",

    // ── ADMIN: Closeout (Final) ──
    expectedRevenue:           "Expected Revenue",
    actualJobCostCogs:         "Actual Job Cost (COGS)",
    totalReviewedCosts:        "Total Reviewed Costs",
    totalLaborCostFinal:       "Total Labor Cost (Final)",
    grossProfitFinalDollar:    "Gross Profit (Final) $",
    grossProfitFinalPct:       "Gross Profit (Final) %",
    allMaterialsReviewed:      "All Materials Reviewed?",
    allWireReviewed:           "All Wire Reviewed?",
    allPipeReviewed:           "All Pipe Reviewed?",
    allExpensesReviewed:       "All Expenses Reviewed?",
    allLaborReviewed:          "All Labor Reviewed",

    // ── ADMIN: Estimated Gross Profit ──
    expectedRevenueAllStatus:          "Expected Revenue (All Status)",
    projectedEstimatedTotalCost:       "Projected Estimated Total Cost",
    projectedEstimatedLaborHours:      "Projected Estimated Labor Hours (from Job Estimates)",
    projectedEstimatedMaterialCost:    "Projected Estimated Material Cost (from Job Estimates)",
    projectedEstimatedLaborCost:       "Projected Estimated Labor Cost (from Job Estimates)",
    projectedGrossProfitDollar:        "Projected Gross Profit $",
    projectedGrossProfitPct:           "Projected Gross Profit %"
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

function gNum(fields, fieldName) {
  const v = fields[fieldName];
  if (v === null || v === undefined || v === "") return null;
  const n = parseFloat(v);
  return isNaN(n) ? null : n;
}

function gBool(fields, fieldName) {
  const v = fields[fieldName];
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string") return ["true","yes","1"].includes(v.trim().toLowerCase());
  return false;
}

function gFormulaBool(fields, fieldName) {
  // Formula fields that return 1/0 or "yes"/"no" or true/false
  const v = fields[fieldName];
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    return s === "1" || s === "true" || s === "yes";
  }
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
    const active   = gBool(f, F.emp.active);
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

      // Power Co
      powerCompanyName:    g(f, F.job.powerCompanyName) || "",
      powerCompanyContact: g(f, F.job.powerCompanyContact) || "",
      powerCompanyPhone:   g(f, F.job.powerCompanyPhone) || "",
      powerCompanyEmail:   g(f, F.job.powerCompanyEmail) || "",
      aicNumber:           g(f, F.job.aicNumber) || "",
      tempWorkOrder:       g(f, F.job.tempWorkOrder) || "",
      permWorkOrder:       g(f, F.job.permWorkOrder) || "",
      meterNumber:         g(f, F.job.meterNumber) || "",

      // Inspections
      permitNumber:             g(f, F.job.permitNumber) || "",
      inspectionAgency:         g(f, F.job.inspectionAgency) || "",
      inspectionAgencyPhone:    g(f, F.job.inspectionAgencyPhone) || "",
      inspectionAgencyEmail:    g(f, F.job.inspectionAgencyEmail) || "",
      inspectionSchedulingLink: g(f, F.job.inspectionSchedulingLink) || "",
      inspectionContacts:       g(f, F.job.inspectionContacts) || "",
      jobInspections:           g(f, F.job.jobInspections) || "",

      // Action links
      wireLink:       extractUrl(g(f, F.job.wireLink)),
      pipeLink:       extractUrl(g(f, F.job.pipeLink)),
      addPhotosLink:  extractUrl(g(f, F.job.addPhotosLink)),
      viewPhotosLink: extractUrl(g(f, F.job.viewPhotosLink)),
      trelloCardId:               g(f, F.job.trelloCardId) || "",
      generatorCommissioningForm: g(f, F.job.generatorCommissioningForm) || "",
      newGeneratorServiceForm:    g(f, F.job.newGeneratorServiceForm) || "",
      taxStatus:                  g(f, F.job.taxStatus) || "",
      powerCompanyIntake:         g(f, F.job.powerCompanyIntake) || "",

      // ── ADMIN: Live Performance ──
      totalRevenueLive:          gNum(f, F.job.totalRevenueLive),
      totalMaterialsLive:        gNum(f, F.job.totalMaterialsLive),
      totalLaborCostLive:        gNum(f, F.job.totalLaborCostLive),
      totalWireCost:             gNum(f, F.job.totalWireCost),
      pipeCost:                  gNum(f, F.job.pipeCost),
      materialsInProgress:       gNum(f, F.job.materialsInProgress),
      grossProfitLiveDollar:     gNum(f, F.job.grossProfitLiveDollar),
      grossProfitLivePct:        gNum(f, F.job.grossProfitLivePct),
      workflowStatus:            g(f, F.job.workflowStatus),
      estimatedLaborHoursRollup: gNum(f, F.job.estimatedLaborHoursRollup),
      hoursRollup:               gNum(f, F.job.hoursRollup),

      // ── ADMIN: Closeout (Final) ──
      expectedRevenue:        gNum(f, F.job.expectedRevenue),
      actualJobCostCogs:      gNum(f, F.job.actualJobCostCogs),
      totalReviewedCosts:     gNum(f, F.job.totalReviewedCosts),
      totalLaborCostFinal:    gNum(f, F.job.totalLaborCostFinal),
      grossProfitFinalDollar: gNum(f, F.job.grossProfitFinalDollar),
      grossProfitFinalPct:    gNum(f, F.job.grossProfitFinalPct),
      allMaterialsReviewed:   gFormulaBool(f, F.job.allMaterialsReviewed),
      allWireReviewed:        gFormulaBool(f, F.job.allWireReviewed),
      allPipeReviewed:        gFormulaBool(f, F.job.allPipeReviewed),
      allExpensesReviewed:    gFormulaBool(f, F.job.allExpensesReviewed),
      allLaborReviewed:       gFormulaBool(f, F.job.allLaborReviewed),

      // ── ADMIN: Estimated Gross Profit ──
      expectedRevenueAllStatus:       gNum(f, F.job.expectedRevenueAllStatus),
      projectedEstimatedTotalCost:    gNum(f, F.job.projectedEstimatedTotalCost),
      projectedEstimatedLaborHours:   gNum(f, F.job.projectedEstimatedLaborHours),
      projectedEstimatedMaterialCost: gNum(f, F.job.projectedEstimatedMaterialCost),
      projectedEstimatedLaborCost:    gNum(f, F.job.projectedEstimatedLaborCost),
      projectedGrossProfitDollar:     gNum(f, F.job.projectedGrossProfitDollar),
      projectedGrossProfitPct:        gNum(f, F.job.projectedGrossProfitPct)
    };
  }).filter(j => !["archived","cancelled","canceled","closed"].includes(normalize(j.status)));

  return resp(200, { ok: true, jobs });
}

async function handleGenerator(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

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

async function handleUpdateJobStatus(body) {
  const { jobId, status } = body || {};
  if (!jobId || !status) return resp(400, { ok: false, error: "Missing jobId or status." });

  const VALID = ["New Lead","Estimating","Awarded","Service Call Scheduled","Ready to Invoice","Completed","Not Awarded"];
  if (!VALID.includes(status)) return resp(400, { ok: false, error: "Invalid status value." });

  // Job Status field ID: fld2FBMjvkOsy9Puu (singleSelect — must use field ID for writes)
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fld2FBMjvkOsy9Puu": status } })
  });

  return resp(200, { ok: true, updatedId: data.id });
}

async function handleUpdatePowerCo(body) {
  const { jobId, powerCompany, powerContact, aicNumber, tempWorkOrder, permWorkOrder, meterNumber } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // Contact name → record ID map
  const CONTACT_IDS = {
    "Dave Baker":    "rec0Pmo9JBNVSdJ23",
    "Dan Shaeffer":  "recejs6S1LHK4rbNN",
    "Earnest Kash":  "reczPDI9EgwALy07k",
    "Tom Shultz":    "rec1Z0Epjw6C8BK90",
    "Gavyn Lopez":   "rec1kjUkMawcHocok",
    "Diane Wintrow": "recf5WUTUQdvUW2QR",
    "Dan Johnson":   "reclI0d8M1mP4BpBM"
  };

  const fields = {};
  // Power Company (Intake) — singleSelect
  if (powerCompany && powerCompany.trim() !== "") {
    fields["fldURTQ0ygHMMIbTU"] = powerCompany.trim();
  }
  // Power Company Contact — linked record, write using record ID
  if (powerContact && CONTACT_IDS[powerContact]) {
    fields["fldhKlMCFsnmHo5PH"] = [{ id: CONTACT_IDS[powerContact] }];
  }
  // Plain text fields
  if (aicNumber     !== undefined) fields["fld1vqpCklUdzgrjO"] = aicNumber;
  if (tempWorkOrder !== undefined) fields["fldmJKSiIQfJm9zhI"] = tempWorkOrder;
  if (permWorkOrder !== undefined) fields["fld6t3TBBz6SwJPh8"] = permWorkOrder;
  if (meterNumber   !== undefined) fields["fldWXpfslcqLlwdTQ"] = meterNumber;

  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true })
  });

  return resp(200, { ok: true, updatedId: data.id });
}

async function handleDeleteExpense(body) {
  const { expenseId } = body || {};
  if (!expenseId) return resp(400, { ok: false, error: "Missing expenseId." });
  await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`, { method: "DELETE" });
  return resp(200, { ok: true, deleted: expenseId });
}

async function handleApproveExpense(body) {
  const { expenseId } = body || {};
  if (!expenseId) return resp(400, { ok: false, error: "Missing expenseId." });
  // Reviewed checkbox field ID: fldwSsga6eashzJsw
  const data = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: { "fldwSsga6eashzJsw": true } })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleScissorLiftsByJob(params) {
  const { jobName } = params || {};
  if (!jobName) return resp(200, { ok: true, lifts: [] });

  const records = await fetchAll(TABLES.scissorLifts, { sortField: "Lift Name", sortDir: "asc" });
  const lifts = records
    .map(r => {
      const f = r.fields || {};
      const photos = (f["Photo"] || []).map(a => a.url);
      return {
        id:          r.id,
        name:        f["Lift Name"] || "",
        status:      f["Status"] || "Available",
        currentJob:  f["Current Job"] || "",
        assignedTo:  f["Assigned To"] || "",
        dateDeployed: f["Date Deployed"] || "",
        notes:       f["Notes"] || "",
        photoUrl:    photos[0] || ""
      };
    })
    .filter(l => l.currentJob === jobName && l.status === "On Job");

  return resp(200, { ok: true, lifts });
}

async function handleJobInspections(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // First get the job name from the job ID
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, inspections: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";

  // Filter inspections by job name via the linked record's display name
  const records = await fetchAll("Job Inspections", {
    filter: `FIND("${jobName}", ARRAYJOIN({Job}))`,
    sortField: "Inspection Date",
    sortDir: "desc"
  });

  const inspections = records.map(r => {
    const f = r.fields || {};
    // Permit number is a lookup
    const permitRaw = f["Permit Number"];
    const permit = Array.isArray(permitRaw) ? permitRaw[0] : (permitRaw || "");
    // Agency phone is a lookup
    const phoneRaw = f["Inspections Agency Phone #"];
    const agencyPhone = Array.isArray(phoneRaw) ? phoneRaw[0] : (phoneRaw || "");

    return {
      id:             r.id,
      inspectionType: f["Inspection Type"]?.name || f["Inspection Type"] || "",
      date:           f["Inspection Date"] || "",
      status:         f["Inspection Status"]?.name || f["Inspection Status"] || "",
      notes:          f["Notes"] || "",
      permitNumber:   permit,
      agencyPhone
    };
  });

  return resp(200, { ok: true, inspections });
}

async function handleCreateInspection(body) {
  const { jobId, inspectionType, date, status, notes } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const fields = {};
  // Job is a linked record — use record ID
  fields["fldqk2pA5w3TSN3q8"] = [{ id: String(jobId) }];
  if (inspectionType) fields["fldR2IQkaeRHXytsR"] = inspectionType;
  if (date)           fields["fldPblyNOIryMLFB6"] = date;
  if (status)         fields["fld7kH2SEHsxaS9vz"] = status;
  if (notes)          fields["fldmz5dOw6In5OkU7"] = notes;

  const data = await atFetch(`${encodeURIComponent("Job Inspections")}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });

  return resp(200, { ok: true, id: data.id });
}

async function handleJobEstimates(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // Get job name first to filter estimates
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, estimates: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";

  const records = await fetchAll("Job Estimates", {
    filter: `FIND("${jobName}", ARRAYJOIN({Job}))`,
    sortField: "Estimate Date",
    sortDir: "desc"
  });

  const estimates = records.map(r => {
    const f = r.fields || {};
    const pdfs = (f["Estimate PDF"] || []).map(att => ({
      url:      att.url,
      filename: att.filename,
      size:     att.size
    }));
    return {
      id:               r.id,
      name:             f["Estimate Name"] || "",
      estimateType:     f["Estimate Type"]?.name || f["Estimate Type"] || "",
      status:           f["Status"]?.name || f["Status"] || "",
      date:             f["Estimate Date"] || "",
      actualEstimate:   f["Actual Estimate Sent"] ?? null,
      laborHours:       f["Estimated Labor Hours"] ?? null,
      materialCost:     f["Estimated Material Cost"] ?? null,
      calculatedTotal:  f["Calculated Estimated Total"] ?? null,
      notes:            f["Notes"] || "",
      pdfs
    };
  });

  return resp(200, { ok: true, estimates });
}

async function handleUpdateEstimate(body) {
  const { estimateId, actualEstimate, laborHours, materialCost } = body || {};
  if (!estimateId) return resp(400, { ok: false, error: "Missing estimateId." });

  const fields = {};
  if (actualEstimate !== undefined && actualEstimate !== null) fields["fldJTAPtFpXH2vRwF"] = Number(actualEstimate);
  if (laborHours     !== undefined && laborHours     !== null) fields["fldH7bJSZikzOYxkm"] = Number(laborHours);
  if (materialCost   !== undefined && materialCost   !== null) fields["fldDEUGzVrfA56aBq"] = Number(materialCost);

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await atFetch(`${encodeURIComponent("Job Estimates")}/${estimateId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

// ── FLEET ──
const FLEET_TABLES = {
  vehicles:    "Fleet Vehicles",
  maintenance: "Fleet Maintenance"
};
const FV = { // Fleet Vehicle field IDs
  name:           "fldBcqDl6ez0GZz9n",
  year:           "fld7E7ubdLAnlbplu",
  make:           "fldiPxOpsxiO3JbqQ",
  model:          "fldXFQ1u0BKpd94Fa",
  color:          "fldR9UNl5MD8QRelB",
  vin:            "fldMCiACFqTxA87Ay",
  plate:          "fldX23ZlkmGHTx52S",
  type:           "flduEmTHcrv24SlJT",
  mileage:        "fldcRmbsqWDMyfzuF",
  mileageDate:    "fldwIxFsMRrZsAAEy",
  oilType:        "fldgT7qDyTXa1SeUC",
  oilCapacity:    "fldgjlvQVc4kOEkqY",
  tireBrand:      "fldkBVEAr6qCkTAZS",
  tireSize:       "fldCC7EoiTXi7BxMR",
  tireInstall:    "fldn7QbuEneDRgJ76",
  notes:          "fldx4pEJ5JS0DFLyh",
  active:         "fldapfWYijFLo7n1P",
  vehicle:        "fldK1hwGh51FgcgN8"  // reverse link in Fleet Vehicles
};
const FM = { // Fleet Maintenance field IDs
  vehicle:        "fld12gpaArqYw7BWU",
  date:           "fldwEhvgTGGEy9E3g",
  mileage:        "fldE7SlKw7n85bZWD",
  serviceTypes:   "fldCiHkwHtsOZmkWk",
  oilBrand:       "fldO7RALeUnXSgC6J",
  oilType:        "fldcgXpATus1HqW81",
  oilQty:         "fldwaUKNsJQvjwlK1",
  tireBrand:      "fldSw3UKcWky8bQlA",
  tireSize:       "fldVEwTlmNaWmRUiJ",
  cost:           "fldwYmFTQLvOuDKIE",
  performedBy:    "fld4mHAqeBjCqSjkB",
  shop:           "fldZddoeHsPrxapz1",
  notes:          "fldwNDO1V7E26vql1"
};

async function handleFleetVehicles() {
  const records = await fetchAll(FLEET_TABLES.vehicles, { sortField: "Vehicle Name", sortDir: "asc" });
  const vehicles = records
    .filter(r => r.fields["Active"] === true)
    .map(r => {
      const f = r.fields || {};
      return {
        id:             r.id,
        name:           f["Vehicle Name"]       || "",
        year:           f["Year"]               || null,
        make:           f["Make"]               || "",
        model:          f["Model"]              || "",
        color:          f["Color"]              || "",
        vin:            f["VIN"]                || "",
        plate:          f["License Plate"]      || "",
        type:           f["Vehicle Type"]?.name || f["Vehicle Type"] || "",
        currentMileage: f["Current Mileage"]    ?? null,
        mileageDate:    f["Mileage Date"]       || "",
        oilType:        f["Oil Type"]           || "",
        oilCapacity:    f["Oil Capacity (qts)"] ?? null,
        tireBrand:      f["Tire Brand"]         || "",
        tireSize:       f["Tire Size"]          || "",
        tireInstallDate: f["Tire Install Date"] || "",
        notes:          f["Notes"]              || "",
        photoUrl:       (f["Photo"] || [])[0]?.url || ""
      };
    });
  return resp(200, { ok: true, vehicles });
}

async function handleFleetServiceHistory(params) {
  const { vehicleId } = params || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });

  // Get vehicle name first to filter by name (more reliable than ID in ARRAYJOIN)
  const vehRecords = await fetchAll(FLEET_TABLES.vehicles, { filter: `RECORD_ID()="${vehicleId}"` });
  if (!vehRecords.length) return resp(200, { ok: true, records: [] });
  const vehName = vehRecords[0].fields[FV.name] || "";

  const records = await fetchAll(FLEET_TABLES.maintenance, {
    filter: `FIND("${vehName}", ARRAYJOIN({Vehicle}))`,
    sortField: "Date",
    sortDir:   "desc"
  });

  const serviceRecords = records.map(r => {
    const f = r.fields || {};
    return {
      id:           r.id,
      date:         f[FM.date]         || "",
      mileage:      f[FM.mileage]      ?? null,
      serviceTypes: (f[FM.serviceTypes] || []).map(s => s.name || s),
      oilBrand:     f[FM.oilBrand]     || "",
      oilType:      f[FM.oilType]      || "",
      oilQty:       f[FM.oilQty]       ?? null,
      tireBrand:    f[FM.tireBrand]    || "",
      tireSize:     f[FM.tireSize]     || "",
      cost:         f[FM.cost]         ?? null,
      performedBy:  f[FM.performedBy]  || "",
      shop:         f[FM.shop]         || "",
      notes:        f[FM.notes]        || ""
    };
  });
  return resp(200, { ok: true, records: serviceRecords });
}

async function handleUpdateFleetVehicle(body) {
  const { vehicleId, currentMileage, oilType, oilCapacity, tireBrand, tireSize, tireInstallDate, vin, plate, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  const fields = {};
  if (currentMileage !== undefined) { fields[FV.mileage] = Number(currentMileage); fields[FV.mileageDate] = new Date().toISOString().slice(0,10); }
  if (oilType        !== undefined) fields[FV.oilType]    = oilType;
  if (oilCapacity    !== undefined) fields[FV.oilCapacity] = Number(oilCapacity);
  if (tireBrand      !== undefined) fields[FV.tireBrand]  = tireBrand;
  if (tireSize       !== undefined) fields[FV.tireSize]   = tireSize;
  if (tireInstallDate!== undefined) fields[FV.tireInstall]= tireInstallDate;
  if (vin            !== undefined) fields[FV.vin]        = vin;
  if (plate          !== undefined) fields[FV.plate]      = plate;
  if (notes          !== undefined) fields[FV.notes]      = notes;
  const data = await atFetch(`${encodeURIComponent(FLEET_TABLES.vehicles)}/${vehicleId}`, {
    method: "PATCH", body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleAddFleetService(body) {
  const { vehicleId, date, mileage, serviceTypes, oilBrand, oilType, oilQty, cost, tireBrand, tireSize, performedBy, shop, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  const fields = {};
  fields[FM.vehicle] = [{ id: String(vehicleId) }];
  if (date)         fields[FM.date]        = date;
  if (mileage)      fields[FM.mileage]     = Number(mileage);
  if (serviceTypes && serviceTypes.length) fields[FM.serviceTypes] = serviceTypes;
  if (oilBrand)     fields[FM.oilBrand]    = oilBrand;
  if (oilType)      fields[FM.oilType]     = oilType;
  if (oilQty)       fields[FM.oilQty]      = Number(oilQty);
  if (cost)         fields[FM.cost]        = Number(cost);
  if (tireBrand)    fields[FM.tireBrand]   = tireBrand;
  if (tireSize)     fields[FM.tireSize]    = tireSize;
  if (performedBy)  fields[FM.performedBy] = performedBy;
  if (shop)         fields[FM.shop]        = shop;
  if (notes)        fields[FM.notes]       = notes;
  const data = await atFetch(`${encodeURIComponent(FLEET_TABLES.maintenance)}`, {
    method: "POST", body: JSON.stringify({ fields, typecast: true })
  });
  return resp(200, { ok: true, id: data.id });
}

async function handleScissorLifts() {
  const records = await fetchAll(TABLES.scissorLifts, { sortField: "Lift Name", sortDir: "asc" });
  const lifts = records.map(r => {
    const f = r.fields || {};
    const photos = (f["Photo"] || []).map(a => a.url);
    return {
      id:           r.id,
      name:         f["Lift Name"] || "",
      status:       f["Status"] || "Available",
      currentJob:   f["Current Job"] || "",
      assignedTo:   f["Assigned To"] || "",
      dateDeployed: f["Date Deployed"] || "",
      notes:        f["Notes"] || "",
      photoUrl:     photos[0] || ""
    };
  });
  return resp(200, { ok: true, lifts });
}

async function handleUpdateScissorLift(body) {
  const { liftId, status, currentJob, assignedTo, dateDeployed, notes } = body || {};
  if (!liftId) return resp(400, { ok: false, error: "Missing liftId." });

  const fields = {};
  // All plain text/singleSelect — no linked records!
  if (status)       fields["fldB9Kwqm0NS3RFFP"] = status;
  if (currentJob !== undefined) fields["fldZpCcD52inR2PGm"] = currentJob;
  if (assignedTo !== undefined) fields["fldkjsgzYiedjTaJ5"] = assignedTo || null;
  if (dateDeployed) fields["fldqRXHkwiFQdjqor"] = dateDeployed;
  if (notes !== undefined) fields["fldG5MLCzQbyClax0"] = notes;

  const data = await atFetch(`${encodeURIComponent(TABLES.scissorLifts)}/${liftId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleDeleteTimeEntry(body) {
  const { entryId } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  await atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${entryId}`, {
    method: "DELETE"
  });

  return resp(200, { ok: true, deleted: entryId });
}

async function handleUpdateTimeEntry(body) {
  const { entryId, reviewed, duration } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const fields = {};
  if (reviewed !== undefined) fields["fldQn7d06doEkrGBv"] = reviewed === true;
  if (duration !== undefined && duration !== null) fields["fld9mz6As3099VPVp"] = Number(duration);

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${entryId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });

  return resp(200, { ok: true, updatedId: data.id });
}

async function handleTimeEntries(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, entries: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";

  const records = await fetchAll(TABLES.timeEntries || "Time Entries", {
    filter: `FIND("${jobName}", ARRAYJOIN({Job Name (Text)}))`,
    sortField: "Work Date",
    sortDir: "desc"
  });

  const entries = records.map(r => {
    const f = r.fields || {};
    return {
      id:          r.id,
      workDate:    f["Work Date"] || "",
      employee:    f["Employee"] || "",
      class:       f["Class"] || "",
      cityTaxes:   f["City Taxes"] || "",
      hours:       f["Hours"] ?? null,
      reviewed:    f["Labor Reviewed"] === true,
      notes:       f["Notes"] || "",
      duration:    f["Duration (Seconds)"] ?? null
    };
  });

  return resp(200, { ok: true, entries });
}

async function handleExpenses(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, expenses: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";

  const records = await fetchAll("Expenses", {
    filter: `FIND("${jobName}", ARRAYJOIN({Job}))`,
    sortField: "Expense Date",
    sortDir: "desc"
  });

  const expenses = records.map(r => {
    const f = r.fields || {};

    // Vendor — use "Vendor Name (from Vendor)" lookup which returns plain strings
    const vendorLookup = f["Vendor Name (from Vendor)"];
    let vendor = "";
    if (Array.isArray(vendorLookup)) {
      vendor = vendorLookup.filter(Boolean).join(", ");
    } else if (vendorLookup && typeof vendorLookup === "object" && vendorLookup.valuesByLinkedRecordId) {
      vendor = Object.values(vendorLookup.valuesByLinkedRecordId).flat().join(", ");
    } else if (typeof vendorLookup === "string") {
      vendor = vendorLookup;
    }
    // Fallback to linked record name property
    if (!vendor) {
      const vendorArr = f["Vendor"];
      vendor = Array.isArray(vendorArr)
        ? vendorArr.map(v => (v && typeof v === "object" ? v.name : String(v)) || "").filter(Boolean).join(", ")
        : "";
    }

    // Job Markup % is a lookup — extract value
    const markupRaw = f["Job Markup %"];
    let markup = null;
    if (markupRaw?.valuesByLinkedRecordId) {
      markup = Object.values(markupRaw.valuesByLinkedRecordId).flat()[0] ?? null;
    } else if (Array.isArray(markupRaw)) {
      markup = markupRaw[0] ?? null;
    } else if (markupRaw != null) {
      markup = markupRaw;
    }

    return {
      id:               r.id,
      date:             f["Expense Date"] || "",
      description:      f["Description"] || "",
      vendor,
      expenseType:      f["Expense Type"]?.name || f["Expense Type"] || "",
      totalCost:        f["Total Cost (Actual)"] ?? null,
      expenseStatus:    f["Expense Status"]?.name || f["Expense Status"] || "",
      billable:         f["Billable?"] === true,
      jobMarkupPct:     markup,
      billableMaterial: f["Billable Material Amount $"] ?? null,
      reviewed:         f["Reviewed"] === true
    };
  });

  return resp(200, { ok: true, expenses });
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "jobs")          return await handleJobs();
      if (action === "generator")     return await handleGenerator(params);
      if (action === "expenses")      return await handleExpenses(params);
      if (action === "timeEntries")   return await handleTimeEntries(params);
      if (action === "scissorLifts")      return await handleScissorLifts();
      if (action === "scissorLiftsByJob") return await handleScissorLiftsByJob(params);
      if (action === "jobInspections")    return await handleJobInspections(params);
      if (action === "jobEstimates")      return await handleJobEstimates(params);
      if (action === "fleetVehicles")     return await handleFleetVehicles();
      if (action === "fleetServiceHistory") return await handleFleetServiceHistory(params);
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login") return await handleLogin(body);
      if (body.action === "updateJobStatus") return await handleUpdateJobStatus(body);
      if (body.action === "updatePowerCo") return await handleUpdatePowerCo(body);
      if (body.action === "updateTimeEntry") return await handleUpdateTimeEntry(body);
      if (body.action === "deleteTimeEntry") return await handleDeleteTimeEntry(body);
      if (body.action === "deleteExpense")   return await handleDeleteExpense(body);
      if (body.action === "approveExpense")  return await handleApproveExpense(body);
      if (body.action === "updateScissorLift")  return await handleUpdateScissorLift(body);
      if (body.action === "createInspection")   return await handleCreateInspection(body);
      if (body.action === "updateEstimate")      return await handleUpdateEstimate(body);
      if (body.action === "updateFleetVehicle")  return await handleUpdateFleetVehicle(body);
      if (body.action === "addFleetService")     return await handleAddFleetService(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Proxy Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

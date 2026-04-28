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
  scissorLifts:     "Scissor Lifts",
  scheduleEntries:  "Schedule Entries"
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
    billingMethod:              "Billing Method",
    baseContractAmount:         "Base Contract Amount",
    totalContractBilled:        "Total Contract Billed",
    customerFirstName:          "Customer 1st Name (Intake)",
    customerLastName:           "Customer Last Name (Intake)",
    customerStreet:             "Job Site Street Address (Intake)",
    customerCity:               "Job Site City (Intake)",
    customerState:              "Job Site State (Intake)",
    customerZip:                "Job Site Zip Code (Intake)",
    customerPhone:              "Customer Phone (Intake)",
    customerEmail:              "Customer Email (Intake)",
    powerCompanyIntake:         "Power Company (Intake)",
    startServiceCall:    "Start Service Call",
    serviceCallCreated:  "Service Call Created",
    projectComplete:     "Project Complete (Ready to Invoice)",
    milesFromShop:       "Miles from Shop",
    notes:               "Notes",
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
    expectedRevenueAllStatus:          "Expected Revenue (All Status)",
    projectedEstimatedTotalCost:       "Projected Estimated Total Cost",
    projectedEstimatedLaborHours:      "Projected Estimated Labor Hours (from Job Estimates)",
    projectedEstimatedMaterialCost:    "Projected Estimated Material Cost (from Job Estimates)",
    projectedEstimatedLaborCost:       "Projected Estimated Labor Cost (from Job Estimates)",
    projectedGrossProfitDollar:        "Projected Gross Profit $",
    projectedGrossProfitPct:           "Projected Gross Profit %"
  },
  gen: {
    assetId:"Generator Asset ID",customer:"Customer Name",customerPhone:"Customer Phone #",
    job:"Job",siteAddress:"Site Address",brand:"Generator Brand",model:"Generator Model",
    kw:"Generator KW",serialNumber:"Generator Serial Number",
    transferSwitchModel:"Transfer Switch Model",transferSwitchSerial:"Transfer Switch Serial Number",
    fuelType:"Fuel Type",installDate:"Install / In-Service Date",servicePlanActive:"Service Plan Active",
    serviceIntervalMonths:"Service Interval Months",nextServiceDue:"Next Service Due",
    warrantyExpiration:"Warranty Expiration",status:"Status",batteryInstallDate:"Battery Install Date",
    batteryAge:"Battery Age",serviceStatus:"Service Status",notes:"Notes"
  },
  svc: {
    serviceRecordId:"Service Record ID",generator:"Generator",customer:"Customer",job:"Job",
    serviceDate:"Service Date",serviceType:"Service Type",technician:"Technician",
    servicePlanVisit:"Service Plan Visit",oilChanged:"Oil Changed",oilFilterChanged:"Oil Filter Changed",
    airFilterChanged:"Air Filter Changed",sparkPlugsChanged:"Spark Plugs Changed",
    batteryTested:"Battery Tested",batteryReplaced:"Battery Replaced",
    loadTestPerformed:"Load Test Performed",firmwareChecked:"Firmware / Settings Checked",
    exerciseChecked:"Exercise Checked",troubleCodesFound:"Trouble Codes Found",
    workNotes:"Work Performed Notes",partsUsed:"Parts Used",laborHours:"Labor Hours",
    generatorHours:"Generator Hours @ Service"
  }
};

// Time Entries field IDs
const TE = {
  employee:   "fldG8nGxyJcXRxBNQ",   // Employee (text)
  employeeLink:"fldYgTcZcQzNslRT5",  // Employee (Linked)
  workDate:   "fldzFwSSjLmAkWYHt",
  duration:   "fld9mz6As3099VPVp",   // Duration (Seconds) — writable
  hours:      "fldC7LWlbpqMd27z9",   // Hours — formula, read only
  cityTaxes:  "flddCniABjh4Xib1c",
  class:      "fld4MG0FcFDnqYmtW",
  jobLink:    "fldmGwS0qXMdC7FlA",   // Job (linked record)
  jobNameText:"fldsemB5S5PivoZjd",   // Job Name (Text) — plain text
  reviewed:   "fldQn7d06doEkrGBv"
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
  const v = fields[fieldName];
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string") { const s = v.trim().toLowerCase(); return s==="1"||s==="true"||s==="yes"; }
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
    headers: { Authorization: `Bearer ${AIRTABLE_API_KEY}`, "Content-Type": "application/json", ...(options.headers || {}) }
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

// ── PAYROLL: fetch time entries by date range ──────────────────────────────
async function handlePayrollEntries(params) {
  const { startDate, endDate } = params || {};
  if (!startDate || !endDate) return resp(400, { ok: false, error: "Missing startDate or endDate." });

  // Use IS_SAME() for date comparison — required for Airtable date fields in filterByFormula
  // Build OR across all 14 days in the pay period
  function addDays(ds, n) {
    const [y,m,d] = ds.split("-").map(Number);
    const dt = new Date(y, m-1, d);
    dt.setDate(dt.getDate() + n);
    return dt.toISOString().slice(0,10);
  }
  const allDates = [];
  for (let i = 0; i <= 13; i++) {
    allDates.push(addDays(startDate, i));
  }
  // IS_SAME({Work Date}, "YYYY-MM-DD", "day") is the correct Airtable date equality check
  const dateClauses = allDates.map(d => `IS_SAME({Work Date},"${d}","day")`).join(",");
  const filter = `OR(${dateClauses})`;
  const records = await fetchAll(TABLES.timeEntries, {
    filter,
    sortField: "Work Date",
    sortDir: "asc"
  });

  // GET responses return fields by NAME not ID — use plain field names here
  const entries = records.map(r => {
    const f = r.fields || {};
    const jobLinks = f["Job"];
    const jobId = Array.isArray(jobLinks) && jobLinks.length
      ? (typeof jobLinks[0] === "string" ? jobLinks[0] : jobLinks[0]?.id || null)
      : null;
    return {
      id:        r.id,
      employee:  f["Employee"] || "",
      workDate:  f["Work Date"] || "",
      duration:  f["Duration (Seconds)"] ?? 0,
      hours:     f["Hours"] ?? 0,
      cityTaxes: f["City Taxes"] || "A No Tax",
      class:     f["Class"] || "",
      jobId:     jobId,
      jobName:   f["Job Name (Text)"] || "",
      reviewed:  f["Labor Reviewed"] === true
    };
  });

  return resp(200, { ok: true, entries });
}

// ── PAYROLL: create new time entry ─────────────────────────────────────────
async function handleCreateTimeEntry(body) {
  const { employee, workDate, duration, class: cls, cityTaxes, jobId } = body || {};
  if (!employee || !workDate) return resp(400, { ok: false, error: "Missing employee or workDate." });

  const fields = {};
  fields[TE.employee]   = employee;
  fields[TE.workDate]   = workDate;
  fields[TE.duration]   = Math.round(Number(duration) || 0);
  fields[TE.class]      = cls || "Contract";
  fields[TE.cityTaxes]  = cityTaxes || "A No Tax";
  if (jobId && String(jobId).startsWith("rec")) {
    fields[TE.jobLink] = [{ id: String(jobId) }];
  }

  const data = await atFetch(`${encodeURIComponent(TABLES.timeEntries)}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
  return resp(200, { ok: true, id: data.id });
}

// ── PAYROLL: update time entry (duration + other fields) ───────────────────
async function handleUpdateTimeEntryPayroll(body) {
  const { entryId, duration, workDate, class: cls, cityTaxes, jobId, reviewed } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const fields = {};
  if (duration  !== undefined && duration  !== null) fields[TE.duration]  = Math.round(Number(duration));
  if (workDate  !== undefined) fields[TE.workDate]  = workDate;
  if (cls       !== undefined) fields[TE.class]     = cls;
  if (cityTaxes !== undefined) fields[TE.cityTaxes] = cityTaxes;
  if (reviewed  !== undefined) fields[TE.reviewed]  = reviewed === true;
  if (jobId !== undefined) {
    fields[TE.jobLink] = (jobId && String(jobId).startsWith("rec")) ? [{ id: String(jobId) }] : [];
  }

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${entryId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

// ══════════════════════════════════════════════════════════════════
// All existing handlers below — unchanged
// ══════════════════════════════════════════════════════════════════

async function handleLogin(body) {
  const { identifier, pin } = body || {};
  if (!identifier || !pin) return resp(400, { ok: false, error: "Missing identifier or PIN." });
  const records = await fetchAll(TABLES.employees);
  const match = records.find(r => {
    const f = r.fields || {};
    const name=normalize(f[F.emp.name]),username=normalize(f[F.emp.username]),email=normalize(f[F.emp.email]);
    const savedPin=String(f[F.emp.pin]||"").trim(),active=gBool(f,F.emp.active),id=normalize(identifier);
    return [name,username,email].includes(id)&&savedPin!==""&&savedPin===String(pin).trim()&&active;
  });
  if (!match) return resp(401, { ok: false, error: "Invalid login. Check your name and PIN." });
  const f = match.fields || {};
  // Recognize four roles: admin, office, viewer, employee. Office acts like
  // admin on the field-app side but is filtered out of inventory + crew pickers.
  const rawRole = normalize(f[F.emp.role]);
  let role;
  if      (rawRole === "admin")  role = "admin";
  else if (rawRole === "office") role = "office";
  else if (rawRole === "viewer") role = "viewer";
  else                            role = "employee";
  return resp(200, { ok: true, user: { id: match.id, name: f[F.emp.name]||"Unknown", role } });
}

// Shared mapper — used by handleJobs (list) and handleJobById (single).
// Keeping the projection in one place ensures the single-record refresh
// path returns the same shape the list-and-state code expects.
function mapJob(r) {
  const f = r.fields || {};
  return {
    id:r.id,name:g(f,F.job.name)||"",po:g(f,F.job.po)||"",status:g(f,F.job.status)||"",
    type:g(f,F.job.type)||"",address:g(f,F.job.address)||"",contractor:g(f,F.job.contractor)||"",
      generatorInstalled:gBool(f,F.job.generatorInstalled),
      powerCompanyName:g(f,F.job.powerCompanyName)||"",powerCompanyContact:g(f,F.job.powerCompanyContact)||"",
      powerCompanyPhone:g(f,F.job.powerCompanyPhone)||"",powerCompanyEmail:g(f,F.job.powerCompanyEmail)||"",
      aicNumber:g(f,F.job.aicNumber)||"",tempWorkOrder:g(f,F.job.tempWorkOrder)||"",
      permWorkOrder:g(f,F.job.permWorkOrder)||"",meterNumber:g(f,F.job.meterNumber)||"",
      permitNumber:g(f,F.job.permitNumber)||"",inspectionAgency:g(f,F.job.inspectionAgency)||"",
      inspectionAgencyPhone:g(f,F.job.inspectionAgencyPhone)||"",inspectionAgencyEmail:g(f,F.job.inspectionAgencyEmail)||"",
      inspectionSchedulingLink:g(f,F.job.inspectionSchedulingLink)||"",inspectionContacts:g(f,F.job.inspectionContacts)||"",
      jobInspections:g(f,F.job.jobInspections)||"",wireLink:extractUrl(g(f,F.job.wireLink)),
      pipeLink:extractUrl(g(f,F.job.pipeLink)),addPhotosLink:extractUrl(g(f,F.job.addPhotosLink)),
      viewPhotosLink:extractUrl(g(f,F.job.viewPhotosLink)),trelloCardId:g(f,F.job.trelloCardId)||"",
      generatorCommissioningForm:g(f,F.job.generatorCommissioningForm)||"",
      newGeneratorServiceForm:g(f,F.job.newGeneratorServiceForm)||"",
      taxStatus:g(f,F.job.taxStatus)||"",powerCompanyIntake:g(f,F.job.powerCompanyIntake)||"",
      billingMethod:g(f,F.job.billingMethod)||"",
      baseContractAmount:gNum(f,F.job.baseContractAmount),
      totalContractBilled:gNum(f,F.job.totalContractBilled),
      customerFirstName:g(f,F.job.customerFirstName)||"",
      customerLastName:g(f,F.job.customerLastName)||"",
      customerStreet:g(f,F.job.customerStreet)||"",
      customerCity:g(f,F.job.customerCity)||"",
      customerState:g(f,F.job.customerState)||"",
      customerZip:g(f,F.job.customerZip)||"",
      customerPhone:g(f,F.job.customerPhone)||"",
      customerEmail:g(f,F.job.customerEmail)||"",
      startServiceCall:gBool(f,F.job.startServiceCall),serviceCallCreated:gBool(f,F.job.serviceCallCreated),
      projectComplete:gBool(f,F.job.projectComplete),milesFromShop:gNum(f,F.job.milesFromShop),
      notes:g(f,F.job.notes)||"",
      totalRevenueLive:gNum(f,F.job.totalRevenueLive),totalMaterialsLive:gNum(f,F.job.totalMaterialsLive),
      totalLaborCostLive:gNum(f,F.job.totalLaborCostLive),totalWireCost:gNum(f,F.job.totalWireCost),
      pipeCost:gNum(f,F.job.pipeCost),materialsInProgress:gNum(f,F.job.materialsInProgress),
      grossProfitLiveDollar:gNum(f,F.job.grossProfitLiveDollar),grossProfitLivePct:gNum(f,F.job.grossProfitLivePct),
      workflowStatus:g(f,F.job.workflowStatus),estimatedLaborHoursRollup:gNum(f,F.job.estimatedLaborHoursRollup),
      hoursRollup:gNum(f,F.job.hoursRollup),
      billableHourlyRate: (() => {
        const v = f["Billable Hourly Rate (from Labor Billable Rates)"];
        if (Array.isArray(v)) return v[0] ?? null;
        return v ?? null;
      })(),
      laborBillableRateId: (() => {
        const v = f["Labor Billable Rates"];
        if (Array.isArray(v) && v.length > 0) {
          return typeof v[0] === "string" ? v[0] : v[0]?.id || null;
        }
        return null;
      })(),
      pCloudInvoicesSentId: f["pCloud Invoices Sent ID"] || null,
      expectedRevenue:gNum(f,F.job.expectedRevenue),
      actualJobCostCogs:gNum(f,F.job.actualJobCostCogs),totalReviewedCosts:gNum(f,F.job.totalReviewedCosts),
      totalLaborCostFinal:gNum(f,F.job.totalLaborCostFinal),grossProfitFinalDollar:gNum(f,F.job.grossProfitFinalDollar),
      grossProfitFinalPct:gNum(f,F.job.grossProfitFinalPct),allMaterialsReviewed:gFormulaBool(f,F.job.allMaterialsReviewed),
      allWireReviewed:gFormulaBool(f,F.job.allWireReviewed),allPipeReviewed:gFormulaBool(f,F.job.allPipeReviewed),
      allExpensesReviewed:gFormulaBool(f,F.job.allExpensesReviewed),allLaborReviewed:gFormulaBool(f,F.job.allLaborReviewed),
      expectedRevenueAllStatus:gNum(f,F.job.expectedRevenueAllStatus),
      projectedEstimatedTotalCost:gNum(f,F.job.projectedEstimatedTotalCost),
      projectedEstimatedLaborHours:gNum(f,F.job.projectedEstimatedLaborHours),
      projectedEstimatedMaterialCost:gNum(f,F.job.projectedEstimatedMaterialCost),
      projectedEstimatedLaborCost:gNum(f,F.job.projectedEstimatedLaborCost),
      projectedGrossProfitDollar:gNum(f,F.job.projectedGrossProfitDollar),
      projectedGrossProfitPct:gNum(f,F.job.projectedGrossProfitPct)
  };
}

async function handleJobs() {
  const records = await fetchAll(TABLES.jobs);
  const jobs = records
    .map(mapJob)
    .filter(j => !["archived","cancelled","canceled","closed"].includes(normalize(j.status)));
  return resp(200, { ok: true, jobs });
}

// Returns a single Job in the same shape as handleJobs. Used to refresh
// rollup-driven fields (Expected Revenue, Projected Gross Profit, etc.)
// after Job Estimates writes so the Est. GP cards don't show stale data.
async function handleJobById(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });
  return resp(200, { ok: true, job: mapJob(records[0]) });
}

async function handleGenerator(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, generator: null, serviceRecords: [] });
  const jobName = jobRecords[0].fields[F.job.name] || "";
  const filter = `FIND("${jobName}", ARRAYJOIN({${F.gen.job}}))`;
  const genRecords = await fetchAll(TABLES.generators, { filter });
  if (!genRecords.length) return resp(200, { ok: true, generator: null, serviceRecords: [] });
  const r = genRecords[0]; const f = r.fields || {};
  const generator = { id:r.id,assetId:g(f,F.gen.assetId)||"",customer:g(f,F.gen.customer)||"",customerPhone:g(f,F.gen.customerPhone)||"",siteAddress:g(f,F.gen.siteAddress)||"",brand:g(f,F.gen.brand)||"",model:g(f,F.gen.model)||"",kw:g(f,F.gen.kw)||"",serialNumber:g(f,F.gen.serialNumber)||"",transferSwitchModel:g(f,F.gen.transferSwitchModel)||"",transferSwitchSerial:g(f,F.gen.transferSwitchSerial)||"",fuelType:g(f,F.gen.fuelType)||"",installDate:g(f,F.gen.installDate)||"",servicePlanActive:gBool(f,F.gen.servicePlanActive),serviceIntervalMonths:g(f,F.gen.serviceIntervalMonths)||"",nextServiceDue:g(f,F.gen.nextServiceDue)||"",warrantyExpiration:g(f,F.gen.warrantyExpiration)||"",status:g(f,F.gen.status)||"",batteryInstallDate:g(f,F.gen.batteryInstallDate)||"",batteryAge:g(f,F.gen.batteryAge)||"",serviceStatus:g(f,F.gen.serviceStatus)||"",notes:g(f,F.gen.notes)||"" };
  const genAssetId = generator.assetId || "";
  const svcFilter = genAssetId ? `FIND("${genAssetId}", ARRAYJOIN({${F.svc.generator}}))` : `FALSE()`;
  const svcRecords = await fetchAll(TABLES.generatorService, { filter: svcFilter, sortField: F.svc.serviceDate, sortDir: "desc" });
  const serviceRecords = svcRecords.map(sr => { const sf=sr.fields||{}; return { id:sr.id,serviceDate:g(sf,F.svc.serviceDate)||"",serviceType:g(sf,F.svc.serviceType)||"",technician:g(sf,F.svc.technician)||"",servicePlanVisit:gBool(sf,F.svc.servicePlanVisit),oilChanged:gBool(sf,F.svc.oilChanged),oilFilterChanged:gBool(sf,F.svc.oilFilterChanged),airFilterChanged:gBool(sf,F.svc.airFilterChanged),sparkPlugsChanged:gBool(sf,F.svc.sparkPlugsChanged),batteryTested:gBool(sf,F.svc.batteryTested),batteryReplaced:gBool(sf,F.svc.batteryReplaced),loadTestPerformed:gBool(sf,F.svc.loadTestPerformed),firmwareChecked:gBool(sf,F.svc.firmwareChecked),exerciseChecked:gBool(sf,F.svc.exerciseChecked),troubleCodesFound:g(sf,F.svc.troubleCodesFound)||"",workNotes:g(sf,F.svc.workNotes)||"",partsUsed:g(sf,F.svc.partsUsed)||"",laborHours:g(sf,F.svc.laborHours)||"",generatorHours:g(sf,F.svc.generatorHours)||"" }; });
  return resp(200, { ok: true, generator, serviceRecords });
}

async function handleUpdateJobStatus(body) {
  const { jobId, status } = body || {};
  if (!jobId || !status) return resp(400, { ok: false, error: "Missing jobId or status." });
  const VALID = ["New Lead","Estimating","Awarded","Service Call Scheduled","Ready to Invoice","Completed","Not Awarded"];
  if (!VALID.includes(status)) return resp(400, { ok: false, error: "Invalid status value." });
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fld2FBMjvkOsy9Puu": status } }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleUpdatePowerCo(body) {
  const { jobId, powerCompany, powerContact, aicNumber, tempWorkOrder, permWorkOrder, meterNumber } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const CONTACT_IDS = { "Dave Baker":"rec0Pmo9JBNVSdJ23","Dan Shaeffer":"recejs6S1LHK4rbNN","Earnest Kash":"reczPDI9EgwALy07k","Tom Shultz":"rec1Z0Epjw6C8BK90","Gavyn Lopez":"rec1kjUkMawcHocok","Diane Wintrow":"recf5WUTUQdvUW2QR","Dan Johnson":"reclI0d8M1mP4BpBM" };
  const fields = {};
  if (powerCompany && powerCompany.trim() !== "") fields["fldURTQ0ygHMMIbTU"] = powerCompany.trim();
  if (powerContact && CONTACT_IDS[powerContact]) fields["fldhKlMCFsnmHo5PH"] = [{ id: CONTACT_IDS[powerContact] }];
  if (aicNumber !== undefined) fields["fld1vqpCklUdzgrjO"] = aicNumber;
  if (tempWorkOrder !== undefined) fields["fldmJKSiIQfJm9zhI"] = tempWorkOrder;
  if (permWorkOrder !== undefined) fields["fld6t3TBBz6SwJPh8"] = permWorkOrder;
  if (meterNumber !== undefined) fields["fldWXpfslcqLlwdTQ"] = meterNumber;
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleStartServiceCall(body) {
  const { jobId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldgar4OL6AL5k1S6": true } }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleCompleteServiceCall(body) {
  const { jobId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldZ4tEiYt6Ke8IlK": true } }) });
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
  const data = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldwSsga6eashzJsw": true } }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleScissorLiftsByJob(params) {
  const { jobName } = params || {};
  if (!jobName) return resp(200, { ok: true, lifts: [] });
  const records = await fetchAll(TABLES.scissorLifts, { sortField: "Lift Name", sortDir: "asc" });
  const lifts = records.map(r => { const f=r.fields||{}; const photos=(f["Photo"]||[]).map(a=>a.url); return { id:r.id,name:f["Lift Name"]||"",status:f["Status"]||"Available",currentJob:f["Current Job"]||"",assignedTo:f["Assigned To"]||"",dateDeployed:f["Date Deployed"]||"",notes:f["Notes"]||"",photoUrl:photos[0]||"",hooksLeft:f["Lift Hooks Left at Job"]===true,boxLeft:f["Lift Box Left at Job"]===true }; }).filter(l => l.currentJob === jobName && l.status === "On Job");
  return resp(200, { ok: true, lifts });
}

async function handleJobInspections(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, inspections: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  const records = await fetchAll("Job Inspections", { filter: `FIND("${jobName}", ARRAYJOIN({Job}))`, sortField: "Inspection Date", sortDir: "desc" });
  const inspections = records.map(r => { const f=r.fields||{}; const permitRaw=f["Permit Number"]; const permit=Array.isArray(permitRaw)?permitRaw[0]:(permitRaw||""); const phoneRaw=f["Inspections Agency Phone #"]; const agencyPhone=Array.isArray(phoneRaw)?phoneRaw[0]:(phoneRaw||""); return { id:r.id,inspectionType:f["Inspection Type"]?.name||f["Inspection Type"]||"",date:f["Inspection Date"]||"",status:f["Inspection Status"]?.name||f["Inspection Status"]||"",notes:f["Notes"]||"",permitNumber:permit,agencyPhone }; });
  return resp(200, { ok: true, inspections });
}

async function handleCreateInspection(body) {
  const { jobId, inspectionType, date, status, notes } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const fields = {};
  fields["fldqk2pA5w3TSN3q8"] = [String(jobId)];
  if (inspectionType) fields["fldR2IQkaeRHXytsR"] = inspectionType;
  if (date)           fields["fldPblyNOIryMLFB6"] = date;
  if (status)         fields["fld7kH2SEHsxaS9vz"] = status;
  if (notes)          fields["fldmz5dOw6In5OkU7"] = notes;
  const data = await atFetch(`${encodeURIComponent("Job Inspections")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
}

async function handleJobEstimates(params) {
  const { jobId, onlySaved } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, estimates: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  const records = await fetchAll("Job Estimates", { filter: `FIND("${jobName}", ARRAYJOIN({Job}))`, sortField: "Estimate Date", sortDir: "desc" });
  let estimates = records.map(r => { const f=r.fields||{}; const pdfs=(f["Estimate PDF"]||[]).map(att=>({url:att.url,filename:att.filename,size:att.size})); return { id:r.id,name:f["Estimate Name"]||"",estimateType:f["Estimate Type"]?.name||f["Estimate Type"]||"",status:f["Status"]?.name||f["Status"]||"",date:f["Estimate Date"]||"",actualEstimate:f["Actual Estimate Sent"]??null,laborHours:f["Estimated Labor Hours"]??null,materialCost:f["Estimated Material Cost"]??null,calculatedTotal:f["Calculated Estimated Total"]??null,notes:f["Notes"]||"",displayNumber:f["Estimate Display #"]||null,snapshot:f["Estimate Snapshot"]||"",pdfs }; });
  if (onlySaved) estimates = estimates.filter(e => e.displayNumber != null);
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
  const data = await atFetch(`${encodeURIComponent("Job Estimates")}/${estimateId}`, { method: "PATCH", body: JSON.stringify({ fields }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleUpdateEstimateStatus(body) {
  const { estimateId, status } = body || {};
  if (!estimateId || !status) return resp(400, { ok: false, error: "Missing estimateId or status." });
  // Job Estimates — Status field ID = fld9GsGvxaNPuCnjo (singleSelect)
  const fields = { "fld9GsGvxaNPuCnjo": status };
  const data = await atFetch(`${encodeURIComponent("Job Estimates")}/${estimateId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

// ── GET NEXT ESTIMATE NUMBER ─────────────────────────────────────────────
// Queries the "Sent Estimate PDFs" table (not Job Estimates) so snapshot-only
// records don't commingle with the source-of-truth Job Estimates.
async function handleGetNextEstimateNumber() {
  const START_AT = 2187;
  let max = 0;
  let offset = undefined;
  try {
    do {
      const qs = "?fields%5B%5D=" + encodeURIComponent("Estimate Display #")
               + (offset ? "&offset=" + encodeURIComponent(offset) : "");
      const page = await atFetch(`${encodeURIComponent("Sent Estimate PDFs")}${qs}`);
      if (page.error) return resp(400, { ok: false, error: page.error });
      (page.records || []).forEach(r => {
        const n = Number(r?.fields?.["Estimate Display #"]);
        if (!isNaN(n) && n > max) max = n;
      });
      offset = page.offset;
    } while (offset);
  } catch (e) {
    // Friendly fallback: if the new table hasn't been created yet, start at 2187
    // so the app still works. The save path will surface a clearer error.
    const msg = String(e?.message || e || "");
    if (/NOT_FOUND|not.*found|could not.*find.*table/i.test(msg)) {
      return resp(200, { ok: true, nextNumber: START_AT, warning: "Sent Estimate PDFs table not found — starting at " + START_AT });
    }
    throw e;
  }
  const next = Math.max(max + 1, START_AT);
  return resp(200, { ok: true, nextNumber: next });
}

// ── SAVE ESTIMATE RECORD ─────────────────────────────────────────────────
// Writes to the "Sent Estimate PDFs" snapshots table (NOT Job Estimates).
// Job Estimates remains the source-of-truth table for Expected Revenue rollups
// and is only populated via the "New Job Estimate" Airtable form.
async function handleSaveEstimate(body) {
  const { estimateId, jobId, estimateDate, estimateNumber, notes, totalAmount, snapshot, jobEstimateIds } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const fields = {};
  fields["Job"] = [jobId];
  if (estimateDate) fields["Estimate Date"] = estimateDate;
  if (totalAmount !== undefined && totalAmount !== null && totalAmount !== "") {
    fields["Total"] = Number(totalAmount);
  }
  if (estimateNumber !== undefined && estimateNumber !== null && estimateNumber !== "") {
    const n = Number(estimateNumber);
    if (!isNaN(n)) fields["Estimate Display #"] = n;
  }
  if (snapshot) {
    fields["Snapshot"] = typeof snapshot === "string" ? snapshot : JSON.stringify(snapshot);
  }
  // Bidirectional traceability: link this snapshot to the Job Estimates
  // record(s) whose totals seeded the builder. Field is multipleRecordLinks
  // (fldPoz43rrlqWRnwC = "Job Estimate" on Sent Estimate PDFs).
  if (Array.isArray(jobEstimateIds)) {
    const cleaned = jobEstimateIds.filter(id => typeof id === "string" && id.startsWith("rec"));
    if (cleaned.length) fields["fldPoz43rrlqWRnwC"] = cleaned;
  }
  // Note: "notes" from the caller is embedded in the Snapshot JSON; no separate column.

  try {
    // PATCH the existing estimate snapshot when estimateId is provided (edit
    // mode); else POST a new record (create mode). Both paths use typecast
    // so any new singleSelect option values get auto-created.
    let data;
    if (estimateId) {
      data = await atFetch(`${encodeURIComponent("Sent Estimate PDFs")}/${estimateId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields, typecast: true })
      });
    } else {
      data = await atFetch(`${encodeURIComponent("Sent Estimate PDFs")}`, {
        method: "POST",
        body: JSON.stringify({ fields, typecast: true })
      });
    }
    if (data.error) return resp(400, { ok: false, error: data.error });
    return resp(200, { ok: true, id: data.id, updated: !!estimateId });
  } catch (e) {
    const msg = String(e?.message || e || "");
    if (/NOT_FOUND|could not.*find.*table/i.test(msg)) {
      return resp(400, { ok: false, error: "The 'Sent Estimate PDFs' table doesn't exist yet. Create it in Airtable with fields: Job (link to Jobs), Estimate Display # (number), Estimate Date (date), Snapshot (long text), Total (currency). Then try Save again." });
    }
    throw e;
  }
}

// ── LIST SAVED ESTIMATE PDF SNAPSHOTS FOR A JOB ──────────────────────────
// Backs the Estimate History panel. Reads from Sent Estimate PDFs only.
async function handleSentEstimatePDFs(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  let records = [];
  try {
    // Fetch all rows and filter in-memory by the Job link (same pattern as
    // handleGetJobInvoices — filterByFormula on linked records is unreliable).
    const all = [];
    let offset = undefined;
    do {
      const qs = (offset ? "?offset=" + encodeURIComponent(offset) : "");
      const page = await atFetch(`${encodeURIComponent("Sent Estimate PDFs")}${qs}`);
      if (page.error) return resp(400, { ok: false, error: page.error });
      all.push(...(page.records || []));
      offset = page.offset;
    } while (offset);
    records = all.filter(r => {
      const jobArr = r.fields?.["Job"];
      return Array.isArray(jobArr) && jobArr.indexOf(jobId) !== -1;
    });
  } catch (e) {
    const msg = String(e?.message || e || "");
    if (/NOT_FOUND|could not.*find.*table/i.test(msg)) {
      // Table not created yet — return empty list so the UI just says "none yet"
      return resp(200, { ok: true, estimates: [] });
    }
    throw e;
  }

  // Sort newest-first by date, falling back to display number
  records.sort((a, b) => {
    const da = a.fields?.["Estimate Date"] || "";
    const db = b.fields?.["Estimate Date"] || "";
    if (db !== da) return db.localeCompare(da);
    return Number(b.fields?.["Estimate Display #"] || 0) - Number(a.fields?.["Estimate Display #"] || 0);
  });

  const estimates = records.map(r => {
    const f = r.fields || {};
    return {
      id:             r.id,
      displayNumber:  f["Estimate Display #"] || null,
      date:           f["Estimate Date"] || "",
      total:          Number(f["Total"] || 0),
      snapshot:       f["Snapshot"] || "",
      // Derive a friendly name from the snapshot JSON if available
      name:           (() => {
        try {
          const s = JSON.parse(f["Snapshot"] || "{}");
          const first = (s.lines || [])[0]?.description || "";
          const head  = first ? first.split(/\r?\n/)[0].trim().slice(0, 80) : (s.jobName || "");
          const num   = f["Estimate Display #"] ? `#${f["Estimate Display #"]} — ` : "";
          return `${num}${head}`.trim() || "Estimate";
        } catch { return `#${f["Estimate Display #"] || ""}`.trim() || "Estimate"; }
      })(),
      // Status is implicit for saved PDFs; surface "Sent" for the history UI
      status:         "Sent",
      actualEstimate: Number(f["Total"] || 0),
      calculatedTotal:Number(f["Total"] || 0)
    };
  });
  return resp(200, { ok: true, estimates });
}

// ── ESTIMATE TEMPLATES ───────────────────────────────────────────────────
// Lists Active templates from the Estimate Templates table. If a contractor
// name is supplied, only templates whose Contractor link resolves to that
// name are returned. With no contractor, all Active templates are returned
// (covers jobs that have no contractor set).
async function handleEstimateTemplates(params) {
  const { contractor } = params || {};
  // ARRAYJOIN() on a multipleRecordLinks field expands to the primary field
  // of the linked table; Companies' primary field is "Company Name", so a
  // FIND on the joined string resolves the linked contractor by name.
  const safeContractor = (contractor || "").replace(/"/g, '\\"').trim();
  const filter = safeContractor
    ? `AND({Active}=TRUE(), FIND("${safeContractor}", ARRAYJOIN({Contractor})))`
    : `{Active}=TRUE()`;
  const records = await fetchAll("Estimate Templates", { filter, sortField: "Template Name", sortDir: "asc" });

  const templates = records.map(r => {
    const f = r.fields || {};
    const contractorIds = Array.isArray(f["Contractor"]) ? f["Contractor"] : [];
    return {
      id:                 r.id,
      name:               f["Template Name"] || "",
      contractorIds,
      active:             f["Active"] === true,
      scopeOfWork:        f["Scope of Work"] || "",
      exclusions:         f["Exclusions"] || "",
      standardTerms:      f["Standard Terms"] || "",
      basePrice:          gNum(f, "Base Price"),
      defaultLaborHours:  gNum(f, "Default Labor Hours"),
      defaultMaterialCost:gNum(f, "Default Material Cost"),
      internalNotes:      f["Internal Notes"] || ""
    };
  });
  return resp(200, { ok: true, templates });
}

// ── CREATE JOB ESTIMATE ──────────────────────────────────────────────────
// POSTs a new Job Estimates record with the four template-derived fields
// snapshotted in. Source Template (fldrni1Lkpw7tMBq8) records which
// template seeded the values; the values themselves are independent
// scalars, so editing the template later does not change this estimate.
async function handleCreateJobEstimate(body) {
  const { jobId, baseAmount, laborHours, materialCost, notes, estimateType, sourceTemplateId, estimateDate } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // NOTE: Estimate Name (fldneXJv6ia3TIPj6) is a formula field on the Job
  // Estimates table — Airtable computes it automatically and rejects writes.
  // The other formulas on this table (Estimated Labor Cost, Calculated
  // Estimated Total) are also skipped here. Only user-editable fields below.
  const fields = {};
  fields["Job"] = [jobId];
  // Status (fld9GsGvxaNPuCnjo, singleSelect) has no schema-level default,
  // so records created here without it land with Status=null and get
  // filtered out of the Est. GP estimates view. Default to "Draft" — the
  // starting state used by existing records.
  fields["Status"] = "Draft";
  // Estimate Type (fld8rcQ3Ni2P1AbUR, singleSelect). User-selected in the
  // New Job Estimate modal pill group; whitelisted here against the four
  // valid options so a stray client value can't trip Airtable's typecast,
  // with "Original" as the fallback for missing/unrecognized input.
  const ESTIMATE_TYPE_OPTS = ["Original", "Addendum", "Change Order", "Extra's"];
  fields["Estimate Type"] = ESTIMATE_TYPE_OPTS.includes(estimateType) ? estimateType : "Original";
  if (estimateDate) fields["Estimate Date"] = estimateDate;
  if (baseAmount   !== undefined && baseAmount   !== null && baseAmount   !== "") fields["Actual Estimate Sent"]    = Number(baseAmount);
  if (laborHours   !== undefined && laborHours   !== null && laborHours   !== "") fields["Estimated Labor Hours"]   = Number(laborHours);
  if (materialCost !== undefined && materialCost !== null && materialCost !== "") fields["Estimated Material Cost"] = Number(materialCost);
  if (notes && String(notes).trim()) fields["Notes"] = String(notes);
  if (sourceTemplateId && String(sourceTemplateId).startsWith("rec")) {
    fields["fldrni1Lkpw7tMBq8"] = [sourceTemplateId];
  }

  const data = await atFetch(`${encodeURIComponent("Job Estimates")}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
  return resp(200, { ok: true, id: data.id });
}

const FLEET_TABLES = { vehicles: "Fleet Vehicles", maintenance: "Fleet Maintenance", mileageLog: "Fleet Mileage Log" };
const FV = { name:"fldBcqDl6ez0GZz9n",year:"fld7E7ubdLAnlbplu",make:"fldiPxOpsxiO3JbqQ",model:"fldXFQ1u0BKpd94Fa",color:"fldR9UNl5MD8QRelB",vin:"fldMCiACFqTxA87Ay",plate:"fldX23ZlkmGHTx52S",type:"flduEmTHcrv24SlJT",mileage:"fldcRmbsqWDMyfzuF",mileageDate:"fldwIxFsMRrZsAAEy",oilType:"fldgT7qDyTXa1SeUC",oilCapacity:"fldgjlvQVc4kOEkqY",tireBrand:"fldkBVEAr6qCkTAZS",tireSize:"fldCC7EoiTXi7BxMR",tireInstall:"fldn7QbuEneDRgJ76",notes:"fldx4pEJ5JS0DFLyh",active:"fldapfWYijFLo7n1P" };

// Fleet Mileage Log field IDs
const ML = {
  date:       "fldpocv4rD2tnP4mI",
  vehicle:    "fldj5TbPqXRgjmujf",
  mileage:    "fldvU5jCOJLnRdxA8",
  recordedBy: "fldD65Hu9x322XfMe",
  notes:      "fldHyERXXifvzyebA"
};

async function handleFleetVehicles() {
  const records = await fetchAll(FLEET_TABLES.vehicles, { sortField: "Vehicle Name", sortDir: "asc" });
  const vehicles = records.filter(r => r.fields["Active"] === true).map(r => { const f=r.fields||{}; return { id:r.id,name:f["Vehicle Name"]||"",year:f["Year"]||null,make:f["Make"]||"",model:f["Model"]||"",color:f["Color"]||"",vin:f["VIN"]||"",plate:f["License Plate"]||"",type:f["Vehicle Type"]?.name||f["Vehicle Type"]||"",currentMileage:f["Current Mileage"]??null,mileageDate:f["Mileage Date"]||"",oilType:f["Oil Type"]||"",oilCapacity:f["Oil Capacity (qts)"]??null,tireBrand:f["Tire Brand"]||"",tireSize:f["Tire Size"]||"",tireInstallDate:f["Tire Install Date"]||"",notes:f["Notes"]||"",photoUrl:(f["Photo"]||[])[0]?.url||"",wrenchSize:f["Oil Drain Wrench Size"]||"",lugTorque:f["Lug Torque (ft-lbs)"]??null }; });
  return resp(200, { ok: true, vehicles });
}

async function handleFleetServiceHistory(params) {
  const { vehicleId } = params || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  const vehRecords = await fetchAll(FLEET_TABLES.vehicles, { filter: `RECORD_ID()="${vehicleId}"` });
  if (!vehRecords.length) return resp(200, { ok: true, records: [] });
  const vehName = vehRecords[0].fields["Vehicle Name"] || "";
  const records = await fetchAll(FLEET_TABLES.maintenance, { filter: `{Vehicle}="${vehName}"`, sortField: "Date", sortDir: "desc" });
  const serviceRecords = records.map(r => { const f=r.fields||{}; const types=(f["Service Types"]||[]).map(s=>(typeof s==="object"?s.name:s)); return { id:r.id,date:f["Date"]||"",mileage:f["Mileage at Service"]??null,serviceTypes:types,oilBrand:f["Filter #"]||"",oilType:f["Oil Type Used"]||"",oilQty:f["Oil Qty (qts)"]??null,tireBrand:f["Tire Brand Installed"]||"",tireSize:f["Tire Size Installed"]||"",cost:f["Cost"]??null,performedBy:f["Performed By"]||"",shop:f["Shop / Location"]||"",notes:f["Notes"]||"" }; });
  return resp(200, { ok: true, records: serviceRecords });
}

async function handleUpdateFleetVehicle(body) {
  const { vehicleId, currentMileage, oilType, oilCapacity, tireBrand, tireSize, tireInstallDate, vin, plate, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  const fields = {};
  if (currentMileage !== undefined) { fields[FV.mileage]=Number(currentMileage); fields[FV.mileageDate]=new Date().toISOString().slice(0,10); }
  if (oilType        !== undefined) fields[FV.oilType]=oilType;
  if (oilCapacity    !== undefined) fields[FV.oilCapacity]=Number(oilCapacity);
  if (tireBrand      !== undefined) fields[FV.tireBrand]=tireBrand;
  if (tireSize       !== undefined) fields[FV.tireSize]=tireSize;
  if (tireInstallDate!== undefined) fields[FV.tireInstall]=tireInstallDate;
  if (vin            !== undefined) fields[FV.vin]=vin;
  if (plate          !== undefined) fields[FV.plate]=plate;
  if (notes          !== undefined) fields[FV.notes]=notes;
  const data = await atFetch(`${encodeURIComponent(FLEET_TABLES.vehicles)}/${vehicleId}`, { method: "PATCH", body: JSON.stringify({ fields }) });
  return resp(200, { ok: true, updatedId: data.id });
}

// ── LOG MILEAGE: creates entry in Fleet Mileage Log AND updates Fleet Vehicles ──
async function handleLogMileage(body) {
  const { vehicleId, mileage, date, recordedBy, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  if (mileage === undefined || mileage === null || mileage === "") {
    return resp(400, { ok: false, error: "Missing mileage." });
  }
  const idStr = String(vehicleId).trim();
  if (!idStr.startsWith("rec")) return resp(400, { ok: false, error: `Invalid vehicleId: ${idStr}` });

  const effectiveDate = date || new Date().toISOString().slice(0,10);
  const mileageNum = Number(mileage);
  if (isNaN(mileageNum) || mileageNum < 0) {
    return resp(400, { ok: false, error: "Invalid mileage value." });
  }

  // 1. Create log entry in Fleet Mileage Log table
  const logFields = {};
  logFields[ML.date]    = effectiveDate;
  logFields[ML.vehicle] = [idStr];
  logFields[ML.mileage] = mileageNum;
  if (recordedBy) logFields[ML.recordedBy] = recordedBy;
  if (notes)      logFields[ML.notes]      = notes;

  const logData = await atFetch(`${encodeURIComponent(FLEET_TABLES.mileageLog)}`, {
    method: "POST",
    body: JSON.stringify({ fields: logFields, typecast: true })
  });

  // 2. Update Fleet Vehicles record with new Current Mileage and Mileage Date
  const vehFields = {};
  vehFields[FV.mileage]     = mileageNum;
  vehFields[FV.mileageDate] = effectiveDate;

  const vehData = await atFetch(`${encodeURIComponent(FLEET_TABLES.vehicles)}/${idStr}`, {
    method: "PATCH",
    body: JSON.stringify({ fields: vehFields })
  });

  return resp(200, { ok: true, logId: logData.id, vehicleId: vehData.id });
}

async function handleAddFleetService(body) {
  const { vehicleId, vehicleName, date, mileage, serviceTypes, oilBrand, oilType, oilQty, cost, tireBrand, tireSize, performedBy, shop, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: `Missing vehicleId. Keys: ${Object.keys(body||{}).join(",")}` });
  const fields = {};
  fields["fld12gpaArqYw7BWU"] = vehicleName ? [vehicleName] : [{ id: String(vehicleId) }];
  if (date)         fields["fldwEhvgTGGEy9E3g"] = date;
  if (mileage)      fields["fldE7SlKw7n85bZWD"] = Number(mileage);
  if (serviceTypes && serviceTypes.length) fields["fldCiHkwHtsOZmkWk"] = serviceTypes;
  if (oilBrand)     fields["fldO7RALeUnXSgC6J"] = oilBrand;
  if (oilType)      fields["fldcgXpATus1HqW81"] = oilType;
  if (oilQty)       fields["fldwaUKNsJQvjwlK1"] = Number(oilQty);
  if (cost)         fields["fldwYmFTQLvOuDKIE"] = Number(cost);
  if (tireBrand)    fields["fldSw3UKcWky8bQlA"] = tireBrand;
  if (tireSize)     fields["fldVEwTlmNaWmRUiJ"] = tireSize;
  if (performedBy)  fields["fld4mHAqeBjCqSjkB"] = performedBy;
  if (shop)         fields["fldZddoeHsPrxapz1"] = shop;
  if (notes)        fields["fldwNDO1V7E26vql1"] = notes;
  const data = await atFetch(`${encodeURIComponent("Fleet Maintenance")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
}

async function handleUpdateFleetService(body) {
  const { serviceRecordId, date, mileage, serviceTypes, oilBrand, oilType, oilQty, cost, tireBrand, tireSize, performedBy, shop, notes } = body || {};
  if (!serviceRecordId) return resp(400, { ok: false, error: "Missing serviceRecordId." });
  const fields = {};
  if (date)         fields["fldwEhvgTGGEy9E3g"] = date;
  if (mileage)      fields["fldE7SlKw7n85bZWD"] = Number(mileage);
  if (serviceTypes) fields["fldCiHkwHtsOZmkWk"] = serviceTypes;
  if (oilBrand !== undefined) fields["fldO7RALeUnXSgC6J"] = oilBrand;
  if (oilType  !== undefined) fields["fldcgXpATus1HqW81"] = oilType;
  if (oilQty)       fields["fldwaUKNsJQvjwlK1"] = Number(oilQty);
  if (cost)         fields["fldwYmFTQLvOuDKIE"] = Number(cost);
  if (tireBrand !== undefined) fields["fldSw3UKcWky8bQlA"] = tireBrand;
  if (tireSize  !== undefined) fields["fldVEwTlmNaWmRUiJ"] = tireSize;
  if (performedBy !== undefined) fields["fld4mHAqeBjCqSjkB"] = performedBy;
  if (shop  !== undefined) fields["fldZddoeHsPrxapz1"] = shop;
  if (notes !== undefined) fields["fldwNDO1V7E26vql1"] = notes;
  const data = await atFetch(`${encodeURIComponent("Fleet Maintenance")}/${serviceRecordId}`, { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleDeleteFleetService(body) {
  const { serviceRecordId } = body || {};
  if (!serviceRecordId) return resp(400, { ok: false, error: "Missing serviceRecordId." });
  await atFetch(`${encodeURIComponent("Fleet Maintenance")}/${serviceRecordId}`, { method: "DELETE" });
  return resp(200, { ok: true, deleted: serviceRecordId });
}

async function handleScissorLifts() {
  const records = await fetchAll(TABLES.scissorLifts, { sortField: "Lift Name", sortDir: "asc" });
  const lifts = records.map(r => { const f=r.fields||{}; const photos=(f["Photo"]||[]).map(a=>a.url); return { id:r.id,name:f["Lift Name"]||"",status:f["Status"]||"Available",currentJob:f["Current Job"]||"",assignedTo:f["Assigned To"]||"",dateDeployed:f["Date Deployed"]||"",notes:f["Notes"]||"",photoUrl:photos[0]||"",hooksLeft:f["Lift Hooks Left at Job"]===true,boxLeft:f["Lift Box Left at Job"]===true }; });
  return resp(200, { ok: true, lifts });
}

async function handleUpdateScissorLift(body) {
  const { liftId, status, currentJob, assignedTo, dateDeployed, notes, hooksLeft, boxLeft } = body || {};
  if (!liftId) return resp(400, { ok: false, error: "Missing liftId." });
  const fields = {};
  if (status)                   fields["fldB9Kwqm0NS3RFFP"] = status;
  if (currentJob !== undefined) fields["fldZpCcD52inR2PGm"] = currentJob;
  if (assignedTo !== undefined) fields["fldkjsgzYiedjTaJ5"] = assignedTo || null;
  if (dateDeployed)             fields["fldqRXHkwiFQdjqor"] = dateDeployed;
  if (notes !== undefined)      fields["fldG5MLCzQbyClax0"] = notes;
  if (hooksLeft !== undefined)  fields["fldlpqrIcnTH8R7Yw"] = hooksLeft === true;
  if (boxLeft   !== undefined)  fields["fldm5zfYDcw0oQHX4"] = boxLeft === true;
  const data = await atFetch(`${encodeURIComponent(TABLES.scissorLifts)}/${liftId}`, { method: "PATCH", body: JSON.stringify({ fields }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleDeleteTimeEntry(body) {
  const { entryId } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });
  await atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${entryId}`, { method: "DELETE" });
  return resp(200, { ok: true, deleted: entryId });
}

async function handleUpdateTimeEntry(body) {
  const { entryId, reviewed, duration, notes } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });
  const fields = {};
  if (reviewed !== undefined) fields["fldQn7d06doEkrGBv"] = reviewed === true;
  if (duration !== undefined && duration !== null) fields["fld9mz6As3099VPVp"] = Number(duration);
  if (notes    !== undefined) fields["Notes"]            = String(notes || "");
  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });
  const data = await atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${entryId}`, { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleTimeEntries(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, entries: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  const records = await fetchAll(TABLES.timeEntries || "Time Entries", { filter: `FIND("${jobName}", ARRAYJOIN({Job Name (Text)}))`, sortField: "Work Date", sortDir: "desc" });
  const entries = records.map(r => { const f=r.fields||{}; return { id:r.id,workDate:f["Work Date"]||"",employee:f["Employee"]||"",class:f["Class"]||"",cityTaxes:f["City Taxes"]||"",hours:f["Hours"]??null,reviewed:f["Labor Reviewed"]===true,notes:f["Notes"]||"",duration:f["Duration (Seconds)"]??null,unbilledHours:f["Unbilled Hours"]??0,unbilledRevenue:f["Unbilled Labor Revenue $"]??0 }; });
  return resp(200, { ok: true, entries });
}

async function handleExpenses(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, expenses: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  const records = await fetchAll("Expenses", { filter: `FIND("${jobName}", ARRAYJOIN({Job}))`, sortField: "Expense Date", sortDir: "desc" });
  const expenses = records.map(r => {
    const f = r.fields || {};
    const vendorLookup = f["Vendor Name (from Vendor)"]; let vendor = "";
    if (Array.isArray(vendorLookup)) { vendor = vendorLookup.filter(Boolean).join(", "); }
    else if (vendorLookup && typeof vendorLookup === "object" && vendorLookup.valuesByLinkedRecordId) { vendor = Object.values(vendorLookup.valuesByLinkedRecordId).flat().join(", "); }
    else if (typeof vendorLookup === "string") { vendor = vendorLookup; }
    if (!vendor) { const vendorArr=f["Vendor"]; vendor=Array.isArray(vendorArr)?vendorArr.map(v=>(v&&typeof v==="object"?v.name:String(v))||"").filter(Boolean).join(", "):""; }
    const markupRaw=f["Job Markup %"]; let markup=null;
    if (markupRaw?.valuesByLinkedRecordId) { markup=Object.values(markupRaw.valuesByLinkedRecordId).flat()[0]??null; }
    else if (Array.isArray(markupRaw)) { markup=markupRaw[0]??null; }
    else if (markupRaw!=null) { markup=markupRaw; }
    return { id:r.id,date:f["Expense Date"]||"",description:f["Description"]||"",vendor,expenseType:f["Expense Type"]?.name||f["Expense Type"]||"",totalCost:f["Total Cost (Actual)"]??null,expenseStatus:f["Expense Status"]?.name||f["Expense Status"]||"",billable:f["Billable?"]===true,jobMarkupPct:markup,billableMaterial:f["Billable Material Amount $"]??null,reviewed:f["Reviewed"]===true,unbilledMaterial:f["Unbilled Material Amount $"]??0 };
  });
  return resp(200, { ok: true, expenses });
}

const SHOP_ADDRESS = "5909 Bandy Rd Homeworth OH 44634";
async function handleCalculateMileage(body) {
  const { jobId, address } = body || {};
  if (!jobId || !address) return resp(400, { ok: false, error: "Missing jobId or address." });
  const apiKey = process.env.GOOGLE_MAPS_API_KEY;
  if (!apiKey) return resp(500, { ok: false, error: "GOOGLE_MAPS_API_KEY env var not set." });
  const origin=encodeURIComponent(SHOP_ADDRESS),dest=encodeURIComponent(address);
  const url=`https://maps.googleapis.com/maps/api/distancematrix/json?origins=${origin}&destinations=${dest}&units=imperial&key=${apiKey}`;
  const res=await fetch(url); const data=await res.json();
  if (data.status !== "OK") return resp(400, { ok: false, error: `Google API error: ${data.status}` });
  const element = data.rows?.[0]?.elements?.[0];
  if (!element || element.status !== "OK") return resp(400, { ok: false, error: "Could not calculate distance for this address." });
  const miles = Math.round(element.distance.value * 0.000621371 * 10) / 10;
  await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldMy1yR7aHtVko9F": miles } }) });
  return resp(200, { ok: true, miles });
}

async function handleAddLiftExpense(body) {
  const { jobId, date, amount, description, billable } = body || {};
  if (!jobId || !amount) return resp(400, { ok: false, error: "Missing jobId or amount." });
  const idStr = String(jobId).trim();
  if (!idStr.startsWith("rec")) return resp(400, { ok: false, error: `Invalid jobId received: ${idStr}` });
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldlTUL8hsPkReBAB":[{id:"recU56ncurkFrM2Nx"}],"fldwbLPIafVtmaSeb":Number(amount),"fldX2x2J0xkRyMY3y":"Scissor Lift","fldelsB2jH2tvt1Cj":description||"Scissor Lift Expense","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  if (date) fields["fldCCPYdyWAOGchWb"] = date;
  const data = await atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
}

async function handleAddGeneralExpense(body) {
  const { jobId, date, type, amount, credit, vendorId, description, billable } = body || {};
  if (!jobId || !amount) return resp(400, { ok: false, error: "Missing jobId or amount." });
  const idStr = String(jobId).trim();
  if (!idStr.startsWith("rec")) return resp(400, { ok: false, error: `Invalid jobId: ${idStr}` });
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldwbLPIafVtmaSeb":Number(amount),"fldX2x2J0xkRyMY3y":type||"Materials","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  if (date)        fields["fldCCPYdyWAOGchWb"] = date;
  if (description) fields["fldelsB2jH2tvt1Cj"] = description;
  if (credit && Number(credit) > 0) fields["fldcld418pREq2bGq"] = Number(credit);
  if (vendorId && String(vendorId).startsWith("rec")) fields["fldlTUL8hsPkReBAB"] = [{ id: String(vendorId) }];
  const data = await atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
}

async function handleUpdateInspection(body) {
  const { inspectionId, status, notes } = body || {};
  if (!inspectionId) return resp(400, { ok: false, error: "Missing inspectionId." });
  const fields = {};
  if (status) fields["fld7kH2SEHsxaS9vz"] = status;
  if (notes !== undefined) fields["fldmz5dOw6In5OkU7"] = notes;
  const data = await atFetch(`${encodeURIComponent("Job Inspections")}/${inspectionId}`, { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, updatedId: data.id });
}

async function handleCompanies() {
  const records = await fetchAll("Companies", { sortField: "Company Name", sortDir: "asc" });
  const companies = records
    .filter(r => r.fields["Company Name"])
    .map(r => ({
      id:             r.id,
      name:           r.fields["Company Name"] || "",
      billingAddress: r.fields["Billing Address"] || "",
      primaryPhone:   r.fields["Primary Phone"] || "",
      primaryEmail:   r.fields["Primary Email"] || ""
    }));
  return resp(200, { ok: true, companies });
}

async function handleVendors() {
  const records = await fetchAll("Vendors", { sortField: "Vendor Name", sortDir: "asc" });
  const vendors = records.filter(r => r.fields["Active"] !== false).map(r => ({ id:r.id, name:r.fields["Vendor Name"]||"" })).filter(v => v.name);
  return resp(200, { ok: true, vendors });
}

// ── LABOR BILLABLE RATES (for per-job rate selector) ──────────────────────
async function handleLaborBillableRates() {
  const records = await fetchAll("Labor Billable Rates", { sortField: "Billable Hourly Rate", sortDir: "asc" });
  const today = new Date().toISOString().slice(0,10);
  const rates = records
    .filter(r => {
      // Only show rates that are still active (no end date, or end date in future)
      const endDate = r.fields["Effective End Date"];
      return !endDate || endDate >= today;
    })
    .map(r => {
      const f = r.fields || {};
      return {
        id:        r.id,
        label:     f["Billable Rate ID"] || "",
        laborType: (f["Labor Type"] && typeof f["Labor Type"] === "object") ? f["Labor Type"].name : (f["Labor Type"] || ""),
        rate:      f["Billable Hourly Rate"] ?? null,
        startDate: f["Effective Start Date"] || "",
        endDate:   f["Effective End Date"] || ""
      };
    });
  return resp(200, { ok: true, rates });
}

async function handleUpdateJobBillableRate(body) {
  const { jobId, rateId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  // Jobs."Labor Billable Rates" field ID = fldcCGetfLtQW2nhm (multipleRecordLinks)
  const fields = {};
  fields["fldcCGetfLtQW2nhm"] = rateId ? [String(rateId)] : [];
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

// ── SAVE INVOICE RECORD ──────────────────────────────────────────────────
// Path B semantics:
//   - `totalAmount` (from the frontend's calcInvTotal / sum of invLines) is
//     the authoritative dollar figure. It is written to the new
//     "Snapshot Total" field (fldFyaBpK8nlnUbvf).
//   - For Contract invoices, `percentToBill` is DERIVED server-side from
//     totalAmount / expectedRevenue — the stale user-input percent is ignored
//     when totalAmount is present.
//   - After deploy, change the "Total Contract Billed" rollup on Jobs to sum
//     "Snapshot Total" (instead of "Contract Invoice Amount") so the
//     Previously-Billed / Contract-Remaining figures are based on real saved
//     dollars, not the percent-times-expected-revenue formula.
async function handleSaveInvoice(body) {
  const {
    invoiceId,            // NEW: if present, PATCH existing record instead of POSTing a new one
    jobId, invoiceDate, billingMode,
    percentToBill,        // legacy — only used if totalAmount not provided
    totalAmount,          // NEW: authoritative amount from the line-item sum
    expectedRevenue,      // NEW: the frontend's view of expected rev for percent derivation
    notes, invoiceNumber, snapshot, invoiceStage
  } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const fields = {};
  fields["fld1fmEklDw6y9hS2"] = [jobId];                            // Job (linked)
  // Only force "Sent" status on the initial create. When editing an existing
  // invoice we leave the status alone — the user might be tweaking a Paid or
  // Disputed invoice and we don't want to silently flip it back to Sent.
  if (!invoiceId) fields["fldXcHqj8xqmOWeLH"] = "Sent";              // Invoice Status (create only)
  if (invoiceDate) fields["fldAEjySdXkUke1Cv"] = invoiceDate;       // Invoice Date
  if (notes)       fields["fldLQrPKHWLrHLOA2"] = notes;             // Invoice Notes
  if (invoiceNumber !== undefined && invoiceNumber !== null && invoiceNumber !== "") {
    const n = Number(invoiceNumber);
    if (!isNaN(n)) fields["fld7FxS299iYDzMa8"] = n;                 // Invoice Display #
  }
  if (snapshot) {
    // Store as JSON string — used by reprint to rebuild identical PDF
    fields["fldJT0EqxsYPUQOg1"] = typeof snapshot === "string" ? snapshot : JSON.stringify(snapshot);
  }
  if (invoiceStage) {
    fields["fldzvSMeApOZs75Pa"] = String(invoiceStage);              // Invoice Stage
  }

  // Snapshot Total — authoritative dollar figure written from line-item sum.
  const totalNum = (totalAmount !== undefined && totalAmount !== null && totalAmount !== "")
    ? Number(totalAmount)
    : null;
  if (totalNum !== null && !isNaN(totalNum)) {
    fields["fldFyaBpK8nlnUbvf"] = totalNum;                          // Snapshot Total
  }

  if (String(billingMode).toLowerCase() === "contract") {
    // Contract invoice — bill by percentage of Expected Revenue.
    fields["fldljpi4PpNPIfI27"] = "Contract % Progress";             // Billing Mode
    fields["fldC4loXTBzC2UKGt"] = "Contract";                        // Invoice Type
    fields["fldejNlo5R194TGMs"] = false;                             // Auto Allocate OFF
    fields["fldRcvTVQ7naHG19t"] = 0;                                 // zero manual labor
    fields["fldcbhc1z8nEftVeY"] = 0;                                 // zero manual material

    // Derive percentToBill from the authoritative totalAmount when both
    // totalAmount and expectedRevenue are known. This keeps the existing
    // "Contract Invoice Amount" formula consistent with Snapshot Total.
    let derivedPct = null; // as a 0..1 fraction for Airtable's percent field
    const erNum = (expectedRevenue !== undefined && expectedRevenue !== null && expectedRevenue !== "")
      ? Number(expectedRevenue) : null;
    if (totalNum !== null && erNum !== null && erNum > 0 && !isNaN(totalNum)) {
      derivedPct = Math.round((totalNum / erNum) * 1e6) / 1e6; // clamp precision
    } else if (percentToBill !== undefined && percentToBill !== null && percentToBill !== "") {
      // Fall back to caller-provided percent (legacy path)
      derivedPct = Number(percentToBill) / 100;
    }
    if (derivedPct !== null && !isNaN(derivedPct)) {
      fields["fldiaGIu4ZzKLz6ra"] = derivedPct;                      // Percent to Bill
    }
  } else {
    // T&M invoice — existing behavior, lets Airtable rollup labor & material.
    fields["fldljpi4PpNPIfI27"] = "T&M Final";                       // Billing Mode
    fields["fldC4loXTBzC2UKGt"] = "Time & Material";                 // Invoice Type
    fields["fldejNlo5R194TGMs"] = true;                              // Auto Allocate ON
    fields["fldRcvTVQ7naHG19t"] = 0;                                 // zero manual labor
    fields["fldcbhc1z8nEftVeY"] = 0;                                 // zero manual material
  }

  // PATCH the existing invoice when invoiceId is provided (edit mode); else
  // POST a new record (create mode). Both paths use typecast for new option
  // values that might appear on stage/status singleSelects.
  let data;
  if (invoiceId) {
    data = await atFetch(`${encodeURIComponent("Invoices")}/${invoiceId}`, {
      method: "PATCH",
      body: JSON.stringify({ fields, typecast: true })
    });
  } else {
    data = await atFetch(`${encodeURIComponent("Invoices")}`, {
      method: "POST",
      body: JSON.stringify({ fields, typecast: true })
    });
  }
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: data.id, updated: !!invoiceId });
}

// ── ADD GENERATOR SERVICE RECORD (quick-log from Generator tab) ─────────
// Keep it lightweight: no truck/parts inventory, no labor billing, just the
// observable facts a tech in the field would log on a service stop.
async function handleAddGeneratorService(body) {
  const {
    generatorId, jobId,
    serviceDate, serviceType, technician,
    servicePlanVisit,
    oilChanged, oilFilterChanged, airFilterChanged, sparkPlugsChanged,
    batteryTested, batteryReplaced, loadTestPerformed,
    firmwareChecked, exerciseChecked,
    troubleCodesFound, workNotes, partsUsed,
    laborHours, generatorHours
  } = body || {};

  if (!generatorId) return resp(400, { ok: false, error: "Missing generatorId." });
  if (!serviceDate) return resp(400, { ok: false, error: "Missing serviceDate." });

  // Build by name — typecast: true handles single-select option creation.
  const fields = {};
  fields["Generator"]   = [generatorId];
  if (jobId)            fields["Job"] = [jobId];
  fields["Service Date"] = serviceDate;
  if (serviceType)      fields["Service Type"] = serviceType;
  if (technician)       fields["Technician"]   = technician;

  // Bools — only write if explicitly passed
  const setBool = (key, v) => { if (v !== undefined) fields[key] = v === true; };
  setBool("Service Plan Visit", servicePlanVisit);
  setBool("Oil Changed",         oilChanged);
  setBool("Oil Filter Changed",  oilFilterChanged);
  setBool("Air Filter Changed",  airFilterChanged);
  setBool("Spark Plugs Changed", sparkPlugsChanged);
  setBool("Battery Tested",      batteryTested);
  setBool("Battery Replaced",    batteryReplaced);
  setBool("Load Test Performed", loadTestPerformed);
  setBool("Firmware / Settings Checked", firmwareChecked);
  setBool("Exercise Checked",    exerciseChecked);

  if (troubleCodesFound) fields["Trouble Codes Found"] = String(troubleCodesFound);
  if (workNotes)         fields["Work Performed Notes"] = String(workNotes);
  if (partsUsed)         fields["Parts Used"]           = String(partsUsed);
  if (laborHours !== undefined && laborHours !== null && laborHours !== "")
    fields["Labor Hours"] = Number(laborHours);
  if (generatorHours !== undefined && generatorHours !== null && generatorHours !== "")
    fields["Generator Hours @ Service"] = Number(generatorHours);

  const data = await atFetch(`${encodeURIComponent("Generator Service")}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: data.id });
}

// ── SET INVOICE STATUS ───────────────────────────────────────────────────
// Generalized status setter (replaces the old markInvoicePaid). Accepts any
// option name; thanks to typecast: true, new options like "Disputed" get
// auto-added to the singleSelect on first use.
async function handleSetInvoiceStatus(body) {
  const { invoiceId, status } = body || {};
  if (!invoiceId) return resp(400, { ok: false, error: "Missing invoiceId." });
  if (!status)    return resp(400, { ok: false, error: "Missing status." });
  const fields = { "fldXcHqj8xqmOWeLH": status };  // Invoice Status
  const data = await atFetch(`${encodeURIComponent("Invoices")}/${invoiceId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true })
  });
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: data.id });
}

// Backward-compat alias — old "markInvoicePaid" callers still work.
async function handleMarkInvoicePaid(body) {
  return handleSetInvoiceStatus({ invoiceId: body?.invoiceId, status: body?.status || "Paid" });
}

// ── GET NEXT INVOICE NUMBER ──────────────────────────────────────────────
async function handleGetNextInvoiceNumber() {
  // Find max "Invoice Display #" across Invoices; start at 1633 if none exist
  const START_AT = 1633;
  let max = 0;
  let offset = undefined;
  // Paginate to cover all records (defensive for large tables)
  do {
    const qs = "?fields%5B%5D=" + encodeURIComponent("Invoice Display #")
             + (offset ? "&offset=" + encodeURIComponent(offset) : "");
    const page = await atFetch(`${encodeURIComponent("Invoices")}${qs}`);
    if (page.error) return resp(400, { ok: false, error: page.error });
    (page.records || []).forEach(r => {
      const n = Number(r?.fields?.["Invoice Display #"]);
      if (!isNaN(n) && n > max) max = n;
    });
    offset = page.offset;
  } while (offset);

  const next = Math.max(max + 1, START_AT);
  return resp(200, { ok: true, nextNumber: next });
}

// ── LIST PAST INVOICES FOR A JOB ─────────────────────────────────────────
// ── ALL INVOICES (cross-job) — backs the global "💰 Invoices" modal ─────
// Same data shape as handleGetJobInvoices but enriched with job name +
// contractor + customer info pulled from the Jobs table. Client filters
// (date range, contractor, customer search) happen in the browser since the
// dataset is small (a few hundred invoices at most).
// ── SCHEDULE ENTRIES ────────────────────────────────────────────────────
// Crew scheduling. One Schedule Entry = a job + date range + assigned crew.
// Multiple entries per job are expected (rough-in week, then trim-out week
// after a gap). Source-of-truth for the calendar in nee-hub.
// NOTE: We use field NAMES here, not field IDs. Airtable's REST API returns
// records keyed by field name unless you pass returnFieldsByFieldId=true.
const SCHED_F = {
  title:     "Title",
  job:       "Job",
  startDate: "Start Date",
  endDate:   "End Date",
  crew:      "Crew",
  notes:     "Notes",
  type:      "Entry Type"
};

async function handleGetScheduleEntries(params) {
  // Optional date-range filter: ?since=YYYY-MM-DD & ?until=YYYY-MM-DD.
  // Filter is "any overlap" — an entry shows if its [start, end] range
  // overlaps the requested window. Job filter: ?jobId=recXXX.
  const since = params?.since || "";
  const until = params?.until || "";
  const jobId = params?.jobId || "";

  const records = await fetchAll(TABLES.scheduleEntries);
  const jobs = await fetchAll(TABLES.jobs);
  const employees = await fetchAll(TABLES.employees);
  const jobById = {};
  jobs.forEach(j => {
    const f = j.fields || {};
    jobById[j.id] = {
      id: j.id,
      name:       g(f, F.job.name)       || "",
      contractor: g(f, F.job.contractor) || "",
      status:     g(f, F.job.status)     || ""
    };
  });
  const empById = {};
  employees.forEach(e => {
    const f = e.fields || {};
    empById[e.id] = { id: e.id, name: f[F.emp.name] || "" };
  });

  const entries = records.map(r => {
    const f = r.fields || {};
    const jobLink = Array.isArray(f[SCHED_F.job]) ? f[SCHED_F.job][0] : null;
    const job = jobLink ? jobById[jobLink] : null;
    const crewIds = Array.isArray(f[SCHED_F.crew]) ? f[SCHED_F.crew] : [];
    return {
      id:         r.id,
      title:      f[SCHED_F.title]     || "",
      type:       f[SCHED_F.type]      || "Job",
      jobId:      jobLink              || "",
      jobName:    job?.name            || "",
      contractor: job?.contractor      || "",
      jobStatus:  job?.status          || "",
      startDate:  f[SCHED_F.startDate] || "",
      endDate:    f[SCHED_F.endDate]   || "",
      crewIds,
      crew:       crewIds.map(id => empById[id]?.name || "").filter(Boolean),
      notes:      f[SCHED_F.notes]     || ""
    };
  });

  // Apply optional filters
  let filtered = entries;
  if (jobId) filtered = filtered.filter(e => e.jobId === jobId);
  if (since || until) {
    filtered = filtered.filter(e => {
      // Empty entries (no dates yet) — keep
      if (!e.startDate && !e.endDate) return true;
      const s = e.startDate || e.endDate;
      const ed = e.endDate || e.startDate;
      // Overlap test: entry overlaps the window if entry.start <= until
      // AND entry.end >= since
      if (since && ed && ed < since) return false;
      if (until && s  && s  > until) return false;
      return true;
    });
  }

  // Sort by start date ascending so the calendar renders chronologically
  filtered.sort((a, b) => (a.startDate || "").localeCompare(b.startDate || ""));

  return resp(200, { ok: true, entries: filtered });
}

async function handleAddScheduleEntry(body) {
  const { jobId, startDate, endDate, crewIds, notes, title, type } = body || {};
  const entryType = type || "Job";
  if (entryType === "Job" && !jobId) return resp(400, { ok: false, error: "Missing jobId for Job entry." });
  if (!startDate) return resp(400, { ok: false, error: "Missing startDate." });
  if (!endDate)   return resp(400, { ok: false, error: "Missing endDate." });

  const fields = {};
  fields[SCHED_F.type] = entryType;
  if (jobId)              fields[SCHED_F.job]       = [jobId];
  fields[SCHED_F.startDate] = startDate;
  fields[SCHED_F.endDate]   = endDate;
  if (Array.isArray(crewIds) && crewIds.length) fields[SCHED_F.crew] = crewIds;
  if (notes) fields[SCHED_F.notes] = String(notes);
  if (title) fields[SCHED_F.title] = String(title);

  const data = await atFetch(`${encodeURIComponent("Schedule Entries")}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: data.id });
}

async function handleUpdateScheduleEntry(body) {
  const { entryId, jobId, startDate, endDate, crewIds, notes, title, type } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });
  const fields = {};
  if (type        !== undefined) fields[SCHED_F.type]      = type || "Job";
  if (jobId       !== undefined) fields[SCHED_F.job]       = jobId ? [jobId] : [];
  if (startDate   !== undefined) fields[SCHED_F.startDate] = startDate || null;
  if (endDate     !== undefined) fields[SCHED_F.endDate]   = endDate   || null;
  if (crewIds     !== undefined) fields[SCHED_F.crew]      = Array.isArray(crewIds) ? crewIds : [];
  if (notes       !== undefined) fields[SCHED_F.notes]     = String(notes || "");
  if (title       !== undefined) fields[SCHED_F.title]     = String(title || "");

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await atFetch(`${encodeURIComponent("Schedule Entries")}/${entryId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true })
  });
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: data.id });
}

async function handleDeleteScheduleEntry(body) {
  const { entryId } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });
  const data = await atFetch(`${encodeURIComponent("Schedule Entries")}/${entryId}`, {
    method: "DELETE"
  });
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, deletedId: entryId });
}

// Helper: list active employees with role exposed, for the crew picker.
// (Filters by Active checkbox; frontend filters further by role to exclude
// office + viewer.) Used by the schedule modal's crew dropdown.
async function handleListEmployeesForScheduling() {
  const records = await fetchAll(TABLES.employees);
  const employees = records
    .filter(r => gBool(r.fields || {}, F.emp.active))
    .map(r => {
      const f = r.fields || {};
      return {
        id:   r.id,
        name: f[F.emp.name] || "",
        role: normalize(f[F.emp.role]) || "employee"
      };
    });
  // Sort by name for a stable picker
  employees.sort((a, b) => a.name.localeCompare(b.name));
  return resp(200, { ok: true, employees });
}

async function handleGetAllInvoices() {
  // 1. Pull all invoices, paginated
  const allInvoices = [];
  let offset = undefined;
  do {
    const qs = (offset ? "?offset=" + encodeURIComponent(offset) : "");
    const page = await atFetch(`${encodeURIComponent("Invoices")}${qs}`);
    if (page.error) return resp(400, { ok: false, error: page.error });
    allInvoices.push(...(page.records || []));
    offset = page.offset;
  } while (offset);

  // 2. Pull all jobs once and build a fast lookup by id. Re-using fetchAll
  // here so we get the same archived-status filter behavior, but we don't
  // actually filter by status here — the user might want to see invoices
  // from a now-archived job. So fetch raw.
  const jobRecs = await fetchAll(TABLES.jobs);
  const jobById = {};
  jobRecs.forEach(r => {
    const f = r.fields || {};
    const customerName = [g(f, F.job.customerFirstName) || "", g(f, F.job.customerLastName) || ""]
      .filter(Boolean).join(" ").trim();
    jobById[r.id] = {
      id:         r.id,
      name:       g(f, F.job.name)       || "",
      contractor: g(f, F.job.contractor) || "",
      customer:   customerName,
      address:    g(f, F.job.address)    || "",
      status:     g(f, F.job.status)     || ""
    };
  });

  // 3. Map invoices into the same shape as handleGetJobInvoices, plus
  // job/contractor/customer fields for filtering & display.
  const invoices = allInvoices.map(r => {
    const f = r.fields || {};
    const jobLink = Array.isArray(f["Job"]) ? f["Job"][0] : null;
    const job = jobLink ? jobById[jobLink] : null;
    return {
      id:              r.id,
      displayNumber:   f["Invoice Display #"] || null,
      invoiceNumber:   f["Invoice Number"]    || "",
      date:            f["Invoice Date"]      || "",
      status:          f["Invoice Status"]    || "",
      billingMode:     f["Billing Mode"]      || "",
      invoiceType:     f["Invoice Type"]      || "",
      total:           Number(f["Invoice Total"] || 0),
      snapshotTotal:   Number(f["Snapshot Total"] || 0),
      contractAmount:  Number(f["Contract Invoice Amount"] || 0),
      notes:           f["Invoice Notes"]     || "",
      snapshot:        f["Invoice Snapshot"]  || "",
      stage:           f["Invoice Stage"]     || "",
      // Joined from job
      jobId:           job?.id         || jobLink || "",
      jobName:         job?.name       || "",
      contractor:      job?.contractor || "",
      customer:        job?.customer   || "",
      address:         job?.address    || "",
      jobStatus:       job?.status     || ""
    };
  });

  // 4. Sort by Invoice Date descending (newest first) — frontend can re-sort
  invoices.sort((a, b) => (b.date || "").localeCompare(a.date || ""));

  return resp(200, { ok: true, invoices });
}

async function handleGetJobInvoices(body) {
  const { jobId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // Fetch all invoices, paginated, and filter client-side by job link.
  // filterByFormula on linked records is unreliable (ARRAYJOIN returns primary
  // field values, not record IDs), so we filter in-memory instead.
  const all = [];
  let offset = undefined;
  do {
    const qs = (offset ? "?offset=" + encodeURIComponent(offset) : "");
    const page = await atFetch(`${encodeURIComponent("Invoices")}${qs}`);
    if (page.error) return resp(400, { ok: false, error: page.error });
    all.push(...(page.records || []));
    offset = page.offset;
  } while (offset);

  // Keep only invoices whose Job link array contains this jobId
  const filtered = all.filter(r => {
    const jobArr = r.fields?.["Job"];
    return Array.isArray(jobArr) && jobArr.indexOf(jobId) !== -1;
  });

  // Sort by Invoice Date descending (newest first)
  filtered.sort((a, b) => {
    const da = a.fields?.["Invoice Date"] || "";
    const db = b.fields?.["Invoice Date"] || "";
    return db.localeCompare(da);
  });

  const invoices = filtered.map(r => {
    const f = r.fields || {};
    return {
      id:              r.id,
      displayNumber:   f["Invoice Display #"] || null,
      invoiceNumber:   f["Invoice Number"]    || "",
      date:            f["Invoice Date"]      || "",
      status:          f["Invoice Status"]    || "",
      billingMode:     f["Billing Mode"]      || "",
      invoiceType:     f["Invoice Type"]      || "",
      total:           Number(f["Invoice Total"] || 0),
      snapshotTotal:   Number(f["Snapshot Total"] || 0),   // authoritative saved total (Path B)
      percentToBill:   f["Percent to Bill"]   || null,
      contractAmount:  Number(f["Contract Invoice Amount"] || 0),
      notes:           f["Invoice Notes"]     || "",
      snapshot:        f["Invoice Snapshot"]  || "",
      stage:           f["Invoice Stage"]     || ""
    };
  });
  return resp(200, { ok: true, invoices });
}

// ── PCLOUD UPLOAD ─────────────────────────────────────────────────────────────
async function getPCloudToken() {
  const email    = process.env.PCLOUD_EMAIL;
  const password = process.env.PCLOUD_PASSWORD;
  if (!email || !password) throw new Error("PCLOUD_EMAIL or PCLOUD_PASSWORD env vars not set.");

  // pCloud direct auth via POST with digest/username+password
  // Try eapi (US region) first, fall back to api (EU)
  const endpoints = ["https://eapi.pcloud.com/userinfo", "https://api.pcloud.com/userinfo"];
  
  for (const endpoint of endpoints) {
    const params = new URLSearchParams({
      getauth: "1",
      logout: "1", 
      username: email,
      password: password,
      authexpire: "0"  // permanent token
    });
    const res  = await fetch(`${endpoint}?${params.toString()}`);
    const json = await res.json();
    if (json.result === 0 && json.token) return json.token;
    if (json.result === 0 && json.auth) return json.auth;
    // If result 2000 = invalid credentials, throw immediately
    if (json.result === 2000) throw new Error("pCloud: Invalid email or password.");
  }
  throw new Error("pCloud auth failed on all endpoints.");
}

async function handleUploadToPCloud(body) {
  const { folderId, filename, pdfBase64 } = body || {};
  if (!folderId || !filename || !pdfBase64) {
    return resp(400, { ok: false, error: "Missing folderId, filename, or pdfBase64." });
  }
  try {
    const token     = await getPCloudToken();
    const pdfBuffer = Buffer.from(pdfBase64, "base64");
    const boundary  = "----NEEBoundary" + Date.now();
    const CRLF      = "\r\n";

    const headerParts = [
      `--${boundary}`,
      `Content-Disposition: form-data; name="folderid"`,
      "", String(folderId),
      `--${boundary}`,
      `Content-Disposition: form-data; name="filename"`,
      "", filename,
      `--${boundary}`,
      `Content-Disposition: form-data; name="file"; filename="${filename}"`,
      "Content-Type: application/pdf",
      "", ""
    ].join(CRLF);

    const footer    = CRLF + `--${boundary}--` + CRLF;
    const headerBuf = Buffer.from(headerParts, "utf8");
    const footerBuf = Buffer.from(footer, "utf8");
    const multipart = Buffer.concat([headerBuf, pdfBuffer, footerBuf]);

    const res = await fetch(`https://api.pcloud.com/uploadfile?auth=${encodeURIComponent(token)}`, {
      method: "POST",
      headers: {
        "Content-Type": `multipart/form-data; boundary=${boundary}`,
        "Content-Length": String(multipart.length)
      },
      body: multipart
    });

    const json = await res.json().catch(() => ({}));
    if (json.result !== 0) {
      return resp(400, { ok: false, error: `pCloud upload error: ${json.error || JSON.stringify(json)}` });
    }
    return resp(200, { ok: true, fileId: json.fileids?.[0] });
  } catch(err) {
    return resp(500, { ok: false, error: err.message });
  }
}

async function handleUpdateJobNotes(body) {
  const { jobId, notes } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldAuZAW19iYPBPxP": notes || "" } }) });
  return resp(200, { ok: true, updatedId: data.id });
}

// Single-call update for the Project Info edit form. PATCHes any subset
// of the seven editable fields in one round-trip; missing keys are left
// untouched server-side. Empty strings clear the field (e.g. "" on
// customerEmail wipes the address) — that's intentional so the edit
// form supports both updating and clearing.
async function handleUpdateJobInfo(body) {
  const { jobId, customerStreet, customerCity, customerState, customerZip, customerPhone, customerEmail, notes } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const fields = {};
  if (customerStreet !== undefined) fields["fldFBJrw64SYC1WdB"] = customerStreet || "";
  if (customerCity   !== undefined) fields["fld46JMp1z6E2DhJt"] = customerCity   || "";
  if (customerState  !== undefined) fields["fldktee97zx5QUPmd"] = customerState  || "";
  if (customerZip    !== undefined) fields["fldMooJ88usuHF6RH"] = customerZip    || "";
  if (customerPhone  !== undefined) fields["fldBf6EC5EQXsPFAQ"] = customerPhone  || "";
  if (customerEmail  !== undefined) fields["fldzGgNmRlSxwpSMX"] = customerEmail  || "";
  if (notes          !== undefined) fields["fldAuZAW19iYPBPxP"] = notes          || "";

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields, typecast: true })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

// POSTs a new Jobs record from the in-app New Project modal. Job Name is
// the only required field. Status defaults to "New Lead" so the new job
// lands in the right sidebar group. Tax Status and Billing Method default
// per spec. Optional fields are sent only when non-blank to avoid
// stomping on Airtable defaults with empty strings. Contractor is
// omitted entirely when Billing Method is Direct Customer.
//
// Returns the new record run through mapJob() so the frontend can splice
// it into state.jobs and selectJob() it without a full list refetch.
async function handleCreateJob(body) {
  const {
    jobName, jobType, taxStatus, billingMethod, contractor,
    customerFirstName, customerLastName,
    customerStreet, customerCity, customerState, customerZip,
    customerPhone, customerEmail, notes
  } = body || {};

  const trimmedName = String(jobName || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Job Name is required." });

  const fields = {};
  fields["Job Name"] = trimmedName;
  fields["Job Status"]     = "New Lead";
  fields["Tax Status"]     = taxStatus     || "Taxable";
  fields["Billing Method"] = billingMethod || "Direct Customer";

  if (jobType && String(jobType).trim()) fields["Job Type"] = String(jobType).trim();
  if ((billingMethod === "Contractor") && contractor && String(contractor).trim()) {
    fields["Contractor (Intake)"] = String(contractor).trim();
  }

  if (customerFirstName && String(customerFirstName).trim()) fields["Customer 1st Name (Intake)"]      = String(customerFirstName).trim();
  if (customerLastName  && String(customerLastName ).trim()) fields["Customer Last Name (Intake)"]     = String(customerLastName ).trim();
  if (customerStreet    && String(customerStreet   ).trim()) fields["Job Site Street Address (Intake)"]= String(customerStreet   ).trim();
  if (customerCity      && String(customerCity     ).trim()) fields["Job Site City (Intake)"]         = String(customerCity     ).trim();
  if (customerState     && String(customerState    ).trim()) fields["Job Site State (Intake)"]        = String(customerState    ).trim().toUpperCase();
  if (customerZip       && String(customerZip      ).trim()) fields["Job Site Zip Code (Intake)"]     = String(customerZip      ).trim();
  if (customerPhone     && String(customerPhone    ).trim()) fields["Customer Phone (Intake)"]        = String(customerPhone    ).trim();
  if (customerEmail     && String(customerEmail    ).trim()) fields["Customer Email (Intake)"]        = String(customerEmail    ).trim();
  if (notes             && String(notes            ).trim()) fields["Notes"]                          = String(notes);

  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
  return resp(200, { ok: true, job: mapJob(data) });
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "jobs")               return await handleJobs();
      if (action === "jobById")            return await handleJobById(params);
      if (action === "generator")          return await handleGenerator(params);
      if (action === "expenses")           return await handleExpenses(params);
      if (action === "timeEntries")        return await handleTimeEntries(params);
      if (action === "payrollEntries")     return await handlePayrollEntries(params);
      if (action === "scissorLifts")       return await handleScissorLifts();
      if (action === "scissorLiftsByJob")  return await handleScissorLiftsByJob(params);
      if (action === "jobInspections")     return await handleJobInspections(params);
      if (action === "jobEstimates")       return await handleJobEstimates(params);
      if (action === "estimateTemplates")  return await handleEstimateTemplates(params);
      if (action === "sentEstimatePDFs")   return await handleSentEstimatePDFs(params);
      if (action === "allInvoices")        return await handleGetAllInvoices();
      if (action === "scheduleEntries")    return await handleGetScheduleEntries(params);
      if (action === "schedulingCrew")     return await handleListEmployeesForScheduling();
      if (action === "fleetVehicles")      return await handleFleetVehicles();
      if (action === "fleetServiceHistory")return await handleFleetServiceHistory(params);
      if (action === "vendors")            return await handleVendors();
      if (action === "companies")          return await handleCompanies();
      if (action === "laborBillableRates") return await handleLaborBillableRates();
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login")                return await handleLogin(body);
      if (body.action === "updateJobStatus")      return await handleUpdateJobStatus(body);
      if (body.action === "updatePowerCo")        return await handleUpdatePowerCo(body);
      if (body.action === "updateTimeEntry")      return await handleUpdateTimeEntry(body);
      if (body.action === "updateTimeEntryPayroll") return await handleUpdateTimeEntryPayroll(body);
      if (body.action === "createTimeEntry")      return await handleCreateTimeEntry(body);
      if (body.action === "deleteTimeEntry")      return await handleDeleteTimeEntry(body);
      if (body.action === "deleteExpense")        return await handleDeleteExpense(body);
      if (body.action === "approveExpense")       return await handleApproveExpense(body);
      if (body.action === "updateScissorLift")    return await handleUpdateScissorLift(body);
      if (body.action === "createInspection")     return await handleCreateInspection(body);
      if (body.action === "updateEstimate")       return await handleUpdateEstimate(body);
      if (body.action === "updateEstimateStatus") return await handleUpdateEstimateStatus(body);
      if (body.action === "getNextEstimateNumber") return await handleGetNextEstimateNumber();
      if (body.action === "saveEstimate")         return await handleSaveEstimate(body);
      if (body.action === "createJobEstimate")    return await handleCreateJobEstimate(body);
      if (body.action === "updateFleetVehicle")   return await handleUpdateFleetVehicle(body);
      if (body.action === "logMileage")           return await handleLogMileage(body);
      if (body.action === "updateJobBillableRate") return await handleUpdateJobBillableRate(body);
      if (body.action === "addFleetService")      return await handleAddFleetService(body);
      if (body.action === "updateFleetService")   return await handleUpdateFleetService(body);
      if (body.action === "deleteFleetService")   return await handleDeleteFleetService(body);
      if (body.action === "startServiceCall")     return await handleStartServiceCall(body);
      if (body.action === "completeServiceCall")  return await handleCompleteServiceCall(body);
      if (body.action === "saveInvoice")          return await handleSaveInvoice(body);
      if (body.action === "markInvoicePaid")      return await handleMarkInvoicePaid(body);
      if (body.action === "setInvoiceStatus")     return await handleSetInvoiceStatus(body);
      if (body.action === "addGeneratorService")  return await handleAddGeneratorService(body);
      if (body.action === "addScheduleEntry")     return await handleAddScheduleEntry(body);
      if (body.action === "updateScheduleEntry")  return await handleUpdateScheduleEntry(body);
      if (body.action === "deleteScheduleEntry")  return await handleDeleteScheduleEntry(body);
      if (body.action === "getNextInvoiceNumber") return await handleGetNextInvoiceNumber();
      if (body.action === "getJobInvoices")       return await handleGetJobInvoices(body);
      if (body.action === "uploadToPCloud")       return await handleUploadToPCloud(body);
      if (body.action === "updateJobNotes")       return await handleUpdateJobNotes(body);
      if (body.action === "updateJobInfo")        return await handleUpdateJobInfo(body);
      if (body.action === "createJob")            return await handleCreateJob(body);
      if (body.action === "updateInspection")     return await handleUpdateInspection(body);
      if (body.action === "calculateMileage")     return await handleCalculateMileage(body);
      if (body.action === "addLiftExpense")       return await handleAddLiftExpense(body);
      if (body.action === "addGeneralExpense")    return await handleAddGeneralExpense(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Proxy Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

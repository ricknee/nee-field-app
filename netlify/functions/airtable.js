// netlify/functions/airtable.js
// Northeastern Electric Field App — Netlify Proxy
// Reads env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID

const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;
const API_ROOT = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

const TABLES = {
  employees:           "Employees",
  jobs:                "Jobs",
  generators:          "Generators",
  generatorService:    "Generator Service",
  warrantyTemplates:   "Warranty Templates",
  warranties:          "Warranties",
  timeEntries:         "Time Entries",
  scissorLifts:        "Scissor Lifts",
  scheduleEntries:     "Schedule Entries",
  inspectionAgencies:  "tblZrG9V7C3lVsXNT",
  inspectionContacts:  "tblnewJMKDPfczRRx",
  powerCompanies:      "tblgxHavdZybnuMhM",
  powerContacts:       "tblvouoPMTYh27FGT",
  contacts:            "tbl7vZpySDNfZX9Sq",
  laborAllocations:    "tblHyJWVAcBczn3hn",
  materialAllocations: "tblMoKg7txcfYczQQ"
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
    contractorLink:          "Contractor",
    powerCompanyLink:        "Power Companies",
    generatorInstalled:      "Generator Installed",
    powerCompanyName:        "Power Company – Name (lookup)",
    powerCompanyContact:     "Power Company – Primary Contact (lookup)",
    powerContactLink:        "Power Company Contacts",
    powerCompanyCellPhone:   "Power Company – Cell Phone (lookup)",
    powerCompanyOfficePhone: "Power Company – Office Phone (lookup)",
    powerCompanyEmail:       "Power Company – Email (lookup)",
    aicNumber:               "AIC Number",
    tempWorkOrder:           "Temporary Work Order #",
    permWorkOrder:           "Permanent Work Order #",
    meterNumber:             "Meter Number",
    permitNumber:            "Permit Number",
    inspectionAgency:        "Inspection Agency Name (from Inspection Agency)",
    inspectionAgencyPhone:   "Inspection Agency Phone #",
    inspectionAgencyEmail:   "Inspection Agency Email Address",
    inspectorPhone:          "Inspector Phone",
    inspectorEmail:          "Inspector Email",
    inspectionSchedulingLink:"Inspection Scheduling Link",
    // Was "Inspection Name (from Inspection Contacts)" — that field name does
    // not exist on Jobs. The real lookup is "Inspector Name". The stale name
    // meant this key returned "" for every job, so the Inspections-tab read-
    // only "Inspection Contacts" cell silently rendered "—" everywhere.
    inspectionContacts:      "Inspector Name (from Inspection Contacts)",
    jobInspections:          "Inspection Name (from Job Inspections)",
    wireLink:                "Wire (Mobile) or THHN (Mobile)",
    pipeLink:                "Add Pipe (Mobile)",
    addPhotosLink:           "Add Photos (Mobile)",
    viewPhotosLink:          "View pCloud Photos",
    trelloCardId:               "Trello Card ID",
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
    // Filtered rollups: include only Status = Sent / Approved / Archived-
    // Completed (Rejected excluded). Naming gotcha: in Airtable, the
    // "Projected Estimated X (from Job Estimates)" fields are the UNFILTERED
    // twins; "Estimated X Rollup (from Job Estimates)" / "Expected Revenue"
    // are the FILTERED ones. Inverted from intuition — trust the names here.
    expectedRevenueAllStatus:          "Expected Revenue",
    projectedEstimatedTotalCost:       "Projected Estimated Total Cost",
    projectedEstimatedLaborHours:      "Estimated Labor Hours Rollup (from Job Estimates)",
    projectedEstimatedMaterialCost:    "Estimated Material Cost Rollup (from Job Estimates)",
    projectedEstimatedLaborCost:       "Estimated Labor Cost Rollup (from Job Estimates)",
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
    serviceRecordId:"Service Record ID",serviceNumber:"Service Number",generator:"Generator",customer:"Customer",job:"Job",
    serviceDate:"Service Date",serviceType:"Service Type",technician:"Technician",technicianName:"Technician Name",
    servicePlanVisit:"Service Plan Visit",oilChanged:"Oil Changed",oilFilterChanged:"Oil Filter Changed",
    airFilterChanged:"Air Filter Changed",sparkPlugsChanged:"Spark Plugs Changed",
    batteryTested:"Battery Tested",batteryReplaced:"Battery Replaced",
    loadTestPerformed:"Load Test Performed",firmwareChecked:"Firmware / Settings Checked",
    exerciseChecked:"Exercise Checked",troubleCodesFound:"Trouble Codes Found",
    workNotes:"Work Performed Notes",partsUsed:"Parts Used",laborHours:"Labor Hours",
    generatorHours:"Generator Hours @ Service"
  },
  warrantyTemplate: {
    name:           "Template Name",
    brand:          "Brand",
    model:          "Model",
    warrantyType:   "Warranty Type",
    durationMonths: "Duration Months",
    notes:          "Notes",
    active:         "Active"
  },
  warranty: {
    name:                "Warranty Name",
    generator:           "Generator",
    warrantyType:        "Warranty Type",
    startDate:           "Start Date",
    endDate:             "End Date",
    durationMonths:      "Duration Months",
    source:              "Source",
    voided:              "Voided",
    voidedReason:        "Voided Reason",
    notes:               "Notes",
    createdFromTemplate: "Created From Template"
  },
  contact: {
    firstName:    "First Name",
    lastName:     "Last Name",
    primaryPhone: "Primary Phone",
    primaryEmail: "Primary Email",
    company:      "Company",
    active:       "Active",
    role:         "Role",
    street:       "Street",
    city:         "City",
    state:        "State",
    zip:          "Zip"
  },
  // Inspection Agencies table — field NAMES (for reading POST/PATCH responses).
  // Write sites use field IDs inline (drift-resistance — see "+ Add new agency"
  // handler). Active must be set to TRUE on create — that's the trigger for the
  // Make.com Google Contacts sync. Never write the Google Contact ID / Sync
  // Status / Last Synced At / Needs Sync to Google fields — those are sync-owned.
  agency: {
    name:           "Inspection Agency Name",
    phone:          "Agency Phone",
    email:          "Agency Email",
    schedulingLink: "Scheduling Link",
    notes:          "Notes",
    active:         "Active"
  },
  // Inspection Contacts table — field NAMES (same read-vs-write split as agency).
  // Same Active=TRUE-on-create rule. Inspector Name is a formula (First + Last)
  // — read-only, never write.
  inspector: {
    nameFormula: "Inspector Name",
    firstName:   "First Name",
    lastName:    "Last Name",
    phone:       "Phone",
    email:       "Email",
    agency:      "Inspection Agency",  // linked → Inspection Agencies
    active:      "Active"
  },
  // Power Companies table — field NAMES (write sites use field IDs inline).
  // Active must be set to TRUE on create — defaults to checked in Airtable, but
  // the create handler should set it explicitly for safety. No Make.com sync
  // trigger on this table (sync lives on Power Company Contacts).
  powerCompany: {
    name:          "Power Company Name",
    utilityRegion: "Utility Region / Territory",
    notes:         "Notes",
    active:        "Active"
  },
  // Power Company Contacts table — field NAMES (same read-vs-write split as
  // inspector). Active=TRUE-on-create is the trigger for the Make.com Google
  // Contacts sync (formula "Needs Sync to Google" fires when Active=TRUE +
  // Cell Phone set + Power Company linked). Contact Name is a TRIM()'d formula
  // (First + Last) — read-only, never write. Never write the Google Contact ID
  // - Rick / Google Contact ID - NEE / Last Synced At / Sync Status fields —
  // those are sync-owned.
  powerContact: {
    nameFormula:      "Contact Name",
    companyName:      "Power Company Name",  // (lookup, read-only)
    firstName:        "First Name",
    lastName:         "Last Name",
    cellPhone:        "Cell Phone",
    officePhone:      "Office Phone",
    email:            "Email Address",
    powerCompanyLink: "Power Company",
    jobRoles:         "Job Roles",
    notes:            "Notes",
    active:           "Active"
  }
};

// Time Entries field IDs
const TE = {
  employee:   "fldG8nGxyJcXRxBNQ",   // Employee (text)
  employeeLink:"fldYgTcZcQzNslRT5",  // Employee (Linked)
  workDate:   "fldzFwSSjLmAkWYHt",
  duration:   "fld9mz6As3099VPVp",   // Duration (Seconds) — writable
  cityTaxes:  "flddCniABjh4Xib1c",
  class:      "fld4MG0FcFDnqYmtW",
  jobLink:    "fldmGwS0qXMdC7FlA",   // Job (linked record)
  reviewed:   "fldQn7d06doEkrGBv"
};

// Payroll Archive — Payroll Runs table
const PR_RUNS = {
  table:           "tbln9nU1BtFmTYMYB",
  payPeriodEnd:    "fldYnkxwJGqMPWDwc",
  payPeriodStart:  "fldrtqWEOB4X5WfW5",
  generatedAt:     "fldVTolQ5UZgOiFfs",
  generatedBy:     "fldZhS7MJMYhL89Nx",
  totalHours:      "fld3xckEXTufGyusi",
  totalBonus:      "fldgtUKA2FpDIP4d8",
  pdf:             "fldSIebm2uhLkpjqD",
  jsonPayload:     "fldVG9Fk2vpedLNMY",
  superseded:      "fld7wDqUZ0MXCY1wP",
  supersedes:      "fldx0Zh3XzQkjGPSL",
  notes:           "fldBoMa33fi9rJQZR"
};

// Payroll Archive — Bonuses table
const PR_BONUSES = {
  table:           "tblpE3emzU3J1P5jx",
  amount:          "flddBFqvKVTfrA2GP",
  employee:        "fldyQ1pxZDpXfp1LE",
  payrollRun:      "fldCxrhDaPrm5OLHb",
  payPeriodStart:  "fldY2cETh9OZoYTij",
  payPeriodEnd:    "fldEOOhDf4msZlrEk"
};

function resp(code, body, extraHeaders) {
  return {
    statusCode: code,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      ...(extraHeaders || {})
    },
    body: JSON.stringify(body)
  };
}

function ensureEnv() {
  if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID)
    throw new Error("Missing env vars AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
}

// Escape a literal string for safe inclusion inside an Airtable filterByFormula
// double-quoted string. Airtable uses backslash escaping inside string literals.
function escapeFormulaString(s) {
  return String(s ?? "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
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

// yyyy-mm-dd ⇄ local-midnight Date helpers, used by the V2 payroll rollups.
function ymdToDate(s) {
  const [y, m, d] = String(s || "").split("-").map(Number);
  if (!y || !m || !d) return null;
  return new Date(y, m - 1, d);
}
function dateToYmd(dt) {
  const y = dt.getFullYear();
  const m = String(dt.getMonth() + 1).padStart(2, "0");
  const d = String(dt.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}
function shiftDays(dt, n) {
  const out = new Date(dt);
  out.setDate(out.getDate() + n);
  return out;
}
function firstLinkedId(v) {
  if (!Array.isArray(v) || !v.length) return null;
  const first = v[0];
  if (typeof first === "string") return first;
  return first?.id || null;
}

// Adds N calendar months to a YYYY-MM-DD string and returns YYYY-MM-DD.
// Uses UTC math to avoid local-timezone day shifts. JS setMonth handles
// month overflow (e.g. Jan 31 + 1 month → Mar 3) which is the standard
// "same calendar day N months later" interpretation we want for warranty
// end dates.
function addMonthsToDateStr(dateStr, months) {
  const [y, m, d] = String(dateStr || "").split("-").map(Number);
  if (!y || !m || !d) return null;
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCMonth(dt.getUTCMonth() + Number(months || 0));
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

// ── Singleselect whitelists ──────────────────────────────────────────────
// Same pattern as ESTIMATE_TYPE_OPTS (see handleCreateJobEstimate): explicit
// option list + fallback, so a stray client value can't trip Airtable's
// typecast and silently create a duplicate option. Keep these in sync with
// the singleselect choices configured on each table.

// Generator Service.Service Type — 7 valid options. "Install / Commissioning"
// is server-set by handleCommissionGenerator; the other six are user-selectable
// in the Generator Service panel (single canonical service-record form; supersedes legacy QSL + Phase 1C NGS).
const SERVICE_TYPE_OPTS = [
  "Install / Commissioning",
  "First Service",
  "Annual Maintenance",
  "Semi-Annual Maintenance",
  "Repair",
  "Warranty Repair",
  "Emergency Service"
];

// Warranties.Warranty Type — fallback "Limited" is the most conservative
// (least coverage) choice if a stray value arrives.
const WARRANTY_TYPE_OPTS = ["Parts & Labor", "Parts Only", "Extended", "Limited"];

// Warranties.Source — fallback "Standard" is the default for warranties
// created from manufacturer templates at commissioning time.
const WARRANTY_SOURCE_OPTS = ["Standard", "Extended Purchase", "Promotional", "Transferred"];

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
    // Pull the linked Employee rec ID alongside the text name. Used by the
    // Payroll Archive flow to populate Bonus.Employee links by exact ID
    // rather than name lookup (avoids collisions on shared first names).
    const empLinks = f["Employee (Linked)"];
    const employeeId = Array.isArray(empLinks) && empLinks.length
      ? (typeof empLinks[0] === "string" ? empLinks[0] : empLinks[0]?.id || null)
      : null;
    return {
      id:         r.id,
      employee:   f["Employee"] || "",
      employeeId,
      workDate:   f["Work Date"] || "",
      duration:   f["Duration (Seconds)"] ?? 0,
      hours:      f["Hours"] ?? 0,
      cityTaxes:  f["City Taxes"] || "A No Tax",
      class:      f["Class"] || "",
      jobId:      jobId,
      jobName:    f["Job Name (Text)"] || "",
      reviewed:   f["Labor Reviewed"] === true
    };
  });

  return resp(200, { ok: true, entries });
}

// ── PAYROLL: create new time entry ─────────────────────────────────────────
async function handleCreateTimeEntry(body) {
  const { employee, employeeId, workDate, duration, class: cls, cityTaxes, jobId } = body || {};
  if (!employee || !workDate) return resp(400, { ok: false, error: "Missing employee or workDate." });

  const fields = {};
  fields[TE.employee]   = employee;
  if (employeeId && String(employeeId).startsWith("rec")) {
    fields[TE.employeeLink] = [String(employeeId)];
  }
  fields[TE.workDate]   = workDate;
  fields[TE.duration]   = Math.round(Number(duration) || 0);
  fields[TE.class]      = cls || "Contract";
  fields[TE.cityTaxes]  = cityTaxes || "A No Tax";
  if (jobId && String(jobId).startsWith("rec")) {
    fields[TE.jobLink] = [String(jobId)];
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
    fields[TE.jobLink] = (jobId && String(jobId).startsWith("rec")) ? [String(jobId)] : [];
  }

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${entryId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id });
}

// ── BACKFILL: reconcile Employee text + Employee (Linked) on Time Entries ──
// One-shot admin endpoint. Idempotent — second run reports zero fixes.
// Auth: ADMIN_BACKFILL_TOKEN env var + body.confirm === "YES".
// Behavior: for each Time Entry, fill in whichever employee field is empty
// when the other side is populated AND the missing side resolves via the
// Employees table. Never overwrites a populated field; text↔link mismatches
// are surfaced as a count + ID list, not silently corrected.
async function handleBackfillTimeEntryEmployeeLinks(body) {
  const token = body?.token;
  if (!token || token !== process.env.ADMIN_BACKFILL_TOKEN) {
    return resp(401, { ok: false, error: "Invalid or missing token." });
  }
  if (body?.confirm !== "YES") {
    return resp(400, { ok: false, error: 'Missing confirmation. Pass {"confirm":"YES"} to proceed.' });
  }

  // Cap how many batched PATCHes run per invocation so a large historical
  // backfill can't blow Netlify's 60s function timeout. Caller re-runs
  // until the response reports complete: true.
  const rawMax = body?.maxBatches;
  const maxBatches = (Number.isInteger(rawMax) && rawMax > 0) ? rawMax : 20;

  const [entries, employees] = await Promise.all([
    // Narrow the scan to rows that might need fixing: Work Date in 2025+
    // AND (text empty OR link empty). The unfiltered fetch on production
    // data (~2500 rows) consumed ~50s before any PATCH could run and hit
    // Netlify's idle timeout; even after dropping bothPopulated rows the
    // scan returned 7600+ rows going back to 2021, dominated by historical
    // typos and departed-staff names with no matching Employees record.
    // Pre-2025 entries are out of scope — they don't affect any current
    // reporting and can be cleaned up manually with Airtable find-and-
    // replace if ever needed. Trade-off: rows with both fields populated-
    // but-disagreeing won't appear in the mismatch count via this scan.
    fetchAll(TABLES.timeEntries, { filter: `AND(DATESTR({Work Date})>="2025-01-01",OR({Employee} = BLANK(), {Employee (Linked)} = BLANK()))` }),
    fetchAll(TABLES.employees)
  ]);

  // name → recId and recId → name. Includes inactive employees so historical
  // entries for departed staff are also linkable.
  const nameToId = new Map();
  const idToName = new Map();
  for (const e of employees) {
    const n = (e.fields?.[F.emp.name] || "").trim();
    if (n) nameToId.set(n, e.id);
    idToName.set(e.id, n);
  }

  let bothPopulated = 0, mismatch = 0, bothEmpty = 0;
  let textOnlyFixed = 0, linkOnlyFixed = 0;
  let textOnlyUnresolved = 0, linkOnlyUnresolved = 0;
  const mismatchIds = [];
  const bothEmptyIds = [];
  const unresolvedTextNamesSet = new Set();
  const unresolvedLinkIdsSet = new Set();
  const patches = [];

  for (const r of entries) {
    const f = r.fields || {};
    const text = (f["Employee"] || "").trim();
    const linkedId = firstLinkedId(f["Employee (Linked)"]);
    const hasText = !!text;
    const hasLink = !!linkedId;

    if (hasText && hasLink) {
      const linkedName = (idToName.get(linkedId) || "").trim();
      if (linkedName && linkedName !== text) {
        mismatch++;
        mismatchIds.push(r.id);
      } else {
        bothPopulated++;
      }
      continue;
    }
    if (!hasText && !hasLink) {
      bothEmpty++;
      bothEmptyIds.push(r.id);
      continue;
    }
    if (hasText && !hasLink) {
      const recId = nameToId.get(text);
      if (recId) {
        patches.push({ id: r.id, fields: { [TE.employeeLink]: [recId] } });
      } else {
        textOnlyUnresolved++;
        unresolvedTextNamesSet.add(text);
      }
      continue;
    }
    // hasLink && !hasText
    const name = idToName.get(linkedId);
    if (name) {
      patches.push({ id: r.id, fields: { [TE.employee]: name } });
    } else {
      linkOnlyUnresolved++;
      unresolvedLinkIdsSet.add(linkedId);
    }
  }

  // Apply batched PATCHes (10 records per call, Airtable cap). Tally fixed
  // counts only after a successful batch write so the response reflects
  // actual mutations. A failed batch doesn't abort — push the error and
  // continue so a single transient failure can't block the rest of the run.
  const errors = [];
  let batchesProcessed = 0;
  for (let i = 0; i < patches.length; i += 10) {
    if (batchesProcessed >= maxBatches) break;
    const chunk = patches.slice(i, i + 10);
    try {
      await atFetch(`${encodeURIComponent(TABLES.timeEntries)}`, {
        method: "PATCH",
        body: JSON.stringify({ records: chunk, typecast: true })
      });
      for (const p of chunk) {
        if (p.fields[TE.employeeLink]) textOnlyFixed++;
        else if (p.fields[TE.employee] !== undefined) linkOnlyFixed++;
      }
    } catch (err) {
      console.error("[backfillTimeEntryEmployeeLinks] batch failed:", err);
      errors.push(err.message || String(err));
    }
    batchesProcessed++;
  }

  const pendingPatches = Math.max(0, patches.length - batchesProcessed * 10);
  const complete = pendingPatches === 0;

  return resp(200, {
    ok: true,
    scanned: entries.length,
    bothPopulated,
    textOnlyFixed,
    linkOnlyFixed,
    mismatch,
    bothEmpty,
    textOnlyUnresolved,
    linkOnlyUnresolved,
    mismatchIds,
    bothEmptyIds,
    unresolvedTextNames: [...unresolvedTextNamesSet],
    unresolvedLinkIds: [...unresolvedLinkIdsSet],
    batchesProcessed,
    pendingPatches,
    complete,
    errors
  });
}

// ── PAYROLL ARCHIVE: upload an attachment to an existing record ────────────
// Airtable's content-host endpoint accepts a base64 file payload directly,
// no public URL hosting needed. Limit is 5 MB per file, per Airtable docs.
// The endpoint addresses by record ID alone — no table in the path.
async function uploadAirtableAttachment(recordId, fieldIdOrName, base64, filename, contentType) {
  ensureEnv();
  const url = `https://content.airtable.com/v0/${AIRTABLE_BASE_ID}/${recordId}/${encodeURIComponent(fieldIdOrName)}/uploadAttachment`;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${AIRTABLE_API_KEY}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ contentType, filename, file: base64 })
  });
  const text = await res.text();
  let json = {};
  try { json = text ? JSON.parse(text) : {}; } catch { json = { raw: text }; }
  if (!res.ok) {
    throw new Error(json?.error?.message || json?.error?.type || `Attachment upload failed (${res.status})`);
  }
  return json;
}

// ── PAYROLL ARCHIVE: find non-superseded run for a given pay period ────────
async function handleFindMatchingPayrollRun(params) {
  const { payPeriodStart, payPeriodEnd } = params || {};
  if (!payPeriodStart || !payPeriodEnd) {
    return resp(400, { ok: false, error: "Missing payPeriodStart or payPeriodEnd." });
  }
  const start = escapeFormulaString(payPeriodStart);
  const end   = escapeFormulaString(payPeriodEnd);
  const filter = `AND(IS_SAME({Pay Period Start},"${start}","day"), IS_SAME({Pay Period End},"${end}","day"), NOT({Superseded}))`;
  const records = await fetchAll(PR_RUNS.table, {
    filter,
    sortField: "Generated At",
    sortDir: "desc"
  });
  if (!records.length) return resp(200, { ok: true, runId: null, generatedAt: null, generatedBy: null });
  const r = records[0];
  return resp(200, {
    ok: true,
    runId: r.id,
    generatedAt: r.fields?.["Generated At"] || null,
    generatedBy: r.fields?.["Generated By"] || null
  });
}

// ── PAYROLL ARCHIVE: create a new Payroll Run with PDF + JSON attachments ──
// Sequence (order matters):
//   1. Create the Payroll Run record bare (totals, pay-period dates, supersedes link).
//   2. Upload PDF + JSON via content.airtable.com /uploadAttachment. On any
//      attachment failure: DELETE the run record and return the error so
//      the user can retry from a clean slate.
//   3. Create Bonus records, chunked in groups of 10 (Airtable batch cap).
//   4. PATCH the prior run's Superseded flag if supersedesId was passed.
// Bonus + supersede failures are non-fatal — we return the error in the
// response payload so the run + PDF (the source of truth) survive.
async function handlePayrollRunCreate(body) {
  const {
    payPeriodStart, payPeriodEnd, generatedBy,
    totalHours, totalBonus,
    pdfBase64, pdfFilename,
    jsonBase64, jsonFilename,
    bonuses,
    supersedesId,
    notes
  } = body || {};

  // Required-field validation
  const missing = [];
  if (!payPeriodStart) missing.push("payPeriodStart");
  if (!payPeriodEnd)   missing.push("payPeriodEnd");
  if (!generatedBy)    missing.push("generatedBy");
  if (totalHours == null) missing.push("totalHours");
  if (totalBonus == null) missing.push("totalBonus");
  if (!pdfBase64)   missing.push("pdfBase64");
  if (!pdfFilename) missing.push("pdfFilename");
  if (!jsonBase64)  missing.push("jsonBase64");
  if (!jsonFilename) missing.push("jsonFilename");
  if (missing.length) return resp(400, { ok: false, error: `Missing: ${missing.join(", ")}` });

  // Log decoded PDF size; warn if approaching the 5 MB Airtable cap.
  const pdfBytes = Math.round(pdfBase64.length * 0.75);
  console.log(`[payrollRunCreate] PDF base64 length=${pdfBase64.length} (~${Math.round(pdfBytes / 1024)} KB decoded)`);
  if (pdfBytes > 4.5 * 1024 * 1024) {
    console.warn(`[payrollRunCreate] PDF approaching Airtable 5MB cap: ${pdfBytes} bytes`);
  }

  // Bonus filtering: drop zero-amount entries; split resolved vs. unresolved
  // by employeeId (frontend supplies it from the Time Entries Employee link).
  const bonusList = Array.isArray(bonuses) ? bonuses : [];
  const nonZero = bonusList.filter(b => Number(b?.amount) > 0);
  const unresolvedBonuses = [];
  const resolvedBonuses = [];
  nonZero.forEach(b => {
    if (typeof b.employeeId === "string" && b.employeeId.startsWith("rec")) {
      resolvedBonuses.push(b);
    } else {
      unresolvedBonuses.push({ employeeName: b.employeeName || null, amount: Number(b.amount) });
    }
  });

  // 1. Create the Payroll Run record bare
  const runFields = {};
  runFields[PR_RUNS.payPeriodStart] = payPeriodStart;
  runFields[PR_RUNS.payPeriodEnd]   = payPeriodEnd;
  runFields[PR_RUNS.generatedAt]    = new Date().toISOString();
  runFields[PR_RUNS.generatedBy]    = String(generatedBy);
  runFields[PR_RUNS.totalHours]     = Number(totalHours);
  runFields[PR_RUNS.totalBonus]     = Number(totalBonus);
  if (supersedesId && String(supersedesId).startsWith("rec")) {
    runFields[PR_RUNS.supersedes] = [supersedesId];
  }
  if (typeof notes === "string" && notes.trim()) {
    runFields[PR_RUNS.notes] = notes.trim();
  }

  const created = await atFetch(`${encodeURIComponent(PR_RUNS.table)}`, {
    method: "POST",
    body: JSON.stringify({ fields: runFields, typecast: true })
  });
  const runId = created.id;

  // 2. Upload attachments. Rollback (DELETE the partial run) if either fails.
  try {
    await uploadAirtableAttachment(runId, PR_RUNS.pdf,         pdfBase64,  pdfFilename,  "application/pdf");
    await uploadAirtableAttachment(runId, PR_RUNS.jsonPayload, jsonBase64, jsonFilename, "application/json");
  } catch (err) {
    try {
      await atFetch(`${encodeURIComponent(PR_RUNS.table)}/${runId}`, { method: "DELETE" });
    } catch (delErr) {
      console.error("[payrollRunCreate] rollback DELETE failed:", delErr);
    }
    return resp(500, { ok: false, error: `Attachment upload failed: ${err.message}` });
  }

  // 3. Create Bonus records, chunked at 10 per batch (Airtable cap).
  let bonusError = null;
  if (resolvedBonuses.length) {
    try {
      for (let i = 0; i < resolvedBonuses.length; i += 10) {
        const chunk = resolvedBonuses.slice(i, i + 10);
        const records = chunk.map(b => {
          const f = {};
          f[PR_BONUSES.amount]         = Number(b.amount);
          f[PR_BONUSES.employee]       = [b.employeeId];
          f[PR_BONUSES.payrollRun]     = [runId];
          f[PR_BONUSES.payPeriodStart] = payPeriodStart;
          f[PR_BONUSES.payPeriodEnd]   = payPeriodEnd;
          return { fields: f };
        });
        await atFetch(`${encodeURIComponent(PR_BONUSES.table)}`, {
          method: "POST",
          body: JSON.stringify({ records, typecast: true })
        });
      }
    } catch (err) {
      console.error("[payrollRunCreate] bonus create failed:", err);
      bonusError = err.message || "Bonus create failed";
    }
  }

  // 4. Patch supersede flag on the prior run, if any.
  let supersedeError = null;
  if (supersedesId && String(supersedesId).startsWith("rec")) {
    try {
      const patchFields = {};
      patchFields[PR_RUNS.superseded] = true;
      await atFetch(`${encodeURIComponent(PR_RUNS.table)}/${supersedesId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: patchFields })
      });
    } catch (err) {
      console.error("[payrollRunCreate] supersede patch failed:", err);
      supersedeError = err.message || "Supersede patch failed";
    }
  }

  return resp(200, {
    ok: true,
    runId,
    supersededId: supersedesId || null,
    bonusError,
    supersedeError,
    unresolvedBonuses
  });
}

// ── PAYROLL ARCHIVE: list saved Payroll Runs for the manager grid ─────────
// Single fetch of every run (no Airtable filter) so we can resolve each
// superseded row's successor.generatedAt from an in-memory id→record map
// without a second round-trip. Filtering happens after the fetch.
//
// Sort: Pay Period End desc, Generated At desc tiebreaker (so a freshly
// saved correction sorts above the predecessor it superseded).
async function handlePayrollRunsList(params) {
  const showSuperseded = String(params?.showSuperseded || "").toLowerCase() === "true";
  const records = await fetchAll(PR_RUNS.table);

  // id → record, used to resolve "Superseded By" link to successor.generatedAt
  const byId = new Map(records.map(r => [r.id, r]));

  let runs = records.map(r => {
    const f = r.fields || {};
    const att = Array.isArray(f["PDF"]) && f["PDF"][0] ? f["PDF"][0] : null;
    const supByLinks = f["Superseded By"];
    const supByRunId = Array.isArray(supByLinks) && supByLinks[0] ? supByLinks[0] : null;
    const supByRun   = supByRunId ? byId.get(supByRunId) : null;
    const supByDate  = supByRun?.fields?.["Generated At"] || null;
    return {
      id:                r.id,
      payPeriodStart:    f["Pay Period Start"] || null,
      payPeriodEnd:      f["Pay Period End"]   || null,
      generatedAt:       f["Generated At"]     || null,
      generatedBy:       f["Generated By"]     || null,
      totalHours:        gNum(f, "Total Hours") ?? 0,
      totalBonus:        gNum(f, "Total Bonus") ?? 0,
      bonusCount:        Array.isArray(f["Bonuses"]) ? f["Bonuses"].length : 0,
      notes:             f["Notes"] || null,
      superseded:        gBool(f, "Superseded"),
      supersededByRunId: supByRunId,
      supersededByDate:  supByDate,
      pdfUrl:            att?.url      || null,
      pdfFilename:       att?.filename || null,
      pdfAvailable:      !!att
    };
  });

  if (!showSuperseded) {
    runs = runs.filter(r => !r.superseded);
  }

  runs.sort((a, b) => {
    const peA = a.payPeriodEnd || "";
    const peB = b.payPeriodEnd || "";
    if (peA !== peB) return peA < peB ? 1 : -1;
    const gaA = a.generatedAt || "";
    const gaB = b.generatedAt || "";
    if (gaA !== gaB) return gaA < gaB ? 1 : -1;
    return 0;
  });

  return resp(200, { ok: true, runs });
}

// ══════════════════════════════════════════════════════════════════
// PAYROLL V2 — hours + bonuses rollups for the Payroll Manager top row
// ══════════════════════════════════════════════════════════════════

// Shared date-range derivation for the hour rollups. Returns local-midnight
// Dates for each window boundary. Pay period anchors on the most recent
// non-superseded run's end date + 1 day, then +13 days; falls back to the
// create-payroll dialog's sliding window when no runs exist yet.
async function computePayrollDateRanges(today) {
  const yearStart  = new Date(today.getFullYear(), 0, 1);
  const monthStart = new Date(today.getFullYear(), today.getMonth(), 1);
  const dow = today.getDay();
  const diffToMon = (dow === 0) ? -6 : 1 - dow;
  const thisWeekStart = shiftDays(today, diffToMon);
  const thisWeekEnd   = shiftDays(thisWeekStart, 5); // Mon..Sat work week

  let payPeriodStart, payPeriodEnd;
  const recentRuns = await fetchAll(PR_RUNS.table, {
    filter: `NOT({Superseded})`,
    sortField: "Pay Period End",
    sortDir: "desc"
  });
  if (recentRuns.length && recentRuns[0].fields?.["Pay Period End"]) {
    const lastEnd = ymdToDate(recentRuns[0].fields["Pay Period End"]);
    payPeriodStart = shiftDays(lastEnd, 1);
    payPeriodEnd   = shiftDays(payPeriodStart, 13);
  } else {
    payPeriodStart = shiftDays(thisWeekStart, -7);
    payPeriodEnd   = shiftDays(payPeriodStart, 13);
  }

  return { yearStart, monthStart, thisWeekStart, thisWeekEnd, payPeriodStart, payPeriodEnd };
}

// Hours-rollup tiles: This Week (Mon–Sat), current Pay Period, This Month, YTD.
// One Time Entries fetch covering Jan 1 → today, bucketed in memory.
async function handlePayrollHoursRollup(params) {
  const todayStr = params?.today || dateToYmd(new Date());
  const today = ymdToDate(todayStr);
  if (!today) return resp(400, { ok: false, error: "Invalid today (expected YYYY-MM-DD)." });

  const { yearStart, monthStart, thisWeekStart, thisWeekEnd, payPeriodStart, payPeriodEnd }
    = await computePayrollDateRanges(today);

  // DATESTR + string compare keeps us out of the IS_AFTER/IS_BEFORE granularity
  // hole (only IS_SAME accepts a "day" unit). Both sides are "YYYY-MM-DD".
  const fromStr = dateToYmd(yearStart);
  const filter = `AND(DATESTR({Work Date})>="${fromStr}",DATESTR({Work Date})<="${todayStr}")`;
  const records = await fetchAll(TABLES.timeEntries, { filter, sortField: "Work Date", sortDir: "asc" });

  let wkHrs = 0, ppHrs = 0, moHrs = 0, ytdHrs = 0;
  for (const r of records) {
    const f = r.fields || {};
    const ds = f["Work Date"];
    if (!ds) continue;
    const d = ymdToDate(ds);
    if (!d) continue;
    const hrs = Number(f["Hours"]) || 0;
    if (d >= yearStart    && d <= today)    ytdHrs += hrs;
    if (d >= monthStart   && d <= today)    moHrs  += hrs;
    if (d >= thisWeekStart && d <= today)   wkHrs  += hrs;
    if (d >= payPeriodStart && d <= today && d <= payPeriodEnd) ppHrs += hrs;
  }

  const r2 = (n) => Math.round(n * 100) / 100;
  return resp(200, {
    ok: true,
    asOf: todayStr,
    ranges: {
      thisWeek:  { start: dateToYmd(thisWeekStart),  end: dateToYmd(thisWeekEnd),  hours: r2(wkHrs)  },
      payPeriod: { start: dateToYmd(payPeriodStart), end: dateToYmd(payPeriodEnd), hours: r2(ppHrs)  },
      thisMonth: { start: dateToYmd(monthStart),     end: todayStr,                hours: r2(moHrs)  },
      ytd:       { start: dateToYmd(yearStart),      end: todayStr,                hours: r2(ytdHrs) }
    }
  });
}

// Office and viewer roles never appear in payroll views — office staff are
// admin support and don't get tracked, viewer is a trial/test account. Blank
// or unrecognized roles default to eligible to match the login fallback.
function isPayrollEligibleRole(empFields) {
  const role = normalize(empFields?.[F.emp.role]);
  return role !== "office" && role !== "viewer";
}

// YTD bonus totals per employee. Employee list = (Active) ∪ (had a non-superseded
// YTD bonus), then restricted to payroll-eligible roles (employee + admin).
// Bonuses linked to superseded runs are excluded — the Bonuses table has no
// Superseded field of its own, so we join through Payroll Runs in memory.
async function handlePayrollBonusesRollup(params) {
  const year = parseInt(params?.year, 10) || new Date().getFullYear();
  const yearStart = `${year}-01-01`;

  const [employees, allRuns] = await Promise.all([
    fetchAll(TABLES.employees),
    fetchAll(PR_RUNS.table)
  ]);
  const supersededRunIds = new Set();
  for (const r of allRuns) {
    if (gBool(r.fields, "Superseded")) supersededRunIds.add(r.id);
  }
  const empById = new Map(employees.map(e => [e.id, e]));

  const bonuses = await fetchAll(PR_BONUSES.table, {
    filter: `DATESTR({Pay Period End})>="${yearStart}"`
  });

  const totalsByEmpId = new Map();
  const empIdsWithBonus = new Set();
  for (const b of bonuses) {
    const f = b.fields || {};
    const runId = firstLinkedId(f["Payroll Run"]);
    if (runId && supersededRunIds.has(runId)) continue;
    const empId = firstLinkedId(f["Employee"]);
    if (!empId) continue;
    // Drop bonuses owned by office/viewer roles so an inactive office worker
    // with a prior bonus can't sneak back into the result via the union.
    const empRec = empById.get(empId);
    if (empRec && !isPayrollEligibleRole(empRec.fields)) continue;
    const amt = Number(f["Amount"]) || 0;
    totalsByEmpId.set(empId, (totalsByEmpId.get(empId) || 0) + amt);
    empIdsWithBonus.add(empId);
  }

  const result = [];
  for (const e of employees) {
    if (!isPayrollEligibleRole(e.fields)) continue;
    const isActive = gBool(e.fields, "Active");
    if (!isActive && !empIdsWithBonus.has(e.id)) continue;
    result.push({
      id: e.id,
      name: e.fields?.["Employee Name"] || "Unknown",
      ytdBonus: Math.round((totalsByEmpId.get(e.id) || 0) * 100) / 100
    });
  }
  result.sort((a, b) => a.name.localeCompare(b.name));
  return resp(200, { ok: true, year, employees: result });
}

// Per-employee bonus history (last N non-superseded). Bonuses table is small
// enough (one row per employee per period) to fetchAll and filter in memory —
// avoids the {Employee}-link/ARRAYJOIN-returns-name pitfall.
async function handlePayrollEmployeeBonusHistory(params) {
  const employeeId = params?.employeeId;
  if (!employeeId || !String(employeeId).startsWith("rec")) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const limit = Math.max(1, Math.min(50, parseInt(params?.limit, 10) || 5));

  const [allRuns, allBonuses, empRecs] = await Promise.all([
    fetchAll(PR_RUNS.table),
    fetchAll(PR_BONUSES.table, { sortField: "Pay Period End", sortDir: "desc" }),
    fetchAll(TABLES.employees, { filter: `RECORD_ID()="${employeeId}"` })
  ]);
  // Defensive: if the employeeId belongs to office/viewer (or was constructed
  // by hand against a non-eligible role), don't leak any bonus history.
  const emp = empRecs[0];
  if (emp && !isPayrollEligibleRole(emp.fields)) {
    return resp(200, { ok: true, employeeId, limit, bonuses: [] });
  }
  const supersededRunIds = new Set();
  const runGenAt = new Map();
  for (const r of allRuns) {
    if (gBool(r.fields, "Superseded")) supersededRunIds.add(r.id);
    runGenAt.set(r.id, r.fields?.["Generated At"] || null);
  }

  const out = [];
  for (const b of allBonuses) {
    if (out.length >= limit) break;
    const f = b.fields || {};
    if (firstLinkedId(f["Employee"]) !== employeeId) continue;
    const runId = firstLinkedId(f["Payroll Run"]);
    if (runId && supersededRunIds.has(runId)) continue;
    out.push({
      id: b.id,
      amount: Math.round((Number(f["Amount"]) || 0) * 100) / 100,
      payPeriodStart: f["Pay Period Start"] || null,
      payPeriodEnd:   f["Pay Period End"]   || null,
      runId,
      runGeneratedAt: runId ? (runGenAt.get(runId) || null) : null
    });
  }

  return resp(200, { ok: true, employeeId, limit, bonuses: out });
}

// Per-employee hour breakdown for one of the four rollup tiles. Same date
// derivation as handlePayrollHoursRollup (shared helper) and the same role
// filter as the bonus rollup, so the popover can't surface office/viewer.
// Total is the raw sum rounded once at the end so it ties cleanly back to
// the tile value rather than drifting through per-employee rounding.
async function handlePayrollHoursBreakdown(params) {
  const VALID_BUCKETS = new Set(["thisWeek", "payPeriod", "thisMonth", "ytd"]);
  const bucket = params?.bucket;
  if (!VALID_BUCKETS.has(bucket)) {
    return resp(400, { ok: false, error: "Invalid bucket. Expected one of: thisWeek, payPeriod, thisMonth, ytd." });
  }

  const todayStr = params?.today || dateToYmd(new Date());
  const today = ymdToDate(todayStr);
  if (!today) return resp(400, { ok: false, error: "Invalid today (expected YYYY-MM-DD)." });

  const ranges = await computePayrollDateRanges(today);

  // Window the response advertises: full work-week / pay-period boundary for
  // those buckets; today as end-cap for thisMonth / ytd. Mirrors the rollup.
  let bucketStart, bucketEnd;
  if      (bucket === "thisWeek")  { bucketStart = ranges.thisWeekStart;  bucketEnd = ranges.thisWeekEnd; }
  else if (bucket === "payPeriod") { bucketStart = ranges.payPeriodStart; bucketEnd = ranges.payPeriodEnd; }
  else if (bucket === "thisMonth") { bucketStart = ranges.monthStart;     bucketEnd = today; }
  else                             { bucketStart = ranges.yearStart;      bucketEnd = today; }

  // Sum range clipped at today — entries beyond today aren't counted even when
  // the bucket window extends into the future (matches the rollup's behavior).
  const sumEnd = today < bucketEnd ? today : bucketEnd;

  const [records, employees] = await Promise.all([
    fetchAll(TABLES.timeEntries, {
      filter: `AND(DATESTR({Work Date})>="${dateToYmd(bucketStart)}",DATESTR({Work Date})<="${dateToYmd(sumEnd)}")`,
      sortField: "Work Date",
      sortDir: "asc"
    }),
    fetchAll(TABLES.employees)
  ]);

  const eligibleEmps = employees.filter(e => isPayrollEligibleRole(e.fields));
  const eligibleSet  = new Set(eligibleEmps.map(e => e.id));

  const hoursByEmpId = new Map();
  let rawTotal = 0;
  for (const r of records) {
    const f = r.fields || {};
    const empId = firstLinkedId(f["Employee (Linked)"]);
    if (!empId || !eligibleSet.has(empId)) continue;
    const hrs = Number(f["Hours"]) || 0;
    hoursByEmpId.set(empId, (hoursByEmpId.get(empId) || 0) + hrs);
    rawTotal += hrs;
  }

  const r2 = (n) => Math.round(n * 100) / 100;
  // Same shape as the bonus rollup's union: keep Active eligible employees
  // (so $0 actives still render) and inactive eligible employees who have
  // positive hours in this bucket (so an ex-employee who left mid-year still
  // appears in YTD). Inactive 0-hour ex-employees are hidden.
  const employeesOut = eligibleEmps
    .filter(e => {
      const hrs = hoursByEmpId.get(e.id) || 0;
      return gBool(e.fields, "Active") || hrs > 0;
    })
    .map(e => ({
      id: e.id,
      name: e.fields?.["Employee Name"] || "Unknown",
      hours: r2(hoursByEmpId.get(e.id) || 0)
    }))
    .sort((a, b) => a.name.localeCompare(b.name));

  return resp(200, {
    ok: true,
    bucket,
    range: { start: dateToYmd(bucketStart), end: dateToYmd(bucketEnd) },
    employees: employeesOut,
    total: r2(rawTotal)
  });
}

// ── My Hours: per-user view of the same four buckets + per-day drill ──
//
// Auth model is trust-the-frontend: every endpoint takes an employeeId at
// face value, since the app has no server session. The defensive role check
// keeps office/viewer accounts out (the higher-value protection — a non-
// payroll-eligible user can't query anyone's hours), but a payroll-eligible
// user could in principle pass another employee's id and view their totals.
// Acceptable for V1 of an internal payroll-visibility feature; flagged as a
// known limitation, deferred to a broader auth rework.

async function handleMyHoursRollup(params) {
  const employeeId = params?.employeeId;
  if (!employeeId || !String(employeeId).startsWith("rec")) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const todayStr = params?.today || dateToYmd(new Date());
  const today = ymdToDate(todayStr);
  if (!today) return resp(400, { ok: false, error: "Invalid today (expected YYYY-MM-DD)." });

  const empRecs = await fetchAll(TABLES.employees, { filter: `RECORD_ID()="${employeeId}"` });
  const emp = empRecs[0];
  if (!emp) return resp(404, { ok: false, error: "Employee not found." });
  if (!isPayrollEligibleRole(emp.fields)) {
    return resp(403, { ok: false, error: "Employee role is not payroll-eligible." });
  }

  // Same date math as the admin tile — Pay Period anchor is the company-wide
  // most-recent-non-superseded-run + 1, NOT a per-employee anchor.
  const { yearStart, monthStart, thisWeekStart, thisWeekEnd, payPeriodStart, payPeriodEnd }
    = await computePayrollDateRanges(today);

  // One Time Entries fetch covering Jan 1 → today, then in-memory filter by
  // the linked employee record id. ARRAYJOIN on {Employee (Linked)} expands
  // to names, not ids, so a name-based filter would collide on shared first
  // names — same gotcha the bonus history handler avoids.
  const filter = `AND(DATESTR({Work Date})>="${dateToYmd(yearStart)}",DATESTR({Work Date})<="${todayStr}")`;
  const records = await fetchAll(TABLES.timeEntries, { filter, sortField: "Work Date", sortDir: "asc" });

  let wkHrs = 0, ppHrs = 0, moHrs = 0, ytdHrs = 0;
  for (const r of records) {
    const f = r.fields || {};
    if (firstLinkedId(f["Employee (Linked)"]) !== employeeId) continue;
    const ds = f["Work Date"];
    if (!ds) continue;
    const d = ymdToDate(ds);
    if (!d) continue;
    const hrs = Number(f["Hours"]) || 0;
    if (d >= yearStart    && d <= today)    ytdHrs += hrs;
    if (d >= monthStart   && d <= today)    moHrs  += hrs;
    if (d >= thisWeekStart && d <= today)   wkHrs  += hrs;
    if (d >= payPeriodStart && d <= today && d <= payPeriodEnd) ppHrs += hrs;
  }

  const r2 = (n) => Math.round(n * 100) / 100;
  return resp(200, {
    ok: true,
    employeeId,
    asOf: todayStr,
    ranges: {
      thisWeek:  { start: dateToYmd(thisWeekStart),  end: dateToYmd(thisWeekEnd),  hours: r2(wkHrs)  },
      payPeriod: { start: dateToYmd(payPeriodStart), end: dateToYmd(payPeriodEnd), hours: r2(ppHrs)  },
      thisMonth: { start: dateToYmd(monthStart),     end: todayStr,                hours: r2(moHrs)  },
      ytd:       { start: dateToYmd(yearStart),      end: todayStr,                hours: r2(ytdHrs) }
    }
  });
}

async function handleMyHoursBreakdown(params) {
  const VALID_BUCKETS = new Set(["thisWeek", "payPeriod", "thisMonth", "ytd"]);
  const employeeId = params?.employeeId;
  const bucket = params?.bucket;
  if (!employeeId || !String(employeeId).startsWith("rec")) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  if (!VALID_BUCKETS.has(bucket)) {
    return resp(400, { ok: false, error: "Invalid bucket. Expected one of: thisWeek, payPeriod, thisMonth, ytd." });
  }
  const todayStr = params?.today || dateToYmd(new Date());
  const today = ymdToDate(todayStr);
  if (!today) return resp(400, { ok: false, error: "Invalid today (expected YYYY-MM-DD)." });

  const empRecs = await fetchAll(TABLES.employees, { filter: `RECORD_ID()="${employeeId}"` });
  const emp = empRecs[0];
  if (!emp) return resp(404, { ok: false, error: "Employee not found." });
  if (!isPayrollEligibleRole(emp.fields)) {
    return resp(403, { ok: false, error: "Employee role is not payroll-eligible." });
  }

  const ranges = await computePayrollDateRanges(today);
  let bucketStart, bucketEnd;
  if      (bucket === "thisWeek")  { bucketStart = ranges.thisWeekStart;  bucketEnd = ranges.thisWeekEnd; }
  else if (bucket === "payPeriod") { bucketStart = ranges.payPeriodStart; bucketEnd = ranges.payPeriodEnd; }
  else if (bucket === "thisMonth") { bucketStart = ranges.monthStart;     bucketEnd = today; }
  else                             { bucketStart = ranges.yearStart;      bucketEnd = today; }

  // Clip the fetch at today — entries beyond today aren't real yet (matches
  // the admin breakdown's behavior for forward-leaning Pay Period).
  const sumEnd = today < bucketEnd ? today : bucketEnd;

  const records = await fetchAll(TABLES.timeEntries, {
    filter: `AND(DATESTR({Work Date})>="${dateToYmd(bucketStart)}",DATESTR({Work Date})<="${dateToYmd(sumEnd)}")`,
    sortField: "Work Date",
    sortDir: "asc"
  });

  // One row per Time Entry — multi-job days produce multiple rows, frontend
  // can group by date if it wants. Total is the raw sum rounded once at the
  // end so it ties cleanly to the rollup tile.
  const r2 = (n) => Math.round(n * 100) / 100;
  const entries = [];
  let rawTotal = 0;
  for (const r of records) {
    const f = r.fields || {};
    if (firstLinkedId(f["Employee (Linked)"]) !== employeeId) continue;
    const hrs = Number(f["Hours"]) || 0;
    entries.push({
      id: r.id,
      workDate: f["Work Date"] || "",
      jobId:    firstLinkedId(f["Job"]),
      jobName:  f["Job Name (Text)"] || "",
      hours:    r2(hrs)
    });
    rawTotal += hrs;
  }

  return resp(200, {
    ok: true,
    employeeId,
    bucket,
    range: { start: dateToYmd(bucketStart), end: dateToYmd(bucketEnd) },
    entries,
    total: r2(rawTotal)
  });
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
  // Est. GP cards: read the filtered rollups (Status = Sent / Approved /
  // Archived-Completed) and compute Total Cost / GP $ / GP % here in JS.
  // The Airtable formula twins for those three derivatives sum unfiltered
  // inputs, so we no longer read them. Inclusion is controlled by the
  // Status filter on the upstream Job-table rollups.
  const expectedRevenueAllStatus       = gNum(f, F.job.expectedRevenueAllStatus);
  const projectedEstimatedMaterialCost = gNum(f, F.job.projectedEstimatedMaterialCost);
  const projectedEstimatedLaborCost    = gNum(f, F.job.projectedEstimatedLaborCost);
  const projectedEstimatedTotalCost = projectedEstimatedMaterialCost + projectedEstimatedLaborCost;
  const projectedGrossProfitDollar  = expectedRevenueAllStatus - projectedEstimatedTotalCost;
  const projectedGrossProfitPct     = expectedRevenueAllStatus > 0
    ? (projectedGrossProfitDollar / expectedRevenueAllStatus)
    : null;
  return {
    id:r.id,name:g(f,F.job.name)||"",po:g(f,F.job.po)||"",status:g(f,F.job.status)||"",
    type:g(f,F.job.type)||"",address:g(f,F.job.address)||"",contractor:g(f,F.job.contractor)||"",
      contractorId: (() => {
        const v = f[F.job.contractorLink];
        if (Array.isArray(v) && v.length > 0) {
          return typeof v[0] === "string" ? v[0] : v[0]?.id || null;
        }
        return null;
      })(),
      generatorInstalled:gBool(f,F.job.generatorInstalled),
      powerCompanyName:g(f,F.job.powerCompanyName)||"",powerCompanyContact:g(f,F.job.powerCompanyContact)||"",
      powerCompanyId: (() => {
        const v = f[F.job.powerCompanyLink];
        if (Array.isArray(v) && v.length > 0) {
          return typeof v[0] === "string" ? v[0] : v[0]?.id || null;
        }
        return null;
      })(),
      powerContactId: (() => {
        const v = f[F.job.powerContactLink];
        if (Array.isArray(v) && v.length > 0) {
          return typeof v[0] === "string" ? v[0] : v[0]?.id || null;
        }
        return null;
      })(),
      // Alias for the existing powerCompanyContact key so the diff-3
      // typeahead hydration can use the parallel naming (powerContactName).
      // Same lookup field, different access name — keeps both consumers
      // working until diff 6 deletes the legacy contact path.
      powerContactName: g(f, F.job.powerCompanyContact) || "",
      // TRANSITIONAL: powerCompanyPhone is the legacy projection key, aliasing Cell Phone for
      // backward compat with index.html:3511. Phase 4 removes this line when the UI rewrite
      // adopts powerCompanyCellPhone + powerCompanyOfficePhone for the two-phone render.
      powerCompanyPhone:g(f,F.job.powerCompanyCellPhone)||"",
      powerCompanyCellPhone:g(f,F.job.powerCompanyCellPhone)||"",
      powerCompanyOfficePhone:g(f,F.job.powerCompanyOfficePhone)||"",
      powerCompanyEmail:g(f,F.job.powerCompanyEmail)||"",
      aicNumber:g(f,F.job.aicNumber)||"",tempWorkOrder:g(f,F.job.tempWorkOrder)||"",
      permWorkOrder:g(f,F.job.permWorkOrder)||"",meterNumber:g(f,F.job.meterNumber)||"",
      permitNumber:g(f,F.job.permitNumber)||"",inspectionAgency:g(f,F.job.inspectionAgency)||"",
      inspectionAgencyPhone:g(f,F.job.inspectionAgencyPhone)||"",inspectionAgencyEmail:g(f,F.job.inspectionAgencyEmail)||"",
      inspectionSchedulingLink:g(f,F.job.inspectionSchedulingLink)||"",inspectionContacts:g(f,F.job.inspectionContacts)||"",
      jobInspections:g(f,F.job.jobInspections)||"",wireLink:extractUrl(g(f,F.job.wireLink)),
      pipeLink:extractUrl(g(f,F.job.pipeLink)),addPhotosLink:extractUrl(g(f,F.job.addPhotosLink)),
      viewPhotosLink:extractUrl(g(f,F.job.viewPhotosLink)),trelloCardId:g(f,F.job.trelloCardId)||"",
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
      inspectionAgencyId: (() => {
        const v = f["Inspection Agency"];
        if (Array.isArray(v) && v.length > 0) {
          return typeof v[0] === "string" ? v[0] : v[0]?.id || null;
        }
        return null;
      })(),
      // Inspection Contacts is a multipleRecordLinks field, but the field-app
      // UI constrains it to a single inspector — we surface only the first.
      inspectorId: (() => {
        const v = f["Inspection Contacts"];
        if (Array.isArray(v) && v.length > 0) {
          return typeof v[0] === "string" ? v[0] : v[0]?.id || null;
        }
        return null;
      })(),
      inspectorName: (() => {
        const v = f[F.job.inspectionContacts]; // "Inspector Name (from Inspection Contacts)" lookup
        if (Array.isArray(v)) return v[0] || "";
        return v || "";
      })(),
      // Both lookups through Inspection Contacts → multipleLookupValues. The
      // UI constrains to a single inspector, so the array is 0–1 entries.
      // Either field may be empty (not every inspector has both phone+email).
      inspectorPhone: (() => {
        const v = f[F.job.inspectorPhone];
        if (Array.isArray(v)) return v[0] || "";
        return v || "";
      })(),
      inspectorEmail: (() => {
        const v = f[F.job.inspectorEmail];
        if (Array.isArray(v)) return v[0] || "";
        return v || "";
      })(),
      inspectionNotRequired: gBool(f, "Inspection Not Required"),
      pCloudInvoicesSentId: f["pCloud Invoices Sent ID"] || null,
      expectedRevenue:gNum(f,F.job.expectedRevenue),
      actualJobCostCogs:gNum(f,F.job.actualJobCostCogs),totalReviewedCosts:gNum(f,F.job.totalReviewedCosts),
      totalLaborCostFinal:gNum(f,F.job.totalLaborCostFinal),grossProfitFinalDollar:gNum(f,F.job.grossProfitFinalDollar),
      grossProfitFinalPct:gNum(f,F.job.grossProfitFinalPct),allMaterialsReviewed:gFormulaBool(f,F.job.allMaterialsReviewed),
      allWireReviewed:gFormulaBool(f,F.job.allWireReviewed),allPipeReviewed:gFormulaBool(f,F.job.allPipeReviewed),
      allExpensesReviewed:gFormulaBool(f,F.job.allExpensesReviewed),allLaborReviewed:gFormulaBool(f,F.job.allLaborReviewed),
      expectedRevenueAllStatus,
      projectedEstimatedTotalCost,
      projectedEstimatedLaborHours:gNum(f,F.job.projectedEstimatedLaborHours),
      projectedEstimatedMaterialCost,
      projectedEstimatedLaborCost,
      projectedGrossProfitDollar,
      projectedGrossProfitPct
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
  const serviceRecords = svcRecords.map(sr => { const sf=sr.fields||{}; return { id:sr.id,serviceRecordId:g(sf,F.svc.serviceRecordId)||"",serviceNumber:g(sf,F.svc.serviceNumber)||"",serviceDate:g(sf,F.svc.serviceDate)||"",serviceType:g(sf,F.svc.serviceType)||"",technician:(()=>{const v=sf[F.svc.technicianName];return Array.isArray(v)?(v[0]||""):(v||"");})(),servicePlanVisit:gBool(sf,F.svc.servicePlanVisit),oilChanged:gBool(sf,F.svc.oilChanged),oilFilterChanged:gBool(sf,F.svc.oilFilterChanged),airFilterChanged:gBool(sf,F.svc.airFilterChanged),sparkPlugsChanged:gBool(sf,F.svc.sparkPlugsChanged),batteryTested:gBool(sf,F.svc.batteryTested),batteryReplaced:gBool(sf,F.svc.batteryReplaced),loadTestPerformed:gBool(sf,F.svc.loadTestPerformed),firmwareChecked:gBool(sf,F.svc.firmwareChecked),exerciseChecked:gBool(sf,F.svc.exerciseChecked),troubleCodesFound:g(sf,F.svc.troubleCodesFound)||"",workNotes:g(sf,F.svc.workNotes)||"",partsUsed:g(sf,F.svc.partsUsed)||"",laborHours:g(sf,F.svc.laborHours)||"",generatorHours:g(sf,F.svc.generatorHours)||"" }; });
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

// Updates the Power Co. tab on a Job. Accepts powerCompanyId and powerContactId
// as record IDs from the typeahead pickers (frontend resolves names → ids via
// handleGetPowerCompanies / handleGetContactsForPowerCompany). Writes BOTH the
// company link (fld3fZ9isIQmcFDna) AND the contact link (fldhKlMCFsnmHo5PH) as
// ["recId"] string-array shape per multipleRecordLinks spec. Empty string on
// either id clears the link via []. No typecast — all targets are link / text
// fields, no singleSelects on this write surface. The legacy Intake singleSelect
// (fldURTQ0ygHMMIbTU) is NOT written — its values are being retired and mapJob
// still reads it transitionally for backward-compat with un-migrated rows.
// Returns the full mapped job so the frontend can refresh the card without a
// second fetch.
async function handleUpdatePowerCo(body) {
  const { jobId, powerCompanyId, powerContactId, aicNumber, tempWorkOrder, permWorkOrder, meterNumber } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const fields = {};
  if (powerCompanyId !== undefined) {
    const trimmed = String(powerCompanyId).trim();
    fields["fld3fZ9isIQmcFDna"] = trimmed ? [trimmed] : [];
  }
  if (powerContactId !== undefined) {
    const trimmed = String(powerContactId).trim();
    fields["fldhKlMCFsnmHo5PH"] = trimmed ? [trimmed] : [];
  }
  if (aicNumber      !== undefined) fields["fld1vqpCklUdzgrjO"] = aicNumber;
  if (tempWorkOrder  !== undefined) fields["fldmJKSiIQfJm9zhI"] = tempWorkOrder;
  if (permWorkOrder  !== undefined) fields["fld6t3TBBz6SwJPh8"] = permWorkOrder;
  if (meterNumber    !== undefined) fields["fldWXpfslcqLlwdTQ"] = meterNumber;
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, updatedId: data.id, job: mapJob(data) });
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

  // Save Estimate writes the rich snapshot JSON to Sent Estimate PDFs, never
  // back to the master Job Estimates record. Fetch both in parallel and join
  // the matching Sent PDF in below so frontend "+ Add as Line" can read the
  // customer-facing scope text via est.snapshot.
  const [records, sentPdfRecords] = await Promise.all([
    fetchAll("Job Estimates", { filter: `FIND("${jobName}", ARRAYJOIN({Job}))`, sortField: "Estimate Date", sortDir: "desc" }),
    fetchSentEstimatePDFsForJob(jobId)
  ]);

  // Newest-first so the cascade's .find() returns the most-recent match.
  // Tiebreaker: Estimate Display # desc.
  const sortedSent = [...sentPdfRecords].sort((a, b) => {
    const da = a.fields?.["Estimate Date"] || "", db = b.fields?.["Estimate Date"] || "";
    if (db !== da) return db.localeCompare(da);
    return Number(b.fields?.["Estimate Display #"] || 0) - Number(a.fields?.["Estimate Display #"] || 0);
  });

  // Cascade: 1) back-link match on "Job Estimate" (fldPoz43rrlqWRnwC), 2)
  // fallback to most-recent same-job Sent PDF whose Total equals the master's
  // Actual Estimate Sent (user-editable currency, not Calculated Estimated
  // Total which is a formula and can drift on rounding). Empty string when
  // no match — frontend falls through to est.notes.
  function resolveSnapshot(estId, actualEstimate) {
    const byBackLink = sortedSent.find(r => {
      const links = r.fields?.["Job Estimate"];
      return Array.isArray(links) && links.indexOf(estId) !== -1;
    });
    if (byBackLink) return byBackLink.fields?.["Snapshot"] || "";
    if (actualEstimate != null) {
      const target = Number(actualEstimate);
      if (!isNaN(target)) {
        const byTotal = sortedSent.find(r => Number(r.fields?.["Total"] || 0) === target);
        if (byTotal) return byTotal.fields?.["Snapshot"] || "";
      }
    }
    return "";
  }

  let estimates = records.map(r => { const f=r.fields||{}; const pdfs=(f["Estimate PDF"]||[]).map(att=>({url:att.url,filename:att.filename,size:att.size})); const actualEstimate=f["Actual Estimate Sent"]??null; const joinedSnapshot=resolveSnapshot(r.id, actualEstimate); return { id:r.id,name:f["Estimate Name"]||"",estimateType:f["Estimate Type"]?.name||f["Estimate Type"]||"",status:f["Status"]?.name||f["Status"]||"",date:f["Estimate Date"]||"",actualEstimate,laborHours:f["Estimated Labor Hours"]??null,materialCost:f["Estimated Material Cost"]??null,calculatedTotal:f["Calculated Estimated Total"]??null,notes:f["Notes"]||"",displayNumber:f["Estimate Display #"]||null,snapshot:joinedSnapshot||(f["Estimate Snapshot"]||""),pdfs }; });
  if (onlySaved) estimates = estimates.filter(e => e.displayNumber != null);
  return resp(200, { ok: true, estimates });
}

// Helper for handleJobEstimates: list all Sent Estimate PDFs records linked
// to a Job. Mirrors handleSentEstimatePDFs's in-memory Job-link filter —
// filterByFormula on multipleRecordLinks is unreliable. Returns [] if the
// table doesn't exist yet so handleJobEstimates degrades gracefully.
async function fetchSentEstimatePDFsForJob(jobId) {
  try {
    const all = [];
    let offset = undefined;
    do {
      const qs = (offset ? "?offset=" + encodeURIComponent(offset) : "");
      const page = await atFetch(`${encodeURIComponent("Sent Estimate PDFs")}${qs}`);
      if (page.error) return [];
      all.push(...(page.records || []));
      offset = page.offset;
    } while (offset);
    return all.filter(r => {
      const jobArr = r.fields?.["Job"];
      return Array.isArray(jobArr) && jobArr.indexOf(jobId) !== -1;
    });
  } catch (e) {
    const msg = String(e?.message || e || "");
    if (/NOT_FOUND|could not.*find.*table/i.test(msg)) return [];
    throw e;
  }
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
  const safeContractor = escapeFormulaString((contractor || "").trim());
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
  fields["fld12gpaArqYw7BWU"] = vehicleName ? [vehicleName] : [String(vehicleId)];
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
  // {Job Name (Text)} is a singleLineText mirror that holds "Job Name (PO suffix)"
  // (e.g. "Jenny Ln 1 (KDJ 358)"), not a bare Job Name. So an exact-name prefilter
  // never matches. Use the original substring prefilter as a loose superset, then
  // enforce exact correctness in-memory by record ID — same pattern as
  // handleGetJobInvoices.
  const safeName = escapeFormulaString(jobName);
  const filter = `FIND("${safeName}", ARRAYJOIN({Job Name (Text)}))`;
  const records = await fetchAll(TABLES.timeEntries || "Time Entries", { filter, sortField: "Work Date", sortDir: "desc" });
  const matched = records.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));
  const entries = matched.map(r => { const f=r.fields||{}; return { id:r.id,workDate:f["Work Date"]||"",employee:f["Employee"]||"",class:f["Class"]||"",cityTaxes:f["City Taxes"]||"",hours:f["Hours"]??null,reviewed:f["Labor Reviewed"]===true,notes:f["Notes"]||"",duration:f["Duration (Seconds)"]??null,unbilledHours:f["Unbilled Hours"]??0,unbilledRevenue:f["Unbilled Labor Revenue $"]??0 }; });
  return resp(200, { ok: true, entries });
}

// {Job} on Labor Billing Allocations is a multipleLookupValues through Time
// Entry → Job, so it returns the job NAME, not a record ID. We can't verify by
// record ID here; defense-in-depth filtering by timeEntryId against the
// reviewed Time Entry set happens on the frontend.
async function handleUnlinkedLaborAllocations(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, allocations: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  const safeName = escapeFormulaString(jobName);
  const filter = `AND(FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n"), {Invoice} = BLANK())`;
  const records = await fetchAll(TABLES.laborAllocations, { filter });
  const allocations = records.map(r => {
    const f = r.fields || {};
    const teArr = f["Time Entry"];
    const jobArr = f["Job"];
    return {
      id: r.id,
      allocatedHours: f["Allocated Hours"] ?? 0,
      allocatedRevenue: f["Allocated Revenue $"] ?? 0,
      timeEntryId: Array.isArray(teArr) ? teArr[0] : null,
      jobName: Array.isArray(jobArr) ? jobArr[0] : (jobArr || "")
    };
  });
  return resp(200, { ok: true, allocations });
}

async function handleExpenses(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, expenses: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  // ARRAYJOIN on a linked-record field expands to the linked record's primary
  // field (Job Name), not record IDs. Use a newline-delimited prefilter for exact
  // name match, then verify the linked record ID in-memory to handle duplicate names.
  const safeName = escapeFormulaString(jobName);
  const filter = `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n")`;
  const allRecords = await fetchAll("Expenses", { filter, sortField: "Expense Date", sortDir: "desc" });
  const records = allRecords.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));
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

// {Job} on Material Billing Allocations is a multipleLookupValues through
// Expense → Job — returns job NAME, not record ID. Defense-in-depth filtering
// by expenseId happens on the frontend.
async function handleUnlinkedMaterialAllocations(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, allocations: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  const safeName = escapeFormulaString(jobName);
  const filter = `AND(FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n"), {Invoice} = BLANK())`;
  const records = await fetchAll(TABLES.materialAllocations, { filter });
  const allocations = records.map(r => {
    const f = r.fields || {};
    const expArr = f["Expense"];
    const jobArr = f["Job"];
    return {
      id: r.id,
      allocatedMaterial: f["Allocated Material Amount $"] ?? 0,
      expenseId: Array.isArray(expArr) ? expArr[0] : null,
      jobName: Array.isArray(jobArr) ? jobArr[0] : (jobArr || "")
    };
  });
  return resp(200, { ok: true, allocations });
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
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldlTUL8hsPkReBAB":["recU56ncurkFrM2Nx"],"fldwbLPIafVtmaSeb":Number(amount),"fldX2x2J0xkRyMY3y":"Scissor Lift","fldelsB2jH2tvt1Cj":description||"Scissor Lift Expense","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  if (date) fields["fldCCPYdyWAOGchWb"] = date;
  const data = await atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
}

async function handleAddGeneralExpense(body) {
  const { jobId, date, type, amount, credit, vendorId, description, billable } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  // Credit-only entries (Manual Material Cost blank, Material Credit > 0) are
  // legitimate — used for returned/refunded supplies. Reject only when both
  // amount and credit are empty/zero. Mirrors frontend validator at
  // saveGeneralExpense (~index.html:7624).
  const hasAmount = amount && Number(amount) > 0;
  const hasCredit = credit && Number(credit) > 0;
  if (!hasAmount && !hasCredit) return resp(400, { ok: false, error: "Missing amount or credit." });
  const idStr = String(jobId).trim();
  if (!idStr.startsWith("rec")) return resp(400, { ok: false, error: `Invalid jobId: ${idStr}` });
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldX2x2J0xkRyMY3y":type||"Materials","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  if (date)        fields["fldCCPYdyWAOGchWb"] = date;
  if (description) fields["fldelsB2jH2tvt1Cj"] = description;
  // Gate both currency writes — leaves Manual Material Cost blank on credit-only
  // entries, matching the 4 existing precedent records entered via Airtable web UI.
  if (hasAmount) fields["fldwbLPIafVtmaSeb"] = Number(amount);
  if (hasCredit) fields["fldcld418pREq2bGq"] = Number(credit);
  if (vendorId && String(vendorId).startsWith("rec")) fields["fldlTUL8hsPkReBAB"] = [String(vendorId)];
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
  const records = await fetchAll("Vendors", {
    sortField: "Vendor Name",
    sortDir: "asc",
    filter: "{Active}=TRUE()"
  });
  const vendors = records
    .map(r => ({
      id: r.id,
      name: r.fields["Vendor Name"] || "",
      phone: r.fields["Primary Phone"] || "",
      email: r.fields["Primary Email"] || "",
      chargesSalesTax: r.fields["Charges Sales Tax"] === true
    }))
    .filter(v => v.name);
  // Pin "Other" to the bottom of the list — escape-hatch UX.
  vendors.sort((a, b) => {
    if (a.name === "Other") return 1;
    if (b.name === "Other") return -1;
    return a.name.localeCompare(b.name);
  });
  return resp(200, { ok: true, vendors });
}

// Creates a new Vendor from the "+ Add new vendor" row on the Expenses-tab
// vendor typeahead. Admin-only (admin OR office role, matching frontend
// isAdmin()); 403s anyone else. Required: employeeId (for the role check),
// name. Optional: phone, email, chargesSalesTax. Active is force-set to TRUE
// on create — handleVendors filters on {Active}=TRUE(), so a new vendor
// must be Active to be discoverable by the typeahead after create.
// Duplicate-name guard (case-insensitive) returns 409 with existingId so the
// frontend can offer to select the existing record instead. No typecast —
// all targets are text/phone/email/checkbox; no singleSelects.
async function handleCreateVendor(body) {
  const { employeeId, name, phone, email, chargesSalesTax } = body || {};

  // ── Admin guard (trust-the-frontend, mirrors airtable.js:1210–1224) ──
  const empId = String(employeeId || "").trim();
  if (!empId.startsWith("rec")) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const empRecs = await fetchAll(TABLES.employees, { filter: `RECORD_ID()="${empId}"` });
  const emp = empRecs[0];
  if (!emp) return resp(404, { ok: false, error: "Employee not found." });
  const role = String(emp.fields[F.emp.role] || "").toLowerCase();
  if (role !== "admin" && role !== "office") {
    return resp(403, { ok: false, error: "Admin role required to create vendors." });
  }

  // ── Validate ──
  const trimmedName = String(name || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Vendor Name is required." });

  // ── Duplicate-name guard (case-insensitive) ──
  const safeName = escapeFormulaString(trimmedName.toLowerCase());
  const existing = await fetchAll("Vendors", {
    filter: `LOWER({Vendor Name})="${safeName}"`
  });
  if (existing.length > 0) {
    return resp(409, {
      ok: false,
      error: `A vendor named "${existing[0].fields["Vendor Name"]}" already exists.`,
      existingId: existing[0].id
    });
  }

  // ── Build fields + create ──
  const fields = {};
  fields["fldcguWbBXsbSyj2B"] = trimmedName;                        // Vendor Name
  fields["fldIM0IjHibKlpz5S"] = true;                                // Active
  if (phone && String(phone).trim()) fields["fldMmOsK1riQu1yfV"] = String(phone).trim();
  if (email && String(email).trim()) fields["fldAUaXdu6HWvTn5V"] = String(email).trim();
  if (chargesSalesTax === true)      fields["fldB4AUNSsP3Gyuhj"] = true;

  const data = await atFetch(`${encodeURIComponent("Vendors")}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });

  return resp(200, {
    ok: true,
    vendor: {
      id:              data.id,
      name:            data.fields?.["Vendor Name"] || trimmedName,
      phone:           data.fields?.["Primary Phone"] || "",
      email:           data.fields?.["Primary Email"] || "",
      chargesSalesTax: data.fields?.["Charges Sales Tax"] === true
    }
  });
}

async function handleListContractors() {
  const records = await fetchAll("Companies", {
    filter: "{Active Contractor}=1",
    sortField: "Company Name",
    sortDir: "asc"
  });
  const contractors = records
    .map(r => ({
      id:           r.id,
      name:         r.fields["Company Name"] || "",
      primaryPhone: r.fields["Primary Phone"] || "",
      primaryEmail: r.fields["Primary Email"] || ""
    }))
    .filter(c => c.name);
  return resp(200, { ok: true, contractors }, { "Cache-Control": "public, max-age=60" });
}

// Lists Contacts linked to a given Company (by Company record ID), used by
// the New Project modal's Contact picker. ALL filtering is client-side:
//   - Company link: filterByFormula can't match against linked-record
//     IDs (ARRAYJOIN resolves to primary-field values), but the raw
//     {Company} field on each fetched record IS the recXXX-id array,
//     so .includes(companyId) is the right shape.
//   - Active: a server-side `{Active}!=FALSE()` filter wrongly drops
//     records where the box is unchecked — in Airtable formula land,
//     blank = 0 = FALSE(), so only records EXPLICITLY checked pass.
//     Legacy Contacts created before Active was added have a blank
//     value and were silently filtered out. Use the same idiom as
//     handleVendors (`r.fields[Active] !== false`) which treats blank
//     as "included" — only an explicitly unchecked box excludes.
// Role is also returned (multipleSelects → joined string) for display.
async function handleListContactsByCompany(params) {
  const companyId = String(params?.companyId || "").trim();
  if (!companyId) return resp(400, { ok: false, error: "Missing companyId." });

  const records = await fetchAll(TABLES.contacts, {});

  const contacts = records
    .filter(r => {
      const links = r.fields[F.contact.company];
      if (!Array.isArray(links) || !links.includes(companyId)) return false;
      return r.fields[F.contact.active] !== false;
    })
    .map(r => {
      // Role is multipleSelects — join for display.
      const roleVal = r.fields[F.contact.role];
      const role = Array.isArray(roleVal) ? roleVal.join(", ") : (roleVal || "");
      return {
        id:           r.id,
        firstName:    r.fields[F.contact.firstName]    || "",
        lastName:     r.fields[F.contact.lastName]     || "",
        primaryPhone: r.fields[F.contact.primaryPhone] || "",
        primaryEmail: r.fields[F.contact.primaryEmail] || "",
        role,
        street:       r.fields[F.contact.street]       || "",
        city:         r.fields[F.contact.city]         || "",
        state:        r.fields[F.contact.state]        || "",
        zip:          r.fields[F.contact.zip]          || ""
      };
    })
    .sort((a, b) => {
      const ln = a.lastName.toLowerCase().localeCompare(b.lastName.toLowerCase());
      if (ln !== 0) return ln;
      return a.firstName.toLowerCase().localeCompare(b.firstName.toLowerCase());
    });

  return resp(200, { ok: true, contacts });
}

// Creates a Contact record linked to a Company. Used by the New Project
// modal's "+ Add new contact" inline create. Linked-record writes use the
// ["recXXX"] string-array shape (NOT [{id:"recXXX"}]) — the object shape
// has silently dropped writes here in the past. typecast is intentionally
// off; callers must send canonical field values.
async function handleCreateContact(body) {
  const firstName    = String(body?.firstName    || "").trim();
  const lastName     = String(body?.lastName     || "").trim();
  const primaryPhone = String(body?.primaryPhone || "").trim();
  const primaryEmail = String(body?.primaryEmail || "").trim();
  const companyId    = String(body?.companyId    || "").trim();

  if (!companyId) return resp(400, { ok: false, error: "Missing companyId." });
  if (!firstName && !lastName) return resp(400, { ok: false, error: "First Name or Last Name is required." });

  const fields = {};
  if (firstName)    fields[F.contact.firstName]    = firstName;
  if (lastName)     fields[F.contact.lastName]     = lastName;
  if (primaryPhone) fields[F.contact.primaryPhone] = primaryPhone;
  if (primaryEmail) fields[F.contact.primaryEmail] = primaryEmail;
  fields[F.contact.company] = [companyId];
  fields[F.contact.active]  = true;

  const data = await atFetch(`${encodeURIComponent(TABLES.contacts)}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });

  const f = data.fields || {};
  return resp(200, {
    ok: true,
    contact: {
      id:           data.id,
      firstName:    f[F.contact.firstName]    || "",
      lastName:     f[F.contact.lastName]     || "",
      primaryPhone: f[F.contact.primaryPhone] || "",
      primaryEmail: f[F.contact.primaryEmail] || ""
    }
  });
}

async function handleGetInspectionAgencies() {
  const records = await fetchAll(TABLES.inspectionAgencies, { sortField: "Inspection Agency Name", sortDir: "asc" });
  const agencies = records
    .map(r => ({ id: r.id, name: r.fields["Inspection Agency Name"] || "" }))
    .filter(a => a.name);
  return resp(200, { ok: true, agencies });
}

// Creates a new Inspection Agency from the "+ Add new agency" modal on the
// Inspections tab. Required: name. Optional: phone, email, schedulingLink,
// notes. Active is force-set to TRUE on create — that fires the Make.com
// Google Contacts sync to both rick@ and nee@ accounts. No typecast — all
// targets are text/phone/email/url/multilineText/checkbox; no singleSelects.
async function handleCreateInspectionAgency(body) {
  const { name, phone, email, schedulingLink, notes } = body || {};
  const trimmedName = String(name || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Agency Name is required." });

  const fields = {};
  fields["fldSJntthANaalIVG"] = trimmedName;        // Inspection Agency Name
  fields["fldcJcwQ4dKnXe5nx"] = true;                // Active (Make.com sync trigger)
  if (phone          && String(phone).trim())          fields["fld5bUALpCtHnehjk"] = String(phone).trim();
  if (email          && String(email).trim())          fields["fldSns7jOVDPfcaFd"] = String(email).trim();
  if (schedulingLink && String(schedulingLink).trim()) fields["fld9Ym5pNfp43spbs"] = String(schedulingLink).trim();
  if (notes          && String(notes).trim())          fields["fldtlCyjRD3XJGjFH"] = String(notes);

  const data = await atFetch(`${encodeURIComponent(TABLES.inspectionAgencies)}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
  return resp(200, {
    ok: true,
    agency: {
      id:   data.id,
      name: data.fields?.[F.agency.name] || trimmedName
    }
  });
}

// Returns the active inspectors linked to a given agency. The frontend caches
// {id, name} per agency, so it can send both: agencyName drives a cheap
// filterByFormula prefilter (lookup field), and agencyId drives an in-memory
// verify pass that defends against substring collisions on agency names —
// same pattern as handleGetJobInvoices and the TODO.md sweep target. Pass
// either or both; at least one is required.
async function handleGetInspectorsForAgency(params) {
  const { agencyName, agencyId } = params || {};
  const trimmedName = String(agencyName || "").trim();
  const trimmedId   = String(agencyId   || "").trim();
  if (!trimmedName && !trimmedId) {
    return resp(400, { ok: false, error: "Missing agencyName or agencyId." });
  }

  let records;
  if (trimmedName) {
    // Escape quotes/backslashes so they can't terminate the filter literal.
    const safeName = escapeFormulaString(trimmedName);
    const filter = `AND(FIND("${safeName}", ARRAYJOIN({Inspection Agency Name})) > 0, {Active}=TRUE())`;
    records = await fetchAll(TABLES.inspectionContacts, { filter, sortField: "Inspector Name", sortDir: "asc" });
  } else {
    records = await fetchAll(TABLES.inspectionContacts, { filter: "{Active}=TRUE()", sortField: "Inspector Name", sortDir: "asc" });
  }

  // Verify the linked Agency record ID in-memory when we have it — same
  // substring-collision guard as handleGetJobInvoices.
  if (trimmedId) {
    records = records.filter(r => {
      const links = r.fields["Inspection Agency"];
      return Array.isArray(links) && links.some(l => (typeof l === "string" ? l : l?.id) === trimmedId);
    });
  }

  const inspectors = records.map(r => {
    const f = r.fields || {};
    return {
      id:    r.id,
      name:  f[F.inspector.nameFormula] || "",
      phone: f[F.inspector.phone] || "",
      email: f[F.inspector.email] || ""
    };
  }).filter(i => i.name);

  return resp(200, { ok: true, inspectors });
}

// Creates a new Inspection Contact (inspector) from the "+ Add new inspector"
// modal. Required: firstName, lastName, agencyId (linked → Inspection Agencies).
// Optional: phone, email. Active is force-set to TRUE on create (Make.com sync
// trigger). Inspector Name is a First+Last formula on the table — read back from
// the POST response, never written. No typecast.
async function handleCreateInspectionContact(body) {
  const { firstName, lastName, phone, email, agencyId } = body || {};
  const trimmedFirst = String(firstName || "").trim();
  const trimmedLast  = String(lastName  || "").trim();
  const trimmedAgency = String(agencyId || "").trim();

  if (!trimmedAgency.startsWith("rec")) return resp(400, { ok: false, error: "Missing or invalid agencyId." });
  if (!trimmedFirst) return resp(400, { ok: false, error: "First Name is required." });
  if (!trimmedLast)  return resp(400, { ok: false, error: "Last Name is required." });

  const fields = {};
  fields["fldbLNgj4Msf7SeCu"] = trimmedFirst;            // First Name
  fields["fld1BOsbSTi6BkEa7"] = trimmedLast;             // Last Name
  fields["fldC6CpQmQ12ABY0z"] = [trimmedAgency];         // Inspection Agency (linked)
  fields["fldF0zIEONjKdtAIR"] = true;                     // Active (Make.com sync trigger)
  if (phone && String(phone).trim()) fields["fldh8oOPBJO0O305Y"] = String(phone).trim();
  if (email && String(email).trim()) fields["fld9auKwBoqGJIRL3"] = String(email).trim();

  const data = await atFetch(`${encodeURIComponent(TABLES.inspectionContacts)}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
  const f = data.fields || {};
  return resp(200, {
    ok: true,
    inspector: {
      id:   data.id,
      name: f[F.inspector.nameFormula] || `${trimmedFirst} ${trimmedLast}`.trim(),
      phone: f[F.inspector.phone] || "",
      email: f[F.inspector.email] || ""
    }
  });
}

// ── POWER COMPANIES + POWER COMPANY CONTACTS (for Power Co. picker on Job) ──
async function handleGetPowerCompanies() {
  const records = await fetchAll(TABLES.powerCompanies, { sortField: F.powerCompany.name, sortDir: "asc" });
  const companies = records
    .map(r => ({ id: r.id, name: r.fields[F.powerCompany.name] || "" }))
    .filter(c => c.name);
  return resp(200, { ok: true, companies });
}

// Returns the active Power Company contacts linked to a given company. The
// frontend caches {id, name, cellPhone, officePhone, email} per company, so it
// can send both: companyName drives a cheap filterByFormula prefilter (lookup
// field), and companyId drives an in-memory verify pass that defends against
// substring collisions on company names — same pattern as
// handleGetInspectorsForAgency. Pass either or both; at least one is required.
async function handleGetContactsForPowerCompany(params) {
  const { companyName, companyId } = params || {};
  const trimmedName = String(companyName || "").trim();
  const trimmedId   = String(companyId   || "").trim();
  if (!trimmedName && !trimmedId) {
    return resp(400, { ok: false, error: "Missing companyName or companyId." });
  }

  let records;
  if (trimmedName) {
    // Escape quotes/backslashes so they can't terminate the filter literal.
    const safeName = escapeFormulaString(trimmedName);
    const filter = `AND(FIND("${safeName}", ARRAYJOIN({${F.powerContact.companyName}})) > 0, {${F.powerContact.active}}=TRUE())`;
    records = await fetchAll(TABLES.powerContacts, { filter, sortField: F.powerContact.nameFormula, sortDir: "asc" });
  } else {
    records = await fetchAll(TABLES.powerContacts, { filter: `{${F.powerContact.active}}=TRUE()`, sortField: F.powerContact.nameFormula, sortDir: "asc" });
  }

  // Verify the linked Power Company record ID in-memory when we have it —
  // same substring-collision guard as handleGetInspectorsForAgency.
  if (trimmedId) {
    records = records.filter(r => {
      const links = r.fields[F.powerContact.powerCompanyLink];
      return Array.isArray(links) && links.some(l => (typeof l === "string" ? l : l?.id) === trimmedId);
    });
  }

  const contacts = records.map(r => {
    const f = r.fields || {};
    return {
      id:          r.id,
      name:        f[F.powerContact.nameFormula] || "",
      cellPhone:   f[F.powerContact.cellPhone]   || "",
      officePhone: f[F.powerContact.officePhone] || "",
      email:       f[F.powerContact.email]       || ""
    };
  }).filter(c => c.name);

  return resp(200, { ok: true, contacts });
}

// Creates a new Power Company from the "+ Add new power company" modal on the
// Power Co. tab. Required: name. Optional: utilityRegion, notes. Active is
// force-set to TRUE on create — Airtable defaults to checked, but the handler
// sets it explicitly for safety. No Make.com sync trigger on this table (sync
// lives on Power Company Contacts). No typecast — all targets are
// text/multilineText/checkbox; no singleSelects on this table.
async function handleCreatePowerCompany(body) {
  const { name, utilityRegion, notes } = body || {};
  const trimmedName = String(name || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Power Company Name is required." });

  const fields = {};
  fields["fldj7HRiBvKNp9DpN"] = trimmedName;        // Power Company Name
  fields["fldFa3QqewblhWOID"] = true;                // Active
  if (utilityRegion && String(utilityRegion).trim()) fields["fld8lBfO5NX2b3Q1H"] = String(utilityRegion).trim();
  if (notes         && String(notes).trim())         fields["fldTpLUm9WJ88gwJs"] = String(notes);

  const data = await atFetch(`${encodeURIComponent(TABLES.powerCompanies)}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
  return resp(200, {
    ok: true,
    company: {
      id:   data.id,
      name: data.fields?.[F.powerCompany.name] || trimmedName
    }
  });
}

// Creates a new Power Company Contact from the "+ Add new contact" modal on
// the Power Co. tab. Required: firstName, cellPhone, companyId. Optional:
// lastName, officePhone, email, jobRoles, notes. Active is force-set to TRUE
// on create — Airtable defaults to checked, but the handler sets it
// explicitly for safety. powerCompanyLink is written as ["recId"] string
// array (multipleRecordLinks shape). No typecast — jobRoles is multiSelect
// but options are seeded by office staff; UI picker only surfaces existing
// options, so typecast is not needed.
async function handleCreatePowerContact(body) {
  const { firstName, lastName, cellPhone, officePhone, email, jobRoles, notes, companyId } = body || {};
  const trimmedFirst = String(firstName || "").trim();
  const trimmedCell  = String(cellPhone || "").trim();
  const trimmedCoId  = String(companyId || "").trim();
  if (!trimmedFirst) return resp(400, { ok: false, error: "First Name is required." });
  if (!trimmedCell)  return resp(400, { ok: false, error: "Cell Phone is required." });
  if (!trimmedCoId)  return resp(400, { ok: false, error: "Power Company is required." });
  const fields = {};
  fields["fldIhD7Wq3hSnlfbH"] = trimmedFirst;                    // First Name
  fields["fldTvD0m1wQ1fZt1T"] = trimmedCell;                     // Cell Phone
  fields["fldDDJG2OmuOtIWmA"] = [trimmedCoId];                   // Power Company link
  fields["fldZmI3sYkwhwlKtk"] = true;                            // Active
  if (lastName    && String(lastName).trim())    fields["fldZH9eCvyXNmUl9d"] = String(lastName).trim();
  if (officePhone && String(officePhone).trim()) fields["fldd4qjr1fgkjM3L6"] = String(officePhone).trim();
  if (email       && String(email).trim())       fields["fldQF88ZawxsH9rL1"] = String(email).trim();
  if (Array.isArray(jobRoles) && jobRoles.length) fields["fldpnd8H4gKfkbOwO"] = jobRoles;
  if (notes       && String(notes).trim())       fields["fld7MUJT2R2SRsYss"] = String(notes);
  const data = await atFetch(`${encodeURIComponent(TABLES.powerContacts)}`, {
    method: "POST",
    body: JSON.stringify({ fields })
  });
  return resp(200, {
    ok: true,
    contact: {
      id:         data.id,
      name:       data.fields?.[F.powerContact.nameFormula] || `${trimmedFirst} ${String(lastName || "").trim()}`.trim(),
      cellPhone:  data.fields?.[F.powerContact.cellPhone]   || trimmedCell,
      officePhone:data.fields?.[F.powerContact.officePhone] || "",
      email:      data.fields?.[F.powerContact.email]       || ""
    }
  });
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

// Builds the filterByFormula for Warranty Templates lookup, used by both
// the standalone GET endpoint and the commissioning orchestrator. Blank
// {Model} means "applies to all models for this brand" (the seeded Cummins
// whole-house templates use this), so when the caller passes a model we
// match either that exact model OR a blank Model. When the caller doesn't
// pass a model, we only match blank-Model templates.
function buildWarrantyTemplateFilter(brand, model) {
  const safeBrand = escapeFormulaString(brand);
  const parts = [
    `{${F.warrantyTemplate.active}}=TRUE()`,
    `LOWER({${F.warrantyTemplate.brand}})="${safeBrand.toLowerCase()}"`
  ];
  const trimmedModel = (model || "").trim();
  if (trimmedModel) {
    const safeModel = escapeFormulaString(trimmedModel);
    parts.push(`OR(LOWER({${F.warrantyTemplate.model}})="${safeModel.toLowerCase()}",{${F.warrantyTemplate.model}}=BLANK())`);
  } else {
    parts.push(`{${F.warrantyTemplate.model}}=BLANK()`);
  }
  return `AND(${parts.join(",")})`;
}

// ── WARRANTY TEMPLATES ──────────────────────────────────────────────────
// Read-side endpoint backing the commissioning panel's template lookup
// (and any future "what would we auto-create?" preview). Filtered by Brand
// (required) and optionally Model. Only Active=TRUE() rows return — the
// Active flag is the kill switch for templates that have been superseded
// or are not yet ready for production use.
async function handleGetWarrantyTemplates(params) {
  const brand = (params?.brand || "").trim();
  const model = (params?.model || "").trim();
  if (!brand) return resp(400, { ok: false, error: "Missing brand." });

  const filter = buildWarrantyTemplateFilter(brand, model);
  const records = await fetchAll(TABLES.warrantyTemplates, { filter });
  const templates = records.map(r => {
    const f = r.fields || {};
    return {
      id:             r.id,
      name:           g(f, F.warrantyTemplate.name) || "",
      brand:          g(f, F.warrantyTemplate.brand) || "",
      model:          g(f, F.warrantyTemplate.model) || "",
      warrantyType:   g(f, F.warrantyTemplate.warrantyType) || "",
      durationMonths: gNum(f, F.warrantyTemplate.durationMonths),
      notes:          g(f, F.warrantyTemplate.notes) || "",
      active:         gBool(f, F.warrantyTemplate.active)
    };
  });
  return resp(200, { ok: true, templates });
}

// ── WARRANTIES (READ) ───────────────────────────────────────────────────
// Returns all warranties attached to a generator, sorted by End Date
// ascending so the soonest-to-expire shows first. filterByFormula can't
// match a linked-record field by record ID directly, so we resolve the
// generator's primary text (Generator Asset ID) and use the same
// FIND(..., ARRAYJOIN({Generator})) trick the Generator Service lookup
// uses (see handleGenerator).
async function handleGetWarranties(params) {
  const generatorId = (params?.generatorId || "").trim();
  if (!generatorId) return resp(400, { ok: false, error: "Missing generatorId." });

  let assetId = "";
  try {
    const genRec = await atFetch(`${encodeURIComponent(TABLES.generators)}/${generatorId}`);
    assetId = genRec?.fields?.[F.gen.assetId] || "";
  } catch (err) {
    return resp(404, { ok: false, error: "Generator not found." });
  }
  // Just-created records may not have their formula assetId computed yet
  // — return empty rather than scanning the whole Warranties table.
  if (!assetId) return resp(200, { ok: true, warranties: [] });

  const safe = escapeFormulaString(assetId);
  const filter = `FIND("${safe}", ARRAYJOIN({${F.warranty.generator}}))`;
  const records = await fetchAll(TABLES.warranties, {
    filter,
    sortField: F.warranty.endDate,
    sortDir:   "asc"
  });
  const warranties = records.map(r => {
    const f = r.fields || {};
    return {
      id:             r.id,
      name:           g(f, F.warranty.name) || "",
      warrantyType:   g(f, F.warranty.warrantyType) || "",
      startDate:      g(f, F.warranty.startDate) || "",
      endDate:        g(f, F.warranty.endDate) || "",
      durationMonths: gNum(f, F.warranty.durationMonths),
      source:         g(f, F.warranty.source) || "",
      voided:         gBool(f, F.warranty.voided),
      voidedReason:   g(f, F.warranty.voidedReason) || "",
      notes:          g(f, F.warranty.notes) || "",
      templateId:     firstLinkedId(f[F.warranty.createdFromTemplate])
    };
  });
  return resp(200, { ok: true, warranties });
}

// ── ADD WARRANTY ────────────────────────────────────────────────────────
// Standalone single-warranty create. The commissioning orchestrator does
// its own warranty inserts inline (so it can roll a single warnings[]
// across all writes); this endpoint is for ad-hoc manual additions like
// extended-purchase or transferred warranties added after commissioning.
async function handleAddWarranty(body) {
  const { generatorId, warrantyType, startDate, durationMonths, source, templateId, notes } = body || {};
  if (!generatorId)              return resp(400, { ok: false, error: "Missing generatorId." });
  if (!startDate)                return resp(400, { ok: false, error: "Missing startDate." });
  if (durationMonths === undefined || durationMonths === null || durationMonths === "")
    return resp(400, { ok: false, error: "Missing durationMonths." });
  const months = Number(durationMonths);
  if (!Number.isFinite(months) || months <= 0)
    return resp(400, { ok: false, error: "durationMonths must be a positive number." });

  const endDate = addMonthsToDateStr(startDate, months);
  if (!endDate) return resp(400, { ok: false, error: "Invalid startDate format (need YYYY-MM-DD)." });

  const fields = {};
  fields[F.warranty.generator]      = [generatorId];
  // Warranty Type whitelist (singleSelect) — fallback "Limited" is the most
  // conservative coverage choice if a stray value somehow arrives.
  fields[F.warranty.warrantyType]   = WARRANTY_TYPE_OPTS.includes(warrantyType) ? warrantyType : "Limited";
  fields[F.warranty.startDate]      = startDate;
  fields[F.warranty.endDate]        = endDate;
  fields[F.warranty.durationMonths] = months;
  // Source whitelist (singleSelect) — fallback "Standard" is the default
  // for warranties created from manufacturer templates.
  fields[F.warranty.source]         = WARRANTY_SOURCE_OPTS.includes(source) ? source : "Standard";
  if (templateId)                    fields[F.warranty.createdFromTemplate] = [templateId];
  if (notes && String(notes).trim()) fields[F.warranty.notes] = String(notes);

  const data = await atFetch(`${encodeURIComponent(TABLES.warranties)}`, {
    method: "POST",
    body: JSON.stringify({ fields, typecast: true })
  });
  if (data.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: data.id });
}

// ── COMMISSION GENERATOR (orchestrator) ─────────────────────────────────
// One-shot commissioning workflow: PATCH/CREATE the Generators record →
// POST a "Install / Commissioning" Generator Service event → POST one
// Warranty per matching Warranty Template (Source = "Standard").
//
// Not transactional (Airtable REST has no transactions). Best-effort: if
// step 1 fails we abort. If steps 2 or 3 fail partially, we accumulate
// reasons in warnings[] and return ok:true with whatever IDs did succeed,
// so the UI can surface "the asset saved but the service event didn't —
// here's what to retry" rather than a generic 500.
//
// Idempotent on warranties: if the generator already has any warranty
// records, the warranty step is skipped (warning recorded). This makes
// re-running commissioning on an existing asset safe.
async function handleCommissionGenerator(body) {
  const {
    jobId, generatorId,
    installDate, brand, model, kw, fuelType, serialNumber,
    transferSwitchModel, transferSwitchSerial,
    batteryInstallDate, servicePlanActive, serviceIntervalMonths,
    assetNotes,
    commissioningDate, technician, generatorHours, commissioningNotes
  } = body || {};

  if (!jobId)       return resp(400, { ok: false, error: "Missing jobId." });
  if (!installDate) return resp(400, { ok: false, error: "Missing installDate." });
  if (!brand)       return resp(400, { ok: false, error: "Missing brand (required for warranty template lookup)." });

  const warnings = [];

  // ── Step 1: PATCH or CREATE the Generators record ─────────────────────
  // Same field shape works for both code paths; only difference is the
  // Job linkage (set on create only).
  const assetFields = {};
  assetFields[F.gen.installDate] = installDate;
  assetFields[F.gen.brand]       = brand;
  if (model && String(model).trim())                     assetFields[F.gen.model] = String(model).trim();
  if (kw !== undefined && kw !== null && kw !== "")     assetFields[F.gen.kw] = String(kw);
  if (fuelType)                                          assetFields[F.gen.fuelType] = fuelType;
  if (serialNumber && String(serialNumber).trim())       assetFields[F.gen.serialNumber] = String(serialNumber).trim();
  if (transferSwitchModel && String(transferSwitchModel).trim())   assetFields[F.gen.transferSwitchModel] = String(transferSwitchModel).trim();
  if (transferSwitchSerial && String(transferSwitchSerial).trim()) assetFields[F.gen.transferSwitchSerial] = String(transferSwitchSerial).trim();
  if (batteryInstallDate)                                assetFields[F.gen.batteryInstallDate] = batteryInstallDate;
  if (servicePlanActive !== undefined)                   assetFields[F.gen.servicePlanActive] = servicePlanActive === true;
  if (serviceIntervalMonths !== undefined && serviceIntervalMonths !== null && serviceIntervalMonths !== "")
    assetFields[F.gen.serviceIntervalMonths] = String(serviceIntervalMonths);
  if (assetNotes && String(assetNotes).trim())           assetFields[F.gen.notes] = String(assetNotes);

  let resolvedGeneratorId = generatorId || null;

  // If caller didn't supply a generatorId, look for one already linked to
  // the Job before falling back to creating a fresh asset. This keeps the
  // re-commissioning path (asset exists, is being PATCHed) intact even
  // when the frontend hasn't passed the ID along.
  if (!resolvedGeneratorId) {
    try {
      const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
      const jobName = jobRecords[0]?.fields?.[F.job.name] || "";
      if (jobName) {
        const safeName = escapeFormulaString(jobName);
        const linkedFilter = `FIND("${safeName}", ARRAYJOIN({${F.gen.job}}))`;
        const linked = await fetchAll(TABLES.generators, { filter: linkedFilter });
        if (linked.length) resolvedGeneratorId = linked[0].id;
      }
    } catch (err) {
      // Lookup failure isn't fatal — we can still create a fresh asset below.
      warnings.push(`Could not check for existing generator on job: ${err.message}`);
    }
  }

  if (resolvedGeneratorId) {
    try {
      const patched = await atFetch(`${encodeURIComponent(TABLES.generators)}/${resolvedGeneratorId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: assetFields, typecast: true })
      });
      resolvedGeneratorId = patched.id;
    } catch (err) {
      return resp(500, { ok: false, error: `Failed to update generator: ${err.message}` });
    }
  } else {
    const createFields = { ...assetFields };
    createFields[F.gen.job] = [jobId];
    try {
      const created = await atFetch(`${encodeURIComponent(TABLES.generators)}`, {
        method: "POST",
        body: JSON.stringify({ fields: createFields, typecast: true })
      });
      resolvedGeneratorId = created.id;
    } catch (err) {
      return resp(500, { ok: false, error: `Failed to create generator: ${err.message}` });
    }
  }

  // ── Shared lookup for steps 2 & 3 dup checks ──────────────────────────
  // Resolve the generator's primary text (Generator Asset ID) once and
  // reuse it for both the commissioning-service dup check (step 2) and
  // the warranties dup check (step 3). For just-created records the
  // formula may not have computed yet — in that case both dup checks
  // see no matches and we create rather than wrongly skip (matches the
  // pre-existing warranty-path behavior).
  const COMM_TYPE = "Install / Commissioning";
  const svcType = SERVICE_TYPE_OPTS.includes(COMM_TYPE) ? COMM_TYPE : SERVICE_TYPE_OPTS[0];

  let assetIdForLookup = "";
  try {
    const genRec = await atFetch(`${encodeURIComponent(TABLES.generators)}/${resolvedGeneratorId}`);
    assetIdForLookup = genRec?.fields?.[F.gen.assetId] || "";
  } catch (err) {
    warnings.push(`Could not re-read generator for dup-checks: ${err.message}`);
  }

  // ── Step 2: POST the commissioning Generator Service event ────────────
  // Idempotent: if an Install / Commissioning record already exists for
  // this generator, reuse its ID rather than piling up duplicate
  // commissioning rows on re-runs (matches step 3's warranty idempotency).
  // Service Type is server-set rather than client-passed, but still
  // validated against SERVICE_TYPE_OPTS so the typecast guard is
  // consistent with other handlers.
  let serviceRecordId = null;
  let existingCommissioningRecord = null;
  if (assetIdForLookup) {
    try {
      const safe = escapeFormulaString(assetIdForLookup);
      const dupFilter = `AND(FIND("${safe}", ARRAYJOIN({${F.svc.generator}})), {${F.svc.serviceType}}="${svcType}")`;
      const existing = await fetchAll(TABLES.generatorService, { filter: dupFilter });
      if (existing.length) existingCommissioningRecord = existing[0];
    } catch (err) {
      warnings.push(`Could not check for existing commissioning record: ${err.message}`);
    }
  }

  if (existingCommissioningRecord) {
    serviceRecordId = existingCommissioningRecord.id;
    warnings.push("Commissioning service record already exists — skipped re-creation.");
  } else {
    const svcFields = {};
    svcFields[F.svc.generator]   = [resolvedGeneratorId];
    svcFields[F.svc.job]         = [jobId];
    svcFields[F.svc.serviceDate] = commissioningDate || installDate;
    svcFields[F.svc.serviceType] = svcType;
    if (technician)         svcFields[F.svc.technician] = String(technician);
    if (generatorHours !== undefined && generatorHours !== null && generatorHours !== "")
      svcFields[F.svc.generatorHours] = Number(generatorHours);
    if (commissioningNotes && String(commissioningNotes).trim())
      svcFields[F.svc.workNotes] = String(commissioningNotes);

    try {
      const svcData = await atFetch(`${encodeURIComponent(TABLES.generatorService)}`, {
        method: "POST",
        body: JSON.stringify({ fields: svcFields, typecast: true })
      });
      serviceRecordId = svcData.id;
    } catch (err) {
      warnings.push(`Failed to create commissioning service record: ${err.message}`);
    }
  }

  // ── Step 3: Create one Warranty per matching Warranty Template ────────
  // Idempotency: skip the whole step if any warranties already exist on
  // this generator, so re-commissioning doesn't pile up duplicates.
  const warrantyIds = [];

  let existingWarrantyCount = 0;
  if (assetIdForLookup) {
    try {
      const safe = escapeFormulaString(assetIdForLookup);
      const existingFilter = `FIND("${safe}", ARRAYJOIN({${F.warranty.generator}}))`;
      const existing = await fetchAll(TABLES.warranties, { filter: existingFilter });
      existingWarrantyCount = existing.length;
    } catch (err) {
      warnings.push(`Could not check existing warranties: ${err.message}`);
    }
  }

  if (existingWarrantyCount > 0) {
    warnings.push("Warranties already existed for this generator — skipped re-creation.");
  } else {
    let templates = [];
    try {
      const tFilter = buildWarrantyTemplateFilter(brand, model);
      templates = await fetchAll(TABLES.warrantyTemplates, { filter: tFilter });
    } catch (err) {
      warnings.push(`Could not look up warranty templates: ${err.message}`);
    }

    if (!templates.length) {
      warnings.push(`No active warranty templates found for brand "${brand}"${model ? ` / model "${model}"` : ""}.`);
    }

    for (const t of templates) {
      const tf = t.fields || {};
      const months = Number(tf[F.warrantyTemplate.durationMonths]);
      const wType  = tf[F.warrantyTemplate.warrantyType];
      const tName  = tf[F.warrantyTemplate.name] || t.id;
      if (!Number.isFinite(months) || months <= 0) {
        warnings.push(`Template "${tName}" has invalid Duration Months — skipped.`);
        continue;
      }
      const wEnd = addMonthsToDateStr(installDate, months);
      const wFields = {};
      wFields[F.warranty.generator]           = [resolvedGeneratorId];
      wFields[F.warranty.warrantyType]        = WARRANTY_TYPE_OPTS.includes(wType) ? wType : "Limited";
      wFields[F.warranty.startDate]           = installDate;
      wFields[F.warranty.endDate]             = wEnd;
      wFields[F.warranty.durationMonths]      = months;
      wFields[F.warranty.source]              = "Standard";
      wFields[F.warranty.createdFromTemplate] = [t.id];
      try {
        const wData = await atFetch(`${encodeURIComponent(TABLES.warranties)}`, {
          method: "POST",
          body: JSON.stringify({ fields: wFields, typecast: true })
        });
        warrantyIds.push(wData.id);
      } catch (err) {
        warnings.push(`Failed to create warranty from template "${tName}": ${err.message}`);
      }
    }
  }

  return resp(200, {
    ok: true,
    generatorId:    resolvedGeneratorId,
    serviceRecordId,
    warrantyIds,
    warnings
  });
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

async function handleUpdateJobNotes(body) {
  const { jobId, notes } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldAuZAW19iYPBPxP": notes || "" } }) });
  return resp(200, { ok: true, updatedId: data.id });
}

// Admin-only Inspections-tab edit. PATCHes four Job fields in a single call:
//   - Inspection Agency       (fldyKKACyUqt9tcEL, linked)
//   - Inspection Contacts     (fld9ApvXJqPhuDcm4, linked — single inspector)
//   - Permit Number           (fldDKGllmOyyyf9qo, text)
//   - Inspection Not Required (fldQ5VJgOYcQBxmCr, checkbox)
// Empty agencyId / inspectorId clear their links; empty permitNumber clears
// the text. Inspectors belong to a specific agency — if the agency is cleared,
// the inspector link is force-cleared too (server-side guard against UI desync).
// No typecast — all four targets are linked-records / text / checkbox; no
// singleSelects in scope, so typecast would only mask broken input.
async function handleUpdateJobInspection(body) {
  const { jobId, agencyId, permitNumber, inspectorId, inspectionNotRequired } = body || {};
  if (!jobId || !String(jobId).startsWith("rec")) {
    return resp(400, { ok: false, error: "Missing or invalid jobId." });
  }
  const fields = {};
  const hasAgency = !!agencyId && String(agencyId).startsWith("rec");
  fields["fldyKKACyUqt9tcEL"] = hasAgency ? [agencyId] : [];
  // Inspector belongs to an agency — if no agency, force-clear the inspector.
  if (hasAgency && inspectorId && String(inspectorId).startsWith("rec")) {
    fields["fld9ApvXJqPhuDcm4"] = [inspectorId];
  } else {
    fields["fld9ApvXJqPhuDcm4"] = [];
  }
  fields["fldDKGllmOyyyf9qo"] = permitNumber || "";
  fields["fldQ5VJgOYcQBxmCr"] = !!inspectionNotRequired;

  const data = await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
  return resp(200, { ok: true, job: mapJob(data) });
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

// POSTs a new Jobs record from the in-app New Project modal. Every new
// job is now a contractor job: Contractor (linked) is required and the
// same Company is written to both Contractor and Billing Company by
// default. Status defaults to "New Lead" so the new job lands in the
// right sidebar group. Optional fields are sent only when non-blank to
// avoid stomping Airtable defaults with empty strings.
//
// Billing Method is force-set to "Contractor" — the radio is gone from
// the UI but downstream invoice-builder reads (index.html:6514, 6605,
// 6903, 7311) still inspect job.billingMethod as a Contract-vs-T&M
// tiebreaker, so we keep the breadcrumb coherent.
//
// Contractor (Intake) is still written with the company name string;
// Make.com and other downstream readers may still depend on it. Plan
// is to remove it in a follow-up cleanup pass once confirmed unused.
//
// LINKED RECORD shape: ["recXXX"] string array, NEVER [{id:"recXXX"}].
// The object shape has silently dropped linked writes in this codebase
// before. typecast is intentionally off.
//
// Returns the new record run through mapJob() so the frontend can splice
// it into state.jobs and selectJob() it without a full list refetch.
async function handleCreateJob(body) {
  const {
    jobName, jobType, taxStatus, contractorId, contractorName, contactId,
    customerFirstName, customerLastName,
    customerStreet, customerCity, customerState, customerZip,
    customerPhone, customerEmail, notes
  } = body || {};

  const trimmedName = String(jobName || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Job Name is required." });

  const trimmedContractorId = String(contractorId || "").trim();
  if (!trimmedContractorId) return resp(400, { ok: false, error: "Contractor is required." });

  const fields = {};
  fields["Job Name"]       = trimmedName;
  fields["Job Status"]     = "New Lead";
  fields["Tax Status"]     = taxStatus || "Taxable";
  fields["Billing Method"] = "Contractor";

  if (jobType && String(jobType).trim()) fields["Job Type"] = String(jobType).trim();

  // Contractor + Billing Company default to the same Company on create.
  fields["Contractor"]      = [trimmedContractorId];
  fields["Billing Company"] = [trimmedContractorId];

  // Keep the legacy text breadcrumb populated for downstream readers.
  if (contractorName && String(contractorName).trim()) {
    fields["Contractor (Intake)"] = String(contractorName).trim();
  }

  const trimmedContactId = String(contactId || "").trim();
  if (trimmedContactId) fields["Primary Contact"] = [trimmedContactId];

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
    body: JSON.stringify({ fields })
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
      if (action === "getWarrantyTemplates") return await handleGetWarrantyTemplates(params);
      if (action === "getWarranties")      return await handleGetWarranties(params);
      if (action === "expenses")           return await handleExpenses(params);
      if (action === "timeEntries")        return await handleTimeEntries(params);
      if (action === "unlinkedLaborAllocations")    return await handleUnlinkedLaborAllocations(params);
      if (action === "unlinkedMaterialAllocations") return await handleUnlinkedMaterialAllocations(params);
      if (action === "payrollEntries")     return await handlePayrollEntries(params);
      if (action === "findMatchingPayrollRun") return await handleFindMatchingPayrollRun(params);
      if (action === "payrollRunsList")    return await handlePayrollRunsList(params);
      if (action === "payrollHoursRollup")          return await handlePayrollHoursRollup(params);
      if (action === "payrollHoursBreakdown")       return await handlePayrollHoursBreakdown(params);
      if (action === "payrollBonusesRollup")        return await handlePayrollBonusesRollup(params);
      if (action === "payrollEmployeeBonusHistory") return await handlePayrollEmployeeBonusHistory(params);
      if (action === "myHoursRollup")               return await handleMyHoursRollup(params);
      if (action === "myHoursBreakdown")            return await handleMyHoursBreakdown(params);
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
      if (action === "listContractors")    return await handleListContractors();
      if (action === "listContactsByCompany") return await handleListContactsByCompany(params);
      if (action === "laborBillableRates") return await handleLaborBillableRates();
      if (action === "getInspectionAgencies") return await handleGetInspectionAgencies();
      if (action === "inspectorsForAgency")   return await handleGetInspectorsForAgency(params);
      if (action === "getPowerCompanies")           return await handleGetPowerCompanies();
      if (action === "getContactsForPowerCompany")  return await handleGetContactsForPowerCompany(params);
      return resp(400, { ok: false, error: "Unknown GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      if (body.action === "login")                return await handleLogin(body);
      if (body.action === "updateJobStatus")      return await handleUpdateJobStatus(body);
      if (body.action === "updatePowerCo")        return await handleUpdatePowerCo(body);
      if (body.action === "createPowerCompany")   return await handleCreatePowerCompany(body);
      if (body.action === "createPowerContact")   return await handleCreatePowerContact(body);
      if (body.action === "updateTimeEntry")      return await handleUpdateTimeEntry(body);
      if (body.action === "updateTimeEntryPayroll") return await handleUpdateTimeEntryPayroll(body);
      if (body.action === "createTimeEntry")      return await handleCreateTimeEntry(body);
      if (body.action === "deleteTimeEntry")      return await handleDeleteTimeEntry(body);
      if (body.action === "backfillTimeEntryEmployeeLinks") return await handleBackfillTimeEntryEmployeeLinks(body);
      if (body.action === "payrollRunCreate")     return await handlePayrollRunCreate(body);
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
      if (body.action === "addWarranty")          return await handleAddWarranty(body);
      if (body.action === "commissionGenerator")  return await handleCommissionGenerator(body);
      if (body.action === "addScheduleEntry")     return await handleAddScheduleEntry(body);
      if (body.action === "updateScheduleEntry")  return await handleUpdateScheduleEntry(body);
      if (body.action === "deleteScheduleEntry")  return await handleDeleteScheduleEntry(body);
      if (body.action === "getNextInvoiceNumber") return await handleGetNextInvoiceNumber();
      if (body.action === "getJobInvoices")       return await handleGetJobInvoices(body);
      if (body.action === "updateJobNotes")       return await handleUpdateJobNotes(body);
      if (body.action === "updateJobInspection")  return await handleUpdateJobInspection(body);
      if (body.action === "createInspectionAgency") return await handleCreateInspectionAgency(body);
      if (body.action === "createInspectionContact") return await handleCreateInspectionContact(body);
      if (body.action === "updateJobInfo")        return await handleUpdateJobInfo(body);
      if (body.action === "createJob")            return await handleCreateJob(body);
      if (body.action === "createContact")        return await handleCreateContact(body);
      if (body.action === "updateInspection")     return await handleUpdateInspection(body);
      if (body.action === "calculateMileage")     return await handleCalculateMileage(body);
      if (body.action === "addLiftExpense")       return await handleAddLiftExpense(body);
      if (body.action === "addGeneralExpense")    return await handleAddGeneralExpense(body);
      if (body.action === "createVendor")         return await handleCreateVendor(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Proxy Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

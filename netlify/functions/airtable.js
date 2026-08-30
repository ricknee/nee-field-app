// netlify/functions/airtable.js
// Northeastern Electric Field App — Netlify Proxy
// Reads env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AUTH_SECRET
import { signToken, authedUser, hasRole, signScope, verifyScope } from "./_auth.js";
import { isSessionRevoked, clearRevocationCache } from "./_revocation.js";
import { scrubFabricatingLinks, airtableWriteBlocked, airtableWritesEnabled, SKIPPED_WRITE, UUID_RE } from "./_airtable-write-guard.js";
import { runIntegrityChecks } from "./_integrity.js";
import { shadowLoginCheck, neonLoginCandidate, loginSource,
         neonEmployees, neonEmployeeById, isEmployeeHandle } from "./_employees.js";
// Shadow-read helpers for the Neon migration. Fail-soft by contract — see _neon.js.
import { neonEnabled, neonQuery, neonExec, neonWrite, shadowCompare } from "./_neon.js";
// Shared with inventory.js — the materials push writes expenses too (Step E).
// The switch is read inside the module, not here — every entry point gates
// itself, so a caller can never forget to. The response's `allocation.skipped`
// reports "disabled" when it is off, which is how the cutover is verified.
import { createLaborAllocation, createMaterialAllocation,
         attachAllocationsToInvoice } from "./_allocations.js";
import { fireJobStatusWebhooks, fireServiceCallWebhook } from "./_job-webhooks.js";
// Creating a job lives in _jobs.js because there are now TWO callers — the New
// Project form and the generator service-call check — and only one of them may
// ever allocate a PO number. See the header of that file.
import { createJobRecord, JobInputError, isJobHandle, jobCreateSource, jobsAreNative } from "./_jobs.js";
import { runGeneratorServiceCheck } from "./_generator-service.js";
// Jobsite photos. Optional infrastructure like _neon.js — see docs/PLAN-job-photos.md.
// Photo storage. netlify/functions/_pcloud.js is deliberately NOT imported —
// pCloud lost the store decision when its app-registration page turned out to
// have been down for months, so no API token could be issued. That file and
// tools/pcloud-*.mjs are kept on disk in case it ever reopens (a mirror-to-
// pCloud option), but nothing calls them. See docs/PLAN-job-photos.md.
// Google People API — contact sync to the two company address books (item 07).
// Optional infrastructure that FAILS SOFT, like _r2.js: unset credentials just
// disable the sync. Nothing here is in ensureEnv(). docs/PLAN-google-contacts.md
import {
  googleStatus, googleConfigured, googleContactsMode,
  getPerson, getPeopleBatch, listConnections, DESTINATIONS,
} from "./_google-contacts.js";

import {
  r2Enabled, r2Status, r2SelfTest, listJobPhotos, presignPut,
  thumbKeyFor, jobPrefix, albumSegment, sanitizeAlbum,
  moveJobPhoto, softDeleteJobPhoto, restoreJobPhoto, purgeJobPhoto,
  listDeletedJobPhotos, listJobDocs,
  jobPrintsPrefix, sanitizePrintName, listJobPrints, listDeletedJobPrints,
  softDeleteJobPrint, restoreJobPrint, purgeJobPrint,
  expensePrefix, listExpenseReceipts, receiptFileKind, summarizeExpenseReceipts,
  softDeleteExpenseReceipt, restoreExpenseReceipt, listDeletedExpenseReceipts, R2Error,
  listByPrefix, liftPrefix, fleetPrefix, presignEquipThumbPut,
  listLiftPhotos, deleteLiftPhoto, deleteAllLiftPhotos,
  presignGet, presignGetDownload, payrollPrefix,
} from "./_r2.js";

/* ============================================================================
 * SECTION MAP — airtable.js  (~3941 lines). Line numbers drift; grep to confirm.
 * Shape: one exported handler() (3831) dispatches on an `action` string —
 * GET reads queryStringParameters.action, POST reads JSON.parse(body).action.
 * Flat `if (action === …)` chains: GET 3839–3877, POST 3882–3932. Unknown → 400.
 * To add an endpoint: write a handleX(), then register it in the right chain.
 *
 * CONVENTIONS (see CLAUDE.md): TABLES maps logical→table name/id; F maps logical→
 * Airtable field NAME and is READ-ONLY (never put an id in F); write sites use
 * field/record ids inline. Linked-record writes = bare ["rec…"] arrays, not
 * [{id}]. escapeFormulaString (301) is the canonical filterByFormula escaper.
 * Never write Make-owned sync fields (Google Contact ID, Sync Status, …).
 * handleGetJobInvoices (3638) is the reference SAFE cross-job pattern (FIND
 * prefilter + verify linked id in memory) — TODO.md lists sites still unsafe.
 *
 * --- INFRASTRUCTURE / HELPERS ---
 *   env + API_ROOT ........... 5     TABLES 9, F (field-name map) 28
 *   TE/PR_RUNS/PR_BONUSES .... 242   field maps for time-entry & payroll tables
 *   resp/ensureEnv ........... 279   escapeFormulaString 301, g/gNum/gBool 305
 *   date + link helpers ...... 336   ymdToDate/dateToYmd/shiftDays, firstLinkedId 355
 *   single-select whitelists . 387   SERVICE_TYPE_OPTS, WARRANTY_TYPE/SOURCE_OPTS
 *   atFetch / fetchAll ....... 405   atFetch 405 (auth+unwrap), fetchAll 418 (paginate)
 *   uploadAirtableAttachment . 694
 *   payroll date helpers ..... 944   computePayrollDateRanges, isPayrollEligibleRole 1016
 *   mapJob ................... 1367  (filtered-vs-unfiltered rollup notes inline)
 *   fleet field maps ......... 1987  FLEET_TABLES/FV/ML; SHOP_ADDRESS 2265
 *   buildWarrantyTemplateFilter 2994 ; SCHED_F 3413
 *
 * --- HANDLERS BY DOMAIN (action → line) ---
 *   AUTH ......... login 1341
 *   JOBS ......... jobs 1513, jobById 1524, createJob 3780, updateJobStatus 1550,
 *                  updateJobInfo 3736, updateJobNotes 3690
 *   TIME ENTRIES . timeEntries 2163, createTimeEntry 495, updateTimeEntry 2151,
 *                  updateTimeEntryPayroll 520, deleteTimeEntry 2144
 *   PAYROLL ...... payrollEntries 437, findMatchingPayrollRun 715, payrollRunCreate 748,
 *                  payrollRunsList 886, payrollHoursRollup 972, payrollBonusesRollup 1025,
 *                  payrollEmployeeBonusHistory 1078, payrollHoursBreakdown 1128,
 *                  myHoursRollup 1212, myHoursBreakdown 1269
 *   EXPENSES ..... expenses 2210, deleteExpense 1607, approveExpense 1614,
 *                  addLiftExpense 2282, addGeneralExpense 2293, calculateMileage 2266,
 *                  unlinkedLaborAllocations 2186, unlinkedMaterialAllocations 2242
 *   ESTIMATES .... jobEstimates 1653, updateEstimate 1729, updateEstimateStatus 1741,
 *                  getNextEstimateNumber 1756, saveEstimate 1789, sentEstimatePDFs 1844
 *                  (helper fetchSentEstimatePDFsForJob 1707), estimateTemplates 1914,
 *                  createJobEstimate 1950
 *   INVOICES ..... saveInvoice 2843, setInvoiceStatus 3360, markInvoicePaid 3374,
 *                  getNextInvoiceNumber 3379, allInvoices 3570, getJobInvoices 3638
 *   POWER CO ..... updatePowerCo 1570, getPowerCompanies 2667,
 *                  getContactsForPowerCompany 2681, createPowerCompany 2728,
 *                  createPowerContact 2760
 *   INSPECTIONS .. jobInspections 1629, createInspection 1640, updateInspection 2317,
 *                  updateJobInspection 3707, getInspectionAgencies 2542,
 *                  createInspectionAgency 2555, inspectorsForAgency 2587,
 *                  createInspectionContact 2632
 *   FLEET ........ fleetVehicles 1999, fleetServiceHistory 2005, updateFleetVehicle 2016,
 *                  logMileage 2034, addFleetService 2075, updateFleetService 2096,
 *                  deleteFleetService 2116
 *   LIFTS ........ scissorLifts 2123, scissorLiftsByJob 1621, updateScissorLift 2129
 *   GENERATOR .... generator 1532, getWarrantyTemplates 3016, getWarranties 3046,
 *                  addWarranty 3092, addGeneratorService 2936, commissionGenerator 3141
 *   SCHEDULE ..... scheduleEntries 3423, addScheduleEntry 3494, updateScheduleEntry 3518,
 *                  deleteScheduleEntry 3540, schedulingCrew 3553
 *   VENDORS/CONTACTS  vendors 2341, createVendor 2374, companies 2327,
 *                  listContractors 2432, listContactsByCompany 2463, createContact 2506
 *   BILLABLE RATES  laborBillableRates 2795, updateJobBillableRate 2818
 *   SERVICE CALLS . startServiceCall 1593, completeServiceCall 1600
 * ========================================================================== */

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
    // ⚠ FIXED 2026-08-08 — this said "Email", and no such column exists on the
    // Employees table. `f["Email"]` was undefined, normalize() turned it into
    // "", and since `identifier` is required non-empty the email branch of
    // handleLogin could never match. **Logging in by email had never worked.**
    // (No employee has an address on file yet either, so nothing changes today
    // beyond the code no longer being a lie.)
    email:    "Primary Email",
    pin:      "PIN",
    // `Role` is the single source of truth. There is also a `Role New` column,
    // which inventory.js used to prefer — but its options are only
    // employee/admin/viewer, with **no `office`**, so preferring it would
    // silently demote the two office users. Both apps now read `Role` only,
    // and the People editor blanks `Role New` on save so it can't drift back.
    role:     "Role",
    active:   "Active"
  },
  job: {
    name:                    "Job Name",
    po:                      "Job PO",
    status:                  "Job Status",
    type:                    "Job Type",
    // Populated on 100% of jobs (verified 2026-07-31 across all 109). Backs the
    // job-list year filter, which defaults to the current year.
    year:                    "Job Year",
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
    // REMOVED 2026-08-05: `wireLink` / `pipeLink`. Both named Airtable fields
    // ("Wire (Mobile) or THHN (Mobile)", "Add Pipe (Mobile)") no longer exist —
    // the JotForm wire/pipe capture was retired in favour of the inventory app's
    // expense push. mapJob returned null for both on every job and neither SPA
    // ever read them. Found when the jobs sync requested them by name and
    // Airtable 422'd UNKNOWN_FIELD_NAME.
    // Legacy JotForm/pCloud URL fields. Still populated for Airtable users who
    // click them there; the app no longer depends on them for photos.
    addPhotosLink:           "Add Photos (Mobile)",
    viewPhotosLink:          "View pCloud Photos",
    // The pCloud folderid for this job's photos — a stable numeric id, and the
    // ONLY safe way to address the folder. The retired Make scenario rebuilt a
    // path from five editable fields plus the current year instead, which is
    // why it died with "[2005] Directory does not exist". Never reconstruct a
    // path; read this.
    pcloudPhotoFolderId:     "pCloud Photo's ID",
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
    birdDate:            "Bird Date",
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
  reviewed:   "fldQn7d06doEkrGBv",
  // Job Name (Text) — the STATIC job-name snapshot. Written on create only, never
  // on update (re-pointing a link must not rewrite an imported name).
  // It was missing from the Step 2 mirror, which is not merely untidy: the Airtable
  // fallback in handleJobTimeEntries PREFILTERS on FIND(name, {Job Name (Text)}),
  // and a blank can never match a FIND — so a Neon-native entry vanished from the
  // job's Time Entries tab entirely whenever that fallback served. Found by the
  // reconciler 2026-08-06.
  jobName:    "fldsemB5S5PivoZjd"
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
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      ...(extraHeaders || {})
    },
    body: JSON.stringify(body)
  };
}

function ensureEnv() {
  if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID)
    throw new Error("Missing env vars AIRTABLE_API_KEY or AIRTABLE_BASE_ID");
  if (!process.env.AUTH_SECRET)
    throw new Error("Missing env var AUTH_SECRET");
}

// ── Authorization policy ────────────────────────────────────────────────────
// Returns the allowed-role array for (method, action), or null = "any signed-in
// role" (general reads). Role tiers mirror the client gating, enforced here so
// the browser can no longer be trusted:
//   PAYROLL      admin+employee — matches isPayrollEligibleRole / .payroll-eligible-only
//   ADMIN_OFFICE admin+office   — back-office money ops (office acts like admin here)
//   ADMIN        admin only     — scheduling (.strict-admin-only), payroll runs, dev tools
//   NON_VIEWER   admin+office+employee (default for writes) — viewer is read-only
// Conservative first pass: it locks out viewers from writes, gates payroll, and
// admin-gates the high-risk ops, without over-restricting employees' field work.
// Tighten specific actions later once real-world usage is observed.
const _PAYROLL      = ["admin", "employee"];
const _ADMIN_OFFICE = ["admin", "office"];
const _ADMIN        = ["admin"];
const _NON_VIEWER   = ["admin", "office", "employee"];

const _PAYROLL_READS = new Set([
  "payrollEntries", "findMatchingPayrollRun", "payrollRunsList",
  "payrollHoursRollup", "payrollHoursBreakdown", "payrollBonusesRollup",
  "payrollEmployeeBonusHistory", "myHoursRollup", "myHoursBreakdown",
  "hoursByJob",
  // Your own PTO balance and your own requests — the employee is taken from the
  // token inside the handler, so admin+employee is the whole audience.
  "ptoBalance",
  // The clock reads your own punches and nobody else's — the employee is taken from
  // the token inside the handler, so _PAYROLL (admin + employee) is the whole
  // audience. Office and viewers have no punches to read.
  "clockStatus",
]);
const _READ_LIKE_POSTS = new Set([
  "getNextEstimateNumber", "getNextInvoiceNumber", "getJobInvoices", "calculateMileage",
]);
const _TIME_SELF_WRITES = new Set([
  "createTimeEntry", "updateTimeEntry", "deleteTimeEntry",
  // Punching is writing your own time, which is exactly what this tier is for. The
  // handlers narrow it further while the feature is being built (TIME_CLOCK=admin),
  // and they take the employee from the token, so nobody can punch anybody else.
  "clockIn", "clockOut", "clockBreak",
  // Changing class mid-shift without leaving the job — travel, then arrive.
  "clockSwitch",
  // Correcting your own start/stop times. The handler enforces own-until-counted
  // (the same rule as expenses); admins may correct anyone's.
  "clockEditTimes",
  // Removing a shift that shouldn't exist. Own-or-admin, enforced in the handler.
  "clockDeletePunch",
  // Asking for time off, and withdrawing your own request. Both take the employee
  // from the token; approving is a separate _ADMIN action.
  "requestPto", "cancelPtoRequest",
  // Minting your OWN home-screen widget link. Self-service — the employee comes
  // from the token, so nobody can create a link that watches somebody else.
  "widgetLink",
]);
const _ADMIN_POSTS = new Set([
  "updateTimeEntryPayroll", "payrollRunCreate",
  // ⚠ Deleting a job estimate has NO STATUS GUARD — owner's explicit call
  // 2026-08-20 — so a Sent or Approved estimate can be erased. Strict admin
  // rather than admin+office precisely because this tier IS the guard; office
  // handles money already earned, not the destruction of what was quoted.
  "deleteJobEstimate",
  "addScheduleEntry", "updateScheduleEntry", "deleteScheduleEntry",
  // One-off migration action, admin only. Gated by role rather than by
  // ADMIN_BACKFILL_TOKEN: it is idempotent, copies rather than mutates, and the
  // token is itself a write-only Netlify secret nobody has a copy of.
  "copyLiftPhotosToR2", "copyFleetPhotosToR2", "copyEstimatePdfsToR2",
  // Same shape again, for the payroll archive (db/schema/052). Admin rather
  // than admin+office even though it only copies: it reads every payroll PDF in
  // the company, and payroll is the one tier office does not get.
  "copyPayrollFilesToR2",
  // Same shape: the Contacts loader for db/schema/048. It has to run in here
  // rather than from a laptop because the LOCAL Airtable PAT is scoped to the
  // sandbox base and 403s on production — only the function holds a prod key.
  "backfillContacts",
  // Opens service-call jobs for generators that have come due. Admin because it
  // CREATES JOBS and each one consumes a PO number that cannot be handed back —
  // and because its dry run is how you decide whether to flip
  // GENERATOR_SERVICE_CALLS on at all. Also runs unattended on the hourly
  // schedule; this action is the manual/preview door onto the same code.
  "generatorServiceCheck",
  // Merging duplicate contacts. Admin because it is the only action here that
  // DELETES a contact row, and because its input is a human decision that no
  // ranking can make — see the Mike Ware case in handleContactMerge.
  "contactMerge",
  // Turning a person's app access on or off. Admin only — this is the action
  // that kills live sessions, and office manages money, not access.
  "setEmployeeActive",
  // Deciding whether someone is paid a salary or hourly-with-overtime. That is a
  // pay decision, so it sits with payroll runs at admin — office manages money
  // that has already been earned, not what somebody earns.
  "setEmployeeSalaried",
  // Resetting a credential, and it signs the person out. Admin only.
  "setEmployeePin",
  // Editing a person includes their ROLE, which is an authorization change.
  "updateEmployee",
  // Creating a person mints a working login. Rate writes move GP on every job
  // that person has ever booked hours to — as money-critical as it gets here.
  "createEmployee", "addEmployeeRaise", "correctEmployeeRate",
  // Counting previously-uncounted punches as payroll hours. Admin only — this is the
  // action that turns the time clock into money.
  "promoteClockPunches",
  // Punching somebody ELSE in or out. Deliberately separate actions from clockIn/
  // clockOut so the self-service path keeps the property that the employee can only
  // come from the token — the privilege is what's gated, not a parameter.
  "adminClockIn", "adminClockOut",
  // Granting time off, and setting how much someone gets. Both turn into paid
  // hours, so admin only — office manages money, not people's leave.
  "decidePtoRequest", "setPtoAllowance",
  // Booking PTO for someone directly, for time off that already happened.
  "adminAddPto",
  // Both create paid hours or next year's entitlement in bulk. Admin only, and
  // both refuse to run without an explicit confirmation.
  "fillHolidays", "ptoRollover",
]);
const _ADMIN_OFFICE_POSTS = new Set([
  // NOTE: deleteExpense is intentionally NOT here — it now defaults to
  // _NON_VIEWER and handleDeleteExpense enforces owner+unreviewed for
  // employees (admin/office may delete any). updateExpense likewise defaults
  // to _NON_VIEWER with in-handler owner/status enforcement.
  "approveExpense", "markInvoicePaid", "setInvoiceStatus",
  "updateJobBillableRate", "createVendor",
  // Adding a contractor/customer to the master list. Same tier as createVendor
  // for the same reason: it is reference data the whole business bills against,
  // not a field action.
  "createCompany",
  // Estimate templates carry the base price, labor hours and material cost that
  // seed a customer quote. Editing one is a back-office money op in the same
  // sense as updateJobBillableRate — the numbers land in front of a customer —
  // so it sits here rather than at the _NON_VIEWER default a write would
  // otherwise get. Reading them stays open to any signed-in role: the picker
  // renders inside the New Job Estimate modal, which is already admin-gated in
  // the UI, and a read leaks nothing a job's own estimate does not.
  // Hard-deleting one sits at the same tier as deleting a photo: irreversible
  // for a natively-created template (no Airtable copy to recover from), but
  // it cannot alter a single figure on an existing quote, because a template's
  // numbers are snapshotted into the estimate at create time.
  "estimateTemplateSave", "estimateTemplateArchive", "estimateTemplateDelete",
  // Which city tax applies to a job's work. Same tier as the billable rate: a job
  // setting that moves money, so admin+office, not the whole crew.
  "updateJobCityTax",
  // Whether a job shows on the clock. Same tier — it decides where people's hours
  // can land, which is a money question even though it looks like a display one.
  "updateJobClockVisibility",
  // Lifts became employee-editable on 2026-08-03 (see the fleet parity change),
  // so updateScissorLift stays _NON_VIEWER — a crew marks a lift on/off a job.
  // But adding equipment to the books, and RETIRING a sold one along with its
  // photos, is not a field action. Same for removing a photo.
  "createScissorLift", "deleteScissorLift", "deleteLiftPhoto",
  // Adding or replacing an equipment photo, and backfilling a thumbnail for one
  // that predates them. All three write objects into the bucket, so they sit
  // with the other equipment-photo writes rather than at _NON_VIEWER.
  "fleetPhotoUploadUrl", "equipThumbUploadUrl",
  // Job notes became readable by every role on 2026-07-31 so crews get job
  // instructions. WRITING them stays admin/office: they can still carry
  // internal status, and until now the UI was the only thing stopping an
  // employee from POSTing to this action (it defaulted to _NON_VIEWER).
  "updateJobNotes",
  // Photo deletion is irreversible (no bucket versioning) and nothing records
  // who took a photo, so the expense-style "own until reviewed" rule can't
  // apply. Moving between albums is NOT here — it's reversible, so any
  // non-viewer may re-file.
  "deleteJobPhotos", "restoreJobPhotos", "purgeJobPhotos",
  // Prints: any non-viewer may UPLOAD one (jobPrintUploadUrls is deliberately
  // not here — a crew member photographing a marked-up sheet is the feature),
  // but removing one is manager-only. A crew that arrives to find the drawing
  // gone cannot do the job, and purge is genuinely unrecoverable.
  "deleteJobPrints", "restoreJobPrints", "purgeJobPrints",
  // Receipts are financial records and there is no "reviewed" state to key an
  // employee self-service window off, so deletion is manager-only.
  "deleteExpenseReceipts", "restoreExpenseReceipts",
  // Deleting a whole panel schedule takes every circuit with it and there is no
  // bin. Creating and filling one in stays _NON_VIEWER — the electrician at the
  // panel is the person who knows what circuit 23 feeds, and gating that on
  // admin means it never gets written down.
  "deletePanelSchedule",
  // Same split for checklists: deleting the whole LIST takes every item with it,
  // so it's manager-only. Adding, ticking and removing a single item are all
  // _NON_VIEWER — the crew keeps the list, that's the point of it.
  "deleteChecklist",
]);

// NOTE: there was a `_GRANT_AUTH_ACTIONS` bypass here, letting the pCloud
// image proxy authenticate from a signed URL because an <img> can't send an
// Authorization header. It is GONE — R2 presigned URLs solve the same problem
// without the function serving bytes, so no action needs to skip the bearer
// check. Don't reintroduce a bypass without a case that genuinely can't be
// served by a presigned URL. (signScope/verifyScope remain in _auth.js.)

// Admin-only GET reads. Diagnostics belong here: r2Status reports which env
// var is wrong and echoes R2's error text, which is exactly the kind of detail
// that shouldn't be readable by every signed-in field tech.
// Strict-admin reads. Diagnostics only — r2Status echoes bucket/account detail
// and R2's own error text, which no field tech needs.
// `people` is admin-only, NOT admin+office: the roster carries each person's
// true cost rate. Office is deliberately excluded from wages.
// `employeePin` returns a live credential — strict admin, never office, and
// deliberately its own action rather than a field on `people` so one tap
// reveals one person instead of shipping every PIN on every screen open.
// `clockRoster` is strict admin, NOT admin+office, for the same reason `people`
// is: it reports where every person is right now, and it backs a screen that can
// start and stop their paid time. Office is excluded from payroll throughout.
// `clockReconcile` compares everyone's hours across two systems — a payroll-wide
// read, so it sits with the roster at strict admin.
const _ADMIN_READS = new Set(["r2Status", "jobCreateStatus", "integrityCheck", "people", "employeePin", "employeeRates",
                              // Item 07 diagnostics. googleContactsReconcile reads every
                              // contact in both company address books, so it sits at strict
                              // admin with the roster rather than with office.
                              "googleStatus", "googleContactsReconcile", "contactDuplicates",
                              "clockRoster", "clockReconcile", "clockPunches",
                              // The approval queue + everyone's leave balances.
                              "ptoRequests"]);

// Admin+office reads. These mirror write tiers that are already _ADMIN_OFFICE,
// so listing must match the actions available on what's listed:
//   jobPhotosDeleted - the recycle bin, whose restore/purge are admin/office
//   jobDocs          - the materials PDF, which itemises unit costs and job
//                      totals. handleExpenses already scopes employees to their
//                      own submissions and hides job totals; this must not
//                      become the back door around that.
//   jobPrintsDeleted - the prints bin, whose restore/purge are admin/office
//
// `jobPrints` itself is deliberately ABSENT from every set here, so authzFor
// returns null and any signed-in role may read it. That is the entire feature:
// a crew opening the drawings on site without a pCloud login. If you find
// yourself adding it here, you have confused prints with jobDocs.
//
// `panelSchedules` / `panelSchedule` are absent for the same reason: the person
// who needs to read what circuit 23 feeds is standing at the panel. So are
// `jobChecklists` / `jobChecklist` — a crew loading the truck at 6am is the
// whole audience for a supply list.
//
// `estimateTemplatesAll` backs the template MANAGER, so it matches the tier of
// the writes it exists to feed (estimateTemplateSave / estimateTemplateArchive)
// — it returns internal notes and archived templates, neither of which the
// picker shows. `estimateTemplates`, the picker read itself, is deliberately
// ABSENT: it renders inside the New Job Estimate modal and returns the same
// scope and terms text that ends up on the estimate anyway.
const _ADMIN_OFFICE_READS = new Set([
  "jobPhotosDeleted", "jobDocs", "jobPrintsDeleted", "estimateTemplatesAll",
]);

function authzFor(method, action) {
  if (method === "GET") {
    if (_ADMIN_READS.has(action))        return _ADMIN;
    if (_ADMIN_OFFICE_READS.has(action)) return _ADMIN_OFFICE;
    return _PAYROLL_READS.has(action) ? _PAYROLL : null;
  }
  // POST
  if (_READ_LIKE_POSTS.has(action))    return null;
  if (_TIME_SELF_WRITES.has(action))   return _PAYROLL;
  if (_ADMIN_POSTS.has(action))        return _ADMIN;
  if (_ADMIN_OFFICE_POSTS.has(action)) return _ADMIN_OFFICE;
  return _NON_VIEWER; // all other writes: any signed-in non-viewer
}

// Parse the POST body's action without throwing on malformed JSON.
function safeBodyAction(event) {
  try { return event.body ? JSON.parse(event.body).action : undefined; }
  catch { return undefined; }
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
// Coerces an Airtable FORMULA field to a boolean. Used only by the five
// "All … Reviewed?" gates on Jobs, which drive the Closeout tab's checkboxes.
//
// ⚠ FIXED 2026-08-05 — these checkboxes had NEVER worked. The formulas return
// "✅ Yes" / "⚠️ Pending Review", but this function only accepted a bare "yes",
// "true" or "1". "✅ Yes" lowercases to "✅ yes", matched nothing, and so all five
// keys were false on every job forever. Measured before the fix: 102 of 112 jobs
// were materials-reviewed, 104 expenses-reviewed and 66 labor-reviewed, while the
// app rendered every box unchecked.
//
// The emoji is part of the value, not decoration — do not strip it and compare.
// "Pending" is tested FIRST so a future "⚠️ Pending — Yes 3 of 4"-style string
// cannot read as true off a loose suffix match.
function gFormulaBool(fields, fieldName) {
  const v = fields[fieldName];
  if (typeof v === "boolean") return v;
  if (typeof v === "number") return v !== 0;
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (!s) return false;
    if (s.includes("pending") || s.includes("⚠")) return false;
    return s === "1" || s === "true" || s === "yes" || s.includes("✅") || s.endsWith(" yes");
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

// Jobs."Contractor (Intake)" — the legacy text breadcrumb the job create keeps
// populated for downstream Make readers. It is a **singleSelect**, and the create
// POST runs with typecast off, so writing a name that is not on the list does not
// silently add an option — it 422s the ENTIRE job create.
//
// That mattered the moment `createCompany` shipped: every one of the 24 active
// contractors happens to be on the list today, so nothing has ever hit it, but
// the first brand-new contractor would have made their first job impossible to
// create. The guard omits the field instead, which is the same "safe fallback"
// the other OPTS arrays use — the linked `Contractor` field is the real data and
// is unaffected.
//
// ⚠ The array itself MOVED to `netlify/functions/_jobs.js` on 2026-08-21, next
// to the only code that reads it. This note stays because the trap belongs with
// the other OPTS whitelists, not because the list is still here. Do NOT fix the
// spellings ("Milla Construcion", "Kalmback") — they are the configured option
// names, same lesson as PR_CITY_TAXES.

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

// ⚠ MUST STAY IDENTICAL TO PR_CITY_TAXES IN index.html, character for character.
// These strings carry QuickBooks Time's OWN spellings — "Massilon", "New
// Philadephia", "N Canton", and "Hayesville" with no "Tax" suffix. They are wrong
// as English and correct as data: the value is stored as free text on a time entry
// and anything that doesn't match verbatim silently falls back to "A No Tax"
// downstream. Do NOT tidy the spellings on either side.
const PR_CITY_TAX_OPTS = [
  "A No Tax", "Alliance Tax", "Amherst Tax", "Ashland City Tax", "Austintown Tax",
  "Canton Tax", "Carrollton City Tax", "Cleveland Tax", "Columbiana Tax",
  "Cuyahoga Falls Tax", "Dennison City Tax", "Grafton Tax", "Green Tax",
  "Hartville Tax", "Hayesville", "Madison City Tax", "Massilon Tax", "Medina Tax",
  "Millersburg City Tax", "Minerva Tax", "N Canton", "New Philadephia",
  "Orrville City Tax", "Rita Tax", "Salem Tax", "Sebring Tax", "Steubenville Tax",
  "Streetsboro Tax", "Strongsville Tax", "Utica Tax", "Wadsworth Tax", "Akron Tax",
  "Other",
];

// Warranties.Source — fallback "Standard" is the default for warranties
// created from manufacturer templates at commissioning time.
const WARRANTY_SOURCE_OPTS = ["Standard", "Extended Purchase", "Promotional", "Transferred"];

async function atFetch(path, options = {}) {
  ensureEnv();
  // See _airtable-write-guard.js: a uuid in a linked-record field does not fail,
  // it FABRICATES the record. Every write in this file passes through here.
  if (airtableWriteBlocked(path, options)) return SKIPPED_WRITE;
  options = scrubFabricatingLinks(path, options);
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
  // Returns records keyed by FIELD ID instead of name. Needed only for the
  // Make-owned Google sync fields, which have no name this codebase has ever
  // verified — `F.*` is read-by-name and would be guessing. Field ids are also
  // the stabler handle: a rename in Airtable breaks a name, never an id.
  if (opts.byFieldId) params.set("returnFieldsByFieldId", "true");
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
// NEON-FIRST since the step-2 cutover; Airtable is the fallback.
//
// `id` MUST stay the Airtable record id: the payroll UI edits and deletes through
// it, and the write handlers address Airtable by rec id. Neon rows carry it in
// `airtable_id`, kept current by the linker in db/etl/time-entries-full.mjs — the
// puller creates a row within the hour while Make writes Airtable at 21:00, so
// there is a window where a row exists in both systems with neither knowing the
// other's id. `unlinked` in the response counts any such rows in the window: they
// display with correct hours but cannot be edited until the linker runs. That is
// visible rather than silent, and self-heals on the next reconcile.
async function handlePayrollEntries(params) {
  const { startDate, endDate } = params || {};
  if (!startDate || !endDate) return resp(400, { ok: false, error: "Missing startDate or endDate." });

  if (neonEnabled()) {
    // Same 14-day window the Airtable path builds, expressed as a range.
    const q = await neonQuery(
      `SELECT t.id::text              AS id,
              t.airtable_id            AS airtable_id,
              t.employee_name          AS employee,
              e.airtable_id            AS employee_id,
              t.work_date::text        AS work_date,
              t.duration_seconds::float8 AS duration,
              t.hours::float8          AS hours,
              t.city_taxes, t.class,
              j.airtable_id            AS job_id,
              t.job_name, t.labor_reviewed
         FROM time_entries t
         LEFT JOIN employees e ON e.id = t.employee_id
         LEFT JOIN jobs      j ON j.id = t.job_id
        WHERE t.work_date >= $1::date AND t.work_date <= ($1::date + 13)
        ORDER BY t.work_date ASC`,
      [startDate]
    );

    if (q?.rows) {
      const entries = q.rows.map(r => ({
        id:         r.id,                       // Neon uuid — always present
        employee:   r.employee || "",
        employeeId: r.employee_id || null,
        workDate:   r.work_date || "",
        duration:   Number(r.duration) || 0,
        hours:      Number(r.hours) || 0,
        cityTaxes:  r.city_taxes || "A No Tax",
        class:      r.class || "",
        jobId:      r.job_id || null,
        jobName:    r.job_name || "",
        reviewed:   r.labor_reviewed === true,
      }));
      // `unlinked` is now a HEALTH SIGNAL, not a functional limitation. It used to
      // mean "uneditable", because the id the UI edits by was the Airtable one and
      // these rows had none. Editing is keyed on the Neon uuid now, so an unlinked
      // row behaves normally and this only reports how far the Airtable mirror is
      // behind. Expect it to be permanently non-zero after Step 3 retires Make.
      const unlinked = q.rows.filter(r => !r.airtable_id).length;
      if (unlinked) console.warn(`payrollEntries: ${unlinked} row(s) not yet mirrored to Airtable`);

      // Who is on a salary. Returned as NAMES because the payroll screen and the
      // payroll PDF key every block, total and colour by employee name — this
      // replaces a hardcoded constant in index.html, not a lookup the client
      // already has. See db/schema/031.
      //
      // ⚠ ABSENT and EMPTY mean different things, deliberately. A failed query
      // omits the key entirely and the client falls back to its old hardcoded
      // list, i.e. today's behaviour. An empty ARRAY is authoritative — it means
      // nobody is salaried, which is a legitimate answer once the toggle exists.
      // Collapsing the two would either make the toggle unable to clear the last
      // salaried person, or let one bad query quietly pay the owners overtime.
      const sq = await neonQuery(`SELECT name FROM employees WHERE salaried`);
      const salaried = sq?.rows ? sq.rows.map(r => r.name).filter(Boolean) : null;
      if (!sq?.rows) console.error(`payrollEntries: salaried lookup failed, client will use its fallback list: ${sq?.error || "no rows"}`);

      return resp(200, { ok: true, entries, _source: "neon", _ms: q.ms,
        ...(unlinked ? { unlinked } : {}), ...(salaried ? { salaried } : {}) });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`payrollEntries: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  return payrollEntriesFromAirtable(startDate, endDate);
}

async function payrollEntriesFromAirtable(startDate, endDate) {

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

  return resp(200, { ok: true, entries, _source: "airtable" });
}

// ── NEON-FIRST time-entry writes (migration Step 2) ────────────────────────
// Neon is the SOURCE OF TRUTH for time entries. All four write paths below write
// Neon FIRST and treat Airtable as the mirror — the exact reverse of the mirror
// that shipped 2026-07-30, which wrote Airtable first and copied into Neon.
//
// WHY THE ORDER HAD TO INVERT: under the old arrangement a failed Airtable write
// meant the entry reached NEITHER system. Survivable while Airtable was
// authoritative; not survivable now that every payroll read is served from Neon.
// It also stops working entirely at Step 3 — once Make is retired, nothing creates
// the Airtable row, so any design that needs one breaks the day Make is switched off.
//
// WHY THESE FAIL CLOSED: see neonWrite in _neon.js. Reads fail soft, writes do not.
// A write that succeeds in Airtable but fails in Neon is invisible to the app.
//
// Airtable is still written, for as long as it is the read fallback and Make is
// alive. A failed Airtable mirror is logged and swallowed: the row is already safe
// in Neon with correct hours and shows on the payroll screen, it simply carries
// airtable_id NULL until the reconciler's linker matches it — the same `unlinked`
// state the system already understands and reports.

// Resolves the client's entry handle to BOTH keys. Reads return the Neon uuid, but
// the Airtable read fallback still returns `rec…` ids BY DESIGN, so both forms have
// to keep resolving for as long as that fallback exists. This is not a transition
// shim and should not be removed when the payroll screen has been reloaded.
// `id::text = $1` rather than `$1::uuid` because a `rec…` string would raise a cast
// error rather than simply failing to match.
async function resolveTimeEntry(entryId) {
  const rows = await neonWrite("timeEntry.resolve",
    `SELECT id, airtable_id FROM time_entries
      WHERE id::text = $1 OR airtable_id = $1
      LIMIT 1`,
    [String(entryId)]);
  return rows?.[0] || null;
}

// Best-effort Airtable mirror. Never throws — the authoritative write already
// landed in Neon, so an Airtable problem must not fail the user's request. This is
// the same fail-soft contract the Neon mirror used to have, pointed the other way.
// ⚠⚠ A LINKED-RECORD FIELD TAKES A REC ID OR NOTHING — NEVER A UUID.
// Added 2026-08-25, after a native job's invoice mirror did exactly that. Every
// one of these POSTs carries `typecast: true`, and typecast does not reject an
// unrecognised link value: it CREATES the record. Saving one invoice on Test 10
// therefore created an Airtable Job literally named
// "846245ef-294f-423b-a2b1-4b4a919607f8", which `_jobs-sync` would have imported
// as a real job at the top of the hour — and whose display name Airtable's
// Invoice Number formula then wrote back over the invoice as
// "846245ef-…-001".
//
// Returns an object to spread, so the call site stays one line and the field is
// ABSENT (not null, not []) when there is no rec id to link. Omitting it is the
// whole point: the Neon row already carries the real job_id, and the mirror is a
// courtesy copy that nothing reads back.
function jobLink(fieldKey, jobId) {
  const s = String(jobId || "").trim();
  return s.startsWith("rec") ? { [fieldKey]: [s] } : {};
}

async function mirrorToAirtable(label, fn) {
  try {
    return await fn();
  } catch (e) {
    console.error(`mirrorToAirtable ${label} failed (ignored): ${e?.message || e}`);
    return null;
  }
}

// ⚠ `job_name` IS A SNAPSHOT — set it on INSERT, never recompute it on UPDATE.
// It records the jobcode name as it stood when the entry was imported. 643 of the
// 3,417 linked rows already disagree with their linked job's current `po_locked`:
// PO corrections (MEC 389 → MEC 398), typo fixes (Jeanie → Jeannie), stray double
// spaces, and a handful where the Job link was deliberately re-pointed to a
// different job while the text was left as originally imported. Recomputing it
// would silently rewrite history on rows people have already been paid against.

// ── PAYROLL: create new time entry ─────────────────────────────────────────
async function handleCreateTimeEntry(body) {
  const { employee, employeeId, workDate, duration, class: cls, cityTaxes, jobId } = body || {};
  if (!employee || !workDate) return resp(400, { ok: false, error: "Missing employee or workDate." });

  const durationSecs = Math.round(Number(duration) || 0);
  const klass  = cls || "Contract";
  const taxes  = cityTaxes || "A No Tax";
  // ⚠ These two KEEP the bare `startsWith("rec")` test, and deliberately — they
  // are not validating the handle, they are extracting the AIRTABLE rec id for
  // the Airtable mirror's linked-record fields. A native employee (slice 5) or
  // a native job (slice 6) genuinely has no rec id to put there, so `null` is
  // the correct answer and the field is simply omitted from the mirror.
  //
  // ⚠ This is the one place the "a guard doesn't 404, it silently DROPS the
  // field from the mirror" note does NOT apply. That warning is about skipping
  // an id that could have been resolved; here there is nothing to resolve to.
  // The Neon insert below takes the handle in either form.
  const jobRec = (jobId      && String(jobId).startsWith("rec"))      ? String(jobId)      : null;
  const empRec = (employeeId && String(employeeId).startsWith("rec")) ? String(employeeId) : null;

  // ⚠⚠ AND THESE TWO ARE FOR THE MIRROR ONLY. The INSERT below gets the RAW
  // handles, because its resolves are `airtable_id = $n OR id::text = $n` and
  // feeding them the rec-stripped value hands a dual-handle clause a NULL.
  //
  // That is precisely what shipped: `jobRec` was passed as $7, so adding hours
  // to a NATIVE job wrote job_id NULL and job_name NULL. The row saved, payroll
  // paid it, and it vanished from the job's Time Entries tab — which INNER JOINs
  // jobs — with no error anywhere. Reported live on Test 10, 2026-08-25.
  //
  // ⚠ The lesson is narrower than "use the dual handle" and worth stating on its
  // own: THE CLAUSE WAS ALREADY CORRECT. A dual-handle resolve fed a single-handle
  // parameter is indistinguishable, at the SQL, from a job that does not exist.
  // Check what reaches $n, not just what $n is compared against.
  const jobHandle = jobId      ? String(jobId).trim()      : null;
  const empHandle = employeeId ? String(employeeId).trim() : null;

  // NEON FIRST — this row is the record from here on, whatever Airtable does.
  // `source = 'Manual'` is not decoration: the te_has_a_key CHECK requires a row to
  // name its origin, and a Neon-native row has neither an Airtable nor a QB id at
  // insert time. It is also what tells the reconciler this row is app-owned.
  // job_name comes from the job's CURRENT po_locked because that is what an import
  // today would record — see the snapshot warning above.
  const rows = await neonWrite("timeEntry.insert",
    `INSERT INTO time_entries
       (employee_name, employee_id, work_date, duration_seconds, city_taxes, class,
        job_id, job_name, labor_reviewed, source)
     VALUES ($1,
             (SELECT id FROM employees WHERE airtable_id = $2 OR id::text = $2),
             $3::date, $4::numeric, $5, $6,
             (SELECT id        FROM jobs WHERE airtable_id = $7 OR id::text = $7),
             (SELECT po_locked FROM jobs WHERE airtable_id = $7 OR id::text = $7),
             false, 'Manual')
     RETURNING id, job_name`,
    [employee, empHandle, workDate, durationSecs, taxes, klass, jobHandle]);
  const neonId = rows?.[0]?.id;
  // Returned from the INSERT rather than re-derived in JS, so the mirror carries
  // BYTE-IDENTICAL text to the authoritative row. Re-querying po_locked here would
  // introduce a second source for the same string, which is how the two sides drift.
  const jobName = rows?.[0]?.job_name || null;

  const fields = {};
  fields[TE.employee]   = employee;
  if (empRec) fields[TE.employeeLink] = [empRec];
  fields[TE.workDate]   = workDate;
  fields[TE.duration]   = durationSecs;
  fields[TE.class]      = klass;
  fields[TE.cityTaxes]  = taxes;
  if (jobRec) fields[TE.jobLink] = [jobRec];
  if (jobName) fields[TE.jobName] = jobName;

  const data = await mirrorToAirtable("createTimeEntry", () =>
    atFetch(`${encodeURIComponent(TABLES.timeEntries)}`, {
      method: "POST",
      body: JSON.stringify({ fields, typecast: true })
    }));

  // Stamp the Airtable id back so the two systems agree on this row's identity.
  // If the mirror failed, the row simply stays unlinked and the linker picks it up.
  if (data?.id && neonId) {
    await mirrorToAirtable("createTimeEntry.stamp", () =>
      neonWrite("timeEntry.stampAirtableId",
        `UPDATE time_entries SET airtable_id = $2 WHERE id = $1`, [neonId, data.id]));
  }
  // `id` is now the NEON uuid. The client treats it as opaque and hands it back on
  // edit, where resolveTimeEntry accepts either form.
  return resp(200, { ok: true, id: neonId, airtableId: data?.id || null });
}

// ── PAYROLL: update time entry (duration + other fields) ───────────────────
async function handleUpdateTimeEntryPayroll(body) {
  const { entryId, duration, workDate, class: cls, cityTaxes, jobId, reviewed } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const target = await resolveTimeEntry(entryId);
  if (!target) return resp(404, { ok: false, error: "Time entry not found." });

  // Only the fields the client actually sent go into the SET list, so an omitted
  // field is left alone rather than nulled. Note job_name is absent even when jobId
  // changes — re-pointing a link does not rewrite the imported name (snapshot rule).
  const sets = [], vals = [target.id];
  const put = (col, v, cast = "") => { vals.push(v); sets.push(`${col} = $${vals.length}${cast}`); };
  if (duration  !== undefined && duration !== null) put("duration_seconds", Math.round(Number(duration)), "::numeric");
  if (workDate  !== undefined) put("work_date", workDate, "::date");
  if (cls       !== undefined) put("class", cls);
  if (cityTaxes !== undefined) put("city_taxes", cityTaxes);
  if (reviewed  !== undefined) put("labor_reviewed", reviewed === true);
  if (jobId !== undefined) {
    // ⚠⚠ THE HANDLE, NOT THE REC ID — cutover slice 6. This used to narrow the
    // value to `startsWith("rec") ? jobId : null` and then resolve on that, so
    // re-pointing an hour at a NATIVE job resolved to NULL and **silently
    // unlinked the entry from every job** — the hours stay, and they vanish from
    // the job's Time Entries tab (which INNER JOINs `jobs`) and from its labor
    // cost. Exactly the shape `_jobs-sync.js` was written to repair.
    //
    // The Airtable mirror below still narrows to a rec id, and correctly: a
    // native job has none, so the link field is cleared there. Neon is the store
    // that has to be right.
    vals.push(jobId ? String(jobId) : null);
    sets.push(`job_id = (SELECT id FROM jobs WHERE airtable_id = $${vals.length} OR id::text = $${vals.length})`);
  }
  if (!sets.length) return resp(400, { ok: false, error: "Nothing to update." });

  await neonWrite("timeEntry.updatePayroll",
    `UPDATE time_entries SET ${sets.join(", ")} WHERE id = $1`, vals);

  // Airtable mirror. Skipped when the row has no Airtable twin — after Step 3 that
  // is every new row, and it is not an error.
  if (target.airtable_id) {
    const fields = {};
    if (duration  !== undefined && duration  !== null) fields[TE.duration]  = Math.round(Number(duration));
    if (workDate  !== undefined) fields[TE.workDate]  = workDate;
    if (cls       !== undefined) fields[TE.class]     = cls;
    if (cityTaxes !== undefined) fields[TE.cityTaxes] = cityTaxes;
    if (reviewed  !== undefined) fields[TE.reviewed]  = reviewed === true;
    if (jobId !== undefined) {
      fields[TE.jobLink] = (jobId && String(jobId).startsWith("rec")) ? [String(jobId)] : [];
    }
    await mirrorToAirtable("updateTimeEntryPayroll", () =>
      atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${target.airtable_id}`, {
        method: "PATCH",
        body: JSON.stringify({ fields })
      }));
  }

  // Approving hours is what creates the labor billing allocation — the thing
  // that makes those hours billable revenue. Until 2026-08-11 an Airtable
  // automation did it, which is why index.html carried a fallback for the
  // "brief automation lag between Review and allocation row creation". Doing it
  // here, in the same request, removes the lag entirely.
  //
  // Inert until ALLOCATIONS_WRITE=on; see _allocations.js for why it cannot be
  // live at the same time as the automation.
  //
  // Only on the way ON. `reviewed === false` deliberately does NOT delete the
  // allocation: un-reviewing in Airtable never did either, and an allocation
  // already attached to a sent invoice must not vanish because someone
  // unticked a box. The "already-allocated" gate makes re-reviewing a no-op.
  let allocation;
  if (reviewed === true) {
    try {
      // Neon uuid FIRST — it is always present, where airtable_id is NULL on
      // every entry pulled since Step 3. The allocation goes Neon-native when
      // there is no twin, which since 2026-08-10 is all of them.
      allocation = await createLaborAllocation(atFetch, target.id, target.airtable_id);
    } catch (e) {
      // Reported, not thrown. The payroll edit itself succeeded and is what the
      // user asked for; failing the whole request would make the hours look
      // unsaved when they are saved. The hourly billing sync is the net.
      console.error(`updateTimeEntryPayroll: allocation failed — ${e?.message || e}`);
      allocation = { created: 0, error: String(e?.message || e) };
    }
  }
  return resp(200, { ok: true, updatedId: target.id, ...(allocation ? { allocation } : {}) });
}

// ══ TIME CLOCK ═══════════════════════════════════════════════════════════════
// Punch in and out from the phone, against a job. See docs/PLAN-time-clock.md.
//
// TWO SWITCHES, both defaulting to the safe answer, because the owner's decision on
// 2026-08-08 was "build the app and replace QB Time later on — not going to use time
// tracking in the app until it's complete and ready for use". QuickBooks Time keeps
// running and keeps being the book of record throughout the build, so the clock must
// be able to exist on production without being reachable and without being counted.
//
//   TIME_CLOCK          off (default) | admin | on     — WHO can punch
//   TIME_CLOCK_PAYROLL  off (default) | on             — do punches BECOME payroll hours
//
// They are separate on purpose. The dangerous half is not letting people punch, it is
// letting a punch turn into money while QB Time is also being paid from. Keeping them
// apart means the whole path — punch, offline replay, promotion, the payroll row — can
// be exercised end to end by an admin, and switched to the crew, and only then made to
// count. Reversing the last step is `DELETE FROM time_entries WHERE source = 'Clock'`,
// which is exact precisely because 018 gave clock rows their own source value.
function timeClockAudience() {
  const v = String(process.env.TIME_CLOCK || "").trim().toLowerCase();
  return (v === "on" || v === "admin") ? v : "off";
}
// A shift running longer than this is treated as a forgotten clock-out and shown
// as a warning, on the person's own screen and on the admin roster.
//
// It deliberately only FLAGS. Auto-closing at a cutoff would invent an end time
// nobody worked, and then that invented time is what gets paid — a human deciding
// is both more honest and, at this company's size, entirely practical.
const LONG_SHIFT_HOURS = 12;

function timeClockFeedsPayroll() {
  return String(process.env.TIME_CLOCK_PAYROLL || "").trim().toLowerCase() === "on";
}
// Whether THIS person may punch right now. Role is already checked by authzFor
// (_TIME_SELF_WRITES -> _PAYROLL = admin + employee); this narrows it further while
// the feature is being built.
function canUseTimeClock(authUser) {
  const mode = timeClockAudience();
  if (mode === "off") return false;
  if (mode === "admin") return (authUser?.role || "").toLowerCase() === "admin";
  return true;
}

// The punching employee ALWAYS comes from the signed token, never from the request
// body. There is no code path here that takes an employee id from the client, so
// there is nothing to forge — one person cannot punch another in or out.
// authUser.id is the AIRTABLE rec id (login deliberately still returns that, not the
// Neon uuid), which is why this resolves through employees.airtable_id.
async function clockEmployee(authUser) {
  const q = await neonQuery(
    `SELECT id, name FROM employees WHERE airtable_id = $1 OR id::text = $1`, [authUser?.id || null]);
  return q?.rows?.[0] || null;
}

// ── CLOCK: current state ───────────────────────────────────────────────────
// Answers three things in one round trip, because the phone asks on every app open:
// is the clock available to me, am I on it, and what have I punched lately.
async function handleClockStatus(params, authUser) {
  const enabled = canUseTimeClock(authUser);
  if (!enabled) return resp(200, { ok: true, enabled: false, open: null, recent: [] });

  const me = await clockEmployee(authUser);
  if (!me) {
    // Signed in, but with no Neon employee row to hang a punch on. Reported rather
    // than treated as "clocked out", because the two look identical on screen and
    // this one will not fix itself by tapping the button again.
    return resp(200, { ok: true, enabled: true, open: null, recent: [],
                       warning: "No employee record found for this login." });
  }

  // break_started_at is the whole "am I on break" state — non-NULL means on break
  // since that instant. break_seconds is what has already been taken and closed.
  // The client needs both to show a frozen timer plus a running break.
  const open = await neonQuery(
    `SELECT started_at, job_name, job_id, class, city_taxes, notes, client_punch_id,
            break_seconds::float8 AS break_seconds, break_started_at,
            original_started_at
       FROM open_punches WHERE employee_id = $1`, [me.id]);

  // Last 14 days. Enough to answer "did I forget to clock out on Tuesday" without
  // shipping a person's whole history to a phone on every open.
  const recent = await neonQuery(
    `SELECT id, started_at, ended_at, work_date, duration_seconds::float8 AS duration_seconds,
            break_seconds::float8 AS break_seconds,
            job_name, class, city_taxes, (time_entry_id IS NOT NULL) AS counted,
            (edited_at IS NOT NULL) AS edited
       FROM clock_punches
      WHERE employee_id = $1 AND work_date >= (CURRENT_DATE - 14)
        AND deleted_at IS NULL
      ORDER BY started_at DESC`, [me.id]);

  return resp(200, {
    ok: true,
    enabled: true,
    // Tells the UI whether to say "these hours count" or "not counted yet" — while
    // QB Time is still the book of record, implying otherwise would be a lie.
    countsTowardPayroll: timeClockFeedsPayroll(),
    longShiftHours: LONG_SHIFT_HOURS,
    open: open?.rows?.[0] || null,
    recent: recent?.rows || [],
  });
}

// ── CLOCK: punch in ────────────────────────────────────────────────────────
// `targetEmp` is the ADMIN path punching somebody else (see handleAdminClockIn).
// When absent — every self-service call — the employee still comes from the token
// and there is nothing a client could forge. Resolving the target is the admin
// wrapper's job, and it is gated by _ADMIN in authzFor before it ever gets here.
async function handleClockIn(body, authUser, targetEmp) {
  if (!canUseTimeClock(authUser)) {
    return resp(403, { ok: false, error: "The time clock isn't switched on yet." });
  }
  const {
    jobId, class: cls, cityTaxes, notes, lat, lon, clientPunchId, startedAt
  } = body || {};

  // Cheap validation BEFORE the employee lookup, so a malformed punch is rejected
  // without a database round trip (and stays reachable in the offline test harness,
  // where there is no Neon to resolve an employee against).
  if (!clientPunchId) {
    // Not optional. It is the only thing that makes an offline replay safe, and a
    // punch without one would duplicate every time the queue retried.
    return resp(400, { ok: false, error: "Missing clientPunchId." });
  }

  // ⚠ THE TIMESTAMP COMES FROM THE PUNCH, NOT FROM THE SERVER.
  // A punch made in a basement is queued on the phone and replayed when signal
  // returns — possibly hours later. Stamping it server-side would record the moment
  // the connection came back, which is not when the person started work. This is the
  // one deliberate divergence in the whole feature; see the fail-closed note on
  // punch-out below. Bounded to a day either side so a phone with a badly wrong clock
  // cannot file a punch in the middle of last year's payroll.
  const started = clampPunchTime(startedAt);
  if (!started) return resp(400, { ok: false, error: "Invalid or out-of-range startedAt." });

  const me = targetEmp || await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

  // ⚠ THE RAW HANDLE, not a rec-stripped one. The LEFT JOIN below is
  // `airtable_id = $3 OR id::text = $3`; handing it a value that has already had
  // uuids filtered out makes a native job resolve to no row, and the LEFT JOIN
  // then writes job_id NULL / job_name NULL rather than failing. Someone
  // punches in on a job and the punch records no job. Same defect as
  // handleCreateTimeEntry, found in the same sweep (2026-08-25).
  const jobHandle = jobId ? String(jobId).trim() : null;

  // Replay guard #1: this punch cycle already ran to completion. The queue is retrying
  // a clock-in whose clock-out has ALREADY been recorded, so re-opening a shift here
  // would put the person back on a clock they finished with.
  const done = await neonQuery(
    `SELECT id FROM clock_punches WHERE client_punch_id = $1`, [clientPunchId]);
  if (done?.rows?.length) {
    return resp(200, { ok: true, alreadyComplete: true, open: null });
  }

  // job_name is the job's CURRENT po_locked, snapshotted at punch time — the same
  // rule as handleCreateTimeEntry, and never recomputed later.
  // ON CONFLICT (employee_id) is what enforces one open shift per person; the
  // uniqueness lives in the database, so two rapid taps cannot both win.
  const rows = await neonWrite("clock.in",
    `INSERT INTO open_punches
       (employee_id, started_at, job_id, job_name, class, city_taxes, notes,
        start_lat, start_lon, client_punch_id)
     SELECT $1, $2::timestamptz, j.id, j.po_locked, $4, $5, $6,
            $7::numeric, $8::numeric, $9
       FROM (SELECT 1) _
       LEFT JOIN jobs j ON j.airtable_id = $3 OR j.id::text = $3
     ON CONFLICT (employee_id) DO NOTHING
     RETURNING started_at, job_name, class, city_taxes, notes, client_punch_id`,
    [me.id, started, jobHandle, cls || null, cityTaxes || null, notes || null,
     numOrNull(lat), numOrNull(lon), clientPunchId]);

  if (rows?.length) return resp(200, { ok: true, open: rows[0] });

  // Nothing inserted => already on the clock. Replay guard #2: if it is the SAME
  // punch id, the first attempt actually succeeded and the phone just never heard
  // back, so this is a success, not a conflict.
  const cur = await neonQuery(
    `SELECT started_at, job_name, class, city_taxes, notes, client_punch_id
       FROM open_punches WHERE employee_id = $1`, [me.id]);
  const openRow = cur?.rows?.[0] || null;
  if (openRow && openRow.client_punch_id === clientPunchId) {
    return resp(200, { ok: true, open: openRow });
  }
  return resp(409, {
    ok: false,
    error: "You're already clocked in.",
    open: openRow,
  });
}

// ── CLOCK: start / end a break ─────────────────────────────────────────────
// Lunch, in other words. The clock keeps running in wall-clock terms — the shift
// still started when it started — but break time is deducted from what gets paid.
//
// Modelled as "one running break plus a total", not as interval rows. See the note
// at the top of db/schema/019_time_clock_breaks.sql for why.
async function handleClockBreak(body, authUser) {
  if (!canUseTimeClock(authUser)) {
    return resp(403, { ok: false, error: "The time clock isn't switched on yet." });
  }
  const { start, at } = body || {};
  const stamp = clampPunchTime(at);
  if (!stamp) return resp(400, { ok: false, error: "Invalid or out-of-range break time." });

  const me = await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

  // The WHERE clause carries the guard, so starting a break twice or ending one
  // that isn't running updates zero rows instead of corrupting the total. Same
  // principle as the ON CONFLICT on punch-in: let the database refuse it.
  const sql = start
    ? `UPDATE open_punches
          SET break_started_at = $2::timestamptz
        WHERE employee_id = $1 AND break_started_at IS NULL
        RETURNING started_at, break_seconds::float8 AS break_seconds, break_started_at`
    // GREATEST(0, …) clamps a backwards clock rather than SUBTRACTING time from the
    // running total, which is what a naive interval add would do to a phone whose
    // clock jumped. Breaks can only ever grow.
    : `UPDATE open_punches
          SET break_seconds    = break_seconds
                               + GREATEST(0, EXTRACT(EPOCH FROM ($2::timestamptz - break_started_at))),
              break_started_at = NULL
        WHERE employee_id = $1 AND break_started_at IS NOT NULL
        RETURNING started_at, break_seconds::float8 AS break_seconds, break_started_at`;

  const rows = await neonWrite(start ? "clock.breakStart" : "clock.breakEnd", sql, [me.id, stamp]);
  if (rows?.length) return resp(200, { ok: true, open: rows[0] });

  // Nothing updated. Say which of the two reasons it was, because "couldn't start
  // a break" is useless to someone standing in a parking lot.
  const cur = await neonQuery(
    `SELECT break_started_at FROM open_punches WHERE employee_id = $1`, [me.id]);
  if (!cur?.rows?.length) return resp(409, { ok: false, error: "You're not clocked in." });
  return resp(409, { ok: false,
    error: start ? "You're already on a break." : "You're not on a break." });
}

// ── CLOCK: punch out ───────────────────────────────────────────────────────
// `targetEmp`: the admin path. See the note on handleClockIn.
async function handleClockOut(body, authUser, targetEmp) {
  if (!canUseTimeClock(authUser)) {
    return resp(403, { ok: false, error: "The time clock isn't switched on yet." });
  }
  const { jobId, class: cls, cityTaxes, notes, lat, lon, clientPunchId, endedAt } = body || {};
  if (!clientPunchId) return resp(400, { ok: false, error: "Missing clientPunchId." });

  const ended = clampPunchTime(endedAt);
  if (!ended) return resp(400, { ok: false, error: "Invalid or out-of-range endedAt." });

  const me = targetEmp || await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

  // Replay guard: the punch-out already landed and the phone is retrying. Return the
  // row it created rather than erroring — from the user's side the punch worked, and
  // it did.
  const done = await neonQuery(
    `SELECT id, work_date, duration_seconds::float8 AS duration_seconds,
            (time_entry_id IS NOT NULL) AS counted
       FROM clock_punches WHERE client_punch_id = $1`, [clientPunchId]);
  if (done?.rows?.length) {
    return resp(200, { ok: true, punch: done.rows[0], replayed: true });
  }

  // Raw handle — see the note in clockIn. Here the miss is quieter still:
  // the SELECT is `COALESCE(j.id, c.job_id)`, so a native job that failed to
  // resolve falls back to the open punch's job_id, which clockIn had already
  // left NULL for the same reason. Two bugs covering for each other.
  const jobHandle = jobId ? String(jobId).trim() : null;

  // ONE STATEMENT, so closing the shift and recording it cannot half-happen. If the
  // insert fails the delete rolls back with it and the person is still on the clock —
  // which is recoverable. The reverse (shift closed, punch lost) is not.
  //
  // GREATEST(ended, started) clamps a backwards punch instead of rejecting it. Phone
  // clocks drift and daylight-saving lands mid-shift; the clock_punch_ordered CHECK
  // would otherwise reject the row, roll back the delete, and strand someone on the
  // clock with no way off it. A zero-length punch is a far better failure than that.
  //
  // work_date is the LOCAL date the shift STARTED — the overnight rule, decided in
  // db/schema/018_time_clock.sql. The zone is named explicitly because Neon's pooler
  // connects in UTC, which would otherwise file every evening punch under tomorrow.
  const rows = await neonWrite("clock.out",
    `WITH shift AS (
       DELETE FROM open_punches WHERE employee_id = $1 RETURNING *
     ), calc AS (
       -- Computed once here rather than inlined twice below, because the break
       -- total is needed both as its own column and as the subtrahend in the paid
       -- duration, and two copies of this expression would eventually disagree.
       SELECT s.*,
              GREATEST($3::timestamptz, s.started_at) AS ended,
              -- Closed breaks, PLUS a break still running at punch-out. Clocking out
              -- while on lunch is a normal thing to do and must not silently pay for
              -- the lunch, so the open break is closed inline here.
              s.break_seconds
                + CASE WHEN s.break_started_at IS NOT NULL
                       THEN GREATEST(0, EXTRACT(EPOCH FROM
                              (GREATEST($3::timestamptz, s.started_at) - s.break_started_at)))
                       ELSE 0 END AS total_break
         FROM shift s
     )
     INSERT INTO clock_punches
       (employee_id, employee_name, started_at, ended_at, work_date, duration_seconds,
        break_seconds, job_id, job_name, class, city_taxes, notes,
        start_lat, start_lon, end_lat, end_lon, client_punch_id,
        -- ⚠ Carried over, not dropped: a start time adjusted WHILE clocked in must
        -- keep its audit trail past the end of the shift. See db/schema/021.
        original_started_at, edited_at, edited_by)
     SELECT c.employee_id, $2, c.started_at, c.ended,
            (c.started_at AT TIME ZONE 'America/New_York')::date,
            -- ⚠ NET worked time — elapsed MINUS breaks. This is what gets promoted
            -- into time_entries and paid. Floored at zero so a long break on a short
            -- shift can't produce negative hours.
            GREATEST(0, EXTRACT(EPOCH FROM (c.ended - c.started_at)) - c.total_break)::numeric,
            c.total_break::numeric,
            COALESCE(j.id, c.job_id), COALESCE(j.po_locked, c.job_name),
            COALESCE($5, c.class), COALESCE($6, c.city_taxes), COALESCE($7, c.notes),
            c.start_lat, c.start_lon, $8::numeric, $9::numeric,
            COALESCE(c.client_punch_id, $10),
            c.original_started_at, c.edited_at, c.edited_by
       FROM calc c
       LEFT JOIN jobs j ON j.airtable_id = $4 OR j.id::text = $4
     RETURNING id, work_date, started_at, ended_at,
               duration_seconds::float8 AS duration_seconds,
               break_seconds::float8 AS break_seconds, job_name`,
    [me.id, me.name || null, ended, jobHandle, cls || null, cityTaxes || null,
     notes || null, numOrNull(lat), numOrNull(lon), clientPunchId]);

  const punch = rows?.[0];
  if (!punch) return resp(409, { ok: false, error: "You're not clocked in." });

  // Promotion into payroll hours. Deliberately AFTER the ledger write and in its own
  // statement: clock_punches is durable either way, so a promotion that fails leaves a
  // recorded punch flagged unpromoted, which promoteClockPunches picks up. Losing the
  // punch to a payroll problem would be the wrong trade.
  let counted = false;
  if (timeClockFeedsPayroll()) {
    try {
      const p = await promoteClockPunch(punch.id);
      counted = !!p;
    } catch (e) {
      console.error(`clock.promote failed (punch ${punch.id} recorded, not counted): ${e?.message || e}`);
    }
  }

  return resp(200, { ok: true, punch: { ...punch, counted } });
}

// ── CLOCK: adjust the times on a punch ─────────────────────────────────────
// "I arrived at 7 and forgot to clock in until 8." The commonest failure of any
// time clock. Refusing to support it doesn't make the data honest — it makes
// people keep the hour on paper instead, which is worse.
//
// WHO MAY EDIT — owner's call 2026-08-08, "all people need to be able to edit
// time": your OWN punches, with NO payroll cutoff. An employee can fix a shift
// that has already been counted. Nobody can touch a coworker's; admins may
// correct anyone's.
//
// ⚠⚠ THE CONSEQUENCE, AND THE WHOLE REASON THIS IS DELICATE.
// Editing a punch that has already been PROMOTED must also correct the
// time_entries row it produced, or the clock and payroll quietly disagree about
// the same shift and only one of them is what people get paid from. So the two
// updates happen in ONE statement below — they cannot half-apply. This is the
// opposite of the expenses rule (own-until-approved), and it is a deliberate
// departure: expenses freeze on approval because someone else has signed them
// off, whereas a wrong clock-in is wrong whenever it's noticed.
//
// The 14-day bound is what stops this reaching into a genuinely closed pay
// period; past that the answer is Payroll, which can see the whole run.
//
// Every edit keeps the ORIGINAL punch timestamps (see db/schema/021) so the real
// tap time stays answerable, plus who changed it and when.
async function handleClockEditTimes(body, authUser) {
  if (!canUseTimeClock(authUser)) {
    return resp(403, { ok: false, error: "The time clock isn't switched on yet." });
  }
  const { punchId, startedAt, endedAt, jobId, class: cls, cityTaxes } = body || {};
  if (startedAt === undefined && endedAt === undefined &&
      jobId === undefined && cls === undefined && cityTaxes === undefined) {
    return resp(400, { ok: false, error: "Nothing to change." });
  }
  if (cityTaxes !== undefined && cityTaxes !== null && !PR_CITY_TAX_OPTS.includes(String(cityTaxes))) {
    return resp(400, { ok: false, error: `Unknown city tax: ${cityTaxes}` });
  }

  const isAdmin = (authUser?.role || "").toLowerCase() === "admin";

  // Times are validated against the wall clock, not the ±36h punch window: a
  // correction is made deliberately at a keyboard, so it gets a wider berth than a
  // punch replayed from a phone. Still bounded — nothing in the future, nothing
  // ancient enough to land in a closed pay period.
  const bound = (iso, label) => {
    if (iso === undefined) return { skip: true };
    const t = Date.parse(iso);
    if (!Number.isFinite(t)) return { error: `${label} isn't a valid time.` };
    if (t > Date.now() + 5 * 60 * 1000) return { error: `${label} can't be in the future.` };
    if (t < Date.now() - 14 * 24 * 3600 * 1000) return { error: `${label} is too far in the past to change here — use Payroll.` };
    return { iso: new Date(t).toISOString() };
  };
  const s = bound(startedAt, "Start time");
  const e = bound(endedAt, "End time");
  if (s.error) return resp(400, { ok: false, error: s.error });
  if (e.error) return resp(400, { ok: false, error: e.error });

  // Resolved only after the times are known to be sane — a malformed edit should
  // say what's wrong with the time, not report a database lookup that never
  // mattered. (Also what keeps the bounds reachable in the offline test harness.)
  const me = await clockEmployee(authUser);
  if (!me && !isAdmin) return resp(400, { ok: false, error: "No employee record found for this login." });

  // ── An OPEN shift ──
  // Start time, and also the job / class / city tax. Clocking in with the wrong
  // job — or none, which is what prompted this — was previously only fixable by
  // clocking out, and someone who does that loses the shift boundary they were
  // trying to preserve.
  if (!punchId) {
    if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

    // ⚠ FIXED statement, not a built-up SET list. Every parameter is referenced on
    // every call, because Postgres rejects a bind that supplies more parameters
    // than the statement uses — so sending only a job (3 placeholders, 6 params)
    // would have failed at runtime, where the offline tests cannot see it.
    //
    // NULL means "not sent, leave alone" for each field. For the job specifically,
    // an empty string is how you CLEAR it — that is what "— No job —" sends, and
    // it's why the job needs three branches where the others need one.
    // ⚠⚠ THE WORST OF THIS FAMILY, because "" is not "unknown" here — it MEANS
    // CLEAR. The old form mapped any non-rec id to "", so editing an open punch
    // on a NATIVE job did not merely fail to set the job, it actively unset the
    // one already there. Three branches, and only the middle one is a uuid's
    // business: undefined = leave alone, "" = clear, anything else = resolve.
    const jobRecOpen = (jobId === undefined) ? null : (jobId ? String(jobId).trim() : "");

    const rows = await neonWrite("clock.editOpen",
      `UPDATE open_punches
          SET started_at = COALESCE($4::timestamptz, started_at),
              -- Only stamp the original when the start actually moves, so editing
              -- the job doesn't mark the shift as time-adjusted.
              original_started_at = CASE WHEN $4::timestamptz IS NULL
                                         THEN original_started_at
                                         ELSE COALESCE(original_started_at, started_at) END,
              -- ⚠ $3::text is not decoration. Postgres cannot infer the type from
              -- an IS NULL / = '' comparison alone and refuses to prepare the
              -- statement: "could not determine data type of parameter $3".
              job_id   = CASE WHEN $3::text IS NULL THEN job_id
                              WHEN $3::text = ''    THEN NULL
                              ELSE (SELECT id FROM jobs WHERE airtable_id = $3::text OR id::text = $3::text) END,
              -- job_name follows job_id for the same reason as on a completed
              -- punch: this is "I picked the wrong job", not a historical snapshot.
              job_name = CASE WHEN $3::text IS NULL THEN job_name
                              WHEN $3::text = ''    THEN NULL
                              ELSE (SELECT po_locked FROM jobs WHERE airtable_id = $3::text OR id::text = $3::text) END,
              class      = COALESCE($5, class),
              city_taxes = COALESCE($6, city_taxes),
              edited_at = now(), edited_by = $2
        WHERE employee_id = $1
        RETURNING started_at, original_started_at, break_seconds::float8 AS break_seconds,
                  break_started_at, job_name, class, city_taxes, client_punch_id`,
      [me.id, me.id, jobRecOpen, s.skip ? null : s.iso,
       cls === undefined ? null : (cls || null),
       cityTaxes === undefined ? null : (cityTaxes || null)]);
    const open = rows?.[0];
    if (!open) return resp(409, { ok: false, error: "You're not clocked in." });
    // A start moved later than a break that has already been taken would make the
    // worked total negative. Cheap to catch, confusing to debug later.
    if (open.break_started_at && Date.parse(open.break_started_at) < Date.parse(open.started_at)) {
      return resp(400, { ok: false, error: "That start time is after your break began." });
    }
    return resp(200, { ok: true, open });
  }

  // ── A COMPLETED punch ──
  const cur = await neonQuery(
    `SELECT c.id, c.employee_id, c.started_at, c.ended_at, c.break_seconds::float8 AS break_seconds,
            c.time_entry_id, COALESCE(e.airtable_id, e.id::text) AS emp_airtable_id
       FROM clock_punches c JOIN employees e ON e.id = c.employee_id
      WHERE c.id = $1 AND c.deleted_at IS NULL`, [String(punchId)]);
  const punch = cur?.rows?.[0];
  if (!punch) return resp(404, { ok: false, error: "Punch not found." });

  // Ownership is the ONLY restriction now. A promoted punch is still editable by
  // its owner — the payroll row is corrected alongside it, below.
  if (!isAdmin && punch.emp_airtable_id !== authUser?.id) {
    return resp(403, { ok: false, error: "You can only change your own punches." });
  }

  const newStart = s.skip ? punch.started_at : s.iso;
  const newEnd   = e.skip ? punch.ended_at   : e.iso;
  if (Date.parse(newEnd) < Date.parse(newStart)) {
    return resp(400, { ok: false, error: "The end time can't be before the start time." });
  }
  // Breaks are preserved as-is and re-deducted. duration_seconds stays NET — see
  // the warning in db/schema/019; recomputing it from the timestamps alone would
  // silently start paying people for lunch.
  const span = (Date.parse(newEnd) - Date.parse(newStart)) / 1000;
  if (span - (Number(punch.break_seconds) || 0) < 0) {
    return resp(400, { ok: false, error: "That's shorter than the break already recorded on it." });
  }

  // ⚠ OVERLAP GUARD. Without this, moving a start time backwards over an earlier
  // shift produces two punches covering the same hour for the same person — which
  // is double-paid time, and completely invisible once both are promoted. Postgres
  // range overlap (`OVERLAPS`) rather than hand-rolled comparisons, because the
  // hand-rolled version of this is famously easy to get wrong at the boundaries.
  // Touching ends are fine: 07:00-12:00 and 12:00-16:00 do not overlap.
  const clash = await neonQuery(
    `SELECT id, started_at, ended_at FROM clock_punches
      WHERE employee_id = $1 AND id <> $2 AND deleted_at IS NULL
        AND (started_at, ended_at) OVERLAPS ($3::timestamptz, $4::timestamptz)
      LIMIT 1`,
    [punch.employee_id, punch.id, newStart, newEnd]);
  if (clash?.rows?.length) {
    const c = clash.rows[0];
    const t = iso => new Date(Date.parse(iso)).toLocaleString("en-US",
      { timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
    return resp(409, { ok: false,
      error: `That overlaps another shift you already have (${t(c.started_at)} – ${t(c.ended_at)}).` });
  }

  // ⚠⚠ ONE STATEMENT, TWO TABLES, ON PURPOSE.
  // If this punch was already promoted, the time_entries row it created is what
  // payroll pays from. Correcting the ledger without correcting the payroll row
  // would leave two records of one shift that disagree, with the wrong one being
  // the one that counts. Doing both in a single statement means there is no
  // window — and no error path — where only half the correction landed.
  //
  // time_entries.hours and .week_start_date are GENERATED columns, so they follow
  // duration_seconds and work_date automatically. In particular the quarter-hour
  // rounding re-applies itself; do not try to set `hours` here.
  // Job / class / city corrections. `$5 IS NULL` means "not sent, leave alone";
  // sending an explicit empty string clears the job.
  //
  // ⚠ job_name MOVES WITH job_id here, which is the OPPOSITE of the snapshot rule
  // in handleUpdateTimeEntryPayroll. That rule protects imported history: 643 rows
  // disagree with their job's current po_locked because of PO corrections and typo
  // fixes, and recomputing would rewrite history people were paid against. This is
  // a different act — someone is saying "this shift was on the wrong job entirely",
  // and leaving the name pointing at the old job would produce a row linked to one
  // job and labelled another, which is worse than either.
  // Same three branches, same reason, same "" = CLEAR semantics as editOpen.
  const jobRecEdit = (jobId === undefined) ? null : (jobId ? String(jobId).trim() : "");

  const rows = await neonWrite("clock.editPunch",
    `WITH j AS (
       SELECT id, po_locked FROM jobs WHERE airtable_id = NULLIF($5, '') OR id::text = NULLIF($5, '')
     ), upd AS (
       UPDATE clock_punches c
          SET original_started_at = COALESCE(c.original_started_at, c.started_at),
              original_ended_at   = COALESCE(c.original_ended_at,   c.ended_at),
              started_at = $2::timestamptz,
              ended_at   = $3::timestamptz,
              duration_seconds = GREATEST(0, EXTRACT(EPOCH FROM ($3::timestamptz - $2::timestamptz)) - c.break_seconds)::numeric,
              -- work_date follows the START, same overnight rule as the punch itself.
              work_date  = ($2::timestamptz AT TIME ZONE 'America/New_York')::date,
              job_id     = CASE WHEN $5 IS NULL THEN c.job_id
                                WHEN $5 = ''    THEN NULL
                                ELSE (SELECT id FROM j) END,
              job_name   = CASE WHEN $5 IS NULL THEN c.job_name
                                WHEN $5 = ''    THEN NULL
                                ELSE (SELECT po_locked FROM j) END,
              class      = COALESCE($6, c.class),
              city_taxes = COALESCE($7, c.city_taxes),
              edited_at = now(), edited_by = $4
        WHERE c.id = $1
        RETURNING c.id, c.time_entry_id, c.work_date, c.started_at, c.ended_at,
                  c.duration_seconds, c.break_seconds, c.job_id, c.job_name,
                  c.class, c.city_taxes, c.original_started_at, c.original_ended_at
     ), te AS (
       -- The promoted payroll row moves with it, in the same statement. See the
       -- warning above handleClockEditTimes: two records of one shift that
       -- disagree is the failure mode, and the payroll one is what people are paid
       -- from. hours/week_start_date are GENERATED and follow automatically.
       UPDATE time_entries t
          SET started_at       = u.started_at,
              ended_at         = u.ended_at,
              duration_seconds = u.duration_seconds,
              work_date        = u.work_date,
              job_id           = u.job_id,
              job_name         = u.job_name,
              class            = u.class,
              city_taxes       = u.city_taxes
         FROM upd u
        WHERE t.id = u.time_entry_id
        RETURNING t.id
     )
     SELECT u.id, u.work_date, u.started_at, u.ended_at,
            u.duration_seconds::float8 AS duration_seconds,
            u.break_seconds::float8 AS break_seconds,
            u.job_name, u.class, u.city_taxes,
            u.original_started_at, u.original_ended_at,
            (u.time_entry_id IS NOT NULL) AS counted,
            (SELECT count(*)::int FROM te) AS payroll_rows_updated
       FROM upd u`,
    [punch.id, newStart, newEnd, me?.id || null, jobRecEdit,
     cls === undefined ? null : (cls || null),
     cityTaxes === undefined ? null : (cityTaxes || null)]);

  const out = rows?.[0] || null;
  // Loud in the logs: a promoted punch whose payroll row did NOT move is exactly
  // the drift this statement exists to prevent, so it must never pass silently.
  if (out?.counted && out.payroll_rows_updated !== 1) {
    console.error(`clock.editPunch: punch ${punch.id} is promoted but updated ` +
                  `${out.payroll_rows_updated} payroll rows — clock and payroll may disagree.`);
  }
  return resp(200, { ok: true, punch: out });
}

// ── CLOCK: delete a punch ──────────────────────────────────────────────────
// For a shift that should not exist at all — clocked in by mistake, or twice on
// two devices. Editing times cannot express that.
//
// Same ownership rule as editing: your own, admin anyone's. SOFT on the clock
// ledger (see db/schema/022) so a deletion isn't the one clock action with no
// trace, but the promoted payroll row is HARD deleted in the same statement —
// payroll has no concept of a soft-deleted entry, and leaving it behind would pay
// for a shift the clock says never happened.
async function handleClockDeletePunch(body, authUser) {
  if (!canUseTimeClock(authUser)) {
    return resp(403, { ok: false, error: "The time clock isn't switched on yet." });
  }
  const punchId = body?.punchId;
  if (!punchId) return resp(400, { ok: false, error: "Missing punchId." });

  const isAdmin = (authUser?.role || "").toLowerCase() === "admin";
  const me = await clockEmployee(authUser);

  const cur = await neonQuery(
    `SELECT c.id, c.time_entry_id, COALESCE(e.airtable_id, e.id::text) AS emp_airtable_id
       FROM clock_punches c JOIN employees e ON e.id = c.employee_id
      WHERE c.id = $1 AND c.deleted_at IS NULL`, [String(punchId)]);
  const punch = cur?.rows?.[0];
  if (!punch) return resp(404, { ok: false, error: "Punch not found." });
  if (!isAdmin && punch.emp_airtable_id !== authUser?.id) {
    return resp(403, { ok: false, error: "You can only remove your own shifts." });
  }

  const rows = await neonWrite("clock.deletePunch",
    `WITH del AS (
       UPDATE clock_punches
          SET deleted_at = now(), deleted_by = $2, time_entry_id = NULL
        WHERE id = $1
        RETURNING id, time_entry_id AS cleared, $3::uuid AS te_id
     ), te AS (
       DELETE FROM time_entries WHERE id = $3::uuid RETURNING id
     )
     SELECT d.id, (SELECT count(*)::int FROM te) AS payroll_rows_deleted FROM del d`,
    [punch.id, me?.id || null, punch.time_entry_id]);

  const out = rows?.[0];
  if (punch.time_entry_id && out?.payroll_rows_deleted !== 1) {
    console.error(`clock.deletePunch: punch ${punch.id} was promoted but removed ` +
                  `${out?.payroll_rows_deleted} payroll rows — payroll may still hold it.`);
  }
  return resp(200, { ok: true, deletedId: punch.id,
                     payrollRowsDeleted: out?.payroll_rows_deleted ?? 0 });
}

// ══ PTO ══════════════════════════════════════════════════════════════════════
// Employees request, an admin approves, and only on approval does a request turn
// into hours. See db/schema/023 and 025.
//
// PTO is tracked for EMPLOYEES ONLY — the salaried people are the owners and take
// time off without it counting against anything (owner's decision 2026-08-08).

const PTO_YEAR = () => new Date().getFullYear();

// The employee's own view: what they have left, and what they've asked for.
async function handlePtoBalance(params, authUser) {
  const me = await clockEmployee(authUser);
  if (!me) return resp(200, { ok: true, tracked: false, reason: "No employee record for this login." });

  const year = Number(params?.year) || PTO_YEAR();

  const bal = await neonQuery(
    `SELECT allowance_hours::float8, carried_in_hours::float8, entitled_hours::float8,
            used_hours::float8, remaining_hours::float8
       FROM v_pto_balances WHERE employee_id = $1 AND year = $2`, [me.id, year]);

  // Hours already asked for but not yet decided. Shown SEPARATELY from remaining
  // rather than deducted: nothing has been granted yet, and quietly reducing the
  // balance for a request that might be declined would misreport what they have.
  const pend = await neonQuery(
    `SELECT coalesce(sum(total_hours), 0)::float8 AS hours
       FROM v_pto_requests
      WHERE employee_id = $1 AND status = 'pending'`, [me.id]);

  const mine = await neonQuery(
    `SELECT id, start_date, end_date, hours_per_day::float8, days, total_hours::float8,
            status, note, decision_note, requested_at, decided_at
       FROM v_pto_requests
      WHERE employee_id = $1 AND end_date >= (CURRENT_DATE - 400)
      ORDER BY start_date DESC LIMIT 25`, [me.id]);

  const b = bal?.rows?.[0] || null;
  return resp(200, {
    ok: true,
    // No allowance row means nobody has set one — reported as untracked rather
    // than as a zero balance, which would read as "you've used it all".
    tracked: !!b,
    year,
    balance: b || null,
    pendingHours: Number(pend?.rows?.[0]?.hours) || 0,
    requests: mine?.rows || [],
    _source: "neon",
  });
}

// Employee asks for time off. The employee is the token's, never the body's.
async function handleRequestPto(body, authUser) {
  const me = await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

  const { startDate, endDate, hoursPerDay, note } = body || {};
  const dOk = s => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
  if (!dOk(startDate) || !dOk(endDate)) {
    return resp(400, { ok: false, error: "Need a start and end date." });
  }
  if (endDate < startDate) return resp(400, { ok: false, error: "The end date is before the start date." });
  const hrs = hoursPerDay == null ? 8 : Number(hoursPerDay);
  if (!Number.isFinite(hrs) || hrs <= 0 || hrs > 24) {
    return resp(400, { ok: false, error: "Hours per day must be between 0 and 24." });
  }

  // Overlapping an existing live request is almost always a double-submit or a
  // forgotten one, and two approvals would book the same day twice.
  const clash = await neonQuery(
    `SELECT id FROM pto_requests
      WHERE employee_id = $1 AND status IN ('pending', 'approved')
        AND (start_date, end_date + 1) OVERLAPS ($2::date, $3::date + 1)
      LIMIT 1`, [me.id, startDate, endDate]);
  if (clash?.rows?.length) {
    return resp(409, { ok: false, error: "You already have a request covering some of those days." });
  }

  const rows = await neonWrite("pto.request",
    `INSERT INTO pto_requests (employee_id, start_date, end_date, hours_per_day, note)
     VALUES ($1, $2::date, $3::date, $4::numeric, NULLIF($5, ''))
     RETURNING id`, [me.id, startDate, endDate, hrs, String(note || "").trim()]);

  const id = rows?.[0]?.id;
  const info = await neonQuery(
    `SELECT days, total_hours::float8 FROM v_pto_requests WHERE id = $1`, [id]);
  const d = info?.rows?.[0];
  if (d && Number(d.days) === 0) {
    // Weekends and company holidays are excluded, so a request can legitimately
    // come to nothing. Say so rather than leaving a 0-hour request in the queue.
    await neonWrite("pto.requestEmpty", `DELETE FROM pto_requests WHERE id = $1`, [id]);
    return resp(400, { ok: false,
      error: "Those dates are all weekend or company holidays — no PTO needed." });
  }
  return resp(200, { ok: true, id, days: d?.days ?? null, totalHours: Number(d?.total_hours) || 0 });
}

// Withdrawing your own request, while it is still undecided.
async function handleCancelPtoRequest(body, authUser) {
  const me = await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });
  const id = body?.requestId;
  if (!id) return resp(400, { ok: false, error: "Missing requestId." });

  const rows = await neonWrite("pto.cancel",
    `UPDATE pto_requests SET status = 'cancelled'
      WHERE id = $1 AND employee_id = $2 AND status = 'pending'
      RETURNING id`, [String(id), me.id]);
  if (!rows?.length) {
    return resp(409, { ok: false, error: "That request is no longer pending, or isn't yours." });
  }
  return resp(200, { ok: true, cancelledId: rows[0].id });
}

// ── ADMIN: the queue, and deciding ─────────────────────────────────────────
async function handlePtoRequests(params) {
  const year = Number(params?.year) || PTO_YEAR();

  // One person's full history, for their card on the People screen. Returned on
  // its own so the roster doesn't have to carry everybody's requests just so one
  // card can show a handful.
  if (params?.employeeId) {
    const hist = await neonQuery(
      `SELECT r.id, r.start_date, r.end_date, r.hours_per_day::float8, r.days,
              r.total_hours::float8, r.status, r.note, r.decision_note,
              r.requested_at, r.decided_at
         FROM v_pto_requests r
        WHERE r.employee_airtable_id = $1
        ORDER BY r.start_date DESC LIMIT 40`, [String(params.employeeId)]);
    return resp(200, { ok: true, employeeId: params.employeeId, history: hist?.rows || [], _source: "neon" });
  }

  const pending = await neonQuery(
    `SELECT id, employee_airtable_id, employee_name, start_date, end_date,
            hours_per_day::float8, days, total_hours::float8, note, requested_at
       FROM v_pto_requests WHERE status = 'pending' ORDER BY start_date`);

  const recent = await neonQuery(
    `SELECT id, employee_name, start_date, end_date, days, total_hours::float8,
            status, decided_at, decision_note
       FROM v_pto_requests
      WHERE status <> 'pending' AND end_date >= (CURRENT_DATE - 120)
      ORDER BY decided_at DESC NULLS LAST LIMIT 30`);

  // Balances for everyone who has an allowance, so a decision can be made with
  // the remaining figure in view rather than from memory.
  const balances = await neonQuery(
    `SELECT airtable_id, name, allowance_hours::float8, carried_in_hours::float8,
            entitled_hours::float8, used_hours::float8, remaining_hours::float8
       FROM v_pto_balances WHERE year = $1 ORDER BY name`, [year]);

  // Anyone payroll-eligible and hourly with NO allowance row — otherwise they
  // simply never appear and nobody notices they were missed.
  const missing = await neonQuery(
    `SELECT COALESCE(e.airtable_id, e.id::text) AS airtable_id, e.name FROM employees e
      WHERE e.active IS TRUE AND lower(coalesce(e.role,'')) = 'employee'
        AND NOT EXISTS (SELECT 1 FROM pto_years p WHERE p.employee_id = e.id AND p.year = $1)
      ORDER BY e.name`, [year]);

  return resp(200, {
    ok: true, year,
    pending:  pending?.rows  || [],
    recent:   recent?.rows   || [],
    balances: balances?.rows || [],
    missingAllowance: missing?.rows || [],
    _source: "neon",
  });
}

// ⚠ DOUBLE-BOOKING GUARD — the thing that makes backfilling safe.
//
// Booking PTO for a day somebody already logged hours on pays them twice for that
// day. It is a live risk precisely because backdated PTO is the normal case: you
// are filling in a day that has already been and gone, and the worked entries for
// that week already exist.
//
// Returns the clashing days so the message can name them, rather than a bare
// refusal the person then has to go hunting to explain.
async function ptoConflicts(requestId) {
  const q = await neonQuery(
    `SELECT to_char(t.work_date, 'YYYY-MM-DD') AS day,
            round(sum(t.hours), 2)::float8 AS hours
       FROM v_pto_request_days d
       JOIN time_entries t
         ON t.employee_id = d.employee_id AND t.work_date = d.work_date
      WHERE d.request_id = $1
        AND coalesce(t.pto_request_id::text, '') <> $1
      GROUP BY 1 ORDER BY 1`, [String(requestId)]);
  return q?.rows || [];
}

// Approve or decline. Approving WRITES THE HOURS — one time entry per eligible
// day — in the same statement that flips the status, so a request can never be
// marked approved without the hours existing (or vice versa).
async function handleDecidePtoRequest(body, authUser) {
  const { requestId, approve, note } = body || {};
  if (!requestId) return resp(400, { ok: false, error: "Missing requestId." });
  const me = await clockEmployee(authUser);

  if (approve !== true) {
    const rows = await neonWrite("pto.decline",
      `UPDATE pto_requests
          SET status = 'declined', decided_by = $2, decided_at = now(),
              decision_note = NULLIF($3, '')
        WHERE id = $1 AND status = 'pending'
        RETURNING id`, [String(requestId), me?.id || null, String(note || "").trim()]);
    if (!rows?.length) return resp(409, { ok: false, error: "That request isn't pending any more." });
    return resp(200, { ok: true, declinedId: rows[0].id });
  }

  // Refuse rather than skip. Silently dropping the clashing days would approve a
  // week and book four days of it, with nothing on screen saying so.
  const clashes = await ptoConflicts(requestId);
  if (clashes.length && body?.force !== true) {
    return resp(409, {
      ok: false,
      conflicts: clashes,
      error: `Already has hours logged on ${clashes.map(c => `${c.day} (${c.hours} h)`).join(", ")}. ` +
             `Approving would pay those days twice.`,
    });
  }

  // ⚠ ONE STATEMENT. The status flip and the hours are written together; there is
  // no path where a request reads "approved" but no PTO was booked, which is the
  // failure that would quietly cost someone their time off.
  //
  // `source = 'Manual'` because te_has_a_key requires a row to declare an origin
  // and these have neither an Airtable nor a QB id. class 'PTO' is what the
  // payroll PDF and v_pto_balances key on. No job: PTO is never costed to work.
  const rows = await neonWrite("pto.approve",
    `WITH req AS (
       UPDATE pto_requests
          SET status = 'approved', decided_by = $2, decided_at = now(),
              decision_note = NULLIF($3, '')
        WHERE id = $1 AND status = 'pending'
        RETURNING id, employee_id
     ), ins AS (
       INSERT INTO time_entries
         (employee_name, employee_id, work_date, duration_seconds, city_taxes, class,
          labor_reviewed, source, pto_request_id)
       SELECT e.name, d.employee_id, d.work_date, d.hours * 3600, 'A No Tax', 'PTO',
              false, 'Manual', d.request_id
         FROM v_pto_request_days d
         JOIN req  ON req.id = d.request_id
         JOIN employees e ON e.id = d.employee_id
       RETURNING id
     )
     SELECT (SELECT id FROM req) AS approved_id,
            (SELECT count(*)::int FROM ins) AS entries_created`,
    [String(requestId), me?.id || null, String(note || "").trim()]);

  const out = rows?.[0];
  if (!out?.approved_id) return resp(409, { ok: false, error: "That request isn't pending any more." });
  return resp(200, { ok: true, approvedId: out.approved_id, entriesCreated: out.entries_created });
}

// ── ADMIN: book PTO directly, no request ───────────────────────────────────
// For time off that already happened and nobody logged — the normal case when a
// clock is new. Deliberately built ON TOP of the request machinery rather than
// beside it: it creates a request and approves it in the same breath, so a
// hand-booked day is indistinguishable downstream from a requested one, carries
// the same pto_request_id link, and stays just as reversible.
async function handleAdminAddPto(body, authUser) {
  const { employeeId, startDate, endDate, hoursPerDay, note } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Pick a person." });
  }
  const dOk = s => /^\d{4}-\d{2}-\d{2}$/.test(String(s || ""));
  const from = startDate, to = endDate || startDate;
  if (!dOk(from) || !dOk(to)) return resp(400, { ok: false, error: "Need a start and end date." });
  if (to < from) return resp(400, { ok: false, error: "The end date is before the start date." });
  const hrs = hoursPerDay == null ? 8 : Number(hoursPerDay);
  if (!Number.isFinite(hrs) || hrs <= 0 || hrs > 24) {
    return resp(400, { ok: false, error: "Hours per day must be between 0 and 24." });
  }

  const emp = await clockEmployeeById(employeeId);
  if (!emp) return resp(404, { ok: false, error: "Employee not found." });
  const me = await clockEmployee(authUser);

  // Created pending first so the day-expansion and conflict views can see it;
  // it is approved (or removed) below. A pending row briefly existing is
  // harmless — it books no hours.
  const made = await neonWrite("pto.adminAdd",
    `INSERT INTO pto_requests (employee_id, start_date, end_date, hours_per_day, note)
     VALUES ($1, $2::date, $3::date, $4::numeric, NULLIF($5, ''))
     RETURNING id`,
    [emp.id, from, to, hrs, String(note || "").trim()]);
  const id = made?.[0]?.id;

  const cleanup = async () => { await neonWrite("pto.adminAdd.undo",
    `DELETE FROM pto_requests WHERE id = $1`, [id]); };

  const info = await neonQuery(
    `SELECT days, total_hours::float8 FROM v_pto_requests WHERE id = $1`, [id]);
  if (Number(info?.rows?.[0]?.days || 0) === 0) {
    await cleanup();
    return resp(400, { ok: false,
      error: "Those dates are all weekend or company holidays — no PTO needed." });
  }

  const clashes = await ptoConflicts(id);
  if (clashes.length && body?.force !== true) {
    await cleanup();
    return resp(409, { ok: false, conflicts: clashes,
      error: `${emp.name} already has hours on ${clashes.map(c => `${c.day} (${c.hours} h)`).join(", ")}. ` +
             `Booking PTO there would pay those days twice.` });
  }

  const rows = await neonWrite("pto.adminAdd.approve",
    `WITH req AS (
       UPDATE pto_requests
          SET status = 'approved', decided_by = $2, decided_at = now(),
              decision_note = 'Booked by admin'
        WHERE id = $1 AND status = 'pending'
        RETURNING id, employee_id
     ), ins AS (
       INSERT INTO time_entries
         (employee_name, employee_id, work_date, duration_seconds, city_taxes, class,
          labor_reviewed, source, pto_request_id)
       SELECT e.name, d.employee_id, d.work_date, d.hours * 3600, 'A No Tax', 'PTO',
              false, 'Manual', d.request_id
         FROM v_pto_request_days d
         JOIN req ON req.id = d.request_id
         JOIN employees e ON e.id = d.employee_id
       RETURNING id
     )
     SELECT (SELECT id FROM req) AS id, (SELECT count(*)::int FROM ins) AS entries_created`,
    [id, me?.id || null]);

  const out = rows?.[0];
  return resp(200, { ok: true, id: out?.id, entriesCreated: out?.entries_created ?? 0 });
}

// ── ADMIN: fill in the paid holidays ───────────────────────────────────────
// Creates 8 h 'Paid Holiday' entries for the eligible employees on each date in
// company_holidays.
//
// ⚠⚠ FORWARD-ONLY, AND `from` IS REQUIRED. Three of 2026's six holidays had
// already passed when this was written, and they were ALREADY PAID through
// QuickBooks Time. Filling those retroactively would pay them a second time. So
// there is no "fill everything" button — you have to say from when, and the sane
// answer is the date the app took over payroll.
//
// Idempotent by construction: a holiday is skipped for anyone who already has ANY
// hours that day. That single rule covers both re-running this (the holiday entry
// is already there) and someone who actually worked the holiday (they should be
// paid for the work; whether they ALSO get holiday pay is a policy call, so it is
// surfaced rather than assumed).
async function handleFillHolidays(body) {
  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(body?.from || "")) ? String(body.from) : null;
  if (!from) {
    return resp(400, { ok: false,
      error: "Need a 'from' date (YYYY-MM-DD). Holidays before it were paid through QuickBooks." });
  }
  const dryRun = body?.dryRun === true;
  if (!dryRun && body?.confirm !== "YES") {
    return resp(400, { ok: false, error: 'Missing confirmation. Pass {"confirm":"YES"}.' });
  }

  // Who WOULD be filled, and who is being skipped and why — computed the same way
  // for the dry run and the real thing, so the preview cannot disagree with what
  // the write then does.
  const plan = await neonQuery(
    `SELECT to_char(h.holiday_date,'YYYY-MM-DD') AS holiday_date, h.name AS holiday, h.hours::float8,
            e.airtable_id, e.name AS employee,
            EXISTS (SELECT 1 FROM time_entries t
                     WHERE t.employee_id = e.id AND t.work_date = h.holiday_date) AS has_hours
       FROM company_holidays h
       CROSS JOIN employees e
      WHERE h.holiday_date >= $1::date
        AND e.active IS TRUE
        AND lower(coalesce(e.role, '')) = 'employee'
      ORDER BY h.holiday_date, e.name`, [from]);

  const rows = (plan?.rows || []).map(r => ({
    date: r.holiday_date,
    holiday: r.holiday,
    employee: r.employee,
    hours: Number(r.hours),
    skip: r.has_hours === true,
  }));
  const toCreate = rows.filter(r => !r.skip);
  const skipped  = rows.filter(r => r.skip);

  if (dryRun) {
    return resp(200, { ok: true, dryRun: true, from,
      wouldCreate: toCreate.length,
      hours: toCreate.reduce((s, r) => s + r.hours, 0),
      creates: toCreate, skipped });
  }

  const made = await neonWrite("pto.fillHolidays",
    `INSERT INTO time_entries
       (employee_name, employee_id, work_date, duration_seconds, city_taxes, class,
        labor_reviewed, source)
     SELECT e.name, e.id, h.holiday_date, h.hours * 3600, 'A No Tax', 'Paid Holiday',
            false, 'Manual'
       FROM company_holidays h
       CROSS JOIN employees e
      WHERE h.holiday_date >= $1::date
        AND e.active IS TRUE
        AND lower(coalesce(e.role, '')) = 'employee'
        AND NOT EXISTS (SELECT 1 FROM time_entries t
                         WHERE t.employee_id = e.id AND t.work_date = h.holiday_date)
     RETURNING id`, [from]);

  return resp(200, { ok: true, from, created: made?.length || 0, skipped: skipped.length, skips: skipped });
}

// ── ADMIN: roll PTO into the next year ─────────────────────────────────────
// Creates next year's allowance rows, carrying each person's unused hours in.
// Deliberately an explicit action rather than something that happens by itself on
// 1 January: the carried figure is a number someone should look at and be willing
// to sign off, not one that appears overnight.
//
// Existing rows are never overwritten — running it twice is safe and changes
// nothing the second time.
async function handlePtoRollover(body) {
  const toYear = Number(body?.toYear) || (new Date().getFullYear() + 1);
  const fromYear = toYear - 1;
  const dryRun = body?.dryRun === true;
  if (!dryRun && body?.confirm !== "YES") {
    return resp(400, { ok: false, error: 'Missing confirmation. Pass {"confirm":"YES"}.' });
  }

  const preview = await neonQuery(
    `SELECT b.name, b.allowance_hours::float8, b.remaining_hours::float8,
            EXISTS (SELECT 1 FROM pto_years p2
                     WHERE p2.employee_id = b.employee_id AND p2.year = $2) AS exists_already
       FROM v_pto_balances b
      WHERE b.year = $1 ORDER BY b.name`, [fromYear, toYear]);

  const rows = (preview?.rows || []).map(r => ({
    employee: r.name,
    allowance: Number(r.allowance_hours),
    // Carried AS-IS, including a negative. Someone who took more than they had
    // genuinely starts the year down, and zeroing that would quietly forgive it —
    // which is a decision for a person, not a default.
    carryIn: Number(r.remaining_hours),
    alreadyHasYear: r.exists_already === true,
  }));

  if (dryRun) {
    return resp(200, { ok: true, dryRun: true, fromYear, toYear,
      wouldCreate: rows.filter(r => !r.alreadyHasYear).length, rows });
  }

  const made = await neonWrite("pto.rollover",
    `INSERT INTO pto_years (employee_id, year, allowance_hours, carried_in_hours, note, updated_at)
     SELECT b.employee_id, $2, b.allowance_hours, b.remaining_hours,
            'Carried over from ' || $1::text, now()
       FROM v_pto_balances b
      WHERE b.year = $1
     ON CONFLICT (employee_id, year) DO NOTHING
     RETURNING employee_id`, [fromYear, toYear]);

  return resp(200, { ok: true, fromYear, toYear, created: made?.length || 0, rows });
}

// ── ADMIN: set someone's yearly allowance ──────────────────────────────────
async function handleSetPtoAllowance(body) {
  const { employeeId, year, allowanceHours, carriedInHours, note } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const yr  = Number(year) || PTO_YEAR();
  const all = Number(allowanceHours);
  const car = carriedInHours == null ? 0 : Number(carriedInHours);
  if (!Number.isFinite(all) || all < 0 || all > 2000) {
    return resp(400, { ok: false, error: "Allowance must be between 0 and 2000 hours." });
  }
  if (!Number.isFinite(car) || car < 0 || car > 2000) {
    return resp(400, { ok: false, error: "Carry-over must be between 0 and 2000 hours." });
  }

  const rows = await neonWrite("pto.setAllowance",
    `INSERT INTO pto_years (employee_id, year, allowance_hours, carried_in_hours, note, updated_at)
     SELECT e.id, $2, $3::numeric, $4::numeric, NULLIF($5, ''), now()
       FROM employees e WHERE e.airtable_id = $1 OR e.id::text = $1
     ON CONFLICT (employee_id, year) DO UPDATE
        SET allowance_hours  = EXCLUDED.allowance_hours,
            carried_in_hours = EXCLUDED.carried_in_hours,
            note             = EXCLUDED.note,
            updated_at       = now()
     RETURNING year, allowance_hours::float8, carried_in_hours::float8`,
    [String(employeeId), yr, all, car, String(note || "").trim()]);
  if (!rows?.length) return resp(404, { ok: false, error: "Employee not found." });
  return resp(200, { ok: true, employeeId, ...rows[0] });
}

// ══ HOME-SCREEN WIDGET ═══════════════════════════════════════════════════════
// A widget host (Scriptable, KWGT, Tasker) polls a URL every so often and draws
// the result. It has no session, and several hosts cannot set request headers at
// all — so this one action authenticates from a signed token in the query string.
//
// ⚠ THIS IS THE BEARER-CHECK BYPASS THE NOTE ABOVE _ADMIN_READS WARNS ABOUT.
// That note says not to reintroduce one "without a case that genuinely can't be
// served by a presigned URL". This is that case: there is nothing to presign,
// because the thing being fetched is live state rather than a stored object, and
// the caller is a widget host that cannot carry a header. What keeps it narrow:
//
//   • ONE read action. The token authorises nothing else — not even other reads.
//   • It returns only that person's own clock state. No pay, no rates, no
//     roster, no money, nothing about anyone else.
//   • Per person, so a leaked URL exposes one person's shift.
//   • Revocable on its own (employees.widget_key, db/schema/028) without
//     touching their login.
//
// If you extend this action to return anything beyond one person's clock, that
// list stops being true and the bypass stops being justified.
const WIDGET_TTL_MS = 365 * 24 * 60 * 60 * 1000;   // a year; revoke by key, not expiry

function widgetScopeParts(airtableId, key) {
  return ["clockwidget", String(airtableId), String(key)];
}

// Short, pre-formatted strings: a widget host like KWGT can only place text, it
// cannot do arithmetic or date maths.
function fmtHm(totalSeconds) {
  const s = Math.max(0, Math.floor(Number(totalSeconds) || 0));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h ? `${h}h ${String(m).padStart(2, "0")}m` : `${m}m`;
}

async function handleClockWidget(params) {
  const employeeId = String(params?.e || "");
  const token      = String(params?.t || "");
  // Deliberately vague on failure. This endpoint is unauthenticated by design, so
  // it must not become a way to test whether a person or token exists.
  const deny = () => resp(200, { ok: false, state: "error", label: "—", today: "—" });
  if (!employeeId || !token) return deny();

  const q = await neonQuery(
    `SELECT id, name, widget_key FROM employees WHERE airtable_id = $1 OR id::text = $1`, [employeeId]);
  const emp = q?.rows?.[0];
  if (!emp?.widget_key) return deny();
  if (!verifyScope(token, widgetScopeParts(employeeId, emp.widget_key))) return deny();

  const open = await neonQuery(
    `SELECT started_at, job_name, class, break_seconds::float8 AS break_seconds, break_started_at
       FROM open_punches WHERE employee_id = $1`, [emp.id]);
  const o = open?.rows?.[0] || null;

  // Everything already banked today, so "today" is the whole day rather than just
  // the shift in progress — which is what the number on a widget should mean.
  const done = await neonQuery(
    `SELECT coalesce(sum(duration_seconds), 0)::float8 AS secs
       FROM clock_punches
      WHERE employee_id = $1 AND deleted_at IS NULL
        AND work_date = (now() AT TIME ZONE 'America/New_York')::date`, [emp.id]);
  let todaySecs = Number(done?.rows?.[0]?.secs) || 0;

  let state = "out", label = "Clocked out";
  if (o) {
    const started = Date.parse(o.started_at);
    const onBreak = !!o.break_started_at;
    let worked = (Date.now() - started) / 1000 - (Number(o.break_seconds) || 0);
    if (onBreak) worked -= (Date.now() - Date.parse(o.break_started_at)) / 1000;
    worked = Math.max(0, worked);
    todaySecs += worked;
    state = onBreak ? "break" : "working";
    label = onBreak ? `Lunch ${fmtHm((Date.now() - Date.parse(o.break_started_at)) / 1000)}`
                    : fmtHm(worked);
  }

  // ?fmt=text returns ONE LINE of plain text instead of JSON. Widget hosts like
  // KWGT can place a string but parsing JSON in their formula language is fiddly
  // and, on some tiers, network+JSON isn't available at all. This makes the whole
  // setup "fetch this URL, show the result".
  //   ?fmt=text        "3h 51m · Sullivan Pullet (AVS 272)"
  //   ?fmt=text&f=today "5h 14m"
  if (String(params?.fmt || "").toLowerCase() === "text") {
    const field = String(params?.f || "").toLowerCase();
    const line =
      field === "today" ? fmtHm(todaySecs)
    : field === "job"   ? (o?.job_name || "")
    : field === "state" ? state
    : (o ? `${label}${o.job_name ? ` · ${o.job_name}` : ""}` : label);
    return {
      statusCode: 200,
      headers: {
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "no-store",
        "Access-Control-Allow-Origin": "*",
      },
      body: line,
    };
  }

  return resp(200, {
    ok: true,
    state,                       // working | break | out
    label,                       // "3h 51m" / "Lunch 24m" / "Clocked out"
    job: o?.job_name || "",
    class: o?.class || "",
    today: fmtHm(todaySecs),
    name: emp.name || "",
    // So a widget can show "as of 11:20" and be honest that it isn't ticking.
    asOf: new Date().toLocaleTimeString("en-US",
      { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }),
  });
}

// Mint (or re-mint) your own widget URL. Self-service: the employee is taken from
// the token, so nobody can create a link that watches somebody else.
async function handleWidgetLink(body, authUser) {
  const me = await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

  // `regenerate` is the revoke button: a new key invalidates every URL ever
  // issued to this person, while leaving their phone signed in.
  const rotate = body?.regenerate === true;
  const rows = await neonWrite("widget.mint",
    `UPDATE employees
        SET widget_key = CASE WHEN widget_key IS NULL OR $2 THEN gen_random_uuid()
                              ELSE widget_key END
      WHERE id = $1
      RETURNING COALESCE(airtable_id, id::text) AS airtable_id, widget_key`, [me.id, rotate]);

  const r = rows?.[0];
  if (!r) return resp(404, { ok: false, error: "Employee not found." });

  const token = signScope(widgetScopeParts(r.airtable_id, r.widget_key), WIDGET_TTL_MS);
  return resp(200, {
    ok: true,
    // Path only — the client prepends its own origin, so this stays correct on
    // any domain without the function needing to know where it is deployed.
    path: `/.netlify/functions/airtable?action=clockWidget` +
          `&e=${encodeURIComponent(r.airtable_id)}&t=${encodeURIComponent(token)}`,
    rotated: rotate,
  });
}

// ══ RECONCILE: the app's clock vs QuickBooks Time ════════════════════════════
// The point of running both systems in parallel. Without this, a soak only proves
// the app didn't crash — not that the hours AGREE, which is the actual gate for
// retiring QuickBooks.
//
// Both sides are keyed to the same person: QB entries arrive in time_entries with
// source 'TSheets' (via the puller, matched on employees.qb_user_id), and clock
// punches carry employee_id directly. So the comparison is per person per day.
//
// ⚠ Clock punches are compared at FULL precision converted to hours, while QB's
// side reads the `hours` column, which is rounded to the quarter hour. A small
// residual difference is therefore EXPECTED and is not drift — see the note on
// `roundedClockHours` below, which applies the same rounding so the two are
// compared like for like.
async function handleClockReconcile(params) {
  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(params?.from || "")) ? String(params.from) : null;
  const to   = /^\d{4}-\d{2}-\d{2}$/.test(String(params?.to   || "")) ? String(params.to)   : null;
  if (!from || !to) {
    return resp(400, { ok: false, error: "Need from and to dates (YYYY-MM-DD)." });
  }

  const q = await neonQuery(
    `WITH qb AS (
       SELECT employee_id, work_date, sum(hours)::float8 AS hours, count(*)::int AS entries
         FROM time_entries
        WHERE work_date BETWEEN $1::date AND $2::date
          AND employee_id IS NOT NULL
          -- Everything that is NOT the app's own clock counts as "the old way":
          -- QB-imported rows and the handful of hand-typed Manual ones.
          AND coalesce(source, '') <> 'Clock'
          -- ⚠ But NOT leave. PTO and paid holidays are source='Manual', so without
          -- this they'd land in the QuickBooks column and every approved PTO day
          -- would read as an 8-hour shortfall against a clock that correctly has
          -- nothing for it. This screen compares HOURS WORKED; the clock never
          -- produces leave, so neither side should count it.
          AND coalesce(class, '') NOT IN ('PTO', 'Paid Holiday')
        GROUP BY 1, 2
     ), ck AS (
       SELECT employee_id, work_date,
              -- Same quarter-hour rule the payroll column applies, so the two
              -- sides are compared like for like rather than differing by rounding.
              (round((sum(duration_seconds) / 3600.0) * 4) / 4)::float8 AS hours,
              count(*)::int AS punches
         FROM clock_punches
        WHERE work_date BETWEEN $1::date AND $2::date
          AND deleted_at IS NULL
        GROUP BY 1, 2
     )
     SELECT e.name AS employee, COALESCE(e.airtable_id, e.id::text) AS employee_id,
            to_char(coalesce(qb.work_date, ck.work_date),'YYYY-MM-DD') AS work_date,
            coalesce(qb.hours, 0)   AS qb_hours,
            coalesce(ck.hours, 0)   AS clock_hours,
            coalesce(ck.hours, 0) - coalesce(qb.hours, 0) AS diff,
            coalesce(qb.entries, 0) AS qb_entries,
            coalesce(ck.punches, 0) AS clock_punches
       FROM qb
       FULL OUTER JOIN ck
         ON qb.employee_id = ck.employee_id AND qb.work_date = ck.work_date
       JOIN employees e ON e.id = coalesce(qb.employee_id, ck.employee_id)
      ORDER BY e.name, coalesce(qb.work_date, ck.work_date)`,
    [from, to]);

  const rows = (q?.rows || []).map(r => ({
    employee: r.employee,
    employeeId: r.employee_id,
    workDate: r.work_date || null,
    qbHours: Number(r.qb_hours) || 0,
    clockHours: Number(r.clock_hours) || 0,
    diff: Number(r.diff) || 0,
    qbEntries: r.qb_entries,
    clockPunches: r.clock_punches,
  }));

  // Per-person totals, which is what the "can we switch yet" decision is made on.
  const byEmployee = {};
  for (const r of rows) {
    const k = r.employee;
    byEmployee[k] ||= { employee: k, qbHours: 0, clockHours: 0, days: 0, daysDiffering: 0 };
    byEmployee[k].qbHours    += r.qbHours;
    byEmployee[k].clockHours += r.clockHours;
    byEmployee[k].days++;
    // A quarter hour is the smallest unit payroll can express, so anything under
    // it is not a real disagreement.
    if (Math.abs(r.diff) >= 0.25) byEmployee[k].daysDiffering++;
  }
  const totals = Object.values(byEmployee).map(t => ({
    ...t,
    qbHours: Math.round(t.qbHours * 100) / 100,
    clockHours: Math.round(t.clockHours * 100) / 100,
    diff: Math.round((t.clockHours - t.qbHours) * 100) / 100,
  })).sort((a, b) => Math.abs(b.diff) - Math.abs(a.diff));

  return resp(200, {
    ok: true, from, to,
    rows: rows.filter(r => Math.abs(r.diff) >= 0.25),   // only the days that disagree
    totals,
    grand: {
      qbHours:    Math.round(totals.reduce((s, t) => s + t.qbHours, 0) * 100) / 100,
      clockHours: Math.round(totals.reduce((s, t) => s + t.clockHours, 0) * 100) / 100,
    },
    _source: "neon",
  });
}

// ── CLOCK: switch class without leaving the job ────────────────────────────
// The QuickBooks Time workflow the crew already has: clock in on Travel, arrive,
// hit Switch, carry on against the SAME job as Contract / Rough & Service /
// Finish. Without it the only way to change class is to clock out and back in,
// which everyone eventually forgets to do — and then the whole day is Travel.
//
// A switch is a segment boundary, not an edit: the travel time is closed and
// banked as its own punch, and a fresh one opens at the same instant. Two rows,
// touching exactly, so the hours are unchanged and each carries its own class.
//
// ⚠ ONE STATEMENT, and the order matters. The DELETE, the closing INSERT and the
// re-opening INSERT are a single atomic step. If this were three calls, a failure
// between them would leave someone clocked OUT mid-shift with no way to tell —
// far worse than the switch simply failing.
async function handleClockSwitch(body, authUser) {
  if (!canUseTimeClock(authUser)) {
    return resp(403, { ok: false, error: "The time clock isn't switched on yet." });
  }
  const { class: cls, cityTaxes, clientPunchId, at, jobId } = body || {};
  if (!cls) return resp(400, { ok: false, error: "Pick what you're switching to." });
  if (!clientPunchId) return resp(400, { ok: false, error: "Missing clientPunchId." });
  if (cityTaxes != null && !PR_CITY_TAX_OPTS.includes(String(cityTaxes))) {
    return resp(400, { ok: false, error: `Unknown city tax: ${cityTaxes}` });
  }
  const stamp = clampPunchTime(at);
  if (!stamp) return resp(400, { ok: false, error: "Invalid or out-of-range switch time." });

  const me = await clockEmployee(authUser);
  if (!me) return resp(400, { ok: false, error: "No employee record found for this login." });

  // Replay guard, same as punch-out: the switch already landed and the phone is
  // retrying, so the segment it closed already exists.
  const done = await neonQuery(
    `SELECT id FROM clock_punches WHERE client_punch_id = $1`, [clientPunchId]);
  if (done?.rows?.length) {
    const cur = await neonQuery(
      `SELECT started_at, job_name, class, city_taxes, client_punch_id,
              break_seconds::float8 AS break_seconds, break_started_at
         FROM open_punches WHERE employee_id = $1`, [me.id]);
    return resp(200, { ok: true, replayed: true, open: cur?.rows?.[0] || null });
  }

  const rows = await neonWrite("clock.switch",
    `WITH shift AS (
       -- break_started_at IS NULL: you cannot switch mid-lunch. What that would
       -- even mean is ambiguous, and guessing would silently mis-bank the break.
       DELETE FROM open_punches
        WHERE employee_id = $1 AND break_started_at IS NULL
        RETURNING *
     ), calc AS (
       -- ⚠ THE BOUNDARY IS ROUNDED UP TO THE NEXT WHOLE MINUTE, and both segments
       -- meet there exactly. This is what QuickBooks Time does, so the crew reads
       -- the same shape of timesheet they already know.
       --
       -- It replaced a one-minute GAP (end 09:44:23, restart 09:45:23), which
       -- avoided a shared timestamp but quietly cost a minute of pay on every
       -- switch. Meeting at a rounded minute costs nothing: Postgres treats
       -- touching ends as non-overlapping, durations cannot double-count, and the
       -- up-to-59-seconds of rounding lands in the employee's favour — the same
       -- direction as the weekly round-up elsewhere in payroll.
       SELECT s.*,
              (SELECT CASE WHEN date_trunc('minute', b) = b THEN b
                           ELSE date_trunc('minute', b) + INTERVAL '1 minute' END
                 FROM (SELECT GREATEST($3::timestamptz, s.started_at) AS b) _r
              ) AS boundary
         FROM shift s
     ), closed AS (
       INSERT INTO clock_punches
         (employee_id, employee_name, started_at, ended_at, work_date, duration_seconds,
          break_seconds, job_id, job_name, class, city_taxes, notes,
          start_lat, start_lon, client_punch_id,
          original_started_at, edited_at, edited_by)
       SELECT c.employee_id, $2, c.started_at, c.boundary,
              (c.started_at AT TIME ZONE 'America/New_York')::date,
              GREATEST(0, EXTRACT(EPOCH FROM (c.boundary - c.started_at)) - c.break_seconds)::numeric,
              c.break_seconds, c.job_id, c.job_name, c.class, c.city_taxes, c.notes,
              c.start_lat, c.start_lon, c.client_punch_id,
              c.original_started_at, c.edited_at, c.edited_by
         FROM calc c
       RETURNING id, class, duration_seconds
     ), reopened AS (
       -- The new segment starts exactly where the old one ended — at the rounded
       -- minute computed in the calc CTE above. See the note there for why that
       -- beats the one-minute gap this replaced.
       --
       -- The JOB can change here too, not just the class: the real move is
       -- office in the morning, then out to a site.
       INSERT INTO open_punches
         (employee_id, started_at, job_id, job_name, class, city_taxes,
          start_lat, start_lon, client_punch_id)
       SELECT c.employee_id, c.boundary,
              COALESCE(j.id, c.job_id), COALESCE(j.po_locked, c.job_name), $4,
              COALESCE($5, c.city_taxes), c.start_lat, c.start_lon, $6
         FROM calc c
         LEFT JOIN jobs j ON j.airtable_id = NULLIF($7::text, '') OR j.id::text = NULLIF($7::text, '')
       RETURNING started_at, job_name, class, city_taxes, client_punch_id,
                 break_seconds::float8 AS break_seconds, break_started_at
     )
     SELECT (SELECT class FROM closed) AS closed_class,
            (SELECT duration_seconds::float8 FROM closed) AS closed_seconds,
            r.* FROM reopened r`,
    [me.id, me.name || null, stamp, String(cls),
     cityTaxes == null ? null : String(cityTaxes), clientPunchId,
     (jobId && String(jobId).startsWith("rec")) ? String(jobId) : ""]);

  const out = rows?.[0];
  if (!out) {
    // Nothing switched. Say which of the two reasons it was.
    const cur = await neonQuery(
      `SELECT break_started_at FROM open_punches WHERE employee_id = $1`, [me.id]);
    if (!cur?.rows?.length) return resp(409, { ok: false, error: "You're not clocked in." });
    return resp(409, { ok: false, error: "You're on a break — come back from that first." });
  }

  return resp(200, {
    ok: true,
    closed: { class: out.closed_class, seconds: Number(out.closed_seconds) || 0 },
    open: {
      started_at: out.started_at, job_name: out.job_name, class: out.class,
      city_taxes: out.city_taxes, client_punch_id: out.client_punch_id,
      break_seconds: 0, break_started_at: null,
    },
  });
}

// Turn one recorded punch into a payroll row. Idempotent: the `time_entry_id IS NULL`
// guard means a second call is a no-op rather than a double-count.
//
// source = 'Clock' is required, not cosmetic — te_has_a_key rejects a row that names
// no origin, and a clock row has neither an Airtable nor a QB id (see 018).
//
// NO AIRTABLE MIRROR, unlike handleCreateTimeEntry. Make left the time path at Step 3
// and the Airtable Time Entries table is a frozen historical copy; writing punches
// into it would put new rows in something nothing reads and everything treats as
// closed. The clock is Neon-native from birth.
async function promoteClockPunch(punchId) {
  const rows = await neonWrite("clock.promote",
    `WITH p AS (
       SELECT * FROM clock_punches WHERE id = $1 AND time_entry_id IS NULL
                                     AND deleted_at IS NULL
     ), ins AS (
       INSERT INTO time_entries
         (employee_name, employee_id, work_date, duration_seconds, city_taxes, class,
          job_id, job_name, labor_reviewed, source, started_at, ended_at)
       SELECT p.employee_name, p.employee_id, p.work_date, p.duration_seconds,
              COALESCE(p.city_taxes, 'A No Tax'), COALESCE(p.class, 'Contract'),
              p.job_id, p.job_name, false, 'Clock', p.started_at, p.ended_at
         FROM p
       RETURNING id
     )
     UPDATE clock_punches c
        SET time_entry_id = (SELECT id FROM ins), promoted_at = now()
      WHERE c.id = $1 AND EXISTS (SELECT 1 FROM ins)
      RETURNING c.time_entry_id`,
    [punchId]);
  return rows?.[0]?.time_entry_id || null;
}

// ── JOB SETTING: which city tax applies to work on this job ────────────────
// Set once per job by a human, because the site address genuinely cannot answer
// it — see the header of db/schema/020_job_city_tax.sql for the reasoning and the
// owner's own example (a Columbiana mailing address outside Columbiana's limits).
//
// Neon-only: there is no Airtable twin, so there is nothing to mirror. That is
// safe precisely because the column is absent from _jobs-sync.js's FIELDS list;
// adding it there would make the hourly sync overwrite this with Airtable's NULL.
//
// Travel is NOT settable here. "Travel is always no city tax" is a company rule,
// so it lives in one place in the clock rather than as a column that could be set
// inconsistently across 112 jobs.
async function handleUpdateJobCityTax(body) {
  const { jobId, cityTax } = body || {};
  if (!jobId || !isJobHandle(jobId)) {
    return resp(400, { ok: false, error: "Missing or invalid jobId." });
  }
  // null/"" clears it back to "not yet decided", which is a legitimate state and
  // distinct from "A No Tax". Anything else must be a known option — a stray value
  // here would be written verbatim into payroll's city_taxes and silently fall back
  // to "A No Tax" downstream, which is the failure this whole feature exists to stop.
  const raw = cityTax == null ? null : String(cityTax).trim();
  const value = (raw === "" ? null : raw);
  if (value !== null && !PR_CITY_TAX_OPTS.includes(value)) {
    return resp(400, { ok: false, error: `Unknown city tax: ${value}` });
  }

  const rows = await neonWrite("job.setCityTax",
    `UPDATE jobs SET city_tax = $2 WHERE airtable_id = $1 OR id::text = $1 RETURNING airtable_id, city_tax`,
    [String(jobId), value]);
  if (!rows?.length) return resp(404, { ok: false, error: "Job not found." });

  return resp(200, { ok: true, jobId, cityTax: rows[0].city_tax ?? null });
}

// ── JOB SETTING: does this job appear on the time clock ────────────────────
// Overhead buckets — Shop Work, Office Work — are jobs only because the clock has
// to pick something. Deciding their visibility from STATUS meant a routine tidy-up
// (marking Shop Work Completed) would silently drop it from every employee's
// picker, and ~500 h/yr would start landing elsewhere unnoticed. This says it out
// loud instead. See db/schema/027.
const CLOCK_VIS_OPTS = ["all", "admin", "hidden"];

async function handleUpdateJobClockVisibility(body) {
  const { jobId, visibility } = body || {};
  if (!jobId || !isJobHandle(jobId)) {
    return resp(400, { ok: false, error: "Missing or invalid jobId." });
  }
  const raw = visibility == null ? null : String(visibility).trim();
  const value = (raw === "" ? null : raw);   // null = back to status-driven
  if (value !== null && !CLOCK_VIS_OPTS.includes(value)) {
    return resp(400, { ok: false, error: `Unknown clock visibility: ${value}` });
  }

  const rows = await neonWrite("job.setClockVisibility",
    `UPDATE jobs SET clock_visibility = $2 WHERE airtable_id = $1 OR id::text = $1
     RETURNING airtable_id, clock_visibility`, [String(jobId), value]);
  if (!rows?.length) return resp(404, { ok: false, error: "Job not found." });
  return resp(200, { ok: true, jobId, clockVisibility: rows[0].clock_visibility ?? null });
}

// ══ WHO'S WORKING — the admin roster ═════════════════════════════════════════
// Owner's ask, 2026-08-08: see who is clocked in, and be able to punch people in
// and out. Editing and adding time deliberately stays in Payroll, which already
// does it properly — this screen is about the LIVE picture, not corrections.
//
// Strict admin (_ADMIN), not admin+office: this reads where every person is right
// now and can start or stop their paid time. Office is already excluded from
// payroll everywhere else in this file and stays excluded here.
async function handleClockRoster(params) {
  if (timeClockAudience() === "off") {
    return resp(200, { ok: true, enabled: false, onClock: [], offClock: [], today: [] });
  }

  // Everyone currently on the clock. employees is joined rather than trusting the
  // punch's own snapshot, because this view is about who is working NOW — the
  // current name is the right one to show.
  const onClock = await neonQuery(
    `SELECT COALESCE(e.airtable_id, e.id::text) AS employee_id, e.name,
            o.started_at, o.job_name, o.class, o.city_taxes,
            o.break_seconds::float8 AS break_seconds, o.break_started_at
       FROM open_punches o
       JOIN employees e ON e.id = o.employee_id
      ORDER BY o.started_at`);

  // Who COULD be clocked in: active, payroll-eligible, not already on the clock.
  // Office and viewers are excluded — they have no hours, so offering to punch
  // them in would be offering to create a payroll row that shouldn't exist.
  const offClock = await neonQuery(
    `SELECT COALESCE(e.airtable_id, e.id::text) AS employee_id, e.name
       FROM employees e
      WHERE e.active IS TRUE
        AND lower(coalesce(e.role, '')) IN ('admin', 'employee')
        AND NOT EXISTS (SELECT 1 FROM open_punches o WHERE o.employee_id = e.id)
      ORDER BY e.name`);

  // Everything already finished today, so a glance answers "has Dave been in yet"
  // and not just "is Dave here this second". Same local-date rule as the punch
  // itself — see the overnight note in db/schema/018_time_clock.sql.
  const today = await neonQuery(
    `SELECT COALESCE(e.airtable_id, e.id::text) AS employee_id, e.name,
            c.started_at, c.ended_at,
            c.duration_seconds::float8 AS duration_seconds,
            c.break_seconds::float8 AS break_seconds,
            c.job_name, (c.time_entry_id IS NOT NULL) AS counted
       FROM clock_punches c
       JOIN employees e ON e.id = c.employee_id
      WHERE c.work_date = (now() AT TIME ZONE 'America/New_York')::date
        AND c.deleted_at IS NULL
      ORDER BY c.ended_at DESC`);

  return resp(200, {
    ok: true,
    enabled: true,
    countsTowardPayroll: timeClockFeedsPayroll(),
    // Hours after which a shift is almost certainly a forgotten clock-out rather
    // than a long day. Sent from the server so the roster and each person's own
    // screen can't drift apart on what counts as "too long".
    longShiftHours: LONG_SHIFT_HOURS,
    onClock:  onClock?.rows  || [],
    offClock: offClock?.rows || [],
    today:    today?.rows    || [],
    _source: "neon",
  });
}

// ── ADMIN: punches over a range, for the timesheet view ────────────────────
// The roster answers "who is on site now". This answers "what did everyone
// actually work", which needs weeks rather than today — grouping is done in the
// client, so this stays a flat, cheap read.
async function handleClockPunches(params) {
  const from = /^\d{4}-\d{2}-\d{2}$/.test(String(params?.from || "")) ? String(params.from) : null;
  const to   = /^\d{4}-\d{2}-\d{2}$/.test(String(params?.to   || "")) ? String(params.to)   : null;
  if (!from || !to) return resp(400, { ok: false, error: "Need from and to dates (YYYY-MM-DD)." });

  const q = await neonQuery(
    // ⚠ Dates are formatted by POSTGRES, not stringified in JS. The driver hands
    // back a DATE column as a JS Date, so String(d).slice(0,10) produced
    // "Mon Aug 10" — which the client then split on "-" and got NaN from, hence
    // "Week of Invalid Date". to_char removes the guesswork about the wire format.
    `SELECT c.id, e.name AS employee, COALESCE(e.airtable_id, e.id::text) AS employee_id,
            to_char(c.work_date, 'YYYY-MM-DD') AS work_date,
            -- Monday of that week. Matches time_entries.week_start_date, which is
            -- what payroll groups on, so a week here is the same week there.
            to_char(c.work_date - (EXTRACT(ISODOW FROM c.work_date)::int - 1),
                    'YYYY-MM-DD') AS week_start,
            c.started_at, c.ended_at, c.class, c.job_name,
            c.duration_seconds::float8 AS duration_seconds,
            c.break_seconds::float8    AS break_seconds,
            (c.time_entry_id IS NOT NULL) AS counted,
            (c.edited_at IS NOT NULL)     AS edited
       FROM clock_punches c
       JOIN employees e ON e.id = c.employee_id
      WHERE c.deleted_at IS NULL
        AND c.work_date BETWEEN $1::date AND $2::date
      ORDER BY e.name, c.work_date DESC, c.started_at`,
    [from, to]);

  const rows = (q?.rows || []).map(r => ({
    id: r.id,
    employee: r.employee,
    employeeId: r.employee_id,
    workDate:  r.work_date,      // already 'YYYY-MM-DD' from to_char
    weekStart: r.week_start,
    startedAt: r.started_at,
    endedAt:   r.ended_at,
    class:     r.class || "",
    job:       r.job_name || "",
    hours:     Math.round(((Number(r.duration_seconds) || 0) / 3600) * 100) / 100,
    breakMins: Math.round((Number(r.break_seconds) || 0) / 60),
    counted:   r.counted === true,
    edited:    r.edited === true,
  }));

  // Overlaps are computed here rather than in the browser: it is a per-person
  // pairwise check, and this is the screen where a double-counted shift should
  // be visible instead of only turning up when someone queries the database.
  const byEmp = {};
  for (const r of rows) (byEmp[r.employee] ||= []).push(r);
  const overlapping = new Set();
  for (const list of Object.values(byEmp)) {
    const sorted = list.slice().sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
    for (let i = 1; i < sorted.length; i++) {
      if (Date.parse(sorted[i].startedAt) < Date.parse(sorted[i - 1].endedAt)) {
        overlapping.add(sorted[i].id); overlapping.add(sorted[i - 1].id);
      }
    }
  }
  rows.forEach(r => { r.overlaps = overlapping.has(r.id); });

  return resp(200, { ok: true, from, to, rows, _source: "neon" });
}

// Resolve the person an admin is acting ON. Airtable rec id, because that is what
// every other people-facing action in this file takes and what the client holds.
async function clockEmployeeById(employeeId) {
  if (!employeeId || !isEmployeeHandle(employeeId)) return null;
  const q = await neonQuery(
    `SELECT id, name FROM employees WHERE airtable_id = $1 OR id::text = $1`, [String(employeeId)]);
  return q?.rows?.[0] || null;
}

// Admin punching somebody else in / out. These are SEPARATE actions rather than an
// optional employeeId on clockIn/clockOut, deliberately: the self-service handlers
// keep the property that the employee can only ever come from the token, so there
// is no parameter on them that a field phone could abuse. The privilege lives in
// its own action with its own _ADMIN tier.
//
// No clientPunchId is required from the caller — an admin at a desk has no offline
// replay queue — so one is minted here to satisfy the same replay guards.
async function handleAdminClockIn(body, authUser) {
  const emp = await clockEmployeeById(body?.employeeId);
  if (!emp) return resp(400, { ok: false, error: "Pick a person to clock in." });
  return await handleClockIn(
    { ...body, clientPunchId: body?.clientPunchId || `admin-${randomId()}` },
    authUser, emp);
}

async function handleAdminClockOut(body, authUser) {
  const emp = await clockEmployeeById(body?.employeeId);
  if (!emp) return resp(400, { ok: false, error: "Pick a person to clock out." });
  // The open shift's own client_punch_id wins inside handleClockOut (COALESCE on
  // the shift row), so this fallback only matters for a shift that somehow has
  // none. Clocking out someone who is on a break closes the break first — that is
  // handled in the punch-out statement, not here.
  return await handleClockOut(
    { ...body, clientPunchId: body?.clientPunchId || `admin-${randomId()}` },
    authUser, emp);
}

function randomId() {
  return (globalThis.crypto?.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`);
}

// ── CLOCK: admin backfill at cutover ───────────────────────────────────────
// Everything punched while TIME_CLOCK_PAYROLL was off sits in clock_punches with
// time_entry_id NULL. This is what counts it, once, when the switch is thrown.
// Admin only, and it refuses to run while the switch is still off so it can't be used
// to sneak hours into payroll ahead of the decision.
async function handlePromoteClockPunches(body) {
  if (!timeClockFeedsPayroll()) {
    return resp(400, { ok: false,
      error: "TIME_CLOCK_PAYROLL is off. Turn it on before promoting punches." });
  }
  if (body?.confirm !== "YES") {
    return resp(400, { ok: false, error: 'Missing confirmation. Pass {"confirm":"YES"}.' });
  }
  const from = typeof body?.from === "string" ? body.from : null;

  const pending = await neonQuery(
    `SELECT id FROM clock_punches
      WHERE time_entry_id IS NULL AND ($1::date IS NULL OR work_date >= $1::date)
        AND deleted_at IS NULL
      ORDER BY started_at`, [from]);

  const ids = (pending?.rows || []).map(r => r.id);
  let promoted = 0;
  const failed = [];
  for (const id of ids) {
    try { if (await promoteClockPunch(id)) promoted++; }
    catch (e) { failed.push({ id, error: String(e?.message || e).slice(0, 200) }); }
  }
  return resp(200, { ok: true, pending: ids.length, promoted, failed });
}

// Accepts an ISO timestamp from the client and rejects anything absurd. Returns an
// ISO string, or null if unusable.
//
// The window is ±36 h around now. It has to be wide enough for a genuinely late
// replay (a phone that stayed in a basement all day, an overnight shift) and tight
// enough that a device with a wrong year cannot file hours into a closed pay period.
function clampPunchTime(iso) {
  const t = iso ? Date.parse(iso) : Date.now();
  if (!Number.isFinite(t)) return null;
  const now = Date.now();
  const WINDOW = 36 * 60 * 60 * 1000;
  if (t > now + WINDOW || t < now - WINDOW) return null;
  return new Date(t).toISOString();
}

function numOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
// ── REMOVED 2026-08-09: handleBackfillTimeEntryEmployeeLinks (139 lines) ──
// A one-shot repair that reconciled Employee text vs Employee (Linked) on the
// AIRTABLE Time Entries table. Dead since Step 3 (2026-08-07): time entries are
// fully Neon and that Airtable table is a frozen historical copy, so the rows it
// existed to fix are no longer read by anything. Nothing in either SPA ever
// called it — it was token-gated and run by hand.
//
// Deleted rather than ported to Neon during Stage 4 of the employees migration.
// If a link-repair is ever needed again it belongs in db/etl/, not in a request
// handler with a 10-second budget — the original had to narrow its scan to 2025+
// just to avoid Netlify timing out.

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
  // Neon-first (audit item 02). Same rule as the Airtable filter below: exact
  // match on both period dates, non-superseded, newest first.
  if (neonEnabled()) {
    const q = await neonQuery(
      // ⚠⚠ COALESCE, NOT a bare airtable_id (cutover slice 2, 2026-08-24). This
      // is the sharpest of the id-form sites: a bare `airtable_id` returns NULL
      // for a native run, the client reads `found.runId || null`, decides there
      // is no prior run for the period, and SKIPS the supersede confirm dialog
      // entirely. The result is two non-superseded runs on one period — which
      // `computePayrollDateRanges` then resolves by generated_at, moving every
      // payroll tile by a fortnight. Silent, and on the screens people are paid
      // from. Every run lookup accepts either form (db/schema/054).
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id, generated_at, generated_by
         FROM payroll_runs
        WHERE pay_period_start = $1::date AND pay_period_end = $2::date AND NOT superseded
        ORDER BY generated_at DESC NULLS LAST LIMIT 1`, [payPeriodStart, payPeriodEnd]);
    if (q?.rows) {
      if (!q.rows.length) return resp(200, { ok: true, runId: null, generatedAt: null, generatedBy: null });
      const n = q.rows[0];
      return resp(200, {
        ok: true,
        runId: n.airtable_id,                                     // ⚠ EITHER id form — the client
        generatedAt: n.generated_at ? new Date(n.generated_at).toISOString() : null,
        generatedBy: n.generated_by || null,                      // passes it back as supersedesId
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`findMatchingPayrollRun: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
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
    if (typeof b.employeeId === "string" && isEmployeeHandle(b.employeeId)) {
      resolvedBonuses.push(b);
    } else {
      unresolvedBonuses.push({ employeeName: b.employeeName || null, amount: Number(b.amount) });
    }
  });

  // ── NEON-FIRST since 2026-08-24 (cutover slice 2, db/schema/054 + 056) ────
  // The gate written into 054's header was met by the 2026-08-09 → 08-22 run:
  // `pdf_key`/`json_key` both stamped, `r2Error` null, and the PDF opened from
  // the Payroll Archive tab. That last one is only evidence because ALL 29 runs
  // carry a key — `payrollRunsList` falls back to Airtable wholesale if even
  // one is null, so with a gap the "Reprint" click would have been reading an
  // Airtable attachment and proving nothing.
  //
  // The keys are the proof of the R2 write, not just a record of it: they are
  // stamped only after BOTH `putBufferToR2` calls resolve, and cleared on any
  // failure. Non-null keys therefore mean the PUTs succeeded.

  // ⚠ CHECKED BEFORE ANYTHING IS WRITTEN, and this is the one hard refusal in
  // the handler. A native run has no Airtable record, so R2 holds the only copy
  // of the artifact people are paid from. With R2 unconfigured there is no safe
  // way to archive at all — better to refuse up front than to create a run and
  // unwind it. R2 is "optional as a group" everywhere else in this codebase;
  // here it is required.
  if (!r2Enabled()) {
    return resp(503, { ok: false,
      error: "The payroll archive (R2) is not configured, so the PDF could not be stored. Nothing was saved." });
  }

  // 1. Create the run in Neon. It is born with NO rec id — Airtable is a mirror
  //    from here down. `supersedes_id` is resolved inline from either id form,
  //    so a run can never exist with the flag set on its predecessor and no
  //    record of what replaced it.
  const runRows = await neonWrite("payrollRun.create",
    `INSERT INTO payroll_runs (pay_period_start, pay_period_end, generated_at, generated_by,
                               total_hours, total_bonus, superseded, notes, supersedes_id, synced_at)
     VALUES ($1::date, $2::date, now(), $3, $4::numeric, $5::numeric, false, $6,
             (SELECT id FROM payroll_runs WHERE airtable_id = $7 OR id::text = $7), now())
     RETURNING id`,
    [payPeriodStart, payPeriodEnd, String(generatedBy),
     Number(totalHours) || 0, Number(totalBonus) || 0,
     (typeof notes === "string" && notes.trim()) ? notes.trim() : null,
     supersedesId ? String(supersedesId) : null]);
  const neonRunId = runRows?.[0]?.id;
  if (!neonRunId) {
    return resp(502, { ok: false, error: "Couldn't archive the payroll run. Nothing was saved — please try again." });
  }

  // 2. THE ARCHIVE — and the contract here is the inverse of what it was.
  //
  // ⚠⚠ WHILE THE RUN WAS AIRTABLE-FIRST this write was reported, not thrown:
  // the PDF was also an Airtable attachment, so an R2 failure cost nothing and
  // `payrollRunsList` fell back to the attachment. Neither of those is true of
  // a native run. R2 is now the ONLY copy, so a failure here has to fail the
  // request — a run row whose PDF cannot be retrieved is worse than no run.
  //
  // The Neon row is deleted on failure so a retry does not leave a shadow run
  // (which would also make this period look superseded to the next save). The
  // client answers a throw with showArchiveErrorModal(), which offers a retry
  // AND still hands over the locally-generated PDF — so refusing loses nothing.
  //
  // The key is stamped, not named after the file: two runs for the same period
  // (an original and its correction) would otherwise collide, and the correction
  // is exactly the case where you must still be able to open the original.
  let pdfKey = null, jsonKey = null;
  try {
    const stamp = new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14);
    const prefix = payrollPrefix(neonRunId);
    await putBufferToR2(`${prefix}${stamp}.pdf`,  Buffer.from(pdfBase64,  "base64"), "application/pdf");
    await putBufferToR2(`${prefix}${stamp}.json`, Buffer.from(jsonBase64, "base64"), "application/json");
    pdfKey  = `${prefix}${stamp}.pdf`;
    jsonKey = `${prefix}${stamp}.json`;
    await neonWrite("payrollRun.files",
      `UPDATE payroll_runs SET pdf_key = $2, json_key = $3, synced_at = now() WHERE id = $1::uuid`,
      [neonRunId, pdfKey, jsonKey]);
  } catch (err) {
    console.error("[payrollRunCreate] R2 archive failed, rolling back the run:", err);
    try {
      await neonWrite("payrollRun.rollback", `DELETE FROM payroll_runs WHERE id = $1::uuid`, [neonRunId]);
    } catch (delErr) {
      console.error("[payrollRunCreate] rollback DELETE failed — orphan run:", neonRunId, delErr);
    }
    return resp(500, { ok: false, error: `Archiving the payroll PDF failed: ${err.message}. Nothing was saved.` });
  }

  // The Airtable mirror's field payload. Built here rather than at the top
  // because nothing above needs it any more.
  const runFields = {};
  runFields[PR_RUNS.payPeriodStart] = payPeriodStart;
  runFields[PR_RUNS.payPeriodEnd]   = payPeriodEnd;
  runFields[PR_RUNS.generatedAt]    = new Date().toISOString();
  runFields[PR_RUNS.generatedBy]    = String(generatedBy);
  runFields[PR_RUNS.totalHours]     = Number(totalHours);
  runFields[PR_RUNS.totalBonus]     = Number(totalBonus);
  // ⚠ Only a rec id can go in an Airtable link. Once a prior run is native its
  // uuid has nothing to point at, and the supersede chain lives in Neon alone —
  // which is why the Neon UPDATE below is unconditional and this is not.
  if (supersedesId && String(supersedesId).startsWith("rec")) {
    runFields[PR_RUNS.supersedes] = [supersedesId];
  }
  if (typeof notes === "string" && notes.trim()) {
    runFields[PR_RUNS.notes] = notes.trim();
  }

  // 3. Bonuses, into Neon. The employee handle is still an Airtable rec id —
  //    employees are slice 5 — but the RUN link is the uuid, so a bonus is
  //    reachable from its run before Airtable has heard of either.
  let bonusError = null;
  const neonBonuses = [];
  if (resolvedBonuses.length) {
    try {
      for (const b of resolvedBonuses) {
        const rows = await neonWrite("payrollBonus.create",
          `INSERT INTO payroll_bonuses (payroll_run_id, employee_airtable_id, employee_name, amount, synced_at)
           VALUES ($1::uuid, $2, $3, $4::numeric, now())
           RETURNING id`,
          [neonRunId, String(b.employeeId), String(b.employeeName || ""), Number(b.amount) || 0]);
        if (rows?.[0]?.id) {
          neonBonuses.push({
            id: rows[0].id,
            employeeId: String(b.employeeId),
            amount: Number(b.amount) || 0
          });
        }
      }
    } catch (err) {
      console.error("[payrollRunCreate] bonus create failed:", err);
      bonusError = err.message || "Bonus create failed";
    }
  }

  // 4. Supersede the prior run — in Neon, UNCONDITIONALLY, accepting either id
  //    form.
  //
  // ⚠⚠ NOT gated on the Airtable PATCH the way it used to be. Once a prior run
  // can itself be native there is no record to patch, and the supersede chain
  // exists in Neon alone. Skipping the flag leaves TWO non-superseded runs on
  // one period, and `computePayrollDateRanges` answers with "the newest
  // non-superseded Pay Period End" — a plausible wrong fortnight on every
  // payroll screen, not an error. The period 2026-07-26 → 08-08 already carries
  // six runs, five superseded, so this is the normal case and not an edge one.
  let supersedeError = null;
  if (supersedesId) {
    try {
      await neonWrite("payrollRun.supersede",
        `UPDATE payroll_runs SET superseded = true, synced_at = now()
          WHERE airtable_id = $1 OR id::text = $1`,
        [String(supersedesId)]);
    } catch (err) {
      console.error("[payrollRunCreate] supersede failed:", err);
      supersedeError = err.message || "Supersede failed";
    }
  }

  // 5. THE AIRTABLE MIRROR — best-effort from here down, the same contract as
  //    slices 1 and 3 and the exact inverse of the Neon mirror this handler
  //    used to carry. The run, its archive and its bonuses are already real; an
  //    Airtable problem must not fail a request whose work has landed.
  const created = await mirrorToAirtable("payrollRunCreate", () =>
    atFetch(`${encodeURIComponent(PR_RUNS.table)}`, {
      method: "POST",
      body: JSON.stringify({ fields: runFields, typecast: true })
    }));
  const runRecId = created?.id || null;

  if (runRecId) {
    // Attachments. A native run does not need them — R2 holds both files and
    // `payrollRunsList` serves from `pdf_key` — but while the mirror exists the
    // Airtable record should not look like an empty shell to anyone reading the
    // base directly. No rollback: a failure here costs a mirror, not the run.
    await mirrorToAirtable("payrollRunCreate.attachments", async () => {
      await uploadAirtableAttachment(runRecId, PR_RUNS.pdf,         pdfBase64,  pdfFilename,  "application/pdf");
      await uploadAirtableAttachment(runRecId, PR_RUNS.jsonPayload, jsonBase64, jsonFilename, "application/json");
    });

    // ⚠ THE STAMP IS SAFE ON THIS TABLE, and the reasoning is not transferable.
    // The slice-0 rule is about R2 KEYS: a handle that flips from uuid to rec
    // id orphans every file keyed on the old one. Payroll keys are
    // `payroll/<neon uuid>/<stamp>.(pdf|json)` — uuid-based — so nothing moves.
    // Contrast expenses in slice 4, where the receipt key IS the rec id and
    // stamping would orphan the files.
    //
    // ⚠ The other slice-0 warning — a failed stamp DUPLICATING the row via an
    // `ON CONFLICT (airtable_id)` sync helper — does not apply either: nothing
    // re-reads Airtable to insert payroll rows. There is no ETL for this table
    // (verified across _*.js, qb-time-pull.js and db/etl/). A failed stamp just
    // leaves the run Neon-only, which every read already handles.
    await neonWrite("payrollRun.stamp",
      `UPDATE payroll_runs SET airtable_id = $2, synced_at = now() WHERE id = $1::uuid`,
      [neonRunId, runRecId]).catch((e) =>
        console.error(`payrollRunCreate: rec id not stamped, run is Neon-only — ${e?.message || e}`));

    // Bonus mirror, chunked at 10 per batch (Airtable cap), then stamped back.
    if (neonBonuses.length && !bonusError) {
      await mirrorToAirtable("payrollRunCreate.bonuses", async () => {
        for (let i = 0; i < neonBonuses.length; i += 10) {
          const chunk = neonBonuses.slice(i, i + 10);
          const records = chunk.map(b => {
            const f = {};
            f[PR_BONUSES.amount]         = b.amount;
            f[PR_BONUSES.employee]       = [b.employeeId];
            f[PR_BONUSES.payrollRun]     = [runRecId];
            f[PR_BONUSES.payPeriodStart] = payPeriodStart;
            f[PR_BONUSES.payPeriodEnd]   = payPeriodEnd;
            return { fields: f };
          });
          const res = await atFetch(`${encodeURIComponent(PR_BONUSES.table)}`, {
            method: "POST",
            body: JSON.stringify({ records, typecast: true })
          });
          // ⚠ Airtable returns created records IN REQUEST ORDER, which is the
          // only reason this positional zip is sound. Do NOT recover the ids by
          // re-reading with FIND(runRecId, ARRAYJOIN({Payroll Run})) the way the
          // old Neon mirror did — that is the cross-job substring trap wearing a
          // different hat, and it would also sweep up bonuses from any run whose
          // rec id happens to share a prefix.
          const back = Array.isArray(res?.records) ? res.records : [];
          for (let j = 0; j < back.length; j++) {
            const local = chunk[j];
            if (!back[j]?.id || !local?.id) continue;
            await neonWrite("payrollBonus.stamp",
              `UPDATE payroll_bonuses SET airtable_id = $2, payroll_run_airtable_id = $3, synced_at = now()
                WHERE id = $1::uuid`,
              [local.id, back[j].id, runRecId]).catch((e) =>
                console.error(`payrollRunCreate: bonus rec id not stamped — ${e?.message || e}`));
          }
        }
      });
    }
  }

  // The prior run's Airtable flag. Only when it HAS a rec id: a native
  // predecessor has nothing to patch, and step 4 already recorded the truth.
  if (supersedesId && String(supersedesId).startsWith("rec")) {
    await mirrorToAirtable("payrollRunCreate.supersede", () => {
      const patchFields = {};
      patchFields[PR_RUNS.superseded] = true;
      return atFetch(`${encodeURIComponent(PR_RUNS.table)}/${supersedesId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: patchFields })
      });
    });
  }

  return resp(200, {
    ok: true,
    // ⚠ Either form, rec id preferred while the mirror still runs. The client
    // hands this back as `supersedesId` on the next save for the same period,
    // and every run lookup accepts both (db/schema/054).
    runId: runRecId || String(neonRunId),
    supersededId: supersedesId || null,
    bonusError,
    supersedeError,
    // Unconditionally true: an R2 failure returned 500 above and never reaches
    // here, so this can no longer be false. Kept because the gate in 054's
    // header names it and someone will look for it.
    pdfArchived: true,
    _airtableMirrored: !!runRecId,
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

  // ── NEON-FIRST (audit item 02/04) ─────────────────────────────────────────
  // The last read in the field app that Airtable still owned, and it was owned
  // for one reason: the PDF is an Airtable ATTACHMENT, whose url is signed and
  // short-lived. There is no link to store, so the only way to hand someone a
  // working one was to ask Airtable at read time. db/schema/052 moved the files
  // to R2 and gave the run a `pdf_key`, which is a stable handle.
  //
  // The "Superseded By" resolution that used to need an in-memory id map is now
  // a LEFT JOIN in the other direction — `s.supersedes_id = r.id` — so the
  // whole-table fetch and the second pass both go away.
  //
  // ⚠ FALLS BACK ON A RUN WITH NO pdf_key, not just on a Neon failure. Until
  // the backfill has run, some runs have their PDF only in Airtable, and a
  // payroll archive that lists a run with no way to open it is worse than a
  // slower page. One missing key sends the WHOLE list back to Airtable rather
  // than serving a half-linked grid.
  const q = await neonQuery(
    // ⚠ COALESCE — the dual handle (cutover slice 2, db/schema/054). Inert
    // today because every run still has a rec id, and that is the point: when
    // handlePayrollRunCreate is finally reversed, this read does not have to
    // move with it. A bare airtable_id would hand the grid a null run id and
    // the row's PDF link would be unopenable.
    `SELECT COALESCE(r.airtable_id, r.id::text) AS airtable_id,
            r.pay_period_start::text  AS pay_period_start,
            r.pay_period_end::text AS pay_period_end, r.generated_at,
            r.generated_by, r.total_hours, r.total_bonus, r.superseded, r.notes,
            r.pdf_key,
            (SELECT count(*) FROM payroll_bonuses b WHERE b.payroll_run_id = r.id) AS bonus_count,
            COALESCE(s.airtable_id, s.id::text) AS superseded_by_id,
            s.generated_at AS superseded_by_date
       FROM payroll_runs r
       LEFT JOIN payroll_runs s ON s.supersedes_id = r.id
      ORDER BY r.pay_period_end DESC NULLS LAST, r.generated_at DESC NULLS LAST`);

  if (q?.rows?.length && q.rows.every(r => r.pdf_key)) {
    const visible = showSuperseded ? q.rows : q.rows.filter(r => r.superseded !== true);
    const runs = await Promise.all(visible.map(async (r) => {
      // Named for the human, not for the object: the R2 key is a timestamp, and
      // a file called "20260808120000.pdf" in someone's Downloads folder is
      // useless a fortnight later.
      const filename = `Payroll ${r.pay_period_start || "run"} to ${r.pay_period_end || ""}.pdf`.trim();
      return {
        id:                r.airtable_id,
        payPeriodStart:    r.pay_period_start || null,
        payPeriodEnd:      r.pay_period_end   || null,
        generatedAt:       r.generated_at ? new Date(r.generated_at).toISOString() : null,
        generatedBy:       r.generated_by || null,
        totalHours:        Number(r.total_hours) || 0,
        totalBonus:        Number(r.total_bonus) || 0,
        bonusCount:        Number(r.bonus_count) || 0,
        notes:             r.notes || null,
        superseded:        r.superseded === true,
        supersededByRunId: r.superseded_by_id || null,
        supersededByDate:  r.superseded_by_date ? new Date(r.superseded_by_date).toISOString() : null,
        pdfUrl:            await presignGetDownload(r.pdf_key, filename),
        pdfFilename:       filename,
        pdfAvailable:      true,
      };
    }));
    return resp(200, { ok: true, runs, _source: "neon", _ms: q.ms });
  }
  // ⚠ REFUSE ON A FAILED READ (2026-08-25): Airtable has been frozen since
  // writes went off, so falling back answers with yesterday's world, silently.
  if (q?.error) {
    console.error(`payrollRunsList: Neon read FAILED — refusing to serve frozen Airtable data: ${q.error}`);
    return resp(503, { ok: false, error: "Can't load payroll runs right now — the database is unavailable. Try again in a moment." });
  }
  // ⚠ THE OTHER FALLBACK STAYS, AND IT IS NOT THE SAME THING. This one fires
  // when Neon ANSWERED but some run has no pdf_key — the PDFs live in R2 and a
  // run missing its key cannot be served from Neon at all. That is a data gap
  // with a known fix (copyPayrollFilesToR2), not a database outage, and Airtable
  // still holds those historical attachments. It goes when the last run has a key.
  else if (q?.rows?.length) console.log("payrollRunsList: some runs have no pdf_key — serving from Airtable until copyPayrollFilesToR2 has run");

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

  // ── NEON-FIRST (audit item 02) ────────────────────────────────────────────
  // This ran on EVERY payroll read — all four handlers call it first — and it
  // paged the whole Airtable Payroll Runs table to find ONE date. It is the
  // 400-600 ms gap between `_ms` (the Neon leg) and wall time that the roadmap
  // noticed at Step 1 and never chased down. In Neon it is an index scan on
  // `pr_runs_open_period_idx`, which is partial on `WHERE NOT superseded` —
  // exactly the predicate below.
  //
  // ⚠ The whole answer is ONE value: the newest non-superseded Pay Period End.
  // Superseding matters and is not decoration: the period 2026-07-26 → 08-08
  // has SIX runs, five of them superseded. Reading the wrong one moves every
  // payroll tile by a fortnight.
  let payPeriodStart, payPeriodEnd, lastEndYmd = null;
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT pay_period_end::text AS pay_period_end
         FROM payroll_runs WHERE NOT superseded AND pay_period_end IS NOT NULL
        ORDER BY pay_period_end DESC LIMIT 1`);
    if (q?.rows?.length) lastEndYmd = q.rows[0].pay_period_end;
    else console.error(`computePayrollDateRanges: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
  }
  // Airtable fallback, unchanged. Kept because an empty Neon here would silently
  // shift every payroll figure rather than fail, which is the worst shape of bug
  // this file has: a plausible wrong number on a screen people are paid from.
  if (!lastEndYmd) {
    const recentRuns = await fetchAll(PR_RUNS.table, {
      filter: `NOT({Superseded})`,
      sortField: "Pay Period End",
      sortDir: "desc"
    });
    lastEndYmd = recentRuns.length ? (recentRuns[0].fields?.["Pay Period End"] || null) : null;
  }
  if (lastEndYmd) {
    const lastEnd = ymdToDate(lastEndYmd);
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

  const fromStr = dateToYmd(yearStart);

  // NEON-FIRST. The Airtable path below pages every entry from Jan 1 to today and
  // buckets them in JS; Postgres does the same work in one pass with FILTER clauses.
  // The outer WHERE bounds everything to [yearStart, today], so each FILTER inherits
  // that bound exactly as the JS loop does.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT round(coalesce(sum(hours) FILTER (WHERE work_date >= $2::date), 0), 2)::float8 AS wk,
              round(coalesce(sum(hours) FILTER (WHERE work_date >= $3::date
                                                  AND work_date <= $4::date), 0), 2)::float8 AS pp,
              round(coalesce(sum(hours) FILTER (WHERE work_date >= $5::date), 0), 2)::float8 AS mo,
              round(coalesce(sum(hours), 0), 2)::float8 AS ytd
         FROM time_entries
        WHERE work_date >= $1::date AND work_date <= $6::date`,
      [fromStr, dateToYmd(thisWeekStart), dateToYmd(payPeriodStart),
       dateToYmd(payPeriodEnd), dateToYmd(monthStart), todayStr]
    );
    if (q?.rows?.length) {
      const n = q.rows[0];
      return resp(200, {
        ok: true,
        asOf: todayStr,
        ranges: {
          thisWeek:  { start: dateToYmd(thisWeekStart),  end: dateToYmd(thisWeekEnd),  hours: Number(n.wk)  },
          payPeriod: { start: dateToYmd(payPeriodStart), end: dateToYmd(payPeriodEnd), hours: Number(n.pp)  },
          thisMonth: { start: dateToYmd(monthStart),     end: todayStr,                hours: Number(n.mo)  },
          ytd:       { start: fromStr,                   end: todayStr,                hours: Number(n.ytd) }
        },
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`payrollHoursRollup: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  // DATESTR + string compare keeps us out of the IS_AFTER/IS_BEFORE granularity
  // hole (only IS_SAME accepts a "day" unit). Both sides are "YYYY-MM-DD".
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
    },
    _source: "airtable"
  });
}

// ── HOURS BY JOB — all-time hours grouped by the static Job Name (Text) ──
// Read-only rollup behind the payroll/admin "Hours by Job" view. Groups every
// Time Entry by its STATIC job-name snapshot (`Job Name (Text)`), NOT the
// linked `Job` record — so the ~79% of historical entries whose project record
// no longer exists still bucket and label correctly (see docs/PLAN-time-
// entries-neon.md). A bucket is flagged `historical` when none of its entries
// still carry a live `Job` link.
//
// This is the first Neon-slice read pattern and is deliberately NOT throwaway:
// the shape ports 1:1 to
//   SELECT job_name, SUM(hours), COUNT(*), MIN(work_date), MAX(work_date)
//   FROM time_entries GROUP BY job_name ORDER BY SUM(hours) DESC;
// NEON-FIRST as of the step-7 cutover (2026-07-30). Neon is now the primary read
// and Airtable is the fallback — the inverse of the step-4b shadow phase.
//
// Why this one first: on production the Airtable path costs ~15.4 s because it pages
// the ENTIRE Time Entries table (146 sequential fetches) to build the buckets, while
// the same answer out of v_hours_by_job takes ~330 ms.
//
// The fallback is not ceremony. Make is still importing into Airtable in parallel, so
// Airtable remains a complete, current copy — if Neon is unset, slow or broken, the
// old path still returns the right answer. `_source` reports which one served the
// request, so a silent, permanent fallback shows up instead of hiding.
async function handleHoursByJob() {
  if (neonEnabled()) {
    const [q, meta] = await Promise.all([
      neonQuery(`SELECT job_name, hours::float8 AS hours, entries,
                        first_date::text AS first_date, last_date::text AS last_date,
                        historical
                   FROM v_hours_by_job ORDER BY hours DESC`),
      neonQuery(`SELECT count(*) FILTER (WHERE job_name IS NULL)::int AS nameless,
                        count(*)::int AS total FROM time_entries`),
    ]);

    if (q?.rows && meta?.rows?.length) {
      const jobs = q.rows.map(r => ({
        jobName:    r.job_name,
        hours:      Number(r.hours),
        entries:    r.entries,
        firstDate:  r.first_date || "",
        lastDate:   r.last_date  || "",
        historical: r.historical === true,
      }));
      const totalHours = Math.round(jobs.reduce((s, j) => s + j.hours, 0) * 100) / 100;
      return resp(200, {
        ok: true,
        jobs,
        summary: {
          jobCount:       jobs.length,
          totalHours,
          totalEntries:   meta.rows[0].total,
          namelessEntries: meta.rows[0].nameless,
        },
        _source: "neon",
        _ms: q.ms,
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`hoursByJob: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  return hoursByJobFromAirtable();
}

async function hoursByJobFromAirtable() {
  const records = await fetchAll(TABLES.timeEntries);
  const buckets = new Map(); // jobName -> aggregate
  let namelessEntries = 0;
  for (const r of records) {
    const f = r.fields || {};
    const name = String(f["Job Name (Text)"] || "").trim();
    if (!name) { namelessEntries++; continue; }
    const hrs = Number(f["Hours"]) || 0;
    const ds  = f["Work Date"] || "";
    const hasLiveLink = Array.isArray(f["Job"]) && f["Job"].length > 0;

    let b = buckets.get(name);
    if (!b) { b = { jobName: name, hours: 0, entries: 0, firstDate: "", lastDate: "", live: false }; buckets.set(name, b); }
    b.hours   += hrs;
    b.entries += 1;
    if (hasLiveLink) b.live = true;
    if (ds) {
      if (!b.firstDate || ds < b.firstDate) b.firstDate = ds;
      if (!b.lastDate  || ds > b.lastDate)  b.lastDate  = ds;
    }
  }

  const jobs = Array.from(buckets.values())
    .map(b => ({
      jobName:    b.jobName,
      hours:      Math.round(b.hours * 100) / 100,
      entries:    b.entries,
      firstDate:  b.firstDate,
      lastDate:   b.lastDate,
      historical: !b.live   // no entry still linked to a current project
    }))
    .sort((a, b) => b.hours - a.hours);

  const totalHours = Math.round(jobs.reduce((s, j) => s + j.hours, 0) * 100) / 100;

  // ── SHADOW READ (migration step 4b) ───────────────────────────────────────
  // Ask Neon the same question and diff it, but ALWAYS return the Airtable
  // answer above as authoritative. Neon being unset/slow/broken must not change
  // a single field of the response — `_shadow` is observability only, and it is
  // omitted entirely when DATABASE_URL isn't configured.
  // Remove this block at cutover, when Neon becomes the primary read.
  return resp(200, {
    ok: true,
    jobs,
    summary: {
      jobCount:       jobs.length,
      totalHours,
      totalEntries:   records.length,
      namelessEntries // entries with no Job Name (Text) — excluded from buckets
    },
    _source: "airtable"
  });
}

// Office and viewer roles never appear in payroll views — office staff are
// admin support and don't get tracked, viewer is a trial/test account. Blank
// or unrecognized roles default to eligible to match the login fallback.
// ── Stage 4: employee reads move to Neon ──────────────────────────────────
// These return records shaped EXACTLY like Airtable's — `{ id, fields }` keyed
// by the same field names — on purpose. Every caller below is payroll code that
// does `isPayrollEligibleRole(e.fields)` or `gBool(f, "Active")`, and this is
// real money: the safest migration of a money path is one where the logic diff
// is empty and only the data source moves. Adapt at the boundary, don't rewrite
// the arithmetic.
//
// Returns null when Neon can't answer, and callers MUST fall back to Airtable.
// null is never "no employees" — an empty employee list in a payroll rollup
// silently drops people from a pay period, which is the worst outcome here.
function _airtableShape(rows) {
  return rows.map(e => ({
    id: e.id,
    fields: { [F.emp.name]: e.name, [F.emp.username]: e.username,
              [F.emp.role]: e.role, [F.emp.active]: e.active },
  }));
}
async function employeesForPayroll(activeOnly = false) {
  const rows = await neonEmployees(activeOnly);
  return rows ? _airtableShape(rows) : null;
}
async function employeeRecordById(airtableId) {
  const e = await neonEmployeeById(airtableId);
  return e ? _airtableShape([e])[0] : null;
}

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

  const employees = await employeesForPayroll().then(r => r ?? fetchAll(TABLES.employees));
  const empById = new Map(employees.map(e => [e.id, e]));

  // ── NEON-FIRST (audit item 02, second slice) ─────────────────────────────
  // The employee list already came from Neon; only the money still came from
  // Airtable, which is why a handler count called this one "migrated". It
  // paged BOTH Payroll Runs and Payroll Bonuses on every call.
  //
  // ⚠ THE PERIOD COLUMNS LIVE ON THE RUN, NOT THE BONUS. Airtable's Bonuses
  // table shows Pay Period Start/End, but they are LOOKUPS through {Payroll
  // Run} — verified row by row against all 31 bonuses, including the one
  // malformed pair (run recdyryDlCxFuAlfo carries start 2026-03-22 with end
  // 2026-02-07, and its four bonuses show exactly that). So joining through
  // the run is a faithful port, not a correction: same rows, same figures,
  // same bad dates where the source is bad.
  //
  // ⚠ LEFT JOIN, deliberately. The Airtable loop kept a bonus with no run
  // link (`if (runId && superseded…)`), and a bonus with no run has no period
  // either, so the date test drops it in both worlds. An INNER JOIN would
  // change that reasoning silently if a run link ever goes missing.
  const totalsByEmpId = new Map();
  const empIdsWithBonus = new Set();
  let rows = null, ms;
  if (neonEnabled()) {
    const q = await neonQuery(
      // ⚠⚠ JOINED ON THE UUID, NOT THE REC ID (cutover slice 2, 2026-08-24).
      // This read used `r.airtable_id = b.payroll_run_airtable_id`, which is the
      // same shape as the `v_invoices` bug in slice 3: a NATIVE run has a NULL
      // `airtable_id`, `NULL = NULL` is not true, and the LEFT JOIN then drops
      // the row at `r.pay_period_end >= $1` — so every bonus on a native run
      // would silently vanish from the year-to-date total. A wrong number on a
      // payroll screen, not an error. Verified equivalent before the swap: all
      // 31 bonuses, 4 employee rows, $12,900 both ways, zero diff.
      `SELECT b.employee_airtable_id AS emp_id, SUM(b.amount)::float8 AS total
         FROM payroll_bonuses b
         LEFT JOIN payroll_runs r ON r.id = b.payroll_run_id
        WHERE r.superseded IS NOT TRUE
          AND r.pay_period_end >= $1::date
        GROUP BY 1`, [yearStart]);
    if (q?.rows) { rows = q.rows; ms = q.ms; }
    else console.error(`payrollBonusesRollup: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
  }

  if (rows) {
    for (const r of rows) {
      const empId = r.emp_id;
      if (!empId) continue;
      // Same role guard as the Airtable path: an inactive office worker with a
      // prior bonus must not sneak back in through the union below.
      const empRec = empById.get(empId);
      if (empRec && !isPayrollEligibleRole(empRec.fields)) continue;
      totalsByEmpId.set(empId, Number(r.total) || 0);
      empIdsWithBonus.add(empId);
    }
  } else {
    const allRuns = await fetchAll(PR_RUNS.table);
    const supersededRunIds = new Set();
    for (const r of allRuns) {
      if (gBool(r.fields, "Superseded")) supersededRunIds.add(r.id);
    }
    const bonuses = await fetchAll(PR_BONUSES.table, {
      filter: `DATESTR({Pay Period End})>="${yearStart}"`
    });
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
  return resp(200, { ok: true, year, employees: result,
                     ...(rows ? { _source: "neon", _ms: ms } : { _source: "airtable" }) });
}

// Per-employee bonus history (last N non-superseded). Bonuses table is small
// enough (one row per employee per period) to fetchAll and filter in memory —
// avoids the {Employee}-link/ARRAYJOIN-returns-name pitfall.
async function handlePayrollEmployeeBonusHistory(params) {
  const employeeId = params?.employeeId;
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const limit = Math.max(1, Math.min(50, parseInt(params?.limit, 10) || 5));

  const empRecs = await employeeRecordById(employeeId)
    .then(r => r ? [r] : fetchAll(TABLES.employees, { filter: `RECORD_ID()="${employeeId}"` }));
  // Defensive: if the employeeId belongs to office/viewer (or was constructed
  // by hand against a non-eligible role), don't leak any bonus history.
  const emp = empRecs[0];
  if (emp && !isPayrollEligibleRole(emp.fields)) {
    return resp(200, { ok: true, employeeId, limit, bonuses: [] });
  }

  // ── NEON-FIRST (audit item 02, second slice) ─────────────────────────────
  // Periods and Generated At come from the RUN — see the note on the rollup
  // above for why that is faithful rather than a correction.
  //
  // ⚠ `runGeneratedAt` is formatted, not returned raw. Airtable hands back an
  // ISO string; a timestamptz would arrive shaped by whatever the driver
  // decides, and this value is rendered straight into the popover. Pinning the
  // format here keeps the Neon and Airtable paths byte-identical on screen.
  if (neonEnabled()) {
    const q = await neonQuery(
      // ⚠⚠ Same uuid join as the rollup above, for the same reason — a native
      // run's bonuses would otherwise disappear from this popover entirely.
      // `run_id` and the ORDER BY tiebreaker take either form too: a bare
      // `b.payroll_run_airtable_id` hands the client a null run handle, and a
      // bare `b.airtable_id` sorts every native row into one indistinct clump.
      // Verified equivalent before the swap: all 31 rows identical.
      `SELECT COALESCE(b.airtable_id, b.id::text)        AS id,
              b.amount::float8                           AS amount,
              r.pay_period_start::text                   AS pay_period_start,
              r.pay_period_end::text                     AS pay_period_end,
              COALESCE(b.payroll_run_airtable_id, r.id::text) AS run_id,
              to_char(r.generated_at AT TIME ZONE 'UTC',
                      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')   AS run_generated_at
         FROM payroll_bonuses b
         LEFT JOIN payroll_runs r ON r.id = b.payroll_run_id
        WHERE b.employee_airtable_id = $1
          AND r.superseded IS NOT TRUE
        ORDER BY r.pay_period_end DESC NULLS LAST, r.generated_at DESC,
                 COALESCE(b.airtable_id, b.id::text)
        LIMIT $2`, [employeeId, limit]);
    if (q?.rows) {
      return resp(200, { ok: true, employeeId, limit, _source: "neon", _ms: q.ms,
        bonuses: q.rows.map(r => ({
          id: r.id,
          amount: Math.round((Number(r.amount) || 0) * 100) / 100,
          payPeriodStart: r.pay_period_start || null,
          payPeriodEnd:   r.pay_period_end   || null,
          runId: r.run_id || null,
          runGeneratedAt: r.run_id ? (r.run_generated_at || null) : null
        })) });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`payrollEmployeeBonusHistory: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  const [allRuns, allBonuses] = await Promise.all([
    fetchAll(PR_RUNS.table),
    fetchAll(PR_BONUSES.table, { sortField: "Pay Period End", sortDir: "desc" })
  ]);
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

  return resp(200, { ok: true, employeeId, limit, bonuses: out, _source: "airtable" });
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

  // ── NEON-FIRST (fixed 2026-08-09) ────────────────────────────────────────
  // This read the AIRTABLE Time Entries table, which Step 3 FROZE on
  // 2026-08-07. The rollup tile above it has served Neon since Step 1, so
  // tapping a tile opened a drill-down built from a copy that stopped being
  // updated — matching on the day of the freeze and drifting further every
  // day since. Nothing errored; the numbers just quietly stopped agreeing.
  //
  // Same union rule as before, expressed in SQL: keep every active eligible
  // employee (so a $0 active still renders) plus inactive eligible employees
  // with hours in the bucket (so someone who left mid-year still appears in
  // YTD). Inactive 0-hour leavers stay hidden.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(e.airtable_id, e.id::text) AS airtable_id, e.name, e.active,
              round(coalesce(sum(t.hours), 0), 2)::float8 AS hours
         FROM employees e
         LEFT JOIN time_entries t
                ON t.employee_id = e.id
               AND t.work_date >= $1::date AND t.work_date <= $2::date
        WHERE lower(coalesce(e.role, 'employee')) NOT IN ('office','viewer')
        GROUP BY e.airtable_id, e.name, e.active
       HAVING e.active OR coalesce(sum(t.hours), 0) > 0
        ORDER BY e.name`,
      [dateToYmd(bucketStart), dateToYmd(sumEnd)]);
    if (q?.rows) {
      const r2n = (n) => Math.round(n * 100) / 100;
      const employeesOut = q.rows.map(r => ({
        id: r.airtable_id, name: r.name || "Unknown", hours: Number(r.hours) || 0,
      }));
      return resp(200, {
        ok: true, bucket,
        range: { start: dateToYmd(bucketStart), end: dateToYmd(bucketEnd) },
        employees: employeesOut,
        total: r2n(employeesOut.reduce((s, e) => s + e.hours, 0)),
        _source: "neon", _ms: q.ms,
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`payrollHoursBreakdown: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  const [records, employees] = await Promise.all([
    fetchAll(TABLES.timeEntries, {
      filter: `AND(DATESTR({Work Date})>="${dateToYmd(bucketStart)}",DATESTR({Work Date})<="${dateToYmd(sumEnd)}")`,
      sortField: "Work Date",
      sortDir: "asc"
    }),
    employeesForPayroll().then(r => r ?? fetchAll(TABLES.employees))
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
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const todayStr = params?.today || dateToYmd(new Date());
  const today = ymdToDate(todayStr);
  if (!today) return resp(400, { ok: false, error: "Invalid today (expected YYYY-MM-DD)." });

  // Neon-first, Airtable fallback (Stage 4). employeeRecordById returns an
  // Airtable-shaped record so the eligibility check below is unchanged.
  const emp = (await employeeRecordById(employeeId))
           ?? (await fetchAll(TABLES.employees, { filter: `RECORD_ID()="${employeeId}"` }))[0];
  if (!emp) return resp(404, { ok: false, error: "Employee not found." });
  if (!isPayrollEligibleRole(emp.fields)) {
    return resp(403, { ok: false, error: "Employee role is not payroll-eligible." });
  }

  // Same date math as the admin tile — Pay Period anchor is the company-wide
  // most-recent-non-superseded-run + 1, NOT a per-employee anchor.
  const { yearStart, monthStart, thisWeekStart, thisWeekEnd, payPeriodStart, payPeriodEnd }
    = await computePayrollDateRanges(today);

  // NEON-FIRST. The Airtable path below is the worst of the rollups: it pages the
  // ENTIRE year of entries for every employee and then throws away all but one
  // person's. In Neon the employee filter is a join on employees.airtable_id, so
  // only that employee's rows are ever read.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT round(coalesce(sum(t.hours) FILTER (WHERE t.work_date >= $3::date), 0), 2)::float8 AS wk,
              round(coalesce(sum(t.hours) FILTER (WHERE t.work_date >= $4::date
                                                    AND t.work_date <= $5::date), 0), 2)::float8 AS pp,
              round(coalesce(sum(t.hours) FILTER (WHERE t.work_date >= $6::date), 0), 2)::float8 AS mo,
              round(coalesce(sum(t.hours), 0), 2)::float8 AS ytd
         FROM time_entries t
         JOIN employees e ON e.id = t.employee_id
        WHERE e.airtable_id = $1 OR e.id::text = $1
          AND t.work_date >= $2::date AND t.work_date <= $7::date`,
      [employeeId, dateToYmd(yearStart), dateToYmd(thisWeekStart), dateToYmd(payPeriodStart),
       dateToYmd(payPeriodEnd), dateToYmd(monthStart), todayStr]
    );
    if (q?.rows?.length) {
      const n = q.rows[0];
      return resp(200, {
        ok: true,
        employeeId,
        asOf: todayStr,
        ranges: {
          thisWeek:  { start: dateToYmd(thisWeekStart),  end: dateToYmd(thisWeekEnd),  hours: Number(n.wk)  },
          payPeriod: { start: dateToYmd(payPeriodStart), end: dateToYmd(payPeriodEnd), hours: Number(n.pp)  },
          thisMonth: { start: dateToYmd(monthStart),     end: todayStr,                hours: Number(n.mo)  },
          ytd:       { start: dateToYmd(yearStart),      end: todayStr,                hours: Number(n.ytd) }
        },
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`myHoursRollup: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
    },
    _source: "airtable"
  });
}

async function handleMyHoursBreakdown(params) {
  const VALID_BUCKETS = new Set(["thisWeek", "payPeriod", "thisMonth", "ytd"]);
  const employeeId = params?.employeeId;
  const bucket = params?.bucket;
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  if (!VALID_BUCKETS.has(bucket)) {
    return resp(400, { ok: false, error: "Invalid bucket. Expected one of: thisWeek, payPeriod, thisMonth, ytd." });
  }
  const todayStr = params?.today || dateToYmd(new Date());
  const today = ymdToDate(todayStr);
  if (!today) return resp(400, { ok: false, error: "Invalid today (expected YYYY-MM-DD)." });

  // Neon-first, Airtable fallback (Stage 4). employeeRecordById returns an
  // Airtable-shaped record so the eligibility check below is unchanged.
  const emp = (await employeeRecordById(employeeId))
           ?? (await fetchAll(TABLES.employees, { filter: `RECORD_ID()="${employeeId}"` }))[0];
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

  // ── NEON-FIRST (fixed 2026-08-09) ────────────────────────────────────────
  // Same bug as the admin breakdown: this paged the AIRTABLE Time Entries
  // table, frozen by Step 3 on 2026-08-07, while the My Hours tiles above it
  // served Neon. An employee tapping a tile saw a list that stopped growing on
  // the day of the freeze — and the total under it disagreed with the tile.
  //
  // The old version fetched EVERY entry in the window and filtered to this
  // employee in JS; here the employee is a join condition, so Postgres reads
  // only their rows via time_entries_employee_idx.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT coalesce(t.airtable_id, t.id::text) AS entry_id,
              to_char(t.work_date,'YYYY-MM-DD') AS work_date, t.job_name, t.hours::float8 AS hours,
              j.airtable_id AS job_airtable_id
         FROM time_entries t
         JOIN employees e ON e.id = t.employee_id
         LEFT JOIN jobs j ON j.id = t.job_id
        WHERE e.airtable_id = $1 OR e.id::text = $1
          AND t.work_date >= $2::date AND t.work_date <= $3::date
        ORDER BY t.work_date`,
      [employeeId, dateToYmd(bucketStart), dateToYmd(sumEnd)]);
    if (q?.rows) {
      const r2n = (n) => Math.round(n * 100) / 100;
      const entries = q.rows.map(r => ({
        id:       r.entry_id,
        // Formatted by Postgres. It used to be String().slice(0,10), which is
        // wrong twice over: the driver hands back a JS Date, and toISOString
        // would shift the day backwards for anyone west of UTC.
        workDate: r.work_date || "",
        jobId:    r.job_airtable_id || null,
        jobName:  r.job_name || "",
        hours:    r2n(Number(r.hours) || 0),
      }));
      return resp(200, {
        ok: true, employeeId, bucket,
        range: { start: dateToYmd(bucketStart), end: dateToYmd(bucketEnd) },
        entries,
        total: r2n(entries.reduce((s, e) => s + e.hours, 0)),
        _source: "neon", _ms: q.ms,
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`myHoursBreakdown: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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

  // ── Stage 3: Neon decides, but ONLY when switched on ────────────────────
  // Gated by LOGIN_SOURCE (see _employees.js). Unset — the default, and what
  // production runs until the shadow logs are proven clean — skips this block
  // entirely and behaves exactly as before.
  //
  // `_source` is echoed on the response for the same reason the payroll reads
  // do it: it is the only way to confirm on production which store actually
  // answered. Reconcile the ANSWER too, not just the label.
  if (loginSource() === "neon") {
    const r = await neonLoginCandidate(identifier, pin);
    if (r.ok) {
      if (r.ambiguous) {
        // Authoritative refusal, deliberately NOT a fallback. Falling through
        // to Airtable here would hand the login to Array.find()'s arbitrary
        // first match — the exact behaviour the ambiguity guard exists to stop.
        console.warn(`login[field]: refusing ambiguous identifier — ${r.n} employees match.`);
        return resp(401, { ok: false, error: "That name matches more than one person. Use your username." });
      }
      if (!r.user) return resp(401, { ok: false, error: "Invalid login. Check your name and PIN." });
      await neonExec("login.lastSeen",
        `UPDATE employees SET last_login_at = now() WHERE airtable_id = $1 OR id::text = $1`, [r.user.id]);
      return resp(200, {
        ok: true, user: r.user, _source: "neon",
        token: signToken({ id: r.user.id, role: r.user.role }),
      });
    }
    // r.ok === false means Neon had no opinion — unset, unreachable, timed out.
    // Fall through to Airtable rather than refuse: a database blip must not
    // stop the crew logging in. The honest cost is that during a Neon outage,
    // an employee deactivated in Neon-but-not-Airtable could get back in —
    // the same fail-soft trade the revocation check makes, for the same reason.
    console.warn("login[field]: Neon unavailable, falling back to Airtable.");
  }

  const records = await fetchAll(TABLES.employees);
  const match = records.find(r => {
    const f = r.fields || {};
    const name=normalize(f[F.emp.name]),username=normalize(f[F.emp.username]),email=normalize(f[F.emp.email]);
    const savedPin=String(f[F.emp.pin]||"").trim(),active=gBool(f,F.emp.active),id=normalize(identifier);
    return [name,username,email].includes(id)&&savedPin!==""&&savedPin===String(pin).trim()&&active;
  });
  if (!match) {
    // Shadow the refusal too — a flip that would ALLOW someone Airtable turns
    // away is the more dangerous direction, and it only shows up here.
    await shadowLoginCheck("field", identifier, pin, null);
    return resp(401, { ok: false, error: "Invalid login. Check your name and PIN." });
  }
  const f = match.fields || {};
  // Recognize four roles: admin, office, viewer, employee. Office acts like
  // admin on the field-app side but is filtered out of inventory + crew pickers.
  const rawRole = normalize(f[F.emp.role]);
  let role;
  if      (rawRole === "admin")  role = "admin";
  else if (rawRole === "office") role = "office";
  else if (rawRole === "viewer") role = "viewer";
  else                            role = "employee";
  const user = { id: match.id, name: f[F.emp.name]||"Unknown", role };
  // Stage 2 of the login flip: Airtable still decides, Neon is checked
  // alongside and only logs when the two disagree. See _employees.js.
  await shadowLoginCheck("field", identifier, pin, user);
  // Stamp the last login for the People screen. neonExec, NOT neonWrite — this
  // is cosmetic and a login must never fail because Neon is unreachable. It is
  // also deliberately not awaited for its result beyond the fail-soft wrapper:
  // the only way to spot an account nobody has used in a year is to record this,
  // but nobody should be locked out if it doesn't land.
  await neonExec("login.lastSeen",
    `UPDATE employees SET last_login_at = now() WHERE airtable_id = $1 OR id::text = $1`, [match.id]);
  // Issue a signed session token the client attaches to every later request.
  return resp(200, { ok: true, user, _source: "airtable", token: signToken({ id: user.id, role: user.role }) });
}

// ══════════════════════════════════════════════════════════════════
// PEOPLE — the admin employee roster. Slice 1 of docs/PLAN-employee-admin.md.
// ══════════════════════════════════════════════════════════════════
// Until this shipped there was no employee screen in either app: every hire,
// raise, role change and leaver was done by opening the Airtable grid.
//
// ── WHY THIS READS TWO PLACES ──────────────────────────────────────────────
// The app is half-migrated, so employee data is split on purpose:
//
//   Airtable owns  name, username, role, ACTIVE, PIN, phone, email, labor type
//                  — because that is what both handleLogin's read. A deactivation
//                    recorded anywhere else would not stop a login.
//   Neon owns      hired_on, terminated_on, termination_note, token_valid_from,
//                  last_login_at — columns Airtable simply does not have.
//
// ── STAGE 5 (2026-08-09): THE AIRTABLE WRITES BELOW ARE DELIBERATELY KEPT ──
// The plan said Stage 5 would drop them. It doesn't, and the reason is the
// login kill switch.
//
// `LOGIN_SOURCE=airtable` is the documented rollback for the riskiest change in
// this whole migration, and it resolves logins against Airtable's Employees
// table. That escape hatch is only worth having while the data behind it is
// current. Stop mirroring, and within a week Airtable is missing new hires,
// holding old PINs, and showing leavers as active — so throwing the switch
// would let a leaver back in and lock a new hire out, which is worse than the
// problem it exists to solve.
//
// Airtable's own Employees records also remain the parents of Labor Cost Rates,
// Bonuses, Job Labor Allocation and Employee Weekly Time. Those tables are
// still there.
//
// So these writes are no longer a dual-authority — Neon is authoritative and
// every read comes from it. They are **fallback maintenance**. Drop them in the
// same commit that retires `LOGIN_SOURCE`, not before, and not separately.
//
// (The old warning here said never to write `active` to Neon because the ETL
// dimension load would erase it. That load is RETIRED as of 2026-08-09 — see
// db/etl/time-entries-full.mjs. Writing Neon is now the whole point.)
//
// The Neon half FAILS SOFT: if it can't be read the roster still renders off
// Airtable with those five columns null. A screen that shows most of the truth
// beats a screen that shows an error.
// Write sites use field IDs inline (repo convention — F.* is for reads only).
// Only `Active` is written in slice 1; identity/contact/PIN writes arrive with
// slice 2. The termination NOTE is a Neon column, not Airtable's `Notes` — that
// field is free-text an admin already uses for other things, and a deactivation
// must not overwrite it.
const _EMP_FLD = {
  active:     "fldJbQBEweYfoo5nz",
  pin:        "fld4vnd5aFyrIajM9",
  name:       "fld9dg0JJjfLqPZ8w",
  username:   "fldvEUEkYeR2yI4rm",
  employeeNo: "fldvsUs0s8CCwrfIN",
  phone:      "fld3S8pHiOF892mPq",
  email:      "fldE8bsEr8CMzscJg",   // "Primary Email" — NOT "Email", which doesn't exist
  role:       "fldLfzy63tqP8nnHQ",
  roleNew:    "fldtqCXIDd9EuE1O5",   // blanked on save; see handleUpdateEmployee
  laborType:  "fldSFlrFRZy3yXHyU",
  notes:      "fldL3LQ7DwqYxwbny",
};

async function handlePeople() {
  // Everyone in one call. This table is ~24 rows and never grows fast, so
  // paging it or splitting roster/detail into two actions would be complexity
  // bought with nothing. The client filters Active vs Former in memory.
  const byAirtableId = new Map();
  // Current cost rate now comes from Neon, NOT Airtable's "Current True Cost
  // Rate" rollup. It used to read the rollup because that was live and Neon's
  // mirror was ETL-stale — that is now inverted: the app writes rates to Neon,
  // so the Airtable rollup is the stale one. Reading it would show an admin the
  // old number right after they'd given someone a raise.
  // Stage 5: the ROSTER itself is Neon's now, not just the extra columns. Neon
  // holds every attribute since db/schema/017, the People screen writes it, and
  // the ETL dimension load that used to overwrite it is retired — so Airtable's
  // copy is the mirror, and reading it here would show stale names and roles.
  const q = await neonQuery(
    `SELECT COALESCE(e.airtable_id, e.id::text) AS airtable_id,
            e.name, e.username, e.role, e.active, e.email, e.phone,
            e.employee_no, e.labor_type, e.notes, e.first_name, e.last_name,
            e.hired_on, e.terminated_on, e.termination_note,
            e.token_valid_from, e.last_login_at, e.salaried,
            r.true_cost_rate, r.base_hourly_wage, r.payroll_burden_pct
       FROM employees e
       LEFT JOIN LATERAL (
              SELECT true_cost_rate, base_hourly_wage, payroll_burden_pct
                FROM labor_cost_rates
               WHERE employee_id = e.id AND effective_end_date IS NULL
               ORDER BY effective_start_date DESC LIMIT 1) r ON true
      ORDER BY e.name`, []);
  const neonOk = !!(q && !q.error && Array.isArray(q.rows));
  if (neonOk) {
    for (const r of q.rows) byAirtableId.set(String(r.airtable_id), r);
  }

  // Airtable-record shape either way, so the projection below is unchanged by
  // this flip. Falling back rather than erroring: a roster is more useful than
  // a blank screen, and `neonOk` already tells the client the Neon-only columns
  // (hire dates, last login, cost rate) are missing.
  const records = neonOk
    ? q.rows.map(r => ({ id: r.airtable_id, fields: {
        [F.emp.name]: r.name || "", [F.emp.username]: r.username || "",
        [F.emp.role]: r.role || "", [F.emp.active]: r.active === true,
        [F.emp.email]: r.email || "", "Primary Phone": r.phone || "",
        "Employee ID": r.employee_no || "", "Default Labor Type": r.labor_type || "",
        "Notes": r.notes || "", "First Name": r.first_name || "", "Last Name": r.last_name || "",
      } }))
    : await fetchAll(TABLES.employees);

  // Leave, for the current year. Its own query rather than another LATERAL on the
  // one above: only the hourly employees have an allowance row at all, so joining
  // it in would push a second nullable shape onto every roster row for the benefit
  // of two of them. Fails soft — no PTO figures is a worse screen, not a broken one.
  const ptoByAirtableId = new Map();
  const pq = await neonQuery(
    `SELECT COALESCE(airtable_id, employee_id::text) AS airtable_id,
            allowance_hours::float8, carried_in_hours::float8,
            entitled_hours::float8, used_hours::float8, remaining_hours::float8,
            holiday_hours::float8, year
       FROM v_pto_balances WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)::int`, []);
  if (pq && !pq.error && Array.isArray(pq.rows)) {
    for (const r of pq.rows) ptoByAirtableId.set(String(r.airtable_id), r);
  }

  const people = records.map(r => {
    const f = r.fields || {};
    const n = byAirtableId.get(r.id) || {};
    const pt = ptoByAirtableId.get(r.id) || null;
    return {
      id:          r.id,
      // null for anyone with no allowance row — the salaried owners, who don't
      // track PTO. The client hides the tiles entirely rather than showing zeros,
      // which would read as "used it all".
      pto: pt ? {
        year:      pt.year,
        entitled:  Number(pt.entitled_hours)  || 0,
        used:      Number(pt.used_hours)      || 0,
        remaining: Number(pt.remaining_hours) || 0,
        // Reporting only — holidays are given, not drawn from the allowance, so
        // this is deliberately NOT subtracted from `remaining`.
        holiday:   Number(pt.holiday_hours)   || 0,
      } : null,
      name:        g(f, F.emp.name) || "",
      firstName:   g(f, "First Name") || "",
      lastName:    g(f, "Last Name") || "",
      username:    g(f, F.emp.username) || "",
      employeeNo:  g(f, "Employee ID") || "",
      phone:       g(f, "Primary Phone") || "",
      // F.emp.email is now "Primary Email" — it used to say "Email", a column
      // that does not exist here, which is why login-by-email never worked.
      // Fixed at the F map, so this and handleLogin agree by construction.
      email:       g(f, F.emp.email) || "",
      role:        normalize(g(f, F.emp.role) || "") || "employee",
      active:      gBool(f, F.emp.active),
      laborType:   g(f, "Default Labor Type") || "",
      // From Neon (see the query above). Falls back to Airtable's rollup only
      // when Neon is unreachable, so the card shows a number rather than a dash
      // during an outage — flagged by `neonOk` either way.
      currentRate: n.true_cost_rate != null ? Number(n.true_cost_rate) : gNum(f, "Current True Cost Rate"),
      currentWage: n.base_hourly_wage   != null ? Number(n.base_hourly_wage)   : null,
      currentBurdenPct: n.payroll_burden_pct != null ? Number(n.payroll_burden_pct) * 100 : null,
      notes:       g(f, "Notes") || "",
      // Neon half — null when Neon is unreachable, or simply not set yet.
      hiredOn:         n.hired_on         ? String(n.hired_on).slice(0, 10) : null,
      terminatedOn:    n.terminated_on    ? String(n.terminated_on).slice(0, 10) : null,
      terminationNote: n.termination_note || "",
      lastLoginAt:     n.last_login_at    ? new Date(n.last_login_at).toISOString() : null,
      // Surfaced so the UI can distinguish "switched off" from "switched off
      // AND their sessions were killed" — they come apart if a deactivation
      // half-failed, and hiding that would hide the one failure that matters.
      sessionsRevoked: !!n.token_valid_from,
      // Paid a salary, so the payroll screen and PDF must not split their hours
      // into Regular/Overtime. This REPLACED a hardcoded name list in index.html
      // (`SALARIED = ["Larry Unruh", ...]`) — renaming one of them on this very
      // screen silently moved them onto hourly pay with overtime on the next run.
      // Neon-only: false during a Neon outage, which is why the client keeps the
      // old name list as a last-resort fallback rather than trusting an empty set.
      salaried: n.salaried === true,
    };
  });

  people.sort((a, b) => a.name.localeCompare(b.name));
  // `neonOk` is not decoration: without it a roster where every hire date is
  // blank looks identical to one where Neon is down. The client says so.
  return resp(200, { ok: true, people, neonOk });
}

// Reveal one employee's PIN, on explicit request, for an admin who needs to
// tell someone what theirs is.
//
// ── WHY THIS IS A SEPARATE ACTION AND NOT A FIELD ON handlePeople ──────────
// Folding the PIN into the roster would put EVERY employee's PIN into one JSON
// response every time the screen opens — sitting in browser memory, devtools
// and any network log, whether or not anyone asked. One person, one deliberate
// tap, one response is the same information with a much smaller blast radius.
//
// ── AND WHY IT'S OK TO SHOW AT ALL ────────────────────────────────────────
// The PIN is stored in PLAINTEXT in Airtable (a known, deliberate gap — see
// _auth.js:12-13), so any admin can already read it by opening the grid. This
// adds no exposure that doesn't already exist; it saves a trip to Airtable.
// Owner asked for it explicitly 2026-08-08.
//
//   ⚠ When PINs are hashed at the login flip (ROADMAP §4), this action CANNOT
//   survive — a hash cannot be un-hashed. It must be replaced by "set a new
//   PIN" at that point, not ported. That is the cost the plan was trying to
//   avoid by never reading PINs; it is accepted, and written down here so it
//   is found at the right moment.
async function handleEmployeePin(params) {
  const { employeeId } = params || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  // Neon holds the PIN now (db/schema/017), and the People screen writes both
  // stores, so read the one that login actually uses. Airtable is the fallback.
  const q = await neonQuery(`SELECT pin FROM employees WHERE airtable_id = $1 OR id::text = $1`, [employeeId]);
  let pin = (q && !q.error && q.rows?.length) ? String(q.rows[0].pin ?? "").trim() : null;
  if (pin === null) {
    const rec = await atFetch(`${encodeURIComponent(TABLES.employees)}/${employeeId}`, { method: "GET" });
    pin = String(rec?.fields?.[F.emp.pin] ?? "").trim();
  }
  // hasPin distinguishes "no PIN on record" from "couldn't read it" — an empty
  // PIN is not cosmetic, handleLogin refuses to match one, so the person
  // genuinely cannot log in until it is set.
  return resp(200, { ok: true, employeeId, pin, hasPin: pin !== "" });
}

// ══════════════════════════════════════════════════════════════════
// COST RATES — what an hour of someone's time costs the company
// ══════════════════════════════════════════════════════════════════
// The chain, verified against all 14 live rate rows:
//
//   base_hourly_wage × (1 + payroll_burden_pct) = true_cost_rate
//        ↓
//   v_job_labor_cost_true   regular hrs × rate + OT hrs × rate × 1.5
//        ↓
//   v_job_financials_true → the GP on the job list
//
// So $26.00 + 25% = $32.50/hr, and that is what every hour booked to a job
// costs. This is real money on the screen the business runs on.
//
// ── RATES ARE EFFECTIVE-DATED, AND THAT IS THE WHOLE SUBTLETY ─────────────
// v_job_labor_cost_true picks the rate that was in force DURING THAT WEEK, not
// the current one. So there are two genuinely different operations, and
// conflating them silently rewrites history:
//
//   A RAISE       new row from a date forward, old row closed the day before.
//                 Past weeks keep the old rate. Finished jobs don't move.
//   A CORRECTION  the stored number was wrong. Edit in place, and every week
//                 that used it recalculates — which is right, because the old
//                 figure was a lie. But it MUST be deliberate.
//
// The UI asks which. Do not merge them into a single "save".
//
// ── NEON IS THE SOURCE OF TRUTH FOR RATES, AS OF THIS CHANGE ──────────────
// GP reads Neon, so Neon is what has to be correct. Airtable's Labor Cost
// Rates table becomes historical, exactly like expenses and invoices did.
// `db/etl/time-entries-full.mjs` MUST no longer reload this table — see the
// comment there. If it did, an app-written rate would be overwritten by the
// stale Airtable copy on the next run, and job costs would silently revert.
//
// `airtable_id` is NOT NULL on this table, so app-created rows carry a
// deterministic synthetic key: `app:<employee rec id>:<start date>`. That makes
// a retry idempotent (ON CONFLICT DO UPDATE) instead of inserting a second
// overlapping row, which would leave two open rates and let the view pick one
// arbitrarily.
function appRateKey(employeeId, startDate) {
  return `app:${employeeId}:${startDate}`;
}

async function handleEmployeeRates(params) {
  const { employeeId } = params || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const q = await neonQuery(
    `SELECT r.airtable_id, r.labor_type, r.effective_start_date, r.effective_end_date,
            r.base_hourly_wage, r.payroll_burden_pct, r.true_cost_rate, r.notes
       FROM labor_cost_rates r
       JOIN employees e ON e.id = r.employee_id
      WHERE e.airtable_id = $1 OR e.id::text = $1
      ORDER BY r.effective_start_date DESC`, [employeeId]);
  if (!q || q.error || !Array.isArray(q.rows)) {
    return resp(503, { ok: false, error: "Can't read rates right now (database unreachable)." });
  }
  const ymd = (v) => (v ? String(new Date(v).toISOString()).slice(0, 10) : null);
  return resp(200, {
    ok: true,
    rates: q.rows.map(r => ({
      id:        r.airtable_id,
      laborType: r.labor_type || "",
      startDate: ymd(r.effective_start_date),
      endDate:   ymd(r.effective_end_date),
      wage:      r.base_hourly_wage  == null ? null : Number(r.base_hourly_wage),
      burden:    r.payroll_burden_pct == null ? null : Number(r.payroll_burden_pct),
      trueCost:  r.true_cost_rate    == null ? null : Number(r.true_cost_rate),
      notes:     r.notes || "",
      current:   r.effective_end_date === null,
    })),
  });
}

// Shared validation. Burden arrives as a PERCENT from the UI (25), and is
// stored as a FRACTION (0.25) because that is what the Airtable column held and
// what every existing row uses — getting this backwards would multiply every
// job's labor cost by 25.
function parseRateInput(body) {
  const wage   = Number(body?.wage);
  const burden = Number(body?.burdenPct);
  if (!Number.isFinite(wage) || wage <= 0 || wage > 500) {
    return { error: "Hourly wage must be a number between 0 and 500." };
  }
  if (!Number.isFinite(burden) || burden < 0 || burden > 200) {
    return { error: "Burden must be a percentage between 0 and 200." };
  }
  const laborType = _LABOR_OPTS.includes(String(body?.laborType || "")) ? String(body.laborType) : "Regular";
  return { wage, burdenFrac: burden / 100, laborType, notes: String(body?.notes ?? "").trim() };
}

// A RAISE. Closes the open row the day before the new start date and inserts
// the new one — in ONE statement, so there is never a moment with two open
// rates or none.
async function handleAddEmployeeRaise(body) {
  const { employeeId, startDate } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(startDate || ""))) {
    return resp(400, { ok: false, error: "Need an effective date (YYYY-MM-DD)." });
  }
  const p = parseRateInput(body);
  if (p.error) return resp(400, { ok: false, error: p.error });

  const rows = await neonWrite("employeeRaise",
    `WITH emp AS (SELECT id FROM employees WHERE airtable_id = $1 OR id::text = $1),
        closed AS (
          UPDATE labor_cost_rates
             SET effective_end_date = ($2::date - 1)
           WHERE employee_id = (SELECT id FROM emp)
             AND effective_end_date IS NULL
             AND effective_start_date < $2::date
       RETURNING id)
     INSERT INTO labor_cost_rates
            (airtable_id, employee_id, labor_type, effective_start_date,
             base_hourly_wage, payroll_burden_pct, true_cost_rate, notes, synced_at)
     SELECT $3, emp.id, $4, $2::date, $5::numeric, $6::numeric,
            round($5::numeric * (1 + $6::numeric), 2), NULLIF($7::text, ''), now()
       FROM emp
     ON CONFLICT (airtable_id) DO UPDATE
            SET labor_type           = EXCLUDED.labor_type,
                effective_start_date = EXCLUDED.effective_start_date,
                base_hourly_wage     = EXCLUDED.base_hourly_wage,
                payroll_burden_pct   = EXCLUDED.payroll_burden_pct,
                true_cost_rate       = EXCLUDED.true_cost_rate,
                notes                = EXCLUDED.notes,
                synced_at            = now()
       RETURNING id, true_cost_rate`,
    [employeeId, startDate, appRateKey(employeeId, startDate),
     p.laborType, p.wage, p.burdenFrac, p.notes]);

  // Zero rows means the employee isn't in Neon — the INSERT selects FROM emp,
  // so an unknown employee writes nothing and reports nothing. Silent no-ops
  // on a money table are exactly what must not happen.
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error(`employeeRaise: no rate written for ${employeeId} — not found in Neon`);
  }
  return resp(200, { ok: true, trueCost: Number(rows[0].true_cost_rate) });
}

// A CORRECTION. Edits one existing row in place, which DOES move historical job
// costs — that is the point, and the UI says so before it calls this.
async function handleCorrectEmployeeRate(body) {
  const { rateId } = body || {};
  if (!rateId) return resp(400, { ok: false, error: "Missing rateId." });
  const p = parseRateInput(body);
  if (p.error) return resp(400, { ok: false, error: p.error });

  const rows = await neonWrite("correctRate",
    `UPDATE labor_cost_rates
        SET labor_type         = $2,
            base_hourly_wage   = $3::numeric,
            payroll_burden_pct = $4::numeric,
            true_cost_rate     = round($3::numeric * (1 + $4::numeric), 2),
            notes              = NULLIF($5::text, ''),
            synced_at          = now()
      -- Dual handle for the same reason as the two jobs sites, though this one
      -- is latent rather than live: all 15 labor_cost_rates rows still carry a
      -- rec id (checked 2026-08-25), so nothing has hit it yet. Neon owns this
      -- table now, so the first rate born here would have been uncorrectable —
      -- silently, with a 404 the UI reads as "No such rate row."
      WHERE airtable_id = $1 OR id::text = $1
  RETURNING id, true_cost_rate`,
    [rateId, p.laborType, p.wage, p.burdenFrac, p.notes]);
  if (!Array.isArray(rows) || rows.length === 0) {
    return resp(404, { ok: false, error: "No such rate row." });
  }
  return resp(200, { ok: true, trueCost: Number(rows[0].true_cost_rate) });
}

// Add a person. NEON FIRST since cutover slice 5 (2026-08-24) — the Neon row is
// the identity now, and Airtable gets a best-effort mirror.
//
// It used to be the other way round, on the reasoning that "its rec id IS the
// identity every other table keys on". That stopped being true: every child
// table FKs to `employees(id)`, the uuid (verified in db/schema/060), and login
// has read Neon since 2026-08-08. The rec id was the identity by habit.
//
// ⚠⚠ THE REC ID IS NOT STAMPED BACK. There is no ETL over `employees` — the ETL
// dimension load was retired precisely because re-running it reactivated
// leavers — so nothing would re-read the mirror and no ON CONFLICT can
// duplicate the row. Leaving `airtable_id` NULL keeps one rule for the whole
// table: a person hired in the app is Neon's, full stop.
//
// ⚠ A starting rate is not optional for anyone who will book hours. An employee
// with NO rate row contributes NOTHING to job labor cost: v_job_labor_cost_true
// left-joins the rate, so their hours come through at NULL and simply vanish
// from GP. Their work would look free. The UI defaults the rate on and warns.
async function handleCreateEmployee(body) {
  const { name, username, role, pin, phone, email, employeeNo,
          laborType, hiredOn, withRate, wage, burdenPct } = body || {};
  const cleanName = String(name ?? "").trim();
  if (!cleanName) return resp(400, { ok: false, error: "Name is required." });
  const nextRole = _ROLE_OPTS.includes(String(role || "").toLowerCase())
    ? String(role).toLowerCase() : null;
  if (!nextRole) return resp(400, { ok: false, error: `Role must be one of: ${_ROLE_OPTS.join(", ")}.` });
  const cleanPin = String(pin ?? "").trim();
  if (!/^\d{4,8}$/.test(cleanPin)) return resp(400, { ok: false, error: "PIN must be 4 to 8 digits." });

  // ⚠⚠ THIS CHECK READS NEON, NOT AIRTABLE — cutover slice 5, and it is a
  // SECURITY change, not a tidy-up.
  //
  // Login matches identifier + PIN, so two people sharing a PIN lets either log
  // in as the other. This used to scan the Airtable Employees table, which after
  // this slice does not contain natively-hired people at all — so their PINs
  // were invisible to it and a second person could be given the same one. The
  // duplicate would then be undetectable from the UI and would silently make
  // `neonLoginCandidate` ambiguous, which it refuses (`ambiguous:true`),
  // locking BOTH of them out on their next login.
  //
  // Fails CLOSED. If Neon cannot answer we do not know whether the PIN is free,
  // and guessing "free" is the answer that creates the collision.
  const dupe = await neonQuery(
    `SELECT name, btrim(pin) = $1 AS pin_clash,
            lower(btrim(name)) = lower(btrim($2)) AS name_clash
       FROM employees
      WHERE (pin IS NOT NULL AND btrim(pin) = $1)
         OR lower(btrim(name)) = lower(btrim($2))`, [cleanPin, cleanName]);
  if (!dupe?.rows) {
    return resp(503, { ok: false, error: "Couldn't check for a duplicate PIN. Nobody was added — please try again." });
  }
  const pinClash = dupe.rows.find(r => r.pin_clash === true);
  if (pinClash) {
    return resp(409, { ok: false, error: `That PIN is already used by ${pinClash.name || "another employee"}. Pick a different one.` });
  }
  if (dupe.rows.some(r => r.name_clash === true)) {
    return resp(409, { ok: false, error: `There is already an employee called ${cleanName}.` });
  }

  const nextLabor = _LABOR_OPTS.includes(String(laborType || "")) ? String(laborType) : "Regular";
  const ymd = (v) => {
    const s = String(v ?? "").trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
  };

  // ── THE HIRE. Neon first and it fails closed: this row is what lets the
  // person log in, so a create reported as done must actually be done.
  //
  // No `ON CONFLICT` any more. It existed to make an Airtable-first create
  // idempotent against the rec id; a native row has no natural key to conflict
  // on, and the duplicate-name/PIN check above is what prevents a double-add.
  const rows = await neonWrite("createEmployee",
    `INSERT INTO employees (name, username, role, active, pin,
                            email, phone, employee_no, labor_type, hired_on)
          VALUES ($1::text,$2::text,$3::text,true,$4::text,$5::text,$6::text,
                  $7::text,$8::text,$9::date)
       RETURNING id`,
    [cleanName, String(username ?? "").trim(), nextRole, cleanPin,
     String(email ?? "").trim(), String(phone ?? "").trim(),
     String(employeeNo ?? "").trim(), nextLabor, ymd(hiredOn)]);
  const newId = rows?.[0]?.id ? String(rows[0].id) : null;
  if (!newId) return resp(502, { ok: false, error: "Couldn't add the employee. Please try again." });

  // The Airtable mirror. Best-effort, and the rec id is deliberately NOT carried
  // back — see the header note. If this fails the person still exists and can
  // still log in, which is the only thing that matters about a new hire.
  await mirrorToAirtable("createEmployee", () =>
    atFetch(encodeURIComponent(TABLES.employees), {
      method: "POST",
      body: JSON.stringify({ fields: {
        [_EMP_FLD.name]:       cleanName,
        [_EMP_FLD.username]:   String(username ?? "").trim(),
        [_EMP_FLD.pin]:        cleanPin,
        [_EMP_FLD.role]:       nextRole,
        [_EMP_FLD.active]:     true,
        [_EMP_FLD.phone]:      String(phone ?? "").trim(),
        [_EMP_FLD.email]:      String(email ?? "").trim(),
        [_EMP_FLD.employeeNo]: String(employeeNo ?? "").trim(),
        [_EMP_FLD.laborType]:  nextLabor,
      } }),
    }));

  if (withRate) {
    const start = ymd(hiredOn) || new Date().toISOString().slice(0, 10);
    const p = parseRateInput({ wage, burdenPct, laborType: nextLabor });
    if (p.error) {
      // The person exists and can log in; only the rate failed. Say exactly
      // that, rather than implying nothing happened.
      return resp(200, { ok: true, employeeId: newId, rateWarning: p.error });
    }
    await neonWrite("createEmployeeRate",
      `INSERT INTO labor_cost_rates
              (airtable_id, employee_id, labor_type, effective_start_date,
               base_hourly_wage, payroll_burden_pct, true_cost_rate, synced_at)
       SELECT $2, e.id, $3, $4::date, $5::numeric, $6::numeric,
              round($5::numeric * (1 + $6::numeric), 2), now()
         FROM employees e WHERE e.airtable_id = $1 OR e.id::text = $1
       ON CONFLICT (airtable_id) DO NOTHING`,
      [newId, appRateKey(newId, start), p.laborType, start, p.wage, p.burdenFrac]);
  }

  return resp(200, { ok: true, employeeId: newId });
}

// Edit a person. Slice 2 — until this existed the People screen was read-only
// plus the access toggle, and changing anyone's name, role or contact details
// meant opening the Airtable grid.
//
// Writes BOTH stores, because employee data is split (see handlePeople):
// identity/contact/role/labor type live in Airtable, the employment dates live
// in Neon. Neon goes first and fails closed, same contract as the other two
// write handlers — a save reported as done must actually be done.
//
// The Neon half is an UPSERT, unlike the revoke path's bare UPDATE. A new hire
// exists in Airtable the moment they're added but only reaches Neon on the next
// ETL run, and refusing to let anyone edit them until then would be a silly
// place to fail. On conflict it touches ONLY the three columns this screen
// owns — name/active/role stay Airtable's, managed by the ETL dimension load.
const _ROLE_OPTS  = ["admin", "office", "viewer", "employee"];
const _LABOR_OPTS = ["Regular", "Prevailing Wage", "Service Rate"];

async function handleUpdateEmployee(body, authUser) {
  const { employeeId, name, username, employeeNo, phone, email,
          role, laborType, hiredOn, terminatedOn, notes } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const cleanName = String(name ?? "").trim();
  if (!cleanName) return resp(400, { ok: false, error: "Name can't be empty." });

  // Same self guard as the access toggle, for the same reason: demoting
  // yourself out of admin locks you out of the only screen that could undo it.
  // Editing your own name or phone is fine — only the role is refused.
  const nextRole = _ROLE_OPTS.includes(String(role || "").toLowerCase())
    ? String(role).toLowerCase() : null;
  if (!nextRole) return resp(400, { ok: false, error: `Role must be one of: ${_ROLE_OPTS.join(", ")}.` });
  if (authUser && employeeId === authUser.id && nextRole !== authUser.role) {
    return resp(400, { ok: false, error: "You can't change your own role. Ask another admin." });
  }

  // Whitelisted rather than passed through with typecast, so a stray client
  // value can't quietly create a new single-select option (see CLAUDE.md).
  const nextLabor = _LABOR_OPTS.includes(String(laborType || "")) ? String(laborType) : null;

  const ymd = (v) => {
    const s = String(v ?? "").trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
  };

  // Mirrors EVERY edited attribute into Neon, not just the dates. Stage 1 of
  // the login flip (db/schema/017_employees_full.sql): Neon has to hold a
  // complete, current employee record before anything can read it, and an
  // attribute that only updates on the next hand-run ETL is not current.
  //
  // ⚠ `termination_note` is deliberately NOT written here. It is "why they
  // left", set by the deactivate dialog — and an earlier version of this
  // statement wrote the general Notes field into it, so editing someone's
  // notes silently overwrote the reason they were let go. General notes now
  // have their own column.
  // ⚠⚠ THIS WAS AN `INSERT … ON CONFLICT (airtable_id)` UPSERT AND HAD TO STOP
  // BEING ONE — cutover slice 5. Handed a native employee's uuid it would not
  // have updated anybody: nothing conflicts, so it would INSERT A SECOND
  // EMPLOYEE whose `airtable_id` is literally that uuid string. The person would
  // then exist twice with the same name and PIN, `neonLoginCandidate` would see
  // two matches and refuse the login as `ambiguous`, and the admin would have
  // been told the edit saved.
  //
  // The upsert existed because "a new hire exists in Airtable the moment they're
  // added but only reaches Neon on the next ETL run, and refusing to let anyone
  // edit them until then would be a silly place to fail." Both halves of that
  // are now false: hires are created in Neon first, and the ETL dimension load
  // is retired. A plain UPDATE is the honest statement — the row must exist.
  const rows = await neonWrite("updateEmployee",
    `UPDATE employees
        SET name          = $2::text,
            username      = $3::text,
            role          = $4::text,
            email         = $5::text,
            phone         = $6::text,
            employee_no   = $7::text,
            labor_type    = $8::text,
            notes         = $9::text,
            hired_on      = $10::date,
            terminated_on = $11::date
      WHERE airtable_id = $1 OR id::text = $1
  RETURNING airtable_id`,
    [employeeId, cleanName, String(username ?? "").trim(), nextRole,
     String(email ?? "").trim(), String(phone ?? "").trim(),
     String(employeeNo ?? "").trim(), nextLabor, String(notes ?? "").trim(),
     ymd(hiredOn), ymd(terminatedOn)]);
  if (!Array.isArray(rows) || rows.length === 0) {
    return resp(404, { ok: false, error: "That employee is not in the database — nothing was changed." });
  }

  // Gated on the row's REC ID — a native hire has no Airtable record to PATCH,
  // and addressing `Employees/<uuid>` would 404 and throw after the real write
  // had already landed.
  const updRecId = rows[0]?.airtable_id || null;
  if (updRecId) {
    await mirrorToAirtable("updateEmployee", () =>
      atFetch(`${encodeURIComponent(TABLES.employees)}/${updRecId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: {
          [_EMP_FLD.name]:       cleanName,
          [_EMP_FLD.username]:   String(username ?? "").trim(),
          [_EMP_FLD.employeeNo]: String(employeeNo ?? "").trim(),
          [_EMP_FLD.phone]:      String(phone ?? "").trim(),
          [_EMP_FLD.email]:      String(email ?? "").trim(),
          [_EMP_FLD.role]:       nextRole,
          [_EMP_FLD.laborType]:  nextLabor,
          [_EMP_FLD.notes]:      String(notes ?? "").trim(),
          // Blanked on every save. `Role New` can only express employee/admin/
          // viewer — it has no `office` — so a value left sitting there is a
          // demotion waiting to happen if any reader ever prefers it again.
          [_EMP_FLD.roleNew]:    null,
        } }),
      }));
  }

  return resp(200, { ok: true, employeeId });
}

// Set a new PIN. Admin-driven reset — there is no self-service "forgot PIN"
// and there cannot be one today: not a single employee record carries a
// Primary Email or Primary Phone, so a reset code has nowhere to go.
//
// ── DUPLICATES ARE REFUSED, AND THAT IS NOT PEDANTRY ───────────────────────
// Login matches identifier + PIN, so two people sharing a PIN means either can
// log in AS the other by typing the other's username. Found live on 2026-08-08:
// Larry (admin), Tisha (office) and Arlene (office) all had 1184, which handed
// both office users a working admin login. Refusing duplicates stops that
// being recreated. The check covers INACTIVE employees too — a former employee
// can be restored, and would collide the moment they were.
//
// Unlike setEmployeeActive there is no self guard: changing your own PIN is
// safe and useful. It will sign you out, which is correct and expected.
async function handleSetEmployeePin(body) {
  const { employeeId, pin } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  const next = String(pin ?? "").trim();
  // Digits only: the login screen is a numeric keypad on a phone, so anything
  // else is a PIN nobody can actually type in the field.
  if (!/^\d{4,8}$/.test(next)) {
    return resp(400, { ok: false, error: "PIN must be 4 to 8 digits." });
  }

  // ⚠⚠ THIS RESOLVES AND CLASH-CHECKS IN NEON — fixed 2026-08-24, same day as
  // slice 5 shipped, because it was MISSED by that sweep and broke in the field.
  //
  // It was `fetchAll(TABLES.employees)` then `.find(r => r.id === employeeId)`.
  // That is an AIRTABLE read, and a natively-hired employee is not in it — so
  // changing their PIN answered **"No such employee."** with a 404, for a person
  // who had just logged in successfully.
  //
  // ⚠ THE LESSON, AND IT IS A NEW ONE: the slice-5 sweep covered the SQL sites,
  // the rec-id guards and the Airtable WRITES, and still missed this because it
  // is an Airtable **READ** used as an existence check. A handler can be fully
  // dual-handled in every statement it writes and still 404 in its first three
  // lines. **On slice 6, grep `fetchAll(TABLES.jobs)` and `atFetch` GETs too,
  // not just the writes.**
  //
  // The clash half mattered just as much and would have failed silently rather
  // than loudly: scanning Airtable cannot see a native hire's PIN, so two people
  // could end up sharing one — and a shared PIN makes `neonLoginCandidate`
  // ambiguous, which it refuses, locking BOTH of them out.
  //
  // Fails CLOSED: if Neon cannot answer we do not know whether the PIN is free,
  // and guessing "free" is the answer that creates the collision.
  const who = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS handle, name,
            (pin IS NOT NULL AND btrim(pin) = $2) AS pin_clash
       FROM employees
      WHERE airtable_id = $1 OR id::text = $1
         OR (pin IS NOT NULL AND btrim(pin) = $2)`, [employeeId, next]);
  if (!who?.rows) {
    return resp(503, { ok: false, error: "Couldn't check that PIN right now. Nothing was changed — please try again." });
  }
  const target = who.rows.find(r => r.handle === String(employeeId));
  if (!target) return resp(404, { ok: false, error: "No such employee." });

  const clash = who.rows.find(r => r.pin_clash === true && r.handle !== String(employeeId));
  if (clash) {
    return resp(409, {
      ok: false,
      error: `That PIN is already used by ${clash.name || "another employee"}. Two people sharing a PIN lets either log in as the other — pick a different one.`,
    });
  }

  // Neon first and fails CLOSED, same shape and same reasoning as
  // handleSetEmployeeActive: a PIN change that silently left old sessions
  // alive would be exactly the wrong thing to report as done. Note this
  // statement touches ONLY token_valid_from — it must never set terminated_on,
  // which would file a working employee as having left.
  // Writes the PIN to Neon as well, so the two stores can't drift apart before
  // login flips. Plaintext by owner decision — see db/schema/017_employees_full.sql.
  const rows = await neonWrite("repinEmployee",
    `UPDATE employees
        SET token_valid_from = $2::timestamptz,
            pin              = $3::text
      WHERE airtable_id = $1 OR id::text = $1
  RETURNING COALESCE(airtable_id, id::text) AS handle, airtable_id`, [employeeId, new Date().toISOString(), next]);
  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error(`employee ${employeeId} is not in Neon yet — cannot sign out their devices`);
  }
  clearRevocationCache();

  // ⚠ Gated on the row's REC ID, not on `employeeId` — cutover slice 5. A
  // natively-hired employee has no Airtable record, so this PATCH would address
  // `Employees/<uuid>`, 404, and throw AFTER the authoritative Neon write had
  // already landed: the PIN would be changed and their devices signed out, and
  // the admin would see a 500 saying it failed. Mirror, not gospel.
  const repinRecId = rows[0]?.airtable_id || null;
  if (repinRecId) {
    await mirrorToAirtable("repinEmployee", () =>
      atFetch(`${encodeURIComponent(TABLES.employees)}/${repinRecId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: { [_EMP_FLD.pin]: next } }),
      }));
  }

  // `target` is a Neon row now, not an Airtable record — plain `.name`.
  return resp(200, { ok: true, employeeId, name: target.name || "" });
}

// Turn an employee's access on or off. THE point of the screen.
//
// ── ORDER MATTERS, AND SO DOES WHICH HALF FAILS CLOSED ─────────────────────
// Two writes: revoke live sessions (Neon) and block future logins (Airtable).
// Neon goes FIRST and THROWS on failure (neonWrite), which is deliberately the
// opposite of the fail-soft read in _revocation.js:
//
//   * Neon fails      -> 500, nothing changed, admin retries. Better than
//                        reporting success while the leaver's phone still works.
//   * Airtable fails  -> sessions are already dead; they could log in again
//                        with their PIN. Recoverable by retrying, and strictly
//                        the safer way round to half-fail.
//
// Doing it the other way — Airtable first — produces the one outcome with no
// tell: the box is unticked, the screen says done, and the phone keeps working.
//
// ── `active` IS NOW MIRRORED TO NEON, AND THAT IS A CHANGE ────────────────
// The standing warning was: never write `active` to Neon, because the ETL
// dimension load (db/etl/time-entries-full.mjs) overwrites it from Airtable on
// every run, so a Neon-only deactivation is silently erased.
//
// That still holds — the point is that this is no longer Neon-ONLY. Both
// stores are written here, so the ETL rewrites the same value it finds, and
// the two agree. It is required for the login flip (Stage 1,
// db/schema/017_employees_full.sql): Neon cannot become the login authority
// while its `active` column is a guess.
//
// If the Airtable half fails after Neon succeeded, the next ETL run flips Neon
// back to active — which is the correct recovery, because Airtable is still
// the authority until login moves. Do not "fix" that by dropping the mirror.
async function handleSetEmployeeActive(body, authUser) {
  const { employeeId, active, note } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  if (typeof active !== "boolean") {
    return resp(400, { ok: false, error: "Missing `active` (true or false)." });
  }
  // No self-lockout. Deactivating yourself while you are the only admin bricks
  // the screen that would undo it, and there is no recovery path in the app.
  if (authUser && employeeId === authUser.id) {
    return resp(400, { ok: false, error: "You can't change your own access. Ask another admin." });
  }

  // ⚠ Every branch below MUST verify a row was actually matched. An UPDATE that
  // hits nothing is a SUCCESSFUL query — neonWrite would not throw — so without
  // this check, deactivating someone who is in Airtable but not yet in Neon
  // (a new hire, or any gap since the last ETL run) would report success while
  // recording no revocation at all. That is precisely the silent lie this whole
  // feature exists to remove, so it is checked rather than assumed.
  // Returns the matched row so the caller can read its REC ID — see the mirror
  // note below the branches.
  const mustHaveMatched = (rows) => {
    if (!Array.isArray(rows) || rows.length === 0) {
      throw new Error(`employee ${employeeId} is not in Neon yet — cannot record a session revocation for them`);
    }
    return rows[0];
  };

  const nowIso = new Date().toISOString();
  let matched;
  if (active === false) {
    // token_valid_from stamped from THIS clock, so it is compared against a
    // token `iat` from the same family of clock rather than Postgres now().
    // terminated_on uses COALESCE so re-deactivating someone doesn't move the
    // date they actually left.
    // ⚠ $2 is cast EXPLICITLY in both places, and the date is derived from the
    // timestamptz rather than re-casting the parameter. Postgres deduces ONE
    // type per parameter across the whole statement, so the earlier form —
    // `token_valid_from = $2` next to `$2::date` — asked it to be timestamptz
    // and date at once and failed with "inconsistent types deduced for
    // parameter $2". It failed on every single revocation; the offline tests
    // could not catch it because they die at the connection before Postgres
    // ever parses the SQL.
    matched = mustHaveMatched(await neonWrite("revokeEmployee",
      `UPDATE employees
          SET token_valid_from = $2::timestamptz,
              active           = false,
              terminated_on    = COALESCE(terminated_on, ($2::timestamptz)::date),
              termination_note = COALESCE(NULLIF($3::text, ''), termination_note)
        WHERE airtable_id = $1 OR id::text = $1
    RETURNING airtable_id`, [employeeId, nowIso, String(note || "").trim()]));
  } else {
    matched = mustHaveMatched(await neonWrite("restoreEmployee",
      `UPDATE employees
          SET token_valid_from = NULL,
              active           = true,
              terminated_on    = NULL,
              termination_note = NULL
        WHERE airtable_id = $1 OR id::text = $1
    RETURNING airtable_id`, [employeeId]));
  }
  // This instance stops honouring the dead token now instead of up to 60s from
  // now. Other instances still take up to REVOCATION_TTL_MS — ≤60s is the real
  // guarantee, this is a courtesy to whoever is doing the clicking.
  clearRevocationCache();

  // ⚠ Gated on the row's REC ID — cutover slice 5. A natively-hired employee
  // has no Airtable record, so this would PATCH `Employees/<uuid>`, 404 and
  // throw — AFTER the revocation had already landed in Neon. The admin would
  // be told the deactivation failed while the person was, in fact, locked out.
  //
  // ⚠⚠ The ORDER-AND-WHICH-HALF-FAILS-CLOSED note above still holds and is why
  // this is a mirror rather than a second authority: Neon revokes the live
  // session and is the half that must not silently fail. Airtable's `Active`
  // box only blocked FUTURE logins, and login has read Neon since 2026-08-08.
  // For a native employee that box does not exist and is not needed.
  const accessRecId = matched?.airtable_id || null;
  if (accessRecId) {
    await mirrorToAirtable("setEmployeeActive", () =>
      atFetch(`${encodeURIComponent(TABLES.employees)}/${accessRecId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields: { [_EMP_FLD.active]: active } }),
      }));
  }

  return resp(200, { ok: true, employeeId, active });
}

// Mark someone as paid a salary, so payroll never splits their hours into
// Regular/Overtime.
//
// ── WHY THIS EXISTS ────────────────────────────────────────────────────────
// It replaced `const SALARIED = ["Larry Unruh", "Miles Unruh", "Rick Unruh"]`,
// hardcoded in index.html and matched against the employee NAME. Renaming any
// of those three — on the People screen, which exists and invites exactly that
// — dropped them out of the list silently and paid them hourly WITH OVERTIME on
// the next payroll run. A live money hazard with no error and no warning, and
// nothing about the name list said it was load-bearing.
//
// NEON ONLY, no Airtable mirror — unlike handleSetEmployeeActive above. There is
// no Airtable column for this; the flag was invented here (db/schema/031), and
// employees have been Neon-owned since Stage 5 retired the ETL dimension load.
// Adding an Airtable twin would create a second copy with no reader.
//
// `labor_type` is NOT this flag and must not be repurposed as one: it reads
// "Regular" for all three of the salaried owners.
async function handleSetEmployeeSalaried(body) {
  const { employeeId, salaried } = body || {};
  if (!employeeId || !isEmployeeHandle(employeeId)) {
    return resp(400, { ok: false, error: "Missing or invalid employeeId." });
  }
  if (typeof salaried !== "boolean") {
    return resp(400, { ok: false, error: "salaried must be true or false." });
  }

  // Same reasoning as mustHaveMatched in handleSetEmployeeActive: a zero-row
  // UPDATE is a SUCCESSFUL query. Without this, setting the flag on anyone not
  // in Neon would report success and change nothing — and the screen would show
  // the toggle in its new position, which is the silent lie to avoid on a
  // setting that decides whether someone is paid overtime.
  const rows = await neonWrite("setEmployeeSalaried",
    `UPDATE employees SET salaried = $2 WHERE airtable_id = $1 OR id::text = $1
      RETURNING COALESCE(airtable_id, id::text) AS airtable_id, name, salaried`, [employeeId, salaried]);
  if (!Array.isArray(rows) || rows.length === 0) {
    return resp(404, { ok: false, error: `Employee ${employeeId} is not in Neon — nothing was changed.` });
  }

  return resp(200, { ok: true, employeeId, salaried: rows[0].salaried === true, name: rows[0].name });
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
    year:gNum(f,F.job.year)??null,
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
      jobInspections:g(f,F.job.jobInspections)||"",
      addPhotosLink:extractUrl(g(f,F.job.addPhotosLink)),
      viewPhotosLink:extractUrl(g(f,F.job.viewPhotosLink)),
      // Drives the in-app Photos tab. Null/blank means this job never had its
      // pCloud folders provisioned — the tab must say so plainly rather than
      // guess at a folder path.
      photoFolderId:g(f,F.job.pcloudPhotoFolderId)||"",
      trelloCardId:g(f,F.job.trelloCardId)||"",
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
      birdDate:g(f,F.job.birdDate)||"",
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
      projectedGrossProfitPct,
    // ── db/schema/065, and they are NULL here on purpose ───────────────────
    // The cost/sell split is a Neon column set; the frozen Airtable copy has no
    // twin for any of it and never will. This mapper still runs on the two
    // job-write paths that echo an Airtable record back to the client, so the
    // keys are emitted — NULL, so the card renders "—" — rather than omitted,
    // which is the standing rule for this pair of mappers: a key present in one
    // and absent from the other is how a field silently disappears. The client
    // re-reads the job from Neon straight after either write.
    markupPct: gNum(f, "Job Markup %") ?? null,
    projectedEstimatedMaterialMarkup: null,
    projectedEstimatedMaterialSell:   null,
    projectedEstimatedLaborSell:      null,
    // db/schema/066, NULL for the same reason as the four above — no Airtable
    // twin, ever. ⚠ NULL here is NOT the same claim as 0: it means "this path
    // cannot know", and `projectedEstimatedTotalCost` above is correspondingly
    // a two-term sum. Emitting 0 would assert the job has no bought-in cost.
    projectedEstimatedOtherCost:      null,
    estimateCount:       null,
    estimateLegacyCount: null,
  };
}

// ── NEON-FIRST job reads ───────────────────────────────────────────────────
// The descriptive spine comes from `jobs` (refreshed hourly by _jobs-sync.js);
// every financial number is COMPUTED LIVE from the GP views, never copied, so it
// cannot go stale between syncs. Labor cost comes from v_job_financials_true —
// see db/schema/006_true_labor_cost.sql for why Airtable's own figure was wrong.
//
// `id` stays the AIRTABLE record id, unlike time entries. Jobs are still
// Airtable-keyed everywhere else in the app — expenses, invoices, photos and the
// inventory app all address a job by `rec…` — so switching it here would break
// all of them. Time entries could move to a uuid because nothing else referenced
// them; jobs cannot, yet.
const JOB_SELECT = `
  -- ⚠⚠ COALESCE, NOT A BARE airtable_id — cutover slice 6. This one column is
  -- the job id the ENTIRE app speaks: mapJobFromNeon returns it as job.id,
  -- and it comes straight back as jobId on expenses, photos, estimates,
  -- invoices, panels, the schedule and every R2 prefix. A bare emit would hand
  -- the client NULL for a native job, and every one of those would break at
  -- once — the job would list, and nothing on it would open.
  SELECT COALESCE(j.airtable_id, j.id::text) AS airtable_id,
         j.name, j.po, j.status, j.job_type, j.job_year,
         j.address_full, j.contractor_name, j.contractor_at_id,
         j.generator_installed, j.power_company_name, j.power_company_contact,
         j.power_company_at_id, j.power_contact_at_id, j.power_company_cell_phone,
         j.power_company_office_phone, j.power_company_email, j.power_company,
         j.aic_number, j.temp_work_order, j.work_order_number, j.meter_number,
         j.permit_number, j.inspection_agency, j.inspection_agency_phone,
         j.inspection_agency_email, j.inspection_agency_at_id,
         j.inspection_scheduling_link, j.inspector_name, j.inspector_phone,
         j.inspector_email, j.inspector_at_id, j.job_inspections,
         j.inspection_not_required, j.add_photos_link, j.view_photos_link,
         j.pcloud_photo_folder_id, j.pcloud_invoices_sent_id, j.trello_card_id,
         j.tax_status, j.billing_method, j.customer_first_name, j.customer_last_name,
         j.address_street, j.address_city, j.address_state, j.address_zip,
         -- App-owned, Neon-only (no Airtable twin). See db/schema/020 and 027.
         j.city_tax, j.clock_visibility, j.overhead,
         j.customer_phone, j.customer_email, j.start_service_call,
         j.service_call_created, j.project_complete, j.miles_from_shop, j.notes,
         j.bird_date::text AS bird_date, j.workflow_status, j.billable_hourly_rate,
         j.labor_billable_rate_at_id,
         -- The markup ACTUAL material is billed at (10% on every job today). The
         -- new-estimate form seeds its markup box from it, so estimated and
         -- actual material stop using two unrelated markup models — which is
         -- what makes est GP and final GP comparable at all. db/schema/065.
         j.markup_pct,
         r.base_contract_amount, r.total_contract_billed, r.total_wire_cost,
         r.reviewed_wire_cost_rollup, r.pipe_cost, r.pipe_cost_reviewed,
         r.expected_revenue, r.hours_rollup,
         r.est_labor_hours_rollup, r.est_labor_cost_rollup, r.est_material_cost_rollup,
         r.est_material_markup_rollup, r.est_material_sell_rollup,
         r.est_labor_sell_rollup, r.est_counted, r.est_legacy_count,
         -- db/schema/066. ⚠⚠ THIS IS THE THIRD OF THREE EXPLICIT COLUMN LISTS a
         -- new rollup has to be added to, and not one of them is a star select.
         -- (No backticks in this comment: it lives inside a JS template literal
         -- and a stray one ends the string mid-query.)
         --   v_job_rollups        the rollup itself
         --   v_job_rollups_true   a passthrough that names all 35 columns
         --   JOB_SELECT           right here
         -- Miss any one and the mapper reads undefined, n(...) || 0 makes it a
         -- clean $0.00, and Estimated Direct Cost quietly drops six figures.
         -- Two of the three were missed on the first pass of this change.
         r.est_other_cost_rollup,
         r.reviewed_expenses_rollup,
         r.total_actual_expenses_audit,
         f.total_revenue_live, f.total_materials_live, f.total_labor_cost_live,
         f.materials_in_progress, f.gross_profit_live_dollar, f.gross_profit_live_pct,
         f.actual_job_cost_cogs, f.total_reviewed_costs, f.total_labor_cost_final,
         f.gross_profit_final_dollar, f.gross_profit_final_pct,
         t.all_labor_reviewed
    FROM jobs j
    LEFT JOIN v_job_rollups_true      r ON r.id = j.id
    LEFT JOIN v_job_financials_true   f ON f.id = j.id
    LEFT JOIN v_job_labor_cost_true_by_job t ON t.job_id = j.id`;

const n  = v => (v === null || v === undefined ? null : Number(v));
const s  = v => (v === null || v === undefined ? "" : String(v));

// Produces the SAME shape mapJob does. Any key added there must be added here or
// the flip silently drops it from the response — that is what the diff harness in
// the commit for this change checks, key by key across every job.
function mapJobFromNeon(r) {
  // Airtable's "All … Reviewed?" formulas, reproduced from the rollups rather than
  // read as stored values. The Airtable originals compare a total against its
  // reviewed twin; labor now comes from the true view instead of the manual
  // allocation checkbox that nothing ever ticked.
  const eq = (a, b) => Number(a || 0) === Number(b || 0);
  const allWire     = eq(r.total_wire_cost, r.reviewed_wire_cost_rollup);
  const allPipe     = eq(r.pipe_cost, r.pipe_cost_reviewed);
  const allExpenses = eq(r.total_actual_expenses_audit, r.reviewed_expenses_rollup);

  // ⚠ THE FILTERED-vs-UNFILTERED TRAP (CLAUDE.md). These four keys are named
  // "projected"/"all status" but deliberately read the FILTERED rollups — estimates
  // with Status Sent / Approved / Archived-Completed only. mapJob does the same and
  // says so: the Est. GP card is meant to ignore Draft and Rejected estimates, so a
  // Not-Awarded job holding a Draft estimate correctly shows zero.
  //
  // The first cut of this function used Neon's `expected_revenue_all_status` and
  // `proj_est_*` — the genuinely unfiltered columns, which is what the names
  // suggest. That made 4 jobs (Cross Club Ministries, David Troyer, Doylestown DG,
  // Guernsey County Dog Shelter) report Draft-estimate revenue the app has always
  // and correctly hidden. Caught by the key-by-key diff against Airtable.
  const expectedRevenueAllStatus       = n(r.expected_revenue) || 0;
  const projectedEstimatedMaterialCost = n(r.est_material_cost_rollup) || 0;
  const projectedEstimatedLaborCost    = n(r.est_labor_cost_rollup) || 0;
  // db/schema/066 — bought-in direct cost (subs, quoted gear, rentals) plus the
  // sales tax paid on it. `est_other_cost_rollup`, the FILTERED twin, for the
  // reason the block above gives: `proj_est_other_cost` is the unfiltered one
  // and would pull Draft estimates into a card that has always hidden them.
  //
  // ⚠ THE AIRTABLE MAPPER (`mapJob`, ~line 5491) DELIBERATELY HAS NO SUCH TERM.
  // Airtable never had a column for this and is not gaining one — the base is
  // frozen and AIRTABLE_WRITES is off. That path can only ever answer with the
  // two-term sum, which is correct for the data it reads.
  const projectedEstimatedOtherCost    = n(r.est_other_cost_rollup) || 0;
  const projectedEstimatedTotalCost = projectedEstimatedMaterialCost + projectedEstimatedLaborCost
                                    + projectedEstimatedOtherCost;
  const projectedGrossProfitDollar  = expectedRevenueAllStatus - projectedEstimatedTotalCost;
  const projectedGrossProfitPct     = expectedRevenueAllStatus > 0
    ? (projectedGrossProfitDollar / expectedRevenueAllStatus) : null;

  return {
    id: r.airtable_id, name: s(r.name), po: s(r.po), status: s(r.status),
    type: s(r.job_type), address: s(r.address_full), contractor: s(r.contractor_name),
    year: n(r.job_year), contractorId: r.contractor_at_id || null,
    generatorInstalled: r.generator_installed === true,
    powerCompanyName: s(r.power_company_name), powerCompanyContact: s(r.power_company_contact),
    powerCompanyId: r.power_company_at_id || null, powerContactId: r.power_contact_at_id || null,
    powerContactName: s(r.power_company_contact),
    powerCompanyPhone: s(r.power_company_cell_phone),
    powerCompanyCellPhone: s(r.power_company_cell_phone),
    powerCompanyOfficePhone: s(r.power_company_office_phone),
    powerCompanyEmail: s(r.power_company_email),
    aicNumber: s(r.aic_number), tempWorkOrder: s(r.temp_work_order),
    permWorkOrder: s(r.work_order_number), meterNumber: s(r.meter_number),
    permitNumber: s(r.permit_number), inspectionAgency: s(r.inspection_agency),
    inspectionAgencyPhone: s(r.inspection_agency_phone),
    inspectionAgencyEmail: s(r.inspection_agency_email),
    inspectionSchedulingLink: s(r.inspection_scheduling_link),
    inspectionContacts: s(r.inspector_name), jobInspections: s(r.job_inspections),
    addPhotosLink: extractUrl(r.add_photos_link), viewPhotosLink: extractUrl(r.view_photos_link),
    photoFolderId: s(r.pcloud_photo_folder_id), trelloCardId: s(r.trello_card_id),
    taxStatus: s(r.tax_status), powerCompanyIntake: s(r.power_company),
    billingMethod: s(r.billing_method),
    baseContractAmount: n(r.base_contract_amount), totalContractBilled: n(r.total_contract_billed),
    customerFirstName: s(r.customer_first_name), customerLastName: s(r.customer_last_name),
    // NULL deliberately survives as null rather than becoming "" — "not yet
    // decided" and "decided: no tax" are different answers and the UI shows both.
    cityTax: r.city_tax ?? null,
    // null = the job's status decides, as normal. See db/schema/027.
    clockVisibility: r.clock_visibility ?? null,
    // Shop Work, Office Work — cost centres, not customer jobs. Owner 2026-08-11:
    // "shop and office work are normally overhead cost so we [don't] worry about
    // gp on those". Shop Work is typed T&M with a billable rate, so hours × rate
    // invented $38,155 of revenue nobody was ever invoiced. See db/schema/038.
    //
    // ⚠ NEON ONLY, and absent from mapJob entirely — the same treatment cityTax
    // and clockVisibility get, because Airtable has no such column. On the
    // Airtable fallback path it arrives undefined, which is falsy, so a job
    // shows normally. That is the right way round: during a Neon outage the app
    // cannot know, and showing a job it shouldn't beats hiding one it should.
    overhead: r.overhead === true,
    customerStreet: s(r.address_street), customerCity: s(r.address_city),
    customerState: s(r.address_state), customerZip: s(r.address_zip),
    customerPhone: s(r.customer_phone), customerEmail: s(r.customer_email),
    startServiceCall: r.start_service_call === true,
    serviceCallCreated: r.service_call_created === true,
    projectComplete: r.project_complete === true,
    milesFromShop: n(r.miles_from_shop), notes: s(r.notes), birdDate: s(r.bird_date),
    totalRevenueLive: n(r.total_revenue_live), totalMaterialsLive: n(r.total_materials_live),
    totalLaborCostLive: n(r.total_labor_cost_live), totalWireCost: n(r.total_wire_cost),
    pipeCost: n(r.pipe_cost), materialsInProgress: n(r.materials_in_progress),
    grossProfitLiveDollar: n(r.gross_profit_live_dollar),
    grossProfitLivePct: n(r.gross_profit_live_pct),
    workflowStatus: r.workflow_status ?? null,
    estimatedLaborHoursRollup: n(r.est_labor_hours_rollup), hoursRollup: n(r.hours_rollup),
    billableHourlyRate: n(r.billable_hourly_rate),
    laborBillableRateId: r.labor_billable_rate_at_id || null,
    inspectionAgencyId: r.inspection_agency_at_id || null,
    inspectorId: r.inspector_at_id || null,
    // The UI constrains a job to one inspector, so the first element is the value.
    inspectorName: s(r.inspector_name).split(", ")[0] || "",
    inspectorPhone: s(r.inspector_phone).split(", ")[0] || "",
    inspectorEmail: s(r.inspector_email).split(", ")[0] || "",
    inspectionNotRequired: r.inspection_not_required === true,
    pCloudInvoicesSentId: r.pcloud_invoices_sent_id || null,
    expectedRevenue: n(r.expected_revenue), actualJobCostCogs: n(r.actual_job_cost_cogs),
    totalReviewedCosts: n(r.total_reviewed_costs),
    totalLaborCostFinal: n(r.total_labor_cost_final),
    grossProfitFinalDollar: n(r.gross_profit_final_dollar),
    grossProfitFinalPct: n(r.gross_profit_final_pct),
    allMaterialsReviewed: allWire && allPipe && allExpenses,
    allWireReviewed: allWire, allPipeReviewed: allPipe,
    allExpensesReviewed: allExpenses,
    allLaborReviewed: r.all_labor_reviewed === true,
    expectedRevenueAllStatus, projectedEstimatedTotalCost,
    projectedEstimatedLaborHours: n(r.est_labor_hours_rollup),
    projectedEstimatedMaterialCost, projectedEstimatedLaborCost,
    projectedGrossProfitDollar, projectedGrossProfitPct,
    // ── db/schema/065 ──────────────────────────────────────────────────────
    // The sell side of the estimate, which the cost-only cards above could never
    // show. `projectedEstimatedMaterialCost` is now genuinely a COST; the markup
    // that used to be buried inside it is its own line.
    markupPct: n(r.markup_pct),
    projectedEstimatedMaterialMarkup: n(r.est_material_markup_rollup),
    projectedEstimatedMaterialSell:   n(r.est_material_sell_rollup),
    projectedEstimatedLaborSell:      n(r.est_labor_sell_rollup),
    // db/schema/066. Emitted as its own key AND folded into
    // projectedEstimatedTotalCost above, because a Direct Cost that jumps with
    // nothing on screen to explain it is the same silent shape as a native row
    // matching nothing — the grid renders a tile for this.
    projectedEstimatedOtherCost:      projectedEstimatedOtherCost,
    // ⚠ How many of the counted estimates predate the split. The GP figures are
    // exact for those (they reproduce the old arithmetic); the two SELL figures
    // are INFERRED. The screen has to say which it is showing.
    estimateCount:       n(r.est_counted),
    estimateLegacyCount: n(r.est_legacy_count),
  };
}

async function handleJobs() {
  if (neonEnabled()) {
    const q = await neonQuery(`${JOB_SELECT} ORDER BY j.name`);
    if (q?.rows?.length) {
      const jobs = q.rows.map(mapJobFromNeon)
        .filter(j => !["archived","cancelled","canceled","closed"].includes(normalize(j.status)));
      return resp(200, { ok: true, jobs, _source: "neon", _ms: q.ms });
    }
    // Guarded on .length, not .rows: an empty jobs table is never a legitimate
    // answer here — the app cannot function with no jobs — so treat it as failure
    // and let Airtable answer rather than blanking the job list.
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`jobs: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  const records = await fetchAll(TABLES.jobs);
  const jobs = records
    .map(mapJob)
    .filter(j => !["archived","cancelled","canceled","closed"].includes(normalize(j.status)));
  return resp(200, { ok: true, jobs, _source: "airtable" });
}

// Returns a single Job in the same shape as handleJobs. Used to refresh
// rollup-driven fields (Expected Revenue, Projected Gross Profit, etc.)
// after Job Estimates writes so the Est. GP cards don't show stale data.
async function handleJobById(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // Flipped alongside handleJobs deliberately. This runs after an estimate write
  // to refresh the Est. GP cards, so if it read Airtable while the list read Neon,
  // saving an estimate would visibly REVERT the job's numbers to the old labor
  // figures. The two must share a source.
  if (neonEnabled()) {
    const q = await neonQuery(`${JOB_SELECT} WHERE j.airtable_id = $1 OR j.id::text = $1`, [jobId]);
    if (q?.rows?.length) {
      return resp(200, { ok: true, job: mapJobFromNeon(q.rows[0]), _source: "neon", _ms: q.ms });
    }
    // ⚠ THE AMBIGUITY THAT JUSTIFIED THE FALLBACK IS GONE. The note here used to
    // read: "No rows is ambiguous — a genuinely unknown job, or a job created in
    // Airtable within the last hour that the sync has not carried over yet."
    // Both halves died on 2026-08-25. The hourly sync was retired, and no job is
    // created in Airtable any more, so there is no such thing as a job Neon has
    // not heard of yet. No rows now means exactly one thing: no such job.
    //
    // ⚠⚠ AND THESE TWO OUTCOMES MUST NOT SHARE AN ANSWER. The first cut of this
    // returned 503 for both, which tells someone looking at a deleted job that
    // the database is down. An error is an outage; an empty result is a fact.
    if (q?.error) {
      console.error(`jobById: Neon read FAILED for ${jobId} — refusing to serve frozen Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that job right now — the database is unavailable. Try again in a moment." });
    }
    return resp(404, { ok: false, error: "Job not found." });
  }
  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });
  return resp(200, { ok: true, job: mapJob(records[0]), _source: "airtable" });
}

// ── GENERATOR — NEON-FIRST as of migration Step 4c ─────────────────────────
// The Airtable path below needs THREE round trips (job -> generator -> service
// history) and reaches the service records through the generator's ASSET ID
// STRING, with the newline-delimited FIND dance to stop "GEN-1" matching
// "GEN-10". Neon answers the whole thing in one query across a real FK, so that
// entire class of substring bug simply does not exist on this path.
//
// ⚠ FALL THROUGH ON A MISS, deliberately — same reasoning as handleJobById.
// Nothing syncs these tables yet (the ETL is hand-run), so a generator added in
// Airtable in the last hour is genuinely absent from Neon. Zero rows is
// therefore AMBIGUOUS — "no generator on this job" or "not copied over yet" —
// and Airtable answers both correctly. Do NOT tighten this to `if (q?.rows)`
// until the writes flip; that is what makes zero rows authoritative, and it
// would hide every Airtable-created generator behind an empty screen.
// ⚠ RESOLVES A GENERATOR BY *EITHER* OF ITS JOBS, since 2026-08-21.
// `generators.job_id` is the INSTALL job — the one where the unit was
// commissioned. When the service-call check opens a new job for a due service
// (`_generator-service.js`), that job has a different rec id, so a lookup on the
// install job alone found nothing and the Generator tab on the service call came
// up EMPTY. The tech standing at the machine got a work order with no serial
// number, no model, and no service history — which is most of the reason to
// open the job at all.
//
// Found by the owner on SEK 293 within an hour of the first six being created.
//
// The base table is joined rather than adding the column to `v_generators`:
// rebuilding a view in this repo has already reinstated a fixed OT bug once
// (006 vs 024), and a join costs nothing here.
async function handleGenerator(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT g.airtable_id, g.id, g.asset_id, g.customer_name, g.brand, g.model, g.kw,
              g.serial_number, g.transfer_switch_model, g.transfer_switch_serial,
              g.fuel_type, g.install_date::text AS install_date, g.service_plan_active,
              g.service_interval_months, g.next_service_due::text AS next_service_due,
              g.warranty_expiration::text AS warranty_expiration, g.status,
              g.battery_install_date::text AS battery_install_date, g.battery_age_years,
              g.service_status, g.notes,
              COALESCE(j.customer_phone, '') AS customer_phone,
              COALESCE(j.address_full, '') AS site_address,
              COALESCE((
                SELECT json_agg(s ORDER BY s.service_date DESC NULLS LAST)
                FROM (
                  SELECT gs.id, gs.airtable_id, gs.service_date::text AS service_date,
                         gs.service_type, gs.technician, gs.service_plan_visit,
                         gs.oil_changed, gs.oil_filter_changed, gs.air_filter_changed,
                         gs.spark_plugs_changed, gs.battery_tested, gs.battery_replaced,
                         gs.load_test_performed, gs.firmware_checked, gs.exercise_checked,
                         gs.trouble_codes, gs.work_performed_notes, gs.parts_used,
                         gs.labor_hours, gs.generator_hours
                  FROM generator_service gs WHERE gs.generator_id = g.id
                ) s
              ), '[]'::json) AS service_records
         FROM v_generators g
         JOIN generators gb ON gb.id = g.id
         LEFT JOIN jobs j ON j.id = g.job_id
        WHERE g.job_airtable_id = $1 OR gb.service_call_job_at_id = $1
           OR g.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1)
        LIMIT 1`, [jobId]);
    if (q?.rows?.length) {
      const r = q.rows[0];
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      const generator = {
        id: r.id, assetId: s(r.asset_id), customer: s(r.customer_name),
        customerPhone: s(r.customer_phone), siteAddress: s(r.site_address),
        brand: s(r.brand), model: s(r.model), kw: s(r.kw),
        serialNumber: s(r.serial_number), transferSwitchModel: s(r.transfer_switch_model),
        transferSwitchSerial: s(r.transfer_switch_serial), fuelType: s(r.fuel_type),
        installDate: s(r.install_date), servicePlanActive: r.service_plan_active === true,
        serviceIntervalMonths: s(r.service_interval_months),
        nextServiceDue: s(r.next_service_due), warrantyExpiration: s(r.warranty_expiration),
        status: s(r.status), batteryInstallDate: s(r.battery_install_date),
        batteryAge: s(r.battery_age_years), serviceStatus: s(r.service_status),
        notes: s(r.notes)
      };
      const serviceRecords = (r.service_records || []).map(sr => ({
        id: sr.id, serviceRecordId: s(sr.airtable_id), serviceNumber: "",
        serviceDate: s(sr.service_date), serviceType: s(sr.service_type),
        technician: s(sr.technician), servicePlanVisit: sr.service_plan_visit === true,
        oilChanged: sr.oil_changed === true, oilFilterChanged: sr.oil_filter_changed === true,
        airFilterChanged: sr.air_filter_changed === true,
        sparkPlugsChanged: sr.spark_plugs_changed === true,
        batteryTested: sr.battery_tested === true, batteryReplaced: sr.battery_replaced === true,
        loadTestPerformed: sr.load_test_performed === true,
        firmwareChecked: sr.firmware_checked === true,
        exerciseChecked: sr.exercise_checked === true,
        troubleCodesFound: s(sr.trouble_codes), workNotes: s(sr.work_performed_notes),
        partsUsed: s(sr.parts_used), laborHours: s(sr.labor_hours),
        generatorHours: s(sr.generator_hours)
      }));
      return resp(200, { ok: true, generator, serviceRecords, _source: "neon", _ms: q.ms });
    }
    // ⚠ AN ERROR AND AN EMPTY ANSWER ARE DIFFERENT THINGS HERE, and the first
    // cut of this conflated them — it returned 503 for both, which would have
    // made every job WITHOUT a generator look like a database outage.
    if (q?.error) {
      console.error(`generator: Neon read FAILED — refusing to serve frozen Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load the generator right now — the database is unavailable. Try again in a moment." });
    }
    // Neon answered and this job simply has no generator. That is the answer.
    return resp(200, { ok: true, generator: null, serviceRecords: [], _source: "neon" });
  }

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, generator: null, serviceRecords: [] });
  const jobName = jobRecords[0].fields[F.job.name] || "";
  // Cross-job filter safety (see CLAUDE.md). A bare FIND is a SUBSTRING test, so
  // "Jenny Ln 1" matches "Jenny Ln 10/11/12" and the wrong job's generator comes
  // back. Two defences, same as handleExpenses / handleGetJobInvoices:
  //   1. newline-delimit both sides so FIND becomes an exact match PER LINKED
  //      ELEMENT rather than anywhere in the joined string
  //   2. verify the linked record id in memory, which also survives two jobs
  //      that genuinely share a name
  // The name is escaped too — it was interpolated raw, so a job name containing
  // a double quote broke the formula outright.
  const safeName = escapeFormulaString(jobName);
  const filter = `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({${F.gen.job}}, "\n") & "\n")`;
  const genCandidates = await fetchAll(TABLES.generators, { filter });
  const genRecords = genCandidates.filter(r =>
    Array.isArray(r.fields?.[F.gen.job]) && r.fields[F.gen.job].includes(jobId));
  if (!genRecords.length) return resp(200, { ok: true, generator: null, serviceRecords: [] });
  const r = genRecords[0]; const f = r.fields || {};
  const generator = { id:r.id,assetId:g(f,F.gen.assetId)||"",customer:g(f,F.gen.customer)||"",customerPhone:g(f,F.gen.customerPhone)||"",siteAddress:g(f,F.gen.siteAddress)||"",brand:g(f,F.gen.brand)||"",model:g(f,F.gen.model)||"",kw:g(f,F.gen.kw)||"",serialNumber:g(f,F.gen.serialNumber)||"",transferSwitchModel:g(f,F.gen.transferSwitchModel)||"",transferSwitchSerial:g(f,F.gen.transferSwitchSerial)||"",fuelType:g(f,F.gen.fuelType)||"",installDate:g(f,F.gen.installDate)||"",servicePlanActive:gBool(f,F.gen.servicePlanActive),serviceIntervalMonths:g(f,F.gen.serviceIntervalMonths)||"",nextServiceDue:g(f,F.gen.nextServiceDue)||"",warrantyExpiration:g(f,F.gen.warrantyExpiration)||"",status:g(f,F.gen.status)||"",batteryInstallDate:g(f,F.gen.batteryInstallDate)||"",batteryAge:g(f,F.gen.batteryAge)||"",serviceStatus:g(f,F.gen.serviceStatus)||"",notes:g(f,F.gen.notes)||"" };
  const genAssetId = generator.assetId || "";
  // Same shape, same fix: an asset id that is a prefix of another (GEN-1 vs
  // GEN-10) would otherwise pull in the wrong generator's service history.
  const svcFilter = genAssetId
    ? `FIND("\n${escapeFormulaString(genAssetId)}\n", "\n" & ARRAYJOIN({${F.svc.generator}}, "\n") & "\n")`
    : `FALSE()`;
  const svcRecords = await fetchAll(TABLES.generatorService, { filter: svcFilter, sortField: F.svc.serviceDate, sortDir: "desc" });
  const serviceRecords = svcRecords.map(sr => { const sf=sr.fields||{}; return { id:sr.id,serviceRecordId:g(sf,F.svc.serviceRecordId)||"",serviceNumber:g(sf,F.svc.serviceNumber)||"",serviceDate:g(sf,F.svc.serviceDate)||"",serviceType:g(sf,F.svc.serviceType)||"",technician:(()=>{const v=sf[F.svc.technicianName];return Array.isArray(v)?(v[0]||""):(v||"");})(),servicePlanVisit:gBool(sf,F.svc.servicePlanVisit),oilChanged:gBool(sf,F.svc.oilChanged),oilFilterChanged:gBool(sf,F.svc.oilFilterChanged),airFilterChanged:gBool(sf,F.svc.airFilterChanged),sparkPlugsChanged:gBool(sf,F.svc.sparkPlugsChanged),batteryTested:gBool(sf,F.svc.batteryTested),batteryReplaced:gBool(sf,F.svc.batteryReplaced),loadTestPerformed:gBool(sf,F.svc.loadTestPerformed),firmwareChecked:gBool(sf,F.svc.firmwareChecked),exerciseChecked:gBool(sf,F.svc.exerciseChecked),troubleCodesFound:g(sf,F.svc.troubleCodesFound)||"",workNotes:g(sf,F.svc.workNotes)||"",partsUsed:g(sf,F.svc.partsUsed)||"",laborHours:g(sf,F.svc.laborHours)||"",generatorHours:g(sf,F.svc.generatorHours)||"" }; });
  return resp(200, { ok: true, generator, serviceRecords });
}

async function handleUpdateJobStatus(body) {
  const { jobId, status } = body || {};
  if (!jobId || !status) return resp(400, { ok: false, error: "Missing jobId or status." });
  const VALID = ["New Lead","Estimating","Awarded","Service Call Scheduled","Ready to Invoice","Completed","Not Awarded"];
  if (!VALID.includes(status)) return resp(400, { ok: false, error: "Invalid status value." });
  // ⚠⚠ NEON FIRST. `handleJobs` and `handleJobById` are NEON-FIRST over an
  // hourly mirror, so an Airtable-only write here does not stick: the frontend
  // patches local state and the card looks right, then a refresh re-reads Neon
  // and the old status comes back. Reported from the field 2026-08-12 —
  // WatersEdge 1 awarded, Airtable said Awarded, Neon still said Estimating.
  //
  // This is the FOURTH time this project has been bitten by "flip a read
  // without its write" (ROADMAP §8 records three in one day). It hides every
  // time for the same reason: the UI patches state after saving, so it only
  // shows up on reload.
  //
  // Fails CLOSED, because Neon is what the app reads. A status change that
  // cannot be recorded must report an error rather than appear to work.
  // ── LOCK THE PO HERE TOO — the automation that used to do it is gone ─────
  // `Fill Job PO - Locked` (and its Service Call twin) copied {Job PO} into
  // {Job PO - Locked} when a job left New Lead. Both were undeployed on
  // 2026-08-20, and NOTHING else wrote that value: Neon only ever received it
  // second-hand, through the hourly sync reading Airtable. So Neon storing
  // `po_locked` never made Neon the owner of it.
  //
  // ⚠⚠ WHY THIS IS NOT COSMETIC. `po_locked` is the string the QuickBooks
  // puller matches timesheet job names against, and `_jobs-sync.js` only links
  // a timesheet when that match is UNIQUE. A blank one means every hour logged
  // to the job stays unlinked — paid but never costed, silently.
  //
  // Locking is literally copying `po`, which is what the Airtable formula
  // produced, so this is byte-exact by construction rather than by a format
  // string somebody has to keep in step.
  //
  // COALESCE, so re-running a status change can never re-point a PO that
  // QuickBooks Time is already using. `prev` captures the value from BEFORE the
  // update, which is how we know whether we filled it just now and therefore
  // whether Airtable needs the mirror.
  // ── AND STAMP THE COMPLETION DATE — same story as the PO lock ────────────
  // `Stamp Project Completed Date` was undeployed on 2026-08-20 and nothing else
  // wrote that field. It is not decoration: the Completed webhook prints it on
  // the Trello card ("Project Completed On …"), and GP reporting groups by it.
  // COALESCE so re-completing a job never moves a date that already exists.
  const nRows = await neonWrite("job.updateStatus",
    `WITH prev AS (SELECT id AS prev_id, po_locked AS old_locked,
                          project_completed_at AS old_completed
                     FROM jobs WHERE airtable_id = $1 OR id::text = $1)
     UPDATE jobs j
        SET status = $2,
            po_locked = CASE WHEN $2 <> 'New Lead' THEN COALESCE(j.po_locked, j.po) ELSE j.po_locked END,
            project_completed_at = CASE WHEN $2 = 'Completed'
                                        THEN COALESCE(j.project_completed_at, current_date)
                                        ELSE j.project_completed_at END,
            synced_at = now()
       FROM prev
      WHERE j.id = prev.prev_id
      RETURNING j.po_locked, prev.old_locked, j.po,
                j.project_completed_at::text AS project_completed_at, prev.old_completed`,
    [jobId, status]);

  const nRow = nRows?.[0] || {};
  const lockedNow = (!nRow.old_locked && nRow.po_locked) ? nRow.po_locked : null;
  const completedNow = (!nRow.old_completed && nRow.project_completed_at)
    ? String(nRow.project_completed_at).slice(0, 10) : null;
  if (status !== "New Lead" && !nRow.po_locked) {
    // Only reachable when `po` itself is missing, which the hourly sync fills.
    // Worth a log rather than a guess: composing the string here would be a
    // second definition of a format QuickBooks already depends on.
    console.error(`updateJobStatus: ${jobId} left New Lead with no PO to lock — timesheets will not link until the sync fills jobs.po`);
  }

  // The PO mirror rides along in the SAME PATCH as the status, deliberately.
  // ⚠ The Awarded scenario's QuickBooks module names the jobcode
  // `{{2.Job PO - Locked}}`, where module 2 is an airtable:ActionGetRecord —
  // Make re-reads the job out of AIRTABLE and was never moved to Neon. Item 04
  // replumbed the TRIGGER only. So the value has to be in Airtable before the
  // webhook fires below, or QuickBooks Time gets a blank jobcode. Writing it in
  // this PATCH guarantees the ordering; a second call would not.
  const patchFields = { "fld2FBMjvkOsy9Puu": status };
  if (lockedNow)    patchFields["fldDFQSF2jJmCDWB4"] = lockedNow;      // Job PO - Locked
  if (completedNow) patchFields["fldDcH5hrH596OTdB"] = completedNow;   // Project Completed At
  const data = await mirrorJobPatch("updateJobStatus", jobId, patchFields);

  // Fire whatever this status change is supposed to fire — pCloud folders at
  // Estimating, Trello + QuickBooks Time at Awarded, Trello-completed at
  // Completed. Four Airtable automations did this until 2026-08-12; see
  // docs/PLAN-replumb-job-webhooks.md and _job-webhooks.js.
  //
  // `data` is the PATCH response, so its fields are the CURRENT ones — exactly
  // what the automation's trigger condition would have evaluated after the same
  // write. That is why no re-read is needed.
  //
  // Inert until JOB_WEBHOOKS=app, and each automation stays deployed until its
  // replacement has been seen to fire once.
  // ⚠ `data` is null for a NATIVE job — the Airtable mirror does not exist, and
  // that is the designed state, not a failure. This was the crash: the first
  // native job could not have its status changed because the PATCH 404'd and
  // the handler reported "failed to update status", while Neon had already been
  // written correctly.
  //
  // ⬜ The status webhooks are SKIPPED for a native job rather than fired with
  // nulls. Three of the four job Make scenarios still describe the job from an
  // Airtable record, so they need the same payload conversion slice 2.5 did for
  // the pCloud hook before a native job can drive them. Logged loudly because a
  // silently un-fired webhook is exactly the failure this migration keeps hitting.
  // ⚠ A native job has no Airtable record, so pass its HANDLE with an empty
  // fields object: _job-webhooks resolves status, name, type, PO, contractor and
  // the automation flags out of Neon. Skipping the call entirely — which is what
  // this did first — meant a native job's status change created no pCloud folders
  // and no Trello card, silently. That is how Test 10 (MIT 301) got none.
  const webhooks = await fireJobStatusWebhooks(data || { id: jobId, fields: {} }, atFetch);
  return resp(200, { ok: true, updatedId: data?.id || jobId, ...(webhooks ? { webhooks } : {}) });
}

// ── MAKE REPORTS BACK WHAT IT CREATED ──────────────────────────────────────
// Replaces the two airtable:ActionUpdateRecords modules at the end of the
// "Airtable – Job Awarded" scenario (4509804). They recorded the new ids and set
// the run-once flags; posting the same facts here puts them in NEON instead,
// which is what finally makes that scenario Airtable-free.
//
// ⚠ UNAUTHENTICATED, like `clockWidget`, and safe for the same narrow reasons.
// Make has no session and cannot hold a bearer token, so the payload carries a
// scope token SIGNED FOR THIS ONE JOB (_job-webhooks.js mints it when it fires
// the webhook). The token names the record, so a leaked one cannot touch any
// other job, and it expires in 24 h. No shared secret has to live in Make.
//
// ⚠ FLAGS ONLY EVER GO TRUE. Make reports what it did; it never un-does. A
// callback that arrives with a flag absent must leave the existing value alone,
// or a partial retry would clear a guard and license a duplicate Trello card.
//
// Airtable is still mirrored while the job mirror exists — one writer (us)
// instead of two, and the old guard keeps working until item 10 removes it.
async function handleJobAutomationResult(body) {
  const recordId = String(body?.recordId || "").trim();
  if (!isJobHandle(recordId)) return resp(400, { ok: false, error: "Missing or invalid recordId." });
  if (!verifyScope(body?.token, ["jobAutomation", recordId])) {
    return resp(403, { ok: false, error: "Invalid or expired callback token." });
  }

  const clean = (v) => { const s = String(v ?? "").trim(); return s || null; };
  const tsheetsJobId   = clean(body?.tsheetsJobId);
  const trelloCardId   = clean(body?.trelloCardId);
  const trelloPoCardId = clean(body?.trelloPoCardId);
  // An id arriving IS the proof that half ran, so it implies the flag. Make can
  // also state the flag outright for a half that ran but returned no id.
  const tsheetsCreated = body?.tsheetsCreated === true || !!tsheetsJobId;
  const trelloCreated  = body?.trelloCreated  === true || !!trelloCardId || !!trelloPoCardId;
  // The Completed scenario reports this one. It has no id to imply it — moving a
  // card and archiving another produce nothing worth storing — so it must be
  // stated outright, and it is the ONLY thing stopping a re-completed job from
  // moving the card again.
  const trelloCompleted = body?.trelloCompleted === true;
  // Service Call reports a whole pCloud tree plus its own run-once flag. Same
  // rule as the others: an id implies its flag, flags only ever go true.
  const pcJob      = clean(body?.pcloudJobFolderId);
  const pcReceipts = clean(body?.pcloudReceiptsFolderId);
  const pcJobsite  = clean(body?.pcloudJobsiteFilesFolderId);
  const pcInvoices = clean(body?.pcloudInvoicesFolderId);
  const pcPhotos   = clean(body?.pcloudPhotosFolderId);
  const svcCallCreated = body?.serviceCallCreated === true;

  // Neon first and failing CLOSED: this is now the authority for the run-once
  // guards, and a result we failed to record is what causes a second jobcode.
  const rows = await neonWrite("job.automationResult",
    `UPDATE jobs SET
       tsheets_job_id    = COALESCE($2, tsheets_job_id),
       trello_card_id    = COALESCE($3, trello_card_id),
       trello_po_card_id = COALESCE($4, trello_po_card_id),
       tsheets_created   = CASE WHEN $5 THEN true ELSE tsheets_created END,
       trello_created    = CASE WHEN $6 THEN true ELSE trello_created END,
       trello_completed  = CASE WHEN $7 THEN true ELSE trello_completed END,
       service_call_created = CASE WHEN $8 THEN true ELSE service_call_created END,
       pcloud_job_folder_id           = COALESCE($9,  pcloud_job_folder_id),
       pcloud_receipts_folder_id      = COALESCE($10, pcloud_receipts_folder_id),
       pcloud_jobsite_files_folder_id = COALESCE($11, pcloud_jobsite_files_folder_id),
       pcloud_invoices_sent_id        = COALESCE($12, pcloud_invoices_sent_id),
       pcloud_photo_folder_id         = COALESCE($13, pcloud_photo_folder_id),
       synced_at = now()
     -- ⚠⚠ DUAL HANDLE, AND IT WAS MISSING UNTIL 2026-08-25. recordId here is
     -- whatever the webhook sent, which for a native job is a uuid. A bare
     -- "airtable_id = $1" matched nothing, the guard below returned 404, and
     -- Make's HTTP module failed the whole scenario with DataError: Not Found
     -- — AFTER it had already created the QuickBooks Time jobcode. So the
     -- jobcode existed, its id was never recorded, the run-once flag stayed
     -- unset, and the Trello branch never ran at all. Seen live on Test 10's
     -- Awarded run at 09:47:54Z.
     WHERE airtable_id = $1 OR id::text = $1
     RETURNING tsheets_job_id, trello_card_id, trello_po_card_id, trello_completed,
               service_call_created, pcloud_job_folder_id`,
    [recordId, tsheetsJobId, trelloCardId, trelloPoCardId, tsheetsCreated, trelloCreated,
     trelloCompleted, svcCallCreated, pcJob, pcReceipts, pcJobsite, pcInvoices, pcPhotos]);
  if (!rows?.length) return resp(404, { ok: false, error: "Job not found." });

  // Mirror, failing soft. Losing this costs Airtable-side consistency until the
  // next hourly sync; losing the Neon write above would cost a duplicate.
  const fields = {};
  if (tsheetsJobId)   fields["fld2VnSP0nXsLmXQq"] = tsheetsJobId;   // TSheets Job ID
  if (trelloCardId)   fields["fldxisALDFRhNC6Cl"] = trelloCardId;   // Trello Card ID
  if (trelloPoCardId) fields["fldTWUzDcPB1EBnqS"] = trelloPoCardId; // Trello Card PO ID
  if (tsheetsCreated) fields["fldWDs8praJa3iGlf"] = true;           // Automation – TSheets Created
  if (trelloCreated)  fields["fldlgoNEaus3XGJel"] = true;           // Automation – Trello Created
  if (trelloCompleted) fields["fldewPWukfRLkgDCa"] = true;          // Automation – Trello Completed
  if (svcCallCreated) fields["fld5MZfIjGCYbIO9x"] = true;           // Service Call Created
  if (pcJob)      fields["fldoicx7bnb2Gdg1D"] = pcJob;              // pCloud Folder ID
  if (pcReceipts) fields["fld06WOq5dA4F9CUA"] = pcReceipts;         // pCloud Job Receipts ID
  if (pcJobsite)  fields["fldn0dg7E42B2Pimg"] = pcJobsite;          // pCloud Jobsite Files ID
  if (pcInvoices) fields["fldVtTkUcuh96TgXh"] = pcInvoices;         // pCloud Invoices Sent ID
  if (pcPhotos)   fields["fld655NnOgjRhaVSe"] = pcPhotos;           // pCloud Photo's ID
  if (Object.keys(fields).length) {
    try {
      await mirrorJobPatch("jobAutomationResult", recordId, fields);
    } catch (e) {
      console.error(`jobAutomationResult: Airtable mirror failed for ${recordId} — ${e?.message || e}`);
    }
  }

  return resp(200, { ok: true, recorded: rows[0] });
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

  // ⚠⚠ NEON FIRST — every one of these six is in JOB_SELECT, so an
  // Airtable-only write reverts on the next refresh. Found by SWEEPING for the
  // shape after handleUpdateJobStatus was caught (`ff21d46`); this handler had
  // it, and so did handleCalculateMileage and handleUpdateJobNotes.
  //
  // It hid particularly well here: the response returns `mapJob(data)` from the
  // Airtable record, so the card repaints with the right values immediately.
  // Only a reload shows the old ones. These fields are set once during power-co
  // setup and rarely re-read in the same session, so "I typed the meter number
  // and later it was blank" would surface hours later, disconnected from the save.
  //
  // Only the fields the client actually sent are touched, matching the Airtable
  // PATCH above — an omitted field must not be nulled.
  const nSets = [], nVals = [jobId];
  const nPut = (col, v) => { nVals.push(v); nSets.push(`${col} = $${nVals.length}`); };
  if (powerCompanyId !== undefined) {
    const t = String(powerCompanyId).trim() || null;
    nPut("power_company_at_id", t);
    // Keep the DISPLAY name in step with the link. Resolvable because
    // power_companies moved to Neon in slice 4 of item 06.
    nVals.push(t);
    nSets.push(`power_company_name = (SELECT name FROM power_companies WHERE airtable_id = $${nVals.length} OR id::text = $${nVals.length})`);
  }
  // ⚠ `power_company_contact` (the contact's display name) is NOT updated: the
  // power-contact table is still Airtable-only, so there is nothing to resolve
  // it against yet. The link id is correct immediately and the name catches up
  // on the hourly sync. Fix this when Contacts land — item 06's last slice.
  if (powerContactId !== undefined) nPut("power_contact_at_id", String(powerContactId).trim() || null);
  if (aicNumber      !== undefined) nPut("aic_number", aicNumber);
  if (tempWorkOrder  !== undefined) nPut("temp_work_order", tempWorkOrder);
  if (permWorkOrder  !== undefined) nPut("work_order_number", permWorkOrder);
  if (meterNumber    !== undefined) nPut("meter_number", meterNumber);
  if (nSets.length) {
    await neonWrite("job.updatePowerCo",
      `UPDATE jobs SET ${nSets.join(", ")}, synced_at = now() WHERE airtable_id = $1 OR id::text = $1`, nVals);
  }

  const data = await mirrorJobPatch("updateJob", jobId, fields);
  // ⚠ `data` is null for a native job — the mirror is absent by design. Answer
  // with the handle the caller sent and the job as Neon holds it; re-reading an
  // Airtable record that does not exist is not a fallback, it is a crash.
  return resp(200, { ok: true, updatedId: data?.id || jobId,
                     ...(data ? { job: mapJob(data) } : {}) });
}

async function handleStartServiceCall(body) {
  const { jobId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  // Same Neon-first rule as handleUpdateJobStatus above — `start_service_call`
  // is in JOB_SELECT, so an Airtable-only write reverts on the next refresh.
  //
  // ⚠ THE STATUS MOVES HERE TOO. Module 22 of the Make scenario used to set
  // "Service Call Scheduled" mid-run, and that module goes away with the rest of
  // its Airtable writes — so without this the status would simply never change.
  // Setting it up front rather than after Make finishes also matches how every
  // other status write in this file works, and means the screen is right even if
  // the folder-building half is slow or fails.
  await neonWrite("job.startServiceCall",
    `UPDATE jobs SET start_service_call = true, status = 'Service Call Scheduled',
                     synced_at = now()
      WHERE airtable_id = $1 OR id::text = $1`, [jobId]);

  const data = await mirrorJobPatch("startServiceCall", jobId, {
    "fldgar4OL6AL5k1S6": true,                     // Start Service Call
    "fld2FBMjvkOsy9Puu": "Service Call Scheduled"  // Job Status
  });

  // Its own hook, and its own trigger shape: not a status change but
  // "Start Service Call" checked AND Job Type = Service Call. The type check
  // lives in the module so the condition stays next to the payload it guards.
  // The webhook needs an Airtable record to describe; a native job has none, so
  // it is skipped rather than fired with nulls. Recorded as a slice-6 gap: the
  // service-call Make scenario has to take a payload before native jobs can use
  // it, the same conversion slice 2.5 did for the pCloud hook.
  // Same as updateJobStatus: the handle is enough, the module reads the rest
  // out of Neon.
  const webhooks = await fireServiceCallWebhook(data || { id: jobId, fields: {} });
  return resp(200, { ok: true, updatedId: data?.id || jobId, ...(webhooks ? { webhooks } : {}) });
}

async function handleCompleteServiceCall(body) {
  const { jobId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  // Neon-first, same reason as the two above: `project_complete` is in
  // JOB_SELECT, so an Airtable-only write silently reverts on refresh.
  //
  // ⚠ AND THE STATUS ADVANCE, which `Project Complete Checked` used to do and
  // nothing has done since it was undeployed on 2026-08-20. The button's own
  // confirm says "Mark this service call as complete and ready to invoice?", so
  // without this the app was promising something it had stopped delivering —
  // the tick landed and the job sat in Service Call Scheduled forever.
  //
  // ⚠ NEVER DRAG A COMPLETED JOB BACKWARDS. The old automation excluded New
  // Lead / Estimating / Completed; the button is only reachable mid-service-call
  // so the first two cannot occur, but a re-tick on a finished job could, and
  // "Completed" → "Ready to Invoice" would be a regression, not a fix.
  const rows = await neonWrite("job.completeServiceCall",
    `WITH prev AS (SELECT id AS prev_id, status AS old_status FROM jobs WHERE airtable_id = $1 OR id::text = $1)
     UPDATE jobs j
        SET project_complete = true,
            status = CASE WHEN j.status = 'Completed' THEN j.status ELSE 'Ready to Invoice' END,
            synced_at = now()
       FROM prev
      WHERE j.id = prev.prev_id
      RETURNING j.status, prev.old_status`, [jobId]);

  const row = rows?.[0] || {};
  const fields = { "fldZ4tEiYt6Ke8IlK": true };                  // Project Complete
  if (row.status && row.status !== row.old_status) {
    fields["fld2FBMjvkOsy9Puu"] = row.status;                    // Job Status
  }
  const data = await mirrorJobPatch("completeServiceCall", jobId, fields);
  return resp(200, { ok: true, updatedId: data?.id || jobId, status: row.status || null });
}

// ── THE FIELD APP'S EXPENSE WRITES ARE NEON-FIRST since 2026-08-24 ────────
// The note that used to sit here said "AIRTABLE STAYS THE IDENTITY AUTHORITY
// for expenses, deliberately … Invert this only when receipts move too." That
// condition was met differently than expected: the receipts did not move, the
// KEY did. `expenses/<handle>/…` is built from whatever id the client holds, so
// a native expense's receipts live under its uuid and a mirrored one's stay
// under its rec id. Both resolve, neither moves — which is exactly why the rec
// id must never be stamped back onto a native row afterwards.
//
// `syncExpenseToNeon` (the local wrapper that copied an Airtable response into
// Neon) is gone with it. Feeding a mirror response back through
// `INSERT … ON CONFLICT (airtable_id)` would insert a SECOND row for a native
// expense, because its airtable_id is NULL and nothing conflicts. The shared
// `_expenses.js` helper stays — the inventory push still writes Airtable-first
// and is slice 4c.

// Shared guard for employee self-service on an existing expense. Managers
// (admin/office) may mutate any expense; an employee may mutate ONLY their own
// AND only while it is still "Not Reviewed" (approval locks it). Returns
// { ok:true, uuid, airtableId } or { ok:false, resp } with the right 400/403.

// ── NEON-FIRST since 2026-08-24 (cutover slice 4) ─────────────────────────
// This is the gate on every expense mutation and on the receipt endpoints, and
// it was a bare `atFetch("Expenses/<id>")` — which 404s on a uuid. Nothing else
// in the slice could flip until it resolved either handle.
//
// It also returns BOTH resolved handles now, because every caller needs to know
// which store owns the row: `airtableId` is null exactly when the expense is
// native, and that is what decides whether the Airtable half of a mutation runs
// at all.
//
// ⚠ The Airtable fallback stays for a row Neon has never heard of — a sync that
// failed, or an expense created directly in the base. It cannot serve a native
// row, but a native row is by definition in Neon, so that gap is not reachable.
async function guardExpenseMutation(expenseId, authUser) {
  if (!expenseId) return { ok: false, resp: resp(400, { ok: false, error: "Missing expenseId." }) };
  const isMgr = authUser && (authUser.role === "admin" || authUser.role === "office");

  let uuid = null, airtableId = null, owner = null, reviewed = null, known = false;
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT id::text AS uuid, airtable_id, submitted_by_at_id, reviewed, expense_status
         FROM expenses WHERE airtable_id = $1 OR id::text = $1 LIMIT 1`, [String(expenseId)]);
    if (q?.rows?.length) {
      const r = q.rows[0];
      known = true;
      uuid = r.uuid; airtableId = r.airtable_id || null;
      owner = r.submitted_by_at_id || null;
      reviewed = r.reviewed === true || String(r.expense_status || "") === "Reviewed";
    } else if (q?.error) {
      console.error(`guardExpenseMutation: Neon read failed, falling back: ${q.error}`);
    }
  }

  if (!known) {
    // A uuid can never resolve here, so refuse rather than let atFetch 404 into
    // an unhandled throw.
    if (!String(expenseId).startsWith("rec")) {
      return { ok: false, resp: resp(404, { ok: false, error: "Expense not found." }) };
    }
    let rec;
    try { rec = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`); }
    catch { return { ok: false, resp: resp(404, { ok: false, error: "Expense not found." }) }; }
    const f = rec.fields || {};
    airtableId = rec.id;
    const submitted = f["Submitted By"];
    owner = (Array.isArray(submitted) ? submitted[0] : submitted) || null;
    const status = f["Expense Status"]?.name || f["Expense Status"] || "";
    reviewed = f["Reviewed"] === true || status === "Reviewed";
  }

  if (isMgr) return { ok: true, uuid, airtableId };
  if (owner !== authUser?.id) {
    return { ok: false, resp: resp(403, { ok: false, error: "You can only change your own expenses." }) };
  }
  if (reviewed) {
    return { ok: false, resp: resp(403, { ok: false, error: "This expense has been approved and can no longer be changed." }) };
  }
  return { ok: true, uuid, airtableId };
}

async function handleDeleteExpense(body, authUser) {
  const { expenseId } = body || {};
  const guard = await guardExpenseMutation(expenseId, authUser);
  if (!guard.ok) return guard.resp;

  // ⚠ NEON FIRST, AND IT NO LONGER SWALLOWS. This was
  // `DELETE ... WHERE airtable_id = $1` with `.catch(() => {})` — so on a native
  // expense it matched nothing, failed silently, and the row survived in the one
  // store that counts while Airtable (which never had it) reported success.
  // Deleting from the authoritative store is the delete; if that fails the
  // request must fail, or the user is told a cost is gone when it is not.
  //
  // Cascades to material_billing_allocations by FK, which is intended — an
  // allocation must not outlive the expense it bills.
  await neonWrite("expense.delete",
    `DELETE FROM expenses WHERE airtable_id = $1 OR id::text = $1`, [String(expenseId)]);

  // The mirror. Only a row that HAS a rec id has anything to delete there.
  if (guard.airtableId) {
    await mirrorToAirtable("deleteExpense", () =>
      atFetch(`${encodeURIComponent("Expenses")}/${guard.airtableId}`, { method: "DELETE" }));
  }
  return resp(200, { ok: true, deleted: expenseId });
}

async function handleApproveExpense(body) {
  const { expenseId } = body || {};
  if (!expenseId) return resp(400, { ok: false, error: "Missing expenseId." });

  // ⚠ NEON FIRST. `reviewed` is the gate on the material allocation below and on
  // reviewed_expenses in GP, so the authoritative store has to be the one that
  // records the approval. Fails closed: an approval that did not land must not
  // report success, because the allocation it triggers reads this value back.
  const upd = await neonWrite("expense.approve",
    `UPDATE expenses SET reviewed = true, synced_at = now()
      WHERE airtable_id = $1 OR id::text = $1
      RETURNING COALESCE(airtable_id, id::text) AS handle, airtable_id`,
    [String(expenseId)]);
  if (!upd?.length) return resp(404, { ok: false, error: "Expense not found." });
  const handle     = upd[0].handle;
  const airtableId = upd[0].airtable_id || null;

  // The mirror. Only sets the checkbox — every derived column is computed by
  // v_expenses now (schema 057), so there is nothing to read back.
  //
  // ⚠ syncExpenseToNeon is NOT called on the response any more. It would
  // overwrite `reviewed` with whatever Airtable echoed and, on a native row,
  // INSERT a duplicate — its ON CONFLICT is on airtable_id, which is NULL here,
  // so nothing conflicts.
  if (airtableId) {
    await mirrorToAirtable("approveExpense", () =>
      atFetch(`${encodeURIComponent("Expenses")}/${airtableId}`,
        { method: "PATCH", body: JSON.stringify({ fields: { "fldwSsga6eashzJsw": true } }) }));
  }

  // Approving an expense is what creates its material billing allocation —
  // otherwise the material is a cost with no route onto an invoice. Was
  // Airtable automation wflNmJsnIhWtSjUlL until 2026-08-11.
  //
  // ⚠ ORDER STILL MATTERS, for a different reason than it used to. The gate
  // reads `unbilled_material_amount_calc`, which is computed from this row's own
  // `reviewed` flag and its cost columns — so the Neon UPDATE above must land
  // first. It used to need the Airtable sync first, because the amount was an
  // Airtable formula; 057 moved that computation into the view.
  let allocation;
  try {
    allocation = await createMaterialAllocation(atFetch, handle);
  } catch (e) {
    // The approval itself succeeded and is what the user asked for. The hourly
    // billing sync will adopt an Airtable-only allocation; a failed one gets
    // created next time the expense is approved, which the gate makes free.
    console.error(`approveExpense: allocation failed — ${e?.message || e}`);
    allocation = { created: 0, error: String(e?.message || e) };
  }
  return resp(200, { ok: true, updatedId: handle, allocation });
}

async function handleScissorLiftsByJob(params) {
  const { jobName } = params || {};
  if (!jobName) return resp(200, { ok: true, lifts: [] });

  // Matches on the job NAME, not an id, because `current_job` mirrors Airtable's
  // singleLineText. Making it a real FK is a behaviour change (renaming a job
  // would move lifts) and belongs in its own decision — see 009_scissor_lifts.sql.
  if (neonEnabled()) {
    const q = await neonQuery(
      `${LIFT_SELECT} WHERE current_job = $1 AND status = 'On Job'${LIFT_ORDER}`, [jobName]);
    if (q?.rows) {
      return resp(200, {
        ok: true, lifts: await attachEquipPhotos("lifts", q.rows.map(mapLiftRow)),
        _source: "neon", _ms: q.ms,
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`scissorLiftsByJob: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  const records = await fetchAll(TABLES.scissorLifts, { sortField: "Lift Name", sortDir: "asc" });
  const lifts = records.map(r => { const f=r.fields||{}; const photos=(f["Photo"]||[]).map(a=>a.url); return { id:r.id,name:f["Lift Name"]||"",status:f["Status"]||"Available",currentJob:f["Current Job"]||"",assignedTo:f["Assigned To"]||"",dateDeployed:f["Date Deployed"]||"",notes:f["Notes"]||"",photoUrl:photos[0]||"",hooksLeft:f["Lift Hooks Left at Job"]===true,boxLeft:f["Lift Box Left at Job"]===true }; }).filter(l => l.currentJob === jobName && l.status === "On Job");
  return resp(200, { ok: true, lifts });
}

async function handleJobInspections(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // ── NEON-FIRST (migration Step 4c) ──────────────────────────────────────
  // Replaces two Airtable round trips (fetch the job for its name, then FIND
  // that name inside ARRAYJOIN({Job}) and re-verify the link in memory) with one
  // query on job_airtable_id. The substring hazard the old comment describes —
  // "Jenny Ln 1" matching "Jenny Ln 10/11/12" — cannot exist on this path.
  //
  // The two LOOKUP fields resolve through real joins: Permit Number off the job,
  // and the agency phone off inspection_agencies, falling back to the job's own
  // copy for an inspection with no agency link.
  //
  // Falls through on a miss — nothing syncs these tables, so an inspection added
  // in Airtable an hour ago is genuinely absent from Neon.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT ji.id, ji.inspection_type, ji.inspection_date::text AS inspection_date,
              ji.inspection_status, ji.notes,
              COALESCE(j.permit_number, '') AS permit_number,
              COALESCE(ia.phone, j.inspection_agency_phone, '') AS agency_phone
         FROM job_inspections ji
         LEFT JOIN jobs j                ON j.id  = ji.job_id
         LEFT JOIN inspection_agencies ia ON ia.id = ji.agency_id
        WHERE (ji.job_airtable_id = $1 OR ji.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
        ORDER BY ji.inspection_date DESC NULLS LAST`, [jobId]);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      return resp(200, {
        ok: true,
        inspections: q.rows.map(r => ({
          id: r.id, inspectionType: s(r.inspection_type), date: s(r.inspection_date),
          status: s(r.inspection_status), notes: s(r.notes),
          permitNumber: s(r.permit_number), agencyPhone: s(r.agency_phone)
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`jobInspections: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, inspections: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  // Cross-job filter safety — see the note in handleGenerator. A bare FIND is a
  // substring test, so inspections from "Jenny Ln 10/11/12" would surface on
  // "Jenny Ln 1". Newline-delimit for an exact per-element match, then verify
  // the linked record id in memory for duplicate names.
  const safeName = escapeFormulaString(jobName);
  const candidates = await fetchAll("Job Inspections", {
    filter: `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n")`,
    sortField: "Inspection Date", sortDir: "desc",
  });
  const records = candidates.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));
  const inspections = records.map(r => { const f=r.fields||{}; const permitRaw=f["Permit Number"]; const permit=Array.isArray(permitRaw)?permitRaw[0]:(permitRaw||""); const phoneRaw=f["Inspections Agency Phone #"]; const agencyPhone=Array.isArray(phoneRaw)?phoneRaw[0]:(phoneRaw||""); return { id:r.id,inspectionType:f["Inspection Type"]?.name||f["Inspection Type"]||"",date:f["Inspection Date"]||"",status:f["Inspection Status"]?.name||f["Inspection Status"]||"",notes:f["Notes"]||"",permitNumber:permit,agencyPhone }; });
  return resp(200, { ok: true, inspections });
}

// Job Inspections single-select whitelists, read out of the base 2026-08-07.
// Airtable's typecast:true would silently CREATE a new option from a stray
// client value; Postgres has no such guard at all, so once the write is
// Neon-first the check has to live here or the columns become free text.
// Unknown values fall back to null rather than a guessed default — an
// inspection with no type is honest, one mislabelled "Rough" is not.
const INSPECTION_TYPE_OPTS   = ["Rough", "Service", "Temp", "Final", "Other"];
const INSPECTION_STATUS_OPTS = ["Scheduled", "Passed", "Failed", "Re-Inspect Needed"];

// ── CREATE INSPECTION — NEON-FIRST (migration Step 4c) ────────────────────
// handleJobInspections already reads Neon, so this has to write there or a new
// inspection would be invisible: that read only falls through to Airtable when
// it finds ZERO rows, and every one of the 22 existing inspections is in Neon.
// On any job that already has one, an Airtable-only write would simply never
// appear. Same partial-results trap as the warranties at 83e022c.
async function handleCreateInspection(body) {
  const { jobId, inspectionType, date, status, notes } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const typeSafe   = INSPECTION_TYPE_OPTS.includes(inspectionType) ? inspectionType : null;
  const statusSafe = INSPECTION_STATUS_OPTS.includes(status) ? status : null;

  const rows = await neonWrite("inspection.insert",
    `INSERT INTO job_inspections
       (job_airtable_id, job_id, inspection_type, inspection_date, inspection_status, notes)
     VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1), $2, $3::date, $4, $5)
     RETURNING id`,
    [String(jobId), typeSafe, date || null, statusSafe, notes || null]);
  const neonId = rows?.[0]?.id;
  if (!neonId) return resp(500, { ok: false, error: "Inspection was not written to Neon." });

  const fields = {};
  Object.assign(fields, jobLink("fldqk2pA5w3TSN3q8", jobId));
  if (typeSafe)   fields["fldR2IQkaeRHXytsR"] = typeSafe;
  if (date)       fields["fldPblyNOIryMLFB6"] = date;
  if (statusSafe) fields["fld7kH2SEHsxaS9vz"] = statusSafe;
  if (notes)      fields["fldmz5dOw6In5OkU7"] = notes;

  const data = await mirrorToAirtable("createInspection", () =>
    atFetch(`${encodeURIComponent("Job Inspections")}`,
      { method: "POST", body: JSON.stringify({ fields, typecast: true }) }));

  // Stamp the Airtable id back so handleUpdateInspection can mirror edits to it
  // and the hand-run ETL updates this row instead of inserting a duplicate.
  if (data?.id) {
    await mirrorToAirtable("createInspection.stamp", () =>
      neonWrite("inspection.stampAirtableId",
        `UPDATE job_inspections SET airtable_id = $2 WHERE id = $1`, [neonId, data.id]));
  }

  return resp(200, { ok: true, id: neonId, airtableId: data?.id || null });
}

async function handleJobEstimates(params) {
  const { jobId, onlySaved } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // ── NEON-FIRST (migration Step 4e) ──────────────────────────────────────
  // Replaces three Airtable round trips — job lookup, the FIND-inside-ARRAYJOIN
  // estimate scan, and a full Sent Estimate PDFs fetch — with one query.
  //
  // The snapshot CASCADE moves into SQL unchanged: prefer the Sent PDF that
  // back-links to this estimate, else the most recent same-job PDF whose Total
  // equals the master's Actual Estimate Sent. That fallback is not hypothetical
  // — 5 of 25 sent PDFs carry no back-link, so a plain join would lose them.
  //
  // Matching on actual_estimate_sent, NOT calculated_estimated_total: the former
  // is the user-entered figure, the latter a formula that can drift on rounding.
  // That distinction is inherited from the Airtable path and is deliberate.
  //
  // PDFs come from R2, not Airtable. Airtable serves attachments on signed URLs
  // that expire, so a Neon read returning stored links would break within hours.
  // Copied by the admin action copyEstimatePdfsToR2 — 15 objects, reconciled.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT e.id, e.airtable_id, e.estimate_type, e.status,
              e.estimate_date::text AS estimate_date, e.actual_estimate_sent,
              e.estimated_labor_hours, e.estimated_material_cost,
              e.calculated_estimated_total, e.notes, e.display_number,
              -- db/schema/065. A NULL material_raw_cost is the legacy tell and
              -- the client keys its "pre-split" badge on it, so it is emitted raw
              -- rather than coalesced into something tidy.
              e.material_raw_cost, e.material_markup, e.labor_sell_rate,
              e.labor_burden_rate, e.price_adjustment, e.calculated_selling_price,
              -- db/schema/066. Emitted raw, NOT coalesced to 0: the card has to
              -- render an empty box for "none recorded" rather than a typed $0,
              -- and the save reads a blank as UNCHANGED.
              e.other_costs, e.sales_tax, e.tax_rate_pct,
              COALESCE(back.snapshot, bytotal.snapshot, e.estimate_snapshot, '') AS snapshot,
              j.name AS job_name,
              -- Seeds for the new-estimate form: the markup the job's ACTUAL
              -- material is billed at, and the rate its labor is charged at. Sent
              -- with the estimates so the form has no second round trip.
              j.markup_pct AS job_markup_pct,
              j.billable_hourly_rate AS job_billable_rate,
              -- db/schema/068. What the server WILL stamp on the next estimate
              -- created here. Sent so the New Estimate form's live GP preview
              -- uses the same burden the save will use — a preview computed at
              -- 32.50 against a save that stamps 33.12 is the quiet
              -- disagreement this file keeps having to design against.
              -- Uncorrelated, so Postgres evaluates the view once per request.
              (SELECT burden_rate FROM v_estimating_labor_rate) AS est_burden_rate
         FROM job_estimates e
         LEFT JOIN jobs j ON j.id = e.job_id
         LEFT JOIN LATERAL (
           -- Dual handle (cutover slice 3). The uuid first: a snapshot saved
           -- against a NATIVE estimate has only that, and matching on
           -- airtable_id alone would silently drop the back-link — which shows
           -- up as an estimate whose scope text has gone missing, not as an
           -- error.
           SELECT s.snapshot FROM sent_estimate_pdfs s
            WHERE (s.estimate_id = e.id
                   OR (e.airtable_id IS NOT NULL AND s.estimate_airtable_id = e.airtable_id))
            ORDER BY s.estimate_date DESC NULLS LAST, s.display_number DESC NULLS LAST
            LIMIT 1
         ) back ON true
         LEFT JOIN LATERAL (
           SELECT s.snapshot FROM sent_estimate_pdfs s
            WHERE back.snapshot IS NULL
              AND s.job_airtable_id = e.job_airtable_id
              AND e.actual_estimate_sent IS NOT NULL
              AND s.total = e.actual_estimate_sent
            ORDER BY s.estimate_date DESC NULLS LAST, s.display_number DESC NULLS LAST
            LIMIT 1
         ) bytotal ON true
        WHERE (e.job_airtable_id = $1 OR e.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
        ORDER BY e.estimate_date DESC NULLS LAST`, [jobId]);
    if (q?.rows) {
      // One R2 listing for the whole job rather than one per estimate.
      let pdfsById = new Map();
      try {
        const objs = await listByPrefix("estimates/");
        for (const o of objs) {
          const m = /^estimates\/([^/]+)\/(.+)$/.exec(o.key);
          if (!m) continue;
          if (!pdfsById.has(m[1])) pdfsById.set(m[1], []);
          pdfsById.get(m[1]).push({
            url: await presignGet(o.key), filename: m[2], size: o.size ?? null,
          });
        }
      } catch { /* R2 unavailable — estimates still render, just without links */ }

      const s = (v) => (v === null || v === undefined ? "" : String(v));
      const n = (v) => (v === null || v === undefined ? null : Number(v));
      let estimates = q.rows.map(r => ({
        id: r.airtable_id || r.id,
        // `Estimate Name` is an Airtable formula: {Estimate Type} & " – " & {Job}
        name: [s(r.estimate_type), s(r.job_name)].filter(Boolean).join(" – "),
        estimateType: s(r.estimate_type), status: s(r.status),
        date: s(r.estimate_date), actualEstimate: n(r.actual_estimate_sent),
        laborHours: n(r.estimated_labor_hours),
        // `materialCost` KEEPS ITS WIRE NAME and now carries material SELL, which
        // is what it has always carried on a marked-up quote — renaming it would
        // only move the confusion. `materialRawCost` is the honest cost figure.
        materialCost: n(r.estimated_material_cost),
        materialRawCost: n(r.material_raw_cost), materialMarkup: n(r.material_markup),
        laborSellRate: n(r.labor_sell_rate), laborBurdenRate: n(r.labor_burden_rate),
        priceAdjustment: n(r.price_adjustment),
        otherCosts: n(r.other_costs), salesTax: n(r.sales_tax),
        taxRatePct: n(r.tax_rate_pct),
        calculatedSellingPrice: n(r.calculated_selling_price),
        // The row predates the cost/markup split, so its material figure cannot
        // be read as a cost. The card says so rather than showing a made-up GP.
        legacyMaterialBasis: r.material_raw_cost === null,
        calculatedTotal: n(r.calculated_estimated_total), notes: s(r.notes),
        displayNumber: r.display_number ?? null, snapshot: s(r.snapshot),
        pdfs: pdfsById.get(r.id) || [],
      }));
      if (onlySaved) estimates = estimates.filter(e => e.displayNumber != null);
      const j0 = q.rows[0] || {};
      return resp(200, {
        ok: true, estimates, _source: "neon", _ms: q.ms,
        jobDefaults: {
          markupPct:  n(j0.job_markup_pct),
          sellRate:   n(j0.job_billable_rate),
          // db/schema/068 — the live crew rate, falling back to the constant
          // only if the view has nothing to compute from.
          burdenRate: n(j0.est_burden_rate) ?? EST_LABOR_RATE,
          fallbackSellRate: EST_LABOR_SELL_RATE,
          salesTaxRate: EST_SALES_TAX_RATE,
        },
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`jobEstimates: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, estimates: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";

  // Save Estimate writes the rich snapshot JSON to Sent Estimate PDFs, never
  // back to the master Job Estimates record. Fetch both in parallel and join
  // the matching Sent PDF in below so frontend "+ Add as Line" can read the
  // customer-facing scope text via est.snapshot.
  // Cross-job filter safety — see the note in handleGenerator. Estimates are
  // money, so a leak here misstates a job's expected revenue and its GP.
  const safeName = escapeFormulaString(jobName);
  const [estCandidates, sentPdfRecords] = await Promise.all([
    fetchAll("Job Estimates", {
      filter: `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n")`,
      sortField: "Estimate Date", sortDir: "desc",
    }),
    fetchSentEstimatePDFsForJob(jobId)
  ]);
  const records = estCandidates.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));

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

// ── THE TWO FORMULAS AIRTABLE USED TO COMPUTE (cutover slice 3) ───────────
// A native estimate has no Airtable record, so nothing computes these unless
// the app does. Both were checked against all 89 existing estimates before this
// was written — reproduced exactly, zero mismatches:
//
//   Estimated Labor Cost       = {Estimated Labor Hours} × 32.50
//   Calculated Estimated Total = {Estimated Labor Cost} + {Estimated Material Cost}
//
// ⚠ `estimated_labor_cost` is not decoration. `v_job_rollups` sums it into
// `est_labor_cost_rollup` and `proj_est_labor_cost`, which IS estimated GP — so
// leaving it null on a native estimate would quietly report a job as more
// profitable than it is.
//
// ⚠ A blank hours field evaluates to 0 in an Airtable formula, not to blank, so
// the coercion below is deliberate rather than sloppy: it is what the formula
// did. The derivation lives in SQL and not in JS so that the create path and the
// partial-update path cannot drift apart — an update that changes only material
// cost still has to recompute the total, and it does it from the stored hours.
//
// ⚠⚠ 32.50 IS THE PREVAILING-WAGE CONSTANT. docs/PLAN-prevailing-wage.md is the
// project that changes it, and it needs this to be findable — it is the only
// place the app now decides an estimate's labor cost.
// `hoursExpr` / `materialExpr` are SQL fragments the caller owns — a bind
// parameter on the create path, `COALESCE($n, <column>)` on the update path so a
// field the caller left out keeps its stored value.
// ⚠⚠ THE `0::numeric` IS LOAD-BEARING, and the casts at the call site with it.
// A bare `0` is an INTEGER literal, so `COALESCE($5, 0)` deduces $5 as integer
// while the `estimated_labor_hours` column it also feeds deduces it as numeric —
// and Postgres refuses the whole statement with "inconsistent types deduced for
// parameter $5". A parameter used in two places must resolve to ONE type.
//
// ⚠⚠ THIS SHIPPED BROKEN ON 2026-08-22 AND THE VERIFICATION IS WHY.
// `PREPARE name(text, numeric, …) AS …` DECLARES the parameter types, which
// resolves the ambiguity before Postgres ever has to deduce it — so the check
// passed and production failed on the first click. The driver sends parameters
// UNTYPED. **Verify with `PREPARE name AS …` and no type list**, which is what
// the driver actually does.
const EST_LABOR_RATE = 32.50;

// ── THE COST/SELL SPLIT (db/schema/065) ──────────────────────────────────
// Everything above describes the model as Airtable left it, and one line of it
// was wrong for five years: `Calculated Estimated Total` adds labor COST to the
// material figure, and on a marked-up quote that figure is the SELLING value.
// So the markup was reported as cost, and estimated GP came out low by exactly
// the markup — always in the same direction, which is why nobody caught it.
// Measured on the Seneca quote: 21.9% reported, 27.5% real, $7,000 mislaid.
//
// An estimate now carries five stored facts instead of two:
//
//   material_raw_cost  what the material costs        ┐ NULL on all 90 rows
//   material_markup    what is added to it            ┘ that predate the split
//   labor_sell_rate    what an hour is CHARGED at     ← from the job, else 75.00
//   labor_burden_rate  what an hour COSTS             ← 32.50 unless overridden
//   price_adjustment   a deliberate final nudge
//
// ⚠ A LEGACY ROW IS `material_raw_cost IS NULL`, AND IT MUST KEEP TODAY'S MATH.
// The `matEntered` fallback below is what guarantees that: with no raw cost, the
// cost basis is the figure as typed and the markup is zero, which reproduces the
// old arithmetic exactly. Verified after the migration — all 117 jobs, zero
// movement in `est_material_cost_rollup`. The old numbers cannot be re-derived
// (implied labor sell across the last 30 estimates ranges $8.33–$200/hr, so
// there is no formula to invert) and this is why they are not touched.
//
// ⚠⚠ 32.50 IS STILL THE PREVAILING-WAGE CONSTANT, now as the `burden` FALLBACK
// rather than a hardcode. docs/PLAN-prevailing-wage.md §2 names this exact hole
// — "the system has one answer to what an hour costs, and it is company-wide".
// `labor_burden_rate` is the column its per-job resolver lands in: that project
// changes what fills the parameter, not this arithmetic.
const EST_LABOR_SELL_RATE = 75.00;

// db/schema/066. The rate the "fill 8%" control on the estimate card uses, over
// a base of (material cost + other costs) — labor is not taxable and a
// subcontractor pays tax on their own purchases, so quotes are not taxed here.
//
// ⚠ THIS IS A CONVENIENCE DEFAULT, NOT A TAX ENGINE. It fills a box the
// estimator can overwrite or clear; `sales_tax` stores DOLLARS, so a job that is
// exempt, on resale, or in a different county is handled by typing the right
// number rather than by teaching this constant about jurisdictions.
const EST_SALES_TAX_RATE = 0.08;

// ONE definition of the four derived figures, called by BOTH write paths for
// the reason the original note gives: an update that changes only the markup
// still has to recompute the price, and it does it from the stored hours.
// Each argument is a SQL fragment the caller owns — a cast bind parameter on the
// create path, `COALESCE($n, <column>)` on the update path.
// ⚠ `other` and `tax` HAVE NO DEFAULT, DELIBERATELY (db/schema/066). A caller
// that forgets them interpolates the string "undefined" into the SQL and
// Postgres refuses the statement — loud, on the first click. Defaulting them to
// NULL would instead save an estimate with its bought-in costs silently zeroed,
// which understates direct cost and OVERSTATES GP. That is the failure this
// whole file exists to stop; a 502 is the better outcome.
function estDerived({ hours, matRaw, matMarkup, matEntered, sellRate, burden, other, tax }) {
  const h    = `COALESCE(${hours}, 0::numeric)`;
  const b    = `COALESCE(${burden}, ${EST_LABOR_RATE})`;
  const sr   = `COALESCE(${sellRate}, ${EST_LABOR_SELL_RATE})`;
  // NULL + anything is NULL, so a row with no raw cost falls straight through to
  // the figure as entered. That propagation IS the legacy behaviour — do not
  // "fix" it with a COALESCE around matRaw.
  const cost = `COALESCE(${matRaw}, ${matEntered}, 0::numeric)`;
  const sell = `COALESCE(${matRaw} + COALESCE(${matMarkup}, 0::numeric), ${matEntered}, 0::numeric)`;
  // Bought-in direct cost. Two columns on the row, ONE term here — they are
  // separate so the tax can be filled and checked on its own, but they are the
  // same kind of money and land in the same place in the arithmetic.
  //
  // ⚠ These COALESCE to 0, unlike matRaw above, and the difference is the
  // point: a NULL raw cost means "this row predates the split, read the entered
  // figure instead", so it must propagate. A NULL other_costs just means the
  // job had none. There is nothing to fall back TO.
  const oth  = `(COALESCE(${other}, 0::numeric) + COALESCE(${tax}, 0::numeric))`;
  return {
    laborCost:    `round(${h} * ${b}, 2)`,
    materialSell: `round(${sell}, 2)`,
    // `calculated_estimated_total` keeps its name and becomes honest: it is the
    // estimated DIRECT COST and always was. The UI label changes with it.
    directCost:   `round(${h} * ${b} + ${cost} + ${oth}, 2)`,
    // ⚠ Other costs enter the price at COST, with no markup on top, and that is
    // not an oversight. This column is a sanity check against the price the
    // estimator actually typed, and on a bid-program quote it reconstructs that
    // program's own pre-overhead subtotal exactly — which is what makes the gap
    // to `actual_estimate_sent` readable as the margin added on top. Marking it
    // up here would invent a number no report contains.
    sellingPrice: `round(${sell} + ${h} * ${sr} + ${oth}, 2)`,
  };
}

// ── KEEP NEON IN STEP AFTER AN ESTIMATE WRITE (migration Step 4e) ─────────
// handleJobEstimates reads Neon first and only falls through on ZERO rows, so on
// any job that already has an estimate an Airtable-only write would simply never
// appear. Same trap as the warranties at 83e022c and the expenses at 6ee42b5.
//
// ⚠ AS OF SLICE 3 THIS IS THE MIRROR PATH, NOT THE WRITE PATH. It is called
// after an Airtable PATCH succeeds on an estimate that HAS a rec id, to carry
// Airtable's computed fields back. Creates no longer come through here — see
// handleCreateJobEstimate, which writes Neon first. Left in place because a
// mirrored estimate edited through Airtable still needs its values carried, and
// because ON CONFLICT (airtable_id) is a no-op for native rows (NULL never
// conflicts), so it cannot damage one.
async function syncEstimateToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const n = (v) => { if (Array.isArray(v)) v = v[0]; const x = Number(v); return Number.isFinite(x) ? x : null; };
  const s = (v) => { const x = Array.isArray(v) ? v[0] : v; return (x === undefined || x === "" || x === null) ? null : String(x); };
  const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : s(v));
  await neonWrite("estimate.sync",
    `INSERT INTO job_estimates
       (airtable_id, job_airtable_id, job_id, estimate_type, status, actual_estimate_sent,
        estimated_labor_hours, estimated_labor_cost, estimated_material_cost,
        calculated_estimated_total, estimate_date, notes, display_number,
        estimate_snapshot, synced_at)
     VALUES ($1,$2,(SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),$3,$4,$5,$6,$7,$8,$9,$10::date,$11,$12,$13, now())
     -- ⚠⚠ THE JOB LINKAGE IS NOT CARRIED BACK (2026-08-25). On the DO UPDATE
     -- branch the app has ALREADY written the authoritative row, so Airtable is
     -- the junior opinion — and on a native job it is a wrong one. The mirror
     -- links to whatever Airtable holds, which for a native job was a record
     -- "typecast: true" fabricated out of the uuid, so this SET replaced a
     -- correct job_id with NULL and the estimate silently left the job it belonged
     -- to. Three of them did exactly that on Test 10 before this was found.
     -- The INSERT branch still sets both: an Airtable-born row has no Neon row
     -- to be junior to. Only the OVERWRITE is gone.
     -- ⚠⚠ AND NEITHER IS THE MONEY, ONCE THE APP HAS SPLIT IT (db/schema/065).
     -- Airtable has no column for raw cost vs markup and never will, so its
     -- "Estimated Material Cost" is the SELL figure and its "Calculated
     -- Estimated Total" is the old formula that adds labor cost to it. Letting
     -- those overwrite a split row leaves the worst possible state: the raw cost
     -- and markup survive while the two figures derived from them are reset to
     -- pre-split values, so the same estimate holds two contradictory accounts
     -- of the same money and neither the card nor the rollup errors.
     -- It cannot fire today (AIRTABLE_WRITES=off makes the mirror return a null
     -- id, so this function is never reached) — which is exactly why the guard
     -- goes in NOW, while the reason is in front of us, rather than being
     -- rediscovered by whoever turns that switch back on.
     -- The tell is per-row, not global: a pre-split estimate has nothing to
     -- protect and still syncs, so nothing about historical rows changes.
     ON CONFLICT (airtable_id) DO UPDATE SET
       estimate_type=EXCLUDED.estimate_type, status=EXCLUDED.status,
       actual_estimate_sent=EXCLUDED.actual_estimate_sent,
       estimated_labor_hours=EXCLUDED.estimated_labor_hours,
       estimated_labor_cost = CASE WHEN job_estimates.material_raw_cost IS NULL AND job_estimates.labor_burden_rate IS NULL
                                   THEN EXCLUDED.estimated_labor_cost ELSE job_estimates.estimated_labor_cost END,
       estimated_material_cost = CASE WHEN job_estimates.material_raw_cost IS NULL AND job_estimates.labor_burden_rate IS NULL
                                   THEN EXCLUDED.estimated_material_cost ELSE job_estimates.estimated_material_cost END,
       calculated_estimated_total = CASE WHEN job_estimates.material_raw_cost IS NULL AND job_estimates.labor_burden_rate IS NULL
                                   THEN EXCLUDED.calculated_estimated_total ELSE job_estimates.calculated_estimated_total END,
       estimate_date=EXCLUDED.estimate_date, notes=EXCLUDED.notes,
       display_number=EXCLUDED.display_number, estimate_snapshot=EXCLUDED.estimate_snapshot,
       synced_at=now()`,
    [rec.id, s(f["Job"]), sel(f["Estimate Type"]), sel(f["Status"]),
     n(f["Actual Estimate Sent"]), n(f["Estimated Labor Hours"]),
     n(f["Estimated Labor Cost"]), n(f["Estimated Material Cost"]),
     n(f["Calculated Estimated Total"]), s(f["Estimate Date"]), s(f["Notes"]),
     n(f["Estimate Display #"]), s(f["Estimate Snapshot"])]).catch(() => {});
}

// Neon-first, dual handle, mirror best-effort (cutover slice 3).
//
// ⚠ The derived columns are recomputed IN THE SAME STATEMENT, from
// `COALESCE($n, <stored column>)`. Editing only the material cost still changes
// the total, and the hours it needs are the ones already in the row. Getting
// this wrong is invisible: the estimate saves, and its GP is quietly stale.
async function handleUpdateEstimate(body) {
  const { estimateId, actualEstimate, laborHours, materialCost,
          materialRawCost, materialMarkup, laborSellRate, laborBurdenRate,
          priceAdjustment, otherCosts, salesTax, taxRatePct } = body || {};
  if (!estimateId) return resp(400, { ok: false, error: "Missing estimateId." });

  const num = (v) => (v === undefined || v === null || v === "" ? null : Number(v));
  const est = num(actualEstimate), hrs = num(laborHours), mat = num(materialCost);
  const raw = num(materialRawCost),  mk  = num(materialMarkup);
  const sr  = num(laborSellRate),    br  = num(laborBurdenRate);
  const adj = num(priceAdjustment);
  const oth = num(otherCosts),       tax = num(salesTax);
  const trp = num(taxRatePct);
  // ⚠⚠ NaN IS NOT CAUGHT BY ANYTHING BELOW, AND POSTGRES ACCEPTS IT.
  // `Number("311,303.98")` is NaN — one thousands separator surviving the
  // client is all it takes — and `numeric` stores 'NaN' quite happily. From
  // there every SUM touching the row is NaN: the job's expected revenue, its
  // direct cost, its GP, and the rollups of every OTHER estimate on it. Nothing
  // throws and the screen reads "NaN%".
  // The null checks above cannot see it (NaN !== null) and COALESCE cannot
  // either (NaN is not NULL). Refuse here, before the write.
  if ([est, hrs, mat, raw, mk, sr, br, adj, oth, tax, trp].some(v => v !== null && !Number.isFinite(v))) {
    return resp(400, { ok: false, error: "One of those figures isn't a number. Check for stray characters and try again." });
  }
  if ([est, hrs, mat, raw, mk, sr, br, adj, oth, tax, trp].every(v => v === null)) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }
  // db/schema/067. Turned into a sentence here rather than left to the CHECK,
  // because this one has a units cause the user can act on: the box takes a
  // PERCENT and stores a fraction, so 725 arrives as 7.25 — a 725% rate.
  if (trp !== null && (!(trp >= 0) || trp > 1)) {
    return resp(400, { ok: false, error: "That tax rate isn't between 0% and 100% — enter it as a percentage, e.g. 7.25." });
  }
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't update the estimate right now — the database is unavailable. Try again in a moment." });
  }

  // Every fragment reads the OLD row when the caller left the field out, which
  // is what makes this a partial update. Postgres evaluates every SET expression
  // against the pre-update row, so the repetition below is safe — none of these
  // can see another's new value.
  //
  // ⚠ The sell-rate fallback reaches the JOB, not a constant, because the rate
  // is already per-job (`labor_billable_rates`, picked on the job) and only 36
  // of 117 jobs are on 75.00. Defaulting everything to 75 would misprice a third
  // of the book. The rate is only STAMPED when the caller sends one — inventing
  // a record of what an old quote charged is not this handler's business.
  const E = {
    hours:      "COALESCE($3, estimated_labor_hours)",
    matRaw:     "COALESCE($5, material_raw_cost)",
    matMarkup:  "COALESCE($6, material_markup)",
    matEntered: "COALESCE($4, estimated_material_cost)",
    sellRate:   "COALESCE($7, labor_sell_rate, (SELECT j.billable_hourly_rate FROM jobs j WHERE j.id = job_estimates.job_id))",
    burden:     "COALESCE($8, labor_burden_rate)",
    // db/schema/066. Same partial-update contract as every fragment above: an
    // empty box means UNCHANGED, not zero. Clearing the Other Costs field on an
    // estimate that has $126,850 in it leaves the $126,850 alone — the client's
    // estCardValues reads a blank the same way, so the live GP on screen and the
    // GP the save produces cannot disagree.
    other:      "COALESCE($10, other_costs)",
    tax:        "COALESCE($11, sales_tax)",
  };
  const d = estDerived(E);
  // ⚠ THE DOUBLE-COUNT GUARD IS THE DATABASE'S, NOT A PRE-CHECK HERE, and that
  // is a correction rather than laziness. The first cut refused any request
  // carrying a markup without a raw cost IN THE BODY — which broke the most
  // ordinary edit there is: changing only the markup on an estimate that
  // already has a raw cost stored. The condition is about the ROW, and only the
  // row's own UPDATE knows it. Caught by the live branch run, not by reasoning.
  let rows;
  try {
    rows = await neonWrite("estimate.update",
    `UPDATE job_estimates SET
       actual_estimate_sent       = COALESCE($2, actual_estimate_sent),
       estimated_labor_hours      = ${E.hours},
       material_raw_cost          = ${E.matRaw},
       material_markup            = ${E.matMarkup},
       labor_sell_rate            = COALESCE($7, labor_sell_rate),
       labor_burden_rate          = COALESCE($8, labor_burden_rate),
       price_adjustment           = COALESCE($9, price_adjustment),
       other_costs                = ${E.other},
       sales_tax                  = ${E.tax},
       -- db/schema/067. A RECORD of the rate used, never an input: nothing
       -- above derives sales_tax from it, so an estimator's hand-typed tax on a
       -- part-exempt job survives every later save.
       tax_rate_pct               = COALESCE($12, tax_rate_pct),
       estimated_material_cost    = ${d.materialSell},
       estimated_labor_cost       = ${d.laborCost},
       calculated_estimated_total = ${d.directCost},
       calculated_selling_price   = ${d.sellingPrice},
       synced_at                  = now()
      WHERE airtable_id = $1 OR id::text = $1
      RETURNING COALESCE(airtable_id, id::text) AS handle, airtable_id,
                estimated_material_cost`,
    [String(estimateId), est, hrs, mat, raw, mk, sr, br, adj, oth, tax, trp]);
  } catch (e) {
    // The one constraint a user can actually trip. Anything else is a fault.
    if (/job_estimates_markup_needs_raw/.test(String(e?.message || e))) {
      return resp(400, { ok: false, error: "Enter the raw material cost before adding a markup — otherwise the markup is counted twice." });
    }
    console.error(`updateEstimate: ${e?.message || e}`);
    return resp(502, { ok: false, error: "Couldn't save that estimate. Please try again." });
  }
  if (!rows?.length) return resp(404, { ok: false, error: "That estimate no longer exists." });

  const recId = rows[0].airtable_id;
  if (recId) {
    const fields = {};
    if (est !== null) fields["fldJTAPtFpXH2vRwF"] = est;
    if (hrs !== null) fields["fldH7bJSZikzOYxkm"] = hrs;
    // The mirror gets the material SELL value — the same thing this field has
    // always held over there. Airtable has no column for the split and is never
    // gaining one (AIRTABLE_WRITES=off makes this a no-op today anyway).
    if (mat !== null || raw !== null || mk !== null) {
      fields["fldDEUGzVrfA56aBq"] = Number(rows[0].estimated_material_cost);
    }
    await mirrorToAirtable("updateEstimate", () =>
      atFetch(`${encodeURIComponent("Job Estimates")}/${recId}`, {
        method: "PATCH", body: JSON.stringify({ fields }),
      }));
  }
  return resp(200, { ok: true, updatedId: rows[0].handle });
}

// Neon-first, dual handle, mirror best-effort (cutover slice 3).
//
// ⚠ Status is not cosmetic: `v_job_rollups` counts only Sent / Approved /
// Archived-Completed estimates into expected revenue, so this write moves a
// job's revenue figure. It has to land in the store the app reads.
async function handleUpdateEstimateStatus(body) {
  const { estimateId, status } = body || {};
  if (!estimateId || !status) return resp(400, { ok: false, error: "Missing estimateId or status." });
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't update the estimate right now — the database is unavailable. Try again in a moment." });
  }

  const rows = await neonWrite("estimate.updateStatus",
    `UPDATE job_estimates SET status = $2, synced_at = now()
      WHERE airtable_id = $1 OR id::text = $1
      RETURNING COALESCE(airtable_id, id::text) AS handle, airtable_id`,
    [String(estimateId), String(status)]);
  if (!rows?.length) return resp(404, { ok: false, error: "That estimate no longer exists." });

  const recId = rows[0].airtable_id;
  if (!recId) return resp(200, { ok: true, updatedId: rows[0].handle });

  // Job Estimates — Status field ID = fld9GsGvxaNPuCnjo (singleSelect)
  const fields = { "fld9GsGvxaNPuCnjo": status };
  const data = await mirrorToAirtable("updateEstimateStatus", () =>
    atFetch(`${encodeURIComponent("Job Estimates")}/${recId}`, {
      method: "PATCH",
      body: JSON.stringify({ fields, typecast: true })
    }));
  if (!data?.id) return resp(200, { ok: true, updatedId: rows[0].handle });
  await syncEstimateToNeon(data);
  return resp(200, { ok: true, updatedId: data.id });
}

// ── GET NEXT ESTIMATE NUMBER ─────────────────────────────────────────────
// Queries the "Sent Estimate PDFs" table (not Job Estimates) so snapshot-only
// records don't commingle with the source-of-truth Job Estimates.
async function handleGetNextEstimateNumber() {
  const START_AT = 2187;

  // ── NEON-FIRST ────────────────────────────────────────────────────────────
  // Replaces paging the whole "Sent Estimate PDFs" table on every press of the
  // button with one MAX(). Safe because `saveEstimate` writes this table in the
  // SAME request as Airtable, so Neon can never be a number behind — this does
  // NOT ride on the hourly sync, which is the thing that would have made it
  // hand out a duplicate.
  //
  // ⚠⚠ THE ORDER OF THIS MIGRATION MATTERS. Verified before flipping: Neon holds
  // 26 rows, max 2214, none missing a number. The day estimates go Neon-NATIVE
  // and stop being minted in Airtable, an Airtable-based scan would freeze at
  // its last mirrored number and re-issue it — exactly how allocations broke
  // when time entries stopped getting an Airtable twin.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(MAX(display_number), 0)::int AS max_no FROM sent_estimate_pdfs`);
    if (q?.rows?.length) {
      const maxNo = Number(q.rows[0].max_no) || 0;
      return resp(200, { ok: true, nextNumber: Math.max(maxNo + 1, START_AT),
                         _source: "neon", _ms: q.ms });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): a number from FROZEN Airtable would be lower than Neon's and COLLIDE with one already issued.
    console.error(`getNextEstimateNumber: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
  Object.assign(fields, jobLink("Job", jobId));
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
  //
  // ⚠ The Airtable LINK still takes rec ids only — a uuid there 422s the write,
  // the same constraint that keeps companies mirrored in slice 1. The back-link
  // Neon stores is resolved separately below and accepts either shape, so a
  // native estimate keeps its provenance even though Airtable cannot hold it.
  const backLinks = Array.isArray(jobEstimateIds)
    ? jobEstimateIds.filter(id => typeof id === "string" && id.trim())
    : [];
  {
    const cleaned = backLinks.filter(id => id.startsWith("rec"));
    if (cleaned.length) fields["fldPoz43rrlqWRnwC"] = cleaned;
  }
  // Note: "notes" from the caller is embedded in the Snapshot JSON; no separate column.

  // ── NEON FIRST (identity cutover slice 3, db/schema/055) ─────────────────
  // This table is the snapshot of what actually went to the customer, and
  // `handleJobEstimates`' snapshot cascade reads it from Neon. It used to be
  // written to Airtable first purely out of habit — `sent_estimate_pdfs`
  // has never had a NOT NULL on `airtable_id` — which made an Airtable outage
  // lose the scope text of a quote that had already been sent.
  //
  // ⚠ FAILS CLOSED without a database, like every other create in this cutover.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't save the estimate right now — the database is unavailable. Try again in a moment." });
  }

  const numOrNull = (v) => (v === undefined || v === null || v === "" || isNaN(Number(v)) ? null : Number(v));
  const totalNum   = numOrNull(totalAmount);
  const displayNum = numOrNull(estimateNumber);
  const snapshotText = snapshot ? (typeof snapshot === "string" ? snapshot : JSON.stringify(snapshot)) : null;

  // The Neon back-link accepts either handle shape: resolve the FIRST estimate
  // the builder was seeded from to its row, and store both of its ids.
  let estRecId = null, estNeonId = null;
  if (backLinks.length) {
    const er = await neonQuery(
      `SELECT id, airtable_id FROM job_estimates
        WHERE airtable_id = $1 OR id::text = $1 LIMIT 1`, [backLinks[0]]);
    if (er?.rows?.length) { estNeonId = er.rows[0].id; estRecId = er.rows[0].airtable_id; }
  }

  try {
    let row;
    if (estimateId) {
      const rows = await neonWrite("sentEstimatePdf.update",
        `UPDATE sent_estimate_pdfs SET
           job_airtable_id = $2, job_id = (SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),
           estimate_airtable_id = COALESCE($3, estimate_airtable_id),
           estimate_id          = COALESCE($4, estimate_id),
           display_number = COALESCE($5, display_number),
           estimate_date  = COALESCE($6::date, estimate_date),
           total          = COALESCE($7, total),
           snapshot       = COALESCE($8, snapshot),
           synced_at      = now()
          WHERE airtable_id = $1 OR id::text = $1
          RETURNING id, airtable_id`,
        [String(estimateId), String(jobId), estRecId, estNeonId, displayNum,
         estimateDate ? String(estimateDate).slice(0, 10) : null, totalNum, snapshotText]);
      if (!rows?.length) return resp(404, { ok: false, error: "That estimate no longer exists." });
      row = rows[0];
    } else {
      const rows = await neonWrite("sentEstimatePdf.create",
        `INSERT INTO sent_estimate_pdfs
           (job_airtable_id, job_id, estimate_airtable_id, estimate_id,
            display_number, estimate_date, total, snapshot, synced_at)
         VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1), $2, $3,
                 $4, $5::date, $6, $7, now())
         RETURNING id, airtable_id`,
        [String(jobId), estRecId, estNeonId, displayNum,
         estimateDate ? String(estimateDate).slice(0, 10) : null, totalNum, snapshotText]);
      row = rows?.[0];
      if (!row) return resp(502, { ok: false, error: "Couldn't save the estimate. Please try again." });
    }

    // The mirror. On an edit it can only run when the row already had a rec id;
    // a native snapshot has no Airtable record to PATCH and never acquires one.
    const recId = row.airtable_id;
    let data = null;
    if (estimateId && recId) {
      data = await mirrorToAirtable("saveEstimate.update", () =>
        atFetch(`${encodeURIComponent("Sent Estimate PDFs")}/${recId}`, {
          method: "PATCH",
          body: JSON.stringify({ fields, typecast: true })
        }));
    } else if (!estimateId) {
      data = await mirrorToAirtable("saveEstimate.create", () =>
        atFetch(`${encodeURIComponent("Sent Estimate PDFs")}`, {
          method: "POST",
          body: JSON.stringify({ fields, typecast: true })
        }));
      if (data?.id) {
        await neonWrite("sentEstimatePdf.stampAirtableId",
          `UPDATE sent_estimate_pdfs SET airtable_id = $2, synced_at = now() WHERE id = $1`,
          [row.id, data.id]).catch((e) =>
            console.error(`saveEstimate: rec id not stamped, snapshot is Neon-only — ${e?.message || e}`));
      }
    }

    return resp(200, { ok: true, id: data?.id || recId || String(row.id),
                       updated: !!estimateId, _airtableMirrored: !!(data?.id || recId) });
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
// Builds the friendly label from the snapshot JSON. Shared so the Neon and
// Airtable paths cannot drift into naming the same estimate differently.
function sentEstimateName(snapshot, displayNumber) {
  const num = displayNumber ? `#${displayNumber} — ` : "";
  try {
    const s = JSON.parse(snapshot || "{}");
    const first = (s.lines || [])[0]?.description || "";
    const head  = first ? first.split(/\r?\n/)[0].trim().slice(0, 80) : (s.jobName || "");
    return `${num}${head}`.trim() || "Estimate";
  } catch { return `#${displayNumber || ""}`.trim() || "Estimate"; }
}

async function handleSentEstimatePDFs(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // ── NEON-FIRST (migration Step 4e) ──────────────────────────────────────
  // The Airtable path below pages the ENTIRE table and filters in memory,
  // because filterByFormula on a linked-record field is unreliable. Here it is
  // a WHERE clause on an indexed column — the whole reason job_airtable_id is
  // stored alongside the FK.
  //
  // Writes already sync (saveEstimate → sent_estimate_pdfs at 08438f3), so this
  // read flip is complete on its own and cannot go stale behind a save.
  if (neonEnabled()) {
    const q = await neonQuery(
      // Dual handle (cutover slice 3): a snapshot saved since the reversal has
      // no rec id, and emitting a bare `airtable_id` handed the client a NULL
      // id — the row rendered, and every button on it did nothing.
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id,
              display_number, estimate_date::text AS estimate_date,
              total, snapshot
         FROM sent_estimate_pdfs
        WHERE (job_airtable_id = $1 OR job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
        ORDER BY estimate_date DESC NULLS LAST, display_number DESC NULLS LAST`, [jobId]);
    if (q?.rows) {
      return resp(200, {
        ok: true,
        estimates: q.rows.map(r => {
          const total = Number(r.total ?? 0);
          return {
            id: r.airtable_id, displayNumber: r.display_number ?? null,
            date: r.estimate_date || "", total,
            snapshot: r.snapshot || "",
            name: sentEstimateName(r.snapshot, r.display_number),
            // Constant on this path, as on the Airtable one: a row in this table
            // exists because an estimate was sent.
            status: "Sent",
            actualEstimate: total, calculatedTotal: total,
          };
        }),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`sentEstimatePDFs: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

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
      // Same helper the Neon path uses — one definition, so the two cannot name
      // the same estimate differently.
      name:           sentEstimateName(f["Snapshot"], f["Estimate Display #"]),
      // Status is implicit for saved PDFs; surface "Sent" for the history UI
      status:         "Sent",
      actualEstimate: Number(f["Total"] || 0),
      calculatedTotal:Number(f["Total"] || 0)
    };
  });
  return resp(200, { ok: true, estimates });
}

// ── ESTIMATE TEMPLATES ───────────────────────────────────────────────────
// NEON-NATIVE as of 2026-08-20 (db/schema/047). Backs the "Load Template"
// dropdown in the New Job Estimate modal AND the template manager behind it.
//
// Templates were previously typed into Airtable by hand and carried across
// hourly by `_billing-sync.js`. With nobody opening Airtable any more, a base
// price could not be corrected without leaving the app — so the write path
// below is the point of the whole exercise, not a nicety.
//
// ⚠⚠ THE VALUES ARE SNAPSHOTTED, NOT LINKED. `handleCreateJobEstimate` copies
// the four numbers and the composed notes into the Job Estimate at create time.
// Editing a template NEVER changes an estimate that already exists, and that is
// deliberate — an estimate is what was quoted, not what the template says today.
//
// ⚠ NO AIRTABLE FALLBACK, unlike most reads in this file. Airtable's copy stops
// being written the moment the templates block leaves `_billing-sync.js`, so a
// fallback would serve a frozen base price into a live customer quote — wrong
// money, silently, which is exactly the trade `handleEstimateTemplatesList` in
// inventory.js already refused. Failing closed costs a blank estimate; falling
// back costs a wrong number nobody catches. Same reasoning, same 503.

// One projection, used by both reads, so the picker and the manager can never
// disagree about what a template is.
//
// ⚠ THE DUAL HANDLE IS LOAD-BEARING (the rule enforced by db/schema/043). A
// natively-created template has `airtable_id IS NULL`; serving that as the id
// renders `<option value="">`, which COLLIDES with the "— Blank estimate —"
// option, so picking your own brand-new template would silently do nothing.
// Every write below resolves on `airtable_id = $1 OR id::text = $1` to match.
//
// contractor_name comes from a live JOIN on companies, not the stored copy:
// once the ETL is gone that copy freezes, and renaming a contractor in Companies
// would orphan its templates from a picker that matches on the NAME (the
// frontend passes `job.contractor` as a name, not an id). The stored value is
// kept only as a COALESCE fallback for a template whose company has since been
// deleted — a stale string beats vanishing from the picker with no explanation.
const ET_SELECT = `
  SELECT COALESCE(t.airtable_id, t.id::text)       AS handle,
         t.template_name, t.contractor_airtable_id,
         COALESCE(c.name, t.contractor_name)       AS contractor_name,
         t.active, t.scope_of_work, t.exclusions, t.standard_terms,
         t.base_price, t.default_labor_hours, t.default_material_cost,
         t.default_material_markup, t.default_labor_sell_rate,
         t.internal_notes, t.updated_at, t.updated_by
    FROM estimate_templates t
    LEFT JOIN companies c ON c.airtable_id = t.contractor_airtable_id`;

// Who to stamp into `updated_by`.
//
// ⚠ THE TOKEN DOES NOT CARRY A NAME. `signToken` puts only { id, role, iat, exp }
// in the payload, so `authUser.name` is always undefined — reading it wrote the
// Airtable rec id into the audit column instead, which is useless to a human
// reading the manager. Found on the first live edit, 2026-08-20.
//
// The id IS the Airtable employee rec id (login deliberately still returns that
// — see the note on clockEmployee), so one indexed lookup resolves it. Falls
// back to the id rather than NULL: a rec id is at least traceable, and this must
// never be the reason a save fails.
async function actorName(authUser) {
  const id = authUser?.id ? String(authUser.id) : null;
  if (!id) return null;
  const q = await neonQuery(`SELECT name FROM employees WHERE airtable_id = $1 OR id::text = $1`, [id]);
  return (q?.rows?.[0]?.name || id).slice(0, 80);
}

function mapTemplateRow(r) {
  const s = (v) => (v === null || v === undefined ? "" : String(v));
  const n = (v) => (v === null || v === undefined ? null : Number(v));
  return {
    id:                  r.handle,          // rec id for Airtable-era rows, uuid for native
    name:                s(r.template_name),
    contractorId:        r.contractor_airtable_id || null,
    contractorName:      s(r.contractor_name),
    active:              r.active === true,
    scopeOfWork:         s(r.scope_of_work),
    exclusions:          s(r.exclusions),
    standardTerms:       s(r.standard_terms),
    basePrice:           n(r.base_price),
    defaultLaborHours:   n(r.default_labor_hours),
    // db/schema/065: this now seeds the RAW cost box, not the marked-up figure.
    defaultMaterialCost:   n(r.default_material_cost),
    defaultMaterialMarkup: n(r.default_material_markup),
    // NULL = "use the job's billable rate", which is what nearly every template
    // should do — only 36 of 117 jobs are on 75.00, so a template that pins a
    // rate is pinning it for jobs that do not charge it.
    defaultLaborSellRate:  n(r.default_labor_sell_rate),
    internalNotes:       s(r.internal_notes),
    updatedAt:           r.updated_at ? String(r.updated_at) : null,
    updatedBy:           s(r.updated_by),
  };
}

// The picker read: ACTIVE templates only, for one job's contractor.
//
// ⚠ A BLANK CONTRACTOR ON THE TEMPLATE NOW MEANS "EVERY JOB" (owner's call,
// 2026-08-20). It previously meant "only jobs that also have no contractor",
// which made a genuinely generic template impossible to build — "Commercial Bid
// — General" had to be pinned to Classical Construction just to be reachable.
// General templates sort BELOW the job's own, so the contractor-specific ones
// stay at the top of the dropdown where they were.
//
// The `Case Farms` / `Case Farms North` substring collision that the old
// Airtable FIND had to defend against cannot occur here at all: this is an
// equality on a resolved name, not a FIND over a joined link field.
async function handleEstimateTemplates(params) {
  // `all=1` widens the list to every active template regardless of contractor,
  // backing the "Show all contractors" toggle under the dropdown. It is a
  // deliberate opt-in rather than the default: Standard Terms are written per
  // contractor, so loading another contractor's template composes THEIR terms
  // into a quote that is about to go out under someone else's name. The filter
  // is a guard rail, and this is the gate in it — not its removal.
  const all  = String(params?.all || "") === "1";
  const want = all ? "" : String(params?.contractor || "").trim();
  const q = await neonQuery(
    `${ET_SELECT}
      WHERE t.active
        AND ($1 = '' OR t.contractor_airtable_id IS NULL
             OR lower(COALESCE(c.name, t.contractor_name, '')) = lower($1))
      ORDER BY (t.contractor_airtable_id IS NULL), t.sort_order NULLS LAST,
               t.template_name ASC`, [want]);

  // `rows` empty is a legitimate answer here (a contractor with no templates),
  // so the fail-closed test is on the ERROR, not on the row count.
  if (!q || q.error) {
    console.error(`estimateTemplates: Neon read failed — ${q?.error || "not configured"}`);
    return resp(503, { ok: false, error: "Templates are unavailable right now. Please try again." });
  }
  return resp(200, {
    ok: true, _source: "neon", _ms: q.ms,
    templates: q.rows.map(mapTemplateRow),
  });
}

// The manager read: EVERY template including archived ones, so an archived
// template can be found again and restored. Archived rows sort last.
async function handleEstimateTemplatesAll() {
  const q = await neonQuery(
    `${ET_SELECT}
      ORDER BY t.active DESC, (t.contractor_airtable_id IS NULL),
               COALESCE(c.name, t.contractor_name, ''), t.template_name`);
  if (!q || q.error) {
    console.error(`estimateTemplatesAll: Neon read failed — ${q?.error || "not configured"}`);
    return resp(503, { ok: false, error: "Templates are unavailable right now. Please try again." });
  }
  return resp(200, { ok: true, _source: "neon", _ms: q.ms, templates: q.rows.map(mapTemplateRow) });
}

// ── SAVE A TEMPLATE (create + update) ────────────────────────────────────
// One handler for both, keyed on whether a handle came in, because the field
// list and every validation rule are identical — splitting them is how the two
// halves drift until only one of them whitelists something.
//
// NEON-ONLY. Nothing is written to Airtable: the Estimate Templates table there
// becomes frozen history at the ETL cutover, and writing to it would just
// re-create the clobber problem from the other direction.
async function handleEstimateTemplateSave(body, authUser) {
  const b = body || {};
  const handle = String(b.templateId || "").trim();
  const name   = String(b.name || "").trim();
  if (!name) return resp(400, { ok: false, error: "Template name is required." });

  // Money fields: "" and undefined both mean "not set" and must land as NULL,
  // NOT as 0. A template with base_price 0 reads as a free job in the picker,
  // where a NULL correctly leaves the field blank for the estimator to fill in.
  // Number("") === 0 is the trap this exists to avoid.
  const money = (v) => {
    if (v === undefined || v === null || String(v).trim() === "") return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };
  const text = (v) => {
    const t = String(v ?? "").trim();
    return t === "" ? null : t;
  };

  // Contractor: the client sends the Airtable rec id from `listContractors`
  // (companies.airtable_id is the id currency for companies — see
  // handleCreateCompany, where jobs link to it by rec id). Blank = a general
  // template that shows on every job.
  //
  // The NAME is resolved here and stored alongside, even though the reads
  // prefer the live join. It is the fallback that keeps a template identifiable
  // if its company is later deleted, and it costs one indexed lookup on a
  // table of 35 rows.
  const contractorId = String(b.contractorId || "").trim() || null;
  let contractorName = null;
  if (contractorId) {
    const c = await neonQuery(`SELECT name FROM companies WHERE airtable_id = $1 OR id::text = $1`, [contractorId]);
    // ⚠ `!c` (DATABASE_URL unset) and `c.error` must be separated from an empty
    // result. Folding them together reports "that contractor no longer exists"
    // for what is actually a deploy fault, and sends the user hunting through
    // Companies for a row that is sitting right there.
    if (!c || c.error) return resp(502, { ok: false, error: "Couldn't look up that contractor. Please try again." });
    if (!c.rows.length) return resp(400, { ok: false, error: "That contractor no longer exists." });
    contractorName = c.rows[0].name || null;
  }

  // Duplicate-name guard, case-insensitive, returning the existing id so the
  // client can offer to open that one instead — the `handleCreateCompany` /
  // `handleCreateVendor` shape, which apiPost already smuggles out as
  // err.existingId. A WARNING, not a constraint: "Case Farms — 2 Barn Setup"
  // and "— 3 Barn Setup" are legitimately separate rows, and a 2026 vs 2027
  // version of one name is a judgement call that belongs to the user.
  // Excludes the row being edited, or renaming nothing would 409 against itself.
  const dupe = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS handle, template_name
       FROM estimate_templates
      WHERE lower(template_name) = lower($1)
        AND ($2 = '' OR COALESCE(airtable_id, id::text) <> $2)
      LIMIT 1`, [name, handle]);
  if (!dupe || dupe.error) return resp(502, { ok: false, error: "Couldn't check for duplicates. Please try again." });
  if (dupe.rows.length) {
    return resp(409, {
      ok: false,
      error: `A template named "${dupe.rows[0].template_name}" already exists.`,
      existingId: dupe.rows[0].handle,
    });
  }

  const active = b.active !== false;   // new templates default ACTIVE
  const who    = await actorName(authUser);
  const vals   = [name, contractorId, contractorName, active,
                  text(b.scopeOfWork), text(b.exclusions), text(b.standardTerms),
                  money(b.basePrice), money(b.defaultLaborHours), money(b.defaultMaterialCost),
                  text(b.internalNotes), who,
                  money(b.defaultMaterialMarkup), money(b.defaultLaborSellRate)];

  let rows;
  try {
    if (handle) {
      rows = await neonWrite("estimateTemplate.update",
        `UPDATE estimate_templates SET
           template_name=$1, contractor_airtable_id=$2, contractor_name=$3, active=$4,
           scope_of_work=$5, exclusions=$6, standard_terms=$7, base_price=$8,
           default_labor_hours=$9, default_material_cost=$10, internal_notes=$11,
           updated_by=$12, default_material_markup=$13, default_labor_sell_rate=$14,
           updated_at=now()
         WHERE airtable_id = $15 OR id::text = $15
         RETURNING COALESCE(airtable_id, id::text) AS handle`, [...vals, handle]);
      if (!rows?.length) return resp(404, { ok: false, error: "That template no longer exists." });
    } else {
      rows = await neonWrite("estimateTemplate.create",
        `INSERT INTO estimate_templates
           (template_name, contractor_airtable_id, contractor_name, active,
            scope_of_work, exclusions, standard_terms, base_price,
            default_labor_hours, default_material_cost, internal_notes,
            updated_by, default_material_markup, default_labor_sell_rate,
            updated_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14, now())
         RETURNING COALESCE(airtable_id, id::text) AS handle`, vals);
    }
  } catch (e) {
    console.error(`estimateTemplateSave: ${e?.message || e}`);
    return resp(502, { ok: false, error: "Couldn't save the template. Please try again." });
  }

  return resp(200, { ok: true, templateId: rows[0]?.handle || handle, created: !handle });
}

// ── ARCHIVE / RESTORE A TEMPLATE ─────────────────────────────────────────
// Soft only — this flips `active`, which is the same flag the picker already
// filters on, so an archived template disappears from the dropdown and stays
// findable in the manager.
//
// ⚠ NO HARD DELETE, deliberately. `job_estimates.source_template_handle` points
// at this row for provenance, and estimates outlive the templates that seeded
// them by years. A DELETE would leave a dangling handle on a real quote to
// save a row in a five-row table.
async function handleEstimateTemplateArchive(body, authUser) {
  const handle = String(body?.templateId || "").trim();
  if (!handle) return resp(400, { ok: false, error: "Missing templateId." });
  const active = body?.active === true;   // explicit true = restore, anything else = archive
  const who    = await actorName(authUser);

  let rows;
  try {
    rows = await neonWrite("estimateTemplate.archive",
      `UPDATE estimate_templates SET active=$2, updated_by=$3, updated_at=now()
        WHERE airtable_id = $1 OR id::text = $1
        RETURNING COALESCE(airtable_id, id::text) AS handle, template_name, active`,
      [handle, active, who]);
  } catch (e) {
    console.error(`estimateTemplateArchive: ${e?.message || e}`);
    return resp(502, { ok: false, error: "Couldn't update the template. Please try again." });
  }
  if (!rows?.length) return resp(404, { ok: false, error: "That template no longer exists." });
  return resp(200, { ok: true, templateId: rows[0].handle, name: rows[0].template_name, active: rows[0].active });
}

// ── DELETE A TEMPLATE FOR GOOD ───────────────────────────────────────────
// Archiving is the normal path and stays the default in the UI. This exists
// because test rows and mistakes accumulate, and an Archived tab full of junk
// is its own problem.
//
// ⚠ It NULLs `source_template_handle` on every estimate that pointed here,
// in the same request. Leaving the handle would be a breadcrumb to a row that
// no longer exists — worse than no breadcrumb, because it reads like data.
// The estimates themselves are untouched: a template's numbers are SNAPSHOTTED
// into the estimate at create time, so deleting the template cannot change a
// single figure on a quote that already went out. That is the property that
// makes a hard delete safe here at all.
//
// The count is returned so the client can say what it just orphaned.
async function handleEstimateTemplateDelete(body) {
  const handle = String(body?.templateId || "").trim();
  if (!handle) return resp(400, { ok: false, error: "Missing templateId." });

  let rows, orphaned = 0;
  try {
    // Order matters: clear the references first. If the DELETE ran first and
    // the UPDATE then failed, the handles would dangle with nothing left to
    // point at and no error surfaced to anyone.
    const cleared = await neonWrite("estimateTemplate.clearRefs",
      `UPDATE job_estimates SET source_template_handle = NULL
        WHERE source_template_handle = $1 RETURNING 1`, [handle]);
    orphaned = cleared?.length || 0;

    rows = await neonWrite("estimateTemplate.delete",
      `DELETE FROM estimate_templates
        WHERE airtable_id = $1 OR id::text = $1
        RETURNING COALESCE(airtable_id, id::text) AS handle, template_name`, [handle]);
  } catch (e) {
    console.error(`estimateTemplateDelete: ${e?.message || e}`);
    return resp(502, { ok: false, error: "Couldn't delete the template. Please try again." });
  }
  if (!rows?.length) return resp(404, { ok: false, error: "That template no longer exists." });
  return resp(200, { ok: true, deletedId: rows[0].handle, name: rows[0].template_name, orphaned });
}

// ── DELETE A JOB ESTIMATE ────────────────────────────────────────────────
// STRICT ADMIN (_ADMIN_POSTS), not admin+office like the other back-office
// money ops.
//
// ⚠⚠ THERE IS NO STATUS GUARD. Owner's explicit call, 2026-08-20, after being
// shown the alternative: a Sent or Approved estimate is a record of what a
// customer was quoted, and this will erase one without complaint. The
// protection is therefore entirely (a) the strict-admin tier and (b) a client
// confirm that names the estimate and its amount. If you are tempted to relax
// either, the guard you are removing is the only one there is.
//
// ⚠ NEON FIRST AS OF SLICE 3, and the order flipped for a reason. It used to
// delete Airtable first, because Airtable was the identity authority and a
// Neon-only delete would be undone by a read that fell back. Neither half of
// that is true now: Neon owns the row, every read is Neon-first, and a native
// estimate has no Airtable record at all — under the old order the very first
// line would have 404'd on the only estimates the app can now create.
//
// The Airtable half is a best-effort mirror. If it fails, Airtable keeps a row
// the app can no longer see, which is the same divergence every other native
// write accepts and is the direction the base is going anyway.
//
// ⚠ The `startsWith("rec")` guard is GONE. It was a real id check when rec ids
// were the only shape; keeping it would have rejected every native estimate —
// the exact id-form regression recorded in docs/TODO.md. The 404 below is the
// check now, and it is a better one: it fails on ids that do not exist rather
// than on ids that do not look familiar.
//
// `sent_estimate_pdfs.estimate_id` is ON DELETE SET NULL, so a PDF snapshot
// SURVIVES its parent estimate. That is deliberate at the schema level: the PDF
// is the thing that actually went to the customer, and it should outlive the
// master record being tidied away.
async function handleDeleteJobEstimate(body) {
  const estimateId = String(body?.estimateId || "").trim();
  if (!estimateId) return resp(400, { ok: false, error: "Missing estimateId." });
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't delete the estimate right now — the database is unavailable. Try again in a moment." });
  }

  let rows;
  try {
    rows = await neonWrite("estimate.delete",
      `DELETE FROM job_estimates
        WHERE airtable_id = $1 OR id::text = $1
        RETURNING COALESCE(airtable_id, id::text) AS handle, airtable_id`, [estimateId]);
  } catch (e) {
    console.error(`deleteJobEstimate: Neon delete failed — ${e?.message || e}`);
    return resp(502, { ok: false, error: "Couldn't delete the estimate. Please try again." });
  }
  if (!rows?.length) return resp(404, { ok: false, error: "That estimate no longer exists." });

  const recId = rows[0].airtable_id;
  if (recId) {
    await mirrorToAirtable("deleteJobEstimate", () =>
      atFetch(`${encodeURIComponent("Job Estimates")}/${recId}`, { method: "DELETE" }));
  }

  return resp(200, { ok: true, deletedId: rows[0].handle });
}

// ── CREATE JOB ESTIMATE ──────────────────────────────────────────────────
// POSTs a new Job Estimates record with the four template-derived fields
// snapshotted in. Source Template (fldrni1Lkpw7tMBq8) records which
// template seeded the values; the values themselves are independent
// scalars, so editing the template later does not change this estimate.
async function handleCreateJobEstimate(body) {
  const { jobId, baseAmount, laborHours, materialCost, notes, estimateType, sourceTemplateId, estimateDate,
          materialRawCost, materialMarkup, laborSellRate, laborBurdenRate, priceAdjustment,
          otherCosts, salesTax, taxRatePct } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // db/schema/065. `materialCost` is still accepted and still means "the figure
  // as typed" — a client that has not picked up the new form creates a
  // legacy-shaped row, which is the correct thing for it to do rather than a
  // half-populated one. See the CHECK note in handleUpdateEstimate.
  if (materialMarkup !== undefined && materialMarkup !== null && String(materialMarkup) !== "" &&
      (materialRawCost === undefined || materialRawCost === null || String(materialRawCost) === "")) {
    return resp(400, { ok: false, error: "Enter the raw material cost before adding a markup — otherwise the markup is counted twice." });
  }

  // NOTE: Estimate Name (fldneXJv6ia3TIPj6) is a formula field on the Job
  // Estimates table — Airtable computes it automatically and rejects writes.
  // The other formulas on this table (Estimated Labor Cost, Calculated
  // Estimated Total) are also skipped here. Only user-editable fields below.
  const fields = {};
  Object.assign(fields, jobLink("Job", jobId));
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
  // The mirror keeps holding material SELL, which is what this Airtable field
  // has always contained. Airtable is not gaining a column for the split.
  const mirrorMaterial = (materialRawCost !== undefined && materialRawCost !== null && materialRawCost !== "")
    ? Number(materialRawCost) + Number(materialMarkup || 0)
    : (materialCost !== undefined && materialCost !== null && materialCost !== "" ? Number(materialCost) : null);
  if (mirrorMaterial !== null) fields["Estimated Material Cost"] = mirrorMaterial;
  if (notes && String(notes).trim()) fields["Notes"] = String(notes);
  // The Airtable link only accepts an Airtable record id, so this guard has to
  // stay. What changed (db/schema/047) is that failing it is no longer the end
  // of the story: templates are natively creatable now, so a template made in
  // the app has a uuid and would silently lose its provenance here.
  if (sourceTemplateId && String(sourceTemplateId).startsWith("rec")) {
    fields["fldrni1Lkpw7tMBq8"] = [sourceTemplateId];
  }

  // ── NEON FIRST (identity cutover slice 3, db/schema/055) ─────────────────
  // Reversed on 2026-08-22. It used to POST Airtable, take the rec id, then
  // sync — so an Airtable outage meant the estimate existed NOWHERE, and every
  // read of this table is Neon-first, so nothing would have back-filled it.
  //
  // ⚠ FAILS CLOSED without a database, like slice 1's creates and for the same
  // reason: an Airtable-only estimate is invisible to the app forever.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't create the estimate right now — the database is unavailable. Try again in a moment." });
  }

  const num = (v) => (v === undefined || v === null || v === "" ? null : Number(v));

  // Same NaN guard as handleUpdateEstimate, and for the same reason: a stray
  // thousands separator makes Number() return NaN, Postgres numeric stores it,
  // and every rollup that touches the row becomes NaN without erroring.
  if ([baseAmount, laborHours, materialCost, materialRawCost, materialMarkup,
       laborSellRate, laborBurdenRate, priceAdjustment, otherCosts, salesTax,
       taxRatePct].map(num).some(v => v !== null && !Number.isFinite(v))) {
    return resp(400, { ok: false, error: "One of those figures isn't a number. Check for stray characters and try again." });
  }

  // ⚠ THE RATES ARE STAMPED HERE, RESOLVED, INCLUDING THE FALLBACKS — that is
  // the difference from the update path. A create knows exactly which rates were
  // in force at the moment of quoting, and an estimate that records them cannot
  // be silently repriced by next year's rate change. `labor_billable_rates` is
  // already effective-dated per job for the same reason.
  const SELL_RATE = `COALESCE($12::numeric, (SELECT j2.billable_hourly_rate FROM jobs j2 WHERE j2.airtable_id = $1 OR j2.id::text = $1), ${EST_LABOR_SELL_RATE})`;
  // db/schema/068. The burden rate is READ FROM THE CREW now, not from the
  // constant — hours-weighted true cost over the last 12 months, overtime
  // premium included. Resolved HERE and stamped, exactly like the sell rate
  // above and for the same reason: a quote records the rate that was in force
  // when it was written, so next year's raises reach new estimates and cannot
  // reprice old ones.
  //
  // ⚠ The 32.50 constant stays as the LAST resort, not the default. The view
  // returns NULL if there are no current rates or no time history, and a NULL
  // reaching the multiplication would cost labor at $0/hr on every new estimate
  // — which reads as a spectacular GP, not as an outage.
  const BURDEN    = `COALESCE($13::numeric, (SELECT burden_rate FROM v_estimating_labor_rate), ${EST_LABOR_RATE})`;
  const d = estDerived({
    hours: "$5::numeric", matRaw: "$10::numeric", matMarkup: "$11::numeric",
    matEntered: "$6::numeric", sellRate: SELL_RATE, burden: BURDEN,
    other: "$15::numeric", tax: "$16::numeric",
  });
  const rows = await neonWrite("estimate.create",
    `INSERT INTO job_estimates
       (job_airtable_id, job_id, estimate_type, status, actual_estimate_sent,
        estimated_labor_hours, estimated_material_cost,
        material_raw_cost, material_markup, labor_sell_rate, labor_burden_rate,
        price_adjustment, other_costs, sales_tax, tax_rate_pct,
        estimated_labor_cost, calculated_estimated_total, calculated_selling_price,
        estimate_date, notes, source_template_handle, synced_at)
     VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END,
             (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1), $2, $3, $4, $5,
             ${d.materialSell},
             $10::numeric, $11::numeric, ${SELL_RATE}, ${BURDEN},
             $14::numeric, $15::numeric, $16::numeric, $17::numeric,
             ${d.laborCost}, ${d.directCost}, ${d.sellingPrice},
             $7::date, $8, $9, now())
     RETURNING id`,
    [String(jobId), fields["Estimate Type"], fields["Status"],
     num(baseAmount), num(laborHours), num(materialCost),
     estimateDate ? String(estimateDate).slice(0, 10) : null,
     notes && String(notes).trim() ? String(notes) : null,
     sourceTemplateId ? String(sourceTemplateId) : null,
     num(materialRawCost), num(materialMarkup),
     num(laborSellRate), num(laborBurdenRate), num(priceAdjustment),
     num(otherCosts), num(salesTax), num(taxRatePct)]);
  const neonId = rows?.[0]?.id;
  if (!neonId) return resp(502, { ok: false, error: "Couldn't create the estimate. Please try again." });

  // The mirror. Best-effort from here on: the estimate is already real.
  //
  // ⚠ The stamp is safe on THIS table, and the reasoning is not transferable.
  // Estimate PDFs in R2 are keyed on the NEON uuid (`estimates/<uuid>/…`, see
  // copyAirtablePhotosToR2), not on the rec id, so a handle that changes from
  // uuid to rec id cannot orphan a file here — the slice-0 rule about
  // back-filling `airtable_id` is about R2 keys, and this table has none that
  // depend on it. The response is sent AFTER the stamp, so the client is handed
  // one handle and only one. Everything that looks an estimate up accepts both
  // anyway.
  const data = await mirrorToAirtable("createJobEstimate", () =>
    atFetch(`${encodeURIComponent("Job Estimates")}`, {
      method: "POST",
      body: JSON.stringify({ fields, typecast: true })
    }));

  if (data?.id) {
    await neonWrite("estimate.stampAirtableId",
      `UPDATE job_estimates SET airtable_id = $2, synced_at = now() WHERE id = $1`,
      [neonId, data.id]).catch((e) =>
        console.error(`createJobEstimate: rec id not stamped, estimate is Neon-only — ${e?.message || e}`));
  }

  return resp(200, { ok: true, id: data?.id || String(neonId), _airtableMirrored: !!data?.id });
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

// ── Fleet: NEON-FIRST, photos from R2 (roadmap Step 4b) ────────────────────
// Same shape as the lifts migration, for the same reason: Airtable attachment
// URLs expire (~2 h), so the photos were re-hosted in R2 first.
//
// Only ACTIVE vehicles are returned, matching the Airtable path — a sold truck
// stays in the table for its service history but leaves the list.
async function handleFleetVehiclesFromNeon() {
  const q = await neonQuery(
    `SELECT id::text, airtable_id, name, year, make, model, color, vin, plate,
            vehicle_type, current_mileage, mileage_date::text AS mileage_date,
            oil_type, oil_capacity, tire_brand, tire_size,
            tire_install_date::text AS tire_install_date, notes, wrench_size, lug_torque
       FROM fleet_vehicles
      WHERE active IS TRUE
      ORDER BY NULLIF(regexp_replace(name, '\\D', '', 'g'), '')::int NULLS LAST, name`);
  if (!q?.rows) return null;

  const vehicles = q.rows.map(r => ({
    id: r.id, airtableId: r.airtable_id || null,
    name: r.name || "", year: r.year ?? null, make: r.make || "", model: r.model || "",
    color: r.color || "", vin: r.vin || "", plate: r.plate || "",
    type: r.vehicle_type || "",
    currentMileage: r.current_mileage ?? null, mileageDate: r.mileage_date || "",
    oilType: r.oil_type || "", oilCapacity: r.oil_capacity == null ? null : Number(r.oil_capacity),
    tireBrand: r.tire_brand || "", tireSize: r.tire_size || "",
    tireInstallDate: r.tire_install_date || "", notes: r.notes || "",
    wrenchSize: r.wrench_size || "",
    lugTorque: r.lug_torque == null ? null : Number(r.lug_torque),
  }));
  return { vehicles: await attachEquipPhotos("fleet", vehicles), ms: q.ms };
}

async function handleFleetVehicles() {
  if (neonEnabled()) {
    const r = await handleFleetVehiclesFromNeon();
    if (r) return resp(200, { ok: true, vehicles: r.vehicles, _source: "neon", _ms: r.ms });
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error("fleetVehicles: Neon read failed, refusing to serve frozen Airtable data");
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  const records = await fetchAll(FLEET_TABLES.vehicles, { sortField: "Vehicle Name", sortDir: "asc" });
  const vehicles = records.filter(r => r.fields["Active"] === true).map(r => { const f=r.fields||{}; return { id:r.id,name:f["Vehicle Name"]||"",year:f["Year"]||null,make:f["Make"]||"",model:f["Model"]||"",color:f["Color"]||"",vin:f["VIN"]||"",plate:f["License Plate"]||"",type:f["Vehicle Type"]?.name||f["Vehicle Type"]||"",currentMileage:f["Current Mileage"]??null,mileageDate:f["Mileage Date"]||"",oilType:f["Oil Type"]||"",oilCapacity:f["Oil Capacity (qts)"]??null,tireBrand:f["Tire Brand"]||"",tireSize:f["Tire Size"]||"",tireInstallDate:f["Tire Install Date"]||"",notes:f["Notes"]||"",photoUrl:(f["Photo"]||[])[0]?.url||"",wrenchSize:f["Oil Drain Wrench Size"]||"",lugTorque:f["Lug Torque (ft-lbs)"]??null }; });
  return resp(200, { ok: true, vehicles });
}

// Resolves either id form — the Airtable fallback still returns `rec…` ids.
async function resolveVehicle(vehicleId) {
  const rows = await neonWrite("fleet.resolve",
    `SELECT id, airtable_id, name FROM fleet_vehicles
      WHERE id::text = $1 OR airtable_id = $1 LIMIT 1`, [String(vehicleId)]);
  return rows?.[0] || null;
}

async function handleFleetServiceHistory(params) {
  const { vehicleId } = params || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });

  // A REAL FOREIGN KEY, unlike the Airtable path below, which filters
  // {Vehicle}="<name>" — two trucks named alike, or a rename, and the service
  // history follows the wrong one. That name is also interpolated unescaped
  // there, so an apostrophe in a vehicle name breaks the formula outright.
  if (neonEnabled()) {
    const target = await resolveVehicle(vehicleId);
    if (!target) return resp(200, { ok: true, records: [], _source: "neon" });
    const q = await neonQuery(
      `SELECT id::text, service_date::text AS service_date, mileage, service_types,
              filter_no, oil_type_used, oil_qty, tire_brand, tire_size, cost,
              performed_by, shop, notes
         FROM fleet_maintenance WHERE vehicle_id = $1
        ORDER BY service_date DESC NULLS LAST`, [target.id]);
    if (q?.rows) {
      return resp(200, {
        ok: true,
        records: q.rows.map(r => ({
          id: r.id, date: r.service_date || "", mileage: r.mileage ?? null,
          serviceTypes: r.service_types || [],
          oilBrand: r.filter_no || "", oilType: r.oil_type_used || "",
          oilQty: r.oil_qty == null ? null : Number(r.oil_qty),
          tireBrand: r.tire_brand || "", tireSize: r.tire_size || "",
          cost: r.cost == null ? null : Number(r.cost),
          performedBy: r.performed_by || "", shop: r.shop || "", notes: r.notes || "",
        })),
        _source: "neon", _ms: q.ms,
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`fleetServiceHistory: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
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

  const target = await resolveVehicle(vehicleId);
  if (!target) return resp(404, { ok: false, error: "Vehicle not found." });

  const sets = [], vals = [target.id];
  const put = (col, v, cast = "") => { vals.push(v); sets.push(`${col} = $${vals.length}${cast}`); };
  // Setting the mileage stamps the date with it, matching the Airtable path —
  // a reading without a date is not much of a reading.
  if (currentMileage !== undefined) {
    put("current_mileage", Number(currentMileage));
    put("mileage_date", new Date().toISOString().slice(0, 10), "::date");
  }
  if (oilType         !== undefined) put("oil_type", oilType || null);
  if (oilCapacity     !== undefined) put("oil_capacity", Number(oilCapacity));
  if (tireBrand       !== undefined) put("tire_brand", tireBrand || null);
  if (tireSize        !== undefined) put("tire_size", tireSize || null);
  if (tireInstallDate !== undefined) put("tire_install_date", tireInstallDate || null, "::date");
  if (vin             !== undefined) put("vin", vin || null);
  if (plate           !== undefined) put("plate", plate || null);
  if (notes           !== undefined) put("notes", notes || null);
  if (!sets.length) return resp(400, { ok: false, error: "Nothing to update." });
  await neonWrite("fleet.update",
    `UPDATE fleet_vehicles SET ${sets.join(", ")} WHERE id = $1`, vals);

  if (!target.airtable_id) return resp(200, { ok: true, updatedId: target.id });
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
  await mirrorToAirtable("updateFleetVehicle", () =>
    atFetch(`${encodeURIComponent(FLEET_TABLES.vehicles)}/${target.airtable_id}`,
      { method: "PATCH", body: JSON.stringify({ fields }) }));
  return resp(200, { ok: true, updatedId: target.id });
}

// ── LOG MILEAGE: creates entry in Fleet Mileage Log AND updates Fleet Vehicles ──
async function handleLogMileage(body) {
  const { vehicleId, mileage, date, recordedBy, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  if (mileage === undefined || mileage === null || mileage === "") {
    return resp(400, { ok: false, error: "Missing mileage." });
  }
  // Accepts BOTH id forms. This used to demand a `rec…` prefix, which quietly
  // rejected every Neon uuid the moment the vehicle list started returning them —
  // logging mileage would have failed with "Invalid vehicleId" on every truck.
  // Caught by the branch test rather than in the field.
  const idStr = String(vehicleId).trim();
  const looksLikeId = idStr.startsWith("rec") || /^[0-9a-f-]{36}$/i.test(idStr);
  if (!looksLikeId) return resp(400, { ok: false, error: `Invalid vehicleId: ${idStr}` });

  const effectiveDate = date || new Date().toISOString().slice(0,10);
  const mileageNum = Number(mileage);
  if (isNaN(mileageNum) || mileageNum < 0) {
    return resp(400, { ok: false, error: "Invalid mileage value." });
  }

  // NEON FIRST, and both writes in ONE statement. This path always did two
  // things — append to the log AND update the vehicle's current reading — and in
  // Airtable they were two round-trips that could half-succeed, leaving a log
  // entry the truck's odometer never caught up with. A CTE makes them atomic.
  const target = await resolveVehicle(idStr);
  if (!target) return resp(404, { ok: false, error: "Vehicle not found." });
  const rows = await neonWrite("fleet.logMileage",
    `WITH ins AS (
       INSERT INTO fleet_mileage_log (vehicle_id, log_date, mileage, recorded_by, notes)
       VALUES ($1, $2::date, $3, $4, $5) RETURNING id
     ), upd AS (
       UPDATE fleet_vehicles SET current_mileage = $3, mileage_date = $2::date WHERE id = $1
     )
     SELECT id FROM ins`,
    [target.id, effectiveDate, mileageNum, recordedBy || null, notes || null]);
  const neonLogId = rows?.[0]?.id;

  if (!target.airtable_id) return resp(200, { ok: true, logId: neonLogId, vehicleId: target.id });

  // 1. Create log entry in Fleet Mileage Log table
  const logFields = {};
  logFields[ML.date]    = effectiveDate;
  logFields[ML.vehicle] = [idStr];
  logFields[ML.mileage] = mileageNum;
  if (recordedBy) logFields[ML.recordedBy] = recordedBy;
  if (notes)      logFields[ML.notes]      = notes;

  // Airtable mirror, both halves fail-soft. The authoritative pair already
  // landed atomically in Neon above; a failure here leaves Airtable behind, not
  // the app.
  logFields[ML.vehicle] = [target.airtable_id];
  const logData = await mirrorToAirtable("logMileage.log", () =>
    atFetch(`${encodeURIComponent(FLEET_TABLES.mileageLog)}`, {
      method: "POST", body: JSON.stringify({ fields: logFields, typecast: true }) }));

  // 2. Update Fleet Vehicles record with new Current Mileage and Mileage Date
  const vehFields = {};
  vehFields[FV.mileage]     = mileageNum;
  vehFields[FV.mileageDate] = effectiveDate;
  await mirrorToAirtable("logMileage.vehicle", () =>
    atFetch(`${encodeURIComponent(FLEET_TABLES.vehicles)}/${target.airtable_id}`, {
      method: "PATCH", body: JSON.stringify({ fields: vehFields }) }));

  // Stamp the mirror's id so the two systems agree on this log row.
  if (logData?.id && neonLogId) {
    await mirrorToAirtable("logMileage.stamp", () =>
      neonWrite("fleet.stampLogId",
        `UPDATE fleet_mileage_log SET airtable_id = $2 WHERE id = $1`, [neonLogId, logData.id]));
  }
  return resp(200, { ok: true, logId: neonLogId, vehicleId: target.id });
}

async function handleAddFleetService(body) {
  const { vehicleId, vehicleName, date, mileage, serviceTypes, oilBrand, oilType, oilQty, cost, tireBrand, tireSize, performedBy, shop, notes } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: `Missing vehicleId. Keys: ${Object.keys(body||{}).join(",")}` });

  const target = await resolveVehicle(vehicleId);
  if (!target) return resp(404, { ok: false, error: "Vehicle not found." });
  const svcRows = await neonWrite("fleet.addService",
    `INSERT INTO fleet_maintenance
       (vehicle_id, service_date, mileage, service_types, filter_no, oil_type_used,
        oil_qty, tire_brand, tire_size, cost, performed_by, shop, notes)
     VALUES ($1,$2::date,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id`,
    [target.id, date || null, mileage ? Number(mileage) : null,
     Array.isArray(serviceTypes) ? serviceTypes : [],
     oilBrand || null, oilType || null, oilQty ? Number(oilQty) : null,
     tireBrand || null, tireSize || null, cost ? Number(cost) : null,
     performedBy || null, shop || null, notes || null]);
  const neonSvcId = svcRows?.[0]?.id;
  if (!target.airtable_id) return resp(200, { ok: true, id: neonSvcId });

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
  const data = await mirrorToAirtable("addFleetService", () =>
    atFetch(`${encodeURIComponent("Fleet Maintenance")}`,
      { method: "POST", body: JSON.stringify({ fields, typecast: true }) }));
  if (data?.id && neonSvcId) {
    await mirrorToAirtable("addFleetService.stamp", () =>
      neonWrite("fleet.stampServiceId",
        `UPDATE fleet_maintenance SET airtable_id = $2 WHERE id = $1`, [neonSvcId, data.id]));
  }
  return resp(200, { ok: true, id: neonSvcId, airtableId: data?.id || null });
}

// Resolves a service record by either id form, same as the vehicle resolver.
async function resolveFleetService(serviceRecordId) {
  const rows = await neonWrite("fleet.resolveService",
    `SELECT id, airtable_id FROM fleet_maintenance
      WHERE id::text = $1 OR airtable_id = $1 LIMIT 1`, [String(serviceRecordId)]);
  return rows?.[0] || null;
}

async function handleUpdateFleetService(body) {
  const { serviceRecordId, date, mileage, serviceTypes, oilBrand, oilType, oilQty, cost, tireBrand, tireSize, performedBy, shop, notes } = body || {};
  if (!serviceRecordId) return resp(400, { ok: false, error: "Missing serviceRecordId." });

  const target = await resolveFleetService(serviceRecordId);
  if (!target) return resp(404, { ok: false, error: "Service record not found." });

  // Mirrors the Airtable field gating exactly, including its quirks: date,
  // mileage, oilQty and cost are gated on TRUTHINESS, so a 0 does not overwrite.
  // Reproduced rather than "fixed" — changing it would silently alter what a
  // blanked field does, which is its own decision.
  const sets = [], vals = [target.id];
  const put = (col, v, cast = "") => { vals.push(v); sets.push(`${col} = $${vals.length}${cast}`); };
  if (date)         put("service_date", date, "::date");
  if (mileage)      put("mileage", Number(mileage));
  if (serviceTypes) put("service_types", Array.isArray(serviceTypes) ? serviceTypes : []);
  if (oilBrand    !== undefined) put("filter_no", oilBrand || null);
  if (oilType     !== undefined) put("oil_type_used", oilType || null);
  if (oilQty)       put("oil_qty", Number(oilQty));
  if (cost)         put("cost", Number(cost));
  if (tireBrand   !== undefined) put("tire_brand", tireBrand || null);
  if (tireSize    !== undefined) put("tire_size", tireSize || null);
  if (performedBy !== undefined) put("performed_by", performedBy || null);
  if (shop        !== undefined) put("shop", shop || null);
  if (notes       !== undefined) put("notes", notes || null);
  if (!sets.length) return resp(400, { ok: false, error: "Nothing to update." });
  await neonWrite("fleet.updateService",
    `UPDATE fleet_maintenance SET ${sets.join(", ")} WHERE id = $1`, vals);
  if (!target.airtable_id) return resp(200, { ok: true, updatedId: target.id });

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
  await mirrorToAirtable("updateFleetService", () =>
    atFetch(`${encodeURIComponent("Fleet Maintenance")}/${target.airtable_id}`,
      { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) }));
  return resp(200, { ok: true, updatedId: target.id });
}

async function handleDeleteFleetService(body) {
  const { serviceRecordId } = body || {};
  if (!serviceRecordId) return resp(400, { ok: false, error: "Missing serviceRecordId." });
  const target = await resolveFleetService(serviceRecordId);
  if (!target) return resp(404, { ok: false, error: "Service record not found." });
  await neonWrite("fleet.deleteService",
    `DELETE FROM fleet_maintenance WHERE id = $1`, [target.id]);
  if (target.airtable_id) {
    await mirrorToAirtable("deleteFleetService", () =>
      atFetch(`${encodeURIComponent("Fleet Maintenance")}/${target.airtable_id}`, { method: "DELETE" }));
  }
  return resp(200, { ok: true, deleted: target.id });
}

// ── Scissor lifts: NEON-FIRST, photos from R2 (roadmap Step 4b) ────────────
// NATURAL SORT. Airtable ordered by Lift Name as text, so "Lift #10" sat between
// #1 and #2. The digits are extracted and sorted numerically; a lift whose name
// has no digits falls back to alphabetical instead of erroring on the cast.
// Done in the query rather than a stored sort column, so renaming a lift cannot
// leave a stale key behind.
const LIFT_SELECT = `
  SELECT id::text, airtable_id, name, status, current_job, assigned_to,
         date_deployed::text AS date_deployed, notes, hooks_left, box_left
    FROM scissor_lifts`;
const LIFT_ORDER = `
   ORDER BY NULLIF(regexp_replace(name, '\\D', '', 'g'), '')::int NULLS LAST, name`;

// Photos live in R2 under lifts/<id>/ and are listed, never stored in Neon —
// see db/schema/009_scissor_lifts.sql for why an Airtable URL could not be kept.
// ONE list call covers every lift, then the objects are grouped by id, so the
// page costs a single R2 round-trip rather than one per lift.
async function attachEquipPhotos(kind, records) {
  if (!r2Enabled() || !records.length) return records.map(l => ({ ...l, photos: [], photoUrl: "" }));
  let objects = [];
  try {
    objects = await listByPrefix(kind + "/");
  } catch (e) {
    // Photos are a nicety; the lift list itself must still render. Same fail-soft
    // stance the Photos tab takes when R2 is unavailable.
    console.error(`${kind}: R2 list failed, returning records without photos: ${e?.message || e}`);
    return records.map(l => ({ ...l, photos: [], photoUrl: "" }));
  }
  const byLift = new Map();
  for (const o of objects) {
    const rest = o.key.slice((kind + "/").length);
    const id = rest.slice(0, rest.indexOf("/"));
    if (!id) continue;
    if (!byLift.has(id)) byLift.set(id, []);
    byLift.get(id).push(o);
  }
  return await Promise.all(records.map(async l => {
    const objs = (byLift.get(l.id) || [])
      .sort((a, b) => new Date(a.lastModified || 0) - new Date(b.lastModified || 0));
    const photos = await Promise.all(objs.map(async o => ({
      key: o.key, name: o.key.slice(o.key.lastIndexOf("/") + 1),
      size: o.size, url: await presignGet(o.key),
    })));
    // photoUrl is kept as the first photo so the existing card/detail markup
    // keeps working unchanged; `photos` is what the new add/remove UI uses.
    //
    // ⚠ `photoThumbUrl` is what the CARDS should use — they render into a 130px
    // (fleet) or 100px (lift) box, and photoUrl is the full-size original.
    // Falls back to the original when no thumbnail exists, which is every photo
    // copied over from Airtable. The DETAIL view keeps using photoUrl: it is
    // the picture you opened the record to look at.
    return { ...l, photos,
      photoUrl: photos[0]?.url || "",
      photoThumbUrl: photos[0]?.thumbUrl || photos[0]?.url || "" };
  }));
}

function mapLiftRow(r) {
  return {
    id: r.id, airtableId: r.airtable_id || null,
    name: r.name || "", status: r.status || "Available",
    currentJob: r.current_job || "", assignedTo: r.assigned_to || "",
    dateDeployed: r.date_deployed || "", notes: r.notes || "",
    hooksLeft: r.hooks_left === true, boxLeft: r.box_left === true,
  };
}

async function handleScissorLifts() {
  if (neonEnabled()) {
    const q = await neonQuery(`${LIFT_SELECT}${LIFT_ORDER}`);
    if (q?.rows) {
      return resp(200, {
        ok: true, lifts: await attachEquipPhotos("lifts", q.rows.map(mapLiftRow)),
        _source: "neon", _ms: q.ms,
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`scissorLifts: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  const records = await fetchAll(TABLES.scissorLifts, { sortField: "Lift Name", sortDir: "asc" });
  const lifts = records.map(r => { const f=r.fields||{}; const photos=(f["Photo"]||[]).map(a=>a.url); return { id:r.id,name:f["Lift Name"]||"",status:f["Status"]||"Available",currentJob:f["Current Job"]||"",assignedTo:f["Assigned To"]||"",dateDeployed:f["Date Deployed"]||"",notes:f["Notes"]||"",photoUrl:photos[0]||"",photos:[],hooksLeft:f["Lift Hooks Left at Job"]===true,boxLeft:f["Lift Box Left at Job"]===true }; });
  return resp(200, { ok: true, lifts, _source: "airtable" });
}

// ── ONE-OFF: copy lift photos from Airtable into R2 (Step 4b) ──────────────
// Runs HERE, on Netlify, rather than in db/etl/scissor-lifts.mjs, for a mundane
// reason: the R2 credentials are write-only Netlify secrets. Nobody has a copy,
// Cloudflare shows an R2 secret key exactly once at creation, and minting a new
// token just to run a migration script is more moving parts than the job needs.
// The deployed function already holds working credentials — so the copy runs
// where they are.
//
// WHY IT HAS TO RUN BEFORE THE READ FLIP. Airtable serves attachments from
// v5.airtableusercontent.com on SIGNED URLs THAT EXPIRE (~2 h). The current
// handler only works because it re-fetches them from Airtable on every request.
// The moment lifts are read from Neon, that stops — so any photo not already in
// R2 dies within the hour, silently, and would have looked fine in testing.
//
// Idempotent: keys already present are skipped, so an interrupted or repeated run
// costs nothing and cannot duplicate. Safe to leave in place after the migration.
async function handleCopyLiftPhotosToR2() {
  return copyAirtablePhotosToR2({
    table: TABLES.scissorLifts, neonTable: "scissor_lifts",
    kind: "lifts", nameField: "Lift Name",
  });
}

// Fleet vehicles have the identical problem and the identical fix — see the
// note above. Same action, different table.
async function handleCopyFleetPhotosToR2() {
  return copyAirtablePhotosToR2({
    table: "Fleet Vehicles", neonTable: "fleet_vehicles",
    kind: "fleet", nameField: "Vehicle Name",
  });
}

// Estimate PDFs have the identical problem to lift and fleet photos: Airtable
// serves attachments on SIGNED URLS THAT EXPIRE, so the moment estimates are
// read from Neon those 15 links die — silently, and they would have looked fine
// in testing because a fresh Airtable read always returns a fresh URL.
//
// The only differences from the equipment copies are the table and the
// attachment field, which is why the helper below took an attachmentField
// parameter rather than a third near-duplicate of it.
async function handleCopyEstimatePdfsToR2() {
  return copyAirtablePhotosToR2({
    table: "Job Estimates", neonTable: "job_estimates",
    kind: "estimates", nameField: "Estimate Name",
    attachmentField: "Estimate PDF",
  });
}

// Server-side PUT of bytes we already hold. Everything else in this file hands
// the browser a presigned URL and gets out of the way; these two paths (copying
// an Airtable attachment, archiving a payroll PDF that arrived as base64) are
// the exceptions, because the bytes are already in the function.
//
// ⚠ presignPut is ASYNC — signing is async in aws4fetch. Without the await,
// fetch receives a Promise and dies with "Failed to parse URL from
// [object Promise]".
async function putBufferToR2(key, buf, contentType) {
  const put = await fetch(await presignPut(key, contentType), {
    method: "PUT", body: buf, headers: { "content-type": contentType },
  });
  if (!put.ok) throw new Error(`R2 upload ${put.status} for ${key}`);
  return key;
}

// ── PAYROLL ARCHIVE → R2 (audit item 04, db/schema/052) ────────────────────
// Copies every existing run's PDF and JSON out of Airtable and records the keys
// in Neon, so `payrollRunsList` can stop asking Airtable for a signed
// attachment url on every page load.
//
// Not folded into copyAirtablePhotosToR2 despite the family resemblance: that
// helper copies MANY attachments per record from ONE field and derives the key
// from the attachment id. A payroll run has exactly TWO files from TWO fields
// and the keys are stored, not derived. Forcing them together would mean two
// passes and a special case in both.
//
// ⚠ It also backfills `supersedes_id`, which is a second thing the grid needs
// and has nowhere else to come from — Airtable's `Supersedes` link is the only
// record of which correction replaced which run. Both passes are idempotent, so
// re-running is free and is how you resume after a partial failure.
async function handleCopyPayrollFilesToR2() {
  if (!r2Enabled())   return resp(503, { ok: false, error: "R2 is not configured." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Neon is not configured." });

  // ⚠ byFieldId, and it is NOT optional here. `PR_RUNS.*` holds field IDs, but
  // Airtable returns records keyed by field NAME by default (see the F.* note in
  // CLAUDE.md) — so `rec.fields["fldSIebm2uhLkpjqD"]` is undefined and every run
  // looks like it has no attachments. The first run of this reported
  // `copied: 0, skipped: 56, ok: true` and had done nothing at all.
  const records = await fetchAll(PR_RUNS.table, { byFieldId: true });
  const rows = await neonWrite("payrollRuns.listForCopy",
    `SELECT id, airtable_id, pdf_key, json_key FROM payroll_runs WHERE airtable_id IS NOT NULL`);
  const byAirtable = new Map(rows.map(r => [r.airtable_id, r]));

  const report = { copied: 0, skipped: 0, failed: 0, unmatched: 0, supersedesLinked: 0, details: [] };

  for (const rec of records) {
    const run = byAirtable.get(rec.id);
    if (!run) {
      // A run in Airtable the mirror never carried over. Reported, not guessed
      // at — copying it under an invented id would strand the file.
      report.unmatched++;
      report.details.push(`unmatched: ${rec.fields?.[PR_RUNS.payPeriodEnd] || rec.id}`);
      continue;
    }

    const prefix = payrollPrefix(run.id);
    const jobs = [
      { field: PR_RUNS.pdf,         col: "pdf_key",  have: run.pdf_key,  ext: ".pdf",  type: "application/pdf" },
      { field: PR_RUNS.jsonPayload, col: "json_key", have: run.json_key, ext: ".json", type: "application/json" },
    ];

    for (const j of jobs) {
      if (j.have) { report.skipped++; continue; }
      const att = (rec.fields?.[j.field] || [])[0];
      if (!att) { report.skipped++; continue; }
      // Keyed on the ATTACHMENT id: two runs can both hold "payroll.pdf", and a
      // rename in Airtable must not orphan the copy.
      const key = `${prefix}${att.id}${j.ext}`;
      try {
        // Download and upload back to back, while the signed url is still valid.
        const dl = await fetch(att.url);
        if (!dl.ok) throw new Error(`download ${dl.status}`);
        const buf = Buffer.from(await dl.arrayBuffer());
        await putBufferToR2(key, buf, att.type || j.type);
        await neonWrite("payrollRuns.setKey",
          `UPDATE payroll_runs SET ${j.col} = $2, synced_at = now() WHERE id = $1`, [run.id, key]);
        report.copied++;
        report.details.push(`copied: ${key} (${buf.length}b)`);
      } catch (e) {
        report.failed++;
        report.details.push(`FAILED: ${key}: ${e.message}`);
      }
    }

    // `Supersedes` holds the rec id of the OLDER run this one replaced.
    const prior = (rec.fields?.[PR_RUNS.supersedes] || [])[0];
    if (prior && byAirtable.get(prior)) {
      try {
        const upd = await neonWrite("payrollRuns.setSupersedes",
          `UPDATE payroll_runs SET supersedes_id = $2, synced_at = now()
            WHERE id = $1 AND supersedes_id IS DISTINCT FROM $2 RETURNING id`,
          [run.id, byAirtable.get(prior).id]);
        if (upd?.length) report.supersedesLinked++;
      } catch (e) {
        report.failed++;
        report.details.push(`FAILED supersedes for ${rec.id}: ${e.message}`);
      }
    }
  }

  // The caller should not trust the R2 half until these agree.
  const after = (await listByPrefix("payroll/")).length;
  const expected = records.reduce((n, r) =>
    n + ((r.fields?.[PR_RUNS.pdf]?.length || 0) > 0 ? 1 : 0)
      + ((r.fields?.[PR_RUNS.jsonPayload]?.length || 0) > 0 ? 1 : 0), 0);

  // ⚠⚠ "NOTHING TO DO" IS NOT SUCCESS WHEN THERE ARE RUNS.
  // `after === expected` was the whole reconciliation, and it passed 0 === 0 on
  // a run that read every field under the wrong key and therefore saw no
  // attachments anywhere. A guard that a bug can satisfy by breaking BOTH sides
  // of its own comparison is not a guard. Same failure the estimate copier hit
  // from the other direction, where `expected` came back 0 while R2 correctly
  // held 15 — in both cases the count was right and the input was wrong.
  const blind = records.length > 0 && expected === 0;
  if (blind) {
    report.details.push(
      `NOTHING FOUND: ${records.length} runs in Airtable and not one attachment on any of them. ` +
      `That is a field-key bug, not an empty archive — check the byFieldId flag.`);
  }

  return resp(200, {
    ok: !blind && report.failed === 0 && report.unmatched === 0 && after === expected,
    ...report,
    runsInAirtable: records.length,
    objectsInR2: after,
    attachmentsInAirtable: expected,
    reconciled: !blind && after === expected,
  });
}

// attachmentField defaults to "Photo" so the two equipment callers are
// unchanged; estimates pass "Estimate PDF".
async function copyAirtablePhotosToR2({ table, neonTable, kind, nameField, attachmentField = "Photo" }) {
  if (!r2Enabled()) return resp(503, { ok: false, error: "R2 is not configured." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Neon is not configured." });

  const records = await fetchAll(table);
  const rows = await neonWrite(`${kind}.listForCopy`,
    `SELECT id, airtable_id FROM ${neonTable} WHERE airtable_id IS NOT NULL`);
  const idByAirtable = new Map(rows.map(r => [r.airtable_id, r.id]));

  // One list up front so a re-run resumes rather than re-uploading.
  const existing = new Set((await listByPrefix(kind + "/")).map(o => o.key));

  const report = { copied: 0, skipped: 0, failed: 0, unmatched: 0, details: [] };
  for (const rec of records) {
    const recId = idByAirtable.get(rec.id);
    if (!recId) {
      // A record in Airtable that the ETL has not loaded yet. Reported, not
      // guessed at — copying it under a made-up id would strand the file.
      report.unmatched++;
      report.details.push(`unmatched: ${rec.fields?.[nameField] || rec.id}`);
      continue;
    }
    for (const att of (rec.fields?.[attachmentField] || [])) {
      // Keyed on the ATTACHMENT id, not the filename: two records can both
      // have "photo.jpg", and a rename in Airtable must not orphan the copy.
      const ext = (att.filename?.match(/\.[a-z0-9]+$/i) || [".jpg"])[0].toLowerCase();
      const key = `${kind}/${recId}/${att.id}${ext}`;
      if (existing.has(key)) { report.skipped++; continue; }
      try {
        // Download and upload back to back, while the signed URL is still valid.
        const img = await fetch(att.url);
        if (!img.ok) throw new Error(`download ${img.status}`);
        const buf = Buffer.from(await img.arrayBuffer());
        // presignPut is ASYNC — signing is async in aws4fetch. Without the await
        // fetch receives a Promise and dies with "Failed to parse URL from
        // [object Promise]". Every other call site in this file awaits it.
        const put = await fetch(await presignPut(key, att.type || "image/jpeg"), {
          method: "PUT", body: buf,
          headers: { "content-type": att.type || "image/jpeg" },
        });
        if (!put.ok) throw new Error(`upload ${put.status}`);
        report.copied++;
        report.details.push(`copied: ${rec.fields[nameField]} → ${key} (${buf.length}b)`);
      } catch (e) {
        report.failed++;
        report.details.push(`FAILED: ${rec.fields?.[nameField]} ${key}: ${e.message}`);
      }
    }
  }

  const after = (await listByPrefix(kind + "/")).length;
  // ⚠ attachmentField, NOT a hardcoded "Photo" — this line was missed when the
  // helper was parameterised for estimate PDFs, so `expected` came back 0 while
  // R2 correctly held 15, and the run reported ok:false on a copy that had
  // completely succeeded. The guard behaved correctly (it refuses to bless a
  // flip until the two sides agree); it was being fed the wrong number.
  const expected = records.reduce((n, r) => n + (r.fields?.[attachmentField]?.length || 0), 0);
  return resp(200, {
    ok: report.failed === 0 && report.unmatched === 0 && after === expected,
    ...report,
    objectsInR2: after,
    attachmentsInAirtable: expected,
    // The caller should not flip anything until these two agree.
    reconciled: after === expected,
  });
}

// Resolves either id form. The Airtable read fallback still returns `rec…` ids,
// so this is permanent rather than a transition shim — same as time entries.
async function resolveLift(liftId) {
  const rows = await neonWrite("lifts.resolve",
    `SELECT id, airtable_id, name FROM scissor_lifts
      WHERE id::text = $1 OR airtable_id = $1 LIMIT 1`, [String(liftId)]);
  return rows?.[0] || null;
}

// ── NEW 2026-08-05: add a lift ─────────────────────────────────────────────
// Did not exist before — lifts could only be created in Airtable directly.
// Born in Neon with no airtable_id; the Airtable mirror stamps one if it lands.
async function handleCreateScissorLift(body) {
  const name = String(body?.name || "").trim();
  if (!name) return resp(400, { ok: false, error: "Missing lift name." });

  const rows = await neonWrite("lifts.insert",
    `INSERT INTO scissor_lifts (name, status, current_job, assigned_to, notes)
     VALUES ($1, $2, $3, $4, $5) RETURNING id`,
    [name, body?.status || "Available", body?.currentJob || null,
     body?.assignedTo || null, body?.notes || null]);
  const liftId = rows?.[0]?.id;

  const fields = { "Lift Name": name, "Status": body?.status || "Available" };
  if (body?.currentJob)  fields["Current Job"] = String(body.currentJob);
  if (body?.assignedTo)  fields["Assigned To"] = String(body.assignedTo);
  if (body?.notes)       fields["Notes"]       = String(body.notes);
  const data = await mirrorToAirtable("createScissorLift", () =>
    atFetch(`${encodeURIComponent(TABLES.scissorLifts)}`, {
      method: "POST", body: JSON.stringify({ fields, typecast: true }) }));
  if (data?.id && liftId) {
    await mirrorToAirtable("createScissorLift.stamp", () =>
      neonWrite("lifts.stampAirtableId",
        `UPDATE scissor_lifts SET airtable_id = $2 WHERE id = $1`, [liftId, data.id]));
  }
  return resp(200, { ok: true, id: liftId, airtableId: data?.id || null });
}

// ── NEW 2026-08-05: delete a sold lift, photos and all ─────────────────────
// Owner's rule: selling a lift removes everything. Safe to hard-delete —
// handleAddLiftExpense links its Expense to the JOB with a hardcoded vendor and
// type "Scissor Lift"; it never references the lift record, so no financial
// history depends on this row.
//
// R2 is cleaned FIRST and deliberately: once the row is gone nothing knows the
// prefix, and the files would sit there paid-for and unreachable forever. There
// is no recycle bin here, unlike job photos — a bin would only be somewhere for
// sold equipment to linger.
async function handleDeleteScissorLift(body) {
  const { liftId } = body || {};
  if (!liftId) return resp(400, { ok: false, error: "Missing liftId." });
  const target = await resolveLift(liftId);
  if (!target) return resp(404, { ok: false, error: "Lift not found." });

  let photosDeleted = 0;
  if (r2Enabled()) {
    try {
      photosDeleted = await deleteAllLiftPhotos(target.id);
    } catch (e) {
      // Orphaned files are wasteful but harmless; refusing to retire a sold lift
      // because storage hiccuped would be worse. Reported, not silently eaten.
      console.error(`deleteScissorLift: R2 cleanup failed for ${target.id}: ${e?.message || e}`);
    }
  }
  await neonWrite("lifts.delete", `DELETE FROM scissor_lifts WHERE id = $1`, [target.id]);
  if (target.airtable_id) {
    await mirrorToAirtable("deleteScissorLift", () =>
      atFetch(`${encodeURIComponent(TABLES.scissorLifts)}/${target.airtable_id}`, { method: "DELETE" }));
  }
  return resp(200, { ok: true, deletedId: target.id, name: target.name, photosDeleted });
}

// ── NEW 2026-08-05: add a photo ────────────────────────────────────────────
// Presigned PUT, so the bytes go browser → R2 directly and never through the
// function — no 4.5 MB payload ceiling, same pattern as jobsite photos.
async function handleLiftPhotoUploadUrl(body) {
  const { liftId, contentType } = body || {};
  if (!liftId) return resp(400, { ok: false, error: "Missing liftId." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Photo storage is not configured." });
  const target = await resolveLift(liftId);
  if (!target) return resp(404, { ok: false, error: "Lift not found." });

  const type = String(contentType || "image/jpeg");
  if (!type.startsWith("image/")) return resp(400, { ok: false, error: "Photos must be images." });
  const ext = type === "image/png" ? ".png" : type === "image/webp" ? ".webp" : ".jpg";
  // Random suffix rather than the client's filename: two phones both send
  // "IMG_0001.jpg", and a client-chosen name is a key-injection surface.
  const key = `${liftPrefix(target.id)}${Date.now()}-${Math.random().toString(16).slice(2, 8)}${ext}`;
  // Two presigned PUTs, matching the jobsite-photo path: the browser uploads a
  // compressed full image AND a ~400px thumbnail. Before this, equipment photos
  // had no thumbnail at all, so a lift card downloaded a multi-megabyte
  // original to fill a 104x78 box.
  return resp(200, { ok: true, key,
    putUrl: await presignPut(key, type),
    thumbKey: thumbKeyFor(key),
    thumbPutUrl: await presignPut(thumbKeyFor(key), type),
    contentType: type });
}

// Backfill a thumbnail for a photo that predates them. The browser fetches the
// original through its presigned GET, resizes it, and PUTs the result here.
//
// Admin+office, matching the other equipment-photo writes: it creates an object
// in the bucket, even though it destroys nothing. `kind` is whitelisted rather
// than interpolated — it becomes a key prefix.
async function handleEquipThumbUploadUrl(body) {
  const { kind, id, key, contentType } = body || {};
  if (kind !== "lifts" && kind !== "fleet") return resp(400, { ok: false, error: "Invalid kind." });
  if (!id || !key) return resp(400, { ok: false, error: "Missing id or key." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Photo storage is not configured." });
  const type = String(contentType || "image/jpeg");
  if (!type.startsWith("image/")) return resp(400, { ok: false, error: "Thumbnails must be images." });
  try {
    return resp(200, { ok: true, thumbPutUrl: await presignEquipThumbPut(kind, String(id), String(key), type) });
  } catch (e) {
    if (e instanceof R2Error) return resp(400, { ok: false, error: e.message });
    throw e;
  }
}

// Fleet had NO upload handler at all — every vehicle photo in R2 arrived via the
// one-off copyFleetPhotosToR2 migration, which means nobody could add or replace
// one from the app. Same shape as the lift handler above, including the
// thumbnail pair.
async function handleFleetPhotoUploadUrl(body) {
  const { vehicleId, contentType } = body || {};
  if (!vehicleId) return resp(400, { ok: false, error: "Missing vehicleId." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Photo storage is not configured." });

  const type = String(contentType || "image/jpeg");
  if (!type.startsWith("image/")) return resp(400, { ok: false, error: "Photos must be images." });
  const ext = type === "image/png" ? ".png" : type === "image/webp" ? ".webp" : ".jpg";
  const key = `${fleetPrefix(String(vehicleId))}${Date.now()}-${Math.random().toString(16).slice(2, 8)}${ext}`;
  return resp(200, { ok: true, key,
    putUrl: await presignPut(key, type),
    thumbKey: thumbKeyFor(key),
    thumbPutUrl: await presignPut(thumbKeyFor(key), type),
    contentType: type });
}

// ── NEW 2026-08-05: remove one photo ───────────────────────────────────────
async function handleDeleteLiftPhoto(body) {
  const { liftId, key } = body || {};
  if (!liftId || !key) return resp(400, { ok: false, error: "Missing liftId or key." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Photo storage is not configured." });
  const target = await resolveLift(liftId);
  if (!target) return resp(404, { ok: false, error: "Lift not found." });
  try {
    // assertKeyInLift inside this refuses a key belonging to another lift.
    await deleteLiftPhoto(target.id, key);
  } catch (e) {
    if (e instanceof R2Error) return resp(400, { ok: false, error: e.message });
    throw e;
  }
  return resp(200, { ok: true, deletedKey: key });
}

async function handleUpdateScissorLift(body) {
  const { liftId, status, currentJob, assignedTo, dateDeployed, notes, hooksLeft, boxLeft } = body || {};
  if (!liftId) return resp(400, { ok: false, error: "Missing liftId." });

  const target = await resolveLift(liftId);
  if (!target) return resp(404, { ok: false, error: "Lift not found." });

  // Only what the client sent is written, so an omitted field is left alone
  // rather than nulled — the job-assign path sends status and job only.
  const sets = [], vals = [target.id];
  const put = (col, v, cast = "") => { vals.push(v); sets.push(`${col} = $${vals.length}${cast}`); };
  if (status)                    put("status", status);
  if (currentJob   !== undefined) put("current_job", currentJob || null);
  if (assignedTo   !== undefined) put("assigned_to", assignedTo || null);
  if (dateDeployed)               put("date_deployed", dateDeployed, "::date");
  if (notes        !== undefined) put("notes", notes || null);
  if (hooksLeft    !== undefined) put("hooks_left", hooksLeft === true);
  if (boxLeft      !== undefined) put("box_left", boxLeft === true);
  if (!sets.length) return resp(400, { ok: false, error: "Nothing to update." });
  await neonWrite("lifts.update",
    `UPDATE scissor_lifts SET ${sets.join(", ")} WHERE id = $1`, vals);

  if (!target.airtable_id) return resp(200, { ok: true, updatedId: target.id });
  const fields = {};
  if (status)                   fields["fldB9Kwqm0NS3RFFP"] = status;
  if (currentJob !== undefined) fields["fldZpCcD52inR2PGm"] = currentJob;
  if (assignedTo !== undefined) fields["fldkjsgzYiedjTaJ5"] = assignedTo || null;
  if (dateDeployed)             fields["fldqRXHkwiFQdjqor"] = dateDeployed;
  if (notes !== undefined)      fields["fldG5MLCzQbyClax0"] = notes;
  if (hooksLeft !== undefined)  fields["fldlpqrIcnTH8R7Yw"] = hooksLeft === true;
  if (boxLeft   !== undefined)  fields["fldm5zfYDcw0oQHX4"] = boxLeft === true;
  await mirrorToAirtable("updateScissorLift", () =>
    atFetch(`${encodeURIComponent(TABLES.scissorLifts)}/${target.airtable_id}`,
      { method: "PATCH", body: JSON.stringify({ fields }) }));
  return resp(200, { ok: true, updatedId: target.id });
}

async function handleDeleteTimeEntry(body) {
  const { entryId } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const target = await resolveTimeEntry(entryId);
  if (!target) return resp(404, { ok: false, error: "Time entry not found." });

  // NEON FIRST, and tombstoned rather than dropped — matching the ETL and the
  // puller. Silently losing payroll history is exactly the thing you want a record
  // of. Keyed on the Neon id now, so it also covers rows that never reached
  // Airtable, which is every new row once Make is retired.
  await neonWrite("timeEntry.delete",
    `WITH gone AS (
       INSERT INTO time_entries_deleted
         (airtable_id, qb_timesheet_id, employee_name, employee_id, work_date,
          duration_seconds, city_taxes, class, labor_type, source, notes, billable,
          job_id, job_name, labor_reviewed, airtable_created_at, deleted_detected_at)
       SELECT airtable_id, qb_timesheet_id, employee_name, employee_id, work_date,
              duration_seconds, city_taxes, class, labor_type, source, notes, billable,
              job_id, job_name, labor_reviewed, airtable_created_at, now()
         FROM time_entries WHERE id = $1
       RETURNING airtable_id)
     DELETE FROM time_entries WHERE id = $1`,
    [target.id]);

  if (target.airtable_id) {
    await mirrorToAirtable("deleteTimeEntry", () =>
      atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${target.airtable_id}`, { method: "DELETE" }));
  }
  return resp(200, { ok: true, deleted: target.id });
}

async function handleUpdateTimeEntry(body) {
  const { entryId, reviewed, duration, notes } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const target = await resolveTimeEntry(entryId);
  if (!target) return resp(404, { ok: false, error: "Time entry not found." });

  // This is the path that sets Labor Reviewed, from the per-job Time Entries tab.
  // The QB puller deliberately EXCLUDES that column from its updates so a re-sync
  // can't wipe the flag — which only works if the flag lands in Neon in the first
  // place. It now does so directly rather than via an Airtable round-trip.
  const sets = [], vals = [target.id];
  const put = (col, v, cast = "") => { vals.push(v); sets.push(`${col} = $${vals.length}${cast}`); };
  if (reviewed !== undefined) put("labor_reviewed", reviewed === true);
  if (duration !== undefined && duration !== null) put("duration_seconds", Number(duration), "::numeric");
  if (notes    !== undefined) put("notes", String(notes || ""));
  if (!sets.length) return resp(400, { ok: false, error: "Nothing to update." });

  await neonWrite("timeEntry.update",
    `UPDATE time_entries SET ${sets.join(", ")} WHERE id = $1`, vals);

  if (target.airtable_id) {
    const fields = {};
    if (reviewed !== undefined) fields["fldQn7d06doEkrGBv"] = reviewed === true;
    if (duration !== undefined && duration !== null) fields["fld9mz6As3099VPVp"] = Number(duration);
    if (notes    !== undefined) fields["Notes"]            = String(notes || "");
    await mirrorToAirtable("updateTimeEntry", () =>
      atFetch(`${encodeURIComponent(TABLES.timeEntries)}/${target.airtable_id}`,
        { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) }));
  }

  // ⚠ THIS is the review path the UI actually uses — `apiPost("updateTimeEntry",
  // { entryId, reviewed: true })` from the per-job Time Entries tab
  // (index.html:8987, :9005, :9021). The allocation hook first went into
  // handleUpdateTimeEntryPayroll ONLY, because that handler accepts `reviewed`
  // and the plan assumed it was where review happened. It is not: the payroll
  // screen's save payload carries entryId/duration/workDate/class/cityTaxes/
  // jobId and no `reviewed` at all, so the hook never fired and the first live
  // review after cutover created nothing.
  //
  // Both handlers carry it now. Accepting `reviewed` is what decides whether the
  // hook belongs, not which screen happens to call it today.
  let allocation;
  if (reviewed === true) {
    try {
      allocation = await createLaborAllocation(atFetch, target.id, target.airtable_id);
    } catch (e) {
      console.error(`updateTimeEntry: allocation failed — ${e?.message || e}`);
      allocation = { created: 0, error: String(e?.message || e) };
    }
  }
  return resp(200, { ok: true, updatedId: target.id, ...(allocation ? { allocation } : {}) });
}

// NEON-FIRST since slice 5 phase A. Airtable is the fallback.
//
// This one waited on the billing chain: it returns `unbilledHours` and
// `unbilledRevenue`, which Airtable derives through Labor Billing Allocations and
// the per-job billable rate. Those now exist as v_time_entry_billing, diffed
// against Airtable across all 14,564 entries with zero mismatches.
//
// Side benefit worth noting: the Airtable path below is one of the sites on the
// FIND-substring cross-job pattern that docs/TODO.md tracks — it prefilters on a
// job-name substring and then verifies the linked record id in memory, because a
// job name that is a prefix of another ("Jenny Ln 1" vs "Jenny Ln 10") would
// otherwise leak entries across jobs. In Neon it is a plain FK equality on job_id,
// so the whole class of bug does not exist on this path.
async function handleTimeEntries(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT t.id::text                        AS id,
              t.work_date::text                 AS work_date,
              t.employee_name, t.class, t.city_taxes, t.notes,
              t.hours::float8                   AS hours,
              t.duration_seconds::float8        AS duration,
              t.labor_reviewed,
              b.unbilled_hours::float8          AS unbilled_hours,
              b.unbilled_labor_revenue::float8  AS unbilled_revenue
         FROM time_entries t
         JOIN jobs j              ON j.id = t.job_id
         LEFT JOIN v_time_entry_billing b ON b.id = t.id
        WHERE j.airtable_id = $1 OR j.id::text = $1
        ORDER BY t.work_date DESC`,
      [jobId]
    );
    if (q?.rows) {
      const entries = q.rows.map(r => ({
        id:              r.id,                       // Neon uuid — the UI edits by it
        workDate:        r.work_date || "",
        employee:        r.employee_name || "",
        class:           r.class || "",
        cityTaxes:       r.city_taxes || "",
        hours:           r.hours ?? null,
        reviewed:        r.labor_reviewed === true,
        notes:           r.notes || "",
        duration:        r.duration ?? null,
        unbilledHours:   Number(r.unbilled_hours) || 0,
        unbilledRevenue: Number(r.unbilled_revenue) || 0,
      }));
      return resp(200, { ok: true, entries, _source: "neon", _ms: q.ms });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`timeEntries: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
  return resp(200, { ok: true, entries, _source: "airtable" });
}

// ⚠⚠ THE TWO READS BEHIND THE INVOICE DRAFT MUST SPEAK THE SAME ID FORM.
// The draft matches these allocations against the job's reviewed time entries
// (index.html), and `timeEntries` went Neon-first on 2026-07-31 — so it returned
// Neon uuids while this handler still returned Airtable rec ids. The two sets
// never intersected, the labor total summed to $0, and on a job with prior
// invoices the hasPriorInvoices guard suppressed the hours × rate fallback that
// would have masked it. Every T&M re-invoice since has quietly proposed
// materials only; Bethel School's $34,937.50 of labor was typed in by hand on
// 2026-08-11. Both id forms go out now and the client accepts either, so neither
// store's shape can break the match again.
//
// Reading Neon closes a second, quieter hole: an allocation created for a
// twinless time entry — every one since Step 3 — has no Airtable row at all, so
// an Airtable-only read could never see it.
//
// The Airtable path below stays as the fallback. Its {Job} is a
// multipleLookupValues through Time Entry → Job, so it returns the job NAME, not
// a record ID, and cannot verify by id — hence the client-side filtering. In
// Neon it is an FK equality on job_id, so that whole class of bug is gone.
async function handleUnlinkedLaborAllocations(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  if (neonEnabled()) {
    // ⚠ COALESCE(bill_rate, the job's rate). The stored rate is authoritative
    // once Airtable's lookup has synced, but a MIRRORED allocation carries no
    // rate until _billing-sync's hourly pass fills it, and this read is what
    // PROPOSES the labor line. The job rate is what that lookup resolves to
    // anyway, so without the fallback work approved minutes ago proposes $0.
    // A job with no rate at all still yields 0 — the honest answer, and the GP
    // audit's finding, not something to invent a number for.
    const q = await neonQuery(
      `SELECT COALESCE(la.airtable_id, la.id::text) AS id,
              t.id::text                 AS time_entry_id,
              t.airtable_id              AS time_entry_airtable_id,
              la.allocated_hours::float8 AS allocated_hours,
              (la.allocated_hours * COALESCE(la.bill_rate, j.billable_hourly_rate))::float8
                                         AS allocated_revenue,
              j.name                     AS job_name
         FROM labor_billing_allocations la
         JOIN time_entries t ON t.id = la.time_entry_id
         JOIN jobs j         ON j.id = t.job_id
        WHERE (j.airtable_id = $1 OR j.id::text = $1)
          AND la.invoice_airtable_id IS NULL
          AND la.invoice_id IS NULL`, [jobId]);
    if (q?.rows) {
      return resp(200, {
        ok: true,
        allocations: q.rows.map(r => ({
          id: r.id,
          allocatedHours:      Number(r.allocated_hours) || 0,
          allocatedRevenue:    Number(r.allocated_revenue) || 0,
          timeEntryId:         r.time_entry_id,
          timeEntryAirtableId: r.time_entry_airtable_id || null,
          jobName:             r.job_name || "",
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`unlinkedLaborAllocations: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
    const teId = Array.isArray(teArr) ? teArr[0] : null;
    return {
      id: r.id,
      allocatedHours: f["Allocated Hours"] ?? 0,
      allocatedRevenue: f["Allocated Revenue $"] ?? 0,
      // Both keys carry the SAME rec id on this path — Airtable has no uuid to
      // offer. The client matches on either, so a fallback response still lines
      // up with the entries it will be compared against (which, if Neon is down
      // for this read, are coming from Airtable too).
      timeEntryId: teId,
      timeEntryAirtableId: teId,
      jobName: Array.isArray(jobArr) ? jobArr[0] : (jobArr || "")
    };
  });
  return resp(200, { ok: true, allocations, _source: "airtable" });
}

async function handleExpenses(params, authUser) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const isMgrScope = authUser && (authUser.role === "admin" || authUser.role === "office");

  // ── NEON-FIRST (migration Step 4d) ──────────────────────────────────────
  // Replaces two Airtable round trips (fetch the job for its name, then FIND
  // that name inside ARRAYJOIN({Job}) and re-verify in memory) with one query.
  //
  // ⚠ THE SCOPE BELOW IS AN AUTHORIZATION BOUNDARY, NOT A FILTER.
  // admin/office see every expense on the job; an EMPLOYEE sees only their own.
  // The ETL did not carry `Submitted By` until 2026-08-07 — it was written for
  // GP aggregation, where who submitted a row is irrelevant — so flipping this
  // read before adding submitted_by_at_id would have leaked every employee's
  // expenses to every employee. Legacy rows have a NULL submitter and are
  // therefore invisible to employees, exactly as the Airtable path behaves
  // (`Submitted By` includes <id> is false when the field is empty).
  //
  // The money columns come from v_expenses, whose derivations are diffed to the
  // cent against Airtable — see db/schema/013_expense_derived_amounts.sql.
  if (neonEnabled()) {
    const q = await neonQuery(
      // ⚠ COALESCE(airtable_id, id) — this read returns the AIRTABLE REC ID.
      // Every expense consumer does atFetch("Expenses/<id>"): guardExpenseMutation,
      // delete, approve, update, and all six receipt handlers. Worse, R2 receipt
      // keys are built FROM this id, so switching to a uuid would 404 the writes
      // AND orphan every existing receipt. The id contract stays rec-shaped until
      // the writes and receipts move together. Same reasoning as the inspection
      // pickers in 4c-2.
      `SELECT COALESCE(e.airtable_id, e.id::text) AS id, e.expense_date::text AS expense_date, e.description,
              e.vendor_name, e.expense_type, e.expense_status, e.billable, e.reviewed,
              e.total_cost_actual_calc          AS total_cost,
              e.billable_material_amount_calc   AS billable_material,
              e.unbilled_material_amount_calc   AS unbilled_material,
              e.submitted_by_name,
              j.markup_pct
         FROM v_expenses e
         LEFT JOIN jobs j ON j.id = e.job_id
        WHERE (e.job_airtable_id = $1 OR e.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
          AND ($2 OR e.submitted_by_at_id = $3)
        ORDER BY e.expense_date DESC NULLS LAST`,
      [jobId, isMgrScope, authUser?.id || null]);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      const n = (v) => (v === null || v === undefined ? null : Number(v));
      return resp(200, {
        ok: true,
        expenses: q.rows.map(r => ({
          id: r.id, date: s(r.expense_date), description: s(r.description),
          vendor: s(r.vendor_name), expenseType: s(r.expense_type),
          totalCost: n(r.total_cost), expenseStatus: s(r.expense_status),
          billable: r.billable === true, jobMarkupPct: n(r.markup_pct),
          billableMaterial: n(r.billable_material), reviewed: r.reviewed === true,
          unbilledMaterial: n(r.unbilled_material) ?? 0,
          submittedBy: s(r.submitted_by_name)
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`expenses: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!jobRecords.length) return resp(200, { ok: true, expenses: [] });
  const jobName = jobRecords[0].fields["Job Name"] || "";
  // ARRAYJOIN on a linked-record field expands to the linked record's primary
  // field (Job Name), not record IDs. Use a newline-delimited prefilter for exact
  // name match, then verify the linked record ID in-memory to handle duplicate names.
  const safeName = escapeFormulaString(jobName);
  const filter = `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n")`;
  const allRecords = await fetchAll("Expenses", { filter, sortField: "Expense Date", sortDir: "desc" });
  const jobRecordsForJob = allRecords.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));
  // SERVER-SIDE SCOPE: admin/office see every expense on the job; an employee
  // sees ONLY the ones they submitted (Submitted By link = their record id).
  // This is the real boundary — the employee UI also hides totals, but even a
  // direct API call can't leak the job total or other people's expenses.
  const isMgr = authUser && (authUser.role === "admin" || authUser.role === "office");
  const records = isMgr
    ? jobRecordsForJob
    : jobRecordsForJob.filter(r => Array.isArray(r.fields?.["Submitted By"]) && r.fields["Submitted By"].includes(authUser?.id));
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
    // Who logged it (lookup of Employee Name through Submitted By). Blank on
    // legacy expenses entered before the field existed. Shown on the admin view.
    const submittedByRaw = f["Submitted By Name"];
    const submittedBy = Array.isArray(submittedByRaw) ? submittedByRaw.filter(Boolean).join(", ") : (submittedByRaw || "");
    return { id:r.id,date:f["Expense Date"]||"",description:f["Description"]||"",vendor,expenseType:f["Expense Type"]?.name||f["Expense Type"]||"",totalCost:f["Total Cost (Actual)"]??null,expenseStatus:f["Expense Status"]?.name||f["Expense Status"]||"",billable:f["Billable?"]===true,jobMarkupPct:markup,billableMaterial:f["Billable Material Amount $"]??null,reviewed:f["Reviewed"]===true,unbilledMaterial:f["Unbilled Material Amount $"]??0,submittedBy };
  });
  return resp(200, { ok: true, expenses });
}

// {Job} on Material Billing Allocations is a multipleLookupValues through
// Expense → Job — returns job NAME, not record ID. Defense-in-depth filtering
// by expenseId happens on the frontend.
//
// ── NEON-FIRST since 2026-08-24 (cutover slice 4) ──────────────────────────
// This was the last handler in the field app with NO Neon path at all, and it
// had two independent problems that one rewrite closes.
//
// ⚠⚠ IT MATCHED BY JOB NAME, because the lookup gives it nothing else. The
// \n-delimited FIND does handle the prefix case ("Jenny Ln 1" vs "Jenny Ln
// 10/11/12"), but it cannot handle DUPLICATE names — and they exist: eight job
// names are shared by two jobs each. **"Strongsville DG" is two jobs, MES 252
// and MES 394**, and MES 394 carries $10,983.26 of unlinked material that this
// endpoint offered under both. It has not mis-billed anyone: the invoice draft
// intersects `expenseId` against the job's OWN expenses, which is the
// defense-in-depth the comment above describes, and it holds. But it means the
// API's answer is only safe because one caller filters it again — so this keys
// on the job id and the answer is correct at the source.
//
// ⚠ AND IT COULD NOT HAVE SEEN A NATIVE ROW. A Neon-native material allocation
// has no Airtable record, and a native expense has no rec id for the lookup to
// resolve through, so both would be invisible here — as an empty picker, not an
// error, which on an invoice draft means silently proposing less material than
// was actually spent.
//
// ⚠ `expense_id` is emitted as COALESCE(rec, uuid) to match the `expenses`
// handler's `id` exactly. Those two sets are INTERSECTED by the invoice draft,
// and two id forms that never intersect is precisely the Bethel School failure
// — $34,937.50 of labor typed in by hand.
async function handleUnlinkedMaterialAllocations(params) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  if (neonEnabled()) {
    // The job takes either handle already. That is ahead of slice 6 on purpose —
    // it costs nothing here and removes one more site from that sweep. The labor
    // twin above was a bare `j.airtable_id = $1`; slice 6 gave it the dual handle too.
    const q = await neonQuery(
      `SELECT COALESCE(a.airtable_id, a.id::text) AS id,
              a.allocated_amount::float8          AS allocated_material,
              COALESCE(e.airtable_id, e.id::text) AS expense_id,
              j.name                              AS job_name
         FROM material_billing_allocations a
         JOIN expenses e ON e.id = a.expense_id
         JOIN jobs j     ON j.id = e.job_id
        WHERE (j.airtable_id = $1 OR j.id::text = $1)
          AND a.invoice_id IS NULL
          AND a.invoice_airtable_id IS NULL`, [String(jobId)]);
    if (q?.rows) {
      return resp(200, {
        ok: true,
        allocations: q.rows.map(r => ({
          id: r.id,
          allocatedMaterial: Number(r.allocated_material) || 0,
          expenseId: r.expense_id,
          jobName: r.job_name || "",
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`unlinkedMaterialAllocations: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  // ⚠⚠ THE AIRTABLE FALLBACK WAS DELETED HERE, in the same commit that made
  // expenses native — as the note left in its place on 2026-08-24 required.
  //
  // It matched allocations by job NAME out of Airtable, so it could not see a
  // Neon-native material allocation (no Airtable row) or one belonging to a
  // native expense (no rec id for the lookup to resolve through). It would have
  // returned a SUBSET, silently, and this endpoint proposes the material line on
  // an invoice draft — a subset here is an invoice that goes out short. There is
  // no honest degraded answer, so this now fails closed.
  //
  // This is the same contract as the slice-1 creates: on a money path, a refusal
  // the user can see beats a plausible number nobody checks.
  return resp(503, { ok: false,
    error: "Couldn't load the material allocations for this job. Please try again." });
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

  // Two very different failures used to look identical (both 400):
  //   1. The integration is broken  — REQUEST_DENIED / OVER_QUERY_LIMIT / bad key.
  //      Affects EVERY job, needs a human. Stays loud: 502 so it shows in the
  //      browser console and in logs.
  //   2. Google worked, but couldn't geocode THIS address — e.g. state routes
  //      written "8250 Ohio 676". Expected, per-job, nothing to fix in code.
  //      Now returns 200 + ok:false.
  // The client throws on `ok === false` regardless (see apiPost, ~index.html:3226)
  // and hides the mileage line either way, so the UI is unchanged. The point is
  // that case 2 no longer logs a red 400 on every open of an affected job —
  // routine console noise trains people to ignore the console, which is exactly
  // where case 1 needs to be visible.
  // Google's `error_message` is where the ACTIONABLE detail lives — e.g.
  // "API keys with referer restrictions cannot be used with this API" or
  // "This API project is not authorized to use this API". `status` alone
  // (REQUEST_DENIED) says only that the key was rejected, not why. Pass it
  // through: it is a config diagnostic, never end-user data, and it contains
  // no credentials.
  if (data.status !== "OK")
    return resp(502, { ok: false, reason: "upstream_error",
      error: `Google API error: ${data.status}`,
      detail: data.error_message || null });
  const element = data.rows?.[0]?.elements?.[0];
  if (!element || element.status !== "OK")
    return resp(200, { ok: false, reason: "address_unresolved",
      error: `Could not calculate distance for this address (${element?.status || "no result"}).` });
  const miles = Math.round(element.distance.value * 0.000621371 * 10) / 10;
  // Neon-first: `miles_from_shop` is in JOB_SELECT. Third instance found by the
  // 2026-08-12 sweep. Mileage is calculated once per job and then read on every
  // load, which is the worst shape for this bug — the value looks right the
  // moment you press the button and is gone by the next visit.
  await neonWrite("job.calculateMileage",
    `UPDATE jobs SET miles_from_shop = $2, synced_at = now() WHERE airtable_id = $1 OR id::text = $1`, [jobId, miles]);
  await mirrorJobPatch("calculateMileage", jobId, { "fldMy1yR7aHtVko9F": miles });
  return resp(200, { ok: true, miles });
}

// ── NEON-FIRST EXPENSE CREATE (cutover slice 4, 2026-08-24) ───────────────
// Shared by handleAddGeneralExpense and handleAddLiftExpense. Returns the new
// row's uuid; the caller mirrors to Airtable and NEVER stamps the rec id back.
//
// ⚠⚠ THE REC ID IS NEVER STAMPED BACK, and expenses are the one table where
// that rule is absolute. R2 receipt keys are `expenses/<handle>/…`, built from
// whatever id the client is holding. If the handle flipped from uuid to rec id
// after a successful mirror, every receipt already uploaded under the old prefix
// would orphan — `listExpenseReceipts` lists ONE prefix, not both. Estimates and
// invoices could be stamped in slice 3 precisely because nothing in R2 is keyed
// on their handle. Expenses are.
//
// ⚠⚠ AND THE MIRROR RESPONSE IS NEVER FED TO syncExpenseToNeon. That helper is
// `INSERT … ON CONFLICT (airtable_id)`, and a native row's airtable_id is NULL,
// so nothing conflicts — it would insert a SECOND expense for the same spend and
// both would count in GP. The Airtable copy is write-only from here.
//
// Safe because nothing re-reads Airtable Expenses on a schedule: qb-time-pull
// does not touch them and _billing-sync pulls only the two allocation tables
// (verified 2026-08-24). ⚠ If an expense ETL is ever added it MUST skip rows it
// cannot match by rec id, or every native expense doubles within the hour.
//
// The four Airtable formula columns are deliberately left NULL — v_expenses
// computes all of them, and since schema 057 nothing reads the stored copies.
async function createExpenseNative({ jobId, expenseType, expenseDate, billable,
                                    manualMaterialCost, materialCredit,
                                    vendorId, description, authUser }) {
  const handle = String(jobId);
  const rows = await neonWrite("expense.create",
    `INSERT INTO expenses
       (job_airtable_id, job_id, expense_type, expense_status, expense_date,
        reviewed, billable, manual_material_cost, material_credit,
        vendor_name, description, submitted_by_at_id, submitted_by_name, synced_at)
     VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),
             $3, 'Not Reviewed', $4::date, false, $5::boolean,
             $6::numeric, $7::numeric,
             (SELECT name FROM expense_vendors WHERE airtable_id = $8 OR id::text = $8),
             $9, $10,
             (SELECT name FROM employees WHERE airtable_id = $10 OR id::text = $10),
             now())
     RETURNING id`,
    [handle.startsWith("rec") ? handle : null, handle,
     expenseType, expenseDate || null, billable === true,
     manualMaterialCost == null ? null : Number(manualMaterialCost),
     materialCredit == null ? null : Number(materialCredit),
     vendorId ? String(vendorId) : null,
     description || null,
     authUser?.id ? String(authUser.id) : null]);
  return rows?.[0]?.id || null;
}

async function handleAddLiftExpense(body, authUser) {
  const { jobId, date, amount, description, billable } = body || {};
  if (!jobId || !amount) return resp(400, { ok: false, error: "Missing jobId or amount." });
  const idStr = String(jobId).trim();
  if (!isJobHandle(idStr)) return resp(400, { ok: false, error: `Invalid jobId received: ${idStr}` });
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldlTUL8hsPkReBAB":["recU56ncurkFrM2Nx"],"fldwbLPIafVtmaSeb":Number(amount),"fldX2x2J0xkRyMY3y":"Scissor Lift","fldelsB2jH2tvt1Cj":description||"Scissor Lift Expense","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  // Submitted By (Employee link) — stamped from the token, never client input.
  // ⚠⚠ REC-ID ONLY, and the guard is not paranoia — cutover slice 5. This is an
  // Airtable LINKED-RECORD field written with `typecast: true`, so handing it a
  // uuid does not error: Airtable CREATES A NEW EMPLOYEES RECORD whose name is
  // that uuid, and links to it. A natively-hired employee submitting one expense
  // would quietly add a junk person to the Employees table, and every expense
  // they filed after that would attribute to it.
  //
  // Dropping the field instead is correct and costs nothing: `submitted_by_at_id`
  // in Neon is what actually scopes an employee to their own expenses, and it is
  // written from the token by createExpenseNative regardless of id form.
  if (authUser?.id && String(authUser.id).startsWith("rec")) fields["fldRWV0eIKwBrXwHV"] = [authUser.id];
  if (date) fields["fldCCPYdyWAOGchWb"] = date;

  const neonId = await createExpenseNative({
    jobId: idStr, expenseType: "Scissor Lift", expenseDate: date,
    billable: billable === true || billable === "true",
    manualMaterialCost: Number(amount), materialCredit: null,
    vendorId: "recU56ncurkFrM2Nx", description: description || "Scissor Lift Expense",
    authUser });
  if (!neonId) return resp(502, { ok: false, error: "Couldn't save the expense. Please try again." });

  await mirrorToAirtable("addLiftExpense", () =>
    atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) }));
  return resp(200, { ok: true, id: String(neonId) });
}

async function handleAddGeneralExpense(body, authUser) {
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
  if (!isJobHandle(idStr)) return resp(400, { ok: false, error: `Invalid jobId: ${idStr}` });
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldX2x2J0xkRyMY3y":type||"Materials","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  if (date)        fields["fldCCPYdyWAOGchWb"] = date;
  if (description) fields["fldelsB2jH2tvt1Cj"] = description;
  // Gate both currency writes — leaves Manual Material Cost blank on credit-only
  // entries, matching the 4 existing precedent records entered via Airtable web UI.
  if (hasAmount) fields["fldwbLPIafVtmaSeb"] = Number(amount);
  if (hasCredit) fields["fldcld418pREq2bGq"] = Number(credit);
  if (vendorId && String(vendorId).startsWith("rec")) fields["fldlTUL8hsPkReBAB"] = [String(vendorId)];
  // Submitted By (Employee link) — stamped from the token, never client input.
  // This is what lets employees see/edit only their own expenses.
  // ⚠⚠ REC-ID ONLY, and the guard is not paranoia — cutover slice 5. This is an
  // Airtable LINKED-RECORD field written with `typecast: true`, so handing it a
  // uuid does not error: Airtable CREATES A NEW EMPLOYEES RECORD whose name is
  // that uuid, and links to it. A natively-hired employee submitting one expense
  // would quietly add a junk person to the Employees table, and every expense
  // they filed after that would attribute to it.
  //
  // Dropping the field instead is correct and costs nothing: `submitted_by_at_id`
  // in Neon is what actually scopes an employee to their own expenses, and it is
  // written from the token by createExpenseNative regardless of id form.
  if (authUser?.id && String(authUser.id).startsWith("rec")) fields["fldRWV0eIKwBrXwHV"] = [authUser.id];

  const neonId = await createExpenseNative({
    jobId: idStr, expenseType: type || "Materials", expenseDate: date,
    billable: billable === true || billable === "true",
    // Gated exactly as the Airtable payload is: a credit-only entry leaves
    // Manual Material Cost NULL, matching the four precedent records entered
    // through the Airtable web UI. NULL and 0 are not the same to the GP views.
    manualMaterialCost: hasAmount ? Number(amount) : null,
    materialCredit:     hasCredit ? Number(credit) : null,
    vendorId: (vendorId && String(vendorId).startsWith("rec")) ? String(vendorId) : null,
    description, authUser });
  if (!neonId) return resp(502, { ok: false, error: "Couldn't save the expense. Please try again." });

  await mirrorToAirtable("addGeneralExpense", () =>
    atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) }));
  return resp(200, { ok: true, id: String(neonId) });
}

// Edit an existing expense. Managers may edit any; an employee may edit only
// their own unreviewed one (enforced by guardExpenseMutation). Fields mirror
// the add form; amount/credit follow the same credit-only rule (set the one
// provided, clear the other). Submitted By is NOT touched here — ownership
// never changes on edit.
async function handleUpdateExpense(body, authUser) {
  const { expenseId, date, type, amount, credit, vendorId, description, billable } = body || {};
  const guard = await guardExpenseMutation(expenseId, authUser);
  if (!guard.ok) return guard.resp;

  const hasAmount = amount != null && Number(amount) > 0;
  const hasCredit = credit != null && Number(credit) > 0;
  if (!hasAmount && !hasCredit) return resp(400, { ok: false, error: "Missing amount or credit." });

  const fields = {
    "fldX2x2J0xkRyMY3y": type || "Materials",
    "fld9Afieu4ofjvhSb": billable === true || billable === "true",
    // ⚠ Manual Material COST — verified against the live schema 2026-08-24. The
    // comment here used to say "Total Cost (Actual)", which is a DIFFERENT and
    // derived column (cost minus credit). Writing the amount into that one
    // would double-count the credit in every GP figure.
    "fldwbLPIafVtmaSeb": hasAmount ? Number(amount) : null,  // Manual Material Cost
    "fldcld418pREq2bGq": hasCredit ? Number(credit) : null   // Material Credit
  };
  if (date !== undefined)        fields["fldCCPYdyWAOGchWb"] = date || null;
  if (description !== undefined) fields["fldelsB2jH2tvt1Cj"] = description || "";
  if (vendorId !== undefined)    fields["fldlTUL8hsPkReBAB"] = (vendorId && String(vendorId).startsWith("rec")) ? [String(vendorId)] : [];

  // ⚠ NEON FIRST, and it fails closed. The edit form is how a mis-typed cost
  // gets corrected, so an update that silently did not land is a wrong number
  // left standing on a job.
  //
  // `vendor_name` is resolved here rather than carried from Airtable's
  // "Vendor Name (from Vendor)" lookup — a native row has no lookup to read.
  const upd = await neonWrite("expense.update",
    `UPDATE expenses SET
       expense_type = $2,
       billable = $3::boolean,
       manual_material_cost = $4::numeric,
       material_credit = $5::numeric,
       expense_date = COALESCE($6::date, expense_date),
       description = COALESCE($7, description),
       vendor_name = (SELECT name FROM expense_vendors WHERE airtable_id = $8 OR id::text = $8),
       synced_at = now()
      WHERE airtable_id = $1 OR id::text = $1
      RETURNING COALESCE(airtable_id, id::text) AS handle`,
    [String(expenseId), type || "Materials", billable === true || billable === "true",
     hasAmount ? Number(amount) : null, hasCredit ? Number(credit) : null,
     date !== undefined ? (date || null) : null,
     description !== undefined ? (description || "") : null,
     (vendorId && String(vendorId).startsWith("rec")) ? String(vendorId) : null]);
  if (!upd?.length) return resp(404, { ok: false, error: "Expense not found." });

  if (guard.airtableId) {
    await mirrorToAirtable("updateExpense", () =>
      atFetch(`${encodeURIComponent("Expenses")}/${guard.airtableId}`,
        { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) }));
  }
  return resp(200, { ok: true, updatedId: upd[0].handle });
}

// ── UPDATE INSPECTION — NEON-FIRST (migration Step 4c) ────────────────────
// Flipped in the SAME commit as handleJobInspections, deliberately. That read
// now returns Neon uuids, and this handler PATCHed Airtable by the id it was
// given — the exact shape of the b79b9a0 regression, where flipping a read
// alone left a downstream write 404ing on a uuid. Read and write move together
// or not at all.
async function handleUpdateInspection(body) {
  const { inspectionId, status, notes } = body || {};
  if (!inspectionId) return resp(400, { ok: false, error: "Missing inspectionId." });

  const idStr = String(inspectionId).trim();
  let airtableId = idStr.startsWith("rec") ? idStr : null;

  if (neonEnabled()) {
    const sets = [], vals = [idStr];
    if (status)            { vals.push(status); sets.push(`inspection_status = $${vals.length}`); }
    if (notes !== undefined) { vals.push(notes); sets.push(`notes = $${vals.length}`); }
    if (sets.length) {
      const rows = await neonWrite("inspection.update",
        `UPDATE job_inspections SET ${sets.join(", ")}
          WHERE id::text = $1 OR airtable_id = $1
          RETURNING id, airtable_id`, vals);
      if (rows?.length) {
        airtableId = rows[0].airtable_id || airtableId;
      } else if (!airtableId) {
        // A uuid that matched nothing is a genuine miss, and there is no Airtable
        // id to fall back to. Say so rather than silently doing nothing.
        return resp(404, { ok: false, error: `Inspection not found: ${idStr}` });
      }
      // A rec id that matched no Neon row still falls through to Airtable below:
      // nothing syncs these tables, so an inspection created in Airtable an hour
      // ago legitimately has no Neon row yet.
    }
  }

  const fields = {};
  if (status) fields["fld7kH2SEHsxaS9vz"] = status;
  if (notes !== undefined) fields["fldmz5dOw6In5OkU7"] = notes;
  if (!airtableId) return resp(200, { ok: true, updatedId: idStr, _source: "neon" });

  const data = await mirrorToAirtable("updateInspection", () =>
    atFetch(`${encodeURIComponent("Job Inspections")}/${airtableId}`,
      { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) }));
  return resp(200, { ok: true, updatedId: data?.id || idStr });
}

async function handleCompanies() {
  // ── NEON-FIRST (audit item 06) ────────────────────────────────────────────
  // The customer/contractor master. 35 rows, 24 of them active contractors.
  //
  // ⚠ Kept in sync with `handleListContractors` below, which reads the SAME
  // table through the `active_contractor` filter. If one flips and the other
  // does not, the job form offers a contractor the company list has never heard
  // of — so they move together, in this commit.
  if (neonEnabled()) {
    const q = await neonQuery(
      // ⚠ COALESCE(airtable_id, id::text) — the DUAL HANDLE (cutover slice 1,
      // db/schema/053). A company created while Airtable was unreachable has no
      // rec id; returning a bare airtable_id would hand the picker a null id and
      // the row would be unselectable. Every lookup accepts either form.
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id,
              name, billing_address, primary_phone, primary_email
         FROM companies WHERE coalesce(name,'') <> '' ORDER BY name`);
    if (q?.rows?.length) {
      return resp(200, { ok: true, _source: "neon", _ms: q.ms, companies: q.rows.map(r => ({
        id:             r.airtable_id,   // ⚠ AIRTABLE id — jobs link to it
        name:           r.name || "",
        billingAddress: r.billing_address || "",
        primaryPhone:   r.primary_phone || "",
        primaryEmail:   r.primary_email || "",
      })) });
    }
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`companies: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
  // ── NEON-FIRST (audit item 06) ────────────────────────────────────────────
  // ⚠⚠ THE TABLE IS `expense_vendors`, NOT `vendors`. Neon already has a
  // `vendors` table and it is the INVENTORY base's — supply houses, with
  // pricing and payment terms. This is the MAIN base's expense-vendor picker,
  // a different table serving a different question ("who did I buy this
  // from?"), and it carries pseudo-vendors like "Other" and "NEE Inventory"
  // that have no place in a purchasing list.
  //
  // The two overlap on real companies with inconsistent spellings — "Wolff
  // Brothers" here vs "Wolff Bros" there, plus CED, Lowe's and Cummins.
  // **Owner's decision 2026-08-12: keep them separate.** Merging is a business
  // question, not a migration one; expenses key on the vendor NAME as text, so
  // a merge would not join anything automatically anyway. Same naming
  // discipline as `material_estimates` vs `job_estimates`.
  //
  // Sort and the "Other"-to-the-bottom rule stay in JS below so both paths
  // order identically.
  if (neonEnabled()) {
    const q = await neonQuery(
      // Dual handle — see the note in handleCompanies (cutover slice 1).
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id,
              name, phone, email, charges_sales_tax
         FROM expense_vendors WHERE active AND coalesce(name,'') <> '' ORDER BY name`);
    if (q?.rows?.length) {
      const vendors = q.rows.map(r => ({
        id: r.airtable_id,               // ⚠ the AIRTABLE id — expenses store it
        name: r.name || "",
        phone: r.phone || "",
        email: r.email || "",
        chargesSalesTax: r.charges_sales_tax === true,
      }));
      vendors.sort((a, b) => {
        if (a.name === "Other") return 1;
        if (b.name === "Other") return -1;
        return a.name.localeCompare(b.name);
      });
      return resp(200, { ok: true, vendors, _source: "neon", _ms: q.ms });
    }
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`vendors: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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

  // ── The role guard that used to live here is GONE, 2026-08-09 ──────────
  // It re-read the employee from Airtable and refused anyone who wasn't
  // admin/office. That predates server-side authz: `createVendor` is now in
  // `_ADMIN_OFFICE_POSTS`, so the dispatcher has already verified the SIGNED
  // token's role before this function is entered. The check was not just
  // redundant, it was weaker — it trusted a `employeeId` from the request
  // body, which any caller could have set to an admin's id.
  //
  // `employeeId` is still accepted and ignored so an older cached client
  // sending it doesn't 400. Nothing reads it.
  //
  // This also removes one of the Airtable Employees reads that Stage 4 of the
  // employees migration would otherwise have had to port.

  // ── Validate ──
  const trimmedName = String(name || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Vendor Name is required." });

  // ⚠ FAILS CLOSED without a database — see handleCreateCompany for the full
  // reasoning. Neon owns expense_vendors and every read of it is Neon-first, so
  // an Airtable-only vendor would be invisible to the picker that created it.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't add a vendor right now — the database is unavailable. Try again in a moment." });
  }

  // ── Duplicate-name guard (case-insensitive), Neon-first ──
  // Backed by `expense_vendors_name_unique` (db/schema/053) so a race that
  // slips past this check still cannot produce two vendors with one name.
  let existingId = null, existingName = null;
  const dup = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS id, name FROM expense_vendors
      WHERE lower(btrim(name)) = lower(btrim($1)) LIMIT 1`, [trimmedName]);
  if (dup?.rows?.length) {
    existingId = dup.rows[0].id;
    existingName = dup.rows[0].name;
  } else if (!dup || dup.error) {
    const safeName = escapeFormulaString(trimmedName.toLowerCase());
    const existing = await fetchAll("Vendors", { filter: `LOWER({Vendor Name})="${safeName}"` });
    if (existing.length > 0) {
      existingId = existing[0].id;
      existingName = existing[0].fields["Vendor Name"];
    }
  }
  if (existingId) {
    return resp(409, {
      ok: false,
      error: `A vendor named "${existingName}" already exists.`,
      existingId
    });
  }

  // ── Build fields + create ──
  const fields = {};
  fields["fldcguWbBXsbSyj2B"] = trimmedName;                        // Vendor Name
  fields["fldIM0IjHibKlpz5S"] = true;                                // Active
  if (phone && String(phone).trim()) fields["fldMmOsK1riQu1yfV"] = String(phone).trim();
  if (email && String(email).trim()) fields["fldAUaXdu6HWvTn5V"] = String(email).trim();
  if (chargesSalesTax === true)      fields["fldB4AUNSsP3Gyuhj"] = true;

  // ── NEON FIRST (identity cutover slice 1, db/schema/053) ─────────────────
  // Reversed on 2026-08-21. It used to POST Airtable first because expenses
  // store the vendor's rec id — but expenses store it as TEXT, not a link, so a
  // uuid there is merely unfamiliar rather than invalid. Nothing 422s.
  const rows = await neonWrite("vendor.create",
    `INSERT INTO expense_vendors (name, phone, email, charges_sales_tax, active, synced_at)
     VALUES ($1,$2,$3,$4,true,now())
     RETURNING id`,
    [trimmedName,
     phone && String(phone).trim() ? String(phone).trim() : null,
     email && String(email).trim() ? String(email).trim() : null,
     chargesSalesTax === true]);
  const neonId = rows?.[0]?.id;

  // Best-effort mirror. See handleCreateCompany for why stamping the rec id
  // back is safe on this table and must not be copied to one with R2 files.
  const data = await mirrorToAirtable("createVendor", () =>
    atFetch(`${encodeURIComponent("Vendors")}`, {
      method: "POST",
      body: JSON.stringify({ fields })
    }));

  if (data?.id && neonId) {
    await neonWrite("vendor.stampAirtableId",
      `UPDATE expense_vendors SET airtable_id = $2, synced_at = now() WHERE id = $1`,
      [neonId, data.id]).catch((e) =>
        console.error(`createVendor: rec id not stamped, vendor is Neon-only — ${e?.message || e}`));
  }

  return resp(200, {
    ok: true,
    vendor: {
      id:              data?.id || String(neonId),
      name:            data?.fields?.["Vendor Name"] || trimmedName,
      phone:           data?.fields?.["Primary Phone"] || (phone ? String(phone).trim() : ""),
      email:           data?.fields?.["Primary Email"] || (email ? String(email).trim() : ""),
      chargesSalesTax: data?.fields?.["Charges Sales Tax"] === true || chargesSalesTax === true
    }
  });
}

// ── Create a company / contractor ──────────────────────────────────────────
// Closes a real gap rather than adding a nicety. Nothing in either app could
// create a company: `handleCompanies` and `handleListContractors` were two
// reads and nothing else. With nobody opening Airtable any more, a new
// contractor had NO route into the system — and `handleCreateJob` REQUIRES a
// contractorId, so the first new customer was a hard stop on creating their job.
// Found while moving Companies to Neon (item 06, slice 3).
//
// AIRTABLE FIRST, deliberately, like every other create in this file: jobs link
// to the company by rec id (`fldWsdLkqmuZLGvfa`), so the record has to exist
// there before anything can point at it. Neon is then kept in step in the same
// request, because handleCompanies/handleListContractors now read Neon — an
// Airtable-only create would be invisible to the picker that just asked for it.
//
// `activeContractor` defaults TRUE: the only way to reach this is the "+ Add new
// contractor" row on the New Project form, and a contractor that does not appear
// in the picker it was created from would be a bug, not a feature.
async function handleCreateCompany(body) {
  const { name, phone, email, billingAddress, activeContractor } = body || {};
  const trimmedName = String(name || "").trim();
  if (!trimmedName) return resp(400, { ok: false, error: "Company name is required." });

  // ⚠ FAILS CLOSED without a database, and that is the point of slice 1.
  // Neon owns companies now, and every read of them is Neon-first — so writing
  // Airtable alone would create a company that is INVISIBLE to the picker that
  // asked for it, permanently, because nothing back-fills this table. A clear
  // "try again" beats a company nobody can select. Same contract as neonWrite.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't add a company right now — the database is unavailable. Try again in a moment." });
  }

  // Duplicate guard, case-insensitive, matching handleCreateVendor. Returns the
  // existing id so the client can offer to select it instead of creating a
  // near-duplicate — which is exactly how "Wolff Brothers" and "Wolff Bros"
  // came to exist in two different tables.
  //
  // ⚠ MOVED TO NEON with the create below (slice 1). Asking Airtable was wrong
  // twice over once the create stopped depending on it: an Airtable outage threw
  // here and defeated the whole point of the reversal, and it could not see a
  // company that had gone native. It now backs onto `companies_name_unique`
  // (db/schema/053), so a race that slips past this check still cannot produce
  // two companies with one name.
  let existingId = null, existingName = null;
  const dup = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS id, name FROM companies
      WHERE lower(btrim(name)) = lower(btrim($1)) LIMIT 1`, [trimmedName]);
  if (dup?.rows?.length) {
    existingId = dup.rows[0].id;
    existingName = dup.rows[0].name;
  } else if (!dup || dup.error) {
    const safe = escapeFormulaString(trimmedName);
    const existing = await fetchAll("Companies", { filter: `LOWER({Company Name})=LOWER("${safe}")` });
    if (existing.length > 0) {
      existingId = existing[0].id;
      existingName = existing[0].fields["Company Name"];
    }
  }
  if (existingId) {
    return resp(409, {
      ok: false,
      error: `A company named "${existingName}" already exists.`,
      existingId,
    });
  }

  const fields = {};
  fields["fldA30AUOUbarysdp"] = trimmedName;                       // Company Name
  fields["fldWzDYqRUShxXUKW"] = activeContractor !== false;        // Active Contractor
  if (phone          && String(phone).trim())          fields["fld55CKQXmThbLIAK"] = String(phone).trim();
  if (email          && String(email).trim())          fields["fldR2oOqbKx6uZtuH"] = String(email).trim();
  if (billingAddress && String(billingAddress).trim()) fields["fldwpTpCF10CObP35"] = String(billingAddress).trim();

  // ── NEON FIRST (identity cutover slice 1, db/schema/053) ─────────────────
  // Reversed on 2026-08-21. It used to POST Airtable, take the rec id, then
  // mirror — so an Airtable outage meant the company existed NOWHERE and the
  // picker that asked for it came back empty. Now the row is real the moment
  // Neon has it, and Airtable is best-effort.
  //
  // ⚠ THE MIRROR STAYS, and that is not laziness. `createJobRecord` posts
  // `Contractor: ["rec…"]` — an Airtable LINKED-RECORD field — so a company with
  // no rec id cannot be attached to a job until jobs go native in slice 6.
  // Companies keep minting rec ids until then. See db/schema/053.
  const rows = await neonWrite("company.create",
    `INSERT INTO companies (name, primary_phone, primary_email,
                            billing_address, active_contractor, synced_at)
     VALUES ($1,$2,$3,$4,$5, now())
     RETURNING id`,
    [trimmedName,
     phone          && String(phone).trim()          ? String(phone).trim()          : null,
     email          && String(email).trim()          ? String(email).trim()          : null,
     billingAddress && String(billingAddress).trim() ? String(billingAddress).trim() : null,
     activeContractor !== false]);
  const neonId = rows?.[0]?.id;

  // ⚠ Stamping `airtable_id` afterwards CHANGES THE HANDLE from uuid to rec id.
  // That is safe here and nowhere near universal: companies have no R2 objects
  // keyed on their handle, and the response below is sent AFTER the stamp, so
  // the client never holds the uuid. Do NOT copy this to a table with files.
  const data = await mirrorToAirtable("createCompany", () =>
    atFetch(`${encodeURIComponent("Companies")}`, {
      method: "POST",
      body: JSON.stringify({ fields }),
    }));

  if (data?.id && neonId) {
    await neonWrite("company.stampAirtableId",
      `UPDATE companies SET airtable_id = $2, synced_at = now() WHERE id = $1`,
      [neonId, data.id]).catch((e) =>
        console.error(`createCompany: rec id not stamped, company is Neon-only — ${e?.message || e}`));
  }

  return resp(200, { ok: true, company: {
    id: data?.id || String(neonId), name: trimmedName,
    primaryPhone: String(phone || "").trim(),
    primaryEmail: String(email || "").trim(),
    _airtableMirrored: !!data?.id,
  } });
}

async function handleListContractors() {
  // Neon-first, moving with handleCompanies above — same table, same commit.
  // ⚠ The 60-second Cache-Control stays: this backs a picker that renders on
  // every job form, and the list changes a handful of times a year.
  if (neonEnabled()) {
    const q = await neonQuery(
      // Dual handle — see handleCompanies (cutover slice 1, db/schema/053).
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id, name, primary_phone, primary_email
         FROM companies WHERE active_contractor AND coalesce(name,'') <> '' ORDER BY name`);
    if (q?.rows?.length) {
      return resp(200, { ok: true, _source: "neon", _ms: q.ms, contractors: q.rows.map(r => ({
        id:           r.airtable_id,
        name:         r.name || "",
        primaryPhone: r.primary_phone || "",
        primaryEmail: r.primary_email || "",
      })) }, { "Cache-Control": "public, max-age=60" });
    }
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`listContractors: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
// ── ONE-OFF LOADER: Airtable Contacts → Neon (db/schema/048) ──────────────
// Runs INSIDE the function rather than from a laptop because the local Airtable
// PAT is scoped to the sandbox base and 403s on production — only Netlify holds
// a prod key. Same reasoning as copyLiftPhotosToR2 and friends, and the same
// reason the inventory loader lives here too.
//
// Idempotent by construction: ON CONFLICT (airtable_id) DO UPDATE, so re-running
// is a refresh, not a duplicate. Run it again any time Airtable drifts ahead.
//
// ⚠ multipleSelects arrive as ARRAYS. They are joined to the same display string
// `g()` produces on the read path, because the whole point of the flip is that
// nothing on screen changes.
async function handleBackfillContacts() {
  if (!neonEnabled()) return resp(400, { ok: false, error: "Neon is not configured." });

  const nz    = (v) => { const s = String(v ?? "").trim(); return s || null; };
  const multi = (v) => (Array.isArray(v) ? (v.filter(Boolean).join(", ") || null) : nz(v));
  const link  = (v) => (Array.isArray(v) && v.length
    ? (typeof v[0] === "string" ? v[0] : v[0]?.id || null) : null);

  // Chunked so one oversized statement can't blow the parameter limit; 100 rows
  // × 12 columns is comfortably inside Postgres's 65535 bound.
  async function upsert(label, rows, cols, sets) {
    let done = 0;
    for (let i = 0; i < rows.length; i += 100) {
      const chunk = rows.slice(i, i + 100);
      const params = [];
      const tuples = chunk.map((vals) => {
        const start = params.length;
        params.push(...vals);
        return `(${vals.map((_, k) => `$${start + k + 1}`).join(",")})`;
      });
      await neonWrite(label,
        `INSERT INTO ${label.split(".")[0]} (${cols.join(",")}) VALUES ${tuples.join(",")}
         ON CONFLICT (airtable_id) DO UPDATE SET ${sets}, synced_at = now()`, params);
      done += chunk.length;
    }
    return done;
  }

  // ⚠ A SECOND PASS, KEYED BY FIELD ID, purely for the two Google person ids
  // (db/schema/049). They are Make-owned sync fields with no name this codebase
  // has ever verified, so reading them through `F.*` would be guessing. One
  // extra pass over 239 records is nothing for a one-off loader, and it keeps
  // the rest of the mapping on the by-name convention the file uses everywhere.
  const G1 = "fld7baYOGRf3mmdl1", G2 = "fldZ4H2ob1lcOmZDp";
  const googleById = new Map();
  try {
    for (const r of await fetchAll(TABLES.contacts, { byFieldId: true })) {
      const f = r.fields || {};
      if (f[G1] || f[G2]) googleById.set(r.id, [nz(f[G1]), nz(f[G2])]);
    }
  } catch (e) {
    // Not fatal: the contact data still loads, the ids just stay unfilled and
    // this can be re-run. Losing the whole backfill over them would be worse.
    console.error(`backfillContacts: Google id pass failed — ${e?.message || e}`);
  }

  const cRecs = await fetchAll(TABLES.contacts, {});
  const cRows = cRecs.map((r) => {
    const f = r.fields || {};
    const [g1, g2] = googleById.get(r.id) || [null, null];
    return [r.id, nz(f[F.contact.firstName]), nz(f[F.contact.lastName]),
            nz(f[F.contact.primaryPhone]), nz(f[F.contact.primaryEmail]),
            link(f[F.contact.company]), multi(f[F.contact.role]),
            nz(f[F.contact.street]), nz(f[F.contact.city]), nz(f[F.contact.state]),
            nz(f[F.contact.zip]), f[F.contact.active] !== false, g1, g2];
  });
  const contacts = await upsert("contacts.backfill", cRows,
    ["airtable_id","first_name","last_name","primary_phone","primary_email",
     "company_airtable_id","role","street","city","state","zip","active",
     "google_person_id_1","google_person_id_2"],
    `first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
     primary_phone=EXCLUDED.primary_phone, primary_email=EXCLUDED.primary_email,
     company_airtable_id=EXCLUDED.company_airtable_id, role=EXCLUDED.role,
     street=EXCLUDED.street, city=EXCLUDED.city, state=EXCLUDED.state,
     zip=EXCLUDED.zip, active=EXCLUDED.active,
     google_person_id_1=COALESCE(EXCLUDED.google_person_id_1, contacts.google_person_id_1),
     google_person_id_2=COALESCE(EXCLUDED.google_person_id_2, contacts.google_person_id_2)`);

  const pRecs = await fetchAll(TABLES.powerContacts, {});
  const pRows = pRecs.map((r) => {
    const f = r.fields || {};
    return [r.id, nz(f[F.powerContact.firstName]), nz(f[F.powerContact.lastName]),
            nz(f[F.powerContact.cellPhone]), nz(f[F.powerContact.officePhone]),
            nz(f[F.powerContact.email]), link(f[F.powerContact.powerCompanyLink]),
            multi(f[F.powerContact.companyName]), multi(f[F.powerContact.jobRoles]),
            nz(f[F.powerContact.notes]), f[F.powerContact.active] !== false];
  });
  const powerContacts = await upsert("power_contacts.backfill", pRows,
    ["airtable_id","first_name","last_name","cell_phone","office_phone","email",
     "power_company_airtable_id","power_company_name","job_roles","notes","active"],
    `first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name,
     cell_phone=EXCLUDED.cell_phone, office_phone=EXCLUDED.office_phone,
     email=EXCLUDED.email,
     power_company_airtable_id=EXCLUDED.power_company_airtable_id,
     power_company_name=EXCLUDED.power_company_name, job_roles=EXCLUDED.job_roles,
     notes=EXCLUDED.notes, active=EXCLUDED.active`);

  // Resolve the FKs once the rows exist. Kept separate from the upserts so a
  // company that has not been loaded yet leaves a NULL rather than failing the
  // whole batch — the link id is stored either way and is what the reads use.
  await neonWrite("contacts.linkCompany",
    `UPDATE contacts c SET company_id = co.id
       FROM companies co WHERE (co.airtable_id = c.company_airtable_id OR co.id::text = c.company_airtable_id)
        AND c.company_id IS DISTINCT FROM co.id`);
  await neonWrite("power_contacts.linkCompany",
    `UPDATE power_contacts pc SET power_company_id = p.id
       FROM power_companies p WHERE (p.airtable_id = pc.power_company_airtable_id OR p.id::text = pc.power_company_airtable_id)
        AND pc.power_company_id IS DISTINCT FROM p.id`);

  return resp(200, { ok: true, contacts, powerContacts });
}

async function handleListContactsByCompany(params) {
  const companyId = String(params?.companyId || "").trim();
  if (!companyId) return resp(400, { ok: false, error: "Missing companyId." });

  // ── NEON-FIRST (item 06, final slice — db/schema/048) ────────────────────
  // One query replaces a fetchAll of the ENTIRE Contacts table on every open of
  // the New Project picker. The partition into own/other happens here for the
  // same reason it did against Airtable: the caller wants the contractor's own
  // people first and everyone else only when they type.
  //
  // ⚠ COALESCE in the sort, not a bare lower(). Airtable's path sorted
  // `(lastName || "")`, so a contact with no surname ("Sarge") sorted as an
  // empty string; a NULL in Postgres would sort to the end instead and quietly
  // reorder the list.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT c.airtable_id, c.first_name, c.last_name, c.primary_phone,
              c.primary_email, c.role, c.street, c.city, c.state, c.zip,
              c.company_airtable_id, co.name AS company_name
         FROM contacts c
         LEFT JOIN companies co ON co.airtable_id = c.company_airtable_id
        WHERE c.active
        ORDER BY lower(coalesce(c.last_name, '')), lower(coalesce(c.first_name, ''))`);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      const shapeN = (r) => ({
        id:           s(r.airtable_id),
        firstName:    s(r.first_name),
        lastName:     s(r.last_name),
        primaryPhone: s(r.primary_phone),
        primaryEmail: s(r.primary_email),
        role:         s(r.role),
        street:       s(r.street),
        city:         s(r.city),
        state:        s(r.state),
        zip:          s(r.zip),
      });
      const own = [], other = [];
      for (const r of q.rows) {
        if (r.company_airtable_id === companyId) own.push(shapeN(r));
        else other.push({ ...shapeN(r), companyId: s(r.company_airtable_id),
                          companyName: s(r.company_name) });
      }
      return resp(200, { ok: true, contacts: own, otherContacts: other,
                         _source: "neon", _ms: q.ms });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`listContactsByCompany: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  const records = await fetchAll(TABLES.contacts, {});

  // Role is multipleSelects — join for display.
  const shape = (r) => {
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
  };
  const byName = (a, b) => {
    const ln = a.lastName.toLowerCase().localeCompare(b.lastName.toLowerCase());
    if (ln !== 0) return ln;
    return a.firstName.toLowerCase().localeCompare(b.firstName.toLowerCase());
  };
  const isActive = (r) => r.fields[F.contact.active] !== false;
  const companyLinks = (r) => {
    const links = r.fields[F.contact.company];
    return Array.isArray(links) ? links : [];
  };

  const contacts = records
    .filter(r => companyLinks(r).includes(companyId) && isActive(r))
    .map(shape)
    .sort(byName);

  // ── EVERY OTHER ACTIVE CONTACT, so the picker can find a person filed under
  // a different company. The real case: a customer first entered under a GC
  // rings up directly, the new job goes under Misc Jobs, and his details are
  // invisible because he is filed under the GC. Retyping them by hand creates
  // a second record for one person, and then his phone number is wrong in one
  // of two places.
  //
  // This costs no extra Airtable traffic — the fetchAll above has always pulled
  // the WHOLE Contacts table and filtered in memory. 239 contacts today.
  //
  // Company NAMES come from Neon (item 06 slice 3 owns `companies`), not from a
  // lookup on the Contacts record: `F.*` is read-by-name and there is no
  // verified name for that lookup field, so resolving by rec id is the honest
  // route. Fails soft — no Neon means no label, not no contacts.
  let nameByAtId = new Map();
  if (neonEnabled()) {
    const q = await neonQuery(
      // ⚠ The `WHERE airtable_id IS NOT NULL` that used to be here is GONE.
      // After db/schema/053 a company can be native, and filtering those out
      // would leave its contacts unlabelled — the mirror image of the bug
      // `createPowerCompany` shipped on 2026-08-12, where a new utility was
      // invisible to the picker that created it.
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id, name FROM companies`);
    if (q?.rows) nameByAtId = new Map(q.rows.map(r => [r.airtable_id, r.name || ""]));
    else console.error(`listContactsByCompany: company names unavailable, rows will be unlabelled: ${q?.error || "no rows"}`);
  }

  const otherContacts = records
    .filter(r => !companyLinks(r).includes(companyId) && isActive(r))
    .map(r => {
      const firstCompany = companyLinks(r)[0] || "";
      return { ...shape(r), companyId: firstCompany, companyName: nameByAtId.get(firstCompany) || "" };
    })
    .sort(byName);

  return resp(200, { ok: true, contacts, otherContacts });
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

  // ── NEON FIRST (identity cutover slice 1) ────────────────────────────────
  // `contacts.airtable_id` was ALREADY nullable — this create was Airtable-first
  // out of habit, not constraint, which is why it is one of the cheapest moves
  // in the cutover. Reversed 2026-08-21.
  //
  // The read it feeds (`handleListContactsByCompany`) has been Neon-first since
  // item 06, so an Airtable-only contact would be invisible to the picker that
  // just created it — the bug this project has been bitten by five times, and
  // the reason the mirror was added here in the first place. Failing closed is
  // the honest version of that same protection.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't add a contact right now — the database is unavailable. Try again in a moment." });
  }

  const rows = await neonWrite("contact.create",
    `INSERT INTO contacts (first_name, last_name, primary_phone,
                           primary_email, company_airtable_id,
                           company_id, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,(SELECT id FROM companies WHERE airtable_id=$5 OR id::text=$5),true,now())
     RETURNING id`,
    [firstName || null, lastName || null, primaryPhone || null,
     primaryEmail || null, companyId]);
  const neonId = rows?.[0]?.id;

  // ⚠ The mirror can only run when the company still has a rec id — `Contact`'s
  // company field is an Airtable LINK, and a uuid in it 422s the create. A
  // native company therefore yields a native-only contact, which is correct and
  // consistent: both are already invisible to Airtable by then.
  const data = /^rec/.test(companyId)
    ? await mirrorToAirtable("createContact", () =>
        atFetch(`${encodeURIComponent(TABLES.contacts)}`, {
          method: "POST",
          body: JSON.stringify({ fields })
        }))
    : null;

  if (data?.id && neonId) {
    await neonWrite("contact.stampAirtableId",
      `UPDATE contacts SET airtable_id = $2, synced_at = now() WHERE id = $1`,
      [neonId, data.id]).catch((e) =>
        console.error(`createContact: rec id not stamped, contact is Neon-only — ${e?.message || e}`));
  }

  const f = data?.fields || {};
  return resp(200, {
    ok: true,
    contact: {
      id:           data?.id || String(neonId),
      firstName:    f[F.contact.firstName]    || firstName,
      lastName:     f[F.contact.lastName]     || lastName,
      primaryPhone: f[F.contact.primaryPhone] || primaryPhone,
      primaryEmail: f[F.contact.primaryEmail] || primaryEmail
    }
  });
}

// Either-form id resolvers for the two inspection dimension tables, same shape
// and same reason as resolveGeneratorIds: once a read is Neon-first the client
// legitimately holds a uuid, but every Airtable linked-record write still needs
// the rec id. Returning BOTH lets a handler write both stores from one lookup.
async function resolveInspectionIds(table, rawId) {
  const id = String(rawId || "").trim();
  if (!id) return { rec: null, neon: null };
  const isUuid = /^[0-9a-f-]{36}$/i.test(id);
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT id, airtable_id FROM ${table} WHERE id::text = $1 OR airtable_id = $1`, [id]);
    const r = q?.rows?.[0];
    if (r) return { rec: r.airtable_id || null, neon: r.id };
  }
  return { rec: isUuid ? null : id, neon: isUuid ? id : null };
}
const resolveAgencyIds    = (id) => resolveInspectionIds("inspection_agencies", id);
const resolveInspectorIds = (id) => resolveInspectionIds("inspection_contacts", id);

async function handleGetInspectionAgencies() {
  // NOTE: no Active filter, matching the Airtable path exactly. `Active` on
  // these tables is the Make contact-sync trigger, not a "show in picker" flag,
  // so filtering on it here would quietly hide agencies from the picker.
  // ⚠ RETURNS THE AIRTABLE REC ID, NOT THE NEON UUID — deliberately, and it is
  // the opposite of what every other flip in this slice does.
  //
  // This picker's output is consumed by handleUpdateJobInspection, which writes
  // it into the JOBS table as an Airtable LINKED RECORD. Jobs has not migrated.
  // And mapJobFromNeon already hands the job's currently-selected agency back as
  // `inspection_agency_at_id` — a rec id. Emit uuids here and the picker's
  // options stop matching the job's stored value, so nothing preselects.
  //
  // So: read from Neon (fast, no substring matching), keep rec ids as the id
  // CURRENCY until Jobs itself moves. Falls back to the uuid only for an agency
  // with no Airtable twin, which can only be one created since the last mirror.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(airtable_id, id::text) AS id, name FROM inspection_agencies
        WHERE coalesce(name,'') <> '' ORDER BY name ASC`);
    if (q?.rows) {
      return resp(200, {
        ok: true,
        agencies: q.rows.map(r => ({ id: r.id, name: r.name })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`getInspectionAgencies: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

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

  // ── NEON-FIRST, fails CLOSED ──────────────────────────────────────────
  // getInspectionAgencies reads Neon and there are already 15 rows there, so it
  // never falls through — an Airtable-only agency would simply never appear in
  // the picker.
  const rows = await neonWrite("inspectionAgency.insert",
    `INSERT INTO inspection_agencies (name, phone, email, scheduling_link, notes, active)
     VALUES ($1, $2, $3, $4, $5, true) RETURNING id`,
    [trimmedName,
     phone          && String(phone).trim()          ? String(phone).trim()          : null,
     email          && String(email).trim()          ? String(email).trim()          : null,
     schedulingLink && String(schedulingLink).trim() ? String(schedulingLink).trim() : null,
     notes          && String(notes).trim()          ? String(notes)                 : null]);
  const neonAgencyId = rows?.[0]?.id;
  if (!neonAgencyId) return resp(500, { ok: false, error: "Agency was not written to Neon." });

  const data = await mirrorToAirtable("createInspectionAgency", () =>
    atFetch(`${encodeURIComponent(TABLES.inspectionAgencies)}`,
      { method: "POST", body: JSON.stringify({ fields }) }));

  if (data?.id) {
    await mirrorToAirtable("createInspectionAgency.stamp", () =>
      neonWrite("inspectionAgency.stampAirtableId",
        `UPDATE inspection_agencies SET airtable_id = $2 WHERE id = $1`, [neonAgencyId, data.id]));
  }

  return resp(200, {
    ok: true,
    agency: {
      // Rec id when the mirror succeeded, so the new agency matches the id
      // currency the picker and handleUpdateJobInspection use.
      id:   data?.id || neonAgencyId,
      name: data?.fields?.[F.agency.name] || trimmedName
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

  // ── NEON-FIRST ────────────────────────────────────────────────────────
  // The Airtable path matches the agency by NAME via FIND (a substring test) and
  // re-verifies the link id in memory when it has one. Here the id path is plain
  // FK equality and the name path an exact match, so the substring collision
  // cannot happen on either.
  if (neonEnabled()) {
    const ids = trimmedId ? await resolveAgencyIds(trimmedId) : { neon: null };
    // ⚠ An agencyId that resolves to nothing must NOT widen to "every active
    // inspector" — that would offer inspectors belonging to other agencies.
    // Only take the name path when no id was supplied at all.
    if (!trimmedId || ids.neon) {
      // Rec id as the id currency here too — same reason as the agency picker:
      // the chosen inspector is written into JOBS as an Airtable linked record.
      const q = await neonQuery(
        `SELECT COALESCE(c.airtable_id, c.id::text) AS id, c.inspector_name, c.phone, c.email
           FROM inspection_contacts c
           LEFT JOIN inspection_agencies a ON a.id = c.agency_id
          WHERE c.active
            AND coalesce(c.inspector_name,'') <> ''
            AND ( ($1::uuid IS NOT NULL AND c.agency_id = $1::uuid)
               OR ($1::uuid IS NULL AND ($2 = '' OR lower(a.name) = lower($2))) )
          ORDER BY c.inspector_name ASC`,
        [ids.neon, trimmedName]);
      if (q?.rows) {
        return resp(200, {
          ok: true,
          inspectors: q.rows.map(r => ({
            id: r.id, name: r.inspector_name || "",
            phone: r.phone || "", email: r.email || ""
          })),
          _source: "neon", _ms: q.ms
        });
      }
      // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
      // Airtable stopped being written on 2026-08-25, so its copy is frozen —
      // falling back now serves data that is stale by construction, and serves
      // it silently. A failed read is an outage; say so and let the caller retry.
      if (q?.error) {
        console.error(`inspectorsForAgency: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
        return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
      }
    }
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

  // ⚠ WAS `startsWith("rec")`, which rejected every uuid the moment
  // getInspectionAgencies went Neon-first — the fleet/handleLogMileage bug
  // exactly. Resolve both forms instead.
  const agencyIds = await resolveAgencyIds(trimmedAgency);
  if (!agencyIds.neon && !agencyIds.rec) return resp(400, { ok: false, error: "Missing or invalid agencyId." });
  if (!trimmedFirst) return resp(400, { ok: false, error: "First Name is required." });
  if (!trimmedLast)  return resp(400, { ok: false, error: "Last Name is required." });

  // ── NEON-FIRST, fails CLOSED ──────────────────────────────────────────
  // inspectorsForAgency now reads Neon and only falls through on ZERO rows, so
  // an Airtable-only contact would be invisible on any agency that already has
  // one — the same partial-results trap as the warranties at 83e022c.
  let neonContactId = null;
  if (agencyIds.neon) {
    const rows = await neonWrite("inspectionContact.insert",
      `INSERT INTO inspection_contacts (agency_id, first_name, last_name, phone, email, active)
       VALUES ($1, $2, $3, $4, $5, true)
       RETURNING id`,
      [agencyIds.neon, trimmedFirst, trimmedLast,
       phone && String(phone).trim() ? String(phone).trim() : null,
       email && String(email).trim() ? String(email).trim() : null]);
    neonContactId = rows?.[0]?.id;
    if (!neonContactId) return resp(500, { ok: false, error: "Inspector was not written to Neon." });
  }

  const fields = {};
  fields["fldbLNgj4Msf7SeCu"] = trimmedFirst;            // First Name
  fields["fld1BOsbSTi6BkEa7"] = trimmedLast;             // Last Name
  if (agencyIds.rec) fields["fldC6CpQmQ12ABY0z"] = [agencyIds.rec];  // Inspection Agency (linked)
  fields["fldF0zIEONjKdtAIR"] = true;                     // Active (Make.com sync trigger)
  if (phone && String(phone).trim()) fields["fldh8oOPBJO0O305Y"] = String(phone).trim();
  if (email && String(email).trim()) fields["fld9auKwBoqGJIRL3"] = String(email).trim();

  // Mirror when Neon holds the row; a direct write when the agency is
  // Airtable-only (created by hand, ETL not re-run).
  const data = neonContactId
    ? await mirrorToAirtable("createInspectionContact", () =>
        atFetch(`${encodeURIComponent(TABLES.inspectionContacts)}`,
          { method: "POST", body: JSON.stringify({ fields }) }))
    : await atFetch(`${encodeURIComponent(TABLES.inspectionContacts)}`,
        { method: "POST", body: JSON.stringify({ fields }) });

  if (neonContactId && data?.id) {
    await mirrorToAirtable("createInspectionContact.stamp", () =>
      neonWrite("inspectionContact.stampAirtableId",
        `UPDATE inspection_contacts SET airtable_id = $2 WHERE id = $1`, [neonContactId, data.id]));
  }

  const f = data?.fields || {};
  return resp(200, {
    ok: true,
    inspector: {
      // The NEON id when Neon holds the row, so the picker hands back a form
      // updateJobInspection can resolve. inspector_name is a generated column
      // there, so the fallback below is only for the Airtable-only path.
      id:   data?.id || neonContactId,
      name: f[F.inspector.nameFormula] || `${trimmedFirst} ${trimmedLast}`.trim(),
      phone: f[F.inspector.phone] || (phone ? String(phone).trim() : ""),
      email: f[F.inspector.email] || (email ? String(email).trim() : "")
    }
  });
}

// ── POWER COMPANIES + POWER COMPANY CONTACTS (for Power Co. picker on Job) ──
async function handleGetPowerCompanies() {
  // ── NEON-FIRST (audit item 06) ────────────────────────────────────────────
  // 9 utilities, all active. Backs the Power Co. tab's company typeahead.
  //
  // ⚠ Its CONTACTS are deliberately still on Airtable — see the note on
  // handleGetContactsForPowerCompany. Moving a parent before its children is
  // fine here because nothing about this read depends on the contacts.
  if (neonEnabled()) {
    const q = await neonQuery(
      // Dual handle — see the note in handleCompanies (cutover slice 1).
      `SELECT COALESCE(airtable_id, id::text) AS airtable_id, name
         FROM power_companies
        WHERE coalesce(name,'') <> '' ORDER BY name`);
    if (q?.rows?.length) {
      return resp(200, { ok: true, _source: "neon", _ms: q.ms,
        companies: q.rows.map(r => ({ id: r.airtable_id, name: r.name || "" })) });
    }
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`getPowerCompanies: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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

  // ── NEON-FIRST (item 06, final slice — db/schema/048) ────────────────────
  // ⚠ THE ID WINS WHEN BOTH ARE GIVEN, matching the Airtable path exactly: it
  // used the NAME as a loose `FIND(...)` prefilter and then re-verified the
  // linked record id in memory, precisely because a name match can collide.
  // Here the id is an equality test, so the name is only consulted when there
  // is no id at all — same answer, without the substring hazard.
  //
  // `name` is a GENERATED column mirroring Airtable's "Contact Name" formula,
  // so the `.filter(c => c.name)` the old path ended with is expressed as
  // `name <> ''` rather than dropped.
  if (neonEnabled()) {
    const q = trimmedId
      ? await neonQuery(
          `SELECT COALESCE(airtable_id, id::text) AS airtable_id, name, cell_phone, office_phone, email
             FROM power_contacts
            WHERE active AND power_company_airtable_id = $1 AND btrim(name) <> ''
            ORDER BY lower(name)`, [trimmedId])
      : await neonQuery(
          `SELECT COALESCE(airtable_id, id::text) AS airtable_id, name, cell_phone, office_phone, email
             FROM power_contacts
            WHERE active AND btrim(name) <> ''
              AND lower(coalesce(power_company_name, '')) LIKE '%' || lower($1) || '%'
            ORDER BY lower(name)`, [trimmedName]);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      return resp(200, { ok: true, _source: "neon", _ms: q.ms,
        contacts: q.rows.map(r => ({
          id:          s(r.airtable_id),
          name:        s(r.name),
          cellPhone:   s(r.cell_phone),
          officePhone: s(r.office_phone),
          email:       s(r.email),
        })) });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error(`getContactsForPowerCompany: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
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

  // ── NEON FIRST (identity cutover slice 1, db/schema/053) ─────────────────
  // This table is the reason the whole "flip a read without its write" lesson
  // exists twice over: `handleGetPowerCompanies` went Neon-first in item 06
  // slice 4 while this write stayed Airtable-only, and **nothing anywhere else
  // writes `power_companies`** — no hourly sync, no loader. A utility created
  // here was invisible to the picker that created it, permanently. Fixed
  // 2026-08-12; reversed to Neon-first here so it also survives an outage.
  //
  // Jobs reference a power company by rec id in a LINKED field, so the mirror
  // stays until slice 6 — same reasoning as companies.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't add a power company right now — the database is unavailable. Try again in a moment." });
  }

  const rows = await neonWrite("powerCompany.create",
    `INSERT INTO power_companies (name, utility_region, notes, active, synced_at)
     VALUES ($1,$2,$3,$4, now())
     RETURNING id`,
    [trimmedName,
     utilityRegion && String(utilityRegion).trim() ? String(utilityRegion).trim() : null,
     notes         && String(notes).trim()         ? String(notes)                : null,
     true]);
  const neonId = rows?.[0]?.id;

  const data = await mirrorToAirtable("createPowerCompany", () =>
    atFetch(`${encodeURIComponent(TABLES.powerCompanies)}`, {
      method: "POST",
      body: JSON.stringify({ fields })
    }));

  try {
    if (data?.id && neonId) {
      await neonWrite("powerCompany.stampAirtableId",
        `UPDATE power_companies SET airtable_id = $2, synced_at = now() WHERE id = $1`,
        [neonId, data.id]);
    }
  } catch (e) {
    console.error(`createPowerCompany: rec id not stamped, company is Neon-only — ${e?.message || e}`);
  }

  return resp(200, {
    ok: true,
    company: {
      id:   data?.id || String(neonId),
      name: data?.fields?.[F.powerCompany.name] || trimmedName
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
  // ── NEON FIRST (identity cutover slice 1) ────────────────────────────────
  // `power_contacts.airtable_id` was already nullable; this was Airtable-first
  // out of habit. Reversed 2026-08-21.
  //
  // ⚠ This table is where the "invisible to the picker that created it" bug
  // actually bit: `createPowerCompany` wrote Airtable while `getPowerCompanies`
  // read Neon. Failing closed is the honest version of the same protection.
  // ⚠ `name` is GENERATED from the parts; never write it.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't add a contact right now — the database is unavailable. Try again in a moment." });
  }

  const rows = await neonWrite("powerContact.create",
    `INSERT INTO power_contacts (first_name, last_name, cell_phone,
                                 office_phone, email, power_company_airtable_id,
                                 power_company_id, job_roles, notes, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,
             (SELECT id FROM power_companies WHERE airtable_id=$6 OR id::text=$6),$7,$8,true,now())
     RETURNING id`,
    [trimmedFirst || null, (lastName && String(lastName).trim()) || null,
     trimmedCell || null, (officePhone && String(officePhone).trim()) || null,
     (email && String(email).trim()) || null, trimmedCoId,
     (Array.isArray(jobRoles) && jobRoles.length ? jobRoles.join(", ") : null),
     (notes && String(notes).trim()) || null]);
  const neonId = rows?.[0]?.id;

  // Only mirrorable while the parent utility still has a rec id — the company
  // field is an Airtable LINK and a uuid in it 422s the create. Same rule as
  // createContact.
  const data = /^rec/.test(trimmedCoId)
    ? await mirrorToAirtable("createPowerContact", () =>
        atFetch(`${encodeURIComponent(TABLES.powerContacts)}`, {
          method: "POST",
          body: JSON.stringify({ fields })
        }))
    : null;

  if (data?.id && neonId) {
    await neonWrite("powerContact.stampAirtableId",
      `UPDATE power_contacts SET airtable_id = $2, synced_at = now() WHERE id = $1`,
      [neonId, data.id]).catch((e) =>
        console.error(`createPowerContact: rec id not stamped, contact is Neon-only — ${e?.message || e}`));
  }

  return resp(200, {
    ok: true,
    contact: {
      id:         data?.id || String(neonId),
      name:       data?.fields?.[F.powerContact.nameFormula] || `${trimmedFirst} ${String(lastName || "").trim()}`.trim(),
      cellPhone:  data?.fields?.[F.powerContact.cellPhone]   || trimmedCell,
      officePhone:data?.fields?.[F.powerContact.officePhone] || (officePhone ? String(officePhone).trim() : ""),
      email:      data?.fields?.[F.powerContact.email]       || (email ? String(email).trim() : "")
    }
  });
}

// ── LABOR BILLABLE RATES (for per-job rate selector) ──────────────────────
async function handleLaborBillableRates() {
  // ── NEON-FIRST (audit item 06) ────────────────────────────────────────────
  // The table has been sitting in Neon since the ETL and was never read — the
  // audit called this one "nearly free" and it was. Same active-rate rule as
  // the Airtable path below: no end date, or an end date not yet passed.
  //
  // `rate_label` is Airtable's `Billable Rate ID`, a FORMULA rendering
  // "Regular - 75 (2026-01-01T00:00:00.000Z)". Stored rather than re-derived:
  // the exact string is what the picker displays, and reproducing that ISO
  // timestamp by hand is the kind of thing that drifts silently.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT airtable_id, rate_label, labor_type, billable_hourly_rate::float8 AS rate,
              effective_start_date::text AS start_date, effective_end_date::text AS end_date
         FROM labor_billable_rates
        WHERE effective_end_date IS NULL OR effective_end_date >= CURRENT_DATE
        ORDER BY billable_hourly_rate ASC`);
    if (q?.rows?.length) {
      return resp(200, { ok: true, _source: "neon", _ms: q.ms, rates: q.rows.map(r => ({
        id:        r.airtable_id,          // ⚠ the AIRTABLE id — handleUpdateJobBillableRate
        label:     r.rate_label || "",     //   writes it into an Airtable LINK field
        laborType: r.labor_type || "",
        rate:      r.rate ?? null,
        startDate: r.start_date || "",
        endDate:   r.end_date || "",
      })) });
    }
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`laborBillableRates: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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
  // ⚠⚠ NEON FIRST — this had the SAME bug as handleUpdateJobStatus (`ff21d46`),
  // and this one moves money. `labor_billable_rate_at_id` AND
  // `billable_hourly_rate` are both in JOB_SELECT, and the rate drives
  // `labor_revenue_tm` = hours × rate, which is T&M revenue and therefore GP.
  //
  // Airtable-only meant: set a job's billable rate, watch it look right, and
  // find it reverted on the next refresh — with the job's revenue still
  // computed at the old rate until `_jobs-sync.js` next ran.
  //
  // ⚠ It is a WINDOW, not permanent loss, and that distinction matters for how
  // hard to chase this class of bug. The hourly sync carries Airtable → Neon,
  // so the value arrives within the hour. The damage is a confusing hour in
  // which the app shows the old rate and GP is computed from it — bad, but not
  // the same as data disappearing.
  //
  // (An earlier draft of this comment blamed the GP audit's three rate-less
  // T&M jobs on exactly this. Checked: only Andy Alleman is left and he has
  // zero hours, because the owner had already fixed the other two. So the
  // window is real and this was NOT its consequence. Left in as a caution
  // against a tidy story that the data does not support.)
  //
  // The rate VALUE is denormalised onto the job because that is what
  // v_job_financials reads — Airtable's lookup does the same thing, and
  // carrying it verbatim keeps the two stores comparable.
  await neonWrite("job.updateBillableRate",
    `UPDATE jobs
        SET labor_billable_rate_at_id = $2,
            billable_hourly_rate = (SELECT billable_hourly_rate FROM labor_billable_rates
                                     WHERE airtable_id = $2 OR id::text = $2),
            synced_at = now()
     -- ⚠⚠ THE RATE LOOKUP TOOK EITHER HANDLE; THE ROW IT WROTE TO DID NOT.
     -- A bare "airtable_id = $1" updated ZERO rows on a native job and this
     -- handler still returned ok — the UI showed the rate saved. It is not
     -- cosmetic: billable_hourly_rate feeds v_job_financials, and the
     -- allocation writers SNAPSHOT the rate into
     -- material_billing_allocations / labor_billing_allocations.
     -- A NULL there bills those hours at $0 while they
     -- still print on the invoice, and the snapshot does not recompute.
     -- Same family as the automation-result miss above, found in the same sweep.
      WHERE airtable_id = $1 OR id::text = $1`, [jobId, rateId ? String(rateId) : null]);

  const fields = {};
  fields["fldcCGetfLtQW2nhm"] = rateId ? [String(rateId)] : [];
  const data = await mirrorJobPatch("updateJob", jobId, fields);
  return resp(200, { ok: true, updatedId: data?.id || jobId });
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
    clientSaveId,         // idempotency key for the CREATE path — see db/schema/064
    jobId, invoiceDate, billingMode,
    percentToBill,        // legacy — only used if totalAmount not provided
    totalAmount,          // NEW: authoritative amount from the line-item sum
    expectedRevenue,      // NEW: the frontend's view of expected rev for percent derivation
    notes, invoiceNumber, snapshot, invoiceStage
  } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // The idempotency key (db/schema/064). Accepted ONLY in canonical uuid form:
  // anything else — an older client that sends nothing, a stray string — is
  // treated as absent, so the save behaves exactly as it did before, just
  // without duplicate protection. A malformed value must never reach the
  // `$12::uuid` cast, which would 500 a save that was otherwise fine.
  const saveKey = UUID_RE.test(String(clientSaveId ?? "").trim())
    ? String(clientSaveId).trim() : null;

  const fields = {};
  Object.assign(fields, jobLink("fld1fmEklDw6y9hS2", jobId));       // Job (linked)
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

  // ── NEON FIRST (identity cutover slice 3, db/schema/055) ─────────────────
  // Reversed on 2026-08-22. This is the highest-consequence reversal in the
  // slice: an invoice is the document a customer is billed from, and under the
  // old order an Airtable outage lost it entirely while the user watched a
  // spinner. Now the invoice is real the moment Neon has it.
  //
  // ⚠ FAILS CLOSED without a database. Every invoice read is Neon-first, so an
  // Airtable-only invoice would be invisible to the app — including to the
  // "previously billed" chain that caps the next contract invoice.
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't save the invoice right now — the database is unavailable. Try again in a moment." });
  }

  const billingModeV = fields["fldljpi4PpNPIfI27"];
  const invoiceTypeV = fields["fldC4loXTBzC2UKGt"];
  const autoAllocate = fields["fldejNlo5R194TGMs"] === true;
  const pctToBill    = fields["fldiaGIu4ZzKLz6ra"];
  const displayNo    = fields["fld7FxS299iYDzMa8"];
  const snapshotText = fields["fldJT0EqxsYPUQOg1"] ?? null;
  const dateVal      = invoiceDate ? String(invoiceDate).slice(0, 10) : null;

  let row;
  if (invoiceId) {
    const rows = await neonWrite("invoice.update",
      // Status is deliberately absent: editing a Paid invoice must not flip it
      // back to Sent, which is the same rule the Airtable branch above follows.
      `UPDATE invoices SET
         job_airtable_id  = CASE WHEN $2 LIKE 'rec%' THEN $2 ELSE NULL END, job_id = (SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),
         billing_mode     = $3, invoice_type = $4, auto_allocate = $5,
         manual_labor     = 0,  manual_material = 0,
         percent_to_bill  = COALESCE($6, percent_to_bill),
         invoice_date     = COALESCE($7::date, invoice_date),
         invoice_notes    = COALESCE($8, invoice_notes),
         invoice_display_no = COALESCE($9, invoice_display_no),
         invoice_snapshot = COALESCE($10, invoice_snapshot),
         invoice_stage    = COALESCE($11, invoice_stage),
         snapshot_total   = COALESCE($12, snapshot_total),
         synced_at        = now()
        WHERE airtable_id = $1 OR id::text = $1
        RETURNING id, airtable_id`,
      [String(invoiceId), String(jobId), billingModeV, invoiceTypeV, autoAllocate,
       pctToBill ?? null, dateVal, notes || null, displayNo ?? null,
       snapshotText, invoiceStage ? String(invoiceStage) : null, totalNum]);
    if (!rows?.length) return resp(404, { ok: false, error: "That invoice no longer exists." });
    row = rows[0];
  } else {
    const rows = await neonWrite("invoice.create",
      // ⚠ `invoice_number` reproduces the Airtable formula, bug and all:
      //     {Job} & "-" & RIGHT("000" & {Invoice Sequence}, 3)
      // where `Invoice Sequence` counts the records in the invoice's own Job
      // LINK field — always 1. So every invoice ever written reads
      // `<job name>-001`, and Bethel School has two of them. It is a label, not
      // an identifier; `invoice_display_no` is the number that identifies an
      // invoice. Changing what a customer-facing document says is not a
      // cutover's job — see docs/TODO.md.
      //
      // ⚠ `invoice_total` is left NULL on purpose. It was Airtable's formula
      // column, and it is stale by construction the moment an allocation
      // changes — which is exactly why db/schema/015 built
      // `v_invoices.invoice_total_calc` and why every read in this file uses
      // that instead. Writing a second, decaying opinion of the total into a
      // money column is how a wrong number gets quoted later.
      `INSERT INTO invoices
         (job_airtable_id, job_id, invoice_number, invoice_status, invoice_type,
          billing_mode, invoice_stage, invoice_date, snapshot_total,
          manual_labor, manual_material, percent_to_bill, auto_allocate,
          invoice_display_no, invoice_notes, invoice_snapshot, client_save_id, synced_at)
       VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1),
               COALESCE((SELECT name FROM jobs WHERE airtable_id = $1 OR id::text = $1), '') || '-001',
               'Sent', $2, $3, $4, $5::date, $6, 0, 0, $7, $8, $9, $10, $11, $12::uuid, now())
       -- ⚠⚠ THE WHOLE POINT OF THIS STATEMENT (db/schema/064). A retried save —
       -- a double-click, a 504 that actually landed, a phone that lost signal —
       -- carries the SAME key and lands here, where it does nothing instead of
       -- minting a second invoice and burning another display number. Six
       -- invoices on Test 10 in a few minutes is what this prevents.
       -- ⚠ The WHERE is not optional. The index is PARTIAL (client_save_id IS
       -- NOT NULL, so the thousands of pre-existing NULL rows stay legal), and
       -- Postgres will only infer a partial index as the conflict arbiter if the
       -- statement repeats its predicate. Without it: "there is no unique or
       -- exclusion constraint matching the ON CONFLICT specification".
       ON CONFLICT (client_save_id) WHERE client_save_id IS NOT NULL DO NOTHING
       RETURNING id, airtable_id`,
      [String(jobId), invoiceTypeV, billingModeV,
       invoiceStage ? String(invoiceStage) : null, dateVal, totalNum,
       pctToBill ?? null, autoAllocate, displayNo ?? null,
       notes || null, snapshotText, saveKey]);
    row = rows?.[0];

    // No row means the conflict fired: this exact save already created an
    // invoice. Return THAT one and report it as a duplicate rather than an
    // error — the caller asked for a save and a save exists, which is success.
    //
    // ⚠ Deliberately skips the allocation claim below by returning early. The
    // first request already swept the job's unlinked allocations onto this
    // invoice; running it again would find nothing, but the early return also
    // keeps the Airtable mirror from being POSTed twice.
    if (!row && saveKey) {
      const dup = await neonQuery(
        `SELECT COALESCE(airtable_id, id::text) AS id, invoice_display_no
           FROM invoices WHERE client_save_id = $1::uuid`, [saveKey]);
      const hit = dup?.rows?.[0];
      if (hit) {
        console.log(`saveInvoice: duplicate save ${saveKey} — returning invoice ${hit.invoice_display_no}`);
        return resp(200, { ok: true, id: hit.id, duplicate: true,
                           displayNumber: hit.invoice_display_no ?? null });
      }
    }
    if (!row) return resp(502, { ok: false, error: "Couldn't save the invoice. Please try again." });
  }

  // The mirror. Best-effort: the invoice exists either way.
  //
  // ⚠ No R2 keys are derived from an invoice handle — invoice PDFs go to pCloud
  // from the browser, and scenario 4723276 has taken its folder path from the
  // PAYLOAD since slice 2.5, so nothing downstream re-reads this rec id. That is
  // what makes the stamp below safe here.
  const recId = row.airtable_id;
  let data = null, stamped = false;
  if (invoiceId && recId) {
    data = await mirrorToAirtable("saveInvoice.update", () =>
      atFetch(`${encodeURIComponent("Invoices")}/${recId}`, {
        method: "PATCH", body: JSON.stringify({ fields, typecast: true })
      }));
  } else if (!invoiceId) {
    data = await mirrorToAirtable("saveInvoice.create", () =>
      atFetch(`${encodeURIComponent("Invoices")}`, {
        method: "POST", body: JSON.stringify({ fields, typecast: true })
      }));
    if (data?.id) {
      try {
        await neonWrite("invoice.stampAirtableId",
          `UPDATE invoices SET airtable_id = $2, synced_at = now() WHERE id = $1`,
          [row.id, data.id]);
        stamped = true;
      } catch (e) {
        console.error(`saveInvoice: rec id not stamped, invoice is Neon-only — ${e?.message || e}`);
      }
    }
  }
  // Carry Airtable's computed columns back, but ONLY onto a row we know carries
  // that rec id.
  //
  // ⚠⚠ THE GUARD IS NOT DEFENSIVE PROGRAMMING, IT IS A DUPLICATE-INVOICE BUG.
  // `syncInvoiceToNeon` is an INSERT … ON CONFLICT (airtable_id). If the POST
  // succeeded and the STAMP then failed, this row's airtable_id is still NULL,
  // nothing conflicts, and the upsert writes a SECOND invoice for the same
  // work — one native, one mirrored, both real, both billable.
  if (data?.id && (recId || stamped)) await syncInvoiceToNeon(data);

  // Claim the job's unlinked allocations onto this invoice. Was two Airtable
  // automations (wflOcxtmkzdxKMVQW labor, wfl7bzJpZY9kcJ27i material), both
  // triggered by `Auto Allocate?` being ticked.
  //
  // Gated on the same flag this handler just wrote rather than on billingMode,
  // so the condition stays identical to the automation's: CONTRACT invoices set
  // it false a few lines up, because a contract invoice bills a percentage of
  // the contract and must NOT sweep up time-and-material allocations.
  //
  // ⚠ BOTH handles are passed (slice 3). The uuid is what the allocation rows
  // and `v_invoices` key on and always exists; the rec id is only for the
  // Airtable link field and is NULL on a native invoice. Passing the rec id
  // alone — as this did until 2026-08-22 — would attach nothing at all to a
  // native invoice, and it would print with no labor and no material on it.
  let allocations;
  if (fields["fldejNlo5R194TGMs"] === true) {
    try {
      allocations = await attachAllocationsToInvoice(
        atFetch, { id: row.id, airtableId: data?.id || recId || null }, jobId);
    } catch (e) {
      // The invoice is saved either way. An unattached allocation shows up as a
      // total that reads LOW, which is visible and re-fixable by saving again —
      // unlike losing the invoice, which is not.
      console.error(`saveInvoice: allocation attach failed — ${e?.message || e}`);
      allocations = { attached: 0, error: String(e?.message || e) };
    }
  }
  return resp(200, { ok: true, id: data?.id || recId || String(row.id),
                     updated: !!invoiceId, _airtableMirrored: !!(data?.id || recId),
                     ...(allocations ? { allocations } : {}) });
}

// ── UPDATE GENERATOR ASSET ──────────────────────────────────────────────
// Editing an asset used to mean RE-RUNNING COMMISSIONING with corrected values,
// because that was the only write path that touched these fields. That is a
// workaround, not a workflow: commissioning also creates a service event and a
// set of warranties, so "fix a typo in the serial number" went through a code
// path whose job is to bring a generator into service.
//
// Neon-first, fails CLOSED, mirrors to Airtable. Accepts either id form.
// Only the fields actually sent are touched, so a partial edit never nulls the
// rest — same rule as handleUpdateTimeEntryPayroll.
async function handleUpdateGenerator(body) {
  const { generatorId } = body || {};
  if (!generatorId) return resp(400, { ok: false, error: "Missing generatorId." });

  const ids = await resolveGeneratorIds(generatorId);
  if (!ids.neon && !ids.rec) return resp(400, { ok: false, error: `Invalid generatorId: ${generatorId}` });

  // column, body key, Airtable field id, coercion
  const MAP = [
    ["brand",                   "brand",                 F.gen.brand,                 "sel"],
    ["model",                   "model",                 F.gen.model,                 "str"],
    ["kw",                      "kw",                    F.gen.kw,                    "num"],
    ["serial_number",           "serialNumber",          F.gen.serialNumber,          "str"],
    ["transfer_switch_model",   "transferSwitchModel",   F.gen.transferSwitchModel,   "str"],
    ["transfer_switch_serial",  "transferSwitchSerial",  F.gen.transferSwitchSerial,  "str"],
    ["fuel_type",               "fuelType",              F.gen.fuelType,              "sel"],
    ["install_date",            "installDate",           F.gen.installDate,           "date"],
    ["service_plan_active",     "servicePlanActive",     F.gen.servicePlanActive,     "bool"],
    ["service_interval_months", "serviceIntervalMonths", F.gen.serviceIntervalMonths, "num"],
    ["battery_install_date",    "batteryInstallDate",    F.gen.batteryInstallDate,    "date"],
    ["warranty_expiration",     "warrantyExpiration",    F.gen.warrantyExpiration,    "date"],
    ["status",                  "status",                F.gen.status,                "sel"],
    ["notes",                   "notes",                 F.gen.notes,                 "str"],
  ];

  const sets = [], vals = [ids.neon || ids.rec], fields = {};
  for (const [col, key, atField, kind] of MAP) {
    if (body[key] === undefined) continue;
    let v = body[key];
    if (kind === "num")  v = (v === "" || v === null) ? null : Number(v);
    if (kind === "bool") v = v === true;
    if (kind === "str" || kind === "sel") v = (v === null || String(v).trim() === "") ? null : String(v).trim();
    if (kind === "date") v = (v === "" || v === null) ? null : String(v).slice(0, 10);
    vals.push(v);
    sets.push(`${col} = $${vals.length}${kind === "date" ? "::date" : ""}`);
    // Airtable wants "" to clear a text field and null to clear a date; a
    // singleSelect takes the value straight through with typecast.
    fields[atField] = (kind === "date") ? v : (v === null ? "" : v);
  }
  if (!sets.length) return resp(400, { ok: false, error: "Nothing to update." });

  const rows = await neonWrite("generator.update",
    `UPDATE generators SET ${sets.join(", ")}
      WHERE id::text = $1 OR airtable_id = $1
      RETURNING id, airtable_id`, vals);
  if (!rows?.length) return resp(404, { ok: false, error: `No generator found for id ${generatorId}.` });

  const atId = rows[0].airtable_id || ids.rec;
  if (atId) {
    await mirrorToAirtable("updateGenerator", () =>
      atFetch(`${encodeURIComponent(TABLES.generators)}/${atId}`,
        { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) }));
  }
  return resp(200, { ok: true, id: rows[0].id, _source: "neon" });
}

// ── GENERATOR SERVICE CALLS (admin door onto the hourly check) ──────────────
// The work is in `_generator-service.js`; this is the manual/preview entry
// point. The same function also runs unattended on the hourly schedule inside
// qb-time-pull.js, so there is one implementation and two triggers.
//
// `{ dryRun: true }` reports exactly which jobs it WOULD create and writes
// nothing — run that first. Each job it creates consumes a PO number, and PO
// numbers cannot be handed back.
//
// ⚠ Answers 200 with `enabled:false` rather than an error when
// GENERATOR_SERVICE_CALLS is unset. That is the normal shipped state, not a
// misconfiguration, and a red error for a switch that is deliberately off would
// send someone looking for a problem that isn't there.
async function handleGeneratorServiceCheck(body) {
  const report = await runGeneratorServiceCheck(atFetch, { dryRun: body?.dryRun === true });
  return resp(report.ok === false ? 500 : 200, report);
}

// ── ADD GENERATOR SERVICE RECORD (quick-log from Generator tab) ─────────
// Keep it lightweight: no truck/parts inventory, no labor billing, just the
// observable facts a tech in the field would log on a service stop.
//
// ⚠ Logging a service is what makes the service plan RECUR. `v_generators`
// derives next_service_due from max(service_date) + interval, so this insert
// moves the due date forward, which is the signal `_generator-service.js`
// watches for. A service done but not logged here leaves the generator looking
// overdue forever and never opens the next call.
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

  // ── NEON-FIRST (migration Step 4c), writes fail CLOSED ───────────────────
  // Accepts BOTH id forms and always will: handleGenerator returns a Neon uuid
  // on its primary path and an Airtable rec id when it falls back, so the client
  // legitimately holds either. This is the same permanent contract time entries
  // took at Step 2 — not a shim.
  const idStr = String(generatorId).trim();
  const isUuid = /^[0-9a-f-]{36}$/i.test(idStr);
  if (!isUuid && !idStr.startsWith("rec")) {
    return resp(400, { ok: false, error: `Invalid generatorId: ${idStr}` });
  }

  // ⚠ THE WHITELIST IS NEW ON THIS PATH, AND IT MATTERS MORE HERE.
  // Airtable wrote Service Type with typecast:true and NO validation, so a stray
  // client value silently created a new single-select option — the exact failure
  // CLAUDE.md warns about. handleCommissionGenerator already validates against
  // SERVICE_TYPE_OPTS; this one never did. Postgres has no typecast guard at all
  // (text accepts anything), so the check has to move into the code or the column
  // becomes free-text by accident.
  const svcTypeSafe = SERVICE_TYPE_OPTS.includes(serviceType) ? serviceType : null;

  const b = (v) => (v === undefined ? false : v === true);
  const n = (v) => (v === undefined || v === null || v === "" ? null : Number(v));
  // ⚠ TWO VALUES, because $2 was doing two jobs. The COLUMN `job_airtable_id`
  // must stay rec-only — it means "the Airtable id", and a uuid in it is a lie.
  // The RESOLVE needs the raw handle, or a native job's service record files
  // itself against job_id NULL and drops off the job's service history.
  const jobRec    = jobId && String(jobId).startsWith("rec") ? String(jobId) : null;
  const jobHandle = jobId ? String(jobId).trim() : null;

  const rows = await neonWrite("generatorService.insert",
    `INSERT INTO generator_service
       (generator_id, job_airtable_id, job_id, service_date, service_type, technician,
        service_plan_visit, oil_changed, oil_filter_changed, air_filter_changed,
        spark_plugs_changed, battery_tested, battery_replaced, load_test_performed,
        firmware_checked, exercise_checked, trouble_codes, work_performed_notes,
        parts_used, labor_hours, generator_hours)
     SELECT g.id, $2, (SELECT id FROM jobs WHERE airtable_id = $21 OR id::text = $21),
            $3::date, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19, $20
       FROM generators g
      WHERE g.id::text = $1 OR g.airtable_id = $1
     RETURNING id, generator_id`,
    [idStr, jobRec, serviceDate, svcTypeSafe, technician || null,
     b(servicePlanVisit), b(oilChanged), b(oilFilterChanged), b(airFilterChanged),
     b(sparkPlugsChanged), b(batteryTested), b(batteryReplaced), b(loadTestPerformed),
     b(firmwareChecked), b(exerciseChecked),
     troubleCodesFound ? String(troubleCodesFound) : null,
     workNotes ? String(workNotes) : null, partsUsed ? String(partsUsed) : null,
     n(laborHours), n(generatorHours), jobHandle]);

  const neonId = rows?.[0]?.id;
  // No row inserted means the SELECT matched no generator. That is a real error,
  // not a reason to write Airtable anyway: a service record that exists only in
  // the mirror is invisible to every read on the primary path. A generator
  // created by hand directly in Airtable needs db/etl/inspections-generators.mjs
  // re-run before service can be logged against it — narrow, because
  // commissioning goes through the app.
  if (!neonId) {
    return resp(404, { ok: false, error: `No generator found for id ${idStr}.` });
  }

  // Resolve the generator's Airtable id for the mirror — the client may have
  // handed us a uuid, which Airtable's linked-record field cannot accept.
  const genAt = (await neonQuery(
    `SELECT airtable_id FROM generators WHERE id = $1`, [rows[0].generator_id]))?.rows?.[0]?.airtable_id || null;

  // Build by name — typecast: true handles single-select option creation.
  const fields = {};
  if (genAt) fields["Generator"] = [genAt];
  Object.assign(fields, jobLink("Job", jobId));
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

  // Best-effort mirror. Never throws and never changes the outcome — the
  // authoritative row is already in Neon, and a failed mirror leaves Airtable
  // stale rather than losing the service record. Same contract as time entries.
  const data = await mirrorToAirtable("addGeneratorService", () =>
    atFetch(`${encodeURIComponent("Generator Service")}`, {
      method: "POST",
      body: JSON.stringify({ fields, typecast: true })
    }));

  // Stamp the Airtable id back so the two sides agree on this row's identity —
  // and so the ETL's ON CONFLICT (airtable_id) updates this row rather than
  // inserting a duplicate on the next hand-run.
  if (data?.id) {
    await mirrorToAirtable("addGeneratorService.stamp", () =>
      neonWrite("generatorService.stampAirtableId",
        `UPDATE generator_service SET airtable_id = $2 WHERE id = $1`, [neonId, data.id]));
  }

  // `id` is the NEON uuid. The client treats it as opaque.
  return resp(200, { ok: true, id: neonId, airtableId: data?.id || null });
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

  // SAFE TO FLIP, unlike the other warranty reads. This endpoint is standalone:
  // commissioning step 3 runs its OWN fetchAll against Warranty Templates and
  // never calls this handler, so minting uuids here cannot reach the Airtable
  // linked-record write that b79b9a0 was about. The only consumer is the
  // frontend picker, whose templateId goes to addWarranty — which resolves
  // either form on both sides.
  if (neonEnabled()) {
    // Mirrors buildWarrantyTemplateFilter exactly: active only, brand matched
    // case-insensitively, and a BLANK model on the template means "applies to
    // every model of this brand" (the seeded Cummins whole-house templates rely
    // on that). With no model supplied, only blank-model templates match.
    const q = await neonQuery(
      `SELECT id, template_name, brand, model, warranty_type, duration_months, notes, active
         FROM warranty_templates
        WHERE active
          AND lower(coalesce(brand,'')) = lower($1)
          AND ( coalesce(model,'') = ''
                OR ($2 <> '' AND lower(model) = lower($2)) )
        ORDER BY duration_months NULLS LAST`, [brand, model]);
    if (q?.rows) {
      return resp(200, {
        ok: true,
        templates: q.rows.map(r => ({
          id: r.id, name: r.template_name || "", brand: r.brand || "",
          model: r.model || "", warrantyType: r.warranty_type || "",
          durationMonths: r.duration_months === null ? null : Number(r.duration_months),
          notes: r.notes || "", active: r.active === true
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`getWarrantyTemplates: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

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
// ⚠ THE ID THIS RECEIVES CAN BE EITHER FORM — and getting that wrong shipped a
// live regression. handleGenerator went Neon-first, so `generator.id` became a
// uuid, and index.html:7601 passes it straight here as genIdForWarranties. The
// old body handed it to atFetch(generators/<id>), which 404s on a uuid, so every
// generator view showed "Warranty lookup failed".
//
// The `startsWith("rec")` sweep did NOT catch this: this handler never validated
// the id, it just forwarded it. The real rule is broader than that grep —
// EVERY handler receiving an id minted by a read that has been flipped needs
// checking, whether or not it validates. Apply that at 4d/4e.
async function resolveGeneratorIds(rawId) {
  const id = String(rawId || "").trim();
  if (!id) return { rec: null, neon: null };
  const isUuid = /^[0-9a-f-]{36}$/i.test(id);
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT id, airtable_id FROM generators WHERE id::text = $1 OR airtable_id = $1`, [id]);
    const r = q?.rows?.[0];
    if (r) return { rec: r.airtable_id || null, neon: r.id };
  }
  // Neon unavailable or the generator is Airtable-only (created by hand, ETL not
  // re-run). A rec id still works against Airtable; a uuid has nowhere to go.
  return { rec: isUuid ? null : id, neon: isUuid ? id : null };
}

async function handleGetWarranties(params) {
  const generatorId = (params?.generatorId || "").trim();
  if (!generatorId) return resp(400, { ok: false, error: "Missing generatorId." });

  const ids = await resolveGeneratorIds(generatorId);

  if (ids.neon && neonEnabled()) {
    const q = await neonQuery(
      `SELECT w.id, w.airtable_id, w.name, w.warranty_type, w.start_date::text AS start_date,
              w.end_date::text AS end_date, w.duration_months, w.source, w.voided,
              w.voided_reason, w.notes, w.template_id
         FROM warranties w
        WHERE w.generator_id = $1
        ORDER BY w.end_date ASC NULLS LAST`, [ids.neon]);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      return resp(200, {
        ok: true,
        warranties: q.rows.map(r => ({
          id: r.id, name: s(r.name), warrantyType: s(r.warranty_type),
          startDate: s(r.start_date), endDate: s(r.end_date),
          durationMonths: r.duration_months === null ? null : Number(r.duration_months),
          source: s(r.source), voided: r.voided === true,
          voidedReason: s(r.voided_reason), notes: s(r.notes),
          templateId: r.template_id || null
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // Zero rows is ambiguous — genuinely no warranties, or warranties that
    // commissioning step 3 wrote to Airtable only (it has not migrated). Fall
    // through when a rec id is available; Airtable answers both correctly.
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`getWarranties: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

  // A uuid with no Neon row cannot be looked up in Airtable at all — say so
  // rather than 404ing as "Generator not found", which it isn't.
  if (!ids.rec) return resp(200, { ok: true, warranties: [], _source: "neon-empty" });

  let assetId = "";
  try {
    const genRec = await atFetch(`${encodeURIComponent(TABLES.generators)}/${ids.rec}`);
    assetId = genRec?.fields?.[F.gen.assetId] || "";
  } catch (err) {
    return resp(404, { ok: false, error: "Generator not found." });
  }
  // Just-created records may not have their formula assetId computed yet
  // — return empty rather than scanning the whole Warranties table.
  if (!assetId) return resp(200, { ok: true, warranties: [] });

  const safe = escapeFormulaString(assetId);
  // Asset ids are prefix-collidable (GEN-1 vs GEN-10), so exact-per-element.
  const filter = `FIND("
${safe}
", "
" & ARRAYJOIN({${F.warranty.generator}}, "
") & "
")`;
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

  // ⚠ END DATE STAYS A JS COMPUTATION, deliberately. JS and Postgres disagree
  // about month addition (Jan 31 + 1 month) — see PLAN-job-warranty-service-log
  // §2. Computing it ONCE here and storing the same string in both places means
  // the two can never disagree; deriving it again in SQL is what would let them.
  const endDate = addMonthsToDateStr(startDate, months);
  if (!endDate) return resp(400, { ok: false, error: "Invalid startDate format (need YYYY-MM-DD)." });

  // Both ids can arrive in either form — same trap that broke getWarranties.
  const ids = await resolveGeneratorIds(generatorId);
  if (!ids.neon && !ids.rec) return resp(400, { ok: false, error: `Invalid generatorId: ${generatorId}` });

  const wType   = WARRANTY_TYPE_OPTS.includes(warrantyType) ? warrantyType : "Limited";
  const wSource = WARRANTY_SOURCE_OPTS.includes(source) ? source : "Standard";

  // ── NEON-FIRST, fails CLOSED — same contract as addGeneratorService ───────
  let neonId = null;
  if (ids.neon) {
    const rows = await neonWrite("warranty.insert",
      `INSERT INTO warranties
         (generator_id, template_id, warranty_type, start_date, end_date,
          duration_months, source, notes)
       VALUES ($1,
               (SELECT id FROM warranty_templates
                 WHERE id::text = $2 OR airtable_id = $2),
               $3, $4::date, $5::date, $6, $7, $8)
       RETURNING id`,
      [ids.neon, templateId ? String(templateId) : null, wType, startDate, endDate,
       months, wSource, notes && String(notes).trim() ? String(notes) : null]);
    neonId = rows?.[0]?.id;
    if (!neonId) return resp(500, { ok: false, error: "Warranty was not written to Neon." });
  }
  // No Neon row for this generator means it is Airtable-only (created by hand,
  // ETL not re-run). Fall back to the Airtable-only write rather than refusing —
  // unlike a service record, a warranty on an unmigrated generator is still read
  // correctly, because getWarranties falls through for exactly that case.

  const fields = {};
  if (ids.rec) fields[F.warranty.generator] = [ids.rec];
  // Warranty Type whitelist (singleSelect) — fallback "Limited" is the most
  // conservative coverage choice if a stray value somehow arrives.
  fields[F.warranty.warrantyType]   = wType;
  fields[F.warranty.startDate]      = startDate;
  fields[F.warranty.endDate]        = endDate;
  fields[F.warranty.durationMonths] = months;
  // Source whitelist (singleSelect) — fallback "Standard" is the default
  // for warranties created from manufacturer templates.
  fields[F.warranty.source]         = wSource;
  // getWarrantyTemplates IS now Neon-first, so templateId arrives as a uuid on
  // the primary path and a rec id on the fallback. Resolve back to the Airtable
  // id for the mirror rather than just guarding on the prefix — a bare guard
  // would silently DROP `Created From Template` from every mirrored warranty,
  // which is the quiet half of the b79b9a0 bug rather than the loud half.
  if (templateId) {
    const tRec = String(templateId).startsWith("rec")
      ? String(templateId)
      : (await neonQuery(`SELECT airtable_id FROM warranty_templates WHERE id::text = $1`,
          [String(templateId)]))?.rows?.[0]?.airtable_id || null;
    if (tRec) fields[F.warranty.createdFromTemplate] = [tRec];
  }
  if (notes && String(notes).trim()) fields[F.warranty.notes] = String(notes);

  // Mirror when Neon holds the row; a direct write when it does not.
  const data = neonId
    ? await mirrorToAirtable("addWarranty", () =>
        atFetch(`${encodeURIComponent(TABLES.warranties)}`, {
          method: "POST", body: JSON.stringify({ fields, typecast: true })
        }))
    : await atFetch(`${encodeURIComponent(TABLES.warranties)}`, {
        method: "POST", body: JSON.stringify({ fields, typecast: true })
      });

  if (neonId && data?.id) {
    await mirrorToAirtable("addWarranty.stamp", () =>
      neonWrite("warranty.stampAirtableId",
        `UPDATE warranties SET airtable_id = $2 WHERE id = $1`, [neonId, data.id]));
  }
  if (!neonId && data?.error) return resp(400, { ok: false, error: data.error });
  return resp(200, { ok: true, id: neonId || data.id, airtableId: data?.id || null });
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

  // ══ NEON-FIRST COMMISSIONING (migration Step 4c) ═════════════════════════
  // Was three sequential Airtable writes that could half-succeed — which is the
  // only reason this handler has a warnings[] array at all. It is now ONE
  // data-modifying CTE, and a data-modifying CTE is a single statement, so it is
  // ATOMIC: the asset, the commissioning event and the warranties all land, or
  // none of them do. Airtable becomes the mirror.
  //
  // Three Airtable round trips also disappear: the job-name fetch, the
  // FIND-inside-ARRAYJOIN scan for an existing generator, and the asset-id
  // re-read that both dup checks needed. The dup checks are now FK tests.
  const COMM_TYPE_NEON = "Install / Commissioning";
  const svcTypeNeon = SERVICE_TYPE_OPTS.includes(COMM_TYPE_NEON) ? COMM_TYPE_NEON : SERVICE_TYPE_OPTS[0];

  // ⚠ MONTH ARITHMETIC STAYS IN JS. The branch-proven CTE used make_interval,
  // but every other path in this codebase uses addMonthsToDateStr, and §5 of the
  // plan warns the two disagree (Jan 31 + 1 month). Both seeded templates are 24
  // and 60 months, which land on the same day-of-month either way, so a branch
  // test could never have exposed a divergence — a 1-month template would.
  // Templates are READ from Neon, end dates computed here, and the computed
  // values passed in. One definition of month addition, no second opinion.
  const tq = await neonQuery(
    `SELECT id, template_name, warranty_type, duration_months
       FROM warranty_templates
      WHERE active
        AND lower(coalesce(brand,'')) = lower($1)
        AND (coalesce(model,'') = '' OR ($2 <> '' AND lower(model) = lower($2)))`,
    [brand, String(model || "").trim()]);
  const neonTemplates = (tq?.rows || []).filter(t => {
    const m = Number(t.duration_months);
    if (!Number.isFinite(m) || m <= 0) {
      warnings.push(`Template "${t.template_name || t.id}" has invalid Duration Months — skipped.`);
      return false;
    }
    return true;
  });
  if (!neonTemplates.length) {
    warnings.push(`No active warranty templates found for brand "${brand}"${model ? ` / model "${model}"` : ""}.`);
  }

  const wTid    = neonTemplates.map(t => t.id);
  const wType   = neonTemplates.map(t => WARRANTY_TYPE_OPTS.includes(t.warranty_type) ? t.warranty_type : "Limited");
  const wEndArr = neonTemplates.map(t => addMonthsToDateStr(installDate, Number(t.duration_months)));
  const wMonths = neonTemplates.map(t => Number(t.duration_months));

  const numOrNull = (v) => (v === undefined || v === null || v === "" ? null : Number(v));
  const strOrNull = (v) => (v !== undefined && v !== null && String(v).trim() ? String(v).trim() : null);

  const cRows = await neonWrite("commissionGenerator.atomic",
    `WITH existing AS (
       SELECT id, airtable_id FROM generators
        WHERE ($1 <> '' AND (id::text = $1 OR airtable_id = $1))
           OR ($1 =  '' AND (job_airtable_id = $2
                            OR job_id = (SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2)))
        LIMIT 1
     ), upd AS (
       UPDATE generators g SET
         job_airtable_id = CASE WHEN $2 LIKE 'rec%' THEN $2 ELSE NULL END,
         job_id = COALESCE((SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2), g.job_id),
         brand = $3, model = COALESCE($4, g.model), kw = COALESCE($5, g.kw),
         serial_number = COALESCE($6, g.serial_number),
         transfer_switch_model = COALESCE($7, g.transfer_switch_model),
         transfer_switch_serial = COALESCE($8, g.transfer_switch_serial),
         fuel_type = COALESCE($9, g.fuel_type), install_date = $10::date,
         service_plan_active = COALESCE($11, g.service_plan_active),
         service_interval_months = COALESCE($12, g.service_interval_months),
         battery_install_date = COALESCE($13::date, g.battery_install_date),
         notes = COALESCE($14, g.notes)
       FROM existing e WHERE g.id = e.id
       RETURNING g.id, g.airtable_id
     ), ins AS (
       INSERT INTO generators
         (job_airtable_id, job_id, customer_name, brand, model, kw, serial_number,
          transfer_switch_model, transfer_switch_serial, fuel_type, install_date,
          service_plan_active, service_interval_months, battery_install_date, notes)
       SELECT CASE WHEN $2 LIKE 'rec%' THEN $2 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),
              -- Snapshot the customer, exactly as the ETL does. Without it a
              -- brand-new generator has a NULL asset_id until the hourly jobs
              -- sync catches up, i.e. no name on screen.
              (SELECT NULLIF(TRIM(COALESCE(customer_first_name,'') || ' ' ||
                                  COALESCE(customer_last_name,'')), '')
                 FROM jobs WHERE airtable_id = $2 OR id::text = $2),
              $3, $4, $5, $6, $7, $8, $9, $10::date, COALESCE($11,false), $12, $13::date, $14
        WHERE NOT EXISTS (SELECT 1 FROM existing)
       RETURNING id, airtable_id
     ), gen AS (
       SELECT id, airtable_id FROM upd UNION ALL SELECT id, airtable_id FROM ins
     ), svc AS (
       INSERT INTO generator_service
         (generator_id, job_airtable_id, job_id, service_date, service_type,
          technician, generator_hours, work_performed_notes)
       SELECT gen.id, CASE WHEN $2 LIKE 'rec%' THEN $2 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),
              $15::date, $16, $17, $18, $19
         FROM gen
        WHERE NOT EXISTS (SELECT 1 FROM generator_service gs
                           WHERE gs.generator_id = gen.id AND gs.service_type = $16)
       RETURNING id
     ), war AS (
       INSERT INTO warranties
         (generator_id, template_id, warranty_type, start_date, end_date,
          duration_months, source)
       SELECT gen.id, t.tid, t.wtype, $10::date, t.wend::date, t.months, 'Standard'
         FROM gen
         CROSS JOIN unnest($20::uuid[], $21::text[], $22::text[], $23::int[])
                    AS t(tid, wtype, wend, months)
        -- Same rule as before: skip the WHOLE step if this generator already has
        -- any warranty, so re-commissioning never piles up duplicates.
        WHERE NOT EXISTS (SELECT 1 FROM warranties w WHERE w.generator_id = gen.id)
       RETURNING id, template_id
     )
     SELECT (SELECT id FROM gen)                      AS generator_id,
            (SELECT airtable_id FROM gen)             AS generator_airtable_id,
            (SELECT count(*) FROM upd)::int           AS was_update,
            (SELECT id FROM svc)                      AS service_id,
            COALESCE((SELECT json_agg(json_build_object('id', id, 'templateId', template_id))
                        FROM war), '[]'::json)        AS warranties`,
    [String(generatorId || "").trim(), jobId, brand, strOrNull(model), numOrNull(kw),
     strOrNull(serialNumber), strOrNull(transferSwitchModel), strOrNull(transferSwitchSerial),
     strOrNull(fuelType), installDate,
     servicePlanActive === undefined ? null : servicePlanActive === true,
     numOrNull(serviceIntervalMonths), strOrNull(batteryInstallDate), strOrNull(assetNotes),
     commissioningDate || installDate, svcTypeNeon, strOrNull(technician),
     numOrNull(generatorHours), strOrNull(commissioningNotes),
     wTid, wType, wEndArr, wMonths]);

  const c = cRows?.[0];
  if (!c?.generator_id) {
    return resp(500, { ok: false, error: "Commissioning was not written to Neon — nothing was created." });
  }
  const neonGeneratorId = c.generator_id;
  const neonServiceId   = c.service_id || null;
  const neonWarranties  = c.warranties || [];
  if (!neonServiceId) warnings.push("Commissioning service record already exists — skipped re-creation.");
  if (!neonWarranties.length && neonTemplates.length)
    warnings.push("Warranties already existed for this generator — skipped re-creation.");

  let resolvedGeneratorId = c.generator_airtable_id || null;

  // ── AIRTABLE MIRROR — best-effort from here down ──────────────────────
  // Neon already holds the whole commissioning, atomically. Every failure below
  // is a WARNING, never a rollback and never a 500: the commissioning happened.
  // This is the inverse of what this handler used to be.

  // 1. The asset. PATCH when Neon already knew an Airtable id, POST when this is
  //    a brand-new generator — then stamp the id back so the two sides agree and
  //    the hand-run ETL updates this row instead of inserting a duplicate.
  if (resolvedGeneratorId) {
    const patched = await mirrorToAirtable("commission.patchAsset", () =>
      atFetch(`${encodeURIComponent(TABLES.generators)}/${resolvedGeneratorId}`, {
        method: "PATCH", body: JSON.stringify({ fields: assetFields, typecast: true })
      }));
    if (!patched?.id) warnings.push("Generator saved in Neon, but the Airtable mirror PATCH failed.");
  } else {
    const createFields = { ...assetFields };
    Object.assign(createFields, jobLink(F.gen.job, jobId));
    const created = await mirrorToAirtable("commission.createAsset", () =>
      atFetch(`${encodeURIComponent(TABLES.generators)}`, {
        method: "POST", body: JSON.stringify({ fields: createFields, typecast: true })
      }));
    if (created?.id) {
      resolvedGeneratorId = created.id;
      await mirrorToAirtable("commission.stampAsset", () =>
        neonWrite("generator.stampAirtableId",
          `UPDATE generators SET airtable_id = $2 WHERE id = $1`, [neonGeneratorId, created.id]));
    } else {
      warnings.push("Generator saved in Neon, but the Airtable mirror POST failed — re-run db/etl/inspections-generators.mjs to reconcile.");
    }
  }

  // 2. The commissioning service event — mirrored ONLY when Neon actually
  //    created one. If Neon skipped it as a duplicate, there is nothing to
  //    mirror, and the old code's separate Airtable dup check is gone with it.
  const serviceRecordId = neonServiceId;
  if (neonServiceId && resolvedGeneratorId) {
    const svcFields = {};
    svcFields[F.svc.generator]   = [resolvedGeneratorId];
    Object.assign(svcFields, jobLink(F.svc.job, jobId));
    svcFields[F.svc.serviceDate] = commissioningDate || installDate;
    svcFields[F.svc.serviceType] = svcTypeNeon;
    if (technician) svcFields[F.svc.technician] = String(technician);
    if (generatorHours !== undefined && generatorHours !== null && generatorHours !== "")
      svcFields[F.svc.generatorHours] = Number(generatorHours);
    if (commissioningNotes && String(commissioningNotes).trim())
      svcFields[F.svc.workNotes] = String(commissioningNotes);

    const svcData = await mirrorToAirtable("commission.createService", () =>
      atFetch(`${encodeURIComponent(TABLES.generatorService)}`, {
        method: "POST", body: JSON.stringify({ fields: svcFields, typecast: true })
      }));
    if (svcData?.id) {
      await mirrorToAirtable("commission.stampService", () =>
        neonWrite("generatorService.stampAirtableId",
          `UPDATE generator_service SET airtable_id = $2 WHERE id = $1`, [neonServiceId, svcData.id]));
    } else {
      warnings.push("Commissioning service record saved in Neon, but the Airtable mirror failed.");
    }
  }

  // 3. Warranties — one POST per row Neon created, each stamped back.
  const warrantyIds = [];
  for (const w of neonWarranties) {
    if (!resolvedGeneratorId) break;
    const tpl    = neonTemplates.find(t => t.id === w.templateId);
    const months = tpl ? Number(tpl.duration_months) : null;

    const wFields = {};
    wFields[F.warranty.generator]    = [resolvedGeneratorId];
    wFields[F.warranty.warrantyType] = (tpl && WARRANTY_TYPE_OPTS.includes(tpl.warranty_type))
      ? tpl.warranty_type : "Limited";
    wFields[F.warranty.startDate]    = installDate;
    if (months) {
      // Same JS computation Neon was given — not a second derivation.
      wFields[F.warranty.endDate]        = addMonthsToDateStr(installDate, months);
      wFields[F.warranty.durationMonths] = months;
    }
    wFields[F.warranty.source] = "Standard";
    // Resolve the template back to its Airtable id. A uuid in a linked-record
    // field is exactly the b79b9a0 bug, in its quiet form: no error, the link
    // just never gets written.
    const tRec = (await neonQuery(
      `SELECT airtable_id FROM warranty_templates WHERE id = $1`, [w.templateId]))?.rows?.[0]?.airtable_id;
    if (tRec) wFields[F.warranty.createdFromTemplate] = [tRec];

    const wData = await mirrorToAirtable("commission.createWarranty", () =>
      atFetch(`${encodeURIComponent(TABLES.warranties)}`, {
        method: "POST", body: JSON.stringify({ fields: wFields, typecast: true })
      }));
    if (wData?.id) {
      warrantyIds.push(wData.id);
      await mirrorToAirtable("commission.stampWarranty", () =>
        neonWrite("warranty.stampAirtableId",
          `UPDATE warranties SET airtable_id = $2 WHERE id = $1`, [w.id, wData.id]));
    } else {
      warnings.push("A warranty saved in Neon did not mirror to Airtable — re-run the ETL to reconcile.");
    }
  }

  // `generatorId` is the NEON uuid now, matching what handleGenerator returns —
  // the client holds one id form for a generator, not two depending on which
  // endpoint it came from. Every consumer resolves either form anyway.
  return resp(200, {
    ok: true,
    generatorId:    neonGeneratorId,
    airtableGeneratorId: resolvedGeneratorId || null,
    serviceRecordId,
    warrantyIds,
    warnings,
    _source: "neon"
  });
}

// ── SET INVOICE STATUS ───────────────────────────────────────────────────
// Generalized status setter (replaces the old markInvoicePaid). Accepts any
// option name; thanks to typecast: true, new options like "Disputed" get
// auto-added to the singleSelect on first use.
// Neon-first, dual handle, mirror best-effort (cutover slice 3).
// This is how an invoice is marked Paid, so it has to land in the store the All
// Invoices tab reads — which has been Neon since Step 4e.
async function handleSetInvoiceStatus(body) {
  const { invoiceId, status } = body || {};
  if (!invoiceId) return resp(400, { ok: false, error: "Missing invoiceId." });
  if (!status)    return resp(400, { ok: false, error: "Missing status." });
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Can't update the invoice right now — the database is unavailable. Try again in a moment." });
  }

  const rows = await neonWrite("invoice.setStatus",
    `UPDATE invoices SET invoice_status = $2, synced_at = now()
      WHERE airtable_id = $1 OR id::text = $1
      RETURNING COALESCE(airtable_id, id::text) AS handle, airtable_id`,
    [String(invoiceId), String(status)]);
  if (!rows?.length) return resp(404, { ok: false, error: "That invoice no longer exists." });

  const recId = rows[0].airtable_id;
  if (recId) {
    const fields = { "fldXcHqj8xqmOWeLH": status };  // Invoice Status
    await mirrorToAirtable("setInvoiceStatus", () =>
      atFetch(`${encodeURIComponent("Invoices")}/${recId}`, {
        method: "PATCH",
        body: JSON.stringify({ fields, typecast: true })
      }));
  }
  return resp(200, { ok: true, id: rows[0].handle });
}

// Backward-compat alias — old "markInvoicePaid" callers still work.
async function handleMarkInvoicePaid(body) {
  return handleSetInvoiceStatus({ invoiceId: body?.invoiceId, status: body?.status || "Paid" });
}

// ── GET NEXT INVOICE NUMBER ──────────────────────────────────────────────
async function handleGetNextInvoiceNumber() {
  // Find max "Invoice Display #" across Invoices; start at 1633 if none exist
  const START_AT = 1633;

  // ── NEON-FIRST ────────────────────────────────────────────────────────────
  // Same reasoning as the estimate counter above. Safe because every invoice
  // create AND update calls `syncInvoiceToNeon` in the same request, so the
  // display number is in Neon before this handler could ever be asked again.
  //
  // ⚠ Verified before flipping, not assumed: Airtable max 1668 across 55
  // records, Neon max 1668 across 55 rows. The 20 rows with a NULL number are
  // blank in Airtable too — MAX() ignores them exactly as the old scan did,
  // which skipped any record whose field did not parse as a number.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(MAX(invoice_display_no), 0)::int AS max_no FROM invoices`);
    if (q?.rows?.length) {
      const maxNo = Number(q.rows[0].max_no) || 0;
      return resp(200, { ok: true, nextNumber: Math.max(maxNo + 1, START_AT),
                         _source: "neon", _ms: q.ms });
    }
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): a number from FROZEN Airtable would be lower than Neon's and COLLIDE with one already issued.
    console.error(`getNextInvoiceNumber: Neon read failed, refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

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

// NEON-FIRST since migration Step 4a. The Airtable path below stays as the
// fallback and is unchanged.
//
// Worth knowing why this one was picked to go first: the old path pages THREE
// whole Airtable tables on every load — Schedule Entries, Jobs and Employees —
// purely to resolve names for the grid. Jobs and employees are already in Neon,
// so the same answer is one query with two joins.
//
// Crew is a real many-to-many (schedule_entry_crew), not an array column. It is
// the first slice to need that, and Step 4c's generator service history will too.
async function handleGetScheduleEntriesFromNeon(params) {
  const since = params?.since || "";
  const until = params?.until || "";
  const jobId = params?.jobId || "";

  const q = await neonQuery(
    `SELECT s.id::text AS id, s.title, s.entry_type, s.notes,
            s.start_date::text AS start_date, s.end_date::text AS end_date,
            j.airtable_id AS job_at_id, j.name AS job_name,
            j.contractor_name, j.status AS job_status,
            -- WARNING: THESE TWO ARRAYS ARE POSITIONALLY PAIRED BY THE CLIENT,
            -- and they used to be built with DIFFERENT filters -- ids on
            -- e.airtable_id IS NOT NULL, names on e.name IS NOT NULL.
            --
            -- For a natively-hired employee (slice 5) airtable_id is NULL, so
            -- their id was dropped while their NAME was kept. The arrays then
            -- had different lengths, and index.html renders the crew by zipping
            -- them by position (~21501, ~21805) -- so the person was missing AND
            -- everyone sorting after them was paired with the wrong id. A
            -- mis-assigned crew member is worse than an absent one.
            --
            -- Both now emit the dual handle and share ONE filter -- "this entry
            -- actually has a crew row" -- so they cannot diverge again whatever
            -- an individual column holds.
            COALESCE(array_agg(COALESCE(e.airtable_id, e.id::text) ORDER BY e.name)
                     FILTER (WHERE c.employee_id IS NOT NULL), '{}') AS crew_ids,
            COALESCE(array_agg(e.name ORDER BY e.name)
                     FILTER (WHERE c.employee_id IS NOT NULL), '{}') AS crew_names
       FROM schedule_entries s
       LEFT JOIN jobs j ON j.id = s.job_id
       LEFT JOIN schedule_entry_crew c ON c.schedule_entry_id = s.id
       LEFT JOIN employees e ON e.id = c.employee_id
      WHERE ($1 = '' OR j.airtable_id = $1 OR j.id::text = $1)
        -- Overlap test, matching the JS below: an entry with no dates at all is
        -- always kept, otherwise it shows when its range meets the window.
        AND ($2 = '' OR s.start_date IS NULL OR COALESCE(s.end_date, s.start_date) >= $2::date)
        AND ($3 = '' OR s.start_date IS NULL OR COALESCE(s.start_date, s.end_date) <= $3::date)
      GROUP BY s.id, j.airtable_id, j.name, j.contractor_name, j.status
      ORDER BY s.start_date ASC NULLS FIRST`,
    [jobId, since, until]);
  if (!q?.rows) return null;

  const entries = q.rows.map(r => ({
    id:         r.id,
    title:      r.title || "",
    type:       r.entry_type || "Job",
    jobId:      r.job_at_id || "",
    jobName:    r.job_name || "",
    contractor: r.contractor_name || "",
    jobStatus:  r.job_status || "",
    startDate:  r.start_date || "",
    endDate:    r.end_date || "",
    // Crew comes back ordered by NAME rather than in Airtable's insertion order —
    // deterministic, and the grid renders these as an unordered set of pills.
    crewIds:    r.crew_ids || [],
    crew:       r.crew_names || [],
    notes:      r.notes || ""
  }));

  const bd = await neonQuery(
    `SELECT airtable_id, name, contractor_name, bird_date::text AS bird_date
       FROM jobs
      WHERE bird_date IS NOT NULL
        AND ($1 = '' OR bird_date >= $1::date)
        AND ($2 = '' OR bird_date <= $2::date)`,
    [since, until]);
  const birdDates = (bd?.rows || []).map(r => ({
    jobId: r.airtable_id, jobName: r.name || "",
    contractor: r.contractor_name || "", date: r.bird_date
  }));

  return { entries, birdDates, ms: q.ms };
}

async function handleGetScheduleEntries(params) {
  // Optional date-range filter: ?since=YYYY-MM-DD & ?until=YYYY-MM-DD.
  // Filter is "any overlap" — an entry shows if its [start, end] range
  // overlaps the requested window. Job filter: ?jobId=recXXX.
  const since = params?.since || "";
  const until = params?.until || "";
  const jobId = params?.jobId || "";

  if (neonEnabled()) {
    const r = await handleGetScheduleEntriesFromNeon(params);
    if (r) return resp(200, { ok: true, entries: r.entries, birdDates: r.birdDates,
                              _source: "neon", _ms: r.ms });
    // ⚠ REFUSE, DO NOT FALL BACK (2026-08-25): Airtable has been frozen since 2026-08-25, so a fallback answers with yesterday's world.
    console.error("scheduleEntries: Neon read failed, refusing to serve frozen Airtable data");
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  const records = await fetchAll(TABLES.scheduleEntries);
  const jobs = await fetchAll(TABLES.jobs);
  // Names for the crew chips. Neon-first (Stage 4); a null means Neon couldn't
  // answer, not that nobody works here, so fall back rather than render a
  // schedule with every crew name blank.
  const employees = (await employeesForPayroll()) ?? (await fetchAll(TABLES.employees));
  const jobById = {};
  jobs.forEach(j => {
    const f = j.fields || {};
    jobById[j.id] = {
      id: j.id,
      name:       g(f, F.job.name)       || "",
      contractor: g(f, F.job.contractor) || "",
      status:     g(f, F.job.status)     || "",
      birdDate:   g(f, F.job.birdDate)   || ""
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

  // Bird move-in dates live on the Job (poultry new-construction). Surface
  // any that fall in the requested window as a lightweight sibling array so
  // the calendar can render a reminder pill on that day. Reuses the jobs we
  // already fetched above — no extra Airtable round-trip.
  const birdDates = Object.values(jobById)
    .filter(j => j.birdDate && (!since || j.birdDate >= since) && (!until || j.birdDate <= until))
    .map(j => ({ jobId: j.id, jobName: j.name, contractor: j.contractor, date: j.birdDate }));

  return resp(200, { ok: true, entries: filtered, birdDates, _source: "airtable" });
}

// ── Schedule writes: NEON-FIRST, Airtable the fail-soft mirror ─────────────
// Same contract as the time-entry writes (see neonWrite in _neon.js): the Neon
// write is authoritative and FAILS CLOSED, because the read above is served from
// Neon and a row that reaches only Airtable would be invisible.
//
// Unlike time entries there is no second writer here — no Make scenario, no
// puller — the app is the only thing that has ever created a schedule entry. So
// the id can be the Neon uuid from the start; nothing else in the base references
// a schedule entry. resolveScheduleEntry still accepts a `rec…` id because the
// Airtable read fallback returns those.
async function resolveScheduleEntry(entryId) {
  const rows = await neonWrite("schedule.resolve",
    `SELECT id, airtable_id FROM schedule_entries
      WHERE id::text = $1 OR airtable_id = $1 LIMIT 1`, [String(entryId)]);
  return rows?.[0] || null;
}

// Crew is replaced wholesale on every write rather than diffed. The set is at
// most a handful of people, and delete-then-insert cannot leave a half-updated
// crew behind if the second statement fails.
async function setScheduleCrew(entryUuid, crewAtIds) {
  await neonWrite("schedule.crew.clear",
    `DELETE FROM schedule_entry_crew WHERE schedule_entry_id = $1`, [entryUuid]);
  // ⚠⚠ THE QUIET HALF OF THE REC-ID TRAP, AND IT SHIPPED — fixed 2026-08-24.
  // This filter was `x.startsWith("rec")`, so a natively-hired employee's uuid
  // was **dropped from the array before the SQL ever ran**. The statement below
  // already resolved either form; it simply never received the id.
  //
  // The failure had no error anywhere: the rest of the crew saved normally and
  // the new hire was just absent from the entry. Silently unschedulable — which
  // is exactly what the slice-5 note predicted for the crew picker, arriving
  // through the write instead of the read that was fixed.
  //
  // ⚠ It also escaped the slice-5 sweep because the grep was for
  // `String(employeeId).startsWith("rec")`. Here the id is an anonymous array
  // element, `x`. **A filter on a LIST of ids reads nothing like a guard on a
  // single one — grep the predicate, not the variable name.** Same shape to
  // look for in slice 6, where crew, job and allocation id arrays all get
  // filtered like this.
  const ids = (Array.isArray(crewAtIds) ? crewAtIds : []).filter(isEmployeeHandle);
  if (!ids.length) return;
  await neonWrite("schedule.crew.set",
    `INSERT INTO schedule_entry_crew (schedule_entry_id, employee_id)
     SELECT $1, e.id FROM employees e WHERE e.airtable_id = ANY($2::text[]) OR e.id::text = ANY($2::text[])
     ON CONFLICT DO NOTHING`, [entryUuid, ids]);
}

async function handleAddScheduleEntry(body) {
  const { jobId, startDate, endDate, crewIds, notes, title, type } = body || {};
  const entryType = type || "Job";
  if (entryType === "Job" && !jobId) return resp(400, { ok: false, error: "Missing jobId for Job entry." });
  if (!startDate) return resp(400, { ok: false, error: "Missing startDate." });
  if (!endDate)   return resp(400, { ok: false, error: "Missing endDate." });

  const rows = await neonWrite("schedule.insert",
    `INSERT INTO schedule_entries
       (title, entry_type, job_id, start_date, end_date, notes, source)
     VALUES ($1, $2, (SELECT id FROM jobs WHERE airtable_id = $3 OR id::text = $3), $4::date, $5::date, $6, 'app')
     RETURNING id`,
    [title ? String(title) : null, entryType, jobId || null,
     startDate, endDate, notes ? String(notes) : null]);
  const neonId = rows?.[0]?.id;
  await setScheduleCrew(neonId, crewIds);

  const fields = {};
  fields[SCHED_F.type] = entryType;
  Object.assign(fields, jobLink(SCHED_F.job, jobId));
  fields[SCHED_F.startDate] = startDate;
  fields[SCHED_F.endDate]   = endDate;
  // ⚠⚠ REC IDS ONLY IN THE MIRROR. `SCHED_F.crew` is an Airtable LINKED-RECORD
  // field written with `typecast: true`, which CREATES a record for a value it
  // does not recognise — so a native hire's uuid would add a junk person to the
  // Employees table and link the schedule to them. Same trap as `Submitted By`
  // on expenses. Dropping them here costs nothing: `setScheduleCrew` above has
  // already written the real crew to Neon, which is what the app reads.
  const crewRecIds = (Array.isArray(crewIds) ? crewIds : []).filter(x => typeof x === "string" && x.startsWith("rec"));
  if (crewRecIds.length) fields[SCHED_F.crew] = crewRecIds;
  if (notes) fields[SCHED_F.notes] = String(notes);
  if (title) fields[SCHED_F.title] = String(title);

  const data = await mirrorToAirtable("addScheduleEntry", () =>
    atFetch(`${encodeURIComponent("Schedule Entries")}`, {
      method: "POST", body: JSON.stringify({ fields, typecast: true })
    }));
  if (data?.id) {
    await mirrorToAirtable("addScheduleEntry.stamp", () =>
      neonWrite("schedule.stampAirtableId",
        `UPDATE schedule_entries SET airtable_id = $2 WHERE id = $1`, [neonId, data.id]));
  }
  return resp(200, { ok: true, id: neonId, airtableId: data?.id || null });
}

async function handleUpdateScheduleEntry(body) {
  const { entryId, jobId, startDate, endDate, crewIds, notes, title, type } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const target = await resolveScheduleEntry(entryId);
  if (!target) return resp(404, { ok: false, error: "Schedule entry not found." });

  // Only the fields the client actually sent are written, so an omitted field is
  // left alone rather than nulled — the drag-to-move path sends dates only.
  const sets = [], vals = [target.id];
  const put = (col, v, cast = "") => { vals.push(v); sets.push(`${col} = $${vals.length}${cast}`); };
  if (type      !== undefined) put("entry_type", type || "Job");
  if (startDate !== undefined) put("start_date", startDate || null, "::date");
  if (endDate   !== undefined) put("end_date",   endDate   || null, "::date");
  if (notes     !== undefined) put("notes", String(notes || ""));
  if (title     !== undefined) put("title", String(title || ""));
  if (jobId     !== undefined) {
    // Dual handle — cutover slice 6. Re-pointing a schedule entry at a native
    // job resolved to NULL before, dropping it off that job's schedule.
    vals.push(jobId || null);
    sets.push(`job_id = (SELECT id FROM jobs WHERE airtable_id = $${vals.length} OR id::text = $${vals.length})`);
  }
  if (sets.length) {
    await neonWrite("schedule.update",
      `UPDATE schedule_entries SET ${sets.join(", ")} WHERE id = $1`, vals);
  }
  if (crewIds !== undefined) await setScheduleCrew(target.id, crewIds);
  if (!sets.length && crewIds === undefined) {
    return resp(400, { ok: false, error: "Nothing to update." });
  }

  const fields = {};
  if (type        !== undefined) fields[SCHED_F.type]      = type || "Job";
  if (jobId       !== undefined) fields[SCHED_F.job]       = jobId ? [jobId] : [];
  if (startDate   !== undefined) fields[SCHED_F.startDate] = startDate || null;
  if (endDate     !== undefined) fields[SCHED_F.endDate]   = endDate   || null;
  // Rec ids only — see the note in handleAddScheduleEntry. A uuid in this
  // linked-record field creates a junk employee via typecast.
  if (crewIds     !== undefined) fields[SCHED_F.crew]      =
    (Array.isArray(crewIds) ? crewIds : []).filter(x => typeof x === "string" && x.startsWith("rec"));
  if (notes       !== undefined) fields[SCHED_F.notes]     = String(notes || "");
  if (title       !== undefined) fields[SCHED_F.title]     = String(title || "");

  // Mirror only when the row has an Airtable twin. Entries created after this
  // flip have none until the mirror succeeds, and that is not an error.
  if (target.airtable_id && Object.keys(fields).length) {
    await mirrorToAirtable("updateScheduleEntry", () =>
      atFetch(`${encodeURIComponent("Schedule Entries")}/${target.airtable_id}`, {
        method: "PATCH", body: JSON.stringify({ fields, typecast: true })
      }));
  }
  return resp(200, { ok: true, id: target.id });
}

async function handleDeleteScheduleEntry(body) {
  const { entryId } = body || {};
  if (!entryId) return resp(400, { ok: false, error: "Missing entryId." });

  const target = await resolveScheduleEntry(entryId);
  if (!target) return resp(404, { ok: false, error: "Schedule entry not found." });

  // No tombstone here, unlike time entries: a schedule entry is a plan, not a
  // financial record, and losing one costs a re-drag rather than someone's pay.
  // schedule_entry_crew is ON DELETE CASCADE, so the crew rows go with it.
  await neonWrite("schedule.delete", `DELETE FROM schedule_entries WHERE id = $1`, [target.id]);

  if (target.airtable_id) {
    await mirrorToAirtable("deleteScheduleEntry", () =>
      atFetch(`${encodeURIComponent("Schedule Entries")}/${target.airtable_id}`, { method: "DELETE" }));
  }
  return resp(200, { ok: true, deletedId: target.id });
}

// Helper: list active employees with role exposed, for the crew picker.
// (Filters by Active checkbox; frontend filters further by role to exclude
// office + viewer.) Used by the schedule modal's crew dropdown.
async function handleListEmployeesForScheduling() {
  // NEON-FIRST. Flipped right after the schedule entries themselves, because the
  // Schedule tab fires both calls in a Promise.all — so leaving this one on
  // Airtable meant the screen was still gated on an Airtable round-trip and the
  // load felt exactly as slow as before. Half a flip buys nothing on a parallel
  // fetch; the slowest leg sets the time.
  //
  // `id` is the employee HANDLE — the rec id wherever one exists, so it is
  // unchanged for everyone hired before 2026-08-24. It is what the crew picker
  // sends back in `crewIds`, what schedule_entry_crew resolves against, and what
  // the payroll screens use; all of those take either form since slice 5.
  //
  // ⚠⚠ `AND airtable_id IS NOT NULL` WAS A FILTER, NOT A SANITY CHECK, and it is
  // gone. It made sense when a row without a rec id could only be corrupt; after
  // slice 5 it describes every natively-hired employee — so a new hire would
  // have been **silently absent from the crew picker**, unschedulable, with no
  // error and nothing in the UI to suggest they had been left out. The kind of
  // omission somebody notices on a Monday morning when a crew is short.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(airtable_id, id::text) AS handle, name,
              lower(coalesce(role, 'employee')) AS role
         FROM employees
        WHERE active IS TRUE
        ORDER BY name`);
    if (q?.rows?.length) {
      return resp(200, {
        ok: true,
        employees: q.rows.map(r => ({ id: r.handle, name: r.name || "", role: r.role || "employee" })),
        _source: "neon", _ms: q.ms
      });
    }
    // Zero active employees is never a legitimate answer — it would empty the crew
    // picker — so treat it as a failure and let Airtable answer.
    // ⚠ EMPTY IS STILL TREATED AS FAILURE HERE, and that judgement is the
    // original author's: this list cannot legitimately come back empty, so an
    // empty answer means something is wrong. What CHANGED on 2026-08-25 is the
    // remedy. Airtable stopped being written that day, so falling back now
    // serves a frozen copy — silently, and looking perfectly normal. Better to
    // say the database is unavailable than to hand back yesterday's world.
    console.error(`schedulingCrew: Neon returned nothing — refusing to serve frozen Airtable data: ${q?.error || "no rows"}`);
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
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
  return resp(200, { ok: true, employees, _source: "airtable" });
}

async function handleGetAllInvoices() {
  // ── NEON-FIRST (migration Step 4e) ──────────────────────────────────────
  // The Airtable path below pages TWO entire tables — every invoice and every
  // job — then joins them in memory. Here it is one query with a join.
  //
  // Deliberately does NOT filter by job status: an invoice on a now-archived
  // job must still be visible, which is why the Airtable path fetches jobs raw
  // rather than through the status-filtering helper. Same behaviour preserved.
  //
  // `total` serves invoice_total_calc — the computed figure, diffed 51/51
  // against Airtable at 015 — not the stored copy that goes stale when an
  // allocation changes.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(v.airtable_id, v.id::text) AS id, v.invoice_display_no, v.invoice_number,
              v.invoice_date::text AS invoice_date, v.invoice_status, v.billing_mode,
              v.invoice_type, v.invoice_total_calc, v.snapshot_total,
              v.contract_invoice_amount, v.invoice_notes, v.invoice_snapshot, v.invoice_stage,
              v.job_airtable_id,
              j.name AS job_name, j.contractor_name, j.address_full, j.status AS job_status,
              NULLIF(TRIM(COALESCE(j.customer_first_name,'') || ' ' ||
                          COALESCE(j.customer_last_name,'')), '') AS customer
         FROM v_invoices v
         LEFT JOIN jobs j ON j.id = v.job_id
        ORDER BY v.invoice_date DESC NULLS LAST`);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      return resp(200, {
        ok: true,
        invoices: q.rows.map(r => ({
          id: r.id, displayNumber: r.invoice_display_no ?? null,
          invoiceNumber: s(r.invoice_number), date: s(r.invoice_date),
          status: s(r.invoice_status), billingMode: s(r.billing_mode),
          invoiceType: s(r.invoice_type),
          total: Number(r.invoice_total_calc ?? 0),
          snapshotTotal: Number(r.snapshot_total ?? 0),
          contractAmount: Number(r.contract_invoice_amount ?? 0),
          notes: s(r.invoice_notes), snapshot: s(r.invoice_snapshot),
          stage: s(r.invoice_stage),
          jobId: s(r.job_airtable_id), jobName: s(r.job_name),
          contractor: s(r.contractor_name), customer: s(r.customer),
          address: s(r.address_full), jobStatus: s(r.job_status),
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`getAllInvoices: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

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

// ── KEEP NEON IN STEP AFTER AN INVOICE WRITE (migration Step 4e) ──────────
// handleGetJobInvoices reads Neon first and only falls through on ZERO rows, so
// on any job that already has an invoice an Airtable-only write would simply
// never appear. Same trap as the warranties, expenses and estimates.
//
// Airtable stays the identity authority: the id contract is rec-shaped because
// every consumer does atFetch("Invoices/<id>"), and the billing allocations
// link to invoices by rec id.
//
// ⚠ snapshot_total matters more here than anywhere else in this migration. It
// is what `Jobs.Total Contract Billed` rolls up, which becomes
// `Previous Contract Billing`, which sets `Contract Remaining` — the cap on what
// the NEXT contract invoice may bill. A stale snapshot_total in Neon does not
// just misreport one invoice; it changes what the customer can be charged.
// See db/schema/015_invoice_contract_chain.sql.
async function syncInvoiceToNeon(rec) {
  if (!rec?.id) return;
  const f = rec.fields || {};
  const n = (v) => { if (Array.isArray(v)) v = v[0]; const x = Number(v); return Number.isFinite(x) ? x : null; };
  const s = (v) => { const x = Array.isArray(v) ? v[0] : v; return (x === undefined || x === "" || x === null) ? null : String(x); };
  const sel = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v.name : s(v));
  await neonWrite("invoice.sync",
    // ⚠⚠ `invoice_total` IS DELIBERATELY NOT CARRIED BACK (2026-08-24). It was
    // Airtable's rollup over the invoice's LINKED allocation records — and since
    // slice 3 an allocation can be Neon-native with no Airtable row at all, so
    // that rollup sees nothing and reads **0.00** on a correct invoice. Invoice
    // 1671 (Test 2) is the proof: $39.74 of native material, and Airtable's
    // total said zero.
    //
    // Carrying it back therefore wrote a known-wrong number into a money column,
    // directly contradicting the native INSERT a few hundred lines up, which
    // leaves it NULL on purpose because "a second, decaying opinion of the total
    // in a money column is how a wrong number gets quoted later."
    //
    // Nothing reads the column — every read uses `v_invoices.invoice_total_calc`
    // — so this changes no output. It removes the trap, not a behaviour.
    // ⚠ Historical rows KEEP the values they already hold: the column is simply
    // no longer in the INSERT or the DO UPDATE SET, so an upsert leaves it be.
    `INSERT INTO invoices
       (airtable_id, job_airtable_id, job_id, invoice_number, invoice_status, invoice_type,
        billing_mode, invoice_stage, invoice_date, snapshot_total,
        manual_labor, manual_material, percent_to_bill, auto_allocate, invoice_display_no,
        invoice_notes, invoice_snapshot, synced_at)
     VALUES ($1,$2,(SELECT id FROM jobs WHERE airtable_id = $2 OR id::text = $2),$3,$4,$5,$6,$7,$8::date,$9,
             $10,$11,$12,$13,$14,$15,$16, now())
     -- ⚠⚠ THE JOB LINKAGE AND invoice_number ARE NO LONGER CARRIED BACK, AND
     -- THAT IS THE FIX FOR A REAL LOSS (2026-08-25). This upsert runs right
     -- after the app has written the authoritative row, so on the DO UPDATE
     -- branch Airtable is always the JUNIOR opinion. On a native job it was a
     -- WRONG one: the mirror POST had linked the invoice to a Job record
     -- typecast had just fabricated from the uuid, so this SET overwrote a
     -- correct job_id with NULL and a correct invoice_number with
     -- "846245ef-…-001" — the fabricated job's display name, straight out of
     -- Airtable's Invoice Number formula. The invoice then belonged to no job
     -- and vanished from the job's invoice history.
     --
     -- The INSERT branch still carries them: a genuinely Airtable-born invoice
     -- has no Neon row to be junior to. Only the overwrite is gone.
     ON CONFLICT (airtable_id) DO UPDATE SET
       invoice_status=EXCLUDED.invoice_status,
       invoice_type=EXCLUDED.invoice_type, billing_mode=EXCLUDED.billing_mode,
       invoice_stage=EXCLUDED.invoice_stage, invoice_date=EXCLUDED.invoice_date,
       snapshot_total=EXCLUDED.snapshot_total,
       manual_labor=EXCLUDED.manual_labor, manual_material=EXCLUDED.manual_material,
       percent_to_bill=EXCLUDED.percent_to_bill, auto_allocate=EXCLUDED.auto_allocate,
       invoice_display_no=EXCLUDED.invoice_display_no, invoice_notes=EXCLUDED.invoice_notes,
       invoice_snapshot=EXCLUDED.invoice_snapshot, synced_at=now()`,
    // ⚠ `Invoice Total` is gone from this array too, not just the SQL. Leaving an
    // unreferenced bind behind would fail at PREPARE with "could not determine
    // data type of parameter $10" — every placeholder after it shifted down one.
    [rec.id, s(f["Job"]), s(f["Invoice Number"]), sel(f["Invoice Status"]), sel(f["Invoice Type"]),
     sel(f["Billing Mode"]), sel(f["Invoice Stage"]), s(f["Invoice Date"]),
     n(f["Snapshot Total"]), n(f["Manual Labor $"]),
     n(f["Manual Material $"]), n(f["Percent to Bill"]), f["Auto Allocate?"] === true,
     n(f["Invoice Display #"]), s(f["Invoice Notes"]), s(f["Invoice Snapshot"])]).catch(() => {});
}

async function handleGetJobInvoices(body) {
  const { jobId } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  // ── NEON-FIRST (migration Step 4e) ──────────────────────────────────────
  // Replaces paging the ENTIRE Invoices table and filtering in memory with a
  // WHERE clause. Money fields come from v_invoices, whose whole chain was
  // diffed against Airtable at 51/51 on every field before this read was wired.
  //
  // ⚠ `total` serves invoice_total_calc, the COMPUTED figure — not the stored
  // copy. That is the point of 015: the stored one goes stale the moment an
  // allocation changes, whereas the computed one cannot.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT COALESCE(airtable_id, id::text) AS id, invoice_display_no, invoice_number,
              invoice_date::text AS invoice_date, invoice_status, billing_mode, invoice_type,
              invoice_total_calc, snapshot_total, percent_to_bill,
              contract_invoice_amount, invoice_notes, invoice_snapshot, invoice_stage
         FROM v_invoices
        WHERE (job_airtable_id = $1 OR job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
        ORDER BY invoice_date DESC NULLS LAST`, [jobId]);
    if (q?.rows) {
      const s = (v) => (v === null || v === undefined ? "" : String(v));
      return resp(200, {
        ok: true,
        invoices: q.rows.map(r => ({
          id: r.id, displayNumber: r.invoice_display_no ?? null,
          invoiceNumber: s(r.invoice_number), date: s(r.invoice_date),
          status: s(r.invoice_status), billingMode: s(r.billing_mode),
          invoiceType: s(r.invoice_type),
          total: Number(r.invoice_total_calc ?? 0),
          snapshotTotal: Number(r.snapshot_total ?? 0),
          percentToBill: r.percent_to_bill === null ? null : Number(r.percent_to_bill),
          contractAmount: Number(r.contract_invoice_amount ?? 0),
          notes: s(r.invoice_notes), snapshot: s(r.invoice_snapshot),
          stage: s(r.invoice_stage),
        })),
        _source: "neon", _ms: q.ms
      });
    }
    // ⚠ LOUD, NOT FALLBACK (2026-08-25). This used to log and read Airtable.
    // Airtable stopped being written on 2026-08-25, so its copy is frozen —
    // falling back now serves data that is stale by construction, and serves
    // it silently. A failed read is an outage; say so and let the caller retry.
    if (q?.error) {
      console.error(`getJobInvoices: Neon read FAILED — refusing to serve stale Airtable data: ${q.error}`);
      return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
    }
  }

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
  // Neon-first: `notes` is in JOB_SELECT. Second instance found by the sweep.
  // Job notes carry crew instructions, so a note that reverts is a crew reading
  // yesterday's plan.
  await neonWrite("job.updateNotes",
    `UPDATE jobs SET notes = $2, synced_at = now() WHERE airtable_id = $1 OR id::text = $1`, [jobId, notes || ""]);
  const data = await mirrorJobPatch("updateJobNotes", jobId, { "fldAuZAW19iYPBPxP": notes || "" });
  return resp(200, { ok: true, updatedId: data?.id || jobId });
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
  if (!jobId || !isJobHandle(jobId)) {
    return resp(400, { ok: false, error: "Missing or invalid jobId." });
  }
  // Resolve BOTH forms. The pickers emit rec ids by design (see
  // handleGetInspectionAgencies), but a agency or inspector created since the
  // last mirror can still arrive as a uuid, and this writes Airtable LINKED
  // RECORD fields, which only accept rec ids.
  const ag  = await resolveAgencyIds(agencyId);
  const ins = await resolveInspectorIds(inspectorId);

  const fields = {};
  const hasAgency = !!ag.rec;
  fields["fldyKKACyUqt9tcEL"] = hasAgency ? [ag.rec] : [];
  // Inspector belongs to an agency — if no agency, force-clear the inspector.
  fields["fld9ApvXJqPhuDcm4"] = (hasAgency && ins.rec) ? [ins.rec] : [];
  fields["fldDKGllmOyyyf9qo"] = permitNumber || "";
  fields["fldQ5VJgOYcQBxmCr"] = !!inspectionNotRequired;

  const data = await mirrorJobPatch("updateJob", jobId, fields);

  // ⚠ WRITE NEON TOO — same bug 10b6e04 fixed in updateJobInfo, same cause.
  // handleJobs and handleJobById are Neon-first and Neon's `jobs` is refreshed
  // HOURLY, so an Airtable-only write here reverts on the next refresh. The
  // denormalised name/phone/email columns are Airtable LOOKUPS through these
  // links, so they are refreshed from the record Airtable just returned rather
  // than recomputed — whatever the lookup resolved to is the truth.
  const jf = data?.fields || {};
  const look = (v) => { const x = Array.isArray(v) ? v[0] : v; return (x === undefined || x === "") ? null : x; };
  await neonWrite("job.updateInspection",
    `UPDATE jobs SET
       inspection_agency_at_id = $2, inspection_agency = $3,
       inspection_agency_phone = $4, inspection_agency_email = $5,
       inspection_scheduling_link = $6,
       inspector_at_id = $7, inspector_name = $8,
       inspector_phone = $9, inspector_email = $10,
       permit_number = $11, inspection_not_required = $12
     WHERE airtable_id = $1 OR id::text = $1`,
    [jobId, hasAgency ? ag.rec : null, look(jf[F.job.inspectionAgency]),
     look(jf[F.job.inspectionAgencyPhone]), look(jf[F.job.inspectionAgencyEmail]),
     look(jf[F.job.inspectionSchedulingLink]),
     (hasAgency && ins.rec) ? ins.rec : null, look(jf[F.job.inspectionContacts]),
     look(jf[F.job.inspectorPhone]), look(jf[F.job.inspectorEmail]),
     permitNumber || null, !!inspectionNotRequired]).catch(() => {});

  return resp(200, { ok: true, job: mapJob(data) });
}

// Single-call update for the Project Info edit form. PATCHes any subset
// of the seven editable fields in one round-trip; missing keys are left
// untouched server-side. Empty strings clear the field (e.g. "" on
// customerEmail wipes the address) — that's intentional so the edit
// form supports both updating and clearing.
async function handleUpdateJobInfo(body) {
  const { jobId, customerStreet, customerCity, customerState, customerZip, customerPhone, customerEmail, notes, birdDate, generatorInstalled } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const fields = {};
  // Generator Installed — the flag that reveals the Generator tab, and until now
  // it had NO WRITE PATH ANYWHERE. It could only be ticked by hand in Airtable,
  // which made the whole commissioning workflow unreachable from the app: the
  // thing that CREATES a generator was gated behind a flag saying one exists.
  if (generatorInstalled !== undefined) fields["fldANNxuzBUrI9aI8"] = generatorInstalled === true;
  if (customerStreet !== undefined) fields["fldFBJrw64SYC1WdB"] = customerStreet || "";
  if (customerCity   !== undefined) fields["fld46JMp1z6E2DhJt"] = customerCity   || "";
  if (customerState  !== undefined) fields["fldktee97zx5QUPmd"] = customerState  || "";
  if (customerZip    !== undefined) fields["fldMooJ88usuHF6RH"] = customerZip    || "";
  if (customerPhone  !== undefined) fields["fldBf6EC5EQXsPFAQ"] = customerPhone  || "";
  if (customerEmail  !== undefined) fields["fldzGgNmRlSxwpSMX"] = customerEmail  || "";
  if (notes          !== undefined) fields["fldAuZAW19iYPBPxP"] = notes          || "";
  // Bird Date is a date-only field — send null (not "") to clear it, so an
  // empty string never trips Airtable's date parsing.
  if (birdDate       !== undefined) fields["fldyKjtcqganpbhNc"] = birdDate || null;

  if (!Object.keys(fields).length) return resp(400, { ok: false, error: "Nothing to update." });

  const data = await mirrorJobPatch("updateJobInfo", jobId, fields, { typecast: true });

  // ⚠ WRITE NEON TOO, or the edit silently reverts for up to an hour.
  // handleJobs and handleJobById are BOTH Neon-first, and Neon's `jobs` is a
  // one-way mirror refreshed HOURLY by _jobs-sync.js. Writing only Airtable —
  // which is what this handler did for every field until 2026-08-07 — means the
  // next refresh re-reads the STALE Neon row and the change disappears. It
  // LOOKED fine because the frontend patches local state after saving, so the
  // value survives until you reload. Found via Generator Installed, where there
  // was no local patch to hide it and the tick simply did nothing.
  //
  // Safe against the hourly sync precisely BECAUSE Airtable is written first:
  // the sync re-derives the same value rather than clobbering this one.
  const nSets = [], nVals = [jobId];
  const put = (col, v, cast = "") => { nVals.push(v); nSets.push(`${col} = $${nVals.length}${cast}`); };
  if (customerStreet !== undefined) put("address_street",  customerStreet || null);
  if (customerCity   !== undefined) put("address_city",    customerCity   || null);
  if (customerState  !== undefined) put("address_state",   customerState  || null);
  if (customerZip    !== undefined) put("address_zip",     customerZip    || null);
  if (customerPhone  !== undefined) put("customer_phone",  customerPhone  || null);
  if (customerEmail  !== undefined) put("customer_email",  customerEmail  || null);
  if (notes          !== undefined) put("notes",           notes          || null);
  if (birdDate       !== undefined) put("bird_date",       birdDate || null, "::date");
  if (generatorInstalled !== undefined) put("generator_installed", generatorInstalled === true);

  // `address_full` is a FORMULA in Airtable, so Airtable recomputes it itself.
  // Neon's copy is a plain synced column and will not — leaving the job card
  // showing the old address beside the new parts.
  //
  // ⚠ REPRODUCE AIRTABLE'S FORMULA, NOT buildJobAddress FROM index.html.
  // They are not the same. Airtable is a LITERAL concat that keeps its
  // separators even when parts are blank; buildJobAddress filters empties. On a
  // job with only a state that is ", , OH" vs "OH" — verified against all 112
  // jobs, where the literal form matches 112/112 and the filtered form 104/112.
  // Using the prettier one here would make Neon disagree with Airtable, and the
  // hourly sync would then revert it — reintroducing exactly the flicker this
  // whole change exists to remove. Neon mirrors; it does not improve.
  //
  // (index.html's client-side version still shows the tidier string until the
  // next refresh. Cosmetic, pre-existing, and not worth diverging the data for.)
  const touchedAddress = [customerStreet, customerCity, customerState, customerZip]
    .some(v => v !== undefined);
  if (touchedAddress) {
    const cur = (await neonQuery(
      `SELECT address_street, address_city, address_state, address_zip
         FROM jobs WHERE airtable_id = $1 OR id::text = $1`, [jobId]))?.rows?.[0] || {};
    const street = customerStreet !== undefined ? customerStreet : (cur.address_street || "");
    const city   = customerCity   !== undefined ? customerCity   : (cur.address_city   || "");
    const state  = customerState  !== undefined ? customerState  : (cur.address_state  || "");
    const zip    = customerZip    !== undefined ? customerZip    : (cur.address_zip    || "");
    put("address_full",
      `${street || ""}, ${city || ""}, ${state || ""} ${zip || ""}`.trim() || null);
  }

  if (nSets.length) {
    // Fail-soft: Airtable already holds the authoritative write for these
    // fields, so a Neon hiccup must not fail an edit the user watched succeed.
    // The hourly sync repairs it.
    await neonWrite("job.updateInfo",
      `UPDATE jobs SET ${nSets.join(", ")} WHERE airtable_id = $1 OR id::text = $1`, nVals).catch(() => {});
  }

  return resp(200, { ok: true, updatedId: data.id });
}

// The in-app New Project modal's endpoint. **The work moved to
// `netlify/functions/_jobs.js` on 2026-08-21** — read that file for the create
// itself, the PO allocation, the Airtable re-read and the Neon mirror, all of
// which are unchanged. This is now only the HTTP shape around it.
//
// It was extracted because a SECOND caller appeared (the generator service-call
// check) and two PO allocators is how duplicate PO numbers come back — see the
// header of _jobs.js for why that is worse than it sounds.
//
// Notes that still belong to the endpoint rather than the module:
//   · Every new job from this form is a contractor job. Billing Method is left
//     at the module's "Contractor" default — the radio is gone from the UI but
//     downstream invoice-builder reads (index.html:6514, 6605, 6903, 7311) still
//     inspect job.billingMethod as a Contract-vs-T&M tiebreaker, so the
//     breadcrumb has to stay coherent.
//   · Returns the new record run through mapJob() so the frontend can splice it
//     into state.jobs and selectJob() it without a full list refetch.
//   · A JobInputError is the caller's fault and answers 400; anything else
//     (Airtable down, network) stays a 500, which is correct.
async function handleCreateJob(body) {
  try {
    const { record, poNumber } = await createJobRecord(atFetch, body || {});
    return resp(200, { ok: true, job: mapJob(record), ...(poNumber != null ? { poNumber } : {}) });
  } catch (e) {
    if (e instanceof JobInputError) return resp(400, { ok: false, error: e.message });
    throw e;
  }
}

// ── Does this job exist? ───────────────────────────────────────────────────
// The only question ten handlers ever asked Airtable. Photos, deleted photos,
// docs, prints, deleted prints, print upload urls, photo upload urls, bulk
// photo ops, panel schedules and checklists all opened with the identical
// three lines: fetch the whole Jobs record by RECORD_ID(), check `.length`,
// throw the record away. Ten Airtable round trips that never read a field.
//
// ⚠ IT ASKS AIRTABLE ANYWAY WHEN NEON SAYS NO, AND THAT IS THE POINT.
// A "not found" is the one answer that is expensive to get wrong — it makes a
// job's photos vanish — so the cheap store only gets to say YES. Neon answering
// yes ends it (the common case, and where the saved round trip lives); Neon
// answering no, erroring, or being switched off all fall through to the same
// Airtable read that runs today. So this cannot be less correct than what it
// replaces, only faster. Two real cases depend on that fallback:
//   · Neon down — every photo tab would 404 in unison.
//   · A job whose create-time Neon insert failed (that write is fail-soft) and
//     which the hourly sync has not adopted yet. It exists; it just isn't here.
//
// Side benefit: the Neon half is parameterised, so the job id never reaches a
// filterByFormula string at all.
// ── WHICH JOB-CREATE MODE IS THIS FUNCTION ACTUALLY RUNNING? ───────────────
// Admin-only. Exists because setting JOB_CREATE_SOURCE in the Netlify UI does
// NOT reach an already-deployed function: Netlify bakes env vars at BUILD time,
// so the variable only applies after a redeploy. Two jobs were created believing
// the switch was live when it was not, and the only tell was noticing that the
// Neon rows still carried an airtable_id.
//
// Same job as r2Status: name the specific misconfiguration instead of leaving
// somebody to infer it from the data afterwards. `native` here is the ONLY value
// that means jobs are born in Neon.
// ── ON-DEMAND INTEGRITY CHECK ─────────────────────────────────────────────
// The same checks the hourly pull runs (_integrity.js), reachable when someone
// wants an answer NOW rather than at the top of the hour — after a big import,
// before a payroll run, or when a number looks wrong.
//
// ⚠ Read-only, like every check in that file. It reports; it never repairs.
async function handleIntegrityCheck() {
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Integrity checks need the database. DATABASE_URL is unset." });
  }
  // The checks want a client with .query(); neonQuery is the wrapper this file
  // uses everywhere else, so adapt rather than opening a second connection.
  const sql = { query: async (text, params) => await neonQuery(text, params || []) };
  const report = await runIntegrityChecks(sql);
  return resp(200, {
    ok: true,
    healthy: report.failures === 0 && report.brokenChecks === 0,
    checked: report.checked,
    failures: report.failures,
    brokenChecks: report.brokenChecks,
    findings: report.findings,
  });
}

async function handleJobCreateStatus() {
  const raw = process.env.JOB_CREATE_SOURCE;
  return resp(200, {
    ok: true,
    // The raw value, so a trailing space or a wrong case is visible rather than
    // silently normalised away.
    rawValue: raw === undefined ? null : raw,
    resolved: jobCreateSource() || "(unset)",
    jobsAreNative: jobsAreNative(),
    poAssignedBy: (jobCreateSource() === "neon" || jobsAreNative()) ? "neon" : "airtable",
    meaning: jobsAreNative()
      ? "Jobs are BORN IN NEON. Airtable gets a fail-soft mirror and the rec id is never stamped back."
      : "Jobs are created in Airtable first. Setting JOB_CREATE_SOURCE=native requires a REDEPLOY to take effect.",

    // ── The mirror kill switch, reported here for the same reason this endpoint
    // exists at all: so flipping an env var stops being guesswork. The RAW value
    // is included because the failure mode is a typo — "Off " or "false" both
    // leave writes ON, deliberately, and only the raw value shows you why.
    airtableWrites: {
      rawValue: process.env.AIRTABLE_WRITES === undefined ? null : process.env.AIRTABLE_WRITES,
      enabled: airtableWritesEnabled(),
      meaning: airtableWritesEnabled()
        ? "Mirrors ARE being written to Airtable. Set AIRTABLE_WRITES=off to stop all 65 of them."
        : "Mirrors are OFF. Nothing in this app writes to Airtable; Neon is the only record.",
      // The combination that cannot work, surfaced before somebody hits it.
      misconfigured: !jobsAreNative() && !airtableWritesEnabled()
        ? "⚠ JOB_CREATE_SOURCE is not 'native' AND writes are off — job creation will refuse, because the non-native path re-reads its own Airtable write."
        : null,
    },
  });
}

// ── GOOGLE CONTACT SYNC: STATUS + RECONCILE (audit item 07) ───────────────
// Both admin-only, both READ-ONLY. See docs/PLAN-google-contacts.md.
//
// Same job as r2Status and jobCreateStatus: name the specific misconfiguration
// rather than leaving somebody to infer it from the data afterwards. Netlify
// bakes env vars at BUILD time, so setting GOOGLE_CONTACTS or GOOGLE_SA_KEY in
// the dashboard does not reach a deployed function until a REDEPLOY.
async function handleGoogleStatus() {
  const status = await googleStatus();
  return resp(200, { ok: true, google: status });
}

// ⚠⚠ THE GATE. Nothing may write to Google until somebody has read this.
//
// 230 of 240 contacts already exist in both accounts, and the stored ids are the
// only thing preventing a cold start from creating 230 duplicates twice over, in
// address books that are live on phones. This answers, per account and WITHOUT
// WRITING ANYTHING: does each stored id still resolve, how many rows have no id
// at all, and does either account hold duplicates of its own?
//
// ⚠ An id that fails to resolve is reported as "missing", NOT quietly upgraded to
// a create. Deciding to re-create is a human decision made from this report,
// because the failure modes look identical from here: a contact genuinely deleted
// in Google and a contact whose id we simply got wrong both return 404.
async function handleGoogleContactsReconcile(params) {
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "The reconcile needs the database. DATABASE_URL is unset." });
  }
  if (!googleConfigured()) {
    return resp(503, {
      ok: false,
      error: "GOOGLE_SA_KEY is unset. Add the service account JSON (base64) in Netlify and REDEPLOY.",
      hint: "GET ?action=googleStatus reports the wiring in detail.",
    });
  }

  // Deep mode also pages every contact in each account to look for
  // within-account duplicates. Much slower, and not needed to decide whether the
  // sync is safe to run, so it is opt-in.
  const deep = String(params?.deep || "") === "1";

  const q = await neonQuery(
    `SELECT c.id, c.first_name, c.last_name, c.primary_phone, c.primary_email,
            c.google_person_id_1, c.google_person_id_2
       FROM contacts c
      ORDER BY c.last_name NULLS LAST, c.first_name NULLS LAST`);
  if (!q?.rows) {
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  const rows = q.rows;

  const out = { totalContacts: rows.length, destinations: [] };

  // 7 of the 240 carry neither a phone nor an email. The owner was told these
  // would sync as name-only entries under the "all 240" decision; counted here so
  // the number is in front of him BEFORE anything is written, not after.
  out.noPhoneNoEmail = rows.filter(r =>
    !String(r.primary_phone || "").trim() && !String(r.primary_email || "").trim()).length;

  for (const dest of DESTINATIONS) {
    const summary = {
      key: dest.key, account: dest.subject, column: dest.column,
      withStoredId: 0, resolved: 0, missing: 0, errors: 0, noId: 0,
      missingSamples: [],
    };

    const withIds = rows.filter(r => r[dest.column]);
    summary.noId = rows.length - withIds.length;
    summary.withStoredId = withIds.length;

    try {
      // Batched: 230 ids is 2 calls, not 230. The one-at-a-time version could
      // not finish inside a Netlify function's timeout — see getPeopleBatch.
      const found = await getPeopleBatch(dest.subject, withIds.map(r => r[dest.column]));
      for (const r of withIds) {
        if (found.has(r[dest.column])) { summary.resolved++; continue; }
        summary.missing++;
        if (summary.missingSamples.length < 10) {
          summary.missingSamples.push({
            contactId: r.id,
            name: [r.first_name, r.last_name].filter(Boolean).join(" "),
            storedId: r[dest.column],
          });
        }
      }
    } catch (e) {
      // ⚠ A failed batch is NOT evidence of absence. Record it as an error so the
      // verdict refuses, rather than reporting 230 contacts as missing and
      // inviting somebody to re-create every one of them.
      summary.errors = withIds.length;
      summary.errorCode = e?.code || "ERROR";
      summary.errorHint = String(e?.message || e).slice(0, 300);
    }

    if (deep && !summary.errors) {
      try {
        const all = await listConnections(dest.subject);
        summary.accountTotalContacts = all.length;
        // Within-account duplicates — the only kind that is a genuine duplicate to
        // a person looking at their phone. Reported, never fixed: Google Contacts
        // has its own Merge & fix, which previews each merge.
        const byName = new Map();
        for (const p of all) {
          const n = (p.names?.[0]?.displayName || "").trim().toLowerCase();
          if (!n) continue;
          byName.set(n, (byName.get(n) || 0) + 1);
        }
        const dupes = [...byName.entries()].filter(([, n]) => n > 1);
        summary.withinAccountDuplicateNames = dupes.length;
        summary.withinAccountDuplicateSamples = dupes.slice(0, 10).map(([name, n]) => ({ name, count: n }));
      } catch (e) {
        summary.deepError = String(e?.message || e).slice(0, 200);
      }
    }

    out.destinations.push(summary);
  }

  // The one-line answer, so nobody has to add the columns up by hand.
  const anyErrors  = out.destinations.some(d => d.errors > 0);
  const anyMissing = out.destinations.some(d => d.missing > 0);
  out.verdict = anyErrors
    ? "NOT SAFE TO RUN — some ids could not be checked at all. Fix the errors first; a run now would treat unchecked rows as creates."
    : anyMissing
      ? "CHECK FIRST — every id was checked, but some no longer resolve in Google. Decide per contact whether to re-create before enabling writes."
      : "SAFE — every stored id resolves in every destination. A sync would UPDATE these, not duplicate them.";

  return resp(200, { ok: true, mode: googleContactsMode(), reconcile: out });
}

// ── PROPOSED CONTACT MERGES — READ-ONLY (item 07 prerequisite) ────────────
// Admin-only. Writes NOTHING, in Neon or in Google. It reports what a merge
// would do so a person can approve it; a separate action would perform one.
//
// ⚠⚠ WHY THIS BLOCKS THE SYNC. The reconcile on 2026-08-27 found 13 duplicate
// PEOPLE among the 240 contacts — 26 rows, the same person entered twice, most
// pairs differing only in phone FORMATTING ("3307046150" vs "(330) 704-6150"),
// which is why nothing ever flagged them. Both halves carry their own Google
// person id, so Google already holds each of these people twice in BOTH
// accounts, and somebody has been merging a few by hand.
//
// Syncing all 240 as-is would therefore RE-CREATE the ones already cleaned up
// and leave the rest duplicated. That is worse than not running at all, and it
// is exactly the outcome the owner asked to avoid.
//
// ⚠ THE MERGE MUST CARRY FORWARD THE GOOGLE ID THAT STILL RESOLVES, which is
// not always the id on the row that wins on data. Keeping a dead id would make
// the very next sync create a duplicate — undoing the merge it just did.
function normPhone(v) { return String(v || "").replace(/\D+/g, ""); }
function normEmail(v) { return String(v || "").trim().toLowerCase(); }
function contactScore(r) {
  // Completeness, deliberately crude — a human approves the result anyway.
  return (r.primary_email ? 4 : 0) + (r.primary_phone ? 2 : 0) +
         (r.company_id ? 1 : 0) + (r.street ? 1 : 0) + (r.role ? 1 : 0);
}

async function handleContactDuplicates() {
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "This report needs the database. DATABASE_URL is unset." });
  }

  const q = await neonQuery(
    `SELECT id, first_name, last_name, primary_phone, primary_email, role, street, city, state, zip,
            company_id, active, google_person_id_1, google_person_id_2
       FROM contacts
      ORDER BY last_name NULLS LAST, first_name NULLS LAST`);
  if (!q?.rows) {
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }

  const groups = new Map();
  for (const r of q.rows) {
    const key = `${String(r.first_name || "").trim().toLowerCase()} ${String(r.last_name || "").trim().toLowerCase()}`.trim();
    if (!key) continue;
    (groups.get(key) || groups.set(key, []).get(key)).push(r);
  }
  const dupeGroups = [...groups.entries()].filter(([, rows]) => rows.length > 1);

  // Which of these Google ids still resolve? One batch call per account for the
  // whole report, not one per contact.
  const live = new Map(); // "<destKey>:<storedId>" -> true
  const googleChecked = googleConfigured();
  const googleErrors = [];
  if (googleChecked) {
    for (const dest of DESTINATIONS) {
      const ids = dupeGroups.flatMap(([, rows]) => rows.map(r => r[dest.column]).filter(Boolean));
      if (!ids.length) continue;
      try {
        const found = await getPeopleBatch(dest.subject, ids);
        for (const id of found.keys()) live.set(`${dest.key}:${id}`, true);
      } catch (e) {
        // Report it rather than letting "not found" stand in for "not asked".
        googleErrors.push({ account: dest.subject, hint: String(e?.message || e).slice(0, 200) });
      }
    }
  }
  const resolves = (destKey, id) => (id ? live.has(`${destKey}:${id}`) : null);

  const pairs = [];
  for (const [name, rows] of dupeGroups) {
    const phones = new Set(rows.map(r => normPhone(r.primary_phone)).filter(Boolean));
    const emails = new Set(rows.map(r => normEmail(r.primary_email)).filter(Boolean));

    // HIGH means the rows agree on a phone or an email, not merely on a name.
    // Two different people can share a name; two rows sharing a mobile number
    // are the same person.
    const phoneAgrees = phones.size === 1 && rows.every(r => !r.primary_phone || normPhone(r.primary_phone) === [...phones][0]);
    const emailAgrees = emails.size === 1 && emails.size > 0;
    const confidence = (phoneAgrees || emailAgrees) ? "HIGH" : "REVIEW";

    // Keeper: most complete row. Ties break on having a live Google id, so the
    // surviving row is the one Google already knows.
    const ranked = [...rows].sort((a, b) => {
      const s = contactScore(b) - contactScore(a);
      if (s) return s;
      const aLive = (resolves(1, a.google_person_id_1) ? 1 : 0) + (resolves(2, a.google_person_id_2) ? 1 : 0);
      const bLive = (resolves(1, b.google_person_id_1) ? 1 : 0) + (resolves(2, b.google_person_id_2) ? 1 : 0);
      return bLive - aLive;
    });
    const keep = ranked[0];
    const drop = ranked.slice(1);

    // ⚠ Field-level merge: take any value the keeper lacks from a dropped row.
    // Nothing is discarded silently — every borrowed field is named in `fills`.
    const fills = {};
    for (const f of ["primary_email", "primary_phone", "role", "street", "city", "state", "zip", "company_id"]) {
      if (!keep[f]) {
        const donor = drop.find(d => d[f]);
        if (donor) fills[f] = { value: donor[f], from: donor.id };
      }
    }

    // ⚠⚠ Google ids are chosen by WHAT STILL RESOLVES, independently of which
    // row won. A merge that keeps a dead id makes the next sync create a
    // duplicate and undo itself.
    const googleIds = {};
    for (const dest of DESTINATIONS) {
      const candidates = rows.map(r => ({ id: r[dest.column], row: r.id, live: resolves(dest.key, r[dest.column]) }))
                             .filter(c => c.id);
      const chosen = candidates.find(c => c.live === true) || candidates[0] || null;
      googleIds[dest.column] = chosen ? {
        use: chosen.id,
        fromRow: chosen.row,
        resolvesInGoogle: chosen.live,
        discarding: candidates.filter(c => c.id !== chosen.id).map(c => ({ id: c.id, resolvesInGoogle: c.live })),
      } : null;
    }

    // ⚠⚠ TWO DIFFERENT SITUATIONS WEAR THE SAME FACE, AND THEY NEED OPPOSITE
    // ORDERS OF WORK.
    //   (a) one id is dead -> somebody already merged this person in Google, and
    //       merging in Neon simply catches up. Nothing is left behind.
    //   (b) BOTH ids still resolve -> Google genuinely holds this person twice.
    //       Merging in Neon drops our reference to one of them, but the contact
    //       itself STAYS in Google forever: the sync never deletes, so nothing
    //       will ever clean it up. We would have quietly orphaned a duplicate on
    //       everyone's phone.
    // So (b) has to be merged in GOOGLE first — its own Merge & fix, which
    // previews each merge — and only then here.
    const googleStillDuplicated = [];
    for (const dest of DESTINATIONS) {
      const g = googleIds[dest.column];
      if (g?.resolvesInGoogle === true && g.discarding?.some(d => d.resolvesInGoogle === true)) {
        googleStillDuplicated.push(dest.subject);
      }
    }

    const reasons = [];
    if (phoneAgrees) reasons.push("same phone number once formatting is ignored");
    if (emailAgrees) reasons.push("same email address");
    if (!phoneAgrees && phones.size > 1) {
      reasons.push(`⚠ phone numbers DIFFER (${[...phones].join(" vs ")}) — could be a typo in one, or two different people`);
    }
    if (Object.keys(fills).length) reasons.push(`keeper is missing ${Object.keys(fills).join(", ")}, filled from the dropped row`);
    for (const dest of DESTINATIONS) {
      const g = googleIds[dest.column];
      if (g?.discarding?.length && g.resolvesInGoogle === true && g.discarding.some(d => d.resolvesInGoogle === false)) {
        reasons.push(`${dest.subject}: keeping the id that still resolves and dropping a dead one (already merged in Google by hand)`);
      }
    }

    if (googleStillDuplicated.length) {
      reasons.push(`⚠ GOOGLE STILL HOLDS BOTH COPIES in ${googleStillDuplicated.join(" and ")} — merge this person there FIRST (Contacts -> Merge & fix), or the extra copy is orphaned on people's phones with nothing left to clean it up.`);
    }

    pairs.push({
      name, confidence, rowCount: rows.length, googleStillDuplicated,
      keep: { id: keep.id, phone: keep.primary_phone, email: keep.primary_email, score: contactScore(keep) },
      drop: drop.map(d => ({ id: d.id, phone: d.primary_phone, email: d.primary_email, score: contactScore(d) })),
      fills, googleIds, reasons,
    });
  }

  pairs.sort((a, b) => (a.confidence === b.confidence ? a.name.localeCompare(b.name) : a.confidence === "HIGH" ? -1 : 1));

  return resp(200, {
    ok: true,
    // Nothing below has been written. This is a proposal.
    wroteAnything: false,
    googleChecked,
    googleErrors,
    counts: {
      totalContacts: q.rows.length,
      duplicatePeople: pairs.length,
      rowsInvolved: pairs.reduce((n, p) => n + p.rowCount, 0),
      wouldDelete: pairs.reduce((n, p) => n + p.drop.length, 0),
      high: pairs.filter(p => p.confidence === "HIGH").length,
      review: pairs.filter(p => p.confidence === "REVIEW").length,
      alreadyMergedInGoogle: pairs.filter(p => !p.googleStillDuplicated.length).length,
      googleStillHasBoth: pairs.filter(p => p.googleStillDuplicated.length).length,
    },
    orderOfWork: [
      "1. In Google Contacts (BOTH accounts), Merge & fix the people listed under googleStillHasBoth. Google previews each merge.",
      "2. Re-run this report — their second id will then read resolvesInGoogle:false.",
      "3. Then apply the Neon merges, which will pick the surviving id on its own.",
      "Merging in Neon FIRST would leave the extra Google copy orphaned, because the sync never deletes.",
    ],
    pairs,
  });
}

// ── APPLY CONTACT MERGES — THE ONLY DESTRUCTIVE ACTION HERE ───────────────
// Admin-only. Deletes contact rows, so everything about it is deliberately
// explicit: it merges ONLY the pairs handed to it, never what it works out for
// itself, and it will not write without confirm:true.
//
//   POST { action:"contactMerge",
//          merges:[{ keep:"<uuid>", drop:["<uuid>", …] }, …],
//          confirm:true }        // omit confirm for a dry run
//
// ⚠⚠ WHY THE CALLER NAMES THE KEEPER RATHER THAN THIS CODE RANKING THEM.
// contactDuplicates ranks by completeness, and on 2026-08-27 that would have got
// Mike Ware exactly backwards: the row carrying the WRONG phone number
// ((330) 260-5049, a transposition of the real (330) 206-5049) also carried a
// role, so it scored higher and would have been kept. Completeness is not
// correctness, and no scorer can know which of two phone numbers is the real
// one. A person does. So the decision arrives as input.
//
// ⚠ Safe to delete these rows only because NOTHING references contacts.id —
// verified 2026-08-27: no foreign keys, and jobs carry customer details as
// copied text (customer_first_name, customer_phone…) rather than a link.
// Re-check that before extending this to any other table.
async function handleContactMerge(body, authUser) {
  if (!neonEnabled()) {
    return resp(503, { ok: false, error: "Merging needs the database. DATABASE_URL is unset." });
  }
  const merges = Array.isArray(body?.merges) ? body.merges : null;
  if (!merges?.length) {
    return resp(400, { ok: false, error: "Nothing to do: pass merges:[{keep, drop:[…]}] from the contactDuplicates report." });
  }
  const dryRun = body?.confirm !== true;

  // Load every row named, in one query, so validation sees real data rather
  // than trusting the ids it was handed.
  const wanted = [...new Set(merges.flatMap(m => [m.keep, ...(Array.isArray(m.drop) ? m.drop : [])]).filter(Boolean))];
  if (wanted.some(id => !/^[0-9a-f-]{36}$/i.test(String(id)))) {
    return resp(400, { ok: false, error: "Every keep/drop must be a contact uuid." });
  }
  const q = await neonQuery(
    `SELECT id, first_name, last_name, primary_phone, primary_email, role, street, city, state, zip,
            company_id, active, google_person_id_1, google_person_id_2
       FROM contacts WHERE id = ANY($1::uuid[])`, [wanted]);
  if (!q?.rows) {
    return resp(503, { ok: false, error: "Can't load that right now — the database is unavailable. Try again in a moment." });
  }
  const byId = new Map(q.rows.map(r => [String(r.id), r]));

  // ── Validate the whole batch BEFORE writing any of it ────────────────────
  const problems = [];
  for (const [n, m] of merges.entries()) {
    const drop = Array.isArray(m.drop) ? m.drop : [];
    const keep = byId.get(String(m.keep));
    if (!keep) { problems.push(`merge ${n}: keep ${m.keep} is not a contact`); continue; }
    if (!drop.length) { problems.push(`merge ${n}: nothing to drop`); continue; }
    if (drop.includes(m.keep)) { problems.push(`merge ${n}: keep and drop name the same row`); continue; }
    for (const d of drop) {
      const row = byId.get(String(d));
      if (!row) { problems.push(`merge ${n}: drop ${d} is not a contact`); continue; }
      // A mistyped uuid that happens to exist would delete a real, unrelated
      // person. Names must agree.
      const nameOf = (r) => `${String(r.first_name || "").trim().toLowerCase()} ${String(r.last_name || "").trim().toLowerCase()}`.trim();
      if (nameOf(row) !== nameOf(keep)) {
        problems.push(`merge ${n}: REFUSING — "${nameOf(keep)}" and "${nameOf(row)}" are different people. Check the uuids.`);
      }
    }
  }
  if (problems.length) return resp(400, { ok: false, error: "Refused — nothing was written.", problems });

  // Which Google ids still resolve? Derived LIVE rather than taken from the
  // request, because a report read an hour ago may already be stale — and
  // keeping a dead id would make the next sync create a duplicate, undoing the
  // merge it had just performed.
  const live = new Set();
  const googleErrors = [];
  if (googleConfigured()) {
    for (const dest of DESTINATIONS) {
      const ids = q.rows.map(r => r[dest.column]).filter(Boolean);
      if (!ids.length) continue;
      try {
        const found = await getPeopleBatch(dest.subject, ids);
        for (const id of found.keys()) live.add(`${dest.key}:${id}`);
      } catch (e) { googleErrors.push({ account: dest.subject, hint: String(e?.message || e).slice(0, 200) }); }
    }
  }

  const FILLABLE = ["primary_email", "primary_phone", "role", "street", "city", "state", "zip", "company_id"];
  const plan = [];
  for (const m of merges) {
    const keep = byId.get(String(m.keep));
    const drops = m.drop.map(d => byId.get(String(d)));

    // Fill only what the keeper LACKS. The keeper's own values always win —
    // that is the whole point of naming it explicitly.
    const set = {};
    for (const f of FILLABLE) {
      if (keep[f]) continue;
      const donor = drops.find(d => d[f]);
      if (donor) set[f] = donor[f];
    }
    for (const dest of DESTINATIONS) {
      const cands = [keep, ...drops].map(r => r[dest.column]).filter(Boolean);
      const chosen = cands.find(id => live.has(`${dest.key}:${id}`)) || cands[0] || null;
      if (chosen && chosen !== keep[dest.column]) set[dest.column] = chosen;
    }

    plan.push({
      name: `${keep.first_name || ""} ${keep.last_name || ""}`.trim(),
      keep: keep.id, drop: drops.map(d => d.id),
      keeping: { phone: keep.primary_phone, email: keep.primary_email },
      set,
      googleIdsUnverified: googleErrors.length ? true : undefined,
    });
  }

  if (dryRun) {
    return resp(200, {
      ok: true, dryRun: true, wroteAnything: false, googleErrors,
      wouldDelete: plan.reduce((n, p) => n + p.drop.length, 0),
      plan,
      note: "Nothing was written. Re-send with confirm:true to apply.",
    });
  }

  // ── Apply ────────────────────────────────────────────────────────────────
  // One statement per merge, UPDATE and DELETE in a single CTE so a row can
  // never be deleted without its survivor having been updated first.
  const applied = [];
  for (const p of plan) {
    const cols = Object.keys(p.set);
    const assigns = cols.map((c, i) => `${c} = $${i + 2}`).join(", ");
    const sql = `WITH upd AS (
                   ${cols.length ? `UPDATE contacts SET ${assigns} WHERE id = $1::uuid RETURNING id`
                                 : `SELECT $1::uuid AS id`}
                 )
                 DELETE FROM contacts
                  WHERE id = ANY($${cols.length + 2}::uuid[])
                    AND EXISTS (SELECT 1 FROM upd)`;
    const params = [p.keep, ...cols.map(c => p.set[c]), p.drop];
    const r = await neonWrite("contactMerge", sql, params);
    applied.push({ name: p.name, keep: p.keep, dropped: p.drop, updated: cols, ok: !r?.error, error: r?.error });
  }

  const failed = applied.filter(a => !a.ok);
  console.log(`contactMerge: ${applied.length - failed.length}/${applied.length} merged by ${authUser?.id || "?"}`);
  return resp(failed.length ? 207 : 200, {
    ok: !failed.length, dryRun: false, googleErrors,
    merged: applied.length - failed.length, failed: failed.length, applied,
    // ⚠ The dropped rows are gone. If any of their Google ids were still live,
    // that Google contact is now unreferenced and the sync will never touch it —
    // which is why the report tells you to merge in Google FIRST.
    reminder: "Re-run contactDuplicates to confirm, then googleContactsReconcile before enabling the sync.",
  });
}

// ── THE AIRTABLE REC ID FOR A JOB HANDLE, OR NULL ─────────────────────────
// Cutover slice 6, added 2026-08-24 after the first native job could not have
// its status changed: `handleUpdateJobStatus` PATCHed `Jobs/<uuid>`, Airtable
// answered 404, and the app reported "failed to update status".
//
// ⚠⚠ THE SLICE-6 SWEEP MISSED THIS ENTIRE CLASS, AND THE MISS HAS A NAME. It
// converted job READS, guards and the emit, and stopped there — but ten handlers
// also WRITE to Airtable addressed by the same id. It is the identical mistake
// slice 5 made with `handleSetEmployeePin` (an Airtable read used as a lookup)
// wearing the other face: **a handler can be fully dual-handled in every Neon
// statement it runs and still fail on the Airtable call two lines later.**
//
// Returns the rec id when one exists, else null — and callers must treat null as
// "skip the mirror", never as an error. A native job has no Airtable record and
// that is the normal, permanent state, not a failure to repair.
//
// Fast path: a handle that already looks like a rec id is one, no query needed.
// That keeps every pre-cutover job on exactly the behaviour it had.
async function jobAirtableId(jobHandle) {
  const id = String(jobHandle || "").trim();
  if (!id) return null;
  if (id.startsWith("rec")) return id;
  const q = await neonQuery(
    `SELECT airtable_id FROM jobs WHERE id::text = $1 LIMIT 1`, [id]);
  return q?.rows?.[0]?.airtable_id || null;
}

// Mirror a PATCH onto the job's Airtable twin, if it has one.
//
// Fail-soft by contract: by the time this runs the authoritative Neon write has
// already landed, so an Airtable problem must not turn a successful save into an
// error the user sees. That is precisely what went wrong on the first native
// job — the status had been written, and the 404 from a doomed PATCH was
// reported as "failed to update status".
async function mirrorJobPatch(label, jobHandle, fields, opts = {}) {
  const recId = await jobAirtableId(jobHandle);
  if (!recId) return null;
  return await mirrorToAirtable(label, () =>
    atFetch(`${encodeURIComponent(TABLES.jobs)}/${recId}`, {
      method: "PATCH",
      body: JSON.stringify(opts.typecast ? { fields, typecast: true } : { fields }),
    }));
}

async function jobExists(jobId) {
  const id = String(jobId || "").trim();
  if (!id) return false;

  const q = await neonQuery(`SELECT 1 FROM jobs WHERE airtable_id = $1 OR id::text = $1 LIMIT 1`, [id]);
  if (q?.rows?.length) return true;
  if (q?.error) console.error(`jobExists: Neon check failed, asking Airtable — ${q.error}`);

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(id)}"` });
  return records.length > 0;
}

/* ── Jobsite photos (docs/PLAN-job-photos.md, slice 1: read-only) ───────────
 * Replaces the "View pCloud Photos" link, which dropped the user into the
 * pCloud web app — requiring a pCloud login and then exposing the whole
 * account's file tree. These handlers serve one job's photo folder and nothing
 * else. Upload still goes through the existing JotForm → Make path; that is
 * slice 2.
 *
 * pCloud is OPTIONAL here. If PCLOUD_ACCESS_TOKEN is unset or the folder id is
 * missing, `jobPhotos` answers 200 with available:false so the tab renders an
 * explanation. A photo problem must never break the job view.
 */


// Admin diagnostic: is R2 wired up correctly? Exists because every wiring
// mistake (typo'd bucket, wrong account id, unscoped token, truncated secret)
// otherwise surfaces to the user as the same useless "photos unavailable".
async function handleR2Status(params) {
  const status = await r2Status();
  // ?selfTest=1 additionally round-trips a real object through the SAME
  // presigned urls the browser uses — but server-side, where neither CORS nor
  // a service worker applies. Those two are what make a browser report every
  // upload failure as an indistinguishable "Failed to fetch".
  if (String(params?.selfTest || "") === "1") {
    return resp(200, { ok: true, r2: status, selfTest: await r2SelfTest() });
  }
  return resp(200, { ok: true, r2: status });
}

// Maps an R2 failure onto a calm answer for the client. Never echo raw R2
// error text to a non-admin — it can name bucket and account internals. The
// admin r2Status action exists for the detailed version.
function r2Unavailable(e, where) {
  const code = e instanceof R2Error ? e.code : null;
  if (code === "NOT_CONFIGURED") return { reason: "not-configured" };
  if (code === "TIMEOUT")        return { reason: "timeout" };
  if (code === "NO_SIGNER") {
    console.error(`_r2: aws4fetch missing in ${where} — check netlify.toml's npm install step`);
    return { reason: "error" };
  }
  console.error(`R2 error in ${where}: ${String(e?.message || e).slice(0, 200)}`);
  return { reason: "error" };
}

// Lists one job's photos. Every URL comes back pre-signed, so the browser
// fetches images straight from Cloudflare — no bytes through this function.
//
// Scoping is by Airtable RECORD ID, not job name. That matters: the FIND-on-
// name pattern used elsewhere in this file matches substrings, so "Jenny Ln 1"
// leaks into "Jenny Ln 10/11/12" (see TODO.md). Record ids can't collide, so
// two jobs with the same name still never see each other's photos.
async function handleJobPhotos(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", photos: [] });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  try {
    const photos = await listJobPhotos(jobId);
    return resp(200, { ok: true, available: true, photos });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "jobPhotos"), photos: [] });
  }
}

// Hands back short-lived upload URLs so the phone PUTs straight to R2.
//
// The bytes never touch this function, which is the whole point: no 4.5 MB
// Netlify payload ceiling, no 10s timeout risk on a slow jobsite connection,
// and no bandwidth cost. The function's only job is to decide the key and
// prove the caller is allowed to write to this job.
//
// Two URLs per photo — the compressed original and its thumbnail. The client
// generates both; R2 is pure storage and won't make thumbnails for us, and a
// gallery that loads full-size images would be unusable on mobile data.
async function handleJobPhotoUploadUrls(body) {
  const jobId = body?.jobId;
  const files = Array.isArray(body?.files) ? body.files : [];
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!files.length) return resp(400, { ok: false, error: "No files requested." });
  if (files.length > 25) return resp(400, { ok: false, error: "Too many photos at once (max 25)." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Photo storage isn't configured." });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  // Keys are server-decided. If the client named them, a caller could write
  // outside its own job's prefix just by sending "../otherjob/x.jpg". The
  // album name is the only client-supplied part of the path, so it is
  // sanitized and encoded (see sanitizeAlbum) before it becomes a segment.
  const album = albumSegment(body?.album);
  // YYYYMMDDHHMMSS is 14 chars, not 15 — slicing 15 off "20260801202906.123Z"
  // kept the decimal point and produced names like "20260801202906.-05-x.jpg".
  const stamp = new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14);
  try {
    const uploads = await Promise.all(files.map(async (f, i) => {
      const contentType = /^image\/(jpeg|png|webp)$/.test(String(f?.contentType || ""))
        ? f.contentType : "image/jpeg";
      const ext = contentType === "image/png" ? "png" : contentType === "image/webp" ? "webp" : "jpg";
      // Random suffix so two techs uploading in the same second can't collide.
      const rand = Math.random().toString(36).slice(2, 8);
      const key = `${jobPrefix(jobId)}${album}${stamp}-${String(i + 1).padStart(2, "0")}-${rand}.${ext}`;
      const tKey = thumbKeyFor(key);
      return {
        key, thumbKey: tKey,
        putUrl:      await presignPut(key, contentType),
        thumbPutUrl: await presignPut(tKey, contentType),
        contentType,
      };
    }));
    return resp(200, { ok: true, uploads, album: sanitizeAlbum(body?.album) });
  } catch (e) {
    const { reason } = r2Unavailable(e, "jobPhotoUploadUrls");
    return resp(502, { ok: false, error: "Could not prepare the upload.", reason });
  }
}

// Sized against Netlify's 10-second synchronous function budget. A move is up
// to four R2 round trips per photo; 12 photos at 5-way concurrency is roughly
// 10 sequential round trips of work, which leaves comfortable headroom. The
// client chunks larger selections into requests of this size.
const BULK_PHOTO_MAX = 12;
const BULK_PHOTO_CONCURRENCY = 5;

/* ── Expense receipts (docs/PLAN-expense-receipts.md, slice 1) ──────────────
 * Photograph the paper slip, or attach a ScanSnap PDF. Stored in R2 keyed by
 * the expense's record id — folder-is-the-record, same as photos, so there is
 * no new table and nothing extra to port to Neon.
 *
 * Authorization deliberately reuses the expense rules rather than inventing
 * new ones: guardExpenseMutation for attaching (owner while unreviewed,
 * admin/office always) and the same per-employee scoping handleExpenses
 * already enforces for reading.
 */

// Presigned PUTs so the phone (or the office PC, for a scan) uploads straight
// to R2. Keys are server-decided — a client-named key could write outside its
// own expense.
async function handleExpenseReceiptUploadUrls(body, authUser) {
  const expenseId = body?.expenseId;
  const files = Array.isArray(body?.files) ? body.files : [];
  if (!files.length) return resp(400, { ok: false, error: "No files requested." });
  if (files.length > 10) return resp(400, { ok: false, error: "Too many receipts at once (max 10)." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Receipt storage isn't configured." });

  // Same window as editing the expense itself: an employee may attach to their
  // own until it is reviewed; admin/office any time.
  const guard = await guardExpenseMutation(expenseId, authUser);
  if (!guard.ok) return guard.resp;

  const stamp = new Date().toISOString().replace(/[-:T]/g, "").slice(0, 14);
  try {
    const uploads = await Promise.all(files.map(async (f, i) => {
      // PDFs are scans and pass through untouched; images are compressed
      // client-side and carry a thumbnail. See receiptFileKind.
      const kind = receiptFileKind(f?.contentType);
      const rand = Math.random().toString(36).slice(2, 8);
      const key = `${expensePrefix(expenseId)}${stamp}-${String(i + 1).padStart(2, "0")}-${rand}.${kind.ext}`;

      return {
        key,
        contentType: kind.contentType,
        isPdf: kind.isPdf,
        putUrl: await presignPut(key, kind.contentType),
        thumbPutUrl: kind.wantsThumb ? await presignPut(thumbKeyFor(key), kind.contentType) : null,
      };
    }));
    return resp(200, { ok: true, uploads });
  } catch (e) {
    const { reason } = r2Unavailable(e, "expenseReceiptUploadUrls");
    return resp(502, { ok: false, error: "Could not prepare the upload.", reason });
  }
}

// Receipts for one expense. Scoped exactly like handleExpenses: admin/office
// see any, an employee sees only receipts on expenses they submitted.
async function handleExpenseReceipts(params, authUser) {
  const expenseId = params?.expenseId;
  if (!expenseId) return resp(400, { ok: false, error: "Missing expenseId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", receipts: [] });

  // Who submitted this expense? The whole Airtable record was fetched for one
  // field. Neon answers it, and only falls back when it does not know the
  // expense at all — the same rule as jobExists: the cheap store may say yes,
  // never no.
  //
  // ⚠ A NULL submitter is NOT "everyone's". Legacy rows predate the ETL
  // carrying `Submitted By` (2026-08-07) and read as NULL here, which fails the
  // owner compare below and hides them from employees — exactly what the
  // Airtable path does, where an empty `Submitted By` is never anyone's id.
  // Same reasoning as the scope clause in handleExpenses.
  let owner = null, known = false;
  const q = await neonQuery(
    `SELECT submitted_by_at_id FROM expenses
      WHERE COALESCE(airtable_id, id::text) = $1 LIMIT 1`, [expenseId]);
  if (q?.rows?.length) { known = true; owner = q.rows[0].submitted_by_at_id || null; }
  // ⚠ ONLY AN ERROR STOPS THIS, not an empty result. Zero rows here means the
  // expense is not in Neon, which the code below already handles; returning 503
  // for that would break every legitimate miss.
  else if (q?.error) {
    console.error(`expenseReceipts: Neon read FAILED — refusing to serve frozen Airtable data: ${q.error}`);
    return resp(503, { ok: false, error: "Can't load receipts right now — the database is unavailable. Try again in a moment." });
  }

  if (!known) {
    let rec;
    try { rec = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`); }
    catch { return resp(404, { ok: false, error: "Expense not found." }); }
    const submitted = rec.fields?.["Submitted By"];
    owner = (Array.isArray(submitted) ? submitted[0] : submitted) || null;
  }

  const role = (authUser?.role || "").toLowerCase();
  if (role !== "admin" && role !== "office" && owner !== authUser?.id) {
    return resp(403, { ok: false, error: "You can only see receipts on your own expenses." });
  }

  try {
    return resp(200, { ok: true, available: true, receipts: await listExpenseReceipts(expenseId) });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "expenseReceipts"), receipts: [] });
  }
}

// Delete / restore a receipt. ADMIN-OFFICE ONLY (see _ADMIN_OFFICE_POSTS):
// receipts are financial records, and unlike an expense row there is no
// "reviewed" state to key an employee window off.
//
// Soft delete — the object moves to a bin nested inside the expense, which the
// photo lifecycle rule deliberately cannot reach, so a deleted receipt is kept
// indefinitely rather than expiring at 30 days.
async function receiptMutation(body, authUser, label, fn) {
  const expenseId = body?.expenseId;
  const keys = Array.isArray(body?.keys) ? body.keys : [];
  if (!expenseId) return resp(400, { ok: false, error: "Missing expenseId." });
  if (!keys.length) return resp(400, { ok: false, error: "No receipts selected." });
  if (keys.length > 20) return resp(400, { ok: false, error: "Too many receipts at once (max 20)." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Receipt storage isn't configured." });

  // Confirms the expense exists and that this caller may touch it at all.
  const guard = await guardExpenseMutation(expenseId, authUser);
  if (!guard.ok) return guard.resp;

  let done = 0;
  const failures = [];
  for (const key of keys) {
    try { await fn(expenseId, key); done++; }
    catch (e) {
      if (e instanceof R2Error && e.code === "KEY_OUTSIDE_EXPENSE") {
        console.error(`${label}: rejected key outside expense ${expenseId}: ${String(key).slice(0, 120)}`);
      }
      failures.push({ key, error: String(e?.message || e).slice(0, 160) });
    }
  }
  return resp(200, { ok: failures.length === 0, done, failed: failures.length, failures });
}

async function handleDeleteExpenseReceipts(body, authUser) {
  return await receiptMutation(body, authUser, "deleteExpenseReceipts",
    (id, key) => softDeleteExpenseReceipt(id, key));
}

async function handleRestoreExpenseReceipts(body, authUser) {
  return await receiptMutation(body, authUser, "restoreExpenseReceipts",
    (id, key) => restoreExpenseReceipt(id, key));
}

// The receipt bin for one expense. Same scoping as reading live receipts.
async function handleDeletedExpenseReceipts(params, authUser) {
  const expenseId = params?.expenseId;
  if (!expenseId) return resp(400, { ok: false, error: "Missing expenseId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", receipts: [] });

  const guard = await guardExpenseMutation(expenseId, authUser);
  if (!guard.ok) return guard.resp;

  try {
    return resp(200, { ok: true, available: true, receipts: await listDeletedExpenseReceipts(expenseId) });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "deletedExpenseReceipts"), receipts: [] });
  }
}

// Receipt presence for every expense on a job, for the approval list — count
// plus a thumbnail of the first one. The point of receipts is being able to see
// the slip while approving the amount, and to spot at a glance which expenses
// have none.
//
// Scoping repeats handleExpenses' rule rather than trusting a client-supplied
// list of expense ids: an employee sees only their own submissions, so this
// can't become a way to enumerate a job's expenses.
async function handleExpenseReceiptSummary(params, authUser) {
  const { jobId } = params || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", receipts: {} });

  const isMgr = authUser && (authUser.role === "admin" || authUser.role === "office");

  // ⚠ THE SCOPE IS AN AUTHORIZATION BOUNDARY, NOT A FILTER — see the long note
  // in handleExpenses. This handler must return receipts for exactly the set of
  // expenses that handler returns, so it deliberately mirrors its query, its
  // scope clause and its fall-back rule rather than inventing its own. If the
  // two ever disagree, the approval list shows a receipt badge on a row the user
  // cannot see, or hides one they can.
  //
  // ⚠ `COALESCE(airtable_id, id::text)` — R2 receipt keys are built FROM the
  // Airtable rec id. Returning a uuid here would orphan every existing receipt.
  //
  // Falls back on zero rows, not just on error, for the same reason
  // handleExpenses does: an empty Neon answer and an unsynced one look
  // identical, and this pair has to agree.
  let visibleIds = null;
  const q = await neonQuery(
    `SELECT COALESCE(e.airtable_id, e.id::text) AS id
       FROM expenses e
      WHERE (e.job_airtable_id = $1 OR e.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
        AND ($2 OR e.submitted_by_at_id = $3)`,
    [jobId, isMgr === true, authUser?.id || null]);
  if (q?.rows?.length) visibleIds = q.rows.map(r => r.id);
  // ⚠ ONLY AN ERROR STOPS THIS, not an empty result. Zero rows here means the
  // expense is not in Neon, which the code below already handles; returning 503
  // for that would break every legitimate miss.
  else if (q?.error) {
    console.error(`expenseReceiptSummary: Neon read FAILED — refusing to serve frozen Airtable data: ${q.error}`);
    return resp(503, { ok: false, error: "Can't load receipts right now — the database is unavailable. Try again in a moment." });
  }

  if (visibleIds === null) {
    const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
    if (!jobRecords.length) return resp(200, { ok: true, available: true, receipts: {} });

    const safeName = escapeFormulaString(jobRecords[0].fields["Job Name"] || "");
    const filter = `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n")`;
    const all = await fetchAll("Expenses", { filter });
    const onJob = all.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));

    const visible = isMgr
      ? onJob
      : onJob.filter(r => Array.isArray(r.fields?.["Submitted By"]) && r.fields["Submitted By"].includes(authUser?.id));
    visibleIds = visible.map(r => r.id);
  }

  try {
    return resp(200, {
      ok: true, available: true,
      receipts: await summarizeExpenseReceipts(visibleIds),
    });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "expenseReceiptSummary"), receipts: {} });
  }
}

// Shared shape for the two bulk photo mutations: validate the job once, then
// apply `fn` per key and report per-key outcomes rather than failing the whole
// batch. Selecting 40 photos and having one bad key abort the lot is the wrong
// behaviour when the user is standing in a parking lot.
async function bulkPhotoOp(body, label, fn, noun = "photos") {
  const jobId = body?.jobId;
  const keys = Array.isArray(body?.keys) ? body.keys : [];
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!keys.length) return resp(400, { ok: false, error: `No ${noun} selected.` });
  // Netlify gives a synchronous function 10 seconds. A move is four R2 calls
  // (copy, copy thumb, delete, delete thumb), so 47 photos done sequentially
  // was ~188 round trips and returned a 504 with nothing moved. The client now
  // chunks; this cap is the backstop that keeps one request inside the budget.
  if (keys.length > BULK_PHOTO_MAX) {
    return resp(400, { ok: false, error: `Too many ${noun} in one request (max ${BULK_PHOTO_MAX}).` });
  }
  if (!r2Enabled()) return resp(503, { ok: false, error: "Photo storage isn't configured." });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  // Run several at once. Sequential was the direct cause of the 504: each
  // photo is up to four R2 round trips, and they are almost entirely waiting
  // on the network, so concurrency cuts wall-clock roughly linearly.
  let done = 0;
  const failures = [];
  let next = 0;
  async function worker() {
    for (;;) {
      const i = next++;
      if (i >= keys.length) return;
      const key = keys[i];
      try { await fn(jobId, key); done++; }
      catch (e) {
        // KEY_OUTSIDE_JOB means the client sent a key belonging to another job.
        // That is either a bug or someone probing — log it loudly either way.
        if (e instanceof R2Error && e.code === "KEY_OUTSIDE_JOB") {
          console.error(`${label}: rejected key outside job ${jobId}: ${String(key).slice(0, 120)}`);
        }
        failures.push({ key, error: String(e?.message || e).slice(0, 160) });
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(BULK_PHOTO_CONCURRENCY, keys.length) }, worker));

  return resp(200, { ok: failures.length === 0, done, failed: failures.length, failures });
}

// Soft delete: photos move to the recycle bin, out of the gallery but
// recoverable for 30 days. R2 has no versioning, so before this a mis-tap on a
// 40-photo selection was unrecoverable — and with no backup yet, the app's own
// Delete button was the most likely way these photos ever got lost.
//
// Admin/office only, because nothing records who took a photo, so the
// "uploader may delete their own until reviewed" rule used for expenses can't
// be enforced here.
async function handleDeleteJobPhotos(body) {
  return await bulkPhotoOp(body, "deleteJobPhotos", (jobId, key) => softDeleteJobPhoto(jobId, key));
}

async function handleRestoreJobPhotos(body) {
  return await bulkPhotoOp(body, "restoreJobPhotos", (jobId, key) => restoreJobPhoto(jobId, key));
}

// The real, permanent delete. purgeJobPhoto refuses any key that isn't already
// in the bin, so this can never be pointed at live photos.
async function handlePurgeJobPhotos(body) {
  return await bulkPhotoOp(body, "purgeJobPhotos", (jobId, key) => purgeJobPhoto(jobId, key));
}

// Generated documents attached to a job — currently the inventory app's
// materials PDF, archived at push time.
//
// ADMIN/OFFICE ONLY (see _ADMIN_READS). The materials PDF itemises unit costs
// and job totals, which is exactly the pricing detail the rest of the app takes
// care to keep from employees — the Expenses view already scopes an employee to
// their own submissions and hides job totals. Receipts an employee entered
// themselves are a different matter and stay visible to them.
async function handleJobDocs(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", docs: [] });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  try {
    return resp(200, { ok: true, available: true, docs: await listJobDocs(jobId) });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "jobDocs"), docs: [] });
  }
}

/* ── Job prints (docs/PLAN-job-prints.md) ───────────────────────────────────
 * The drawings a crew needs on site — opened from the job, no pCloud login.
 * Same machinery as photos (presigned PUT/GET, folder-is-the-record, nothing in
 * Airtable) with three deliberate differences:
 *
 *  1. READING IS OPEN TO EVERY SIGNED-IN ROLE. jobDocs above is admin/office
 *     because the materials PDF itemises unit costs; prints are the opposite,
 *     and being readable in the field is the entire point.
 *  2. The original filename is preserved. "E-1 Rev B.pdf" is how a crew knows
 *     which sheet it is holding — a server-generated name throws that away.
 *     That makes the name client-supplied, hence sanitizePrintName.
 *  3. No compression and no thumbnail. A print is a document; the browser's
 *     PDF viewer renders it better than any tile, and running a 300 dpi sheet
 *     through the image compressor would destroy the only thing on it that
 *     matters — the small text.
 *
 * This does NOT replace pCloud. pCloud stays the office document tree; this is
 * a field-accessible copy of the drawings the crew actually needs.
 */
async function handleJobPrints(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", prints: [] });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  try {
    return resp(200, { ok: true, available: true, prints: await listJobPrints(jobId) });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "jobPrints"), prints: [] });
  }
}

// The prints bin. Admin/office only, matching the restore/purge actions on it.
async function handleJobPrintsDeleted(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", prints: [] });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  try {
    return resp(200, { ok: true, available: true, prints: await listDeletedJobPrints(jobId) });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "jobPrintsDeleted"), prints: [] });
  }
}

// Prints are documents, not photos: whatever the browser reports is stored
// as-is where it is recognised, and anything unrecognised becomes a plain
// download rather than being coerced. Coercion is what bit receipts — an
// unrecognised type there fell back to image/jpeg, so a .docx was stored with a
// .jpg extension and rendered as a permanently broken tile.
const PRINT_CONTENT_TYPES = new Set([
  "application/pdf",
  "image/jpeg", "image/png", "image/webp", "image/heic", "image/heif",
  "image/vnd.dwg", "application/dxf",
]);

function printContentType(raw, name) {
  const t = String(raw || "").toLowerCase().split(";")[0].trim();
  if (PRINT_CONTENT_TYPES.has(t)) return t;
  // Windows hands over an empty type often enough to matter, so fall back to
  // the extension — the same sniff order uploadReceiptFiles uses client-side.
  if (/\.pdf$/i.test(name || "")) return "application/pdf";
  if (/\.(jpe?g)$/i.test(name || "")) return "image/jpeg";
  if (/\.png$/i.test(name || "")) return "image/png";
  return "application/octet-stream";
}

// Presigned PUTs straight to R2 — the bytes never pass through this function,
// which is what makes a 40 MB drawing set possible at all (Netlify caps a
// function payload at 4.5 MB).
//
// Any non-viewer may upload: a crew member photographing a marked-up sheet on
// site is a legitimate print, and gating that on admin would mean it never
// happens. Removing one is admin/office — see _ADMIN_OFFICE_POSTS.
async function handleJobPrintUploadUrls(body) {
  const jobId = body?.jobId;
  const files = Array.isArray(body?.files) ? body.files : [];
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!files.length) return resp(400, { ok: false, error: "No files requested." });
  if (files.length > 15) return resp(400, { ok: false, error: "Too many prints at once (max 15)." });
  if (!r2Enabled()) return resp(503, { ok: false, error: "Print storage isn't configured." });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  try {
    // The filename is the only client-supplied part of the key and it is
    // sanitized before it becomes one — a raw name could carry a slash (a
    // forged path segment) or ".." (a climb out of this job's prefix).
    //
    // Uploading the same name twice REPLACES the earlier file. That is the
    // right default for a document — "here is the corrected E-1" — and the
    // client warns before it happens, having already listed what is there.
    const seen = new Set();
    const uploads = await Promise.all(files.map(async (f, i) => {
      const contentType = printContentType(f?.contentType, f?.name);
      let name = sanitizePrintName(f?.name);
      if (!name) name = `print-${String(i + 1).padStart(2, "0")}${contentType === "application/pdf" ? ".pdf" : ""}`;
      // Two files sanitizing to the same name inside ONE request would have the
      // second silently overwrite the first before anyone could see either.
      if (seen.has(name.toLowerCase())) name = `${i + 1}-${name}`;
      seen.add(name.toLowerCase());

      const key = `${jobPrintsPrefix(jobId)}${name}`;
      return { key, name, putUrl: await presignPut(key, contentType), contentType };
    }));
    return resp(200, { ok: true, uploads });
  } catch (e) {
    const { reason } = r2Unavailable(e, "jobPrintUploadUrls");
    return resp(502, { ok: false, error: "Could not prepare the upload.", reason });
  }
}

// Soft delete — out of the list, into the prints bin, still restorable.
async function handleDeleteJobPrints(body) {
  return await bulkPhotoOp(body, "deleteJobPrints", (jobId, key) => softDeleteJobPrint(jobId, key), "prints");
}

async function handleRestoreJobPrints(body) {
  return await bulkPhotoOp(body, "restoreJobPrints", (jobId, key) => restoreJobPrint(jobId, key), "prints");
}

// Permanent. This is the one that actually reclaims storage — a binned print
// still costs, and the prints bin is deliberately outside the lifecycle rule
// that expires deleted photos after 30 days, so nothing here leaves on its own.
async function handlePurgeJobPrints(body) {
  return await bulkPhotoOp(body, "purgeJobPrints", (jobId, key) => purgeJobPrint(jobId, key), "prints");
}

/* ── Panel schedules (docs/PLAN-panel-schedules.md) ─────────────────────────
 * The grid that goes in the panel door: circuit numbers down both sides, odd on
 * the left, even on the right, and what each breaker feeds.
 *
 * THIS DOMAIN IS NEON-NATIVE. It has no Airtable table and never will — it is
 * the first thing in this app born in Neon instead of migrated to it. So unlike
 * every other handler here there is no Airtable write, no mirror, and no
 * fallback: `neonWrite` throughout, which FAILS CLOSED. A panel schedule that
 * silently failed to save is worse than an error, because the crew walks away
 * from the panel believing it is recorded.
 *
 * Reads are open to every signed-in role, for the same reason prints are: the
 * electrician standing at the panel is the person who needs this.
 */

// Panels are keyed on the AIRTABLE job id, not a FK to Neon's jobs.id — the
// jobs table refreshes hourly, so a job created ten minutes ago is not in Neon
// yet and a FK would reject the first panel added to it. job_id backfills.
const PANEL_MAX_CIRCUITS = 84;

function mapPanel(row) {
  return {
    id: row.id,
    name: row.name,
    voltage: row.voltage || "",
    circuits: row.circuits,
    feed: row.feed || "",
    mounting: row.mounting || "",
    enclosure: row.enclosure || "",
    location: row.location || "",
    fedFrom: row.fed_from || "",
    notes: row.notes || "",
    updatedAt: row.updated_at,
    updatedBy: row.updated_by || "",
    // Present on the list read only — how much of the panel is actually filled
    // in, so a crew can see at a glance which panels still need walking.
    filled: row.filled != null ? Number(row.filled) : undefined,
  };
}

// Even, in range, and a number. Returns null when the value is unusable so the
// caller can 400 rather than letting the CHECK constraint raise a 500 that
// reads like a bug to whoever is standing in the panel room.
function cleanCircuitCount(raw) {
  const n = Math.round(Number(raw));
  if (!Number.isFinite(n) || n < 2 || n > PANEL_MAX_CIRCUITS) return null;
  return n % 2 === 0 ? n : null;
}

async function handlePanelSchedules(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Panel schedules are unavailable (database not configured)." });

  const rows = await neonWrite("panels.list",
    `SELECT p.*,
            (SELECT count(*) FROM panel_circuits c
              WHERE c.panel_id = p.id AND c.description <> '')::int AS filled
       FROM panel_schedules p
      WHERE (p.job_airtable_id = $1 OR p.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
      ORDER BY p.name`,
    [String(jobId)]);
  return resp(200, { ok: true, panels: (rows || []).map(mapPanel) });
}

// One panel plus every circuit. Circuits are returned for the FULL width of the
// panel even where no row exists yet — a panel created and never edited has no
// rows in panel_circuits at all, and the editor still has to render 42 empty
// numbered slots. generate_series is what makes that the database's job rather
// than a gap-filling loop on the client that would drift from `circuits`.
async function handlePanelSchedule(params) {
  const panelId = params?.panelId;
  if (!panelId) return resp(400, { ok: false, error: "Missing panelId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Panel schedules are unavailable (database not configured)." });

  const rows = await neonWrite("panels.get",
    `SELECT * FROM panel_schedules WHERE id::text = $1`, [String(panelId)]);
  const panel = rows?.[0];
  if (!panel) return resp(404, { ok: false, error: "Panel not found." });

  const circuits = await neonWrite("panels.circuits",
    `SELECT g AS number,
            COALESCE(c.description, '') AS description,
            c.watts, c.amps, c.poles
       FROM generate_series(1, $2::int) g
       LEFT JOIN panel_circuits c ON c.panel_id = $1::uuid AND c.number = g
      ORDER BY g`,
    [String(panelId), panel.circuits]);

  return resp(200, {
    ok: true,
    panel: mapPanel(panel),
    circuits: (circuits || []).map(c => ({
      number: Number(c.number),
      description: c.description || "",
      watts: c.watts, amps: c.amps, poles: c.poles,
    })),
  });
}

async function handleCreatePanelSchedule(body, authUser) {
  const jobId = body?.jobId;
  const name  = String(body?.name || "").trim();
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!name)  return resp(400, { ok: false, error: "Give the panel a name." });

  // Input is validated BEFORE the database check on purpose: a bad circuit
  // count is wrong whatever the infrastructure is doing, and a 503 would hide
  // the real problem behind one the user cannot act on.
  const circuits = cleanCircuitCount(body?.circuits);
  if (circuits === null) {
    return resp(400, { ok: false, error: `Circuits must be an even number between 2 and ${PANEL_MAX_CIRCUITS}.` });
  }
  if (!neonEnabled()) return resp(503, { ok: false, error: "Panel schedules are unavailable (database not configured)." });

  // ⚠ This used to read "deliberately not a Neon lookup: a job created in the
  // last hour is not in Neon yet." That is no longer true — handleCreateJob
  // inserts into Neon in the same request (2026-08-20) — and jobExists preserves
  // the guarantee regardless, because it re-asks Airtable whenever Neon says no.
  // A job the hourly sync has not adopted still gets its panels.
  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  const rows = await neonWrite("panels.create",
    `INSERT INTO panel_schedules
       (job_airtable_id, job_id, name, voltage, circuits, location, fed_from, updated_by)
     VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1), $2, $3, $4, $5, $6, $7)
     RETURNING *`,
    [String(jobId), name, String(body?.voltage || "").trim() || null, circuits,
     String(body?.location || "").trim() || null, String(body?.fedFrom || "").trim() || null,
     authUser?.name || null]);

  return resp(200, { ok: true, panel: mapPanel(rows[0]) });
}

// Saves the WHOLE panel in one request — header plus every circuit. Not
// per-cell: a panel room is where signal goes to die, and 42 in-flight autosaves
// is 42 chances to half-save a schedule. One request either lands or doesn't,
// and the client keeps a local draft until it does.
async function handleSavePanelSchedule(body, authUser) {
  const panelId = body?.panelId;
  if (!panelId) return resp(400, { ok: false, error: "Missing panelId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Panel schedules are unavailable (database not configured)." });

  const existing = (await neonWrite("panels.forSave",
    `SELECT * FROM panel_schedules WHERE id::text = $1`, [String(panelId)]))?.[0];
  if (!existing) return resp(404, { ok: false, error: "Panel not found." });

  const name = body?.name != null ? String(body.name).trim() : existing.name;
  if (!name) return resp(400, { ok: false, error: "Give the panel a name." });

  let circuits = existing.circuits;
  if (body?.circuits != null) {
    circuits = cleanCircuitCount(body.circuits);
    if (circuits === null) {
      return resp(400, { ok: false, error: `Circuits must be an even number between 2 and ${PANEL_MAX_CIRCUITS}.` });
    }
  }

  // SHRINKING DESTROYS DATA. Going 42 → 30 orphans circuits 31-42, which may be
  // the only record of what those breakers feed. The count of what would be lost
  // is returned so the client can name it in the confirm; `confirmShrink` is the
  // client saying the user saw that number and accepted it.
  if (circuits < existing.circuits) {
    const lost = (await neonWrite("panels.shrinkCheck",
      `SELECT count(*)::int AS n FROM panel_circuits
        WHERE panel_id = $1::uuid AND number > $2 AND description <> ''`,
      [String(panelId), circuits]))?.[0]?.n || 0;
    if (lost > 0 && !body?.confirmShrink) {
      // `error` carries the human sentence because that is the only field the
      // client's apiPost lifts onto the thrown Error — a machine code here would
      // surface to the user as the literal string "shrink-would-discard".
      return resp(409, {
        ok: false,
        error: `Shrinking to ${circuits} circuits discards ${lost} filled-in circuit${lost === 1 ? "" : "s"}. Tap Save again to confirm.`,
        code: "shrink-would-discard",
        lost,
      });
    }
    await neonWrite("panels.trim",
      `DELETE FROM panel_circuits WHERE panel_id = $1::uuid AND number > $2`,
      [String(panelId), circuits]);
  }

  await neonWrite("panels.update",
    `UPDATE panel_schedules
        SET name = $2, voltage = $3, circuits = $4, location = $5, fed_from = $6,
            feed = $7, mounting = $8, enclosure = $9, notes = $10,
            updated_at = now(), updated_by = $11,
            -- Column-to-column, NOT a parameter, so it deliberately keeps the bare
            -- airtable_id (slice 6 checked it). It heals a legacy panel whose
            -- job_airtable_id was set before job_id existed. A native job never
            -- reaches this branch: its rows are written with job_id already
            -- populated and job_airtable_id NULL, so the COALESCE is a no-op.
            job_id = COALESCE(job_id, (SELECT id FROM jobs WHERE airtable_id = job_airtable_id))
      WHERE id = $1::uuid`,
    [String(panelId), name, String(body?.voltage ?? existing.voltage ?? "").trim() || null, circuits,
     String(body?.location ?? existing.location ?? "").trim() || null,
     String(body?.fedFrom ?? existing.fed_from ?? "").trim() || null,
     String(body?.feed ?? existing.feed ?? "").trim() || null,
     String(body?.mounting ?? existing.mounting ?? "").trim() || null,
     String(body?.enclosure ?? existing.enclosure ?? "").trim() || null,
     String(body?.notes ?? existing.notes ?? "").trim() || null,
     authUser?.name || null]);

  // Circuits arrive as the whole panel. Out-of-range numbers are dropped rather
  // than rejected: a stale editor open on a 42-way panel that someone else
  // shrank to 30 should save the 30 it shares, not fail the request outright.
  const sent = Array.isArray(body?.circuits_list) ? body.circuits_list : [];
  const nums = [], descs = [], poles = [];
  for (const c of sent) {
    const n = Math.round(Number(c?.number));
    if (!Number.isFinite(n) || n < 1 || n > circuits) continue;
    nums.push(n);
    descs.push(String(c?.description ?? "").trim().slice(0, 200));
    // `poles` marks a ganged breaker and lives on the FIRST circuit of the span
    // only: 2 on circuit 1 means it also occupies circuit 3 (same side, next
    // number up). The covered circuits carry null, which is what stops two
    // adjacent 2-pole breakers from reading as one 4-pole.
    const p = Math.round(Number(c?.poles));
    poles.push(p === 2 || p === 3 ? p : 0);
  }

  if (nums.length) {
    // One statement, not one per circuit: 42 sequential round trips to Neon is
    // most of the 10 seconds Netlify gives a synchronous function.
    await neonWrite("panels.saveCircuits",
      `INSERT INTO panel_circuits (panel_id, number, description, poles)
       SELECT $1::uuid, n, d, NULLIF(p, 0)
         FROM unnest($2::int[], $3::text[], $4::int[]) AS t(n, d, p)
       ON CONFLICT (panel_id, number)
       DO UPDATE SET description = EXCLUDED.description, poles = EXCLUDED.poles`,
      [String(panelId), nums, descs, poles]);
  }

  const saved = (await neonWrite("panels.afterSave",
    `SELECT * FROM panel_schedules WHERE id = $1::uuid`, [String(panelId)]))?.[0];
  return resp(200, { ok: true, panel: mapPanel(saved) });
}

// Admin/office only. The circuits go with it (ON DELETE CASCADE) and there is no
// bin — a panel schedule is cheap to re-walk, unlike a photo, and a soft-delete
// tier here would be machinery nobody asked for.
async function handleDeletePanelSchedule(body) {
  const panelId = body?.panelId;
  if (!panelId) return resp(400, { ok: false, error: "Missing panelId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Panel schedules are unavailable (database not configured)." });

  const rows = await neonWrite("panels.delete",
    `DELETE FROM panel_schedules WHERE id::text = $1 RETURNING id`, [String(panelId)]);
  if (!rows?.length) return resp(404, { ok: false, error: "Panel not found." });
  return resp(200, { ok: true });
}

/* ── Job checklists (docs/PLAN-job-checklists.md) ───────────────────────────
 * The Trello checklist a crew keeps per job — "Supplies from shop", "Punch
 * list" — brought into the app. Name a list, type items one per line, tick
 * them off while loading the truck.
 *
 * Neon-native like panel schedules: no Airtable table, no mirror, `neonWrite`
 * throughout so writes fail CLOSED.
 *
 * TICKED IS NOT DELETED. `done` flips and the row stays, because the client
 * moves ticked items into a collapsed "Loaded" section that can be reopened.
 * A mis-tap on a phone in a truck must not be able to lose the line — you
 * would arrive without the pipe and never know which item went missing.
 */
function mapChecklist(row) {
  return {
    id: row.id,
    name: row.name,
    createdBy: row.created_by || "",
    updatedAt: row.updated_at,
    open: row.open != null ? Number(row.open) : undefined,
    done: row.done_count != null ? Number(row.done_count) : undefined,
  };
}

function mapChecklistItem(row) {
  return {
    id: row.id,
    body: row.body,
    done: row.done === true,
    doneBy: row.done_by || "",
    doneAt: row.done_at,
    position: Number(row.position),
  };
}

async function handleJobChecklists(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  const rows = await neonWrite("checklists.list",
    `SELECT c.*,
            (SELECT count(*) FROM checklist_items i WHERE i.checklist_id = c.id AND NOT i.done)::int AS open,
            (SELECT count(*) FROM checklist_items i WHERE i.checklist_id = c.id AND     i.done)::int AS done_count
       FROM job_checklists c
      WHERE (c.job_airtable_id = $1 OR c.job_id = (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1))
      ORDER BY c.created_at`,
    [String(jobId)]);
  const lists = (rows || []).map(mapChecklist);
  // The action-row badge is "how many things do I still need?" across the whole
  // job, so it is summed here rather than by the client counting lists.
  return resp(200, { ok: true, lists, openTotal: lists.reduce((s, l) => s + (l.open || 0), 0) });
}

async function handleJobChecklist(params) {
  const listId = params?.listId;
  if (!listId) return resp(400, { ok: false, error: "Missing listId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  const rows = await neonWrite("checklists.get",
    `SELECT * FROM job_checklists WHERE id::text = $1`, [String(listId)]);
  const list = rows?.[0];
  if (!list) return resp(404, { ok: false, error: "List not found." });

  const items = await neonWrite("checklists.items",
    `SELECT * FROM checklist_items WHERE checklist_id = $1::uuid ORDER BY position, created_at`,
    [String(listId)]);

  return resp(200, { ok: true, list: mapChecklist(list), items: (items || []).map(mapChecklistItem) });
}

async function handleCreateChecklist(body, authUser) {
  const jobId = body?.jobId;
  const name  = String(body?.name || "").trim().slice(0, 120);
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!name)  return resp(400, { ok: false, error: "Give the list a name." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  // ⚠ The note that used to sit here said "deliberately NOT a Neon lookup",
  // citing the keying trap in db/schema/008 — a job created ten minutes ago was
  // not in Neon yet, so checking Neon would have refused the first list anyone
  // made on a new job. That reason expired twice over: handleCreateJob inserts
  // into Neon in the same request now (2026-08-20), and jobExists re-asks
  // Airtable whenever Neon says no, so it could not have refused it even then.
  // The KEYING is unchanged — job_airtable_id is still the key, job_id still a
  // nullable FK that backfills.
  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  const rows = await neonWrite("checklists.create",
    `INSERT INTO job_checklists (job_airtable_id, job_id, name, created_by)
     VALUES (CASE WHEN $1 LIKE 'rec%' THEN $1 ELSE NULL END, (SELECT id FROM jobs WHERE airtable_id = $1 OR id::text = $1), $2, $3)
     RETURNING *`,
    [String(jobId), name, authUser?.name || null]);
  return resp(200, { ok: true, list: mapChecklist(rows[0]) });
}

// One item at a time, because that is how they are typed: a line, Enter, the
// next line. A batch endpoint would need the client to hold unsaved text, which
// is the thing this design avoids.
async function handleAddChecklistItem(body, authUser) {
  const listId = body?.listId;
  const text   = String(body?.body ?? body?.text ?? "").trim().slice(0, 300);
  if (!listId) return resp(400, { ok: false, error: "Missing listId." });
  if (!text)   return resp(400, { ok: false, error: "Type something to add." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  const exists = await neonWrite("checklists.forAdd",
    `SELECT id FROM job_checklists WHERE id::text = $1`, [String(listId)]);
  if (!exists?.length) return resp(404, { ok: false, error: "List not found." });

  // position = end of the list, computed server-side so two people adding at
  // once can't land on the same number.
  const rows = await neonWrite("checklists.addItem",
    `INSERT INTO checklist_items (checklist_id, body, position, created_by)
     VALUES ($1::uuid, $2,
             (SELECT COALESCE(MAX(position), 0) + 1 FROM checklist_items WHERE checklist_id = $1::uuid),
             $3)
     RETURNING *`,
    [String(listId), text, authUser?.name || null]);

  await neonWrite("checklists.touch",
    `UPDATE job_checklists SET updated_at = now() WHERE id = $1::uuid`, [String(listId)]);
  return resp(200, { ok: true, item: mapChecklistItem(rows[0]) });
}

// Idempotent on purpose: the client queues ticks made with no signal and
// replays them, so the same "done: true" may arrive twice. Setting a row that
// is already done to done is a no-op, not an error.
async function handleSetChecklistItemDone(body, authUser) {
  const itemId = body?.itemId;
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });
  const done = body?.done !== false;

  const rows = await neonWrite("checklists.setDone",
    `UPDATE checklist_items
        SET done = $2,
            done_at = CASE WHEN $2 THEN now() ELSE NULL END,
            done_by = CASE WHEN $2 THEN $3   ELSE NULL END
      WHERE id::text = $1
      RETURNING *`,
    [String(itemId), done, authUser?.name || null]);
  if (!rows?.length) return resp(404, { ok: false, error: "Item not found." });
  return resp(200, { ok: true, item: mapChecklistItem(rows[0]) });
}

// The client sends EVERY item id in the order it is showing them — open items
// in their new order, then the loaded ones — and positions are rewritten 1..N
// to match. Sending only the dragged item and its new index would need the
// server to re-derive everyone else's position, and two crews dragging at once
// would interleave into an order neither of them chose.
//
// `WHERE checklist_id` is the guard that matters: without it, a crafted id list
// could renumber items belonging to another job's list.
async function handleReorderChecklistItems(body) {
  const listId = body?.listId;
  const ids = Array.isArray(body?.itemIds) ? body.itemIds.map(String) : [];
  if (!listId) return resp(400, { ok: false, error: "Missing listId." });
  if (!ids.length) return resp(400, { ok: false, error: "No items to reorder." });

  // A malformed id would abort the whole statement on the uuid cast, so the
  // shape is checked here rather than letting Postgres raise. Before the
  // neonEnabled() check, like the panel validators: a bad id is a bad id
  // whatever the database is doing, and a 503 would hide it.
  if (!ids.every(id => /^[0-9a-f-]{36}$/i.test(id))) {
    return resp(400, { ok: false, error: "Bad item id." });
  }
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  const rows = await neonWrite("checklists.reorder",
    `UPDATE checklist_items c
        SET position = t.ord
       FROM unnest($2::uuid[]) WITH ORDINALITY AS t(id, ord)
      WHERE c.id = t.id AND c.checklist_id = $1::uuid
      RETURNING c.id`,
    [String(listId), ids]);

  await neonWrite("checklists.touchOrder",
    `UPDATE job_checklists SET updated_at = now() WHERE id = $1::uuid`, [String(listId)]);
  return resp(200, { ok: true, moved: rows?.length || 0 });
}

// Deleting an ITEM is _NON_VIEWER: you typed it wrong, you fix it. Deleting a
// whole LIST is admin/office — see _ADMIN_OFFICE_POSTS.
async function handleDeleteChecklistItem(body) {
  const itemId = body?.itemId;
  if (!itemId) return resp(400, { ok: false, error: "Missing itemId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  const rows = await neonWrite("checklists.deleteItem",
    `DELETE FROM checklist_items WHERE id::text = $1 RETURNING id`, [String(itemId)]);
  if (!rows?.length) return resp(404, { ok: false, error: "Item not found." });
  return resp(200, { ok: true });
}

async function handleDeleteChecklist(body) {
  const listId = body?.listId;
  if (!listId) return resp(400, { ok: false, error: "Missing listId." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Checklists are unavailable (database not configured)." });

  const rows = await neonWrite("checklists.delete",
    `DELETE FROM job_checklists WHERE id::text = $1 RETURNING id`, [String(listId)]);
  if (!rows?.length) return resp(404, { ok: false, error: "List not found." });
  return resp(200, { ok: true });
}

// Recycle-bin listing. Admin/office only — employees shouldn't be browsing
// what was deleted.
async function handleJobPhotosDeleted(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
  if (!r2Enabled()) return resp(200, { ok: true, available: false, reason: "not-configured", photos: [] });

  if (!(await jobExists(jobId))) return resp(404, { ok: false, error: "Job not found." });

  try {
    return resp(200, { ok: true, available: true, photos: await listDeletedJobPhotos(jobId) });
  } catch (e) {
    return resp(200, { ok: true, available: false, ...r2Unavailable(e, "jobPhotosDeleted"), photos: [] });
  }
}

// Re-filing a photo is not destructive (copy-then-delete, and a failed delete
// leaves a duplicate rather than a hole), so any non-viewer may do it.
async function handleMoveJobPhotos(body) {
  const album = sanitizeAlbum(body?.album) || "";
  return await bulkPhotoOp(body, "moveJobPhotos", (jobId, key) => moveJobPhoto(jobId, key, album));
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") return resp(200, { ok: true });
    ensureEnv();

    // ── Server-side authn + authz (see _auth.js) ─────────────────────────────
    // Every action except `login` requires a valid signed token; role is then
    // checked per action. The browser's claimed role is no longer trusted.
    const reqAction = event.httpMethod === "GET"
      ? event.queryStringParameters?.action
      : safeBodyAction(event);
    // Hoisted so expense handlers can scope/authorize by the signed-in user
    // (see-own, edit/delete-until-approved). Null only for the login action.
    // ⚠ The ONE action that skips the bearer check. A home-screen widget host has
    // no session and often cannot set headers, so `clockWidget` carries its own
    // signed, per-person, single-purpose token in the query string and verifies
    // it itself. See the long note above handleClockWidget for why this is narrow
    // enough to be safe, and what would stop making it so.
    if (event.httpMethod === "GET" && reqAction === "clockWidget") {
      return await handleClockWidget(event.queryStringParameters || {});
    }

    // ⚠ The SECOND action that skips the bearer check, and for the same reason:
    // Make.com posts the result of the Awarded scenario back here and has no
    // session to present. It carries a scope token signed for that one job
    // instead — see handleJobAutomationResult, which verifies it before touching
    // anything. Narrow by construction: the token names the record.
    if (event.httpMethod === "POST" && reqAction === "jobAutomationResult") {
      let parsed = null;
      try { parsed = JSON.parse(event.body || "{}"); } catch { parsed = null; }
      if (!parsed) return resp(400, { ok: false, error: "Malformed JSON body." });
      return await handleJobAutomationResult(parsed);
    }

    // ── Warm-up: wake Neon while the browser is still parsing the app ────────
    // Neon scales to zero after ~5 minutes idle, so the first request of the
    // day pays a compute wake AND a cold page cache. The browser has to
    // download and parse ~24,000 lines of index.html before it can issue a
    // single API call, and that window is currently spent with the database
    // asleep. Firing this from the top of <head> overlaps the two.
    //
    // ⚠ UNAUTHENTICATED, and that is safe here in a way it would not be for
    // any other action: it runs BEFORE the app knows who you are (30-day
    // tokens mean the crew never sees a login screen — boot goes straight to
    // loadJobs), it takes no parameters, it touches no table, and it returns
    // nothing but ok/ms. There is no data to leak because none is read.
    //
    // ⚠ It does NOT warm the page cache — `SELECT 1` wakes the compute and
    // nothing more. Making it warm the cache would mean running an expensive
    // unauthenticated query on every page load, which is a much worse trade.
    // The query cost was dealt with separately in db/schema/030.
    if (event.httpMethod === "GET" && reqAction === "warmup") {
      if (!neonEnabled()) return resp(200, { ok: true, warmed: false, reason: "no-database" });
      const t0 = Date.now();
      const q = await neonQuery("SELECT 1 AS ok");
      return resp(200, { ok: true, warmed: !!q?.rows?.length, ms: Date.now() - t0 });
    }

    let authUser = null;
    if (reqAction !== "login") {
      authUser = authedUser(event);
      if (!authUser) return resp(401, { ok: false, error: "Not signed in. Please log in again." });
      // A valid signature is not the same as a live session. Tokens last 30 days
      // and carry no server state, so deactivating someone used to leave their
      // phone working until the token expired. See _revocation.js — this is the
      // check that makes the People screen's Active toggle mean anything.
      if (await isSessionRevoked(authUser)) {
        return resp(401, { ok: false, error: "Your access has been turned off. Please log in again." });
      }
      if (!hasRole(authUser.role, authzFor(event.httpMethod, reqAction))) {
        return resp(403, { ok: false, error: "You don't have permission to do that." });
      }
    }

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;
      const params = event.queryStringParameters || {};
      if (action === "jobs")               return await handleJobs();
      if (action === "jobById")            return await handleJobById(params);
      if (action === "r2Status")           return await handleR2Status(params);
      if (action === "integrityCheck")     return await handleIntegrityCheck();
      if (action === "jobCreateStatus")    return await handleJobCreateStatus();
      if (action === "googleStatus")       return await handleGoogleStatus();
      if (action === "googleContactsReconcile") return await handleGoogleContactsReconcile(params);
      if (action === "contactDuplicates")  return await handleContactDuplicates();
      if (action === "people")             return await handlePeople();
      if (action === "employeePin")        return await handleEmployeePin(params);
      if (action === "employeeRates")      return await handleEmployeeRates(params);
      if (action === "jobPhotos")          return await handleJobPhotos(params);
      if (action === "jobPhotosDeleted")   return await handleJobPhotosDeleted(params);
      if (action === "jobPrints")          return await handleJobPrints(params);
      if (action === "jobPrintsDeleted")   return await handleJobPrintsDeleted(params);
      if (action === "panelSchedules")     return await handlePanelSchedules(params);
      if (action === "panelSchedule")      return await handlePanelSchedule(params);
      if (action === "jobChecklists")      return await handleJobChecklists(params);
      if (action === "jobChecklist")       return await handleJobChecklist(params);
      if (action === "jobDocs")            return await handleJobDocs(params);
      if (action === "expenseReceipts")    return await handleExpenseReceipts(params, authUser);
      if (action === "expenseReceiptSummary") return await handleExpenseReceiptSummary(params, authUser);
      if (action === "deletedExpenseReceipts") return await handleDeletedExpenseReceipts(params, authUser);
      if (action === "generator")          return await handleGenerator(params);
      if (action === "getWarrantyTemplates") return await handleGetWarrantyTemplates(params);
      if (action === "getWarranties")      return await handleGetWarranties(params);
      if (action === "expenses")           return await handleExpenses(params, authUser);
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
      if (action === "clockStatus")                 return await handleClockStatus(params, authUser);
      if (action === "clockRoster")                 return await handleClockRoster(params);
      if (action === "clockReconcile")              return await handleClockReconcile(params);
      if (action === "clockPunches")                return await handleClockPunches(params);
      if (action === "ptoBalance")                  return await handlePtoBalance(params, authUser);
      if (action === "ptoRequests")                 return await handlePtoRequests(params);
      if (action === "myHoursRollup")               return await handleMyHoursRollup(params);
      if (action === "myHoursBreakdown")            return await handleMyHoursBreakdown(params);
      if (action === "hoursByJob")                  return await handleHoursByJob();
      if (action === "scissorLifts")       return await handleScissorLifts();
      if (action === "scissorLiftsByJob")  return await handleScissorLiftsByJob(params);
      if (action === "jobInspections")     return await handleJobInspections(params);
      if (action === "jobEstimates")       return await handleJobEstimates(params);
      if (action === "estimateTemplates")  return await handleEstimateTemplates(params);
      if (action === "estimateTemplatesAll") return await handleEstimateTemplatesAll();
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
      if (body.action === "clockIn")              return await handleClockIn(body, authUser);
      if (body.action === "clockOut")             return await handleClockOut(body, authUser);
      if (body.action === "clockBreak")           return await handleClockBreak(body, authUser);
      if (body.action === "clockSwitch")          return await handleClockSwitch(body, authUser);
      if (body.action === "clockEditTimes")       return await handleClockEditTimes(body, authUser);
      if (body.action === "clockDeletePunch")     return await handleClockDeletePunch(body, authUser);
      if (body.action === "widgetLink")           return await handleWidgetLink(body, authUser);
      if (body.action === "requestPto")           return await handleRequestPto(body, authUser);
      if (body.action === "cancelPtoRequest")     return await handleCancelPtoRequest(body, authUser);
      if (body.action === "decidePtoRequest")     return await handleDecidePtoRequest(body, authUser);
      if (body.action === "setPtoAllowance")      return await handleSetPtoAllowance(body);
      if (body.action === "adminAddPto")          return await handleAdminAddPto(body, authUser);
      if (body.action === "fillHolidays")         return await handleFillHolidays(body);
      if (body.action === "ptoRollover")          return await handlePtoRollover(body);
      if (body.action === "adminClockIn")         return await handleAdminClockIn(body, authUser);
      if (body.action === "adminClockOut")        return await handleAdminClockOut(body, authUser);
      if (body.action === "promoteClockPunches")  return await handlePromoteClockPunches(body);
      if (body.action === "deleteTimeEntry")      return await handleDeleteTimeEntry(body);
      if (body.action === "copyLiftPhotosToR2")   return await handleCopyLiftPhotosToR2();
      if (body.action === "copyFleetPhotosToR2")  return await handleCopyFleetPhotosToR2();
      if (body.action === "copyEstimatePdfsToR2") return await handleCopyEstimatePdfsToR2();
      if (body.action === "copyPayrollFilesToR2") return await handleCopyPayrollFilesToR2();
      if (body.action === "backfillContacts")     return await handleBackfillContacts();
      if (body.action === "contactMerge")         return await handleContactMerge(body, authUser);
      if (body.action === "generatorServiceCheck") return await handleGeneratorServiceCheck(body);
      if (body.action === "payrollRunCreate")     return await handlePayrollRunCreate(body);
      if (body.action === "deleteExpense")        return await handleDeleteExpense(body, authUser);
      if (body.action === "updateExpense")        return await handleUpdateExpense(body, authUser);
      if (body.action === "approveExpense")       return await handleApproveExpense(body);
      if (body.action === "updateScissorLift")    return await handleUpdateScissorLift(body);
      if (body.action === "createScissorLift")    return await handleCreateScissorLift(body);
      if (body.action === "deleteScissorLift")    return await handleDeleteScissorLift(body);
      if (body.action === "liftPhotoUploadUrl")   return await handleLiftPhotoUploadUrl(body);
      if (body.action === "fleetPhotoUploadUrl")  return await handleFleetPhotoUploadUrl(body);
      if (body.action === "equipThumbUploadUrl")  return await handleEquipThumbUploadUrl(body);
      if (body.action === "deleteLiftPhoto")      return await handleDeleteLiftPhoto(body);
      if (body.action === "createInspection")     return await handleCreateInspection(body);
      if (body.action === "updateEstimate")       return await handleUpdateEstimate(body);
      if (body.action === "updateEstimateStatus") return await handleUpdateEstimateStatus(body);
      if (body.action === "getNextEstimateNumber") return await handleGetNextEstimateNumber();
      if (body.action === "saveEstimate")         return await handleSaveEstimate(body);
      if (body.action === "createJobEstimate")    return await handleCreateJobEstimate(body);
      if (body.action === "estimateTemplateSave")    return await handleEstimateTemplateSave(body, authUser);
      if (body.action === "estimateTemplateArchive") return await handleEstimateTemplateArchive(body, authUser);
      if (body.action === "estimateTemplateDelete")  return await handleEstimateTemplateDelete(body);
      if (body.action === "deleteJobEstimate")       return await handleDeleteJobEstimate(body);
      if (body.action === "updateFleetVehicle")   return await handleUpdateFleetVehicle(body);
      if (body.action === "logMileage")           return await handleLogMileage(body);
      if (body.action === "updateJobBillableRate") return await handleUpdateJobBillableRate(body);
      if (body.action === "updateJobCityTax")     return await handleUpdateJobCityTax(body);
      if (body.action === "updateJobClockVisibility") return await handleUpdateJobClockVisibility(body);
      if (body.action === "addFleetService")      return await handleAddFleetService(body);
      if (body.action === "updateFleetService")   return await handleUpdateFleetService(body);
      if (body.action === "deleteFleetService")   return await handleDeleteFleetService(body);
      if (body.action === "startServiceCall")     return await handleStartServiceCall(body);
      if (body.action === "completeServiceCall")  return await handleCompleteServiceCall(body);
      if (body.action === "saveInvoice")          return await handleSaveInvoice(body);
      if (body.action === "markInvoicePaid")      return await handleMarkInvoicePaid(body);
      if (body.action === "setInvoiceStatus")     return await handleSetInvoiceStatus(body);
      if (body.action === "addGeneratorService")  return await handleAddGeneratorService(body);
      if (body.action === "updateGenerator")      return await handleUpdateGenerator(body);
      if (body.action === "addWarranty")          return await handleAddWarranty(body);
      if (body.action === "commissionGenerator")  return await handleCommissionGenerator(body);
      if (body.action === "addScheduleEntry")     return await handleAddScheduleEntry(body);
      if (body.action === "updateScheduleEntry")  return await handleUpdateScheduleEntry(body);
      if (body.action === "deleteScheduleEntry")  return await handleDeleteScheduleEntry(body);
      if (body.action === "getNextInvoiceNumber") return await handleGetNextInvoiceNumber();
      if (body.action === "jobPhotoUploadUrls")   return await handleJobPhotoUploadUrls(body);
      if (body.action === "expenseReceiptUploadUrls") return await handleExpenseReceiptUploadUrls(body, authUser);
      if (body.action === "deleteExpenseReceipts")  return await handleDeleteExpenseReceipts(body, authUser);
      if (body.action === "restoreExpenseReceipts") return await handleRestoreExpenseReceipts(body, authUser);
      if (body.action === "moveJobPhotos")        return await handleMoveJobPhotos(body);
      if (body.action === "deleteJobPhotos")      return await handleDeleteJobPhotos(body);
      if (body.action === "restoreJobPhotos")     return await handleRestoreJobPhotos(body);
      if (body.action === "purgeJobPhotos")       return await handlePurgeJobPhotos(body);
      if (body.action === "jobPrintUploadUrls")   return await handleJobPrintUploadUrls(body);
      if (body.action === "createPanelSchedule")  return await handleCreatePanelSchedule(body, authUser);
      if (body.action === "savePanelSchedule")    return await handleSavePanelSchedule(body, authUser);
      if (body.action === "deletePanelSchedule")  return await handleDeletePanelSchedule(body);
      if (body.action === "createChecklist")      return await handleCreateChecklist(body, authUser);
      if (body.action === "addChecklistItem")     return await handleAddChecklistItem(body, authUser);
      if (body.action === "setChecklistItemDone") return await handleSetChecklistItemDone(body, authUser);
      if (body.action === "deleteChecklistItem")  return await handleDeleteChecklistItem(body);
      if (body.action === "reorderChecklistItems") return await handleReorderChecklistItems(body);
      if (body.action === "deleteChecklist")      return await handleDeleteChecklist(body);
      if (body.action === "deleteJobPrints")      return await handleDeleteJobPrints(body);
      if (body.action === "restoreJobPrints")     return await handleRestoreJobPrints(body);
      if (body.action === "purgeJobPrints")       return await handlePurgeJobPrints(body);
      if (body.action === "getJobInvoices")       return await handleGetJobInvoices(body);
      if (body.action === "updateJobNotes")       return await handleUpdateJobNotes(body);
      // authUser is passed so the handler can refuse a self-lockout — the
      // signed identity, never a client-supplied one.
      if (body.action === "setEmployeeActive")    return await handleSetEmployeeActive(body, authUser);
      if (body.action === "setEmployeeSalaried")  return await handleSetEmployeeSalaried(body);
      if (body.action === "setEmployeePin")       return await handleSetEmployeePin(body);
      if (body.action === "updateEmployee")       return await handleUpdateEmployee(body, authUser);
      if (body.action === "createEmployee")       return await handleCreateEmployee(body);
      if (body.action === "addEmployeeRaise")     return await handleAddEmployeeRaise(body);
      if (body.action === "correctEmployeeRate")  return await handleCorrectEmployeeRate(body);
      if (body.action === "updateJobInspection")  return await handleUpdateJobInspection(body);
      if (body.action === "createInspectionAgency") return await handleCreateInspectionAgency(body);
      if (body.action === "createInspectionContact") return await handleCreateInspectionContact(body);
      if (body.action === "updateJobInfo")        return await handleUpdateJobInfo(body);
      if (body.action === "createJob")            return await handleCreateJob(body);
      if (body.action === "createContact")        return await handleCreateContact(body);
      if (body.action === "createCompany")        return await handleCreateCompany(body);
      if (body.action === "updateInspection")     return await handleUpdateInspection(body);
      if (body.action === "calculateMileage")     return await handleCalculateMileage(body);
      if (body.action === "addLiftExpense")       return await handleAddLiftExpense(body, authUser);
      if (body.action === "addGeneralExpense")    return await handleAddGeneralExpense(body, authUser);
      if (body.action === "createVendor")         return await handleCreateVendor(body);
      return resp(400, { ok: false, error: "Unknown POST action." });
    }

    return resp(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("NEE Proxy Error:", err);
    return resp(500, { ok: false, error: err.message || "Server error." });
  }
}

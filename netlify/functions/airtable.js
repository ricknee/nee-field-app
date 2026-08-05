// netlify/functions/airtable.js
// Northeastern Electric Field App — Netlify Proxy
// Reads env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AUTH_SECRET
import { signToken, authedUser, hasRole } from "./_auth.js";
// Shadow-read helpers for the Neon migration. Fail-soft by contract — see _neon.js.
import { neonEnabled, neonQuery, neonExec, neonWrite, shadowCompare } from "./_neon.js";
// Jobsite photos. Optional infrastructure like _neon.js — see docs/PLAN-job-photos.md.
// Photo storage. netlify/functions/_pcloud.js is deliberately NOT imported —
// pCloud lost the store decision when its app-registration page turned out to
// have been down for months, so no API token could be issued. That file and
// tools/pcloud-*.mjs are kept on disk in case it ever reopens (a mirror-to-
// pCloud option), but nothing calls them. See docs/PLAN-job-photos.md.
import {
  r2Enabled, r2Status, r2SelfTest, listJobPhotos, presignPut,
  thumbKeyFor, jobPrefix, albumSegment, sanitizeAlbum,
  moveJobPhoto, softDeleteJobPhoto, restoreJobPhoto, purgeJobPhoto,
  listDeletedJobPhotos, listJobDocs,
  jobPrintsPrefix, sanitizePrintName, listJobPrints, listDeletedJobPrints,
  softDeleteJobPrint, restoreJobPrint, purgeJobPrint,
  expensePrefix, listExpenseReceipts, receiptFileKind, summarizeExpenseReceipts,
  softDeleteExpenseReceipt, restoreExpenseReceipt, listDeletedExpenseReceipts, R2Error,
  listByPrefix,
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
 *                  updateTimeEntryPayroll 520, deleteTimeEntry 2144,
 *                  backfillTimeEntryEmployeeLinks 550 (ADMIN_BACKFILL_TOKEN-gated)
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
]);
const _READ_LIKE_POSTS = new Set([
  "getNextEstimateNumber", "getNextInvoiceNumber", "getJobInvoices", "calculateMileage",
]);
const _TIME_SELF_WRITES = new Set([
  "createTimeEntry", "updateTimeEntry", "deleteTimeEntry",
]);
const _ADMIN_POSTS = new Set([
  "updateTimeEntryPayroll", "payrollRunCreate", "backfillTimeEntryEmployeeLinks",
  "addScheduleEntry", "updateScheduleEntry", "deleteScheduleEntry",
  // One-off migration action, admin only. Gated by role rather than by
  // ADMIN_BACKFILL_TOKEN: it is idempotent, copies rather than mutates, and the
  // token is itself a write-only Netlify secret nobody has a copy of.
  "copyLiftPhotosToR2",
]);
const _ADMIN_OFFICE_POSTS = new Set([
  // NOTE: deleteExpense is intentionally NOT here — it now defaults to
  // _NON_VIEWER and handleDeleteExpense enforces owner+unreviewed for
  // employees (admin/office may delete any). updateExpense likewise defaults
  // to _NON_VIEWER with in-handler owner/status enforcement.
  "approveExpense", "markInvoicePaid", "setInvoiceStatus",
  "updateJobBillableRate", "createVendor",
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
const _ADMIN_READS = new Set(["r2Status"]);

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
const _ADMIN_OFFICE_READS = new Set(["jobPhotosDeleted", "jobDocs", "jobPrintsDeleted"]);

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
      return resp(200, { ok: true, entries, _source: "neon", _ms: q.ms, ...(unlinked ? { unlinked } : {}) });
    }
    console.error(`payrollEntries: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
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
  const jobRec = (jobId      && String(jobId).startsWith("rec"))      ? String(jobId)      : null;
  const empRec = (employeeId && String(employeeId).startsWith("rec")) ? String(employeeId) : null;

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
             (SELECT id FROM employees WHERE airtable_id = $2),
             $3::date, $4::numeric, $5, $6,
             (SELECT id        FROM jobs WHERE airtable_id = $7),
             (SELECT po_locked FROM jobs WHERE airtable_id = $7),
             false, 'Manual')
     RETURNING id`,
    [employee, empRec, workDate, durationSecs, taxes, klass, jobRec]);
  const neonId = rows?.[0]?.id;

  const fields = {};
  fields[TE.employee]   = employee;
  if (empRec) fields[TE.employeeLink] = [empRec];
  fields[TE.workDate]   = workDate;
  fields[TE.duration]   = durationSecs;
  fields[TE.class]      = klass;
  fields[TE.cityTaxes]  = taxes;
  if (jobRec) fields[TE.jobLink] = [jobRec];

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
    const rec = (jobId && String(jobId).startsWith("rec")) ? String(jobId) : null;
    vals.push(rec);
    sets.push(`job_id = (SELECT id FROM jobs WHERE airtable_id = $${vals.length})`);
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
  return resp(200, { ok: true, updatedId: target.id });
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
    console.error(`payrollHoursRollup: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
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
    console.error(`hoursByJob: Neon read failed, falling back to Airtable: ${q?.error || meta?.error || "no rows"}`);
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
        WHERE e.airtable_id = $1
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
    console.error(`myHoursRollup: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
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
  const user = { id: match.id, name: f[F.emp.name]||"Unknown", role };
  // Issue a signed session token the client attaches to every later request.
  return resp(200, { ok: true, user, token: signToken({ id: user.id, role: user.role }) });
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
      projectedGrossProfitPct
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
  SELECT j.airtable_id, j.name, j.po, j.status, j.job_type, j.job_year,
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
         j.customer_phone, j.customer_email, j.start_service_call,
         j.service_call_created, j.project_complete, j.miles_from_shop, j.notes,
         j.bird_date::text AS bird_date, j.workflow_status, j.billable_hourly_rate,
         j.labor_billable_rate_at_id,
         r.base_contract_amount, r.total_contract_billed, r.total_wire_cost,
         r.reviewed_wire_cost_rollup, r.pipe_cost, r.pipe_cost_reviewed,
         r.expected_revenue, r.hours_rollup,
         r.est_labor_hours_rollup, r.est_labor_cost_rollup, r.est_material_cost_rollup,
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
  const projectedEstimatedTotalCost = projectedEstimatedMaterialCost + projectedEstimatedLaborCost;
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
    projectedGrossProfitDollar, projectedGrossProfitPct
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
    console.error(`jobs: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
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
    const q = await neonQuery(`${JOB_SELECT} WHERE j.airtable_id = $1`, [jobId]);
    if (q?.rows?.length) {
      return resp(200, { ok: true, job: mapJobFromNeon(q.rows[0]), _source: "neon", _ms: q.ms });
    }
    // No rows here is ambiguous — a genuinely unknown job, or a job created in
    // Airtable within the last hour that the sync has not carried over yet. Fall
    // through to Airtable, which answers both correctly.
    console.error(`jobById: Neon miss for ${jobId}, falling back to Airtable: ${q?.error || "no rows"}`);
  }
  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${jobId}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });
  return resp(200, { ok: true, job: mapJob(records[0]), _source: "airtable" });
}

async function handleGenerator(params) {
  const jobId = params?.jobId;
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });
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

// Shared guard for employee self-service on an existing expense. Managers
// (admin/office) may mutate any expense; an employee may mutate ONLY their own
// AND only while it is still "Not Reviewed" (approval locks it). Returns
// { ok:true, record } or { ok:false, resp } with the right 400/403.
async function guardExpenseMutation(expenseId, authUser) {
  if (!expenseId) return { ok: false, resp: resp(400, { ok: false, error: "Missing expenseId." }) };
  const rec = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`);
  const f = rec.fields || {};
  const isMgr = authUser && (authUser.role === "admin" || authUser.role === "office");
  if (isMgr) return { ok: true, record: rec };
  const owns = Array.isArray(f["Submitted By"]) && f["Submitted By"].includes(authUser?.id);
  const status = f["Expense Status"]?.name || f["Expense Status"] || "";
  const reviewed = f["Reviewed"] === true || status === "Reviewed";
  if (!owns)    return { ok: false, resp: resp(403, { ok: false, error: "You can only change your own expenses." }) };
  if (reviewed) return { ok: false, resp: resp(403, { ok: false, error: "This expense has been approved and can no longer be changed." }) };
  return { ok: true, record: rec };
}

async function handleDeleteExpense(body, authUser) {
  const { expenseId } = body || {};
  const guard = await guardExpenseMutation(expenseId, authUser);
  if (!guard.ok) return guard.resp;
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
  // Cross-name filter safety — see the note in handleGenerator. Newline-delimited
  // so FIND is an exact match per linked contractor rather than a substring:
  // without it, "Case Farms" also matches "Case Farms North" and that
  // contractor's templates appear under the wrong one.
  //
  // No in-memory id verification here, unlike the job sites: the caller passes a
  // contractor NAME, not a record id, so there is nothing to verify against.
  // Exact-per-element matching is the whole fix available.
  const safeContractor = escapeFormulaString((contractor || "").trim());
  const filter = safeContractor
    ? `AND({Active}=TRUE(), FIND("\n${safeContractor}\n", "\n" & ARRAYJOIN({Contractor}, "\n") & "\n"))`
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
  if (!r2Enabled()) return resp(503, { ok: false, error: "R2 is not configured." });
  if (!neonEnabled()) return resp(503, { ok: false, error: "Neon is not configured." });

  const records = await fetchAll(TABLES.scissorLifts);
  const rows = await neonWrite("lifts.listForCopy",
    `SELECT id, airtable_id FROM scissor_lifts WHERE airtable_id IS NOT NULL`);
  const idByAirtable = new Map(rows.map(r => [r.airtable_id, r.id]));

  // One list up front so a re-run resumes rather than re-uploading.
  const existing = new Set((await listByPrefix("lifts/")).map(o => o.key));

  const report = { copied: 0, skipped: 0, failed: 0, unmatched: 0, details: [] };
  for (const rec of records) {
    const liftId = idByAirtable.get(rec.id);
    if (!liftId) {
      // A lift in Airtable that the ETL has not loaded yet. Reported, not
      // guessed at — copying it under a made-up id would strand the file.
      report.unmatched++;
      report.details.push(`unmatched: ${rec.fields?.["Lift Name"] || rec.id}`);
      continue;
    }
    for (const att of (rec.fields?.["Photo"] || [])) {
      // Keyed on the ATTACHMENT id, not the filename: two lifts can both have
      // "photo.jpg", and a rename in Airtable must not orphan the copy.
      const ext = (att.filename?.match(/\.[a-z0-9]+$/i) || [".jpg"])[0].toLowerCase();
      const key = `lifts/${liftId}/${att.id}${ext}`;
      if (existing.has(key)) { report.skipped++; continue; }
      try {
        // Download and upload back to back, while the signed URL is still valid.
        const img = await fetch(att.url);
        if (!img.ok) throw new Error(`download ${img.status}`);
        const buf = Buffer.from(await img.arrayBuffer());
        const put = await fetch(presignPut(key, att.type || "image/jpeg"), {
          method: "PUT", body: buf,
          headers: { "content-type": att.type || "image/jpeg" },
        });
        if (!put.ok) throw new Error(`upload ${put.status}`);
        report.copied++;
        report.details.push(`copied: ${rec.fields["Lift Name"]} → ${key} (${buf.length}b)`);
      } catch (e) {
        report.failed++;
        report.details.push(`FAILED: ${rec.fields?.["Lift Name"]} ${key}: ${e.message}`);
      }
    }
  }

  const after = (await listByPrefix("lifts/")).length;
  const expected = records.reduce((n, r) => n + (r.fields?.["Photo"]?.length || 0), 0);
  return resp(200, {
    ok: report.failed === 0 && report.unmatched === 0 && after === expected,
    ...report,
    objectsInR2: after,
    attachmentsInAirtable: expected,
    // The caller should not flip anything until these two agree.
    reconciled: after === expected,
  });
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
  return resp(200, { ok: true, updatedId: target.id });
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
        WHERE j.airtable_id = $1
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
    console.error(`timeEntries: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
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

async function handleExpenses(params, authUser) {
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
  await atFetch(`${encodeURIComponent(TABLES.jobs)}/${jobId}`, { method: "PATCH", body: JSON.stringify({ fields: { "fldMy1yR7aHtVko9F": miles } }) });
  return resp(200, { ok: true, miles });
}

async function handleAddLiftExpense(body, authUser) {
  const { jobId, date, amount, description, billable } = body || {};
  if (!jobId || !amount) return resp(400, { ok: false, error: "Missing jobId or amount." });
  const idStr = String(jobId).trim();
  if (!idStr.startsWith("rec")) return resp(400, { ok: false, error: `Invalid jobId received: ${idStr}` });
  const fields = { "fldPNFIzq1grsdxYi":[idStr],"fldlTUL8hsPkReBAB":["recU56ncurkFrM2Nx"],"fldwbLPIafVtmaSeb":Number(amount),"fldX2x2J0xkRyMY3y":"Scissor Lift","fldelsB2jH2tvt1Cj":description||"Scissor Lift Expense","fldJTg0ekrdZ4Jqr6":"Not Reviewed","fld9Afieu4ofjvhSb":billable===true||billable==="true" };
  // Submitted By (Employee link) — stamped from the token, never client input.
  if (authUser?.id) fields["fldRWV0eIKwBrXwHV"] = [authUser.id];
  if (date) fields["fldCCPYdyWAOGchWb"] = date;
  const data = await atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
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
  if (!idStr.startsWith("rec")) return resp(400, { ok: false, error: `Invalid jobId: ${idStr}` });
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
  if (authUser?.id) fields["fldRWV0eIKwBrXwHV"] = [authUser.id];
  const data = await atFetch(`${encodeURIComponent("Expenses")}`, { method: "POST", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, id: data.id });
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
    "fldwbLPIafVtmaSeb": hasAmount ? Number(amount) : null,  // Total Cost (Actual)
    "fldcld418pREq2bGq": hasCredit ? Number(credit) : null   // Material Credit
  };
  if (date !== undefined)        fields["fldCCPYdyWAOGchWb"] = date || null;
  if (description !== undefined) fields["fldelsB2jH2tvt1Cj"] = description || "";
  if (vendorId !== undefined)    fields["fldlTUL8hsPkReBAB"] = (vendorId && String(vendorId).startsWith("rec")) ? [String(vendorId)] : [];

  const data = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`, { method: "PATCH", body: JSON.stringify({ fields, typecast: true }) });
  return resp(200, { ok: true, updatedId: data.id });
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
        // Cross-job filter safety — see handleGenerator. Exact-per-element match,
        // then verify the linked record id so a same-named job can't resolve to
        // the wrong generator and attach a service record to it.
        const safeName = escapeFormulaString(jobName);
        const linkedFilter = `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({${F.gen.job}}, "\n") & "\n")`;
        const linkedAll = await fetchAll(TABLES.generators, { filter: linkedFilter });
        const linked = linkedAll.filter(r =>
          Array.isArray(r.fields?.[F.gen.job]) && r.fields[F.gen.job].includes(jobId));
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
      // Exact-per-element: a substring hit here makes the DUPLICATE CHECK fire on
      // another generator, silently skipping a service record that should exist.
      const dupFilter = `AND(FIND("
${safe}
", "
" & ARRAYJOIN({${F.svc.generator}}, "
") & "
"), {${F.svc.serviceType}}="${svcType}")`;
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
      // Exact-per-element — this count decides whether warranties get created.
      const existingFilter = `FIND("
${safe}
", "
" & ARRAYJOIN({${F.warranty.generator}}, "
") & "
")`;
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
            COALESCE(array_agg(e.airtable_id ORDER BY e.name)
                     FILTER (WHERE e.airtable_id IS NOT NULL), '{}') AS crew_ids,
            COALESCE(array_agg(e.name ORDER BY e.name)
                     FILTER (WHERE e.name IS NOT NULL), '{}') AS crew_names
       FROM schedule_entries s
       LEFT JOIN jobs j ON j.id = s.job_id
       LEFT JOIN schedule_entry_crew c ON c.schedule_entry_id = s.id
       LEFT JOIN employees e ON e.id = c.employee_id
      WHERE ($1 = '' OR j.airtable_id = $1)
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
    console.error("scheduleEntries: Neon read failed, falling back to Airtable");
  }

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
  const ids = (Array.isArray(crewAtIds) ? crewAtIds : []).filter(x => typeof x === "string" && x.startsWith("rec"));
  if (!ids.length) return;
  await neonWrite("schedule.crew.set",
    `INSERT INTO schedule_entry_crew (schedule_entry_id, employee_id)
     SELECT $1, e.id FROM employees e WHERE e.airtable_id = ANY($2::text[])
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
     VALUES ($1, $2, (SELECT id FROM jobs WHERE airtable_id = $3), $4::date, $5::date, $6, 'app')
     RETURNING id`,
    [title ? String(title) : null, entryType, jobId || null,
     startDate, endDate, notes ? String(notes) : null]);
  const neonId = rows?.[0]?.id;
  await setScheduleCrew(neonId, crewIds);

  const fields = {};
  fields[SCHED_F.type] = entryType;
  if (jobId)              fields[SCHED_F.job]       = [jobId];
  fields[SCHED_F.startDate] = startDate;
  fields[SCHED_F.endDate]   = endDate;
  if (Array.isArray(crewIds) && crewIds.length) fields[SCHED_F.crew] = crewIds;
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
    vals.push(jobId || null);
    sets.push(`job_id = (SELECT id FROM jobs WHERE airtable_id = $${vals.length})`);
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
  if (crewIds     !== undefined) fields[SCHED_F.crew]      = Array.isArray(crewIds) ? crewIds : [];
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
  // `id` stays the AIRTABLE employee id: it is what the crew picker sends back in
  // `crewIds`, what schedule_entry_crew resolves against, and what the payroll
  // screens use. Employees are still an Airtable-owned dimension.
  if (neonEnabled()) {
    const q = await neonQuery(
      `SELECT airtable_id, name, lower(coalesce(role, 'employee')) AS role
         FROM employees
        WHERE active IS TRUE AND airtable_id IS NOT NULL
        ORDER BY name`);
    if (q?.rows?.length) {
      return resp(200, {
        ok: true,
        employees: q.rows.map(r => ({ id: r.airtable_id, name: r.name || "", role: r.role || "employee" })),
        _source: "neon", _ms: q.ms
      });
    }
    // Zero active employees is never a legitimate answer — it would empty the crew
    // picker — so treat it as a failure and let Airtable answer.
    console.error(`schedulingCrew: Neon read failed, falling back to Airtable: ${q?.error || "no rows"}`);
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
  const { jobId, customerStreet, customerCity, customerState, customerZip, customerPhone, customerEmail, notes, birdDate } = body || {};
  if (!jobId) return resp(400, { ok: false, error: "Missing jobId." });

  const fields = {};
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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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

  let rec;
  try { rec = await atFetch(`${encodeURIComponent("Expenses")}/${expenseId}`); }
  catch { return resp(404, { ok: false, error: "Expense not found." }); }

  const role = (authUser?.role || "").toLowerCase();
  if (role !== "admin" && role !== "office") {
    const submitted = rec.fields?.["Submitted By"];
    const owner = Array.isArray(submitted) ? submitted[0] : submitted;
    if (owner !== authUser?.id) {
      return resp(403, { ok: false, error: "You can only see receipts on your own expenses." });
    }
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

  const jobRecords = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!jobRecords.length) return resp(200, { ok: true, available: true, receipts: {} });

  const safeName = escapeFormulaString(jobRecords[0].fields["Job Name"] || "");
  const filter = `FIND("\n${safeName}\n", "\n" & ARRAYJOIN({Job}, "\n") & "\n")`;
  const all = await fetchAll("Expenses", { filter });
  const onJob = all.filter(r => Array.isArray(r.fields?.Job) && r.fields.Job.includes(jobId));

  const isMgr = authUser && (authUser.role === "admin" || authUser.role === "office");
  const visible = isMgr
    ? onJob
    : onJob.filter(r => Array.isArray(r.fields?.["Submitted By"]) && r.fields["Submitted By"].includes(authUser?.id));

  try {
    return resp(200, {
      ok: true, available: true,
      receipts: await summarizeExpenseReceipts(visible.map(r => r.id)),
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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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
      WHERE p.job_airtable_id = $1
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

  // The job must exist in AIRTABLE — that is still the jobs master. Deliberately
  // not a Neon lookup: a job created in the last hour is not in Neon yet, and
  // refusing to schedule its panels is exactly the failure this design avoids.
  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

  const rows = await neonWrite("panels.create",
    `INSERT INTO panel_schedules
       (job_airtable_id, job_id, name, voltage, circuits, location, fed_from, updated_by)
     VALUES ($1, (SELECT id FROM jobs WHERE airtable_id = $1), $2, $3, $4, $5, $6, $7)
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
      WHERE c.job_airtable_id = $1
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

  // Airtable is still the jobs master. Deliberately not a Neon lookup — see the
  // keying note in db/schema/008_job_checklists.sql.
  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

  const rows = await neonWrite("checklists.create",
    `INSERT INTO job_checklists (job_airtable_id, job_id, name, created_by)
     VALUES ($1, (SELECT id FROM jobs WHERE airtable_id = $1), $2, $3)
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

  const records = await fetchAll(TABLES.jobs, { filter: `RECORD_ID()="${escapeFormulaString(jobId)}"` });
  if (!records.length) return resp(404, { ok: false, error: "Job not found." });

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
    let authUser = null;
    if (reqAction !== "login") {
      authUser = authedUser(event);
      if (!authUser) return resp(401, { ok: false, error: "Not signed in. Please log in again." });
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
      if (action === "myHoursRollup")               return await handleMyHoursRollup(params);
      if (action === "myHoursBreakdown")            return await handleMyHoursBreakdown(params);
      if (action === "hoursByJob")                  return await handleHoursByJob();
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
      if (body.action === "copyLiftPhotosToR2")   return await handleCopyLiftPhotosToR2();
      if (body.action === "payrollRunCreate")     return await handlePayrollRunCreate(body);
      if (body.action === "deleteExpense")        return await handleDeleteExpense(body, authUser);
      if (body.action === "updateExpense")        return await handleUpdateExpense(body, authUser);
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
      if (body.action === "updateJobInspection")  return await handleUpdateJobInspection(body);
      if (body.action === "createInspectionAgency") return await handleCreateInspectionAgency(body);
      if (body.action === "createInspectionContact") return await handleCreateInspectionContact(body);
      if (body.action === "updateJobInfo")        return await handleUpdateJobInfo(body);
      if (body.action === "createJob")            return await handleCreateJob(body);
      if (body.action === "createContact")        return await handleCreateContact(body);
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

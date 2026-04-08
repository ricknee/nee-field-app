const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;

const AIRTABLE_API_ROOT = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}`;

const TABLES = {
  employees: "Employees",
  jobs: "Jobs",
  generators: "Generators"
};

const FIELD_MAP = {
  employees: {
    name: ["Name", "Employee Name", "Full Name"],
    username: ["Username", "User Name", "Login Username", "Email"],
    email: ["Email", "Work Email"],
    pin: ["PIN", "Pin", "Passcode", "Password", "Login PIN"],
    role: ["Role", "User Role", "Access Level"],
    active: ["Active", "Is Active", "Enabled"]
  },

  jobs: {
    name: ["Job Name", "Name"],
    po: ["Job PO", "PO", "Project PO"],
    status: ["Status"],
    company: ["Company", "Contractor", "Customer"],
    contact: ["Primary Contact", "Contact", "Customer Contact"],
    address: ["Full Address", "Address", "Job Address", "Location Address"],
    powerCompany: ["Power Company", "Utility Company"],
    aicNumber: ["AIC #", "AIC Number"],
    tempWorkOrder: ["Temporary Work Order", "Temp Work Order"],
    permWorkOrder: ["Permanent Work Order", "Perm Work Order"],
    generatorInstalled: ["Generator Installed", "Has Generator", "Installed Generator"],
    generatorCommissioned: ["Generator Commissioned", "Commissioned"],
    generatorCommissionDate: ["Commission Date", "Generator Commission Date"],
    nextServiceDate: ["Next Service Date"],
    generatorMake: ["Generator Make", "Make"],
    generatorModel: ["Generator Model", "Model"],
    generatorSerial: ["Generator Serial #", "Generator Serial", "Serial Number"],
    atsSerial: ["ATS Serial #", "ATS Serial"],
    serviceIntervalMonths: ["Service Interval (Months)", "Service Interval Months"],
    notes: ["Notes", "Job Notes", "Commission Notes"]
  },

  generators: {
    name: ["Generator ID", "Name"],
    linkedJob: ["Job", "Linked Job", "Original Job"],
    generatorMake: ["Generator Make", "Make"],
    generatorModel: ["Generator Model", "Model"],
    generatorSerial: ["Generator Serial #", "Generator Serial", "Serial Number"],
    atsSerial: ["ATS Serial #", "ATS Serial"],
    commissionDate: ["Commission Date"],
    nextServiceDate: ["Next Service Date"],
    serviceIntervalMonths: ["Service Interval (Months)", "Service Interval Months"],
    commissionedBy: ["Commissioned By"],
    notes: ["Notes", "Commission Notes"]
  }
};

function headers() {
  return {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS"
  };
}

function response(statusCode, body) {
  return {
    statusCode,
    headers: headers(),
    body: JSON.stringify(body)
  };
}

function ensureEnv() {
  if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID) {
    throw new Error("Missing Netlify environment variables AIRTABLE_API_KEY or AIRTABLE_BASE_ID.");
  }
}

function getField(fields, candidates = []) {
  for (const key of candidates) {
    if (Object.prototype.hasOwnProperty.call(fields, key)) {
      return fields[key];
    }
  }
  return null;
}

function asBool(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const v = value.trim().toLowerCase();
    return ["true", "yes", "1", "checked"].includes(v);
  }
  return false;
}

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

async function airtableFetch(path, options = {}) {
  ensureEnv();

  const res = await fetch(`${AIRTABLE_API_ROOT}/${path}`, {
    ...options,
    headers: {
      Authorization: `Bearer ${AIRTABLE_API_KEY}`,
      "Content-Type": "application/json",
      ...(options.headers || {})
    }
  });

  const text = await res.text();
  let json = {};
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const message =
      json?.error?.message ||
      json?.message ||
      `Airtable request failed with status ${res.status}`;
    throw new Error(message);
  }

  return json;
}

async function fetchAllRecords(tableName, options = {}) {
  const params = new URLSearchParams();

  if (options.view) params.set("view", options.view);
  if (options.filterByFormula) params.set("filterByFormula", options.filterByFormula);
  if (options.sortField) {
    params.set("sort[0][field]", options.sortField);
    params.set("sort[0][direction]", options.sortDirection || "asc");
  }

  const records = [];
  let offset = null;

  do {
    const qs = new URLSearchParams(params);
    if (offset) qs.set("offset", offset);

    const data = await airtableFetch(
      `${encodeURIComponent(tableName)}?${qs.toString()}`,
      { method: "GET" }
    );

    records.push(...(data.records || []));
    offset = data.offset;
  } while (offset);

  return records;
}

async function createRecord(tableName, fields) {
  return airtableFetch(encodeURIComponent(tableName), {
    method: "POST",
    body: JSON.stringify({ fields })
  });
}

async function updateRecord(tableName, recordId, fields) {
  return airtableFetch(`${encodeURIComponent(tableName)}/${recordId}`, {
    method: "PATCH",
    body: JSON.stringify({ fields })
  });
}

function mapUser(record) {
  const f = record.fields || {};
  const roleRaw = getField(f, FIELD_MAP.employees.role) || "employee";
  const role = normalize(roleRaw) === "admin" ? "admin" : "employee";

  return {
    id: record.id,
    name: getField(f, FIELD_MAP.employees.name) || "Unknown User",
    username: getField(f, FIELD_MAP.employees.username) || "",
    email: getField(f, FIELD_MAP.employees.email) || "",
    role,
    active: getField(f, FIELD_MAP.employees.active) == null
      ? true
      : asBool(getField(f, FIELD_MAP.employees.active))
  };
}

function mapJob(record) {
  const f = record.fields || {};
  return {
    id: record.id,
    name: getField(f, FIELD_MAP.jobs.name) || "",
    po: getField(f, FIELD_MAP.jobs.po) || "",
    status: getField(f, FIELD_MAP.jobs.status) || "",
    company: arrayToDisplay(getField(f, FIELD_MAP.jobs.company)),
    contact: arrayToDisplay(getField(f, FIELD_MAP.jobs.contact)),
    address: getField(f, FIELD_MAP.jobs.address) || "",
    powerCompany: arrayToDisplay(getField(f, FIELD_MAP.jobs.powerCompany)),
    aicNumber: getField(f, FIELD_MAP.jobs.aicNumber) || "",
    tempWorkOrder: getField(f, FIELD_MAP.jobs.tempWorkOrder) || "",
    permWorkOrder: getField(f, FIELD_MAP.jobs.permWorkOrder) || "",
    generatorInstalled: asBool(getField(f, FIELD_MAP.jobs.generatorInstalled)),
    generatorCommissioned: asBool(getField(f, FIELD_MAP.jobs.generatorCommissioned)),
    generatorCommissionDate: getField(f, FIELD_MAP.jobs.generatorCommissionDate) || "",
    nextServiceDate: getField(f, FIELD_MAP.jobs.nextServiceDate) || "",
    generatorMake: getField(f, FIELD_MAP.jobs.generatorMake) || "",
    generatorModel: getField(f, FIELD_MAP.jobs.generatorModel) || "",
    generatorSerial: getField(f, FIELD_MAP.jobs.generatorSerial) || "",
    atsSerial: getField(f, FIELD_MAP.jobs.atsSerial) || "",
    serviceIntervalMonths: getField(f, FIELD_MAP.jobs.serviceIntervalMonths) || "",
    notes: getField(f, FIELD_MAP.jobs.notes) || ""
  };
}

function arrayToDisplay(value) {
  if (Array.isArray(value)) return value.join(", ");
  return value || "";
}

function valueMatchesLogin(record, identifier, pin) {
  const f = record.fields || {};
  const userName = normalize(getField(f, FIELD_MAP.employees.username));
  const name = normalize(getField(f, FIELD_MAP.employees.name));
  const email = normalize(getField(f, FIELD_MAP.employees.email));
  const savedPin = String(getField(f, FIELD_MAP.employees.pin) || "").trim();

  const id = normalize(identifier);
  const providedPin = String(pin || "").trim();

  const userMatch = [userName, name, email].includes(id);
  const pinMatch = savedPin !== "" && savedPin === providedPin;

  const activeValue = getField(f, FIELD_MAP.employees.active);
  const isActive = activeValue == null ? true : asBool(activeValue);

  return userMatch && pinMatch && isActive;
}

async function handleLogin(body) {
  const { identifier, pin } = body || {};

  if (!identifier || !pin) {
    return response(400, { ok: false, error: "Missing identifier or PIN." });
  }

  const records = await fetchAllRecords(TABLES.employees);
  const match = records.find((record) => valueMatchesLogin(record, identifier, pin));

  if (!match) {
    return response(401, { ok: false, error: "Invalid login." });
  }

  const user = mapUser(match);
  return response(200, { ok: true, user });
}

async function handleJobs() {
  const records = await fetchAllRecords(TABLES.jobs);

  const jobs = records
    .map(mapJob)
    .filter(job => {
      const status = normalize(job.status);
      return !["archived", "cancelled", "canceled"].includes(status);
    })
    .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")));

  return response(200, { ok: true, jobs });
}

async function handleCommission(body) {
  const {
    jobId,
    commissionDate,
    nextServiceDate,
    generatorMake,
    generatorModel,
    generatorSerial,
    atsSerial,
    serviceIntervalMonths,
    commissionedBy,
    notes
  } = body || {};

  if (!jobId) {
    return response(400, { ok: false, error: "Missing jobId." });
  }

  const jobFields = {};
  setMappedField(jobFields, FIELD_MAP.jobs.generatorCommissioned, true);
  setMappedField(jobFields, FIELD_MAP.jobs.generatorCommissionDate, commissionDate || null);
  setMappedField(jobFields, FIELD_MAP.jobs.nextServiceDate, nextServiceDate || null);
  setMappedField(jobFields, FIELD_MAP.jobs.generatorMake, generatorMake || "");
  setMappedField(jobFields, FIELD_MAP.jobs.generatorModel, generatorModel || "");
  setMappedField(jobFields, FIELD_MAP.jobs.generatorSerial, generatorSerial || "");
  setMappedField(jobFields, FIELD_MAP.jobs.atsSerial, atsSerial || "");
  setMappedField(jobFields, FIELD_MAP.jobs.serviceIntervalMonths, serviceIntervalMonths || null);
  setMappedField(jobFields, FIELD_MAP.jobs.notes, notes || "");

  await updateRecord(TABLES.jobs, jobId, jobFields);

  const generatorFields = {};
  setMappedField(generatorFields, FIELD_MAP.generators.linkedJob, [jobId]);
  setMappedField(generatorFields, FIELD_MAP.generators.generatorMake, generatorMake || "");
  setMappedField(generatorFields, FIELD_MAP.generators.generatorModel, generatorModel || "");
  setMappedField(generatorFields, FIELD_MAP.generators.generatorSerial, generatorSerial || "");
  setMappedField(generatorFields, FIELD_MAP.generators.atsSerial, atsSerial || "");
  setMappedField(generatorFields, FIELD_MAP.generators.commissionDate, commissionDate || null);
  setMappedField(generatorFields, FIELD_MAP.generators.nextServiceDate, nextServiceDate || null);
  setMappedField(generatorFields, FIELD_MAP.generators.serviceIntervalMonths, serviceIntervalMonths || null);
  setMappedField(generatorFields, FIELD_MAP.generators.commissionedBy, commissionedBy || "");
  setMappedField(generatorFields, FIELD_MAP.generators.notes, notes || "");

  try {
    await createRecord(TABLES.generators, generatorFields);
  } catch (err) {
    return response(200, {
      ok: true,
      warning: "Job was updated, but generator asset record could not be created. Check your Generators table field names.",
      errorDetail: err.message
    });
  }

  return response(200, { ok: true, message: "Generator commissioning saved." });
}

function setMappedField(target, candidates, value) {
  if (!Array.isArray(candidates) || !candidates.length) return;
  target[candidates[0]] = value;
}

export async function handler(event) {
  try {
    if (event.httpMethod === "OPTIONS") {
      return response(200, { ok: true });
    }

    ensureEnv();

    if (event.httpMethod === "GET") {
      const action = event.queryStringParameters?.action;

      if (action === "jobs") {
        return await handleJobs();
      }

      return response(400, { ok: false, error: "Unsupported GET action." });
    }

    if (event.httpMethod === "POST") {
      const body = event.body ? JSON.parse(event.body) : {};
      const action = body.action;

      if (action === "login") {
        return await handleLogin(body);
      }

      if (action === "commission") {
        return await handleCommission(body);
      }

      return response(400, { ok: false, error: "Unsupported POST action." });
    }

    return response(405, { ok: false, error: "Method not allowed." });
  } catch (err) {
    console.error("Netlify Airtable proxy error:", err);
    return response(500, {
      ok: false,
      error: err.message || "Server error."
    });
  }
}

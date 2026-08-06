// Inspections, Generators, Warranties → Neon (roadmap Step 4c).
// Same shape as db/etl/fleet.mjs. Schema: db/schema/011_inspections_generators_warranties.sql
// Plan: docs/PLAN-4c-inspections-generators-warranties.md
//
// SEVEN tables, ~75 rows. Load order matters — every child carries a real FK:
//   inspection_agencies -> inspection_contacts -> job_inspections
//   generators -> generator_service
//   warranty_templates -> warranties
//
// ── NO PHOTO STEP, unlike fleet and lifts ──────────────────────────────────
// Owner's call 2026-08-06: `Inspection Contacts.Files / Images` is dead (0 of 5
// rows), and `Job Inspections.Attachments` (2 photos on 2 of 22 rows, read by no
// code) is deliberately let go. Inspection photos belong in the existing
// job-photo path if they are ever wanted. There is no copyXToR2 to run after
// this script.
//
// ── THE JOB FK IS A DIRECT LOOKUP HERE, NOT NAME MATCHING ──────────────────
// Unlike time entries — where Neon's job_id comes from job_name -> po_locked TEXT
// matching and self-heals hourly — these tables carry the Airtable Job RECORD ID,
// and jobs.airtable_id is populated. So the FK resolves exactly, or not at all.
// job_airtable_id is still stored as the durable key: the jobs table refreshes
// hourly, so a job created minutes ago has no Neon row to point at yet.
//
//   node db/etl/inspections-generators.mjs
//   NEON_URL='postgres://...branch...' node db/etl/inspections-generators.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { neon } from "../../netlify/functions/node_modules/@neondatabase/serverless/index.mjs";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.REPO_ROOT || path.resolve(HERE, "..", "..");
const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8").split(/\r?\n/)
    .filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);
const KEY  = env.AIRTABLE_PROD_READ_PAT || env.AIRTABLE_API_KEY;
const BASE = env.AIRTABLE_PROD_BASE_ID || "appiqWg6SvKcGfMAu";
const NEON = process.env.NEON_URL || env.NEON_URL || env.DATABASE_URL;
if (!KEY)  throw new Error("no Airtable PAT in .env (AIRTABLE_PROD_READ_PAT)");
if (!NEON) throw new Error("no Neon connection string (NEON_URL in .env)");

const sql = neon(NEON);
const sleep = ms => new Promise(r => setTimeout(r, ms));
const nul  = v => (v === undefined || v === "" ? null : v);
const num  = v => (v === undefined || v === "" || v === null ? null : Number(v));
const bool = v => v === true;
const first = v => (Array.isArray(v) && v.length ? v[0] : null);
// Airtable returns singleSelect as {name} in some field configs and a bare string
// in others; the live handlers defend against both, so this does too.
const sel  = v => (v && typeof v === "object" ? v.name : nul(v));
// A lookup field is ALWAYS an array, even when it holds one value.
const look = v => { const x = first(v); return x && typeof x === "object" ? nul(x.name ?? x.value) : nul(x); };
// Airtable dates come back "2026-05-12" or full ISO; Postgres wants the date part.
const day  = v => { const x = nul(v); return x ? String(x).slice(0, 10) : null; };

async function fetchAll(table) {
  const out = []; let offset;
  do {
    const u = new URL(`https://api.airtable.com/v0/${BASE}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (offset) u.searchParams.set("offset", offset);
    const r = await fetch(u, { headers: { Authorization: `Bearer ${KEY}` } });
    if (!r.ok) throw new Error(`${table} ${r.status}: ${await r.text()}`);
    const d = await r.json();
    out.push(...d.records); offset = d.offset;
    if (offset) await sleep(220);
  } while (offset);
  return out;
}

console.log(`extracting Step 4c tables from ${BASE} (read-only) ...`);
const [agencies, contacts, inspections, gens, gensvc, wtemplates, warranties] = await Promise.all([
  fetchAll("Inspection Agencies"), fetchAll("Inspection Contacts"), fetchAll("Job Inspections"),
  fetchAll("Generators"), fetchAll("Generator Service"),
  fetchAll("Warranty Templates"), fetchAll("Warranties"),
]);
console.log(`  ${agencies.length} agencies, ${contacts.length} contacts, ${inspections.length} inspections`);
console.log(`  ${gens.length} generators, ${gensvc.length} service records`);
console.log(`  ${wtemplates.length} warranty templates, ${warranties.length} warranties`);

// jobs.airtable_id -> jobs.id. Built once; a miss is expected and survivable
// (the hourly sync backfills), which is why job_airtable_id is the stored key.
const jobRows = await sql.query(`SELECT id, airtable_id FROM jobs WHERE airtable_id IS NOT NULL`);
const jobById = new Map(jobRows.map(r => [r.airtable_id, r.id]));
console.log(`  ${jobById.size} jobs available for FK resolution`);

const now = new Date().toISOString();
let jobMiss = 0;
const resolveJob = (link) => {
  const at = first(link);
  if (!at) return [null, null];
  const id = jobById.get(at) || null;
  if (!id) jobMiss++;
  return [at, id];
};

// ── 1. agencies ────────────────────────────────────────────────────────────
const agencyByAt = new Map();
for (const rec of agencies) {
  const f = rec.fields || {};
  const [row] = await sql.query(
    `INSERT INTO inspection_agencies
       (airtable_id, name, phone, email, scheduling_link, notes, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (airtable_id) DO UPDATE SET
       name=EXCLUDED.name, phone=EXCLUDED.phone, email=EXCLUDED.email,
       scheduling_link=EXCLUDED.scheduling_link, notes=EXCLUDED.notes,
       active=EXCLUDED.active, updated_at=now(), synced_at=EXCLUDED.synced_at
     RETURNING id`,
    [rec.id, f["Inspection Agency Name"] || "(unnamed)", nul(f["Agency Phone"]),
     nul(f["Agency Email"]), nul(f["Scheduling Link"]), nul(f["Notes"]),
     bool(f["Active"]), now]);
  agencyByAt.set(rec.id, row.id);
}
console.log(`  loaded ${agencyByAt.size} agencies`);

// ── 2. contacts ────────────────────────────────────────────────────────────
// inspector_name is a GENERATED column — never write it, it is derived from the
// two name parts so it cannot drift the way Airtable's formula could.
const contactByAt = new Map();
for (const rec of contacts) {
  const f = rec.fields || {};
  const [row] = await sql.query(
    `INSERT INTO inspection_contacts
       (airtable_id, agency_id, first_name, last_name, phone, email, notes, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (airtable_id) DO UPDATE SET
       agency_id=EXCLUDED.agency_id, first_name=EXCLUDED.first_name,
       last_name=EXCLUDED.last_name, phone=EXCLUDED.phone, email=EXCLUDED.email,
       notes=EXCLUDED.notes, active=EXCLUDED.active, updated_at=now(),
       synced_at=EXCLUDED.synced_at
     RETURNING id`,
    [rec.id, agencyByAt.get(first(f["Inspection Agency"])) || null,
     nul(f["First Name"]), nul(f["Last Name"]), nul(f["Phone"]), nul(f["Email"]),
     nul(f["Notes"]), bool(f["Active"]), now]);
  contactByAt.set(rec.id, row.id);
}
console.log(`  loaded ${contactByAt.size} contacts`);

// ── 3. job inspections ─────────────────────────────────────────────────────
// `Inspection Agency (Linked)` is the REAL record link; `Inspection Agency` is a
// lookup shadowing it. Migrate the link. See the schema file.
for (const rec of inspections) {
  const f = rec.fields || {};
  const [jobAt, jobId] = resolveJob(f["Job"]);
  await sql.query(
    `INSERT INTO job_inspections
       (airtable_id, job_airtable_id, job_id, agency_id, inspector_id,
        inspection_type, inspection_date, inspection_status, notes, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7::date,$8,$9,$10)
     ON CONFLICT (airtable_id) DO UPDATE SET
       job_airtable_id=EXCLUDED.job_airtable_id, job_id=EXCLUDED.job_id,
       agency_id=EXCLUDED.agency_id, inspector_id=EXCLUDED.inspector_id,
       inspection_type=EXCLUDED.inspection_type, inspection_date=EXCLUDED.inspection_date,
       inspection_status=EXCLUDED.inspection_status, notes=EXCLUDED.notes,
       synced_at=EXCLUDED.synced_at`,
    [rec.id, jobAt || "(none)", jobId,
     agencyByAt.get(first(f["Inspection Agency (Linked)"])) || null,
     contactByAt.get(first(f["Inspector"])) || null,
     sel(f["Inspection Type"]), day(f["Inspection Date"]),
     sel(f["Inspection Status"]), nul(f["Notes"]), now]);
}
console.log(`  loaded ${inspections.length} job inspections`);

// ── 4. generators ──────────────────────────────────────────────────────────
// customer_name is SNAPSHOTTED from Airtable's {Customer} lookup, not left to a
// join. Without it the asset id goes NULL for any generator whose job has not
// reached Neon yet — see the schema file.
const genByAt = new Map();
for (const rec of gens) {
  const f = rec.fields || {};
  const [jobAt, jobId] = resolveJob(f["Job"]);
  const [row] = await sql.query(
    `INSERT INTO generators
       (airtable_id, job_airtable_id, job_id, customer_name, brand, model, kw,
        serial_number, transfer_switch_model, transfer_switch_serial, fuel_type,
        install_date, service_plan_active, service_interval_months, warranty_expiration,
        status, notes, battery_install_date, service_call_created, job_type,
        tax_status, billing_method, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::date,$13,$14,$15::date,$16,$17,
             $18::date,$19,$20,$21,$22,$23)
     ON CONFLICT (airtable_id) DO UPDATE SET
       job_airtable_id=EXCLUDED.job_airtable_id, job_id=EXCLUDED.job_id,
       customer_name=EXCLUDED.customer_name, brand=EXCLUDED.brand, model=EXCLUDED.model,
       kw=EXCLUDED.kw, serial_number=EXCLUDED.serial_number,
       transfer_switch_model=EXCLUDED.transfer_switch_model,
       transfer_switch_serial=EXCLUDED.transfer_switch_serial, fuel_type=EXCLUDED.fuel_type,
       install_date=EXCLUDED.install_date, service_plan_active=EXCLUDED.service_plan_active,
       service_interval_months=EXCLUDED.service_interval_months,
       warranty_expiration=EXCLUDED.warranty_expiration, status=EXCLUDED.status,
       notes=EXCLUDED.notes, battery_install_date=EXCLUDED.battery_install_date,
       service_call_created=EXCLUDED.service_call_created, job_type=EXCLUDED.job_type,
       tax_status=EXCLUDED.tax_status, billing_method=EXCLUDED.billing_method,
       synced_at=EXCLUDED.synced_at
     RETURNING id`,
    // ⚠ NOT f["Customer"]. That is a LOOKUP, and over the REST API a lookup into a
    // linked table returns RECORD IDS — ["recGDL5n6zXHshAdq"], not "Betty Huber".
    // Airtable's own Asset ID formula reads {Customer} and renders a name, because
    // inside a formula a lookup resolves to its display value. The API does not.
    // Using it produced "recGDL5n6zXHshAdq - 20KW Cummins" on all 11 generators.
    // `Customer Name` is the formula field and is already the flat display string.
    [rec.id, jobAt || "(none)", jobId, nul(f["Customer Name"]),
     sel(f["Generator Brand"]), nul(f["Generator Model"]), num(f["Generator KW"]),
     nul(f["Generator Serial Number"]), nul(f["Transfer Switch Model"]),
     nul(f["Transfer Switch Serial Number"]), sel(f["Fuel Type"]),
     day(f["Install / In-Service Date"]), bool(f["Service Plan Active"]),
     num(f["Service Interval Months"]), day(f["Warranty Expiration"]),
     sel(f["Status"]), nul(f["Notes"]), day(f["Battery Install Date"]),
     bool(f["Service Call Created"]), sel(f["Job Type"]), sel(f["Tax Status"]),
     sel(f["Billing Method"]), now]);
  genByAt.set(rec.id, row.id);
}
console.log(`  loaded ${genByAt.size} generators`);

// ── 5. generator service ───────────────────────────────────────────────────
// Nine SEPARATE booleans, not an array — see the schema file for why this
// differs from fleet_maintenance.service_types.
for (const rec of gensvc) {
  const f = rec.fields || {};
  const [jobAt, jobId] = resolveJob(f["Job"]);
  await sql.query(
    `INSERT INTO generator_service
       (airtable_id, generator_id, job_airtable_id, job_id, service_date, service_type,
        technician, service_plan_visit, oil_changed, oil_filter_changed, air_filter_changed,
        spark_plugs_changed, battery_tested, battery_replaced, load_test_performed,
        firmware_checked, exercise_checked, trouble_codes, work_performed_notes,
        parts_used, labor_hours, generator_hours, synced_at)
     VALUES ($1,$2,$3,$4,$5::date,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
             $20,$21,$22,$23)
     ON CONFLICT (airtable_id) DO UPDATE SET
       generator_id=EXCLUDED.generator_id, job_airtable_id=EXCLUDED.job_airtable_id,
       job_id=EXCLUDED.job_id, service_date=EXCLUDED.service_date,
       service_type=EXCLUDED.service_type, technician=EXCLUDED.technician,
       service_plan_visit=EXCLUDED.service_plan_visit, oil_changed=EXCLUDED.oil_changed,
       oil_filter_changed=EXCLUDED.oil_filter_changed,
       air_filter_changed=EXCLUDED.air_filter_changed,
       spark_plugs_changed=EXCLUDED.spark_plugs_changed,
       battery_tested=EXCLUDED.battery_tested, battery_replaced=EXCLUDED.battery_replaced,
       load_test_performed=EXCLUDED.load_test_performed,
       firmware_checked=EXCLUDED.firmware_checked,
       exercise_checked=EXCLUDED.exercise_checked, trouble_codes=EXCLUDED.trouble_codes,
       work_performed_notes=EXCLUDED.work_performed_notes, parts_used=EXCLUDED.parts_used,
       labor_hours=EXCLUDED.labor_hours, generator_hours=EXCLUDED.generator_hours,
       synced_at=EXCLUDED.synced_at`,
    [rec.id, genByAt.get(first(f["Generator"])) || null, jobAt, jobId,
     day(f["Service Date"]), sel(f["Service Type"]), look(f["Technician Name"]),
     bool(f["Service Plan Visit"]), bool(f["Oil Changed"]), bool(f["Oil Filter Changed"]),
     bool(f["Air Filter Changed"]), bool(f["Spark Plugs Changed"]),
     bool(f["Battery Tested"]), bool(f["Battery Replaced"]),
     bool(f["Load Test Performed"]), bool(f["Firmware / Settings Checked"]),
     bool(f["Exercise Checked"]), nul(f["Trouble Codes Found"]),
     nul(f["Work Performed Notes"]), nul(f["Parts Used"]),
     num(f["Labor Hours"]), num(f["Generator Hours @ Service"]), now]);
}
console.log(`  loaded ${gensvc.length} generator service records`);

// ── 6. warranty templates ──────────────────────────────────────────────────
const tplByAt = new Map();
for (const rec of wtemplates) {
  const f = rec.fields || {};
  const [row] = await sql.query(
    `INSERT INTO warranty_templates
       (airtable_id, template_name, brand, model, warranty_type, duration_months,
        notes, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     ON CONFLICT (airtable_id) DO UPDATE SET
       template_name=EXCLUDED.template_name, brand=EXCLUDED.brand, model=EXCLUDED.model,
       warranty_type=EXCLUDED.warranty_type, duration_months=EXCLUDED.duration_months,
       notes=EXCLUDED.notes, active=EXCLUDED.active, synced_at=EXCLUDED.synced_at
     RETURNING id`,
    [rec.id, f["Template Name"] || "(unnamed)", nul(f["Brand"]), nul(f["Model"]),
     sel(f["Warranty Type"]), num(f["Duration Months"]), nul(f["Notes"]),
     bool(f["Active"]), now]);
  tplByAt.set(rec.id, row.id);
}
console.log(`  loaded ${tplByAt.size} warranty templates`);

// ── 7. warranties ──────────────────────────────────────────────────────────
for (const rec of warranties) {
  const f = rec.fields || {};
  await sql.query(
    `INSERT INTO warranties
       (airtable_id, generator_id, template_id, name, warranty_type, start_date,
        end_date, duration_months, source, voided, voided_reason, notes, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6::date,$7::date,$8,$9,$10,$11,$12,$13)
     ON CONFLICT (airtable_id) DO UPDATE SET
       generator_id=EXCLUDED.generator_id, template_id=EXCLUDED.template_id,
       name=EXCLUDED.name, warranty_type=EXCLUDED.warranty_type,
       start_date=EXCLUDED.start_date, end_date=EXCLUDED.end_date,
       duration_months=EXCLUDED.duration_months, source=EXCLUDED.source,
       voided=EXCLUDED.voided, voided_reason=EXCLUDED.voided_reason,
       notes=EXCLUDED.notes, synced_at=EXCLUDED.synced_at`,
    [rec.id, genByAt.get(first(f["Generator"])) || null,
     tplByAt.get(first(f["Created From Template"])) || null,
     nul(f["Warranty Name"]), sel(f["Warranty Type"]), day(f["Start Date"]),
     day(f["End Date"]), num(f["Duration Months"]), sel(f["Source"]),
     bool(f["Voided"]), nul(f["Voided Reason"]), nul(f["Notes"]), now]);
}
console.log(`  loaded ${warranties.length} warranties`);
if (jobMiss) console.log(`  ⚠ ${jobMiss} row(s) had a Job link with no matching Neon job (expected if a job is <1 h old)`);

// ── acceptance checks ──────────────────────────────────────────────────────
const [c] = await sql.query(
  `SELECT (SELECT count(*) FROM inspection_agencies)::int  AS agencies,
          (SELECT count(*) FROM inspection_contacts)::int  AS contacts,
          (SELECT count(*) FROM job_inspections)::int      AS inspections,
          (SELECT count(*) FROM generators)::int           AS generators,
          (SELECT count(*) FROM generator_service)::int    AS gensvc,
          (SELECT count(*) FROM warranty_templates)::int   AS templates,
          (SELECT count(*) FROM warranties)::int           AS warranties,
          (SELECT count(*) FROM generator_service WHERE generator_id IS NULL)::int AS svc_orphan,
          (SELECT count(*) FROM warranties WHERE generator_id IS NULL)::int        AS war_orphan,
          (SELECT count(*) FROM generators WHERE customer_name IS NULL)::int       AS gen_nocust`);

const checks = [
  ["inspection agencies",   agencies.length,    c.agencies],
  ["inspection contacts",   contacts.length,    c.contacts],
  ["job inspections",       inspections.length, c.inspections],
  ["generators",            gens.length,        c.generators],
  ["generator service",     gensvc.length,      c.gensvc],
  ["warranty templates",    wtemplates.length,  c.templates],
  ["warranties",            warranties.length,  c.warranties],
  // ⚠ These compare AIRTABLE'S orphan count to Neon's, NOT to zero.
  // Hard-coding 0 was wrong and made the first run look like a migration failure:
  // Airtable genuinely holds 1 service record and 2 warranties with NO Generator
  // link at all (recAT6hONzrizD0BZ; rec0HEjmoyTKGhu4n and recRegOAczeI73bS2, both
  // also blank-named). Faithfully carrying a pre-existing data gap is correct
  // behaviour. What must never happen is Neon inventing orphans Airtable does not
  // have — which is exactly what an equality check against Airtable catches and a
  // check against 0 cannot distinguish.
  ["service rows w/o a generator",
   gensvc.filter(r => !first(r.fields?.["Generator"])).length,     c.svc_orphan],
  ["warranties w/o a generator",
   warranties.filter(r => !first(r.fields?.["Generator"])).length, c.war_orphan],
  ["generators w/o a customer name", 0,         c.gen_nocust],
];
console.log("\n== ACCEPTANCE CHECKS (Airtable vs Neon) ==");
console.table(checks.map(([check, airtable, neon]) => ({ check, airtable, neon, ok: airtable === neon })));

// ── THE CHECK THAT MATTERS: the four ported formulas, against REAL rows ────
// Fixtures proved the SQL runs. This proves it agrees with Airtable on every
// live generator — asset id string, next service date, status badge, battery age.
const vrows = await sql.query(
  `SELECT airtable_id, asset_id, next_service_due::text AS nsd, service_status, battery_age_years
   FROM v_generators`);
const vByAt = new Map(vrows.map(r => [r.airtable_id, r]));
const diffs = [];
for (const rec of gens) {
  const f = rec.fields || {}, v = vByAt.get(rec.id);
  if (!v) { diffs.push({ gen: rec.id, field: "(row)", airtable: "present", neon: "MISSING" }); continue; }
  const cmp = [
    ["asset id",     nul(f["Generator Asset ID"]),                       v.asset_id],
    ["next service", day(f["Next Service Due"]),                         v.nsd],
    ["status",       nul(f["Service Status"]),                           v.service_status],
    ["battery age",  f["Battery Age"] ?? null, v.battery_age_years ?? null],
  ];
  for (const [field, at, ne] of cmp) {
    // Airtable's blank -> our NULL is the documented deviation, not a diff.
    if (at === null && ne === null) continue;
    if (String(at ?? "") !== String(ne ?? "")) {
      diffs.push({ gen: f["Generator Asset ID"] || rec.id, field, airtable: at, neon: ne });
    }
  }
}
console.log("\n== PORTED FORMULAS vs AIRTABLE (all live generators) ==");
if (diffs.length) { console.table(diffs); }
else console.log(`  no differences across ${gens.length} generators × 4 derived fields`);

const failed = checks.filter(([, a, b]) => a !== b);
console.log(failed.length || diffs.length
  ? `\n${failed.length} CHECK(S) FAILED, ${diffs.length} FORMULA DIFF(S)`
  : "\nAll checks passed — Neon copy verified against production Airtable.");
process.exit(failed.length || diffs.length ? 1 : 0);

// Fleet → Neon (roadmap Step 4b, fleet half). Same shape as db/etl/scissor-lifts.mjs.
//
// THREE tables, not one: Fleet Vehicles (11 rows, 9 photos), Fleet Maintenance
// (91) and Fleet Mileage Log (8). `Job Vehicle Trips` is deliberately NOT here —
// it has 0 rows and no handler references it.
//
// Vehicles load FIRST because both child tables carry a real FK to them. In
// Airtable the link is resolved by NAME (handleFleetServiceHistory filters on
// {Vehicle}="<name>"), which is fragile — two vehicles named alike, or a rename,
// and the history follows the wrong truck. Here it is a uuid FK.
//
// PHOTOS ARE NOT COPIED BY THIS SCRIPT. Airtable attachment URLs expire (~2 h),
// so they must be re-hosted in R2 — but the R2 credentials are write-only Netlify
// secrets that nobody holds a copy of. The copy therefore runs inside the
// deployed function, as the admin action `copyFleetPhotosToR2`. Same reasoning
// and same pattern as the lifts migration.
//
//   node db/etl/fleet.mjs
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
const nul = v => (v === undefined || v === "" ? null : v);
const num = v => (v === undefined || v === "" || v === null ? null : Number(v));
const first = v => (Array.isArray(v) && v.length ? v[0] : null);
// Airtable returns singleSelect as {name} in some field configs and a bare string
// in others; the live handlers already defend against both, so this does too.
const sel = v => (v && typeof v === "object" ? v.name : nul(v));

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

console.log(`extracting Fleet from ${BASE} (read-only) ...`);
const [vehicles, maintenance, mileage] = await Promise.all([
  fetchAll("Fleet Vehicles"), fetchAll("Fleet Maintenance"), fetchAll("Fleet Mileage Log"),
]);
console.log(`  ${vehicles.length} vehicles, ${maintenance.length} service records, ${mileage.length} mileage entries`);

const now = new Date().toISOString();
const vehByAt = new Map();
const vehByName = new Map();
for (const rec of vehicles) {
  const f = rec.fields || {};
  const [row] = await sql.query(
    `INSERT INTO fleet_vehicles
       (airtable_id, name, year, make, model, color, vin, plate, vehicle_type,
        current_mileage, mileage_date, oil_type, oil_capacity, tire_brand, tire_size,
        tire_install_date, notes, wrench_size, lug_torque, active, synced_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::date,$12,$13,$14,$15,$16::date,$17,$18,$19,$20,$21)
     ON CONFLICT (airtable_id) DO UPDATE SET
       name=EXCLUDED.name, year=EXCLUDED.year, make=EXCLUDED.make, model=EXCLUDED.model,
       color=EXCLUDED.color, vin=EXCLUDED.vin, plate=EXCLUDED.plate,
       vehicle_type=EXCLUDED.vehicle_type, current_mileage=EXCLUDED.current_mileage,
       mileage_date=EXCLUDED.mileage_date, oil_type=EXCLUDED.oil_type,
       oil_capacity=EXCLUDED.oil_capacity, tire_brand=EXCLUDED.tire_brand,
       tire_size=EXCLUDED.tire_size, tire_install_date=EXCLUDED.tire_install_date,
       notes=EXCLUDED.notes, wrench_size=EXCLUDED.wrench_size, lug_torque=EXCLUDED.lug_torque,
       active=EXCLUDED.active, synced_at=EXCLUDED.synced_at
     RETURNING id`,
    [rec.id, f["Vehicle Name"] || "(unnamed)", num(f["Year"]), nul(f["Make"]), nul(f["Model"]),
     nul(f["Color"]), nul(f["VIN"]), nul(f["License Plate"]), sel(f["Vehicle Type"]),
     num(f["Current Mileage"]), nul(f["Mileage Date"]), nul(f["Oil Type"]),
     num(f["Oil Capacity (qts)"]), nul(f["Tire Brand"]), nul(f["Tire Size"]),
     nul(f["Tire Install Date"]), nul(f["Notes"]), nul(f["Oil Drain Wrench Size"]),
     num(f["Lug Torque (ft-lbs)"]), f["Active"] === true, now]);
  vehByAt.set(rec.id, row.id);
  if (f["Vehicle Name"]) vehByName.set(String(f["Vehicle Name"]).trim().toLowerCase(), row.id);
}
console.log(`  loaded ${vehByAt.size} vehicles`);

// The child tables' Vehicle link comes back as either a record id or - because
// handleAddFleetService writes it with typecast - the vehicle NAME. Resolve both.
let unmatched = 0;
const resolveVehicle = (v) => {
  const raw = first(v);
  if (!raw) return null;
  const byId = vehByAt.get(raw);
  if (byId) return byId;
  const byName = vehByName.get(String(raw).trim().toLowerCase());
  if (byName) return byName;
  unmatched++;
  return null;
};

for (const rec of maintenance) {
  const f = rec.fields || {};
  const types = (f["Service Types"] || []).map(s => (typeof s === "object" ? s.name : s));
  await sql.query(
    `INSERT INTO fleet_maintenance
       (airtable_id, vehicle_id, service_date, mileage, service_types, filter_no,
        oil_type_used, oil_qty, tire_brand, tire_size, cost, performed_by, shop, notes, synced_at)
     VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
     ON CONFLICT (airtable_id) DO UPDATE SET
       vehicle_id=EXCLUDED.vehicle_id, service_date=EXCLUDED.service_date,
       mileage=EXCLUDED.mileage, service_types=EXCLUDED.service_types,
       filter_no=EXCLUDED.filter_no, oil_type_used=EXCLUDED.oil_type_used,
       oil_qty=EXCLUDED.oil_qty, tire_brand=EXCLUDED.tire_brand, tire_size=EXCLUDED.tire_size,
       cost=EXCLUDED.cost, performed_by=EXCLUDED.performed_by, shop=EXCLUDED.shop,
       notes=EXCLUDED.notes, synced_at=EXCLUDED.synced_at`,
    [rec.id, resolveVehicle(f["Vehicle"]), nul(f["Date"]), num(f["Mileage at Service"]),
     types, nul(f["Filter #"]), nul(f["Oil Type Used"]), num(f["Oil Qty (qts)"]),
     nul(f["Tire Brand Installed"]), nul(f["Tire Size Installed"]), num(f["Cost"]),
     nul(f["Performed By"]), nul(f["Shop / Location"]), nul(f["Notes"]), now]);
}
console.log(`  loaded ${maintenance.length} service records`);

for (const rec of mileage) {
  const f = rec.fields || {};
  await sql.query(
    `INSERT INTO fleet_mileage_log
       (airtable_id, vehicle_id, log_date, mileage, recorded_by, notes, synced_at)
     VALUES ($1,$2,$3::date,$4,$5,$6,$7)
     ON CONFLICT (airtable_id) DO UPDATE SET
       vehicle_id=EXCLUDED.vehicle_id, log_date=EXCLUDED.log_date, mileage=EXCLUDED.mileage,
       recorded_by=EXCLUDED.recorded_by, notes=EXCLUDED.notes, synced_at=EXCLUDED.synced_at`,
    [rec.id, resolveVehicle(f["Vehicle"]), nul(f["Date"]), num(f["Mileage"]),
     nul(f["Recorded By"]), nul(f["Notes"]), now]);
}
console.log(`  loaded ${mileage.length} mileage entries`);
if (unmatched) console.log(`  ⚠ ${unmatched} child row(s) had a Vehicle link that resolved to nothing`);

// ── acceptance checks ─────────────────────────────────────────────────────
const [c] = await sql.query(
  `SELECT (SELECT count(*) FROM fleet_vehicles)::int AS vehicles,
          (SELECT count(*) FROM fleet_maintenance)::int AS maintenance,
          (SELECT count(*) FROM fleet_mileage_log)::int AS mileage,
          (SELECT count(*) FROM fleet_vehicles WHERE active)::int AS active,
          (SELECT count(*) FROM fleet_maintenance WHERE vehicle_id IS NULL)::int AS svc_no_vehicle,
          (SELECT count(*) FROM fleet_mileage_log WHERE vehicle_id IS NULL)::int AS log_no_vehicle`);
const atActive = vehicles.filter(v => v.fields?.["Active"] === true).length;
const checks = [
  ["vehicles",          vehicles.length,    c.vehicles],
  ["active vehicles",   atActive,           c.active],
  ["service records",   maintenance.length, c.maintenance],
  ["mileage entries",   mileage.length,     c.mileage],
  ["service rows w/o a vehicle", 0,         c.svc_no_vehicle],
  ["mileage rows w/o a vehicle", 0,         c.log_no_vehicle],
];
console.log("\n== ACCEPTANCE CHECKS (Airtable vs Neon) ==");
console.table(checks.map(([check, airtable, neon]) => ({ check, airtable, neon, ok: airtable === neon })));
const failed = checks.filter(([, a, b]) => a !== b);
console.log(failed.length ? `\n${failed.length} CHECK(S) FAILED` : "\nAll checks passed.");
console.log("\nPhotos are NOT copied by this script — run the admin action copyFleetPhotosToR2.");
process.exit(failed.length ? 1 : 0);

// ── CONTACTS DIRECTORY (owner 2026-11-15) ──────────────────────────────────
// One screen to look up and edit every person the business deals with:
//   Customers / Contractors  → companies + contacts
//   Power Co.                → power_companies + power_contacts
//   Inspection               → inspection_agencies + inspection_contacts
//
// Neon-only: Airtable is gone (CLAUDE.md), so there is no mirror here.
// Ids on this screen are the Neon uuids. Read: any signed-in role. Save:
// admin + office (authzFor in airtable.js).
//
// ⚠⚠ JOBS CARRY COPIES of power-co and inspection details (name, phones,
// email) — the job card reads those columns, not the contact. A save here
// re-copies onto every job linked to that person/org IN THE SAME REQUEST, or
// the job would keep showing the old number forever (the hourly sync that
// used to refresh them is retired). Contractor contacts are read LIVE on the
// job, so they need nothing. A job's CUSTOMER name/phone/email are the job's
// own fields, not a link — editing a customer contact here does not change
// existing jobs, by design.
//
// ⚠ A contractor's NAME is not editable here: it is baked into every PO
// ("Bellaire (CLB 312)"), the contractor code, QB Time groups and pCloud
// folders. Renaming it is a bigger job than a text box.
//
// No delete. "Inactive" hides a person/org from pickers but keeps old jobs
// intact.

import { neonQuery, neonWrite } from "./_neon.js";

const str = (v) => {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s ? s : null;
};

// Editable fields per kind: client key → column. `active` is boolean.
export const DIRECTORY_KINDS = {
  company: {
    table: "companies", nameCol: "name",
    cols: { name: "name", phone: "primary_phone", email: "primary_email",
            address: "billing_address", active: "active_contractor" },
    createOnly: ["name"],
  },
  powerCompany: {
    table: "power_companies", nameCol: "name",
    cols: { name: "name", region: "utility_region", notes: "notes", active: "active" },
  },
  inspectionAgency: {
    table: "inspection_agencies", nameCol: "name", touch: "updated_at",
    cols: { name: "name", phone: "phone", email: "email", link: "scheduling_link",
            notes: "notes", active: "active" },
  },
  contact: {
    table: "contacts", touch: "synced_at",
    org: { col: "company_id", table: "companies", legacyCol: "company_airtable_id" },
    cols: { firstName: "first_name", lastName: "last_name", role: "role",
            phone: "primary_phone", email: "primary_email", street: "street",
            city: "city", state: "state", zip: "zip", active: "active" },
  },
  powerContact: {
    table: "power_contacts", touch: "synced_at",
    org: { col: "power_company_id", table: "power_companies",
           legacyCol: "power_company_airtable_id", nameCopyCol: "power_company_name" },
    cols: { firstName: "first_name", lastName: "last_name", role: "job_roles",
            phone: "cell_phone", officePhone: "office_phone", email: "email",
            notes: "notes", active: "active" },
  },
  inspectionContact: {
    table: "inspection_contacts", touch: "updated_at",
    org: { col: "agency_id", table: "inspection_agencies" },
    cols: { firstName: "first_name", lastName: "last_name", phone: "phone",
            email: "email", notes: "notes", active: "active" },
  },
};

// The roles a contact under a company can carry. "Customer" / "Friends &
// Family" / blank show on the Customers tab; the rest are the contractor's own
// people and show on the Contractors tab (and in the job's contractor-contact
// picker — keep GC_ROLES in index.html in step).
export const CONTACT_ROLE_OPTIONS = ["Customer", "Contractor Contact", "Owner", "Foreman",
  "Jobsite Contact", "Inspector", "Friends & Family"];

export async function handleContactsDirectory() {
  const [orgs, people] = await Promise.all([
    neonQuery(
      `SELECT 'company' AS kind, id::text AS id, name, primary_phone AS phone,
              primary_email AS email, billing_address AS address, NULL AS region,
              NULL AS link, NULL AS notes, active_contractor AS active
         FROM companies
       UNION ALL
       SELECT 'powerCompany', id::text, name, NULL, NULL, NULL, utility_region,
              NULL, notes, active
         FROM power_companies
       UNION ALL
       SELECT 'inspectionAgency', id::text, name, phone, email, NULL, NULL,
              scheduling_link, notes, active
         FROM inspection_agencies
       ORDER BY 3`),
    neonQuery(
      `SELECT 'contact' AS kind, id::text AS id, company_id::text AS org_id,
              first_name, last_name, role, primary_phone AS phone, NULL AS office_phone,
              primary_email AS email, street, city, state, zip, NULL AS notes, active
         FROM contacts
       UNION ALL
       SELECT 'powerContact', id::text, power_company_id::text, first_name, last_name,
              job_roles, cell_phone, office_phone, email, NULL, NULL, NULL, NULL, notes, active
         FROM power_contacts
       UNION ALL
       SELECT 'inspectionContact', id::text, agency_id::text, first_name, last_name,
              NULL, phone, NULL, email, NULL, NULL, NULL, NULL, notes, active
         FROM inspection_contacts`),
  ]);
  if (!orgs?.rows || !people?.rows) {
    const e = new Error(orgs?.error || people?.error || "Contacts couldn't be loaded.");
    e.statusCode = 503;
    throw e;
  }
  const s = (v) => (v === null || v === undefined ? "" : String(v));
  return {
    orgs: orgs.rows.map(r => ({
      kind: r.kind, id: r.id, name: s(r.name), phone: s(r.phone), email: s(r.email),
      address: s(r.address), region: s(r.region), link: s(r.link), notes: s(r.notes),
      active: r.active !== false,
    })),
    people: people.rows.map(r => ({
      kind: r.kind, id: r.id, orgId: r.org_id || "", firstName: s(r.first_name),
      lastName: s(r.last_name), role: s(r.role), phone: s(r.phone),
      officePhone: s(r.office_phone), email: s(r.email), street: s(r.street),
      city: s(r.city), state: s(r.state), zip: s(r.zip), notes: s(r.notes),
      active: r.active !== false,
    })),
    roleOptions: CONTACT_ROLE_OPTIONS,
  };
}

export class DirectoryInputError extends Error {}

// A unique-name index (companies_name_unique, db/schema/053) turns a duplicate
// into a plain message instead of a 500.
async function writeOrExplain(label, sql, vals) {
  try {
    return await neonWrite(label, sql, vals);
  } catch (e) {
    const m = String(e?.message || e);
    if (e?.code === "23505" || /duplicate key|unique constraint/i.test(m)) {
      throw new DirectoryInputError("Something with that name already exists — search for it instead.");
    }
    throw e;
  }
}

// POST { kind, id?, orgId?, fields:{…} } — no id = create.
export async function handleContactsDirectorySave(body) {
  const kind = String(body?.kind || "");
  const def = DIRECTORY_KINDS[kind];
  if (!def) throw new DirectoryInputError("Unknown contact type.");
  const id = str(body?.id);
  const fields = body?.fields && typeof body.fields === "object" ? body.fields : {};

  const sets = [], vals = [];
  const put = (col, v) => { vals.push(v); sets.push({ col, p: `$${vals.length}` }); };
  for (const [key, col] of Object.entries(def.cols)) {
    if (!(key in fields)) continue;
    if (id && def.createOnly?.includes(key)) continue;
    if (key === "active") { put(col, fields.active !== false); continue; }
    let v = str(fields[key]);
    if (key === "state" && v) v = v.toUpperCase();
    if (key === "role" && kind === "contact" && v && !CONTACT_ROLE_OPTIONS.includes(v)) {
      throw new DirectoryInputError("Pick a type from the list.");
    }
    put(col, v);
  }

  // Name rules: an org needs a name; a person needs a first or last name.
  if (def.nameCol && !id && !str(fields.name)) throw new DirectoryInputError("A name is required.");
  if (def.nameCol && id && "name" in fields && !def.createOnly?.includes("name") && !str(fields.name)) {
    throw new DirectoryInputError("A name is required.");
  }
  if (!def.nameCol) {
    const has = (k) => k in fields ? !!str(fields[k]) : null;
    const first = has("firstName"), last = has("lastName");
    if (!id && !first && !last) throw new DirectoryInputError("A first or last name is required.");
    if (id && first === false && last === false) throw new DirectoryInputError("A first or last name is required.");
  }

  // Which company / utility / agency the person is filed under.
  if (def.org && (body?.orgId !== undefined || !id)) {
    const orgId = str(body?.orgId);
    if (!orgId) throw new DirectoryInputError("Pick who this person works for.");
    vals.push(orgId);
    const p = `$${vals.length}`;
    sets.push({ col: def.org.col, p: `(SELECT id FROM ${def.org.table} WHERE id::text = ${p})` });
    if (def.org.legacyCol) {
      sets.push({ col: def.org.legacyCol,
                  p: `(SELECT COALESCE(airtable_id, id::text) FROM ${def.org.table} WHERE id::text = ${p})` });
    }
    if (def.org.nameCopyCol) {
      sets.push({ col: def.org.nameCopyCol, p: `(SELECT name FROM ${def.org.table} WHERE id::text = ${p})` });
    }
  }

  let rowId;
  if (id) {
    if (!sets.length) return { ok: true, id, jobsUpdated: 0 };
    vals.push(id);
    const touch = def.touch ? `, ${def.touch} = now()` : "";
    const rows = await writeOrExplain(`directory.update.${kind}`,
      `UPDATE ${def.table} SET ${sets.map(x => `${x.col} = ${x.p}`).join(", ")}${touch}
        WHERE id::text = $${vals.length} RETURNING id::text AS id`, vals);
    if (!rows?.length) throw new DirectoryInputError("That contact no longer exists.");
    rowId = rows[0].id;
  } else {
    if (!sets.some(x => x.col === "active") && def.cols.active) sets.push({ col: def.cols.active, p: "true" });
    const rows = await writeOrExplain(`directory.create.${kind}`,
      `INSERT INTO ${def.table} (${sets.map(x => x.col).join(", ")})
       VALUES (${sets.map(x => x.p).join(", ")}) RETURNING id::text AS id`, vals);
    rowId = rows?.[0]?.id;
    if (!rowId) throw new Error(`directory.create.${kind}: no id returned`);
    return { ok: true, id: rowId, jobsUpdated: 0 };
  }

  // Re-copy onto the jobs that carry this person's / org's details.
  const jobsUpdated = await refreshJobCopies(kind, rowId);
  return { ok: true, id: rowId, jobsUpdated };
}

// ⚠ Matches both id forms jobs store: the legacy rec id and the uuid text.
async function refreshJobCopies(kind, id) {
  let sql = null;
  if (kind === "powerContact") {
    sql = `UPDATE jobs j SET power_company_contact = x.name, power_company_cell_phone = x.cell_phone,
                  power_company_office_phone = x.office_phone, power_company_email = x.email,
                  synced_at = now()
             FROM power_contacts x
            WHERE x.id::text = $1 AND j.power_contact_at_id IN (x.id::text, x.airtable_id)`;
  } else if (kind === "powerCompany") {
    sql = `UPDATE jobs j SET power_company_name = x.name, synced_at = now()
             FROM power_companies x
            WHERE x.id::text = $1 AND j.power_company_at_id IN (x.id::text, x.airtable_id)`;
  } else if (kind === "inspectionAgency") {
    sql = `UPDATE jobs j SET inspection_agency = x.name, inspection_agency_phone = x.phone,
                  inspection_agency_email = x.email, inspection_scheduling_link = x.scheduling_link,
                  synced_at = now()
             FROM inspection_agencies x
            WHERE x.id::text = $1 AND j.inspection_agency_at_id IN (x.id::text, x.airtable_id)`;
  } else if (kind === "inspectionContact") {
    sql = `UPDATE jobs j SET inspector_name = x.inspector_name, inspector_phone = x.phone,
                  inspector_email = x.email, synced_at = now()
             FROM inspection_contacts x
            WHERE x.id::text = $1 AND j.inspector_at_id IN (x.id::text, x.airtable_id)`;
  }
  if (!sql) return 0;
  const rows = await neonWrite(`directory.jobCopies.${kind}`, `${sql} RETURNING j.id`, [id]);
  return rows?.length || 0;
}

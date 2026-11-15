// ── GOOGLE CONTACTS SYNC — Neon → rick@northeasternelec.com ────────────────
// Owner 2026-11-15: "I wish I could get these contacts to show up under my
// rick@ email on the proper groups". rick@ ONLY (not nee@), every person
// labelled by TAB and by COMPANY:
//
//   contacts, role Customer / Friends & Family / blank → "NEE Customers"
//   contacts, any other role                            → "NEE Contractors"
//   power_contacts                                      → "NEE Power Co."
//   inspection_contacts                                 → "NEE Inspection"
//   …plus a label named after their company / utility / agency.
//
// Builds on _google-contacts.js — read its header first. The rules from there
// that matter most here:
//   • ID-FIRST. A stored google_person_id_1 is always an UPDATE, never a create.
//   • Only a genuine 404 means "gone". A stored id that no longer resolves is
//     NEVER re-created (it is usually a person merged in Google). It is looked
//     for again by phone/email and re-linked on exactly one unclaimed match;
//     otherwise REPORTED. An old id that answers with a merged contact under a
//     new name is re-pointed at that name.
//   • Two app rows on ONE Google contact (an app duplicate) → the second is
//     reported "shared" and left alone, so they can't overwrite each other.
//   • NO DELETE. Not people, not labels, not label memberships.
//   • A create's new id is written to Neon IMMEDIATELY, before anything else
//     can fail — an unrecorded create happens again next run.
//
// Before creating, the person is looked for in rick@'s address book by email /
// phone and ADOPTED if exactly one contact matches — so people already typed
// into the phone (and the power contacts the old Make sync created) are linked,
// not duplicated. Two or more matches = reported, left alone.
//
// Updates MERGE: the app's phone/email go first, but extra phones/emails added
// on the phone are kept. Addresses/companies only replace when the app has one.
//
// MODES (GOOGLE_CONTACTS): off/dry → nothing is written by the hourly run or on
// save. The PREVIEW (dryRun) works in every mode once credentials are set, so
// the owner can read exactly what would happen before anything is switched on.

import { createHash } from "node:crypto";
import {
  DESTINATIONS, googleConfigured, googleContactsMode, getPeopleBatch, createPerson,
  updatePerson, listConnections, listContactGroups, createContactGroup,
  addToContactGroup, toStoredId, toResourceName, personFromContact,
  personFromPowerContact, GoogleError,
} from "./_google-contacts.js";

export const TAB_LABELS = {
  customers: "NEE Customers", contractors: "NEE Contractors",
  power: "NEE Power Co.", inspection: "NEE Inspection",
};
// Keep in step with CT_CUSTOMER_ROLES in index.html.
const CUSTOMER_ROLES = ["customer", "friends & family", ""];
const TABLE = { contact: "contacts", powerContact: "power_contacts", inspectionContact: "inspection_contacts" };

const digits10 = (s) => { const d = String(s || "").replace(/\D/g, ""); return d.length >= 10 ? d.slice(-10) : ""; };
const lc = (s) => String(s || "").trim().toLowerCase();

export function personFromInspectionContact(row) {
  const person = {};
  const first = (row.first_name || "").trim(), last = (row.last_name || "").trim();
  if (first || last) person.names = [{ givenName: first || undefined, familyName: last || undefined }];
  if ((row.email || "").trim()) person.emailAddresses = [{ value: row.email.trim() }];
  if ((row.phone || "").trim()) person.phoneNumbers = [{ value: row.phone.trim() }];
  if ((row.org_name || "").trim()) person.organizations = [{ name: row.org_name.trim(), title: "Inspector" }];
  return person;
}

// Every person the app holds, with what Google should show and which labels.
export async function loadSyncPeople(q, only) {
  const rows = await q(
    `SELECT 'contact' AS kind, c.id::text AS id, c.google_person_id_1 AS gid, c.google_sync_hash AS hash,
            c.active, c.first_name, c.last_name, c.role, c.primary_phone, c.primary_email,
            NULL::text AS cell_phone, NULL::text AS office_phone, NULL::text AS email, NULL::text AS phone,
            c.street, c.city, c.state, c.zip, co.name AS org_name
       FROM contacts c LEFT JOIN companies co ON co.id = c.company_id
     UNION ALL
     SELECT 'powerContact', p.id::text, p.google_person_id_1, p.google_sync_hash, p.active,
            p.first_name, p.last_name, p.job_roles, NULL, NULL, p.cell_phone, p.office_phone, p.email, NULL,
            NULL, NULL, NULL, NULL, pc.name
       FROM power_contacts p LEFT JOIN power_companies pc ON pc.id = p.power_company_id
     UNION ALL
     SELECT 'inspectionContact', i.id::text, i.google_person_id_1, i.google_sync_hash, i.active,
            i.first_name, i.last_name, NULL, NULL, NULL, NULL, NULL, i.email, i.phone,
            NULL, NULL, NULL, NULL, a.name
       FROM inspection_contacts i LEFT JOIN inspection_agencies a ON a.id = i.agency_id`, []);
  return rows
    .filter(r => !only || (r.kind === only.kind && r.id === only.id))
    .map(r => {
      let person, tab;
      if (r.kind === "contact") {
        person = personFromContact({ ...r, company_name: r.org_name });
        tab = CUSTOMER_ROLES.includes(lc(r.role)) ? "customers" : "contractors";
      } else if (r.kind === "powerContact") {
        person = personFromPowerContact({ ...r, job_roles: r.role, power_company_name: r.org_name });
        tab = "power";
      } else {
        person = personFromInspectionContact(r);
        tab = "inspection";
      }
      const labels = [TAB_LABELS[tab]];
      if ((r.org_name || "").trim()) labels.push(r.org_name.trim());
      const name = [r.first_name, r.last_name].map(s => (s || "").trim()).filter(Boolean).join(" ") || "(no name)";
      const phones = (person.phoneNumbers || []).map(p => digits10(p.value)).filter(Boolean);
      const emails = (person.emailAddresses || []).map(e => lc(e.value)).filter(Boolean);
      const hash = createHash("sha1").update(JSON.stringify({ person, labels })).digest("hex");
      return { kind: r.kind, id: r.id, gid: r.gid || null, storedHash: r.hash || null, hash,
               active: r.active !== false, name, person, labels, phones, emails, tab };
    });
}

// The app's values first; Google-only extras kept. Only the fields the app
// manages; Google's metadata is dropped from what is sent back.
export function mergePerson(existing, desired) {
  const ex = existing || {};
  const out = {};
  const strip = (arr, keys) => (arr || []).map(o => Object.fromEntries(keys.filter(k => o[k] != null && o[k] !== "").map(k => [k, o[k]])));
  out.names = desired.names || strip(ex.names, ["givenName", "familyName", "middleName", "unstructuredName"]);
  const dEmails = desired.emailAddresses || [];
  const dE = new Set(dEmails.map(e => lc(e.value)));
  out.emailAddresses = [...dEmails, ...strip(ex.emailAddresses, ["value", "type"]).filter(e => !dE.has(lc(e.value)))];
  const dPhones = desired.phoneNumbers || [];
  const dP = new Set(dPhones.map(p => digits10(p.value) || p.value));
  out.phoneNumbers = [...dPhones, ...strip(ex.phoneNumbers, ["value", "type"]).filter(p => !dP.has(digits10(p.value) || p.value))];
  out.organizations = desired.organizations || strip(ex.organizations, ["name", "title"]);
  out.addresses = desired.addresses ||
    strip(ex.addresses, ["streetAddress", "city", "region", "postalCode", "country", "type"]);
  for (const k of Object.keys(out)) if (!out[k] || !out[k].length) delete out[k];
  return out;
}

// What Google holds, in the same shape mergePerson produces, for comparing.
function comparable(p) {
  return mergePerson(p, {});
}
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);

// ── A COPY BEFORE ANY WRITE (schema 095) ──────────────────────────────────
// Owner 2026-11-15: "keep a copy of my current contacts before the edit gets
// pushed". A live run writes NOTHING to Google unless a snapshot of the
// account from the last 20 h exists; if none, it takes one first, and if THAT
// fails the run stops. The snapshot holds every contact with exactly the
// fields this sync can change (PERSON_FIELDS) + the label list, so anything it
// touched can be put back. Newest 30 kept per account.
export async function ensureGoogleSnapshot({ q, write, subject, reason = "before sync" }) {
  const recent = await q(
    `SELECT id, taken_at FROM google_contacts_snapshots
      WHERE account = $1 AND taken_at > now() - interval '20 hours'
      ORDER BY taken_at DESC LIMIT 1`, [subject]);
  if (recent?.length) return { ok: true, taken: false, id: recent[0].id, takenAt: recent[0].taken_at };
  const contacts = await listConnections(subject);
  const groups = await listContactGroups(subject);
  const ins = await write("google.snapshot",
    `INSERT INTO google_contacts_snapshots (account, reason, contact_count, group_count, data)
     VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING id, taken_at`,
    [subject, reason, contacts.length, groups.length, JSON.stringify({ contacts, groups })]);
  const row = ins?.[0];
  if (!row?.id) throw new Error("the snapshot didn't save");
  await write("google.snapshotPrune",
    `DELETE FROM google_contacts_snapshots WHERE account = $1 AND id NOT IN
       (SELECT id FROM google_contacts_snapshots WHERE account = $1 ORDER BY taken_at DESC LIMIT 30)`, [subject]);
  return { ok: true, taken: true, id: row.id, takenAt: row.taken_at, contacts: contacts.length };
}

// One pass. opts: { q, write, dryRun, budgetMs, only:{kind,id} }
//   q(text, params) → rows        write(label, text, params) → rows
export async function runGoogleContactsSync({ q, write, dryRun = true, budgetMs = 7000, only = null } = {}) {
  const started = Date.now();
  const timeLeft = () => budgetMs - (Date.now() - started);
  const dest = DESTINATIONS[0];
  const mode = googleContactsMode();
  const live = !dryRun && mode === "on";
  const report = {
    ok: true, mode, account: dest.subject, preview: !live,
    counts: { create: 0, adopt: 0, relink: 0, update: 0, labelsOnly: 0, unchanged: 0, missingInGoogle: 0,
              sharedInGoogle: 0, ambiguous: 0, noDetails: 0, inactive: 0, notReached: 0 },
    labels: { toCreate: [], adds: {} },
    items: [], errors: [], budgetHit: false,
  };
  if (!googleConfigured()) {
    return { ...report, ok: false, error: "Google isn't connected (no credentials for rick@)." };
  }

  if (live) {
    try {
      report.snapshot = await ensureGoogleSnapshot({ q, write, subject: dest.subject });
    } catch (e) {
      return { ...report, ok: false, preview: true,
        error: `Couldn't save a copy of ${dest.subject}'s contacts first, so nothing was written to Google: ${String(e?.message || e).slice(0, 200)}` };
    }
  }

  const people = await loadSyncPeople(q, only);
  // Everyone's stored id, so an adopt can never hand one Google person to two rows.
  const takenIds = new Set(people.map(p => p.gid).filter(Boolean));

  const work = [];
  for (const p of people) {
    if (!p.active) { report.counts.inactive++; continue; }
    if (!p.gid && !p.phones.length && !p.emails.length) {
      report.counts.noDetails++;
      report.items.push({ action: "skip", kind: p.kind, name: p.name, detail: "no phone or email — nothing worth putting in Google" });
      continue;
    }
    if (p.gid && p.storedHash === p.hash && !only) { report.counts.unchanged++; continue; }
    work.push(p);
  }

  // Read the existing people (2 calls for ~230) and, only if anyone needs
  // creating, rick@'s whole list to adopt from.
  const withId = work.filter(p => p.gid);
  const existing = withId.length ? await getPeopleBatch(dest.subject, withId.map(p => p.gid)) : new Map();
  // ⚠ When Google merges two contacts, the old id can still answer — with the
  // SURVIVOR, whose resourceName is different. The survivor's id is the one to
  // store, and it counts as taken.
  const canonOf = (gid) => { const c = gid && existing.get(gid); return c?.resourceName ? toStoredId(c.resourceName) : null; };
  for (const p of withId) { const cn = canonOf(p.gid); if (cn) takenIds.add(cn); }
  const claimed = new Map(); // canonical id -> app person who claimed it this run
  // People whose stored id is gone are looked for again by phone/email.
  const needAdopt = work.some(p => !p.gid || !existing.get(p.gid));
  const byPhone = new Map(), byEmail = new Map();
  if (needAdopt) {
    for (const c of await listConnections(dest.subject)) {
      const rn = toStoredId(c.resourceName);
      for (const ph of c.phoneNumbers || []) { const d = digits10(ph.value); if (d) (byPhone.get(d) || byPhone.set(d, new Set()).get(d)).add(rn); }
      for (const em of c.emailAddresses || []) { const e = lc(em.value); if (e) (byEmail.get(e) || byEmail.set(e, new Set()).get(e)).add(rn); }
      existing.set(rn, existing.get(rn) || c);
    }
  }

  // Labels: find the ones that exist; the rest are created on a live run.
  const groups = new Map();
  for (const g of await listContactGroups(dest.subject)) {
    if (g.groupType === "USER_CONTACT_GROUP" && g.name) groups.set(lc(g.name), g.resourceName);
  }
  const labelAdds = new Map(); // label name -> [stored ids]
  const doneHashes = [];        // {kind,id,hash} written once labels are in

  for (const p of work) {
    if (live && timeLeft() < 1500) { report.budgetHit = true; break; }
    try {
      let gid = p.gid, current = gid ? existing.get(gid) : null, action, relinkWhy = "";
      const hitsFor = () => {
        const hits = new Set();
        for (const e of p.emails) for (const rn of byEmail.get(e) || []) hits.add(rn);
        for (const d of p.phones) for (const rn of byPhone.get(d) || []) hits.add(rn);
        return hits;
      };
      if (gid && current) {
        const cn = canonOf(gid);
        if (cn && cn !== gid) { gid = cn; relinkWhy = "merged in Google — re-linked to the merged contact"; }
      }
      if (gid && !current) {
        // The old id is gone. Find the person again — but only an exact,
        // unclaimed, single match. Never re-created.
        const hits = hitsFor();
        const free = [...hits].filter(rn => !takenIds.has(rn));
        if (free.length === 1) {
          gid = free[0]; current = existing.get(gid); takenIds.add(gid);
          relinkWhy = "old Google contact gone — found again by phone/email, re-linked";
        } else {
          report.counts.missingInGoogle++;
          report.items.push({ action: "missing", kind: p.kind, name: p.name,
            detail: free.length > 1 ? `old Google contact gone; ${free.length} contacts now match — left alone`
                  : hits.size ? "old Google contact gone; its phone/email belongs to a contact another app person is linked to (an app duplicate?) — left alone"
                  : "old Google contact gone and nothing matches its phone/email — left alone, never re-created" });
          continue;
        }
      }
      if (gid && claimed.has(gid)) {
        report.counts.sharedInGoogle++;
        report.items.push({ action: "shared", kind: p.kind, name: p.name,
          detail: `same Google contact as ${claimed.get(gid)} in the app — left alone (merge the app duplicate)` });
        continue;
      }
      if (relinkWhy && live) {
        await write("google.relink", `UPDATE ${TABLE[p.kind]} SET google_person_id_1 = $2 WHERE id::text = $1`, [p.id, gid]);
      }
      if (!gid) {
        const hits = hitsFor();
        const free = [...hits].filter(rn => !takenIds.has(rn));
        if (free.length > 1 || (hits.size && !free.length)) {
          report.counts.ambiguous++;
          report.items.push({ action: "ambiguous", kind: p.kind, name: p.name,
            detail: free.length > 1 ? `matches ${free.length} different Google contacts — left alone`
                                    : "matches a Google contact already linked to someone else in the app — left alone" });
          continue;
        }
        if (free.length === 1) {
          gid = free[0]; current = existing.get(gid); action = "adopt";
          takenIds.add(gid);
          if (live) await write("google.adopt", `UPDATE ${TABLE[p.kind]} SET google_person_id_1 = $2 WHERE id::text = $1`, [p.id, gid]);
        } else {
          action = "create";
          if (live) {
            const created = await createPerson(dest.subject, p.person);
            gid = toStoredId(created.resourceName);
            takenIds.add(gid);
            // ⚠⚠ Immediately — see the header.
            await write("google.create", `UPDATE ${TABLE[p.kind]} SET google_person_id_1 = $2 WHERE id::text = $1`, [p.id, gid]);
          }
        }
      }
      if (gid) claimed.set(gid, p.name);
      if (action !== "create") {
        const before = comparable(current);
        const merged = mergePerson(current, p.person);
        const changed = !same(before, merged);
        if (changed && live) await updatePerson(dest.subject, gid, current.etag, merged);
        if (!action) action = relinkWhy ? "relink" : changed ? "update" : "labelsOnly";
        // Say WHAT would change, so the preview can be judged, not just counted.
        const FIELD = { names: "name", phoneNumbers: "phone", emailAddresses: "email",
                        organizations: "company/title", addresses: "address" };
        const what = changed ? Object.keys(FIELD).filter(k => !same(before[k], merged[k])).map(k => FIELD[k]) : [];
        const fields = what.length ? ` (${what.join(", ")})` : "";
        if (action === "update" || action === "adopt" || action === "relink") {
          report.items.push({ action, kind: p.kind, name: p.name,
            detail: action === "adopt" ? `found already in Google${changed ? " — will set" + fields : ""}`
                  : action === "relink" ? `${relinkWhy}${changed ? " — will set" + fields : ""}`
                  : `will set${fields}` });
        }
      } else {
        report.items.push({ action, kind: p.kind, name: p.name, detail: "new in Google" });
      }
      report.counts[action]++;
      // Labels the person isn't in yet.
      const have = new Set(((current?.memberships) || [])
        .map(m => m.contactGroupMembership?.contactGroupResourceName).filter(Boolean));
      for (const label of p.labels) {
        const rn = groups.get(lc(label));
        if (rn && have.has(rn)) continue;
        (labelAdds.get(label) || labelAdds.set(label, []).get(label)).push(gid || `(new) ${p.name}`);
      }
      doneHashes.push({ kind: p.kind, id: p.id, hash: p.hash });
    } catch (e) {
      report.counts.notReached++;
      report.errors.push({ name: p.name, error: String(e?.message || e).slice(0, 200) });
      if (e instanceof GoogleError && (e.code === "AUTH" || e.code === "FORBIDDEN" || e.code === "RATE_LIMIT")) break;
    }
  }

  for (const [label, ids] of labelAdds) {
    report.labels.adds[label] = ids.length;
    if (!groups.has(lc(label))) report.labels.toCreate.push(label);
  }

  if (live) {
    for (const [label, ids] of labelAdds) {
      try {
        let rn = groups.get(lc(label));
        if (!rn) { rn = (await createContactGroup(dest.subject, label)).resourceName; groups.set(lc(label), rn); }
        await addToContactGroup(dest.subject, rn, ids.filter(id => !id.startsWith("(new)")));
      } catch (e) {
        report.errors.push({ label, error: String(e?.message || e).slice(0, 200) });
        // Their fingerprints are not stored, so next run tries the label again.
        const failed = new Set(ids);
        for (let i = doneHashes.length - 1; i >= 0; i--) {
          const p = people.find(x => x.kind === doneHashes[i].kind && x.id === doneHashes[i].id);
          if (p && (failed.has(p.gid) || p.labels.includes(label))) doneHashes.splice(i, 1);
        }
      }
    }
    for (const h of doneHashes) {
      await write("google.hash", `UPDATE ${TABLE[h.kind]} SET google_sync_hash = $2 WHERE id::text = $1`, [h.id, h.hash]);
    }
  }
  report.remaining = Math.max(0, work.length - (report.counts.create + report.counts.adopt + report.counts.relink +
    report.counts.update + report.counts.labelsOnly + report.counts.missingInGoogle + report.counts.sharedInGoogle +
    report.counts.ambiguous + report.counts.notReached));
  // Keep the preview readable on a phone: the routine "labels only" rows are
  // counted, not listed.
  report.items.sort((a, b) => a.action.localeCompare(b.action) || a.name.localeCompare(b.name));
  return report;
}

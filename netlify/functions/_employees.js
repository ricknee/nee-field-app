// Shared employee lookup for login — Stages 2 and 3 of the employees/login
// migration. See docs/PLAN-employee-admin.md and db/schema/017_employees_full.sql.
// ---------------------------------------------------------------------------
// WHAT THIS IS FOR. Employees are the last Airtable-owned dimension in the
// field app, and login is the last thing reading them. Moving it is the moment
// Airtable goes dark — and it is also the single change in this migration
// that, done wrong, locks every user out of BOTH apps at once.
//
// ── THE KILL SWITCH ───────────────────────────────────────────────────────
// Which store decides is an ENV VAR, not a deploy:
//
//   LOGIN_SOURCE unset / "airtable"  →  Airtable decides (today). Neon runs as
//                                       a shadow and only logs disagreements.
//   LOGIN_SOURCE = "neon"            →  Neon decides, Airtable is the fallback
//                                       when Neon can't be reached.
//
// So this code ships inert, gets switched on when the shadow logs are clean,
// and is switched OFF again in seconds from the Netlify dashboard if anything
// looks wrong — no revert, no rebuild, no waiting for a deploy. Given the
// blast radius, that is worth more than the small amount of code it costs.
//
// ── THE TWO APPS DO NOT CURRENTLY AGREE, WHICH IS WHY THIS IS SHARED ───────
// airtable.js  matches  full name | username | email
// inventory.js matches  full name | FIRST name | username        (no email)
//
// So "patrick" logs into the inventory app and not the field app. Nobody
// noticed because everyone uses their username. Post-flip there can only be
// one rule, and this is it: the UNION of both — full name, first name,
// username, email. A union can only ever accept logins that already worked in
// one of the two apps, so nobody who can log in today stops being able to.
//
// ── AMBIGUITY IS REFUSED, NOT GUESSED ─────────────────────────────────────
// Airtable's version uses Array.find(), which silently takes the FIRST record
// when several match. Adding first-name matching makes that materially worse:
// two Daves and a shared PIN would hand one of them the other's session, with
// no error anywhere. Here, more than one match is a REFUSAL. There are no
// first-name collisions among active staff today (checked), so this changes
// nothing now and prevents a genuinely nasty class of bug later.
import { neonQuery } from "./_neon.js";

export function loginSource() {
  return String(process.env.LOGIN_SOURCE || "").trim().toLowerCase() === "neon"
    ? "neon" : "airtable";
}

// The four roles the apps understand. Anything else — including null — is an
// employee, matching both handleLogin's existing fallback exactly.
export function normalizeRole(raw) {
  const r = String(raw || "").trim().toLowerCase();
  return (r === "admin" || r === "office" || r === "viewer") ? r : "employee";
}

// ── TRI-STATE, AND THE DISTINCTION IS THE WHOLE POINT ─────────────────────
// A shadow can treat "no match" and "database unreachable" as the same thing.
// An AUTHORITY cannot: one means refuse this person, the other means we have
// no idea and must ask Airtable instead. Conflating them would either lock
// everybody out during a Neon blip, or quietly let people in during one.
//
//   { ok:false }                     Neon unavailable — caller MUST fall back
//   { ok:true, user:null }           Neon answered: no such login. Authoritative.
//   { ok:true, ambiguous:true, n }   Several matched. Authoritative refusal.
//   { ok:true, user:{id,name,role} } Exactly one.
//
// ⚠⚠ `user.id` is the AIRTABLE record id, never the Neon uuid. Everything
// downstream is built on rec ids — the revocation check keys on airtable_id,
// handlePeople returns rec ids, and the expense self-service scope compares
// authUser.id against Airtable's "Submitted By". Returning a uuid here would
// break all three at once, silently. This is the same trap that has already
// cost this migration real time on jobs and on the inventory cart.
export async function neonLoginCandidate(identifier, pin) {
  const id = String(identifier || "").trim().toLowerCase();
  const p  = String(pin || "").trim();
  // Not "unavailable" — an empty identifier or PIN is a real, answerable no.
  if (!id || !p) return { ok: true, user: null };

  // An empty stored PIN must never match, even against an empty submitted one —
  // both handleLogin's already guard this, and it is the difference between
  // "no PIN set yet" and "anyone can walk in".
  const q = await neonQuery(
    `SELECT airtable_id, name, role
       FROM employees
      WHERE active
        AND pin IS NOT NULL AND btrim(pin) <> '' AND btrim(pin) = $2
        AND ( lower(btrim(name))                    = $1
           OR lower(btrim(username))                = $1
           OR lower(btrim(email))                   = $1
           OR lower(split_part(btrim(name), ' ', 1)) = $1 )`, [id, p]);

  // q === null   → DATABASE_URL unset (deploy fault)
  // q.error      → unreachable, timeout, SQL error
  // Both mean "no opinion", NOT "refused".
  if (!q || q.error || !Array.isArray(q.rows)) return { ok: false };

  if (q.rows.length === 0) return { ok: true, user: null };
  if (q.rows.length > 1)   return { ok: true, ambiguous: true, n: q.rows.length };
  const r = q.rows[0];
  return { ok: true, user: { id: r.airtable_id, name: r.name || "", role: normalizeRole(r.role) } };
}

// ── Stage 4 readers: the secondary employee lookups ───────────────────────
// Everything that isn't login but still needed the Airtable Employees table —
// payroll rollups, the crew pickers, my-hours. All of them want the same two
// shapes, so they share these instead of each growing its own query.
//
// Both return null on ANY failure, which callers must treat as "ask Airtable"
// and NEVER as "no employees". The difference matters: an empty crew picker is
// an annoyance, but an empty employee list in a payroll rollup silently drops
// people from a pay period.
//
// Ids are AIRTABLE rec ids throughout, not Neon uuids — same contract as login,
// and for the same reason: every caller passes them straight back to code that
// expects rec ids.
export async function neonEmployeeById(airtableId) {
  const id = String(airtableId || "").trim();
  if (!id) return null;
  const q = await neonQuery(
    `SELECT airtable_id, name, username, role, active, labor_type
       FROM employees WHERE airtable_id = $1`, [id]);
  if (!q || q.error || !Array.isArray(q.rows) || q.rows.length === 0) return null;
  const r = q.rows[0];
  return {
    id: r.airtable_id, name: r.name || "", username: r.username || "",
    role: normalizeRole(r.role), active: r.active === true, laborType: r.labor_type || "",
  };
}

// `activeOnly` mirrors each caller's existing Airtable filter. The payroll
// rollups deliberately pass false: they union active staff with anyone who had
// hours or a bonus in the period, so a leaver still appears on the pay run they
// worked. Filtering them out here would look tidy and quietly underpay someone.
export async function neonEmployees(activeOnly = true) {
  const q = await neonQuery(
    `SELECT airtable_id, name, username, role, active, labor_type
       FROM employees
      ${activeOnly ? "WHERE active" : ""}
      ORDER BY name`, []);
  if (!q || q.error || !Array.isArray(q.rows)) return null;
  return q.rows.map(r => ({
    id: r.airtable_id, name: r.name || "", username: r.username || "",
    role: normalizeRole(r.role), active: r.active === true, laborType: r.labor_type || "",
  }));
}

// Compares what Airtable decided against what Neon would have, and logs only
// when they differ. Never throws, never returns anything the caller acts on —
// this must not be able to affect a login while Airtable is authoritative.
//
// `airtableUser` is the { id, role } the app is about to issue a token for, or
// null if the login was refused.
export async function shadowLoginCheck(app, identifier, pin, airtableUser) {
  try {
    const r = await neonLoginCandidate(identifier, pin);

    // No opinion is not a mismatch. It is also exactly why the flip could not
    // reuse the old two-state version of this function.
    if (!r.ok) return;

    if (r.ambiguous) {
      console.warn(`login-shadow[${app}]: AMBIGUOUS in Neon — ${r.n} employees match that identifier+PIN. Airtable ${airtableUser ? `allowed ${airtableUser.id}` : "refused"}.`);
      return;
    }
    if (!r.user && !airtableUser) return;                  // both say no
    if (airtableUser && !r.user) {
      // The dangerous direction to ship on: someone who can log in today would
      // be refused after the flip. Either Neon is stale, or the identifier form
      // isn't covered by the union rule.
      console.warn(`login-shadow[${app}]: Airtable allowed ${airtableUser.id} but Neon found NO match — would LOCK OUT after the flip.`);
      return;
    }
    if (!airtableUser && r.user) {
      // The other dangerous direction: the flip would let someone in who is
      // refused today. Usually means Neon's copy is stale (a PIN or Active
      // changed in Airtable without the app writing it through).
      console.warn(`login-shadow[${app}]: Airtable refused but Neon would ALLOW ${r.user.id} — Neon copy is likely stale.`);
      return;
    }
    if (r.user.id !== airtableUser.id) {
      console.warn(`login-shadow[${app}]: DIFFERENT PERSON — Airtable ${airtableUser.id}, Neon ${r.user.id}.`);
      return;
    }
    if (r.user.role !== airtableUser.role) {
      console.warn(`login-shadow[${app}]: role differs for ${r.user.id} — Airtable "${airtableUser.role}", Neon "${r.user.role}".`);
      return;
    }
    // Agreement is logged too, and quietly. Without a positive signal there is
    // no way to tell "the shadow agrees on every login" from "the shadow never
    // ran", and those justify very different levels of confidence in the flip.
    console.log(`login-shadow[${app}]: match ${r.user.id} (${r.user.role})`);
  } catch (e) {
    console.warn(`login-shadow[${app}]: check failed (ignored): ${String(e?.message || e).slice(0, 200)}`);
  }
}

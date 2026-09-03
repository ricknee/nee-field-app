// Shared employee lookup for login. See docs/PLAN-employee-admin.md and
// db/schema/017_employees_full.sql.
// ---------------------------------------------------------------------------
// WHAT THIS IS FOR. Employees were the last Airtable-owned dimension in the
// field app, and login was the last thing reading them. This is the module that
// moved it — the single change in the migration that, done wrong, locks every
// user out of BOTH apps at once.
//
// ── THE KILL SWITCH IS GONE, AND THAT IS THE POINT ────────────────────────
// `LOGIN_SOURCE` staged this in three steps: Airtable decides with Neon
// shadowing, then Neon decides with Airtable as the fallback (2026-08-08), then
// — 2026-09-03 — Neon decides and there is no fallback. The env var and
// `shadowLoginCheck` went with that last step, because a switch is only worth
// its complexity while there is somewhere to switch BACK to, and there is not:
// Airtable has taken no employee write since AIRTABLE_WRITES=off (2026-08-25),
// so its PIN and Active columns are frozen. Reverting login to it would accept
// PINs the crew has already changed and admit people deactivated since.
//
// ⛔ DO NOT REINTRODUCE AN AIRTABLE LOGIN PATH. If Neon is unreachable both
// handleLogin's now answer 503, deliberately — see the notes there.
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

// The four roles the apps understand. Anything else — including null — is an
// employee, matching both handleLogin's existing fallback exactly.
// ── IS THIS A PLAUSIBLE EMPLOYEE HANDLE? (cutover slice 5, 2026-08-24) ─────
// Replaces fifteen copies of `String(employeeId).startsWith("rec")`, every one
// of which would have rejected a natively-hired employee's own id with a 400 —
// they could log in and then be refused by their own PIN screen, their hours,
// their rate history and the People screen.
//
// ⚠⚠ THIS IS THE `b79b9a0` TRAP IN ITS LOUD FORM. That regression was handlers
// which never validated an id and silently forwarded it; the note from it was
// "grepping startsWith('rec') is NOT sufficient". True — but the inverse is
// just as real, and this slice is where it bites: a guard that DOES validate
// hard-fails the new id form. Both halves have to be swept, and a grep for the
// guard only finds the second.
//
// Deliberately a SUPERSET of the old test: anything starting with "rec" still
// passes exactly as before, so no rec id that works today can start failing.
// The uuid branch is the only new acceptance.
export function isEmployeeHandle(v) {
  const s = String(v ?? "").trim();
  if (s.startsWith("rec")) return true;
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s);
}

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
// ⚠⚠ `user.id` IS THE EMPLOYEE'S HANDLE, AND IT IS THE REC ID WHENEVER ONE
// EXISTS — `COALESCE(airtable_id, id::text)` since cutover slice 5 (2026-08-24).
//
// It was a bare `airtable_id`, with the note: "everything downstream is built on
// rec ids — the revocation check keys on airtable_id, handlePeople returns rec
// ids, and the expense self-service scope compares authUser.id against
// Airtable's Submitted By. Returning a uuid here would break all three at once,
// silently."
//
// All of that is still true, and COALESCE is what keeps it true. An employee who
// HAS a rec id still gets that rec id back, byte for byte — so no existing
// session, token or stored id changes, and the three consumers above are
// untouched. Only a NATIVE hire (no Airtable row, impossible before this slice)
// ever yields a uuid, and every one of those consumers now resolves either form.
//
// ⚠ Never "simplify" this to `id`. That version logs the entire crew out: the
// login id is baked into a 30-day HMAC token, so every phone in the field is
// holding a rec id and will keep sending it for up to a month.
export async function neonLoginCandidate(identifier, pin) {
  const id = String(identifier || "").trim().toLowerCase();
  const p  = String(pin || "").trim();
  // Not "unavailable" — an empty identifier or PIN is a real, answerable no.
  if (!id || !p) return { ok: true, user: null };

  // An empty stored PIN must never match, even against an empty submitted one —
  // both handleLogin's already guard this, and it is the difference between
  // "no PIN set yet" and "anyone can walk in".
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS handle, name, role
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
  return { ok: true, user: { id: r.handle, name: r.name || "", role: normalizeRole(r.role) } };
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
// Ids follow the same contract as login: `COALESCE(airtable_id, id::text)`, so a
// rec id stays a rec id and only a native hire yields a uuid. Every caller passes
// these straight back into code that resolves an employee, and since slice 5
// every one of those sites takes either form.
//
// ⚠ The PARAMETER is a handle, not a rec id — the argument is still named
// `airtableId` at the call sites for churn's sake, but a uuid is equally valid
// and must be, because that is what a native hire's session carries.
export async function neonEmployeeById(handle) {
  const id = String(handle || "").trim();
  if (!id) return null;
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS handle, name, username, role, active, labor_type
       FROM employees WHERE airtable_id = $1 OR id::text = $1`, [id]);
  if (!q || q.error || !Array.isArray(q.rows) || q.rows.length === 0) return null;
  const r = q.rows[0];
  return {
    id: r.handle, name: r.name || "", username: r.username || "",
    role: normalizeRole(r.role), active: r.active === true, laborType: r.labor_type || "",
  };
}

// `activeOnly` mirrors each caller's existing Airtable filter. The payroll
// rollups deliberately pass false: they union active staff with anyone who had
// hours or a bonus in the period, so a leaver still appears on the pay run they
// worked. Filtering them out here would look tidy and quietly underpay someone.
export async function neonEmployees(activeOnly = true) {
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS handle, name, username, role, active, labor_type
       FROM employees
      ${activeOnly ? "WHERE active" : ""}
      ORDER BY name`, []);
  if (!q || q.error || !Array.isArray(q.rows)) return null;
  return q.rows.map(r => ({
    id: r.handle, name: r.name || "", username: r.username || "",
    role: normalizeRole(r.role), active: r.active === true, laborType: r.labor_type || "",
  }));
}

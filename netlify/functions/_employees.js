// Shared employee lookup for login — Stage 2 of the employees/login migration.
// See docs/PLAN-employee-admin.md and db/schema/017_employees_full.sql.
// ---------------------------------------------------------------------------
// WHAT THIS IS FOR. Employees are the last Airtable-owned dimension in the
// field app, and login is the last thing reading them. Moving it is the moment
// Airtable goes dark — and it is also the single change in this whole
// migration that, done wrong, locks every user out of BOTH apps at once.
//
// So it moves in two steps. Right now this module is a SHADOW: Airtable still
// decides who gets in, and this runs alongside so we can see whether it would
// have reached the same verdict on real logins. Only once that is quiet does
// Stage 3 make it authoritative. Same strangler pattern as the payroll reads.
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
export function normalizeRole(raw) {
  const r = String(raw || "").trim().toLowerCase();
  return (r === "admin" || r === "office" || r === "viewer") ? r : "employee";
}

// Returns:
//   { id, name, role }        one unambiguous match
//   { ambiguous: true, n }    several matched — caller must refuse
//   null                      no match, OR Neon unavailable (indistinguishable
//                             on purpose while this is a shadow; Stage 3 has to
//                             tell them apart before it can be authoritative)
export async function neonLoginCandidate(identifier, pin) {
  const id = String(identifier || "").trim().toLowerCase();
  const p  = String(pin || "").trim();
  if (!id || !p) return null;

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

  if (!q || q.error || !Array.isArray(q.rows) || q.rows.length === 0) return null;
  if (q.rows.length > 1) return { ambiguous: true, n: q.rows.length };
  const r = q.rows[0];
  return { id: r.airtable_id, name: r.name || "", role: normalizeRole(r.role) };
}

// Compares what Airtable decided against what Neon would have, and logs only
// when they differ. Never throws, never returns anything the caller acts on —
// this must not be able to affect a login while it is a shadow.
//
// `airtableUser` is the { id, role } the app is about to issue a token for, or
// null if the login was refused.
export async function shadowLoginCheck(app, identifier, pin, airtableUser) {
  try {
    const neon = await neonLoginCandidate(identifier, pin);

    // Neon unreachable/unconfigured is not a mismatch — it is no opinion. It
    // reads the same as "no match" here, which is exactly why Stage 3 cannot
    // ship until this function can distinguish them.
    if (!neon && !airtableUser) return;

    if (neon?.ambiguous) {
      console.warn(`login-shadow[${app}]: AMBIGUOUS in Neon — ${neon.n} employees match that identifier+PIN. Airtable ${airtableUser ? `allowed ${airtableUser.id}` : "refused"}.`);
      return;
    }
    if (airtableUser && !neon) {
      // The dangerous direction to ship on: someone who can log in today would
      // be refused after the flip. Either Neon is stale, or the identifier form
      // (e.g. first name) isn't covered.
      console.warn(`login-shadow[${app}]: Airtable allowed ${airtableUser.id} but Neon found NO match — would LOCK OUT after the flip.`);
      return;
    }
    if (!airtableUser && neon) {
      // The other dangerous direction: the flip would let someone in who is
      // refused today. Usually means Neon's copy is stale (a PIN or Active
      // changed in Airtable without the app writing it through).
      console.warn(`login-shadow[${app}]: Airtable refused but Neon would ALLOW ${neon.id} — Neon copy is likely stale.`);
      return;
    }
    if (neon.id !== airtableUser.id) {
      console.warn(`login-shadow[${app}]: DIFFERENT PERSON — Airtable ${airtableUser.id}, Neon ${neon.id}.`);
      return;
    }
    if (neon.role !== airtableUser.role) {
      console.warn(`login-shadow[${app}]: role differs for ${neon.id} — Airtable "${airtableUser.role}", Neon "${neon.role}".`);
      return;
    }
    // Agreement is logged too, and quietly. Without a positive signal there is
    // no way to tell "the shadow agrees on every login" from "the shadow never
    // ran", and those justify very different levels of confidence in Stage 3.
    console.log(`login-shadow[${app}]: match ${neon.id} (${neon.role})`);
  } catch (e) {
    console.warn(`login-shadow[${app}]: check failed (ignored): ${String(e?.message || e).slice(0, 200)}`);
  }
}

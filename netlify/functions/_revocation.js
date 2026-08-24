// Session revocation — the piece that makes "deactivate" actually mean
// "logged out". Slice 1 of docs/PLAN-employee-admin.md.
// ---------------------------------------------------------------------------
// THE PROBLEM. Session tokens are stateless HMAC (_auth.js): a signature and a
// 30-day expiry, verified with no database read. That is what makes them cheap,
// and it is also why unchecking `Active` in Airtable does nothing to a phone
// that is already signed in — handleLogin checks `Active`, and handleLogin is
// the one thing a signed-in user never calls again. A leaver kept full access
// for up to 30 days.
//
// THE FIX. Each employee may carry a `token_valid_from` stamp in Neon. A token
// whose `iat` predates it is dead. Deactivating someone sets the stamp; the
// check below rejects their next request.
//
// WHY IT ISN'T EXPENSIVE. The loader reads only the rows that HAVE a stamp —
// zero on a normal day, a handful ever — and caches for 60s in module scope. A
// warm function instance therefore adds one tiny query per minute, not one per
// request. The cost of the whole feature is bounded by that.
//
// ── THE TWO CONTRACTS, WHICH ARE OPPOSITE ON PURPOSE ───────────────────────
// Reading this list (here) FAILS SOFT. Writing to it (handleSetEmployeeActive)
// FAILS CLOSED. That looks inconsistent and is not:
//
//   * If Neon is unreachable we cannot know who is revoked. Rejecting everyone
//     would take every crew member offline over a database blip, to protect
//     against a leaver who is already blocked from logging in again. So the
//     check allows, and logs. The honest cost: during a full Neon outage, an
//     already-issued revoked token works again.
//   * But an admin who clicks "deactivate" and sees it succeed must not be
//     lied to. A revocation we could not record is a revocation that did not
//     happen, so that write throws (neonWrite) rather than half-succeeding.
//
// Same reasoning as _neon.js vs _auth.js: never break a request the app could
// serve, but never claim to have done something you didn't.
import { neonQuery } from "./_neon.js";

// Bound on how stale the list may be — i.e. the worst case between clicking the
// toggle and the phone going dead. Short enough to be a usable answer to "they
// quit this morning", long enough that the query is noise.
export const REVOCATION_TTL_MS = 60 * 1000;

// { loadedAt, byId: Map<airtableId, epochMs> } | null
let _cache = null;
let _lastLoadFailure = null;   // logged once per distinct cause, not per request

// Drop the cache so the next check re-reads. Called right after a revocation is
// written, so the instance that performed it stops honouring the old session
// immediately rather than up to TTL later. Other instances still take up to
// REVOCATION_TTL_MS — the ≤60s guarantee is the real one, this is just a
// courtesy to the admin doing the clicking.
export function clearRevocationCache() { _cache = null; }

// Seed the cache directly, bypassing Neon. Used by the offline test suite,
// which has no database; production always populates it from loadRevocations().
export function primeRevocationCache(entries, nowMs = Date.now()) {
  _cache = { loadedAt: nowMs, byId: new Map(entries) };
}

async function loadRevocations(nowMs) {
  if (_cache && nowMs - _cache.loadedAt < REVOCATION_TTL_MS) return _cache.byId;

  // ⚠⚠ `COALESCE(airtable_id, id::text)` — cutover slice 5, and this one is a
  // SECURITY hole if it regresses, not a cosmetic id mismatch.
  //
  // The map is keyed by the handle, and `isSessionRevoked` looks up `user.id`
  // from the token. A natively-hired employee has `airtable_id NULL`, so a bare
  // emit here keyed their entry under the string "null" while their session
  // carries a uuid — the lookup misses, the function returns "not revoked", and
  // **deactivating that person would not end their session at all.** They would
  // keep full access for the remaining life of a 30-day token, which is the
  // precise failure this whole file was written to fix.
  //
  // It fails that way silently: the admin clicks the toggle, the write succeeds,
  // the UI says done, and the phone keeps working.
  const q = await neonQuery(
    `SELECT COALESCE(airtable_id, id::text) AS handle, token_valid_from
       FROM employees
      WHERE token_valid_from IS NOT NULL`, []);

  // null = Neon not configured; q.error = unreachable/timeout/SQL error. Both
  // mean "cannot answer", which is NOT the same as "nobody is revoked" — so do
  // not cache it. Caching a failure as an empty list would turn a 4-second blip
  // into a 60-second hole.
  if (!q || q.error || !Array.isArray(q.rows)) {
    const why = q?.error || (q ? "no rows returned" : "Neon not configured");
    if (_lastLoadFailure !== why) {
      _lastLoadFailure = why;
      console.error(`_revocation: cannot load revocation list, allowing through: ${why}`);
    }
    return null;
  }

  _lastLoadFailure = null;
  const byId = new Map();
  for (const r of q.rows) {
    const t = new Date(r.token_valid_from).getTime();
    if (Number.isFinite(t)) byId.set(String(r.handle), t);
  }
  _cache = { loadedAt: nowMs, byId };
  return byId;
}

// True only when we KNOW this session was issued before its owner was revoked.
// Any uncertainty returns false (allow) — see the contract note above.
export async function isSessionRevoked(user, nowMs = Date.now()) {
  if (!user || !user.id) return false;
  const byId = await loadRevocations(nowMs);
  if (!byId) return false;                  // fail soft
  const validFrom = byId.get(String(user.id));
  if (validFrom === undefined) return false;  // the normal case: not revoked
  // Strictly before: a token minted at exactly the revocation instant is
  // treated as newer. That case cannot arise in practice (a deactivated
  // employee cannot log in to get one) and erring toward allow keeps this
  // consistent with the fail-soft direction of everything else here.
  return (user.iat || 0) < validFrom;
}

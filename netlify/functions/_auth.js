// Shared server-side authorization for the NEE field + inventory functions.
// ---------------------------------------------------------------------------
// Stateless HMAC-signed session tokens — NO session store, no extra Airtable
// reads. Both airtable.js and inventory.js import this and share one AUTH_SECRET,
// so a token issued by either function validates in both.
//
// Token format:  base64url(JSON{id,role,iat,exp}) + "." + base64url(HMAC_SHA256)
// The signature covers the payload segment; verify recomputes it and compares
// in constant time, then checks expiry. Tamper with id/role/exp -> signature
// mismatch -> rejected.
//
// This proves IDENTITY + ROLE server-side. It does NOT harden the PIN login
// itself (still plaintext compare in each handleLogin) — that's a separate pass.
// ---------------------------------------------------------------------------
import { createHmac, timingSafeEqual } from "node:crypto";

// 30 days — matches the existing client session lifetimes (nee_user_v2 / nee_inv_v2).
export const TOKEN_TTL_MS = 30 * 24 * 60 * 60 * 1000;

function b64url(buf) {
  return Buffer.from(buf).toString("base64")
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function fromB64url(s) {
  return Buffer.from(String(s).replace(/-/g, "+").replace(/_/g, "/"), "base64");
}

// Fail closed: no secret -> cannot sign or verify. Callers' ensureEnv() should
// also assert AUTH_SECRET so the failure surfaces clearly at request entry.
function secret() {
  const s = process.env.AUTH_SECRET;
  if (!s) throw new Error("Missing env var AUTH_SECRET");
  return s;
}

function sign(payloadB64) {
  return b64url(createHmac("sha256", secret()).update(payloadB64).digest());
}

// Issue a token for a freshly-authenticated user. `nowMs` is injectable for tests.
export function signToken({ id, role }, nowMs = Date.now()) {
  const payload = { id, role: role || "employee", iat: nowMs, exp: nowMs + TOKEN_TTL_MS };
  const p = b64url(JSON.stringify(payload));
  return `${p}.${sign(p)}`;
}

// Verify a raw token string. Returns { id, role, iat } on success, or null on
// ANY failure (missing, malformed, bad signature, expired). Never throws.
//
// `iat` is carried out because signature+expiry alone cannot answer "has this
// session been revoked since it was issued?" — see _revocation.js, which
// compares it against the owner's token_valid_from stamp. Every token has
// always had an iat; this only stopped discarding it.
export function verifyToken(token, nowMs = Date.now()) {
  if (!token || typeof token !== "string") return null;
  const i = token.indexOf(".");
  if (i < 1 || i === token.length - 1) return null;
  const p = token.slice(0, i);
  const sig = token.slice(i + 1);
  let expected;
  try { expected = sign(p); } catch { return null; }     // no secret -> reject
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !timingSafeEqual(a, b)) return null;
  let payload;
  try { payload = JSON.parse(fromB64url(p).toString("utf8")); } catch { return null; }
  if (!payload || !payload.id || typeof payload.exp !== "number") return null;
  if (nowMs >= payload.exp) return null;
  return {
    id: payload.id,
    role: payload.role || "employee",
    // Number(...) rather than a bare read: iat comes from JSON a client could
    // have tampered with. The signature already makes tampering impossible, but
    // a non-numeric iat must not silently become NaN inside a `<` comparison
    // that decides access — NaN compares false and would quietly grant.
    iat: Number(payload.iat) || 0,
  };
}

// Pull the bearer token out of a Netlify event. Same-origin browser calls send
// `Authorization: Bearer <token>` (no preflight needed). Header casing varies by
// platform, so check both.
export function tokenFromEvent(event) {
  const h = (event && event.headers) || {};
  const raw = h.authorization || h.Authorization || "";
  const m = /^Bearer\s+(.+)$/i.exec(raw);
  return m ? m[1].trim() : null;
}

// Convenience: verified { id, role } from an event, or null.
export function authedUser(event, nowMs = Date.now()) {
  return verifyToken(tokenFromEvent(event), nowMs);
}

// allowed === null  -> any authenticated role is fine (general reads).
// allowed === [..]  -> role must be in the list.
export function hasRole(role, allowed) {
  if (allowed === null) return true;
  return Array.isArray(allowed) && allowed.includes(role);
}

// ── Scoped grants ──────────────────────────────────────────────────────────
// A short-lived capability bound to specific resource parts, carried IN A URL
// rather than an Authorization header.
//
// Why this exists: a browser <img src="..."> cannot send an Authorization
// header, so any endpoint that returns bytes to an <img> tag can't be
// protected by the bearer token the rest of the API uses. The alternatives are
// worse — leaving the endpoint unauthenticated, or fetching every image as a
// blob in JS (which forfeits browser caching, the thing keeping photo
// bandwidth survivable).
//
// So: the authenticated JSON call that lists a job's photos mints one grant per
// photo over (jobId, fileid); the image endpoint verifies the grant instead of
// a bearer token. A grant is useless for anything but the exact resource it
// names, expires on its own, and cannot be forged without AUTH_SECRET.
//
// This is NOT a general-purpose auth bypass. Only add an action to the grant
// path when it returns bytes to an <img>/<video> tag and the resource parts
// fully pin down what may be served.
export const SCOPE_TTL_MS = 6 * 60 * 60 * 1000;   // outlives a workday's gallery browsing

// NUL separator, written via fromCharCode so the byte is unambiguous in source
// rather than an invisible control character pasted into the file.
const SEP = String.fromCharCode(0);

function scopeSig(parts, exp) {
  // NUL-joined so parts can't be shifted across the boundary — ("ab","c") and
  // ("a","bc") must not produce the same signature.
  const msg = parts.map(p => String(p)).join(SEP) + "|" + exp;
  return b64url(createHmac("sha256", secret()).update(msg).digest());
}

export function signScope(parts, ttlMs = SCOPE_TTL_MS, nowMs = Date.now()) {
  const exp = nowMs + ttlMs;
  return `${exp}.${scopeSig(parts, exp)}`;
}

// Returns true only for an unexpired grant whose signature matches these exact
// parts. Never throws (a missing AUTH_SECRET rejects, same as verifyToken).
export function verifyScope(token, parts, nowMs = Date.now()) {
  if (!token || typeof token !== "string") return false;
  const i = token.indexOf(".");
  if (i < 1 || i === token.length - 1) return false;
  const exp = Number(token.slice(0, i));
  if (!Number.isFinite(exp) || nowMs >= exp) return false;
  let expected;
  try { expected = scopeSig(parts, exp); } catch { return false; }
  const a = Buffer.from(token.slice(i + 1));
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

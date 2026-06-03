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

// Verify a raw token string. Returns { id, role } on success, or null on ANY
// failure (missing, malformed, bad signature, expired). Never throws to callers.
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
  return { id: payload.id, role: payload.role || "employee" };
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

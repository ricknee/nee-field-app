// Shared Google People API client — contact sync to the two company address books.
// See docs/PLAN-google-contacts.md.
// ---------------------------------------------------------------------------
// CONTRACT: this is OPTIONAL infrastructure and FAILS SOFT. Like _r2.js and
// _neon.js, and unlike _auth.js, none of these env vars belong in ensureEnv().
// Saving a contact must succeed whether or not Google is reachable — the sync
// failing may never fail the write that triggered it.
//
// Callers get thrown GoogleError objects (with .code) rather than nulls, so
// "this person isn't in Google yet" and "the credentials are wrong" stay
// distinguishable. That distinction is load-bearing here: treating a bad
// credential as "not found" would make the sync CREATE, which is the one
// outcome this whole design exists to prevent (see below).
// ---------------------------------------------------------------------------
// ⚠⚠ THE HAZARD THAT SHAPES EVERY FUNCTION IN THIS FILE.
// 230 of the 240 contacts ALREADY EXIST in Google, in TWO accounts, and the
// only record of which Google person each one is lives in Neon
// (contacts.google_person_id_1/2, db/schema/049). A sync that starts cold, or
// that mistakes an auth failure for a miss, does not update those 230 — it
// CREATES 230 duplicates, twice over, in an address book that is live on
// people's phones. Recovering costs an afternoon of manual merging.
//
// Hence: ID-FIRST, ALWAYS. A row with a stored person id is an update, never a
// create. Nothing in this module creates anything unless the caller has already
// established there is no id to update.
// ---------------------------------------------------------------------------
// ⛔ THERE IS NO DELETE IN THIS FILE, AND THERE MUST NOT BE.
// Owner's decision, 2026-08-27: both accounts stay, and nee@ is becoming the
// office address book. What looks like duplication is TWO ADDRESS BOOKS, not
// duplicated data — every contact exists once per account by design. Deleting
// the nee@ copies would not de-duplicate anyone's phone; it would empty the one
// office staff are about to depend on. Genuine within-account duplicates belong
// to Google Contacts' own Merge & fix, which previews each merge.
// ---------------------------------------------------------------------------
// WHY A SERVICE ACCOUNT AND NOT OAUTH REFRESH TOKENS:
// Both destinations are on the company's own Workspace domain, so one service
// account with domain-wide delegation impersonates both. That means no consent
// screen, no per-account approval flow, and no refresh token that can be
// revoked by someone clicking "remove access" in their Google account. Rotation
// is a new key rather than a re-consent.
//
// NO NEW NPM DEPENDENCY. The JWT assertion is signed with node:crypto, which
// deliberately sidesteps netlify/functions/package.json entirely — see the
// CLAUDE.md note about the install step being the only reason declared
// dependencies ship at all. This module is importable with no node_modules
// present, so `node tests/handlers.test.mjs` stays offline and install-free.
// ---------------------------------------------------------------------------

import { createSign } from "node:crypto";

const DEFAULT_TIMEOUT_MS = 10000;

// The People API scope. Read-write on the impersonated user's own contacts.
// Narrower than `contacts.readonly` would allow and wider than we'd like, but
// there is no "create and update but never delete" scope — the no-delete rule
// is enforced by this file having no delete function, not by Google.
const SCOPE = "https://www.googleapis.com/auth/contacts";

const PEOPLE_BASE = "https://people.googleapis.com/v1";

// The fields we read back and write. Kept in one place because People API
// requires personFields on every read and updatePersonFields on every write,
// and a mismatch between them is the classic way an update silently drops data:
// updating with a narrower mask than you read does not merge, it REPLACES the
// masked fields and leaves the rest — so the two lists must stay aligned.
export const PERSON_FIELDS = "names,emailAddresses,phoneNumbers,organizations,addresses,metadata";
export const UPDATE_FIELDS = "names,emailAddresses,phoneNumbers,organizations,addresses";

// ── DESTINATIONS ──────────────────────────────────────────────────────────
// Recovered 2026-08-27 from the RETIRED Make blueprints — schema 049 says this
// mapping is "recorded NOWHERE", which was true of Airtable and false of Make:
// an undeployed scenario keeps its blueprint. Resolving each __IMTCONN__ in
// scenario 4729925 against connections_list gave:
//
//   google_person_id_1  <- Make conn 4769144 "Google - Rick"  -> rick@…
//   google_person_id_2  <- Make conn 4769161 "NEE -Google"    -> nee@…
//
// Defaulted here rather than left env-only ON PURPOSE: a missing env var would
// otherwise resolve to "no destinations" and the sync would report success
// having written nothing, which is the silent-failure shape this codebase keeps
// getting bitten by. Override per-environment if the accounts ever change.
export const DESTINATIONS = [
  { key: 1, column: "google_person_id_1", subject: process.env.GOOGLE_CONTACTS_DEST_1 || "rick@northeasternelec.com" },
  { key: 2, column: "google_person_id_2", subject: process.env.GOOGLE_CONTACTS_DEST_2 || "nee@northeasternelec.com" },
];

export class GoogleError extends Error {
  constructor(code, message, detail) {
    super(message);
    this.name = "GoogleError";
    this.code = code;
    this.detail = detail;
  }
}

// ── MODE ──────────────────────────────────────────────────────────────────
// Three states, and the default is inert. Same shape as GENERATOR_SERVICE_CALLS
// and TIME_CLOCK: unset must mean "this feature does not exist yet", so the
// code can ship and sit in production untouched while the credentials are
// still being wired up.
//
//   unset / anything else -> "off"  : does nothing, reports enabled:false
//   "dry"                 -> "dry"  : reports what it WOULD write, writes nothing
//   "on"                  -> "on"   : live
//
// ⚠ `dry` is not a developer convenience, it is the gate. Given the hazard at
// the top of this file, the first run against production MUST be a dry run that
// somebody reads.
export function googleContactsMode() {
  const v = String(process.env.GOOGLE_CONTACTS || "").trim().toLowerCase();
  return v === "on" ? "on" : v === "dry" ? "dry" : "off";
}

export function googleContactsEnabled() {
  return googleContactsMode() !== "off";
}

export function googleWritesLive() {
  return googleContactsMode() === "on";
}

// ── CREDENTIALS ───────────────────────────────────────────────────────────
// The service account JSON, base64-encoded into one env var. Base64 because
// the raw JSON contains a PEM private key full of newlines, and env vars
// carrying literal newlines survive some tooling and not others — encoding it
// removes a whole class of "works locally, fails on deploy".
let _credCache;

function credentials() {
  if (_credCache !== undefined) return _credCache;

  const raw = process.env.GOOGLE_SA_KEY;
  if (!raw) { _credCache = null; return null; }

  try {
    // Tolerate both a base64 blob and raw JSON pasted directly, because someone
    // WILL paste the file contents straight in and the failure would otherwise
    // be an unhelpful JSON parse error.
    const text = raw.trim().startsWith("{")
      ? raw
      : Buffer.from(raw, "base64").toString("utf8");
    const json = JSON.parse(text);

    if (!json.client_email || !json.private_key) {
      throw new GoogleError("BAD_KEY", "Service account JSON is missing client_email or private_key.");
    }
    _credCache = {
      clientEmail: json.client_email,
      privateKey: json.private_key,
      tokenUri: json.token_uri || "https://oauth2.googleapis.com/token",
      // The numeric id that has to be pasted into the Admin console's
      // domain-wide delegation screen. Surfaced in googleStatus() so nobody has
      // to go dig the JSON back out to check it matches.
      clientId: json.client_id || null,
      projectId: json.project_id || null,
    };
    return _credCache;
  } catch (e) {
    if (e instanceof GoogleError) throw e;
    _credCache = null;
    throw new GoogleError("BAD_KEY", `GOOGLE_SA_KEY could not be decoded: ${e?.message || e}`);
  }
}

export function googleConfigured() {
  try { return credentials() !== null; } catch { return false; }
}

function b64url(input) {
  return Buffer.from(input).toString("base64")
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

// ── ACCESS TOKENS ─────────────────────────────────────────────────────────
// One token per impersonated subject, cached until shortly before it expires.
// Tokens last an hour; a Netlify function instance rarely lives that long, but
// a reconcile pass over 240 contacts × 2 accounts would otherwise mint two
// tokens per contact and get rate-limited for no reason.
const _tokens = new Map(); // subject -> { token, expiresAt }

async function getAccessToken(subject, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const cached = _tokens.get(subject);
  // 60s of margin so a token can't expire mid-request.
  if (cached && cached.expiresAt > Date.now() + 60_000) return cached.token;

  const cred = credentials();
  if (!cred) throw new GoogleError("NOT_CONFIGURED", "GOOGLE_SA_KEY is unset.");

  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const claims = b64url(JSON.stringify({
    iss: cred.clientEmail,
    sub: subject,              // <- the impersonation. Without this the token is
                               //    the service account's OWN empty contact list.
    scope: SCOPE,
    aud: cred.tokenUri,
    iat: now,
    exp: now + 3600,
  }));

  let signature;
  try {
    signature = createSign("RSA-SHA256").update(`${header}.${claims}`).sign(cred.privateKey);
  } catch (e) {
    throw new GoogleError("BAD_KEY", `Could not sign with the service account private key: ${e?.message || e}`);
  }
  const assertion = `${header}.${claims}.${b64url(signature)}`;

  const res = await withTimeout((signal) => fetch(cred.tokenUri, {
    method: "POST",
    headers: { "content-type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
      assertion,
    }),
    signal,
  }), timeoutMs);

  const body = await res.json().catch(() => ({}));
  if (!res.ok || !body.access_token) {
    // These three are the mistakes that actually happen when wiring DWD up, and
    // each has a completely different fix. Google reports all of them as
    // "invalid_grant" or "unauthorized_client" with no further help, and the
    // wrong guess costs an hour.
    const err = String(body.error || res.status);
    const hint =
      err === "unauthorized_client"
        ? `Domain-wide delegation is not authorising this service account for ${subject}. Two causes: (a) the client ID + scope pair is missing from Admin console → Security → API controls → Domain-wide delegation, or (b) ${subject} is a GROUP or a mail-only ALIAS rather than a licensed user — delegation can only impersonate a real user, and this is what that looks like.`
      : err === "invalid_grant"
        ? `Google rejected the assertion for ${subject}. Usually the subject does not exist on this domain, or the server clock has drifted more than 5 minutes.`
      : err === "invalid_scope"
        ? `The scope ${SCOPE} is not among those authorised for this client ID in the Admin console.`
      : `Token request failed (${err}).`;
    throw new GoogleError("AUTH", hint, JSON.stringify(body).slice(0, 400));
  }

  _tokens.set(subject, {
    token: body.access_token,
    expiresAt: Date.now() + (Number(body.expires_in || 3600) * 1000),
  });
  return body.access_token;
}

function withTimeout(fn, ms) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ms);
  return Promise.resolve(fn(ctrl.signal)).finally(() => clearTimeout(timer));
}

// ── PEOPLE API ────────────────────────────────────────────────────────────
async function callPeople(subject, path, { method = "GET", body, query, timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const token = await getAccessToken(subject, timeoutMs);
  const url = new URL(`${PEOPLE_BASE}/${path.replace(/^\/+/, "")}`);
  for (const [k, v] of Object.entries(query || {})) {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  }

  const res = await withTimeout((signal) => fetch(url.toString(), {
    method,
    headers: {
      authorization: `Bearer ${token}`,
      ...(body ? { "content-type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : undefined,
    signal,
  }), timeoutMs);

  if (res.status === 404) throw new GoogleError("NOT_FOUND", `No such person in ${subject}.`);

  const json = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = json?.error?.message || `HTTP ${res.status}`;
    // ⚠ A 400 mentioning the etag is the single most common update failure, and
    // it is NOT a reason to fall back to creating — that would duplicate. The
    // caller re-reads the person and retries.
    if (res.status === 400 && /etag/i.test(msg)) {
      throw new GoogleError("STALE_ETAG", `The stored etag is stale — re-read the person and retry. (${msg})`);
    }
    if (res.status === 429 || res.status === 503) {
      throw new GoogleError("RATE_LIMIT", `Google is throttling or unavailable: ${msg}`);
    }
    if (res.status === 403) {
      throw new GoogleError("FORBIDDEN", `Refused for ${subject}: ${msg}. Usually the People API is not enabled on the Cloud project.`);
    }
    throw new GoogleError("HTTP", msg, JSON.stringify(json).slice(0, 400));
  }
  return json;
}

// Read one person. Returns null ONLY for a genuine "this id no longer exists"
// — every other failure throws, so a caller can never mistake an outage for an
// absence and go on to create a duplicate.
export async function getPerson(subject, resourceName, timeoutMs = DEFAULT_TIMEOUT_MS) {
  try {
    return await callPeople(subject, resourceName, {
      query: { personFields: PERSON_FIELDS }, timeoutMs,
    });
  } catch (e) {
    if (e instanceof GoogleError && e.code === "NOT_FOUND") return null;
    throw e;
  }
}

// ⚠ updateContact REPLACES the fields named in updatePersonFields and requires
// the person's CURRENT etag. So every update is read-then-write, and the etag
// must not be cached across runs — a stored one is stale the moment anybody
// edits that contact on their phone.
export async function updatePerson(subject, resourceName, etag, person, timeoutMs = DEFAULT_TIMEOUT_MS) {
  if (!etag) throw new GoogleError("NO_ETAG", "updatePerson needs a current etag — read the person first.");
  return await callPeople(subject, `${resourceName}:updateContact`, {
    method: "PATCH",
    query: { updatePersonFields: UPDATE_FIELDS, personFields: PERSON_FIELDS },
    body: { ...person, etag },
    timeoutMs,
  });
}

// ⚠⚠ The only function here that can produce a duplicate. Callers must have
// established there is no stored person id first — see the hazard note at the
// top. Returns the created person, whose .resourceName is the id that MUST be
// written back to Neon in the same request: a create whose id is not recorded
// is a create that will happen again on the next run.
export async function createPerson(subject, person, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return await callPeople(subject, "people:createContact", {
    method: "POST",
    query: { personFields: PERSON_FIELDS },
    body: person,
    timeoutMs,
  });
}

// Page the impersonated user's whole contact list. Only needed for the
// reconcile's "does this account hold duplicates of its own?" question — the
// sync itself never needs it, because it works from stored ids.
export async function listConnections(subject, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const out = [];
  let pageToken;
  do {
    const page = await callPeople(subject, "people/me/connections", {
      query: { personFields: PERSON_FIELDS, pageSize: 1000, pageToken },
      timeoutMs,
    });
    out.push(...(page.connections || []));
    pageToken = page.nextPageToken;
  } while (pageToken);
  return out;
}

// ── DIAGNOSTICS ───────────────────────────────────────────────────────────
// Same job as r2Status: name the specific misconfiguration rather than leaving
// somebody to infer it from the data afterwards.
//
// ⚠ Netlify bakes env vars at BUILD time, so setting GOOGLE_CONTACTS or
// GOOGLE_SA_KEY in the dashboard does NOT reach an already-deployed function
// until a REDEPLOY. That has already cost this project two mis-created jobs on
// a different switch, which is why rawValue is echoed below.
export async function googleStatus(timeoutMs = DEFAULT_TIMEOUT_MS) {
  const mode = googleContactsMode();
  const raw = process.env.GOOGLE_CONTACTS;

  let cred = null;
  let credError = null;
  try { cred = credentials(); } catch (e) { credError = e.message; }

  const base = {
    mode,
    rawValue: raw === undefined ? null : raw,
    writesLive: mode === "on",
    meaning:
      mode === "off" ? "INERT. The sync does nothing. Set GOOGLE_CONTACTS=dry and REDEPLOY to see what it would write."
    : mode === "dry" ? "DRY RUN. Reports what it would write and writes nothing."
    : "LIVE. Contact changes are written to Google.",
    serviceAccount: cred?.clientEmail || null,
    clientIdForDelegation: cred?.clientId || null,
    scopeForDelegation: SCOPE,
    destinations: DESTINATIONS.map(d => ({ key: d.key, column: d.column, account: d.subject })),
  };

  if (credError) return { ok: false, reason: "bad-key", hint: credError, ...base };
  if (!cred) {
    return {
      ok: false, reason: "not-configured",
      hint: "GOOGLE_SA_KEY is unset. Add the service account JSON, base64-encoded, in the Netlify dashboard — then REDEPLOY.",
      ...base,
    };
  }

  // Prove each impersonation independently. They fail separately and for
  // different reasons — one account being a group while the other is a real
  // user is exactly the case this catches.
  const checks = [];
  for (const d of DESTINATIONS) {
    try {
      const page = await callPeople(d.subject, "people/me/connections", {
        query: { personFields: "names", pageSize: 1 }, timeoutMs,
      });
      checks.push({
        account: d.subject, key: d.key, ok: true,
        totalContacts: page.totalPeople ?? null,
      });
    } catch (e) {
      checks.push({
        account: d.subject, key: d.key, ok: false,
        code: e?.code || "ERROR",
        hint: e?.message || String(e),
      });
    }
  }

  return { ok: checks.every(c => c.ok), reason: checks.every(c => c.ok) ? null : "impersonation", checks, ...base };
}

// ── BUILDING A GOOGLE PERSON FROM A NEON ROW ──────────────────────────────
// Kept here rather than in the handler so the reconcile and the writer produce
// byte-identical payloads — a dry run that reports a different shape from what
// the live run sends is worse than no dry run at all.
export function personFromContact(row) {
  const person = {};
  const first = (row.first_name || "").trim();
  const last = (row.last_name || "").trim();
  if (first || last) {
    person.names = [{ givenName: first || undefined, familyName: last || undefined }];
  }
  const email = (row.primary_email || "").trim();
  if (email) person.emailAddresses = [{ value: email }];
  const phone = (row.primary_phone || "").trim();
  if (phone) person.phoneNumbers = [{ value: phone }];
  if ((row.company_name || "").trim() || (row.role || "").trim()) {
    person.organizations = [{
      name: (row.company_name || "").trim() || undefined,
      title: (row.role || "").trim() || undefined,
    }];
  }
  const street = (row.street || "").trim();
  if (street || (row.city || "").trim()) {
    person.addresses = [{
      streetAddress: street || undefined,
      city: (row.city || "").trim() || undefined,
      region: (row.state || "").trim() || undefined,
      postalCode: (row.zip || "").trim() || undefined,
    }];
  }
  return person;
}

// Power contacts have a different column shape (two phones, a generated name)
// and no per-person id yet — see docs/PLAN-google-contacts.md §5.
export function personFromPowerContact(row) {
  const person = {};
  const first = (row.first_name || "").trim();
  const last = (row.last_name || "").trim();
  if (first || last) {
    person.names = [{ givenName: first || undefined, familyName: last || undefined }];
  } else if ((row.name || "").trim()) {
    // `name` is GENERATED in Postgres (Airtable's "Contact Name" was a formula).
    // Read it, never store a copy that can drift.
    person.names = [{ unstructuredName: row.name.trim() }];
  }
  const email = (row.email || "").trim();
  if (email) person.emailAddresses = [{ value: email }];
  const phones = [];
  if ((row.cell_phone || "").trim()) phones.push({ value: row.cell_phone.trim(), type: "mobile" });
  if ((row.office_phone || "").trim()) phones.push({ value: row.office_phone.trim(), type: "work" });
  if (phones.length) person.phoneNumbers = phones;
  if ((row.power_company_name || "").trim()) {
    person.organizations = [{
      name: row.power_company_name.trim(),
      title: (row.job_roles || "").trim() || undefined,
    }];
  }
  return person;
}

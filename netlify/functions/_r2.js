// Shared Cloudflare R2 client — jobsite photo storage.
// See docs/PLAN-job-photos.md.
// ---------------------------------------------------------------------------
// CONTRACT: R2 is OPTIONAL infrastructure. Like _neon.js and _pcloud.js, and
// unlike _auth.js, none of these env vars belong in ensureEnv(). A job view
// must still render when R2 is unconfigured or down — the Photos tab reports
// itself unavailable and nothing else changes.
//
// Callers get thrown R2Error objects (with .code) rather than nulls, so "this
// job has no photos yet" and "the credentials are wrong" stay distinguishable.
// ---------------------------------------------------------------------------
// WHY PRESIGNED URLS, NOT A BYTE PROXY:
// The pCloud design had to stream every image through the function, because
// pCloud restricts the referrer on its download links to pcloud.com — an <img>
// tag on our domain simply can't load one. R2 has no such restriction: we hand
// the browser a signed URL and it fetches from Cloudflare directly. That means
// no function invocation per thumbnail, no Netlify bandwidth per photo, and no
// 4.5 MB payload ceiling on upload. It is the single biggest reason R2 beats
// pCloud here mechanically, whatever the workflow arguments said.
//
// Consequence: the signScope/verifyScope grants and the jobPhoto byte-proxy
// built for pCloud are no longer needed for R2 reads. A presigned URL IS the
// capability — scoped to one object, expiring on its own.
// ---------------------------------------------------------------------------
// The signer is imported LAZILY on first use, so this module stays importable
// with no node_modules present and `node tests/handlers.test.mjs` remains
// offline and install-free. Same pattern as _neon.js.
//
// NOTE: aws4fetch is declared in netlify/functions/package.json. That file is
// only installed because netlify.toml runs `npm install --prefix
// netlify/functions --omit=dev`. If that command is ever removed, this import
// fails at runtime with "Cannot find package aws4fetch" — see CLAUDE.md.

const DEFAULT_TIMEOUT_MS = 8000;

// How long a signed image URL stays valid. Long enough to browse a gallery and
// come back after lunch, short enough that a URL leaked into a screenshot or a
// shared log stops working on its own.
export const VIEW_URL_TTL_SECONDS = 6 * 60 * 60;

// Upload URLs are single-purpose and used immediately.
export const UPLOAD_URL_TTL_SECONDS = 10 * 60;

export class R2Error extends Error {
  constructor(message, code) {
    super(message);
    this.name = "R2Error";
    this.code = code ?? null;
  }
}

export function r2Enabled() {
  return !!(process.env.R2_ACCOUNT_ID && process.env.R2_ACCESS_KEY_ID
         && process.env.R2_SECRET_ACCESS_KEY && process.env.R2_BUCKET);
}

function config() {
  const accountId = process.env.R2_ACCOUNT_ID;
  const accessKeyId = process.env.R2_ACCESS_KEY_ID;
  const secretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
  const bucket = process.env.R2_BUCKET;
  if (!accountId || !accessKeyId || !secretAccessKey || !bucket) {
    throw new R2Error("R2 is not configured", "NOT_CONFIGURED");
  }
  return {
    accountId, accessKeyId, secretAccessKey, bucket,
    endpoint: `https://${accountId}.r2.cloudflarestorage.com`,
  };
}

let _client = null;
let _initFailure = null;

async function getClient() {
  if (_client) return _client;
  const c = config();
  try {
    const { AwsClient } = await import("aws4fetch");
    _client = new AwsClient({
      accessKeyId: c.accessKeyId,
      secretAccessKey: c.secretAccessKey,
      service: "s3",
      // R2 ignores the region but SigV4 requires one in the signature. "auto"
      // is what Cloudflare's own docs use; anything else still signs, but keep
      // it consistent so signatures are reproducible in tests.
      region: "auto",
    });
    return _client;
  } catch (e) {
    // Never swallow this. A bare catch here would make a missing dependency
    // look identical to "this job has no photos" — the exact failure mode that
    // cost three days on the Neon driver (see CLAUDE.md).
    const msg = String(e?.message || e).slice(0, 300);
    if (_initFailure !== msg) {
      _initFailure = msg;
      console.error(`_r2: signer init FAILED, photo storage disabled: ${msg}`);
    }
    throw new R2Error(`R2 signer unavailable: ${msg}`, "NO_SIGNER");
  }
}

// Object keys are grouped by job so one prefix lists exactly one job's photos.
// Thumbnails sit beside the original with a suffix rather than in a parallel
// tree, so a single list call returns both and they can never drift apart.
export function jobPrefix(jobId) {
  return `jobs/${String(jobId)}/`;
}

// Albums are one optional path segment between the job and the file:
//   jobs/recBethel/Gym/20260731-01-a3f9.jpg      -> album "Gym"
//   jobs/recBethel/20260731-01-a3f9.jpg          -> no album
// The folder name IS the album — no table, no schema, nothing to keep in sync,
// and photos uploaded before albums existed keep working untouched.
//
// The name is percent-encoded in the key so spaces and punctuation survive a
// round trip ("Panel Room" stays "Panel Room", not "panel-room").
export const MAX_ALBUM_LEN = 60;

// Slashes would forge extra path segments, and "." / ".." could climb out of
// the job's prefix entirely — the client never picks raw keys, but an album
// name reaches the key builder, so it is sanitized at the boundary.
export function sanitizeAlbum(name) {
  const s = String(name ?? "").replace(/[/\\]/g, " ").replace(/\s+/g, " ").trim();
  if (!s || s === "." || s === "..") return null;
  return s.slice(0, MAX_ALBUM_LEN);
}

export function albumSegment(album) {
  const clean = sanitizeAlbum(album);
  return clean ? `${encodeURIComponent(clean)}/` : "";
}

// Album a key belongs to, or null for a loose photo at the job root.
export function albumFromKey(jobId, key) {
  const rel = String(key).slice(jobPrefix(jobId).length);
  const i = rel.indexOf("/");
  if (i < 0) return null;
  try { return decodeURIComponent(rel.slice(0, i)); }
  catch { return rel.slice(0, i); }   // tolerate a hand-made key
}
export function isThumbKey(key) {
  return /_thumb\.[a-z0-9]+$/i.test(String(key));
}
export function thumbKeyFor(key) {
  return String(key).replace(/(\.[a-z0-9]+)$/i, "_thumb$1");
}

async function withTimeout(fn, timeoutMs) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fn(ac.signal);
  } catch (e) {
    if (e?.name === "AbortError") throw new R2Error("R2 timed out", "TIMEOUT");
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// ── Signed URLs ────────────────────────────────────────────────────────────

// A time-limited URL the browser can use directly — <img src>, a download
// link, or an upload target. Never returns credentials themselves.
async function presign(method, key, { ttl, contentType } = {}) {
  const c = config();
  const client = await getClient();
  const url = new URL(`${c.endpoint}/${c.bucket}/${encodeURI(key)}`);
  url.searchParams.set("X-Amz-Expires", String(ttl));

  const signed = await client.sign(
    new Request(url, { method, ...(contentType ? { headers: { "Content-Type": contentType } } : {}) }),
    { aws: { signQuery: true } }
  );
  return signed.url;
}

export function presignGet(key, ttl = VIEW_URL_TTL_SECONDS) {
  return presign("GET", key, { ttl });
}

export function presignPut(key, contentType = "image/jpeg", ttl = UPLOAD_URL_TTL_SECONDS) {
  return presign("PUT", key, { ttl, contentType });
}

// Connectivity probe for the admin diagnostic. Does the cheapest real call
// there is — list one key — so it exercises the credentials, the account id in
// the endpoint, and the bucket name all at once. Reports which of those is
// wrong instead of collapsing everything into "photos unavailable".
export async function r2Status(timeoutMs = DEFAULT_TIMEOUT_MS) {
  if (!r2Enabled()) {
    const missing = ["R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"]
      .filter(k => !process.env[k]);
    return { ok: false, reason: "not-configured", missing };
  }
  const c = config();
  try {
    const client = await getClient();
    const url = new URL(`${c.endpoint}/${c.bucket}`);
    url.searchParams.set("list-type", "2");
    url.searchParams.set("max-keys", "1");

    const res = await withTimeout(
      (signal) => client.fetch(url.toString(), { method: "GET", signal }),
      timeoutMs
    );
    if (res.ok) {
      const xml = await res.text();
      const objects = parseListXml(xml);
      return { ok: true, bucket: c.bucket, accountId: c.accountId, hasObjects: objects.length > 0 };
    }
    const body = await res.text().catch(() => "");
    // Map the three mistakes that actually happen when wiring this up. Each
    // produces a different HTTP status, and each has a different fix.
    const hint =
      res.status === 403 ? "Credentials rejected, or the token isn't scoped to this bucket. Check R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY, and that the token lists this bucket."
    : res.status === 404 ? `Bucket "${c.bucket}" not found on this account. Check R2_BUCKET spelling and R2_ACCOUNT_ID.`
    : res.status === 401 ? "Signature rejected — usually a truncated or whitespace-padded secret key."
    : "Unexpected response from R2.";
    return { ok: false, reason: "http", status: res.status, hint, detail: body.slice(0, 300) };
  } catch (e) {
    if (e instanceof R2Error && e.code === "NO_SIGNER") {
      return { ok: false, reason: "no-signer", hint: "aws4fetch isn't installed in the function bundle — check netlify.toml still runs the npm install step." };
    }
    return { ok: false, reason: "error", detail: String(e?.message || e).slice(0, 300) };
  }
}

// Full round-trip through the SAME presigned urls the browser uses, executed
// server-side where no CORS rule and no service worker can interfere.
//
// This exists because a browser cannot tell these apart — R2 rejecting a
// signature returns an error response, and if that response carries no CORS
// headers the browser hides it and `fetch` throws a bare "Failed to fetch",
// identical to a CORS block. Running the same urls from here removes every
// browser variable: if PUT succeeds here, the signature is fine and the
// problem is in the browser; if it fails here, the signing is wrong.
export async function r2SelfTest(timeoutMs = DEFAULT_TIMEOUT_MS) {
  const steps = [];
  const key = "diagnostics/selftest.txt";
  const record = (name, ok, detail) => { steps.push({ step: name, ok, detail }); return ok; };

  try {
    const putUrl = await presignPut(key, "text/plain", 300);
    record("presign-put", true, `${putUrl.split("?")[0]} (+${putUrl.split("?")[1]?.length || 0} bytes of query)`);

    const putRes = await withTimeout(
      (signal) => fetch(putUrl, { method: "PUT", headers: { "Content-Type": "text/plain" }, body: "selftest", signal }),
      timeoutMs
    );
    const putBody = putRes.ok ? "" : (await putRes.text().catch(() => "")).slice(0, 300);
    if (!record("put", putRes.ok, `HTTP ${putRes.status}${putBody ? " " + putBody : ""}`)) {
      return { ok: false, steps };
    }

    const getUrl = await presignGet(key, 300);
    const getRes = await withTimeout((signal) => fetch(getUrl, { signal }), timeoutMs);
    const text = getRes.ok ? (await getRes.text()).slice(0, 40) : (await getRes.text().catch(() => "")).slice(0, 300);
    record("get", getRes.ok && text === "selftest", `HTTP ${getRes.status} body="${text}"`);

    // Clean up with header-signed auth (not presigned) so a failure here can't
    // be confused with a presigning problem.
    try {
      const client = await getClient();
      const c = config();
      const delRes = await withTimeout(
        (signal) => client.fetch(`${c.endpoint}/${c.bucket}/${encodeURI(key)}`, { method: "DELETE", signal }),
        timeoutMs
      );
      record("delete", delRes.ok || delRes.status === 204, `HTTP ${delRes.status}`);
    } catch (e) {
      record("delete", false, String(e?.message || e).slice(0, 200));
    }

    return { ok: steps.every(s => s.ok), steps };
  } catch (e) {
    record("threw", false, String(e?.message || e).slice(0, 300));
    return { ok: false, steps };
  }
}

// ── Listing ────────────────────────────────────────────────────────────────

// Minimal ListObjectsV2 XML reader. The response shape is small and fixed, so
// a targeted parse beats pulling in an XML dependency for four fields. If R2
// ever returns something unexpected we get [] rather than a throw, and the
// caller reports "no photos" — which is why callers must check for errors
// separately rather than treating [] as proof of an empty bucket.
function parseListXml(xml) {
  const out = [];
  const re = /<Contents>([\s\S]*?)<\/Contents>/g;
  let m;
  while ((m = re.exec(xml))) {
    const block = m[1];
    const pick = (tag) => {
      const t = new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`).exec(block);
      return t ? t[1] : null;
    };
    const key = pick("Key");
    if (!key) continue;
    out.push({
      key,
      size: Number(pick("Size") || 0),
      lastModified: pick("LastModified"),
      etag: (pick("ETag") || "").replace(/^"|"$/g, ""),
    });
  }
  return out;
}

// Lists every object under one job's prefix, following continuation tokens so
// a job with more than 1000 photos doesn't silently truncate.
export async function listJobObjects(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  return await listByPrefix(jobPrefix(jobId), timeoutMs);
}

// Lists every object under an arbitrary prefix, following continuation tokens so
// a prefix with more than 1000 objects cannot silently truncate.
export async function listByPrefix(prefix, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const c = config();
  const client = await getClient();

  const all = [];
  let token = null;
  do {
    const url = new URL(`${c.endpoint}/${c.bucket}`);
    url.searchParams.set("list-type", "2");
    url.searchParams.set("prefix", prefix);
    url.searchParams.set("max-keys", "1000");
    if (token) url.searchParams.set("continuation-token", token);

    const res = await withTimeout(
      (signal) => client.fetch(url.toString(), { method: "GET", signal }),
      timeoutMs
    );
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      // 403 here is nearly always the token lacking this bucket, or the wrong
      // account id in the endpoint. Both look like "no photos" if swallowed.
      throw new R2Error(`R2 list failed: HTTP ${res.status} ${body.slice(0, 200)}`, "HTTP_" + res.status);
    }
    const xml = await res.text();
    all.push(...parseListXml(xml));

    const t = /<NextContinuationToken>([\s\S]*?)<\/NextContinuationToken>/.exec(xml);
    token = /<IsTruncated>true<\/IsTruncated>/.test(xml) && t ? t[1] : null;
  } while (token);

  return all;
}

// The gallery view: full-size originals only, each paired with its thumbnail
// if one was uploaded, newest first, every URL pre-signed and ready for an
// <img> tag.
async function buildPhotoList(jobId, objects, keep, decorate, timeoutMs) {
  const thumbs = new Set(objects.filter(o => isThumbKey(o.key)).map(o => o.key));
  const originals = objects
    .filter(o => !isThumbKey(o.key) && keep(o.key))
    .sort((a, b) => new Date(b.lastModified || 0) - new Date(a.lastModified || 0));

  return await Promise.all(originals.map(async (o) => {
    const tKey = thumbKeyFor(o.key);
    return {
      key: o.key,
      name: o.key.slice(o.key.lastIndexOf("/") + 1),
      size: o.size,
      uploadedAt: o.lastModified,
      url: await presignGet(o.key),
      // No thumb means an older or backfilled upload — the client falls back to
      // the full image rather than rendering a broken tile.
      thumbUrl: thumbs.has(tKey) ? await presignGet(tKey) : null,
      ...decorate(o),
    };
  }));
}

// The gallery. The current bin is a separate top-level prefix so it never
// appears here at all; the LEGACY nested bin still has to be excluded by hand,
// or photos deleted before 2026-08-03 would reappear in their old album.
// _docs is excluded too — PDFs would render as broken image tiles.
export async function listJobPhotos(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listJobObjects(jobId, timeoutMs);
  return await buildPhotoList(
    jobId, objects,
    (key) => !isLegacyDeletedKey(jobId, key) && !isDocKey(jobId, key),
    (o) => ({ album: albumFromKey(jobId, o.key) }),
    timeoutMs
  );
}

// Generated documents for a job, newest first. No thumbnails — these are PDFs,
// and the browser's own viewer opens them from the signed URL.
export async function listJobDocs(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listJobObjects(jobId, timeoutMs);
  const docs = objects
    .filter(o => isDocKey(jobId, o.key))
    .sort((a, b) => new Date(b.lastModified || 0) - new Date(a.lastModified || 0));

  return await Promise.all(docs.map(async (o) => ({
    key: o.key,
    name: o.key.slice(o.key.lastIndexOf("/") + 1),
    size: o.size,
    uploadedAt: o.lastModified,
    url: await presignGet(o.key),
  })));
}

// The recycle bin, newest deletion first. `deletedFrom` is the album it will
// go back to on restore; `deletedAt` is when it was binned, which is what the
// 30-day expiry is measured from.
// Reads BOTH bin locations: the current top-level one and the legacy nested
// one, so nothing deleted before the move becomes stranded and unrecoverable.
export async function listDeletedJobPhotos(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const [binned, inJob] = await Promise.all([
    listByPrefix(DELETED_ROOT + jobPrefix(jobId), timeoutMs),
    listJobObjects(jobId, timeoutMs),
  ]);
  const objects = binned.concat(inJob.filter(o => isLegacyDeletedKey(jobId, o.key)));

  return await buildPhotoList(
    jobId, objects,
    () => true,   // everything gathered above is already binned
    (o) => ({ deletedFrom: deletedFromAlbum(jobId, o.key), deletedAt: o.lastModified }),
    timeoutMs
  );
}

// ── Mutation ───────────────────────────────────────────────────────────────

// Every mutating helper takes jobId and refuses any key outside that job's
// prefix. The client sends keys back to us (it got them from listJobPhotos), so
// without this a signed-in user could delete or move any object in the bucket
// by editing one string.
function assertKeyInJob(jobId, key) {
  const k = String(key || "");
  // Live photos sit under jobs/<id>/; binned ones under _deleted/jobs/<id>/ (and
  // legacy ones under jobs/<id>/_deleted/). All three are this job and nothing
  // else is.
  const owned = k.startsWith(jobPrefix(jobId)) || k.startsWith(DELETED_ROOT + jobPrefix(jobId));
  if (!owned || k.includes("..")) {
    throw new R2Error("That photo does not belong to this job", "KEY_OUTSIDE_JOB");
  }
  return k;
}

async function deleteObject(key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const c = config();
  const client = await getClient();
  const res = await withTimeout(
    (signal) => client.fetch(`${c.endpoint}/${c.bucket}/${encodeURI(key)}`, { method: "DELETE", signal }),
    timeoutMs
  );
  // S3 delete is idempotent — 204 for both "deleted" and "was never there".
  if (!res.ok && res.status !== 204 && res.status !== 404) {
    throw new R2Error(`R2 delete failed: HTTP ${res.status}`, "HTTP_" + res.status);
  }
}

async function copyObject(srcKey, destKey, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const c = config();
  const client = await getClient();
  const res = await withTimeout(
    (signal) => client.fetch(`${c.endpoint}/${c.bucket}/${encodeURI(destKey)}`, {
      method: "PUT",
      headers: { "x-amz-copy-source": `/${c.bucket}/${encodeURI(srcKey)}` },
      signal,
    }),
    timeoutMs
  );
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new R2Error(`R2 copy failed: HTTP ${res.status} ${body.slice(0, 200)}`, "HTTP_" + res.status);
  }
}

// Moves a photo AND its thumbnail from one key to another. R2 has no rename,
// so this is copy-then-delete.
//
// The order matters: if the delete fails we are left with a harmless duplicate,
// whereas delete-then-copy would lose the photo outright. A duplicate is
// visible and re-movable; a lost jobsite photo is not recoverable.
async function moveObjectPair(srcKey, destKey, timeoutMs) {
  if (destKey === srcKey) return { key: srcKey, moved: false };

  await copyObject(srcKey, destKey, timeoutMs);
  // A missing thumb must not abort the move - the gallery falls back to the
  // full image, and backfilled photos have no thumb at all.
  try { await copyObject(thumbKeyFor(srcKey), thumbKeyFor(destKey), timeoutMs); } catch { /* no thumb */ }

  await deleteObject(srcKey, timeoutMs);
  try { await deleteObject(thumbKeyFor(srcKey), timeoutMs); } catch { /* already gone */ }

  return { key: destKey, moved: true };
}

// Moves a photo into another album.
export async function moveJobPhoto(jobId, key, album, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInJob(jobId, key);
  const filename = k.slice(k.lastIndexOf('/') + 1);
  return await moveObjectPair(k, jobPrefix(jobId) + albumSegment(album) + filename, timeoutMs);
}

// -- Recycle bin -----------------------------------------------------------
// Deleting moves photos here instead of erasing them. R2 has no versioning, so
// without this a mis-tap on a 40-photo selection is unrecoverable - and the
// app's own Delete button is the most likely way these photos ever get lost.
//
// The bin is a TOP-LEVEL prefix holding the photo's original key verbatim:
//   jobs/<id>/Gym/x.jpg   ->   _deleted/jobs/<id>/Gym/x.jpg
//
// Two reasons it lives at the top rather than nested inside each job:
//
//  1. R2 lifecycle rules match a LITERAL prefix, no wildcards. With the bin
//     nested at jobs/<id>/_deleted/ there is no single prefix that matches
//     every job's bin - it would need one rule per job, forever. One rule on
//     '_deleted/' now expires the lot, and expenses/ is excluded by
//     construction rather than by remembering to exclude it.
//  2. Keeping the original key verbatim means restore is just stripping the
//     prefix, and the album survives with no marker segment to invent.
export const DELETED_ROOT = '_deleted/';

// Where a live key goes when binned, and where a binned key came from.
export function deletedKeyFor(key)     { return DELETED_ROOT + String(key); }
export function restoredKeyFor(key)    { return String(key).slice(DELETED_ROOT.length); }

export function isDeletedKey(jobId, key) {
  return String(key).startsWith(DELETED_ROOT + jobPrefix(jobId));
}

// LEGACY bin location, written before 2026-08-03. Still readable, restorable
// and purgeable so nothing already deleted becomes stranded - but nothing new
// is written here, and the lifecycle rule will NOT expire it. Purge these from
// the UI when convenient; then this can go.
export function isLegacyDeletedKey(jobId, key) {
  return String(key).startsWith(jobPrefix(jobId) + '_deleted/');
}

// ── Job documents ──────────────────────────────────────────────────────────
// Generated PDFs that belong to a job — currently the inventory app's materials
// list, which until now only ever landed in one person's Downloads folder.
//
// They get their own segment because listJobPhotos returns EVERY non-thumb
// object under the job prefix; a PDF dropped in among the photos would render
// as a broken image tile in the gallery.
export const DOCS_SEGMENT = "_docs";

export function jobDocsPrefix(jobId) {
  return `${jobPrefix(jobId)}${DOCS_SEGMENT}/`;
}

export function isDocKey(jobId, key) {
  return String(key).startsWith(jobDocsPrefix(jobId));
}

// The album a binned photo goes back to: '' when it was loose, else the name.
export function deletedFromAlbum(jobId, key) {
  const k = String(key);
  if (isLegacyDeletedKey(jobId, k)) {
    // Legacy layout: jobs/<id>/_deleted/<album|_none>/<file>
    const rel = k.slice((jobPrefix(jobId) + '_deleted/').length);
    const i = rel.indexOf('/');
    if (i < 0) return '';
    const seg = rel.slice(0, i);
    if (seg === '_none') return '';
    try { return decodeURIComponent(seg); } catch { return seg; }
  }
  // Current layout: the original key is preserved verbatim under the bin root.
  return albumFromKey(jobId, restoredKeyFor(k)) || '';
}

// Soft delete: out of the gallery, into the bin, still recoverable.
export async function softDeleteJobPhoto(jobId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInJob(jobId, key);
  if (isDeletedKey(jobId, k) || isLegacyDeletedKey(jobId, k)) return { key: k, moved: false };
  return await moveObjectPair(k, deletedKeyFor(k), timeoutMs);
}

// Puts a binned photo back in the album it came from.
export async function restoreJobPhoto(jobId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInJob(jobId, key);
  if (isDeletedKey(jobId, k)) return await moveObjectPair(k, restoredKeyFor(k), timeoutMs);
  if (isLegacyDeletedKey(jobId, k)) {
    // Rebuild the live key from the remembered album, then restore into the
    // CURRENT layout so legacy entries drain rather than round-trip.
    const filename = k.slice(k.lastIndexOf('/') + 1);
    const album = deletedFromAlbum(jobId, k);
    return await moveObjectPair(k, jobPrefix(jobId) + albumSegment(album) + filename, timeoutMs);
  }
  return { key: k, moved: false };   // not in the bin
}

// Permanent, no undo. Deliberately refuses anything not already in the bin, so
// 'empty the bin' can never be pointed at live photos by a bad key.
export async function purgeJobPhoto(jobId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInJob(jobId, key);
  if (!isDeletedKey(jobId, k) && !isLegacyDeletedKey(jobId, k)) {
    throw new R2Error('Only photos already in Recently deleted can be permanently removed', 'NOT_DELETED');
  }
  await deleteObject(k, timeoutMs);
  await deleteObject(thumbKeyFor(k), timeoutMs);   // tolerates a missing thumb
}

// ── Expense receipts ───────────────────────────────────────────────────────
// Same folder-is-the-record idea as photos: key by the owning expense's record
// id and list by prefix. No table, no schema, nothing new to port to Neon —
// which matters with Airtable mid-retirement.
//
// Receipts arrive TWO ways and they are not the same kind of file:
//   phone photo  -> JPEG, compressed client-side, has a _thumb twin
//   ScanSnap     -> PDF, often multi-page, uploaded UNTOUCHED, no thumbnail
// A multi-page scan is ONE receipt; page count is irrelevant to the model.
//
// Receipts are deliberately EXCLUDED from the recycle bin's lifecycle rule —
// they are financial records that may be wanted years later. The bin lives at
// the top-level `_deleted/` prefix, so `expenses/` is excluded by construction.
export function expensePrefix(expenseId) {
  return `expenses/${String(expenseId)}/`;
}

// R2's list response carries no Content-Type, so derive it from the extension.
// The client branches on this: images open in the lightbox, PDFs in the
// browser's own viewer.
export function contentTypeForKey(key) {
  const ext = String(key).toLowerCase().split(".").pop();
  if (ext === "pdf")  return "application/pdf";
  if (ext === "png")  return "image/png";
  if (ext === "webp") return "image/webp";
  return "image/jpeg";
}

export async function listExpenseReceipts(expenseId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listByPrefix(expensePrefix(expenseId), timeoutMs);
  const thumbs = new Set(objects.filter(o => isThumbKey(o.key)).map(o => o.key));

  const originals = objects
    .filter(o => !isThumbKey(o.key))
    .sort((a, b) => new Date(a.lastModified || 0) - new Date(b.lastModified || 0));

  return await Promise.all(originals.map(async (o) => {
    const tKey = thumbKeyFor(o.key);
    const contentType = contentTypeForKey(o.key);
    const isPdf = contentType === "application/pdf";
    return {
      key: o.key,
      name: o.key.slice(o.key.lastIndexOf("/") + 1),
      contentType,
      isPdf,
      size: o.size,
      uploadedAt: o.lastModified,
      url: await presignGet(o.key),
      // PDFs never have one — generating a thumbnail from a PDF needs pdf.js,
      // which is a heavy dependency for a tile. The client shows a document
      // icon instead of falling back to rendering the file as an image.
      thumbUrl: (!isPdf && thumbs.has(tKey)) ? await presignGet(tKey) : null,
    };
  }));
}

// Decides how one uploaded receipt is handled, from its declared content type.
// Pure on purpose: this is the rule that keeps a ScanSnap PDF out of the image
// path, and it must be testable without a signer or a network.
//
// Anything unrecognised falls back to JPEG rather than being stored as an
// unknown blob — the client only ever offers images and PDFs.
export function receiptFileKind(contentType) {
  const raw = String(contentType || "");
  const ct = /^image\/(jpeg|png|webp)$/.test(raw) ? raw
           : raw === "application/pdf" ? "application/pdf"
           : "image/jpeg";
  const isPdf = ct === "application/pdf";
  return {
    contentType: ct,
    isPdf,
    ext: isPdf ? "pdf" : ct === "image/png" ? "png" : ct === "image/webp" ? "webp" : "jpg",
    // A PDF gets no thumbnail: generating one needs pdf.js, too heavy for a
    // tile. The client shows a document icon instead.
    wantsThumb: !isPdf,
  };
}

// Guards a receipt key the client hands back, the same way assertKeyInJob does
// for photos: without it a signed-in user could reach another expense's
// receipts by editing one string.
export function assertKeyInExpense(expenseId, key) {
  const k = String(key || "");
  if (!k.startsWith(expensePrefix(expenseId)) || k.includes("..")) {
    throw new R2Error("That receipt does not belong to this expense", "KEY_OUTSIDE_EXPENSE");
  }
  return k;
}

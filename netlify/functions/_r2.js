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
async function presign(method, key, { ttl, contentType, params } = {}) {
  const c = config();
  const client = await getClient();
  const url = new URL(`${c.endpoint}/${c.bucket}/${encodeURI(key)}`);
  url.searchParams.set("X-Amz-Expires", String(ttl));
  // Response-header overrides must go on BEFORE signing — they are part of the
  // canonical query string, so appending one afterwards invalidates the
  // signature and R2 answers 403.
  for (const [k, v] of Object.entries(params || {})) url.searchParams.set(k, v);

  const signed = await client.sign(
    new Request(url, { method, ...(contentType ? { headers: { "Content-Type": contentType } } : {}) }),
    { aws: { signQuery: true } }
  );
  return signed.url;
}

export function presignGet(key, ttl = VIEW_URL_TTL_SECONDS) {
  return presign("GET", key, { ttl });
}

// A URL that DOWNLOADS rather than previews.
//
// This is the whole mechanism behind "open it in my PDF app": no web page can
// launch a named app, but a downloaded file gets handed to the OS, which opens
// it with whatever the device has registered for PDFs. Chrome and Safari both
// render a PDF inline unless the response says attachment.
//
// It cannot be done from the client — <a download> is IGNORED cross-origin, and
// R2 is a different origin — so the instruction has to be baked into the signed
// URL here.
//
// Second benefit, and the bigger one on a jobsite: the downloaded copy is
// OFFLINE. This link dies after VIEW_URL_TTL_SECONDS; the file on the phone
// does not. Pull the drawings on wifi, read them in a basement with no signal.
export function presignGetDownload(key, filename, ttl = VIEW_URL_TTL_SECONDS) {
  return presign("GET", key, {
    ttl,
    params: { "response-content-disposition": attachmentDisposition(filename) },
  });
}

// Split out as a pure function so it is testable: presign itself needs the
// aws4fetch signer, which is lazy-imported and deliberately absent from the
// offline test suite. The escaping is the part that can be got wrong.
//
// sanitizePrintName has already restricted a print's name, but this value ends
// up in an HTTP header — a stray quote truncates the filename, and a newline
// would be header injection if anything ever reaches here unsanitized.
export function attachmentDisposition(filename) {
  const safe = String(filename || "download")
    .replace(/[^A-Za-z0-9 ._()-]/g, "_")
    .slice(0, 120) || "download";
  return `attachment; filename="${safe}"`;
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
// _docs and _prints are excluded too — PDFs would render as broken image
// tiles, and albumFromKey would turn each segment into a phantom album.
export async function listJobPhotos(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listJobObjects(jobId, timeoutMs);
  return await buildPhotoList(
    jobId, objects,
    (key) => !isLegacyDeletedKey(jobId, key) && !isDocKey(jobId, key) && !isPrintKey(jobId, key),
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

// Moves ONE object. Copy-then-delete for the same reason as the pair below: a
// failed delete leaves a harmless duplicate, a failed copy after a delete loses
// the file. Used by prints, which have no thumbnail to carry along — running
// them through moveObjectPair would just add two round trips guaranteed to 404.
async function moveObject(srcKey, destKey, timeoutMs) {
  if (destKey === srcKey) return { key: srcKey, moved: false };
  await copyObject(srcKey, destKey, timeoutMs);
  await deleteObject(srcKey, timeoutMs);
  return { key: destKey, moved: true };
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

// ── Job prints (docs/PLAN-job-prints.md) ───────────────────────────────────
// The drawings a crew needs on site: prints, specs, marked-up sheets. They get
// their own segment for two reasons, and the second is the important one.
//
//  1. Same as _docs — listJobPhotos returns EVERY non-thumb object under the
//     job prefix, so a 30 MB PDF sitting among the photos renders as a broken
//     image tile, and albumFromKey would invent an album named "_prints".
//  2. The segment IS the permission. _docs is admin/office because it itemises
//     unit costs; _prints is readable by every signed-in role, which is the
//     entire point of the feature. A file's LOCATION is its visibility, so
//     there is no per-file flag anyone can tick wrong. Do not replace this
//     with a "shared" checkbox on one combined list: one mis-tick would put
//     job costing in front of the whole crew, and nothing about the file would
//     show that it was wrong.
export const PRINTS_SEGMENT = "_prints";

export function jobPrintsPrefix(jobId) {
  return `${jobPrefix(jobId)}${PRINTS_SEGMENT}/`;
}

export function isPrintKey(jobId, key) {
  return String(key).startsWith(jobPrintsPrefix(jobId));
}

// Prints keep their ORIGINAL filename, unlike photos, which get a
// server-generated one. "E-1 Rev B.pdf" is the revision system — renaming it
// to 20260805-01-a3f9.pdf would throw away the only thing telling a crew which
// sheet they are looking at.
//
// That means a client-supplied string reaches the key builder, so it is
// sanitized at the boundary exactly like an album name (sanitizeAlbum):
// slashes forge extra path segments and ".." climbs out of the job's prefix.
// The whitelist is tighter than it looks necessary because presign() builds
// the URL with encodeURI, which leaves '#', '?' and '&' intact — a print named
// "Panel #3.pdf" would sign one URL and address a different object.
export const MAX_PRINT_NAME_LEN = 120;

export function sanitizePrintName(name) {
  const raw = String(name ?? "").replace(/[/\\]/g, " ");
  const cleaned = raw
    // Runs of dots collapse to one. Without the slashes there is no traversal
    // left to do, but assertKeyInPrints refuses ANY key containing '..' — so a
    // print that kept them would upload happily and then be impossible to
    // delete, which is the one operation that reclaims storage.
    .replace(/\.{2,}/g, ".")
    .replace(/[^A-Za-z0-9 ._()-]/g, "_")   // keeps "E-1 Rev B (2).pdf" intact
    .replace(/\s+/g, " ")
    .replace(/^[.\s]+/, "")                 // no leading dot: ".." and hidden keys
    .trim();
  if (!cleaned || cleaned === "." || cleaned === "..") return null;
  // Trim from the FRONT so the extension survives — a 200-char name truncated
  // from the back would lose ".pdf" and open as a download of unknown type.
  return cleaned.length > MAX_PRINT_NAME_LEN
    ? cleaned.slice(cleaned.length - MAX_PRINT_NAME_LEN)
    : cleaned;
}

// Prints are binned NESTED inside their own segment, not in the top-level
// `_deleted/` root the photos use:
//
//   jobs/<id>/_prints/E-1.pdf            live
//   jobs/<id>/_prints/_deleted/E-1.pdf   binned, still recoverable
//
// Two reasons, both learned from the code above rather than guessed:
//  - listDeletedJobPhotos keeps EVERYTHING under `_deleted/jobs/<id>/`
//    (`keep = () => true`), so a print binned there would show up in the photo
//    recycle bin as a broken tile and could be "restored" into an album.
//  - the lifecycle rule on `_deleted/` expires photos after 30 days. A print
//    should not silently evaporate; it leaves when someone says so. Same
//    reasoning as receipts, which are nested for the same reason.
const PRINT_DELETED_SEGMENT = "_deleted/";

export function isPrintDeletedKey(jobId, key) {
  return String(key).startsWith(jobPrintsPrefix(jobId) + PRINT_DELETED_SEGMENT);
}

// Mutating helpers refuse any key outside this job's PRINTS prefix — stricter
// than assertKeyInJob, which would happily accept a photo key. A print delete
// must never be able to point at the gallery.
function assertKeyInPrints(jobId, key) {
  const k = String(key || "");
  if (!isPrintKey(jobId, k) || k.includes("..")) {
    throw new R2Error("That print does not belong to this job", "KEY_OUTSIDE_JOB");
  }
  return k;
}

function printListEntry(o) {
  const name = o.key.slice(o.key.lastIndexOf("/") + 1);
  return {
    key: o.key,
    name,
    size: o.size,
    isPdf: /\.pdf$/i.test(name),
  };
}

async function buildPrintList(objects, stamp) {
  const sorted = objects.sort((a, b) => new Date(b.lastModified || 0) - new Date(a.lastModified || 0));
  return await Promise.all(sorted.map(async (o) => {
    const entry = printListEntry(o);
    return {
      ...entry,
      [stamp]: o.lastModified,
      // Two URLs for the same object: one previews, one downloads. Signing is
      // local HMAC with no network call, so the second costs nothing worth
      // saving, and having both in hand is what lets the client offer
      // "open" and "download for your PDF app" without another round trip.
      url: await presignGet(o.key),
      downloadUrl: await presignGetDownload(o.key, entry.name),
    };
  }));
}

// One job's prints, newest first, every URL pre-signed so the browser opens the
// PDF straight from Cloudflare. No thumbnails: these are drawings, and the
// browser's own viewer renders them better than any tile we could make.
export async function listJobPrints(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listByPrefix(jobPrintsPrefix(jobId), timeoutMs);
  return await buildPrintList(objects.filter(o => !isPrintDeletedKey(jobId, o.key)), "uploadedAt");
}

export async function listDeletedJobPrints(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listByPrefix(jobPrintsPrefix(jobId) + PRINT_DELETED_SEGMENT, timeoutMs);
  return await buildPrintList(objects, "deletedAt");
}

// Soft delete: out of the list, into the nested bin, still recoverable. Uses
// the single-object move — a print has no thumbnail to carry with it.
export async function softDeleteJobPrint(jobId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInPrints(jobId, key);
  if (isPrintDeletedKey(jobId, k)) return { key: k, moved: false };
  const filename = k.slice(k.lastIndexOf("/") + 1);
  return await moveObject(k, jobPrintsPrefix(jobId) + PRINT_DELETED_SEGMENT + filename, timeoutMs);
}

export async function restoreJobPrint(jobId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInPrints(jobId, key);
  if (!isPrintDeletedKey(jobId, k)) return { key: k, moved: false };
  const filename = k.slice(k.lastIndexOf("/") + 1);
  return await moveObject(k, jobPrintsPrefix(jobId) + filename, timeoutMs);
}

// Permanent, no undo — and the only thing that actually reclaims storage, which
// is why it exists. Refuses anything not already in the bin, so "delete
// forever" can never be pointed at a live print by a bad key.
export async function purgeJobPrint(jobId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInPrints(jobId, key);
  if (!isPrintDeletedKey(jobId, k)) {
    throw new R2Error("Only prints already deleted can be permanently removed", "NOT_DELETED");
  }
  await deleteObject(k, timeoutMs);
  return { key: k, purged: true };
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

  // Deleted receipts live in a nested _deleted/ sub-prefix, so they come back
  // in this list and must be filtered out or a deleted receipt would still show.
  const originals = objects
    .filter(o => !isThumbKey(o.key) && !isDeletedReceiptKey(expenseId, o.key))
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

// A cheap per-expense summary for the approval list: how many receipts, and a
// thumbnail for the first one. Not the full listing — the Expenses table only
// needs "is there one, and roughly what does it look like".
//
// One list call PER EXPENSE, run a few at a time. Deliberately not one big
// list over `expenses/`: that would scale with every receipt ever stored,
// while this scales with the expenses on the job being viewed — naturally
// bounded, and it stays bounded as the business grows.
export async function summarizeExpenseReceipts(expenseIds, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const ids = [...new Set((expenseIds || []).filter(Boolean))];
  const out = {};
  let next = 0;

  async function worker() {
    for (;;) {
      const i = next++;
      if (i >= ids.length) return;
      const id = ids[i];
      try {
        const objects = await listByPrefix(expensePrefix(id), timeoutMs);
        const originals = objects
          .filter(o => !isThumbKey(o.key) && !isDeletedReceiptKey(id, o.key))
          .sort((a, b) => new Date(a.lastModified || 0) - new Date(b.lastModified || 0));
        if (!originals.length) { out[id] = { count: 0, thumbUrl: null, isPdf: false }; continue; }

        const first = originals[0];
        const isPdf = contentTypeForKey(first.key) === "application/pdf";
        const tKey  = thumbKeyFor(first.key);
        const hasThumb = !isPdf && objects.some(o => o.key === tKey);
        out[id] = {
          count: originals.length,
          isPdf,
          // A PDF has no thumbnail by design; the client shows a document icon.
          thumbUrl: hasThumb ? await presignGet(tKey) : null,
        };
      } catch {
        // One expense failing must not blank the whole column — report it as
        // unknown rather than as "no receipt", which would be a lie.
        out[id] = { count: null, thumbUrl: null, isPdf: false };
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(6, ids.length) }, worker));
  return out;
}

// ── Deleted receipts ───────────────────────────────────────────────────────
// Deliberately NOT the top-level `_deleted/` bin the photos use. That prefix is
// covered by a 30-day lifecycle rule, and receipts are financial records the
// owner decided are exempt from auto-purge (docs/PLAN-expense-receipts.md §9).
//
// Nesting the bin INSIDE the expense keeps it under `expenses/`, which the rule
// never matches — so a deleted receipt persists until someone removes it on
// purpose, and the nightly backup has it either way.
//
//   expenses/<id>/20260803-01-ab.jpg           live
//   expenses/<id>/_deleted/20260803-01-ab.jpg  deleted, kept indefinitely
const RECEIPT_DELETED_SEGMENT = "_deleted/";

export function isDeletedReceiptKey(expenseId, key) {
  return String(key).startsWith(expensePrefix(expenseId) + RECEIPT_DELETED_SEGMENT);
}

export async function softDeleteExpenseReceipt(expenseId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInExpense(expenseId, key);
  if (isDeletedReceiptKey(expenseId, k)) return { key: k, moved: false };
  const filename = k.slice(k.lastIndexOf("/") + 1);
  return await moveObjectPair(k, expensePrefix(expenseId) + RECEIPT_DELETED_SEGMENT + filename, timeoutMs);
}

export async function restoreExpenseReceipt(expenseId, key, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const k = assertKeyInExpense(expenseId, key);
  if (!isDeletedReceiptKey(expenseId, k)) return { key: k, moved: false };
  const filename = k.slice(k.lastIndexOf("/") + 1);
  return await moveObjectPair(k, expensePrefix(expenseId) + filename, timeoutMs);
}

export async function listDeletedExpenseReceipts(expenseId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listByPrefix(expensePrefix(expenseId) + RECEIPT_DELETED_SEGMENT, timeoutMs);
  const originals = objects
    .filter(o => !isThumbKey(o.key))
    .sort((a, b) => new Date(b.lastModified || 0) - new Date(a.lastModified || 0));

  return await Promise.all(originals.map(async (o) => {
    const contentType = contentTypeForKey(o.key);
    const isPdf = contentType === "application/pdf";
    const tKey = thumbKeyFor(o.key);
    return {
      key: o.key,
      name: o.key.slice(o.key.lastIndexOf("/") + 1),
      contentType, isPdf,
      size: o.size,
      deletedAt: o.lastModified,
      url: await presignGet(o.key),
      thumbUrl: (!isPdf && objects.some(x => x.key === tKey)) ? await presignGet(tKey) : null,
    };
  }));
}

// ── Scissor lift photos (roadmap Step 4b) ──────────────────────────────────
// Same folder-is-the-record idea as job photos and expense receipts: the prefix
// IS the ownership, so there is no table of photo keys to keep in step with the
// files. Existing top-level prefixes are jobs/, expenses/ and _deleted/, so
// lifts/ collides with nothing.
//
// The files got here from Airtable because ATTACHMENT URLS EXPIRE (~2 h) — see
// db/schema/009_scissor_lifts.sql. Storing an Airtable URL would have broken
// every lift photo the same afternoon.
// Lifts and fleet vehicles are the same problem twice — a record with one
// picture, sold/retired as a unit — so they share an implementation and differ
// only in the top-level segment.
const equipPrefix = (kind, id) => `${kind}/${String(id)}/`;

// Same guard shape as assertKeyInJob: a key belonging to a different record, or
// one climbing out via "..", is refused rather than acted on. The client never
// picks raw keys, but the delete endpoint takes one, so it is checked here.
function assertKeyInEquip(kind, id, key) {
  const k = String(key || "");
  if (!k.startsWith(equipPrefix(kind, id)) || k.includes("..")) {
    throw new R2Error(`That photo does not belong to this ${kind === "lifts" ? "lift" : "vehicle"}`,
      "KEY_OUTSIDE_RECORD");
  }
  return k;
}

async function listEquipPhotos(kind, id, timeoutMs) {
  const objects = await listByPrefix(equipPrefix(kind, id), timeoutMs);
  return await Promise.all(
    objects
      .filter(o => !isThumbKey(o.key))
      .sort((a, b) => new Date(a.lastModified || 0) - new Date(b.lastModified || 0))
      .map(async o => ({
        key: o.key,
        name: o.key.slice(o.key.lastIndexOf("/") + 1),
        size: o.size,
        url: await presignGet(o.key),
      })));
}

// NO RECYCLE BIN, unlike job photos. Selling a lift or a truck removes
// everything, photos included — a bin would only be a place for disposed
// equipment to sit and cost money. Deliberate divergence, not an oversight.
async function deleteEquipPhoto(kind, id, key, timeoutMs) {
  const k = assertKeyInEquip(kind, id, key);
  await deleteObject(k, timeoutMs);
  await deleteObject(thumbKeyFor(k), timeoutMs);   // tolerates a missing thumb
}

// Everything under one record. Used when it is retired — nothing else will ever
// clean these up, since the row that pointed at them is gone.
async function deleteAllEquipPhotos(kind, id, timeoutMs) {
  const objects = await listByPrefix(equipPrefix(kind, id), timeoutMs);
  for (const o of objects) await deleteObject(o.key, timeoutMs);
  return objects.length;
}

export const liftPrefix  = (id) => equipPrefix("lifts", id);
export const fleetPrefix = (id) => equipPrefix("fleet", id);
export const listLiftPhotos  = (id, t = DEFAULT_TIMEOUT_MS) => listEquipPhotos("lifts", id, t);
export const listFleetPhotos = (id, t = DEFAULT_TIMEOUT_MS) => listEquipPhotos("fleet", id, t);
export const deleteLiftPhoto  = (id, key, t = DEFAULT_TIMEOUT_MS) => deleteEquipPhoto("lifts", id, key, t);
export const deleteFleetPhoto = (id, key, t = DEFAULT_TIMEOUT_MS) => deleteEquipPhoto("fleet", id, key, t);
export const deleteAllLiftPhotos  = (id, t = DEFAULT_TIMEOUT_MS) => deleteAllEquipPhotos("lifts", id, t);
export const deleteAllFleetPhotos = (id, t = DEFAULT_TIMEOUT_MS) => deleteAllEquipPhotos("fleet", id, t);

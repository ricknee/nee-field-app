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
  const c = config();
  const client = await getClient();
  const prefix = jobPrefix(jobId);

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
export async function listJobPhotos(jobId, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const objects = await listJobObjects(jobId, timeoutMs);
  const thumbs = new Set(objects.filter(o => isThumbKey(o.key)).map(o => o.key));

  const originals = objects
    .filter(o => !isThumbKey(o.key))
    .sort((a, b) => new Date(b.lastModified || 0) - new Date(a.lastModified || 0));

  return await Promise.all(originals.map(async (o) => {
    const tKey = thumbKeyFor(o.key);
    const hasThumb = thumbs.has(tKey);
    return {
      key: o.key,
      name: o.key.slice(o.key.lastIndexOf("/") + 1),
      size: o.size,
      uploadedAt: o.lastModified,
      url: await presignGet(o.key),
      // No thumb means an older or backfilled upload — the client falls back to
      // the full image rather than rendering a broken tile.
      thumbUrl: hasThumb ? await presignGet(tKey) : null,
    };
  }));
}

// Shared pCloud client — jobsite photo viewing/upload.
// See docs/PLAN-job-photos.md.
// ---------------------------------------------------------------------------
// CONTRACT: pCloud is OPTIONAL infrastructure. Like _neon.js and unlike
// _auth.js, nothing here belongs in ensureEnv(). A job view must still render
// when PCLOUD_ACCESS_TOKEN is unset or pCloud is down — the Photos tab simply
// reports itself unavailable. Never let a photo failure 500 a job read.
//
// Callers get thrown PcloudError objects (with .code) rather than nulls,
// because "no photos in this folder" and "the token is dead" need to be told
// apart at the call site — the _neon.js lesson: failing soft must not mean
// saying nothing.
// ---------------------------------------------------------------------------
// SECURITY: PCLOUD_ACCESS_TOKEN is a full read/write credential for the entire
// NEE pCloud account. It is server-side only and must NEVER be sent to the
// browser, logged, or embedded in a response. This is the whole reason uploads
// proxy through the function instead of going straight from the phone —
// pCloud has no presigned-upload equivalent to S3/R2.
//
// It is also why every fileid a client asks for must be verified against the
// job's own folder before bytes are served (see assertFileInFolder). Without
// that check, a signed-in viewer could walk the entire company file tree by
// guessing fileids.

const DEFAULT_TIMEOUT_MS = 8000;

// pCloud splits accounts across two regions and the wrong host returns
// "invalid access token" — which reads exactly like a bad credential and
// burns an afternoon. The OAuth response carries the right one:
//   locationid 1 => api.pcloud.com (US), locationid 2 => eapi.pcloud.com (EU).
// NEE's links are my.pcloud.com, which is US, so that is the default.
const API_HOST = () => process.env.PCLOUD_API_HOST || "api.pcloud.com";

export class PcloudError extends Error {
  constructor(message, code) {
    super(message);
    this.name = "PcloudError";
    this.code = code ?? null;
  }
}

export function pcloudEnabled() {
  return !!process.env.PCLOUD_ACCESS_TOKEN;
}

function requireToken() {
  const t = process.env.PCLOUD_ACCESS_TOKEN;
  if (!t) throw new PcloudError("pCloud is not configured", "NOT_CONFIGURED");
  return t;
}

function buildUrl(method, params = {}) {
  const url = new URL(`https://${API_HOST()}/${method}`);
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
  }
  return url;
}

async function withTimeout(fn, timeoutMs) {
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), timeoutMs);
  try {
    return await fn(ac.signal);
  } catch (e) {
    if (e?.name === "AbortError") throw new PcloudError("pCloud timed out", "TIMEOUT");
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

// JSON call. pCloud always answers HTTP 200 and signals failure in the body's
// `result` field, so checking res.ok alone silently treats errors as success.
async function pcApi(method, params = {}, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const token = requireToken();
  const url = buildUrl(method, { ...params, access_token: token });

  const res = await withTimeout(
    (signal) => fetch(url, { signal, headers: { Accept: "application/json" } }),
    timeoutMs
  );
  if (!res.ok) throw new PcloudError(`pCloud HTTP ${res.status} on ${method}`, "HTTP_" + res.status);

  const data = await res.json();
  if (data.result !== 0) {
    // 1000/2000/2094 = auth problems, 2005 = directory does not exist,
    // 2009 = file not found, 1014 = no thumb for this file type.
    throw new PcloudError(data.error || `pCloud error ${data.result} on ${method}`, data.result);
  }
  return data;
}

// Binary call (getthumb). Returns raw bytes, not a link.
async function pcBinary(method, params = {}, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const token = requireToken();
  const url = buildUrl(method, { ...params, access_token: token });

  const res = await withTimeout((signal) => fetch(url, { signal }), timeoutMs);
  if (!res.ok) throw new PcloudError(`pCloud HTTP ${res.status} on ${method}`, "HTTP_" + res.status);

  const ct = res.headers.get("content-type") || "";
  // A JSON body from a binary endpoint means pCloud rejected the request
  // (bad token, unsupported file type). Surface it instead of handing the
  // browser an "image" that is actually an error blob.
  if (ct.includes("application/json")) {
    const data = await res.json().catch(() => ({}));
    throw new PcloudError(data.error || `pCloud error on ${method}`, data.result ?? "BAD_BINARY");
  }
  const buf = Buffer.from(await res.arrayBuffer());
  return { buffer: buf, contentType: ct || "application/octet-stream" };
}

// ── Reads ──────────────────────────────────────────────────────────────────

// Lists a folder's immediate contents. Returns [] for an empty folder and
// throws PcloudError 2005 when the folder id is wrong/deleted — the caller
// must tell those apart, since 2005 means the job is misconfigured while []
// just means nobody has taken photos yet.
export async function listFolder(folderid, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const data = await pcApi("listfolder", { folderid, nofiles: 0 }, timeoutMs);
  return data.metadata?.contents || [];
}

// Image entries in a folder, newest first. Non-images (stray PDFs, notes) are
// filtered out — the gallery only knows how to render pictures.
export async function listFolderImages(folderid, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const contents = await listFolder(folderid, timeoutMs);
  return contents
    .filter((e) => !e.isfolder && typeof e.contenttype === "string" && e.contenttype.startsWith("image/"))
    .map((e) => ({
      fileid:   e.fileid,
      name:     e.name,
      size:     e.size ?? null,
      width:    e.width ?? null,
      height:   e.height ?? null,
      created:  e.created ?? null,
      modified: e.modified ?? null,
      // false for formats pCloud can't thumbnail (e.g. raw HEIC) — the client
      // must fall back to the full-size fetch rather than render a broken tile.
      thumb:    !!e.thumb,
      contentType: e.contenttype,
      parentFolderId: e.parentfolderid ?? null,
    }))
    .sort((a, b) => new Date(b.created || 0) - new Date(a.created || 0));
}

// Authorization guard: confirm this fileid really lives in this folder before
// serving its bytes. A signed-in user may only read photos belonging to the
// job they asked for. Cheap because listfolder is one call and the result is
// what the gallery already needed.
export async function assertFileInFolder(fileid, folderid, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const contents = await listFolder(folderid, timeoutMs);
  const hit = contents.find((e) => !e.isfolder && String(e.fileid) === String(fileid));
  if (!hit) throw new PcloudError("File is not in this job's photo folder", "NOT_IN_FOLDER");
  return hit;
}

// Thumbnail bytes. pCloud requires width and height to be 16..2048 (height
// 1024) AND divisible by 4 or 5 — an off-spec size is a hard error, so sizes
// are validated here rather than discovered in production.
export async function getThumbBytes(fileid, size = "320x320", timeoutMs = DEFAULT_TIMEOUT_MS) {
  const m = /^(\d+)x(\d+)$/.exec(String(size));
  if (!m) throw new PcloudError(`Bad thumb size "${size}"`, "BAD_SIZE");
  const w = Number(m[1]), h = Number(m[2]);
  const ok = (n, max) => n >= 16 && n <= max && (n % 4 === 0 || n % 5 === 0);
  if (!ok(w, 2048) || !ok(h, 1024)) throw new PcloudError(`Bad thumb size "${size}"`, "BAD_SIZE");

  return await pcBinary("getthumb", { fileid, size, crop: 0 }, timeoutMs);
}

// Full-size bytes. Two hops: getfilelink hands back hosts + a path, then we
// fetch it. That second fetch MUST stay server-side — pCloud restricts the
// referrer on those links to pcloud.com, so handing one to an <img> tag in
// the browser fails. That restriction is the reason this function exists.
export async function getFileBytes(fileid, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const link = await pcApi("getfilelink", { fileid }, timeoutMs);
  const host = Array.isArray(link.hosts) ? link.hosts[0] : null;
  if (!host || !link.path) throw new PcloudError("pCloud returned no download link", "NO_LINK");

  const res = await withTimeout(
    (signal) => fetch(`https://${host}${link.path}`, { signal }),
    timeoutMs
  );
  if (!res.ok) throw new PcloudError(`pCloud file fetch HTTP ${res.status}`, "HTTP_" + res.status);

  const buf = Buffer.from(await res.arrayBuffer());
  return { buffer: buf, contentType: res.headers.get("content-type") || "image/jpeg" };
}

// ── Writes (Slice 2) ───────────────────────────────────────────────────────

// Uploads one file into a folder BY FOLDER ID.
//
// Never add a path-based variant. The Make scenario this replaces uploaded by
// a path rebuilt from five editable Airtable fields plus the *current* year,
// which is exactly why it died with "[2005] Directory does not exist" on
// 2026-07-01 and breaks every January. The folder id is already on the job
// record; it cannot drift.
//
// renameifexists=1 makes pCloud disambiguate rather than clobber, so a
// filename collision can never destroy an existing photo.
export async function uploadFile({ folderid, filename, bytes, contentType = "image/jpeg" }, timeoutMs = 20000) {
  const token = requireToken();
  if (!folderid) throw new PcloudError("Missing pCloud folder id", "NO_FOLDER");
  if (!filename) throw new PcloudError("Missing filename", "NO_FILENAME");

  const url = buildUrl("uploadfile", {
    folderid,
    access_token: token,
    nopartial: 1,        // don't keep a truncated file if the upload dies mid-flight
    renameifexists: 1,
  });

  const form = new FormData();
  form.append("file", new Blob([bytes], { type: contentType }), filename);

  const res = await withTimeout(
    (signal) => fetch(url, { method: "POST", body: form, signal }),
    timeoutMs
  );
  if (!res.ok) throw new PcloudError(`pCloud upload HTTP ${res.status}`, "HTTP_" + res.status);

  const data = await res.json();
  if (data.result !== 0) throw new PcloudError(data.error || `pCloud upload error ${data.result}`, data.result);

  const meta = Array.isArray(data.metadata) ? data.metadata[0] : data.metadata;
  if (!meta) throw new PcloudError("pCloud accepted the upload but returned no metadata", "NO_METADATA");
  return {
    fileid: meta.fileid,
    name: meta.name,
    size: meta.size ?? null,
    contentType: meta.contenttype || contentType,
    parentFolderId: meta.parentfolderid ?? folderid,
  };
}

// Creates (or returns) a no-login public link to a folder — for sharing a
// job's photos with a customer or inspector who has no app account.
export async function getFolderPublicLink(folderid, { expire } = {}, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const data = await pcApi("getfolderpublink", { folderid, ...(expire ? { expire } : {}) }, timeoutMs);
  return { code: data.code || null, link: data.link || null, linkId: data.linkid ?? null };
}

# Plan: Jobsite photos — in-app viewing + direct upload (pCloud)

**Status:** Approved 2026-07-31. Slice 1 in progress.
**Store decision: pCloud** (R2 considered and rejected — see §3).
**Make.com: not touched.** Scenario 4522457 keeps running until Slice 2 has soaked.

**One-line:** Give every job an in-app photo gallery that reads its existing pCloud photo folder
(no pCloud login for the user), then replace the JotForm upload with a direct app → pCloud upload
that files by **folder ID** instead of a rebuilt path.

---

## 1. What exists today (verified 2026-07-31)

**Button:** `index.html:2136` `#btnAddPhotos` — a plain `<a target="_blank">` whose href comes from
the Airtable Jobs field `Add Photos (Mobile)` (`F.job.addPhotosLink`, airtable.js:147). Wired by
`setActionBtn` (index.html:4233). Sibling `#btnViewPhotos` ← `View pCloud Photos` (airtable.js:148).
Neither touches a Netlify function — they are just links.

The Add link opens JotForm form `260246511955053` ("Jobsite Photo Upload") with `?jobPo=<Job PO>`.
Then **Make scenario 4522457 "Submitt Photos"** (team 6575, hook 2590669, active) runs:

| # | Module | What it does |
|---|---|---|
| 1 | `jotform:watchForSubmissions` | fires on submit |
| 6 | `airtable:ActionSearchRecords` | Jobs, `{Job PO} = "{{1.request.q3_jobPo}}"`, limit 1 |
| 2 | `builtin:BasicFeeder` | iterate `uploadPhotos[]` |
| 5 | `http:DownloadFile` | pull each photo back off jotform.com |
| 4 | `pcloud:uploadFile` | upload **by path string**, `overwrite: false` |

Path is rebuilt from five text fields on every run:

```
/Northeastern Electric Jobs/NEE Jobs/{{formatDate(now;"YYYY")}}/{{Contractor Name (Text)}}/{{Job PO}}/{{Jobsite Files (pCloud)}}/{{pCloud Photo Upload}}
```

Filename is `{{Job Name}} {{Job Type}}.jpg` — the same name for every photo on a job.
Observed cost: ~9–11 Make ops and 4–9 MB transfer per submission; a sample photo was
**2.5 MB at 4000×1848**, uploaded raw.

### The break, with evidence

Execution history shows a hard failure on **2026-07-01T18:00:25Z**, status 3:

> `[2005] Directory does not exist.` — causeModule `pcloud / uploadFile`

Structural, not flaky:

1. **`formatDate(now,"YYYY")` is the *current* year, not the job's.** Any job created in one year
   whose photos are uploaded the next points at a folder that doesn't exist. Breaks every January
   and on every carry-over job.
2. **The path is rebuilt from five human-editable strings.** Rename a contractor, fix a Job PO
   typo, or edit `Jobsite Files (pCloud)` / `pCloud Photo Upload` and it silently stops resolving.
3. **The job record already holds the answer and the scenario ignores it.** Jobs carries
   `pCloud Photo's ID` — a real pCloud **folderid** (e.g. `30344195184`). Uploading by folderid
   cannot hit failures 1 or 2 at all.
4. **Identical filename for every photo**, `overwrite:false` → renames and collisions.
5. **Job lookup is an exact unescaped match** on `Job PO`; a stale prefill or a `"` in the PO
   returns 0 records and the run dies or uploads with empty path segments.
6. **JotForm is an extra hop with its own failure modes** — plan caps, and `http:DownloadFile`
   404s if JotForm purges the upload.

**Viewing today:** `View pCloud Photos` = `https://my.pcloud.com/#/filemanager?folder=30344195184`
— the pCloud web app, so it **requires a pCloud login**, and once in, the user can browse the whole
account.

---

## 2. Target design

### Why viewing ships first (changed from the first draft)

The original draft did upload first. Viewing-first is better here:

- **It works on five years of existing photos immediately.** Every job's folder is already
  populated. Ship viewing and the feature is useful on day one across the whole job history —
  upload-first would only help photos taken after the deploy.
- **It's read-only.** No writes to pCloud, no risk to existing files, nothing to roll back.
- **It proves out the pCloud integration and the token** before anything writes bytes.
- **It fixes the complaint that actually stings** — techs having to log into pCloud.
- Make scenario 4522457 keeps working untouched the whole time.

### Slice 1 — in-app gallery (read-only)

```
[Job → 📷 Photos tab]
  └─ GET action=jobPhotos&jobId=…
       └─ read job's pCloud Photo's ID (folderid)
            └─ pCloud listfolder(folderid)  → [{fileid, name, size, thumb, created, contenttype}]
                 └─ return JSON list (no bytes)

  thumbnails → GET action=jobPhoto&jobId=…&fileid=…&size=thumb
  full size  → GET action=jobPhoto&jobId=…&fileid=…&size=full
       └─ function fetches bytes from pCloud and returns the image with a long Cache-Control
```

The pCloud key lives **server-side only** (`PCLOUD_ACCESS_TOKEN` in Netlify env). The user's device
never holds a pCloud credential and can only reach fileids that are inside the requested job's
folder — the handler must verify that, not trust the client's fileid.

### Slice 2 — direct upload

```
[📷 Add Photos]
  └─ <input type="file" accept="image/*" capture="environment" multiple>
       └─ client compress → JPEG ~1600–2048px long edge, q≈0.75  (2.5 MB → ~400 KB, HEIC→JPEG free)
            └─ POST action=uploadJobPhoto  (one photo per call, 2–3 concurrent)
                 └─ pCloud uploadfile (multipart, folderid, never a path)
```

### The viewing catch

pCloud's `getfilelink` and `getpublinkdownload` both state:

> "This method can't be used from web applications. Referrer is restricted to pcloud.com."

So a pCloud URL **cannot** be dropped into an `<img src>` on the app domain. Options in order:

| Approach | Verdict |
|---|---|
| **`getthumblink` + `<img referrerpolicy="no-referrer">`** | Docs don't list the restriction on `getthumblink`. **Spike this — 30 min, needs the token.** If it works, thumbnails come straight from pCloud and cost us nothing. |
| **Function serves the bytes** (fetch server-side, return image + long `Cache-Control`) | Always works — server-to-server has no referrer. Costs Netlify bandwidth. **Assume this; treat the spike as upside.** |
| **`getfolderpublink`** → no-login pCloud page | Not a gallery, but ideal for the "send photos to the customer" button. |

`getthumblink` sizes: width 16–2048, height 16–1024, **each divisible by 4 or 5** (use `320x320`).
Thumbs exist only where the file's metadata has `thumb: true`.

### Caching is a requirement, not an optimization

Netlify moved to credit-based pricing in 2026 (300 credits/month on Free, bandwidth at 20
credits/GB ≈ 15 GB; accounts created before 2025-09-04 may be grandfathered on the old 100 GB).
**Confirm which plan/model this account is on before Slice 1 ships.**

Rough load: a 30-photo gallery at ~30 KB per thumb ≈ 0.9 MB per *first* view, and one function
invocation per thumbnail. Uncached, a few crews scrolling galleries would be material against
either allowance — in invocations more than bytes. Cached properly, each photo is fetched once
per device, ever. So:

- Immutable `Cache-Control: private, max-age=31536000` on `action=jobPhoto` (fileid + size is a
  stable key — pCloud fileids don't get reused for different content). `private` keeps job photos
  out of shared caches, at the cost of no cross-user dedupe.
- **`sw.js` is fine — verified.** Its `/.netlify/` branch `return`s without calling
  `respondWith`, so the service worker simply doesn't intercept and the browser's own HTTP cache
  honours the header normally. The comment there says "never cache", which describes the SW, not
  the browser. No `netlify.toml` reroute needed.
- `resp()` hardcodes `Cache-Control: no-store`; the image path needs its own response builder
  (`respImage`).

---

## 3. Options considered

| Option | Effort | Fixes upload? | View w/o login? | Verdict |
|---|---|---|---|---|
| **pCloud, by folderid, app-served gallery** | Medium | ✅ root cause | ✅ | **CHOSEN** |
| Cloudflare R2 (presigned PUT/GET) | Medium | ✅ | ✅ natively, no proxy | Rejected — see below |
| App → existing Make webhook | Low | Partly — drops JotForm, keeps Make | ❌ | Rejected as an endpoint; viable emergency stopgap |
| Airtable attachment field | Lowest | ✅ | ✅ | ❌ Worst Neon migration shape (see PLAN-expense-receipts) |
| Fix the Make scenario only (folderid + unique filename) | Lowest | ✅ mostly | ❌ | Parked at user's request; keep as the emergency lever |

### Why R2 was rejected

R2 is the cleaner engineering answer — presigned PUT *and* GET, no referrer restriction, no proxy,
free egress, 10 GB free, S3 API. It lost on workflow, decisively:

1. **Five years of job photos already sit in pCloud.** R2 starts empty, so photos would live in two
   places forever, or need a migration project.
2. **Receipt PDFs already use the same per-job folder structure.** Choosing pCloud collapses the
   deferred `PLAN-expense-receipts` work onto this same plumbing — one system, not two. That
   consolidation was R2's main argument, and the existing folder structure takes it away.
3. pCloud is already bought and paid for, and the office lives in that folder tree.

**Consequence to fold in:** `docs/PLAN-expense-receipts.md` says R2. It should be revised to pCloud
(`pCloud Job Receipts ID` is already on every job) once Slice 1 proves the integration.

---

## 4. Better ideas folded in

1. **Slice 3: give photos a database row.** Today *nothing* records that a photo exists — the only
   evidence is a file in a folder. Slice 1 reads the folder live, which is fine and needs no
   schema. But a `Job Photos` table (Airtable now, Neon later, mirroring the `expense_receipts`
   shape) later buys captions, before/after tags, who shot it, gallery loads without hitting
   pCloud, and pulling photos into service reports and invoices.
   ```
   job_photos: id, job_id→Jobs, pcloud_fileid, pcloud_folderid, filename, content_type,
               width, height, bytes, caption, tag(before|after|issue|closeout),
               uploaded_by→Employees, uploaded_at, client_ref UNIQUE
   ```
2. **Real filenames** on upload: `{JobPO}_{YYYYMMDD-HHmmss}_{initials}_{seq}.jpg`. Collisions gone,
   and the pCloud folder stays self-describing for whoever opens it directly.
3. **Compress on the client.** 2.5 MB → ~400 KB, converts iPhone HEIC → JPEG for free, works on bad
   jobsite LTE, stays far inside Netlify's ~4.5 MB effective binary payload limit. Watch EXIF
   orientation — canvas re-encode drops it, so use
   `createImageBitmap(blob, {imageOrientation:'from-image'})` or every iPhone photo lands sideways.
4. **Idempotency via `client_ref`** — reuse the inventory "Push ID" pattern so a retry on flaky
   signal doesn't double-upload.
5. **Offline queue (later).** Basements and steel buildings have no signal. Queue blobs in
   IndexedDB, upload on reconnect, pending badge. Biggest real-world reliability win after Slice 2.
6. **Per-photo progress and retry**, not one all-or-nothing spinner — techs shoot 5–15 at a time.
7. **Customer share link** — `getfolderpublink` with an `expire`, as "Send photos to customer".
   Covers people who aren't app users at all.
8. **Download / share from the gallery** — save to camera roll, native share sheet, download-all.
9. **Keep the Airtable `Add Photos (Mobile)` / `View pCloud Photos` fields.** Airtable users may
   still click them; just stop the app depending on them.

---

## 5. Risks

| Risk | Mitigation |
|---|---|
| `getthumblink` also referrer-locked | Spike first; fall back to function-served bytes (the assumed path) |
| `PCLOUD_ACCESS_TOKEN` is a full-account credential | Server-side only, never in `index.html`; confirm expiry behavior |
| Wrong API host (`api.pcloud.com` US vs `eapi.pcloud.com` EU) | OAuth response returns `hostname`/`locationid`; store as `PCLOUD_API_HOST`. Links are `my.pcloud.com` → likely US |
| A client could request an arbitrary fileid | Handler verifies the fileid's `parentfolderid` matches the job's folder before serving bytes |
| Job missing `pCloud Photo's ID` | Fail loudly ("photo folder not set up for this job"), never fall back to a path |
| Netlify bandwidth / invocations | Immutable caching, thumbs in the grid, full size only on tap; confirm plan tier |
| ~~`sw.js` swallows caching~~ | **Resolved** — the SW doesn't intercept `/.netlify/`; browser HTTP caching applies |

## 6. Slices

- **Slice 1 — viewing (read-only).** `_pcloud.js` helper, `jobPhotos` + `jobPhoto` actions, in-app
  Photos tab with grid + lightbox, `#btnViewPhotos` opens it instead of pCloud. Existing photos
  work immediately. Make untouched.
- **Slice 2 — upload.** `uploadJobPhoto` + camera/compress UI on `#btnAddPhotos`. JotForm no longer
  used by the app. Make scenario left running as a fallback during soak.
- **Slice 3 — `Job Photos` table**, captions/tags, backfill from a `listfolder` sweep.
- **Slice 4 — hardening.** Offline queue, per-photo retry, customer share link, download-all.
- **Slice 5 — retire.** After soak: pause (don't delete) Make scenario 4522457 and the JotForm form,
  matching the wire/pipe retirement pattern.

## 7. Touch points

- `index.html:2136` `#btnAddPhotos` / `#btnViewPhotos`; `setActionBtn` (:4233); job render (:3684).
- Browser→cloud upload precedent: `uploadPDFToPCloud` (index.html:3272).
- `airtable.js`: `F.job.addPhotosLink` (:147) / `viewPhotosLink` (:148); add
  `pcloudPhotoFolderId` ← `pCloud Photo's ID`; surface it in `mapJob` (:1828).
- New actions in the GET dispatch chain (~airtable.js:4509): `jobPhotos`, `jobPhoto`.
  Slice 2 adds `uploadJobPhoto` to the POST chain (~:4574). Tier them in `authzFor` (:418) —
  reads default to `null` (any signed-in role), upload `_NON_VIEWER`, delete `_ADMIN_OFFICE`.
- `resp()` (:355) is `no-store` — the image route needs its own cacheable response builder.
- New env: `PCLOUD_ACCESS_TOKEN`, `PCLOUD_API_HOST`. Add to `.env.example` + CLAUDE.md.
  **Fails soft like `_neon.js`, not closed like `_auth.js`** — no token means the Photos tab shows
  "photos unavailable", never a 500 on the job view.
- Tests: `tests/handlers.test.mjs` with a mocked pCloud fetch (stays offline).

## 8. Blocked on

1. **`PCLOUD_ACCESS_TOKEN`** — register an app at pCloud's developer dashboard, run the OAuth
   code flow once, capture the token and the returned `hostname`. Nothing server-side can be
   tested until this exists.
2. **Netlify plan check** — free vs Pro, old bandwidth model vs 2026 credits.

## 9. Open questions

1. Should the gallery show *all* files in the photo folder, or filter to image types only?
   (Assume images only, ignore stray PDFs/docs.)
2. Sort newest-first or oldest-first? (Assume newest-first.)
3. Who can delete a photo — admin/office only, or the uploader within a window like
   `guardExpenseMutation`? (Assume no delete in Slice 1–2; add in Slice 4.)

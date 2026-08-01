# Jobsite photos — SHIPPED (Cloudflare R2)

**Status:** Complete and in production as of 2026-08-01. ~970 photos live, backed up twice daily.
**Store: Cloudflare R2.** pCloud was chosen first and abandoned mid-build — see §2.
**Make.com: untouched.** Scenario 4522457 and the JotForm form still run; retiring them is §7.

**One-line:** Replaced the JotForm → Make → pCloud photo path with camera → compressed on the
phone → straight to Cloudflare R2, plus an in-app gallery with albums, soft delete, and a
twice-daily backup to two local drives.

---

## 1. What it replaced, and why it was broken

The old path: **📷 Add Photos** opened JotForm form `260246511955053`, which fired Make scenario
**4522457 "Submitt Photos"** — Airtable job lookup → download each photo back off jotform.com →
`pcloud:uploadFile` **by path string**:

```
/Northeastern Electric Jobs/NEE Jobs/{{formatDate(now;"YYYY")}}/{{Contractor Name (Text)}}/{{Job PO}}/{{Jobsite Files (pCloud)}}/{{pCloud Photo Upload}}
```

Observed failure, 2026-07-01: **`[2005] Directory does not exist`** from `pcloud/uploadFile`.
Structural, not flaky:

1. `formatDate(now,"YYYY")` is the **current** year, not the job's — breaks every January and on
   every carry-over job.
2. The path is rebuilt from five human-editable fields; any rename silently breaks it.
3. The job record already held `pCloud Photo's ID` — a stable folder id the scenario ignored.
4. Every photo got the same filename, `{{Job Name}} {{Job Type}}.jpg`.
5. Viewing meant `my.pcloud.com/#/filemanager?folder=…` — a pCloud **login**, and then the whole
   company file tree.

## 2. Why pCloud lost the store decision after winning it

pCloud was chosen on workflow grounds (folders and receipts already there, already paid for) and
abandoned two hours later on a hard blocker: **`docs.pcloud.com/my_apps/` — app registration —
has been down for months**, so no OAuth client and no API token can be created.

The documented fallback (`userinfo?getauth=1`) authenticates the password (verified: a bad
password returns `2000`, this account returns `1022`) and then demands a second factor under an
**undocumented parameter** — `code` is ignored and `1022` repeats.

Make.com still reaches pCloud only because **Make registered its own pCloud app years ago**, before
the page broke, and holds a non-expiring token (connection `24595`, `expire: null`). Make's API
never exposes connection secrets, so it can't be borrowed.

R2 then turned out to be mechanically better anyway: **presigned URLs** mean the browser talks to
Cloudflare directly, so no bytes pass through the function — no per-thumbnail invocation, no
Netlify bandwidth per photo, and no 4.5 MB payload ceiling on upload. The pCloud design would have
had to proxy every image, because pCloud restricts download-link referrers to pcloud.com.

## 3. How it works now

```
📷 Add Photos
  └─ album picker (existing albums as chips, or type a new one)
       └─ file picker: camera OR a batch from the camera roll
            └─ per photo: compress to 2048px q0.75 (~400 KB) + a 400px thumbnail
                 └─ POST jobPhotoUploadUrls  (chunks of 10; the function caps one request at 12)
                      └─ PUT both files straight to R2, 3 at a time

🖼 View Photos
  └─ GET jobPhotos → presigned GET urls
       └─ album tiles → grid → lightbox (full size, prev/next, Save)
       └─ Select → Move to album / Delete
       └─ 🗑 Recently deleted (admin/office) → Restore / Delete permanently
```

### Object layout

```
jobs/<airtable record id>/<album>/<stamp>-<n>-<rand>.jpg          the photo
jobs/<airtable record id>/<album>/<stamp>-<n>-<rand>_thumb.jpg    its thumbnail
jobs/<airtable record id>/_deleted/<album>/…                      recycle bin
jobs/<airtable record id>/_deleted/_none/…                        was loose when deleted
```

Scoping is by **Airtable record id, not job name**. The `FIND`-on-name pattern used elsewhere in
`airtable.js` matches substrings, so "Jenny Ln 1" leaks into "Jenny Ln 10/11/12" (TODO.md). Record
ids can't collide, so two jobs named the same never see each other's photos. Tested.

The album is the **only** client-supplied part of a key, so it is sanitized (slashes neutralised,
`..` rejected, 60 chars) and percent-encoded. Every mutation re-validates the key against the job's
own prefix server-side — the client sends keys back to us, so without that a signed-in user could
reach another job's photos by editing one string. Tested.

## 4. Files

| File | Role |
|---|---|
| `netlify/functions/_r2.js` | R2 client: presign, list, move, soft delete, restore, purge, self-test. Fails soft like `_neon.js`; `aws4fetch` lazy-imported so tests stay offline. |
| `netlify/functions/airtable.js` | Actions: `jobPhotos`, `jobPhotosDeleted`, `r2Status`, `jobPhotoUploadUrls`, `moveJobPhotos`, `deleteJobPhotos`, `restoreJobPhotos`, `purgeJobPhotos` |
| `index.html` | Album picker modal, gallery modal, lightbox, selection toolbar, client-side compression |
| `tools/backup-photos.ps1` | rclone **copy** (never sync) to F: and P: |
| `tools/install-backup-task.ps1` | Scheduled task, 3×/day with wake-catch-up |
| `netlify/functions/_pcloud.js`, `tools/pcloud-*.mjs` | **Dead.** Kept only in case pCloud reopens registration. |

Env: `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`. Optional as a group,
never in `ensureEnv()`. The bucket also needs a **CORS policy** allowing `PUT` from
`https://hub.northeasternelec.com` — bucket config, not env, and invisible until uploads fail.

Diagnose with `GET ?action=r2Status&selfTest=1` (admin): it round-trips a real object through the
same presigned urls the browser uses, **server-side**, which is the only way to tell a signing
problem from a CORS block — a browser reports both as a bare "Failed to fetch".

## 5. Authorization

| Action | Who |
|---|---|
| View photos, upload, move between albums | any signed-in non-viewer |
| Delete (to recycle bin), restore, purge, list the bin | admin / office |
| `r2Status` | admin |

Delete is admin/office because **nothing records who took a photo**, so the expense-style "your own
until reviewed" rule can't be enforced. `purgeJobPhoto` refuses any key not already in the bin, so
permanent delete can never be aimed at a live photo.

## 6. Backup

Two destinations, three times a day, `rclone copy` — **never `sync`**. Sync would mirror deletions
into the backup, so deleting photos in the app would erase them from the backup on the next run,
protecting against nothing.

- `F:\NEE-Job-Photos` — physical, offline, survives an account problem
- `P:\NEE Job Photos Backup` — pCloud Drive, off-site, survives the building

Runs 08:30 / 12:30 / 17:00 with `-StartWhenAvailable`, so a sleeping PC catches up on next wake.
Three slots rather than one because a missing drive **exits "skipped" and Windows counts that as
having run** — a single trigger could miss days while still looking healthy. Uses a **separate
read-only** R2 token.

First run 2026-08-01: 1,944 files (~970 photos) to both, verified.

## 7. Still open

1. **The 30-day recycle-bin expiry is a promise, not a mechanism.** Nothing purges `_deleted/`.
   Needs an R2 lifecycle rule on that prefix, or the bin grows forever. Low urgency (storage is
   pennies, and "keeps everything" errs safe) but it is an unfinished edge.
2. **Retire JotForm + Make.** After a week of real use, pause form `260246511955053` and scenario
   `4522457` — pause, don't delete, matching the wire/pipe retirement pattern. No code change.
3. **Offline upload queue.** Basements and steel buildings have no signal. Worth building around
   observed field behaviour rather than guesses.
4. **Receipts on R2.** `docs/PLAN-expense-receipts.md` specifies R2 and is now consistent with
   reality — the upload path, compression and presigning already exist, so it is largely assembly.
5. **An office-browsable pCloud copy.** The backup is keyed by record id, which is deliberate but
   not readable. A copy named by Job PO would need an Airtable id→name mapping step.

## 8. Things that cost time, recorded so they don't again

- **A stale service worker looks exactly like a CORS rejection.** `sw.js` intercepted the
  cross-origin `PUT`, `cache.put()` threw on a non-GET, and the page got a bare "Failed to fetch".
  Both the bucket CORS (verified `204` by direct OPTIONS probe) and the deployed `sw.js` were fine.
  Fixed by skipping non-GET and cross-origin in the worker, **and** forcing `registration.update()`
  on load — a registered worker can serve the old script for 24h, so a fix shipped *inside* sw.js
  can sit unapplied while index.html is already current.
- **A 403 from R2 with no CORS headers also surfaces as "Failed to fetch".** The real cause was an
  **Object Read only** token. `r2Status&selfTest=1` exists because of this.
- **Netlify gives a sync function 10 seconds.** Moving 47 photos was ~188 sequential R2 round trips
  → 504 while the work carried on server-side, so the user saw a failure *and* the photos moved.
  Fixed with client chunking + server concurrency.
- **`capture="environment"` makes phones ignore `multiple`.** One photo per tap.
- **Windows PowerShell 5.1 reads `.ps1` as ANSI without a BOM**, so em dashes in comments break the
  parse. The `tools/*.ps1` scripts are deliberately ASCII-only.

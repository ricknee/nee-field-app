# Plan: job prints & documents in the field app

**Status:** BUILT 2026-08-05 — un-parked at the owner's request ("i can use this feature"). Slices 1,
2 and permanent delete all landed together. ⬜ Not yet smoke-tested on production.

*(Was PARKED 2026-08-03, same day it was planned, on the grounds that Trello already got prints to
crews. The visibility decision in §3 survived un-parking unchanged — it was the expensive part.)*

**One-line:** A 📐 Prints button in the job action row to upload drawings against a job, which
**crews can open on site** — without a pCloud login.

## What shipped (2026-08-05)

Placement changed from the plan below: prints are **not** on the Project Info tab. The button sits in
the **action-buttons row above the tab bar** (`index.html`, next to 📷/🖼), so it is on screen from
whichever tab you are on and is one tap from the top of the job — and it carries a **count badge**,
so "does this job have drawings?" is answered without opening anything.

| Piece | Where |
|---|---|
| `_prints/` prefix, nested bin, sanitizer, list/soft-delete/restore/purge | `_r2.js` |
| `jobPrints`, `jobPrintsDeleted` (GET) · `jobPrintUploadUrls`, `deleteJobPrints`, `restoreJobPrints`, `purgeJobPrints` (POST) | `airtable.js` |
| 📐 button + badge, modal, drag-and-drop, upload, delete/restore/purge | `index.html` |
| 5 cases: open-read tier, upload/remove tiers, prefix isolation, filename safety, purge guards | `tests/handlers.test.mjs` |

Three things worth knowing that the plan below did not anticipate:

- **The prints bin is nested (`_prints/_deleted/`), not the top-level `_deleted/` root the photos
  use.** `listDeletedJobPhotos` keeps *everything* under `_deleted/jobs/<id>/`, so a print binned
  there would have appeared in the photo recycle bin and been restorable into an album. Nesting also
  keeps prints out of the 30-day lifecycle expiry — a drawing should leave when someone says so, not
  on a timer. Same reasoning as receipts.
- **Filenames are preserved, which makes the filename the dangerous string.** `sanitizePrintName`
  strips slashes, collapses dot-runs and whitelists characters — the whitelist is tighter than it
  looks because `presign()` builds the URL with `encodeURI`, which leaves `#`, `?` and `&` intact, so
  "Panel #3.pdf" would sign one URL and address a different object. The dot-collapse was found by a
  test: without it, a print named `a .. b.pdf` uploads fine and can then **never be deleted**,
  because `assertKeyInPrints` refuses any key containing `..`.
- **Same name = replace.** A print's key *is* its filename, so re-uploading `E-1.pdf` overwrites,
  which is usually the intent ("here is the corrected sheet"). The client warns first, and offers to
  upload the non-clashing files only.

**Permanent delete exists** (owner request: "i would like the option to delete prints if need be for
storage sake"). Soft delete alone does not reclaim anything — a binned object still costs, and the
prints bin is deliberately outside the lifecycle rule. So the bin offers **↩ Restore** and **✕ Delete
forever**, admin/office only. The modal also shows the job's **total print storage**, so a job
carrying 300 MB is visible rather than a surprise.

---

## 1. Why it's worth doing

Prints live in pCloud today (`pCloud Full Prints ID`, `Drawn Jobsite Prints`, `Jobsite Prints`,
`Cads`, `Specs` — five separate folder-id fields on Jobs). That's fine for the office and useless
in the field: opening them means a pCloud login, and then the whole company file tree.

It is exactly the problem the photo gallery solved. Same fix, same machinery.

**This does not replace pCloud.** pCloud stays the office document tree and the Make automation
keeps building the folders. This is a field-accessible copy of the drawings a crew actually needs
on site.

## 2. What already exists

| Piece | Where | Reuse |
|---|---|---|
| R2 client, presigned PUT/GET | `_r2.js` | as-is |
| `jobDocs` (GET) — list a job's documents | `airtable.js` | extend |
| `jobDocUploadUrl` (POST) — presigned PDF upload | `inventory.js` | copy to `airtable.js` |
| `_docs/` prefix + gallery exclusion | `_r2.js` | as-is |
| Documents strip in the Photos view | `index.html` | extend |
| Backup to F: and P: | `tools/backup-photos.ps1` | **free** — copies the whole bucket |

## 3. The one real decision: visibility

`jobDocs` is currently **admin/office only**, deliberately — it lists the inventory materials PDF,
which itemises unit costs and job totals. Crews must not see that.

But prints are the opposite: the whole point is that **employees can open them**.

So documents need two visibility classes. Keep the folder-is-the-record pattern rather than
inventing metadata:

```
jobs/<job id>/_docs/…      internal   — materials PDFs, costs      admin/office
jobs/<job id>/_prints/…    shared     — prints, drawings, specs    everyone
```

Two prefixes, two listing actions, no schema, no per-file permission flags to get wrong. A file's
location *is* its permission, which is the same reasoning that made albums and the recycle bin
simple.

> ⚠ Do not solve this with a "shared" checkbox on a single list. One mis-ticked box puts job
> costing in front of the whole crew, and nothing about the file's location would show it was
> wrong.

## 4. Scope

**In:**
- Upload PDFs (and images — a phone photo of a marked-up print is a legitimate print)
- List, open in the browser's viewer, download
- Delete — admin/office only, soft-delete into the existing recycle bin
- Works on desktop: prints come off a PC, so the picker cannot assume a camera

**Out of v1:** versioning ("Rev B"), per-print notes, marking one as current. Filenames carry that
today and can keep doing so.

## 5. Slices — ✅ all three built 2026-08-05

*Kept as written for the record; the "Prints button on Project Info" placement was superseded — see
"What shipped" above. Permanent delete was added to slice 2 on the owner's storage request.*

**Slice 1 — upload + view (~3 h)**
- `_r2.js`: `jobPrintsPrefix(jobId)`, `listJobPrints`, exclude `_prints/` from the photo gallery
  the same way `_docs/` is
- `airtable.js`: `jobPrintUploadUrl` (POST, `_NON_VIEWER`), `jobPrints` (GET, **any signed-in
  role** — that's the point)
- `index.html`: a **📐 Prints** button on Project Info; list with name, size, date; tap to open
- Tests: authorization (employee CAN read prints, CANNOT read `jobDocs`), key ownership

**Slice 2 — delete + tidy (~1 h)**
- Soft delete reusing `softDeleteJobPhoto`'s machinery
- Sensible upload names: `<Job PO> — <original filename>`

## 6. Risks

| Risk | Mitigation |
|---|---|
| A cost document lands in `_prints/` by mistake | Upload is per-button: the Prints button can only ever write to `_prints/`. There is no picker that chooses between them. |
| Big files — prints can be 20-50 MB | Presigned PUT straight to R2, so no Netlify limit. Do NOT run them through the image compressor. |
| Storage growth | Real, unlike photos. A 40 MB print set per job × 100 jobs = 4 GB, which is most of the free tier. Watch it; R2 is $0.015/GB-month beyond. |
| Crews open a stale revision | Out of scope for v1 — filenames carry revisions. Worth revisiting if it bites. |

## 7. Open question

**Does this duplicate pCloud, or feed from it?** Uploading twice (once to pCloud for the office,
once here for the field) is friction. The alternative — mirroring pCloud → R2 automatically —
needs pCloud API access, which does not exist for us (see `PLAN-job-photos.md` §2).

Manual upload is the honest answer for now. Revisit only if pCloud's developer platform reopens.

# Google contacts → rick@ — rollout (2026-11-15)

Owner, 2026-11-15: contacts should show up in **rick@northeasternelec.com**
Google Contacts, **labelled by tab and by company**. **rick@ only** — nee@ is no
longer a destination (supersedes the 2026-08-27 "both" decision; nee@ is left
untouched). Background and the hazards: `docs/PLAN-google-contacts.md`.

| Step | Who | State |
|---|---|---|
| 1. Merge & fix in Google (rick@) | owner | ⬜ |
| 2. Merge the 12 duplicate people in the app | Claude | ⬜ after 1 |
| 3. Build the sync, inert | Claude | ✅ `_google-contacts-sync.js`, schema 094 |
| 4. Preview → read it → switch on | owner + Claude | ⬜ after 2 |

## Step 1 — owner, ~10 min
contacts.google.com signed in as **rick@** → **Merge & fix** → accept the
suggested merges. Say when done.

## Step 2 — merge duplicates in the app
1. `GET ?action=contactDuplicates` (strict admin) — for each pair it says which
   Google ids still resolve. Since step 1, a pair should show one id dead.
2. Owner's answers already in: **Mike Ware = (330) 206-5049**; **Jeff Miller**
   (jmiller@neagri.com) = ONE person, **Northeast Ag, Owner** — keep row
   `b1eff0f9`, fold in `5c77a829` (Aviary) and `86d92ade` (Miller Poultry,
   contractor contact on 2 jobs — the job links must move to the keeper).
3. Apply with `POST contactMerge` (dry by default, applies only the merges it is
   handed). Anything Google still holds twice → back to the owner, not merged.
4. Re-run `contactDuplicates` → expect 0 (Jim Beeson Farms / Sarge are NOT
   duplicates — they just have no last name).

## Step 3 — what was built (inert)
- **One switch:** `GOOGLE_CONTACTS` in Netlify — unset/`dry` = the hourly run and
  on-save sync do nothing; `on` = live. (Redeploy after changing it.)
- **Preview works in every mode:** Contacts → **🔄 Google sync** (strict admin)
  shows what WOULD happen; nothing is written.
- New / linked / updated / labels-only / missing / ambiguous are all listed.
- Before creating, a person is looked for in rick@ by phone/email and **linked**
  if exactly one contact matches (covers the 29 power-co + 5 inspection people,
  who have never had an id). Two matches → left alone and listed.
- A stored id Google no longer has → **listed, never re-created**.
- Updates keep extra phones/emails added on the phone. **Nothing is ever deleted**
  — no people, no labels, no label memberships.
- Labels: `NEE Customers`, `NEE Contractors`, `NEE Power Co.`, `NEE Inspection`
  + the company / utility / agency name. Missing labels are created.
- Live: a save on the Contacts screen syncs that person at once; the hourly
  `qb-time-pull` catches up the rest (~6 s a run, after the timesheet pull).

## Step 4 — switch on
1. Owner opens Contacts → 🔄 Google sync → reads the preview. Expected roughly:
   ~230 linked (update/labels), ~34 power/inspection mostly new or linked,
   a handful skipped (no phone or email), **0 missing** if steps 1–2 are done.
2. Anything in ⚠ missing / ambiguous → resolve first.
3. Set `GOOGLE_CONTACTS=on` in Netlify → **redeploy**.
4. Contacts → 🔄 Google sync → **Sync now**, repeated until "still to do" is 0
   (or leave it to the hourly run).
5. Spot-check on the phone: a person, their two labels, an extra phone kept.
6. Rollback: unset `GOOGLE_CONTACTS` + redeploy. What was written stays in
   Google (nothing deletes); clear `google_sync_hash` to force a re-check.

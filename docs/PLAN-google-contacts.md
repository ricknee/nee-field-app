# Plan — Google contacts direct (audit item 07)

**Status: SLICES 1-2 SHIPPED AND INERT (`45a4b0a`). Setup is MID-FLIGHT and BLOCKED on an org
policy — see ⏸ RESUME HERE below, and start there rather than at the top.**
✅ **Route A CONFIRMED 2026-08-27** — the owner is a Google Workspace admin on the domain, so this
is service account + domain-wide delegation, not OAuth refresh tokens. Route B below is dead
weight kept only in case the delegation step hits a wall.
✅ **Both destinations stay** (§7.4). ⛔ **Nothing is ever deleted** (§7.4).
✅ **All four owner decisions are CLOSED** (§7): both destinations, no deletion, sync all 240, power
contacts created fresh. **Nothing is open but the key.** Written 2026-08-27 after the owner chose the direct path over replumbing Make:
*"i want to go direct with contacts."*

> **Read this block first.** Two things recorded elsewhere as unknown are now **solved** (§1), and
> the credential story changed shape because of it. The audit's "needs a Google Cloud project +
> O## ⏸ RESUME HERE — stopped mid-setup 2026-08-27

**Owner stopped partway through the Google Cloud setup: *"im gonna have to come back this is hard
and i need to leave."* Do not restart the console walkthrough from the top — most of it is done.
Pick up at THE NEXT CLICK below.**

### ✅ Done in Google Cloud

| | |
|---|---|
| Cloud project | **NEE Field App** (`nee-field-app`), in org `northeasternelec.com` (org id starts `309373…`) |
| People API | **ENABLED** on that project — verified on screen |
| Service account | **`contacts-sync`** created |
| **Its Client ID / Unique ID** | **`112988291121215224869`** ← this is what the Admin console needs. Not a secret. |
| Owner's Cloud rights | **Owner of the PROJECT only.** No org-level IAM at all — the org node reports "you don't have permission to view the permissions of the selected resource". |

⬜ **UNCONFIRMED: the domain-wide delegation entry.** The owner was given the Admin console step
(`https://admin.google.com/ac/owl/domainwidedelegation`, Client ID above + scope
`https://www.googleapis.com/auth/contacts`) but never confirmed adding it. **Check before
diagnosing anything else** — a missing entry and a non-user mailbox produce the same
`unauthorized_client`.

### ⛔ WHERE IT BLOCKED

Creating the service account's JSON key is refused by an inherited **org policy**:

```
iam.disableServiceAccountKeyCreation   ("Service account key creation is disabled")
```

This is Google's **Secure by Default** enforcement, applied automatically to newer orgs — nobody
locked it down deliberately, and there is no other admin to ask. The owner is the whole company.

### 👉 THE NEXT CLICK

Project Owner does **not** include the right to override an org policy, but
`roles/orgpolicy.policyAdmin` **can be granted at the project level**, and a project can override
an inherited boolean constraint. So:

1. https://console.cloud.google.com/iam-admin/iam?project=nee-field-app — confirm the header reads
   *Permissions for project "NEE Field App"*, click the **pencil** on the `rick@northeasternelec.com`
   row → **+ Add another role** → **Organization Policy Administrator** → Save. **Wait a minute**;
   IAM propagation lag looks exactly like failure.
2. https://console.cloud.google.com/iam-admin/orgpolicies/iam-disableServiceAccountKeyCreation?project=nee-field-app
   — resource selector on **NEE Field App**, not the org → **Manage policy** → **Override parent's
   policy** → **Add rule** → **Enforcement: Off** → **Set policy**.
3. Then `contacts-sync` → **Keys** → **Add key → Create new key → JSON**.
4. Netlify env `GOOGLE_SA_KEY` = base64 of that file, `GOOGLE_CONTACTS` left unset, then **REDEPLOY**.

⚠ **Timebox step 2.** Some Secure-by-Default enforcements cannot be overridden below the org. If it
refuses, Route A is genuinely closed — go to Route B and do not keep digging.

### ⚠⚠ IF ROUTE B IS NEEDED — the trap that would cost a week

Route B (§2) needs no key file and no policy change, so the org policy stops mattering. **But the
OAuth consent screen MUST be created as `User type: Internal`.**

**External + Testing issues refresh tokens that expire after SEVEN DAYS.** The sync would work for a
week and then stop — silently, in a system whose failure mode is already silence. Internal never
expires and needs no Google verification, because every user is on the domain.

Route B steps, in the order they were given to the owner:
1. Consent screen → **Internal** → app name `NEE Contacts Sync`.
2. Credentials → **OAuth client ID** → **Web application** → authorised redirect URI exactly
   `https://developers.google.com/oauthplayground`.
3. https://developers.google.com/oauthplayground → gear → **Use your own OAuth credentials** →
   scope `https://www.googleapis.com/auth/contacts` → authorise **as rick@**, exchange, keep the
   refresh token. Repeat **in an incognito window as nee@** for the second.
4. Netlify: `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `GOOGLE_REFRESH_TOKEN_1` (rick@),
   `GOOGLE_REFRESH_TOKEN_2` (nee@). Redeploy.

⬜ **`_google-contacts.js` does NOT support Route B yet** — it implements the service-account JWT
flow only. Adding the refresh-token path is ~1 h and touches `getAccessToken()` alone; everything
above it (the People API wrapper, the reconcile, the no-delete rule) is auth-agnostic and unchanged.

### Code state — slices 1-2 are SHIPPED AND INERT

`45a4b0a`. `GOOGLE_CONTACTS` is unset in production, so none of this runs. 241 tests green.
Nothing here is waiting on a code change — it is waiting on a credential.

---

Auth consent + refresh token(s)" is still true in outline and **wrong in the details** — see §2.

## ⛔ THIS BUILD TOUCHES ZERO AIRTABLE — owner's instruction, 2026-08-27

*"remember airtable is leaving so not include airtable."*

**Nothing in this plan reads or writes Airtable, and nothing in it may.** The base is going
PAT-read-only and then archived; a design with an Airtable leg in it is a design that breaks on a
date already on the calendar. Verified against what this build actually needs:

| What the sync needs | Where it lives now |
|---|---|
| The 230 Google person ids | **Neon** — `contacts.google_person_id_1/2` (schema 049 captured them for exactly this reason) |
| Contact names / phones / emails / company | **Neon** — `contacts`, `power_contacts` (schema 048) |
| The trigger condition (job reached Awarded / Service Call Scheduled) | **Neon** — job status has been Neon-served since the cutover |
| The two contact-GROUP ids for power contacts | **constants**, recorded in §5 and in schema 049's header |
| Which Google account is which | **§1 of this document** |

**Where the new person id gets written: Neon, and only Neon.** Airtable used to be the id store —
that is precisely the role being replaced. See §3.

⚠ Airtable appears below **only as historical provenance** — the `fld…` ids in §1 explain where the
values in Neon came from, and the retired formula in §4 is quoted as the spec to reproduce. Neither
is a dependency. If any future edit to this plan introduces an Airtable read, that is a defect.

---

## 0. Where this stands, measured 2026-08-27

| | verified |
|---|---|
| The five Make sync scenarios | **`isActive: false`** — `4729925` confirmed directly, the other four by their connection usage. Nothing has synced a contact to Google since 2026-08-20. |
| `contacts` in Neon | **240**, all `active` — **230 carry BOTH Google person ids**, 10 carry neither, and **7 of those 10 have no phone and no email** |
| `power_contacts` | **26** (not the 25 in schema 049 — recounted 2026-08-27, all carry an `airtable_id`, so it is a stale count and not drift), and **no per-person id exists** |
| Drift so far | **none.** `airtable_id IS NULL` returns **0** contacts, so nobody has created one since the flip. |
| OAuth code in the repo | **none.** The only Google thing present is `GOOGLE_MAPS_API_KEY`, for mileage. |

**So nothing is broken yet, and that is a deadline rather than a reprieve.** The sync is dead;
it has cost nothing only because no contact has been added in the week since. The first new
contact is the moment Google's copy starts drifting, silently, with no error anywhere.

---

## 1. ✅ SOLVED — which Google account is which

`db/schema/049_contact_google_ids.sql` says the destination mapping is *"NOT recorded anywhere"*
and that whoever builds the sync "has to establish the mapping before writing." **That is no longer
true. It was recoverable from the retired Make blueprints, which still exist even though the
scenarios are undeployed.**

Method, so it can be re-run if this is ever doubted: fetch scenario `4729925`'s blueprint, find the
`google-contacts:createAContact` modules and the `airtable:ActionUpdateRecords` module that consumes
each one's output, then resolve the `__IMTCONN__` connection id against `connections_list`.

| **Neon column (the anchor)** | *was Airtable field* | writes from | Make connection | **Google account** | uid |
|---|---|---|---|---|---|
| `google_person_id_1` | `fld7baYOGRf3mmdl1` | module 6 → 10 | `4769144` "Google - Rick" | **rick@northeasternelec.com** | 102385516730302296763 |
| `google_person_id_2` | `fldZ4H2ob1lcOmZDp` | module 12 → 13 | `4769161` "NEE -Google" | **nee@northeasternelec.com** | 108079752496166473589 |

(`4613185` is the Airtable connection, not a destination. Ignore it.)

**Both destinations are on the company's own domain.** That is the fact that reshapes §2.

---

## 2. What the owner has to provide — smaller than the audit says

The audit line reads *"Needs a Google Cloud project + OAuth + refresh token(s) from the owner."*
Because both destinations are `@northeasternelec.com`, there are two routes and **the first is
strictly better** if the domain is Google Workspace.

### ✅ Route A — service account + domain-wide delegation (preferred)

1. Google Cloud project → enable the **People API**.
2. Create a **service account**, generate a **JSON key**.
3. In the **Workspace Admin console** → Security → API controls → Domain-wide delegation, add the
   service account's client id with scope `https://www.googleapis.com/auth/contacts`.
4. Hand over the JSON key. That is all.

Why it is better: **no OAuth consent screen, no user-approval flow, and no refresh token that can
expire or be revoked** by someone clicking "remove access" in their Google account. One key
impersonates `rick@` for destination 1 and `nee@` for destination 2. Rotation is a new key, not a
re-consent.

✅ **Confirmed 2026-08-27: the domain is Workspace and the owner is an admin.** This is the route.

⚠ **The one thing left that can bite: DWD impersonates a real, licensed USER.** If either mailbox
is a Google Group or a mail-only alias rather than a licensed user, the token request fails with
`unauthorized_client` — which reads exactly like a scope or clock-skew problem and is neither.
Evidence says both are real accounts: distinct uids (`102385516730302296763` vs
`108079752496166473589`) and 192 **distinct** person ids captured in each column, which an alias
pointing at one mailbox could not produce. Confirm the licence, then stop worrying about it.

### Route B — two OAuth refresh tokens (fallback, if not Workspace)

Cloud project + People API + OAuth consent screen (Internal if Workspace, else External + test
users), then a one-time consent **per account** to mint a refresh token for `rick@` and one for
`nee@`. Two secrets instead of one, and each can be revoked out from under the sync.

### Env vars either way

Follow the house kill-switch pattern (`GENERATOR_SERVICE_CALLS` is the closest model):

- `GOOGLE_CONTACTS` — **unset = inert** (the sync does nothing and reports `enabled:false`),
  `dry` = report what it would write and write nothing, `on` = live.
- Route A: `GOOGLE_SA_KEY` (the JSON, base64), `GOOGLE_CONTACTS_DEST_1` / `_DEST_2` (the two
  addresses above, so the mapping is configuration rather than a constant in code).
- Route B: `GOOGLE_OAUTH_CLIENT_ID` / `_SECRET`, `GOOGLE_REFRESH_TOKEN_1` / `_2`.

**Fail soft, not closed** — like `_r2.js` and `_neon.js`, and unlike `_auth.js`. A contact must
save whether or not Google is reachable. The sync failing must never fail the write.

---

## 3. ⚠⚠ The hazard that governs the whole design

**A sync that starts cold does not update 230 contacts. It creates 230 duplicates — twice over —
in an address book that is live on people's phones.**

The 230 ids in `google_person_id_1/2` are the only thing standing between the build and an
afternoon of manual merging in two accounts. Therefore:

1. **Id-first, always.** A row with a person id is an `updateContact`, never a create. A row
   without one is a create *only after* a match attempt (§5).
2. **`dry` mode is not optional and ships first.** The first artifact of this build is a report:
   per contact, per destination — update / create / skip, and why. Read it before `on`.
3. **Write the id back in the same request that creates the person.** This is the same rule that
   `createJobNative` follows for its mirror, and the same failure that hit allocations when time
   entries stopped getting an Airtable twin: a create whose id is not recorded is a create that
   will happen again.

⚠ **People API `updateContact` requires the record's current `etag`.** A stale etag is a 400, not
a silent no-op — so every update is read-then-write (`people.get` → `updateContact`), and the etag
must not be cached across runs. This is the main reason the update path costs more than it looks.

---

## 4. What the five retired scenarios actually did

All five are undeployed; their behaviour is the spec to reproduce, not something to preserve
verbatim.

| Scenario | Covered |
|---|---|
| `4729925` Sync Awarded Job Contacts → Google | job contacts, both destinations |
| `4735255` Sync Power Company Contacts → Google | power contacts |
| `4739000` Sync Inspection Contacts → Google | inspection contacts |
| `4739070` Sync Inspection Agencies → Google | agencies |
| `4739137` Sync Vendor Contacts → Google | vendor contacts |

⚠ Two of the five had **never once fired**, so "reproduce what Make did" is not the same as
"reproduce what was actually happening." Check execution history per scenario before treating any
of them as a live requirement.

**The trigger rule needs an explicit decision (§7).** The old Airtable formula flagged a contact
when *either* Google id was blank **and** its job had reached Awarded or Service Call Scheduled.
That rule is why 10 of 240 have no ids — they are **not failures**.

---

## 5. Power contacts — the half with no id

**26** rows (§0 explains the 25/26 discrepancy), and schema 049 is explicit that their two Airtable
fields held contact **group** ids
(`contactGroups/36e512d0097f117f`, `contactGroups/593386b00fa9ca08`), identical on every row. They
say which Google *label* to file under, not which person a row became. **There is no id to match
on.**

✅ **Owner picked option 2 on 2026-08-27 — create all 26 fresh.** The other two are kept for the
reasoning only:

1. **Match by email, then phone, then exact name** against `people.connections.list` for each
   destination. Safest-sounding, most code, and still capable of a wrong match on a shared office
   number.
2. **Create all 25 fresh** and accept the duplicates in exchange for a clean id anchor going
   forward. 25 manual merges is an hour, not an afternoon — and it is bounded, unlike option 1's
   failure mode.
3. **Leave power contacts out of the first cut** entirely and ship the 240 job contacts.

⚠⚠ **Creating fresh needs a SCHEMA CHANGE that does not exist yet.** Schema 049 deliberately added
`google_person_id_1/2` to `contacts` **only** — it explicitly declined to add them to
`power_contacts`, on the grounds that storing the same constant group id 26 times would imply a
per-row identity that did not exist. That reasoning was right then and is obsolete now: once these
are created fresh, each row **does** have a per-person identity, and it must be stored or every run
re-creates all 26. So slice 4 opens with `db/schema/066_power_contact_google_ids.sql` (next free
number as of 2026-08-27 — ⚠ re-check it, a parallel session may have taken 066) adding the same
two columns to `power_contacts`. **Without that column pair, “create fresh” is a duplicate
generator, not a migration.**

⚠ `power_contacts.name` is **GENERATED** in Postgres (Airtable's "Contact
Name" was a formula). Never store a copy that can drift; read it, don't write it. The two group
ids should become configuration so the sync can file contacts under the right label.

---

## 6. Slices, ~10-14 h

Ordered so the dangerous write comes last and is preceded by evidence.

| # | Slice | Est |
|---|---|---|
| 1 | `_google-contacts.js` — auth (Route A or B), token cache, People API wrapper, and `GET ?action=googleStatus` naming the specific misconfiguration. **Model it on `r2Status`**, which exists precisely because "it doesn't work" is not a diagnosis. Lazy-import the auth dep so the test suite stays offline. | 2 h |
| 2 | **Reconcile, read-only.** For each contact × destination: does the stored person id still resolve? Report update / create / missing / conflict. **This is the gate.** Nothing writes until its output is read. | 2 h |
| 3 | `contacts` write path — **all 240** (§7.2): 230 updates by id (read-then-write for the etag), 10 creates, id stamped back in the same request. No job-status gate. | 3 h |
| 4 | `power_contacts` — **create all 26 fresh** (§7.3), id stamped back to Neon on create. Needs a new column pair; no matching logic. | 1-2 h |
| 5 | Wire into the create/update handlers + `GOOGLE_CONTACTS` switch + `dry`. Fail soft. | 2 h |
| 6 | Live `dry` run against production, read it line by line, then flip. | 1-2 h |

**Tests:** offline unit tests against a stubbed People API in `tests/handlers.test.mjs`, following
the `_r2.js` stub pattern (⚠ note from the lifts build: aws4fetch calls `fetch(Request)`, so stubs
read `.url` — whatever HTTP client this uses, check the same thing). ⚠⚠ **Offline tests cannot
catch a broken live call.** The `dry` run in slice 6 is the real verification, and per the
native-job post-mortem: **deploying is not evidence — re-query after the first real run.**

---

## 7. Open decisions — owner

1. **Workspace or not?** Decides Route A vs B (§2). Ask first; everything else waits on it.
2. ~~**Trigger rule?**~~ ✅ **DECIDED 2026-08-27 — SYNC ALL 240.** Owner: *“sync all 240.”* The
   retired Awarded / Service-Call-Scheduled gate is **dropped**, which is the right call now that
   `nee@` is the office address book — and it makes the code simpler, because there is no job-status
   join to evaluate at all. **Measured the same day: all 240 are `active`**, so there is no
   active/inactive gate either. The sync condition is: every row in `contacts`.

   ⚠ **7 of the 240 have neither phone nor email** — name only — and all 7 are inside the 10 that
   were never synced. So the old gate was not the only reason those 10 have no ids; **7 of them
   had nothing worth syncing.** They will now be created as name-only entries in both accounts.
   That is harmless and consistent with “all 240”, but if staff would rather not see 7 blank
   entries, skipping rows with no phone AND no email is a one-line filter. **Flagging, not
   assuming** — say the word either way.
3. ~~**Power contacts?**~~ ✅ **DECIDED 2026-08-27 — CREATE FRESH.** Owner: *“create the power
   contacts fresh.”* No fuzzy matching by name/phone/email; each gets a clean id anchor written
   back to Neon on create. Any duplicate this produces against a pre-existing Google entry is
   bounded and mergeable by hand.

   ⚠ **It is 26 rows, not 25.** Schema 049 and the audit both say 25; the table holds **26** as of
   2026-08-27, and **all 26 carry an `airtable_id`** (0 native), so this is not drift — the earlier
   count was simply captured before the last row loaded. All 26 are `active` and **every one has at
   least one phone or email**, so there is no empty-entry question on this half.
4. ~~**Still two destinations?**~~ ✅ **DECIDED 2026-08-27 — BOTH STAY.** Owner: *“i want them on
   rick@northeasternelec.com and nee@northeasternelec.com. in the future office and staff will rely
   on this one for contacts.”* So `nee@` is not a spare copy to be economised away — it is becoming
   **the office address book**, and its completeness is a requirement, not a nice-to-have.

### ⛔ And therefore: THIS BUILD DELETES NOTHING

The owner asked, before the destinations were settled: *“i dont want duplicates if we for sure dont
lose some. so delete them if they are duplicates.”* That question is now **answered without any
deletion**, and the reasoning is worth keeping because the instinct will recur.

**What looked like duplicates is two address books, not duplicated data.** Every one of the 230
contacts exists once in `rick@` and once in `nee@`, by design — the old sync wrote both, which is
why the retired Airtable formula fired when *either* id was blank. Anyone signed into **both**
accounts on one phone sees every contact twice. Nothing is corrupted, and **cross-account copies
must not be deleted**: clearing `nee@` would not de-duplicate a second person’s phone, it would
empty the address book office and staff are about to depend on.

Rules that follow, and they are load-bearing:

- **The sync never deletes. Ever.** Not as a cleanup step, not behind a flag. A job that runs on
   every contact save must not hold a delete path.
- **Genuine WITHIN-account duplicates** (the same person twice inside `rick@` alone) are a
   different thing, are not yet known to exist, and are for **Google Contacts’ own Merge & fix** —
   it previews each merge and is safer than anything written here. Slice 2 reports whether any
   exist; it does not fix them.
- ⚠ If a contact ever *is* deleted in Google by hand, **the stored id in Neon must be cleared in
   the same breath**, or the next run re-creates it from `google_person_id_1/2`. A delete that
   leaves the id behind is a delete that undoes itself.

---

## 8. ⛔ The Make fallback is DEAD — it was Airtable at both ends

Earlier revisions of this file, and the audit, kept "replumb the five webhooks and let Make do the
Google half, ~2 h, no Google credentials" as a cheap stall-breaker. **Delete that idea. It cannot
work after Airtable goes.**

Measured from scenario `4729925`'s blueprint on 2026-08-27 — the Google sync scenario contains:

| module | count | what it does |
|---|---|---|
| `airtable:ActionGetRecord` | **2** | reads the CONTACT out of Airtable to get the fields to sync |
| `airtable:ActionUpdateRecords` | **4** | writes the returned Google person id BACK into Airtable |

So Make is Airtable-bound on **both** ends: it sources the contact from Airtable and it stores the
person id in Airtable. With `AIRTABLE_WRITES=off` those four writes already go nowhere, so even
reactivated today the scenario would sync a contact and then **lose the person id** — which is
precisely the duplicate-generating failure §3 exists to prevent. Once the base is archived the two
reads fail as well and it stops entirely.

**The direct build is therefore not the preferred option. It is the only one**, and the "cheap
fallback" line in `docs/AUDIT-airtable-remaining.md` should be read as superseded by this section.

> 🔑 **The general lesson, and it applies past contacts.** "Let Make keep doing that half" reads as
> an Airtable-free answer because the *trigger* moved to the app. It is not. **Ask what the
> scenario does in its MIDDLE, not just what fires it** — the same trap recorded on item 04, where
> three of four job scenarios still read the job back out of Airtable after their triggers were
> replumbed. A retired automation is only Airtable-free if its modules are.

---

Related: `docs/AUDIT-airtable-remaining.md` (item 07, and the running order),
`db/schema/048_contacts.sql`, `db/schema/049_contact_google_ids.sql`.

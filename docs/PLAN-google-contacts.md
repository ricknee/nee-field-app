# Plan — Google contacts direct (audit item 07)

**Status: NOT STARTED. Owner-gated on Google credentials — see §2, which is now much smaller
than it was.** Written 2026-08-27 after the owner chose the direct path over replumbing Make:
*"i want to go direct with contacts."*

> **Read this block first.** Two things recorded elsewhere as unknown are now **solved** (§1), and
> the credential story changed shape because of it. The audit's "needs a Google Cloud project +
> OAuth consent + refresh token(s)" is still true in outline and **wrong in the details** — see §2.

---

## 0. Where this stands, measured 2026-08-27

| | verified |
|---|---|
| The five Make sync scenarios | **`isActive: false`** — `4729925` confirmed directly, the other four by their connection usage. Nothing has synced a contact to Google since 2026-08-20. |
| `contacts` in Neon | **240** — **230 carry BOTH Google person ids**, 10 carry neither |
| `power_contacts` | **25**, and **no per-person id exists** (schema 049 explains why) |
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

| Neon column | Airtable field | writes from | Make connection | **Google account** | uid |
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

⚠ **Requires that `northeasternelec.com` really is Google Workspace and that the owner is a
Workspace admin.** Both accounts being custom-domain Google identities is strong evidence, not
proof. **Confirm this before building** — it decides the whole auth module.

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

25 rows, and schema 049 is explicit that their two Airtable fields held contact **group** ids
(`contactGroups/36e512d0097f117f`, `contactGroups/593386b00fa9ca08`), identical on every row. They
say which Google *label* to file under, not which person a row became. **There is no id to match
on.**

Three options, and the owner picks:

1. **Match by email, then phone, then exact name** against `people.connections.list` for each
   destination. Safest-sounding, most code, and still capable of a wrong match on a shared office
   number.
2. **Create all 25 fresh** and accept the duplicates in exchange for a clean id anchor going
   forward. 25 manual merges is an hour, not an afternoon — and it is bounded, unlike option 1's
   failure mode.
3. **Leave power contacts out of the first cut** entirely and ship the 240 job contacts.

⚠ Whichever is chosen, `power_contacts.name` is **GENERATED** in Postgres (Airtable's "Contact
Name" was a formula). Never store a copy that can drift; read it, don't write it. The two group
ids should become configuration so the sync can file contacts under the right label.

---

## 6. Slices, ~10-14 h

Ordered so the dangerous write comes last and is preceded by evidence.

| # | Slice | Est |
|---|---|---|
| 1 | `_google-contacts.js` — auth (Route A or B), token cache, People API wrapper, and `GET ?action=googleStatus` naming the specific misconfiguration. **Model it on `r2Status`**, which exists precisely because "it doesn't work" is not a diagnosis. Lazy-import the auth dep so the test suite stays offline. | 2 h |
| 2 | **Reconcile, read-only.** For each contact × destination: does the stored person id still resolve? Report update / create / missing / conflict. **This is the gate.** Nothing writes until its output is read. | 2 h |
| 3 | `contacts` write path — 230 updates by id (read-then-write for the etag), 10 creates, id stamped back in the same request. | 3 h |
| 4 | `power_contacts` — per the §7 decision. | 0-2 h |
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
2. **Trigger rule:** keep "only contacts whose job reached Awarded / Service Call Scheduled", or
   sync all 240? The 10 with no ids are the only rows this changes today.
3. **Power contacts:** match, create-fresh, or defer (§5).
4. **Still two destinations?** Both accounts are on the company domain, and syncing to both is
   twice the API traffic and twice the duplicate risk. If `nee@` is a shared account nobody's phone
   actually reads from, collapsing to one is a real simplification. **Do not assume it** — removing
   contacts from someone's phone is the kind of change that gets noticed a week later.

---

## 8. The cheap fallback, recorded but not recommended

Replumbing the five webhooks to fire from the Netlify function and letting Make keep the Google
half is ~2 h and needs no Google credentials. **The owner has now twice chosen the direct path**
(2026-08-12 and 2026-08-27), so this is a stall-breaker, not a plan.

⚠ It is also no longer the 2 h the audit quotes: those five scenarios are **undeployed**, so the
fallback now additionally means reactivating them and clearing their queues.

---

Related: `docs/AUDIT-airtable-remaining.md` (item 07, and the running order),
`db/schema/048_contacts.sql`, `db/schema/049_contact_google_ids.sql`.

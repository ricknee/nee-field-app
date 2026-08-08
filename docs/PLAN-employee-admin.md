# Plan — Employee admin ("People" screen)

**Status:** ✅ **Slices 1 + 2 SHIPPED and SMOKE-VERIFIED on production 2026-08-08.** Slice 3 dropped
(owner: employees see wages on their pay stubs). Slices 4-5 open.
**And login itself has moved to Neon** — see the box below.
Written 2026-08-07 at the owner's request.
**Size:** ~8-12 h across 4 slices. Slice 1 (~3-4 h) delivers the thing that was actually asked
for: *turn someone off and they can't get back into the app.*

> ## ✅ Verified on production, 2026-08-08
>
> **Revocation, end to end — the one thing that had never been proven.** Nicholas was restored,
> logged in at **20:34:57**, and was revoked at **20:35:08**. His token predates the stamp, so
> every request is rejected; `active=false` means he cannot log back in either. **Both halves.**
>
> **Login serves from Neon in both apps.** `_source:"neon"`, `id:"recxH3WzXlvhl7z9u"` — the
> **Airtable** rec id, not the Neon uuid — `Rick Unruh` / `admin`, and `last_login_at` moving in
> Neon seconds later. Every downstream call (`jobs`, `listContractors`, `items`, `locations`,
> `reorderAlerts`, `ordersCount`) returned 200 on the issued token.
>
> **Roll back in ~30 s, no code revert:**
> `netlify env:unset LOGIN_SOURCE && netlify deploy --build --prod`
>
> **Slice 1, as shipped:** `db/schema/016_employee_admin.sql` (applied to Neon 2026-08-08 —
> 12 employees, 0 revoked, so behaviour is unchanged until the toggle is used) ·
> `netlify/functions/_revocation.js` · `verifyToken` now returns `iat` · the check wired into
> **both** dispatchers · `handlePeople` + `handleSetEmployeeActive` · the `👥 People` tab in
> `index.html` (strict-admin-only). 109 tier-1 tests pass, 9 new.
>
> **One thing built that the plan above didn't call for**, because it was found mid-build:
> `handleSetEmployeeActive` uses `RETURNING` and asserts a row came back. **A zero-row `UPDATE`
> is a successful query**, so without it, deactivating anyone who is in Airtable but not yet in
> Neon (any hire since the last ETL run) would have reported success while recording no
> revocation — the exact silent lie this feature exists to remove.
>
> **Known test gap, stated honestly:** that zero-row branch is only covered offline, where the
> call fails at the *connection* instead. The green tick proves the endpoint cannot report
> success without a working Neon write; it does **not** prove `mustHaveMatched()` fires. Real
> coverage needs a live-Neon test against a branch — the same gap already noted for
> `createTimeEntry`.
>
> ### ⚠⚠ That gap bit immediately — and here is the 10-second check that closes it
>
> **Every revocation failed on production**, with
> `revokeEmployee: inconsistent types deduced for parameter $2`. Postgres deduces **one type per
> parameter for the whole statement**, and the original SQL used `token_valid_from = $2` beside
> `COALESCE(terminated_on, $2::date)` — asking `$2` to be timestamptz and date at once. The
> offline suite cannot catch this class of bug at all: it dies at the connection before Postgres
> ever parses the SQL, so the write path stays green while being categorically broken.
>
> (The fail-closed design did work exactly as intended — nothing was written and the admin was
> told. Restoring access kept working throughout, because that statement has no `$2`.)
>
> **Before shipping ANY new parameterised Neon write, `PREPARE` it against Neon and read back the
> deduced types.** No parameters needed, no data touched:
>
> ```sql
> PREPARE chk AS <the exact SQL, $1/$2/... untouched>;
> SELECT parameter_types::text FROM pg_prepared_statements WHERE name='chk';
> DEALLOCATE chk;
> ```
>
> The fixed statement reports `{text, timestamp with time zone, text}`; the old one raises the
> production error verbatim. Casting **every** use of a parameter explicitly (`$2::timestamptz`,
> and deriving the date as `($2::timestamptz)::date`) is what makes deduction unambiguous.

---

## Why this exists

There is **no employee screen anywhere in either app.** `isAdmin()` in `index.html` is a role
*gate*, not a panel — it hides buttons. The only employee endpoints are `handleEmployees`
(`inventory.js:142`, a name/role list for pickers) and `handleListEmployeesForScheduling`
(`airtable.js:6084`). Every actual change to a person — hiring, a raise, a role, turning off a
leaver — is made by opening the Airtable grid.

The owner's ask: *"a place to make it active and not active… so if someone quits I can remove
their permissions to login to the app."*

---

## ⚠ The three things that make this more than a form

### 1. Deactivating someone does NOT log them out. Not for up to 30 days.

This is the important finding, and it is the whole reason Slice 1 exists.

Session tokens are **stateless HMAC** (`_auth.js:41-65`). `verifyToken` checks a signature and an
expiry and **touches no database at all** — by design, it was built to add zero reads per request.
`TOKEN_TTL_MS` is **30 days** (`_auth.js:18`).

Both `handleLogin`s check `Active` (`airtable.js:1982`, `inventory.js:112`). So unchecking `Active`
in Airtable blocks a **new** login — and does nothing whatsoever to a phone that is already logged
in. A crew member who quits today keeps full field-app access, on the phone in their pocket, until
their token expires. Deleting the Airtable record doesn't help either; nothing re-reads it.

**A checkbox alone does not answer the owner's question.** It needs a revocation path — see
*Design: revocation* below.

### 2. Hire date and termination date do not exist. Anywhere.

Not in Airtable `Employees` (tblZHWkJrruPPxeUg), not in Neon `employees`. Today "former employee"
is `Active` unchecked, with no date, no reason, and no way to tell a leaver from someone whose
checkbox got knocked. These are new columns whichever way the migration goes.

### 3. Two role fields, and a dead email login

Live inconsistencies found while scoping, both cheap to fix and both belonging in this pass:

- **`Role` vs `Role New`.** The field app reads `F.emp.role` → `"Role"` (`airtable.js:124`). The
  inventory app reads `"Role New" || "Role"` (`inventory.js:129`). If the two ever disagree, the
  same person is a different role in the two apps. An admin screen that edits *one* of them and
  silently leaves the other is worse than no screen. **Converge on one field in this pass.**
- **`F.emp.email` is `"Email"` — that field does not exist.** The Airtable column is
  **`Primary Email`**. So `f["Email"]` is `undefined`, `normalize` (`airtable.js:601`) turns it
  into `""`, and since `identifier` is required non-empty, the email branch of the field-app login
  can never match. **Logging in by email has never worked.** One-line fix.

---

## What's there to build on

**Airtable `Employees` (tblZHWkJrruPPxeUg)** — Employee Name, First Name, Last Name, Username,
Role, Role New, Active, PIN, Employee ID, Primary Phone, Primary Email, Default Labor Type,
Current True Cost Rate (rollup), Notes, + links to Time Entries, Labor Cost Rates, Labor Billable
Rates, Expenses, Schedule Entries, Bonuses, Generator Service, and three labor-allocation tables.

**Neon `employees`** — deliberately thin: `id, airtable_id, name, username, role, active,
created_at, qb_user_id`. No PIN, no email, no phone, no first/last, no labor type.

**Neon `labor_cost_rates`** — fully mirrored and already the right shape for wage history:
`employee_id` (real FK), `labor_type`, `effective_start_date`, `effective_end_date`,
`base_hourly_wage`, `payroll_burden_pct`, `true_cost_rate`, `notes`. This is where pay lives —
**not** on the employee record. `Employees.Current True Cost Rate` is just a rollup of it.

---

## Design

### Where writes go (and the ETL clobber trap)

The app is half-migrated, so this screen writes to **two places on purpose**:

| Field group | Written to | Why |
|---|---|---|
| Name, username, role, **active**, PIN, phone, email, labor type | **Airtable** | Both `handleLogin`s read Airtable `Employees`. Writing `active` only to Neon would not stop a login. |
| `hired_on`, `terminated_on`, `termination_note`, `token_valid_from`, `last_login_at` | **Neon** | Airtable has no such columns, and these are app-internal. |

> ⚠⚠ **Do not move `active` to Neon early.** `db/etl/time-entries-full.mjs:241-247` is a **live
> dimension load** that upserts `employees` with `ON CONFLICT DO UPDATE SET name, username, role,
> active` from Airtable — and its own comment says these "stay live even when" the rest of the ETL
> is skipped. An `active=false` written into Neon is **erased by the next ETL run.** Same trap as
> the pCloud folder ids and the `expenses` mirror.
>
> The new columns above are safe: that upsert names its columns explicitly and does not touch them.

When login flips to Neon (ROADMAP §4, *"(Shared, last) — Login"*), the Airtable half of these
writes drops out and the dimension load is retired in the same commit. Not before.

### Revocation — how "not active" actually locks someone out

Add to Neon `employees`:

```sql
token_valid_from timestamptz   -- NULL for everyone normally; set = revoke
```

Deactivating a person sets `active = false` in Airtable **and** `token_valid_from = now()` in Neon,
in one handler. A new `assertNotRevoked()` in `_auth.js` compares the token's `iat` against it and
401s anything older.

Making that cheap: the check reads only
`SELECT airtable_id, token_valid_from FROM employees WHERE token_valid_from IS NOT NULL` —
a handful of rows, ever — into a **module-scoped cache with a 60 s TTL**. A warm function instance
therefore adds **one small query per minute**, not one per request. Worst-case lag between hitting
the toggle and the phone going dead is ~60 s.

Ship the same mechanism as a separate **"Force logout"** button (revoke without deactivating), for
the lost-phone case. It's the same column.

> **Owner decision needed — what happens if Neon is unreachable?**
> Recommendation: **fail soft** (allow the request, log a warning), matching the `_neon.js`
> contract that Neon being down must never take the app off the air. The cost is honest: during a
> full Neon outage, a revoked token would work again. Fail-closed is one line the other way, but it
> means a Neon blip logs out every crew member in the field. Recommend soft; say so out loud rather
> than discovering it later.

### PIN handling

The screen is **write-only** for PINs: "Set a new PIN", never "here is their PIN". PINs are still a
plaintext compare (`_auth.js:12-13` says so, and hashing is scheduled for the login flip). A
write-only UI does **not** entrench that — it means the hashing pass changes one write site and
zero read sites. **Do not add a "show PIN" affordance**, however convenient it sounds.

### Authorization

`_ADMIN` only, in `authzFor` (~`airtable.js:407`) — **not** `_ADMIN_OFFICE`. This screen exposes
wages. Office is deliberately excluded.

Two guards that are cheap now and painful later:

- **No self-lockout.** An admin cannot deactivate, demote, or revoke themselves. Deactivating the
  only admin bricks the panel.
- **Deactivation is not deletion.** Time entries, expenses, labor allocations, payroll history and
  bonuses all stay linked and keep showing in payroll. A former employee's hours are already
  handled — `handlePayrollBonuses` (`airtable.js:1659`) and the rollup at `:1780` already union
  "active" with "had hours/bonuses", precisely so leavers don't vanish mid-period. Don't break that.

---

## What the screen contains

**Roster** — admin-only `👥 People` tab. `Active` / `Former` toggle (default Active), search box.
Columns: Name · Role · Current true cost rate · Hired · Last login. A former employee's row shows
their termination date instead of last login.

**Detail pane** — one person:

| Group | Fields |
|---|---|
| Identity | First Name, Last Name, display name, Username, Employee ID |
| Contact | Primary Phone, Primary Email |
| Access | Role (one field — see problem 3), **Active toggle**, Set PIN, **Force logout**, Last login |
| Employment | Hired on, Terminated on, Termination note, Default Labor Type |
| Pay | Current true cost rate, then full wage history from `labor_cost_rates`: start, end, base wage, burden %, true cost, notes — current row highlighted |
| Activity (read-only) | Hours YTD, date of last time entry, recent jobs, open expenses |
| Notes | free text |

`last_login_at` is new — one `UPDATE` in `handleLogin`. It's the only way to notice an account
nobody has used in a year, which is exactly the account worth turning off.

---

## Slices

| # | What | Size |
|---|---|---|
| **1** | ✅ **BUILT 2026-08-08**, ⬜ needs prod smoke. `db/schema/016_employee_admin.sql` · `isSessionRevoked` in a new `_revocation.js` + 60 s cache (kept OUT of `_auth.js` so that file stays pure crypto with no I/O, and stays trivially testable) · wired into both functions' authz path · `handlePeople` · `handleSetEmployeeActive` (Neon revoke first and fails closed, then Airtable `Active`; self-lockout guard) · `👥 People` tab with the Active/Former toggle. **Answers the original ask.** | ~3-4 h |
| **2** | Edit identity/contact/role/labor type · Set PIN · hire + termination dates · `last_login_at` write · **converge `Role`/`Role New`** · fix `F.emp.email` → `Primary Email` · Force logout button | ~2-3 h |
| **3** | Wage history: read-only table first, then "add a raise" — which must **close the current row's `effective_end_date` and insert the new row in one transaction.** Get this wrong and every job's historical labor cost moves. Money-critical; see the true-labor-cost notes. | ~2-3 h |
| **4** | Add a new employee end-to-end (Airtable record + Neon row + first wage rate + PIN) | ~1-2 h |
| **5** | ⏸ **Self-service "forgot PIN"** — owner's intent 2026-08-08, *later*. See below. | ~5-6 h + setup |

Slices 2-4 are optional and independent. Slice 1 stands alone.

---

## Slice 5 (later) — self-service "forgot PIN"

Owner's stated intent 2026-08-08: *"later on I'll add [that] change with phone numbers or email."*
Deliberately deferred, not rejected. Admin reset (shipped) covers the need for a crew of 8 where
the owner is usually on site.

> ### ⚠ The blocker is data, not code
> **Not one of the 11 employee records has a Primary Email or a Primary Phone.** Both fields exist
> on the Airtable table and both are empty on every row, verified 2026-08-08. A reset flow has
> nowhere to send a code, so **populating those fields is the prerequisite** — and it is free,
> needs no code, and can be done in the Airtable grid today or through slice 2's edit form.
>
> Until they are filled in, this slice cannot start. Fill them in as people are onboarded and the
> blocker clears itself.

Then, roughly:

1. **Pick the channel. SMS beats email here** — field crew live on their phones and several have
   no work email at all. Cost is per-message and small at this volume.
2. Provider + env vars (Twilio or similar), added to `ensureEnv()`'s *optional* group like the R2
   keys — **fail soft**, so an unconfigured provider disables the "Forgot PIN" link rather than
   breaking login.
3. A `password_resets` table in Neon: single-use token, short TTL (10 min), tied to an employee id,
   consumed on use. Never reuse `token_valid_from` for this.
4. "Forgot PIN" on the login screen → matches identifier → texts a code → verify → set a new PIN
   through the **same** `handleSetEmployeePin` path, so the duplicate-PIN refusal and the
   sign-out-everywhere behaviour come along for free.
5. **Rate-limit it.** This is the first unauthenticated write the app would have; without a limit
   it is a free SMS pump and an account-enumeration oracle. Respond identically whether or not the
   identifier matched.

> **Do this at, or after, the PIN-hashing pass** (ROADMAP §4, login flip). A reset flow that writes
> plaintext PINs would have to be rewritten immediately afterwards, and hashing makes this slice
> *easier* — a reset is the natural way to set a hashed PIN, and `employeePin` (reveal) has to be
> retired at that point anyway since a hash cannot be un-hashed.

---

## Acceptance tests

Add to `tests/handlers.test.mjs` (offline, mocked) — one case per bullet:

1. A token minted **before** `token_valid_from` → **401**. Minted after → **200**.
2. `token_valid_from IS NULL` → unaffected (the normal case, and the regression that would take
   the whole app down).
3. Neon unreachable during the revocation check → request **succeeds** (fail-soft contract).
4. Admin deactivating themselves → **400**, and they stay active.
5. Deactivating a person does not alter their time entries, expenses, or payroll rows.
6. Non-admin (`office`, `employee`, `viewer`) hitting any `people*` action → **403**.
7. Slice 3: adding a raise leaves exactly one open-ended rate row for that employee.

Manual smoke (the one that matters, and the only proof that counts):
**Log in on a second device → deactivate that person from the People tab → within ~60 s, the second
device 401s and bounces to the login screen → and cannot log back in.**

---

## Traps, collected

1. **Stateless tokens** — a checkbox is not a lock. Without `token_valid_from`, deactivation is
   cosmetic for up to 30 days.
2. **The ETL dimension load overwrites `active`** (`db/etl/time-entries-full.mjs:241-247`). Neon is
   not the place to record deactivation until login flips.
3. **`Role` vs `Role New`** — the two apps read different fields.
4. **`F.emp.email` names a column that doesn't exist**; email login is dead code today.
5. **Pay is not on the employee record.** It's `labor_cost_rates` rows with effective dates. Editing
   a wage in place rewrites history; a raise is a close + an insert.
6. **Never display a PIN** — it keeps the future hashing pass to one write site.
7. **Leavers must keep appearing in payroll.** The bonus/hours rollups already union active with
   had-activity. Don't "clean that up".

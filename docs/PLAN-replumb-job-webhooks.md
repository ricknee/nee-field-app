# Plan — replumb the four job-lifecycle webhooks

**Status: 🟨 BUILT AND CUT OVER 1 OF 4 — 2026-08-12.** `JOB_WEBHOOKS=app` is live
(`f39fb22`, `netlify/functions/_job-webhooks.js`). Audit item 04. All four scripts read off the
live base; nothing below is inferred.

| # | Hook | App fires it | Airtable automation | Proven by |
|---|---|---|---|---|
| 1 | pCloud folders (Estimating) | ✅ | ✅ **UNDEPLOYED** | WatersEdge 1 → Estimating: **one** folder, flag set |
| 2 | Awarded → Trello + QB Time | ✅ | ✅ **UNDEPLOYED** 2026-08-12 | WatersEdge 1 awarded — **two Make executions**, see below |
| 3 | Completed → Trello by year | ✅ | ✅ **UNDEPLOYED** 2026-08-12 | cost of failure is cosmetic — a card doesn't move lists |
| 4 | Service call started | ✅ | ✅ **UNDEPLOYED** 2026-08-12 | cost of failure is cosmetic — a scenario doesn't run |

## ✅ ALL FOUR DONE — 2026-08-12. No Airtable automation triggers a job webhook any more.

### How #2 was proven, and the reasoning trap it exposed

The original plan said *"wait for a real award to prove it, then undeploy"* — **which is
circular.** While the automation is deployed, both it and the app POST to the same hook and
Make's flags dedupe, so a Trello card appears either way. You cannot tell which one produced it.
The proof would only have arrived *after* the thing it was meant to justify.

**Make's execution log breaks the circle.** Each POST starts its own scenario run, so both are
visible even though only one does work. WatersEdge 1, awarded 2026-08-12:

| Time | Operations | What it was |
|---|---|---|
| `10:03:39.545` | **8 ops**, 19 KB | did the work — Trello card + QuickBooks Time job |
| `10:03:43.116` | **3 ops**, 6 KB | arrived 3.5 s later, no-opped on the flags |

Two runs = the app's POST arrived and Make accepted it. **Use this technique for any future
replumb**: keep the old trigger, fire the new one alongside, and count executions. It gives
proof without a leap of faith and without test debris in Trello or QuickBooks Time.

### ⚠ The bug the same award uncovered

Awarding WatersEdge 1 was the first time anyone had walked the status-change path since
`handleJobs` went Neon-first. `handleUpdateJobStatus` wrote **Airtable only**, so the status
reverted on refresh — the fourth instance of "flip a read without its write" in this project.
Fixed in `ff21d46`, along with `handleStartServiceCall` and `handleCompleteServiceCall`, which
had the identical shape and had never been exercised either.

**Nothing about the webhook work caused it. The webhook work is what walked the road.**

**The doubled state is safe and can sit indefinitely.** While both fire, Make's own guard flags
make the second call a no-op. So 2–4 need no test jobs forced through — undeploy each as a real
job happens to hit that status. **Do not undeploy one before seeing its replacement fire.**

> ⚠ **#1 has changed risk posture now.** Until today a failure meant "the automation didn't run";
> from today it means **no pCloud folder is created at all**, because nothing else is listening.
> The app's POST failure is logged (`job-webhook pcloud FAILED`) but does not fail the status
> change, by design — so a silent miss looks like a job with no folder rather than an error.
> Worth a glance the first few times.

**Rollback:** `netlify env:unset JOB_WEBHOOKS` + rebuild, and re-publish `wfltqVP8ORwHh2Mnx` —
its configuration is intact, undeployed rather than deleted.

**Why it matters:** these four are the reason the Airtable mirror writes still exist. Item 10 —
dropping the mirrors, the step that actually ends Airtable's role — is gated on them.

---

## 1. The four, as they actually are

| # | Automation | Fires when | Hook | Payload |
|---|---|---|---|---|
| 1 | `wfltqVP8ORwHh2Mnx` pCloud folders | status = **Estimating** AND `Automation – pCloud Folders Created` = false | `cd41jmwo…` | `{event:"create_pcloud_folders", recordId, jobName, jobPO, contractor, year}` |
| 2 | `wfl2KJpZRPK1tDz5D` Awarded → Trello + QB Time | status = **Awarded** AND Trello-created = false AND TSheets-created = false | `br272oam…` | `{recordId, jobName, jobPO (Locked), jobType, contractor, jobAddress, trelloCreated, tsheetsCreated}` |
| 3 | `wflP3hvinWk4saqmX` Completed → Trello by year | status = **Completed** AND `fldewPWukfRLkgDCa` = false | `is3nj997…` | `{recordId}` |
| 4 | `wflMovlr8seWxSUul` Service Call | `Start Service Call` = true AND Job Type = Service Call | `gpvkreyo…` | `{recordId}` |

## 2. ⚠⚠ THE FINDING THAT CHANGES THE PLAN

The audit scopes 04 as *"each is a `fetch` to an existing hook with a matched payload"*. That is
true of the **caller** and it is not the whole job.

**Three of the four send little more than `recordId`.** Make then reads the job back **out of
Airtable** to do its work. So replumbing the caller moves the *trigger* out of Airtable while
leaving Make's *read* firmly inside it.

| # | Payload carries | Can Make work without Airtable? |
|---|---|---|
| 1 | name, PO, contractor, year | **Yes** — everything it needs is in the payload |
| 2 | name, PO, type, contractor, address, both flags | **Yes** |
| 3 | `recordId` only | **No** — must look the record up |
| 4 | `recordId` only | **No** — must look the record up |

**Consequence for item 10:** dropping the job mirror writes would break scenarios 3 and 4 even
after 04 is "done", because the rec id they receive would point at a record that no longer gets
written. Item 10 must therefore also enrich those two payloads — **which is a Make-side edit, not
a code change here.**

This is the same shape as the trap that cost a week of billing: *retiring Make from a path stops
minting the ids other paths key on.* Here it is one step further out — the ids still exist, but
only because we keep writing them.

## 3. Idempotency is not uniform, and that matters

Each has a guard, and they are guarded in three different places:

- **#1 writes its own flag back** (`Automation – pCloud Folders Created = true`) after a
  successful POST. A replumb **must** keep writing it, or every status re-save creates another
  set of pCloud folders.
- **#2 passes both flags in the payload** and lets Make decide and set them.
- **#3 and #4 have their flags only in the trigger condition** — the script does not write them,
  so Make does.

**So "the trigger condition" is doing real work in three of four cases.** A naive replumb that
just POSTs on every status change would fire repeatedly. Whatever calls these must reproduce the
*condition*, not only the call.

## 4. What to build

In `handleUpdateJobStatus` (and `handleStartServiceCall` for #4), after the status write lands:

1. Re-read the job's current flags from Neon.
2. If the new status matches and the flag is false → POST the same payload to the same hook.
3. For #1 only, write the flag back — to **both** stores, since Make's own read of it is what
   stops a duplicate run.
4. Undeploy the corresponding automation **only after** its replacement has been seen to fire.

Ship behind an env switch (`JOB_WEBHOOKS` = unset | `app`) so the code lands inert and the
automations stay authoritative until the switch flips — the pattern used for
`ALLOCATIONS_WRITE`.

> ⚠ **Do not undeploy all four at once.** They fire on different, infrequent events (a job
> reaching Estimating, being Awarded, being Completed). A mistake is invisible until the next job
> happens to hit that status, which could be days. Move one, watch it fire once for real, then
> move the next.

## 5. What "done" looks like

- A job moved to Estimating creates its pCloud folders exactly once, with the flag set.
- A job moved to Awarded produces a Trello card and a QuickBooks Time job, once.
- A job Completed lands in the right Trello "Completed by year" list.
- Starting a service call still triggers its scenario.
- All four Airtable automations undeployed, **not deleted** — they are the only remaining
  specification of the payloads, exactly as the billing-allocation four were.

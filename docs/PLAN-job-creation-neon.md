# Plan — job creation → Neon, with PO numbering

**Status: DESIGN DONE 2026-08-12, NOT BUILT.** Audit item 05. Everything below was read off the
live base, not inferred.

**One-line:** `handleCreateJob` POSTs to Airtable and returns; the job reaches Neon up to an hour
later via `_jobs-sync.js`, which is why a new job shows an empty Time Entries tab for its first
hour.

---

## 1. ⚠ THE AUDIT'S GATE IS WRONG — 05 does NOT depend on 04

`AUDIT-airtable-remaining.md` says item 05 is *"Gated on 04"*, on the reasoning that creating a
job fires the pCloud folder webhook. **It does not.** Checked against the live automations:

| Automation | Fires when | Deployed |
|---|---|---|
| `wfltJAiEaavVLA0wB` **Assign PO Number to New Jobs** | status = **New Lead** AND PO empty | ✅ |
| `wfldmUmT6uBbJFgnu` **Update Contractor Field** | contractor link not empty | ✅ |
| `wflGOWii6JG6qpk21` **Create Contact from Job Intake** | intake flag = 1 | ✅ |
| `wfltqVP8ORwHh2Mnx` **→ pCloud (Estimating)** | status = **Estimating** | ✅ |
| `wfliIxFjBS50fbKwX` **Fill Job PO - Locked** | status = **Estimating** AND locked PO empty | ✅ |

`handleCreateJob` sets `Job Status = "New Lead"`. The pCloud webhook and the PO-locked fill both
trigger on **Estimating**, a later status change — `handleUpdateJobStatus` territory, which is
04's job. **So 05 can be built and shipped before 04.**

## 2. PO numbering, decoded

A counter table, `tbl8s6L1i6wotlEsn`, one row per year:

| field | meaning | 2026 row |
|---|---|---|
| `fldtG0ZdTJBm1HBtI` | year, as text | `"2026"` |
| `fldo1fgaylm7cBAWF` | last used | `285` |
| `fldz2HUm5uctDXIQ3` | next (a formula, last + 1) | `286` |

The automation: find the row for the year → write `next` onto the job's `fldHXB9IXBdEQNWHM` →
write the same value back into `last used`, so the formula rolls forward.

**It is a read-then-write with no lock.** Two jobs created in the same instant can both read 286.
Airtable has lived with that because jobs are created by one person at a time.

### ⚠⚠ The rule that must survive: DO NOT derive the next PO from the jobs table

112 jobs carry numbers from **102 to 436**, and **22 of them sit at or above the counter's next
value**. Those are hand-assigned — the Dollar General jobs use the general contractor's own
numbering (`Adena DG (31614) (PEA 435)`, `Bethel School (MIB 433)`). So `max(po) + 1` would jump
the sequence to 437 and abandon 150 unused numbers.

**The counter is the authority; the jobs table is not.** Verified healthy 2026-08-12: last used
285, next 286, and **no job holds 286** — the sequence and the counter are in step.

### What the number is, and is not

`fldHXB9IXBdEQNWHM` is a plain integer (433). The `MIB 433` you see is assembled elsewhere:
`fldDFQSF2jJmCDWB4` (*PO — Locked*) holds the full string `"Bethel School (MIB 433)"` and is
filled at **Estimating**, not at creation. Neon's `jobs.po` / `jobs.po_locked` carry that display
string, **not** the number — so Neon has no column for the numeric PO yet. That is the one schema
addition this needs.

## 3. What to build

1. **`job_po_counters`** in Neon — `(year int primary key, last_used int)`. Allocation is
   `UPDATE … SET last_used = last_used + 1 WHERE year = $1 RETURNING last_used`, which is atomic
   under Postgres row locking and fixes the race Airtable has always had.
2. **Seed it from Airtable** — 2025 → 99, 2026 → 285, 2027 → 99. Diff before trusting.
3. **`jobs.po_number int`** — the numeric PO Neon currently lacks.
4. **`handleCreateJob`** writes Neon first, allocates the PO, then mirrors to Airtable **with the
   PO already set** — which is what stops the automation firing, since its trigger requires the
   PO field to be *empty*. No undeploy needed on day one.
5. Once soaked, undeploy `wfltJAiEaavVLA0wB`.

> ⚠⚠ **THE COLLISION RISK, and why step 4 is shaped that way.** If the app allocates a PO *and*
> the automation is still live *and* the mirror writes an empty PO, both assign and the job ends
> up with two different numbers — or two jobs share one. Writing the PO in the same POST makes
> the automation's `isEmpty` condition false, so it stands down on its own. That is a safer
> cutover than undeploying first and hoping nothing creates a job in the gap.

> ⚠ Keep the Airtable mirror. Everything downstream of a job — contractor field, contact
> creation, and later the Estimating-time pCloud folder — still triggers off the Airtable record.
> This slice moves *where the job is born*, not *where it lives*.

## 4. Ship it inert

Same pattern as `ALLOCATIONS_WRITE` and `TIME_CLOCK_PAYROLL`: an env switch, off by default, so
the code can land and be smoke-tested before it owns job identity. `JOB_CREATE_SOURCE` =
unset/`airtable` (today) | `neon`.

## 5. The gate before it counts

PO numbers **are** job identity — they appear on invoices, in pCloud folder names, on Trello
cards and in QuickBooks Time. A duplicate is not a display bug. Before flipping:

- create a job with the switch off → PO assigned by Airtable, as today;
- create one with it on → PO assigned by Neon, counter advances by exactly 1, Airtable record
  carries the same number, automation did not fire;
- confirm `SELECT count(*) FROM jobs GROUP BY po_number HAVING count(*) > 1` is empty.

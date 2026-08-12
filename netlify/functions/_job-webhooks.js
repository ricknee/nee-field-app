// ── Job-lifecycle webhooks, fired by the app ───────────────────────────────
// Added 2026-08-12. Audit item 04. Replaces the CALLER of four deployed
// Airtable automations, each of which runs a script that POSTs to a Make hook:
//
//   wfltqVP8ORwHh2Mnx  Estimating  -> pCloud folders
//   wfl2KJpZRPK1tDz5D  Awarded     -> Trello card + QuickBooks Time job
//   wflP3hvinWk4saqmX  Completed   -> Trello "Completed by year"
//   wflMovlr8seWxSUul  Service call started
//
// Every payload and condition below was transcribed from the live scripts, not
// reconstructed. Full decode and the scope finding: docs/PLAN-replumb-job-webhooks.md.
//
// ⚠⚠ THIS DOES NOT FREE MAKE FROM AIRTABLE, and the plan says so at length.
// Scenarios 3 and 4 receive only `recordId` and read the job back out of
// Airtable, so item 10 (dropping the job mirror writes) still needs their Make
// payloads enriched first. What this changes is who TRIGGERS them.
//
// ⚠⚠ SHIPS INERT. `JOB_WEBHOOKS=app` must be set, and each automation should be
// undeployed only after its replacement has been SEEN to fire once for real.
// They fire on infrequent events — a job reaching Estimating, being Awarded —
// so a mistake stays invisible until the next job happens to hit that status,
// which can be days. Move one at a time.

const HOOKS = {
  pcloud:      "https://hook.us1.make.com/cd41jmwojyuhehlap05p2va1lcnnx5vz",
  awarded:     "https://hook.us1.make.com/br272oamyugrnnaq64xdfaci6pjr8way",
  completed:   "https://hook.us1.make.com/is3nj9971v5hj3hp5dr7df229k7moet9",
  serviceCall: "https://hook.us1.make.com/gpvkreyon7i2azvv47sk29e6ivgxe2ut",
};

// Field NAMES, because an Airtable PATCH response is keyed by name unless
// returnFieldsByFieldId is set. The automations used ids; this is the same
// field either way. ⚠ The three "Automation – …" names contain an EN DASH (–),
// not a hyphen. Getting that wrong reads undefined, which is falsy, which fires
// the webhook every single time.
const N = {
  status:        "Job Status",
  jobName:       "Job Name",
  jobPO:         "Job PO",
  jobPOLocked:   "Job PO - Locked",
  jobType:       "Job Type",
  contractor:    "Contractor",
  address:       "Job Address - Full",
  pcloudDone:    "Automation – pCloud Folders Created",
  trelloDone:    "Automation – Trello Created",
  tsheetsDone:   "Automation – TSheets Created",
  completedDone: "Automation – Trello Completed",
  startService:  "Start Service Call",
};

export function jobWebhooksEnabled() {
  return String(process.env.JOB_WEBHOOKS || "").toLowerCase() === "app";
}

// Airtable returns a linked-record field as an array of ids, or as a string
// when the script asked for it as one. getCellValueAsString() in the automations
// yields the display name; over REST we get ids. Neither is reliably a plain
// string here, so normalise and let the caller pass a better value if it has one.
const str = (v) => {
  if (v === undefined || v === null) return "";
  if (Array.isArray(v)) return v.map(x => (x && typeof x === "object" ? (x.name ?? x.id ?? "") : x)).join(", ");
  if (typeof v === "object") return String(v.name ?? v.id ?? "");
  return String(v);
};

// One POST, with the failure contained. A webhook that doesn't fire is a missing
// Trello card — annoying and re-runnable. A webhook that throws would fail the
// STATUS CHANGE the user actually asked for, which is worse: the job would look
// unsaved when it is saved.
async function post(hook, payload, label) {
  try {
    const r = await fetch(hook, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!r.ok) throw new Error(`${r.status}`);
    return { fired: label };
  } catch (e) {
    console.error(`job-webhook ${label} FAILED (job saved anyway): ${e?.message || e}`);
    return { failed: label, error: String(e?.message || e) };
  }
}

// Called after a job's status write lands, with the Airtable record the PATCH
// returned — so the flags below are the CURRENT ones, post-write, exactly what
// the automation's trigger condition would have evaluated.
//
// Returns a report rather than throwing. `null` means the switch is off.
export async function fireJobStatusWebhooks(record, atFetch) {
  if (!jobWebhooksEnabled()) return null;
  const f = record?.fields || {};
  const status = str(f[N.status]);
  const out = [];

  // ── 1. Estimating → pCloud folders ──────────────────────────────────────
  // ⚠ The flag write-back is NOT optional. This automation sets
  // "Automation – pCloud Folders Created" itself after a successful POST, and
  // that flag is the only thing standing between a re-saved status and a second
  // set of folders in pCloud. Make does not set it for us.
  if (status === "Estimating" && f[N.pcloudDone] !== true) {
    const r = await post(HOOKS.pcloud, {
      event: "create_pcloud_folders",
      recordId: record.id,
      jobName: str(f[N.jobName]),
      jobPO: str(f[N.jobPO]),
      contractor: str(f[N.contractor]),
      year: new Date().getFullYear().toString(),
    }, "pcloud");
    out.push(r);
    if (r.fired) {
      try {
        await atFetch(`Jobs/${record.id}`, {
          method: "PATCH",
          body: JSON.stringify({ fields: { [N.pcloudDone]: true } }),
        });
      } catch (e) {
        // Loud: the folders now exist and the guard does not. The next status
        // save would create them again.
        console.error(`job-webhook pcloud: folders created but FLAG NOT SET on ${record.id} — ${e?.message || e}`);
        out.push({ failed: "pcloud-flag", error: String(e?.message || e) });
      }
    }
  }

  // ── 2. Awarded → Trello + QuickBooks Time ───────────────────────────────
  // Both flags must be false, matching the trigger. The flags are PASSED to
  // Make rather than written here — Make decides which half still needs doing
  // and sets them itself. Do not "helpfully" set them.
  if (status === "Awarded" && f[N.trelloDone] !== true && f[N.tsheetsDone] !== true) {
    out.push(await post(HOOKS.awarded, {
      recordId: record.id,
      jobName: str(f[N.jobName]),
      jobPO: str(f[N.jobPOLocked]),          // ⚠ the LOCKED PO here, not Job PO
      jobType: str(f[N.jobType]),
      contractor: str(f[N.contractor]),
      jobAddress: str(f[N.address]),
      trelloCreated: f[N.trelloDone] === true,
      tsheetsCreated: f[N.tsheetsDone] === true,
    }, "awarded"));
  }

  // ── 3. Completed → Trello "Completed by year" ───────────────────────────
  // recordId ONLY — Make reads the rest out of Airtable. See the plan's §2:
  // this one cannot survive the mirror writes going away without a Make edit.
  if (status === "Completed" && f[N.completedDone] !== true) {
    out.push(await post(HOOKS.completed, { recordId: record.id }, "completed"));
  }

  return out.length ? out : [];
}

// ── 4. Service call started ────────────────────────────────────────────────
// Its own entry point because the trigger is not a status: "Start Service Call"
// checked AND Job Type = Service Call. Also recordId-only.
export async function fireServiceCallWebhook(record) {
  if (!jobWebhooksEnabled()) return null;
  const f = record?.fields || {};
  if (f[N.startService] !== true) return [];
  if (str(f[N.jobType]) !== "Service Call") return [];
  return [await post(HOOKS.serviceCall, { recordId: record.id }, "service-call")];
}

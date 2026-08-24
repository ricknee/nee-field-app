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

// Needed to resolve linked-record fields to their display NAMES — see the note
// in fireJobStatusWebhooks. Read-only and fails soft, like every other consumer.
import { neonQuery, neonWrite } from "./_neon.js";
import { signScope } from "./_auth.js";

// Where Make posts its result back to. Netlify sets URL to the site's primary
// address on every build, so this needs no configuration and follows the site if
// it ever moves. The fallback is only for local `netlify dev`.
const SELF_URL = process.env.URL || "https://hub.northeasternelec.com";

// 24 h, not the scope default. Make retries a failed run later, and a token that
// expired in the meantime would silently drop the result — leaving the ids and
// the run-once flags unwritten, which is exactly the state that creates a second
// Trello card next time.
const CALLBACK_TTL_MS = 24 * 60 * 60 * 1000;

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
  // Read-only fallbacks for the Completed payload, used when Neon has not yet
  // carried a value over. Airtable is still the mirror, so it usually has them.
  completedAt:    "Project Completed At",
  trelloCardId:   "Trello Card ID",
  trelloPoCardId: "Trello Card PO ID",
  tsheetsJobId:   "TSheets Job ID",
  notes:          "Notes",
  serviceCallCreated: "Service Call Created",
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

  // ⚠⚠ LINKED FIELDS COME BACK AS RECORD IDS OVER REST — the automations used
  // `getCellValueAsString()`, which renders the DISPLAY NAME.
  //
  // This is the project's own documented trap (ROADMAP §8: "an Airtable LOOKUP
  // returns record IDs over the REST API, but renders as a display name inside
  // a formula"), and it cost a live failure on 2026-08-12: `Contractor` arrived
  // as `["rec84ohJ7tgrxp8tX"]`, we sent Make `"rec84ohJ7tgrxp8tX"` where the
  // automation had always sent `"Misc Jobs"`, and scenario 4509211 rejected the
  // bundle — 2 operations instead of 24, no pCloud folders for Test 2.
  //
  // Neon already stores the resolved name, so take it from there rather than
  // resolving the link with another Airtable round trip. Falls back to whatever
  // Airtable gave us, which is no worse than the broken behaviour it replaces.
  // ⚠ THE JOIN IS NOT DECORATION. The Awarded scenario read a SECOND Airtable
  // record — the Companies row — purely for three per-contractor configuration
  // values: which QuickBooks Time customer to hang the jobcode under, and which
  // two Trello lists to file the cards in. Sending them makes that read
  // deletable, which is the whole point of db/schema/044.
  let neonJob = null;
  try {
    const q = await neonQuery(
      `SELECT j.contractor_name, j.po, j.po_locked, j.address_full,
              j.pcloud_folders_created, j.trello_created, j.tsheets_created,
              j.trello_completed,
              j.project_completed_at::text AS project_completed_at,
              j.trello_card_id, j.trello_po_card_id, j.tsheets_job_id,
              c.tsheets_group_id, c.trello_list_id, c.trello_list_job_po_id
         FROM jobs j
         LEFT JOIN companies c ON c.airtable_id = j.contractor_at_id
        WHERE j.airtable_id = $1 OR j.id::text = $1`,
      [record?.id]);
    neonJob = q?.rows?.[0] || null;
  } catch (e) {
    console.error(`job-webhook: Neon lookup failed, falling back to Airtable values — ${e?.message || e}`);
  }
  const contractorName = neonJob?.contractor_name || str(f[N.contractor]);

  // ── THE RUN-ONCE GUARDS, read from BOTH stores ──────────────────────────
  // Done if EITHER says done. Not a hedge — the two failure modes are wildly
  // asymmetric. Firing twice bills a second QuickBooks Time jobcode and leaves a
  // duplicate Trello card on the board; failing to fire is a missing card that
  // anyone can re-trigger by re-saving the status. So the expensive mistake is
  // the one we refuse to make.
  //
  // ⚠ A NEON NULL IS "UNKNOWN", NOT "NOT DONE". The columns land empty
  // (db/schema/045) and fill on the next hourly sync, so treating NULL as false
  // during that window would re-fire every job somebody happened to re-save.
  // Reading it as `=== true` gives exactly that: unknown contributes nothing and
  // the Airtable value decides.
  const done = (neonVal, atVal) => neonVal === true || atVal === true;
  const pcloudDone    = done(neonJob?.pcloud_folders_created, f[N.pcloudDone]);
  const trelloDone    = done(neonJob?.trello_created,         f[N.trelloDone]);
  const tsheetsDone   = done(neonJob?.tsheets_created,        f[N.tsheetsDone]);
  const completedDone = done(neonJob?.trello_completed,       f[N.completedDone]);

  // ── 1. Estimating → pCloud folders ──────────────────────────────────────
  // ⚠ The flag write-back is NOT optional. This automation sets
  // "Automation – pCloud Folders Created" itself after a successful POST, and
  // that flag is the only thing standing between a re-saved status and a second
  // set of folders in pCloud. Make does not set it for us.
  if (status === "Estimating" && !pcloudDone) {
    const r = await post(HOOKS.pcloud, {
      event: "create_pcloud_folders",
      recordId: record.id,
      jobName: str(f[N.jobName]),
      // `Job PO` is a FORMULA, so REST returns the rendered string — safe. Neon's
      // `po` is the same value and is used only if Airtable withheld it.
      jobPO: str(f[N.jobPO]) || str(neonJob?.po),
      contractor: contractorName,
      year: new Date().getFullYear().toString(),
    }, "pcloud");
    out.push(r);
    if (r.fired) {
      // Neon FIRST, and not inside the try below: this is the guard the app will
      // read once Airtable's copy stops being maintained. Fails soft — the
      // Airtable write underneath still guards today, and the hourly sync
      // repairs Neon from it.
      try {
        await neonWrite("job.pcloudFlag",
          `UPDATE jobs SET pcloud_folders_created = true, synced_at = now() WHERE airtable_id = $1 OR id::text = $1`,
          [record.id]);
      } catch (e) {
        console.error(`job-webhook pcloud: Neon flag not set on ${record.id} — ${e?.message || e}`);
      }
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
  if (status === "Awarded" && !trelloDone && !tsheetsDone) {
    // The three ids below are what module 6 ("Get Companies ID") existed to
    // fetch. With them in the payload that module can be deleted and the
    // scenario stops reading Airtable for configuration.
    //
    // ⚠ A MISSING ID IS NOT A NEW FAULT, but it is now visible. Seven companies
    // have neither id in Airtable either (JC Herbert, Marco Construction and the
    // five added since May), so awarding one of their jobs would have handed Make
    // a blank list id and failed there instead. None of the seven has a job yet.
    // Log it here rather than let it surface as a Make error nobody reads.
    if (!neonJob?.tsheets_group_id || !neonJob?.trello_list_id) {
      console.error(`job-webhook awarded: ${contractorName || record.id} is missing ` +
        `tsheets_group_id/trello_list_id — QuickBooks Time or Trello will reject this one ` +
        `(fill them in on the company, see db/schema/044)`);
    }
    out.push(await post(HOOKS.awarded, {
      recordId: record.id,
      jobName: str(f[N.jobName]),
      jobPO: str(f[N.jobPOLocked]) || str(neonJob?.po_locked),  // ⚠ LOCKED PO, not Job PO
      jobType: str(f[N.jobType]),
      contractor: contractorName,            // ⚠ the NAME — see the note above
      jobAddress: str(f[N.address]) || str(neonJob?.address_full),
      trelloCreated: trelloDone,
      tsheetsCreated: tsheetsDone,
      // Per-contractor automation config — replaces Make's second Airtable read.
      tsheetsGroupId:     str(neonJob?.tsheets_group_id),
      trelloListId:       str(neonJob?.trello_list_id),
      trelloListJobPoId:  str(neonJob?.trello_list_job_po_id),
      // ── Where Make reports back, replacing its two Airtable WRITE modules ──
      // Those modules record the new ids and set the run-once flags. Posting the
      // same facts here puts them in Neon instead — and immediately, which also
      // ends the hour-long wait before a new job's Trello card id is known.
      //
      // The token is signed for THIS job and nothing else, so the endpoint needs
      // no session and no shared secret in Make. Same shape as the clock widget.
      callbackUrl: `${SELF_URL}/.netlify/functions/airtable`,
      callbackToken: signScope(["jobAutomation", record.id], CALLBACK_TTL_MS),
    }, "awarded"));
  }

  // ── 3. Completed → Trello "Completed by year" ───────────────────────────
  // recordId ONLY — Make reads the rest out of Airtable. See the plan's §2:
  // this one cannot survive the mirror writes going away without a Make edit.
  if (status === "Completed" && !completedDone) {
    // Everything module 2 (airtable:ActionGetRecord) existed to fetch. With these
    // in the payload the Completed scenario reads nothing from Airtable, and the
    // final ActionUpdateRecords becomes the same callback the Awarded one uses.
    //
    // ⚠⚠ `completedYear` REPLACES A HARDCODED "Completed - 2026". Module 12
    // matched the Trello list by that literal string, so on 2026-01-01 every
    // completed job would have quietly found no list and moved nowhere — the same
    // shape as the Airtable PO automation's hardcoded year that item 05 fixed.
    //
    // ⚠ The card description drops the third line. It was
    // `{{2.Photos (Mobile)}}`, and NO SUCH FIELD EXISTS on the record — the real
    // ones are "Add Photos (Mobile)" and "View pCloud Photos" — so it has always
    // rendered blank. Porting it as blank keeps the output identical; adding a
    // real photo link would be a behaviour change and belongs in its own commit.
    const completedAt = str(neonJob?.project_completed_at) || str(f[N.completedAt]);
    out.push(await post(HOOKS.completed, {
      recordId: record.id,
      jobPO: str(f[N.jobPOLocked]) || str(neonJob?.po_locked),
      jobAddress: str(f[N.address]) || str(neonJob?.address_full),
      projectCompletedAt: completedAt,
      completedYear: (completedAt || "").slice(0, 4) || new Date().getFullYear().toString(),
      trelloCardId:   str(neonJob?.trello_card_id)    || str(f[N.trelloCardId]),
      trelloPoCardId: str(neonJob?.trello_po_card_id) || str(f[N.trelloPoCardId]),
      tsheetsJobId:   str(neonJob?.tsheets_job_id)    || str(f[N.tsheetsJobId]),
      trelloCompleted: completedDone,
      callbackUrl: `${SELF_URL}/.netlify/functions/airtable`,
      callbackToken: signScope(["jobAutomation", record.id], CALLBACK_TTL_MS),
    }, "completed"));
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

  // Everything modules 2 and 6 (the two airtable:ActionGetRecord) supplied. This
  // scenario is the biggest of the four — it builds a seven-folder pCloud tree,
  // a QuickBooks jobcode and two Trello cards — and every one of those reads a
  // value that now travels in the payload instead.
  let j = null;
  try {
    const q = await neonQuery(
      `SELECT j.po, j.contractor_name, j.address_full, j.notes,
              j.customer_first_name, j.customer_last_name, j.customer_phone,
              j.trello_created, j.tsheets_created, j.service_call_created,
              c.tsheets_group_id, c.trello_list_id, c.trello_list_job_po_id
         FROM jobs j
         LEFT JOIN companies c ON c.airtable_id = j.contractor_at_id
        WHERE j.airtable_id = $1 OR j.id::text = $1`, [record.id]);
    j = q?.rows?.[0] || null;
  } catch (e) {
    console.error(`job-webhook service-call: Neon lookup failed — ${e?.message || e}`);
  }

  // ⚠ A RUN-ONCE GUARD THAT NEVER EXISTED. Nothing filtered on "Service Call
  // Created", so re-ticking Start Service Call rebuilt the whole pCloud tree,
  // minted a second QuickBooks jobcode and made two more Trello cards. The
  // scenario wrote the flag and then nobody read it. Same both-stores rule as
  // the status webhooks: done if EITHER store says done.
  if (j?.service_call_created === true || f[N.serviceCallCreated] === true) {
    console.log(`job-webhook service-call: ${record.id} already has one — not firing again`);
    return [];
  }

  // ⚠ `Job PO`, NOT `Job PO - Locked`. This scenario names its folders, its
  // jobcode and its cards from the unlocked PO — the Awarded one uses the locked
  // value. They are usually the same string and are not the same field.
  const jobPO = str(f[N.jobPO]) || str(j?.po);
  return [await post(HOOKS.serviceCall, {
    recordId: record.id,
    jobPO,
    contractor:   str(j?.contractor_name) || str(f[N.contractor]),
    jobAddress:   str(f[N.address]) || str(j?.address_full),
    customerFirstName: str(j?.customer_first_name),
    customerLastName:  str(j?.customer_last_name),
    customerPhone:     str(j?.customer_phone),
    notes:        str(j?.notes) || str(f[N.notes]),
    tsheetsGroupId:    str(j?.tsheets_group_id),
    trelloListId:      str(j?.trello_list_id),
    trelloListJobPoId: str(j?.trello_list_job_po_id),
    trelloCreated:  j?.trello_created  === true || f[N.trelloDone]  === true,
    tsheetsCreated: j?.tsheets_created === true || f[N.tsheetsDone] === true,
    // The card's "Add Photos" line was an Airtable FORMULA read back mid-run.
    // Built here instead so Make needs no round trip. ⚠ The sibling lines for
    // Wire/Pipe (Mobile) are gone on purpose: those fields no longer exist —
    // the JotForm wire/pipe path was retired — so they rendered blank anyway.
    addPhotosLink: jobPO
      ? `📸 Add Photos\nhttps://form.jotform.com/260246511955053?jobPo=${encodeURIComponent(jobPO)}`
      : "",
    callbackUrl: `${SELF_URL}/.netlify/functions/airtable`,
    callbackToken: signScope(["jobAutomation", record.id], CALLBACK_TTL_MS),
  }, "service-call")];
}

// ── The fabricated-record guard ────────────────────────────────────────────
// Every Airtable write in this codebase funnels through an `atFetch` — three
// copies of it (airtable.js, inventory.js, _jobs.js/makeAtFetch), all the same
// shape. This is the one place that sees every one of them, which is why the
// guard lives here rather than at each call site.
//
// ── WHAT IT PREVENTS, AND IT IS NOT HYPOTHETICAL ───────────────────────────
// `typecast: true` does NOT reject an unrecognised value in a linked-record
// field. It CREATES the record. On 2026-08-25 an invoice mirror sent
// `fields["Job"] = ["846245ef-294f-423b-a2b1-4b4a919607f8"]` and Airtable
// obligingly made a Job named after the uuid — which `_jobs-sync` would then
// have imported as a real job, and whose display name Airtable's Invoice Number
// formula wrote back over the invoice. One line of payload, two corrupted
// stores, no error anywhere.
//
// The nine job-link sites were fixed individually with `jobLink()`. This exists
// because that list is not closed: employees, vendors, companies and contacts
// all still mint rec ids today, and the day any of them goes native, every
// linked-record write that carries their handle becomes the same bug. A guard
// at the choke point covers code that has not been written yet.
//
// ── WHY IT STRIPS RATHER THAN THROWS ───────────────────────────────────────
// These writes are MIRRORS. The Neon row is already the authority and has
// already been written; failing the request would only lose the courtesy copy.
// So the field is dropped, the rest of the mirror lands, and the console carries
// the detail — loud enough to find, quiet enough not to break a save that
// actually succeeded.
//
// ⚠ A field whose array becomes EMPTY is deleted, never sent as `[]`. An empty
// array is not "leave alone" to Airtable — it CLEARS the link. Several call
// sites deliberately send `[]` to clear a link; those carry no uuid and are
// untouched.

// Canonical v4-ish uuid shape. Deliberately not anchored to version bits: the
// point is "this is a Neon handle, not a rec id", and every id this app mints is
// a Postgres uuid.
export const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ── THE MIRROR KILL SWITCH ─────────────────────────────────────────────────
// `AIRTABLE_WRITES` — unset or `on` (today) = mirrors are written. `off` = every
// POST/PATCH/PUT/DELETE through any atFetch is skipped.
//
// It is an env var and not a code change for the same reason LOGIN_SOURCE and
// JOB_CREATE_SOURCE are: it moves 65 write sites across ~40 handlers at once,
// and moving back takes seconds with no rebuild. Editing 40 handlers to achieve
// the same thing would be 40 chances to get one wrong.
//
// ⚠ SAFE ONLY BECAUSE EVERY REMAINING WRITE IS A MIRROR. Verified 2026-08-25 by
// call graph, not by grep — the grep pass produced four false positives
// (handleAddLiftExpense and handleAddGeneralExpense call createExpenseNative
// through an imported helper, and 502 if Neon fails). Billing allocations were
// the one genuine exception, Airtable-FIRST until the same day; they are
// Neon-native now. Before flipping this, re-answer that question for anything
// added since: is Neon written first, and does the caller need the response?
//
// ⚠⚠ ONE PATH STILL READS ITS OWN WRITE BACK: createJobRecord's non-native
// branch POSTs the job and then re-reads the record for Airtable's computed
// `Job PO`. It is dormant while JOB_CREATE_SOURCE=native, and _jobs.js refuses
// loudly rather than letting it half-run — see the guard there.
export function airtableWritesEnabled() {
  return String(process.env.AIRTABLE_WRITES ?? "on").trim().toLowerCase() !== "off";
}

// What a skipped write resolves to. NOT null: ~65 call sites do `data?.id` or
// `created.id`, and a null would turn a deliberate skip into a TypeError in
// handlers that are otherwise working perfectly. `id: null` reads as "nothing
// was created", which is exactly true, and every caller already handles it
// because a mirror has always been allowed to fail.
export const SKIPPED_WRITE = Object.freeze({ id: null, fields: {}, records: [], skipped: true });

const WRITE_METHODS = new Set(["POST", "PATCH", "PUT", "DELETE"]);
// One line per table per cold start. The switch being on is a deliberate state,
// not an incident; logging every skipped mirror would bury the checks that
// matter in the same log.
const announced = new Set();

/** True when this request is a write and the switch is off. */
export function airtableWriteBlocked(label, options) {
  if (airtableWritesEnabled()) return false;
  if (!WRITE_METHODS.has(String(options?.method || "GET").toUpperCase())) return false;
  const table = String(label || "").split("/")[0];
  if (!announced.has(table)) {
    announced.add(table);
    console.log(`airtable-write: SKIPPED (AIRTABLE_WRITES=off) — ${table}. The Neon row is the record.`);
  }
  return true;
}

const isUuidString = (v) => typeof v === "string" && UUID_RE.test(v.trim());

/**
 * Returns `options` unchanged unless the body carries a linked-record array
 * containing a uuid, in which case a copy is returned with those entries gone.
 *
 * ⚠ Only ARRAYS are inspected. A uuid in a plain text field is legitimate and
 * common here — `Push ID` is a uuid, and several columns store a Neon handle as
 * text on purpose. Linked-record fields are the only ones Airtable will
 * fabricate a record for, and they are always arrays.
 */
export function scrubFabricatingLinks(label, options) {
  const method = String(options?.method || "GET").toUpperCase();
  if (method !== "POST" && method !== "PATCH" && method !== "PUT") return options;
  if (!options?.body || typeof options.body !== "string") return options;

  let parsed;
  try { parsed = JSON.parse(options.body); } catch { return options; }

  let changed = false;
  const scrub = (fields) => {
    if (!fields || typeof fields !== "object" || Array.isArray(fields)) return;
    for (const [key, val] of Object.entries(fields)) {
      if (!Array.isArray(val) || !val.length) continue;
      const kept = val.filter(v => !isUuidString(v));
      if (kept.length === val.length) continue;
      changed = true;
      const dropped = val.filter(isUuidString);
      if (kept.length) fields[key] = kept;
      else delete fields[key];
      console.error(
        `airtable-write ${label}: refused to send ${dropped.length} uuid(s) into linked-record ` +
        `field ${key} — ${dropped.join(", ")}. With typecast this CREATES a record; the field ` +
        `was ${kept.length ? "trimmed" : "omitted"} instead. The Neon row already holds the real link.`);
    }
  };

  scrub(parsed.fields);
  if (Array.isArray(parsed.records)) for (const r of parsed.records) scrub(r?.fields);

  return changed ? { ...options, body: JSON.stringify(parsed) } : options;
}

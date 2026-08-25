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

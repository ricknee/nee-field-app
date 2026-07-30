// Shared read-only Neon client — the dual-read ("shadow read") phase of the
// time-entries strangler migration. See docs/PLAN-time-entries-neon.md step 4b.
// ---------------------------------------------------------------------------
// CONTRACT: Neon is a SHADOW during this phase. Airtable remains the source of
// truth and the authoritative response. Every helper here FAILS SOFT — if
// DATABASE_URL is unset, the query errors, or it runs long, callers get `null`
// and must return their normal Airtable answer unchanged.
//
// This is deliberately NOT like _auth.js, which fails CLOSED. Auth must break
// the request when it can't verify; a shadow read must never break a request
// the app could otherwise serve. Do not add DATABASE_URL to ensureEnv().
//
// When the cutover completes (plan steps 5-6) and Neon becomes authoritative,
// this file's contract changes and the fail-soft wrappers must be revisited.
// ---------------------------------------------------------------------------
// The driver is imported LAZILY, on first use with DATABASE_URL set — same
// pattern as the jsPDF CDN lazy-load in index.html. This keeps the module
// importable with no node_modules present, so `node tests/handlers.test.mjs`
// stays fully offline and install-free even though airtable.js imports this.

// Keep the shadow read well under the user-visible latency budget: the Airtable
// read has already happened by the time we get here, so this is pure added wait.
const DEFAULT_TIMEOUT_MS = 4000;

let _sql = null;

export function neonEnabled() {
  return !!process.env.DATABASE_URL;
}

async function getSql() {
  if (!process.env.DATABASE_URL) return null;
  if (_sql) return _sql;
  try {
    const { neon } = await import("@neondatabase/serverless");
    _sql = neon(process.env.DATABASE_URL);
    return _sql;
  } catch {
    return null;   // driver absent -> shadow read silently disabled
  }
}

// Runs a read and returns { rows, ms } — or null on ANY failure (disabled,
// timeout, SQL error, network). Never throws.
export async function neonQuery(text, params = [], timeoutMs = DEFAULT_TIMEOUT_MS) {
  const sql = await getSql();
  if (!sql) return null;
  const t0 = Date.now();
  try {
    const rows = await Promise.race([
      sql.query(text, params),
      new Promise((_, rej) => setTimeout(() => rej(new Error("neon timeout")), timeoutMs)),
    ]);
    return { rows, ms: Date.now() - t0 };
  } catch (e) {
    return { rows: null, ms: Date.now() - t0, error: String(e?.message || e).slice(0, 200) };
  }
}

// Best-effort WRITE. Same fail-soft contract as neonQuery: Airtable is still the
// authoritative write and its result is what the caller returns, so a Neon failure
// here must never surface to the user or roll anything back. It is logged and
// swallowed.
//
// Used by the app's time-entry write paths to mirror manual entries into Neon.
// Those are rare (19 rows in all of 2026) but they are the ONLY time rows the QB
// puller will never see — without this mirror they would vanish the moment reads
// move to Neon.
export async function neonExec(label, text, params = [], timeoutMs = DEFAULT_TIMEOUT_MS) {
  const r = await neonQuery(text, params, timeoutMs);
  if (!r) return false;                       // Neon not configured — nothing to do
  if (r.error) {
    console.error(`neonExec ${label} failed (ignored): ${r.error}`);
    return false;
  }
  return true;
}

// Builds the `_shadow` block attached to dual-read responses. Compares two
// keyed maps of numbers (e.g. jobName -> hours) and reports where they differ.
// `tolerance` absorbs float noise only — it is NOT a licence to ignore drift.
export function shadowCompare({ label, airtable, neon: neonMap, ms, error, tolerance = 0.01, maxSamples = 10 }) {
  if (error)   return { label, ok: false, ms, error };
  if (!neonMap) return { label, ok: false, ms, error: "no rows" };

  const mismatches = [];
  let airtableTotal = 0, neonTotal = 0;
  for (const [k, v] of airtable) {
    airtableTotal += v;
    const n = neonMap.get(k);
    if (n === undefined) {
      if (mismatches.length < maxSamples) mismatches.push({ key: k, airtable: v, neon: null, reason: "missing in neon" });
    } else if (Math.abs(n - v) > tolerance) {
      if (mismatches.length < maxSamples) mismatches.push({ key: k, airtable: v, neon: n, reason: "value differs" });
    }
  }
  for (const [k, v] of neonMap) {
    neonTotal += v;
    // Rows Neon still has but Airtable no longer does — the deletion-drift case
    // the ETL's reconcile pass is meant to clear. Surface it, don't hide it.
    if (!airtable.has(k) && mismatches.length < maxSamples) {
      mismatches.push({ key: k, airtable: null, neon: v, reason: "stale in neon (deleted upstream?)" });
    }
  }

  const r2 = n => Math.round(n * 100) / 100;
  return {
    label,
    ok: true,
    ms,
    match: mismatches.length === 0 && Math.abs(airtableTotal - neonTotal) <= tolerance,
    counts:  { airtable: airtable.size, neon: neonMap.size },
    totals:  { airtable: r2(airtableTotal), neon: r2(neonTotal) },
    mismatchCount: mismatches.length,
    mismatches,   // capped at maxSamples
  };
}

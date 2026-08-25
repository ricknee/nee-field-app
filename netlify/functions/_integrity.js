// ── Hourly integrity checks ────────────────────────────────────────────────
// Added 2026-08-25, after a day in which ELEVEN defects were found by hand and
// not one of them threw. The pattern every single time: a query matched nothing,
// a fallback served an empty list, or a handler reported success having written
// nothing. Silence is this system's failure mode, so this is the thing that
// breaks the silence.
//
// ⚠⚠ IT FIXES NOTHING, ON PURPOSE. Every check is a SELECT. A repair that runs
// unattended on money records is how a small wrong number becomes a large one —
// these checks say what is wrong and leave the decision to a human.
//
// ── THE RULE FOR ADDING A CHECK ────────────────────────────────────────────
// A check that fires every hour on the same historical rows is not an alarm, it
// is noise, and noise is how the one that matters gets ignored. So every check
// here is scoped so that a CLEAN system reports zero:
//   - historical messes are excluded by date or by `airtable_id IS NULL`
//   - "recent" means the window in which somebody could still act on it
// If you add one that cannot be scoped to zero, fix the data first or leave it
// out. The 87 unlinked allocations on 12 completed jobs ($274,509, billed in
// QuickBooks years ago) are the worked example: real, known, deliberately not
// alarmed on.

// Each check: a name, a severity, one SQL statement returning zero rows when
// healthy, and a formatter for what to say when it does not.
//
// ⚠ `sql` here is neon()'s client and its .query() resolves to a BARE ARRAY,
// not { rows } — the mistake that made the ghost-job fix silently do nothing.
// The runner normalises both shapes so a driver change cannot re-open that hole.
export const CHECKS = [
  {
    name: "duplicate-po",
    severity: "critical",
    // A duplicate PO silently un-costs BOTH jobs' hours: QuickBooks jobcodes key
    // on the PO string, so hours land on whichever the lookup finds first.
    sql: `SELECT po_number, job_year, count(*) AS n,
                 string_agg(name, ' | ' ORDER BY name) AS jobs
            FROM jobs
           WHERE po_number IS NOT NULL
           GROUP BY po_number, job_year
          HAVING count(*) > 1`,
    say: (r) => `PO ${r.po_number} (${r.job_year}) is on ${r.n} jobs: ${r.jobs}`,
  },
  {
    name: "ghost-job",
    severity: "critical",
    // A native job's Airtable mirror re-imported as a second job. Fixed by
    // schema 062, and this is the tripwire that says the fix stopped working.
    sql: `SELECT a.name, a.po_number, a.id::text AS native_id, b.airtable_id
            FROM jobs a
            JOIN jobs b ON b.name = a.name AND b.po_number IS NOT DISTINCT FROM a.po_number
                       AND b.id <> a.id
           WHERE a.airtable_id IS NULL AND b.airtable_id IS NOT NULL`,
    say: (r) => `native job "${r.name}" (PO ${r.po_number}) has a twin carrying ${r.airtable_id} — the mirror was re-imported`,
  },
  {
    name: "fabricated-job",
    severity: "critical",
    // A job whose NAME is a uuid did not come from a person. It is what
    // `typecast: true` creates when a Neon handle reaches a linked-record field
    // — see _airtable-write-guard.js — and the hourly sync then imports it as a
    // real job. Two of these were made on 2026-08-25 before the guard existed.
    //
    // ⚠ Distinct from the ghost check above, which joins on name equality: a
    // ghost carries the REAL job's name, a fabrication carries the uuid.
    sql: `SELECT name, COALESCE(airtable_id, '(native)') AS handle, po
            FROM jobs WHERE name ~ '^[0-9a-f]{8}-[0-9a-f]{4}-'`,
    say: (r) => `job named "${r.name}" (${r.handle}) was FABRICATED from a uuid by an Airtable link write — delete it in both stores`,
  },
  {
    name: "job-missing-po-locked",
    severity: "critical",
    // po_locked is what the QuickBooks puller matches a timesheet against. NULL
    // means hours can never attach to this job.
    //
    // ⚠ SCOPED TO AWARDED AND BEYOND, and the first draft of this check got it
    // wrong: it flagged 25 jobs, and most were New Leads. The PO LOCKS AT AWARD
    // TIME — a New Lead with no locked PO is correct, not broken. A check that
    // fires on the normal state of things is the noise this file is supposed to
    // avoid. Awarded onwards is the point at which somebody can log hours, so it
    // is the point at which a missing lock costs money.
    sql: `SELECT name, po, po_number, status
            FROM jobs
           WHERE COALESCE(po_locked, '') = ''
             AND status IN ('Awarded','Service Call Scheduled','Ready to Invoice','Completed')`,
    say: (r) => `${r.status} job "${r.name}" (PO ${r.po_number}) has no locked PO — QuickBooks hours cannot attach${r.po ? `; its PO reads "${r.po}"` : ""}`,
  },
  {
    name: "job-missing-markup",
    severity: "critical",
    // A NULL markup bills material at COST, and allocations SNAPSHOT it, so the
    // loss is permanent for every allocation written before anyone notices.
    sql: `SELECT name, po_number FROM jobs
           WHERE markup_pct IS NULL AND created_at > now() - interval '30 days'`,
    say: (r) => `job "${r.name}" (PO ${r.po_number}) has no markup — its material bills at COST and allocations freeze that`,
  },
  {
    name: "invoice-total-zero",
    severity: "critical",
    // The $0-invoice: a snapshot says money, the computed total says nothing is
    // attached. This is what a duplicate save produces, and what a broken
    // allocation join produced in slice 3.
    sql: `SELECT COALESCE(airtable_id, id::text) AS handle, invoice_display_no,
                 snapshot_total, invoice_total_calc
            FROM v_invoices
           WHERE COALESCE(snapshot_total, 0) > 0
             AND COALESCE(invoice_total_calc, 0) = 0
             -- ⚠ T&M ONLY. A CONTRACT invoice computes from expected revenue and
             -- percent-to-bill, so it reads $0 whenever the job has no estimate
             -- revenue recorded — normal, and 12 historical invoices look like
             -- that. Only a T&M invoice is supposed to be the sum of what is
             -- allocated to it, so only there does $0 mean "nothing attached".
             AND invoice_type = 'Time & Material'
             AND synced_at > now() - interval '45 days'`,
    say: (r) => `invoice #${r.invoice_display_no} shows $${r.snapshot_total} but nothing is allocated to it (computes $0)`,
  },
  {
    name: "native-expense-billed-without-invoice",
    severity: "warning",
    // The bill-once rule, inverted: material marked billed with no invoice
    // behind it. Scoped to NATIVE expenses so the known historical set — 87
    // allocations on 12 completed jobs, billed in QuickBooks — stays silent.
    sql: `SELECT e.description, e.billed_material_amount, j.name AS job
            FROM expenses e
            LEFT JOIN jobs j ON j.id = e.job_id
           WHERE e.airtable_id IS NULL
             AND COALESCE(e.billed_material_amount, 0) > 0
             AND NOT EXISTS (
               SELECT 1 FROM material_billing_allocations m
                WHERE m.expense_id = e.id
                  AND (m.invoice_id IS NOT NULL OR m.invoice_airtable_id IS NOT NULL))`,
    say: (r) => `expense "${r.description}" on ${r.job} is marked $${r.billed_material_amount} billed with no invoice behind it`,
  },
  {
    name: "hours-not-linked-to-a-job",
    severity: "warning",
    // Hours paid but not costed — the recurring GP leak. Only counts entries
    // whose job_name resolves to exactly one job, i.e. ones that SHOULD have
    // linked; the ~11k historical unmatched rows are excluded by that join.
    sql: `SELECT t.employee_name, t.work_date, t.hours, t.job_name
            FROM time_entries t
           WHERE t.job_id IS NULL AND COALESCE(t.job_name, '') <> ''
             AND t.work_date > current_date - 45
             AND (SELECT count(*) FROM jobs j
                   WHERE lower(trim(j.po_locked)) = lower(trim(t.job_name))) = 1`,
    say: (r) => `${r.hours}h for ${r.employee_name} on ${r.work_date} names "${r.job_name}" but is not linked to it — paid, not costed`,
  },
  {
    name: "clock-left-running",
    severity: "warning",
    // Operational rather than structural: nobody works an 18-hour shift, so this
    // is somebody who forgot to clock out and whose hours will be wrong.
    sql: `SELECT p.employee_id::text AS employee_id, e.name AS employee,
                 p.started_at, p.job_name
            FROM open_punches p
            LEFT JOIN employees e ON e.id = p.employee_id
           WHERE p.started_at < now() - interval '18 hours'`,
    say: (r) => `${r.employee || r.employee_id} has been clocked in since ${r.started_at}${r.job_name ? ` on ${r.job_name}` : ""}`,
  },
];

const rowsOf = (r) => (Array.isArray(r) ? r : (r?.rows || []));

/**
 * Runs every check. Never throws: a broken check must not stop the hourly job
 * that carries it — that job's first duty is pulling timesheets.
 *
 * Returns { ok, checked, failures, findings[] } where `findings` is already
 * formatted for a human.
 */
export async function runIntegrityChecks(sql, opts = {}) {
  const limit = Number(opts.perCheckLimit) || 10;
  const findings = [];
  let checked = 0, broken = 0;

  for (const check of CHECKS) {
    try {
      const rows = rowsOf(await sql.query(check.sql));
      checked++;
      if (!rows.length) continue;
      const shown = rows.slice(0, limit).map(r => {
        try { return check.say(r); } catch { return JSON.stringify(r); }
      });
      findings.push({
        check: check.name,
        severity: check.severity,
        count: rows.length,
        detail: shown,
        truncated: rows.length > shown.length,
      });
    } catch (e) {
      broken++;
      // A check that cannot run is itself worth saying out loud — a renamed
      // column would otherwise turn an alarm into permanent silence, which is
      // the exact failure this file exists to prevent.
      findings.push({
        check: check.name, severity: "check-broken", count: 0,
        detail: [`the check itself failed: ${e?.message || e}`], truncated: false,
      });
    }
  }

  const failures = findings.filter(f => f.severity !== "check-broken")
                           .reduce((n, f) => n + f.count, 0);

  // The log line is the minimum viable alarm: one line per finding, at error
  // level so it stands out from the pull's normal chatter. A quiet run says
  // nothing at all, which is what "healthy" should look like.
  for (const f of findings) {
    console.error(`integrity [${f.severity}] ${f.check}: ${f.count} — ` +
      f.detail.join(" ;; ") + (f.truncated ? ` ;; (+${f.count - f.detail.length} more)` : ""));
  }

  return { ok: broken === 0, checked, failures, brokenChecks: broken, findings };
}

/**
 * Best-effort POST of the findings to a webhook, so they reach somebody who is
 * not reading function logs. Optional by design: unset INTEGRITY_WEBHOOK and the
 * checks still run and still log.
 *
 * ⚠ Only posts when something is WRONG. A webhook that fires hourly saying
 * "all clear" is a webhook people mute, and a muted alarm is worse than none.
 */
export async function reportIntegrity(report, url = process.env.INTEGRITY_WEBHOOK) {
  if (!url || !report?.findings?.length) return { posted: false };
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        source: "nee-field-app integrity",
        at: new Date().toISOString(),
        failures: report.failures,
        brokenChecks: report.brokenChecks,
        findings: report.findings,
      }),
    });
    return { posted: res.ok, status: res.status };
  } catch (e) {
    console.error(`integrity: webhook post failed (ignored) — ${e?.message || e}`);
    return { posted: false, error: String(e?.message || e) };
  }
}

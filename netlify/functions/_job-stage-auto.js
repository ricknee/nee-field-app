// First hours move a job to 🔨 On site — db/schema/099, owner 2026-11-19.
//
// "when the 1st hour is clocked or recorded can u change the status to on
// site ... all jobs just go to onsite. then i change to rough if i wanna."
//
// Runs hourly from qb-time-pull, AFTER the timesheet pull, so the hours it
// just wrote count. One UPDATE:
//   • Awarded jobs only — stage is a sub-status of Awarded.
//   • Only from Pre-con or not set. A stage set by hand (rough, finish, punch
//     list, on hold, complete) is never touched. Residential jobs go to On
//     site too; the owner moves those to Rough & Service by hand.
//   • Only hours RECORDED after jobs.stage_auto_after (time_entries.created_at
//     is stamped once on insert; the pull's upsert never rewrites it). That
//     column moves on every stage or status change, so a job put back to
//     Pre-con after prep hours stays there until NEW hours come in.
//   • Stamps stage_auto_after itself, so it fires once per job.
//
// `q(text, params)` returns the rows array. Fails soft in the caller — the
// timesheet pull must never stop for this.

export const AUTO_STAGE_SQL =
  `UPDATE jobs j
      SET stage = 'onsite', stage_auto_after = now(), synced_at = now()
    WHERE j.status = 'Awarded'
      AND (j.stage IS NULL OR j.stage = 'preconstruction')
      AND EXISTS (SELECT 1 FROM time_entries t
                   WHERE t.job_id = j.id AND t.hours > 0
                     AND (j.stage_auto_after IS NULL OR t.created_at > j.stage_auto_after))
    RETURNING j.name`;

export async function runAutoStageOnsite(q) {
  const rows = await q(AUTO_STAGE_SQL, []);
  const moved = (rows || []).map(r => r.name);
  return { ok: true, movedToOnsite: moved.length, jobs: moved };
}

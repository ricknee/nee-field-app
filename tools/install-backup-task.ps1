# Registers the nightly photo backup as a Windows scheduled task.
# See docs/PLAN-job-photos.md.
# ---------------------------------------------------------------------------
# Run ONCE, from an elevated PowerShell (right-click -> Run as administrator):
#
#   powershell -ExecutionPolicy Bypass -File tools\install-backup-task.ps1
#
# THE PROBLEM THIS SOLVES: the office PC sleeps, so a 2am schedule silently
# never runs and the backup looks healthy while doing nothing. Three settings
# handle that, in order of how much they can be relied on:
#
#   1. -StartWhenAvailable   run as soon as possible after a MISSED start.
#                            This is the one that always works: whenever the PC
#                            next wakes up, the task catches up.
#   2. Daytime schedule      default 12:30pm, when the PC is already awake.
#                            A backup does not have to run at 2am.
#   3. -WakeToRun            ask Windows to wake the machine. Genuinely useful,
#                            but BIOS settings or "fast startup" can disable
#                            wake timers, so it is a bonus, not the plan.
#
# Deliberately NOT relying on wake alone: a backup you believe is running but
# is not is worse than no backup, because you stop keeping the other copy.

param(
  # SEVERAL times a day, not one. If the external drive is unplugged when the
  # task fires, the script exits "skipped" - and Windows counts that as the
  # task having RUN, so -StartWhenAvailable will not re-fire it. A single daily
  # trigger could therefore miss days in a row with no signal that anything is
  # wrong. Three triggers means three chances to catch the drive connected.
  #
  # (Repetition -- "every 4 hours" -- would be the obvious alternative, but
  # Windows disables StartWhenAvailable catch-up on repeating triggers, which
  # is the behaviour we most want to keep.)
  [string[]]$Times    = @("08:30", "12:30", "17:00"),
  [string]$TaskName   = "NEE Photo Backup",
  [string]$ScriptPath = (Join-Path $PSScriptRoot "backup-photos.ps1"),
  [switch]$Wake                                  # also try to wake the PC
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ScriptPath)) {
  Write-Output "Cannot find $ScriptPath"
  exit 1
}
$ScriptPath = (Resolve-Path $ScriptPath).Path

# Task Scheduler needs a real user context to see the R2_* user environment
# variables and the F: drive letter, so the task runs as the logged-on user.
$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`" -Verify"

$triggers = @($Times | ForEach-Object { New-ScheduledTaskTrigger -Daily -At $_ })

$settings = New-ScheduledTaskSettingsSet `
  -StartWhenAvailable `
  -DontStopIfGoingOnBatteries `
  -AllowStartIfOnBatteries `
  -RunOnlyIfNetworkAvailable `
  -ExecutionTimeLimit (New-TimeSpan -Hours 3) `
  -MultipleInstances IgnoreNew

if ($Wake) { $settings.WakeToRun = $true }

Register-ScheduledTask `
  -TaskName    $TaskName `
  -Action      $action `
  -Trigger     $triggers `
  -Settings    $settings `
  -Description "Copies the Cloudflare R2 jobsite-photo bucket to the external drive. Never deletes (rclone copy, not sync)." `
  -Force | Out-Null

Write-Output ""
Write-Output "Registered '$TaskName'"
Write-Output ("  runs      : daily at " + ($Times -join ", "))
Write-Output "  script    : $ScriptPath"
Write-Output "  if missed : runs as soon as the PC is next awake"
Write-Output "  if F: off : skips quietly and tries again at the next time slot"
if ($Wake) { Write-Output "  wake      : will try to wake the PC (needs wake timers enabled in Power Options)" }
Write-Output ""
Write-Output "Test it right now without waiting for the schedule:"
Write-Output "  Start-ScheduledTask -TaskName '$TaskName'"
Write-Output ""
Write-Output "Check when it last ran and whether it succeeded:"
Write-Output "  Get-ScheduledTaskInfo -TaskName '$TaskName'"
Write-Output ""
Write-Output "LastTaskResult 0 = success, 2 = drive was not connected (skipped, not a failure)."

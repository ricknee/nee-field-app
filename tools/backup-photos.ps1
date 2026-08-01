# Nightly backup of the R2 jobsite-photo bucket to an external drive.
# See docs/PLAN-job-photos.md.
# ---------------------------------------------------------------------------
#   powershell -ExecutionPolicy Bypass -File tools\backup-photos.ps1
#
# WHY THIS EXISTS: R2 is designed for eleven nines of durability, so Cloudflare
# losing the photos is not a realistic risk. The realistic risks are an
# accidental delete (the app has a Delete button), a lapsed account, or a
# compromised key - and durability protects against none of those. One copy in
# one account is not a backup no matter how durable it is.
#
# THE CRITICAL CHOICE: this uses `rclone copy`, NEVER `rclone sync`.
#   copy  = only ever adds files to the backup
#   sync  = makes the backup mirror the source, INCLUDING deletions
# With sync, deleting 47 photos in the app would delete them from the backup on
# the next run, and the backup would have protected you from the single most
# likely way you lose photos. Do not "improve" this to sync.
#
# CREDENTIALS: set these once as USER environment variables (not in this file,
# which is committed and served publicly from the site root):
#   setx R2_ACCOUNT_ID        "44ac..."
#   setx R2_BACKUP_KEY_ID     "..."
#   setx R2_BACKUP_SECRET     "..."
# Use a SEPARATE, READ-ONLY R2 token for these (Object Read only, scoped to the
# bucket). A backup job has no business holding a key that can delete.

param(
  [string]$Destination = "F:\NEE-Job-Photos",
  [string]$Bucket      = "nee-job-photos",
  [string]$LogDir      = "F:\NEE-Job-Photos\_logs",
  [switch]$Verify                                   # adds an rclone check pass
)

$ErrorActionPreference = "Stop"

function Write-Log($msg) {
  $line = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
  Write-Output $line
  if ($script:LogFile) { Add-Content -Path $script:LogFile -Value $line -Encoding utf8 }
}

# -- Preconditions ----------------------------------------------------------

# An unplugged drive must fail loudly, not silently create F:\ on the system
# disk and report success - a backup that quietly writes nowhere is worse than
# no backup, because you stop worrying about it.
$driveRoot = Split-Path -Qualifier $Destination
if (-not (Test-Path "$driveRoot\")) {
  Write-Output "BACKUP SKIPPED: drive $driveRoot is not connected."
  exit 2
}

$rclone = (Get-Command rclone -ErrorAction SilentlyContinue)
if (-not $rclone) {
  Write-Output "rclone is not on PATH. Download the single .exe from https://rclone.org/downloads/ and put it somewhere on PATH."
  exit 1
}

$acct   = $env:R2_ACCOUNT_ID
$keyId  = $env:R2_BACKUP_KEY_ID
$secret = $env:R2_BACKUP_SECRET
if (-not $acct -or -not $keyId -or -not $secret) {
  Write-Output "Missing credentials. Set R2_ACCOUNT_ID, R2_BACKUP_KEY_ID and R2_BACKUP_SECRET as user environment variables (see the header of this file), then open a NEW terminal."
  exit 1
}

New-Item -ItemType Directory -Force -Path $Destination | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir      | Out-Null
$script:LogFile = Join-Path $LogDir ("backup-{0}.log" -f (Get-Date -Format "yyyy-MM"))

# -- rclone remote, defined inline ------------------------------------------
# Configuring the remote through environment variables means there is no
# rclone.conf holding the keys, and nothing to keep in sync between machines.
$env:RCLONE_CONFIG_R2_TYPE              = "s3"
$env:RCLONE_CONFIG_R2_PROVIDER          = "Cloudflare"
$env:RCLONE_CONFIG_R2_ACCESS_KEY_ID     = $keyId
$env:RCLONE_CONFIG_R2_SECRET_ACCESS_KEY = $secret
$env:RCLONE_CONFIG_R2_ENDPOINT          = "https://$acct.r2.cloudflarestorage.com"
$env:RCLONE_CONFIG_R2_REGION            = "auto"
$env:RCLONE_CONFIG_R2_NO_CHECK_BUCKET   = "true"   # a read-only token can't create buckets

Write-Log "Backup starting: R2:$Bucket -> $Destination"

# --transfers 8: photos are small and this is mostly network wait.
# --checksum: compare by hash, not timestamp - a re-uploaded photo with the
#             same name and a new body must be picked up.
# NOTE: copy, not sync. See the header.
& rclone copy "R2:$Bucket" $Destination `
    --transfers 8 `
    --checksum `
    --log-level INFO `
    --log-file $script:LogFile `
    --stats 30s `
    --stats-one-line

$copyExit = $LASTEXITCODE
if ($copyExit -ne 0) {
  Write-Log "BACKUP FAILED: rclone copy exited $copyExit"
  exit $copyExit
}

# -- Report -----------------------------------------------------------------
$localCount = (Get-ChildItem -Path $Destination -Recurse -File -ErrorAction SilentlyContinue |
               Where-Object { $_.FullName -notlike "$LogDir*" }).Count
Write-Log "Backup OK. $localCount files now in $Destination"

# -- Optional verification --------------------------------------------------
# --one-way: only complain about things in R2 that are missing locally. Files
# present locally but gone from R2 are EXPECTED - that is the whole point of a
# backup that keeps deleted photos.
if ($Verify) {
  Write-Log "Verifying (one-way: everything in R2 must exist locally)..."
  & rclone check "R2:$Bucket" $Destination --one-way --checksum --log-file $script:LogFile --log-level NOTICE
  if ($LASTEXITCODE -eq 0) { Write-Log "Verify OK: every object in R2 is present in the backup." }
  else                     { Write-Log "VERIFY FOUND DIFFERENCES (exit $LASTEXITCODE) - see the log above." }
}

exit 0

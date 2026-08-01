# Backup of the R2 jobsite-photo bucket to one or more local drives.
# See docs/PLAN-job-photos.md.
# ---------------------------------------------------------------------------
#   powershell -ExecutionPolicy Bypass -File tools\backup-photos.ps1 -Verify
#
# WHY THIS EXISTS: R2 is designed for eleven nines of durability, so Cloudflare
# losing the photos is not a realistic risk. The realistic risks are an
# accidental delete (the app has a Delete button), a lapsed account, or a
# compromised key - and durability protects against none of those. One copy in
# one account is not a backup no matter how durable it is.
#
# TWO DESTINATIONS BY DEFAULT:
#   F: external drive  - physical, offline, unaffected by any account problem
#   P: pCloud Drive    - off-site, survives the building
# Different failure modes, so both is meaningfully better than either. Each is
# handled independently: an unplugged F: does not stop the P: copy, and vice
# versa. A missing drive is SKIPPED, not treated as a failure - but the run
# only reports success if at least one destination actually got the files.
#
# THE CRITICAL CHOICE: this uses `rclone copy`, NEVER `rclone sync`.
#   copy  = only ever adds files to the backup
#   sync  = makes the backup mirror the source, INCLUDING deletions
# With sync, deleting photos in the app would delete them from the backup on
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
#
# FOLDER NAMES: photos land under jobs\<airtable record id>\<album>\. Record ids
# are what guarantees two jobs with the same name never mix, but they are not
# readable. This is a BACKUP, not the office's browsing copy - restoring means
# copying files back, not hunting through folders by eye.

param(
  [string[]]$Destinations = @("F:\NEE-Job-Photos", "P:\NEE Job Photos Backup"),
  [string]$Bucket         = "nee-job-photos",
  [switch]$Verify                                   # adds an rclone check pass
)

$ErrorActionPreference = "Stop"
$script:LogFile = $null

function Write-Log($msg) {
  $line = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
  Write-Output $line
  if ($script:LogFile) { Add-Content -Path $script:LogFile -Value $line -Encoding utf8 }
}

# -- Preconditions ----------------------------------------------------------

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

# -- rclone remote, defined inline ------------------------------------------
# Configuring the remote through environment variables means there is no
# rclone.conf holding the keys, and nothing to keep in sync between machines.
$env:RCLONE_CONFIG_R2_TYPE              = "s3"
$env:RCLONE_CONFIG_R2_PROVIDER          = "Cloudflare"
$env:RCLONE_CONFIG_R2_ACCESS_KEY_ID     = $keyId
$env:RCLONE_CONFIG_R2_SECRET_ACCESS_KEY = $secret
$env:RCLONE_CONFIG_R2_ENDPOINT          = "https://$acct.r2.cloudflarestorage.com"
$env:RCLONE_CONFIG_R2_REGION            = "auto"
$env:RCLONE_CONFIG_R2_NO_CHECK_BUCKET   = "true"   # a read-only token cannot create buckets

$succeeded = 0
$skipped   = @()
$failed    = @()

foreach ($dest in $Destinations) {

  # An unplugged drive - or pCloud Drive not running - must be skipped, not
  # silently written to the system disk. A backup that quietly writes nowhere
  # is worse than no backup, because you stop keeping the other copy.
  $driveRoot = Split-Path -Qualifier $dest
  if (-not (Test-Path "$driveRoot\")) {
    Write-Output ("SKIPPED {0}: drive {1} is not available." -f $dest, $driveRoot)
    $skipped += $dest
    continue
  }

  $logDir = Join-Path $dest "_logs"
  New-Item -ItemType Directory -Force -Path $dest   | Out-Null
  New-Item -ItemType Directory -Force -Path $logDir | Out-Null
  $script:LogFile = Join-Path $logDir ("backup-{0}.log" -f (Get-Date -Format "yyyy-MM"))

  Write-Log "Backup starting: R2:$Bucket -> $dest"

  # --transfers 4: pCloud Drive is a network mount and does not love heavy
  #   parallelism; 4 is comfortable for both destinations.
  # --checksum: compare by hash, not timestamp - a re-uploaded photo with the
  #   same name and a new body must still be picked up.
  # NOTE: copy, not sync. See the header.
  & rclone copy "R2:$Bucket" $dest `
      --transfers 4 `
      --checksum `
      --log-level INFO `
      --log-file $script:LogFile `
      --stats 30s `
      --stats-one-line

  if ($LASTEXITCODE -ne 0) {
    Write-Log ("FAILED: rclone copy exited {0}" -f $LASTEXITCODE)
    $failed += $dest
    continue
  }

  $count = (Get-ChildItem -Path $dest -Recurse -File -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -notlike "$logDir*" }).Count
  Write-Log "OK. $count files now in $dest"
  $succeeded++

  if ($Verify) {
    # --one-way: only complain about things in R2 missing locally. Files present
    # locally but gone from R2 are EXPECTED - that is the whole point of a
    # backup that keeps deleted photos.
    Write-Log "Verifying (one-way: everything in R2 must exist here)..."
    & rclone check "R2:$Bucket" $dest --one-way --checksum --log-file $script:LogFile --log-level NOTICE
    if ($LASTEXITCODE -eq 0) { Write-Log "Verify OK: every object in R2 is present in this copy." }
    else                     { Write-Log ("VERIFY FOUND DIFFERENCES (exit {0}) - see the log above." -f $LASTEXITCODE) }
  }
}

# -- Summary ----------------------------------------------------------------
$script:LogFile = $null
Write-Output ""
Write-Output ("Destinations backed up : {0}" -f $succeeded)
if ($skipped.Count) { Write-Output ("Skipped (not available): {0}" -f ($skipped -join ", ")) }
if ($failed.Count)  { Write-Output ("FAILED                 : {0}" -f ($failed  -join ", ")) }

if ($failed.Count)    { exit 1 }   # something was reachable and still went wrong
if ($succeeded -eq 0) { exit 2 }   # nothing was plugged in - try again next slot
exit 0

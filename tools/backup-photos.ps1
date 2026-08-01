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
# Each is handled independently: an unplugged F: does not stop the P: copy. A
# missing drive is SKIPPED, not failed - but the run only reports success if at
# least one destination actually received files.
#
# READABLE FOLDER NAMES: R2 stores photos under the Airtable RECORD ID, which
# is what guarantees two jobs with the same name never mix - but it means a
# backup full of "rec0i9ooATrs9r978" folders that nobody can browse. So this
# script looks the record ids up in Airtable and copies each job into a folder
# named after its Job PO. Set -Flat to skip that and mirror record ids instead.
#
# Renaming a job in Airtable creates a NEW folder here on the next run; the old
# one stays, because this script never deletes. That is the correct trade for a
# backup - a stale duplicate is harmless, a missing photo is not.
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
#   setx R2_ACCOUNT_ID          "44ac..."
#   setx R2_BACKUP_KEY_ID       "..."          READ-ONLY R2 token
#   setx R2_BACKUP_SECRET       "..."
#   setx AIRTABLE_PROD_READ_PAT "pat..."       read-only Airtable PAT, names only
# Both tokens are read-only on purpose: a backup job has no business holding a
# key that can delete. Without the Airtable one the script still runs, just
# with record-id folder names.

param(
  [string[]]$Destinations = @("F:\NEE-Job-Photos", "P:\NEE Job Photos Backup"),
  [string]$Bucket         = "nee-job-photos",
  [string]$BaseId         = "appiqWg6SvKcGfMAu",
  [switch]$Flat,                                    # keep raw record-id folders
  [switch]$Verify                                   # adds an rclone check pass
)

$ErrorActionPreference = "Stop"
$script:LogFile = $null

function Write-Log($msg) {
  $line = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
  Write-Output $line
  if ($script:LogFile) { Add-Content -Path $script:LogFile -Value $line -Encoding utf8 }
}

# Windows forbids \ / : * ? " < > | in names, and silently mangles trailing dots
# and spaces. Job POs contain parentheses and spaces, which are fine.
function Get-SafeName($name) {
  $s = [string]$name
  foreach ($ch in @('\','/',':','*','?','"','<','>','|')) { $s = $s.Replace($ch, '-') }
  $s = $s.Trim().TrimEnd('.', ' ')
  if ($s.Length -gt 120) { $s = $s.Substring(0, 120).Trim() }
  return $s
}

# -- Preconditions ----------------------------------------------------------

if (-not (Get-Command rclone -ErrorAction SilentlyContinue)) {
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

# -- Job id -> readable name ------------------------------------------------

function Get-JobNameMap($baseId) {
  $pat = $env:AIRTABLE_PROD_READ_PAT
  if (-not $pat) { $pat = $env:AIRTABLE_API_KEY }
  if (-not $pat) { return $null }

  $map = @{}
  $offset = $null
  do {
    # Ask for only the two name fields - this token should never need more, and
    # a narrow request is a narrow blast radius if it ever leaks.
    $uri = "https://api.airtable.com/v0/$baseId/Jobs?pageSize=100&fields%5B%5D=Job%20PO&fields%5B%5D=Job%20Name"
    if ($offset) { $uri += "&offset=$offset" }
    $res = Invoke-RestMethod -Uri $uri -Headers @{ Authorization = "Bearer $pat" } -Method Get
    foreach ($r in $res.records) {
      $label = $r.fields.'Job PO'
      if (-not $label) { $label = $r.fields.'Job Name' }
      if ($label) { $map[$r.id] = Get-SafeName $label }
    }
    $offset = $res.offset
  } while ($offset)
  return $map
}

$jobMap = $null
if (-not $Flat) {
  try {
    $jobMap = Get-JobNameMap $BaseId
    if ($null -eq $jobMap) {
      Write-Output "No Airtable token (AIRTABLE_PROD_READ_PAT) - falling back to record-id folder names."
    } else {
      Write-Output ("Job names loaded: {0}" -f $jobMap.Count)
    }
  } catch {
    # Never let a naming lookup stop the actual backup.
    Write-Output ("Could not read job names ({0}) - falling back to record-id folder names." -f $_.Exception.Message)
    $jobMap = $null
  }
}

# Record ids that actually have photos in R2.
$jobIds = @()
if ($jobMap) {
  $lsd = & rclone lsf "R2:$Bucket/jobs/" --dirs-only 2>$null
  if ($LASTEXITCODE -eq 0) { $jobIds = @($lsd | ForEach-Object { $_.TrimEnd('/') } | Where-Object { $_ }) }
  if (-not $jobIds.Count) {
    Write-Output "Could not list jobs in the bucket - falling back to a flat copy."
    $jobMap = $null
  }
}

# -- Copy -------------------------------------------------------------------

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
  $destFailed = $false

  # --transfers 4: pCloud Drive is a network mount and does not love heavy
  #   parallelism; 4 is comfortable for both destinations.
  # --checksum: compare by hash, not timestamp - a re-uploaded photo with the
  #   same name and a new body must still be picked up.
  # NOTE: copy, not sync. See the header.
  $rcArgs = @("--transfers", "4", "--checksum", "--log-level", "INFO",
              "--log-file", $script:LogFile, "--stats", "30s", "--stats-one-line")

  if ($jobMap) {
    foreach ($id in $jobIds) {
      $name = $jobMap[$id]
      # A job with photos but no Airtable record (deleted job) must still be
      # backed up - park it under its record id rather than dropping it.
      if (-not $name) { $name = Join-Path "_unknown-job" $id }
      & rclone copy "R2:$Bucket/jobs/$id" (Join-Path $dest $name) @rcArgs
      if ($LASTEXITCODE -ne 0) {
        Write-Log ("FAILED on job {0} ({1}): rclone exited {2}" -f $id, $name, $LASTEXITCODE)
        $destFailed = $true
        break
      }
    }
  } else {
    & rclone copy "R2:$Bucket" $dest @rcArgs
    if ($LASTEXITCODE -ne 0) {
      Write-Log ("FAILED: rclone copy exited {0}" -f $LASTEXITCODE)
      $destFailed = $true
    }
  }

  if ($destFailed) { $failed += $dest; continue }

  $count = (Get-ChildItem -Path $dest -Recurse -File -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -notlike "$logDir*" }).Count
  Write-Log "OK. $count files now in $dest"
  $succeeded++

  if ($Verify) {
    # Only meaningful for the flat layout - the readable layout deliberately
    # rearranges paths, so a path-for-path check would report every file as
    # missing. The per-job copies above already fail loudly on error.
    if ($jobMap) {
      Write-Log "Verify skipped: readable layout rearranges paths (see the note in this script)."
    } else {
      Write-Log "Verifying (one-way: everything in R2 must exist here)..."
      & rclone check "R2:$Bucket" $dest --one-way --checksum --log-file $script:LogFile --log-level NOTICE
      if ($LASTEXITCODE -eq 0) { Write-Log "Verify OK: every object in R2 is present in this copy." }
      else                     { Write-Log ("VERIFY FOUND DIFFERENCES (exit {0}) - see the log above." -f $LASTEXITCODE) }
    }
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

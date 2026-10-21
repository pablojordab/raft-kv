# Task 10: manual/scripted election check (Windows / PowerShell version).
#
# Same check as election_check.sh: builds the node binary, launches a real
# 3-process cluster on localhost, and verifies from each node's own log
# output that exactly one node is Leader, the other two are Followers, all
# three agree on the term, and that this outcome is stable over a few
# seconds (no constant re-elections).
#
# Usage (from the repo root):   powershell -ExecutionPolicy Bypass -File test\election_check.ps1

$SettleSeconds = 3
$ObserveSeconds = 4
$BasePort = 9101

$WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) ("raft-kv-check-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $WorkDir | Out-Null
$Bin = Join-Path $WorkDir "raft-kv-node.exe"
$Procs = @()

function Stop-Cluster {
    foreach ($p in $Procs) {
        if ($p -and -not $p.HasExited) { try { $p.Kill() } catch {} }
    }
}

Write-Host "Building node binary..."
go build -o $Bin ./cmd
if ($LASTEXITCODE -ne 0) { Write-Host "FAIL: build failed"; exit 1 }

function Get-Addr($id) { return "127.0.0.1:$($BasePort + $id)" }
function Get-Peers($self) {
    $parts = @()
    foreach ($id in 1..3) { if ($id -ne $self) { $parts += "$id=$(Get-Addr $id)" } }
    return ($parts -join ",")
}

foreach ($id in 1..3) {
    # ($args is a reserved automatic variable in PowerShell, hence $nodeArgs.)
    $nodeArgs = @("-id=$id", "-addr=$(Get-Addr $id)", "-peers=$(Get-Peers $id)")
    # Node logs go to stderr (Go's log package), so redirect stderr to a file.
    $p = Start-Process -FilePath $Bin -ArgumentList $nodeArgs -NoNewWindow -PassThru `
        -RedirectStandardOutput (Join-Path $WorkDir "node$id.out.log") `
        -RedirectStandardError  (Join-Path $WorkDir "node$id.log")
    $Procs += $p
}
Write-Host "Launched 3 nodes (logs in $WorkDir)"

# Returns @{State=..; Term=..} from a node's latest status line, or $null.
function Get-LastState($id) {
    $file = Join-Path $WorkDir "node$id.log"
    if (-not (Test-Path $file)) { return $null }
    $m = Select-String -Path $file -Pattern 'state=(\w+) term=(\d+)' | Select-Object -Last 1
    if (-not $m) { return $null }
    return @{ State = $m.Matches[0].Groups[1].Value; Term = [int]$m.Matches[0].Groups[2].Value }
}

# Returns a string "leaders followers distinct-terms leader-id term".
function Get-Snapshot {
    $leaders = 0; $followers = 0; $leaderId = 0; $term = 0; $terms = @()
    foreach ($id in 1..3) {
        $s = Get-LastState $id
        if (-not $s) { continue }
        if ($s.State -eq "Leader") { $leaders++; $leaderId = $id; $term = $s.Term }
        elseif ($s.State -eq "Follower") { $followers++ }
        $terms += $s.Term
    }
    $distinct = ($terms | Sort-Object -Unique).Count
    return "$leaders $followers $distinct $leaderId $term"
}

Start-Sleep -Seconds ($SettleSeconds + 1)
$first = Get-Snapshot
Start-Sleep -Seconds $ObserveSeconds
$second = Get-Snapshot

Write-Host "First sample  (leaders followers distinct-terms leader-id term): $first"
Write-Host "Second sample (leaders followers distinct-terms leader-id term): $second"

# NOTE: PowerShell variable names are case-insensitive ($f and $F are the
# SAME variable), so every name below is deliberately distinct.
$fields = $second -split " "
$numLeaders = [int]$fields[0]
$numFollowers = [int]$fields[1]
$numDistinctTerms = [int]$fields[2]
$leaderId = $fields[3]
$leaderTerm = $fields[4]
$failed = $false
if ($numLeaders -ne 1) { Write-Host "FAIL: expected exactly 1 leader, saw $numLeaders"; $failed = $true }
if ($numFollowers -ne 2) { Write-Host "FAIL: expected exactly 2 followers, saw $numFollowers"; $failed = $true }
if ($numDistinctTerms -ne 1) { Write-Host "FAIL: nodes disagree on the term"; $failed = $true }
if ($first -ne $second) { Write-Host "FAIL: cluster state changed between samples (not stable)"; $failed = $true }

Stop-Cluster
if (-not $failed) {
    Write-Host "PASS: node $leaderId is the stable leader for term $leaderTerm; the other two are followers."
    exit 0
}
Write-Host "Logs kept in $WorkDir for inspection."
exit 1
<#
.SYNOPSIS
  End-to-end smoke test for the Mini Cloud Platform.

.DESCRIPTION
  Assumes the control plane, at least one worker, and the proxy are already
  running (see README "Quick start"). Then automatically:

    1. checks the control plane is up and has >=1 healthy node,
    2. deploys nginx and waits for all replicas to become running,
    3. fetches the app through the reverse proxy,
    4. kills one container with `docker rm -f` and waits for the platform to
       self-heal back to the desired replica count (a NEW container replaces it),
    5. (optional) tears the deployment down.

  Every step prints PASS/FAIL; the script exits non-zero if any step fails.

.EXAMPLE
  .\scripts\smoke_test.ps1
  .\scripts\smoke_test.ps1 -Replicas 4 -Cleanup
#>
[CmdletBinding()]
param(
    [string]$ControlPlane = "http://localhost:8000",
    [string]$Proxy        = "http://localhost:8080",
    [string]$Name         = "smoke",
    [string]$Image        = "nginx:alpine",
    [int]$Replicas        = 3,
    [double]$CpuReq       = 0.5,
    [double]$MemReqMb     = 128,
    [int]$ContainerPort   = 80,
    [switch]$Cleanup                       # delete the deployment when done
)

$ErrorActionPreference = "Stop"
$script:failed = $false

function Step($msg)  { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Ok($msg)    { Write-Host "  [PASS] $msg" -ForegroundColor Green }
function Fail($msg)  { Write-Host "  [FAIL] $msg" -ForegroundColor Red; $script:failed = $true }
function Info($msg)  { Write-Host "  $msg" -ForegroundColor DarkGray }

function Get-Deployment {
    try { return Invoke-RestMethod "$ControlPlane/deployments/$Name" -TimeoutSec 5 }
    catch { return $null }
}

# Poll $Check (a scriptblock returning $true/$false) until it passes or times out.
function Wait-Until([string]$desc, [scriptblock]$Check, [int]$TimeoutSec = 60, [int]$IntervalSec = 2) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (& $Check) { Ok $desc; return $true }
        Start-Sleep -Seconds $IntervalSec
    }
    Fail "$desc (timed out after ${TimeoutSec}s)"
    return $false
}

# --------------------------------------------------------------------------- #
Step "1. Control plane reachable"
try {
    $health = Invoke-RestMethod "$ControlPlane/healthz" -TimeoutSec 5
    Ok "control plane healthy at $ControlPlane"
} catch {
    Fail "cannot reach control plane at $ControlPlane — is it running?"
    exit 1
}

Step "2. At least one healthy worker node"
$nodes = Invoke-RestMethod "$ControlPlane/nodes" -TimeoutSec 5
$healthy = @($nodes | Where-Object { $_.status -eq "healthy" })
if ($healthy.Count -ge 1) {
    Ok "$($healthy.Count) healthy node(s): $($healthy.id -join ', ')"
} else {
    Fail "no healthy worker nodes registered — start a worker first"
    exit 1
}

Step "3. Deploy '$Name' ($Image x$Replicas)"
if (Get-Deployment) {
    Info "deployment '$Name' already exists — deleting it first"
    Invoke-RestMethod "$ControlPlane/deployments/$Name" -Method Delete -TimeoutSec 10 | Out-Null
    Start-Sleep -Seconds 3
}
$body = @{
    name = $Name; image = $Image; replicas = $Replicas
    cpu_req = $CpuReq; mem_req_mb = $MemReqMb; container_port = $ContainerPort
} | ConvertTo-Json
Invoke-RestMethod "$ControlPlane/deployments" -Method Post -Body $body -ContentType "application/json" | Out-Null
Ok "deployment created"

Step "4. Wait for $Replicas replicas to become running (allows for startup grace)"
Wait-Until "all $Replicas replicas running" {
    $d = Get-Deployment
    $d -and $d.available_replicas -eq $Replicas
} -TimeoutSec 60 | Out-Null
$dep = Get-Deployment
if ($dep) {
    Info ("replicas: " + (($dep.replicas | ForEach-Object { "$($_.node_id):$($_.status)" }) -join ", "))
}

Step "5. Reach the app through the reverse proxy"
$hits = @{}
for ($i = 0; $i -lt ($Replicas * 2); $i++) {
    try {
        $r = Invoke-WebRequest "$Proxy/$Name/" -TimeoutSec 5 -UseBasicParsing
        $backend = $r.Headers["x-mc-backend"]
        if ($backend) { $hits[$backend] = 1 }
    } catch { }
}
if ($hits.Count -ge 1) {
    Ok "proxy served the app; backends hit: $($hits.Keys -join ', ')"
    if ($hits.Count -gt 1) { Info "load balancing across $($hits.Count) replicas confirmed" }
} else {
    Fail "proxy did not return a healthy response at $Proxy/$Name/"
}

Step "6. Self-healing: kill a container and watch it come back"
$dep = Get-Deployment
$victim = $dep.replicas | Where-Object { $_.container_id } | Select-Object -First 1
if (-not $victim) {
    Fail "no container to kill"
} else {
    $victimCid = $victim.container_id
    Info "killing container $($victimCid.Substring(0,12)) (replica $($victim.id))"
    docker rm -f $victimCid | Out-Null
    # Heal = desired replicas running again AND the killed container id is gone.
    Wait-Until "platform self-healed back to $Replicas running (new container)" {
        $d = Get-Deployment
        if (-not $d) { return $false }
        $cids = @($d.replicas | ForEach-Object { $_.container_id })
        ($d.available_replicas -eq $Replicas) -and ($cids -notcontains $victimCid)
    } -TimeoutSec 40 | Out-Null
}

if ($Cleanup) {
    Step "7. Cleanup"
    Invoke-RestMethod "$ControlPlane/deployments/$Name" -Method Delete -TimeoutSec 10 | Out-Null
    Ok "deployment '$Name' deleted"
}

# --------------------------------------------------------------------------- #
Write-Host ""
if ($script:failed) {
    Write-Host "SMOKE TEST FAILED" -ForegroundColor Red
    exit 1
} else {
    Write-Host "SMOKE TEST PASSED — deploy, scheduling, proxy LB, and self-healing all work." -ForegroundColor Green
    exit 0
}

# Runs W1, W2, W3 and W5 against baseline (origin) and protected (gateway).
# W4 is separate because it stops Redis: .\benchmarks\w4-redis-failure.ps1
param(
    [string]$Duration = "30s",
    [int]$SpikeVus = 1000,
    [int]$Products = 100000
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

New-Item -ItemType Directory -Force -Path "benchmarks\results" | Out-Null

$jobs = @(
    @{ Name = "w1-uniform"; Script = "benchmarks/k6/w1-uniform.js"; Extra = @() },
    @{ Name = "w2-hotkey"; Script = "benchmarks/k6/w2-hotkey.js"; Extra = @("-e", "SPIKE_VUS=$SpikeVus") },
    @{ Name = "w3-moving"; Script = "benchmarks/k6/w3-moving-hotspot.js"; Extra = @() },
    @{ Name = "w5-overload"; Script = "benchmarks/k6/w5-overload.js"; Extra = @() }
)
$modes = @(
    @{ Name = "baseline"; Url = "http://host.docker.internal:8081" },
    @{ Name = "protected"; Url = "http://host.docker.internal:8080" }
)

function Invoke-K6([string]$Script, [string]$Url, [string]$Export, [string[]]$Extra) {
    $target = $Url
    if (Get-Command k6 -ErrorAction SilentlyContinue) {
        $target = $Url -replace 'host.docker.internal', 'localhost'
        $local = @("run", $Script, "--summary-export", $Export, "-e", "BASE_URL=$target", "-e", "DURATION=$Duration", "-e", "PRODUCTS=$Products") + $Extra
        & k6 @local
    } else {
        $dockerArgs = @(
            "run", "--rm",
            "-v", "${PWD}:/scripts",
            "grafana/k6:0.54.0",
            "run", "/scripts/$($Script -replace '\\','/')",
            "--summary-export", "/scripts/$($Export -replace '\\','/')",
            "-e", "BASE_URL=$target",
            "-e", "DURATION=$Duration",
            "-e", "PRODUCTS=$Products"
        ) + $Extra
        & docker @dockerArgs
    }
    if ($LASTEXITCODE -ne 0) { throw "k6 failed for $Script $Url" }
}

foreach ($mode in $modes) {
    foreach ($job in $jobs) {
        $export = "benchmarks/results/$($job.Name)-$($mode.Name).json"
        Write-Host "Running $($job.Name) against $($mode.Name) ($($mode.Url))"
        Invoke-K6 $job.Script $mode.Url $export $job.Extra
    }
}

Write-Host "Summaries written to benchmarks/results"

# Hot traffic through the gateway, with Redis stopped mid-run and started again.
param(
    [int]$Vus = 100,
    [int]$Products = 100000
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force -Path "benchmarks\results" | Out-Null

$k6 = Start-Process -FilePath "docker" -ArgumentList @(
    "run", "--rm",
    "-v", "${PWD}:/scripts",
    "grafana/k6:0.54.0",
    "run", "/scripts/benchmarks/k6/w4-redis-failure.js",
    "--summary-export", "/scripts/benchmarks/results/w4-protected.json",
    "-e", "BASE_URL=http://host.docker.internal:8080",
    "-e", "DURATION=70s",
    "-e", "VUS=$Vus",
    "-e", "PRODUCTS=$Products"
) -NoNewWindow -PassThru

Start-Sleep -Seconds 20
Write-Host "Stopping Redis nodes"
docker compose stop redis-1 redis-2 redis-3
Start-Sleep -Seconds 20
Write-Host "Starting Redis nodes"
docker compose start redis-1 redis-2 redis-3
if ($k6 -ne $null) { Wait-Process -Id $k6.Id }
Write-Host "Expect X-Cache BYPASS while Redis is down, and no 5xx responses."

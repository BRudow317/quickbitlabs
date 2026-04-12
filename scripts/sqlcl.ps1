#!/usr/bin/env pwsh
param(
    [string]$env = "homelab"
)

# Get the directory of this script
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$envFile = Join-Path $scriptDir ".env.md"

if (-not (Test-Path $envFile)) {
    Write-Error "Environment file not found: $envFile"
    exit 1
}

# Parse the .env.md file
$envContent = Get-Content $envFile -Raw

# Extract credentials for the specified environment
$userPattern = "oracle_${env}_user=(.+)"
$passPattern = "oracle_${env}_pass=(.+)"
$hostPattern = "oracle_${env}_host=(.+)"
$portPattern = "oracle_${env}_port=(.+)"
$servicePattern = "oracle_${env}_service_name=(.+)"

if ($envContent -match $userPattern) { $user = $matches[1].Trim() }
if ($envContent -match $passPattern) { $pass = $matches[1].Trim() }
if ($envContent -match $hostPattern) { $dbHost = $matches[1].Trim() }
if ($envContent -match $portPattern) { $port = $matches[1].Trim() }
if ($envContent -match $servicePattern) { $service = $matches[1].Trim() }

$connectionString = "${user}/${pass}@${dbHost}:${port}/${service}"
# Set ORACLE_HOME if not already set
if (-not $env:ORACLE_HOME) {
    $env:ORACLE_HOME = "C:\ORACLEHOME\WINDOWS.X64_193000_db_home"
}

# Set ORACLE_SID
$env:ORACLE_SID = $service

# Find sqlplus
$sqlplusPath = Join-Path $env:ORACLE_HOME "bin\sqlplus.exe"
if (-not (Test-Path $sqlplusPath)) {
    Write-Error "sqlplus not found at $env:ORACLE_HOME\bin\sqlplus.exe"
    Write-Error "Please verify ORACLE_HOME is set correctly"
    exit 1
}

# Start sqlplus with the connection string
# "$user/$pass@$dbHost`:$port/$service"
& $sqlplusPath $connectionString

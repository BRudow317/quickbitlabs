# run-spring-local.ps1
# Run from the project directory (current terminal directory).
# Loads .env into the current PowerShell process, confirms, runs mvnw, then restores env.

$projectRoot = (Get-Location).Path
$envPath     = Join-Path $projectRoot ".env"
$mvnwPath    = Join-Path $projectRoot "mvnw.cmd"

if (-not (Test-Path -LiteralPath $envPath)) {
  Write-Error "Missing .env file at: $envPath"
  exit 1
}

if (-not (Test-Path -LiteralPath $mvnwPath)) {
  Write-Error "Missing mvnw.cmd at: $mvnwPath (are you in the project root?)"
  exit 1
}

# Parse .env into an ordered dictionary (preserves file order)
$vars = [System.Collections.Specialized.OrderedDictionary]::new()

Get-Content -LiteralPath $envPath | ForEach-Object {
  $line = $_.Trim()
  if (-not $line -or $line.StartsWith('#')) { return }

  if ($line.StartsWith('export ')) { $line = $line.Substring(7).Trim() }

  if ($line -notmatch '^[A-Za-z_][A-Za-z0-9_]*=') { return }

  $name, $value = $line -split '=', 2
  $name  = $name.Trim()
  $value = $value.Trim()

  if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
    $value = $value.Substring(1, $value.Length - 2)
  }

  if (-not $vars.Contains($name)) { $vars.Add($name, $value) } else { $vars[$name] = $value }
}

if ($vars.Count -eq 0) {
  Write-Error "No KEY=VALUE entries found in $envPath"
  exit 1
}

Write-Host "Project root: $projectRoot"
Write-Host "Will load $($vars.Count) variables from .env for this run:`n"
$vars.Keys | ForEach-Object { Write-Host "  - $_" }

do {
  $resp = (Read-Host "`nContinue and run mvnw with these variables? (y/n)").Trim().ToLowerInvariant()
} while ($resp -notin @('y','n'))

if ($resp -ne 'y') {
  Write-Host "Aborted."
  exit 0
}

# Snapshot previous values to restore after
$previous = @{}
foreach ($k in $vars.Keys) {
  $previous[$k] = [System.Environment]::GetEnvironmentVariable($k, 'Process')
}

try {
  foreach ($k in $vars.Keys) {
    Set-Item -Path ("Env:{0}" -f $k) -Value ([string]$vars[$k])
  }

  # Run from project root so relative paths behave as expected
  Push-Location -LiteralPath $projectRoot
  try {
    & $mvnwPath spring-boot:run
    exit $LASTEXITCODE
  }
  finally {
    Pop-Location
  }
}
finally {
  foreach ($k in $vars.Keys) {
    if ($null -eq $previous[$k]) {
      Remove-Item -Path ("Env:{0}" -f $k) -ErrorAction SilentlyContinue
    } else {
      Set-Item -Path ("Env:{0}" -f $k) -Value ([string]$previous[$k])
    }
  }
}

# LoadVars.ps1
# Run from the project directory (current terminal directory).
# Loads .env into the current PowerShell session.

$projectRoot = (Get-Location).Path
$envPath     = Join-Path $projectRoot ".env"

if (-not (Test-Path -LiteralPath $envPath)) {
  Write-Error "Missing .env file at: $envPath"
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
Write-Host "Loading $($vars.Count) environment variables from .env:`n"
$vars.Keys | ForEach-Object { Write-Host "  - $_" }

# Load variables into current session
foreach ($k in $vars.Keys) {
  Set-Item -Path ("Env:{0}" -f $k) -Value ([string]$vars[$k])
}

Write-Host "`nEnvironment variables loaded successfully into current session." -ForegroundColor Green

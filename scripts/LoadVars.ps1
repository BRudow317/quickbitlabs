# LoadVars.ps1
# Loads .env into the current PowerShell session.
# Searches for .env in this order:
#   1. Specified path (via -EnvPath parameter)
#   2. Current working directory
#   3. Script root directory
#
# Usage:
#   .\LoadVars.ps1                              # Search in current dir, then script root
#   .\LoadVars.ps1 -EnvPath "C:\path\to\.env"   # Use specific path

param(
  [Parameter(Mandatory=$false)]
  [string]$EnvPath
)

# Determine which path to use (priority: explicit param > current dir > script root)
$envPathResolved = $null

if ($EnvPath) {
  # User provided explicit path
  if (Test-Path -LiteralPath $EnvPath) {
    $envPathResolved = (Resolve-Path -LiteralPath $EnvPath).Path
    Write-Host "Using .env from explicit path: $envPathResolved"
  } else {
    Write-Error "Provided .env path does not exist: $EnvPath"
    exit 1
  }
} else {
  # Check current working directory
  $currentDirEnv = Join-Path (Get-Location).Path ".env"
  if (Test-Path -LiteralPath $currentDirEnv) {
    $envPathResolved = (Resolve-Path -LiteralPath $currentDirEnv).Path
    Write-Host "Found .env in current directory: $envPathResolved"
  } else {
    # Check script root directory
    $scriptRoot = $PSScriptRoot
    $scriptRootEnv = Join-Path $scriptRoot ".env"
    if (Test-Path -LiteralPath $scriptRootEnv) {
      $envPathResolved = (Resolve-Path -LiteralPath $scriptRootEnv).Path
      Write-Host "Found .env in script root: $envPathResolved"
    }
  }
}

if (-not $envPathResolved) {
  Write-Error @"
Missing .env file. Searched in:
  1. Current directory: $(Join-Path (Get-Location).Path '.env')
  2. Script root: $(Join-Path $PSScriptRoot '.env')
Provide explicit path with: .\LoadVars.ps1 -EnvPath 'C:\path\to\.env'
"@
  exit 1
}

# Parse .env into an ordered dictionary (preserves file order)
$vars = [System.Collections.Specialized.OrderedDictionary]::new()

Get-Content -LiteralPath $envPathResolved | ForEach-Object {
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
  Write-Error "No KEY=VALUE entries found in $envPathResolved"
  exit 1
}

Write-Host "Loading $($vars.Count) environment variables from .env:`n"
$vars.Keys | ForEach-Object { Write-Host "  - $_" }

# Load variables into current session
foreach ($k in $vars.Keys) {
  Set-Item -Path ("Env:{0}" -f $k) -Value ([string]$vars[$k])
}

Write-Host "`nEnvironment variables loaded successfully into current session." -ForegroundColor Green
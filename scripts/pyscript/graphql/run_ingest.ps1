<#
Reads a .env-style file (KEY=VALUE) and exports to the current process, then runs main.py.
Usage:
  .\run_ingest.ps1 -EnvFile ".\.env"

Requires:
  - Python installed
  - venv optionally at .\.venv (or use system python)
#>

param(
  [string]$EnvFile = ".\.env",
  [string]$VenvPath = ".\.venv",
  [switch]$NoVenv
)

function Load-DotEnv([string]$Path) {
  if (!(Test-Path $Path)) {
    throw "Env file not found: $Path"
  }

  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line.Length -eq 0) { return }
    if ($line.StartsWith("#")) { return }

    $idx = $line.IndexOf("=")
    if ($idx -lt 1) { return }

    $key = $line.Substring(0, $idx).Trim()
    $val = $line.Substring($idx + 1).Trim()

    # Strip surrounding quotes
    if (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'"))) {
      $val = $val.Substring(1, $val.Length - 2)
    }

    [System.Environment]::SetEnvironmentVariable($key, $val, "Process")
  }
}

Load-DotEnv $EnvFile

if (-not $NoVenv) {
  $activate = Join-Path $VenvPath "Scripts\Activate.ps1"
  if (Test-Path $activate) {
    . $activate
  } else {
    Write-Host "Venv activate not found at $activate; continuing with system Python."
  }
}

python .\main.py

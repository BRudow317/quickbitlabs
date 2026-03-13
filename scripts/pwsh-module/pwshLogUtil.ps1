
# Define log directory
$LogRoot = "$home\.logs"
$SessionLogs = "$LogRoot\SessionLogs"
$timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
$todayDate = (Get-Date).ToString('yyyy-MM-dd')
$entry = "[{0}] {1,-7} {2}" -f $timestamp, $Level, $Message

# Set preferences so all streams emit output
$ErrorActionPreference = "Stop"
$WarningPreference = "Continue"
$VerbosePreference = "Continue"
$DebugPreference = "Continue"
$InformationPreference = "Continue"

# Create log directories and files if they don't exist
if (-not (Test-Path $LogRoot) ) {
    New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null
}
if (-not (Test-Path $SessionLogs) ) {
    New-Item -ItemType Directory -Force -Path $SessionLogs | Out-Null
}
$LogFiles = @{
    Output = Join-Path $SessionLogs "$todayDate-output.log"
    Error = Join-Path $SessionLogs "$todayDate-error.log"
    Warning = Join-Path $SessionLogs "$todayDate-warning.log"
    Verbose = Join-Path $SessionLogs "$todayDate-verbose.log"
    Debug = Join-Path $SessionLogs "$todayDate-debug.log"
    I
    nfo = Join-Path $SessionLogs "$todayDate-info.log"
}
if (-not (Test-Path $LogFiles.Output) ) {
    New-Item -ItemType File -Force -Path "$LogFiles.Output" | Out-Null
}
function Invoke-SessionLogTee {
    param(
        [Parameter(Mandatory)][ScriptBlock]$Script,
        [string]$customLog
    )

    & $Script *>&1 | ForEach-Object {
        switch ($_.GetType().Name) {
            'ErrorRecord' {
                $_ | Tee-Object -FilePath $LogFiles.Error -Append | Write-Error
            }
            'WarningRecord' {
                $_.Message | Tee-Object -FilePath $LogFiles.Warning -Append | Write-Warning
            }
            'VerboseRecord' {
                $_.Message | Tee-Object -FilePath $LogFiles.Verbose -Append | Write-Verbose
            }
            'DebugRecord' {
                $_.Message | Tee-Object -FilePath $LogFiles.Debug -Append | Write-Debug
            }
            'InformationRecord' {
                $_.MessageData | Tee-Object -FilePath $LogFiles.Info -Append | Write-Information -InformationAction Continue
            }
            default {
                $_ | Tee-Object -FilePath $LogFiles.Output -Append
            }
        }
    }
}



Capturing both file and console output:
Get-Process | Tee-Object -FilePath $LogPaths.Output

# Your main work section
{
    Write-Output "Running diagnostics..."
    Write-Warning "Low disk space"
    Write-Verbose "Starting operation details"
    Write-Debug "Current counter = 42"
    Write-Error "Test error occurred"
    Write-Information "Completed diagnostics"
}
1> $LogFiles.Output `
2> $LogFiles.Error `
3> $LogFiles.Warning `
4> $LogFiles.Verbose `
5> $LogFiles.Debug `
6> $LogFiles.Info


# The { … } block is just a script block — anything inside executes with streams redirected.
# All streams  *> all.log
# To append, use >> instead of >:
# Each n> operator pipes that stream to the given log file.

Get-Content $LogFiles.Error
Get-Content $LogFiles.Output | Out-GridView

function Write-Log {
    param(
        [Parameter(Mandatory)] [string]$Message,
        [ValidateSet('INFO','WARN','ERROR','DEBUG','VERBOSE')] [string]$Level = 'INFO'
    )

    $timestamp = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
    $entry = "[{0}] {1,-7} {2}" -f $timestamp, $Level, $Message

    switch ($Level) {
        'INFO' { Write-Information $entry -InformationAction Continue }
        'WARN' { Write-Warning $entry }
        'ERROR' { Write-Error $entry }
        'DEBUG' { Write-Debug $entry }
        'VERBOSE' { Write-Verbose $entry }
    }
}

Invoke-MultiStreamTee -Script {
    Write-Output "Normal output"
    Write-Warning "Low memory warning"
    Write-Verbose "Detailed trace" -Verbose
    Write-Debug "Debug details" -Debug
    Write-Error "Simulated failure"
    Write-Information "Informational message" -InformationAction Continue
}

function Invoke-Logged {
    param([ScriptBlock]$Script, [string]$Path = "C:\Logs\session.log")
    & $Script *>&1 | Tee-Object -FilePath $Path -Append
}


Invoke-Logged -Script {
    Write-Output "Processing..."
    Write-Error "Oops!"
    Write-Warning "Watch this!"
    Write-Information "Done" -InformationAction Continue
}

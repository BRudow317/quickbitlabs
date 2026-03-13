function Get-ExceptionDetails {
    # $PSItem is an automatic variable representing the current error object
        # $_ is an alias for $PSItem
        # $.Exception # .NET usage for an exception object
    param(#[ErrorRecord]$Exception,
          [System.Management.Automation.ErrorRecord]$Exception,#$Error,
          [ValidateSet("Continue", "SilentlyContinue", "Stop", "Inquire", "Ignore")]
          [string]$ErrorActionPreference = "Continue",
          [string]$scriptFileName = $(Split-Path -Leaf $PSCommandPath)
    )
    $ExceptionDetailsString = ""

    # Write-Host "An error occurred: $($_.Exception.Message)"
    $ExceptionDetailsString += "`n----------Get ExceptionDetails Begin Error Details--------------------`n" 
    $ExceptionDetailsString +=  "-----PWSH Exception Category Info:-----`n" # error category/context
    $ExceptionDetailsString +=  "$($Exception.CategoryInfo)" + "`n`n" # PowerShell error category info
    $ExceptionDetailsString +=  "-----PWSH Fully Qualified Error ID:-----`n"
    $ExceptionDetailsString +=  "$($Exception.FullyQualifiedErrorId)" + "`n`n" # PowerShell stable error identifier
    $ExceptionDetailsString +=  "-----PWSH Script Stack Trace:-----`n"
    $ExceptionDetailsString +=  "$($Exception.ScriptStackTrace)" + "`n`n" # PowerShell stack trace
    $ExceptionDetailsString +=  "-----.NET Stack Trace:-----`n"
    $ExceptionDetailsString +=  "$($Exception.Exception.StackTrace)" + "`n`n" # the .NET exception (use .Message, .GetType().FullName)
    $ExceptionDetailsString +=  "-----.NET Exception type:-----`n"
    $ExceptionDetailsString +=  "$($Exception.Exception.GetType().FullName)" + "`n`n" # type of .NET exception
    $ExceptionDetailsString +=  "-----PWSH Target Object:-----`n"
    $ExceptionDetailsString +=  "$($Exception.TargetObject)" + "`n`n" # object involved in the error
    $ExceptionDetailsString +=  "-----PWSH Invocation Info:-----`n"
    $ExceptionDetailsString +=  "$($Exception.InvocationInfo)" + "`n`n" # command, position, script line info
    $ExceptionDetailsString +=  "-----.NET Exception Message:-----`n"
    $ExceptionDetailsString +=  "$($Exception.Exception.Message)" + "`n`n" # message from the .NET exception
    $ExceptionDetailsString += "----------Get ExceptionDetails End Error Details--------------------`n`n"
    return $ExceptionDetailsString
}

 # Useful Try-Catch Handling
function Test-ScriptBlock {
    param (
        [scriptblock]$ScriptBlock
    )
    Try {
        & $ScriptBlock -ErrorActionPreference continue
    }
    Catch{
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference "SilentlyContinue")
    }
}
# ----Start of File: MyUtilModule.psm1---------------------------------------------------------------------------------------

# Dynamically import all .ps1 helper files in the module directory
$helperFiles = Get-ChildItem -Path $PSScriptRoot -Filter '*.ps1' -Exclude 'MyUtilModule.psm1'
foreach ($file in $helperFiles) {
    . $file.FullName
    Write-Host "Imported helper file: $($file.Name)"
}

# Dynamically export all functions defined by helper files
$functions = (Get-Command -Module $MyInvocation.MyCommand.Module | Where-Object { $_.CommandType -eq 'Function' }).Name
Export-ModuleMember -Function $functions # -Verbose

# Write-Host "Exported functions: $($functions -join ', ')"
Write-Host "Module MyUtilModule loaded functions."
# ---- End of File: MyUtilModule.psm1-------------------------------------------------------------------------------------
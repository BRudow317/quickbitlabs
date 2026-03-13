# ----Start of File: customPrompts-------------------------------------------------------------------------------------
# Global setup
$Global:CustomPrompt
$Global:promptColor = "Default"
$Global:rainbowIncrement = 0

# Build an ordered list of colors for rainbow iteration for PS7
$colorMap_V7 = [ordered]@{
    "brightcyan"     = "`e[96m"   # BrightCyan
    "grey"           = "`e[90m"   # BrightBlack # Grey
    "cyan"           = "`e[36m"   # Cyan
    "purple"         = "`e[35m"   # Magenta # Purple
    "brightgreen"    = "`e[92m"   # BrightGreen
    "brightwhite"    = "`e[97m"   # BrightWhite
    "red"            = "`e[31m"   # Red
    "gold"           = "`e[33m"   # Yellow # Gold
    "blue"           = "`e[34m"   # Blue
    "brightblue"     = "`e[94m"   # BrightBlue
    "orange"         = "`e[91m"   # BrightRed # Orange
    "green"          = "`e[32m"   # Green
    "white"          = "`e[37m"   # White
    "pink"           = "`e[95m"   # BrightMagenta # Pink
    "yellow"         = "`e[93m"   # BrightYellow # Yellowish
}

# - Black, DarkBlue, DarkGreen, DarkCyan, DarkRed, DarkMagenta, DarkYellow
# - Gray, Blue, Green, Cyan, Red, Magenta, Yellow, White
# Build an ordered list of colors for rainbow iteration for PS5
$colorMap_V5 = [ordered]@{
    "brightcyan"     = "Cyan"   # BrightCyan
    "grey"           = "Gray"    # BrightBlack # Grey
    "cyan"           = "DarkCyan"   # Cyan
    "purple"         = "DarkMagenta"   # DarkMagenta # Purple
    "brightgreen"    = "Green"   # BrightGreen
    "white"          = "White"   # White
    "red"            = "Red"   # Red
    "gold"           = "DarkYellow"   # Yellow # Gold
    "blue"           = "DarkBlue"   # Blue
    "brightblue"     = "Blue"   # BrightBlue
    "orange"         = "DarkRed"   # BrightRed # Orange
    "green"          = "DarkGreen"   # Green
    #"brightwhite"    = "White"   # BrightWhite
    "pink"           = "Magenta"   # BrightMagenta # Pink
    "yellow"         = "Yellow"   # BrightYellow # Yellowish
}

# Switch-PromptColor -Color "Blue"
function Switch-PromptColor {
    param( [string]$Color )
    #Write-Host "function called Set-Rainbow_V7 with Color: $Color"
    if (-not $PSBoundParameters.ContainsKey('Color') -or [string]::IsNullOrWhiteSpace($Color) ) {
        $Color = Read-Host "What Color do you want Prompt?"
    }
    $Global:promptColor = $Color
}

# Vaule that returns for each Prompt injection
function prompt {
    Try{
        return Get-CustomPrompt -colorChoice $Global:promptColor
    } Catch{
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference SilentlyContinue)
    }
}

# --- Custom Prompt Function ---
function Get-CustomPrompt {
    param([string]$colorChoice = "Default")
    try {
        #Write-Host "function called Get-CustomPrompt with Color: $colorChoice"
        if ($null -eq $colorChoice -or $colorChoice.ToLower() -eq "default") {
            Set-DefaultPrompt
            return $Global:globalCustomPrompt
        }
        $pwshVersion = $PSVersionTable["PSVersion"].ToString()[0] #?? "0"# $null coalescing operator only available in pwsh7
        if ($null -eq $pwshVersion) {$pwshVersion = "0"}
        $colorChoice = $colorChoice.ToLower()
        # To correct later: Add Version 5 support
        if ( ($pwshVersion -eq "7") -or ($pwshVersion -eq "5")) {
            if ($colorChoice -eq "rainbow") {
                & "Set-Rainbow_V$pwshVersion"
            }
            # To correct Later: Add validation for colorChoice here
            elseif ($null -ne $colorChoice) {
                & "Set-StaticColor_V$pwshVersion -colorChoice $colorChoice"
            }
            else {
                Write-Host "Invalid Color Choice."
                Set-DefaultPrompt
            }
        }
        else {
            Write-Host "Powershell Version $pwshVersion Unsupported"
            Set-DefaultPrompt
        }
        return $Global:globalCustomPrompt
    }
    catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference SilentlyContinue)
        Set-DefaultPrompt
        return $Global:globalCustomPrompt
    }
}

# Set-DefaultPrompt
function Set-DefaultPrompt {
    $Global:promptColor = "Default"
    $Global:globalCustomPrompt = "PS $(Get-Location) > "
}

# --- Rainbow Prompt Function ---
function Set-Rainbow_V7 {
    try {
        #Write-Host "function called Set-Rainbow_V7"
        $newPromptArray = ("PS $(Get-Location) > ").ToCharArray()
        $Global:globalCustomPrompt = ""
        $keys = @($colorMap_V7.Keys) # sets $keys to an array instead of list
        #Write-Output $keys.GetType().Name
        foreach ($char in $newPromptArray) {
            if ($Global:rainbowIncrement -ge $colorMap_V7.Count) {
                $Global:rainbowIncrement = 0
            }
            if ($char -ne " ") {
                $colorByIndex = $keys[$Global:rainbowIncrement]
                # Get the color code based on the current increment
                $Global:globalCustomPrompt += $colorMap_V7[$colorByIndex] + $char
                $Global:rainbowIncrement++
            }
            elseif ($char -eq " ") {
                $Global:globalCustomPrompt += $char
                $Global:rainbowIncrement++
            }
            else {
                throw "Exception thrown in rainbow()"
            }
        }
    }
    catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference SilentlyContinue)
        Set-DefaultPrompt
    }
}

# --- Static Color Prompt Function ---
function Set-StaticColor_V7 {
    param([string]$colorChoice = "Default")
    try{
        if ($colorMap_V7.ContainsKey($colorChoice)) {
            $colorCode = $colorMap_V7[$colorChoice]
            $Global:globalCustomPrompt = $colorCode + "PS $(Get-Location) > "
        }
        else {
            Write-Host "Invalid color choice: $colorChoice"
            Set-DefaultPrompt
        }
    }
    catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference SilentlyContinue)
        Set-DefaultPrompt
    }
}

function Set-Rainbow_V5 {
    # Still needs to be implemented
    param([string]$colorChoice = "Default")
    Write-Host "function called Set-Rainbow_V5"
    $Global:globalCustomPrompt = "PS $(Get-Location) > "
}

function Set-StaticColor_V5 {
    param([string]$colorChoice = "Default")
    try {
        Write-Host "function called Set-StaticColor_V5 with Color: $colorChoice"
        if( $colorArray.ContainsKey($colorChoice.ToLower())) {
            $colorChoice = $colorMap_V5[$colorChoice]
            $Global:promptColor = $colorChoice
            $Global:globalCustomPrompt = "$(Get-Host).UI.RawUI.ForegroundColor = '$colorName'; PS $(Get-Location) > "
        }
        else {
            Write-Host "Invalid color choice: $colorChoice"
            Set-DefaultPrompt
        }
        $Global:globalCustomPrompt = "PS $(Get-Location) > "
    }
    catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference SilentlyContinue)
        Set-DefaultPrompt
    }
}

# ---- End of File-------------------------------------------------------------------------------------

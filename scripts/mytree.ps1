function Invoke-MyTree {
param(
    [Alias('f')]
    [switch]$IncludeFiles,

    [string]$Root = '.',

    [string]$Safety = '1000',

    [ValidateSet('path', 'json', 'tree')]
    [string]$Output = 'path',

    [Alias('w')]
    [switch]$WindowsPaths,

    [string[]]$Exclude = @()
)

$default_exclusions = @(
    'venv',
    '.venv',
    '.git',
    '__pycache__',
    'tests/*',
    '.trash',
    '.bin',
    'node_modules',
    'dist',
    'build',
    'database',
    '.pytest_cache',
    '.vscode',
    '.logs',
    '.cache',
    'cache',
    '*.log',
    '*.tmp',
    'scripts/*'
)

$all_exclusions = $default_exclusions + $Exclude
$glob_chars = [char[]]'*?[]'
$name_excludes = @()
$path_globs = @()

foreach ($rule in $all_exclusions) {
    $normalized = ($rule -replace '\\', '/').Trim('/')

    if ($normalized.IndexOfAny($glob_chars) -ge 0 -or $normalized.Contains('/')) {
        $path_globs += $normalized
    }
    else {
        $name_excludes += $normalized
    }
}

function Test-Excluded {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RelativePath,

        [Parameter(Mandatory = $true)]
        [string]$LeafName
    )

    if ($name_excludes -contains $LeafName) {
        return $true
    }

    foreach ($glob in $path_globs) {
        if ($RelativePath -like $glob -or $RelativePath -like "*/$glob") {
            return $true
        }
    }

    return $false
}

function Format-OutputPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if ($WindowsPaths) {
        return $Path
    }

    return $Path -replace '\\', '/'
}

if ($Safety -ieq 'off') {
    $max_items = $null
}
else {
    $parsed_safety = 0

    if (-not [int]::TryParse($Safety, [ref]$parsed_safety) -or $parsed_safety -lt 1) {
        throw "Safety must be a positive integer or 'off': $Safety"
    }

    $max_items = $parsed_safety
}

$resolved_root = Resolve-Path -LiteralPath $Root -ErrorAction Stop
$root_item = Get-Item -LiteralPath $resolved_root.Path -ErrorAction Stop

if (-not $root_item.PSIsContainer) {
    throw "Root path must be a directory: $Root"
}

$root = $root_item.FullName

if ($Output -ieq 'json') {
    $state = @{ count = 0; limit_hit = $false }

    function Build-JsonTree {
        param(
            [string]$StartPath,
            [hashtable]$State
        )

        $root_node = [ordered]@{}
        $pending = [System.Collections.Generic.Queue[object]]::new()
        $pending.Enqueue(@($StartPath, $root_node))

        while ($pending.Count -gt 0 -and -not $State.limit_hit) {
            $entry = $pending.Dequeue()
            $current_dir  = $entry[0]
            $current_node = $entry[1]

            $dirs  = Get-ChildItem -LiteralPath $current_dir -Directory -Force -ErrorAction SilentlyContinue | Sort-Object Name
            $files = Get-ChildItem -LiteralPath $current_dir -File      -Force -ErrorAction SilentlyContinue | Sort-Object Name

            # Dirs first - sorted
            $dirs | ForEach-Object {
                if ($State.limit_hit) { return }
                $relative = ([System.IO.Path]::GetRelativePath($root, $_.FullName)) -replace '\\', '/'
                if (-not (Test-Excluded -RelativePath $relative -LeafName $_.Name)) {
                    if ($null -ne $max_items -and $State.count -ge $max_items) {
                        $State.limit_hit = $true; return
                    }
                    $child_node = [ordered]@{}
                    $current_node[$_.Name] = $child_node
                    $State.count++
                    # Enqueue child node ref alongside its path - hashtables are refs
                    # so mutations to $child_node in later iterations reflect in the tree.
                    $pending.Enqueue(@($_.FullName, $child_node))
                }
            }

            # Files second - sorted
            if ($IncludeFiles) {
                $files | ForEach-Object {
                    if ($State.limit_hit) { return }
                    $relative = ([System.IO.Path]::GetRelativePath($root, $_.FullName)) -replace '\\', '/'
                    if (-not (Test-Excluded -RelativePath $relative -LeafName $_.Name)) {
                        if ($null -ne $max_items -and $State.count -ge $max_items) {
                            $State.limit_hit = $true; return
                        }
                        $current_node[$_.Name] = $null
                        $State.count++
                    }
                }
            }
        }

        return $root_node
    }

    $root_key = Format-OutputPath -Path $root
    $tree = [ordered]@{ $root_key = Build-JsonTree -StartPath $root -State $state }

    if ($state.limit_hit) {
        Write-Warning "Output truncated at $max_items items. Use -Safety off or -Safety <n> to raise the limit."
    }

    $tree | ConvertTo-Json -Depth 100
}
elseif ($Output -ieq 'tree') {
    $state = @{ count = 0; limit_hit = $false }

    function Write-Tree {
        param(
            [string]$DirPath,
            [string]$Prefix = '',
            [hashtable]$State
        )

        $dirs  = Get-ChildItem -LiteralPath $DirPath -Directory -Force -ErrorAction SilentlyContinue |
                 Sort-Object Name |
                 Where-Object {
                     $relative = ([System.IO.Path]::GetRelativePath($root, $_.FullName)) -replace '\\', '/'
                     -not (Test-Excluded -RelativePath $relative -LeafName $_.Name)
                 }

        $files = if ($IncludeFiles) {
            Get-ChildItem -LiteralPath $DirPath -File -Force -ErrorAction SilentlyContinue |
            Sort-Object Name |
            Where-Object {
                $relative = ([System.IO.Path]::GetRelativePath($root, $_.FullName)) -replace '\\', '/'
                -not (Test-Excluded -RelativePath $relative -LeafName $_.Name)
            }
        } else { @() }

        # Dirs first, files second - both sorted
        $items = @($dirs) + @($files)

        for ($i = 0; $i -lt $items.Count; $i++) {
            if ($State.limit_hit) { return }

            $item    = $items[$i]
            $is_last = $i -eq ($items.Count - 1)

            $connector    = if ($is_last) { '└── ' } else { '├── ' }
            $continuation = if ($is_last) { '    ' } else { '│   ' }

            if ($null -ne $max_items -and $State.count -ge $max_items) {
                $State.limit_hit = $true; return
            }

            if ($item.PSIsContainer) {
                Write-Output "$Prefix$connector$($item.Name)/"
                $State.count++
                Write-Tree -DirPath $item.FullName -Prefix "$Prefix$continuation" -State $State
            }
            else {
                Write-Output "$Prefix$connector$($item.Name)"
                $State.count++
            }
        }
    }

    $root_label = Format-OutputPath -Path $root
    Write-Output "$root_label/"
    Write-Tree -DirPath $root -Prefix '' -State $state

    if ($state.limit_hit) {
        Write-Warning "Output truncated at $max_items items. Use -Safety off or -Safety <n> to raise the limit."
    }
}
else {
    $pending = [System.Collections.Generic.Queue[string]]::new()
    $pending.Enqueue($root)
    $item_count = 0
    $limit_hit = $false

    while ($pending.Count -gt 0) {
        $current = $pending.Dequeue()

        $dirs  = Get-ChildItem -LiteralPath $current -Directory -Force -ErrorAction SilentlyContinue | Sort-Object Name
        $files = Get-ChildItem -LiteralPath $current -File      -Force -ErrorAction SilentlyContinue | Sort-Object Name

        # Dirs first - sorted
        $dirs | ForEach-Object {
            if ($limit_hit) { return }
            $relative = ([System.IO.Path]::GetRelativePath($root, $_.FullName)) -replace '\\', '/'
            if (-not (Test-Excluded -RelativePath $relative -LeafName $_.Name)) {
                if ($null -ne $max_items -and $item_count -ge $max_items) {
                    $limit_hit = $true; return
                }
                Format-OutputPath -Path $_.FullName
                $item_count++
                $pending.Enqueue($_.FullName)
            }
        }

        if ($limit_hit) { break }

        # Files second - sorted
        if ($IncludeFiles) {
            $files | ForEach-Object {
                if ($limit_hit) { return }
                $relative = ([System.IO.Path]::GetRelativePath($root, $_.FullName)) -replace '\\', '/'
                if (-not (Test-Excluded -RelativePath $relative -LeafName $_.Name)) {
                    if ($null -ne $max_items -and $item_count -ge $max_items) {
                        $limit_hit = $true; return
                    }
                    Format-OutputPath -Path $_.FullName
                    $item_count++
                }
            }
        }

        if ($limit_hit) { break }
    }

    if ($limit_hit) {
        Write-Warning "Output truncated at $max_items items. Use -Safety off or -Safety <n> to raise the limit."
    }
}
}

Set-Alias -Name mytree -Value Invoke-MyTree
Export-ModuleMember -Function Invoke-MyTree -Alias mytree
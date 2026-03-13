# ----Start of file excelUtil-----------------------------------------------------------------
$Global:openWorkbooks = [ordered]@{}
function Open-ExcelWorkbook {
    param ([string]$filePath,
        [ValidateSet("True", "False")] [string]$showWorkbook = "False"
    )
    Try {
        if(Test-Path $filePath) {
            $excel = New-Object -ComObject Excel.Application
            $workbook = $excel.Workbooks.Open("$filePath")
            $Global:openWorkbooks["$filePath"] = $workbook
            if ($showWorkbook -eq "True") {
                $excel.Visible = $true
            } else {
                $excel.Visible = $false
            }
            Write-Host "Opened Excel workbook: $filePath"
            return $workbook
        }
        else {
            throw "File not found: $filePath"
        }
    }
    Catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference "Stop")
    }
}

function Get-WorkSheets {
    param ([object]$workbook)
    Try {
        $WorkSheets = [ordered]@{}
        foreach ($sheet in $workbook.Sheets) {
            $WorkSheets["$($sheet.Name)"] = $sheet
        }
        return $WorkSheets
    }
    Catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference "Stop")
    }
}

function ConvertWbToCsv {
    param ([string]$WorkbookPath,
        [string]$outputCsv = "$($WorkbookPath)".ToString().Replace(".xlsx", ".csv")
    )
    $finalCsvData = @()
    Open-ExcelWorkbook -filePath $WorkbookPath -showWorkbook "False"
    $workbook = $Global:openWorkbooks["$WorkbookPath"]
    $WorkSheets = Get-WorkSheets -workbook $workbook
    Try {
        foreach ($sheetName in $WorkSheets.Keys) {
            $csvData = @()
            $rowRange = $WorkSheets[$sheetName].UsedRange.Rows.Count
            $colRange = $WorkSheets[$sheetName].UsedRange.Columns.Count
            for ($rowNum=1; $rowNum -lt $rowRange; $rowNum++){
                $rowRecord = ""
                for($colNum=1; $colNum -lt $colRange; $colNum++) {
                    $cellText = $WorkSheets[$sheetName].Cells.Item($colNum, $rowNum).Text
                    if(-not [string]::IsNullOrEmpty($cellText)) {
                        # Not sure to what extent these replacements are needed for CSV compliance
                        #$cellText = $cellText.Replace('"','""')
                        #$cellText = $cellText.Replace("`r`n", '<<<CRLF>>>')
                        #$cellText = $cellText.Replace("`n",   '<<<LF>>>')
                        #$cellText = $cellText.Replace("`r",   '<<<CR>>>')
                        $cellText = '"' + $cellText + '"'
                    }else{
                        $cellText = ""
                    }
                    if ($colNum + 1 -lt $colRange) {
                        $rowRecord += "$cellText" + ","
                    }
                    $rowRecord += "$cellText"
                }
                $csvData += $rowRecord
            }
            $finalCsvData = $csvData -join "`n"
        }
        # Save to CSV
        $finalCsvData | Export-Csv -Path $outputCsv -NoTypeInformation -Encoding UTF8
        Write-Host "Converted workbook to CSV: $outputCsv"
        Close-ExcelWorkbook -filePath "$WorkbookPath" -saveChanges "False"
    }
    Catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference "Stop")
    }
}

function Close-ExcelWorkbook {
    param ([string]$filePath,
        [ValidateSet("True", "False")] [string]$saveChanges = "False"
    )
    Try {
        if ($Global:openWorkbooks.ContainsKey("$filePath")) {
            $workbook = $Global:openWorkbooks["$filePath"]
            if ($saveChanges -eq "True") {
                $workbook.Save()
            }
            $workbook.Close()
            $excel = $workbook.Application
            $excel.Quit()
            [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
            Remove-Variable excel
            Remove-Variable workbook
            [GC]::Collect()
            [GC]::WaitForPendingFinalizers()
            $Global:openWorkbooks.Remove("$filePath")
            Write-Host "Closed Excel workbook: $filePath"
        } else {
            throw "Workbook not found in open workbooks: $filePath"
        }
    }
    Catch {
        Write-Host $(Get-ExceptionDetails -Exception $_ -ErrorActionPreference "Stop")
    }
}

function Close-ExcelWbForce {
    # param([string]$filePath)
    # Attempt to get an active instance of the Excel Application COM object
    try {
        $excel = [Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')
    }
    catch {
        Write-Host "Could not find a running instance of Microsoft Excel."
        exit
    }

    Write-Host "Found active Excel instance. Open workbooks:"
    
    # Loop through all open workbooks in that instance and display their FullName property
    foreach ($workbook in $excel.Workbooks) {
        # FullName provides the complete path and filename
        $path = $workbook.FullName
       
        if (-not [string]::IsNullOrEmpty($path)) {
            Write-Host $path
        }
        else {
            # This case handles new, unsaved workbooks (e.g., Book1, Book2)
            Write-Host "Unsaved workbook: $($workbook.Name)"
        }
    }
    # $excel = New-Object -ComObject Excel.Application
    # $workbook = $excel.Workbooks.Open("$filePath")
    # $workbook.Close()
    # #$excel = $workbook.Application
    # $excel.Visible = $true
    # $excel.Quit()
    # [System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null
    # Remove-Variable excel
    # Remove-Variable workbook
    # [GC]::Collect()
    # [GC]::WaitForPendingFinalizers()
}

# ----End of file excelUtil-----------------------------------------------------------------
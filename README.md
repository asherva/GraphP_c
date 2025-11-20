$excelPath = "C:\data\input.xlsx"
$outPath   = "C:\data\output.txt"

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$wb = $excel.Workbooks.Open($excelPath)
$ws = $wb.Sheets.Item(1)

$rows = @()

# כותרת קבועה
$rows += "GarageId`tאחוז סופי"

$row = 2   # שורה אחרי הכותרת באקסל

while ($ws.Cells.Item($row,1).Value() -ne $null) {

    $id = $ws.Cells.Item($row,1).Value()
    $pct = $ws.Cells.Item($row,2).Value()

    # אם הערך הוא "86%" → "86"
    if ($pct -is [string] -and $pct.Contains("%")) {
        $pct = $pct.Replace("%","")
    }

    # אם הערך הוא 0.86 → 86
    if ($pct -is [double] -and $pct -ge 0 -and $pct -le 1) {
        $pct = $pct * 100
    }

    $rows += "$id`t$pct"
    $row++
}

$rows | Out-File $outPath -Encoding UTF8

$wb.Close($false)
$excel.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null

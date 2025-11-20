$excelPath = "C:\data\input.xlsx"
$outPath   = "C:\data\output.txt"

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$wb = $excel.Workbooks.Open($excelPath)
$ws = $wb.Sheets.Item(1)

$rows = @()

# כותרת קבועה
$rows += "GarageId`tאחוז סופי"

$row = 2

while ($ws.Cells.Item($row,1).Text -ne "") {

    # שולף בדיוק מה שרואים באקסל
    $id  = $ws.Cells.Item($row,1).Text
    $pct = $ws.Cells.Item($row,2).Text

    # המרת "86%" → "86"
    if ($pct.Contains("%")) {
        $pct = $pct.Replace("%","")
    }

    $rows += "$id`t$pct"
    $row++
}

$rows | Out-File $outPath -Encoding UTF8

$wb.Close($false)
$excel.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null

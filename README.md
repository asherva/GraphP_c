$excelPath = "C:\Users\AIG\Desktop\all\data\input.xlsx"
$outPath   = "C:\Users\AIG\Desktop\all\data\output.txt"
$column    = 1   # מספר העמודה (1 = A)

$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$wb = $excel.Workbooks.Open($excelPath)
$ws = $wb.Sheets.Item(1)

$lines = @()
$row = 1

while ($ws.Cells.Item($row, $column).Value() -ne $null) {

    $v = $ws.Cells.Item($row, $column).Value()

    # אם הערך בין 0 ל-1 -> כנראה אחוז, המרה ל-100
    if ($v -is [double] -and $v -ge 0 -and $v -le 1) {
        $v = $v * 100
    }

    # אם כתוב טקסט כמו "12%"
    if ($v -is [string] -and $v.Contains("%")) {
        $v = $v.Replace("%","")
    }

    $lines += $v
    $row++
}

$lines | Out-File $outPath -Encoding UTF8

$wb.Close($false)
$excel.Quit()
[System.Runtime.Interopservices.Marshal]::ReleaseComObject($excel) | Out-Null

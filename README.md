Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='C:' OR DeviceID='D:'" |
Select-Object DeviceID,
    @{Name='Free(GB)';Expression={[math]::Round($_.FreeSpace/1GB,2)}},
    @{Name='Total(GB)';Expression={[math]::Round($_.Size/1GB,2)}},
    @{Name='FreePercent';Expression={[math]::Round(($_.FreeSpace/$_.Size)*100,2)}}

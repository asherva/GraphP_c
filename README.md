$RemoteMachine = "TableauServer01"    # שם השרת המרוחק

# פרמטרים ל-Tableau
$Server = "http://TableauServer01"
$TableauUser = "TableauUser"
$Site = "Default"

Invoke-Command -ComputerName $RemoteMachine -ScriptBlock {
    param($Server, $TableauUser, $Site)

    # קריאת הסיסמה המוצפנת מהשרת המרוחק
    $SecurePassword = Get-Content "C:\Secure\TableauPassword.txt" | ConvertTo-SecureString
    $Cred = New-Object System.Management.Automation.PSCredential ($TableauUser, $SecurePassword)
    $PlainPassword = $Cred.GetNetworkCredential().Password

    # התחברות ל-Tableau והפעלת רענון
    tabcmd login -s $Server -u $TableauUser -p $PlainPassword --site $Site
    tabcmd refreshextracts --workbook "SalesReport"
    tabcmd logout
} -ArgumentList $Server, $TableauUser, $Site

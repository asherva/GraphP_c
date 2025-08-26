# קריאה מהקובץ ופענוח
$SecurePassword = Get-Content "C:\Secure\TableauPassword.txt" | ConvertTo-SecureString
$Credential = New-Object System.Management.Automation.PSCredential ("USERNAME", $SecurePassword)

# חילוץ הסיסמה כטקסט (נדרש כי tabcmd לא תומך SecureString)
$PlainPassword = $Credential.GetNetworkCredential().Password

# פרטי החיבור
$Server = "https://your-tableau-server"
$User = "USERNAME"
$Site = "yoursite"   # אם יש site name

# התחברות לשרת
tabcmd login -s $Server -u $User -p $PlainPassword --site $Site

# רענון וורקבוק (דוגמה: workbook בשם SalesReport)
tabcmd refreshextracts --workbook "SalesReport" --project "Default"

# יציאה
tabcmd logout

# בקשת סיסמה בצורה מוסתרת
$SecurePassword = Read-Host "Enter Tableau Password" -AsSecureString

# שמירה לקובץ מוצפן (רק אותו משתמש/שרת יוכל לקרוא אותו)
$SecurePassword | ConvertFrom-SecureString | Out-File "C:\Secure\TableauPassword.txt"

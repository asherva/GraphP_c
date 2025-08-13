import pyodbc
import pandas as pd

# פרטי ההתחברות
server = 'SERVER_NAME\\INSTANCE'  # לדוגמה: 'SQLSRV01\\MSSQL2019'
database = 'SalesDB'
username = 'YourUser'
password = 'YourPassword'

# חיבור ל-MSSQL עם סיסמה
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)
conn = pyodbc.connect(conn_str)

# קריאת נתונים מטבלת המכירות
df = pd.read_sql("SELECT TOP 10 * FROM Sales", conn)

print(df)

conn.close()

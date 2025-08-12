import pyodbc
import pandas as pd

def load_sales_data():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=YOUR_SERVER;'
        'DATABASE=YOUR_DB;'
        'UID=YOUR_USER;'
        'PWD=YOUR_PASSWORD'
    )
    query = "SELECT * FROM SalesTable"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

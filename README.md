# db.py
import json
import pyodbc
import pandas as pd
from typing import Optional

def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def connect_mssql(cfg: dict):
    driver = cfg.get("driver", "ODBC Driver 17 for SQL Server")
    server = cfg["server"]
    database = cfg["database"]
    trusted = cfg.get("trusted_connection", False)

    if trusted:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
    else:
        user = cfg["username"]
        pwd = cfg["password"]
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};PWD={pwd};"
            "TrustServerCertificate=yes;"
        )
    return pyodbc.connect(conn_str)

def load_sales_table(query: Optional[str] = None, cfg_path="config.json") -> pd.DataFrame:
    cfg = load_config(cfg_path)["mssql"]
    conn = connect_mssql(cfg)
    if query is None:
        # החלף את SalesTable בשם הטבלה שלך או השתמש ב־VIEW מותאם
        query = "SELECT TOP (10000) * FROM SalesTable"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

if __name__ == "__main__":
    df = load_sales_table()
    print(df.head())
    print(df.shape)

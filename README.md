import pyodbc

def connect_mssql():
    connection_string = """
        DRIVER={ODBC Driver 18 for SQL Server};
        SERVER=שרת_או_IP\\אינסטנס;
        DATABASE=SalesDB;
        Trusted_Connection=yes;
        TrustServerCertificate=yes;
    """
    return pyodbc.connect(connection_string)

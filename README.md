import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== הגדרות חיבור ל-Tableau Server =====
SERVER = "https://your-tableau-server"
USERNAME = "your-username"
PASSWORD = "your-password"
API_VERSION = "3.22"  # גרסת ה-API בשרת שלך
MAX_WORKERS = 10      # מספר חיבורים במקביל
VERIFY_SSL = False    # עקיפת בדיקת SSL

# ביטול אזהרות SSL
requests.packages.urllib3.disable_warnings()

# ===== פונקציות =====
def signin():
    """התחברות לשרת Tableau"""
    url = f"{SERVER}/api/{API_VERSION}/auth/signin"
    payload = {
        "credentials": {
            "name": USERNAME,
            "password": PASSWORD,
            "site": {"contentUrl": ""}
        }
    }
    r = requests.post(url, json=payload, verify=VERIFY_SSL)
    r.raise_for_status()
    res = r.json()["credentials"]
    return res["token"], res["site"]["id"], res["user"]["id"]

def get_sites(token):
    """החזרת רשימת כל ה-Sites בשרת"""
    url = f"{SERVER}/api/{API_VERSION}/sites?pageSize=1000"
    headers = {"X-Tableau-Auth": token}
    r = requests.get(url, headers=headers, verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()["sites"]["site"]

def graphql_query(token, site_id, query):
    """הרצת שאילתת GraphQL מול ה-Metadata API"""
    url = f"{SERVER}/api/metadata/graphql"
    headers = {
        "X-Tableau-Auth": token,
        "X-Tableau-Site-Id": site_id,
        "Content-Type": "application/json"
    }
    r = requests.post(url, json={"query": query}, headers=headers, verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()

def process_site(site, token):
    """סריקת Site בודד והחזרת כל הנתונים שלו"""
    site_id = site["id"]
    site_name = site["name"]

    print(f"🔍 סורק את האתר: {site_name}")

    query = """
    {
      workbooks {
        name
        datasources {
          name
          upstreamTables {
            name
            schema
            connectionType
          }
          ... on CustomSQLTable {
            query
          }
        }
      }
    }
    """

    rows = []
    try:
        data = graphql_query(token, site_id, query)
        if "data" in data and "workbooks" in data["data"]:
            for wb in data["data"]["workbooks"]:
                wb_name = wb["name"]
                for ds in wb.get("datasources", []):
                    ds_name = ds["name"]
                    sql_text = ds.get("query", None)  # custom SQL אם יש
                    if ds.get("upstreamTables"):
                        for tbl in ds["upstreamTables"]:
                            rows.append({
                                "Site": site_name,
                                "Workbook": wb_name,
                                "DataSource": ds_name,
                                "Table": tbl["name"],
                                "Schema": tbl.get("schema", ""),
                                "ConnectionType": tbl.get("connectionType", ""),
                                "SQL": sql_text
                            })
                    else:
                        # אם אין טבלה, עדיין נשמור את ה-SQL
                        rows.append({
                            "Site": site_name,
                            "Workbook": wb_name,
                            "DataSource": ds_name,
                            "Table": "",
                            "Schema": "",
                            "ConnectionType": "",
                            "SQL": sql_text
                        })
    except Exception as e:
        print(f"⚠️ שגיאה בסריקת האתר {site_name}: {e}")
        rows.append({
            "Site": site_name,
            "Workbook": "ERROR",
            "DataSource": "",
            "Table": "",
            "Schema": "",
            "ConnectionType": "",
            "SQL": f"שגיאה: {e}"
        })

    return rows

# ===== ריצה ראשית =====
if __name__ == "__main__":
    token, default_site_id, user_id = signin()
    sites = get_sites(token)

    all_rows = []

    # ריצה במקביל על כל ה-Sites
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_site = {executor.submit(process_site, site, token): site for site in sites}
        for future in as_completed(future_to_site):
            site_rows = future.result()
            all_rows.extend(site_rows)

    # יצירת קובץ פלט
    df = pd.DataFrame(all_rows)
    df.to_excel("tableau_all_sites_sql_metadata.xlsx", index=False)

    print("✅ ניתוח הושלם. הקובץ 'tableau_all_sites_sql_metadata.xlsx' נוצר בהצלחה.")



מה התוכנית עושה
מתחברת ל־Tableau Server עם שם משתמש וסיסמה.

מביאה את כל ה־Sites.

רצה במקביל (ThreadPoolExecutor) על כל Site כדי להאיץ את התהליך.

לכל Site:

מביאה את כל ה־Workbooks.

לכל Workbook מביאה את כל ה־Data Sources.

לכל Data Source מביאה את:

שמות הטבלאות (upstreamTables).

קוד ה־Custom SQL אם קיים.

שומרת את כל התוצאות בקובץ Excel אחד.

מטפלת בשגיאות – גם אם Site או Workbook אחד נופל, ממשיכים הלאה.

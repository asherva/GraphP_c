import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== הגדרות =====
SERVER = "https://your-tableau-server"
USERNAME = "your-username"
PASSWORD = "your-password"
API_VERSION = "3.22"
MAX_WORKERS = 10
VERIFY_SSL = False  # עקיפת בדיקת SSL

# ביטול אזהרות SSL
requests.packages.urllib3.disable_warnings()

# ===== פונקציות בדיקה =====
def test_rest_api():
    """בודק חיבור ל-REST API ומחזיר token ו-site_id"""
    print("🔍 בודק חיבור ל-REST API...")
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
    print("✅ REST API מחובר")
    return res["token"], res["site"]["id"], res["user"]["id"]

def test_metadata_api(token, site_id):
    """בודק חיבור ל-Metadata API"""
    print("🔍 בודק חיבור ל-Metadata API...")
    query = "{ workbooks { name } }"
    url = f"{SERVER}/api/metadata/graphql"
    headers = {
        "X-Tableau-Auth": token,
        "X-Tableau-Site-Id": site_id,
        "Content-Type": "application/json"
    }
    r = requests.post(url, json={"query": query}, headers=headers, verify=VERIFY_SSL)
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"Metadata API מחזיר שגיאה: {data['errors']}")
    print(f"✅ Metadata API פעיל – נמצאו {len(data['data']['workbooks'])} דוחות (או פחות לפי הרשאותיך)")

# ===== פונקציות עיקריות =====
def get_sites(token):
    url = f"{SERVER}/api/{API_VERSION}/sites?pageSize=1000"
    headers = {"X-Tableau-Auth": token}
    r = requests.get(url, headers=headers, verify=VERIFY_SSL)
    r.raise_for_status()
    return r.json()["sites"]["site"]

def graphql_query(token, site_id, query):
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
                    sql_text = ds.get("query", None)
                    if ds.get("upstreamTables"):
                        for tbl in ds["upstreamTables"]:
                            rows.append({
                                "Site": site_name,
                                "Workbook": wb_name,
                                "DataSource": ds_name,
                                "Table": tbl["name"],
                                "Schema": tbl.get("schema", ""),
                                "ConnectionType": tbl.get("connectionType", ""),

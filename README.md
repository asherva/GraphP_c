import requests
import json

# ===== הגדרות =====
SERVER_URL = "https://my-tableau-server.com"
USERNAME = "my_user"
PASSWORD = "my_password"
SITE_NAME = ""  # ריק אם זה Default
WORKBOOK_NAME = "שם הדוח שלך"
API_VERSION = "3.18"

# ===== התחברות =====
signin_url = f"{SERVER_URL}/api/{API_VERSION}/auth/signin"
payload = {
    "credentials": {
        "name": USERNAME,
        "password": PASSWORD,
        "site": {
            "contentUrl": SITE_NAME
        }
    }
}
headers = {"Content-Type": "application/json"}

res = requests.post(signin_url, json=payload, headers=headers, verify=False)
if res.status_code != 200:
    raise Exception(f"❌ שגיאת התחברות: {res.text}")

signin_data = res.json()
TOKEN = signin_data["credentials"]["token"]
SITE_ID = signin_data["credentials"]["site"]["id"]

print(f"✅ התחברת בהצלחה ל-site {SITE_ID}")

# ===== שאילתת GraphQL =====
graphql_url = f"{SERVER_URL}/api/{API_VERSION}/sites/{SITE_ID}/metadata/graphql"

query = f"""
{{
  workbooks(filter: {{ name: "{WORKBOOK_NAME}" }}) {{
    name
    embeddedDatasources {{
      name
      upstreamTables {{
        name
        schema
        connectionType
        fullName
        database {{
          name
          connectionType
          hostName
        }}
      }}
    }}
  }}
}}
"""

graphql_headers = {
    "Content-Type": "application/json",
    "X-Tableau-Auth": TOKEN
}

graphql_payload = {"query": query}

res = requests.post(graphql_url, headers=graphql_headers, json=graphql_payload, verify=False)
if res.status_code != 200:
    raise Exception(f"❌ שגיאה ב-GraphQL: {res.text}")

data = res.json()

# ===== הצגת המידע =====
if "data" in data and "workbooks" in data["data"] and data["data"]["workbooks"]:
    wb = data["data"]["workbooks"][0]
    print(f"\n📄 דוח: {wb['name']}")
    for ds in wb["embeddedDatasources"]:
        print(f"  📊 Data Source: {ds['name']}")
        for tbl in ds["upstreamTables"]:
            db = tbl.get("database", {})
            print(f"    🗂 Table: {tbl['fullName']} (Schema: {tbl['schema']})")
            print(f"       🔹 DB: {db.get('name')} | Host: {db.get('hostName')} | Type: {db.get('connectionType')}")
else:
    print("⚠️ לא נמצא דוח בשם הזה.")

# ===== התנתקות =====
signout_url = f"{SERVER_URL}/api/{API_VERSION}/auth/signout"
requests.post(signout_url, headers={"X-Tableau-Auth": TOKEN}, verify=False)
print("\n🚪 נותקת מהשרת.")

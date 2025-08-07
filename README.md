import requests
import urllib3
import json
import sys

# 🔒 עקיפת אזהרות SSL (לסביבות בדיקה בלבד)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== הגדרות ==========
SERVER = "https://your-tableau-server"  # כולל https
VERSION = "2023.1"                      # בדוק ב-/api/versions
SITE = ""                               # השאר ריק אם default site

USE_TOKEN = True                        # True = שימוש ב-PAT, False = משתמש/סיסמה

# התחברות עם טוקן
TOKEN_NAME = "your-token-name"
TOKEN_SECRET = "your-token-secret"

# התחברות עם שם משתמש/סיסמה (אם לא משתמש ב-TOKEN)
USERNAME = "your-username"
PASSWORD = "your-password"

# ========== התחברות ==========
def signin():
    print("🔐 מתחבר לשרת Tableau...")
    url = f"{SERVER}/api/{VERSION}/auth/signin"

    if USE_TOKEN:
        credentials = {
            "personalAccessTokenName": TOKEN_NAME,
            "personalAccessTokenSecret": TOKEN_SECRET,
            "site": {"contentUrl": SITE}
        }
    else:
        credentials = {
            "name": USERNAME,
            "password": PASSWORD,
            "site": {"contentUrl": SITE}
        }

    headers = {"Content-Type": "application/json"}

    try:
        resp = requests.post(url, json={"credentials": credentials}, headers=headers, verify=False)
    except requests.exceptions.RequestException as e:
        sys.exit(f"❌ שגיאת רשת: {e}")

    if resp.status_code != 200:
        sys.exit(f"❌ התחברות נכשלה (HTTP {resp.status_code}):\n{resp.text}")

    try:
        data = resp.json()
        token = data["credentials"]["token"]
        site_id = data["credentials"]["site"]["id"]
        print("✅ התחברות הצליחה")
        print(f"📎 site_id: {site_id}")
        return token, site_id
    except Exception as e:
        sys.exit(f"❌ שגיאה בפענוח תגובה:\n{e}")

# ========== שאילתת GraphQL ==========
def run_graphql(token, site_id):
    print("\n📡 מריץ שאילתת GraphQL...")
    url = f"{SERVER}/api/{VERSION}/sites/{site_id}/graphql"
    headers = {
        "X-Tableau-Auth": token,
        "Content-Type": "application/json"
    }
    query = {
        "query": """
        {
          workbooks {
            name
            dataSources {
              name
              fields {
                name
                dataType
                isCalculated
                formula
              }
              upstreamTables {
                name
                schema
                fullName
                connectionType
              }
            }
          }
        }
        """
    }

    try:
        resp = requests.post(url, headers=headers, json=query, verify=False)
    except requests.exceptions.RequestException as e:
        sys.exit(f"❌ שגיאת רשת בביצוע GraphQL:\n{e}")

    if resp.status_code != 200:
        sys.exit(f"❌ GraphQL נכשל (HTTP {resp.status_code}):\n{resp.text}")

    try:
        return resp.json()
    except Exception as e:
        sys.exit(f"❌ שגיאה בפענוח תגובת GraphQL:\n{e}")

# ========== הדפסת תוצאה ==========
def print_workbooks(data):
    workbooks = data.get("data", {}).get("workbooks", [])
    print(f"\n🔎 נמצאו {len(workbooks)} דוחות:\n")

    for wb in workbooks:
        print(f"📘 דוח: {wb['name']}")
        for ds in wb.get("dataSources", []):
            print(f"  🔗 מקור מידע: {ds['name']}")
            for f in ds.get("fields", []):
                if f["isCalculated"]:
                    print(f"    🧠 חישוב: {f['name']} = {f['formula']}")
                else:
                    print(f"    📄 שדה: {f['name']} ({f['dataType']})")
            for t in ds.get("upstreamTables", []):
                print(f"    🗂️ טבלה: {t['fullName']} (schema: {t['schema']})")
        print("")

# ========== הרצת התוכנית ==========
def main():
    token, site_id = signin()
    result = run_graphql(token, site_id)
    print_workbooks(result)

if __name__ == "__main__":
    main()

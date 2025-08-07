import requests
import json
import urllib3

# עקיפת בדיקות SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== הגדרות ==========
TABLEAU_SERVER = "https://your-server-address"  # כולל https
API_VERSION = "2023.1"  # עדכן לגרסה שלך
SITE = ""  # השאר ריק אם זה Default site

# בחר אחת מהאפשרויות:
USE_TOKEN = False

# אם משתמש בסיסמה:
USERNAME = "your_username"
PASSWORD = "your_password"

# אם משתמש בטוקן:
TOKEN_NAME = "your_token_name"
TOKEN_SECRET = "your_token_secret"

# ========== התחברות ==========
def signin():
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/auth/signin"

    if USE_TOKEN:
        payload = {
            "credentials": {
                "personalAccessTokenName": TOKEN_NAME,
                "personalAccessTokenSecret": TOKEN_SECRET,
                "site": {"contentUrl": SITE}
            }
        }
    else:
        payload = {
            "credentials": {
                "name": USERNAME,
                "password": PASSWORD,
                "site": {"contentUrl": SITE}
            }
        }

    headers = {"Content-Type": "application/json"}

    resp = requests.post(url, json=payload, headers=headers, verify=False)
    if resp.status_code != 200:
        raise Exception(f"❌ התחברות נכשלה: {resp.status_code}\n{resp.text}")

    data = resp.json()
    token = data["credentials"]["token"]
    site_id = data["credentials"]["site"]["id"]
    return token, site_id

# ========== שאילתה ==========
def fetch_workbooks(token, site_id):
    graphql_url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/graphql"
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
                fullName
                schema
                connectionType
              }
            }
          }
        }
        """
    }

    resp = requests.post(graphql_url, headers=headers, json=query, verify=False)
    if resp.status_code != 200:
        raise Exception(f"❌ GraphQL נכשלה: {resp.status_code}\n{resp.text}")

    return resp.json()

# ========== הרצה ==========
def main():
    try:
        print("🔐 מנסה להתחבר לשרת Tableau...")
        token, site_id = signin()
        print("✅ התחברות הצליחה")

        print("📡 שולח שאילתת GraphQL...")
        result = fetch_workbooks(token, site_id)

        workbooks = result["data"]["workbooks"]
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

    except Exception as e:
        print(f"\n[שגיאה] {e}")

if __name__ == "__main__":
    main()

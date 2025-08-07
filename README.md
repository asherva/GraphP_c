import requests
import json
from urllib3.exceptions import InsecureRequestWarning
import urllib3

# עקיפת אזהרות SSL
urllib3.disable_warnings(InsecureRequestWarning)

# --------------------
# הגדרות התחברות
# --------------------
TABLEAU_SERVER = "https://your-tableau-server"
API_VERSION = "2023.1"  # גרסה מתאימה לשרת שלך
USERNAME = "your_username"
PASSWORD = "your_password"
SITE = ""  # השאר ריק אם מדובר ב-Default site

# --------------------
# התחברות לטאבלו וקבלת טוקן
# --------------------
def signin():
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/auth/signin"

    payload = {
        "credentials": {
            "name": USERNAME,
            "password": PASSWORD,
            "site": {
                "contentUrl": SITE
            }
        }
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(f"Login failed: {response.status_code} {response.text}")

    data = response.json()
    token = data['credentials']['token']
    site_id = data['credentials']['site']['id']

    return token, site_id

# --------------------
# הרצת שאילתת GraphQL
# --------------------
def run_graphql_query(token, site_id, query):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/graphql"
    headers = {
        "Content-Type": "application/json",
        "X-Tableau-Auth": token
    }

    response = requests.post(url, headers=headers, json={"query": query}, verify=False)
    if response.status_code != 200:
        raise Exception(f"GraphQL query failed: {response.status_code} {response.text}")

    return response.json()

# --------------------
# שאילתת GraphQL: דוחות + מקורות + טבלאות
# --------------------
GRAPHQL_QUERY = """
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

# --------------------
# הרצת התוכנית
# --------------------
def main():
    try:
        token, site_id = signin()
        print("[✓] התחברת בהצלחה לשרת Tableau")

        result = run_graphql_query(token, site_id, GRAPHQL_QUERY)
        workbooks = result["data"]["workbooks"]

        for workbook in workbooks:
            print(f"\n📘 דוח: {workbook['name']}")
            for ds in workbook.get("dataSources", []):
                print(f"  🔗 מקור מידע: {ds['name']}")
                for field in ds.get("fields", []):
                    if field['isCalculated']:
                        print(f"    🧠 שדה מחושב: {field['name']} = {field['formula']}")
                    else:
                        print(f"    📄 שדה רגיל: {field['name']} ({field['dataType']})")
                for table in ds.get("upstreamTables", []):
                    print(f"    🗂️ טבלה: {table['fullName']} (schema: {table['schema']}, type: {table['connectionType']})")

    except Exception as e:
        print(f"[✗] שגיאה: {e}")

if __name__ == "__main__":
    main()

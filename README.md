import requests
import json
import urllib3

# ביטול אזהרות SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# הגדרות התחברות
TABLEAU_SERVER = "https://your-server-url"
API_VERSION = "3.19"  # שנה לפי הגרסה שלך
SITE_CONTENT_URL = ""  # ריק אם זו ברירת מחדל
TOKEN_NAME = "your-token-name"
TOKEN_SECRET = "your-token-secret"

# GraphQL שאילתה לדוגמה – קבל רשימת דשבורדים
GRAPHQL_QUERY = {
    "query": """
    {
      workbooks {
        name
        id
        owner {
          username
        }
      }
    }
    """
}

def sign_in():
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/auth/signin"
    headers = {"Content-Type": "application/json"}
    payload = {
        "credentials": {
            "personalAccessTokenName": TOKEN_NAME,
            "personalAccessTokenSecret": TOKEN_SECRET,
            "site": {"contentUrl": SITE_CONTENT_URL}
        }
    }

    print(f"🔐 Signing in to: {url}")
    response = requests.post(url, json=payload, headers=headers, verify=False)

    try:
        data = response.json()
    except ValueError:
        print("❌ שגיאה: תגובת JSON לא תקינה:")
        print(response.text)
        return None, None

    if response.status_code != 200:
        print(f"❌ התחברות נכשלה: {response.status_code}")
        print(json.dumps(data, indent=2))
        return None, None

    token = data["credentials"]["token"]
    site_id = data["credentials"]["site"]["id"]
    print("✅ התחברות הצליחה")
    return token, site_id

def run_graphql_query(token, site_id):
    url = f"{TABLEAU_SERVER}/api/{API_VERSION}/sites/{site_id}/graphql"
    headers = {
        "X-Tableau-Auth": token,
        "Content-Type": "application/json"
    }

    print(f"📡 מבצע שאילתת GraphQL ל: {url}")
    response = requests.post(url, json=GRAPHQL_QUERY, headers=headers, verify=False)

    try:
        data = response.json()
    except ValueError:
        print("❌ שגיאת JSON ב-GraphQL:")
        print(response.text)
        return

    if response.status_code != 200:
        print(f"❌ GraphQL נכשל: קוד {response.status_code}")
        print(json.dumps(data, indent=2))
        return

    print("✅ תוצאה:")
    print(json.dumps(data, indent=2))

def main():
    token, site_id = sign_in()
    if token and site_id:
        run_graphql_query(token, site_id)

if __name__ == "__main__":
    main()

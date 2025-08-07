import requests

url = "https://your-server/api/3.19/auth/signin"  # דוגמה ל-API
payload = {...}
headers = {'Content-Type': 'application/json'}

response = requests.post(url, json=payload, headers=headers, verify=False)

print("📡 Status Code:", response.status_code)

try:
    data = response.json()
    print("✅ JSON Data:", data)
except ValueError as e:
    print("❌ JSON Decode Error:", e)
    print("📄 Response Text (might be HTML or plain text):")
    print(response.text)

graphql_query = {
    "query": "{ workbooks { name } }"
}
resp = requests.post(
    f"https://your-server/api/2023.1/sites/{site_id}/graphql",
    headers={"X-Tableau-Auth": token, "Content-Type": "application/json"},
    json=graphql_query,
    verify=False
)

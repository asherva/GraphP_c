{
  "mssql": {
    "server": "YOUR_SERVER\\INSTANCE",
    "database": "SalesDB",
    "username": "db_user",
    "password": "db_password",
    "trusted_connection": false,
    "driver": "ODBC Driver 18 for SQL Server"
  },
  "openai": {
    "use_openai": true,
    "api_key_env": "OPENAI_API_KEY",
    "model": "gpt-4o-mini",
    "embedding_model": "text-embedding-3-small"
  },
  "vectorstore": {
    "index_path": "faiss_index"
  }
}

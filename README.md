import os
import pandas as pd
import numpy as np
import pyodbc
from sentence_transformers import SentenceTransformer
import faiss

# ===== הגדרות חיבור ל-MSSQL =====
MSSQL_SERVER = "SERVER_NAME"     # לדוגמה: "localhost\\SQLEXPRESS"
MSSQL_DATABASE = "DB_NAME"
MSSQL_USERNAME = "USER"
MSSQL_PASSWORD = "PASSWORD"
MSSQL_TABLE    = "Sales"         # שם הטבלה

# ===== חיבור ל-MSSQL וטעינת הנתונים =====
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD}"
)

with pyodbc.connect(conn_str) as conn:
    query = f"SELECT * FROM {MSSQL_TABLE}"
    df = pd.read_sql(query, conn)

df.columns = [c.strip().replace(" ", "_") for c in df.columns]
print(f"נטענו {len(df)} שורות ו-{len(df.columns)} עמודות מטבלה {MSSQL_TABLE}.")

# ===== המרת שורות לטקסטים =====
def row_to_text(row):
    return "; ".join(f"{col}: {row[col]}" for col in df.columns)

docs = [row_to_text(r) for _, r in df.iterrows()]

# ===== יצירת Embeddings =====
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"
model = SentenceTransformer(EMBED_MODEL_NAME)
embeddings = model.encode(docs, convert_to_numpy=True)

# ===== בניית אינדקס FAISS =====
dim = embeddings.shape[1]
index = faiss.IndexFlatL2(dim)
index.add(embeddings)
print(f"אינדקס נבנה עם {index.ntotal} פריטים.")

# ===== פונקציית חיפוש =====
def search(query, top_k=5):
    q_emb = model.encode([query], convert_to_numpy=True)
    D, I = index.search(q_emb, top_k)
    results = []
    for dist, idx in zip(D[0], I[0]):
        results.append({
            "score": float(dist),
            "row": df.iloc[idx].to_dict()
        })
    return results

# ===== ניסוי חיפוש =====
while True:
    q = input("\nשאלה או חיפוש ('quit' ליציאה): ").strip()
    if q.lower() in ("quit", "exit"):
        break
    hits = search(q, top_k=3)
    for h in hits:
        print(f"\nציון: {h['score']:.4f}")
        for k, v in h["row"].items():
            print(f"{k}: {v}")

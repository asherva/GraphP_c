import os
import pandas as pd
import numpy as np
import pyodbc
from sentence_transformers import SentenceTransformer
import faiss
from transformers import pipeline

# ===== הגדרות חיבור ל-MSSQL =====
MSSQL_SERVER = "SERVER_NAME"
MSSQL_DATABASE = "DB_NAME"
MSSQL_USERNAME = "USER"
MSSQL_PASSWORD = "PASSWORD"
MSSQL_TABLE    = "Sales"

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
print(f"נטענו {len(df)} שורות ו-{len(df.columns)} עמודות.")

# ===== המרת שורות לטקסטים =====
def row_to_text(row):
    return "; ".join(f"{col}: {row[col]}" for col in df.columns)

docs = [row_to_text(r) for _, r in df.iterrows()]

# ===== יצירת Embeddings =====
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"
embedder = SentenceTransformer(EMBED_MODEL_NAME)
embeddings = embedder.encode(docs, convert_to_numpy=True)

# ===== בניית אינדקס FAISS =====
dim = embeddings.shape[1]
index = faiss.IndexFlatL2(dim)
index.add(embeddings)
print(f"אינדקס FAISS נבנה עם {index.ntotal} פריטים.")

# ===== טעינת מודל שפה מקומי =====
LOCAL_LLM_MODEL = "mistralai/Mistral-7B-Instruct-v0.1"  # אפשר להחליף במודל אחר שהורדת
llm = pipeline("text-generation", model=LOCAL_LLM_MODEL, device_map="auto")

# ===== פונקציית חיפוש =====
def search(query, top_k=5):
    q_emb = embedder.encode([query], convert_to_numpy=True)
    D, I = index.search(q_emb, top_k)
    results = []
    for dist, idx in zip(D[0], I[0]):
        results.append({
            "score": float(dist),
            "text": docs[idx],
            "row": df.iloc[idx].to_dict()
        })
    return results

# ===== פונקציה לעניית שאלות (RAG) =====
def answer_question(query, top_k=5):
    results = search(query, top_k)
    context = "\n".join([r["text"] for r in results])
    prompt = (
        "הנך עוזר חכם למכירות. "
        "הנה נתוני ההקשר:\n"
        f"{context}\n"
        f"שאלה: {query}\n"
        "ענה בצורה מפורטת ומבוססת על הנתונים בלבד."
    )
    resp = llm(prompt, max_length=512, do_sample=False)
    return resp[0]["generated_text"]

# ===== ממשק CLI =====
while True:
    q = input("\nשאלה ('quit' ליציאה): ").strip()
    if q.lower() in ("quit", "exit"):
        break
    answer = answer_question(q, top_k=5)
    print("\n--- תשובה ---")
    print(answer)
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

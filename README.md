import pyodbc
import pandas as pd
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import subprocess

# ===== הגדרות חיבור ל-MSSQL =====
server = "SERVER_NAME"
database = "DB_NAME"
username = "USER"
password = "PASS"
table_name = "Sales"  # שם טבלת המכירות

# ===== חיבור למסד הנתונים וקריאת טבלה =====
def load_sales_table():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    conn = pyodbc.connect(conn_str)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ===== בניית אינדקס FAISS =====
def build_faiss_index(df):
    model = SentenceTransformer("all-MiniLM-L6-v2")  # מודל embedding מקומי
    texts = df.astype(str).agg(" ".join, axis=1).tolist()
    embeddings = model.encode(texts, convert_to_numpy=True)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)
    return index, texts, model

# ===== חיפוש בטבלה =====
def search(query, index, texts, model, top_k=5):
    query_emb = model.encode([query], convert_to_numpy=True)
    distances, indices = index.search(query_emb, top_k)
    return [texts[i] for i in indices[0]]

# ===== שליחת שאלה ל-Ollama =====
def ollama_generate(prompt, model="mistral"):
    cmd = ["ollama", "run", model]
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = process.communicate(input=prompt)
    return out.strip()

# ===== פונקציה לשאלות =====
def answer_question(query, index, texts, model):
    results = search(query, index, texts, model)
    context = "\n".join(results)
    prompt = (
        "הנך עוזר חכם לניתוח נתוני מכירות. "
        "השתמש אך ורק בנתונים הבאים כדי לענות:\n"
        f"{context}\n\n"
        f"שאלה: {query}\n"
        "תשובה:"
    )
    return ollama_generate(prompt, model="mistral")  # אפשר להחליף ל-llama3

# ===== הרצה =====
if __name__ == "__main__":
    print("📥 טוען את טבלת המכירות...")
    df = load_sales_table()
    index, texts, emb_model = build_faiss_index(df)
    print("✅ המערכת מוכנה. אפשר לשאול שאלות!")

    while True:
        q = input("\nהכנס שאלה (או 'exit' ליציאה): ")
        if q.lower() == "exit":
            break
        answer = answer_question(q, index, texts, emb_model)
        print("\n💡 תשובה:", answer)print(f"נטענו {len(df)} שורות ו-{len(df.columns)} עמודות.")

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

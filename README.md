""" Sales RAG (MSSQL + Ollama Embeddings + Ollama LLM) — 100% פנימי

תלויות (התקנה חד-פעמית): pip install pandas pyodbc faiss-cpu requests

נדרש להתקין ולהריץ Ollama מקומית, ולהוריד את המודלים: ollama pull nomic-embed-text   # מודל Embedding ollama pull llama3.1           # או mistral / gemma / llama3

ברירת מחדל: שימוש ב-REST API המקומי של Ollama (http://localhost:11434) אין יציאה לאינטרנט מעבר לזה. """

import os import json import time import struct from typing import List, Dict, Any, Tuple

import pandas as pd import numpy as np import pyodbc import faiss import requests

======================== הגדרות ========================

חיבור למסד הנתונים (עדכן לערכים שלך)

MSSQL_SERVER   = os.getenv("MSSQL_SERVER", "localhost\SQLEXPRESS") MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "SalesDB") MSSQL_USERNAME = os.getenv("MSSQL_USERNAME", "sa") MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "YourStrong!Passw0rd") MSSQL_TABLE    = os.getenv("MSSQL_TABLE", "Sales")

Ollama

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434") EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")  # ממד וקטור אופייני: 768/1024 (תלוי מודל) LLM_MODEL   = os.getenv("LLM_MODEL", "llama3.1")            # או "mistral", "llama3", "gemma" וכו'

קבצי מטמון/אינדקס (לא חובה)

INDEX_DIR   = os.getenv("INDEX_DIR", "./rag_index") INDEX_FILE  = os.path.join(INDEX_DIR, "faiss.index") META_FILE   = os.path.join(INDEX_DIR, "meta.parquet") DIM_FILE    = os.path.join(INDEX_DIR, "dim.txt")

os.makedirs(INDEX_DIR, exist_ok=True)

======================== עזרי Ollama ========================

def ollama_embeddings(texts: List[str], model: str = EMBED_MODEL, timeout: int = 120) -> np.ndarray: """מבקש embedding עבור רשימת טקסטים מהשרת המקומי של Ollama. משתמש ב-POST /api/embeddings. מחזיר np.ndarray בצורה (N, D). """ vectors = [] url = f"{OLLAMA_HOST}/api/embeddings" for t in texts: payload = {"model": model, "prompt": t} r = requests.post(url, json=payload, timeout=timeout) r.raise_for_status() data = r.json() vec = np.array(data.get("embedding", []), dtype=np.float32) if vec.size == 0: raise RuntimeError("Ollama החזיר embedding ריק. ודא שהמודל תומך ב-embeddings.") vectors.append(vec) return np.vstack(vectors)

def ollama_generate(prompt: str, model: str = LLM_MODEL, temperature: float = 0.1, timeout: int = 300) -> str: """יוצר תשובה מה-LLM המקומי ב-Ollama דרך /api/generate (non-stream).""" url = f"{OLLAMA_HOST}/api/generate" payload = { "model": model, "prompt": prompt, "options": { "temperature": temperature, }, "stream": False } r = requests.post(url, json=payload, timeout=timeout) r.raise_for_status() data = r.json() # הפורמט מחזיר 'response' עם הטקסט המלא כאשר stream=False return data.get("response", "").strip()

======================== MSSQL ========================

def load_sales_table() -> pd.DataFrame: conn_str = ( f"DRIVER={{ODBC Driver 17 for SQL Server}};" f"SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}" ) with pyodbc.connect(conn_str) as conn: df = pd.read_sql(f"SELECT * FROM {MSSQL_TABLE}", conn) # ניקוי בסיסי לשמות עמודות df.columns = [c.strip().replace(" ", "_") for c in df.columns] return df

======================== המרה לטקסטים ========================

def row_to_text(row: pd.Series) -> str: # ניתן להתאים: לבחור עמודות מרכזיות בלבד, או לתרגם שמות לעברית return "; ".join(f"{col}: {row[col]}" for col in row.index)

def df_to_texts(df: pd.DataFrame) -> List[str]: return [row_to_text(r) for _, r in df.iterrows()]

======================== FAISS ========================

def build_faiss(embeddings: np.ndarray) -> faiss.IndexFlatIP: # נשתמש ב-Inner Product עם vectors מנורמלים (יעיל לקוסיין) dim = embeddings.shape[1] index = faiss.IndexFlatIP(dim) return index

def normalize_rows(x: np.ndarray) -> np.ndarray: norms = np.linalg.norm(x, axis=1, keepdims=True) + 1e-12 return x / norms

def save_index(index: faiss.Index, df_meta: pd.DataFrame, dim: int): faiss.write_index(index, INDEX_FILE) df_meta.to_parquet(META_FILE, index=False) with open(DIM_FILE, "w", encoding="utf-8") as f: f.write(str(dim))

def load_index() -> Tuple[faiss.Index, pd.DataFrame, int]: index = faiss.read_index(INDEX_FILE) df_meta = pd.read_parquet(META_FILE) with open(DIM_FILE, "r", encoding="utf-8") as f: dim = int(f.read().strip()) return index, df_meta, dim

======================== RAG ========================

def ensure_index(df: pd.DataFrame, force_rebuild: bool = False) -> Tuple[faiss.Index, pd.DataFrame, int]: """בונה או טוען אינדקס: ממיר את השורות לטקסטים, מפיק embeddings ב-Ollama, ומאחסן ב-FAISS. מחזיר (index, df_meta, dim). """ if (not force_rebuild) and os.path.exists(INDEX_FILE) and os.path.exists(META_FILE) and os.path.exists(DIM_FILE): try: idx, meta, dim = load_index() return idx, meta, dim except Exception: pass

texts = df_to_texts(df)
print(f"יוצר embeddings ל-{len(texts)} שורות... (יכול לקחת זמן בפעם הראשונה)")
embs = ollama_embeddings(texts, EMBED_MODEL)  # צורה (N, D)
embs = normalize_rows(embs).astype(np.float32)

index = build_faiss(embs)
index.add(embs)

# meta נשמור: אינדקס-שורה מקורי + טקסט המקור (לא חובה לשמור הכל)
meta = pd.DataFrame({
    "row_id": np.arange(len(df), dtype=np.int32),
    "text": texts
})

save_index(index, meta, embs.shape[1])
return index, meta, embs.shape[1]

def search_similar(query: str, index: faiss.Index, df_meta: pd.DataFrame, top_k: int = 5) -> List[Dict[str, Any]]: q_emb = ollama_embeddings([query], EMBED_MODEL)  # (1, D) q_emb = normalize_rows(q_emb).astype(np.float32) D, I = index.search(q_emb, top_k)  # D: similarities, I: indices results = [] for score, idx in zip(D[0], I[0]): if idx == -1: continue results.append({ "score": float(score), "row_id": int(df_meta.loc[idx, "row_id"]), "text": df_meta.loc[idx, "text"], }) return results

def build_prompt(query: str, hits: List[Dict[str, Any]]) -> str: ctx = "\n".join(f"[DOC {i+1}] {h['text']}" for i, h in enumerate(hits)) prompt = ( "את/ה עוזר/ת ניתוח מכירות פנימי/ת. \n" "ענה/י בעברית, מדויק, תמציתי כשאפשר, והצג/י חישובים אם רלוונטי. \n" "הסתמך/י אך ורק על ההקשר הבא: \n" f"{ctx}\n\n" f"שאלה: {query}\n" "תשובה:" ) return prompt

def answer_question(query: str, index: faiss.Index, df_meta: pd.DataFrame, top_k: int = 6) -> Dict[str, Any]: hits = search_similar(query, index, df_meta, top_k=top_k) prompt = build_prompt(query, hits) answer = ollama_generate(prompt, model=LLM_MODEL) return {"answer": answer, "evidence": hits}

======================== CLI ========================

def main(): print("📥 טוען טבלה מ-MSSQL...") df = load_sales_table() print(f"✅ נטענו {len(df)} שורות, {len(df.columns)} עמודות.")

print("🏗️ בונה/טוען אינדקס FAISS מקומי...")
index, df_meta, dim = ensure_index(df, force_rebuild=False)
print(f"✅ אינדקס מוכן. dim={dim}, items={index.ntotal}")

print("\nאפשר לשאול שאלות (כתוב 'exit' ליציאה):")
while True:
    q = input("\nשאלה: ").strip()
    if q.lower() in ("exit", "quit"): break
    out = answer_question(q, index, df_meta, top_k=6)
    print("\n— תשובה —\n" + out["answer"]) 
    print("\n(מסמכי הקשר):")
    for i, h in enumerate(out["evidence"], 1):
        print(f" {i}. score={h['score']:.3f} | {h['text'][:160]}...")

if name == "main": main()

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

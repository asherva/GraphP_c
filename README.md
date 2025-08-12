# app_internal.py
import streamlit as st
import pyodbc
import pandas as pd
from sentence_transformers import SentenceTransformer
import faiss
from langchain.docstore.document import Document
from langchain_community.vectorstores import FAISS
import numpy as np
from gpt4all import GPT4All  # או החלף ל-Ollama אם תרצה

# --- טעינת נתונים מ-MSSQL ---
def load_sales_table():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=YOUR_SERVER\\INSTANCE;"
        "DATABASE=SalesDB;"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    query = "SELECT TOP 10000 * FROM SalesTable"  # החלף לשם הטבלה שלך
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# --- יצירת Vector Store ---
def df_to_docs(df):
    docs = []
    for _, row in df.iterrows():
        text = " | ".join([f"{col}: {row[col]}" for col in df.columns])
        docs.append(Document(page_content=text))
    return docs

def build_faiss_index(docs, model):
    texts = [d.page_content for d in docs]
    embeddings = model.encode(texts, show_progress_bar=True)
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(np.array(embeddings).astype("float32"))
    return index

# --- שאילת LLM מקומי (GPT4All) ---
def query_gpt4all(llm, question, context):
    prompt = f"""בהתבסס על המידע הבא:
{context}

ענה על השאלה:
{question}
"""
    response = llm.prompt(prompt)
    return response

# --- הממשק ---
st.title("💼 צ'אט עסקי פנימי - MSSQL + LLM מקומי")

@st.cache_data(ttl=3600)
def prepare_data():
    df = load_sales_table()
    model = SentenceTransformer("all-MiniLM-L6-v2")
    docs = df_to_docs(df)
    index = build_faiss_index(docs, model)
    return df, docs, model, index

df, docs, model, index = prepare_data()
st.write(f"טעונים {len(df)} שורות נתונים.")

llm = GPT4All("mistral-7b-instruct.Q4_0.bin")  # ודא שהמודל קיים במחשב שלך

question = st.text_input("כתוב שאלה עסקית:")

if question:
    q_emb = model.encode([question]).astype("float32")
    D, I = index.search(q_emb, 3)  # קח 3 תוצאות רלוונטיות
    context = "\n\n".join([docs[i].page_content for i in I[0]])
    with st.spinner("מחשב תשובה..."):
        answer = query_gpt4all(llm, question, context)
    st.markdown("### תשובה:")
    st.write(answer)            return self.emb.embed_documents(texts)
        else:
            arr = self.st.encode(texts, show_progress_bar=False)
            return [list(x) for x in arr]

def build_vectorstore_from_df(df: pd.DataFrame, cfg_path="config.json", text_cols=None, recreate=False):
    cfg = load_config(cfg_path)
    vs_cfg = cfg.get("vectorstore", {})
    index_path = vs_cfg.get("index_path", "faiss_index")

    # בחר עמודות אם לא סופקו
    if text_cols is None:
        # ברירת מחדל: כל העמודות טקסטואליות (אתה יכול לשנות)
        text_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
        if not text_cols:
            # אם אין עמודות טקסט — המיר שורות לייצוג
            text_cols = df.columns.tolist()

    docs = df_to_documents(df, text_cols)
    texts = [d.page_content for d in docs]

    embedder = Embedder(cfg)

    if recreate or not os.path.exists(index_path + ".pkl"):
        # צור embeddings
        vectors = embedder.embed_texts(texts)
        # build FAISS
        store = FAISS.from_documents(docs, embedder.emb if embedder.use_openai else None) if embedder.use_openai else None
        # אם אין תמיכה ישירה ב־from_documents עם sentence-transformers, נבנה ידני:
        if store is None:
            import faiss
            dim = len(vectors[0])
            xb = np.array(vectors).astype("float32")
            index = faiss.IndexFlatL2(dim)
            index.add(xb)
            store = FAISS(embedding_function=None, index=index, docs=docs)  # embedding_function None לשימוש פנימי
            # שים לב: הדרך הזו שומרת אינדקס אך לא בונה חיבור חכם כמו ספריות חדשות יותר.
        store.save_local(index_path)
    else:
        store = FAISS.load_local(index_path, embedder.emb if embedder.use_openai else None)

    return store

def create_qa_chain(vectorstore, cfg_path="config.json"):
    cfg = load_config(cfg_path)
    if cfg["openai"].get("use_openai", True):
        model_name = cfg["openai"].get("model", "gpt-4o-mini")
        llm = ChatOpenAI(model=model_name, temperature=0)
    else:
        raise ValueError("Local LLM not implemented in this example. Set use_openai=true in config.")
    retriever = vectorstore.as_retriever(search_type="similarity", search_kwargs={"k": 4})
    qa = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever, return_source_documents=True)
    return qa

def ask_question(qa_chain, question: str):
    res = qa_chain({"query": question})
    answer = res["result"]
    sources = res.get("source_documents", [])
    return answer, sources

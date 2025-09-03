import os
import time
import pandas as pd
import streamlit as st
from datetime import datetime
from threading import Thread
from sqlalchemy import create_engine, text
from langchain_community.llms import Ollama
from langchain.chains import ConversationalRetrievalChain
from langchain_community.vectorstores import FAISS
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.memory import ConversationBufferMemory

# -----------------------------
# הגדרות
# -----------------------------
DATA_PATH = "insurance_sales.csv"
USE_SQL = False   # שנה ל-True אם יש לך SQL DB
SQL_CONN = "sqlite:///insurance.db"  # חיבור לדוגמה
OLLAMA_MODEL = "llama3"
UPDATE_INTERVAL = 60  # שניות

# -----------------------------
# טעינת נתונים
# -----------------------------
def load_data():
    if USE_SQL:
        engine = create_engine(SQL_CONN)
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM sales", conn)
    else:
        if not os.path.exists(DATA_PATH):
            df = pd.DataFrame({
                "date": pd.date_range(start="2025-01-01", periods=10),
                "client": ["חיים", "שרה", "דוד", "רונית", "אבי"]*2,
                "agent": ["אליס","בוב","צ׳ארלי","דוד","אווה"]*2,
                "policy": ["ביטוח חיים","רכב","בריאות","דירה","נסיעות"]*2,
                "premium": [1000,2000,1500,1200,2500]*2
            })
            df.to_csv(DATA_PATH,index=False)
        else:
            df = pd.read_csv(DATA_PATH, parse_dates=["date"])
    return df

df = load_data()

# -----------------------------
# עדכון דאטה כל דקה
# -----------------------------
def update_data_periodically():
    global df
    while True:
        time.sleep(UPDATE_INTERVAL)
        new_row = {
            "date": datetime.now().date(),
            "client": "חדש",
            "agent": "סוכןX",
            "policy": "ביטוח חיים",
            "premium": 1234
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(DATA_PATH,index=False)

Thread(target=update_data_periodically, daemon=True).start()

# -----------------------------
# Embeddings + Vectorstore
# -----------------------------
text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
docs = text_splitter.create_documents(df.astype(str).apply(lambda row: " | ".join(row), axis=1).tolist())

embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
vectorstore = FAISS.from_documents(docs, embeddings)

# -----------------------------
# LangChain – LLM + RAG
# -----------------------------
llm = Ollama(model=OLLAMA_MODEL)

memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)

qa_chain = ConversationalRetrievalChain.from_llm(
    llm=llm,
    retriever=vectorstore.as_retriever(search_kwargs={"k": 5}),
    memory=memory,
    return_source_documents=True
)

# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Ollama3 Insurance Chat AI", layout="wide")
st.title("🔒 Ollama3 + LangChain – Insurance Chat AI")

q = st.text_input("שאל שאלה על מכירות הביטוח:")

if q:
    try:
        result = qa_chain({"question": q})
        answer = result["answer"]
        sources = result.get("source_documents", [])
    except Exception as e:
        answer = f"שגיאה: {e}"
        sources = []

    # תשובה בעברית מימין לשמאל
    st.markdown(f"<div style='text-align:right; direction:rtl;'>{answer}</div>", unsafe_allow_html=True)

    # הצגת מקורות
    if sources:
        st.subheader("🔍 מקורות מידע")
        src_texts = [s.page_content for s in sources]
        st.write(src_texts)

    # טבלה מהדאטה
    st.subheader("📊 נתוני מכירות אחרונים")
    st.dataframe(df.tail(10))

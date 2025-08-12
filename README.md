from sentence_transformers import SentenceTransformer
from langchain.vectorstores import FAISS
from langchain.docstore.document import Document
import os

# טען מודל מקומי ל־embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")

def df_to_docs(df):
    docs = []
    for _, row in df.iterrows():
        text = " | ".join([f"{col}: {row[col]}" for col in df.columns])
        docs.append(Document(page_content=text))
    return docs

docs = df_to_docs(sales_df)
texts = [d.page_content for d in docs]

# צור embeddings
embeddings = model.encode(texts, show_progress_bar=True)

# בנה FAISS
import faiss
dim = embeddings.shape[1]
index = faiss.IndexFlatL2(dim)
index.add(embeddings)

vectorstore = FAISS(embedding_function=None, index=index, docs=docs)        parts = [f"{col}: {row[col]}" for col in text_cols if pd.notnull(row.get(col))]
        content = " | ".join(parts)
        docs.append(Document(page_content=content, metadata=row.to_dict()))
    return docs

# יצירת embeddings - אפשר לבחור OpenAI או local
class Embedder:
    def __init__(self, cfg):
        self.cfg = cfg
        if cfg["openai"].get("use_openai", True):
            os.environ[cfg["openai"]["api_key_env"]] = os.getenv(cfg["openai"]["api_key_env"], "")
            self.emb = OpenAIEmbeddings(model=cfg["openai"].get("embedding_model", "text-embedding-3-small"))
            self.use_openai = True
        else:
            self.use_openai = False
            model_name = cfg.get("sentence_transformer_model", "all-MiniLM-L6-v2")
            self.st = SentenceTransformer(model_name)

    def embed_texts(self, texts):
        if self.use_openai:
            return self.emb.embed_documents(texts)
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

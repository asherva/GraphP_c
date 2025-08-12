from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document

def prepare_vector_db(df):
    # ממירים את ה־DataFrame לרשימת טקסטים
    texts = [str(row.to_dict()) for _, row in df.iterrows()]
    docs = [Document(page_content=text) for text in texts]

    splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
    split_docs = splitter.split_documents(docs)

    embeddings = OpenAIEmbeddings()  # או מודל embeddings פנימי
    vector_db = Chroma.from_documents(split_docs, embeddings, persist_directory="./sales_db")
    vector_db.persist()
    return vector_db

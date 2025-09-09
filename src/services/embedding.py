from sentence_transformers import SentenceTransformer

model = SentenceTransformer('BAAI/bge-m3')

def generate_embeddings(text):
    try:
        return model.encode(text)
    except Exception as e:
        print(f"Error generando embedding: {e}")
        return None
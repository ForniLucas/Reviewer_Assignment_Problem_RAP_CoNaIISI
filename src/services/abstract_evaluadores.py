import pandas as pd
from scholarly import scholarly
import time
from src.services.embedding import generate_embeddings
from src.db.database import insert_evaluator_abstracts, get_evaluadores_from_db

def fetch_author_publications(author_id, max_pubs=10):
    """
    Busca un autor por su id de Google Scholar y devuelve sus n publicaciones mas citadas.
    """
    try:
        author = scholarly.fill(scholarly.search_author_id(author_id), sections=['publications'])
        

        # se ordena de mayor a menor segun 'num_citations'.
        sorted_pubs = sorted(author.get('publications', []), key=lambda p: p.get('num_citations', 0), reverse=True)
        
        publications_data = []
        

        for pub_summary in sorted_pubs[:max_pubs]:
            pub_details = scholarly.fill(pub_summary)
            abstract = pub_details.get('bib', {}).get('abstract', None)
            
            if abstract and abstract.strip():
                publications_data.append({'abstract': abstract})
            time.sleep(1) # pausa para no saturar los servidores de Google Scholar, lo recomendo gemini
            
        print(f"Se encontraron {len(publications_data)} publicaciones con abstract para {author.get('name')}.")
        return pd.DataFrame(publications_data)

    except Exception as e:
        print(f"No se pudo procesar al autor {author_id}. Error: {e}")
        return pd.DataFrame()


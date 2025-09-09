from src.db.dbsetup import create_tables
from src.db.database import get_evaluadores_from_db, insert_evaluator_abstracts
from src.processing.procesarexcel import procesar
from src.services.embedding import generate_embeddings
from src.services.abstract_evaluadores import fetch_author_publications
from src.optimizacion.calcular_costos import calculo_costos 
from src.optimizacion.asignacion import asignar


def main():
    #print("Creando tablas...")
    #create_tables()
    #
#
    #print("\nProcesando Excel...")
    #procesar(r'data\ASIGNACION.xlsx')
#
    #print("\nProcesando abstracts evaluadores...")
    #evaluadores_a_procesar = get_evaluadores_from_db()
    #
    #if not evaluadores_a_procesar:
    #   print("No se encontraron evaluadores con GS ids")
    #else:
    #    print(f"Hay {len(evaluadores_a_procesar)} evaluadores con GS ids.")
#
    #    for db_id, scholar_id in evaluadores_a_procesar:
    #        print(f"\n Evaluador ID: {db_id}, ID GS: {scholar_id} ")
    #        
    #        df_pubs = fetch_author_publications(scholar_id, max_pubs=5)
    #        
    #        if not df_pubs.empty:
    #            print(f"Generando embeddings para los articulos de los evaluadores")
    #            df_pubs['embedding'] = df_pubs['abstract'].apply(lambda x: generate_embeddings(x).tolist() if x else None) # Asegúrate de manejar None
    #            df_pubs = df_pubs.dropna(subset=['embedding']) 
    #            insert_evaluator_abstracts(db_id, df_pubs)
#
    #print("\nCalculando costo")
    if calculo_costos():

        print("\nAsignando de Revisores")
        asignar()
    else:
        print("El calculo de costos fallo.")


if __name__ == "__main__":
    main()
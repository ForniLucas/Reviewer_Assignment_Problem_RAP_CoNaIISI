import psycopg2
from dotenv import load_dotenv
import os
from src.db.dbconnection import get_db_connection
import pandas as pd
load_dotenv()



def insert_data_to_postgres(df):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS papersprueba (
                id SERIAL PRIMARY KEY,
                authors TEXT,
                title TEXT,
                abstract TEXT,
                abstract_embedding VECTOR(1024)
            );
        """)

        for _, row in df.iterrows():
            if row['abstract_embedding'] is not None:
                cur.execute(
                    "INSERT INTO papersprueba (authors, title, abstract, abstract_embedding) VALUES (%s, %s, %s, %s)",
                    (row['authors'], row['title'], row['abstract'], row['abstract_embedding'].tolist())
                )
        conn.commit()

    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        cur.close()
        conn.close()

def find_most_similar(abstract_text, embedding_func, top_n=10):
    input_embedding = embedding_func(abstract_text)
    if input_embedding is None:
        return "Error generando los embedding"
    
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT title, authors, abstract, abstract_embedding <=> CAST(%s AS vector) AS distance
            FROM papers
            ORDER BY distance
            LIMIT %s;
        """, (input_embedding.tolist(), top_n))
        
        results = cur.fetchall()
        output = []
        for title, authors, abstract, distance in results:
            similarity = 1 - distance
            result = f"""
Similitud: {similarity:.4f}
Titulo: {title}
Autores: {authors}
Abstract: {abstract}
                    {'='*50}
                    """
            output.append(result)
        return "\n".join(output)
    except Exception as e:
        return f"Error buscando en la db: {e}"
    finally:
        cur.close()
        conn.close()




# database.py
def get_evaluadores_from_db():
    """Obtiene TODOS los evaluadores de la base de datos."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        # Se elimina "WHERE google_scholar_id IS NOT NULL" y se agrega nombreyapellido
        cur.execute("SELECT id_evaluador, nombreyapellido, google_scholar_id FROM evaluadores;")
        
        # Devolver como una lista de diccionarios para fácil acceso
        evaluadores = [{'id_evaluador': row[0], 'nombreyapellido': row[1], 'google_scholar_id': row[2]} for row in cur.fetchall()]
        cur.close()
        return evaluadores
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al obtener evaluadores: {error}")
        return []
    finally:
        if conn:
            conn.close()

#def get_evaluadores_from_db():
    """Obtiene los evaluadores que tienen un ID de Google Scholar."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id_evaluador, google_scholar_id FROM evaluadores WHERE google_scholar_id IS NOT NULL;")
        evaluadores = cur.fetchall()
        return evaluadores
    except Exception as e: 
        print(f"Error al obtener evaluadores de la base de datos: {e}") 
        return []
    finally:
        cur.close()
        conn.close()

        
# database.py
def insert_evaluator_abstracts(evaluador_id, df_abstracts):
    """Inserta los abstracts y embeddings de un evaluador, borrando los anteriores."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # --- LÍNEA NUEVA ---
        # Primero, borramos los abstracts viejos de este evaluador para evitar duplicados.
        cur.execute("DELETE FROM abstract_evaluador WHERE id_evaluador = %s;", (evaluador_id,))
        # -------------------

        for _, row in df_abstracts.iterrows():
            embedding_list = row['embedding']
            if embedding_list is not None:
                cur.execute(
                    """INSERT INTO abstract_evaluador (id_evaluador, texto_abstract, embedding) 
                       VALUES (%s, %s, %s);""",
                    (evaluador_id, row['abstract'], embedding_list)
                )
        conn.commit()
        print(f"Se insertaron {len(df_abstracts)} abstracts para el evaluador {evaluador_id}.")
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar abstracts para el evaluador {evaluador_id}: {e}")
    finally:
        cur.close()
        conn.close()
        
#def insert_evaluator_abstracts(evaluador_id, df_abstracts):
    """Inserta los abstracts y embeddings de un evaluador en la base de datos."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for _, row in df_abstracts.iterrows():
            embedding_list = row['embedding']
            
            cur.execute(
                """INSERT INTO abstract_evaluador (id_evaluador, texto_abstract, embedding) 
                   VALUES (%s, %s, %s);""",
                (evaluador_id, row['abstract'], embedding_list)
            )
        conn.commit()
        print(f" Se insertaron {len(df_abstracts)} abstracts para el evaluador {evaluador_id}.")
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar abstracts para el evaluador {evaluador_id}: {e}")
    finally:
        cur.close()
        conn.close()


def calculate_all_costs_in_db():
    """
    Calcula la distancia coseno min (costo) entre cada artículo y cada evaluador
    para todos los pares posibles.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    

    sql_query = """
        SELECT
            a.id_articulo,
            abs_eval.id_evaluador,
            MIN(a.embedding <=> abs_eval.embedding) AS costo
        FROM
            articulos a
        CROSS JOIN abstract_evaluador abs_eval
        WHERE
            a.embedding IS NOT NULL AND abs_eval.embedding IS NOT NULL
        GROUP BY
            a.id_articulo,
            abs_eval.id_evaluador
        ORDER BY
            a.id_articulo,
            abs_eval.id_evaluador;
    """
    
    try:

        
        cur.execute(sql_query)
        cost_data = cur.fetchall()
        print(f"Se obtuvieron {len(cost_data)} costos.")
        return cost_data
    except Exception as e:
        print(f"Error al calcular los costos en la base de datos: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def save_costs_to_db(cost_data):
    """
    Guarda los costos calculados en la tabla costos_asignacion.
    Limpia la tabla para evitar datos de ejecuciones anteriores.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        print("Limpiando la tabla de costos anteriores.")
        cur.execute("TRUNCATE TABLE costos_asignacion RESTART IDENTITY;")
        
        print(f" metiendo {len(cost_data)} costos en la base de datos")

        cur.executemany(
            "INSERT INTO costos_asignacion (id_articulo, id_evaluador, costo) VALUES (%s, %s, %s);",
            cost_data
        )
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error al guardar los costos en la base de datos: {e}")
    finally:
        cur.close()
        conn.close()



def save_final_assignments(assignments_df):
    """
    Guarda las asignaciones en la tabla 'asignaciones'.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        print("Limpiando la tabla de asignaciones anteriores.")
        cur.execute("TRUNCATE TABLE asignaciones RESTART IDENTITY;")

        assignments_tuples = [tuple(x) for x in assignments_df.to_numpy()]
        print(f"Insertando {len(assignments_tuples)} nuevas asignaciones óptimas...")

        cur.executemany(
            "INSERT INTO asignaciones (id_articulo, id_evaluador, costo_asignacion) VALUES (%s, %s, %s);",
            assignments_tuples
        )
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error al guardar las asignaciones: {e}")
    finally:
        cur.close()
        conn.close()


def get_data_for_optimization():

    conn = get_db_connection()
    if conn is None:
        print("No se pudo establecer conexion con la db")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    try:

        cur = conn.cursor()


        costs_query = "SELECT id_articulo, id_evaluador, costo FROM costos_asignacion;"
        cur.execute(costs_query)
        costs_data = cur.fetchall()
        costs_cols = [desc[0] for desc in cur.description]
        df_costs = pd.DataFrame(costs_data, columns=costs_cols)



        evaluadores_query = "SELECT id_evaluador, provincia, carga_maxima FROM evaluadores;"
        cur.execute(evaluadores_query)
        evaluadores_data = cur.fetchall()
        evaluadores_cols = [desc[0] for desc in cur.description]
        df_evaluadores = pd.DataFrame(evaluadores_data, columns=evaluadores_cols)

        

        articulos_query = """
            SELECT a.id_articulo, a.provincias_autores, et.id_eje, et.nombre as eje_tematico
            FROM articulos a
            JOIN ejes_tematicos et ON a.eje_tematico = et.nombre;
        """
        cur.execute(articulos_query)
        articulos_data = cur.fetchall()
        articulos_cols = [desc[0] for desc in cur.description]
        df_articulos = pd.DataFrame(articulos_data, columns=articulos_cols)

        
        cur.execute(articulos_query)
        articulos_data = cur.fetchall()
        articulos_cols = [desc[0] for desc in cur.description]
        df_articulos = pd.DataFrame(articulos_data, columns=articulos_cols)



        ejes_query = "SELECT id_eje, nombre FROM ejes_tematicos ORDER BY id_eje;"
        cur.execute(ejes_query)
        ejes_data = cur.fetchall()
        ejes_cols = [desc[0] for desc in cur.description]
        df_ejes = pd.DataFrame(ejes_data, columns=ejes_cols)



        evaluador_eje_query = "SELECT id_evaluador, id_eje FROM evaluador_eje;"
        cur.execute(evaluador_eje_query)
        evaluador_eje_data = cur.fetchall()
        

        evaluador_eje = {}
        for eval_id, eje_id in evaluador_eje_data:
            evaluador_eje[(eval_id, eje_id)] = True
        


        cur.close()
        return df_costs, df_evaluadores, df_articulos, df_ejes, evaluador_eje

    except Exception as error:
        print(f"Error al obtener datos: {error}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}
    finally:
        if conn:
            conn.close()



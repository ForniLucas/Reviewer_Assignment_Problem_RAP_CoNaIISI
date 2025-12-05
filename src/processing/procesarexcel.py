import pandas as pd
import numpy as np
from src.db.dbconnection import get_db_connection
from src.services.embedding import generate_embeddings
from src.db.database import get_evaluadores_from_db, insert_evaluator_abstracts
from src.services.abstract_evaluadores import fetch_author_publications

def procesar(path):
    # =================================================================
    # 1. LECTURA DEL ARCHIVO EXCEL
    # =================================================================
    try:
        xls = pd.ExcelFile(path, engine='openpyxl')
        df_filiacion = pd.read_excel(xls, sheet_name='Filiación-Provincia')
        df_ejes = pd.read_excel(xls, sheet_name='Ejes temáticos')
        df_evaluadores = pd.read_excel(xls, sheet_name='Evaluadores')
        df_trabajos = pd.read_excel(xls, sheet_name='Trabajos')
        
        try:
            df_publicaciones_manuales = pd.read_excel(xls, sheet_name='abstracts a mano')
        except ValueError:
            print("Alerta: No se encontró la hoja 'abstracts a mano'. Se continuará sin procesar abstracts manuales.")
            df_publicaciones_manuales = pd.DataFrame(columns=['id', 'Abstract'])

    except FileNotFoundError:
        print("Error: No se encontró el archivo ASIGNACION.xlsx en la carpeta 'data'.")
        exit()

    # =================================================================
    # 2. PROCESAMIENTO DE DATOS PRIMARIOS (FILIACIONES, EJES, EVALUADORES, ARTÍCULOS)
    # =================================================================
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("Conexión establecida para procesar datos iniciales.")
        
        # --- Procesamiento de Filiaciones y Ejes ---
        df_filiacion.drop_duplicates(subset=['Filiación'], inplace=True)
        df_filiacion['Filiacion_norm'] = df_filiacion['Filiación'].str.lower().str.strip()
        mapa_filiacion_provincia = df_filiacion.set_index('Filiacion_norm')['Provincia'].to_dict()

        cur.execute("TRUNCATE TABLE ejes_tematicos RESTART IDENTITY CASCADE;")
        for eje in df_ejes['Ejes temáticos']:
            cur.execute("INSERT INTO ejes_tematicos (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING;", (eje,))
        
        cur.execute("TRUNCATE TABLE filiaciones_provincias RESTART IDENTITY CASCADE;")
        for index, row in df_filiacion.iterrows():
            cur.execute("INSERT INTO filiaciones_provincias (filiacion, provincia) VALUES (%s, %s);", (row['Filiación'], row['Provincia']))
        conn.commit()

        # --- Procesamiento de Evaluadores ---
        cur.execute("TRUNCATE TABLE evaluadores RESTART IDENTITY CASCADE;")
        cur.execute("TRUNCATE TABLE evaluador_eje RESTART IDENTITY CASCADE;")
        
        cur.execute("SELECT id_eje, nombre FROM ejes_tematicos;")
        ejes_db = {nombre: id for id, nombre in cur.fetchall()}

        for _, row in df_evaluadores.iterrows():
            filiacion_norm = str(row['Filiación']).lower().strip()
            provincia = mapa_filiacion_provincia.get(filiacion_norm, 'Desconocida')
            scholar_id_raw = row.get('google_scholar_id')
            google_id_for_db = scholar_id_raw if pd.notna(scholar_id_raw) else None
            
            cur.execute(
                """INSERT INTO evaluadores (id_evaluador, nombreyapellido, filiacion, provincia, email, carga_maxima, google_scholar_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id_evaluador;""",
                (row['Id'], row['Nombres'], row['Filiación'], provincia, row['Correo electrónico'], row['¿Cuántos trabajos está dispuesto a evaluar?'], google_id_for_db )
            )
            id_evaluador = cur.fetchone()[0]

            for eje_nombre, id_eje in ejes_db.items():
                if eje_nombre in row and pd.notna(row[eje_nombre]):
                    cur.execute("INSERT INTO evaluador_eje (id_evaluador, id_eje) VALUES (%s, %s);", (id_evaluador, id_eje))
        conn.commit()

        # --- Procesamiento de Artículos ---
        print("Procesando artículos, esto puede tardar por los embeddings...")
        cur.execute("TRUNCATE TABLE articulos RESTART IDENTITY CASCADE;")

        for _, row in df_trabajos.iterrows():
            provincias_autores = set()
            for i in range(1, 10): 
                col_institucion = f'Institución (Autor {i})'
                if col_institucion in row and pd.notna(row[col_institucion]):
                    institucion_norm = str(row[col_institucion]).lower().strip()
                    provincia = mapa_filiacion_provincia.get(institucion_norm)
                    if provincia:
                        provincias_autores.add(provincia)

            abstract = row['Resumen']
            embedding = None
            if pd.notna(abstract) and abstract.strip():
                embedding = generate_embeddings(abstract)
                if embedding is not None:
                    embedding = embedding.tolist()

            cur.execute(
                """INSERT INTO articulos (id_articulo, titulo, abstract, embedding, provincias_autores, eje_tematico) 
                    VALUES (%s, %s, %s, %s, %s, %s);""",
                (row['ID envío'], row['Título'], abstract, embedding, list(provincias_autores), row['Título de la categoría'])
            )
        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error durante el procesamiento de datos: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Procesamiento inicial de Excel finalizado.")

    # =================================================================
    # 3. PROCESAMIENTO DE ABSTRACTS DE EVALUADORES (LÓGICA INTEGRADA)
    # =================================================================
    print("\n--- Iniciando el procesamiento de abstracts de evaluadores ---")
    
    # Obtenemos la lista completa de evaluadores desde la BBDD
    evaluadores = get_evaluadores_from_db()
    if not evaluadores:
        print("No se encontraron evaluadores en la base de datos para procesar.")
        return

    # Creamos un diccionario para buscar abstracts manuales eficientemente
    manual_abstracts_dict = df_publicaciones_manuales.groupby('id')['Abstract'].apply(list).to_dict()

    # Iteramos sobre cada evaluador para obtener sus abstracts
    for evaluador in evaluadores:
        id_evaluador = evaluador['id_evaluador']
        google_scholar_id = evaluador['google_scholar_id']
        nombre = evaluador['nombreyapellido']
        
        abstracts_df = pd.DataFrame()

        # Decidimos la fuente de los abstracts: Google Scholar o Excel
        if pd.notna(google_scholar_id) and google_scholar_id.strip():
            print(f"Buscando en Google Scholar para '{nombre}' (ID: {id_evaluador})...")
            abstracts_df = fetch_author_publications(google_scholar_id)
        else:
            if id_evaluador in manual_abstracts_dict:
                abstracts_list = manual_abstracts_dict[id_evaluador]
                abstracts_df = pd.DataFrame(abstracts_list, columns=['abstract'])
                print(f"Se encontraron {len(abstracts_df)} abstracts manuales para '{nombre}' (ID: {id_evaluador}).")
            else:
                print(f"Alerta: '{nombre}' no tiene Google Scholar ID ni abstracts manuales definidos. Se omitirá.")
        
        # Si encontramos abstracts, generamos embeddings y los insertamos
        if not abstracts_df.empty:
            try:
                # ... dentro del bucle 'for evaluador in evaluadores:' ...
                print(f"Generando embeddings para los abstracts de '{nombre}'...")
                abstracts_df['abstract'] = abstracts_df['abstract'].astype(str)

# ESTA ES LA LÍNEA CORRECTA
                abstracts_df['embedding'] = abstracts_df['abstract'].apply(lambda x: generate_embeddings(x).tolist() if generate_embeddings(x) is not None else None)
                
                abstracts_df.dropna(subset=['embedding'], inplace=True)

                if not abstracts_df.empty:
                    insert_evaluator_abstracts(id_evaluador, abstracts_df)
                else:
                    print(f"No se pudieron generar embeddings para los abstracts de '{nombre}'.")
            except Exception as e:
                print(f"Error al procesar/insertar abstracts para el evaluador {id_evaluador}: {e}")

    print("\n¡Proceso de carga y procesamiento de abstracts completado!")
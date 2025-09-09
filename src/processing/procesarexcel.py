import pandas as pd
import numpy as np
from src.db.dbconnection import get_db_connection
from src.services.embedding import generate_embeddings



def procesar(path):
    try:
        xls = pd.ExcelFile(path, engine='openpyxl')
        df_filiacion = pd.read_excel(xls, sheet_name='Filiación-Provincia')
        df_ejes = pd.read_excel(xls, sheet_name='Ejes temáticos')
        df_evaluadores = pd.read_excel(xls, sheet_name='Evaluadores')
        df_trabajos = pd.read_excel(xls, sheet_name='Trabajos')
    except FileNotFoundError:
        print("no se esta ASIGNACION.xlsx en data")
        exit()


    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("conexion establecida")
    except Exception as e:
        print(f"error al conectar con la base de datos: {e}")
        exit()


    df_filiacion.drop_duplicates(subset=['Filiación'], inplace=True)
    df_filiacion['Filiacion_norm'] = df_filiacion['Filiación'].str.lower().str.strip()
    mapa_filiacion_provincia = df_filiacion.set_index('Filiacion_norm')['Provincia'].to_dict()


    try:
    
        cur.execute("TRUNCATE TABLE ejes_tematicos RESTART IDENTITY CASCADE;")
        for eje in df_ejes['Ejes temáticos']:
            cur.execute("INSERT INTO ejes_tematicos (nombre) VALUES (%s) ON CONFLICT (nombre) DO NOTHING;", (eje,))
        

        cur.execute("TRUNCATE TABLE filiaciones_provincias RESTART IDENTITY CASCADE;")
        for index, row in df_filiacion.iterrows():
            cur.execute("INSERT INTO filiaciones_provincias (filiacion, provincia) VALUES (%s, %s);", (row['Filiación'], row['Provincia']))
        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error en el procesado de filiaciones y ejes: {e}")
        cur.close()
        conn.close()
        exit()


    try:

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


    except Exception as e:
        conn.rollback()
        print(f"Error en el procesado de evaluadores y ejes: {e}")
        cur.close()
        conn.close()
        exit()


    try:
        print("Procesadon articulos esto puede tardar por los embeddings")
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
            if pd.isna(abstract) or not abstract.strip():
                print(f"Alerta: Artículo ID {row['ID envío']} no tiene abstract. Se omite el embedding.")
                embedding = None
            else:
                embedding = generate_embeddings(abstract).tolist()


            cur.execute(
                """INSERT INTO articulos (id_articulo, titulo, abstract, embedding, provincias_autores, eje_tematico) 
                    VALUES (%s, %s, %s, %s, %s, %s);""",
                (row['ID envío'], row['Título'], abstract, embedding, list(provincias_autores), row['Título de la categoría'])
            )

        conn.commit()


    except Exception as e:
        conn.rollback()
        print(f"Error en el procesado de articulos: {e}")

    finally:

        cur.close()
        conn.close()

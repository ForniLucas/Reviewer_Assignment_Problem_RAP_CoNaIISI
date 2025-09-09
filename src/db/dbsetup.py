import psycopg2
from dotenv import load_dotenv
import os
from src.db.dbconnection import get_db_connection


load_dotenv()


def create_tables():

    commands = (
        """
        CREATE TABLE IF NOT EXISTS filiaciones_provincias (
            filiacion TEXT PRIMARY KEY,
            provincia TEXT
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS ejes_tematicos (
            id_eje SERIAL PRIMARY KEY,
            nombre VARCHAR(255) NOT NULL UNIQUE
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS evaluadores (
            id_evaluador INTEGER NOT NULL PRIMARY KEY,
            nombreyapellido VARCHAR(255) NOT NULL,
            email VARCHAR(255),
            filiacion TEXT REFERENCES filiaciones_provincias(filiacion),
            provincia TEXT,
            carga_maxima INT DEFAULT 2,
            google_scholar_id VARCHAR(50) UNIQUE
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS evaluador_eje (
            id_evaluador INT NOT NULL REFERENCES evaluadores(id_evaluador) ON DELETE CASCADE,
            id_eje INT NOT NULL REFERENCES ejes_tematicos(id_eje) ON DELETE CASCADE,
            PRIMARY KEY (id_evaluador, id_eje)
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS abstract_evaluador (
            id_abstract SERIAL PRIMARY KEY,
            id_evaluador INT NOT NULL,
            texto_abstract TEXT NOT NULL,
            embedding VECTOR(1024),
            CONSTRAINT fk_evaluador
                FOREIGN KEY(id_evaluador) 
                REFERENCES evaluadores(id_evaluador)
                ON DELETE CASCADE
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS articulos (
            id_articulo INTEGER NOT NULL PRIMARY KEY,
            titulo TEXT NOT NULL,
            abstract TEXT NOT NULL,
            eje_tematico VARCHAR(255),
            embedding VECTOR(1024),
            provincias_autores TEXT[]
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS asignaciones (
            id_asignacion SERIAL PRIMARY KEY,
            id_articulo INT NOT NULL,
            id_evaluador INT NOT NULL,
            estado_revision VARCHAR(50) DEFAULT 'Asignado',
            costo_asignacion FLOAT,
            fecha_asignacion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_articulo
                FOREIGN KEY(id_articulo) 
                REFERENCES articulos(id_articulo),
            CONSTRAINT fk_evaluador
                FOREIGN KEY(id_evaluador) 
                REFERENCES evaluadores(id_evaluador)
        );
        """,

        """
        CREATE TABLE IF NOT EXISTS costos_asignacion (
            id_articulo INT NOT NULL,
            id_evaluador INT NOT NULL,
            costo FLOAT NOT NULL,
            PRIMARY KEY (id_articulo, id_evaluador),
            FOREIGN KEY (id_articulo) REFERENCES articulos(id_articulo) ON DELETE CASCADE,
            FOREIGN KEY (id_evaluador) REFERENCES evaluadores(id_evaluador) ON DELETE CASCADE
        );
        """
    )
    
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return 

        with conn.cursor() as cur:
            print("Creando tablas en la base de datos...")
            for command in commands:
                cur.execute(command)

        
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error al crear las tablas: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


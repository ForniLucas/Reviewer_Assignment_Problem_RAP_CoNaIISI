# main.py

from src.db.dbsetup import create_tables
from src.processing.procesarexcel import procesar
from src.optimizacion.calcular_costos import calculo_costos 
from src.optimizacion.asignacion import asignar

def main():
    """
    Función principal que orquesta todo el proceso de asignación.
    """
    #print("1. Creando tablas en la base de datos (si no existen)...")
    #create_tables()
    
    #La función 'procesar' ahora se encarga de todo: leer el Excel,
    #insertar los datos y procesar los abstracts de TODOS los evaluadores.
    #print("\n2. Procesando Excel y abstracts de evaluadores...")
    #procesar(r'data\ASIGNACION.xlsx')

    #print("\n3. Calculando costos de asignación...")
    #if calculo_costos():
    #    print("\n4. Ejecutando optimización y guardando asignaciones...")
    #    asignar()
    #else:
    #   print("El cálculo de costos falló. No se procederá con la asignación.")
    asignar()
    print("\n--- Proceso finalizado ---")


if __name__ == "__main__":
    main()
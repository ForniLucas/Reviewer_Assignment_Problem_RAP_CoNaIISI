import pandas as pd
import numpy as np
from ortools.graph.python import min_cost_flow
from src.db.database import get_data_for_optimization, save_final_assignments

# revisores que necesita cada artículo.
REVISORES_POR_TRABAJO = 3

# mas de 2 trabajos, le mande mil porque por lo general los costos andan en 400-500
PENALIZACION_CARGA_ALTA = 1000

# Nodo ficticio
NODO_FICTICIO = 0


def asignar():
    df_costs, df_evaluadores, df_articulos, df_ejes, evaluador_eje = get_data_for_optimization()
   
    if df_costs.empty or df_evaluadores.empty or df_articulos.empty or df_ejes.empty:
        print("Error: No se pudieron cargar los datos necesarios de la base de datos")
        return


    

    articulos_provincias = df_articulos.set_index('id_articulo')['provincias_autores'].to_dict()
    articulo_eje = df_articulos.set_index('id_articulo')['id_eje'].to_dict()
    evaluadores_info = df_evaluadores.set_index('id_evaluador').to_dict('index')
    
    

    costos_dict = {}
    for _, row in df_costs.iterrows():
        key = (int(row['id_evaluador']), int(row['id_articulo']))
        costos_dict[key] = float(row['costo'])

    all_articulos_ids = df_articulos['id_articulo'].unique().astype(int)
    all_evaluadores_ids = df_evaluadores['id_evaluador'].unique().astype(int)
    all_ejes_ids = df_ejes['id_eje'].unique().astype(int)

    print(f"Artículos: {len(all_articulos_ids)}")
    print(f"Evaluadores: {len(all_evaluadores_ids)}")  
    print(f"Ejes temáticos: {len(all_ejes_ids)}")

    total_supply_evaluadores = sum(info['carga_maxima'] for info in evaluadores_info.values())
    total_demand_articulos = len(all_articulos_ids) * REVISORES_POR_TRABAJO


    smcf = min_cost_flow.SimpleMinCostFlow()

    start_nodes = []
    end_nodes = []
    capacities = []
    unit_costs = []

    # Evaluadores al ficticio Ei a Df

    for eval_id in all_evaluadores_ids:
     max_carga = evaluadores_info[eval_id]['carga_maxima']
     start_nodes.append(int(eval_id))        
     end_nodes.append(NODO_FICTICIO)          
     capacities.append(int(max_carga))
     unit_costs.append(0)

    # Evaluadores primeras 2 asignaciones
    evaluadores_en_tracks = []
    for eval_id in all_evaluadores_ids:
        for eje_id in all_ejes_ids:
            if (eval_id, eje_id) in evaluador_eje:
                copia_id = int(eval_id * 10 + eje_id)
                evaluadores_en_tracks.append(copia_id)
                start_nodes.append(int(eval_id))
                end_nodes.append(copia_id)
                capacities.append(2)  # Primeras 2 asignaciones
                unit_costs.append(0)  # Sin costo

    print(f"Ei a Eim: {len(evaluadores_en_tracks)}")

    # Evaluadores asignaciones 3 o mas
    evaluadores_en_tracks_2 = []
    for eval_id in all_evaluadores_ids:
        max_carga = evaluadores_info[eval_id]['carga_maxima']
        if max_carga > 2:
            for eje_id in all_ejes_ids:
                if (eval_id, eje_id) in evaluador_eje:
                    # ID de la copia: evaluador_id * 100 + eje_id
                    copia_id = int(eval_id * 100 + eje_id)
                    evaluadores_en_tracks_2.append(copia_id)
                    
                    start_nodes.append(int(eval_id))
                    end_nodes.append(copia_id)
                    capacities.append(int(max_carga - 2))
                    unit_costs.append(PENALIZACION_CARGA_ALTA)  

    print(f"Ei a Eir: {len(evaluadores_en_tracks_2)}")

    # Primeras 2 asignaciones a eje tematico
    evaluadores_en_tracks_F = []
    for copia_id in evaluadores_en_tracks:
        eval_id = int(copia_id / 10)
        eje_id = copia_id % 10
        max_carga = evaluadores_info[eval_id]['carga_maxima']
        copia_f_id = int(eval_id * 1000 + eje_id)
        evaluadores_en_tracks_F.append(copia_f_id)
        start_nodes.append(copia_id)
        end_nodes.append(copia_f_id)
        capacities.append(int(max_carga))
        unit_costs.append(0)

    # 3 o mas asignaciones a eje tematico
    for copia_id in evaluadores_en_tracks_2:
        eval_id = int(copia_id / 100)
        eje_id = copia_id % 100
        max_carga = evaluadores_info[eval_id]['carga_maxima']
        copia_f_id = int(eval_id * 1000 + eje_id)
        start_nodes.append(copia_id)
        end_nodes.append(copia_f_id)
        capacities.append(int(max_carga))
        unit_costs.append(0)

    print(f"Eim a EiTu y Eir: {len(evaluadores_en_tracks_F)}")

    # eje a articulo con costos de embeddings
    asignaciones_creadas = 0
    for copia_f_id in evaluadores_en_tracks_F:
        eval_id = int(copia_f_id / 1000)
        eje_id = copia_f_id % 1000
        
        for art_id in all_articulos_ids:
            if articulo_eje[art_id] == eje_id:
                provincia_evaluador = evaluadores_info[eval_id]['provincia']
                provincias_articulo = articulos_provincias[art_id]
                
                if provincia_evaluador in provincias_articulo:
                    continue 
                

                costo_key = (eval_id, art_id)
                if costo_key in costos_dict:
                    # Escalar el costo y convertir a entero 
                    costo_embedding = int(costos_dict[costo_key] * 1000)
                    
                    start_nodes.append(copia_f_id)
                    end_nodes.append(int(art_id))
                    capacities.append(1)
                    unit_costs.append(costo_embedding)
                    asignaciones_creadas += 1

    print(f"EiTu a Aj: {asignaciones_creadas}")

    # aniadir al modelo todo
    smcf.add_arcs_with_capacity_and_unit_cost(
        np.array(start_nodes), np.array(end_nodes), np.array(capacities), np.array(unit_costs)
    )

    # oferta y demanda

 

    print(f"Oferta total: {total_supply_evaluadores}")
    print(f"Demanda total : {total_demand_articulos}")

    # Nodo Ficticio
    balance_value = -(total_supply_evaluadores - total_demand_articulos)  # Negativo
    smcf.set_node_supply(NODO_FICTICIO, int(balance_value))
    print(f"Nodo ficticio: {balance_value}")

    # oferta
    for eval_id in all_evaluadores_ids:
        smcf.set_node_supply(int(eval_id), int(evaluadores_info[eval_id]['carga_maxima']))

    # TRANSBORDO
    for copia_id in evaluadores_en_tracks:
        smcf.set_node_supply(copia_id, 0)

    #TRANSBORDO
    for copia_id in evaluadores_en_tracks_2:
        smcf.set_node_supply(copia_id, 0)

    # TRANSBORDO
    for copia_id in evaluadores_en_tracks_F:
        smcf.set_node_supply(copia_id, 0)

    # DEMANDA
    for art_id in all_articulos_ids:
        smcf.set_node_supply(int(art_id), -REVISORES_POR_TRABAJO)

    print("Modelo construido")



    status = smcf.solve()

    if status != smcf.OPTIMAL:
        print("No se pudo encontrar una solución optima.")
        print(f"Estado del solver: {status}")
        return

    print(f"Solucion optima encontrada con un costo total de {smcf.optimal_cost()}!")


    asignaciones = []
    
    print("\nAsignaciones realizadas:")
    print("=" * 80)
    
    for i in range(smcf.num_arcs()):

        if smcf.flow(i) <= 0:
            continue
            
        nodo_origen = smcf.tail(i)
        nodo_destino = smcf.head(i)


        if nodo_destino in all_articulos_ids and nodo_origen >= 1000:
            
            # Extraer el ID del evaluador del nodo copia final
            id_evaluador = int(nodo_origen / 1000)
            id_articulo = nodo_destino
            costo_real = smcf.unit_cost(i) / 1000.0  # Revertir la escala
            
            
            for _ in range(smcf.flow(i)):
                asignaciones.append({
                    'id_articulo': id_articulo,
                    'id_evaluador': id_evaluador,
                    'costo_asignacion': costo_real
                })
                print(f"Evaluador {id_evaluador} Artículo {id_articulo} (Costo: {costo_real:.6f})")

    if not asignaciones:
        print("El modelo no genero ninguna asignacion.")
        return

 
    df_asignaciones = pd.DataFrame(asignaciones)
    save_final_assignments(df_asignaciones)
    

    print(f"Se han guardado {len(df_asignaciones)} asignaciones en la base de datos.")
    
    # Estadísticas adicionales
    evaluadores_asignados = df_asignaciones['id_evaluador'].nunique()
    articulos_asignados = df_asignaciones['id_articulo'].nunique()
    carga_por_evaluador = df_asignaciones['id_evaluador'].value_counts()
    
    print(f"Evaluadores que recibieron asignaciones: {evaluadores_asignados}")
    print(f"Articulos que recibieron al menos una asignacion: {articulos_asignados}")
    print(f"Carga max asignada a un evaluador: {carga_por_evaluador.max()}")
    print(f"Carga promedio por evaluador activo: {carga_por_evaluador.mean():.2f}")
    print("=" * 80)



from src.db.database import calculate_all_costs_in_db, save_costs_to_db


def calculo_costos():


    cost_data = calculate_all_costs_in_db()

    if not cost_data:
        print("No se pudieron calcular los costos desde la base de datos")
        return False 


    save_costs_to_db(cost_data)


    return True
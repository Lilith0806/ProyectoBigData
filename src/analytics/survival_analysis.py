import os
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test

def run_survival_analysis():
    print("Conectando a MongoDB para extraer datos de supervivencia...")
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["lifemap_db"]
    
    # Extraer campos de ciclo de vida
    cursor = db.businesses.find(
        {},
        {"business_id": 1, "dias_negocio": 1, "cerro": 1, "stars": 1, "categories": 1, "_id": 0}
    )
    df = pd.DataFrame(list(cursor))
    print(f"Total negocios analizados: {len(df)}")

    # T: duracion en dias | E: evento (1 = cerro, 0 = censurado/sigue abierto)
    T = df["dias_negocio"]
    E = df["cerro"]

    kmf = KaplanMeierFitter()
    kmf.fit(T, event_observed=E, label="Restaurantes Filadelfia (Global)")

    os.makedirs("results", exist_ok=True)

    # 1. Grafica Kaplan-Meier Global
    plt.figure(figsize=(10, 6))
    kmf.plot_survival_function(ci_show=True)
    plt.title("Curva de Supervivencia Kaplan-Meier - Negocios Gastronómicos (Filadelfia)")
    plt.xlabel("Tiempo de vida del negocio (días)")
    plt.ylabel("Probabilidad de permanecer abierto S(t)")
    plt.grid(True, linestyle="--", alpha=0.6)
    
    global_fig_path = "results/curva_kaplan_meier_global.png"
    plt.savefig(global_fig_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Grafica global guardada en: {global_fig_path}")

    # 2. Comparacion por nivel de reputacion: Estrellas Altas (>= 4.0) vs Bajas (< 4.0)
    df["reputacion"] = df["stars"].apply(lambda s: "Reputacion Alta (>=4)" if s >= 4.0 else "Reputacion Media/Baja (<4)")

    kmf_alta = KaplanMeierFitter()
    kmf_baja = KaplanMeierFitter()

    mask_alta = df["reputacion"] == "Reputacion Alta (>=4)"
    
    plt.figure(figsize=(10, 6))
    kmf_alta.fit(T[mask_alta], event_observed=E[mask_alta], label="Estrellas >= 4.0")
    kmf_baja.fit(T[~mask_alta], event_observed=E[~mask_alta], label="Estrellas < 4.0")

    kmf_alta.plot_survival_function(ci_show=True)
    kmf_baja.plot_survival_function(ci_show=True)

    # Prueba de Log-Rank para verificar significancia estadistica
    results = logrank_test(T[mask_alta], T[~mask_alta], event_observed_A=E[mask_alta], event_observed_B=E[~mask_alta])

    plt.title(f"Supervivencia segun Calificacion Yelp (Log-Rank p-value: {results.p_value:.4e})")
    plt.xlabel("Tiempo de vida del negocio (días)")
    plt.ylabel("Probabilidad de permanecer abierto S(t)")
    plt.grid(True, linestyle="--", alpha=0.6)

    comp_fig_path = "results/curva_kaplan_meier_reputacion.png"
    plt.savefig(comp_fig_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Grafica comparativa guardada en: {comp_fig_path}")
    print(f"Mediana de supervivencia global: {kmf.median_survival_time_} dias")
    print(f"Log-Rank p-value: {results.p_value:.4e}")

if __name__ == "__main__":
    run_survival_analysis()

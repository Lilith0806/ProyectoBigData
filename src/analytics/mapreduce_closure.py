import os
import sys
import time
import json
import numpy as np
import pandas as pd
import pygeohash as pgh
import pymongo

# 1. Asegurar ruta absoluta al interprete de Python para evitar el alias de Windows Store
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_primary_category(cat_str):
    if not cat_str:
        return "Other"
    cats = [c.strip() for c in cat_str.split(",")]
    targets = ["Restaurants", "Food", "Bars", "Coffee & Tea", "Pizza", "Sandwiches", "American (Traditional)", "Italian", "Mexican", "Breakfast & Brunch"]
    for t in targets:
        if t in cats:
            return t
    return cats[0]

def run_mapreduce_closure():
    spark = SparkSession.builder \
        .appName("LifeMap-MapReduce-Closure") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.python.worker.reuse", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client["lifemap_db"]

    print("Cargando negocios desde MongoDB...")
    cursor = db.businesses.find({}, {"business_id": 1, "categories": 1, "location": 1, "dias_negocio": 1, "cerro": 1, "_id": 0})
    raw_b = list(cursor)
    
    processed = []
    for b in raw_b:
        coords = b.get("location", {}).get("coordinates", [-75.1652, 39.9526])
        lon, lat = coords[0], coords[1]
        geohash_cell = pgh.encode(lat, lon, precision=6)
        cat = get_primary_category(b.get("categories", ""))
        dias = max(int(b.get("dias_negocio", 1)), 1)
        cerro = int(b.get("cerro", 0))
        processed.append({
            "business_id": b["business_id"],
            "category": cat,
            "geohash": geohash_cell,
            "dias_negocio": dias,
            "cerro": cerro
        })

    pdf_base = pd.DataFrame(processed)
    df_spark = spark.createDataFrame(pdf_base)
    
    # Cachear el DataFrame para optimizar las operaciones
    df_spark = df_spark.cache()
    rdd_base = df_spark.rdd

    # ============================================================
    # 1. MAPREDUCE CON RDD: EMITIR (clave, (cerro, dias)) -> reduceByKey
    # ============================================================
    print("\n--- Ejecutando MapReduce con RDD: Tasas por Categoria ---")
    rdd_cat = rdd_base.map(lambda r: (r["category"], (r["cerro"], r["dias_negocio"]))) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    cat_rates_rdd = rdd_cat.mapValues(lambda v: {
        "cierres": v[0],
        "dias_negocio": v[1],
        "tasa_anualizada": round((v[0] / v[1]) * 365, 4) if v[1] >= 180 else 0.0
    }).collectAsMap()

    # Validacion contra DataFrames groupBy
    df_cat_val = df_spark.groupBy("category").agg(
        F.sum("cerro").alias("sum_cerro"),
        F.sum("dias_negocio").alias("sum_dias")
    ).withColumn("tasa_df", F.round((F.col("sum_cerro") / F.col("sum_dias")) * 365, 4)).toPandas()

    print("Validacion RDD reduceByKey vs DataFrame groupBy:")
    for _, row in df_cat_val.head(5).iterrows():
        cat = row["category"]
        rdd_tasa = cat_rates_rdd[cat]["tasa_anualizada"]
        df_tasa = row["tasa_df"]
        print(f" - [{cat}] RDD: {rdd_tasa} | DF: {df_tasa} | Coinciden: {abs(rdd_tasa - df_tasa) < 1e-4}")

    # MapReduce por Geohash
    print("\n--- Ejecutando MapReduce con RDD: Tasas por Geohash ---")
    rdd_geo = rdd_base.map(lambda r: (r["geohash"], (r["cerro"], r["dias_negocio"]))) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    geo_rates_rdd = rdd_geo.mapValues(lambda v: {
        "cierres": v[0],
        "dias_negocio": v[1],
        "tasa_anualizada": round((v[0] / v[1]) * 365, 4) if v[1] >= 180 else 0.0
    }).collectAsMap()

    # Cargar agregaciones en MongoDB
    db.stats_category.drop()
    cat_docs = [{"category": k, **v} for k, v in cat_rates_rdd.items()]
    db.stats_category.insert_many(cat_docs)
    db.stats_category.create_index("category", unique=True)

    db.stats_geohash.drop()
    geo_docs = [{"geohash": k, **v} for k, v in geo_rates_rdd.items()]
    db.stats_geohash.insert_many(geo_docs)
    db.stats_geohash.create_index("geohash", unique=True)
    print(f"Guardadas colecciones stats_category ({len(cat_docs)}) y stats_geohash ({len(geo_docs)}).")

    # ============================================================
    # 2. EXPERIMENTO DE BALANCEO DE CARGA Y SESGO (CRITERIO E1.8)
    # ============================================================
    print("\n================ EXPERIMENTO DE BALANCEO DE CARGA ================")
    partition_configs = [2, 8, 32, 200]
    bench_results = []

    for parts in partition_configs:
        repartitioned = df_spark.repartition(parts)
        times = []
        for rep in range(3):
            t0 = time.time()
            repartitioned.groupBy("category").agg(
                F.sum("cerro"),
                F.sum("dias_negocio")
            ).collect()
            times.append(time.time() - t0)
        
        counts_df = repartitioned.withColumn("pid", F.spark_partition_id()) \
                                 .groupBy("pid").count().toPandas()
        
        mean_time = float(np.mean(times))
        std_time = float(np.std(times))
        min_rows = int(counts_df["count"].min())
        max_rows = int(counts_df["count"].max())
        std_rows = float(counts_df["count"].std())

        bench_results.append({
            "particiones": parts,
            "repeticiones_s": [round(t, 4) for t in times],
            "media_s": round(mean_time, 4),
            "desv_est_s": round(std_time, 4),
            "min_filas_particion": min_rows,
            "max_filas_particion": max_rows,
            "desv_est_filas": round(std_rows, 2)
        })
        print(f"Particiones: {parts:3d} | Media: {mean_time:.4f}s (+/- {std_time:.4f}s) | Filas [Min: {min_rows}, Max: {max_rows}, Std: {std_rows:.1f}]")

    os.makedirs("results", exist_ok=True)
    with open("results/benchmark_balanceo.json", "w", encoding="utf-8") as f:
        json.dump(bench_results, f, indent=4)
    print("Experimento completado y guardado en results/benchmark_balanceo.json")

    spark.stop()

if __name__ == "__main__":
    run_mapreduce_closure()

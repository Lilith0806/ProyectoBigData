import os
import sys
import json
import numpy as np
import pandas as pd
import pygeohash as pgh
import pymongo

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

def run_analytics_delta():
    spark = SparkSession.builder \
        .appName("LifeMap-Delta-Analytics") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = client["lifemap_db"]

    print("Extrayendo negocios y enriqueciendo con geohash...")
    cursor = db.businesses.find({}, {"business_id": 1, "categories": 1, "location": 1, "dias_negocio": 1, "cerro": 1, "_id": 0})
    raw_b = list(cursor)
    
    processed = []
    b_geo_map = {}
    for b in raw_b:
        coords = b.get("location", {}).get("coordinates", [-75.1652, 39.9526])
        lat, lon = coords[1], coords[0]
        gh = pgh.encode(lat, lon, precision=6)
        cat = get_primary_category(b.get("categories", ""))
        dias = max(int(b.get("dias_negocio", 1)), 1)
        cerro = int(b.get("cerro", 0))
        bid = b["business_id"]
        b_geo_map[bid] = gh
        processed.append({"business_id": bid, "category": cat, "geohash": gh, "dias_negocio": dias, "cerro": cerro})

    df_b = spark.createDataFrame(processed).cache()

    # 1. MapReduce Cierres por Geohash
    rdd_geo = df_b.rdd.map(lambda r: (r["geohash"], (r["cerro"], r["dias_negocio"]))) \
                      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    geo_rates = rdd_geo.mapValues(lambda v: {
        "cierres": v[0],
        "dias_negocio": v[1],
        "tasa_anualizada": round((v[0] / v[1]) * 365, 4) if v[1] >= 180 else 0.0
    }).collectAsMap()

    # 2. Porcentaje de reseñas negativas por celda Geohash
    print("Calculando distribucion de resenas negativas por geohash...")
    rev_cursor = db.reviews.find({"stars": {"$exists": True}}, {"business_id": 1, "stars": 1, "_id": 0}).limit(100000)
    rev_list = []
    for r in rev_cursor:
        bid = r.get("business_id")
        if bid in b_geo_map:
            rev_list.append({"geohash": b_geo_map[bid], "es_negativa": 1 if r["stars"] <= 2.0 else 0})

    df_rev = spark.createDataFrame(rev_list)
    neg_by_geo = df_rev.groupBy("geohash").agg(
        F.count("es_negativa").alias("total_reviews"),
        F.sum("es_negativa").alias("total_negativas")
    ).withColumn("pct_negativas", F.round((F.col("total_negativas") / F.col("total_reviews")) * 100, 2)).toPandas()

    neg_dict = neg_by_geo.set_index("geohash").to_dict(orient="index")

    # Actualizar MongoDB con porcentaje de negativas
    geo_docs = []
    for gh, data in geo_rates.items():
        rev_info = neg_dict.get(gh, {"pct_negativas": 0.0, "total_reviews": 0})
        geo_docs.append({
            "geohash": gh,
            **data,
            "pct_resenas_negativas": rev_info["pct_negativas"]
        })

    db.stats_geohash.drop()
    db.stats_geohash.insert_many(geo_docs)
    print(f"Coleccion stats_geohash actualizada con tasas y % negativas ({len(geo_docs)} docs).")

    # 3. Comparacion de Sesgo: Reparto Con Clave vs Sin Clave (E1.8)
    print("\nEvaluando sesgo de reparto: Con Clave (por categoria) vs Sin Clave (8 particiones)...")
    sin_clave = df_b.repartition(8).withColumn("pid", F.spark_partition_id()).groupBy("pid").count().toPandas()
    con_clave = df_b.repartition(8, "category").withColumn("pid", F.spark_partition_id()).groupBy("pid").count().toPandas()

    print("\n--- Conteo Sin Clave (Round-robin) ---")
    print(sin_clave.to_string(index=False))
    print(f"Desviacion estandar sin clave: {sin_clave['count'].std():.2f}")

    print("\n--- Conteo Con Clave (Hash partitioning por Categoria) ---")
    print(con_clave.to_string(index=False))
    print(f"Desviacion estandar con clave: {con_clave['count'].std():.2f}")

    spark.stop()

if __name__ == "__main__":
    run_analytics_delta()

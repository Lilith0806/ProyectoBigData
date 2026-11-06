import json
import logging
from datetime import datetime
from pathlib import Path
import dask.bag as db
import dask.dataframe as dd
import pandas as pd
from pymongo import MongoClient, GEOSPHERE, ASCENDING

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MONGO_URI = "mongodb://localhost:27017/lifemap_db"
DATA_DIR = Path("data")

def clean_and_ingest():
    client = MongoClient(MONGO_URI)
    database = client["lifemap_db"]
    businesses_col = database["businesses"]
    reviews_col = database["reviews"]

    logging.info("1. Leyendo business.json con Dask Bag...")
    raw_b = db.read_text(str(DATA_DIR / "business.json")).map(json.loads)
    total_b_inicial = raw_b.count().compute()
    logging.info(f"Total negocios crudos: {total_b_inicial}")

    # Regla 1: Coordenadas válidas y no nulas
    b_valid_coords = raw_b.filter(
        lambda b: b.get("latitude") is not None 
        and b.get("longitude") is not None
        and -90 <= float(b["latitude"]) <= 90
        and -180 <= float(b["longitude"]) <= 180
    )
    c_coords = b_valid_coords.count().compute()
    logging.info(f"Negocios tras descarte de coordenadas invalidas: {c_coords} (Descartados: {total_b_inicial - c_coords})")

    # Regla 2: Área Metropolitana de Philadelphia (PA) y categoría Restaurants/Food
    def filter_philly_food(b):
        state = (b.get("state") or "").strip().upper()
        categories = (b.get("categories") or "").lower()
        if state != "PA":
            return False
        keywords = ["restaurant", "food", "bar", "cafe", "bakery", "diner"]
        return any(k in categories for k in keywords)

    b_philly_food = b_valid_coords.filter(filter_philly_food)
    c_philly = b_philly_food.count().compute()
    logging.info(f"Negocios en Philadelphia (Restaurants/Food): {c_philly}")

    # Convertir a Dask DataFrame para deduplicación
    df_b = b_philly_food.to_dataframe()
    df_b = df_b.drop_duplicates(subset=["business_id"]).compute()
    logging.info(f"Negocios finales unicos seleccionados: {len(df_b)}")

    target_business_ids = set(df_b["business_id"])

    # 2. Ingesta y filtrado de Reseñas
    logging.info("2. Leyendo review.json con Dask...")
    raw_r = db.read_text(str(DATA_DIR / "review.json")).map(json.loads)

    # Filtrar solo reseñas de los negocios seleccionados con texto valido
    def filter_reviews(r):
        bid = r.get("business_id")
        text = r.get("text")
        date_str = r.get("date")
        return (bid in target_business_ids) and bool(text and text.strip()) and bool(date_str)

    r_filtered = raw_r.filter(filter_reviews)
    df_r = r_filtered.to_dataframe().compute()
    logging.info(f"Total reseñas filtradas para Philadelphia: {len(df_r)}")

    # Regla: Deduplicación de reseñas por review_id
    total_r_antes = len(df_r)
    df_r = df_r.drop_duplicates(subset=["review_id"])
    logging.info(f"Reseñas tras eliminar duplicados: {len(df_r)} (Descartados: {total_r_antes - len(df_r)})")

    # Asegurar formato fecha
    df_r["date_dt"] = pd.to_datetime(df_r["date"], errors="coerce")
    df_r = df_r.dropna(subset=["date_dt"])

    # 3. Construcción de la Vida de cada Negocio
    logging.info("3. Calculando ciclo de vida, dias-negocio, censura y entrada tardia...")
    stats_r = df_r.groupby("business_id").agg(
        primera_resena=("date_dt", "min"),
        ultima_resena=("date_dt", "max"),
        total_resenas=("review_id", "count")
    ).reset_index()

    # Cruce con datos del negocio
    df_merged = pd.merge(df_b, stats_r, on="business_id", how="inner")

    # Calculo de dias-negocio y estado de censura
    df_merged["dias_negocio"] = (df_merged["ultima_resena"] - df_merged["primera_resena"]).dt.days
    # Minimo 1 dia para evitar divisiones por cero en tasas
    df_merged["dias_negocio"] = df_merged["dias_negocio"].apply(lambda d: max(int(d), 1))
    
    # Evento observado: is_open == 0 es cierre (1). Si is_open == 1, esta censurado (0)
    df_merged["cerro"] = df_merged["is_open"].apply(lambda x: 1 if int(x) == 0 else 0)
    df_merged["censurado"] = df_merged["is_open"].apply(lambda x: 1 if int(x) == 1 else 0)

    # 4. Formatear GeoJSON y Cargar en MongoDB
    logging.info("4. Formateando GeoJSON e insertando en MongoDB...")
    docs_to_insert = []
    for _, row in df_merged.iterrows():
        doc = {
            "business_id": row["business_id"],
            "name": row.get("name"),
            "city": row.get("city"),
            "state": row.get("state"),
            "categories": row.get("categories"),
            "stars": float(row.get("stars", 0.0)),
            "review_count": int(row.get("review_count", 0)),
            "is_open": int(row.get("is_open", 1)),
            "location": {
                "type": "Point",
                "coordinates": [float(row["longitude"]), float(row["latitude"])]
            },
            "primera_resena": row["primera_resena"].strftime("%Y-%m-%d %H:%M:%S"),
            "ultima_resena": row["ultima_resena"].strftime("%Y-%m-%d %H:%M:%S"),
            "total_resenas_calc": int(row["total_resenas"]),
            "dias_negocio": int(row["dias_negocio"]),
            "cerro": int(row["cerro"]),
            "censurado": int(row["censurado"])
        }
        docs_to_insert.append(doc)

    businesses_col.drop()
    businesses_col.create_index([("location", GEOSPHERE)])
    businesses_col.create_index([("business_id", ASCENDING)], unique=True)
    businesses_col.create_index([("categories", ASCENDING)])

    if docs_to_insert:
        businesses_col.insert_many(docs_to_insert, ordered=False)
        logging.info(f"Cargados {len(docs_to_insert)} negocios en coleccion 'businesses' con indice 2dsphere.")

    # Cargar reseñas limpias en lotes
    reviews_col.drop()
    reviews_col.create_index([("review_id", ASCENDING)], unique=True)
    reviews_col.create_index([("business_id", ASCENDING)])

    batch_size = 50000
    rev_records = []
    logging.info("Cargando resenas limpias por lotes en MongoDB...")
    for _, row in df_r.iterrows():
        rev_records.append({
            "review_id": row["review_id"],
            "business_id": row["business_id"],
            "stars": float(row.get("stars", 0)),
            "text": row.get("text"),
            "date": row["date_dt"].strftime("%Y-%m-%d %H:%M:%S")
        })
        if len(rev_records) >= batch_size:
            reviews_col.insert_many(rev_records, ordered=False)
            rev_records = []

    if rev_records:
        reviews_col.insert_many(rev_records, ordered=False)

    total_rev_final = reviews_col.count_documents({})
    logging.info(f"Carga completa. Total resenas en MongoDB: {total_rev_final}")
    logging.info("Pipeline de ingesta y limpieza finalizado exitosamente.")

if __name__ == "__main__":
    clean_and_ingest()

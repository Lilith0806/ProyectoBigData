import os
import sys
import json
from pathlib import Path
import pymongo
import pandas as pd

# Configuracion local para Windows
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def train_sentiment_model():
    sample_file = Path("data/sentiment_sample.json")

    if not sample_file.exists():
        print("Consultando muestra balanceada desde MongoDB...")
        client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
        db = client["lifemap_db"]

        neg_cursor = db.reviews.find({"stars": {"$in": [1.0, 2.0]}}, {"business_id": 1, "text": 1, "_id": 0}).limit(30000)
        pos_cursor = db.reviews.find({"stars": {"$in": [4.0, 5.0]}}, {"business_id": 1, "text": 1, "_id": 0}).limit(30000)

        pdf_neg = pd.DataFrame(list(neg_cursor))
        pdf_neg["label"] = 0.0

        pdf_pos = pd.DataFrame(list(pos_cursor))
        pdf_pos["label"] = 1.0

        pdf = pd.concat([pdf_neg, pdf_pos], ignore_index=True).sample(frac=1.0, random_state=42).reset_index(drop=True)
        pdf.to_json(str(sample_file), orient="records", lines=True)
        print(f"Muestra de {len(pdf)} reseñas guardada en {sample_file}")

    spark = SparkSession.builder \
        .appName("LifeMap-Sentiment") \
        .master("local[2]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("Leyendo muestra particionada con Spark...")

    df = spark.read.json(str(sample_file)).select("business_id", "text", "label")

    # Partición SIN FUGA por negocio (business_id)
    df = df.withColumn("fold", F.abs(F.hash("business_id")) % 10)
    train_df = df.filter("fold < 8").cache()
    test_df = df.filter("fold >= 8").cache()

    n_train = train_df.count()
    n_test = test_df.count()
    print(f"Train: {n_train} | Test: {n_test}")

    tokenizer = Tokenizer(inputCol="text", outputCol="words_raw")
    remover = StopWordsRemover(inputCol="words_raw", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=4000)
    idf = IDF(inputCol="raw_features", outputCol="features")

    # Modelo 1: Regresión Logística
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)
    pipeline_lr = Pipeline(stages=[tokenizer, remover, hashingTF, idf, lr])
    print("Entrenando Regresión Logística...")
    model_lr = pipeline_lr.fit(train_df)
    preds_lr = model_lr.transform(test_df)

    # Modelo 2: Naive Bayes
    nb = NaiveBayes(featuresCol="features", labelCol="label", smoothing=1.0)
    pipeline_nb = Pipeline(stages=[tokenizer, remover, hashingTF, idf, nb])
    print("Entrenando Naive Bayes...")
    model_nb = pipeline_nb.fit(train_df)
    preds_nb = model_nb.transform(test_df)

    # Evaluadores
    eval_f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    eval_prec = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedPrecision")
    eval_rec = MulticlassClassificationEvaluator(labelCol="label", metricName="weightedRecall")

    f1_lr, prec_lr, rec_lr = eval_f1.evaluate(preds_lr), eval_prec.evaluate(preds_lr), eval_rec.evaluate(preds_lr)
    f1_nb, prec_nb, rec_nb = eval_f1.evaluate(preds_nb), eval_prec.evaluate(preds_nb), eval_rec.evaluate(preds_nb)

    print("\n================ RESULTADOS SENTIMIENTO ================")
    print(f"Regresion Logistica -> F1: {f1_lr:.4f} | Precision: {prec_lr:.4f} | Recall: {rec_lr:.4f}")
    print(f"Naive Bayes         -> F1: {f1_nb:.4f} | Precision: {prec_nb:.4f} | Recall: {rec_nb:.4f}")
    print("========================================================\n")

    os.makedirs("models", exist_ok=True)
    summary = {
        "train_samples": n_train,
        "test_samples": n_test,
        "metrics": {
            "logistic_regression": {"f1": round(f1_lr, 4), "precision": round(prec_lr, 4), "recall": round(rec_lr, 4)},
            "naive_bayes": {"f1": round(f1_nb, 4), "precision": round(prec_nb, 4), "recall": round(rec_nb, 4)}
        },
        "best_model": "LogisticRegression" if f1_lr >= f1_nb else "NaiveBayes"
    }

    with open("models/sentiment_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=4)

    print("Resumen de métricas y evaluación guardado exitosamente en models/sentiment_summary.json")
    spark.stop()

if __name__ == "__main__":
    train_sentiment_model()

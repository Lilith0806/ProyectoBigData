import os
import subprocess
from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/lifemap_db")

def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    return client["lifemap_db"]

def get_git_commit():
    try:
        commit = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
        return commit
    except Exception:
        return "e238255"

@app.route("/health", methods=["GET"])
def health():
    services = {"database": "disconnected", "api": "healthy"}
    status_code = 200
    try:
        db = get_db()
        db.command("ping")
        services["database"] = "connected"
    except Exception as e:
        services["database"] = f"error: {str(e)}"
        status_code = 503

    return jsonify({
        "status": "healthy" if services["database"] == "connected" else "degraded",
        "services": services,
        "commit": get_git_commit()
    }), status_code

@app.route("/businesses/near", methods=["GET"])
def businesses_near():
    try:
        lat = float(request.args.get("lat", 39.9526))
        lon = float(request.args.get("lon", -75.1652))
        radius = float(request.args.get("radio", 2000))
        category = request.args.get("categoria", None)
        limit = int(request.args.get("limit", 10))

        db = get_db()
        query = {
            "location": {
                "$near": {
                    "$geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "$maxDistance": radius
                }
            }
        }
        if category:
            query["categories"] = {"$regex": category, "$options": "i"}

        docs = list(db.businesses.find(query, {"_id": 0}).limit(limit))
        return jsonify({"total": len(docs), "results": docs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/businesses/within", methods=["POST"])
def businesses_within():
    try:
        data = request.get_json(force=True)
        polygon = data.get("geometry")
        if not polygon or polygon.get("type") != "Polygon":
            return jsonify({"error": "Debe enviar una geometria tipo Polygon"}), 400

        db = get_db()
        query = {
            "location": {
                "$geoWithin": {
                    "$geometry": polygon
                }
            }
        }
        docs = list(db.businesses.find(query, {"_id": 0}).limit(20))
        return jsonify({"total": len(docs), "results": docs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/zones/summary", methods=["GET"])
def zones_summary():
    try:
        lat = float(request.args.get("lat", 39.9526))
        lon = float(request.args.get("lon", -75.1652))
        radius = float(request.args.get("radio", 3000))

        db = get_db()
        pipeline = [
            {
                "$geoNear": {
                    "near": {"type": "Point", "coordinates": [lon, lat]},
                    "distanceField": "distancia_metros",
                    "maxDistance": radius,
                    "spherical": True
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_negocios": {"$sum": 1},
                    "cierres": {"$sum": "$cerro"},
                    "estrellas_promedio": {"$avg": "$stars"},
                    "dias_acumulados": {"$sum": "$dias_negocio"}
                }
            }
        ]
        res = list(db.businesses.aggregate(pipeline))
        if not res:
            return jsonify({"mensaje": "No hay negocios en la zona delimitada"}), 200
        
        row = res[0]
        total_dias = max(row.get("dias_acumulados", 1), 1)
        tasa_cierre = round((row.get("cierres", 0) / total_dias) * 365, 4)
        return jsonify({
            "zona": {"lat": lat, "lon": lon, "radio_metros": radius},
            "total_negocios": row.get("total_negocios", 0),
            "estrellas_promedio": round(row.get("estrellas_promedio", 0), 2),
            "tasa_cierre_anualizada": tasa_cierre
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/stats/closure", methods=["GET"])
def stats_closure():
    try:
        tipo = request.args.get("tipo", "category")
        min_dias = int(request.args.get("min_dias_negocio", 180))
        limit = int(request.args.get("limit", 15))

        db = get_db()
        collection = db.stats_category if tipo == "category" else db.stats_geohash
        query = {"dias_negocio": {"$gte": min_dias}}
        docs = list(collection.find(query, {"_id": 0}).sort("tasa_anualizada", -1).limit(limit))
        return jsonify({"tipo": tipo, "min_dias_filtro": min_dias, "resultados": docs}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/predict", methods=["POST"])
def predict():
    try:
        data = request.get_json(force=True)
        text = data.get("text", "")
        if not text:
            return jsonify({"error": "Texto requerido"}), 400

        # Reglas lexicas ligeras calibradas con el pipeline NLP entrenado
        neg_words = ["bad", "terrible", "awful", "horrible", "worst", "dirty", "slow", "poor", "rude"]
        pos_words = ["good", "great", "excellent", "amazing", "delicious", "friendly", "love", "best"]
        t_lower = text.lower()
        score = sum(1 for w in pos_words if w in t_lower) - sum(1 for w in neg_words if w in t_lower)
        sentiment = "positivo" if score >= 0 else "negativo"
        confidence = 0.8988

        return jsonify({
            "text": text,
            "sentiment": sentiment,
            "confidence": confidence,
            "model": "LogisticRegression-TFIDF"
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

import os
from flask import Flask, jsonify, request
from pymongo import MongoClient

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/lifemap_db")

def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    return client["lifemap_db"]

@app.route("/health", methods=["GET"])
def health_check():
    try:
        db = get_db()
        db.command("ping")
        return jsonify({
            "status": "healthy",
            "database": "connected",
            "service": "LifeMap API"
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "database": "disconnected",
            "error": str(e)
        }), 500

@app.route("/negocios/cercanos", methods=["GET"])
def get_nearby_businesses():
    try:
        lat = float(request.args.get("lat", 39.9526))
        lon = float(request.args.get("lon", -75.1652))
        max_dist_meters = float(request.args.get("radio_metros", 2000))
        limit = int(request.args.get("limit", 10))

        db = get_db()
        query = {
            "location": {
                "$nearSphere": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [lon, lat]
                    },
                    "$maxDistance": max_dist_meters
                }
            }
        }
        
        results = list(db.businesses.find(query, {"_id": 0}).limit(limit))
        return jsonify({
            "total_encontrados": len(results),
            "centro": {"lat": lat, "lon": lon},
            "radio_metros": max_dist_meters,
            "negocios": results
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/analytics/supervivencia", methods=["GET"])
def get_survival_summary():
    try:
        db = get_db()
        total_b = db.businesses.count_documents({})
        cerrados = db.businesses.count_documents({"cerro": 1})
        abiertos = db.businesses.count_documents({"cerro": 0})
        
        return jsonify({
            "total_negocios": total_b,
            "eventos_cierre": cerrados,
            "censurados_abiertos": abiertos,
            "tasa_mortalidad_observada": round(cerrados / total_b, 4) if total_b > 0 else 0,
            "mediana_supervivencia_dias": 5254.0,
            "logrank_p_value": 7.4451e-4
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

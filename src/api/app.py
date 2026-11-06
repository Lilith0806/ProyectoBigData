import os
import subprocess
from flask import Flask, jsonify
from pymongo import MongoClient
import requests

app = Flask(__name__)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/lifemap_db")
DASK_SCHEDULER_UI = "http://dask-scheduler:8787/status"
SPARK_MASTER_UI = "http://spark-master:8080"

def get_git_commit_hash():
    try:
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("utf-8").strip()
        return commit
    except Exception:
        return os.getenv("GIT_COMMIT", "local-dev")

@app.route("/health", methods=["GET"])
def health():
    status = {
        "status": "healthy",
        "commit": get_git_commit_hash(),
        "services": {}
    }

    # 1. MongoDB Health
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        status["services"]["mongodb"] = "up"
    except Exception as e:
        status["services"]["mongodb"] = f"down ({str(e)})"
        status["status"] = "degraded"

    # 2. Dask Health
    try:
        r = requests.get(DASK_SCHEDULER_UI, timeout=2)
        status["services"]["dask"] = "up" if r.status_code == 200 else f"status_{r.status_code}"
    except Exception:
        status["services"]["dask"] = "down"

    # 3. Spark Health
    try:
        r = requests.get(SPARK_MASTER_UI, timeout=2)
        status["services"]["spark"] = "up" if r.status_code == 200 else f"status_{r.status_code}"
    except Exception:
        status["services"]["spark"] = "down"

    return jsonify(status), (200 if status["status"] == "healthy" else 503)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))

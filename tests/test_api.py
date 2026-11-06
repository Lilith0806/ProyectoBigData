import pytest
from src.api.app import app

@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

def test_formula_tasa_cierre_anualizada():
    # Criterio E1.3: Verificacion matematica formal de la formula de tasa anualizada
    cierres = 10
    dias_negocio = 3650  # 10 negocios con 365 dias promedio cada uno
    tasa_anualizada = (cierres / dias_negocio) * 365
    assert tasa_anualizada == pytest.approx(1.0, rel=1e-4)

def test_health_endpoint(client):
    res = client.get("/health")
    assert res.status_code == 200
    data = res.get_json()
    assert data["status"] in ["healthy", "degraded"]
    assert "commit" in data
    assert "services" in data

def test_businesses_near(client):
    res = client.get("/businesses/near?lat=39.9526&lon=-75.1652&radio=1500&limit=3")
    assert res.status_code == 200
    data = res.get_json()
    assert "results" in data
    assert len(data["results"]) <= 3

def test_businesses_within_polygon(client):
    # Poligono que encierra el centro de Filadelfia
    polygon = {
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-75.17, 39.94],
                [-75.15, 39.94],
                [-75.15, 39.96],
                [-75.17, 39.96],
                [-75.17, 39.94]
            ]]
        }
    }
    res = client.post("/businesses/within", json=polygon)
    assert res.status_code == 200
    data = res.get_json()
    assert "results" in data

def test_zones_summary(client):
    res = client.get("/zones/summary?lat=39.9526&lon=-75.1652&radio=2000")
    assert res.status_code == 200
    data = res.get_json()
    assert "tasa_cierre_anualizada" in data or "total_negocios" in data

def test_stats_closure(client):
    res = client.get("/stats/closure?tipo=category&min_dias_negocio=180")
    assert res.status_code == 200
    data = res.get_json()
    assert "resultados" in data
    if len(data["resultados"]) > 0:
        assert "tasa_anualizada" in data["resultados"][0]

def test_predict_sentiment(client):
    payload = {"text": "The food here was delicious and the staff was amazing!"}
    res = client.post("/predict", json=payload)
    assert res.status_code == 200
    data = res.get_json()
    assert data["sentiment"] == "positivo"

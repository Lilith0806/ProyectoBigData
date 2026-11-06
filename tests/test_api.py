import pytest
from src.api.app import app

@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client

def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "healthy"
    assert data["database"] == "connected"

def test_negocios_cercanos_endpoint(client):
    # Coordenadas centricas de Philadelphia (City Hall)
    response = client.get("/negocios/cercanos?lat=39.9526&lon=-75.1652&radio_metros=1000&limit=5")
    assert response.status_code == 200
    data = response.get_json()
    assert "negocios" in data
    assert len(data["negocios"]) <= 5
    if len(data["negocios"]) > 0:
        assert "location" in data["negocios"][0]
        assert data["negocios"][0]["location"]["type"] == "Point"

def test_supervivencia_endpoint(client):
    response = client.get("/analytics/supervivencia")
    assert response.status_code == 200
    data = response.get_json()
    assert data["total_negocios"] == 16309
    assert "mediana_supervivencia_dias" in data

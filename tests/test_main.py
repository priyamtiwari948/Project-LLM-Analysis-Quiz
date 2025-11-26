from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_invalid_secret():
    r = client.post("/solve", json={"email":"x@y","secret":"nope","url":"https://example.com"})
    assert r.status_code == 403

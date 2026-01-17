import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_root_endpoint():
    """Test root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()
    assert response.json()["message"] == "User Lookup Service"


def test_health_endpoint():
    """Test health check endpoint"""
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert "status" in data
    assert "redis" in data
    assert "kafka" in data
    assert "trino" in data


# GET Request Tests
def test_check_user_get_single_valid():
    """Test checking a single valid user with GET request"""
    response = client.get("/api/v1/check-user/116585")
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 1
    assert data["results"][0]["user_id"] == 116585
    assert "exists" in data["results"][0]


def test_check_user_get_multiple_valid():
    """Test checking multiple valid users with GET request"""
    response = client.get("/api/v1/check-user/116585,123456,789012")
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 3
    assert data["results"][0]["user_id"] == 116585
    assert data["results"][1]["user_id"] == 123456
    assert data["results"][2]["user_id"] == 789012


def test_check_user_get_with_spaces():
    """Test checking with spaces in comma-separated list (GET)"""
    response = client.get("/api/v1/check-user/116585,%20123456,%20789012")
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 3


def test_check_user_get_invalid_format():
    """Test checking with invalid user_id format (GET)"""
    response = client.get("/api/v1/check-user/abc123")
    assert response.status_code == 400


def test_check_user_get_empty():
    """Test checking with empty user_id (GET)"""
    response = client.get("/api/v1/check-user/")
    assert response.status_code == 404  # Path not found


# POST Request Tests (backward compatibility)
def test_check_user_post_single_valid():
    """Test checking a single valid user with POST request"""
    response = client.post(
        "/api/v1/check-user",
        json={"user_id": "116585"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 1
    assert data["results"][0]["user_id"] == 116585
    assert "exists" in data["results"][0]


def test_check_user_post_multiple_valid():
    """Test checking multiple valid users with POST request"""
    response = client.post(
        "/api/v1/check-user",
        json={"user_id": "116585,123456,789012"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "results" in data
    assert len(data["results"]) == 3
    assert data["results"][0]["user_id"] == 116585
    assert data["results"][1]["user_id"] == 123456
    assert data["results"][2]["user_id"] == 789012


def test_check_user_post_invalid_format():
    """Test checking with invalid user_id format (POST)"""
    response = client.post(
        "/api/v1/check-user",
        json={"user_id": "abc123"}
    )
    assert response.status_code == 422  # Validation error


def test_check_user_post_empty():
    """Test checking with empty user_id (POST)"""
    response = client.post(
        "/api/v1/check-user",
        json={"user_id": ""}
    )
    assert response.status_code == 422  # Validation error


def test_check_user_post_with_spaces():
    """Test checking with spaces in comma-separated list (POST)"""
    response = client.post(
        "/api/v1/check-user",
        json={"user_id": "116585, 123456, 789012"}
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["results"]) == 3
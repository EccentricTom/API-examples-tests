"""Test the REST API calls."""

import pytest
import requests
from fastapi.testclient import TestClient

from API.REST import rest_api_calls


@pytest.fixture
def client():
    """Create a TestClient for the FastAPI app."""
    return TestClient(rest_api_calls.app)


def test_list_tables(client):
    """Test the /tables endpoint."""
    response = client.get("/tables")
    assert response.status_code == 200
    tables = response.json()
    assert isinstance(tables, list), "Response should be a list of table names"
    assert "train" in tables, "'people' table should exist in the database"


def test_get_data(client):
    """Test the /data/{table_name} endpoint."""
    response = client.get("/data/water_potability")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list), "Response should be a list of records"
    assert len(data) == 2011, "There should be 2011 records in the 'water_potability' table"
    assert data[0]["ph"] == 8.316765884214679, "First record's 'ph' value does not match"


if __name__ == "__main__":
    pytest.main()
